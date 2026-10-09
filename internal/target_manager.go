package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/ttab/elephant-api/replicant"
	"github.com/ttab/elephant-api/repository"
	"github.com/ttab/elephant-api/repository/repositoryconnect"
	"github.com/ttab/elephant-replicant/postgres"
	"github.com/ttab/elephantine"
	"github.com/ttab/elephantine/pg/joblock"
	"github.com/ttab/koonkie"
	"golang.org/x/oauth2"
)

// workerGiveUpAfter is how long a target's worker may spend failing before the
// job lock gives up on it instead of restarting it again. The failure modes
// that reach it are all external to the target itself — the target repository
// unreachable, its credentials rejected, our own database down — so the budget
// has to be long enough to sit out an incident at the other end: an hour is
// twelve of the five minute default HealthyRuntime, and a target that has been
// failing continuously for that long is something a human has to look at.
//
// Giving up stops replication for that target until the process restarts or
// the target is reconfigured, which is why this is an hour and not the handful
// of minutes the migration guide uses as its example.
const workerGiveUpAfter = time.Hour

type targetWorker struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// TargetManager manages all target worker goroutines. It loads enabled targets
// from the database on startup and listens for change notifications to
// start/stop/reconfigure workers.
type TargetManager struct {
	logger        *slog.Logger
	db            *pgxpool.Pool
	source        repository.Documents
	logMetrics    *koonkie.PrometheusFollowerMetrics
	encryptionKey []byte

	mu      sync.Mutex
	workers map[string]*targetWorker
	// runCtx is the context Run was given, which every worker is started
	// from. Reconcile needs it so that a worker it starts outlives the
	// subscriber's connection attempt that called it.
	runCtx context.Context
}

// NewTargetManager creates a new target manager.
func NewTargetManager(
	logger *slog.Logger,
	db *pgxpool.Pool,
	source repository.Documents,
	logMetrics *koonkie.PrometheusFollowerMetrics,
	encryptionKey []byte,
) *TargetManager {
	return &TargetManager{
		logger:        logger,
		db:            db,
		source:        source,
		logMetrics:    logMetrics,
		encryptionKey: encryptionKey,
		workers:       make(map[string]*targetWorker),
	}
}

// Run loads all enabled targets, starts workers for them, and then listens for
// notifications to manage the workers.
func (tm *TargetManager) Run(
	ctx context.Context, notifications <-chan TargetNotification,
) error {
	tm.mu.Lock()
	tm.runCtx = ctx
	tm.mu.Unlock()

	err := tm.Reconcile(ctx)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			tm.stopAll()

			return ctx.Err()
		case n := <-notifications:
			tm.handleNotification(ctx, n)
		}
	}
}

// Reconcile brings the running workers in step with the table: it starts a
// worker for every enabled target that has none and stops the workers of
// targets that are disabled or gone. Run calls it on start, and the LISTEN
// subscriber calls it before its first listen and after every reconnect,
// since a notification published while the connection was dead is lost. A
// target whose row changed while its worker kept running is not detected;
// that is what a repeated ConfigureTarget is for.
//
// Before Run has started, there is nothing to reconcile against and Run will
// do the first pass itself.
func (tm *TargetManager) Reconcile(ctx context.Context) error {
	tm.mu.Lock()
	runCtx := tm.runCtx
	tm.mu.Unlock()

	if runCtx == nil {
		return nil
	}

	targets, err := postgres.New(tm.db).ListEnabledTargets(ctx)
	if err != nil {
		return fmt.Errorf("list enabled targets: %w", err)
	}

	enabled := make(map[string]bool, len(targets))

	for _, t := range targets {
		enabled[t.Name] = true
	}

	tm.mu.Lock()
	running := slices.Collect(maps.Keys(tm.workers))
	tm.mu.Unlock()

	var started, stopped int

	for _, name := range running {
		if enabled[name] {
			continue
		}

		tm.logger.Info("stopping worker for a target that is no longer enabled",
			"target", name)
		tm.stopWorker(name)

		stopped++
	}

	for _, t := range targets {
		if tm.startWorker(runCtx, t.Name) {
			tm.logger.Info("starting worker", "target", t.Name)

			started++
		}
	}

	tm.logger.Info("reconciled workers with the enabled targets",
		"enabled", len(targets),
		"started", started,
		"stopped", stopped,
	)

	return nil
}

func (tm *TargetManager) handleNotification(
	ctx context.Context, n TargetNotification,
) {
	tm.logger.Info("received target notification",
		"target", n.Name,
		"action", n.Action,
	)

	switch n.Action {
	case TargetActionConfigure:
		tm.stopWorker(n.Name)
		tm.startWorker(ctx, n.Name)
	case TargetActionRemove:
		tm.stopWorker(n.Name)
	case TargetActionStart:
		tm.startWorker(ctx, n.Name)
	case TargetActionStop:
		tm.stopWorker(n.Name)
	}
}

// startWorker starts a worker for the target unless one is running, and
// reports whether it did.
func (tm *TargetManager) startWorker(ctx context.Context, name string) bool {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if _, exists := tm.workers[name]; exists {
		return false
	}

	workerCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})

	tw := &targetWorker{
		cancel: cancel,
		done:   done,
	}

	tm.workers[name] = tw

	go func() {
		defer cancel()
		defer close(done)

		tm.runWorker(workerCtx, name)

		// The job lock can give up, and then the worker is gone while
		// its entry is still in the map, making a later start
		// notification a no-op. Drop the entry so the target can be
		// started again.
		tm.forgetWorker(name, tw)
	}()

	return true
}

func (tm *TargetManager) runWorker(ctx context.Context, name string) {
	logger := tm.logger.With("target", name)

	err := joblock.Run(
		ctx, tm.db, logger,
		"replicant:"+name, "replicant:"+name,
		joblock.Options{
			GiveUpAfter: workerGiveUpAfter,
		},
		func(ctx context.Context) error {
			return tm.workerFunc(ctx, logger, name)
		},
	)
	if err != nil && ctx.Err() == nil {
		logger.Error("worker exited with error",
			elephantine.LogKeyError, err,
		)
	}
}

func (tm *TargetManager) workerFunc(
	ctx context.Context, logger *slog.Logger, name string,
) error {
	q := postgres.New(tm.db)

	target, err := q.GetTarget(ctx, name)
	if err != nil {
		return fmt.Errorf("load target config: %w", err)
	}

	var syncConfig replicant.SyncConfig

	err = json.Unmarshal(target.Config, &syncConfig)
	if err != nil {
		return fmt.Errorf("unmarshal sync config: %w", err)
	}

	clientSecret, err := DecryptSecret(tm.encryptionKey, target.ClientSecret)
	if err != nil {
		return fmt.Errorf("decrypt client secret: %w", err)
	}

	auth, err := elephantine.AuthenticationConfigFromSettings(
		ctx,
		elephantine.AuthenticationSettings{
			OIDCConfig:   target.OidcConfig,
			ClientID:     target.ClientID,
			ClientSecret: clientSecret,
		},
		[]string{"doc_admin"},
	)
	if err != nil {
		return fmt.Errorf("set up target authentication: %w", err)
	}

	targetClient := oauth2.NewClient(ctx, auth.TokenSource)

	targetDocs := repositoryconnect.NewDocumentsServiceClient(
		targetClient, target.RepositoryUrl,
	)

	cFilter, err := NewContentFilterFromSyncConfig(&syncConfig)
	if err != nil {
		return fmt.Errorf("create content filter: %w", err)
	}

	var state LogState

	stateKey := name + ":log_state"

	err = LoadState(ctx, q, stateKey, &state)
	if err != nil {
		return fmt.Errorf("load log state: %w", err)
	}

	state.Position = max(state.Position, target.StartFrom)

	if syncConfig.AllAttachments && len(syncConfig.IncludeAttachments) > 0 {
		logger.Warn(
			"running with both 'all-attachments' and 'include-attachments', all attachments will be included")
	}

	logger.Info("starting replication",
		elephantine.LogKeyEventID, state.Position)

	lf := koonkie.NewLogFollower(tm.source, koonkie.FollowerOptions{
		Metrics:      tm.logMetrics.WithName(name),
		StartAfter:   state.Position,
		CaughtUp:     state.CaughtUp,
		WaitDuration: 10 * time.Second,
	})

	w := &Worker{
		name:           name,
		logger:         logger,
		db:             tm.db,
		source:         tm.source,
		target:         targetDocs,
		cFilter:        cFilter,
		lf:             lf,
		acceptErrors:   syncConfig.AcceptErrors,
		ignoreSubs:     syncConfig.IgnoreSubs,
		ignoreTypes:    syncConfig.IgnoreTypes,
		allAttachments: syncConfig.AllAttachments,
		incAttachments: attachmentRefsFromProto(syncConfig.IncludeAttachments),
	}

	return w.Replicate(ctx)
}

// forgetWorker removes a worker's entry, but only if it is still the entry for
// the running worker: stopWorker may already have removed it, and a
// reconfigure may have replaced it with a new one.
func (tm *TargetManager) forgetWorker(name string, tw *targetWorker) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if tm.workers[name] == tw {
		delete(tm.workers, name)
	}
}

func (tm *TargetManager) stopWorker(name string) {
	tm.mu.Lock()
	tw, exists := tm.workers[name]

	if !exists {
		tm.mu.Unlock()

		return
	}

	delete(tm.workers, name)

	tm.mu.Unlock()

	tw.cancel()
	<-tw.done
}

func (tm *TargetManager) stopAll() {
	tm.mu.Lock()
	workers := maps.Clone(tm.workers)

	tm.workers = make(map[string]*targetWorker)

	tm.mu.Unlock()

	for _, tw := range workers {
		tw.cancel()
	}

	for _, tw := range workers {
		<-tw.done
	}
}

// GetWorkerState returns the proto TargetState for the named target.
func (tm *TargetManager) GetWorkerState(name string) replicant.TargetState {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	_, exists := tm.workers[name]
	if !exists {
		return replicant.TargetState_TARGET_STATE_STOPPED
	}

	return replicant.TargetState_TARGET_STATE_RUNNING
}

func attachmentRefsFromProto(
	attachments []*replicant.AttachmentForType,
) []AttachmentRef {
	refs := make([]AttachmentRef, 0, len(attachments))

	for _, a := range attachments {
		refs = append(refs, AttachmentRef{
			DocType: a.Type,
			Name:    a.Name,
		})
	}

	return refs
}

// MetricsRegisterer returns the registerer to use for registering metrics.
// Exposed for use during application setup.
func MetricsRegisterer() prometheus.Registerer {
	return prometheus.DefaultRegisterer
}
