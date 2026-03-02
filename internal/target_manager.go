package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/ttab/elephant-api/replicant"
	"github.com/ttab/elephant-api/repository"
	"github.com/ttab/elephant-replicant/postgres"
	"github.com/ttab/elephantine"
	"github.com/ttab/elephantine/pg"
	"github.com/ttab/koonkie"
	"golang.org/x/oauth2"
)

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
	q := postgres.New(tm.db)

	targets, err := q.ListEnabledTargets(ctx)
	if err != nil {
		return fmt.Errorf("list enabled targets: %w", err)
	}

	tm.logger.Info("found enabled targets", "count", len(targets))

	for _, t := range targets {
		tm.logger.Info("starting worker", "target", t.Name)
		tm.startWorker(ctx, t.Name)
	}

	for {
		select {
		case <-ctx.Done():
			tm.stopAll()

			return ctx.Err() //nolint: wrapcheck
		case n := <-notifications:
			tm.handleNotification(ctx, n)
		}
	}
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

func (tm *TargetManager) startWorker(ctx context.Context, name string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if _, exists := tm.workers[name]; exists {
		return
	}

	workerCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})

	tw := &targetWorker{
		cancel: cancel,
		done:   done,
	}

	tm.workers[name] = tw

	go func() {
		defer close(done)

		tm.runWorker(workerCtx, name)
	}()
}

func (tm *TargetManager) runWorker(ctx context.Context, name string) {
	logger := tm.logger.With("target", name)

	err := pg.RunInJobLock(
		ctx, tm.db, logger,
		"replicant:"+name, "replicant:"+name,
		pg.JobLockOptions{},
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

	targetDocs := repository.NewDocumentsProtobufClient(
		target.RepositoryUrl, targetClient,
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
	workers := make(map[string]*targetWorker, len(tm.workers))

	for k, v := range tm.workers {
		workers[k] = v
	}

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
