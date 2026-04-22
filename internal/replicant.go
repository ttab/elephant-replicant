package internal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/ttab/elephant-api/replicant"
	"github.com/ttab/elephant-api/repository"
	"github.com/ttab/elephant-replicant/postgres"
	"github.com/ttab/elephantine"
	"github.com/ttab/elephantine/pg"
	"github.com/ttab/koonkie"
	"github.com/twitchtv/twirp"
)

// DefaultTargetConfig holds the configuration for the default target built from
// environment variables.
type DefaultTargetConfig struct {
	RepositoryURL      string
	OIDCConfig         string
	ClientID           string
	ClientSecret       string //nolint: gosec
	StartFrom          int64
	IgnoreTypes        []string
	IgnoreSubs         []string
	IgnoreSections     []string
	IncludeAttachments []AttachmentRef
	AllAttachments     bool
	AcceptErrors       bool
}

type Parameters struct {
	Server            *elephantine.APIServer
	Logger            *slog.Logger
	Database          *pgxpool.Pool
	Documents         repository.Documents
	MetricsRegisterer prometheus.Registerer
	AuthInfoParser    elephantine.AuthInfoParser
	CORSHosts         []string
	DefaultTarget     *DefaultTargetConfig
	EncryptionKey     []byte
}

var (
	ErrSkipped  = errors.New("skipped event")
	ErrConflict = errors.New("document has been updated in target")
)

type AttachmentRef struct {
	DocType string
	Name    string
}

func AttachmentRefFromString(str string) (AttachmentRef, error) {
	name, docType, ok := strings.Cut(str, ".")
	if !ok {
		return AttachmentRef{}, fmt.Errorf(
			"invalid attachment reference %q", str)
	}

	return AttachmentRef{
		DocType: docType,
		Name:    name,
	}, nil
}

type LogState struct {
	CaughtUp bool
	Position int64
}

const (
	TypeDocumentVersion = "document"
	TypeNewStatus       = "status"
	TypeACLUpdate       = "acl"
	TypeDeleteDocument  = "delete_document"
	TypeRestoreFinished = "restore_finished"
	TypeWorkflow        = "workflow"
)

func Run(ctx context.Context, p Parameters) error {
	grace := elephantine.NewGracefulShutdown(p.Logger, 10*time.Second)

	logMetrics, err := koonkie.NewPrometheusFollowerMetrics(
		p.MetricsRegisterer, "replicant_follower")
	if err != nil {
		return fmt.Errorf("set up log follower metrics: %w", err)
	}

	fanOut := pg.NewFanOut[TargetNotification](TargetNotifyChannel)

	err = registerDefaultTarget(ctx, p)
	if err != nil {
		return fmt.Errorf("register default target: %w", err)
	}

	manager := NewTargetManager(
		p.Logger, p.Database, p.Documents, logMetrics, p.EncryptionKey,
	)

	notifications := make(chan TargetNotification, 16)

	go fanOut.ListenAll(ctx, notifications)

	app := Application{
		logger:        p.Logger,
		db:            p.Database,
		fanOut:        fanOut,
		manager:       manager,
		encryptionKey: p.EncryptionKey,
	}

	opts, err := elephantine.NewDefaultServiceOptions(
		p.Logger, p.AuthInfoParser, prometheus.DefaultRegisterer,
		elephantine.ServiceAuthRequired)
	if err != nil {
		return fmt.Errorf("set up service config: %w", err)
	}

	service := replicant.NewReplicationServer(&app,
		twirp.WithServerJSONSkipDefaults(true),
		twirp.WithServerHooks(opts.Hooks),
	)

	p.Server.RegisterAPI(service, opts)

	group := elephantine.NewErrGroup(ctx, p.Logger)

	group.Go("target-manager", func(ctx context.Context) error {
		return manager.Run(grace.CancelOnStop(ctx), notifications)
	})

	group.Go("pg-subscribe", func(ctx context.Context) error {
		pg.Subscribe(grace.CancelOnStop(ctx), p.Logger, p.Database, fanOut) //nolint:staticcheck

		return nil
	})

	group.Go("server", func(ctx context.Context) error {
		return p.Server.ListenAndServe(grace.CancelOnQuit(ctx))
	})

	group.Go("cleanup", func(ctx context.Context) error {
		return mappingCleanup(grace.CancelOnStop(ctx), p.Database)
	})

	return group.Wait() //nolint: wrapcheck
}

func registerDefaultTarget(ctx context.Context, p Parameters) error {
	if p.DefaultTarget == nil {
		return nil
	}

	q := postgres.New(p.Database)

	exists, err := q.TargetExists(ctx, "default")
	if err != nil {
		return fmt.Errorf("check if default target exists: %w", err)
	}

	if exists {
		return nil
	}

	dt := p.DefaultTarget

	syncConfig := replicant.SyncConfig{
		AcceptErrors:   dt.AcceptErrors,
		AllAttachments: dt.AllAttachments,
		IgnoreTypes:    dt.IgnoreTypes,
		IgnoreSubs:     dt.IgnoreSubs,
	}

	for _, s := range dt.IgnoreSections {
		docType, sectionUUID, ok := strings.Cut(s, ":")
		if !ok {
			return fmt.Errorf("invalid section filter %q", s)
		}

		syncConfig.IgnoreSections = append(syncConfig.IgnoreSections,
			&replicant.SectionForType{
				Type:        docType,
				SectionUuid: sectionUUID,
			})
	}

	for _, a := range dt.IncludeAttachments {
		syncConfig.IncludeAttachments = append(syncConfig.IncludeAttachments,
			&replicant.AttachmentForType{
				Type: a.DocType,
				Name: a.Name,
			})
	}

	configJSON, err := json.Marshal(&syncConfig)
	if err != nil {
		return fmt.Errorf("marshal sync config: %w", err)
	}

	encryptedSecret, err := EncryptSecret(p.EncryptionKey, dt.ClientSecret)
	if err != nil {
		return fmt.Errorf("encrypt client secret: %w", err)
	}

	err = q.UpsertTarget(ctx, postgres.UpsertTargetParams{
		Name:          "default",
		RepositoryUrl: dt.RepositoryURL,
		OidcConfig:    dt.OIDCConfig,
		ClientID:      dt.ClientID,
		ClientSecret:  encryptedSecret,
		StartFrom:     dt.StartFrom,
		Config:        configJSON,
		Enabled:       true,
	})
	if err != nil {
		return fmt.Errorf("upsert default target: %w", err)
	}

	p.Logger.Info("registered default target from environment variables")

	return nil
}

func mappingCleanup(ctx context.Context, db *pgxpool.Pool) error {
	for {
		run := time.After(1 * time.Hour)

		select {
		case <-ctx.Done():
			return ctx.Err() //nolint: wrapcheck
		case <-run:
		}

		q := postgres.New(db)

		err := q.RemoveOldMappings(ctx, pg.Time(
			time.Now().AddDate(0, -6, 0)))
		if err != nil {
			return fmt.Errorf("remove old mappings: %w", err)
		}
	}
}

var _ replicant.Replication = &Application{}

type Application struct {
	logger        *slog.Logger
	db            *pgxpool.Pool
	fanOut        *pg.FanOut[TargetNotification]
	manager       *TargetManager
	encryptionKey []byte
}

// SendDocument implements replicant.Replication.
func (a *Application) SendDocument(
	ctx context.Context, _ *replicant.SendDocumentRequest,
) (*replicant.SendDocumentResponse, error) {
	_, err := elephantine.RequireAnyScope(ctx, "doc_admin", "doc_write")
	if err != nil {
		return nil, err
	}

	return nil, twirp.NewError(twirp.Unimplemented, "soon")
}

// ConfigureTarget implements replicant.Replication.
func (a *Application) ConfigureTarget(
	ctx context.Context, req *replicant.ConfigureTargetRequest,
) (*replicant.ConfigureTargetResponse, error) {
	_, err := elephantine.RequireAnyScope(ctx, "doc_admin")
	if err != nil {
		return nil, err
	}

	if req.GetName() == "" {
		return nil, elephantine.InvalidArgumentf("name", "must not be empty")
	}

	if req.GetRepositoryUrl() == "" {
		return nil, elephantine.InvalidArgumentf("repository_url", "must not be empty")
	}

	if req.GetOidcConfig() == "" {
		return nil, elephantine.InvalidArgumentf("oidc_config", "must not be empty")
	}

	if req.GetClientId() == "" {
		return nil, elephantine.InvalidArgumentf("client_id", "must not be empty")
	}

	if req.GetClientSecret() == "" {
		return nil, elephantine.InvalidArgumentf("client_secret", "must not be empty")
	}

	syncConfig := req.GetConfig()
	if syncConfig == nil {
		syncConfig = &replicant.SyncConfig{}
	}

	configJSON, err := json.Marshal(syncConfig)
	if err != nil {
		return nil, fmt.Errorf("marshal sync config: %w", err)
	}

	encryptedSecret, err := EncryptSecret(a.encryptionKey, req.GetClientSecret())
	if err != nil {
		return nil, fmt.Errorf("encrypt client secret: %w", err)
	}

	err = postgres.New(a.db).UpsertTarget(ctx, postgres.UpsertTargetParams{
		Name:          req.GetName(),
		RepositoryUrl: req.GetRepositoryUrl(),
		OidcConfig:    req.GetOidcConfig(),
		ClientID:      req.GetClientId(),
		ClientSecret:  encryptedSecret,
		StartFrom:     req.GetStartFrom(),
		Config:        configJSON,
		Enabled:       true,
	})
	if err != nil {
		return nil, fmt.Errorf("upsert target: %w", err)
	}

	err = a.fanOut.Publish(ctx, a.db, TargetNotification{
		Name:   req.GetName(),
		Action: TargetActionConfigure,
	})
	if err != nil {
		return nil, fmt.Errorf("publish configure notification: %w", err)
	}

	return &replicant.ConfigureTargetResponse{}, nil
}

// RemoveTarget implements replicant.Replication.
func (a *Application) RemoveTarget(
	ctx context.Context, req *replicant.RemoveTargetRequest,
) (*replicant.RemoveTargetResponse, error) {
	_, err := elephantine.RequireAnyScope(ctx, "doc_admin")
	if err != nil {
		return nil, err
	}

	if req.GetName() == "" {
		return nil, elephantine.InvalidArgumentf("name", "must not be empty")
	}

	q := postgres.New(a.db)

	err = q.DeleteTarget(ctx, req.GetName())
	if err != nil {
		return nil, fmt.Errorf("delete target: %w", err)
	}

	stateKey := req.GetName() + ":log_state"

	err = q.RemoveTargetData(ctx, req.GetName())
	if err != nil {
		return nil, fmt.Errorf("remove target data: %w", err)
	}

	err = q.RemoveTargetMappings(ctx, req.GetName())
	if err != nil {
		return nil, fmt.Errorf("remove target mappings: %w", err)
	}

	err = q.RemoveTargetState(ctx, stateKey)
	if err != nil {
		return nil, fmt.Errorf("remove target state: %w", err)
	}

	err = a.fanOut.Publish(ctx, a.db, TargetNotification{
		Name:   req.GetName(),
		Action: TargetActionRemove,
	})
	if err != nil {
		return nil, fmt.Errorf("publish remove notification: %w", err)
	}

	return &replicant.RemoveTargetResponse{}, nil
}

// ChangeTargetState implements replicant.Replication.
func (a *Application) ChangeTargetState(
	ctx context.Context, req *replicant.ChangeTargetStateRequest,
) (*replicant.ChangeTargetStateResponse, error) {
	_, err := elephantine.RequireAnyScope(ctx, "doc_admin")
	if err != nil {
		return nil, err
	}

	if req.GetName() == "" {
		return nil, elephantine.InvalidArgumentf("name", "must not be empty")
	}

	q := postgres.New(a.db)

	exists, err := q.TargetExists(ctx, req.GetName())
	if err != nil {
		return nil, fmt.Errorf("check target exists: %w", err)
	}

	if !exists {
		return nil, twirp.NewError(twirp.NotFound, "target not found")
	}

	var (
		enabled bool
		action  string
	)

	switch req.GetAction() {
	case replicant.TargetAction_TARGET_ACTION_START:
		enabled = true
		action = TargetActionStart
	case replicant.TargetAction_TARGET_ACTION_STOP:
		enabled = false
		action = TargetActionStop
	case replicant.TargetAction_TARGET_ACTION_UNSPECIFIED:
		return nil, elephantine.InvalidArgumentf("action", "must be start or stop")
	}

	err = q.SetTargetEnabled(ctx, postgres.SetTargetEnabledParams{
		Name:    req.GetName(),
		Enabled: enabled,
	})
	if err != nil {
		return nil, fmt.Errorf("set target enabled: %w", err)
	}

	err = a.fanOut.Publish(ctx, a.db, TargetNotification{
		Name:   req.GetName(),
		Action: action,
	})
	if err != nil {
		return nil, fmt.Errorf("publish state change notification: %w", err)
	}

	return &replicant.ChangeTargetStateResponse{}, nil
}

// ListTargets implements replicant.Replication.
func (a *Application) ListTargets(
	ctx context.Context, _ *replicant.ListTargetsRequest,
) (*replicant.ListTargetsResponse, error) {
	_, err := elephantine.RequireAnyScope(ctx, "doc_admin")
	if err != nil {
		return nil, err
	}

	rows, err := postgres.New(a.db).ListTargets(ctx)
	if err != nil {
		return nil, fmt.Errorf("list targets: %w", err)
	}

	targets := make([]*replicant.TargetInfo, 0, len(rows))

	for _, r := range rows {
		targets = append(targets, &replicant.TargetInfo{
			Name:          r.Name,
			RepositoryUrl: r.RepositoryUrl,
			Enabled:       r.Enabled,
		})
	}

	return &replicant.ListTargetsResponse{
		Targets: targets,
	}, nil
}

// GetTargetState implements replicant.Replication.
func (a *Application) GetTargetState(
	ctx context.Context, req *replicant.GetTargetStateRequest,
) (*replicant.GetTargetStateResponse, error) {
	_, err := elephantine.RequireAnyScope(ctx, "doc_admin")
	if err != nil {
		return nil, err
	}

	if req.GetName() == "" {
		return nil, elephantine.InvalidArgumentf("name", "must not be empty")
	}

	exists, err := postgres.New(a.db).TargetExists(ctx, req.GetName())
	if err != nil {
		return nil, fmt.Errorf("check target exists: %w", err)
	}

	if !exists {
		return nil, twirp.NewError(twirp.NotFound, "target not found")
	}

	state := a.manager.GetWorkerState(req.GetName())

	return &replicant.GetTargetStateResponse{
		State: state,
	}, nil
}
