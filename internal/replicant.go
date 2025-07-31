package internal

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
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

type Parameters struct {
	Server             *elephantine.APIServer
	Logger             *slog.Logger
	Database           *pgxpool.Pool
	Documents          repository.Documents
	TargetDocuments    repository.Documents
	MinEventID         int64
	MetricsRegisterer  prometheus.Registerer
	AuthInfoParser     elephantine.AuthInfoParser
	CORSHosts          []string
	IgnoreTypes        []string
	IgnoreCreators     []string
	IgnoreSubs         []string
	IncludeAttachments []AttachmentRef
	AllAttachments     bool
	StartEvent         int64
}

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

func Run(ctx context.Context, p Parameters) error {
	grace := elephantine.NewGracefulShutdown(p.Logger, 10*time.Second)

	var state LogState

	err := LoadState(ctx, postgres.New(p.Database), "log_state", &state)
	if err != nil {
		return fmt.Errorf("load log state: %w", err)
	}

	state.Position = min(state.Position, p.MinEventID)

	logMetrics, err := koonkie.NewPrometheusFollowerMetrics(
		p.MetricsRegisterer, "replicant_follower")
	if err != nil {
		return fmt.Errorf("set up log follower metrics: %w", err)
	}

	lf := koonkie.NewLogFollower(p.Documents, koonkie.FollowerOptions{
		Metrics:      logMetrics,
		StartAfter:   state.Position,
		CaughtUp:     state.CaughtUp,
		WaitDuration: 10 * time.Second,
	})

	app := Application{
		p:  p,
		lf: lf,
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

	group.Go("replicator", func(ctx context.Context) error {
		return app.Replicate(grace.CancelOnStop(ctx))
	})

	group.Go("server", func(ctx context.Context) error {
		return p.Server.ListenAndServe(grace.CancelOnQuit(ctx))
	})

	return group.Wait()
}

var _ replicant.Replication = &Application{}

type Application struct {
	p  Parameters
	lf *koonkie.LogFollower
}

// SendDocument implements replicant.Replication.
func (a *Application) SendDocument(
	ctx context.Context, req *replicant.SendDocumentRequest,
) (*replicant.SendDocumentResponse, error) {
	_, err := elephantine.RequireAnyScope(ctx, "doc_admin", "doc_write")
	if err != nil {
		return nil, err
	}

	return nil, twirp.NewError(twirp.Unimplemented, "soon")
}

func (a *Application) Replicate(ctx context.Context) error {
	for {
		var lastSaved int64

		pos, caughtUp := a.lf.GetState()

		items, err := a.lf.GetNext(ctx)
		if err != nil {
			return fmt.Errorf("failed to read eventlog: %w", err)
		}

		for _, item := range items {
			pos = item.Id

			_, err := a.handleEvent(ctx, item, caughtUp)
			if errors.Is(err, ErrSkipped) {
				continue
			} else if err != nil {
				return fmt.Errorf("handle event %d: %w", item.Id, err)
			}

			lastSaved = pos
		}

		if lastSaved != pos {
			err = StoreState(ctx, postgres.New(a.p.Database), "log_state", LogState{
				Position: pos,
				CaughtUp: caughtUp,
			})
			if err != nil {
				return fmt.Errorf("persist log state: %w", err)
			}
		}
	}
}

func (a *Application) handleEvent(
	ctx context.Context, evt *repository.EventlogItem, caughtUp bool,
) (_ int64, outErr error) {
	docUUID := uuid.MustParse(evt.Uuid)

	if evt.Event == "workflow" {
		return 0, ErrSkipped
	}

	if slices.Contains(a.p.IgnoreSubs, evt.UpdaterUri) {
		return 0, ErrSkipped
	}

	if slices.Contains(a.p.IgnoreTypes, evt.Type) {
		return 0, ErrSkipped
	}

	tx, err := a.p.Database.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin transaction: %w", err)
	}

	defer pg.Rollback(tx, &outErr)

	q := postgres.New(tx)

	var isNew bool

	targetVersion, err := q.GetDocumentVersion(ctx, docUUID)
	if errors.Is(err, pgx.ErrNoRows) {
		isNew = true
	} else if err != nil {
		return 0, fmt.Errorf("get current target version: %w", err)
	}

	update := repository.UpdateRequest{
		Uuid: evt.Uuid,
		ImportDirective: &repository.ImportDirective{
			OriginallyCreated: evt.Timestamp,
			OriginalCreator:   evt.UpdaterUri,
		},
	}

	updateType := evt.Type

	if !caughtUp {
		// If we're not caught up we might just get one event per
		// document, so we'll need to fill in the blanks here.
		updateType = "document"

		metaRes, err := a.p.Documents.GetMeta(ctx,
			&repository.GetMetaRequest{
				Uuid: evt.Uuid,
			})
		if elephantine.IsTwirpErrorCode(err, twirp.NotFound) {
			// Just ignore deleted documents.
			return 0, ErrSkipped
		} else if err != nil {
			return 0, fmt.Errorf("get source meta: %w", err)
		}

		evt.Version = metaRes.Meta.CurrentVersion
		update.Acl = metaRes.Meta.Acl

		// Replace import directive with original creation info.
		update.ImportDirective = &repository.ImportDirective{
			OriginallyCreated: metaRes.Meta.Created,
			OriginalCreator:   metaRes.Meta.CreatorUri,
		}

		for status, info := range metaRes.Meta.Heads {
			// We only set statuses that refer to the version we're
			// replicating. History will be truncated while we're
			// catching up.
			if info.Version != metaRes.Meta.CurrentVersion {
				continue
			}

			update.Status = append(update.Status,
				&repository.StatusUpdate{
					Name: status,
					Meta: info.Meta,
				})
		}
	}

	switch updateType {
	case "document":
		docRes, err := a.p.Documents.Get(ctx,
			&repository.GetDocumentRequest{
				Uuid:    evt.Uuid,
				Version: evt.Version,
			})
		if elephantine.IsTwirpErrorCode(err, twirp.NotFound) {
			// Just ignore deleted documents.
			return 0, ErrSkipped
		} else if err != nil {
			return 0, fmt.Errorf("get source document: %w", err)
		}

		update.Document = docRes.Document
	case "status":
		mappedVersion, err := q.GetTargetVersion(ctx,
			postgres.GetTargetVersionParams{
				ID:            docUUID,
				SourceVersion: evt.Version,
			})
		if errors.Is(err, pgx.ErrNoRows) {
			// No record of the version that the status refers to, skip.
			return 0, ErrSkipped
		}

		statusRes, err := a.p.Documents.GetStatus(ctx, &repository.GetStatusRequest{
			Uuid: evt.Uuid,
			Name: evt.Status,
			Id:   evt.StatusId,
		})
		if elephantine.IsTwirpErrorCode(err, twirp.NotFound) {
			// Just ignore deleted documents.
			return 0, ErrSkipped
		} else if err != nil {
			return 0, fmt.Errorf("get source status: %w", err)
		}

		update.Status = append(update.Status, &repository.StatusUpdate{
			Name:    evt.Status,
			Version: mappedVersion,
			Meta:    statusRes.Status.Meta,
		})
	case "acl":
		metaRes, err := a.p.Documents.GetMeta(ctx,
			&repository.GetMetaRequest{
				Uuid: evt.Uuid,
			})
		if elephantine.IsTwirpErrorCode(err, twirp.NotFound) {
			// Just ignore deleted documents.
			return 0, ErrSkipped
		} else if err != nil {
			return 0, fmt.Errorf("get source meta: %w", err)
		}

		update.Acl = metaRes.Meta.Acl
	default:
		return 0, ErrSkipped
	}

	// We just let new documents overwrite whatever is there.
	if !isNew {
		update.IfMatch = targetVersion
	}

	upRes, err := a.p.TargetDocuments.Update(ctx, &update)
	if elephantine.IsTwirpErrorCode(err, twirp.FailedPrecondition) {
		return 0, ErrConflict
	} else if err != nil {
		return 0, fmt.Errorf("update target: %w", err)
	}

	if updateType == "document" {
		err = q.SetDocumentVersion(ctx, postgres.SetDocumentVersionParams{
			ID:            docUUID,
			TargetVersion: upRes.Version,
		})
		if err != nil {
			return 0, fmt.Errorf("record new target version: %w", err)
		}

		err = q.AddVersionMapping(ctx, postgres.AddVersionMappingParams{
			ID:            docUUID,
			SourceVersion: evt.Version,
			TargetVersion: upRes.Version,
		})
		if err != nil {
			return 0, fmt.Errorf("record new version mapping: %w", err)
		}
	}

	err = StoreState(ctx, q, "log_state", LogState{
		Position: evt.Id,
		CaughtUp: caughtUp,
	})
	if err != nil {
		return 0, fmt.Errorf("persist log state: %w", err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return 0, fmt.Errorf("commit state: %w", err)
	}

	return upRes.Version, nil
}

var (
	ErrSkipped  = errors.New("skipped event")
	ErrConflict = errors.New("document has been updated in target")
)
