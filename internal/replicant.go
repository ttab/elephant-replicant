package internal

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
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
	IgnoreSubs         []string
	IncludeAttachments []AttachmentRef
	AllAttachments     bool
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

func Run(ctx context.Context, p Parameters) error {
	grace := elephantine.NewGracefulShutdown(p.Logger, 10*time.Second)

	var state LogState

	err := LoadState(ctx, postgres.New(p.Database), "log_state", &state)
	if err != nil {
		return fmt.Errorf("load log state: %w", err)
	}

	state.Position = max(state.Position, p.MinEventID)

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

	group.Go("cleanup", func(ctx context.Context) error {
		return app.mappingCleanup(grace.CancelOnStop(ctx))
	})

	return group.Wait() //nolint: wrapcheck
}

var _ replicant.Replication = &Application{}

type Application struct {
	p  Parameters
	lf *koonkie.LogFollower
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

const (
	TypeDocumentVersion = "document"
	TypeNewStatus       = "status"
	TypeACLUpdate       = "acl"
	TypeDeleteDocument  = "delete_document"
	TypeRestoreFinished = "restore_finished"
	TypeWorkflow        = "workflow"
)

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

			if item.Event == TypeWorkflow {
				// Workflows describes effects rather than changes.
				continue
			}

			err := a.handleEvent(ctx, item, caughtUp)
			switch {
			case errors.Is(err, ErrSkipped):
				a.p.Logger.Debug("skipped import of document",
					elephantine.LogKeyEventID, item.Id,
					elephantine.LogKeyEventType, item.Event,
					elephantine.LogKeyDocumentUUID, item.Uuid,
					elephantine.LogKeyError, err,
				)
			case errors.Is(err, ErrConflict):
				a.p.Logger.Info("conflict with change in target repo",
					elephantine.LogKeyEventID, item.Id,
					elephantine.LogKeyEventType, item.Event,
					elephantine.LogKeyDocumentUUID, item.Uuid,
					elephantine.LogKeyError, err,
				)
			case err != nil:
				return fmt.Errorf("handle event %d (%s): %w",
					item.Id, item.Uuid, err)
			default:
				lastSaved = pos
			}
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
) (outErr error) {
	docUUID := uuid.MustParse(evt.Uuid)

	if slices.Contains(a.p.IgnoreSubs, evt.UpdaterUri) {
		return fmt.Errorf("ignored sub: %w", ErrSkipped)
	}

	if slices.Contains(a.p.IgnoreTypes, evt.Type) {
		return fmt.Errorf("ignored type: %w", ErrSkipped)
	}

	// Separate handling of deletes.
	if evt.Type == TypeDeleteDocument {
		return a.handleDeleteEvent(ctx, evt, docUUID)
	}

	tx, err := a.p.Database.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	defer pg.Rollback(tx, &outErr)

	q := postgres.New(tx)

	var isNew bool

	targetVersion, err := q.GetDocumentVersion(ctx, docUUID)
	if errors.Is(err, pgx.ErrNoRows) {
		isNew = true
	} else if err != nil {
		return fmt.Errorf("get current target version: %w", err)
	}

	update := repository.UpdateRequest{
		Uuid: evt.Uuid,
		ImportDirective: &repository.ImportDirective{
			OriginallyCreated: evt.Timestamp,
			OriginalCreator:   evt.UpdaterUri,
		},
	}

	updateType := evt.Event

	if !caughtUp {
		// If we're not caught up we might just get one event per
		// document, so we'll need to fill in the blanks here.
		updateType = TypeDocumentVersion

		metaRes, err := a.p.Documents.GetMeta(ctx,
			&repository.GetMetaRequest{
				Uuid: evt.Uuid,
			})
		if elephantine.IsTwirpErrorCode(err, twirp.NotFound) {
			// Just ignore deleted documents.
			return fmt.Errorf("document not found for meta read: %w", ErrSkipped)
		} else if err != nil {
			return fmt.Errorf("get source meta: %w", err)
		}

		evt.Version = metaRes.Meta.CurrentVersion

		// Replace import directive with original creation info and set
		// ACLs on first ingest of the document. Also grab attachments
		// on first encounter.
		if isNew {
			update.Acl = metaRes.Meta.Acl

			update.ImportDirective = &repository.ImportDirective{
				OriginallyCreated: metaRes.Meta.Created,
				OriginalCreator:   metaRes.Meta.CreatorUri,
			}

			for _, info := range metaRes.Meta.Attachments {
				evt.AttachedObjects = append(evt.AttachedObjects, info.Name)
			}
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
	case TypeDocumentVersion:
		docRes, err := a.p.Documents.Get(ctx,
			&repository.GetDocumentRequest{
				Uuid:    evt.Uuid,
				Version: evt.Version,
			})
		if elephantine.IsTwirpErrorCode(err, twirp.NotFound) {
			// Just ignore deleted documents.
			return fmt.Errorf("document not found: %w", ErrSkipped)
		} else if err != nil {
			return fmt.Errorf("get source document: %w", err)
		}

		update.Document = docRes.Document

		err = a.prepareAttachments(ctx, evt, &update)
		if err != nil {
			return fmt.Errorf("transfer attachments: %w", err)
		}
	case TypeNewStatus:
		mappedVersion, err := q.GetTargetVersion(ctx,
			postgres.GetTargetVersionParams{
				ID:            docUUID,
				SourceVersion: evt.Version,
			})
		if errors.Is(err, pgx.ErrNoRows) {
			// No record of the version that the status refers to, skip.
			return ErrSkipped
		}

		statusRes, err := a.p.Documents.GetStatus(ctx, &repository.GetStatusRequest{
			Uuid: evt.Uuid,
			Name: evt.Status,
			Id:   evt.StatusId,
		})
		if elephantine.IsTwirpErrorCode(err, twirp.NotFound) {
			// Just ignore deleted documents.
			return fmt.Errorf("document not found: %w", ErrSkipped)
		} else if err != nil {
			return fmt.Errorf("get source status: %w", err)
		}

		update.Status = append(update.Status, &repository.StatusUpdate{
			Name:    evt.Status,
			Version: mappedVersion,
			Meta:    statusRes.Status.Meta,
		})
	case TypeACLUpdate:
		metaRes, err := a.p.Documents.GetMeta(ctx,
			&repository.GetMetaRequest{
				Uuid: evt.Uuid,
			})
		if elephantine.IsTwirpErrorCode(err, twirp.NotFound) {
			// Just ignore deleted documents.
			return fmt.Errorf("document not found: %w", ErrSkipped)
		} else if err != nil {
			return fmt.Errorf("get source meta: %w", err)
		}

		update.Acl = metaRes.Meta.Acl
	default:
		return fmt.Errorf("unhandled event type %q: %w",
			updateType, ErrSkipped)
	}

	// We just let new documents overwrite whatever is there.
	if !isNew {
		update.IfMatch = targetVersion
	}

	upRes, err := a.p.TargetDocuments.Update(ctx, &update)
	if elephantine.IsTwirpErrorCode(err, twirp.FailedPrecondition) {
		return ErrConflict
	} else if err != nil {
		return fmt.Errorf("update target: %w", err)
	}

	if updateType == TypeDocumentVersion {
		err = q.SetDocumentVersion(ctx, postgres.SetDocumentVersionParams{
			ID:            docUUID,
			TargetVersion: upRes.Version,
		})
		if err != nil {
			return fmt.Errorf("record new target version: %w", err)
		}

		err = q.AddVersionMapping(ctx, postgres.AddVersionMappingParams{
			ID:            docUUID,
			SourceVersion: evt.Version,
			TargetVersion: upRes.Version,
			Created:       pg.Time(time.Now()),
		})
		if err != nil {
			return fmt.Errorf("record new version mapping: %w", err)
		}
	}

	err = StoreState(ctx, q, "log_state", LogState{
		Position: evt.Id,
		CaughtUp: caughtUp,
	})
	if err != nil {
		return fmt.Errorf("persist log state: %w", err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("commit state: %w", err)
	}

	return nil
}

func (a *Application) prepareAttachments(
	ctx context.Context,
	evt *repository.EventlogItem,
	request *repository.UpdateRequest,
) error {
	if len(evt.AttachedObjects) == 0 {
		return nil
	}

	request.AttachObjects = make(map[string]string)

	for _, name := range evt.AttachedObjects {
		if !a.shouldReplicateAttachment(name, evt.Type) {
			continue
		}

		attachments, err := a.p.Documents.GetAttachments(ctx, &repository.GetAttachmentsRequest{
			AttachmentName: name,
			Documents:      []string{evt.Uuid},
			DownloadLink:   true,
		})
		if err != nil {
			return fmt.Errorf("get download link for %q: %w", name, err)
		}

		if len(attachments.Attachments) == 0 {
			// Ignore attachments if they have been deleted.
			continue
		}

		obj := attachments.Attachments[0]

		uploadID, err := a.transferAttachment(ctx, obj)
		if err != nil {
			return fmt.Errorf("transfer %q: %w", name, err)
		}

		request.AttachObjects[name] = uploadID
	}

	return nil
}

func (a *Application) transferAttachment(
	ctx context.Context,
	obj *repository.AttachmentDetails,
) (_ string, outErr error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, obj.DownloadLink, nil)
	if err != nil {
		return "", fmt.Errorf("create download request: %w", err)
	}

	res, err := http.DefaultClient.Do(req) //nolint: bodyclose
	if err != nil {
		return "", fmt.Errorf("make download request: %w", err)
	}

	defer elephantine.Close("download body", res.Body, &outErr)

	if res.StatusCode != http.StatusOK {
		return "", fmt.Errorf(
			"failed to download attachment, server responded with: %s",
			res.Status)
	}

	upload, err := a.p.TargetDocuments.CreateUpload(ctx, &repository.CreateUploadRequest{
		Name:        obj.Filename,
		ContentType: obj.ContentType,
		// TODO: No meta in AttachmentDetails?
	})
	if err != nil {
		return "", fmt.Errorf("create upload: %w", err)
	}

	upReq, err := http.NewRequestWithContext(ctx, http.MethodPut,
		upload.Url, res.Body)
	if err != nil {
		return "", fmt.Errorf("create upload request: %w", err)
	}

	upReq.ContentLength = res.ContentLength
	upReq.Header.Add("Content-Type", obj.ContentType)

	upRes, err := http.DefaultClient.Do(upReq) //nolint: bodyclose
	if err != nil {
		return "", fmt.Errorf("make upload request: %w", err)
	}

	defer elephantine.Close("upload body", upRes.Body, &outErr)

	if upRes.StatusCode != http.StatusOK {
		return "", fmt.Errorf("failed to upload attachment, server responded with: %s",
			res.Status)
	}

	return upload.Id, nil
}

func (a *Application) shouldReplicateAttachment(name string, docType string) bool {
	if a.p.AllAttachments {
		return true
	}

	for _, r := range a.p.IncludeAttachments {
		if name == r.Name && docType == r.DocType {
			return true
		}
	}

	return false
}

func (a *Application) handleDeleteEvent(
	ctx context.Context, evt *repository.EventlogItem, docUUID uuid.UUID,
) (outErr error) {
	tx, err := a.p.Database.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	defer pg.Rollback(tx, &outErr)

	q := postgres.New(tx)

	err = q.RemoveDocument(ctx, docUUID)
	if err != nil {
		return fmt.Errorf("remove document target entry: %w", err)
	}

	err = q.RemoveDocumentVersionMappings(ctx, docUUID)
	if err != nil {
		return fmt.Errorf("remove document version mappings: %w", err)
	}

	_, err = a.p.TargetDocuments.Delete(ctx, &repository.DeleteDocumentRequest{
		Uuid: evt.Uuid,
		Meta: map[string]string{
			"original_delete_record": strconv.FormatInt(evt.DeleteRecordId, 10),
		},
	})
	if err != nil {
		return fmt.Errorf("delete document: %w", err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("commit state: %w", err)
	}

	return nil
}

func (a *Application) mappingCleanup(ctx context.Context) error {
	for {
		run := time.After(1 * time.Hour)

		select {
		case <-ctx.Done():
			return ctx.Err() //nolint: wrapcheck
		case <-run:
		}

		q := postgres.New(a.p.Database)

		// Remove mappings older than six months.
		err := q.RemoveOldMappings(ctx, pg.Time(
			time.Now().AddDate(0, -6, 0)))
		if err != nil {
			return fmt.Errorf("remove old mappings: %w", err)
		}
	}
}
