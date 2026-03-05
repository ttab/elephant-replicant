package internal

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	rpc_newsdoc "github.com/ttab/elephant-api/newsdoc"
	"github.com/ttab/elephant-api/repository"
	"github.com/ttab/elephant-replicant/postgres"
	"github.com/ttab/elephantine"
	"github.com/ttab/elephantine/pg"
	"github.com/ttab/koonkie"
	"github.com/twitchtv/twirp"
)

// Worker handles replication for a single target.
type Worker struct {
	name           string
	logger         *slog.Logger
	db             *pgxpool.Pool
	source         repository.Documents
	target         repository.Documents
	cFilter        *ContentFilter
	lf             *koonkie.LogFollower
	acceptErrors   bool
	ignoreSubs     []string
	ignoreTypes    []string
	allAttachments bool
	incAttachments []AttachmentRef
}

// Replicate runs the replication loop for this worker's target.
func (w *Worker) Replicate(ctx context.Context) error {
	for {
		var lastSaved int64

		pos, caughtUp := w.lf.GetState()

		items, err := w.lf.GetNext(ctx)
		if err != nil {
			return fmt.Errorf("read eventlog: %w", err)
		}

		for _, item := range items {
			pos = item.Id

			if item.Event == TypeWorkflow {
				continue
			}

			err := w.handleEvent(ctx, item, caughtUp)

			switch {
			case errors.Is(err, ErrSkipped):
				w.logger.Debug("skipped import of document",
					elephantine.LogKeyEventID, item.Id,
					elephantine.LogKeyEventType, item.Event,
					elephantine.LogKeyDocumentUUID, item.Uuid,
					elephantine.LogKeyError, err,
				)
			case errors.Is(err, ErrConflict):
				w.logger.Info("conflict with change in target repo",
					elephantine.LogKeyEventID, item.Id,
					elephantine.LogKeyEventType, item.Event,
					elephantine.LogKeyDocumentUUID, item.Uuid,
					elephantine.LogKeyError, err,
				)
			case err != nil && w.acceptErrors:
				w.logger.Error("error from target repo",
					elephantine.LogKeyEventID, item.Id,
					elephantine.LogKeyEventType, item.Event,
					elephantine.LogKeyDocumentUUID, item.Uuid,
					elephantine.LogKeyError, err,
				)
			case err != nil:
				return fmt.Errorf("handle event %d (%s): %w",
					item.Id, item.Uuid, err)
			default:
				w.logger.Debug("handled event",
					elephantine.LogKeyEventID, item.Id,
					elephantine.LogKeyEventType, item.Event,
					elephantine.LogKeyDocumentUUID, item.Uuid,
				)

				lastSaved = pos
			}
		}

		if lastSaved != pos {
			err = StoreState(ctx, postgres.New(w.db), w.stateKey(), LogState{
				Position: pos,
				CaughtUp: caughtUp,
			})
			if err != nil {
				return fmt.Errorf("persist log state: %w", err)
			}
		}
	}
}

func (w *Worker) stateKey() string {
	return w.name + ":log_state"
}

func (w *Worker) handleEvent(
	ctx context.Context, evt *repository.EventlogItem, caughtUp bool,
) (outErr error) {
	docUUID := uuid.MustParse(evt.Uuid)

	if slices.Contains(w.ignoreSubs, evt.UpdaterUri) {
		return fmt.Errorf("ignored sub: %w", ErrSkipped)
	}

	if slices.Contains(w.ignoreTypes, evt.Type) {
		return fmt.Errorf("ignored type: %w", ErrSkipped)
	}

	if evt.Type == TypeDeleteDocument {
		return w.handleDeleteEvent(ctx, evt, docUUID)
	}

	var checkRes *repository.GetDocumentResponse

	if w.cFilter.HasFilters(evt.Type) {
		res, err := w.source.Get(ctx,
			&repository.GetDocumentRequest{
				Uuid: evt.Uuid,
			})
		if elephantine.IsTwirpErrorCode(err, twirp.NotFound) {
			return fmt.Errorf("document not found for content filtering: %w", ErrSkipped)
		} else if err != nil {
			return fmt.Errorf("get document for content based filtering: %w", err)
		}

		checkRes = res

		doc := rpc_newsdoc.DocumentFromRPC(res.Document)

		if !w.cFilter.Check(doc) {
			return fmt.Errorf("ignored because of content filter: %w", ErrSkipped)
		}
	}

	tx, err := w.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	defer pg.Rollback(tx, &outErr)

	q := postgres.New(tx)

	var isNew bool

	targetVersion, err := q.GetDocumentVersion(ctx, postgres.GetDocumentVersionParams{
		TargetName: w.name,
		ID:         docUUID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		isNew = true
	} else if err != nil {
		return fmt.Errorf("get current target version: %w", err)
	}

	if isNew {
		err := w.reconcileTypeDifferences(
			ctx, docUUID.String(), evt.Type)
		if err != nil {
			return fmt.Errorf("reconcile type differences for new document: %w", err)
		}
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
		updateType = TypeDocumentVersion

		metaRes, err := w.source.GetMeta(ctx,
			&repository.GetMetaRequest{
				Uuid: evt.Uuid,
			})
		if elephantine.IsTwirpErrorCode(err, twirp.NotFound) {
			return fmt.Errorf("document not found for meta read: %w", ErrSkipped)
		} else if err != nil {
			return fmt.Errorf("get source meta: %w", err)
		}

		evt.Version = metaRes.Meta.CurrentVersion

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
		if checkRes != nil && checkRes.Version == evt.Version {
			update.Document = checkRes.Document
		} else {
			res, err := w.source.Get(ctx,
				&repository.GetDocumentRequest{
					Uuid:    evt.Uuid,
					Version: evt.Version,
				})
			if elephantine.IsTwirpErrorCode(err, twirp.NotFound) {
				return fmt.Errorf("document not found: %w", ErrSkipped)
			} else if err != nil {
				return fmt.Errorf("get source document: %w", err)
			}

			update.Document = res.Document
		}

		err = w.prepareAttachments(ctx, evt, &update)
		if err != nil {
			return fmt.Errorf("transfer attachments: %w", err)
		}
	case TypeNewStatus:
		mappedVersion, err := q.GetTargetVersion(ctx,
			postgres.GetTargetVersionParams{
				TargetName:    w.name,
				ID:            docUUID,
				SourceVersion: evt.Version,
			})
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrSkipped
		}

		statusRes, err := w.source.GetStatus(ctx, &repository.GetStatusRequest{
			Uuid: evt.Uuid,
			Name: evt.Status,
			Id:   evt.StatusId,
		})
		if elephantine.IsTwirpErrorCode(err, twirp.NotFound) {
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
		metaRes, err := w.source.GetMeta(ctx,
			&repository.GetMetaRequest{
				Uuid: evt.Uuid,
			})
		if elephantine.IsTwirpErrorCode(err, twirp.NotFound) {
			return fmt.Errorf("document not found: %w", ErrSkipped)
		} else if err != nil {
			return fmt.Errorf("get source meta: %w", err)
		}

		update.Acl = metaRes.Meta.Acl
	default:
		return fmt.Errorf("unhandled event type %q: %w",
			updateType, ErrSkipped)
	}

	if !isNew {
		update.IfMatch = targetVersion
	}

	var upRes *repository.UpdateResponse

	for {
		res, err := w.target.Update(ctx, &update)

		switch {
		case elephantine.IsTwirpErrorCode(err, twirp.FailedPrecondition):
			return ErrConflict
		case elephantine.IsTwirpErrorCode(err, twirp.NotFound) && update.Document == nil:
			fetchRes, err := w.source.Get(ctx,
				&repository.GetDocumentRequest{
					Uuid: evt.Uuid,
				})
			if err != nil {
				return fmt.Errorf("fetch document for backfill: %w", err)
			}

			update.Document = fetchRes.Document

			continue
		case err != nil:
			return fmt.Errorf("update target: %w", err)
		}

		upRes = res

		break
	}

	if updateType == TypeDocumentVersion {
		err = q.SetDocumentVersion(ctx, postgres.SetDocumentVersionParams{
			TargetName:    w.name,
			ID:            docUUID,
			TargetVersion: upRes.Version,
		})
		if err != nil {
			return fmt.Errorf("record new target version: %w", err)
		}

		err = q.AddVersionMapping(ctx, postgres.AddVersionMappingParams{
			TargetName:    w.name,
			ID:            docUUID,
			SourceVersion: evt.Version,
			TargetVersion: upRes.Version,
			Created:       pg.Time(time.Now()),
		})
		if err != nil {
			return fmt.Errorf("record new version mapping: %w", err)
		}
	}

	err = StoreState(ctx, q, w.stateKey(), LogState{
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

func (w *Worker) reconcileTypeDifferences(
	ctx context.Context, docUUID string, sourceType string,
) error {
	docRes, err := w.target.Get(ctx, &repository.GetDocumentRequest{
		Uuid: docUUID,
	})
	if elephantine.IsTwirpErrorCode(err, twirp.NotFound) {
		return nil
	} else if err != nil {
		return fmt.Errorf("get target document: %w", err)
	}

	if docRes.Document.Type == sourceType {
		return nil
	}

	_, err = w.target.Delete(ctx, &repository.DeleteDocumentRequest{
		Uuid: docUUID,
	})
	if err != nil {
		return fmt.Errorf("delete target document: %w", err)
	}

	w.logger.WarnContext(ctx,
		"deleted document in target to reconcile type differences",
		elephantine.LogKeyDocumentUUID, docUUID,
		elephantine.LogKeyDocumentType, sourceType,
		"old_type", docRes.Document.Type,
	)

	return nil
}

func (w *Worker) prepareAttachments(
	ctx context.Context,
	evt *repository.EventlogItem,
	request *repository.UpdateRequest,
) error {
	if len(evt.AttachedObjects) == 0 {
		return nil
	}

	request.AttachObjects = make(map[string]string)

	for _, name := range evt.AttachedObjects {
		if !w.shouldReplicateAttachment(name, evt.Type) {
			continue
		}

		attachments, err := w.source.GetAttachments(ctx, &repository.GetAttachmentsRequest{
			AttachmentName: name,
			Documents:      []string{evt.Uuid},
			DownloadLink:   true,
		})
		if err != nil {
			return fmt.Errorf("get download link for %q: %w", name, err)
		}

		if len(attachments.Attachments) == 0 {
			continue
		}

		obj := attachments.Attachments[0]

		uploadID, err := w.transferAttachment(ctx, obj)
		if err != nil {
			return fmt.Errorf("transfer %q: %w", name, err)
		}

		request.AttachObjects[name] = uploadID
	}

	return nil
}

func (w *Worker) transferAttachment(
	ctx context.Context,
	obj *repository.AttachmentDetails,
) (_ string, outErr error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, obj.DownloadLink, nil)
	if err != nil {
		return "", fmt.Errorf("create download request: %w", err)
	}

	res, err := http.DefaultClient.Do(req) //nolint: bodyclose,gosec
	if err != nil {
		return "", fmt.Errorf("make download request: %w", err)
	}

	defer elephantine.Close("download body", res.Body, &outErr)

	if res.StatusCode != http.StatusOK {
		return "", fmt.Errorf(
			"failed to download attachment, server responded with: %s",
			res.Status)
	}

	upload, err := w.target.CreateUpload(ctx, &repository.CreateUploadRequest{
		Name:        obj.Filename,
		ContentType: obj.ContentType,
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

	upRes, err := http.DefaultClient.Do(upReq) //nolint: bodyclose,gosec
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

func (w *Worker) shouldReplicateAttachment(name string, docType string) bool {
	if w.allAttachments {
		return true
	}

	for _, r := range w.incAttachments {
		if name == r.Name && docType == r.DocType {
			return true
		}
	}

	return false
}

func (w *Worker) handleDeleteEvent(
	ctx context.Context, evt *repository.EventlogItem, docUUID uuid.UUID,
) (outErr error) {
	tx, err := w.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	defer pg.Rollback(tx, &outErr)

	q := postgres.New(tx)

	err = q.RemoveDocument(ctx, postgres.RemoveDocumentParams{
		TargetName: w.name,
		ID:         docUUID,
	})
	if err != nil {
		return fmt.Errorf("remove document target entry: %w", err)
	}

	err = q.RemoveDocumentVersionMappings(ctx, postgres.RemoveDocumentVersionMappingsParams{
		TargetName: w.name,
		ID:         docUUID,
	})
	if err != nil {
		return fmt.Errorf("remove document version mappings: %w", err)
	}

	_, err = w.target.Delete(ctx, &repository.DeleteDocumentRequest{
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
