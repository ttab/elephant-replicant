-- name: SetState :exec
INSERT INTO state(name, value)
       VALUES (@name, @value)
ON CONFLICT (name)
   DO UPDATE SET value = @value;

-- name: GetState :one
SELECT value FROM state
WHERE name = @name;

-- name: SetDocumentVersion :exec
INSERT INTO document(id, target_version)
VALUES(@id, @target_version)
ON CONFLICT (id) DO UPDATE
   SET target_version = excluded.target_version;

-- name: GetDocumentVersion :one
SELECT target_version FROM document
WHERE id = @id;

-- name: AddVersionMapping :exec
INSERT INTO version_mapping(id, source_version, target_version, created)
VALUES (@id, @source_version, @target_version, @created)
ON CONFLICT (id, source_version) DO UPDATE
   SET target_version = excluded.target_version,
       created = excluded.created;

-- name: GetTargetVersion :one
SELECT target_version
FROM version_mapping
WHERE id = @id AND source_version = @source_version;

-- name: RemoveOldMappings :exec
DELETE FROM version_mapping
WHERE created < @cutoff;

-- name: RemoveDocument :exec
DELETE FROM document WHERE id = @id;

-- name: RemoveDocumentVersionMappings :exec
DELETE FROM version_mapping
WHERE id = @id;


