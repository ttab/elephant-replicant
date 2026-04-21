-- name: SetState :exec
INSERT INTO state(name, value)
       VALUES (@name, @value)
ON CONFLICT (name)
   DO UPDATE SET value = @value;

-- name: GetState :one
SELECT value FROM state
WHERE name = @name;

-- name: SetDocumentVersion :exec
INSERT INTO document(target_name, id, target_version)
VALUES(@target_name, @id, @target_version)
ON CONFLICT (target_name, id) DO UPDATE
   SET target_version = excluded.target_version;

-- name: GetDocumentVersion :one
SELECT target_version FROM document
WHERE target_name = @target_name AND id = @id;

-- name: AddVersionMapping :exec
INSERT INTO version_mapping(target_name, id, source_version, target_version, created)
VALUES (@target_name, @id, @source_version, @target_version, @created)
ON CONFLICT (target_name, id, source_version) DO UPDATE
   SET target_version = excluded.target_version,
       created = excluded.created;

-- name: GetTargetVersion :one
SELECT target_version
FROM version_mapping
WHERE target_name = @target_name AND id = @id AND source_version = @source_version;

-- name: RemoveOldMappings :exec
DELETE FROM version_mapping
WHERE created < @cutoff;

-- name: RemoveDocument :exec
DELETE FROM document WHERE target_name = @target_name AND id = @id;

-- name: RemoveDocumentVersionMappings :exec
DELETE FROM version_mapping
WHERE target_name = @target_name AND id = @id;

-- name: UpsertTarget :exec
INSERT INTO replication_target(name, repository_url, oidc_config, client_id, client_secret, start_from, config, enabled)
VALUES (@name, @repository_url, @oidc_config, @client_id, @client_secret, @start_from, @config, @enabled)
ON CONFLICT (name) DO UPDATE
   SET repository_url = excluded.repository_url,
       oidc_config = excluded.oidc_config,
       client_id = excluded.client_id,
       client_secret = excluded.client_secret,
       start_from = excluded.start_from,
       config = excluded.config,
       enabled = excluded.enabled,
       updated = now();

-- name: GetTarget :one
SELECT name, repository_url, oidc_config, client_id, client_secret,
       start_from, config, enabled, created, updated
FROM replication_target
WHERE name = @name;

-- name: ListEnabledTargets :many
SELECT name, repository_url, oidc_config, client_id, client_secret,
       start_from, config, enabled, created, updated
FROM replication_target
WHERE enabled = true
ORDER BY name;

-- name: ListTargets :many
SELECT name, repository_url
FROM replication_target
ORDER BY name;

-- name: DeleteTarget :exec
DELETE FROM replication_target WHERE name = @name;

-- name: SetTargetEnabled :exec
UPDATE replication_target
SET enabled = @enabled, updated = now()
WHERE name = @name;

-- name: TargetExists :one
SELECT EXISTS(SELECT 1 FROM replication_target WHERE name = @name);

-- name: RemoveTargetData :exec
DELETE FROM document WHERE target_name = @target_name;

-- name: RemoveTargetMappings :exec
DELETE FROM version_mapping WHERE target_name = @target_name;

-- name: RemoveTargetState :exec
DELETE FROM state WHERE name = @name;
