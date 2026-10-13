# Elephant Replicant

<p>
  <img src="https://github.com/ttab/elephant-replicant/raw/main/docs/elephant-replicant.png?raw=true" width="256" alt="Elephant Replicant">
</p>

Replicates data to another Elephant environment. The replicant uses optimistic locking to prevent overwrites of documents that have been modified in the destination. This is not replication as a method of providing a backup or standby instance, rather it's a solution for keeping a stage or QA environment updated with relevant data.

ACL:s will always be replicated.

Attachments will only be replicated if `-all-attachments` is set or if they have been explicitly enabled by document type and attachment name using `-include-attachments`.

## Calling the API

The service definition lives in
[elephant-api](https://github.com/ttab/elephant-api/blob/main/replicant/service.proto),
not here. Every method of `elephant.replicant.Replication` is served twice,
and both mounts accept protobuf or JSON:

| Family | Path | Protocols |
|---|---|---|
| Connect | `POST /elephant.replicant.Replication/<Method>` | Connect, plus gRPC and gRPC-Web in-cluster |
| Twirp | `POST /twirp/elephant.replicant.Replication/<Method>` | Twirp |

Both mounts are registered on an `elephantine.APIServer` with one set of
service options, so they share the authentication middleware, the hooks and
the interceptors: a call with a missing or invalid token is answered
`unauthenticated` (401) before it reaches a handler, rendered as an error body
of whichever protocol the caller is speaking, and the scope checks are the
same on both. The Twirp mount is kept for the clients that already use it,
`elephant-cli` and the bruno collection among them, and goes away in a future
major release.

The two differ in three ways a caller that moves has to know about:

* The error body is `{"code","message","details"}` instead of
  `{"code","msg","meta"}`. The code strings are the same; the metadata
  (`argument` on an invalid argument, `required_any_of_scopes` on a missing
  scope) moves from the `meta` map into an `elephantine.rpc.ErrorMeta`
  detail. A Go client reads it with `rpc.Meta(err)`.
* Three codes get a different HTTP status: `failed_precondition` is 400
  rather than 412, `canceled` 499 rather than 408 and `deadline_exceeded`
  504 rather than 408. Read the code from the body, not the status.
* A JSON response spells its field names in lowerCamelCase (`repositoryUrl`)
  where Twirp spells them as the `.proto` declares them (`repository_url`).
  Requests accept either spelling on both mounts.

`internal/testdata/error_bodies/` holds the raw error body each mount answers
for the same failure; `TestErrorParity` is what keeps the two mounts agreeing
on code, message and metadata.

## Encryption key

Client secrets are encrypted at rest using AES-256-GCM. The service requires a 64-character hex-encoded encryption key provided via the `ENCRYPTION_KEY` environment variable (or `--encryption-key` flag).

Use the provided script to generate and store a key in Vault:

``` shell
./scripts/set-encryption-key ele000-stage
```

This generates a random key and stores it at `ele000-stage/services/replicant/encryption-key`. The script will not overwrite an existing key.

To read it back for use in configuration:

``` shell
vault kv get -mount=ele000-stage -field=value services/replicant/encryption-key
```

## Example configuration

Configuration for running replication to a local repository instance:

``` shell
ADDR=:1280
PROFILE_ADDR=:1281

IGNORE_TYPES=core/article+meta,tt/wire,tt/wire-provider,tt/wire-source
IGNORE_SUBS=core://application/elephant-wires
#INCLUDE_ATTACHMENTS=image.core/image,laygout.tt/print-layout
ALL_ATTACHMENTS=true

# Replicate from prod
REPOSITORY_ENDPOINT=https://repository.api.tt.ecms.se
OIDC_CONFIG=https://login.tt.se/realms/elephant/.well-known/openid-configuration
CLIENT_ID=replicant-send
CLIENT_SECRET=xoxo

# Replicate from stage
# REPOSITORY_ENDPOINT=https://repository.stage.tt.se
# OIDC_CONFIG=https://login.stage.tt.se/realms/elephant/.well-known/openid-configuration
# CLIENT_ID=replicant-send
# CLIENT_SECRET=xoxo

TARGET_REPOSITORY_ENDPOINT=http://localhost:1080
TARGET_OIDC_CONFIG=https://login.stage.tt.se/realms/elephant/.well-known/openid-configuration
TARGET_CLIENT_ID=replicant-receive
TARGET_CLIENT_SECRET=xoxo

ENCRYPTION_KEY=<64-character hex key from Vault>
```
