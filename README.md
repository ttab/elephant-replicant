# Elephant Replicant

<p>
  <img src="https://github.com/ttab/elephant-replicant/raw/main/docs/elephant-replicant.png?raw=true" width="256" alt="Elephant Replicant">
</p>

Replicates data to another Elephant environment. The replicant uses optimistic locking to prevent overwrites of documents that have been modified in the destination. This is not replication as a method of providing a backup or standby instance, rather it's a solution for keeping a stage or QA environment updated with relevant data.

ACL:s will always be replicated.

Attachments will only be replicated if `-all-attachments` is set or if they have been explicitly enabled by document type and attachment name using `-include-attachments`.

## Documentation

| Document | What it settles |
|---|---|
| [`docs/architecture.md`](docs/architecture.md) | How the service is built: the process model, the worker loop, catch-up and tail, conflicts, storage and the API. Start here to understand or change it. |
| [`docs/ops.md`](docs/ops.md) | The operator's-eye view: dependencies, deployment shape, bootstrap order, the failure modes with the signal for each, and what to watch. |
| [`docs/observability.md`](docs/observability.md) | Every metric the service exports and what a change in it means. |
| [`CONTEXT.md`](CONTEXT.md) | What the words mean: source, target, catch-up, conflict, version mapping. |
| [`docs/adr/`](docs/adr/) | Why the deliberate decisions went the way they did. |
| [`CHANGELOG.md`](CHANGELOG.md) | What changed for a consumer, per release. |

This README is orientation and the working reference: the layout, how to
build and run it, and every configuration flag. `mage docs:links` checks that
the relative links and heading anchors across these files resolve.

## Repository layout

```
cmd/replicant/      the binary: flags, pools, credentials, then internal.Run
internal/           the service: Run and the API handlers (replicant.go), the
                    target manager and its job locks (target_manager.go), the
                    per-target worker loop (worker.go), the section filter,
                    the log state helpers and the secret encryption;
                    api_test.go and testdata/ pin the two RPC mounts to each other
postgres/           sqlc output over postgres/queries.sql; never edited by hand
schema/             tern migrations and the reporting table list
magefiles/          mage targets: sql:*, docs:links, grantReporting
scripts/            set-encryption-key: creates the key in Vault
docs/               architecture, ops, observability, ADRs
```

## Build & development tools

Go 1.27 and [mage](https://magefile.org). Code generators run in pinned
docker images through mage; never install them locally.

```shell
go build -o /dev/null ./...     # compile check
go vet ./... && golangci-lint run ./...
go test ./...                   # no backing services needed today
mage sql:generate               # recompile postgres/queries.sql with sqlc
mage sql:migrate                # migrate the local database
mage sql:rollback <version>     # roll it back
mage docs:links                 # check the documentation links
mage grantReporting             # grant the reporting role on the listed tables
```

The tests mount the Replication service on both stacks without a database and
check that the two answer the same error for the same call. Set
`REGENERATE_GOLDEN=1` to rewrite `internal/testdata/error_bodies/` after a
deliberate change to an error body.

## Running a local dev instance

1. A local Postgres with a database and role named `elephant-replicant`:
   `mage sql:db` creates them, `mage sql:migrate` migrates. The service never
   migrates its own schema.
2. An encryption key: any 64 hex characters will do locally,
   `openssl rand -hex 32`.
3. Source credentials with `doc_read_all` and `eventlog_read` in the source
   environment, and target credentials with `doc_admin` in the target
   environment. The source and target cannot be the same endpoint.
4. A `.env` with the configuration below, then `go run ./cmd/replicant run`.

On the first start the `TARGET_*` variables become the target named
`default` and replication begins from `START_EVENT`. On every later start the
stored target wins, and the environment is ignored; change the target over
the API or drop the `replication_target` row. Without
`TARGET_REPOSITORY_ENDPOINT` the service starts with no target and waits for
`ConfigureTarget`.

### Resetting a local dev environment

`mage sql:dropDB && mage sql:db && mage sql:migrate` removes every target,
position and mapping. The target repository keeps what was replicated.

## Calling the API

The service definition lives in
[elephant-api](https://github.com/ttab/elephant-api/blob/main/replicant/service.proto),
not here. Every method of `elephant.replicant.Replication` is served twice,
and both mounts accept protobuf or JSON:

| Family | Path | Protocols |
|---|---|---|
| Connect | `POST /elephant.replicant.Replication/<Method>` | Connect, plus gRPC and gRPC-Web in-cluster |
| Twirp | `POST /twirp/elephant.replicant.Replication/<Method>` | Twirp |

Every method needs a token with `doc_admin`. Both mounts share the
authentication middleware and the scope checks, and the Twirp mount is kept
for the clients that already use it until a future major release. What differs
for a caller that moves, the error body, three HTTP statuses and the JSON
field names, is in
[architecture](docs/architecture.md#error-bodies); the methods and what each
does are in [the API section](docs/architecture.md#the-api) there.

A worked example on the Connect path; add the `/twirp` prefix for the other mount:

```shell
curl -s -X POST https://replicant.api.tt.ecms.se/elephant.replicant.Replication/ListTargets \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -d '{}'
```

The replicant is itself a Connect client: it calls the source repository and
every target repository on their Connect paths, so each of them has to run
elephant-repository v1.9.0 or later.

## Configuration reference

Every flag has an environment variable, and `.env` in the working directory
is loaded first. The defaults are the exported constants where one exists, so
that the number is written down once.

### Listeners and process

| Flag / env | Default | What it does |
|---|---|---|
| `--addr` / `ADDR` | `:1080` | The API: both RPC mounts, `/health/alive`, `/version`. |
| `--profile-addr` / `PROFILE_ADDR` | `:1081` | `/metrics`, `/health/ready`, `/debug/pprof/`. |
| `--tls-addr` / `TLS_ADDR` (`TLS_LISTEN_ADDR`) | `:1443` | The API over TLS; only listened on when the certificate is set. |
| `--cert-file` / `TLS_CERT_PATH`, `--key-file` / `TLS_KEY_PATH` | | The TLS certificate and key. |
| `--log-level` / `LOG_LEVEL` | `info` | `debug` is what shows skipped events and the handled-event trace. |
| `--cors-hosts` / `CORS_HOSTS` | | Origins allowed to call the API from a browser. |
| `--encryption-key` / `ENCRYPTION_KEY` | required | 64 hex characters, the AES-256 key the target secrets are encrypted with. **Never change it once targets exist**; see [ops](docs/ops.md#the-encryption-key-was-rotated). |

### Database

| Flag / env | Default | What it does |
|---|---|---|
| `--db` / `CONN_STRING` | a local dev DSN | The direct connection string. Carries everything without a bouncer, and the `LISTEN` with one. |
| `--db-bouncer` / `BOUNCER_CONN_STRING` | | A transaction pooler's connection string. When set, and different from `CONN_STRING`, every query goes through it and the direct pool is pinned at two connections for the `LISTEN`. |
| `--db-max-conns` / `DB_MAX_CONNS` | `8` (`DefaultDBMaxConns`) | Size of the query pool. Each enabled target needs two connections, so the default covers three targets; raise it by two per further target. Zero or less leaves it to pgx, which sizes it from the node's CPU count. |

### Source

| Flag / env | Default | What it does |
|---|---|---|
| `--repository-endpoint` / `REPOSITORY_ENDPOINT` | `http://editorial-repository:1080` | The source repository. It must serve Connect, which is elephant-repository v1.9.0 or later. |
| `--oidc-config` / `OIDC_CONFIG` | | The source environment's OIDC discovery URL, which also validates the tokens of API callers. |
| `--client-id` / `CLIENT_ID`, `--client-secret` / `CLIENT_SECRET` | | The source client; needs `doc_read_all` and `eventlog_read`. Verified at start. |
| `--jwt-audience` / `JWT_AUDIENCE`, `--jwt-scope-prefix` / `JWT_SCOPE_PREFIX` | | Token validation for API callers, as in every elephant service. |

### The default target

Read into the `replication_target` row named `default` **on the first start
only**. Each maps to a `SyncConfig` field; see
[architecture](docs/architecture.md#sync-config) for what each does to an
event.

| Flag / env | Default | What it does |
|---|---|---|
| `--target-repository-endpoint` / `TARGET_REPOSITORY_ENDPOINT` | | The target repository, v1.9.0 or later. Unset means no default target. Must differ from the source. |
| `--target-oidc-config` / `TARGET_OIDC_CONFIG` | | The target environment's OIDC discovery URL. |
| `--target-client-id` / `TARGET_CLIENT_ID`, `--target-client-secret` / `TARGET_CLIENT_SECRET` | | The target client; needs `doc_admin` there. Stored encrypted. |
| `--start-event` / `START_EVENT` | `0` | `start_from`: a floor on the position, so it only ever moves a target forward. |
| `--ignore-types` / `IGNORE_TYPES` | | Document types to skip. |
| `--ignore-subs` / `IGNORE_SUBS` | | Updater URIs to skip, such as `core://application/elephant-wires`. |
| `--ignore-section` / `IGNORE_SECTIONS` | | `type:section-uuid` pairs; a document of that type linking to that section is skipped. Costs a document fetch per event of that type. |
| `--all-attachments` / `ALL_ATTACHMENTS` | `false` | Transfer every attached object. |
| `--include-attachments` / `INCLUDE_ATTACHMENTS` | | Otherwise, `name.type` pairs to transfer, such as `image.core/image`. |
| `--accept-errors` / `ACCEPT_ERRORS` | `false` | Log and skip an update the target refuses instead of restarting on it. Trades a stuck target for silently lost updates. |

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

## Pending work

**`SendDocument` is unimplemented.** The proto declares it with a `force`
flag, and it is the operation every recovery in [ops](docs/ops.md) wants: to
re-send one document, past a conflict, without waiting for its next event.
Today the recovery is manual, deleting the document in the target and its
`document` row.

**A `configure` lost while the `LISTEN` connection was dead is not
recovered.** The subscriber reconnects within twelve minutes and reconciles
enabled against running, which heals a lost start, stop or remove, but a row
that changed under a running worker looks like an unchanged one. Comparing
the row's `updated` with the worker's start time would close the gap.

**Attachment transfers have no timeout.** The download from the source and
the upload to the target use `http.DefaultClient`, inside the event's
transaction, so a stalled S3 connection stalls the worker while it keeps its
lock. The upload's error message also reports the download's status rather
than the upload's.

**Nothing counts conflicts, skips or accepted errors.** The per-event
outcomes are log lines only, so the one failure mode that is both silent and
permanent, a document edited in the target, has no metric.

**Retiring the Twirp mount** waits for the next major release and for
`elephant-cli` to move to the Connect client; the `Replicant` bruno
collection and `elephant-mcp`'s registration are the other two Twirp callers.
