# elephant-replicant — operations

Audience: whoever is holding the pager, triaging a stage environment that has
stopped receiving production content, or deploying the service. This is the
operator's-eye view: what the pieces are, what they depend on, how content
moves, and what it looks like when something breaks.

It does not repeat the design. The authorities are:

| Document | What it settles |
|---|---|
| [`architecture.md`](architecture.md) | How the service is built: the process model, the worker loop, catch-up and tail, conflicts, the API. The design reference this document summarises. |
| [`observability.md`](observability.md) | Every metric the service exports and what a change in it means. This document says which to watch and what each is the signal for; that one defines them. |
| [`../README.md`](../README.md) | Orientation, the local development workflow, and the configuration reference: every flag and its environment variable. |
| [`../CONTEXT.md`](../CONTEXT.md) | What the words mean. |

## What the service is

One process that follows the production repository's eventlog and replays it
into one or more target repositories, so that stage holds production content.
It is a **copy forward, never back**, and it is **not a backup**: a target
that has been edited keeps its edits, and a target that has been purged is
refilled with the present, not the history.

The process is two halves that share a fate:

* **The API**, on `:1080`, through which targets are configured, started,
  stopped and listed. It is an operator surface that sees a few calls a year.
* **The target workers**, one per enabled target, each under its own job
  lock, each following the source and writing to its target. A worker's
  failures are contained: it is restarted with backoff and given up on after
  an hour, and neither takes the process down.

Everything else, the `LISTEN` that carries target changes between replicas
and the hourly mapping cleanup, is required: if either fails, the process
exits and is restarted.

## Components

| Repository | What it is to us |
|---|---|
| [ttab/elephant-replicant](https://github.com/ttab/elephant-replicant) (this repo) | The service. |
| [ttab/elephant-repository](https://github.com/ttab/elephant-repository) | Both ends: the source whose eventlog we follow, and every target we write to. **Every one of them must run v1.9.0 or later**, since the replicant calls them on their Connect paths. |
| [ttab/elephant-api](https://github.com/ttab/elephant-api) | The `elephant.replicant.Replication` declaration and the generated clients. |
| [ttab/deploy](https://github.com/ttab/deploy) (`apps/replicant`) | The production deployment: a Flux kustomization with the Deployment, ingress, network policy and config. |
| [ttab/helm-elephant](https://github.com/ttab/helm-elephant) (`charts/replicant`) | The chart for other environments. |
| [ttab/elephant-cli](https://github.com/ttab/elephant-cli), [ttab/bruno-elephant](https://github.com/ttab/bruno-elephant) | The two callers of the API: `elephant-cli replicant.replication ...` and the `Replicant` bruno collection. |

## Deployment shape

| Role | Configuration | Runs |
|---|---|---|
| Production, ele000 | `deploy/apps/replicant`: one replica, `Recreate` strategy, source `http://editorial-repository:1080`, target `https://repository.stage.tt.se`, `ALL_ATTACHMENTS=true`, wires and `+meta` types ignored, one event section ignored. Image `ghcr.io/ttab/elephant-replicant:<tag>`. | The only instance; stage runs no replicant. In October 2026 it replicated one target, `stage`, tailing at around 18,000 source events a day. |

Replicas are safe to add: each target's job lock keeps one worker per target
across the fleet, the API is stateless, and a target notification reaches
every replica. What scales is availability, not throughput, since a target is
one worker however many replicas there are. Each replica needs its own
`DB_MAX_CONNS` worth of connections.

Memory is small: 48 to 200 MiB requested in the two deployment shapes, with
`GOMEMLIMIT` set from the container limit in production. Attachment transfers
are streamed, not buffered.

## Runtime dependencies

| Dependency | Needed for | What happens without it |
|---|---|---|
| Postgres (`CONN_STRING`, optionally through `BOUNCER_CONN_STRING`) | Everything: targets, positions, mappings, job locks, notifications. | The process does not start, or if it goes away later: workers lose their locks and restart, the API answers `internal`, the subscriber restarts with backoff up to a minute, and the cleanup task's failure exits the process. |
| The source repository (`REPOSITORY_ENDPOINT`) and its OIDC provider (`OIDC_CONFIG`) | The eventlog and every document, meta, status and attachment read. | At start: the token check fails and the process exits. Later: every worker fails on `read eventlog`, restarts with backoff, and after an hour every target is given up on. Nothing is lost; the position is where it was. |
| Each target repository and its OIDC provider (the target row) | Writing to that target. | That target's worker fails and is given up on after an hour; the others are unaffected. With `accept_errors`, a target that answers errors rather than being unreachable has its events **dropped** instead. |
| S3 (`*.s3.*.amazonaws.com`, via presigned URLs from both repositories) | Attachment transfer, when the sync config selects any. | A document event with a selected attachment fails, and with it the target's progress, as above. |
| The encryption key (`ENCRYPTION_KEY`) | Decrypting the target client secrets. | The process refuses to start without a key. With the wrong key every worker fails at `decrypt client secret` and is given up on. |

What is truly required to replicate: Postgres, the source, the target and the
right key. The API needs only Postgres.

## Endpoints and ports

| Port | Default | What is on it |
|---|---|---|
| `ADDR` | `:1080` | The Replication service on `/elephant.replicant.Replication/` and `/twirp/elephant.replicant.Replication/`, `GET /health/alive`, `GET /version`. Exposed at `replicant.api.tt.ecms.se` in production. |
| `PROFILE_ADDR` | `:1081` | `/metrics`, `/health/ready`, `/debug/pprof/`, `/debug/vars`. Cluster-internal. |
| `TLS_ADDR` | `:1443` | The API over TLS, only when `TLS_CERT_PATH` and `TLS_KEY_PATH` are set. Not used in production, where the ingress terminates TLS. |

Outbound: the source on `:1080` inside the cluster, the target repository,
both OIDC providers and S3 on `443`.

## Data flow

```
 1. configure                     2. replicate (one worker per target, under replicant:<target>)

 operator ──► ConfigureTarget     source Eventlog / CompactedEventlog ──► follower ──► per event:
                 │ (doc_admin)                                                           ├─ filters (type, sub, section, scheduler usable)
                 ▼                                                                       ├─ Get / GetMeta / GetStatus from source
           replication_target ──NOTIFY replicant_target──► every replica's manager       ├─ attachments: GET link ──► PUT target upload
                                                              │ stop + start worker      ├─ Update / Delete on target, IfMatch = last target version
                                                              ▼                          └─ one transaction: document, version_mapping, state
                                                        worker reloads its row
```

The operational weight is in step 2's last line. **The position is only
committed with a successfully handled event**, so a worker that dies
mid-event restarts on that event, and a target that is given up on resumes
from exactly where it stopped when it is started again. Skipped events and
conflicts advance the position at the end of the batch without touching the
target.

A conflict is the one outcome that is both silent and permanent: the document
stops receiving updates in that target until it is deleted in the source or
an operator clears it. See the failure mode below.

## Single-leader work

| Lock | Does | When nobody holds it |
|---|---|---|
| `replicant:<target>` | Runs that target's worker. Ping 10 s, stale 40 s, restart backoff up to 60 s, given up on after an hour of continuous failure. | That target is not replicating. `pg_job_lock_held{name}` is 0; `GetTargetState` says `STOPPED` on every replica. |

The hourly `version_mapping` cleanup runs in every replica without a lock;
the deletes are idempotent.

## Where state lives

| Store | Holds | Authoritative for |
|---|---|---|
| Postgres `replication_target` | The targets and their encrypted secrets. | Which targets exist and whether they are enabled. The environment is only read into it once. |
| Postgres `state`, `document`, `version_mapping` | Per-target position, last-written target versions, source-to-target version map. | Where each target resumes, and which documents are considered in sync. |
| Postgres `job_lock` | Which replica runs which target. | Nothing durable; stale rows are taken over. |
| The target repository | The replicated content. | Itself. The replicant never reads it back except to detect a type change or to check a version. |

Dropping a target's `document` rows makes the replicant treat every document
as new on its next event, which recreates deleted ones and conflicts on
existing ones; dropping its `state` row restarts it from `start_from` in
catch-up mode. Both are recovery tools, not routine.

## Bootstrap order

1. Postgres exists, with the schema migrated: `mage sql:migrate` locally,
   `go run ./cmd/setup db migrate` in elephant-platform. The service never
   migrates its own schema.
2. The encryption key is in Vault at `services/replicant/encryption-key`
   (`scripts/set-encryption-key <mount>` creates one and refuses to overwrite)
   and reaches the pod as `ENCRYPTION_KEY`. **Set it before the first target
   is configured and never change it afterwards**; see the failure modes.
3. The source credentials (`replicant-send`, scopes `doc_read_all` and
   `eventlog_read`) and, per target, a client with `doc_admin` in the target
   environment (`replicant-receive` in stage).
4. Start the service. With `TARGET_REPOSITORY_ENDPOINT` set and no `default`
   row, the environment becomes the `default` target and starts replicating
   from `START_EVENT`. Otherwise configure a target over the API.

Out of order: a target configured before the key is in place cannot be, since
the process does not start without one. A key rotated after targets exist
breaks every target.

## Failure modes

### Stage stops receiving content, and nothing is paging

The worker for the target has been given up on: an hour of failing against
the source, the target, the database or the key, and the lock stopped
restarting it. The process is healthy, the API answers, and
`/health/ready` is `ok`, because readiness says nothing about targets.

* Signal: `pg_job_lock_held{name="replicant:<target>"}` summed over replicas
  is 0 while `ListTargets` reports the target enabled; `GetTargetState`
  answers `STOPPED`; `pg_job_lock_restarts_total{name}` rose and then went
  flat. The last `worker exited with error` log line names the cause.
* Action: fix the cause, then `ChangeTargetState` with `TARGET_ACTION_START`,
  or restart the pod, which starts every enabled target. The target resumes
  from its stored position; nothing was lost.

### A target is restarting over and over

The same failure on every start. `pg_job_lock_restarts_total{name}` rising,
`pg_job_lock_held` flapping, and the log alternating between `starting
replication` and the error. Read the error: `set up target authentication`
or a 401 from the target is credentials; `decrypt client secret` is the key;
`read eventlog` is the source; `update target` without `accept_errors` is
one event the target refuses. The budget is an hour, after which it becomes
the failure mode above.

### A document in stage never updates

The document was edited in the target, directly or by something else writing
to stage, and every update since has conflicted. **This is by design and it
is permanent for the document**, see
[ADR-0004](adr/0004-edits-in-the-target-win.md).

* Signal: `conflict with change in target repo` at info level with the
  document UUID; there is no metric. A document that has it once has it on
  every later event.
* Action: delete the document in the target and delete its row from
  `document` for that target name; the next source event recreates it. There
  is no RPC for this yet: `SendDocument` is declared but unimplemented.

### A status is set in production but never in stage

Three legitimate causes, none of them errors: the status is `usable` and was
set by `internal://scheduler`, which is skipped on purpose; the status is on a
version the target never received, because the document was skipped or
conflicted; or the version was replicated more than six months ago and its
mapping has been cleaned up. All three are a `skipped import of document`
debug line. A fourth, during catch-up, is that only status heads on the
current version are carried.

### Changing the environment did nothing

`TARGET_*`, `IGNORE_*`, `ALL_ATTACHMENTS` and `START_EVENT` are read into
the `default` target only when no such row exists. Once it does, the row wins.

* Action: `ConfigureTarget` with the name `default` and the full desired
  configuration, which restarts the worker with it. Raising `start_from` moves
  the target forward; lowering it does nothing, since the stored position
  wins when it is larger.

### Content is replicating, but with the history missing

The target is in catch-up: `eventlog_follower_position{state="compact"}`.
Each document arrives once, at its current version with its current heads,
and intermediate versions and statuses are not replayed. This is what a long
stop looks like when it ends, and it is correct; it becomes a problem only if
`state` never flips to `tail`, which means the worker is not reaching the end
of the log, and the restart cause above applies.

### A target is skipping documents silently

`accept_errors` is set and the target repository is refusing updates:
`error from target repo` at error level, one per dropped event, and the
position keeps moving. The refusal is usually a schema the target does not
have, or a scope the target client lacks. Each dropped event is that
document's update lost until its next event. Turning `accept_errors` off
turns the drop into a restart loop on the first refused event.

### ConfigureTarget was accepted, but no worker changed

The notification did not arrive: the `LISTEN` connection was dead when it was
published. The subscriber detects that within twelve minutes (a ping every
five minutes, seven minutes of grace) and reconnects, and on reconnect it
reconciles the workers with the table, so a lost `start`, `stop` or `remove`
heals itself by then. A lost `configure` does not: the worker keeps running
on the old row, and nothing can tell a changed row from an unchanged one.

* Signal: no `received target notification` log line after the call, and
  `GetTargetState` or the worker's behaviour disagreeing with the row. A
  `listener ping timeout, reconnecting` warning followed by `reconciled
  workers with the enabled targets` is the reconnect happening.
* Action: wait for the reconnect, or issue the `ConfigureTarget` again once
  `reconciled workers` has been logged; a repeat is harmless, since it
  restarts the worker from its stored position. A pod restart also does it.

### The pool is saturated

More targets than `DB_MAX_CONNS` allows for, or a bouncer with too small a
server pool. `pgxpool_empty_acquires_total{pool="main"}` rising, then
`pg_job_lock_transitions_total{state="lost"}`, then restarts, since a lock
ping that waits more than five seconds loses the lock. Raise `DB_MAX_CONNS`
by two per target.

### The encryption key was rotated

Every target fails at `decrypt client secret` on every restart and is given
up on within the hour. The old secrets are unrecoverable without the old key.

* Action: restore the old key, or `ConfigureTarget` every target again with
  its secret under the new key. Then start each target.

### A worker is stuck without failing

The follower position is flat while the lock is held and nothing is logged.
The likely place is an attachment transfer: the download and the upload use
`http.DefaultClient` with no timeout, so a stalled S3 connection holds the
worker inside its transaction indefinitely, while the lock pings keep the
lock. Restart the pod. Timeouts on the transfer are pending work.

## What to watch, in order

1. `sum by (name) (pg_job_lock_held{name=~"replicant:.*"})` against the
   enabled targets: a 0 is a target nobody is replicating, and it is the
   failure that does not page by itself.
2. `eventlog_follower_position{state="tail"}` against the source's latest
   event id: the lag, and whether it is moving. Flat while the lock is held is
   a stuck worker.
3. `rate(pg_job_lock_restarts_total[15m])`: a worker failing on the same
   thing; it goes quiet when the hour is up, so catch it while it is noisy.
4. `error from target repo` log rate: with `accept_errors` set this is the
   count of updates being lost.
5. `pgxpool_empty_acquires_total{pool="main"}`: headroom, and the warning
   before the lock pings start losing.

## Common operations

**List the targets:** `elephant-cli replicant.replication list-targets
--env prod`, or the bruno collection. The `enabled` flag is the row;
`GetTargetState` is whether this replica is running it.

**Add or change a target:** `ConfigureTarget` with every field, since it is an
upsert of the whole row, and the secret in clear text; it is encrypted before
it is stored. The worker restarts with the new configuration.

**Stop or start a target:** `ChangeTargetState` with `TARGET_ACTION_STOP` or
`TARGET_ACTION_START`. Stop keeps the row and the position; start resumes.

**Remove a target:** `RemoveTarget` deletes the row, the position, and the
`document` and `version_mapping` rows for the target. The target repository
keeps what was replicated.

**Replay from a point:** `ConfigureTarget` with a higher `start_from`; it is
a floor on the stored position, so it only ever moves forward. To move
backward, stop the target and update or delete its `state` row by hand, which
restarts it in catch-up.

**Check what a replica is running:** `GetTargetState` per target, and
`pg_job_lock_held` for the fleet view.

**Deploy:** tag `vX.Y.Z` on `main`; the build workflow pushes
`ghcr.io/ttab/elephant-replicant:vX.Y.Z`; bump the image in `deploy`. One
replica with `Recreate` means a short gap in replication per deploy, during
which the position simply waits.

## Security

Inbound, every method needs a token from the environment's OIDC provider with
`doc_admin`, except `SendDocument`, which also accepts `doc_write` and does
nothing. There is no anonymous path but `/health/alive`, `/version` and the
profile listener.

Outbound, the replicant holds two kinds of credentials: the source client in
the environment, with read-only scopes, and a client per target with
`doc_admin` in the *target's* environment, stored encrypted in Postgres. The
encryption key is the thing that turns the table into secrets; it lives in
Vault and nowhere else. The replicant never writes to the source.

The network policy in production allows egress to the source repository in
the cluster, the two OIDC providers, the stage repository and S3, and nothing
else.

## Not in place yet

* **`SendDocument`** is declared and answers `unimplemented`. It is the
  operation every recovery above wants: re-send one document, forcing past a
  conflict.
* **A `configure` lost while the `LISTEN` connection was dead is not
  recovered** by the reconcile on reconnect, which can only see enabled
  versus running. Comparing the row's `updated` timestamp with the worker's
  start would close it.
* **No timeouts on attachment transfers**, and no size limit. A stalled S3
  connection stalls the worker.
* **No counters for conflicts, skips or accepted errors.** The failure modes
  that are decided per event are visible only in the logs.
* **`GetTargetState` is per replica.** With more than one replica it answers
  for the replica the request landed on, so a target running elsewhere reads
  as `STOPPED`; `pg_job_lock_held` is the fleet-wide answer.
