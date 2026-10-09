# Architecture

How elephant-replicant is put together: what runs in the process, how an
event in the source repository's eventlog becomes an update in a target
repository, what the service remembers about each target, and the API surface
that manages targets. Start here to understand the system or to change it.

| Document | What it settles |
|---|---|
| [`ops.md`](ops.md) | The operator's-eye view: dependencies, deployment shape, bootstrap order, and the failure modes with the signal that shows each one. |
| [`observability.md`](observability.md) | Every metric the service exports and what a change in it means. |
| [`../README.md`](../README.md) | Orientation, the development workflow, and the configuration reference: every flag and its environment variable. |
| [`../CONTEXT.md`](../CONTEXT.md) | What the words mean: source, target, catch-up, conflict, version mapping. |
| [`adr/`](adr/) | Why the deliberate decisions went the way they did. |

This document does not describe how to run the service or what to do when it
breaks; that is [`ops.md`](ops.md).

The service is built on [elephantine](https://github.com/ttab/elephantine):
the API server with its two RPC mounts, authentication, the `pg` pools,
`pg.FanOut` for notifications, `joblock.Run` for the per-target leader
election, and graceful shutdown. The eventlog is followed with
[koonkie](https://github.com/ttab/koonkie). The service definition is
`elephant.replicant.Replication` in
[elephant-api](https://github.com/ttab/elephant-api/blob/main/replicant/service.proto).

## What the service does

The replicant copies documents from one Elephant repository, the **source**,
into one or more other repositories, the **targets**. It follows the source's
eventlog and replays each event as an `Update` or `Delete` call against the
target, carrying the document content, the statuses, the ACL and, when
configured, the attached objects. It exists to keep a stage or QA environment
populated with production content; **it is not a backup or a standby**, and it
never replicates in the other direction.

Two properties shape everything below:

* **A document that has been edited in the target stops receiving updates.**
  Every update is conditional on the target version the replicant wrote last,
  and a conflict is logged and skipped, not retried. See
  [conflicts](#conflicts-edits-in-the-target-win).
* **Catching up replicates the present, not the history.** Behind the log,
  the worker reads the compacted eventlog and writes each document's current
  version and current status heads. Only once it has caught up does it replay
  events one by one. See [catch-up and tail](#catch-up-and-tail).

## Process model

`cmd/replicant` parses the flags, builds the database pools, verifies the
source credentials, and hands over to `internal.Run`, which starts four tasks
under one `elephantine.ErrGroup`:

* **server**: the API server on `ADDR` (default `:1080`), with the Twirp and
  Connect mounts of the Replication service, `GET /health/alive` and
  `GET /version`; and the profile server on `PROFILE_ADDR` (default `:1081`)
  with `/metrics`, `/health/ready` and `/debug/pprof/`. A TLS listener on
  `TLS_ADDR` (default `:1443`) is added when `TLS_CERT_PATH` is set.
* **target-manager**: loads every enabled target from `replication_target`,
  starts a worker per target, and then applies target notifications as they
  arrive. One goroutine per target; see [the target manager](#the-target-manager).
* **pg-subscribe**: a `pg.NewSubscriber` with a `LISTEN` on the
  `replicant_target` channel that feeds the in-process `pg.FanOut` the target
  manager reads. The `LISTEN` runs on the direct pool, because a session-level
  `LISTEN` does not survive a transaction pooler
  ([ADR-0001](adr/0001-listen-on-a-direct-pool.md)), and so do the pings that
  prove the connection alive. See
  [the subscriber](#the-subscriber).
* **cleanup**: once an hour, deletes `version_mapping` rows older than six
  months. See [version mappings](#version-mappings).

**If the server, the target manager or the cleanup returns an error, the
group context is cancelled, the others stop, and the process exits
non-zero.** Kubernetes restarts it. The subscriber is the exception: it runs
with retries and is restarted with backoff, one second growing to a minute, for
as long as it fails, so a database failover that resets the `LISTEN`
connection restarts the subscriber, not the process. A target worker failing does not reach this level either: the
target manager logs and contains it, as described below.

Shutdown is two-phase through `elephantine.NewGracefulShutdown` with a ten
second grace: the target manager, the subscriber and the cleanup are cancelled
at *stop*, the API server at *quit* ten seconds later, so an in-flight RPC can
finish while the workers wind down.

### Database pools

`pg.NewPools` builds the pools from `CONN_STRING`, `BOUNCER_CONN_STRING` and
`DB_MAX_CONNS`:

* Without a bouncer: one direct pool of `DB_MAX_CONNS` connections (default
  8) carries queries, the job locks and the `LISTEN`.
* With a bouncer: the **main** pool of `DB_MAX_CONNS` connections goes
  through the bouncer and carries everything except the `LISTEN`, which gets a
  **pubsub** pool of two direct connections.

The default of 8 is derived from the worker shape: each enabled target holds at
most one transaction and one job lock, and a lock ping that cannot get a
connection within five seconds loses the lock and restarts the worker, so a
target needs two connections and three targets need six. The admin RPCs, the
cleanup and (without a bouncer) the `LISTEN` take the rest. **Raise
`DB_MAX_CONNS` when running more than three targets.**

## Targets

A target is a row in `replication_target`: a name, the target repository URL,
the OIDC discovery URL, a client id and an encrypted client secret, a
`start_from` event id, a JSON `SyncConfig`, and an `enabled` flag. Targets are
created and changed through the API, and the service reads them from the
table; nothing about a target lives in configuration files.

### The default target

The environment variables a single-target deployment was configured with
(`TARGET_REPOSITORY_ENDPOINT`, `TARGET_OIDC_CONFIG`, `TARGET_CLIENT_*`,
`IGNORE_*`, `INCLUDE_ATTACHMENTS`, `ALL_ATTACHMENTS`, `START_EVENT`,
`ACCEPT_ERRORS`) are turned into a target named `default` **on the first start
that finds no such row**. After that the row is authoritative: **changing the
environment variables does not change an existing default target**. Use
`ConfigureTarget` with the name `default`, or remove the target and restart.
A deployment with no `TARGET_REPOSITORY_ENDPOINT` registers nothing and waits
for `ConfigureTarget`.

### Sync config

`SyncConfig` decides what a target receives:

| Field | Effect |
|---|---|
| `ignore_types` | Events for these document types are skipped. |
| `ignore_subs` | Events whose updater URI is one of these are skipped; `core://application/elephant-wires` keeps wire ingestion out of stage. |
| `ignore_sections` | A list of `(type, section UUID)`. A document of that type that links to that section is skipped, which needs the document to be fetched before the decision; see [content filtering](#content-filtering). |
| `all_attachments` | Every attached object the event names is transferred. |
| `include_attachments` | Otherwise, only the `(type, name)` pairs listed here are transferred. The environment form is `name.type`, as in `image.core/image`. |
| `accept_errors` | A failed update is logged and skipped instead of stopping the worker. See [accept errors](#accept-errors). |

The proto's comments on `AttachmentForType` have the two fields' descriptions
swapped; the code and the `name.type` environment form agree that `type` is
the document type and `name` the attachment name.

## The target manager

The target manager owns one goroutine per enabled target and keeps that set
in step with the table:

* On start it lists the enabled targets and starts a worker for each.
* A **target notification**, `{"name", "action"}` on the `replicant_target`
  channel, is published by the API handlers after they commit and is received
  by every replica through [the subscriber](#the-subscriber). `configure` stops and restarts the
  worker so it reloads its row, `remove` and `stop` stop it, `start` starts
  it. A notification for a name with a running worker is a no-op for `start`,
  and a stop waits for the worker to finish.
* `GetTargetState` answers `RUNNING` if the manager has a goroutine for the
  name and `STOPPED` otherwise; `STARTING` and `STOPPING` are declared in the
  proto but never reported.

### The subscriber

Target notifications are the only way a running replica learns that a target
changed, so the `LISTEN` connection that carries them has to be known to be
alive. `pg.NewSubscriber` proves it with pings: every five minutes it sends
`NOTIFY listener_ping` through the same direct pool, and the listen loop waits for
notifications with a deadline seven minutes after the last ping it received.
A connection that has gone silently dead, through a bouncer or load balancer
idle timeout or a partition, therefore fails the wait within twelve minutes
of dying, is closed, and is reopened five seconds later. Any other error on
the connection, such as a failover resetting it, ends `Run`, and the task's
retries start it again, a second later at first and up to a minute later if it
keeps failing. The pings go through the direct pool rather than the bouncer,
as in every other elephant service, so the health of the direct connection is
never judged through the bouncer.

**A notification published while the connection was dead is gone.** To
cover that, the subscriber calls `TargetManager.Reconcile` before its first
listen and after every reconnect: it lists the enabled targets, starts a
worker for each that has none, and stops the workers of targets that are
disabled or removed. A `start`, `stop` or `remove` lost in the gap therefore
takes effect at the reconnect. A `configure` whose row changed under a worker
that kept running is the one case reconciliation cannot see, since nothing
distinguishes a changed row from an unchanged one; issuing `ConfigureTarget`
again is the fix, and `GetTargetState` is the check. `Run` uses the same
reconcile pass to start the workers, so a reconcile that fires before the
manager is running is a no-op rather than a race.

### The job lock

Each worker runs under `joblock.Run` with the lock name `replicant:<target>`,
so **exactly one replica replicates a given target at a time**, and a second
replica can take over when the holder dies. The lock is pinged every ten
seconds with a five second timeout and counts as stale after forty; a ping
that fails loses the lock, and the worker is restarted under it.

The lock's restart policy is also the worker's supervision: a worker that
returns an error is restarted with exponential backoff, and **a worker that
has spent an hour failing is given up on**. The manager then drops it from its
map, so `GetTargetState` reports `STOPPED` while the row still says `enabled`,
and a later `start` notification or a process restart can bring it back. The
hour and why it is not shorter are in
[ADR-0002](adr/0002-a-failing-worker-gives-up-after-an-hour.md).

### Starting a worker

`workerFunc` is what runs under the lock, from the top on every restart:

1. Load the target row and decode its `SyncConfig`.
2. Decrypt the client secret with the encryption key and build an OAuth2
   client-credentials token source against the target's OIDC provider, asking
   for `doc_admin`. A target whose credentials are refused fails here, every
   restart, until the hour is up.
3. Build the target `Documents` client, the content filter, and load the log
   state `<target>:log_state`. **The position is the larger of the stored
   position and the row's `start_from`**, so raising `start_from` on an
   existing target skips ahead and lowering it does nothing.
4. Start a koonkie log follower from that position and hand it to the worker
   loop.

## Data flow

```
 source repository                        replicant                          target repository
 ─────────────────                        ─────────                          ─────────────────
 Documents.Eventlog ──(tail, 100/batch, ──► koonkie follower ──► handleEvent ──► Documents.Update
 Documents.CompactedEventlog (catch-up)    10s long poll)          │              Documents.Delete
                                                                  │              Documents.CreateUpload
 Documents.Get / GetMeta / GetStatus ◄────────────────────────────┤
 Documents.GetAttachments (download link) ◄───────────────────────┤
                                                                  ▼
                                      one transaction on the main pool per event:
                                      document(target_name,id) ── last target version
                                      version_mapping ──────────── source → target version
                                      state(<target>:log_state) ── position, caught up
```

### The worker loop

`Worker.Replicate` asks the follower for the next batch and handles each
event in turn. `workflow` events are ignored outright. The outcome of an event
decides what happens to the position:

| Outcome | Log level | Position |
|---|---|---|
| handled | debug | committed with the event, inside its transaction |
| skipped (`ErrSkipped`) | debug | advanced at the end of the batch |
| conflict (`ErrConflict`) | info | advanced at the end of the batch |
| error, `accept_errors` set | error | advanced at the end of the batch |
| error, `accept_errors` unset | returned | **not advanced**; the worker restarts on the same event |

So without `accept_errors` an event that keeps failing holds the target at
that position, through the restart backoff, until the hour is up and the
worker is given up on. With it, the event is lost and the target moves on.

### Catch-up and tail

The log state records a position and whether the follower is caught up. The
follower reads the **compacted** eventlog while behind, which yields one event
per document in the window rather than every event, and switches to the plain
eventlog, the **tail**, once it reaches the end.

In catch-up every event is treated as a document event: the worker reads the
document's current meta, takes its current version as the version to copy,
and folds in every status head that points at that version, except a `usable`
set by `internal://scheduler`. For a document the target has not seen, the
current ACL and the current attached objects come from the meta as well; for
one it has, the ACL is left alone. **The history between the stored position
and now is never replicated**: intermediate versions, statuses on older
versions, ACL changes that were later changed back. The trade-off is
[ADR-0003](adr/0003-catch-up-replicates-the-present.md).

In the tail each event is replayed as itself:

* `document`: fetch that version, replicate it, with any ACL the event
  carries folded onto the update (the repository has carried ACL changes on
  the document event since v1.8.0). Attachments are transferred if configured.
* `status`: look up the target version the source version was mapped to, and
  set the status on it. **A status on a version the target never received, or
  whose mapping has been cleaned up, is skipped.**
* `acl`: read the current ACL from meta and apply it.
* `delete_document`: delete in the target with the original delete record id
  as metadata, and forget the document locally; see
  [deletes](#deletes).

A `usable` status created by `internal://scheduler` is skipped in both modes,
on the grounds that it is the scheduler's output rather than editorial
intent; the commit that added the rule records no further reasoning.

### Content filtering

`ignore_sections` is the one filter that cannot be decided from the event: the
worker fetches the document, and skips it if a `section` link matches. The
fetched document is reused for the update when it is the version the event
names, so the filter costs one extra `Get` only when it is not.

### Conflicts: edits in the target win

The `document` table remembers, per target, the target version the replicant
last wrote. Every update of a known document carries that as `IfMatch`, and
the target refuses with `failed_precondition` if its current version is
another one, which means somebody or something else wrote to the document in
the target. The worker logs the conflict and skips the event.

**The consequence is permanent for that document.** The remembered version is
not updated on a conflict, so every later event for the document conflicts
too, until the document is deleted in the source, which deletes it in the
target and forgets it locally, or an operator deletes it in the target and
removes its `document` row so the next event recreates it. The decision is
[ADR-0004](adr/0004-edits-in-the-target-win.md).

Two related paths are not conflicts:

* A `not_found` on an update that carried no document body means the target
  has lost the document, typically a purge in stage. The worker fetches the
  current document from the source and retries the update with it.
* A document that exists in the target under a **different type** than the
  source's is deleted in the target before the first update, so that the
  update creates it afresh; the deletion is logged at warning level.

### Attachments

For a document event that names attached objects, each object the sync config
selects is streamed through the replicant: `GetAttachments` with a download
link on the source, `CreateUpload` on the target, a `GET` of the link piped
into a `PUT` of the presigned upload URL, and the upload id attached to the
update. The streams use `http.DefaultClient`, which has no timeout, and the
transfer happens inside the event's transaction while the job lock is held.

### Deletes

A `delete_document` event removes the document's `document` row and all its
`version_mapping` rows, then deletes it in the target with
`original_delete_record` in the delete metadata, in one transaction. A later
restore in the source arrives as ordinary events for a document the target
does not know, and is replicated as a new document.

### Version mappings

`version_mapping(target_name, id, source_version, target_version)` is what
lets a status event in the tail find the target version to set the status on.
The cleanup task deletes rows older than six months every hour, so **a status
set on a version replicated more than six months ago does not reach the
target**. That is accepted: statuses that old are not what a stage environment
is kept populated for.

### Accept errors

With `accept_errors` set, an error from the target that is neither a conflict
nor a skip is logged at error level and the event is dropped. It keeps a
target moving past a document the target repository refuses, at the cost of
silently losing that document's update; the error log line is the only trace.
Without it, the worker restarts on the event until it succeeds or the hour is
up.

## Secrets

Target client secrets are encrypted at rest with AES-256-GCM under the
32-byte key in `ENCRYPTION_KEY`, with a random nonce per encryption. The key is
required to start, and **a changed or lost key makes every stored target
unusable**: the worker fails at decryption on every restart. Reconfiguring each
target with its secret is the recovery. The source credentials are not stored;
they come from the environment on every start.

## Storage

| Table | Holds | Who writes it |
|---|---|---|
| `replication_target` | One row per target: endpoint, OIDC, credentials, `start_from`, sync config, `enabled`. | The API handlers; the default-target registration on first start. |
| `state` | `<target>:log_state`: `{"Position", "CaughtUp"}`. | The worker, in the event transaction or at the end of a batch. |
| `document` | `(target_name, id)` to the target version last written. | The worker on a document update; removed on delete and on `RemoveTarget`. |
| `version_mapping` | `(target_name, id, source_version)` to target version, with a created timestamp. | The worker on a document update; the cleanup task after six months. |
| `job_lock` | The per-target leader locks. | `joblock.Run`. |

Migrations are tern files in `schema/` and are never run by the service
itself. `schema/reporting_tables.json` names the tables elephant-platform
grants the reporting role read access to; only `state` is listed.

## The API

`elephant.replicant.Replication` is served on both stacks, with one set of
service options in front of both:

| Family | Path | Protocols |
|---|---|---|
| Connect | `POST /elephant.replicant.Replication/<Method>` | Connect, plus gRPC and gRPC-Web in-cluster |
| Twirp | `POST /twirp/elephant.replicant.Replication/<Method>` | Twirp |

Authentication is required: a missing or invalid token is answered
`unauthenticated` (401) by the shared middleware before a handler runs. The
handlers construct their errors with the `elephantine/rpc` helpers and the
Twirp mount translates them back, so the two mounts answer the same code,
message and metadata; `TestErrorParity` holds them to it and
`internal/testdata/error_bodies/` pins the raw bodies. The Twirp mount is kept
for `elephant-cli` and the bruno collection and goes away in a future major
release.

| Method | Scope | Does |
|---|---|---|
| `ConfigureTarget` | `doc_admin` | Upserts the target with `enabled = true`, encrypts the secret, publishes `configure`. |
| `RemoveTarget` | `doc_admin` | Deletes the row, the target's `document`, `version_mapping` and log state, publishes `remove`. The target repository is untouched. |
| `ChangeTargetState` | `doc_admin` | Sets `enabled` and publishes `start` or `stop`. `not_found` for an unknown name. |
| `GetTargetState` | `doc_admin` | `RUNNING` or `STOPPED`, from this replica's worker map. |
| `ListTargets` | `doc_admin` | Name, repository URL and `enabled` for every target. |
| `SendDocument` | `doc_admin` or `doc_write` | **Unimplemented**; answers `unimplemented`. |

The API is an operator surface. Over the year to October 2026 it saw four
calls in production, all from TT's own tooling.

### Error bodies

The two mounts differ in three ways a caller that moves has to know about:
the error body is `{"code","message","details"}` with the metadata in an
`elephantine.rpc.ErrorMeta` detail rather than `{"code","msg","meta"}`;
`failed_precondition` is 400 rather than 412, `canceled` 499 rather than 408
and `deadline_exceeded` 504 rather than 408; and a JSON response spells its
fields in lowerCamelCase (`repositoryUrl`) where Twirp spells them as the
`.proto` declares them (`repository_url`). Requests accept either spelling.

### Outbound credentials

| Direction | Credential | Scopes |
|---|---|---|
| Source repository | `OIDC_CONFIG`, `CLIENT_ID`, `CLIENT_SECRET`, from the environment | `doc_read_all`, `eventlog_read` |
| Each target repository | The target row's OIDC config, client id and decrypted secret | `doc_admin` |

The source token is fetched once at start to verify the credentials, and the
process refuses to start without it.
