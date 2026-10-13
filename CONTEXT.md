# Elephant Replicant

The domain language of a service that copies documents from one Elephant
repository into others by replaying the source's eventlog. This file is a
glossary; the mechanisms are in [`docs/architecture.md`](docs/architecture.md).

The words of the document platform itself are not redefined here. Document,
version, URI, type, status, status head, ACL, scope, event, eventlog,
attached object, upload, delete record and lock are
[`elephant-repository`'s](https://github.com/ttab/elephant-repository/blob/main/CONTEXT.md)
and mean the same thing in this repository.

## Ends

**Source**:
The repository whose eventlog is followed and whose documents are read. There
is exactly one per process, configured in the environment, and it is never
written to.
_Avoid_: origin, upstream, primary, master

**Target**:
A repository that documents are written to, together with everything the
service knows about writing to it: its endpoint, its credentials, its sync
config, its position. Stored as a row in `replication_target` and named by
the operator.
_Avoid_: destination, replica, downstream, sink, replication target (in prose; the table keeps the name)

**Default target**:
The target named `default`, created once from the `TARGET_*` and `IGNORE_*`
environment variables when no such row exists. After that it is a target
like any other.
_Avoid_: env target, static target, legacy target

## The worker

**Worker**:
The goroutine that replicates one target: a follower on the source, a client
on the target, and the loop between them. One per enabled target, under the
job lock `replicant:<target>`.
_Avoid_: replicator, syncer, job, runner

**Position**:
The id of the last source event a target has handled, stored in `state` under
`<target>:log_state` together with whether the follower is caught up.
_Avoid_: offset, cursor, checkpoint, watermark

**Catch-up**:
The mode a worker is in while it is behind the source's log: it reads the
compacted eventlog and replicates each document's present state rather than
its history. Reported as `state="compact"`.
_Avoid_: backfill, bootstrap, initial sync, replay (replay is what happens in the tail)

**Tail**:
The mode a worker is in once it has reached the end of the log: it long-polls
the eventlog and replays each event as itself. Reported as `state="tail"`.
_Avoid_: live mode, real-time mode, streaming

**Start from**:
A target's `start_from`: a floor on its position, applied each time the
worker starts. Raising it skips ahead; lowering it does nothing.
_Avoid_: start event (the environment variable keeps that name), start offset

## Outcomes of an event

**Handled**:
The event was replicated and its position committed in the same transaction.
_Avoid_: processed, applied, synced

**Skipped**:
The event was deliberately not replicated and the position moved past it:
an ignored type, sub or section, a scheduler-set `usable`, a document the
source no longer has, or a status on a version the target never received.
Nothing is retried.
_Avoid_: ignored (the sync config *ignores* types; the worker *skips* events), dropped, filtered

**Conflict**:
A target refused an update because the document's version there is not the
one the service last wrote, which means it was edited in the target. The event
is skipped, and every later event for that document conflicts too.
_Avoid_: version mismatch, precondition failure, collision, clash

**Accepted error**:
An error from the target that is logged and skipped because the target's sync
config has `accept_errors` set. The update is lost.
_Avoid_: tolerated error, soft failure, ignored error

**Given up**:
The state of a worker whose job lock stopped restarting it after an hour of
continuous failure. The target stays enabled and reads as `STOPPED` until it
is started again or the process restarts.
_Avoid_: dead, crashed, failed permanently, disabled (disabled is the row's flag)

## What the service remembers

**Target version**:
The version of a document in the target that the service last wrote, kept in
`document` per target, and sent as `IfMatch` on the next update. It is what
makes a conflict detectable.
_Avoid_: remote version, replicated version, last version

**Version mapping**:
The record that source version *n* of a document became target version *m*,
in `version_mapping`. It is what lets a status on a source version be set on
the right target version, and it is cleaned up after six months.
_Avoid_: version map, version pair, translation

**Sync config**:
The per-target `SyncConfig`: what to ignore, which attachments to carry, and
whether to accept errors.
_Avoid_: filter config, replication settings, target options

**Content filter**:
The part of the sync config that needs the document to decide: today the
ignored sections, matched against a document's `section` links.
_Avoid_: section filter (one instance of it), document filter

**Target notification**:
The `{"name", "action"}` message published on the `replicant_target` channel
after a target changes, so every replica's target manager stops, starts or
restarts the worker. Actions: `configure`, `remove`, `start`, `stop`.
_Avoid_: event (events are the source's), signal, trigger

## Known exceptions

The shipped identifiers that disagree with the words above are kept as they
are: the table is `replication_target`, the proto service is `Replication`
with `ConfigureTarget` and `ChangeTargetState`, the environment variable is
`START_EVENT`, and the proto's `AttachmentForType` documents its `type` and
`name` fields the wrong way round (`type` is the document type, `name` the
attachment name, as the `name.type` environment form and the code agree).
