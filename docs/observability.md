# Observability

What elephant-replicant exports, and what a change in each number means. The
operational reading, which to watch first and which failure mode each one is
the signal for, is [`ops.md`](ops.md); the mechanisms behind them are in
[`architecture.md`](architecture.md).

| Document | What it settles |
|---|---|
| [`architecture.md`](architecture.md) | How the service is built: the process model, the worker loop, catch-up and tail, conflicts, the API. |
| [`ops.md`](ops.md) | Dependencies, deployment shape, failure modes and what to watch. |
| [`../README.md`](../README.md) | Build, run and configure. |

Metrics are registered on the default Prometheus registerer and served on the
profile listener, `PROFILE_ADDR` (default `:1081`), at `/metrics`. **Nothing
in this repository declares a collector of its own**; every series comes from
a library, with the replicant's identifiers in the labels:

* `eventlog_follower_position{follower,state}` from koonkie, one series per
  target.
* `pg_job_lock_*{name}` from `elephantine/pg/joblock`, one lock per target.
* `pgxpool_*{pool}` from `elephantine/pg`, for the `main` pool and, behind a
  bouncer, the `pubsub` pool.
* `rpc_*` from `elephantine/rpc`, for the Replication service on both mounts.
* `task_restarts_total` from `elephantine.ErrGroup`, registered but never
  incremented, since no task runs with retries.

The service registers no readiness checks, so `/health/ready` is always `ok`
and there is no `health_check_up` series. Readiness says the process is up,
not that any target is replicating.

## Replication progress

The pair to watch per target is the follower position against the lock: a
position that is not moving while the lock is held is a stuck worker, a
position that is not moving while the lock is unheld is a stopped one.

* `eventlog_follower_position{follower="<target>",state}`: the source event id
  the target's follower last reported, set after every batch. `state` is
  `compact` while the follower is catching up through the compacted eventlog
  and `tail` once it streams the plain one. **A target in `compact` is
  replicating the present rather than the history**, so expect it after a
  long stop and expect it to end; one that stays in `compact` is not reaching
  the end of the log. In `tail`, the gap between this and the source
  repository's latest event id is the replication lag; it is measured in
  events, not seconds, and the number to compare against lives in the source
  repository, not here. The series is written by whichever replica holds the
  target's lock and goes stale on the others.

There is no counter for skipped events, conflicts or accepted errors. Those
are log lines: `skipped import of document` and `handled event` at debug,
`conflict with change in target repo` at info, `error from target repo` at
error. Counting them is a log query.

## Job locks

Each target's worker runs under the lock `replicant:<target>`. The lock
metrics are the supervision signal for the worker, since the worker's own
failures are contained there rather than surfaced as a task restart.

* `pg_job_lock_held{name="replicant:<target>"}`: 1 on the replica holding it.
  `sum by (name)` should be exactly 1 for every enabled target. **0 for an
  enabled target means nobody is replicating it**: either every replica gave
  up on it after an hour of failures, or the process is down.
* `pg_job_lock_transitions_total{name,state}`: alert on the `lost` rate. A
  lost lock means the holder could not ping within five seconds, which is
  either a saturated pool or a database that is not answering, and every
  handover restarts the worker from the stored position.
* `pg_job_lock_restarts_total{name}`: counts restarts of the worker after an
  error return. **Routinely zero.** A rising rate is a worker failing on the
  same thing on every start: credentials refused by the target, the source
  unreachable, the encryption key wrong, or without `accept_errors` an event
  the target keeps refusing. The rate falls off as the backoff grows, and
  stops entirely when the hour is up and the lock gives up, which is when
  `pg_job_lock_held` goes to 0.

## Database pools

* `pgxpool_*{pool="main"}`: the query pool, sized by `DB_MAX_CONNS` (default
  8). `pgxpool_empty_acquires_total` rising is the pool saturating: with each
  target needing two connections, this is what adding targets without raising
  `DB_MAX_CONNS` looks like, and the lock pings are the first to suffer.
  `pgxpool_acquired_conns` against `pgxpool_max_conns` is the headroom.
* `pgxpool_*{pool="pubsub"}`: only present behind a bouncer. Two direct
  connections, of which the `LISTEN` holds one; anything but a flat line is
  surprising.

## RPC

Both mounts report into one set of series, with `service` equal to
`elephant.replicant.Replication`. The traffic is operator traffic, a handful
of calls a year, so these are for confirming a call happened rather than for
rates.

* `rpc_requests_total{service,method,customer}`: calls that reached a handler.
* `rpc_duration_seconds{service,method,customer}`: handler runtime. Every
  method is a few queries; a slow one is the database.
* `rpc_responses_total{service,method,status,customer}`: responses by HTTP
  status. A Connect `failed_precondition` is a 400, a Twirp one a 412.
* `rpc_protocol_responses_total{service,method,protocol,code,client_id}`:
  responses by protocol and RPC code. `protocol="twirp"` going to zero for
  every method is what says the Twirp mount has no callers left; `client_id`
  names the application the token was issued to, which is how to find the
  callers that still have to move.

A call the authentication middleware refuses is counted as a response and not
as a request, so a gap between the two is callers turned away with 401.

## Conventions

A gauge written by a lock holder, which here is the follower position, keeps
its last value on a replica that lost the lock; read it across replicas with
`max by (follower)`, not `sum`. Every series is per process, and a Recreate
deployment restarts from zero, so counters reset on every deploy.
