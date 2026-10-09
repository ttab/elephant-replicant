# A failing target worker gives up after an hour

A target's worker runs under `joblock.Run`, whose restart policy is the
worker's supervision. Its failure budget, `GiveUpAfter`, is set to one hour
(`workerGiveUpAfter` in `internal/target_manager.go`), twelve times the
library's five minute `HealthyRuntime`, rather than the few minutes the
elephantine migration guide uses as its example. The failures that reach the
budget are all external to the target: the target repository unreachable or
refusing the credentials, the source down, our own database gone. An hour is
long enough to sit out an incident at the other end without a human; a target
that has failed continuously for longer than that is something a human has to
look at anyway.

Giving up is deliberately not "forever": the budget was introduced with the
move to `joblock.Run` in #69, where the old `pg.RunInJobLock` restarted
immediately and indefinitely and a worker whose dependency was down turned
into a tight loop of lock release and re-acquire against the shared database.

## Consequences

* A given-up target stays enabled in the table and reads as `STOPPED`, and
  nothing restarts it until a `start` notification or a process restart. That
  is the one failure in the service that does not page by itself;
  `pg_job_lock_held` going to 0 for an enabled target is the signal.
* The manager drops a given-up worker from its map so that a later `start`
  is not a no-op.
