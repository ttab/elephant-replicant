# LISTEN runs on a direct pool, queries go through the bouncer

Target notifications travel over PostgreSQL `LISTEN`/`NOTIFY` on the
`replicant_target` channel, and a session-level `LISTEN` does not survive a
transaction-mode pooler such as PgBouncer. When the service was moved behind
the bouncer (#69, ELE-1600), the issue stated that the replicant had no
`LISTEN` and needed no pool split; that was wrong, `internal/replicant.go`
subscribes. So the service keeps two pools when `BOUNCER_CONN_STRING` is set:
the main pool through the bouncer for queries, job locks and `NOTIFY`, and a
direct pool of two connections for the `LISTEN` alone. Without a bouncer the
two are the same pool.

## Consequences

* The split is `pg.NewPools` with `WithBouncer` and `WithPubSub`; do not
  collapse it back to one pool when reworking the database setup, however
  small the service looks.
* The startup log line `created connection pools` reports `bouncer` so that
  an operator can tell which shape is running (#70).
