# Changelog

All notable changes to this project after v1.0.0 are documented here. The
entries below are derived from release tags; see the linked PRs for full
detail.

## [v1.4.0] - Unreleased

**New API surface (Connect):** every method of `elephant.replicant.Replication`
is now served a second time, on `POST /elephant.replicant.Replication/<Method>`
in addition to `POST /twirp/elephant.replicant.Replication/<Method>`. The new
paths speak the [Connect](https://connectrpc.com/) protocol in protobuf or
JSON, and, to callers inside the cluster, gRPC and gRPC-Web. Both mounts wrap
the same handlers behind the same authentication middleware and the same scope
checks, so nothing about a call but its encoding depends on which family it
arrived on. **The Twirp paths are unchanged and stay**: they are removed in a
future major release, not on a traffic timer. For a caller that moves, the
error body is `{"code","message","details"}` with the metadata in an
`elephantine.rpc.ErrorMeta` detail rather than `{"code","msg","meta"}`;
`failed_precondition` is answered 400 rather than 412, `canceled` 499 rather
than 408 and `deadline_exceeded` 504 rather than 408; and a JSON response
spells its fields in lowerCamelCase (`repositoryUrl`) where Twirp spells them
as the `.proto` declares them (`repository_url`). Requests accept either
spelling on both mounts. An ingress that routes on `/twirp/` needs a sibling
rule for the new paths before a client can use them.

**Behaviour change (authentication):** a call with a missing or invalid token
is answered `unauthenticated` (401) by elephantine's shared authentication
middleware before it reaches a handler, rendered in the protocol the caller is
speaking. An invalid token was answered `permission_denied` (403); anything
keyed on 403 for a bad token reads 401 after the upgrade.

**Behaviour change (request bodies):** request bodies are capped at 8 MiB on
both listeners, which comes in with the elephantine upgrade. A request that
declares a larger `Content-Length` is refused with `413` before it reaches a
handler. Bodies were unbounded before; no Replication request comes near the
limit.

**Behaviour change (database pools):** the query pool is sized explicitly by
the new `DB_MAX_CONNS` (`--db-max-conns`), default 8, where pgx used to size
it at `max(4, NumCPU())` from the node's cpuset. Each enabled target needs two
connections, so raise it when running more than three targets. The new
`BOUNCER_CONN_STRING` (`--db-bouncer`) routes every query through a
transaction pooler such as PgBouncer; the LISTEN session that cannot survive
transaction pooling then gets a direct pool of its own, pinned at two
connections. Unset, or equal to `CONN_STRING`, the service runs one direct
pool as before. (#69)

**Behaviour change (worker supervision):** a target's worker that keeps
failing is restarted with the library's backoff and given up on after an hour
of continuous failure, where it used to be restarted immediately and forever.
A given-up target stays stopped until the process restarts or the target is
reconfigured or started again over the API; `pg_job_lock_restarts_total` and
`pg_job_lock_held` are the signals. (#69)

**Behaviour change (target notifications):** the `LISTEN` that carries
target changes between replicas is now health-checked: the subscriber sends
itself a ping every five minutes and reconnects when seven minutes pass
without one, where before a connection that died silently was never noticed
and every `ConfigureTarget`, `ChangeTargetState` and `RemoveTarget` after it
was lost on that replica until a restart. On every connect and reconnect the
service reconciles its workers with the `replication_target` table, starting
the enabled targets that have no worker and stopping the ones that are
disabled or gone, so a start, stop or remove issued while the connection was
dead takes effect within about twelve minutes. A `ConfigureTarget` whose row
changed while the worker kept running is still not detected across that gap;
issue it again. `GetTargetState` is the check.

**Build:** the module's `go` directive is `1.27.2` and the release image
builds on `golang:1.27.2-alpine3.24` on top of `alpine:3.24`, up from Go
1.26.4 and Alpine 3.23. The plaintext listener serves HTTP/2 alongside
HTTP/1.1, which is what makes gRPC reachable in-cluster; HTTP/1.1 callers
are unaffected.

Changes:

- Connect is served alongside Twirp on `/elephant.replicant.Replication/`.
  The Twirp errors the handlers return are translated for the Connect mount
  until the handlers move to the `elephantine/rpc` vocabulary, and
  `TestErrorParity` holds the two mounts to the same code, message and
  metadata for the error paths answered before storage is touched.
- The default allowed CORS request headers gain `Connect-Protocol-Version`
  and `Connect-Timeout-Ms`.
- `rpc_protocol_responses_total{service,method,protocol,code,client_id}` is
  reported by both mounts: `protocol="twirp"` going to zero for a method is
  what says its Twirp callers have moved.
- The startup log line `created connection pools` reports `bouncer` from the
  pool setup instead of `separate_pubsub_pool`. (#70)
- The subscriber is `pg.NewSubscriber`, with the pings sent through the query
  pool, and is restarted every five seconds for as long as it fails rather
  than taking the process down; the deprecated `pg.Subscribe` is gone.
- Pool metrics are exported as `pgxpool_*{pool="main"}`, plus `pool="pubsub"`
  when the LISTEN pool is separate, and the job lock metrics `pg_job_lock_held`
  and `pg_job_lock_transitions_total` carry one series per target. (#69)
- Dependency upgrades: Go to 1.27.2, elephantine to v0.30.3, elephant-api to
  v0.28.0, ttab/mage to v0.15.0, koonkie to v0.2.0, pgx to v5.11.0, urfave/cli
  to v3.14.0, client_golang to v1.25.0 and the x/ modules. The unused `twirp`
  mage namespace import is dropped. CI moves to actions/checkout@v7,
  actions/setup-go@v7, golangci-lint v2.14 and `tonistiigi/binfmt:qemu-v10.2.3`.
- The golangci-lint configuration is aligned with the other elephant
  services: `gomodguard_v2` and `modernize` are enabled, test files are
  exempt from `goconst`, `gosec` and `wrapcheck`.
