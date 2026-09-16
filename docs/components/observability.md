# Component: Observability

`internal/observability` provides Prometheus metrics, structured logging,
and HTTP health/readiness endpoints. Metrics and logging are wired into
`cmd/raftiq`; health/readiness are implemented but **not** registered by
`main.go` — see [`../AUDIT.md`](../AUDIT.md).

## Metrics (`metrics.go`)

`NewMetrics(...)` registers a namespaced (`raftiq`) set of Prometheus
collectors across several subsystems:

| Subsystem | Example metric names | Covers |
|---|---|---|
| `raft` | `raftiq_raft_current_term`, `raftiq_raft_role`, `raftiq_raft_commit_index`, `raftiq_raft_last_applied`, `raftiq_raft_last_log_index`, `raftiq_raft_log_size`, `raftiq_raft_elections_total`, `raftiq_raft_election_duration_seconds`, `raftiq_raft_leader_changes_total`, `raftiq_raft_vote_requests_total`, `raftiq_raft_votes_granted_total`, `raftiq_raft_append_entries_total`, `raftiq_raft_append_entries_failures_total`, `raftiq_raft_append_entries_duration_seconds`, `raftiq_raft_snapshots_created_total`, `raftiq_raft_snapshots_installed_total` | Consensus state and activity |
| `rpc` | `raftiq_rpc_requests_total`, `raftiq_rpc_errors_total`, `raftiq_rpc_duration_seconds` | Transport-layer RPC activity |
| `storage` | `raftiq_storage_operations_total`, `raftiq_storage_operation_errors_total`, `raftiq_storage_operation_duration_seconds`, `raftiq_storage_sync_total`, `raftiq_storage_sync_errors_total`, `raftiq_storage_sync_duration_seconds` | WAL read/write/sync activity |
| `kv` | `raftiq_kv_operations_total`, `raftiq_kv_operation_errors_total` | KV command application |
| `scheduler` | `raftiq_scheduler_scheduled_jobs_total`, `raftiq_scheduler_executed_jobs_total`, `raftiq_scheduler_job_execution_failures_total`, `raftiq_scheduler_lease_acquisitions_total`, `raftiq_scheduler_lease_losses_total` | Scheduler/worker job lifecycle (library-only — see [`scheduler.md`](scheduler.md)/[`worker.md`](worker.md)) |

Each package that reports metrics defines its own small interface
(`raft.Metrics`, `storage.StorageMetrics`, `transport.RPCMetrics`,
`kv.KVMetrics`, `scheduler.SchedulerMetrics`, `worker.WorkerMetrics`) with
a `Noop*` default, and `internal/observability.Metrics` implements all of
them — this is what lets every package be unit-tested without a real
Prometheus registry (tests just use the `Noop*` implementations or a fake)
while still getting real metrics wired in by `cmd/raftiq/main.go`.

`cmd/raftiq/main.go` exposes these at `-metrics-addr` via
`promhttp.Handler()` on `/metrics`. `deploy/grafana/dashboards/raftiq-overview.json`
is a pre-built dashboard matching this metric set; `deploy/prometheus/` is
currently an empty directory, so you'll need to write your own scrape
config pointed at each node's `-metrics-addr`.

## Logging (`logging.go`)

```go
type LogFormat string
type LoggingConfig struct { /* ... */ }

func NewLogger(config LoggingConfig) *slog.Logger
func NewProductionLogger(output io.Writer) *slog.Logger  // JSON
func NewDevelopmentLogger(output io.Writer) *slog.Logger // human-readable
```

Built on `log/slog`. `cmd/raftiq/main.go` builds a logger from
`-log-level` and attaches it across the node, server, and transport layers
via each package's `SetLogger` method.

## Health and readiness (`health.go`, `raft_readiness.go`) — implemented, not wired in

```go
type HealthProbe func() error
type HealthServer struct { /* ... */ }

func NewHealthServer(address string) *HealthServer
func (s *HealthServer) RegisterReadinessProbe(name string, probe HealthProbe)
func (s *HealthServer) Serve() error

func RaftReadinessProbe(node *raft.RaftNode) HealthProbe
```

`HealthServer` serves a liveness endpoint (always healthy if the process
is up) and a readiness endpoint that runs all registered probes.
`RaftReadinessProbe(node)` is a ready-made probe — currently it only
checks that `node` is non-nil, not that the node is an active
leader/follower with a healthy commit pipeline, so treat it as a basic
liveness-style check rather than a deep health signal today.

**`cmd/raftiq/main.go` never constructs a `HealthServer` or registers
`RaftReadinessProbe`.** There is no `/healthz` or `/readyz` endpoint on
the running binary — only `/metrics` is actually served. If you need
health/readiness endpoints (e.g. for a Kubernetes liveness/readiness
probe), you currently need to wire `HealthServer` in yourself, following
the pattern `NewMetricsServer`/`main.go`'s metrics wiring already
demonstrates for the equivalent HTTP-server-lifecycle code.

## `MetricsServer` (`http.go`)

A small `net/http` server wrapper (`NewMetricsServer`, `Serve`,
`Shutdown`) — this is what `main.go` actually uses for `/metrics`.
`HealthServer` is a parallel, independent type with the same shape but
serves health/readiness instead; the two are not the same server instance
and would need to be started separately if you add health checks.
