# Operations

## Starting a cluster

There's no cluster-management tooling (no operator, no Kubernetes
manifests, no Docker Compose — see [`AUDIT.md`](AUDIT.md)). A cluster is a
set of independently started `raftiq` processes that share a `-peers`
list. See the [README](../README.md#multi-node-cluster) for the exact
commands.

Startup order doesn't matter for correctness — a node that starts before
its peers are up will simply fail RPCs to them until they're reachable,
and Raft will proceed once enough voters are online to reach quorum.

## Ports

Each node exposes three listeners:

| Port (flag) | Protocol | Purpose |
|---|---|---|
| `-raft-addr` | gRPC (plaintext) | Peer-to-peer Raft RPCs |
| `-kv-addr` | gRPC (plaintext) | Client KV API |
| `-metrics-addr` | HTTP | Prometheus `/metrics` |

Both gRPC listeners are plaintext by default — see
[`transport/security.md`](transport/security.md) before exposing them
beyond a trusted network.

## Storage paths

Each node's WAL lives at `<data-dir>/node-<id>.wal`. Back this up (or
snapshot the volume) if you need disaster recovery beyond "reconstruct
from the surviving majority" — see [`persistence.md`](persistence.md).

## Graceful shutdown

`main.go` listens for `SIGINT`/`SIGTERM` and calls a `shutdown()` helper
that stops the Raft node, the application server, the Raft transport, and
both gRPC/HTTP listeners in sequence, logging any errors along the way. A
`kill -TERM` or Ctrl-C is the supported way to stop a node.

## Adding/removing a node from a running cluster

There is currently no operational path for this beyond writing Go code
that embeds `raft.RaftNode` and calls `AddMember`/`RemoveMember` directly
— there's no RPC or CLI flag exposing it. See
[`raft/membership.md`](raft/membership.md) for what the underlying
mechanism does, and [`AUDIT.md`](AUDIT.md) for why it isn't reachable from
the shipped binary today. Practically, this means: to resize a cluster
today, you need to either write a small Go program driving
`RaftNode.AddMember`/`RemoveMember` against a leader's in-process
`RaftNode`, or treat `-peers` as fixed at bootstrap time.

## Logs

Structured logs via `log/slog`, level controlled by `-log-level`. Every
RPC handler and major state transition logs with consistent fields
(`node_id`, `error`, `duration`, etc. depending on call site) — grep for
the operation name (e.g. `"kv put failed"`) to find the relevant log call
in code if a message needs more context than what's printed.

## Metrics and dashboards

`/metrics` on `-metrics-addr` exposes Prometheus metrics under the
`raftiq` namespace. `deploy/grafana/dashboards/raftiq-overview.json` is a
ready-made dashboard; `deploy/grafana/provisioning/` has matching
datasource/dashboard provisioning config for a Grafana instance pointed at
a Prometheus that's scraping your nodes. Note: `deploy/prometheus/` itself
is currently an empty directory — you'll need to write your own scrape
config (point it at each node's `-metrics-addr`).

## Recovery after a crash

A node restarted with the same `-id` and `-data-dir` will replay its WAL
(`OpenWAL`) and rejoin the cluster automatically — no manual recovery
steps are required as long as the WAL file itself is intact. See
[`raft/failure-recovery.md`](raft/failure-recovery.md).

If the WAL file is lost or corrupted beyond the automatic tail-truncation
recovery `OpenWAL` performs (see [`storage/wal.md`](storage/wal.md)), the
only supported recovery path today is to remove that node from the
cluster (conceptually — there's no exposed `RemoveMember` path either, see
above) and bring up a fresh node with an empty data directory, which will
need to catch up via `AppendEntries` backfill or `InstallSnapshot` once
reachable.

## Common operational failures

See [`troubleshooting.md`](troubleshooting.md) for symptom-to-cause
mappings.
