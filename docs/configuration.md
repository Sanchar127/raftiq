# Configuration

RaftIQ is configured entirely through CLI flags on `cmd/raftiq`. There is
no config file format (YAML/JSON/TOML) and no `internal/config` package in
the repository — if you were expecting one, it doesn't exist yet.

## Flags (`cmd/raftiq/main.go`)

| Flag | Default | Meaning |
|---|---|---|
| `-id` | *(required)* | Unique Raft node ID. Startup fails with an error if empty. |
| `-raft-addr` | `:7000` | Listen address for the peer-to-peer Raft gRPC server. |
| `-kv-addr` | `:8000` | Listen address for the client-facing KV gRPC server. |
| `-metrics-addr` | `:9090` | Listen address for the Prometheus `/metrics` HTTP endpoint. |
| `-data-dir` | `./data` | Directory for the node's WAL file (`node-<id>.wal`). Created if missing. |
| `-peers` | *(empty)* | Comma-separated `id=host:port` list. Include every voter, including this node's own ID/address. |
| `-heartbeat` | `100ms` | Raft heartbeat interval (leader → followers). |
| `-election` | `1s` | Raft election timeout base (randomized per node internally). |
| `-log-level` | `info` | One of `debug`, `info`, `warn`, `error`. |

`-peers` format example:

```text
-peers node1=localhost:7001,node2=localhost:7002,node3=localhost:7003
```

`main.go` skips adding a peer entry whose ID equals `-id` when building the
transport's peer list (it doesn't dial itself), but the entry should still
be present in `-peers` on every node so all nodes agree on the full voter
set at bootstrap.

## Validation

`validateConfig` (in `main.go`) checks:
- `-id` is non-empty.
- `-heartbeat` and `-election` are each greater than zero.
- `-election` is strictly greater than `-heartbeat` (otherwise a node could
  time out an election before ever seeing a heartbeat).
- `-raft-addr`, `-kv-addr`, `-metrics-addr`, `-data-dir` are all non-empty.
- `-log-level` is one of `debug`, `info`, `warn`, `error`.

A validation failure prints to stderr and exits with status `2` before any
storage or network resources are touched.

## What is *not* configurable via flags today

- **TLS/mTLS**: no cert/key flags exist. See
  [`transport/security.md`](transport/security.md).
- **Config file / env vars**: none.
- **Scheduler/worker startup**: no flag starts these; they're not
  instantiated by `main.go` at all. See
  [`components/scheduler.md`](components/scheduler.md).
- **Dynamic cluster join**: `-peers` is read once at startup via
  `BootstrapMembership()`. Adding a node to a running cluster requires
  driving `RaftNode.AddMember` from Go code — see
  [`raft/membership.md`](raft/membership.md).
- **WAL fsync policy / batching**: `WALStorage.Sync()` behavior is fixed in
  code, not flag-tunable.

## Storage layout

Each node's data lives under `-data-dir` as a single WAL file:

```text
<data-dir>/node-<id>.wal
```

There's no separate snapshot file — snapshots are themselves framed
records inside the same WAL (`recordSnapshot`); see
[`storage/wal.md`](storage/wal.md).
