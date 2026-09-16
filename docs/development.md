# Development

## Setup

```bash
git clone https://github.com/sanchar127/raftiq.git
cd raftiq
go mod download
go build ./...
```

Go 1.26+ is required (see `go.mod`). No external services are needed to
run the test suite — `internal/raft` tests use `LocalTransport` and
`MemoryStorage`, not real network/disk.

## Day-to-day loop

```bash
gofmt -w .            # or: make fmt
go vet ./...          # or: make vet
go test ./...         # or: make test
go test -race ./...   # or: make test-race
```

`make check` runs the full CI-equivalent set locally: `fmt-check`, `vet`,
`lint` (`golangci-lint`, config in `.golangci.yml`), `test`, `test-race`,
`verify` (`go mod verify`), and `vuln` (`govulncheck`). Install the tools
it needs with `make install-tools`.

## Regenerating protobuf code

If you edit `api/proto/raftiq.proto`, regenerate `raftiq.pb.go` and
`raftiq_grpc.pb.go` with `protoc` and the Go/gRPC plugins matching the
versions pinned in `go.mod` (`google.golang.org/protobuf`,
`google.golang.org/grpc`). There's no `make proto` target in the current
`Makefile` — regenerate manually and diff the output before committing.

## Working on consensus code (`internal/raft`)

- Keep it free of `net`, gRPC, and disk-I/O imports. It should only ever
  talk to the outside world through `Transport` and `Storage`.
- Write a `LocalTransport`-based test in `node_test.go` alongside any
  behavioral change — that's how ~150 existing tests exercise multi-node
  scenarios (elections, partitions, joint consensus, restarts) without a
  real network. `LocalTransport.Block`/`Unblock`/`BlockBidirectional` let
  you simulate partitions directly.
- Re-read the safety rules in [`CONTRIBUTING.md`](../CONTRIBUTING.md)
  before changing anything in the commit/replication/membership paths.

## Working on storage (`internal/storage`)

- `MemoryStorage` and `WALStorage` both implement `Storage` — new tests
  that don't specifically need WAL/disk behavior should generally use
  `MemoryStorage` for speed, and WAL-specific tests (recovery, corruption,
  truncation) belong in `wal_test.go`.
- Any change to the record format in `wal.go` needs a recovery test
  proving old-format files (or partially-written new-format files) don't
  silently corrupt state on load.

## Running a local multi-node cluster manually

See [`operations.md`](operations.md) for the full command sequence. For
fast iteration during development, `tests/chaos/` sets up multi-node
clusters programmatically in-process — read `tests/chaos/leader_kill_test.go`
for the pattern if you want a scripted local cluster instead of three
separate terminals.

## Debugging

- Set `-log-level debug` when running `cmd/raftiq` for verbose structured
  logs (`log/slog`-based, one JSON/text line per RPC and state transition).
- `RaftNode.SetLogger`/`SetMetrics` let you attach logging/metrics in tests
  too — several tests assert on emitted metrics via the `NoopMetrics`/
  fake-metrics pattern in `internal/raft/metrics.go`.
- For a stuck election or split-brain-looking symptom, check
  `raftiq_raft_current_term` and `raftiq_raft_role` per node on `/metrics`
  first — see [`troubleshooting.md`](troubleshooting.md).

## Before opening a PR

Read [`CONTRIBUTING.md`](../CONTRIBUTING.md) in full — it covers coding
standards, the Raft safety invariants, and PR expectations in detail.
