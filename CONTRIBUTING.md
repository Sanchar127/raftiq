# Contributing to raftkv

Thanks for your interest in contributing. This project is being built incrementally in the open — see the roadmap in [`README.md`](README.md) for what's still planned.

## Getting started

```bash
git clone https://github.com/sanchar127/raftkv.git
cd raftiq
go build ./...
go test ./...
```

The core packages (`internal/raft`, `internal/storage`, `internal/kv`, `internal/lock`) depend only on the Go standard library and build fully offline. The `internal/transport` (gRPC) package requires network access to fetch dependencies.

## Development guidelines

- **Keep `internal/raft` pure.** The consensus core must not import `net`, `net/rpc`, gRPC, or any disk I/O package directly — it only talks to the outside world through the `Transport` and `Persister` interfaces. This is what keeps consensus logic unit-testable without a real cluster.
- **Write unit tests for new logic**, especially anything touching election, replication, or the log-matching property. `internal/raft/raft_test.go` uses an in-memory fake `Transport` — extend it rather than spinning up real network tests for core logic changes.
- **Run `go vet ./...` and `gofmt -l .`** before opening a PR; CI will reject unformatted code.
- **Explain correctness reasoning in comments** for anything touching consensus safety (log matching, commit-index advancement, fencing tokens) — a one-line "why," not just "what."

## Pull requests

1. Fork the repo and create a feature branch.
2. Make your change with tests.
3. Ensure `go build ./...`, `go vet ./...`, and `go test ./...` all pass.
4. Open a PR describing the change and, if it touches consensus behavior, the correctness reasoning behind it.

## Reporting issues

Please include:
- Go version (`go version`)
- Steps to reproduce
- Expected vs. actual behavior
- For consensus bugs: cluster size, timing config, and whether it reproduces under `internal/raft`'s fake-transport tests or only against real gRPC/network

## Code of conduct

Be respectful and constructive. Disagreements about design are welcome and expected — personal attacks are not.
