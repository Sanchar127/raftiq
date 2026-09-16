# Security Policy

## Reporting a vulnerability

If you believe you've found a security vulnerability in RaftIQ, please
report it privately rather than opening a public issue. Use GitHub's
"Report a vulnerability" flow under the repository's Security tab, or
contact the maintainer directly through the contact information on their
GitHub profile (`sanchar127`).

Please include:
- A description of the vulnerability and its potential impact.
- Steps to reproduce it, including cluster topology/config if relevant.
- Whether it requires network access, disk access, or a malicious peer.

We'll acknowledge reports as promptly as we can and follow up with a fix
timeline once the issue is understood. As a single-maintainer, early-stage
open-source project, there is currently no formal SLA on response time.

## Supported versions

RaftIQ does not yet have tagged releases with a formal support window.
Security fixes are applied to the `main` branch. Once tagged releases
exist, this section will be updated with a supported-version table.

## Transport security — current state

Read this section carefully before running RaftIQ across an untrusted
network.

- **TLS/mTLS is implemented as a library capability** in
  `internal/transport/tls.go` (`LoadTLSConfig`, `LoadTLSServerConfig`,
  `LoadTLSClientConfig`, with SAN verification), and is unit-tested.
- **It is not currently wired into the `raftiq` CLI.** `cmd/raftiq/main.go`
  has no flags for certificates or key material, and starts both the Raft
  and KV gRPC servers over plaintext. If you run the stock `raftiq` binary
  across a network you do not fully trust, **all Raft RPC and KV traffic is
  unencrypted and unauthenticated.**
- To use TLS/mTLS today, you must embed `internal/transport.GRPCTransport`,
  `internal/transport.Server`, and the `tls.go` helpers in your own `main`
  package rather than using `cmd/raftiq` as-is. See `docs/transport/security.md`.
- There is no authentication or authorization layer for the KV client API
  (`client/` package / `KVService` gRPC) beyond whatever TLS client-cert
  verification you configure yourself. Anyone who can reach the KV port can
  read and write any key.

## Known security limitations

- No built-in authentication/authorization for KV reads/writes.
- No rate limiting or request quotas on either gRPC service.
- No encryption at rest for the WAL (`internal/storage/wal.go` writes plain
  binary records; anyone with filesystem access to `-data-dir` can read all
  committed state).
- The distributed-locking and scheduler/worker fencing mechanisms
  (`internal/lock`, `internal/scheduler`, `internal/worker`) protect against
  *stale* actors (partitioned/slow nodes acting on outdated leases), not
  against *malicious* ones — a compromised node with valid credentials can
  still submit arbitrary commands.

If you plan to run RaftIQ in production or across untrusted networks, treat
the items above as required hardening work, not optional extras.