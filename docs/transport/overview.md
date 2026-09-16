# Transport: Overview

`internal/transport` implements the network boundary: gRPC clients/servers
for Raft peer RPCs and the client-facing KV API, plus TLS/mTLS support.
`internal/raft/transport.go` defines the `Transport` interface that
`RaftNode` depends on; everything here either implements that interface
(`GRPCTransport`, `LocalTransport`) or sits on the server side receiving
calls and forwarding them into a local `RaftNode`/`Server`.

## Pieces

| Type | File | Role |
|---|---|---|
| `GRPCTransport` | `grpc_raft_transport.go` | Client side: calls peers' `RequestVote`/`PreVote`/`AppendEntries`/`InstallSnapshot` over gRPC |
| `RaftService` | `raft_service.go` | Server side: gRPC service implementing `RaftService`, forwards to a local `RaftNode` |
| `KVService` | `kv_service.go` | Server side: gRPC service implementing `KVService`, forwards to `server.Server` |
| `Server` | `server.go` | Thin wrapper around `*grpc.Server` used for both the Raft and KV listeners |
| TLS helpers | `tls.go` | `LoadTLSConfig`/`LoadTLSServerConfig`/`LoadTLSClientConfig`, mTLS with SAN verification |
| `LocalTransport` | `internal/raft/local_transport.go` (not `internal/transport`) | In-process `Transport` implementation for tests, supports partition simulation |

## Wire protocol

`api/proto/raftiq.proto` defines two gRPC services:

```protobuf
service RaftService {
  rpc RequestVote(RequestVoteRequest) returns (RequestVoteResponse);
  rpc PreVote(PreVoteRequest) returns (PreVoteResponse);
  rpc AppendEntries(AppendEntriesRequest) returns (AppendEntriesResponse);
  rpc InstallSnapshot(InstallSnapshotRequest) returns (InstallSnapshotResponse);
}
service KVService {
  rpc Get(GetRequest) returns (GetResponse);
  rpc Put(PutRequest) returns (PutResponse);
  rpc Delete(DeleteRequest) returns (DeleteResponse);
}
```

That's the entire network surface. There is no RPC for membership changes
(`AddMember`/`RemoveMember`) or for locking (`AcquireLock`/`FencedPut`) —
see [`../AUDIT.md`](../AUDIT.md).

## Sub-topics

- [`local.md`](local.md) — the in-process test transport
- [`grpc.md`](grpc.md) — the real network transport
- [`security.md`](security.md) — TLS/mTLS, and its current wiring gap
