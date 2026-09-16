# Transport: gRPC

The real, network-facing transport, used by `cmd/raftiq`.

## Client side: `GRPCTransport`

`internal/transport/grpc_raft_transport.go`. Implements the `raft.Transport`
interface. `NewGRPCTransport()` creates an empty transport;
`AddPeer(id, address)` dials a peer and stores the connection;
`RemovePeer(id)` tears one down; `Close()` tears down all of them.
`RequestVote`/`PreVote`/`AppendEntries`/`InstallSnapshot` translate the
`raft` package's Go argument/reply types to/from the generated protobuf
types (`api/proto`) and make the actual gRPC call, with `contextError`
handling cancellation the same way `LocalTransport` does.

`SetTLSConfig(*tls.Config)` exists and works, but `cmd/raftiq/main.go`
never calls it — see [`security.md`](security.md).

## Server side: `RaftService` and `KVService`

`internal/transport/raft_service.go` (`RaftService`) and
`internal/transport/kv_service.go` (`KVService`) are the gRPC service
implementations registered against a `*grpc.Server`. Each is a thin
adapter: decode the proto request, call the corresponding method on the
injected dependency (`raftRPC`/`kvRPC` — minimal interfaces satisfied by
`*raft.RaftNode` and `*server.Server` respectively), encode the proto
response. `RaftService.observeRPC` centralizes metrics/logging around each
call via the injected `RPCMetrics` interface (`internal/transport/metrics.go`).

## `transport.Server`

`internal/transport/server.go`. A small wrapper around `*grpc.Server`:
`NewServer(address, opts...)` creates a listener,
`RegisterRaftService`/`RegisterKVService` attach one of the services above,
`Serve()`/`Shutdown(ctx)` run/stop it. `cmd/raftiq/main.go` creates two
independent `transport.Server` instances — one for `-raft-addr`, one for
`-kv-addr` — rather than sharing a single gRPC server for both services.

## Client library

`client/` provides a Go client for `KVService` (`client.Dial`,
`client.Client.Get`/`Put`/`Delete` — see `client/client.go`, `dial.go`,
`grpc_kv.go`). There is currently no equivalent client-side convenience
wrapper for driving membership changes or locking, since those aren't
exposed as RPCs at all (see [`../AUDIT.md`](../AUDIT.md)).

## Integration tests

`internal/transport/grpc_integration_test.go` and
`grpc_raft_cluster_test.go` exercise the real gRPC stack end-to-end on
loopback addresses — these are what actually validate the proto wire
format and gRPC plumbing, complementing `internal/raft`'s
`LocalTransport`-based consensus-logic tests.
