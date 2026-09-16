# Transport: LocalTransport

`internal/raft/local_transport.go`. An in-process implementation of the
`Transport` interface used almost exclusively by tests
(`internal/raft/node_test.go`, `internal/raft/local_transport_test.go`) —
it's what lets ~150 Raft tests exercise multi-node behavior without a real
network.

## How it works

`NewLocalTransport()` creates a router that holds direct references to
in-process `*RaftNode` instances (`AddNode`/`RemoveNode`) and a peer
address table (`AddPeer`). RPC methods (`RequestVote`, `PreVote`,
`AppendEntries`, `InstallSnapshot`) look up the target node
(`peer(target)`) and call its handler method directly — no serialization,
no sockets.

## Partition simulation

```go
t.Block(nodeA, nodeB)              // drop nodeA -> nodeB messages
t.Unblock(nodeA, nodeB)
t.BlockBidirectional(nodeA, nodeB) // drop both directions
t.UnblockBidirectional(nodeA, nodeB)
t.IsBlocked(nodeA, nodeB)
```

Every RPC method calls `checkBlocked(from, to)` before delivering,
returning an error (as if the RPC had failed/timed out) if the link is
currently blocked. This is how `tests/chaos/network_partition_test.go`
simulates both symmetric and asymmetric partitions — asymmetric meaning
`Block(A, B)` without also blocking `B, A`, so messages flow one way but
not the other.

## Context handling

RPC calls respect the caller's `context.Context` — `contextError(ctx)`
checks for cancellation/deadline before/during delivery, so tests can
exercise RPC-timeout behavior (e.g. a peer that would technically be
reachable but the caller gave up) without needing real latency.

## Relationship to `GRPCTransport`

`LocalTransport` and `GRPCTransport` implement the exact same `Transport`
interface, so `RaftNode` code paths (replication, elections, snapshots)
are identical whether driven by tests or by real gRPC — the only thing
that differs is how bytes get from one node to another. This is the
central reason `internal/raft`'s test suite can be this extensive without
being slow or flaky: no real sockets, no real timing dependent on OS
scheduling beyond RaftIQ's own configured timeouts.
