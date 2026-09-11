package chaos_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sanchar127/raftiq/internal/kv"
	"github.com/sanchar127/raftiq/internal/raft"
	"github.com/sanchar127/raftiq/internal/server"
)

const (
	linearizabilityWaitTimeout = 5 * time.Second
	linearizabilityKey         = "linearizable-key"
	linearizabilityClients     = 6
	linearizabilityOperations  = 20
)

type linearizabilityOperation struct {
	ID          int
	Kind        string
	Value       string
	Observed    string
	Found       bool
	Err         error
	StartedAt   time.Time
	CompletedAt time.Time
}

type linearizabilityCluster struct {
	transport *raft.LocalTransport
	nodes     []*raft.RaftNode
	servers   []*server.Server
	stores    []*kv.Store
}

func TestLinearizability(t *testing.T) {
	cluster := newLinearizabilityCluster(t)
	cluster.start()

	t.Cleanup(cluster.stop)

	leader := cluster.waitForLeader(linearizabilityWaitTimeout)
	if leader == nil {
		t.Fatal("cluster failed to elect a leader")
	}

	leaderIndex := cluster.indexOf(leader)

	t.Logf(
		"linearizability test leader: node=%s term=%d",
		leader.ID(),
		leader.State().Persistent.CurrentTerm,
	)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		linearizabilityWaitTimeout,
	)
	defer cancel()

	// Establish a known initial state before concurrent operations begin.
	if err := cluster.servers[leaderIndex].Put(
		ctx,
		linearizabilityKey,
		[]byte("initial"),
	); err != nil {
		t.Fatalf("initial Put() failed: %v", err)
	}

	// First prove the strongest and simplest client-visible invariant:
	// once a Put completes, a later Get must observe that value.
	value, found, err := cluster.servers[leaderIndex].Get(
		ctx,
		linearizabilityKey,
	)
	if err != nil {
		t.Fatalf("initial Get() failed: %v", err)
	}

	if !found {
		t.Fatal("initial key was not found")
	}

	if string(value) != "initial" {
		t.Fatalf(
			"initial Get() returned %q, want %q",
			string(value),
			"initial",
		)
	}

	history := runConcurrentHistory(
		t,
		cluster.servers[leaderIndex],
	)

	assertLinearizableHistory(t, history)

	t.Logf(
		"linearizability history validated: operations=%d",
		len(history),
	)
}

func runConcurrentHistory(
	t *testing.T,
	srv *server.Server,
) []linearizabilityOperation {
	t.Helper()

	history := make([]linearizabilityOperation, 0, linearizabilityClients*linearizabilityOperations)
	var historyMu sync.Mutex

	startBarrier := make(chan struct{})
	var wg sync.WaitGroup

	var operationIDMu sync.Mutex
	nextOperationID := 0

	nextID := func() int {
		operationIDMu.Lock()
		defer operationIDMu.Unlock()

		id := nextOperationID
		nextOperationID++

		return id
	}

	for client := 0; client < linearizabilityClients; client++ {
		wg.Add(1)

		go func(clientID int) {
			defer wg.Done()

			<-startBarrier

			for operation := 0; operation < linearizabilityOperations; operation++ {
				id := nextID()

				// Mix reads and writes. Every client uses deterministic values
				// so the resulting history is easy to diagnose on failure.
				if (clientID+operation)%3 == 0 {
					value := fmt.Sprintf(
						"client-%d-operation-%d",
						clientID,
						operation,
					)

					record := linearizabilityOperation{
						ID:        id,
						Kind:      "put",
						Value:     value,
						StartedAt: time.Now(),
					}

					ctx, cancel := context.WithTimeout(
						context.Background(),
						2*time.Second,
					)

					err := srv.Put(
						ctx,
						linearizabilityKey,
						[]byte(value),
					)

					cancel()

					record.CompletedAt = time.Now()
					record.Err = err

					historyMu.Lock()
					history = append(history, record)
					historyMu.Unlock()

					if err != nil {
						t.Errorf(
							"client %d operation %d Put() failed: %v",
							clientID,
							operation,
							err,
						)
					}

					continue
				}

				record := linearizabilityOperation{
					ID:        id,
					Kind:      "get",
					StartedAt: time.Now(),
				}

				ctx, cancel := context.WithTimeout(
					context.Background(),
					2*time.Second,
				)

				value, found, err := srv.Get(
					ctx,
					linearizabilityKey,
				)

				cancel()

				record.CompletedAt = time.Now()
				record.Err = err
				record.Found = found

				if found {
					record.Observed = string(value)
				}

				historyMu.Lock()
				history = append(history, record)
				historyMu.Unlock()

				if err != nil {
					t.Errorf(
						"client %d operation %d Get() failed: %v",
						clientID,
						operation,
						err,
					)
				}
			}
		}(client)
	}

	close(startBarrier)
	wg.Wait()

	historyMu.Lock()
	defer historyMu.Unlock()

	result := make([]linearizabilityOperation, len(history))
	copy(result, history)

	return result
}

func assertLinearizableHistory(
	t *testing.T,
	history []linearizabilityOperation,
) {
	t.Helper()

	if len(history) == 0 {
		t.Fatal("linearizability history is empty")
	}

	// Every successful operation must have a valid interval.
	for _, operation := range history {
		if operation.Err != nil {
			continue
		}

		if operation.CompletedAt.Before(operation.StartedAt) {
			t.Fatalf(
				"operation %d completed before it started: start=%s complete=%s",
				operation.ID,
				operation.StartedAt.Format(time.RFC3339Nano),
				operation.CompletedAt.Format(time.RFC3339Nano),
			)
		}
	}

	// Sort by invocation time for diagnostics and deterministic checking.
	sorted := make([]linearizabilityOperation, len(history))
	copy(sorted, history)

	for i := 1; i < len(sorted); i++ {
		for j := i; j > 0 && sorted[j].StartedAt.Before(sorted[j-1].StartedAt); j-- {
			sorted[j], sorted[j-1] = sorted[j-1], sorted[j]
		}
	}

	// Construct a sequential explanation of the successful operations.
	//
	// For a single register, every Put defines the value of the register.
	// A Get must observe the most recent Put in the chosen linearization.
	//
	// Real-time precedence is mandatory:
	//
	//   A completes before B starts
	//                    ↓
	//              A must be before B
	//
	// Concurrent operations may be ordered either way.
	successful := make([]linearizabilityOperation, 0, len(sorted))

	for _, operation := range sorted {
		if operation.Err == nil {
			successful = append(successful, operation)
		}
	}

	if len(successful) == 0 {
		t.Fatal("all linearizability operations failed")
	}

	if err := checkSingleRegisterLinearizability(successful); err != nil {
		t.Fatal(err)
	}
}

func checkSingleRegisterLinearizability(
	history []linearizabilityOperation,
) error {
	if len(history) == 0 {
		return nil
	}

	// A backtracking search is used rather than simply sorting by start or
	// completion time. This is important because overlapping operations can
	// linearize in either order.
	remaining := make([]linearizabilityOperation, len(history))
	copy(remaining, history)

	return searchLinearization(
		remaining,
		"initial",
		0,
	)
}

func searchLinearization(
	remaining []linearizabilityOperation,
	currentValue string,
	depth int,
) error {
	if len(remaining) == 0 {
		return nil
	}

	// Try every operation that is legally allowed to be the next operation.
	//
	// An operation cannot be placed after another operation if the first
	// operation completed before the second one started.
	for i, candidate := range remaining {
		legal := true

		for j, other := range remaining {
			if i == j {
				continue
			}

			if other.CompletedAt.Before(candidate.StartedAt) ||
				other.CompletedAt.Equal(candidate.StartedAt) {
				// 'other' completed before candidate started, therefore
				// other must already have been linearized.
				//
				// Since it is still in remaining, candidate cannot be chosen
				// yet.
				legal = false
				break
			}
		}

		if !legal {
			continue
		}

		nextValue := currentValue

		switch candidate.Kind {
		case "put":
			nextValue = candidate.Value

		case "get":
			if !candidate.Found {
				return fmt.Errorf(
					"invalid linearizability history: Get operation %d reported key missing",
					candidate.ID,
				)
			}

			if candidate.Observed != currentValue {
				continue
			}

		default:
			return fmt.Errorf(
				"invalid linearizability history: unknown operation kind %q",
				candidate.Kind,
			)
		}

		next := make([]linearizabilityOperation, 0, len(remaining)-1)
		next = append(next, remaining[:i]...)
		next = append(next, remaining[i+1:]...)

		if err := searchLinearization(
			next,
			nextValue,
			depth+1,
		); err == nil {
			return nil
		}
	}

	return fmt.Errorf(
		"no valid linearization found at depth %d with current value %q",
		depth,
		currentValue,
	)
}

func newLinearizabilityCluster(t *testing.T) *linearizabilityCluster {
	t.Helper()

	transport := raft.NewLocalTransport()

	nodes := []*raft.RaftNode{
		raft.NewRaftNode("node-1"),
		raft.NewRaftNode("node-2"),
		raft.NewRaftNode("node-3"),
	}

	stores := []*kv.Store{
		kv.NewStore(),
		kv.NewStore(),
		kv.NewStore(),
	}

	servers := []*server.Server{
		server.NewServer(nodes[0], stores[0]),
		server.NewServer(nodes[1], stores[1]),
		server.NewServer(nodes[2], stores[2]),
	}

	peerIDs := []raft.NodeID{
		nodes[0].ID(),
		nodes[1].ID(),
		nodes[2].ID(),
	}

	electionTimeouts := []int{10, 12, 14}

	for i, node := range nodes {
		node.SetElectionTimeout(electionTimeouts[i])

		if err := transport.AddNode(node); err != nil {
			t.Fatalf(
				"add node %s to transport: %v",
				node.ID(),
				err,
			)
		}

		if err := node.SetTransport(
			transport,
			peerIDsWithoutSelf(peerIDs, node.ID()),
		); err != nil {
			t.Fatalf(
				"set transport for node %s: %v",
				node.ID(),
				err,
			)
		}
	}

	return &linearizabilityCluster{
		transport: transport,
		nodes:     nodes,
		servers:   servers,
		stores:    stores,
	}
}

func (c *linearizabilityCluster) start() {
	for i, node := range c.nodes {
		if err := node.Start(); err != nil {
			panic(fmt.Sprintf(
				"start node %s: %v",
				node.ID(),
				err,
			))
		}

		if err := c.servers[i].Start(); err != nil {
			panic(fmt.Sprintf(
				"start server for node %s: %v",
				node.ID(),
				err,
			))
		}
	}
}

func (c *linearizabilityCluster) stop() {
	for _, srv := range c.servers {
		srv.Stop()
	}

	for _, node := range c.nodes {
		node.Stop()
	}

	c.transport.Close()
}

func (c *linearizabilityCluster) waitForLeader(
	timeout time.Duration,
) *raft.RaftNode {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		var leader *raft.RaftNode

		for _, node := range c.nodes {
			if node.State().Role != raft.Leader {
				continue
			}

			if leader != nil {
				leader = nil
				break
			}

			leader = node
		}

		if leader != nil {
			return leader
		}

		time.Sleep(10 * time.Millisecond)
	}

	return nil
}

func (c *linearizabilityCluster) indexOf(
	target *raft.RaftNode,
) int {
	for i, node := range c.nodes {
		if node == target {
			return i
		}
	}

	panic("raft node not found in cluster")
}
