package chaos_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sanchar127/raftiq/internal/kv"
	"github.com/sanchar127/raftiq/internal/raft"
	"github.com/sanchar127/raftiq/internal/server"
)

const (
	raceStressClients    = 8
	raceStressOperations = 50
	raceStressKeys       = 8

	raceStressTimeout = 15 * time.Second
)

type raceStressCluster struct {
	transport *raft.LocalTransport
	nodes     []*raft.RaftNode
	servers   []*server.Server
	stores    []*kv.Store
}

func TestRaceStress(t *testing.T) {
	cluster := newRaceStressCluster(t)
	defer cluster.stop()

	cluster.start(t)

	ctx, cancel := context.WithTimeout(context.Background(), raceStressTimeout)
	defer cancel()

	leaderIndex := cluster.waitForLeader(t, ctx)

	var successfulPuts atomic.Int64
	var successfulGets atomic.Int64

	var wg sync.WaitGroup
	start := make(chan struct{})

	for clientID := 0; clientID < raceStressClients; clientID++ {
		wg.Add(1)

		go func(id int) {
			defer wg.Done()

			<-start

			for operation := 0; operation < raceStressOperations; operation++ {
				if ctx.Err() != nil {
					return
				}

				key := fmt.Sprintf(
					"stress-key-%d",
					(operation+id)%raceStressKeys,
				)

				switch operation % 2 {
				case 0:
					value := fmt.Sprintf(
						"client-%d-operation-%d",
						id,
						operation,
					)

					if err := cluster.servers[leaderIndex].Put(
						ctx,
						key,
						[]byte(value),
					); err != nil {
						t.Errorf(
							"client %d Put(%q) failed at operation %d: %v",
							id,
							key,
							operation,
							err,
						)
						return
					}

					successfulPuts.Add(1)

				default:
					if _, _, err := cluster.servers[leaderIndex].Get(
						ctx,
						key,
					); err != nil {
						t.Errorf(
							"client %d Get(%q) failed at operation %d: %v",
							id,
							key,
							operation,
							err,
						)
						return
					}

					successfulGets.Add(1)
				}
			}
		}(clientID)
	}

	close(start)
	wg.Wait()

	if ctx.Err() != nil {
		t.Fatalf("stress test timed out: %v", ctx.Err())
	}

	if successfulPuts.Load() == 0 {
		t.Fatal("stress test completed without successful puts")
	}

	if successfulGets.Load() == 0 {
		t.Fatal("stress test completed without successful gets")
	}

	t.Logf(
		"stress completed: puts=%d gets=%d",
		successfulPuts.Load(),
		successfulGets.Load(),
	)

	cluster.waitForReplication(t, ctx)
	cluster.assertSingleLeader(t)
	cluster.assertLogsConverged(t)
}

func newRaceStressCluster(t *testing.T) *raceStressCluster {
	t.Helper()

	transport := raft.NewLocalTransport()

	cluster := &raceStressCluster{
		transport: transport,
		nodes:     make([]*raft.RaftNode, 3),
		servers:   make([]*server.Server, 3),
		stores:    make([]*kv.Store, 3),
	}

	ids := []raft.NodeID{
		"stress-node-1",
		"stress-node-2",
		"stress-node-3",
	}

	timeouts := []int{10, 12, 14}

	for i, id := range ids {
		node := raft.NewRaftNode(id)
		node.SetElectionTimeout(timeouts[i])

		store := kv.NewStore()
		srv := server.NewServer(node, store)

		if err := transport.AddNode(node); err != nil {
			t.Fatalf("add node %s: %v", id, err)
		}

		cluster.nodes[i] = node
		cluster.servers[i] = srv
		cluster.stores[i] = store
	}

	for i, node := range cluster.nodes {
		peers := make([]raft.NodeID, 0, len(cluster.nodes)-1)

		for j, peer := range cluster.nodes {
			if i == j {
				continue
			}

			peers = append(peers, peer.ID())
		}

		if err := node.SetTransport(transport, peers); err != nil {
			t.Fatalf(
				"set transport for %s: %v",
				node.ID(),
				err,
			)
		}
	}

	return cluster
}

func (c *raceStressCluster) start(t *testing.T) {
	t.Helper()

	for i, node := range c.nodes {
		if err := node.Start(); err != nil {
			t.Fatalf(
				"start raft node %d (%s): %v",
				i,
				node.ID(),
				err,
			)
		}
	}

	for i, srv := range c.servers {
		if err := srv.Start(); err != nil {
			t.Fatalf(
				"start server %d: %v",
				i,
				err,
			)
		}
	}
}

func (c *raceStressCluster) stop() {
	for i := len(c.nodes) - 1; i >= 0; i-- {
		c.nodes[i].Stop()
	}

	c.transport.Close()

	for i := len(c.servers) - 1; i >= 0; i-- {
		c.servers[i].Stop()
	}
}

func (c *raceStressCluster) waitForLeader(
	t *testing.T,
	ctx context.Context,
) int {
	t.Helper()

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		for i, node := range c.nodes {
			if node.State().Role == raft.Leader {
				return i
			}
		}

		select {
		case <-ctx.Done():
			t.Fatalf(
				"timed out waiting for leader: %v",
				ctx.Err(),
			)
		case <-ticker.C:
		}
	}
}

func (c *raceStressCluster) waitForReplication(
	t *testing.T,
	ctx context.Context,
) {
	t.Helper()

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		leaderIndex := -1

		for i, node := range c.nodes {
			if node.State().Role == raft.Leader {
				leaderIndex = i
				break
			}
		}

		if leaderIndex >= 0 {
			leaderLog := c.nodes[leaderIndex].Log()

			replicated := true

			for i, node := range c.nodes {
				if i == leaderIndex {
					continue
				}

				if node.Log().LastIndex() < leaderLog.LastIndex() {
					replicated = false
					break
				}
			}

			if replicated {
				return
			}
		}

		select {
		case <-ctx.Done():
			t.Fatalf(
				"timed out waiting for log replication: %v",
				ctx.Err(),
			)
		case <-ticker.C:
		}
	}
}

func (c *raceStressCluster) assertSingleLeader(t *testing.T) {
	t.Helper()

	leaders := 0

	for _, node := range c.nodes {
		if node.State().Role == raft.Leader {
			leaders++
		}
	}

	if leaders != 1 {
		t.Fatalf(
			"expected exactly one leader, found %d",
			leaders,
		)
	}
}

func (c *raceStressCluster) assertLogsConverged(t *testing.T) {
	t.Helper()

	lastIndex := c.nodes[0].Log().LastIndex()

	for i := 1; i < len(c.nodes); i++ {
		if got := c.nodes[i].Log().LastIndex(); got != lastIndex {
			t.Fatalf(
				"log length mismatch: node %s has index %d, expected %d",
				c.nodes[i].ID(),
				got,
				lastIndex,
			)
		}
	}

	for index := raft.LogIndex(1); index <= lastIndex; index++ {
		expected, ok := c.nodes[0].Log().Get(index)
		if !ok {
			t.Fatalf(
				"node %s missing log entry %d",
				c.nodes[0].ID(),
				index,
			)
		}

		for i := 1; i < len(c.nodes); i++ {
			got, ok := c.nodes[i].Log().Get(index)
			if !ok {
				t.Fatalf(
					"node %s missing log entry %d",
					c.nodes[i].ID(),
					index,
				)
			}

			if got.Term != expected.Term {
				t.Fatalf(
					"log term mismatch at index %d: node %s has term %d, expected %d",
					index,
					c.nodes[i].ID(),
					got.Term,
					expected.Term,
				)
			}

			if string(got.Data) != string(expected.Data) {
				t.Fatalf(
					"log data mismatch at index %d on node %s",
					index,
					c.nodes[i].ID(),
				)
			}
		}
	}
}
