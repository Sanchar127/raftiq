package raft

import (
	"path/filepath"
	"testing"

	"github.com/sanchar127/raftiq/internal/storage"
)

const raftBenchmarkBatchSize = 1000

func newBenchmarkLeader(b *testing.B) *RaftNode {
	b.Helper()

	node := NewRaftNode("benchmark-node")

	if err := node.BootstrapMembership(); err != nil {
		b.Fatal(err)
	}

	if _, err := node.startElection(); err != nil {
		b.Fatal(err)
	}

	node.becomeLeader()

	return node
}

func newBenchmarkWALLeader(b *testing.B) (*RaftNode, func()) {
	b.Helper()

	path := filepath.Join(b.TempDir(), "raftiq-benchmark.wal")

	store, err := storage.OpenWAL(path)
	if err != nil {
		b.Fatal(err)
	}

	node, err := NewRaftNodeWithStorage("benchmark-node", store)
	if err != nil {
		store.Close()
		b.Fatal(err)
	}

	if err := node.BootstrapMembership(); err != nil {
		store.Close()
		b.Fatal(err)
	}

	if _, err := node.startElection(); err != nil {
		store.Close()
		b.Fatal(err)
	}

	node.becomeLeader()

	return node, func() {
		if err := store.Close(); err != nil {
			b.Errorf("close benchmark WAL: %v", err)
		}
	}
}

func drainBenchmarkApplyCh(node *RaftNode) chan struct{} {
	done := make(chan struct{})

	go func() {
		for {
			select {
			case <-node.ApplyCh():
			case <-done:
				return
			}
		}
	}()

	return done
}

func BenchmarkRaftPropose(b *testing.B) {
	data := []byte("raftiq-benchmark-value")

	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		node := newBenchmarkLeader(b)
		done := drainBenchmarkApplyCh(node)

		b.StartTimer()

		for j := 0; j < raftBenchmarkBatchSize; j++ {
			if _, err := node.Propose(data); err != nil {
				b.Fatal(err)
			}
		}

		b.StopTimer()
		close(done)
	}

	b.ReportMetric(
		float64(raftBenchmarkBatchSize),
		"proposals/batch",
	)
}

func BenchmarkRaftProposeWAL(b *testing.B) {
	data := []byte("raftiq-benchmark-value")

	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		node, cleanup := newBenchmarkWALLeader(b)
		done := drainBenchmarkApplyCh(node)

		b.StartTimer()

		for j := 0; j < raftBenchmarkBatchSize; j++ {
			if _, err := node.Propose(data); err != nil {
				b.StopTimer()
				close(done)
				cleanup()
				b.Fatal(err)
			}
		}

		b.StopTimer()
		close(done)
		cleanup()
	}

	b.ReportMetric(
		float64(raftBenchmarkBatchSize),
		"proposals/batch",
	)
}
