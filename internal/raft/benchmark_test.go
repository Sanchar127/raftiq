package raft

import "testing"

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
