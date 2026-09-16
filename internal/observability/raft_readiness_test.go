package observability

import (
	"strings"
	"testing"

	"github.com/sanchar127/raftiq/internal/raft"
)

func TestRaftReadinessProbeNilNode(t *testing.T) {
	probe := RaftReadinessProbe(nil)

	if err := probe(); err == nil {
		t.Fatal("expected nil Raft node to fail readiness")
	}
}

func TestRaftReadinessProbeStoppedNode(t *testing.T) {
	node := raft.NewRaftNode(raft.NodeID("node1"))

	probe := RaftReadinessProbe(node)

	err := probe()
	if err == nil {
		t.Fatal("expected stopped Raft node to fail readiness")
	}

	if !strings.Contains(err.Error(), "not running") {
		t.Fatalf("expected not-running error, got %v", err)
	}
}

func TestRaftReadinessProbeRunningNode(t *testing.T) {
	node := raft.NewRaftNode(raft.NodeID("node1"))

	if err := node.Start(); err != nil {
		t.Fatalf("start Raft node: %v", err)
	}
	defer node.Stop()

	probe := RaftReadinessProbe(node)

	if err := probe(); err != nil {
		t.Fatalf("expected running Raft node to be ready: %v", err)
	}
}
