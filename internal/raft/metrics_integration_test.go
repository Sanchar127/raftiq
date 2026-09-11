package raft_test

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sanchar127/raftiq/internal/observability"
	"github.com/sanchar127/raftiq/internal/raft"
)

func TestRequestVoteMetrics(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := observability.NewMetrics(registry)

	node := raft.NewRaftNode("node-1")
	node.SetMetrics(observability.NewRaftMetrics("node-1", metrics))

	reply := node.RequestVote(raft.RequestVoteArgs{
		Term:         1,
		CandidateID:  "node-2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if !reply.VoteGranted {
		t.Fatal("expected vote to be granted")
	}

	expectedRequests := `
# HELP raftiq_raft_vote_requests_total Total number of RequestVote RPCs processed.
# TYPE raftiq_raft_vote_requests_total counter
raftiq_raft_vote_requests_total{node_id="node-1"} 1
`

	if err := testutil.GatherAndCompare(
		registry,
		strings.NewReader(expectedRequests),
		"raftiq_raft_vote_requests_total",
	); err != nil {
		t.Fatalf("unexpected vote request metrics: %v", err)
	}

	expectedGranted := `
# HELP raftiq_raft_votes_granted_total Total number of votes granted.
# TYPE raftiq_raft_votes_granted_total counter
raftiq_raft_votes_granted_total{node_id="node-1"} 1
`

	if err := testutil.GatherAndCompare(
		registry,
		strings.NewReader(expectedGranted),
		"raftiq_raft_votes_granted_total",
	); err != nil {
		t.Fatalf("unexpected granted vote metrics: %v", err)
	}
}

func TestRequestVoteMetricsRejectedVote(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := observability.NewMetrics(registry)

	node := raft.NewRaftNode("node-1")
	node.SetMetrics(observability.NewRaftMetrics("node-1", metrics))

	first := node.RequestVote(raft.RequestVoteArgs{
		Term:        1,
		CandidateID: "node-2",
	})

	if !first.VoteGranted {
		t.Fatal("expected first vote to be granted")
	}

	second := node.RequestVote(raft.RequestVoteArgs{
		Term:        1,
		CandidateID: "node-3",
	})

	if second.VoteGranted {
		t.Fatal("expected second vote to be rejected")
	}

	expectedRequests := `
# HELP raftiq_raft_vote_requests_total Total number of RequestVote RPCs processed.
# TYPE raftiq_raft_vote_requests_total counter
raftiq_raft_vote_requests_total{node_id="node-1"} 2
`

	if err := testutil.GatherAndCompare(
		registry,
		strings.NewReader(expectedRequests),
		"raftiq_raft_vote_requests_total",
	); err != nil {
		t.Fatalf("unexpected vote request metrics: %v", err)
	}

	expectedGranted := `
# HELP raftiq_raft_votes_granted_total Total number of votes granted.
# TYPE raftiq_raft_votes_granted_total counter
raftiq_raft_votes_granted_total{node_id="node-1"} 1
`

	if err := testutil.GatherAndCompare(
		registry,
		strings.NewReader(expectedGranted),
		"raftiq_raft_votes_granted_total",
	); err != nil {
		t.Fatalf("unexpected granted vote metrics: %v", err)
	}
}

func TestAppendEntriesMetricsSuccess(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := observability.NewMetrics(registry)

	node := raft.NewRaftNode("node-1")
	node.SetMetrics(observability.NewRaftMetrics("node-1", metrics))

	reply := node.AppendEntries(raft.AppendEntriesArgs{
		Term:     1,
		LeaderID: "leader-1",
	})

	if !reply.Success {
		t.Fatal("expected AppendEntries to succeed")
	}

	expected := `
# HELP raftiq_raft_append_entries_total Total number of AppendEntries RPCs processed.
# TYPE raftiq_raft_append_entries_total counter
raftiq_raft_append_entries_total{node_id="node-1",peer_id="leader-1",result="success"} 1
`

	if err := testutil.GatherAndCompare(
		registry,
		strings.NewReader(expected),
		"raftiq_raft_append_entries_total",
	); err != nil {
		t.Fatalf("unexpected AppendEntries metrics: %v", err)
	}

	gathered, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}

	foundDuration := false

	for _, family := range gathered {
		if family.GetName() != "raftiq_raft_append_entries_duration_seconds" {
			continue
		}

		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "peer_id" &&
					label.GetValue() == "leader-1" {
					foundDuration = true
				}
			}
		}
	}

	if !foundDuration {
		t.Fatal("expected AppendEntries duration metric")
	}
}

func TestAppendEntriesMetricsFailure(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := observability.NewMetrics(registry)

	node := raft.NewRaftNode("node-1")
	node.SetMetrics(observability.NewRaftMetrics("node-1", metrics))

	reply := node.AppendEntries(raft.AppendEntriesArgs{
		Term:         1,
		LeaderID:     "leader-1",
		PrevLogIndex: 1,
		PrevLogTerm:  1,
	})

	if reply.Success {
		t.Fatal("expected AppendEntries to fail")
	}

	expected := `
# HELP raftiq_raft_append_entries_total Total number of AppendEntries RPCs processed.
# TYPE raftiq_raft_append_entries_total counter
raftiq_raft_append_entries_total{node_id="node-1",peer_id="leader-1",result="failure"} 1
`

	if err := testutil.GatherAndCompare(
		registry,
		strings.NewReader(expected),
		"raftiq_raft_append_entries_total",
	); err != nil {
		t.Fatalf("unexpected AppendEntries metrics: %v", err)
	}

	expectedFailures := `
# HELP raftiq_raft_append_entries_failures_total Total number of failed AppendEntries operations.
# TYPE raftiq_raft_append_entries_failures_total counter
raftiq_raft_append_entries_failures_total{node_id="node-1",peer_id="leader-1"} 1
`

	if err := testutil.GatherAndCompare(
		registry,
		strings.NewReader(expectedFailures),
		"raftiq_raft_append_entries_failures_total",
	); err != nil {
		t.Fatalf("unexpected AppendEntries failure metrics: %v", err)
	}

	gathered, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}

	foundDuration := false

	for _, family := range gathered {
		if family.GetName() != "raftiq_raft_append_entries_duration_seconds" {
			continue
		}

		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "peer_id" &&
					label.GetValue() == "leader-1" {
					foundDuration = true
				}
			}
		}
	}

	if !foundDuration {
		t.Fatal("expected AppendEntries duration metric")
	}
}