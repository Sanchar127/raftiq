package observability

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/sanchar127/raftiq/internal/raft"
)

func newTestRaftMetrics(t *testing.T) (*RaftMetrics, *prometheus.Registry) {
	t.Helper()

	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry)
	raftMetrics := NewRaftMetrics("node-1", metrics)

	return raftMetrics, registry
}

func TestRaftMetricsSetCurrentTerm(t *testing.T) {
	metrics, registry := newTestRaftMetrics(t)

	metrics.SetCurrentTerm(raft.Term(7))

	expected := `
# HELP raftiq_raft_current_term Current Raft term.
# TYPE raftiq_raft_current_term gauge
raftiq_raft_current_term{node_id="node-1"} 7
`

	if err := testutil.GatherAndCompare(
		registry,
		strings.NewReader(expected),
		"raftiq_raft_current_term",
	); err != nil {
		t.Fatalf("unexpected current term metric: %v", err)
	}
}

func TestRaftMetricsSetRole(t *testing.T) {
	metrics, registry := newTestRaftMetrics(t)

	metrics.SetRole(raft.Leader)

	expected := `
# HELP raftiq_raft_role Current Raft role. Exactly one role is set to 1.
# TYPE raftiq_raft_role gauge
raftiq_raft_role{node_id="node-1",role="candidate"} 0
raftiq_raft_role{node_id="node-1",role="follower"} 0
raftiq_raft_role{node_id="node-1",role="leader"} 1
`

	if err := testutil.GatherAndCompare(
		registry,
		strings.NewReader(expected),
		"raftiq_raft_role",
	); err != nil {
		t.Fatalf("unexpected role metrics: %v", err)
	}
}

func TestRaftMetricsSetIndexes(t *testing.T) {
	metrics, registry := newTestRaftMetrics(t)

	metrics.SetCommitIndex(raft.LogIndex(10))
	metrics.SetLastApplied(raft.LogIndex(9))
	metrics.SetLastLogIndex(raft.LogIndex(15))
	metrics.SetLogSize(6)

	expectedCommit := `
# HELP raftiq_raft_commit_index Highest Raft log index known to be committed.
# TYPE raftiq_raft_commit_index gauge
raftiq_raft_commit_index{node_id="node-1"} 10
`

	expectedApplied := `
# HELP raftiq_raft_last_applied Highest Raft log index applied to the state machine.
# TYPE raftiq_raft_last_applied gauge
raftiq_raft_last_applied{node_id="node-1"} 9
`

	expectedLastLog := `
# HELP raftiq_raft_last_log_index Highest Raft log index currently stored.
# TYPE raftiq_raft_last_log_index gauge
raftiq_raft_last_log_index{node_id="node-1"} 15
`

	expectedLogSize := `
# HELP raftiq_raft_log_size Number of entries currently retained in the Raft log.
# TYPE raftiq_raft_log_size gauge
raftiq_raft_log_size{node_id="node-1"} 6
`

	assertMetric(
		t,
		registry,
		"raftiq_raft_commit_index",
		expectedCommit,
	)

	assertMetric(
		t,
		registry,
		"raftiq_raft_last_applied",
		expectedApplied,
	)

	assertMetric(
		t,
		registry,
		"raftiq_raft_last_log_index",
		expectedLastLog,
	)

	assertMetric(
		t,
		registry,
		"raftiq_raft_log_size",
		expectedLogSize,
	)
}

func TestRaftMetricsElection(t *testing.T) {
	metrics, registry := newTestRaftMetrics(t)

	metrics.IncElections()
	metrics.IncElections()

	metrics.ObserveElectionDuration(
		250*time.Millisecond,
		"won",
	)

	metrics.ObserveElectionDuration(
		400*time.Millisecond,
		"lost",
	)

	metrics.ObserveElectionDuration(
		50*time.Millisecond,
		"failed",
	)

	expectedElections := `
# HELP raftiq_raft_elections_total Total number of Raft elections started.
# TYPE raftiq_raft_elections_total counter
raftiq_raft_elections_total{node_id="node-1"} 2
`

	if err := testutil.GatherAndCompare(
		registry,
		strings.NewReader(expectedElections),
		"raftiq_raft_elections_total",
	); err != nil {
		t.Fatalf("unexpected election counter: %v", err)
	}

	gathered, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}

	expectedResults := map[string]bool{
		"won":    false,
		"lost":   false,
		"failed": false,
	}

	for _, metricFamily := range gathered {
		if metricFamily.GetName() != "raftiq_raft_election_duration_seconds" {
			continue
		}

		for _, metric := range metricFamily.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() != "result" {
					continue
				}

				if _, ok := expectedResults[label.GetValue()]; ok {
					expectedResults[label.GetValue()] = true
				}
			}
		}
	}

	for result, found := range expectedResults {
		if !found {
			t.Fatalf(
				"expected election duration metric with result=%s",
				result,
			)
		}
	}
}

func TestRaftMetricsLeadershipAndVoting(t *testing.T) {
	metrics, registry := newTestRaftMetrics(t)

	metrics.IncLeaderChanges()
	metrics.IncLeaderChanges()

	metrics.IncVoteRequests()
	metrics.IncVoteRequests()
	metrics.IncVoteRequests()

	metrics.IncVotesGranted()

	expectedLeaderChanges := `
# HELP raftiq_raft_leader_changes_total Total number of Raft leadership changes.
# TYPE raftiq_raft_leader_changes_total counter
raftiq_raft_leader_changes_total{node_id="node-1"} 2
`

	expectedVoteRequests := `
# HELP raftiq_raft_vote_requests_total Total number of RequestVote RPCs processed.
# TYPE raftiq_raft_vote_requests_total counter
raftiq_raft_vote_requests_total{node_id="node-1"} 3
`

	expectedVotesGranted := `
# HELP raftiq_raft_votes_granted_total Total number of votes granted.
# TYPE raftiq_raft_votes_granted_total counter
raftiq_raft_votes_granted_total{node_id="node-1"} 1
`

	assertMetric(
		t,
		registry,
		"raftiq_raft_leader_changes_total",
		expectedLeaderChanges,
	)

	assertMetric(
		t,
		registry,
		"raftiq_raft_vote_requests_total",
		expectedVoteRequests,
	)

	assertMetric(
		t,
		registry,
		"raftiq_raft_votes_granted_total",
		expectedVotesGranted,
	)
}

func TestRaftMetricsAppendEntries(t *testing.T) {
	metrics, registry := newTestRaftMetrics(t)

	peerID := raft.NodeID("node-2")

	metrics.IncAppendEntries(peerID, "success")
	metrics.IncAppendEntries(peerID, "success")
	metrics.IncAppendEntries(peerID, "failure")

	metrics.IncAppendEntriesFailures(peerID)

	metrics.ObserveAppendEntriesDuration(
		peerID,
		100*time.Millisecond,
	)

	expected := `
# HELP raftiq_raft_append_entries_total Total number of AppendEntries RPCs processed.
# TYPE raftiq_raft_append_entries_total counter
raftiq_raft_append_entries_total{node_id="node-1",peer_id="node-2",result="failure"} 1
raftiq_raft_append_entries_total{node_id="node-1",peer_id="node-2",result="success"} 2
`

	if err := testutil.GatherAndCompare(
		registry,
		strings.NewReader(expected),
		"raftiq_raft_append_entries_total",
	); err != nil {
		t.Fatalf("unexpected AppendEntries metric: %v", err)
	}

	expectedFailures := `
# HELP raftiq_raft_append_entries_failures_total Total number of failed AppendEntries operations.
# TYPE raftiq_raft_append_entries_failures_total counter
raftiq_raft_append_entries_failures_total{node_id="node-1",peer_id="node-2"} 1
`

	if err := testutil.GatherAndCompare(
		registry,
		strings.NewReader(expectedFailures),
		"raftiq_raft_append_entries_failures_total",
	); err != nil {
		t.Fatalf("unexpected AppendEntries failure metric: %v", err)
	}

	assertHistogramObserved(
		t,
		registry,
		"raftiq_raft_append_entries_duration_seconds",
		"peer_id",
		"node-2",
	)
}

func TestRaftMetricsSnapshots(t *testing.T) {
	metrics, registry := newTestRaftMetrics(t)

	metrics.IncSnapshotsCreated()
	metrics.IncSnapshotsCreated()
	metrics.IncSnapshotsInstalled()

	expectedCreated := `
# HELP raftiq_raft_snapshots_created_total Total number of snapshots created.
# TYPE raftiq_raft_snapshots_created_total counter
raftiq_raft_snapshots_created_total{node_id="node-1"} 2
`

	expectedInstalled := `
# HELP raftiq_raft_snapshots_installed_total Total number of snapshots installed.
# TYPE raftiq_raft_snapshots_installed_total counter
raftiq_raft_snapshots_installed_total{node_id="node-1"} 1
`

	assertMetric(
		t,
		registry,
		"raftiq_raft_snapshots_created_total",
		expectedCreated,
	)

	assertMetric(
		t,
		registry,
		"raftiq_raft_snapshots_installed_total",
		expectedInstalled,
	)
}

func TestRoleLabel(t *testing.T) {
	tests := []struct {
		name string
		role raft.Role
		want string
	}{
		{
			name: "follower",
			role: raft.Follower,
			want: "follower",
		},
		{
			name: "candidate",
			role: raft.Candidate,
			want: "candidate",
		},
		{
			name: "leader",
			role: raft.Leader,
			want: "leader",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := roleLabel(tt.role); got != tt.want {
				t.Fatalf(
					"roleLabel(%v) = %q, want %q",
					tt.role,
					got,
					tt.want,
				)
			}
		})
	}
}

func assertMetric(
	t *testing.T,
	registry *prometheus.Registry,
	metricName string,
	expected string,
) {
	t.Helper()

	if err := testutil.GatherAndCompare(
		registry,
		strings.NewReader(expected),
		metricName,
	); err != nil {
		t.Fatalf(
			"unexpected metric %q: %v",
			metricName,
			err,
		)
	}
}

func assertHistogramObserved(
	t *testing.T,
	registry *prometheus.Registry,
	metricName string,
	labelName string,
	labelValue string,
) {
	t.Helper()

	gathered, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}

	for _, metricFamily := range gathered {
		if metricFamily.GetName() != metricName {
			continue
		}

		for _, metric := range metricFamily.GetMetric() {
			matchesLabel := false

			for _, label := range metric.GetLabel() {
				if label.GetName() == labelName &&
					label.GetValue() == labelValue {
					matchesLabel = true
				}
			}

			if matchesLabel &&
				metric.GetHistogram().GetSampleCount() != 1 {
				t.Fatalf(
					"histogram sample count = %d, want 1",
					metric.GetHistogram().GetSampleCount(),
				)
			}

			if matchesLabel {
				return
			}
		}
	}

	t.Fatalf(
		"histogram %q with %s=%s was not found",
		metricName,
		labelName,
		labelValue,
	)
}
