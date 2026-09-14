package raft

import (
	"testing"

	"github.com/sanchar127/raftiq/internal/model"
)

func TestConfigurationMajority(t *testing.T) {
	tests := []struct {
		name string
		size int
		want int
	}{
		{
			name: "empty",
			size: 0,
			want: 1,
		},
		{
			name: "one",
			size: 1,
			want: 1,
		},
		{
			name: "two",
			size: 2,
			want: 2,
		},
		{
			name: "three",
			size: 3,
			want: 2,
		},
		{
			name: "four",
			size: 4,
			want: 3,
		},
		{
			name: "five",
			size: 5,
			want: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			configuration := model.Configuration{
				Voters: make([]model.NodeID, tt.size),
			}

			if got := configurationMajority(configuration); got != tt.want {
				t.Fatalf(
					"configurationMajority() = %d, want %d",
					got,
					tt.want,
				)
			}
		})
	}
}

func TestConfigurationHasQuorum(t *testing.T) {
	configuration := model.Configuration{
		Voters: []model.NodeID{
			"node-1",
			"node-2",
			"node-3",
		},
	}

	tests := []struct {
		name  string
		votes []NodeID
		want  bool
	}{
		{
			name:  "no votes",
			votes: nil,
			want:  false,
		},
		{
			name: "one vote",
			votes: []NodeID{
				"node-1",
			},
			want: false,
		},
		{
			name: "two votes",
			votes: []NodeID{
				"node-1",
				"node-2",
			},
			want: true,
		},
		{
			name: "three votes",
			votes: []NodeID{
				"node-1",
				"node-2",
				"node-3",
			},
			want: true,
		},
		{
			name: "non-voter does not count",
			votes: []NodeID{
				"node-1",
				"node-4",
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			votes := make(map[NodeID]struct{}, len(tt.votes))

			for _, voterID := range tt.votes {
				votes[voterID] = struct{}{}
			}

			if got := configurationHasQuorum(
				configuration,
				votes,
			); got != tt.want {
				t.Fatalf(
					"configurationHasQuorum() = %v, want %v",
					got,
					tt.want,
				)
			}
		})
	}
}

func TestMembershipHasQuorumStable(t *testing.T) {
	membership := model.Membership{
		Current: model.Configuration{
			Voters: []NodeID{
				"node-1",
				"node-2",
				"node-3",
			},
		},
	}

	votes := map[NodeID]struct{}{
		"node-1": {},
		"node-2": {},
	}

	if !membershipHasQuorum(membership, votes) {
		t.Fatal("expected stable membership quorum")
	}
}

func TestMembershipHasQuorumJointRequiresBothMajorities(t *testing.T) {
	membership := model.Membership{
		Current: model.Configuration{
			Voters: []NodeID{
				"node-1",
				"node-2",
				"node-3",
			},
		},
		Joint: &model.JointConfiguration{
			Old: model.Configuration{
				Voters: []NodeID{
					"node-1",
					"node-2",
					"node-3",
				},
			},
			New: model.Configuration{
				Voters: []NodeID{
					"node-1",
					"node-4",
					"node-5",
				},
			},
		},
	}

	tests := []struct {
		name  string
		votes map[NodeID]struct{}
		want  bool
	}{
		{
			name: "old majority only",
			votes: map[NodeID]struct{}{
				"node-1": {},
				"node-2": {},
			},
			want: false,
		},
		{
			name: "new majority only",
			votes: map[NodeID]struct{}{
				"node-1": {},
				"node-4": {},
			},
			want: false,
		},
		{
			name: "both majorities",
			votes: map[NodeID]struct{}{
				"node-1": {},
				"node-2": {},
				"node-4": {},
			},
			want: true,
		},
		{
			name: "single old voter",
			votes: map[NodeID]struct{}{
				"node-2": {},
			},
			want: false,
		},
		{
			name: "single new voter",
			votes: map[NodeID]struct{}{
				"node-4": {},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := membershipHasQuorum(
				membership,
				tt.votes,
			); got != tt.want {
				t.Fatalf(
					"membershipHasQuorum() = %v, want %v",
					got,
					tt.want,
				)
			}
		})
	}
}
