package raft

import "github.com/sanchar127/raftiq/internal/model"

func configurationMajority(configuration model.Configuration) int {
	return len(configuration.Voters)/2 + 1
}

func configurationHasQuorum(
	configuration model.Configuration,
	votes map[NodeID]struct{},
) bool {
	required := configurationMajority(configuration)

	if required == 0 {
		return false
	}

	granted := 0

	for _, voterID := range configuration.Voters {
		if _, ok := votes[voterID]; ok {
			granted++
		}
	}

	return granted >= required
}

func membershipHasQuorum(
	membership model.Membership,
	votes map[NodeID]struct{},
) bool {
	if membership.Joint != nil {
		return configurationHasQuorum(
			membership.Joint.Old,
			votes,
		) &&
			configurationHasQuorum(
				membership.Joint.New,
				votes,
			)
	}

	return configurationHasQuorum(
		membership.Current,
		votes,
	)
}
func membershipIsVoter(
	membership model.Membership,
	nodeID NodeID,
) bool {
	if nodeID == "" {
		return false
	}

	if membership.Joint != nil {
		return configurationContainsVoter(
			membership.Joint.Old,
			nodeID,
		) ||
			configurationContainsVoter(
				membership.Joint.New,
				nodeID,
			)
	}

	return configurationContainsVoter(
		membership.Current,
		nodeID,
	)
}

func configurationContainsVoter(
	configuration model.Configuration,
	nodeID NodeID,
) bool {
	for _, voterID := range configuration.Voters {
		if voterID == nodeID {
			return true
		}
	}

	return false
}
