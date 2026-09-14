package model

type NodeID string
type Term uint64
type LogIndex uint64

type LogEntry struct {
	Index LogIndex
	Term  Term
	Data  []byte
}

// Configuration represents a Raft voter configuration.
type Configuration struct {
	Voters []NodeID
}

// JointConfiguration represents the temporary configuration used
// during a membership change.
//
// A quorum must be satisfied in both Old and New configurations
// while the cluster is in the joint-consensus phase.
type JointConfiguration struct {
	Old Configuration
	New Configuration
}

// Membership represents the cluster's current voting configuration.
//
// When Joint is nil, Current is the active configuration.
//
// When Joint is non-nil, the cluster is transitioning between
// Joint.Old and Joint.New and both configurations participate
// in quorum decisions.
type Membership struct {
	Current Configuration
	Joint   *JointConfiguration
}

type PersistentState struct {
	CurrentTerm Term
	VotedFor    NodeID
	Membership  Membership
}
