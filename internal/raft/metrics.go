package raft

import "time"

type Metrics interface {
	SetCurrentTerm(term Term)
	SetRole(role Role)
	SetCommitIndex(index LogIndex)
	SetLastApplied(index LogIndex)
	SetLastLogIndex(index LogIndex)
	SetLogSize(size int)

	IncElections()
	ObserveElectionDuration(duration time.Duration, result string)
	IncLeaderChanges()

	IncVoteRequests()
	IncVotesGranted()

	IncAppendEntries(peerID NodeID, result string)
	IncAppendEntriesFailures(peerID NodeID)
	ObserveAppendEntriesDuration(peerID NodeID, duration time.Duration)

	IncSnapshotsCreated()
	IncSnapshotsInstalled()
}

type NoopMetrics struct{}

func (NoopMetrics) SetCurrentTerm(Term)                                {}
func (NoopMetrics) SetRole(Role)                                       {}
func (NoopMetrics) SetCommitIndex(LogIndex)                            {}
func (NoopMetrics) SetLastApplied(LogIndex)                            {}
func (NoopMetrics) SetLastLogIndex(LogIndex)                           {}
func (NoopMetrics) SetLogSize(int)                                     {}
func (NoopMetrics) IncElections()                                      {}
func (NoopMetrics) ObserveElectionDuration(time.Duration, string)      {}
func (NoopMetrics) IncLeaderChanges()                                  {}
func (NoopMetrics) IncVoteRequests()                                   {}
func (NoopMetrics) IncVotesGranted()                                   {}
func (NoopMetrics) IncAppendEntries(NodeID, string)                    {}
func (NoopMetrics) IncAppendEntriesFailures(NodeID)                    {}
func (NoopMetrics) ObserveAppendEntriesDuration(NodeID, time.Duration) {}
func (NoopMetrics) IncSnapshotsCreated()                               {}
func (NoopMetrics) IncSnapshotsInstalled()                             {}
