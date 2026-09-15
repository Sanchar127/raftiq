package raft

import (
	"fmt"
	"io"
	"log/slog"
	"time"
)

func (n *RaftNode) persistStateLocked() error {
	if err := n.storage.SaveState(n.state.Persistent); err != nil {
		return fmt.Errorf(
			"save persistent state: %w",
			err,
		)
	}

	if err := n.storage.Sync(); err != nil {
		return fmt.Errorf(
			"sync persistent state: %w",
			err,
		)
	}

	return nil
}

func (n *RaftNode) updateStateMetricsLocked() {
	n.metrics.SetCurrentTerm(
		n.state.Persistent.CurrentTerm,
	)

	n.metrics.SetRole(
		n.state.Role,
	)

	n.metrics.SetCommitIndex(
		n.state.Volatile.CommitIndex,
	)

	n.metrics.SetLastApplied(
		n.state.Volatile.LastApplied,
	)

	n.metrics.SetLastLogIndex(
		n.log.LastIndex(),
	)

	n.metrics.SetLogSize(
		n.log.Size(),
	)
}

func (n *RaftNode) finishElectionLocked(result string) {
	if n.electionStartedAt.IsZero() {
		return
	}

	n.metrics.ObserveElectionDuration(
		time.Since(n.electionStartedAt),
		result,
	)

	n.electionStartedAt = time.Time{}
}

func cloneEntries(entries []LogEntry) []LogEntry {
	cloned := make([]LogEntry, len(entries))

	for i, entry := range entries {
		cloned[i] = entry
		cloned[i].Data = append([]byte(nil), entry.Data...)
	}

	return cloned
}

func discardRaftLogger() *slog.Logger {
	return slog.New(
		slog.NewTextHandler(io.Discard, nil),
	)
}

func (n *RaftNode) getLogger() *slog.Logger {
	n.mu.RLock()
	logger := n.logger
	n.mu.RUnlock()

	if logger == nil {
		return discardRaftLogger()
	}

	return logger
}
