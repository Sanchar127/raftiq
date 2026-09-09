package raft

import (
	"fmt"

	"github.com/sanchar127/raftiq/internal/model"
)

type Log struct {
	entries           []LogEntry
	lastIncludedIndex LogIndex
	lastIncludedTerm  Term
}

func NewLog() *Log {
	return &Log{
		entries: make([]LogEntry, 0),
	}
}

func (l *Log) LastIndex() LogIndex {
	if len(l.entries) == 0 {
		return l.lastIncludedIndex
	}

	return l.entries[len(l.entries)-1].Index
}

func (l *Log) LastTerm() Term {
	if len(l.entries) == 0 {
		return l.lastIncludedTerm
	}

	return l.entries[len(l.entries)-1].Term
}

func (l *Log) Append(entry LogEntry) error {
	expectedIndex := l.LastIndex() + 1

	if entry.Index != expectedIndex {
		return fmt.Errorf(
			"invalid log index: got %d, expected %d",
			entry.Index,
			expectedIndex,
		)
	}

	l.entries = append(l.entries, entry)

	return nil
}

func (l *Log) TruncateFrom(index LogIndex) {
	if index == 0 {
		l.entries = nil
		return
	}

	if index <= l.lastIncludedIndex {
		return
	}

	for i, entry := range l.entries {
		if entry.Index >= index {
			l.entries = l.entries[:i]
			return
		}
	}
}

func (l *Log) LastIncludedIndex() LogIndex {
	return l.lastIncludedIndex
}

func (l *Log) LastIncludedTerm() Term {
	return l.lastIncludedTerm
}

func (l *Log) Compact(snapshot model.Snapshot) error {
	if snapshot.LastIncludedIndex < l.lastIncludedIndex {
		return fmt.Errorf(
			"cannot move snapshot backwards: current %d, requested %d",
			l.lastIncludedIndex,
			snapshot.LastIncludedIndex,
		)
	}

	if snapshot.LastIncludedIndex > l.LastIndex() {
		return fmt.Errorf(
			"cannot compact beyond last log index: snapshot %d, last index %d",
			snapshot.LastIncludedIndex,
			l.LastIndex(),
		)
	}

	if snapshot.LastIncludedIndex == l.lastIncludedIndex {
		if snapshot.LastIncludedTerm != l.lastIncludedTerm {
			return fmt.Errorf(
				"snapshot term mismatch at index %d: current term %d, requested term %d",
				snapshot.LastIncludedIndex,
				l.lastIncludedTerm,
				snapshot.LastIncludedTerm,
			)
		}

		return nil
	}

	entry, ok := l.Get(snapshot.LastIncludedIndex)
	if !ok {
		return fmt.Errorf(
			"cannot compact: snapshot boundary index %d not found",
			snapshot.LastIncludedIndex,
		)
	}

	if entry.Term != snapshot.LastIncludedTerm {
		return fmt.Errorf(
			"snapshot term mismatch at index %d: log term %d, snapshot term %d",
			snapshot.LastIncludedIndex,
			entry.Term,
			snapshot.LastIncludedTerm,
		)
	}

	remaining := make([]LogEntry, 0, len(l.entries))

	for _, entry := range l.entries {
		if entry.Index > snapshot.LastIncludedIndex {
			remaining = append(remaining, entry)
		}
	}

	l.entries = remaining
	l.lastIncludedIndex = snapshot.LastIncludedIndex
	l.lastIncludedTerm = snapshot.LastIncludedTerm

	return nil
}

func (l *Log) Get(index LogIndex) (LogEntry, bool) {
	if index == l.lastIncludedIndex && index != 0 {
		return LogEntry{
			Index: index,
			Term:  l.lastIncludedTerm,
		}, true
	}

	for _, entry := range l.entries {
		if entry.Index == index {
			return entry, true
		}
	}

	return LogEntry{}, false
}

func (l *Log) RestoreSnapshot(snapshot model.Snapshot) error {
	if snapshot.LastIncludedIndex < l.lastIncludedIndex {
		return fmt.Errorf(
			"cannot restore snapshot backwards: current %d, requested %d",
			l.lastIncludedIndex,
			snapshot.LastIncludedIndex,
		)
	}

	if snapshot.LastIncludedIndex == l.lastIncludedIndex {
		if snapshot.LastIncludedTerm != l.lastIncludedTerm {
			return fmt.Errorf(
				"snapshot term mismatch at index %d: current term %d, requested term %d",
				snapshot.LastIncludedIndex,
				l.lastIncludedTerm,
				snapshot.LastIncludedTerm,
			)
		}

		return nil
	}

	// Determine whether the follower has the same log entry
	// as the snapshot boundary.
	boundaryMatches := false

	if entry, ok := l.Get(snapshot.LastIncludedIndex); ok {
		boundaryMatches = entry.Term == snapshot.LastIncludedTerm
	}

	if boundaryMatches {
		// The snapshot agrees with our log at the boundary.
		// Preserve the suffix after the snapshot index.
		remaining := make([]LogEntry, 0, len(l.entries))

		for _, entry := range l.entries {
			if entry.Index > snapshot.LastIncludedIndex {
				remaining = append(remaining, entry)
			}
		}

		l.entries = remaining
	} else {
		// The snapshot conflicts with our log or the boundary
		// is missing. The existing suffix cannot be trusted.
		l.entries = nil
	}

	l.lastIncludedIndex = snapshot.LastIncludedIndex
	l.lastIncludedTerm = snapshot.LastIncludedTerm

	return nil
}
