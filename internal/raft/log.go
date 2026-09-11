package raft

import (
	"fmt"
	"sync"

	"github.com/sanchar127/raftiq/internal/model"
)

type Log struct {
	mu sync.RWMutex

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
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.lastIndexLocked()
}

func (l *Log) LastTerm() Term {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.lastTermLocked()
}

func (l *Log) Append(entry LogEntry) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	expectedIndex := l.lastIndexLocked() + 1

	if entry.Index != expectedIndex {
		return fmt.Errorf(
			"invalid log index: got %d, expected %d",
			entry.Index,
			expectedIndex,
		)
	}

	entry.Data = cloneBytes(entry.Data)
	l.entries = append(l.entries, entry)

	return nil
}

func (l *Log) TruncateFrom(index LogIndex) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.truncateFromLocked(index)
}

func (l *Log) LastIncludedIndex() LogIndex {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.lastIncludedIndex
}

func (l *Log) LastIncludedTerm() Term {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.lastIncludedTerm
}

func (l *Log) Compact(snapshot model.Snapshot) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if snapshot.LastIncludedIndex < l.lastIncludedIndex {
		return fmt.Errorf(
			"cannot move snapshot backwards: current %d, requested %d",
			l.lastIncludedIndex,
			snapshot.LastIncludedIndex,
		)
	}

	if snapshot.LastIncludedIndex > l.lastIndexLocked() {
		return fmt.Errorf(
			"cannot compact beyond last log index: snapshot %d, last index %d",
			snapshot.LastIncludedIndex,
			l.lastIndexLocked(),
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

	entry, ok := l.getLocked(snapshot.LastIncludedIndex)
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
			entry.Data = cloneBytes(entry.Data)
			remaining = append(remaining, entry)
		}
	}

	l.entries = remaining
	l.lastIncludedIndex = snapshot.LastIncludedIndex
	l.lastIncludedTerm = snapshot.LastIncludedTerm

	return nil
}

func (l *Log) Get(index LogIndex) (LogEntry, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.getLocked(index)
}

func (l *Log) RestoreSnapshot(snapshot model.Snapshot) error {
	l.mu.Lock()
	defer l.mu.Unlock()

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

	if entry, ok := l.getLocked(snapshot.LastIncludedIndex); ok {
		boundaryMatches = entry.Term == snapshot.LastIncludedTerm
	}

	if boundaryMatches {
		// The snapshot agrees with our log at the boundary.
		// Preserve the suffix after the snapshot index.
		remaining := make([]LogEntry, 0, len(l.entries))

		for _, entry := range l.entries {
			if entry.Index > snapshot.LastIncludedIndex {
				entry.Data = cloneBytes(entry.Data)
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

func (l *Log) lastIndexLocked() LogIndex {
	if len(l.entries) == 0 {
		return l.lastIncludedIndex
	}

	return l.entries[len(l.entries)-1].Index
}

func (l *Log) lastTermLocked() Term {
	if len(l.entries) == 0 {
		return l.lastIncludedTerm
	}

	return l.entries[len(l.entries)-1].Term
}

func (l *Log) getLocked(index LogIndex) (LogEntry, bool) {
	if index == l.lastIncludedIndex && index != 0 {
		return LogEntry{
			Index: index,
			Term:  l.lastIncludedTerm,
		}, true
	}

	for _, entry := range l.entries {
		if entry.Index == index {
			entry.Data = cloneBytes(entry.Data)
			return entry, true
		}
	}

	return LogEntry{}, false
}

func (l *Log) truncateFromLocked(index LogIndex) {
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

func cloneBytes(data []byte) []byte {
	if data == nil {
		return nil
	}

	return append([]byte(nil), data...)
}

func (l *Log) Size() int {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return len(l.entries)
}