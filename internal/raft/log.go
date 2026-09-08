package raft

import "fmt"

type Log struct {
	entries []LogEntry
}

func NewLog() *Log {
	return &Log{
		entries: make([]LogEntry, 0),
	}
}

func (l *Log) LastIndex() LogIndex {
	if len(l.entries) == 0 {
		return 0
	}

	return l.entries[len(l.entries)-1].Index
}

func (l *Log) LastTerm() Term {
	if len(l.entries) == 0 {
		return 0
	}

	return l.entries[len(l.entries)-1].Term
}

func (l *Log) Get(index LogIndex) (LogEntry, bool) {
	for _, entry := range l.entries {
		if entry.Index == index {
			return entry, true
		}
	}

	return LogEntry{}, false
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

	for i, entry := range l.entries {
		if entry.Index >= index {
			l.entries = l.entries[:i]
			return
		}
	}
}
