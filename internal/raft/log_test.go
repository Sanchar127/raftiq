package raft

import "testing"

func TestNewLog(t *testing.T) {
	log := NewLog()

	if log.LastIndex() != 0 {
		t.Fatalf("expected last index 0, got %d", log.LastIndex())
	}

	if log.LastTerm() != 0 {
		t.Fatalf("expected last term 0, got %d", log.LastTerm())
	}
}

func TestLogAppend(t *testing.T) {
	log := NewLog()

	entries := []LogEntry{
		{Index: 1, Term: 1, Data: []byte("SET x=10")},
		{Index: 2, Term: 1, Data: []byte("SET y=20")},
		{Index: 3, Term: 2, Data: []byte("DELETE x")},
	}

	for _, entry := range entries {
		if err := log.Append(entry); err != nil {
			t.Fatalf("append failed: %v", err)
		}
	}

	if log.LastIndex() != 3 {
		t.Fatalf("expected last index 3, got %d", log.LastIndex())
	}

	if log.LastTerm() != 2 {
		t.Fatalf("expected last term 2, got %d", log.LastTerm())
	}
}

func TestLogAppendRejectsInvalidIndex(t *testing.T) {
	log := NewLog()

	if err := log.Append(LogEntry{
		Index: 1,
		Term:  1,
	}); err != nil {
		t.Fatalf("first append failed: %v", err)
	}

	err := log.Append(LogEntry{
		Index: 3,
		Term:  1,
	})

	if err == nil {
		t.Fatal("expected invalid index error, got nil")
	}

	if log.LastIndex() != 1 {
		t.Fatalf("log changed after rejected append: last index = %d", log.LastIndex())
	}
}

func TestLogGet(t *testing.T) {
	log := NewLog()

	entry := LogEntry{
		Index: 1,
		Term:  1,
		Data:  []byte("SET x=10"),
	}

	if err := log.Append(entry); err != nil {
		t.Fatalf("append failed: %v", err)
	}

	got, ok := log.Get(1)
	if !ok {
		t.Fatal("expected entry to exist")
	}

	if got.Index != entry.Index {
		t.Fatalf("expected index %d, got %d", entry.Index, got.Index)
	}

	if got.Term != entry.Term {
		t.Fatalf("expected term %d, got %d", entry.Term, got.Term)
	}

	if string(got.Data) != string(entry.Data) {
		t.Fatalf("expected data %q, got %q", entry.Data, got.Data)
	}

	_, ok = log.Get(2)
	if ok {
		t.Fatal("expected missing entry at index 2")
	}
}

func TestLogTruncateFrom(t *testing.T) {
	log := NewLog()

	for i := LogIndex(1); i <= 5; i++ {
		if err := log.Append(LogEntry{
			Index: i,
			Term:  Term(i),
		}); err != nil {
			t.Fatalf("append failed: %v", err)
		}
	}

	log.TruncateFrom(3)

	if log.LastIndex() != 2 {
		t.Fatalf("expected last index 2 after truncation, got %d", log.LastIndex())
	}

	if _, ok := log.Get(3); ok {
		t.Fatal("entry 3 should have been removed")
	}

	if _, ok := log.Get(4); ok {
		t.Fatal("entry 4 should have been removed")
	}

	if _, ok := log.Get(5); ok {
		t.Fatal("entry 5 should have been removed")
	}
}

func TestLogTruncateFromBeyondEnd(t *testing.T) {
	log := NewLog()

	for i := LogIndex(1); i <= 3; i++ {
		if err := log.Append(LogEntry{
			Index: i,
			Term:  1,
		}); err != nil {
			t.Fatalf("append failed: %v", err)
		}
	}

	log.TruncateFrom(10)

	if log.LastIndex() != 3 {
		t.Fatalf("expected log to remain unchanged, got last index %d", log.LastIndex())
	}
}
