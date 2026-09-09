package raft

import (
	"fmt"
	"github.com/sanchar127/raftiq/internal/model"
	"testing"
)

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

func TestLogCompact(t *testing.T) {
	log := NewLog()

	for i := LogIndex(1); i <= 5; i++ {
		if err := log.Append(LogEntry{
			Index: i,
			Term:  Term(i),
			Data:  []byte{byte(i)},
		}); err != nil {
			t.Fatalf("append failed: %v", err)
		}
	}

	snapshot := model.Snapshot{
		LastIncludedIndex: 3,
		LastIncludedTerm:  3,
		Data:              []byte(`{"x":"value"}`),
	}

	if err := log.Compact(snapshot); err != nil {
		t.Fatalf("compact failed: %v", err)
	}

	if log.LastIncludedIndex() != 3 {
		t.Fatalf(
			"expected last included index 3, got %d",
			log.LastIncludedIndex(),
		)
	}

	if log.LastIncludedTerm() != 3 {
		t.Fatalf(
			"expected last included term 3, got %d",
			log.LastIncludedTerm(),
		)
	}

	if log.LastIndex() != 5 {
		t.Fatalf("expected last index 5, got %d", log.LastIndex())
	}

	entry, ok := log.Get(3)
	if !ok {
		t.Fatal("expected snapshot boundary entry to exist")
	}

	if entry.Index != 3 {
		t.Fatalf("expected boundary index 3, got %d", entry.Index)
	}

	if entry.Term != 3 {
		t.Fatalf("expected boundary term 3, got %d", entry.Term)
	}

	if _, ok := log.Get(1); ok {
		t.Fatal("entry 1 should have been compacted")
	}

	if _, ok := log.Get(2); ok {
		t.Fatal("entry 2 should have been compacted")
	}

	if _, ok := log.Get(4); !ok {
		t.Fatal("entry 4 should remain after compaction")
	}

	if _, ok := log.Get(5); !ok {
		t.Fatal("entry 5 should remain after compaction")
	}
}

func TestLogCompactEntireLog(t *testing.T) {
	log := NewLog()

	for i := LogIndex(1); i <= 5; i++ {
		if err := log.Append(LogEntry{
			Index: i,
			Term:  2,
		}); err != nil {
			t.Fatalf("append failed: %v", err)
		}
	}

	snapshot := model.Snapshot{
		LastIncludedIndex: 5,
		LastIncludedTerm:  2,
		Data:              []byte(`snapshot`),
	}

	if err := log.Compact(snapshot); err != nil {
		t.Fatalf("compact failed: %v", err)
	}

	if log.LastIndex() != 5 {
		t.Fatalf("expected last index 5, got %d", log.LastIndex())
	}

	if log.LastTerm() != 2 {
		t.Fatalf("expected last term 2, got %d", log.LastTerm())
	}

	if _, ok := log.Get(5); !ok {
		t.Fatal("expected snapshot boundary entry to exist")
	}

	if _, ok := log.Get(4); ok {
		t.Fatal("entry 4 should have been compacted")
	}
}

func TestLogCompactRejectsFutureSnapshot(t *testing.T) {
	log := NewLog()

	for i := LogIndex(1); i <= 3; i++ {
		if err := log.Append(LogEntry{
			Index: i,
			Term:  1,
		}); err != nil {
			t.Fatalf("append failed: %v", err)
		}
	}

	err := log.Compact(model.Snapshot{
		LastIncludedIndex: 4,
		LastIncludedTerm:  1,
	})

	if err == nil {
		t.Fatal("expected compact beyond log to fail")
	}

	if log.LastIndex() != 3 {
		t.Fatalf(
			"log changed after rejected compaction: last index = %d",
			log.LastIndex(),
		)
	}
}

func TestLogCompactRejectsSnapshotRollback(t *testing.T) {
	log := NewLog()

	for i := LogIndex(1); i <= 5; i++ {
		if err := log.Append(LogEntry{
			Index: i,
			Term:  1,
		}); err != nil {
			t.Fatalf("append failed: %v", err)
		}
	}

	if err := log.Compact(model.Snapshot{
		LastIncludedIndex: 4,
		LastIncludedTerm:  1,
	}); err != nil {
		t.Fatalf("initial compact failed: %v", err)
	}

	err := log.Compact(model.Snapshot{
		LastIncludedIndex: 3,
		LastIncludedTerm:  1,
	})

	if err == nil {
		t.Fatal("expected snapshot rollback to be rejected")
	}

	if log.LastIncludedIndex() != 4 {
		t.Fatalf(
			"snapshot boundary changed after rejected rollback: got %d",
			log.LastIncludedIndex(),
		)
	}
}

func TestLogCompactRejectsTermMismatch(t *testing.T) {
	log := NewLog()

	for i := LogIndex(1); i <= 5; i++ {
		if err := log.Append(LogEntry{
			Index: i,
			Term:  Term(i),
		}); err != nil {
			t.Fatalf("append failed: %v", err)
		}
	}

	err := log.Compact(model.Snapshot{
		LastIncludedIndex: 3,
		LastIncludedTerm:  99,
	})

	if err == nil {
		t.Fatal("expected snapshot term mismatch to be rejected")
	}

	if log.LastIncludedIndex() != 0 {
		t.Fatalf(
			"log changed after rejected term mismatch: snapshot index = %d",
			log.LastIncludedIndex(),
		)
	}
}

func TestLogTruncateFromDoesNotRemoveSnapshotBoundary(t *testing.T) {
	log := NewLog()

	for i := LogIndex(1); i <= 5; i++ {
		if err := log.Append(LogEntry{
			Index: i,
			Term:  1,
		}); err != nil {
			t.Fatalf("append failed: %v", err)
		}
	}

	if err := log.Compact(model.Snapshot{
		LastIncludedIndex: 3,
		LastIncludedTerm:  1,
	}); err != nil {
		t.Fatalf("compact failed: %v", err)
	}

	log.TruncateFrom(3)

	if log.LastIncludedIndex() != 3 {
		t.Fatalf(
			"snapshot boundary was removed: got %d",
			log.LastIncludedIndex(),
		)
	}

	entry, ok := log.Get(3)
	if !ok {
		t.Fatal("snapshot boundary should still exist")
	}

	if entry.Term != 1 {
		t.Fatalf("expected boundary term 1, got %d", entry.Term)
	}

	if log.LastIndex() != 5 {
		t.Fatalf(
			"expected entries after snapshot to remain, got last index %d",
			log.LastIndex(),
		)
	}

	if _, ok := log.Get(4); !ok {
		t.Fatal("entry 4 should remain after protected truncation")
	}

	if _, ok := log.Get(5); !ok {
		t.Fatal("entry 5 should remain after protected truncation")
	}
}

func TestLogTruncateFromBeforeSnapshotDoesNotRemoveBoundary(t *testing.T) {
	log := NewLog()

	for i := LogIndex(1); i <= 5; i++ {
		if err := log.Append(LogEntry{
			Index: i,
			Term:  1,
		}); err != nil {
			t.Fatalf("append failed: %v", err)
		}
	}

	if err := log.Compact(model.Snapshot{
		LastIncludedIndex: 3,
		LastIncludedTerm:  1,
	}); err != nil {
		t.Fatalf("compact failed: %v", err)
	}

	log.TruncateFrom(1)

	if log.LastIncludedIndex() != 3 {
		t.Fatalf(
			"snapshot boundary changed: got %d",
			log.LastIncludedIndex(),
		)
	}

	if _, ok := log.Get(3); !ok {
		t.Fatal("snapshot boundary should still exist")
	}
}

func TestLogCompactSameSnapshotIsIdempotent(t *testing.T) {
	log := NewLog()

	for i := LogIndex(1); i <= 5; i++ {
		if err := log.Append(LogEntry{
			Index: i,
			Term:  2,
		}); err != nil {
			t.Fatalf("append failed: %v", err)
		}
	}

	snapshot := model.Snapshot{
		LastIncludedIndex: 3,
		LastIncludedTerm:  2,
	}

	if err := log.Compact(snapshot); err != nil {
		t.Fatalf("first compact failed: %v", err)
	}

	if err := log.Compact(snapshot); err != nil {
		t.Fatalf("second identical compact should succeed: %v", err)
	}

	if log.LastIncludedIndex() != 3 {
		t.Fatalf(
			"expected snapshot index 3, got %d",
			log.LastIncludedIndex(),
		)
	}

	if log.LastIndex() != 5 {
		t.Fatalf("expected last index 5, got %d", log.LastIndex())
	}
}

func TestLogRestoreSnapshot(t *testing.T) {
	log := NewLog()

	for i := LogIndex(1); i <= 5; i++ {
		if err := log.Append(LogEntry{
			Index: i,
			Term:  1,
			Data:  []byte(fmt.Sprintf("entry-%d", i)),
		}); err != nil {
			t.Fatalf("append failed: %v", err)
		}
	}

	snapshot := model.Snapshot{
		LastIncludedIndex: 3,
		LastIncludedTerm:  1,
		Data:              []byte(`{"key":"value"}`),
	}

	if err := log.RestoreSnapshot(snapshot); err != nil {
		t.Fatalf("restore snapshot failed: %v", err)
	}

	if log.LastIncludedIndex() != 3 {
		t.Fatalf(
			"expected snapshot boundary 3, got %d",
			log.LastIncludedIndex(),
		)
	}

	if log.LastIncludedTerm() != 1 {
		t.Fatalf(
			"expected snapshot term 1, got %d",
			log.LastIncludedTerm(),
		)
	}

	if log.LastIndex() != 5 {
		t.Fatalf(
			"expected last index 5, got %d",
			log.LastIndex(),
		)
	}

	entry, ok := log.Get(3)
	if !ok {
		t.Fatal("expected snapshot boundary to be readable")
	}

	if entry.Term != 1 {
		t.Fatalf(
			"expected boundary term 1, got %d",
			entry.Term,
		)
	}

	if _, ok := log.Get(2); ok {
		t.Fatal("expected compacted entry 2 to be unavailable")
	}

	if _, ok := log.Get(4); !ok {
		t.Fatal("expected entry 4 to remain")
	}

	if _, ok := log.Get(5); !ok {
		t.Fatal("expected entry 5 to remain")
	}
}
