package storage

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/sanchar127/raftiq/internal/model"
)

func TestWALSnapshotRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raftiq.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL() returned error: %v", err)
	}
	defer wal.Close()

	expected := model.Snapshot{
		LastIncludedIndex: 10,
		LastIncludedTerm:  3,
		Data:              []byte(`{"name":"raftiq"}`),
	}

	if err := wal.SaveSnapshot(expected); err != nil {
		t.Fatalf("SaveSnapshot() returned error: %v", err)
	}

	if err := wal.Sync(); err != nil {
		t.Fatalf("Sync() returned error: %v", err)
	}

	actual, err := wal.LoadSnapshot()
	if err != nil {
		t.Fatalf("LoadSnapshot() returned error: %v", err)
	}

	if actual.LastIncludedIndex != expected.LastIncludedIndex {
		t.Fatalf(
			"expected index %d, got %d",
			expected.LastIncludedIndex,
			actual.LastIncludedIndex,
		)
	}

	if actual.LastIncludedTerm != expected.LastIncludedTerm {
		t.Fatalf(
			"expected term %d, got %d",
			expected.LastIncludedTerm,
			actual.LastIncludedTerm,
		)
	}

	if !bytes.Equal(actual.Data, expected.Data) {
		t.Fatalf("snapshot data does not match")
	}
}
