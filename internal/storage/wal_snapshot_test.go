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

func TestWALSnapshotPersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raftiq.wal")

	expected := model.Snapshot{
		LastIncludedIndex: 42,
		LastIncludedTerm:  7,
		Data:              []byte(`{"foo":"bar"}`),
	}

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL() returned error: %v", err)
	}

	if err := wal.SaveSnapshot(expected); err != nil {
		t.Fatalf("SaveSnapshot() returned error: %v", err)
	}

	if err := wal.Sync(); err != nil {
		t.Fatalf("Sync() returned error: %v", err)
	}

	if err := wal.Close(); err != nil {
		t.Fatalf("Close() returned error: %v", err)
	}

	reopened, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("reopen WAL returned error: %v", err)
	}
	defer reopened.Close()

	actual, err := reopened.LoadSnapshot()
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

func TestWALSnapshotLatestWins(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raftiq.wal")

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL() returned error: %v", err)
	}
	defer wal.Close()

	first := model.Snapshot{
		LastIncludedIndex: 10,
		LastIncludedTerm:  2,
		Data:              []byte(`{"version":1}`),
	}

	second := model.Snapshot{
		LastIncludedIndex: 20,
		LastIncludedTerm:  4,
		Data:              []byte(`{"version":2}`),
	}

	if err := wal.SaveSnapshot(first); err != nil {
		t.Fatalf("SaveSnapshot(first) returned error: %v", err)
	}

	if err := wal.SaveSnapshot(second); err != nil {
		t.Fatalf("SaveSnapshot(second) returned error: %v", err)
	}

	actual, err := wal.LoadSnapshot()
	if err != nil {
		t.Fatalf("LoadSnapshot() returned error: %v", err)
	}

	if actual.LastIncludedIndex != second.LastIncludedIndex {
		t.Fatalf(
			"expected latest index %d, got %d",
			second.LastIncludedIndex,
			actual.LastIncludedIndex,
		)
	}

	if actual.LastIncludedTerm != second.LastIncludedTerm {
		t.Fatalf(
			"expected latest term %d, got %d",
			second.LastIncludedTerm,
			actual.LastIncludedTerm,
		)
	}

	if !bytes.Equal(actual.Data, second.Data) {
		t.Fatalf("latest snapshot data does not match")
	}
}

func TestWALSnapshotLoadReturnsCopy(t *testing.T) {
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

	actual, err := wal.LoadSnapshot()
	if err != nil {
		t.Fatalf("LoadSnapshot() returned error: %v", err)
	}

	actual.Data[0] = 'X'

	reloaded, err := wal.LoadSnapshot()
	if err != nil {
		t.Fatalf("second LoadSnapshot() returned error: %v", err)
	}

	if !bytes.Equal(reloaded.Data, expected.Data) {
		t.Fatalf("LoadSnapshot() returned aliased snapshot data")
	}
}
