package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/sanchar127/raftiq/internal/raft"
)

func TestWALStoragePersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "raftiq.wal")

	storage, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL() error = %v", err)
	}

	state := raft.PersistentState{
		CurrentTerm: 7,
		VotedFor:    "node-2",
	}

	entries := []raft.LogEntry{
		{
			Index: 1,
			Term:  7,
			Data:  []byte("put:key=value"),
		},
		{
			Index: 2,
			Term:  7,
			Data:  []byte("put:other=value"),
		},
	}

	if err := storage.SaveState(state); err != nil {
		t.Fatalf("SaveState() error = %v", err)
	}

	if err := storage.AppendEntries(entries); err != nil {
		t.Fatalf("AppendEntries() error = %v", err)
	}

	if err := storage.Sync(); err != nil {
		t.Fatalf("Sync() error = %v", err)
	}

	if err := storage.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reopened, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("reopen WAL error = %v", err)
	}
	defer reopened.Close()

	gotState, err := reopened.LoadState()
	if err != nil {
		t.Fatalf("LoadState() error = %v", err)
	}

	if gotState != state {
		t.Fatalf("LoadState() = %+v, want %+v", gotState, state)
	}

	gotEntries, err := reopened.LoadEntries()
	if err != nil {
		t.Fatalf("LoadEntries() error = %v", err)
	}

	if len(gotEntries) != len(entries) {
		t.Fatalf(
			"LoadEntries() returned %d entries, want %d",
			len(gotEntries),
			len(entries),
		)
	}

	for i := range entries {
		if gotEntries[i].Index != entries[i].Index {
			t.Errorf(
				"entry %d index = %d, want %d",
				i,
				gotEntries[i].Index,
				entries[i].Index,
			)
		}

		if gotEntries[i].Term != entries[i].Term {
			t.Errorf(
				"entry %d term = %d, want %d",
				i,
				gotEntries[i].Term,
				entries[i].Term,
			)
		}

		if string(gotEntries[i].Data) != string(entries[i].Data) {
			t.Errorf(
				"entry %d data = %q, want %q",
				i,
				gotEntries[i].Data,
				entries[i].Data,
			)
		}
	}
}

func TestWALStorageCreatesFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "raftiq.wal")

	_, err := OpenWAL(path)
	if err == nil {
		t.Fatal("OpenWAL() error = nil, want error for missing directory")
	}

	if _, err := os.Stat(path); err == nil {
		t.Fatal("WAL file unexpectedly exists")
	}
}
