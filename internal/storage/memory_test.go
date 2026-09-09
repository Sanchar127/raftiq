package storage

import (
	"testing"

	"github.com/sanchar127/raftiq/internal/model"
)

func TestMemoryStorageStateRoundTrip(t *testing.T) {
	storage := NewMemoryStorage()

	want := model.PersistentState{
		CurrentTerm: 7,
		VotedFor:    "node-2",
	}

	if err := storage.SaveState(want); err != nil {
		t.Fatalf("SaveState() error = %v", err)
	}

	got, err := storage.LoadState()
	if err != nil {
		t.Fatalf("LoadState() error = %v", err)
	}

	if got != want {
		t.Fatalf("LoadState() = %+v, want %+v", got, want)
	}
}

func TestMemoryStorageEntriesRoundTrip(t *testing.T) {
	storage := NewMemoryStorage()

	entries := []model.LogEntry{
		{
			Index: 1,
			Term:  1,
			Data:  []byte("first"),
		},
		{
			Index: 2,
			Term:  1,
			Data:  []byte("second"),
		},
	}

	if err := storage.AppendEntries(entries); err != nil {
		t.Fatalf("AppendEntries() error = %v", err)
	}

	got, err := storage.LoadEntries()
	if err != nil {
		t.Fatalf("LoadEntries() error = %v", err)
	}

	if len(got) != len(entries) {
		t.Fatalf("got %d entries, want %d", len(got), len(entries))
	}

	for i := range entries {
		if got[i].Index != entries[i].Index {
			t.Errorf(
				"entry %d index = %d, want %d",
				i,
				got[i].Index,
				entries[i].Index,
			)
		}

		if got[i].Term != entries[i].Term {
			t.Errorf(
				"entry %d term = %d, want %d",
				i,
				got[i].Term,
				entries[i].Term,
			)
		}

		if string(got[i].Data) != string(entries[i].Data) {
			t.Errorf(
				"entry %d data = %q, want %q",
				i,
				got[i].Data,
				entries[i].Data,
			)
		}
	}
}

func TestMemoryStorageLoadEntriesReturnsCopy(t *testing.T) {
	storage := NewMemoryStorage()

	entries := []model.LogEntry{
		{
			Index: 1,
			Term:  1,
			Data:  []byte("hello"),
		},
	}

	if err := storage.AppendEntries(entries); err != nil {
		t.Fatalf("AppendEntries() error = %v", err)
	}

	got, err := storage.LoadEntries()
	if err != nil {
		t.Fatalf("LoadEntries() error = %v", err)
	}

	got[0].Data[0] = 'X'

	again, err := storage.LoadEntries()
	if err != nil {
		t.Fatalf("LoadEntries() error = %v", err)
	}

	if string(again[0].Data) != "hello" {
		t.Fatalf(
			"internal data was modified through returned slice: %q",
			again[0].Data,
		)
	}
}

func TestMemoryStorageAppendEntriesCopiesInput(t *testing.T) {
	storage := NewMemoryStorage()

	data := []byte("hello")

	entries := []model.LogEntry{
		{
			Index: 1,
			Term:  1,
			Data:  data,
		},
	}

	if err := storage.AppendEntries(entries); err != nil {
		t.Fatalf("AppendEntries() error = %v", err)
	}

	data[0] = 'X'

	got, err := storage.LoadEntries()
	if err != nil {
		t.Fatalf("LoadEntries() error = %v", err)
	}

	if string(got[0].Data) != "hello" {
		t.Fatalf(
			"stored data was modified through input slice: %q",
			got[0].Data,
		)
	}
}
