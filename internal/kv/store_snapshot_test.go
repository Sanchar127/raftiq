package kv

import (
	"bytes"
	"testing"
)

func TestStoreSnapshotRestore(t *testing.T) {
	store := NewStore()

	store.Put("name", []byte("raftiq"))
	store.Put("binary", []byte{0x00, 0x01, 0x02, 0xff})

	snapshot, err := store.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() returned error: %v", err)
	}

	restored := NewStore()

	if err := restored.Restore(snapshot); err != nil {
		t.Fatalf("Restore() returned error: %v", err)
	}

	value, ok := restored.Get("name")
	if !ok {
		t.Fatal("expected name to exist")
	}

	if string(value) != "raftiq" {
		t.Fatalf("expected %q, got %q", "raftiq", string(value))
	}

	value, ok = restored.Get("binary")
	if !ok {
		t.Fatal("expected binary to exist")
	}

	expected := []byte{0x00, 0x01, 0x02, 0xff}

	if !bytes.Equal(value, expected) {
		t.Fatalf("expected %v, got %v", expected, value)
	}
}

func TestStoreSnapshotEmpty(t *testing.T) {
	store := NewStore()

	snapshot, err := store.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() returned error: %v", err)
	}

	restored := NewStore()

	if err := restored.Restore(snapshot); err != nil {
		t.Fatalf("Restore() returned error: %v", err)
	}

	if _, ok := restored.Get("missing"); ok {
		t.Fatal("expected restored store to be empty")
	}
}

func TestStoreRestoreMalformedData(t *testing.T) {
	store := NewStore()

	err := store.Restore([]byte(`not valid json`))
	if err == nil {
		t.Fatal("expected Restore() to return an error")
	}
}

func TestStoreRestoreReplacesExistingState(t *testing.T) {
	store := NewStore()

	store.Put("old", []byte("value"))

	snapshot := []byte(`{"new":"dmFsdWU="}`)

	if err := store.Restore(snapshot); err != nil {
		t.Fatalf("Restore() returned error: %v", err)
	}

	if _, ok := store.Get("old"); ok {
		t.Fatal("old key should have been removed")
	}

	value, ok := store.Get("new")
	if !ok {
		t.Fatal("new key should exist")
	}

	if string(value) != "value" {
		t.Fatalf("expected %q, got %q", "value", value)
	}
}
