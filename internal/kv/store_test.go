package kv

import "testing"

func TestStorePutGet(t *testing.T) {
	store := NewStore()

	store.Put("name", []byte("raftiq"))

	value, ok := store.Get("name")
	if !ok {
		t.Fatal("expected key to exist")
	}

	if string(value) != "raftiq" {
		t.Fatalf("expected raftiq, got %q", value)
	}
}

func TestStoreGetMissingKey(t *testing.T) {
	store := NewStore()

	_, ok := store.Get("missing")
	if ok {
		t.Fatal("expected missing key")
	}
}

func TestStoreDelete(t *testing.T) {
	store := NewStore()

	store.Put("name", []byte("raftiq"))

	if !store.Delete("name") {
		t.Fatal("expected delete to succeed")
	}

	_, ok := store.Get("name")
	if ok {
		t.Fatal("expected key to be deleted")
	}
}

func TestStoreDeleteMissingKey(t *testing.T) {
	store := NewStore()

	if store.Delete("missing") {
		t.Fatal("expected delete to report false")
	}
}
