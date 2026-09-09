package kv

import (
	"bytes"
	"errors"
	"testing"

	"github.com/sanchar127/raftiq/internal/lock"
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

func TestStoreSnapshotRestoreFencedState(t *testing.T) {
	store := NewStore()

	_, acquired, err := store.AcquireLock(
		"job:1",
		"worker-A",
		1000,
		42,
	)
	if err != nil {
		t.Fatalf("AcquireLock() returned error: %v", err)
	}

	if !acquired {
		t.Fatal("expected lock to be acquired")
	}

	if err := store.FencedPut(
		"job:1",
		[]byte("worker-a-result"),
		1,
	); err != nil {
		t.Fatalf("FencedPut() returned error: %v", err)
	}

	snapshot, err := store.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() returned error: %v", err)
	}

	restored := NewStore()

	if err := restored.Restore(snapshot); err != nil {
		t.Fatalf("Restore() returned error: %v", err)
	}

	value, ok := restored.GetFenced("job:1")
	if !ok {
		t.Fatal("expected fenced resource to exist after restore")
	}

	if string(value.Value) != "worker-a-result" {
		t.Fatalf(
			"expected %q, got %q",
			"worker-a-result",
			string(value.Value),
		)
	}

	if value.FencingToken != 1 {
		t.Fatalf(
			"expected fencing token 1, got %d",
			value.FencingToken,
		)
	}

	lockValue, ok := restored.GetLock("job:1")
	if !ok {
		t.Fatal("expected lock to exist after restore")
	}

	if lockValue.FencingToken != 1 {
		t.Fatalf(
			"expected restored lock fencing token 1, got %d",
			lockValue.FencingToken,
		)
	}

	if lockValue.OwnerID != "worker-A" {
		t.Fatalf(
			"expected restored owner worker-A, got %q",
			lockValue.OwnerID,
		)
	}
}

func TestStoreSnapshotRestorePreservesFencingAfterExpiration(t *testing.T) {
	store := NewStore()

	_, acquired, err := store.AcquireLock(
		"job:1",
		"worker-A",
		1000,
		42,
	)
	if err != nil {
		t.Fatalf("first AcquireLock() returned error: %v", err)
	}

	if !acquired {
		t.Fatal("expected first lock acquisition")
	}

	if err := store.FencedPut(
		"job:1",
		[]byte("worker-a-result"),
		1,
	); err != nil {
		t.Fatalf("first FencedPut() returned error: %v", err)
	}

	_, expired, err := store.ExpireLock("job:1", 1)
	if err != nil {
		t.Fatalf("ExpireLock() returned error: %v", err)
	}

	if !expired {
		t.Fatal("expected first lock to expire")
	}

	_, acquired, err = store.AcquireLock(
		"job:1",
		"worker-B",
		2000,
		43,
	)
	if err != nil {
		t.Fatalf("second AcquireLock() returned error: %v", err)
	}

	if !acquired {
		t.Fatal("expected second lock acquisition")
	}

	snapshot, err := store.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() returned error: %v", err)
	}

	restored := NewStore()

	if err := restored.Restore(snapshot); err != nil {
		t.Fatalf("Restore() returned error: %v", err)
	}

	// The old resource value survives the snapshot.
	value, ok := restored.GetFenced("job:1")
	if !ok {
		t.Fatal("expected fenced resource to survive restore")
	}

	if string(value.Value) != "worker-a-result" {
		t.Fatalf(
			"expected old resource value, got %q",
			string(value.Value),
		)
	}

	// But the active lock belongs to worker-B and has token 2.
	currentLock, ok := restored.GetLock("job:1")
	if !ok {
		t.Fatal("expected worker-B lock to survive restore")
	}

	if currentLock.OwnerID != "worker-B" {
		t.Fatalf(
			"expected worker-B ownership, got %q",
			currentLock.OwnerID,
		)
	}

	if currentLock.FencingToken != 2 {
		t.Fatalf(
			"expected current fencing token 2, got %d",
			currentLock.FencingToken,
		)
	}

	// Zombie worker-A must still be rejected after recovery.
	err = restored.FencedPut(
		"job:1",
		[]byte("zombie-worker-a-result"),
		1,
	)
	if !errors.Is(err, lock.ErrStaleFencingToken) {
		t.Fatalf(
			"expected stale token rejection, got %v",
			err,
		)
	}

	// Current worker-B must still be allowed.
	if err := restored.FencedPut(
		"job:1",
		[]byte("worker-b-result"),
		2,
	); err != nil {
		t.Fatalf("worker-B FencedPut() returned error: %v", err)
	}

	value, ok = restored.GetFenced("job:1")
	if !ok {
		t.Fatal("expected fenced resource after worker-B write")
	}

	if string(value.Value) != "worker-b-result" {
		t.Fatalf(
			"expected worker-B result, got %q",
			string(value.Value),
		)
	}

	if value.FencingToken != 2 {
		t.Fatalf(
			"expected resource token 2, got %d",
			value.FencingToken,
		)
	}
}
