package kv

import (
	"errors"
	"testing"
	"time"

	"github.com/sanchar127/raftiq/internal/lock"
)

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

func TestStoreFencedPutRejectsStaleToken(t *testing.T) {
	store := NewStore()

	_, acquired, err := store.AcquireLock(
		"job-1",
		"worker-a",
		time.Now().Add(time.Minute).UnixNano(),
		1,
	)
	if err != nil {
		t.Fatalf("acquire lock: %v", err)
	}

	if !acquired {
		t.Fatal("expected lock acquisition")
	}

	err = store.FencedPut(
		"job-1",
		[]byte("worker-a"),
		1,
	)
	if err != nil {
		t.Fatalf("fenced put with valid token: %v", err)
	}

	_, expired, err := store.ExpireLock("job-1", 1)
	if err != nil {
		t.Fatalf("expire lock: %v", err)
	}

	if !expired {
		t.Fatal("expected lock expiration")
	}

	_, acquired, err = store.AcquireLock(
		"job-1",
		"worker-b",
		time.Now().Add(time.Minute).UnixNano(),
		2,
	)
	if err != nil {
		t.Fatalf("reacquire lock: %v", err)
	}

	if !acquired {
		t.Fatal("expected second lock acquisition")
	}

	err = store.FencedPut(
		"job-1",
		[]byte("zombie-worker-a"),
		1,
	)
	if !errors.Is(err, lock.ErrStaleFencingToken) {
		t.Fatalf(
			"expected stale fencing token error, got %v",
			err,
		)
	}

	current, ok := store.GetFenced("job-1")
	if !ok {
		t.Fatal("expected existing fenced value")
	}

	if string(current.Value) != "worker-a" {
		t.Fatalf(
			"expected original value to remain, got %q",
			current.Value,
		)
	}

	if current.FencingToken != 1 {
		t.Fatalf(
			"expected fencing token 1, got %d",
			current.FencingToken,
		)
	}

	err = store.FencedPut(
		"job-1",
		[]byte("worker-b"),
		2,
	)
	if err != nil {
		t.Fatalf("fenced put with current token: %v", err)
	}

	current, ok = store.GetFenced("job-1")
	if !ok {
		t.Fatal("expected updated fenced value")
	}

	if string(current.Value) != "worker-b" {
		t.Fatalf(
			"expected worker-b value, got %q",
			current.Value,
		)
	}

	if current.FencingToken != 2 {
		t.Fatalf(
			"expected fencing token 2, got %d",
			current.FencingToken,
		)
	}
}
