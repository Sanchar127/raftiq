package main

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/sanchar127/raftiq/internal/kv"
	"github.com/sanchar127/raftiq/internal/raft"
	"github.com/sanchar127/raftiq/internal/scheduler"
)

func TestRuntimeStartRejectsNilContext(t *testing.T) {
	t.Parallel()

	r := &runtime{}

	if err := r.Start(nil); err == nil {
		t.Fatal("expected Start(nil) to return an error")
	}
}

func TestRuntimeShutdownIsIdempotent(t *testing.T) {
	t.Parallel()

	r := &runtime{}

	r.Shutdown()
	r.Shutdown()
	r.Shutdown()
}

func TestRuntimeShutdownWaitsForBackgroundServices(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node := raft.NewRaftNode("runtime-test")
	store := kv.NewStore()
	applier := kv.NewApplier(store)

	selector, err := scheduler.NewHashWorkerSelector(
		[]string{"worker-1"},
	)
	if err != nil {
		t.Fatalf("NewHashWorkerSelector() error = %v", err)
	}

	jobScheduler, err := scheduler.New(
		node,
		store,
		applier,
		selector,
		scheduler.Config{
			Interval: time.Hour,
			Lease:    time.Minute,
		},
	)
	if err != nil {
		t.Fatalf("scheduler.New() error = %v", err)
	}

	jobScheduler.SetLogger(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)

	r := &runtime{
		logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		executionCtx:    ctx,
		executionCancel: cancel,
		jobScheduler:    jobScheduler,
	}

	r.startScheduler()

	shutdownDone := make(chan struct{})

	go func() {
		r.Shutdown()
		close(shutdownDone)
	}()

	select {
	case <-shutdownDone:
	case <-time.After(2 * time.Second):
		t.Fatal("runtime Shutdown() did not return")
	}
}

func TestRuntimeShutdownWaitsForTrackedGoroutine(t *testing.T) {
	t.Parallel()

	r := &runtime{}

	started := make(chan struct{})
	released := make(chan struct{})

	r.backgroundWG.Add(1)

	go func() {
		defer r.backgroundWG.Done()

		close(started)
		<-released
	}()

	<-started

	shutdownDone := make(chan struct{})

	go func() {
		r.Shutdown()
		close(shutdownDone)
	}()

	select {
	case <-shutdownDone:
		t.Fatal("Shutdown() returned before background goroutine exited")
	case <-time.After(50 * time.Millisecond):
	}

	close(released)

	select {
	case <-shutdownDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown() did not return after background goroutine exited")
	}
}

func TestRuntimeShutdownWaitGroupSupportsMultipleServices(t *testing.T) {
	t.Parallel()

	r := &runtime{}

	var wg sync.WaitGroup
	wg.Add(2)

	r.backgroundWG.Add(2)

	go func() {
		defer wg.Done()
		defer r.backgroundWG.Done()

		time.Sleep(50 * time.Millisecond)
	}()

	go func() {
		defer wg.Done()
		defer r.backgroundWG.Done()

		time.Sleep(100 * time.Millisecond)
	}()

	done := make(chan struct{})

	go func() {
		r.Shutdown()
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("Shutdown() returned before tracked services completed")
	case <-time.After(25 * time.Millisecond):
	}

	wg.Wait()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown() did not return after tracked services completed")
	}
}
