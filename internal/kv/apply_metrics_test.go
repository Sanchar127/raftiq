package kv

import (
	"errors"
	"testing"
	"time"

	"github.com/sanchar127/raftiq/internal/model"
	"github.com/sanchar127/raftiq/internal/raft"
)

type fakeKVMetrics struct {
	operations      map[string]int
	operationErrors map[string]int
}

func newFakeKVMetrics() *fakeKVMetrics {
	return &fakeKVMetrics{
		operations:      make(map[string]int),
		operationErrors: make(map[string]int),
	}
}

func (m *fakeKVMetrics) IncOperation(operation string) {
	m.operations[operation]++
}

func (m *fakeKVMetrics) IncOperationError(operation string) {
	m.operationErrors[operation]++
}

func TestApplyWithMetricsSuccessfulOperation(t *testing.T) {
	store := NewStore()
	metrics := newFakeKVMetrics()

	data, err := EncodeCommand(Command{
		Type:  CommandPut,
		Key:   "name",
		Value: []byte("raftiq"),
	})
	if err != nil {
		t.Fatalf("EncodeCommand() error = %v", err)
	}

	result := ApplyWithMetrics(
		store,
		raft.LogEntry{
			Index: 1,
			Data:  data,
		},
		metrics,
	)

	if result.Err != nil {
		t.Fatalf("ApplyWithMetrics() error = %v", result.Err)
	}

	if got := metrics.operations[string(CommandPut)]; got != 1 {
		t.Fatalf("PUT operation count = %d, want 1", got)
	}

	if got := metrics.operationErrors[string(CommandPut)]; got != 0 {
		t.Fatalf("PUT error count = %d, want 0", got)
	}
}

func TestApplyWithMetricsOperationError(t *testing.T) {
	store := NewStore()
	metrics := newFakeKVMetrics()

	data, err := EncodeCommand(Command{
		Type:      CommandClaimJob,
		JobID:     "missing-job",
		OwnerID:   "worker-1",
		ExpiresAt: time.Now().Add(time.Minute).UnixNano(),
	})
	if err != nil {
		t.Fatalf("EncodeCommand() error = %v", err)
	}

	result := ApplyWithMetrics(
		store,
		raft.LogEntry{
			Index: 1,
			Data:  data,
		},
		metrics,
	)

	if !errors.Is(result.Err, ErrJobNotFound) {
		t.Fatalf(
			"error = %v, want ErrJobNotFound",
			result.Err,
		)
	}

	if got := metrics.operations[string(CommandClaimJob)]; got != 1 {
		t.Fatalf(
			"CLAIM_JOB operation count = %d, want 1",
			got,
		)
	}

	if got := metrics.operationErrors[string(CommandClaimJob)]; got != 1 {
		t.Fatalf(
			"CLAIM_JOB error count = %d, want 1",
			got,
		)
	}
}

func TestApplyWithMetricsDecodeError(t *testing.T) {
	store := NewStore()
	metrics := newFakeKVMetrics()

	result := ApplyWithMetrics(
		store,
		raft.LogEntry{
			Index: 1,
			Data:  []byte("{invalid-json"),
		},
		metrics,
	)

	if result.Err == nil {
		t.Fatal("expected decode error")
	}

	if got := metrics.operations[KVOperationDecode]; got != 1 {
		t.Fatalf(
			"decode operation count = %d, want 1",
			got,
		)
	}

	if got := metrics.operationErrors[KVOperationDecode]; got != 1 {
		t.Fatalf(
			"decode error count = %d, want 1",
			got,
		)
	}
}

func TestApplyWithMetricsUnknownCommand(t *testing.T) {
	store := NewStore()
	metrics := newFakeKVMetrics()

	data, err := EncodeCommand(Command{
		Type: CommandType("UNKNOWN"),
	})
	if err != nil {
		t.Fatalf("EncodeCommand() error = %v", err)
	}

	result := ApplyWithMetrics(
		store,
		raft.LogEntry{
			Index: 1,
			Data:  data,
		},
		metrics,
	)

	if result.Err == nil {
		t.Fatal("expected unknown command error")
	}

	if got := metrics.operations["UNKNOWN"]; got != 1 {
		t.Fatalf(
			"UNKNOWN operation count = %d, want 1",
			got,
		)
	}

	if got := metrics.operationErrors["UNKNOWN"]; got != 1 {
		t.Fatalf(
			"UNKNOWN error count = %d, want 1",
			got,
		)
	}

	if _, ok := metrics.operations[KVOperationUnknown]; ok {
		t.Fatalf(
			"unexpected separate %q operation metric",
			KVOperationUnknown,
		)
	}
}

func TestApplyWithMetricsNilMetrics(t *testing.T) {
	store := NewStore()

	data, err := EncodeCommand(Command{
		Type:  CommandPut,
		Key:   "name",
		Value: []byte("raftiq"),
	})
	if err != nil {
		t.Fatalf("EncodeCommand() error = %v", err)
	}

	result := ApplyWithMetrics(
		store,
		raft.LogEntry{
			Index: 1,
			Data:  data,
		},
		nil,
	)

	if result.Err != nil {
		t.Fatalf("ApplyWithMetrics() error = %v", result.Err)
	}
}

func TestApplyWithMetricsJobCommand(t *testing.T) {
	store := NewStore()
	metrics := newFakeKVMetrics()

	err := store.CreateJob(model.Job{
		ID:    "job-1",
		State: model.JobPending,
	})
	if err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}

	data, err := EncodeCommand(Command{
		Type:      CommandClaimJob,
		JobID:     "job-1",
		OwnerID:   "worker-1",
		ExpiresAt: time.Now().Add(time.Minute).UnixNano(),
	})
	if err != nil {
		t.Fatalf("EncodeCommand() error = %v", err)
	}

	result := ApplyWithMetrics(
		store,
		raft.LogEntry{
			Index: 42,
			Data:  data,
		},
		metrics,
	)

	if result.Err != nil {
		t.Fatalf("ApplyWithMetrics() error = %v", result.Err)
	}

	if result.Job == nil {
		t.Fatal("expected job result")
	}

	if got := metrics.operations[string(CommandClaimJob)]; got != 1 {
		t.Fatalf(
			"CLAIM_JOB operation count = %d, want 1",
			got,
		)
	}

	if got := metrics.operationErrors[string(CommandClaimJob)]; got != 0 {
		t.Fatalf(
			"CLAIM_JOB error count = %d, want 0",
			got,
		)
	}
}
