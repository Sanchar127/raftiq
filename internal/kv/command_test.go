package kv

import (
	"testing"

	"github.com/sanchar127/raftiq/internal/model"
	"github.com/sanchar127/raftiq/internal/raft"
)

func TestCommandRoundTrip(t *testing.T) {
	original := Command{
		Type:  CommandPut,
		Key:   "name",
		Value: []byte("raftiq"),
	}

	data, err := EncodeCommand(original)
	if err != nil {
		t.Fatal(err)
	}

	decoded, err := DecodeCommand(data)
	if err != nil {
		t.Fatal(err)
	}

	if decoded.Type != original.Type {
		t.Fatalf("expected type %q, got %q", original.Type, decoded.Type)
	}

	if decoded.Key != original.Key {
		t.Fatalf("expected key %q, got %q", original.Key, decoded.Key)
	}

	if string(decoded.Value) != string(original.Value) {
		t.Fatalf("expected value %q, got %q", original.Value, decoded.Value)
	}
}

func TestApplyPut(t *testing.T) {
	store := NewStore()

	command := Command{
		Type:  CommandPut,
		Key:   "name",
		Value: []byte("raftiq"),
	}

	data, err := EncodeCommand(command)
	if err != nil {
		t.Fatal(err)
	}

	entry := raft.LogEntry{
		Index: 1,
		Term:  1,
		Data:  data,
	}

	res := Apply(store, entry)
	if res.Err != nil {
		t.Fatalf("apply failed: %v", res.Err)
	}

	value, ok := store.Get("name")
	if !ok {
		t.Fatal("expected key to exist")
	}

	if string(value) != "raftiq" {
		t.Fatalf("expected raftiq, got %q", value)
	}
}

func TestApplyDelete(t *testing.T) {
	store := NewStore()

	store.Put("name", []byte("raftiq"))

	command := Command{
		Type: CommandDelete,
		Key:  "name",
	}

	data, err := EncodeCommand(command)
	if err != nil {
		t.Fatal(err)
	}

	entry := raft.LogEntry{
		Index: 2,
		Term:  1,
		Data:  data,
	}

	res := Apply(store, entry)
	if res.Err != nil {
		t.Fatalf("apply failed: %v", res.Err)
	}

	if _, ok := store.Get("name"); ok {
		t.Fatal("expected key to be deleted")
	}
}

func TestEncodeDecodeLockExpireCommand(t *testing.T) {
	original := Command{
		Type:         CommandLockExpire,
		Key:          "job-1",
		FencingToken: 41,
	}

	data, err := EncodeCommand(original)
	if err != nil {
		t.Fatalf("encode command: %v", err)
	}

	decoded, err := DecodeCommand(data)
	if err != nil {
		t.Fatalf("decode command: %v", err)
	}

	if decoded.Type != CommandLockExpire {
		t.Fatalf(
			"expected command type %q, got %q",
			CommandLockExpire,
			decoded.Type,
		)
	}

	if decoded.Key != original.Key {
		t.Fatalf(
			"expected key %q, got %q",
			original.Key,
			decoded.Key,
		)
	}

	if decoded.FencingToken != original.FencingToken {
		t.Fatalf(
			"expected fencing token %d, got %d",
			original.FencingToken,
			decoded.FencingToken,
		)
	}
}
func TestEncodeDecodeClaimJobCommand(t *testing.T) {
	original := Command{
		Type:      CommandClaimJob,
		JobID:     "job-1",
		OwnerID:   "worker-1",
		ExpiresAt: 123456789,
	}

	data, err := EncodeCommand(original)
	if err != nil {
		t.Fatalf("encode command: %v", err)
	}

	decoded, err := DecodeCommand(data)
	if err != nil {
		t.Fatalf("decode command: %v", err)
	}

	if decoded.Type != CommandClaimJob {
		t.Fatalf(
			"expected command type %q, got %q",
			CommandClaimJob,
			decoded.Type,
		)
	}

	if decoded.JobID != original.JobID {
		t.Fatalf(
			"expected job ID %q, got %q",
			original.JobID,
			decoded.JobID,
		)
	}

	if decoded.OwnerID != original.OwnerID {
		t.Fatalf(
			"expected owner ID %q, got %q",
			original.OwnerID,
			decoded.OwnerID,
		)
	}

	if decoded.ExpiresAt != original.ExpiresAt {
		t.Fatalf(
			"expected expiration %d, got %d",
			original.ExpiresAt,
			decoded.ExpiresAt,
		)
	}
}

func TestApplyClaimJob(t *testing.T) {
	store := NewStore()

	job := model.Job{
		ID:      "job-1",
		Payload: []byte("send-email"),
		State:   model.JobPending,
	}

	if err := store.CreateJob(job); err != nil {
		t.Fatalf("CreateJob() returned error: %v", err)
	}

	command := Command{
		Type:      CommandClaimJob,
		JobID:     "job-1",
		OwnerID:   "worker-1",
		ExpiresAt: 9999,
	}

	data, err := EncodeCommand(command)
	if err != nil {
		t.Fatalf("encode command: %v", err)
	}

	entry := raft.LogEntry{
		Index: 42,
		Term:  3,
		Data:  data,
	}

	result := Apply(store, entry)
	if result.Err != nil {
		t.Fatalf("Apply() returned error: %v", result.Err)
	}

	got, ok := store.GetJob(job.ID)
	if !ok {
		t.Fatal("expected job to exist")
	}

	if got.State != model.JobScheduled {
		t.Fatalf(
			"expected state %q, got %q",
			model.JobScheduled,
			got.State,
		)
	}

	if got.AssignedWorkerID != "worker-1" {
		t.Fatalf(
			"expected worker-1, got %q",
			got.AssignedWorkerID,
		)
	}

	if got.FencingToken != 1 {
		t.Fatalf(
			"expected fencing token 1, got %d",
			got.FencingToken,
		)
	}

	if got.Attempt != 1 {
		t.Fatalf(
			"expected attempt 1, got %d",
			got.Attempt,
		)
	}
}

func TestApplyClaimJobIsIdempotentForSameWorker(t *testing.T) {
	store := NewStore()

	job := model.Job{
		ID:    "job-1",
		State: model.JobPending,
	}

	if err := store.CreateJob(job); err != nil {
		t.Fatalf("CreateJob() returned error: %v", err)
	}

	command := Command{
		Type:      CommandClaimJob,
		JobID:     "job-1",
		OwnerID:   "worker-1",
		ExpiresAt: 9999,
	}

	data, err := EncodeCommand(command)
	if err != nil {
		t.Fatalf("encode command: %v", err)
	}

	first := Apply(store, raft.LogEntry{
		Index: 1,
		Term:  1,
		Data:  data,
	})

	if first.Err != nil {
		t.Fatalf("first Apply() returned error: %v", first.Err)
	}

	second := Apply(store, raft.LogEntry{
		Index: 2,
		Term:  1,
		Data:  data,
	})

	if second.Err != nil {
		t.Fatalf("second Apply() returned error: %v", second.Err)
	}

	got, ok := store.GetJob(job.ID)
	if !ok {
		t.Fatal("expected job to exist")
	}

	if got.FencingToken != 1 {
		t.Fatalf(
			"expected fencing token to remain 1, got %d",
			got.FencingToken,
		)
	}

	if got.Attempt != 1 {
		t.Fatalf(
			"expected attempt to remain 1, got %d",
			got.Attempt,
		)
	}
}
