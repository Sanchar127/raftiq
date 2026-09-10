package kv

import (
	"errors"
	"testing"

	"github.com/sanchar127/raftiq/internal/model"
)

func TestStoreCreateAndGetJob(t *testing.T) {
	store := NewStore()

	job := model.Job{
		ID:               "job-1",
		Payload:          []byte("send-email"),
		State:            model.JobPending,
		ScheduledAt:      100,
		AssignedWorkerID: "",
		FencingToken:     0,
		Attempt:          0,
		CreatedIndex:     10,
	}

	if err := store.CreateJob(job); err != nil {
		t.Fatalf("CreateJob() returned error: %v", err)
	}

	got, ok := store.GetJob(job.ID)
	if !ok {
		t.Fatal("expected job to exist")
	}

	if got.ID != job.ID {
		t.Fatalf("expected job ID %q, got %q", job.ID, got.ID)
	}

	if string(got.Payload) != string(job.Payload) {
		t.Fatalf(
			"expected payload %q, got %q",
			string(job.Payload),
			string(got.Payload),
		)
	}

	if got.State != model.JobPending {
		t.Fatalf(
			"expected state %q, got %q",
			model.JobPending,
			got.State,
		)
	}

	if got.CreatedIndex != 10 {
		t.Fatalf(
			"expected created index 10, got %d",
			got.CreatedIndex,
		)
	}
}

func TestStoreCreateJobRejectsDuplicate(t *testing.T) {
	store := NewStore()

	job := model.Job{
		ID:      "job-1",
		Payload: []byte("task"),
		State:   model.JobPending,
	}

	if err := store.CreateJob(job); err != nil {
		t.Fatalf("first CreateJob() returned error: %v", err)
	}

	if err := store.CreateJob(job); err == nil {
		t.Fatal("expected duplicate job creation to fail")
	}
}

func TestStoreCreateJobRejectsInvalidJob(t *testing.T) {
	store := NewStore()

	err := store.CreateJob(model.Job{
		Payload: []byte("task"),
		State:   model.JobPending,
	})

	if !errors.Is(err, ErrInvalidJob) {
		t.Fatalf(
			"expected ErrInvalidJob, got %v",
			err,
		)
	}
}

func TestStoreUpdateJob(t *testing.T) {
	store := NewStore()

	job := model.Job{
		ID:      "job-1",
		Payload: []byte("task"),
		State:   model.JobPending,
		Attempt: 0,
	}

	if err := store.CreateJob(job); err != nil {
		t.Fatalf("CreateJob() returned error: %v", err)
	}

	job.State = model.JobRunning
	job.AssignedWorkerID = "worker-1"
	job.FencingToken = 42
	job.Attempt = 1

	if err := store.UpdateJob(job); err != nil {
		t.Fatalf("UpdateJob() returned error: %v", err)
	}

	got, ok := store.GetJob(job.ID)
	if !ok {
		t.Fatal("expected job to exist")
	}

	if got.State != model.JobRunning {
		t.Fatalf(
			"expected state %q, got %q",
			model.JobRunning,
			got.State,
		)
	}

	if got.AssignedWorkerID != "worker-1" {
		t.Fatalf(
			"expected worker-1, got %q",
			got.AssignedWorkerID,
		)
	}

	if got.FencingToken != 42 {
		t.Fatalf(
			"expected fencing token 42, got %d",
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

func TestStoreUpdateMissingJob(t *testing.T) {
	store := NewStore()

	err := store.UpdateJob(model.Job{
		ID:    "missing",
		State: model.JobRunning,
	})

	if !errors.Is(err, ErrJobNotFound) {
		t.Fatalf(
			"expected ErrJobNotFound, got %v",
			err,
		)
	}
}

func TestStoreDeleteJob(t *testing.T) {
	store := NewStore()

	job := model.Job{
		ID:      "job-1",
		Payload: []byte("task"),
		State:   model.JobPending,
	}

	if err := store.CreateJob(job); err != nil {
		t.Fatalf("CreateJob() returned error: %v", err)
	}

	if !store.DeleteJob(job.ID) {
		t.Fatal("expected DeleteJob() to return true")
	}

	if _, ok := store.GetJob(job.ID); ok {
		t.Fatal("expected deleted job to be absent")
	}

	if store.DeleteJob(job.ID) {
		t.Fatal("expected second DeleteJob() to return false")
	}
}

func TestStoreJobPayloadIsDefensivelyCopied(t *testing.T) {
	store := NewStore()

	payload := []byte("original")

	job := model.Job{
		ID:      "job-1",
		Payload: payload,
		State:   model.JobPending,
	}

	if err := store.CreateJob(job); err != nil {
		t.Fatalf("CreateJob() returned error: %v", err)
	}

	payload[0] = 'X'

	got, ok := store.GetJob(job.ID)
	if !ok {
		t.Fatal("expected job to exist")
	}

	if string(got.Payload) != "original" {
		t.Fatalf(
			"store payload was mutated through caller buffer: %q",
			string(got.Payload),
		)
	}

	got.Payload[0] = 'Y'

	again, ok := store.GetJob(job.ID)
	if !ok {
		t.Fatal("expected job to exist")
	}

	if string(again.Payload) != "original" {
		t.Fatalf(
			"store payload was mutated through returned buffer: %q",
			string(again.Payload),
		)
	}
}

func TestStoreListPendingJobsReturnsDeterministicOrder(t *testing.T) {
	store := NewStore()

	jobs := []model.Job{
		{
			ID:          "job-c",
			State:       model.JobPending,
			ScheduledAt: 200,
		},
		{
			ID:          "job-b",
			State:       model.JobPending,
			ScheduledAt: 100,
		},
		{
			ID:          "job-a",
			State:       model.JobPending,
			ScheduledAt: 100,
		},
		{
			ID:          "job-d",
			State:       model.JobScheduled,
			ScheduledAt: 50,
		},
	}

	for _, job := range jobs {
		if err := store.CreateJob(job); err != nil {
			t.Fatalf("CreateJob(%q) error = %v", job.ID, err)
		}
	}

	got := store.ListPendingJobs()

	if len(got) != 3 {
		t.Fatalf("ListPendingJobs() returned %d jobs, want 3", len(got))
	}

	wantIDs := []model.JobID{
		"job-a",
		"job-b",
		"job-c",
	}

	for i, wantID := range wantIDs {
		if got[i].ID != wantID {
			t.Fatalf(
				"job[%d].ID = %q, want %q",
				i,
				got[i].ID,
				wantID,
			)
		}
	}
}

func TestStoreListPendingJobsReturnsIndependentPayloads(t *testing.T) {
	store := NewStore()

	payload := []byte("original")

	err := store.CreateJob(model.Job{
		ID:      "job-1",
		State:   model.JobPending,
		Payload: payload,
	})
	if err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}

	got := store.ListPendingJobs()

	if len(got) != 1 {
		t.Fatalf("ListPendingJobs() returned %d jobs, want 1", len(got))
	}

	got[0].Payload[0] = 'X'

	stored, ok := store.GetJob("job-1")
	if !ok {
		t.Fatal("GetJob() did not find job")
	}

	if string(stored.Payload) != "original" {
		t.Fatalf(
			"stored payload changed through ListPendingJobs(): %q",
			stored.Payload,
		)
	}
}
