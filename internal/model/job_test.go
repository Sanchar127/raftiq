package model

import (
	"bytes"
	"testing"
)

func TestJob(t *testing.T) {
	payload := []byte(`{"task":"send-email"}`)

	job := Job{
		ID:               JobID("job-1"),
		Payload:          append([]byte(nil), payload...),
		State:            JobPending,
		ScheduledAt:      1_757_500_000,
		AssignedWorkerID: "worker-1",
		FencingToken:     42,
		Attempt:          1,
		CreatedIndex:     10,
	}

	if job.ID != "job-1" {
		t.Fatalf("expected job ID job-1, got %q", job.ID)
	}

	if !bytes.Equal(job.Payload, payload) {
		t.Fatalf("job payload does not match")
	}

	if job.State != JobPending {
		t.Fatalf(
			"expected job state %q, got %q",
			JobPending,
			job.State,
		)
	}

	if job.ScheduledAt != 1_757_500_000 {
		t.Fatalf(
			"expected scheduled timestamp 1757500000, got %d",
			job.ScheduledAt,
		)
	}

	if job.AssignedWorkerID != "worker-1" {
		t.Fatalf(
			"expected worker ID worker-1, got %q",
			job.AssignedWorkerID,
		)
	}

	if job.FencingToken != 42 {
		t.Fatalf(
			"expected fencing token 42, got %d",
			job.FencingToken,
		)
	}

	if job.Attempt != 1 {
		t.Fatalf(
			"expected attempt 1, got %d",
			job.Attempt,
		)
	}

	if job.CreatedIndex != 10 {
		t.Fatalf(
			"expected created index 10, got %d",
			job.CreatedIndex,
		)
	}
}

func TestJobStates(t *testing.T) {
	tests := []struct {
		name  string
		state JobState
		want  string
	}{
		{
			name:  "pending",
			state: JobPending,
			want:  "PENDING",
		},
		{
			name:  "scheduled",
			state: JobScheduled,
			want:  "SCHEDULED",
		},
		{
			name:  "running",
			state: JobRunning,
			want:  "RUNNING",
		},
		{
			name:  "succeeded",
			state: JobSucceeded,
			want:  "SUCCEEDED",
		},
		{
			name:  "failed",
			state: JobFailed,
			want:  "FAILED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if string(tt.state) != tt.want {
				t.Fatalf(
					"expected state %q, got %q",
					tt.want,
					tt.state,
				)
			}
		})
	}
}
