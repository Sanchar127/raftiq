package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sanchar127/raftiq/internal/model"
)

func TestNewRejectsInvalidWorker(t *testing.T) {
	t.Parallel()

	handler := HandlerFunc(func(
		context.Context,
		model.Job,
	) error {
		return nil
	})

	tests := []struct {
		name    string
		id      string
		handler JobHandler
	}{
		{
			name:    "missing worker ID",
			handler: handler,
		},
		{
			name: "missing handler",
			id:   "worker-1",
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := New(tt.id, tt.handler)
			if err == nil {
				t.Fatal("expected error")
			}

			if !errors.Is(err, ErrInvalidWorker) {
				t.Fatalf(
					"expected ErrInvalidWorker, got %v",
					err,
				)
			}
		})
	}
}

func TestNewCreatesWorker(t *testing.T) {
	t.Parallel()

	handler := HandlerFunc(func(
		context.Context,
		model.Job,
	) error {
		return nil
	})

	worker, err := New("worker-1", handler)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if worker.ID() != "worker-1" {
		t.Fatalf(
			"expected worker ID %q, got %q",
			"worker-1",
			worker.ID(),
		)
	}
}

func TestWorkerExecuteCallsHandler(t *testing.T) {
	t.Parallel()

	called := false

	handler := HandlerFunc(func(
		ctx context.Context,
		job model.Job,
	) error {
		called = true

		if job.ID != "job-1" {
			t.Fatalf("unexpected job ID %q", job.ID)
		}

		if string(job.Payload) != "hello" {
			t.Fatalf(
				"unexpected payload %q",
				job.Payload,
			)
		}

		return nil
	})

	worker, err := New("worker-1", handler)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	job := model.Job{
		ID:      "job-1",
		Payload: []byte("hello"),
		State:   model.JobScheduled,
	}

	if err := worker.Execute(
		context.Background(),
		job,
	); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	if !called {
		t.Fatal("expected handler to be called")
	}
}

func TestWorkerExecutePropagatesHandlerError(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("job failed")

	handler := HandlerFunc(func(
		context.Context,
		model.Job,
	) error {
		return expectedErr
	})

	worker, err := New("worker-1", handler)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	job := model.Job{
		ID:    "job-1",
		State: model.JobScheduled,
	}

	err = worker.Execute(context.Background(), job)
	if err == nil {
		t.Fatal("expected execution error")
	}

	if !errors.Is(err, expectedErr) {
		t.Fatalf(
			"expected wrapped handler error %v, got %v",
			expectedErr,
			err,
		)
	}
}

func TestWorkerExecuteRejectsInvalidJob(t *testing.T) {
	t.Parallel()

	handler := HandlerFunc(func(
		context.Context,
		model.Job,
	) error {
		return nil
	})

	worker, err := New("worker-1", handler)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	err = worker.Execute(
		context.Background(),
		model.Job{},
	)
	if err == nil {
		t.Fatal("expected invalid job error")
	}

	if !errors.Is(err, ErrInvalidJob) {
		t.Fatalf(
			"expected ErrInvalidJob, got %v",
			err,
		)
	}
}

func TestWorkerExecuteRejectsCancelledContext(t *testing.T) {
	t.Parallel()

	called := false

	handler := HandlerFunc(func(
		context.Context,
		model.Job,
	) error {
		called = true
		return nil
	})

	worker, err := New("worker-1", handler)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	job := model.Job{
		ID:    "job-1",
		State: model.JobScheduled,
	}

	err = worker.Execute(ctx, job)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf(
			"expected context.Canceled, got %v",
			err,
		)
	}

	if called {
		t.Fatal("handler must not run with cancelled context")
	}
}

type fakeJobSource struct {
	jobs []model.Job
}

func (s *fakeJobSource) GetJob(id model.JobID) (model.Job, bool) {
	for _, job := range s.jobs {
		if job.ID == id {
			return job, true
		}
	}
	return model.Job{}, false
}

func (s *fakeJobSource) ListPendingJobs() []model.Job {
	pending := make([]model.Job, 0)
	for _, job := range s.jobs {
		if job.AssignedWorkerID == "" {
			pending = append(pending, job)
		}
	}
	return pending
}

func (s *fakeJobSource) ListAssignedJobs(
	workerID string,
) []model.Job {
	jobs := make([]model.Job, 0)

	for _, job := range s.jobs {
		if job.AssignedWorkerID == workerID {
			jobs = append(jobs, job)
		}
	}

	return jobs
}

func TestWorkerRunExecutesAssignedJobs(t *testing.T) {
	t.Parallel()

	executed := make(chan model.JobID, 1)

	handler := HandlerFunc(func(
		ctx context.Context,
		job model.Job,
	) error {
		executed <- job.ID
		return nil
	})

	worker, err := New("worker-1", handler)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	source := &fakeJobSource{
		jobs: []model.Job{
			{
				ID:               "job-1",
				State:            model.JobScheduled,
				AssignedWorkerID: "worker-1",
			},
			{
				ID:               "job-2",
				State:            model.JobScheduled,
				AssignedWorkerID: "worker-2",
			},
		},
	}

	if err := worker.ConfigureLoop(
		source,
		Config{Interval: 10 * time.Millisecond},
	); err != nil {
		t.Fatalf("ConfigureLoop() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = worker.Run(ctx)
	}()

	select {
	case jobID := <-executed:
		if jobID != "job-1" {
			t.Fatalf("expected job-1, got %q", jobID)
		}

	case <-time.After(time.Second):
		t.Fatal("worker did not execute assigned job")
	}
}

func TestWorkerRunStopsOnCancellation(t *testing.T) {
	t.Parallel()

	handler := HandlerFunc(func(
		context.Context,
		model.Job,
	) error {
		return nil
	})

	worker, err := New("worker-1", handler)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	source := &fakeJobSource{}

	if err := worker.ConfigureLoop(
		source,
		Config{Interval: 10 * time.Millisecond},
	); err != nil {
		t.Fatalf("ConfigureLoop() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)

	go func() {
		done <- worker.Run(ctx)
	}()

	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf(
				"expected context.Canceled, got %v",
				err,
			)
		}

	case <-time.After(time.Second):
		t.Fatal("worker did not stop after cancellation")
	}
}
