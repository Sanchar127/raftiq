package worker

import (
	"context"
	"errors"
	"testing"

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
