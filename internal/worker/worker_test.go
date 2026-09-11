package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sanchar127/raftiq/internal/kv"
	"github.com/sanchar127/raftiq/internal/model"
	"github.com/sanchar127/raftiq/internal/raft"
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
	mu sync.Mutex

	jobs           []model.Job
	ownershipValid bool

	ownershipChecked bool
}

func (s *fakeJobSource) ListAssignedJobs(
	workerID string,
) []model.Job {
	s.mu.Lock()
	defer s.mu.Unlock()

	jobs := make([]model.Job, 0, len(s.jobs))

	for _, job := range s.jobs {
		if job.AssignedWorkerID != workerID {
			continue
		}

		if job.State != model.JobScheduled {
			continue
		}

		job.Payload = append([]byte(nil), job.Payload...)
		jobs = append(jobs, job)
	}

	return jobs
}

func (s *fakeJobSource) ValidateJobOwnership(
	jobID model.JobID,
	workerID string,
	fencingToken uint64,
	now int64,
) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.ownershipChecked = true

	if !s.ownershipValid {
		return false
	}

	for _, job := range s.jobs {
		if job.ID != jobID {
			continue
		}

		return job.AssignedWorkerID == workerID &&
			job.FencingToken == fencingToken &&
			job.State == model.JobScheduled
	}

	return false
}

func (s *fakeJobSource) OwnershipWasChecked() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.ownershipChecked
}

func newTestRaftApplier(
	t *testing.T,
) (*raft.RaftNode, *kv.Store, *kv.Applier) {
	t.Helper()

	node := raft.NewRaftNode("node-1")
	store := kv.NewStore()
	applier := kv.NewApplier(store)

	if err := node.Start(); err != nil {
		t.Fatalf("Raft Start() error = %v", err)
	}

	t.Cleanup(func() {
		node.Stop()
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() {
		done <- applier.Run(ctx, node.ApplyCh())
	}()

	t.Cleanup(func() {
		cancel()

		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Errorf(
					"Applier.Run() error = %v, want context.Canceled",
					err,
				)
			}

		case <-time.After(time.Second):
			t.Error("Applier.Run() did not stop")
		}
	})

	deadline := time.Now().Add(5 * time.Second)

	for time.Now().Before(deadline) {
		if node.State().Role == raft.Leader {
			return node, store, applier
		}

		time.Sleep(5 * time.Millisecond)
	}

	t.Fatalf(
		"single-node Raft did not become leader; role=%v",
		node.State().Role,
	)

	return nil, nil, nil
}

func claimTestJob(
	t *testing.T,
	store *kv.Store,
	jobID model.JobID,
	workerID string,
) model.Job {
	t.Helper()

	err := store.CreateJob(model.Job{
		ID:      jobID,
		Payload: []byte("payload"),
		State:   model.JobPending,
	})
	if err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}

	job, err := store.ClaimJob(
		jobID,
		workerID,
		time.Now().Add(time.Minute).UnixNano(),
		1,
	)
	if err != nil {
		t.Fatalf("ClaimJob() error = %v", err)
	}

	return job
}

func TestWorkerRunExecutesAssignedJobs(t *testing.T) {
	node, store, applier := newTestRaftApplier(t)

	job := claimTestJob(
		t,
		store,
		"job-1",
		"worker-1",
	)

	executed := make(chan model.Job, 1)

	handler := HandlerFunc(func(
		ctx context.Context,
		job model.Job,
	) error {
		executed <- job
		return nil
	})

	worker, err := New("worker-1", handler)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := worker.ConfigureLoop(
		store,
		Config{
			Interval: 10 * time.Millisecond,
			Raft:     node,
			Applier:  applier,
		},
	); err != nil {
		t.Fatalf("ConfigureLoop() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = worker.Run(ctx)
	}()

	select {
	case executedJob := <-executed:
		if executedJob.ID != job.ID {
			t.Fatalf(
				"expected job ID %q, got %q",
				job.ID,
				executedJob.ID,
			)
		}

		if executedJob.State != model.JobRunning {
			t.Fatalf(
				"handler received state %q, want %q",
				executedJob.State,
				model.JobRunning,
			)
		}

	case <-time.After(time.Second):
		t.Fatal("worker did not execute assigned job")
	}

	waitForJobState(
		t,
		store,
		job.ID,
		model.JobSucceeded,
	)
}

func TestWorkerRunMarksFailedJob(t *testing.T) {
	node, store, applier := newTestRaftApplier(t)

	job := claimTestJob(
		t,
		store,
		"job-1",
		"worker-1",
	)

	handlerErr := errors.New("handler failed")

	handler := HandlerFunc(func(
		ctx context.Context,
		job model.Job,
	) error {
		return handlerErr
	})

	worker, err := New("worker-1", handler)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := worker.ConfigureLoop(
		store,
		Config{
			Interval: 10 * time.Millisecond,
			Raft:     node,
			Applier:  applier,
		},
	); err != nil {
		t.Fatalf("ConfigureLoop() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = worker.Run(ctx)
	}()

	waitForJobState(
		t,
		store,
		job.ID,
		model.JobFailed,
	)
}

func TestWorkerRunStopsOnCancellation(t *testing.T) {
	t.Parallel()

	node := raft.NewRaftNode("node-1")
	store := &fakeJobSource{
		ownershipValid: true,
	}
	applier := kv.NewApplier(kv.NewStore())

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

	if err := worker.ConfigureLoop(
		store,
		Config{
			Interval: 10 * time.Millisecond,
			Raft:     node,
			Applier:  applier,
		},
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

func TestWorkerRunSkipsJobWhenOwnershipIsLost(t *testing.T) {
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
		ownershipValid: false,
		jobs: []model.Job{
			{
				ID:               "job-1",
				State:            model.JobScheduled,
				AssignedWorkerID: "worker-1",
				FencingToken:     11,
			},
		},
	}

	node := raft.NewRaftNode("node-1")
	applier := kv.NewApplier(kv.NewStore())

	if err := worker.ConfigureLoop(
		source,
		Config{
			Interval: 10 * time.Millisecond,
			Raft:     node,
			Applier:  applier,
		},
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
		t.Fatalf(
			"stale worker must not execute job %q",
			jobID,
		)

	case <-time.After(100 * time.Millisecond):
		// Expected: ownership validation prevented execution.
	}

	if !source.OwnershipWasChecked() {
		t.Fatal("expected worker to validate job ownership")
	}
}

func waitForJobState(
	t *testing.T,
	store *kv.Store,
	jobID model.JobID,
	expected model.JobState,
) {
	t.Helper()

	deadline := time.Now().Add(time.Second)

	for time.Now().Before(deadline) {
		job, ok := store.GetJob(jobID)
		if ok && job.State == expected {
			return
		}

		time.Sleep(5 * time.Millisecond)
	}

	job, ok := store.GetJob(jobID)
	if !ok {
		t.Fatalf(
			"job %q not found while waiting for state %q",
			jobID,
			expected,
		)
	}

	t.Fatalf(
		"job %q state = %q, want %q",
		jobID,
		job.State,
		expected,
	)
}


type recordingWorkerMetrics struct {
	mu sync.Mutex

	executedJobs         int
	executionFailures    int
	leaseLosses          int
}

func (m *recordingWorkerMetrics) IncExecutedJobs() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.executedJobs++
}

func (m *recordingWorkerMetrics) IncJobExecutionFailures() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.executionFailures++
}

func (m *recordingWorkerMetrics) IncLeaseLosses() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.leaseLosses++
}

func (m *recordingWorkerMetrics) Counts() (
	executedJobs int,
	executionFailures int,
	leaseLosses int,
) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.executedJobs, m.executionFailures, m.leaseLosses
}

func TestWorkerRunExecutesAssignedJobs(t *testing.T) {
	node, store, applier := newTestRaftApplier(t)

	job := claimTestJob(
		t,
		store,
		"job-1",
		"worker-1",
	)

	metrics := &recordingWorkerMetrics{}
	executed := make(chan model.Job, 1)

	handler := HandlerFunc(func(
		ctx context.Context,
		job model.Job,
	) error {
		executed <- job
		return nil
	})

	worker, err := New("worker-1", handler)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := worker.ConfigureLoop(
		store,
		Config{
			Interval: 10 * time.Millisecond,
			Raft:     node,
			Applier:  applier,
			Metrics:  metrics,
		},
	); err != nil {
		t.Fatalf("ConfigureLoop() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = worker.Run(ctx)
	}()

	select {
	case executedJob := <-executed:
		if executedJob.ID != job.ID {
			t.Fatalf(
				"expected job ID %q, got %q",
				job.ID,
				executedJob.ID,
			)
		}

		if executedJob.State != model.JobRunning {
			t.Fatalf(
				"handler received state %q, want %q",
				executedJob.State,
				model.JobRunning,
			)
		}

	case <-time.After(time.Second):
		t.Fatal("worker did not execute assigned job")
	}

	waitForJobState(
		t,
		store,
		job.ID,
		model.JobSucceeded,
	)

	executedJobs, executionFailures, leaseLosses :=
		metrics.Counts()

	if executedJobs != 1 {
		t.Fatalf(
			"executed jobs = %d, want 1",
			executedJobs,
		)
	}

	if executionFailures != 0 {
		t.Fatalf(
			"execution failures = %d, want 0",
			executionFailures,
		)
	}

	if leaseLosses != 0 {
		t.Fatalf(
			"lease losses = %d, want 0",
			leaseLosses,
		)
	}
}