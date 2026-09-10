package model

type JobID string

type JobState string

const (
	JobPending   JobState = "PENDING"
	JobScheduled JobState = "SCHEDULED"
	JobRunning   JobState = "RUNNING"
	JobSucceeded JobState = "SUCCEEDED"
	JobFailed    JobState = "FAILED"
)

type Job struct {
	ID JobID

	Payload []byte

	State JobState

	// ScheduledAt is the Unix timestamp in nanoseconds at which the job
	// becomes eligible for scheduling.
	ScheduledAt int64

	AssignedWorkerID string
	FencingToken     uint64

	Attempt uint32

	CreatedIndex LogIndex
}
