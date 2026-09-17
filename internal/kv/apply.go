package kv

import (
	"errors"
	"fmt"

	"github.com/sanchar127/raftiq/internal/lock"
	"github.com/sanchar127/raftiq/internal/model"
	"github.com/sanchar127/raftiq/internal/raft"
)

const (
	kvOperationDecode  = "decode"
	kvOperationUnknown = "unknown"
)

// Apply applies a Raft log entry to the KV state machine without metrics.
func Apply(store *Store, entry raft.LogEntry) ApplyResult {
	return ApplyWithMetrics(store, entry, NoopKVMetrics{})
}

// ApplyWithMetrics applies a Raft log entry and records operation metrics.
func ApplyWithMetrics(
	store *Store,
	entry raft.LogEntry,
	metrics KVMetrics,
) (result ApplyResult) {
	if metrics == nil {
		metrics = NoopKVMetrics{}
	}

	command, err := DecodeCommand(entry.Data)
	if err != nil {
		metrics.IncOperation(kvOperationDecode)
		metrics.IncOperationError(kvOperationDecode)

		return ApplyResult{
			Err: fmt.Errorf(
				"decode raft command at index %d: %w",
				entry.Index,
				err,
			),
		}
	}

	operation := string(command.Type)
	metrics.IncOperation(operation)

	defer func() {
		if result.Err != nil {
			metrics.IncOperationError(operation)
		}
	}()

	return applyCommand(store, command, entry)
}

func applyCommand(
	store *Store,
	command Command,
	entry raft.LogEntry,
) ApplyResult {
	switch command.Type {
	case CommandPut, CommandDelete, CommandReadBarrier:
		return applyKVCommand(store, command)

	case CommandLockAcquire, CommandLockExpire, CommandFencedPut:
		return applyLockCommand(store, command, entry)

	case CommandCreateJob, CommandClaimJob:
		return applyJobCreationCommand(store, command, entry)

	case CommandJobStart, CommandJobSucceeded, CommandJobFailed:
		return applyJobTransitionCommand(store, command)

	case CommandJobReclaim:
		return applyJobReclaimCommand(store, command)

	default:
		return ApplyResult{
			Err: fmt.Errorf(
				"unknown command type %q",
				command.Type,
			),
		}
	}
}

func applyKVCommand(store *Store, command Command) ApplyResult {
	switch command.Type {
	case CommandPut:
		store.Put(command.Key, command.Value)

	case CommandDelete:
		store.Delete(command.Key)

	case CommandReadBarrier:
		return ApplyResult{}
	}

	return ApplyResult{}
}

func applyLockCommand(
	store *Store,
	command Command,
	entry raft.LogEntry,
) ApplyResult {
	switch command.Type {
	case CommandLockAcquire:
		_, _, err := store.AcquireLock(
			command.Key,
			command.OwnerID,
			command.ExpiresAt,
			entry.Index,
		)
		if err != nil {
			return ApplyResult{
				Err: fmt.Errorf(
					"acquire lock %q: %w",
					command.Key,
					err,
				),
			}
		}

	case CommandLockExpire:
		_, _, err := store.ExpireLock(
			command.Key,
			command.FencingToken,
		)
		if err != nil {
			return ApplyResult{
				Err: fmt.Errorf(
					"expire lock %q: %w",
					command.Key,
					err,
				),
			}
		}

	case CommandFencedPut:
		return applyFencedPut(store, command)
	}

	return ApplyResult{}
}

func applyFencedPut(store *Store, command Command) ApplyResult {
	err := store.FencedPut(
		command.Key,
		command.Value,
		command.FencingToken,
	)
	if err == nil {
		return ApplyResult{}
	}

	if errors.Is(err, lock.ErrStaleFencingToken) ||
		errors.Is(err, lock.ErrLockNotFound) {
		return ApplyResult{Err: err}
	}

	return ApplyResult{
		Err: fmt.Errorf(
			"fenced put %q: %w",
			command.Key,
			err,
		),
	}
}

func applyJobCreationCommand(
	store *Store,
	command Command,
	entry raft.LogEntry,
) ApplyResult {
	if command.Type == CommandClaimJob {
		return applyClaimJob(store, command, entry)
	}

	job := model.Job{
		ID:           model.JobID(command.JobID),
		Payload:      append([]byte(nil), command.Payload...),
		State:        model.JobPending,
		ScheduledAt:  command.ScheduledAt,
		CreatedIndex: entry.Index,
	}

	if err := store.CreateJob(job); err != nil {
		return ApplyResult{
			Job: &job,
			Err: fmt.Errorf(
				"create job %q: %w",
				command.JobID,
				err,
			),
		}
	}

	return ApplyResult{
		Job: &job,
	}
}

func applyClaimJob(
	store *Store,
	command Command,
	entry raft.LogEntry,
) ApplyResult {
	job, err := store.ClaimJob(
		model.JobID(command.JobID),
		command.OwnerID,
		command.ExpiresAt,
		entry.Index,
	)
	if err != nil {
		return ApplyResult{
			Err: fmt.Errorf(
				"claim job %q: %w",
				command.JobID,
				err,
			),
		}
	}

	return ApplyResult{
		Job: &job,
	}
}

func applyJobTransitionCommand(
	store *Store,
	command Command,
) ApplyResult {
	fromState, toState := jobTransitionStates(command.Type)

	job, err := store.TransitionJobState(
		model.JobID(command.JobID),
		command.OwnerID,
		command.ExecutionID,
		command.FencingToken,
		fromState,
		toState,
		command.At,
	)

	return ApplyResult{
		Job: &job,
		Err: err,
	}
}

func jobTransitionStates(
	commandType CommandType,
) (model.JobState, model.JobState) {
	switch commandType {
	case CommandJobStart:
		return model.JobScheduled, model.JobRunning

	case CommandJobSucceeded:
		return model.JobRunning, model.JobSucceeded

	case CommandJobFailed:
		return model.JobRunning, model.JobFailed

	default:
		panic(fmt.Sprintf(
			"invalid job transition command %q",
			commandType,
		))
	}
}

func applyJobReclaimCommand(
	store *Store,
	command Command,
) ApplyResult {
	job, err := store.ReclaimExpiredJob(
		model.JobID(command.JobID),
		command.FencingToken,
		command.At,
	)

	return ApplyResult{
		Job: &job,
		Err: err,
	}
}
