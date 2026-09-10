package kv

import (
	"errors"
	"fmt"

	"github.com/sanchar127/raftiq/internal/lock"
	"github.com/sanchar127/raftiq/internal/model"
	"github.com/sanchar127/raftiq/internal/raft"
)

func Apply(store *Store, entry raft.LogEntry) ApplyResult {
	command, err := DecodeCommand(entry.Data)
	if err != nil {
		return ApplyResult{
			Err: fmt.Errorf(
				"decode raft command at index %d: %w",
				entry.Index,
				err,
			),
		}
	}

	switch command.Type {
	case CommandPut:
		store.Put(command.Key, command.Value)

	case CommandDelete:
		store.Delete(command.Key)

	case CommandReadBarrier:

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
		err := store.FencedPut(
			command.Key,
			command.Value,
			command.FencingToken,
		)
		if err != nil {
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

	case CommandClaimJob:
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

	case CommandJobStart:
		job, err := store.TransitionJobState(
			model.JobID(command.JobID),
			command.OwnerID,
			command.FencingToken,
			model.JobScheduled,
			model.JobRunning,
			command.At,
		)

		return ApplyResult{
			Job: &job,
			Err: err,
		}

	case CommandJobSucceeded:
		job, err := store.TransitionJobState(
			model.JobID(command.JobID),
			command.OwnerID,
			command.FencingToken,
			model.JobRunning,
			model.JobSucceeded,
			command.At,
		)

		return ApplyResult{
			Job: &job,
			Err: err,
		}
	case CommandJobFailed:
		job, err := store.TransitionJobState(
			model.JobID(command.JobID),
			command.OwnerID,
			command.FencingToken,
			model.JobRunning,
			model.JobFailed,
			command.At,
		)

		return ApplyResult{
			Job: &job,
			Err: err,
		}

	case CommandJobReclaim:
		job, err := store.ReclaimExpiredJob(
			model.JobID(command.JobID),
			command.FencingToken,
			command.At,
		)

		return ApplyResult{
			Job: &job,
			Err: err,
		}
	default:
		return ApplyResult{
			Err: fmt.Errorf(
				"unknown command type %q",
				command.Type,
			),
		}
	}

	return ApplyResult{}
}
