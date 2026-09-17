package kv

import (
	"encoding/json"
	"fmt"

	"github.com/sanchar127/raftiq/internal/lock"
	"github.com/sanchar127/raftiq/internal/model"
)

type snapshotState struct {
	Version   uint64                    `json:"version"`
	Data      map[string][]byte         `json:"data"`
	Locks     map[string]lock.Lock      `json:"locks"`
	NextToken uint64                    `json:"next_token"`
	Fenced    map[string]FencedValue    `json:"fenced"`
	Jobs      map[model.JobID]model.Job `json:"jobs"`
}

const snapshotVersion uint64 = 1

func (s *Store) Snapshot() ([]byte, error) {
	logger := s.getLogger()

	s.mu.RLock()
	defer s.mu.RUnlock()

	data := make(map[string][]byte, len(s.data))

	for key, value := range s.data {
		data[key] = append([]byte(nil), value...)
	}

	locks := make(map[string]lock.Lock, len(s.locks.Locks))

	for key, value := range s.locks.Locks {
		locks[key] = value
	}

	fenced := make(map[string]FencedValue, len(s.fenced))

	for key, value := range s.fenced {
		fenced[key] = FencedValue{
			Value:        append([]byte(nil), value.Value...),
			FencingToken: value.FencingToken,
		}
	}

	jobs := make(map[model.JobID]model.Job, len(s.jobs))

	for id, job := range s.jobs {
		jobs[id] = cloneJob(job)
	}

	snapshot := snapshotState{
		Version:   snapshotVersion,
		Data:      data,
		Locks:     locks,
		NextToken: s.locks.NextToken,
		Fenced:    fenced,
		Jobs:      jobs,
	}

	result, err := json.Marshal(snapshot)
	if err != nil {
		logger.Error(
			"KV snapshot serialization failed",
			"operation", "snapshot",
			"error", err,
		)

		return nil, fmt.Errorf("marshal KV snapshot: %w", err)
	}

	logger.Info(
		"KV snapshot created",
		"operation", "snapshot",
		"version", snapshotVersion,
		"data_entries", len(data),
		"locks", len(locks),
		"fenced_values", len(fenced),
		"jobs", len(jobs),
		"snapshot_bytes", len(result),
	)

	return result, nil
}

func validateSnapshotState(snapshot snapshotState) error {
	for key, currentLock := range snapshot.Locks {
		if key == "" {
			return fmt.Errorf(
				"snapshot contains lock with empty map key",
			)
		}

		if currentLock.Key == "" {
			return fmt.Errorf(
				"snapshot lock %q has empty key",
				key,
			)
		}

		if currentLock.Key != key {
			return fmt.Errorf(
				"snapshot lock map key %q does not match lock key %q",
				key,
				currentLock.Key,
			)
		}

		if currentLock.OwnerID == "" {
			return fmt.Errorf(
				"snapshot lock %q has empty owner",
				key,
			)
		}

		if currentLock.FencingToken == 0 {
			return fmt.Errorf(
				"snapshot lock %q has invalid fencing token 0",
				key,
			)
		}

		if currentLock.FencingToken > snapshot.NextToken {
			return fmt.Errorf(
				"snapshot lock %q fencing token %d exceeds next token %d",
				key,
				currentLock.FencingToken,
				snapshot.NextToken,
			)
		}
	}

	for id, job := range snapshot.Jobs {
		if id == "" {
			return fmt.Errorf(
				"snapshot contains job with empty map key",
			)
		}

		if job.ID == "" {
			return fmt.Errorf(
				"snapshot job map key %q has empty job ID",
				id,
			)
		}

		if job.ID != id {
			return fmt.Errorf(
				"snapshot job map key %q does not match job ID %q",
				id,
				job.ID,
			)
		}

		switch job.State {
		case model.JobPending:
			if job.AssignedWorkerID != "" || job.FencingToken != 0 {
				return fmt.Errorf(
					"pending job %q retains active ownership",
					id,
				)
			}

		case model.JobScheduled, model.JobRunning:
			if err := validateActiveJobOwnership(
				id,
				job,
				snapshot.Locks,
			); err != nil {
				return err
			}

		case model.JobSucceeded, model.JobFailed:
			// Terminal jobs may retain their worker, fencing token,
			// execution ID, and corresponding lock. This matches the
			// current lifecycle implementation.

		default:
			return fmt.Errorf(
				"snapshot job %q has unknown state %q",
				id,
				job.State,
			)
		}
	}

	return nil
}

func validateActiveJobOwnership(
	id model.JobID,
	job model.Job,
	locks map[string]lock.Lock,
) error {
	if job.AssignedWorkerID == "" {
		return fmt.Errorf(
			"active job %q has empty assigned worker",
			id,
		)
	}

	if job.FencingToken == 0 {
		return fmt.Errorf(
			"active job %q has invalid fencing token 0",
			id,
		)
	}

	currentLock, ok := locks[string(id)]
	if !ok {
		return fmt.Errorf(
			"active job %q has no corresponding lock",
			id,
		)
	}

	if currentLock.OwnerID != job.AssignedWorkerID {
		return fmt.Errorf(
			"active job %q worker %q does not match lock owner %q",
			id,
			job.AssignedWorkerID,
			currentLock.OwnerID,
		)
	}

	if currentLock.FencingToken != job.FencingToken {
		return fmt.Errorf(
			"active job %q fencing token %d does not match lock token %d",
			id,
			job.FencingToken,
			currentLock.FencingToken,
		)
	}

	return nil
}

func (s *Store) Restore(data []byte) error {
	logger := s.getLogger()

	logger.Info(
		"KV snapshot restore started",
		"operation", "restore",
		"snapshot_bytes", len(data),
	)

	var raw map[string]json.RawMessage

	if err := json.Unmarshal(data, &raw); err != nil {
		logger.Error(
			"KV snapshot restore failed",
			"operation", "restore",
			"stage", "decode_envelope",
			"error", err,
		)

		return fmt.Errorf("unmarshal KV snapshot: %w", err)
	}

	if raw == nil {
		raw = make(map[string]json.RawMessage)
	}

	// Versioned snapshot.
	if versionRaw, ok := raw["version"]; ok {
		var version uint64

		if err := json.Unmarshal(versionRaw, &version); err != nil {
			logger.Error(
				"KV snapshot restore failed",
				"operation", "restore",
				"stage", "decode_version",
				"error", err,
			)

			return fmt.Errorf("decode KV snapshot version: %w", err)
		}

		if version != snapshotVersion {
			logger.Warn(
				"KV snapshot restore rejected",
				"operation", "restore",
				"version", version,
				"supported_version", snapshotVersion,
			)

			return fmt.Errorf(
				"unsupported KV snapshot version %d",
				version,
			)
		}

		var snapshot snapshotState

		if err := json.Unmarshal(data, &snapshot); err != nil {
			logger.Error(
				"KV snapshot restore failed",
				"operation", "restore",
				"stage", "decode_snapshot",
				"error", err,
			)

			return fmt.Errorf("decode KV snapshot: %w", err)
		}

		if snapshot.Data == nil {
			snapshot.Data = make(map[string][]byte)
		}

		if snapshot.Locks == nil {
			snapshot.Locks = make(map[string]lock.Lock)
		}

		if snapshot.Fenced == nil {
			snapshot.Fenced = make(map[string]FencedValue)
		}

		if snapshot.Jobs == nil {
			snapshot.Jobs = make(map[model.JobID]model.Job)
		}

		if err := validateSnapshotState(snapshot); err != nil {
			logger.Warn(
				"KV snapshot restore rejected",
				"operation", "restore",
				"stage", "validate_snapshot",
				"error", err,
			)

			return fmt.Errorf("validate KV snapshot: %w", err)
		}

		restoredData := cloneData(snapshot.Data)
		restoredLocks := cloneLocks(snapshot.Locks)
		restoredFenced := cloneFenced(snapshot.Fenced)
		restoredJobs := make(map[model.JobID]model.Job, len(snapshot.Jobs))

		for id, job := range snapshot.Jobs {
			restoredJobs[id] = cloneJob(job)
		}

		s.mu.Lock()
		s.data = restoredData
		s.locks = &lock.State{
			Locks:     restoredLocks,
			NextToken: snapshot.NextToken,
		}
		s.fenced = restoredFenced
		s.jobs = restoredJobs
		s.mu.Unlock()

		logger.Info(
			"KV snapshot restored",
			"operation", "restore",
			"version", version,
			"data_entries", len(restoredData),
			"locks", len(restoredLocks),
			"fenced_values", len(restoredFenced),
			"jobs", len(restoredJobs),
			"next_token", snapshot.NextToken,
		)

		return nil
	}

	// Legacy version-0 KV-only snapshot.
	var legacyData map[string][]byte

	if err := json.Unmarshal(data, &legacyData); err != nil {
		logger.Error(
			"legacy KV snapshot restore failed",
			"operation", "restore",
			"stage", "decode_legacy_snapshot",
			"error", err,
		)

		return fmt.Errorf("decode legacy KV snapshot: %w", err)
	}

	if legacyData == nil {
		legacyData = make(map[string][]byte)
	}

	restoredData := cloneData(legacyData)

	s.mu.Lock()
	s.data = restoredData

	// Legacy snapshots contain no lock state.
	s.locks = lock.NewState()
	s.fenced = make(map[string]FencedValue)
	s.jobs = make(map[model.JobID]model.Job)

	s.mu.Unlock()

	logger.Info(
		"legacy KV snapshot restored",
		"operation", "restore",
		"version", 0,
		"data_entries", len(restoredData),
	)

	return nil
}
