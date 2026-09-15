package kv

import (
	"encoding/json"
	"fmt"

	"github.com/sanchar127/raftiq/internal/lock"
	"github.com/sanchar127/raftiq/internal/model"
)

type snapshotEnvelope struct {
	Version   uint64               `json:"version"`
	Data      map[string][]byte    `json:"data"`
	Locks     map[string]lock.Lock `json:"locks"`
	NextToken uint64               `json:"next_token"`
}

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
