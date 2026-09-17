package storage

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/sanchar127/raftiq/internal/model"
)

// SaveSnapshot appends a snapshot record.
//
// The snapshot becomes the latest recovered snapshot after the record has
// been successfully written. Call Sync to make it durable.
func (s *WALStorage) SaveSnapshot(
	snapshot model.Snapshot,
) (err error) {
	start := time.Now()

	var metrics StorageMetrics

	defer func() {
		metrics.IncOperation(StorageOperationSaveSnapshot)

		if err != nil {
			metrics.IncOperationError(StorageOperationSaveSnapshot)
		}

		metrics.ObserveOperationDuration(
			StorageOperationSaveSnapshot,
			time.Since(start),
		)
	}()

	s.mu.Lock()
	defer s.mu.Unlock()

	metrics = s.metrics
	if metrics == nil {
		metrics = NoopStorageMetrics{}
	}

	if err = s.ensureOpen(); err != nil {
		return err
	}

	snapshot.Data = cloneBytes(snapshot.Data)

	payload, err := encodeSnapshotPayload(snapshot)
	if err != nil {
		return fmt.Errorf("encode snapshot: %w", err)
	}

	record, err := encodeRecord(recordSnapshot, payload)
	if err != nil {
		return fmt.Errorf("encode snapshot record: %w", err)
	}

	if err = s.appendRecord(record); err != nil {
		return fmt.Errorf(
			"write snapshot record: %w",
			err,
		)
	}

	s.snapshot = &snapshot

	return nil
}

func (s *WALStorage) LoadSnapshot() (
	snapshot model.Snapshot,
	err error,
) {
	start := time.Now()

	var metrics StorageMetrics

	defer func() {
		metrics.IncOperation(StorageOperationLoadSnapshot)

		if err != nil {
			metrics.IncOperationError(StorageOperationLoadSnapshot)
		}

		metrics.ObserveOperationDuration(
			StorageOperationLoadSnapshot,
			time.Since(start),
		)
	}()

	s.mu.RLock()
	defer s.mu.RUnlock()

	metrics = s.metrics
	if metrics == nil {
		metrics = NoopStorageMetrics{}
	}

	if err = s.ensureOpenRead(); err != nil {
		return model.Snapshot{}, err
	}

	if s.snapshot == nil {
		return model.Snapshot{}, nil
	}

	snapshot = *s.snapshot
	snapshot.Data = cloneBytes(s.snapshot.Data)

	return snapshot, nil
}

func encodeSnapshotPayload(
	snapshot model.Snapshot,
) ([]byte, error) {
	data, err := json.Marshal(snapshot)
	if err != nil {
		return nil, fmt.Errorf(
			"encode snapshot: %w",
			err,
		)
	}

	if uint64(len(data)) > uint64(maxRecordPayloadSize) {
		return nil, fmt.Errorf(
			"snapshot payload too large: %d bytes, maximum %d",
			len(data),
			maxRecordPayloadSize,
		)
	}

	return data, nil
}

func decodeSnapshotPayload(
	payload []byte,
) (model.Snapshot, error) {
	var snapshot model.Snapshot

	if err := json.Unmarshal(
		payload,
		&snapshot,
	); err != nil {
		return model.Snapshot{}, fmt.Errorf(
			"decode snapshot payload: %w",
			err,
		)
	}

	snapshot.Data = cloneBytes(snapshot.Data)

	return snapshot, nil
}
