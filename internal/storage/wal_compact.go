package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/sanchar127/raftiq/internal/model"
)

func (s *WALStorage) Compact(
	snapshot model.Snapshot,
) (err error) {
	start := time.Now()

	var metrics StorageMetrics

	defer func() {
		metrics.IncOperation(StorageOperationCompact)

		if err != nil {
			metrics.IncOperationError(StorageOperationCompact)
		}

		metrics.ObserveOperationDuration(
			StorageOperationCompact,
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

	if snapshot.LastIncludedIndex == 0 {
		return fmt.Errorf(
			"cannot compact at snapshot index 0",
		)
	}

	if s.snapshot == nil {
		return fmt.Errorf(
			"cannot compact without persisted snapshot",
		)
	}

	if s.snapshot.LastIncludedIndex !=
		snapshot.LastIncludedIndex {
		return fmt.Errorf(
			"snapshot index mismatch: stored %d, requested %d",
			s.snapshot.LastIncludedIndex,
			snapshot.LastIncludedIndex,
		)
	}

	if s.snapshot.LastIncludedTerm !=
		snapshot.LastIncludedTerm {
		return fmt.Errorf(
			"snapshot term mismatch: stored %d, requested %d",
			s.snapshot.LastIncludedTerm,
			snapshot.LastIncludedTerm,
		)
	}

	entries := make([]model.LogEntry, 0, len(s.entries))

	for _, entry := range s.entries {
		if entry.Index <= snapshot.LastIncludedIndex {
			continue
		}

		entry.Data = cloneBytes(entry.Data)
		entries = append(entries, entry)
	}

	stateRecord, err := encodeStateRecord(s.state)
	if err != nil {
		return fmt.Errorf(
			"encode state for compaction: %w",
			err,
		)
	}

	snapshotPayload, err := encodeSnapshotPayload(snapshot)
	if err != nil {
		return fmt.Errorf(
			"encode snapshot for compaction: %w",
			err,
		)
	}

	snapshotRecord, err := encodeRecord(
		recordSnapshot,
		snapshotPayload,
	)
	if err != nil {
		return fmt.Errorf(
			"encode snapshot record for compaction: %w",
			err,
		)
	}

	entriesRecord, err := encodeEntriesRecord(entries)
	if err != nil {
		return fmt.Errorf(
			"encode entries for compaction: %w",
			err,
		)
	}

	walPath := s.file.Name()
	dir := filepath.Dir(walPath)

	tempFile, err := os.CreateTemp(
		dir,
		".raftiq-wal-compact-*",
	)
	if err != nil {
		return fmt.Errorf(
			"create WAL compaction temporary file: %w",
			err,
		)
	}

	tempPath := tempFile.Name()

	cleanup := func() {
		_ = tempFile.Close()
		_ = os.Remove(tempPath)
	}

	if err := writeFull(tempFile, stateRecord); err != nil {
		cleanup()

		return fmt.Errorf(
			"write compacted state record: %w",
			err,
		)
	}

	if err := writeFull(tempFile, snapshotRecord); err != nil {
		cleanup()

		return fmt.Errorf(
			"write compacted snapshot record: %w",
			err,
		)
	}

	if len(entries) > 0 {
		if err := writeFull(tempFile, entriesRecord); err != nil {
			cleanup()

			return fmt.Errorf(
				"write compacted entries record: %w",
				err,
			)
		}
	}

	if err := tempFile.Sync(); err != nil {
		cleanup()

		return fmt.Errorf(
			"sync compacted WAL: %w",
			err,
		)
	}

	if err := tempFile.Close(); err != nil {
		_ = os.Remove(tempPath)

		return fmt.Errorf(
			"close compacted WAL: %w",
			err,
		)
	}

	if err := s.file.Close(); err != nil {
		_ = os.Remove(tempPath)

		return fmt.Errorf(
			"close current WAL before compaction: %w",
			err,
		)
	}

	s.file = nil

	if err := os.Rename(tempPath, walPath); err != nil {
		reopened, reopenErr := os.OpenFile(
			walPath,
			os.O_RDWR|os.O_APPEND,
			0o600,
		)

		if reopenErr == nil {
			s.file = reopened
			s.syncFn = reopened.Sync
		}

		_ = os.Remove(tempPath)

		if reopenErr != nil {
			return fmt.Errorf(
				"rename compacted WAL: %w; reopen original WAL: %w",
				err,
				reopenErr,
			)
		}

		return fmt.Errorf(
			"rename compacted WAL: %w",
			err,
		)
	}

	if err := syncDirectory(dir); err != nil {
		return fmt.Errorf(
			"sync WAL directory after rename: %w",
			err,
		)
	}

	file, err := os.OpenFile(
		walPath,
		os.O_RDWR|os.O_APPEND,
		0o600,
	)
	if err != nil {
		s.file = nil

		return fmt.Errorf(
			"reopen compacted WAL: %w",
			err,
		)
	}

	s.file = file
	s.syncFn = file.Sync
	s.entries = entries

	snapshotCopy := snapshot
	snapshotCopy.Data = cloneBytes(snapshot.Data)
	s.snapshot = &snapshotCopy

	return nil
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return fmt.Errorf(
			"open WAL directory: %w",
			err,
		)
	}

	if err := dir.Sync(); err != nil {
		_ = dir.Close()

		return fmt.Errorf(
			"sync WAL directory: %w",
			err,
		)
	}

	if err := dir.Close(); err != nil {
		return fmt.Errorf(
			"close WAL directory: %w",
			err,
		)
	}

	return nil
}
