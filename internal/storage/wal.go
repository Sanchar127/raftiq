package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/sanchar127/raftiq/internal/model"
)

type WALStorage struct {
	mu sync.RWMutex

	file    *os.File
	syncFn  func() error
	writeFn func(*os.File, []byte) error

	state    model.PersistentState
	entries  []model.LogEntry
	snapshot *model.Snapshot

	metrics StorageMetrics
}

// OpenWAL opens or creates a WAL and reconstructs the latest state from
// all valid records. If the WAL ends with a partially-written record,
// the incomplete tail is truncated.
func OpenWAL(path string) (*WALStorage, error) {
	file, err := os.OpenFile(
		path,
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0o600,
	)
	if err != nil {
		return nil, fmt.Errorf("open WAL: %w", err)
	}

	storage := &WALStorage{
		file:    file,
		syncFn:  file.Sync,
		writeFn: writeFull,
		entries: make([]model.LogEntry, 0),
		metrics: NoopStorageMetrics{},
	}

	if err := storage.recover(); err != nil {
		_ = file.Close()

		return nil, fmt.Errorf("recover WAL: %w", err)
	}

	return storage, nil
}

// SetMetrics replaces the storage metrics implementation.
//
// A nil implementation disables metrics by restoring the no-op implementation.
func (s *WALStorage) SetMetrics(metrics StorageMetrics) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if metrics == nil {
		s.metrics = NoopStorageMetrics{}
		return
	}

	s.metrics = metrics
}

// Sync forces all WAL data written so far to stable storage.
//
// A successful Sync is the durability boundary used by the Raft layer.
func (s *WALStorage) Sync() (err error) {
	start := time.Now()

	var metrics StorageMetrics

	defer func() {
		metrics.IncSync()

		if err != nil {
			metrics.IncSyncError()
		}

		metrics.ObserveSyncDuration(time.Since(start))
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

	if s.syncFn == nil {
		s.syncFn = s.file.Sync
	}

	if err = s.syncFn(); err != nil {
		if errors.Is(err, syscall.ENOSPC) {
			return fmt.Errorf("%w: %w", ErrWALDiskFull, err)
		}

		return fmt.Errorf("sync WAL: %w", err)
	}

	return nil
}

func (s *WALStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.file == nil {
		return nil
	}

	if err := s.file.Close(); err != nil {
		return fmt.Errorf("close WAL: %w", err)
	}

	s.file = nil

	return nil
}

// recover reconstructs the in-memory state from the WAL.
//
// A partially-written final record is treated as a crash tail and removed.
// Any complete record with an invalid checksum or malformed payload causes
// recovery to fail because silently accepting such data could produce an
// invalid Raft state.
func (s *WALStorage) recover() error {
	if s.file == nil {
		return ErrClosedStorage
	}

	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek WAL start: %w", err)
	}

	var validOffset int64

	for {
		recordStart, err := s.file.Seek(0, io.SeekCurrent)
		if err != nil {
			return fmt.Errorf("get WAL offset: %w", err)
		}

		recordType, payload, err := decodeRecord(s.file)

		if errors.Is(err, io.EOF) {
			// EOF at the current valid boundary means the WAL ended
			// cleanly. EOF after a record has started means the final
			// record is incomplete and must be discarded.
			if recordStart == validOffset {
				break
			}

			if err := s.file.Truncate(validOffset); err != nil {
				return fmt.Errorf(
					"truncate incomplete WAL tail at offset %d: %w",
					validOffset,
					err,
				)
			}
			break
		}

		if errors.Is(err, io.ErrUnexpectedEOF) {
			if err := s.file.Truncate(validOffset); err != nil {
				return fmt.Errorf(
					"truncate incomplete WAL tail at offset %d: %w",
					validOffset,
					err,
				)
			}
			break
		}

		if err != nil {
			return fmt.Errorf(
				"decode WAL record at offset %d: %w",
				recordStart,
				err,
			)
		}

		endOffset, err := s.file.Seek(0, io.SeekCurrent)
		if err != nil {
			return fmt.Errorf("get WAL record end offset: %w", err)
		}

		validOffset = endOffset

		switch recordType {
		case recordState:
			state, err := decodeStatePayload(payload)
			if err != nil {
				return fmt.Errorf("decode state: %w", err)
			}
			s.state = state

		case recordEntries:
			entries, err := decodeEntriesPayload(payload)
			if err != nil {
				return fmt.Errorf("decode entries: %w", err)
			}

			if err := validateRecoveredAppend(s.entries, entries); err != nil {
				return fmt.Errorf("validate recovered entries: %w", err)
			}

			s.entries = appendEntriesCopy(s.entries, entries)

		case recordSnapshot:
			snapshot, err := decodeSnapshotPayload(payload)
			if err != nil {
				return fmt.Errorf("decode snapshot: %w", err)
			}

			if err := validateRecoveredSnapshot(s.snapshot, snapshot); err != nil {
				return fmt.Errorf(
					"validate recovered snapshot: %w",
					err,
				)
			}

			snapshot.Data = cloneBytes(snapshot.Data)
			s.snapshot = &snapshot

		case recordReplaceSuffix:
			from, entries, err := decodeReplaceSuffixPayload(payload)
			if err != nil {
				return fmt.Errorf("decode suffix replacement: %w", err)
			}

			if err := validateReplaceSuffix(
				s.snapshot,
				from,
				entries,
			); err != nil {
				return fmt.Errorf(
					"validate recovered suffix replacement: %w",
					err,
				)
			}

			s.entries = replaceSuffixCopy(s.entries, from, entries)

			if err := validateLog(s.entries); err != nil {
				return fmt.Errorf(
					"validate recovered log after replacement: %w",
					err,
				)
			}

		default:
			return fmt.Errorf(
				"unknown WAL record type: %d",
				recordType,
			)
		}
	}

	if _, err := s.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("seek WAL end: %w", err)
	}

	return nil
}

func (s *WALStorage) appendRecord(record []byte) error {
	startOffset, err := s.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("get WAL write offset: %w", err)
	}

	if s.writeFn == nil {
		s.writeFn = writeFull
	}

	if err := s.writeFn(s.file, record); err != nil {
		writeErr := err

		if errors.Is(err, syscall.ENOSPC) {
			writeErr = fmt.Errorf("%w: %w", ErrWALDiskFull, err)
		}

		if rollbackErr := s.file.Truncate(startOffset); rollbackErr != nil {
			closeErr := s.file.Close()
			s.file = nil

			if closeErr != nil {
				return fmt.Errorf(
					"WAL write failed: %w; rollback failed: %w; close failed: %w",
					writeErr,
					rollbackErr,
					closeErr,
				)
			}

			return fmt.Errorf(
				"WAL write failed: %w; rollback failed: %w",
				writeErr,
				rollbackErr,
			)
		}

		if _, seekErr := s.file.Seek(0, io.SeekEnd); seekErr != nil {
			closeErr := s.file.Close()
			s.file = nil

			if closeErr != nil {
				return fmt.Errorf(
					"WAL write failed: %w; restore position failed: %w; close failed: %w",
					writeErr,
					seekErr,
					closeErr,
				)
			}

			return fmt.Errorf(
				"WAL write failed: %w; restore position failed: %w",
				writeErr,
				seekErr,
			)
		}

		return writeErr
	}

	return nil
}

func (s *WALStorage) ensureOpen() error {
	if s.file == nil {
		return ErrClosedStorage
	}

	return nil
}

func (s *WALStorage) ensureOpenRead() error {
	if s.file == nil {
		return ErrClosedStorage
	}

	return nil
}

var _ Storage = (*WALStorage)(nil)
