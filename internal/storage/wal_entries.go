package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/sanchar127/raftiq/internal/model"
)

// AppendEntries appends new log entries to the end of the current log.
//
// The caller is responsible for ensuring that the supplied entries form a
// valid append at the current log boundary. For Raft conflict resolution,
// use ReplaceSuffix.
func (s *WALStorage) AppendEntries(
	entries []model.LogEntry,
) (err error) {
	start := time.Now()

	var metrics StorageMetrics

	defer func() {
		metrics.IncOperation(StorageOperationAppendEntries)

		if err != nil {
			metrics.IncOperationError(StorageOperationAppendEntries)
		}

		metrics.ObserveOperationDuration(
			StorageOperationAppendEntries,
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

	if len(entries) == 0 {
		return nil
	}

	if err = validateEntries(entries); err != nil {
		return fmt.Errorf("validate entries: %w", err)
	}

	if err = validateAppend(s.entries, entries); err != nil {
		return err
	}

	record, err := encodeEntriesRecord(entries)
	if err != nil {
		return fmt.Errorf("encode entries record: %w", err)
	}

	if err = s.appendRecord(record); err != nil {
		return fmt.Errorf("write entries record: %w", err)
	}

	s.entries = appendEntriesCopy(s.entries, entries)

	return nil
}

// ReplaceSuffix atomically replaces the Raft log suffix beginning at from.
//
// Existing entries with index >= from are removed and the supplied entries
// are appended in their place.
//
// An empty entries slice therefore means:
//
//	truncate all entries with index >= from
//
// The operation is represented by a single WAL record, so recovery can
// replay the same logical operation after a process crash.
func (s *WALStorage) ReplaceSuffix(
	from model.LogIndex,
	entries []model.LogEntry,
) (err error) {
	start := time.Now()

	var metrics StorageMetrics

	defer func() {
		metrics.IncOperation(StorageOperationReplaceSuffix)

		if err != nil {
			metrics.IncOperationError(StorageOperationReplaceSuffix)
		}

		metrics.ObserveOperationDuration(
			StorageOperationReplaceSuffix,
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

	if err = validateReplaceSuffix(
		s.snapshot,
		from,
		entries,
	); err != nil {
		return err
	}

	record, err := encodeReplaceSuffixRecord(from, entries)
	if err != nil {
		return fmt.Errorf(
			"encode suffix replacement: %w",
			err,
		)
	}

	if err = s.appendRecord(record); err != nil {
		return fmt.Errorf(
			"write suffix replacement record: %w",
			err,
		)
	}

	s.entries = replaceSuffixCopy(
		s.entries,
		from,
		entries,
	)

	return nil
}

func (s *WALStorage) LoadEntries() (
	entries []model.LogEntry,
	err error,
) {
	start := time.Now()

	var metrics StorageMetrics

	defer func() {
		metrics.IncOperation(StorageOperationLoadEntries)

		if err != nil {
			metrics.IncOperationError(StorageOperationLoadEntries)
		}

		metrics.ObserveOperationDuration(
			StorageOperationLoadEntries,
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
		return nil, err
	}

	return cloneEntries(s.entries), nil
}

func encodeEntriesRecord(
	entries []model.LogEntry,
) ([]byte, error) {
	var payload bytes.Buffer

	if uint64(len(entries)) > uint64(^uint32(0)) {
		return nil, fmt.Errorf(
			"too many entries: %d",
			len(entries),
		)
	}

	if err := binary.Write(
		&payload,
		binary.BigEndian,
		uint32(len(entries)),
	); err != nil {
		return nil, fmt.Errorf(
			"encode entry count: %w",
			err,
		)
	}

	for i, entry := range entries {
		if err := encodeEntry(
			&payload,
			entry,
		); err != nil {
			return nil, fmt.Errorf(
				"encode entry %d: %w",
				i,
				err,
			)
		}
	}

	return encodeRecord(
		recordEntries,
		payload.Bytes(),
	)
}

func encodeReplaceSuffixRecord(
	from model.LogIndex,
	entries []model.LogEntry,
) ([]byte, error) {
	var payload bytes.Buffer

	if err := binary.Write(
		&payload,
		binary.BigEndian,
		uint64(from),
	); err != nil {
		return nil, fmt.Errorf(
			"encode replacement index: %w",
			err,
		)
	}

	if uint64(len(entries)) > uint64(^uint32(0)) {
		return nil, fmt.Errorf(
			"too many replacement entries: %d",
			len(entries),
		)
	}

	if err := binary.Write(
		&payload,
		binary.BigEndian,
		uint32(len(entries)),
	); err != nil {
		return nil, fmt.Errorf(
			"encode replacement entry count: %w",
			err,
		)
	}

	for i, entry := range entries {
		if err := encodeEntry(
			&payload,
			entry,
		); err != nil {
			return nil, fmt.Errorf(
				"encode replacement entry %d: %w",
				i,
				err,
			)
		}
	}

	return encodeRecord(
		recordReplaceSuffix,
		payload.Bytes(),
	)
}

func encodeEntry(
	payload *bytes.Buffer,
	entry model.LogEntry,
) error {
	if err := binary.Write(
		payload,
		binary.BigEndian,
		uint64(entry.Index),
	); err != nil {
		return fmt.Errorf(
			"encode entry index: %w",
			err,
		)
	}

	if err := binary.Write(
		payload,
		binary.BigEndian,
		uint64(entry.Term),
	); err != nil {
		return fmt.Errorf(
			"encode entry term: %w",
			err,
		)
	}

	if uint64(len(entry.Data)) > uint64(^uint32(0)) {
		return fmt.Errorf(
			"entry data too large: %d bytes",
			len(entry.Data),
		)
	}

	if err := binary.Write(
		payload,
		binary.BigEndian,
		uint32(len(entry.Data)),
	); err != nil {
		return fmt.Errorf(
			"encode entry data length: %w",
			err,
		)
	}

	if _, err := payload.Write(entry.Data); err != nil {
		return fmt.Errorf(
			"encode entry data: %w",
			err,
		)
	}

	return nil
}

func decodeEntriesPayload(
	payload []byte,
) ([]model.LogEntry, error) {
	reader := bytes.NewReader(payload)

	var entryCount uint32

	if err := binary.Read(
		reader,
		binary.BigEndian,
		&entryCount,
	); err != nil {
		return nil, fmt.Errorf(
			"decode entry count: %w",
			err,
		)
	}

	// Every encoded entry requires at least:
	// index (8) + term (8) + data length (4).
	const minimumEntrySize = 20

	if uint64(entryCount) >
		uint64(reader.Len())/minimumEntrySize {
		return nil, fmt.Errorf(
			"invalid entry count %d for %d remaining bytes",
			entryCount,
			reader.Len(),
		)
	}

	entries := make(
		[]model.LogEntry,
		0,
		int(entryCount),
	)

	for i := uint32(0); i < entryCount; i++ {
		entry, err := decodeEntry(reader)
		if err != nil {
			return nil, fmt.Errorf(
				"decode entry %d: %w",
				i,
				err,
			)
		}

		entries = append(entries, entry)
	}

	if reader.Len() != 0 {
		return nil, fmt.Errorf(
			"unexpected trailing entry data: %d bytes",
			reader.Len(),
		)
	}

	if err := validateEntries(entries); err != nil {
		return nil, fmt.Errorf(
			"validate decoded entries: %w",
			err,
		)
	}

	return entries, nil
}

func decodeEntry(
	reader *bytes.Reader,
) (model.LogEntry, error) {
	var index uint64

	if err := binary.Read(
		reader,
		binary.BigEndian,
		&index,
	); err != nil {
		return model.LogEntry{}, fmt.Errorf(
			"decode entry index: %w",
			err,
		)
	}

	var term uint64

	if err := binary.Read(
		reader,
		binary.BigEndian,
		&term,
	); err != nil {
		return model.LogEntry{}, fmt.Errorf(
			"decode entry term: %w",
			err,
		)
	}

	var dataLength uint32

	if err := binary.Read(
		reader,
		binary.BigEndian,
		&dataLength,
	); err != nil {
		return model.LogEntry{}, fmt.Errorf(
			"decode entry data length: %w",
			err,
		)
	}

	if uint64(dataLength) > uint64(reader.Len()) {
		return model.LogEntry{}, fmt.Errorf(
			"invalid entry data length: %d, remaining payload: %d",
			dataLength,
			reader.Len(),
		)
	}

	data := make(
		[]byte,
		dataLength,
	)

	if _, err := io.ReadFull(
		reader,
		data,
	); err != nil {
		return model.LogEntry{}, fmt.Errorf(
			"decode entry data: %w",
			err,
		)
	}

	return model.LogEntry{
		Index: model.LogIndex(index),
		Term:  model.Term(term),
		Data:  data,
	}, nil
}

func decodeReplaceSuffixPayload(
	payload []byte,
) (model.LogIndex, []model.LogEntry, error) {
	reader := bytes.NewReader(payload)

	var from uint64

	if err := binary.Read(
		reader,
		binary.BigEndian,
		&from,
	); err != nil {
		return 0, nil, fmt.Errorf(
			"decode replacement index: %w",
			err,
		)
	}

	var entryCount uint32

	if err := binary.Read(
		reader,
		binary.BigEndian,
		&entryCount,
	); err != nil {
		return 0, nil, fmt.Errorf(
			"decode replacement entry count: %w",
			err,
		)
	}

	const minimumEntrySize = 20

	if uint64(entryCount) >
		uint64(reader.Len())/minimumEntrySize {
		return 0, nil, fmt.Errorf(
			"invalid replacement entry count %d for %d remaining bytes",
			entryCount,
			reader.Len(),
		)
	}

	entries := make(
		[]model.LogEntry,
		0,
		int(entryCount),
	)

	for i := uint32(0); i < entryCount; i++ {
		entry, err := decodeEntry(reader)
		if err != nil {
			return 0, nil, fmt.Errorf(
				"decode replacement entry %d: %w",
				i,
				err,
			)
		}

		entries = append(entries, entry)
	}

	if reader.Len() != 0 {
		return 0, nil, fmt.Errorf(
			"unexpected trailing replacement data: %d bytes",
			reader.Len(),
		)
	}

	return model.LogIndex(from), entries, nil
}
