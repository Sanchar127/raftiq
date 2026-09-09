package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"

	"github.com/sanchar127/raftiq/internal/model"
)

const (
	recordState          byte   = 1
	recordEntries        byte   = 2
	recordSnapshot       byte   = 3
	recordReplaceSuffix  byte   = 4
	maxRecordPayloadSize uint32 = 16 << 20

	recordHeaderSize = 1 + 4
	recordFooterSize = 4
)

var (
	ErrClosedStorage = errors.New("WAL storage is closed")
	ErrInvalidLog    = errors.New("invalid Raft log")
)

type WALStorage struct {
	mu sync.RWMutex

	file *os.File

	state    model.PersistentState
	entries  []model.LogEntry
	snapshot *model.Snapshot
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
		entries: make([]model.LogEntry, 0),
	}

	if err := storage.recover(); err != nil {
		_ = file.Close()

		return nil, fmt.Errorf("recover WAL: %w", err)
	}

	return storage, nil
}

// SaveState appends a new persistent-state record.
//
// Durability is established by Sync. Callers must not treat a successful
// SaveState call as durable until Sync has completed successfully.
func (s *WALStorage) SaveState(state model.PersistentState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.ensureOpen(); err != nil {
		return err
	}

	record, err := encodeStateRecord(state)
	if err != nil {
		return fmt.Errorf("encode state record: %w", err)
	}

	if err := writeFull(s.file, record); err != nil {
		return fmt.Errorf("write state record: %w", err)
	}

	s.state = state

	return nil
}

func (s *WALStorage) LoadState() (model.PersistentState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if err := s.ensureOpenRead(); err != nil {
		return model.PersistentState{}, err
	}

	return s.state, nil
}

// AppendEntries appends new log entries to the end of the current log.
//
// The caller is responsible for ensuring that the supplied entries form a
// valid append at the current log boundary. For Raft conflict resolution,
// use ReplaceSuffix.
func (s *WALStorage) AppendEntries(entries []model.LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.ensureOpen(); err != nil {
		return err
	}

	if len(entries) == 0 {
		return nil
	}

	if err := validateEntries(entries); err != nil {
		return fmt.Errorf("validate entries: %w", err)
	}

	if err := validateAppend(s.entries, entries); err != nil {
		return err
	}

	record, err := encodeEntriesRecord(entries)
	if err != nil {
		return fmt.Errorf("encode entries record: %w", err)
	}

	if err := writeFull(s.file, record); err != nil {
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
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.ensureOpen(); err != nil {
		return err
	}

	if from == 0 {
		return fmt.Errorf(
			"%w: suffix replacement index must be greater than zero",
			ErrInvalidLog,
		)
	}

	if err := validateEntries(entries); err != nil {
		return fmt.Errorf("validate replacement entries: %w", err)
	}

	if len(entries) > 0 {
		if entries[0].Index != from {
			return fmt.Errorf(
				"%w: replacement starts at index %d, want %d",
				ErrInvalidLog,
				entries[0].Index,
				from,
			)
		}
	}

	record, err := encodeReplaceSuffixRecord(from, entries)
	if err != nil {
		return fmt.Errorf("encode suffix replacement: %w", err)
	}

	if err := writeFull(s.file, record); err != nil {
		return fmt.Errorf("write suffix replacement record: %w", err)
	}

	s.entries = replaceSuffixCopy(s.entries, from, entries)

	return nil
}

func (s *WALStorage) LoadEntries() ([]model.LogEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if err := s.ensureOpenRead(); err != nil {
		return nil, err
	}

	return cloneEntries(s.entries), nil
}

// SaveSnapshot appends a snapshot record.
//
// The snapshot becomes the latest recovered snapshot after the record has
// been successfully written. Call Sync to make it durable.
func (s *WALStorage) SaveSnapshot(snapshot model.Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.ensureOpen(); err != nil {
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

	if err := writeFull(s.file, record); err != nil {
		return fmt.Errorf("write snapshot record: %w", err)
	}

	s.snapshot = &snapshot

	return nil
}

func (s *WALStorage) LoadSnapshot() (model.Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if err := s.ensureOpenRead(); err != nil {
		return model.Snapshot{}, err
	}

	if s.snapshot == nil {
		return model.Snapshot{}, nil
	}

	snapshot := *s.snapshot
	snapshot.Data = cloneBytes(s.snapshot.Data)

	return snapshot, nil
}

// Sync forces all WAL data written so far to stable storage.
//
// A successful Sync is the durability boundary used by the Raft layer.
func (s *WALStorage) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.ensureOpen(); err != nil {
		return err
	}

	if err := s.file.Sync(); err != nil {
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

		if err == io.EOF {
			validOffset = recordStart
			break
		}

		if err == io.ErrUnexpectedEOF {
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

			snapshot.Data = cloneBytes(snapshot.Data)
			s.snapshot = &snapshot

		case recordReplaceSuffix:
			from, entries, err := decodeReplaceSuffixPayload(payload)
			if err != nil {
				return fmt.Errorf(
					"decode suffix replacement: %w",
					err,
				)
			}

			if err := validateEntries(entries); err != nil {
				return fmt.Errorf(
					"validate recovered replacement entries: %w",
					err,
				)
			}

			if len(entries) > 0 && entries[0].Index != from {
				return fmt.Errorf(
					"%w: replacement starts at %d, want %d",
					ErrInvalidLog,
					entries[0].Index,
					from,
				)
			}

			s.entries = replaceSuffixCopy(
				s.entries,
				from,
				entries,
			)

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

func encodeStateRecord(state model.PersistentState) ([]byte, error) {
	var payload bytes.Buffer

	if err := binary.Write(
		&payload,
		binary.BigEndian,
		uint64(state.CurrentTerm),
	); err != nil {
		return nil, fmt.Errorf("encode current term: %w", err)
	}

	votedFor := []byte(state.VotedFor)

	if uint64(len(votedFor)) > uint64(^uint32(0)) {
		return nil, fmt.Errorf(
			"voted-for value too large: %d bytes",
			len(votedFor),
		)
	}

	if err := binary.Write(
		&payload,
		binary.BigEndian,
		uint32(len(votedFor)),
	); err != nil {
		return nil, fmt.Errorf("encode voted-for length: %w", err)
	}

	if _, err := payload.Write(votedFor); err != nil {
		return nil, fmt.Errorf("encode voted-for: %w", err)
	}

	return encodeRecord(recordState, payload.Bytes())
}

func encodeEntriesRecord(entries []model.LogEntry) ([]byte, error) {
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
		return nil, fmt.Errorf("encode entry count: %w", err)
	}

	for i, entry := range entries {
		if err := encodeEntry(&payload, entry); err != nil {
			return nil, fmt.Errorf(
				"encode entry %d: %w",
				i,
				err,
			)
		}
	}

	return encodeRecord(recordEntries, payload.Bytes())
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
		if err := encodeEntry(&payload, entry); err != nil {
			return nil, fmt.Errorf(
				"encode replacement entry %d: %w",
				i,
				err,
			)
		}
	}

	return encodeRecord(recordReplaceSuffix, payload.Bytes())
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
		return fmt.Errorf("encode entry index: %w", err)
	}

	if err := binary.Write(
		payload,
		binary.BigEndian,
		uint64(entry.Term),
	); err != nil {
		return fmt.Errorf("encode entry term: %w", err)
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
		return fmt.Errorf("encode entry data length: %w", err)
	}

	if _, err := payload.Write(entry.Data); err != nil {
		return fmt.Errorf("encode entry data: %w", err)
	}

	return nil
}

func encodeRecord(recordType byte, payload []byte) ([]byte, error) {
	if uint64(len(payload)) > uint64(maxRecordPayloadSize) {
		return nil, fmt.Errorf(
			"record payload too large: %d bytes, maximum %d",
			len(payload),
			maxRecordPayloadSize,
		)
	}

	var record bytes.Buffer

	if err := record.WriteByte(recordType); err != nil {
		return nil, fmt.Errorf("encode record type: %w", err)
	}

	if err := binary.Write(
		&record,
		binary.BigEndian,
		uint32(len(payload)),
	); err != nil {
		return nil, fmt.Errorf("encode record length: %w", err)
	}

	if _, err := record.Write(payload); err != nil {
		return nil, fmt.Errorf("encode record payload: %w", err)
	}

	checksum := crc32.ChecksumIEEE(record.Bytes())

	if err := binary.Write(
		&record,
		binary.BigEndian,
		checksum,
	); err != nil {
		return nil, fmt.Errorf("encode record checksum: %w", err)
	}

	return record.Bytes(), nil
}

func decodeRecord(reader io.Reader) (byte, []byte, error) {
	var recordType byte

	if err := binary.Read(
		reader,
		binary.BigEndian,
		&recordType,
	); err != nil {
		return 0, nil, err
	}

	var payloadLength uint32

	if err := binary.Read(
		reader,
		binary.BigEndian,
		&payloadLength,
	); err != nil {
		return 0, nil, err
	}

	if payloadLength > maxRecordPayloadSize {
		return 0, nil, fmt.Errorf(
			"WAL record payload too large: %d bytes, maximum %d",
			payloadLength,
			maxRecordPayloadSize,
		)
	}

	payload := make([]byte, payloadLength)

	if _, err := io.ReadFull(reader, payload); err != nil {
		return 0, nil, err
	}

	var storedChecksum uint32

	if err := binary.Read(
		reader,
		binary.BigEndian,
		&storedChecksum,
	); err != nil {
		return 0, nil, err
	}

	var headerAndPayload bytes.Buffer

	if err := headerAndPayload.WriteByte(recordType); err != nil {
		return 0, nil, err
	}

	if err := binary.Write(
		&headerAndPayload,
		binary.BigEndian,
		payloadLength,
	); err != nil {
		return 0, nil, err
	}

	if _, err := headerAndPayload.Write(payload); err != nil {
		return 0, nil, err
	}

	expectedChecksum := crc32.ChecksumIEEE(
		headerAndPayload.Bytes(),
	)

	if storedChecksum != expectedChecksum {
		return 0, nil, fmt.Errorf(
			"WAL checksum mismatch: got %08x, want %08x",
			storedChecksum,
			expectedChecksum,
		)
	}

	return recordType, payload, nil
}

func decodeStatePayload(
	payload []byte,
) (model.PersistentState, error) {
	reader := bytes.NewReader(payload)

	var term uint64

	if err := binary.Read(
		reader,
		binary.BigEndian,
		&term,
	); err != nil {
		return model.PersistentState{}, fmt.Errorf(
			"decode current term: %w",
			err,
		)
	}

	var votedForLength uint32

	if err := binary.Read(
		reader,
		binary.BigEndian,
		&votedForLength,
	); err != nil {
		return model.PersistentState{}, fmt.Errorf(
			"decode voted-for length: %w",
			err,
		)
	}

	if uint64(votedForLength) > uint64(reader.Len()) {
		return model.PersistentState{}, fmt.Errorf(
			"invalid voted-for length: %d, remaining payload: %d",
			votedForLength,
			reader.Len(),
		)
	}

	votedFor := make([]byte, votedForLength)

	if _, err := io.ReadFull(reader, votedFor); err != nil {
		return model.PersistentState{}, fmt.Errorf(
			"decode voted-for: %w",
			err,
		)
	}

	if reader.Len() != 0 {
		return model.PersistentState{}, fmt.Errorf(
			"unexpected trailing state data: %d bytes",
			reader.Len(),
		)
	}

	return model.PersistentState{
		CurrentTerm: model.Term(term),
		VotedFor:    model.NodeID(votedFor),
	}, nil
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

	data := make([]byte, dataLength)

	if _, err := io.ReadFull(reader, data); err != nil {
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

func encodeSnapshotPayload(
	snapshot model.Snapshot,
) ([]byte, error) {
	data, err := json.Marshal(snapshot)
	if err != nil {
		return nil, fmt.Errorf("encode snapshot: %w", err)
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

	if err := json.Unmarshal(payload, &snapshot); err != nil {
		return model.Snapshot{}, fmt.Errorf(
			"decode snapshot payload: %w",
			err,
		)
	}

	snapshot.Data = cloneBytes(snapshot.Data)

	return snapshot, nil
}

func validateEntries(entries []model.LogEntry) error {
	for i, entry := range entries {
		if entry.Index == 0 {
			return fmt.Errorf(
				"%w: entry %d has zero index",
				ErrInvalidLog,
				i,
			)
		}

		if i > 0 {
			previous := entries[i-1]

			if entry.Index != previous.Index+1 {
				return fmt.Errorf(
					"%w: entry indexes are not contiguous: %d followed by %d",
					ErrInvalidLog,
					previous.Index,
					entry.Index,
				)
			}
		}
	}

	return nil
}

func validateLog(entries []model.LogEntry) error {
	if len(entries) == 0 {
		return nil
	}

	return validateEntries(entries)
}

func validateAppend(
	existing []model.LogEntry,
	additions []model.LogEntry,
) error {
	if len(existing) == 0 {
		return nil
	}

	expected := existing[len(existing)-1].Index + 1

	if additions[0].Index != expected {
		return fmt.Errorf(
			"%w: append starts at index %d, want %d",
			ErrInvalidLog,
			additions[0].Index,
			expected,
		)
	}

	return nil
}

func validateRecoveredAppend(
	existing []model.LogEntry,
	additions []model.LogEntry,
) error {
	if len(additions) == 0 {
		return nil
	}

	return validateAppend(existing, additions)
}

func replaceSuffixCopy(
	existing []model.LogEntry,
	from model.LogIndex,
	replacement []model.LogEntry,
) []model.LogEntry {
	cut := len(existing)

	for i, entry := range existing {
		if entry.Index >= from {
			cut = i
			break
		}
	}

	result := make(
		[]model.LogEntry,
		0,
		cut+len(replacement),
	)

	result = append(result, existing[:cut]...)
	result = appendEntriesCopy(result, replacement)

	return result
}

func appendEntriesCopy(
	dst []model.LogEntry,
	src []model.LogEntry,
) []model.LogEntry {
	for _, entry := range src {
		copied := entry
		copied.Data = cloneBytes(entry.Data)
		dst = append(dst, copied)
	}

	return dst
}

func cloneEntries(entries []model.LogEntry) []model.LogEntry {
	if len(entries) == 0 {
		return make([]model.LogEntry, 0)
	}

	result := make([]model.LogEntry, len(entries))

	for i, entry := range entries {
		result[i] = entry
		result[i].Data = cloneBytes(entry.Data)
	}

	return result
}

func cloneBytes(data []byte) []byte {
	if data == nil {
		return nil
	}

	return append([]byte(nil), data...)
}

func writeFull(file *os.File, data []byte) error {
	if file == nil {
		return ErrClosedStorage
	}

	for len(data) > 0 {
		n, err := file.Write(data)

		if n > 0 {
			data = data[n:]
		}

		if err != nil {
			return err
		}

		if n == 0 {
			return io.ErrShortWrite
		}
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
