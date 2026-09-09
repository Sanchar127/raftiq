package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"

	"github.com/sanchar127/raftiq/internal/raft"
)

const (
	recordState          byte   = 1
	recordEntries        byte   = 2
	maxRecordPayloadSize uint32 = 16 << 20
)

type WALStorage struct {
	mu      sync.RWMutex
	file    *os.File
	state   raft.PersistentState
	entries []raft.LogEntry
}

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
		entries: make([]raft.LogEntry, 0),
	}

	if err := storage.recover(); err != nil {
		_ = file.Close()

		return nil, fmt.Errorf("recover WAL: %w", err)
	}

	return storage, nil
}

func (s *WALStorage) SaveState(state raft.PersistentState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	record, err := encodeStateRecord(state)
	if err != nil {
		return err
	}

	if _, err := s.file.Write(record); err != nil {
		return fmt.Errorf("write state record: %w", err)
	}

	s.state = state

	return nil
}

func (s *WALStorage) LoadState() (raft.PersistentState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.state, nil
}

func (s *WALStorage) AppendEntries(entries []raft.LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(entries) == 0 {
		return nil
	}

	record, err := encodeEntriesRecord(entries)
	if err != nil {
		return err
	}

	if _, err := s.file.Write(record); err != nil {
		return fmt.Errorf("write entries record: %w", err)
	}

	for _, entry := range entries {
		entry.Data = append([]byte(nil), entry.Data...)
		s.entries = append(s.entries, entry)
	}

	return nil
}

func (s *WALStorage) LoadEntries() ([]raft.LogEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entries := make([]raft.LogEntry, len(s.entries))

	for i, entry := range s.entries {
		entries[i] = entry
		entries[i].Data = append([]byte(nil), entry.Data...)
	}

	return entries, nil
}

func (s *WALStorage) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()

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

func (s *WALStorage) recover() error {
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek WAL: %w", err)
	}

	for {
		recordType, payload, err := decodeRecord(s.file)

		if err == io.EOF {
			break
		}

		if err == io.ErrUnexpectedEOF {
			break
		}

		if err != nil {
			return fmt.Errorf("decode WAL record: %w", err)
		}

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

			for _, entry := range entries {
				entry.Data = append([]byte(nil), entry.Data...)
				s.entries = append(s.entries, entry)
			}

		default:
			return fmt.Errorf("unknown WAL record type: %d", recordType)
		}
	}

	if _, err := s.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("seek WAL end: %w", err)
	}

	return nil
}

func encodeStateRecord(state raft.PersistentState) ([]byte, error) {
	var payload bytes.Buffer

	if err := binary.Write(
		&payload,
		binary.BigEndian,
		uint64(state.CurrentTerm),
	); err != nil {
		return nil, fmt.Errorf("encode current term: %w", err)
	}

	votedFor := []byte(state.VotedFor)

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

func encodeEntriesRecord(entries []raft.LogEntry) ([]byte, error) {
	var payload bytes.Buffer

	if err := binary.Write(
		&payload,
		binary.BigEndian,
		uint32(len(entries)),
	); err != nil {
		return nil, fmt.Errorf("encode entry count: %w", err)
	}

	for _, entry := range entries {
		if err := binary.Write(
			&payload,
			binary.BigEndian,
			uint64(entry.Index),
		); err != nil {
			return nil, fmt.Errorf("encode entry index: %w", err)
		}

		if err := binary.Write(
			&payload,
			binary.BigEndian,
			uint64(entry.Term),
		); err != nil {
			return nil, fmt.Errorf("encode entry term: %w", err)
		}

		if err := binary.Write(
			&payload,
			binary.BigEndian,
			uint32(len(entry.Data)),
		); err != nil {
			return nil, fmt.Errorf("encode entry data length: %w", err)
		}

		if _, err := payload.Write(entry.Data); err != nil {
			return nil, fmt.Errorf("encode entry data: %w", err)
		}
	}

	return encodeRecord(recordEntries, payload.Bytes())
}

func encodeRecord(recordType byte, payload []byte) ([]byte, error) {
	if uint64(len(payload)) > uint64(^uint32(0)) {
		return nil, fmt.Errorf("record payload too large: %d", len(payload))
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
	recordType := []byte{0}

	if _, err := io.ReadFull(reader, recordType); err != nil {
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

	if err := headerAndPayload.WriteByte(recordType[0]); err != nil {
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

	expectedChecksum := crc32.ChecksumIEEE(headerAndPayload.Bytes())

	if storedChecksum != expectedChecksum {
		return 0, nil, fmt.Errorf(
			"WAL checksum mismatch: got %08x, want %08x",
			storedChecksum,
			expectedChecksum,
		)
	}

	return recordType[0], payload, nil
}

func decodeStatePayload(payload []byte) (raft.PersistentState, error) {
	reader := bytes.NewReader(payload)

	var term uint64

	if err := binary.Read(reader, binary.BigEndian, &term); err != nil {
		return raft.PersistentState{}, fmt.Errorf("decode current term: %w", err)
	}

	var votedForLength uint32

	if err := binary.Read(
		reader,
		binary.BigEndian,
		&votedForLength,
	); err != nil {
		return raft.PersistentState{}, fmt.Errorf(
			"decode voted-for length: %w",
			err,
		)
	}

	votedFor := make([]byte, votedForLength)

	if _, err := io.ReadFull(reader, votedFor); err != nil {
		return raft.PersistentState{}, fmt.Errorf(
			"decode voted-for: %w",
			err,
		)
	}

	if reader.Len() != 0 {
		return raft.PersistentState{}, fmt.Errorf(
			"unexpected trailing state data: %d bytes",
			reader.Len(),
		)
	}

	return raft.PersistentState{
		CurrentTerm: raft.Term(term),
		VotedFor:    raft.NodeID(votedFor),
	}, nil
}

func decodeEntriesPayload(payload []byte) ([]raft.LogEntry, error) {
	reader := bytes.NewReader(payload)

	var entryCount uint32

	if err := binary.Read(
		reader,
		binary.BigEndian,
		&entryCount,
	); err != nil {
		return nil, fmt.Errorf("decode entry count: %w", err)
	}

	entries := make([]raft.LogEntry, 0, entryCount)

	for i := uint32(0); i < entryCount; i++ {
		var index uint64

		if err := binary.Read(
			reader,
			binary.BigEndian,
			&index,
		); err != nil {
			return nil, fmt.Errorf("decode entry index: %w", err)
		}

		var term uint64

		if err := binary.Read(
			reader,
			binary.BigEndian,
			&term,
		); err != nil {
			return nil, fmt.Errorf("decode entry term: %w", err)
		}

		var dataLength uint32

		if err := binary.Read(
			reader,
			binary.BigEndian,
			&dataLength,
		); err != nil {
			return nil, fmt.Errorf("decode entry data length: %w", err)
		}

		data := make([]byte, dataLength)

		if _, err := io.ReadFull(reader, data); err != nil {
			return nil, fmt.Errorf("decode entry data: %w", err)
		}

		entries = append(entries, raft.LogEntry{
			Index: raft.LogIndex(index),
			Term:  raft.Term(term),
			Data:  data,
		})
	}

	if reader.Len() != 0 {
		return nil, fmt.Errorf(
			"unexpected trailing entry data: %d bytes",
			reader.Len(),
		)
	}

	return entries, nil
}

var _ Storage = (*WALStorage)(nil)
