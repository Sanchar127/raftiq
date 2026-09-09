package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/sanchar127/raftiq/internal/raft"
)

const (
	recordState   byte = 1
	recordEntries byte = 2
)

func encodeStateRecord(state raft.PersistentState) ([]byte, error) {
	var payload bytes.Buffer

	if err := binary.Write(&payload, binary.BigEndian, uint64(state.CurrentTerm)); err != nil {
		return nil, fmt.Errorf("encode current term: %w", err)
	}

	votedFor := []byte(state.VotedFor)

	if err := binary.Write(&payload, binary.BigEndian, uint32(len(votedFor))); err != nil {
		return nil, fmt.Errorf("encode voted-for length: %w", err)
	}

	if _, err := payload.Write(votedFor); err != nil {
		return nil, fmt.Errorf("encode voted-for: %w", err)
	}

	return encodeRecord(recordState, payload.Bytes())
}

func encodeEntriesRecord(entries []raft.LogEntry) ([]byte, error) {
	var payload bytes.Buffer

	if err := binary.Write(&payload, binary.BigEndian, uint32(len(entries))); err != nil {
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
	if len(payload) > int(^uint32(0)) {
		return nil, fmt.Errorf("record payload too large: %d", len(payload))
	}

	var record bytes.Buffer

	record.WriteByte(recordType)

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

	return record.Bytes(), nil
}

func decodeRecord(reader io.Reader) (byte, []byte, error) {
	recordType := []byte{0}

	if _, err := io.ReadFull(reader, recordType); err != nil {
		return 0, nil, err
	}

	var payloadLength uint32

	if err := binary.Read(reader, binary.BigEndian, &payloadLength); err != nil {
		return 0, nil, err
	}

	payload := make([]byte, payloadLength)

	if _, err := io.ReadFull(reader, payload); err != nil {
		return 0, nil, err
	}

	return recordType[0], payload, nil
}
