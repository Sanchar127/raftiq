package storage

import (
	"bytes"
	"testing"

	"github.com/sanchar127/raftiq/internal/raft"
)

func TestEncodeStateRecordRoundTrip(t *testing.T) {
	want := raft.PersistentState{
		CurrentTerm: 42,
		VotedFor:    "node-2",
	}

	record, err := encodeStateRecord(want)
	if err != nil {
		t.Fatalf("encodeStateRecord() error = %v", err)
	}

	recordType, payload, err := decodeRecord(bytes.NewReader(record))
	if err != nil {
		t.Fatalf("decodeRecord() error = %v", err)
	}

	if recordType != recordState {
		t.Fatalf("record type = %d, want %d", recordType, recordState)
	}

	if len(payload) == 0 {
		t.Fatal("decoded state payload is empty")
	}
}

func TestEncodeEntriesRecordRoundTrip(t *testing.T) {
	entries := []raft.LogEntry{
		{
			Index: 1,
			Term:  3,
			Data:  []byte("first"),
		},
		{
			Index: 2,
			Term:  3,
			Data:  []byte("second"),
		},
	}

	record, err := encodeEntriesRecord(entries)
	if err != nil {
		t.Fatalf("encodeEntriesRecord() error = %v", err)
	}

	recordType, payload, err := decodeRecord(bytes.NewReader(record))
	if err != nil {
		t.Fatalf("decodeRecord() error = %v", err)
	}

	if recordType != recordEntries {
		t.Fatalf("record type = %d, want %d", recordType, recordEntries)
	}

	if len(payload) == 0 {
		t.Fatal("decoded entries payload is empty")
	}
}

func TestDecodeRecordRejectsTruncatedRecord(t *testing.T) {
	record, err := encodeEntriesRecord([]raft.LogEntry{
		{
			Index: 1,
			Term:  1,
			Data:  []byte("hello"),
		},
	})
	if err != nil {
		t.Fatalf("encodeEntriesRecord() error = %v", err)
	}

	truncated := record[:len(record)-1]

	_, _, err = decodeRecord(bytes.NewReader(truncated))
	if err == nil {
		t.Fatal("decodeRecord() error = nil, want error")
	}
}
