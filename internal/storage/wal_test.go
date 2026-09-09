package storage

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/sanchar127/raftiq/internal/model"
)

func TestWALStoragePersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "raftiq.wal")

	storage, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL() error = %v", err)
	}

	state := model.PersistentState{
		CurrentTerm: 7,
		VotedFor:    "node-2",
	}

	entries := []model.LogEntry{
		{
			Index: 1,
			Term:  7,
			Data:  []byte("put:key=value"),
		},
		{
			Index: 2,
			Term:  7,
			Data:  []byte("put:other=value"),
		},
	}

	if err := storage.SaveState(state); err != nil {
		t.Fatalf("SaveState() error = %v", err)
	}

	if err := storage.AppendEntries(entries); err != nil {
		t.Fatalf("AppendEntries() error = %v", err)
	}

	if err := storage.Sync(); err != nil {
		t.Fatalf("Sync() error = %v", err)
	}

	if err := storage.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reopened, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("reopen WAL error = %v", err)
	}
	defer reopened.Close()

	gotState, err := reopened.LoadState()
	if err != nil {
		t.Fatalf("LoadState() error = %v", err)
	}

	if gotState != state {
		t.Fatalf("LoadState() = %+v, want %+v", gotState, state)
	}

	gotEntries, err := reopened.LoadEntries()
	if err != nil {
		t.Fatalf("LoadEntries() error = %v", err)
	}

	if len(gotEntries) != len(entries) {
		t.Fatalf(
			"LoadEntries() returned %d entries, want %d",
			len(gotEntries),
			len(entries),
		)
	}

	for i := range entries {
		if gotEntries[i].Index != entries[i].Index {
			t.Errorf(
				"entry %d index = %d, want %d",
				i,
				gotEntries[i].Index,
				entries[i].Index,
			)
		}

		if gotEntries[i].Term != entries[i].Term {
			t.Errorf(
				"entry %d term = %d, want %d",
				i,
				gotEntries[i].Term,
				entries[i].Term,
			)
		}

		if string(gotEntries[i].Data) != string(entries[i].Data) {
			t.Errorf(
				"entry %d data = %q, want %q",
				i,
				gotEntries[i].Data,
				entries[i].Data,
			)
		}
	}
}

func TestWALStorageCreatesFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "raftiq.wal")

	_, err := OpenWAL(path)
	if err == nil {
		t.Fatal("OpenWAL() error = nil, want error for missing directory")
	}

	if _, err := os.Stat(path); err == nil {
		t.Fatal("WAL file unexpectedly exists")
	}
}

func TestWALStorageRecoversValidRecordsBeforeTruncatedTail(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "raftiq.wal")

	storage, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL() error = %v", err)
	}

	state := model.PersistentState{
		CurrentTerm: 5,
		VotedFor:    "node-1",
	}

	entries := []model.LogEntry{
		{
			Index: 1,
			Term:  5,
			Data:  []byte("command-1"),
		},
		{
			Index: 2,
			Term:  5,
			Data:  []byte("command-2"),
		},
	}

	if err := storage.SaveState(state); err != nil {
		t.Fatalf("SaveState() error = %v", err)
	}

	if err := storage.AppendEntries(entries); err != nil {
		t.Fatalf("AppendEntries() error = %v", err)
	}

	if err := storage.Sync(); err != nil {
		t.Fatalf("Sync() error = %v", err)
	}

	if err := storage.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Simulate a crash while the next record was being written.
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("open WAL for corruption simulation: %v", err)
	}

	partialRecord, err := encodeEntriesRecord([]model.LogEntry{
		{
			Index: 3,
			Term:  5,
			Data:  []byte("incomplete"),
		},
	})
	if err != nil {
		file.Close()
		t.Fatalf("encodeEntriesRecord() error = %v", err)
	}

	// Write only part of the record.
	partialLength := len(partialRecord) / 2

	if _, err := file.Write(partialRecord[:partialLength]); err != nil {
		file.Close()
		t.Fatalf("write partial record: %v", err)
	}

	if err := file.Close(); err != nil {
		t.Fatalf("close WAL after partial write: %v", err)
	}

	reopened, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("reopen WAL error = %v", err)
	}
	defer reopened.Close()

	gotState, err := reopened.LoadState()
	if err != nil {
		t.Fatalf("LoadState() error = %v", err)
	}

	if gotState != state {
		t.Fatalf("LoadState() = %+v, want %+v", gotState, state)
	}

	gotEntries, err := reopened.LoadEntries()
	if err != nil {
		t.Fatalf("LoadEntries() error = %v", err)
	}

	if len(gotEntries) != len(entries) {
		t.Fatalf(
			"LoadEntries() returned %d entries, want %d",
			len(gotEntries),
			len(entries),
		)
	}

	for i := range entries {
		if gotEntries[i].Index != entries[i].Index {
			t.Errorf(
				"entry %d index = %d, want %d",
				i,
				gotEntries[i].Index,
				entries[i].Index,
			)
		}

		if gotEntries[i].Term != entries[i].Term {
			t.Errorf(
				"entry %d term = %d, want %d",
				i,
				gotEntries[i].Term,
				entries[i].Term,
			)
		}

		if string(gotEntries[i].Data) != string(entries[i].Data) {
			t.Errorf(
				"entry %d data = %q, want %q",
				i,
				gotEntries[i].Data,
				entries[i].Data,
			)
		}
	}
}

func TestWALStorageRejectsUnknownRecordType(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "raftiq.wal")

	storage, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL() error = %v", err)
	}

	if err := storage.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}

	unknownRecord, err := encodeRecord(99, []byte("invalid"))
	if err != nil {
		file.Close()
		t.Fatalf("encodeRecord() error = %v", err)
	}

	if _, err := file.Write(unknownRecord); err != nil {
		file.Close()
		t.Fatalf("write unknown record: %v", err)
	}

	if err := file.Close(); err != nil {
		t.Fatalf("close WAL: %v", err)
	}

	_, err = OpenWAL(path)
	if err == nil {
		t.Fatal("OpenWAL() error = nil, want unknown record error")
	}
}

func TestWALStorageRejectsMalformedStatePayload(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "raftiq.wal")

	record, err := encodeRecord(recordState, []byte{1, 2, 3})
	if err != nil {
		t.Fatalf("encodeRecord() error = %v", err)
	}

	if err := os.WriteFile(path, record, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	_, err = OpenWAL(path)
	if err == nil {
		t.Fatal("OpenWAL() error = nil, want malformed state error")
	}
}

func TestWALStorageRejectsMalformedEntriesPayload(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "raftiq.wal")

	record, err := encodeRecord(recordEntries, []byte{
		0,
		0,
		0,
		1,
	})
	if err != nil {
		t.Fatalf("encodeRecord() error = %v", err)
	}

	if err := os.WriteFile(path, record, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	_, err = OpenWAL(path)
	if err == nil {
		t.Fatal("OpenWAL() error = nil, want malformed entries error")
	}
}

func TestDecodeRecordRejectsChecksumMismatch(t *testing.T) {
	record, err := encodeEntriesRecord([]model.LogEntry{
		{
			Index: 1,
			Term:  1,
			Data:  []byte("hello"),
		},
	})
	if err != nil {
		t.Fatalf("encodeEntriesRecord() error = %v", err)
	}

	// Keep the record structurally valid but corrupt one payload byte.
	record[5] ^= 0xff

	_, _, err = decodeRecord(bytes.NewReader(record))
	if err == nil {
		t.Fatal("decodeRecord() error = nil, want checksum mismatch")
	}
}

func TestDecodeRecordRejectsOversizedPayload(t *testing.T) {
	var record bytes.Buffer

	if err := record.WriteByte(recordEntries); err != nil {
		t.Fatalf("WriteByte() error = %v", err)
	}

	if err := binary.Write(
		&record,
		binary.BigEndian,
		maxRecordPayloadSize+1,
	); err != nil {
		t.Fatalf("binary.Write() error = %v", err)
	}

	_, _, err := decodeRecord(bytes.NewReader(record.Bytes()))
	if err == nil {
		t.Fatal("decodeRecord() error = nil, want oversized payload error")
	}
}
