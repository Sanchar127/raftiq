package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"syscall"

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

	if !reflect.DeepEqual(gotState, state) {
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

func TestWALStoragePersistsMembershipAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "raftiq-membership.wal")

	storage, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL() error = %v", err)
	}

	state := model.PersistentState{
		CurrentTerm: 11,
		VotedFor:    "node-2",
		Membership: model.Membership{
			Current: model.Configuration{
				Voters: []model.NodeID{
					"node-1",
					"node-2",
					"node-3",
				},
			},
		},
	}

	if err := storage.SaveState(state); err != nil {
		_ = storage.Close()
		t.Fatalf("SaveState() error = %v", err)
	}

	if err := storage.Sync(); err != nil {
		_ = storage.Close()
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

	got, err := reopened.LoadState()
	if err != nil {
		t.Fatalf("LoadState() error = %v", err)
	}

	if !reflect.DeepEqual(got, state) {
		t.Fatalf(
			"LoadState() = %+v, want %+v",
			got,
			state,
		)
	}
}

func TestWALStoragePersistsJointMembershipAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "raftiq-joint-membership.wal")

	storage, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL() error = %v", err)
	}

	state := model.PersistentState{
		CurrentTerm: 12,
		VotedFor:    "node-1",
		Membership: model.Membership{
			Current: model.Configuration{
				Voters: []model.NodeID{
					"node-1",
					"node-2",
					"node-3",
				},
			},
			Joint: &model.JointConfiguration{
				Old: model.Configuration{
					Voters: []model.NodeID{
						"node-1",
						"node-2",
						"node-3",
					},
				},
				New: model.Configuration{
					Voters: []model.NodeID{
						"node-1",
						"node-2",
						"node-3",
						"node-4",
					},
				},
			},
		},
	}

	if err := storage.SaveState(state); err != nil {
		_ = storage.Close()
		t.Fatalf("SaveState() error = %v", err)
	}

	if err := storage.Sync(); err != nil {
		_ = storage.Close()
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

	got, err := reopened.LoadState()
	if err != nil {
		t.Fatalf("LoadState() error = %v", err)
	}

	if !reflect.DeepEqual(got, state) {
		t.Fatalf(
			"LoadState() = %+v, want %+v",
			got,
			state,
		)
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

	if !reflect.DeepEqual(gotState, state) {
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

func TestWALStorageReplaceSuffix(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "raftiq.wal")

	storage, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL() error = %v", err)
	}
	defer storage.Close()

	initial := []model.LogEntry{
		{Index: 1, Term: 1, Data: []byte("one")},
		{Index: 2, Term: 1, Data: []byte("two")},
		{Index: 3, Term: 2, Data: []byte("old-three")},
		{Index: 4, Term: 2, Data: []byte("old-four")},
	}

	if err := storage.AppendEntries(initial); err != nil {
		t.Fatalf("AppendEntries() error = %v", err)
	}

	replacement := []model.LogEntry{
		{Index: 3, Term: 3, Data: []byte("new-three")},
		{Index: 4, Term: 3, Data: []byte("new-four")},
		{Index: 5, Term: 3, Data: []byte("new-five")},
	}

	if err := storage.ReplaceSuffix(3, replacement); err != nil {
		t.Fatalf("ReplaceSuffix() error = %v", err)
	}

	got, err := storage.LoadEntries()
	if err != nil {
		t.Fatalf("LoadEntries() error = %v", err)
	}

	want := replacement

	want = append(
		[]model.LogEntry{
			{Index: 1, Term: 1, Data: []byte("one")},
			{Index: 2, Term: 1, Data: []byte("two")},
		},
		want...,
	)

	assertEntriesEqual(t, got, want)
}
func assertEntriesEqual(
	t *testing.T,
	got []model.LogEntry,
	want []model.LogEntry,
) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf(
			"got %d entries, want %d",
			len(got),
			len(want),
		)
	}

	for i := range want {
		if got[i].Index != want[i].Index {
			t.Errorf(
				"entry %d index = %d, want %d",
				i,
				got[i].Index,
				want[i].Index,
			)
		}

		if got[i].Term != want[i].Term {
			t.Errorf(
				"entry %d term = %d, want %d",
				i,
				got[i].Term,
				want[i].Term,
			)
		}

		if !bytes.Equal(got[i].Data, want[i].Data) {
			t.Errorf(
				"entry %d data = %q, want %q",
				i,
				got[i].Data,
				want[i].Data,
			)
		}
	}
}
func TestWALStorageReplaceSuffixSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "raftiq.wal")

	storage, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL() error = %v", err)
	}

	initial := []model.LogEntry{
		{Index: 1, Term: 1, Data: []byte("one")},
		{Index: 2, Term: 1, Data: []byte("two")},
		{Index: 3, Term: 2, Data: []byte("old")},
	}

	if err := storage.AppendEntries(initial); err != nil {
		t.Fatalf("AppendEntries() error = %v", err)
	}

	replacement := []model.LogEntry{
		{Index: 3, Term: 3, Data: []byte("new")},
		{Index: 4, Term: 3, Data: []byte("four")},
	}

	if err := storage.ReplaceSuffix(3, replacement); err != nil {
		t.Fatalf("ReplaceSuffix() error = %v", err)
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

	got, err := reopened.LoadEntries()
	if err != nil {
		t.Fatalf("LoadEntries() error = %v", err)
	}

	want := []model.LogEntry{
		{Index: 1, Term: 1, Data: []byte("one")},
		{Index: 2, Term: 1, Data: []byte("two")},
		{Index: 3, Term: 3, Data: []byte("new")},
		{Index: 4, Term: 3, Data: []byte("four")},
	}

	assertEntriesEqual(t, got, want)
}
func TestWALStorageReplaceSuffixTruncatesOnly(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "raftiq.wal")

	storage, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL() error = %v", err)
	}
	defer storage.Close()

	entries := []model.LogEntry{
		{Index: 1, Term: 1, Data: []byte("one")},
		{Index: 2, Term: 1, Data: []byte("two")},
		{Index: 3, Term: 1, Data: []byte("three")},
	}

	if err := storage.AppendEntries(entries); err != nil {
		t.Fatalf("AppendEntries() error = %v", err)
	}

	if err := storage.ReplaceSuffix(3, nil); err != nil {
		t.Fatalf("ReplaceSuffix() error = %v", err)
	}

	got, err := storage.LoadEntries()
	if err != nil {
		t.Fatalf("LoadEntries() error = %v", err)
	}

	want := entries[:2]

	assertEntriesEqual(t, got, want)
}
func TestWALStorageRejectsNonContiguousAppend(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "raftiq.wal")

	storage, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL() error = %v", err)
	}
	defer storage.Close()

	initial := []model.LogEntry{
		{Index: 1, Term: 1, Data: []byte("one")},
		{Index: 2, Term: 1, Data: []byte("two")},
	}

	if err := storage.AppendEntries(initial); err != nil {
		t.Fatalf("initial AppendEntries() error = %v", err)
	}

	err = storage.AppendEntries([]model.LogEntry{
		{Index: 4, Term: 1, Data: []byte("four")},
	})

	if err == nil {
		t.Fatal("AppendEntries() error = nil, want invalid append error")
	}
}

func TestWALStorageRejectsOperationsAfterClose(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "raftiq.wal")

	storage, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL() error = %v", err)
	}

	if err := storage.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if err := storage.Sync(); !errors.Is(err, ErrClosedStorage) {
		t.Fatalf("Sync() error = %v, want ErrClosedStorage", err)
	}

	if err := storage.SaveState(model.PersistentState{}); !errors.Is(err, ErrClosedStorage) {
		t.Fatalf("SaveState() error = %v, want ErrClosedStorage", err)
	}

	if _, err := storage.LoadEntries(); !errors.Is(err, ErrClosedStorage) {
		t.Fatalf("LoadEntries() error = %v, want ErrClosedStorage", err)
	}
}
func TestWALStorageRejectsCorruptedRecordOnRecovery(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "raftiq.wal")

	storage, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL() error = %v", err)
	}

	entries := []model.LogEntry{
		{
			Index: 1,
			Term:  1,
			Data:  []byte("command-1"),
		},
		{
			Index: 2,
			Term:  1,
			Data:  []byte("command-2"),
		},
		{
			Index: 3,
			Term:  1,
			Data:  []byte("command-3"),
		},
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

	// Reconstruct the exact encoded record that was written.
	record, err := encodeEntriesRecord(entries)
	if err != nil {
		t.Fatalf("encodeEntriesRecord() error = %v", err)
	}

	walData, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}

	offset := bytes.Index(walData, record)
	if offset < 0 {
		t.Fatal("encoded entries record not found in WAL")
	}

	// Corrupt a byte inside the payload while leaving the record
	// structurally complete. The stored CRC is now invalid.
	payloadOffset := offset + recordHeaderSize

	if payloadOffset >= offset+len(record)-recordFooterSize {
		t.Fatal("calculated corruption offset is outside record payload")
	}

	walData[payloadOffset] ^= 0xff

	if err := os.WriteFile(path, walData, 0o600); err != nil {
		t.Fatalf("WriteFile() corrupted WAL error = %v", err)
	}

	_, err = OpenWAL(path)
	if err == nil {
		t.Fatal("OpenWAL() error = nil, want checksum corruption error")
	}

	if !bytes.Contains(
		[]byte(err.Error()),
		[]byte("checksum mismatch"),
	) {
		t.Fatalf(
			"OpenWAL() error = %v, want checksum mismatch",
			err,
		)
	}
}

func TestWALCompactRemovesSnapshotPrefixAndRecovers(t *testing.T) {
	path := filepath.Join(
		t.TempDir(),
		"raftiq.wal",
	)

	storage, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}

	state := model.PersistentState{
		CurrentTerm: 7,
		VotedFor:    "node-2",
	}

	if err := storage.SaveState(state); err != nil {
		t.Fatalf("save state: %v", err)
	}

	entries := make([]model.LogEntry, 0, 10)

	for index := model.LogIndex(1); index <= 10; index++ {
		entries = append(entries, model.LogEntry{
			Index: index,
			Term:  7,
			Data:  []byte(fmt.Sprintf("entry-%d", index)),
		})
	}

	if err := storage.AppendEntries(entries); err != nil {
		t.Fatalf("append entries: %v", err)
	}

	snapshot := model.Snapshot{
		LastIncludedIndex: 5,
		LastIncludedTerm:  7,
		Data:              []byte(`{"value":"snapshot-state"}`),
	}

	if err := storage.SaveSnapshot(snapshot); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}

	if err := storage.Sync(); err != nil {
		t.Fatalf("sync before compaction: %v", err)
	}

	if err := storage.Compact(snapshot); err != nil {
		t.Fatalf("compact WAL: %v", err)
	}

	if err := storage.Close(); err != nil {
		t.Fatalf("close WAL: %v", err)
	}

	reopened, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("reopen WAL: %v", err)
	}

	defer func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("close reopened WAL: %v", err)
		}
	}()

	recoveredState, err := reopened.LoadState()
	if err != nil {
		t.Fatalf("load recovered state: %v", err)
	}

	if !reflect.DeepEqual(recoveredState, state) {
		t.Fatalf(
			"recovered state mismatch: got %#v, want %#v",
			recoveredState,
			state,
		)
	}

	recoveredSnapshot, err := reopened.LoadSnapshot()
	if err != nil {
		t.Fatalf("load recovered snapshot: %v", err)
	}

	if !reflect.DeepEqual(recoveredSnapshot, snapshot) {
		t.Fatalf(
			"recovered snapshot mismatch: got %#v, want %#v",
			recoveredSnapshot,
			snapshot,
		)
	}

	recoveredEntries, err := reopened.LoadEntries()
	if err != nil {
		t.Fatalf("load recovered entries: %v", err)
	}

	if len(recoveredEntries) != 5 {
		t.Fatalf(
			"recovered entry count: got %d, want 5",
			len(recoveredEntries),
		)
	}

	for i, entry := range recoveredEntries {
		expectedIndex := model.LogIndex(i + 6)

		if entry.Index != expectedIndex {
			t.Errorf(
				"entry %d index: got %d, want %d",
				i,
				entry.Index,
				expectedIndex,
			)
		}

		expectedData := []byte(
			fmt.Sprintf("entry-%d", expectedIndex),
		)

		if !bytes.Equal(entry.Data, expectedData) {
			t.Errorf(
				"entry %d data: got %q, want %q",
				i,
				entry.Data,
				expectedData,
			)
		}
	}
}

func TestWALCompactReducesFileSize(t *testing.T) {
	path := filepath.Join(
		t.TempDir(),
		"raftiq.wal",
	)

	storage, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}

	state := model.PersistentState{
		CurrentTerm: 3,
		VotedFor:    "node-1",
	}

	if err := storage.SaveState(state); err != nil {
		t.Fatalf("save state: %v", err)
	}

	entries := make([]model.LogEntry, 0, 100)

	for index := model.LogIndex(1); index <= 100; index++ {
		entries = append(entries, model.LogEntry{
			Index: index,
			Term:  3,
			Data: []byte(fmt.Sprintf(
				"large-entry-payload-%d-%s",
				index,
				string(make([]byte, 100)),
			)),
		})
	}

	if err := storage.AppendEntries(entries); err != nil {
		t.Fatalf("append entries: %v", err)
	}

	snapshot := model.Snapshot{
		LastIncludedIndex: 90,
		LastIncludedTerm:  3,
		Data:              []byte("snapshot-state"),
	}

	if err := storage.SaveSnapshot(snapshot); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}

	if err := storage.Sync(); err != nil {
		t.Fatalf("sync WAL: %v", err)
	}

	before, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat WAL before compaction: %v", err)
	}

	if err := storage.Compact(snapshot); err != nil {
		t.Fatalf("compact WAL: %v", err)
	}

	after, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat WAL after compaction: %v", err)
	}

	if after.Size() >= before.Size() {
		t.Fatalf(
			"WAL did not shrink: before=%d after=%d",
			before.Size(),
			after.Size(),
		)
	}

	if err := storage.Close(); err != nil {
		t.Fatalf("close WAL: %v", err)
	}
}

func TestWALStorageRejectsTooManyMembershipVoters(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "too-many-voters.wal")

	storage, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL() error = %v", err)
	}

	statePayload := new(bytes.Buffer)

	if err := binary.Write(
		statePayload,
		binary.BigEndian,
		uint64(1),
	); err != nil {
		t.Fatalf("encode term: %v", err)
	}

	if err := binary.Write(
		statePayload,
		binary.BigEndian,
		uint32(0),
	); err != nil {
		t.Fatalf("encode voted-for length: %v", err)
	}

	if err := binary.Write(
		statePayload,
		binary.BigEndian,
		stateMembershipMagic,
	); err != nil {
		t.Fatalf("encode membership magic: %v", err)
	}

	if err := statePayload.WriteByte(stateMembershipVersion); err != nil {
		t.Fatalf("encode membership version: %v", err)
	}

	if err := binary.Write(
		statePayload,
		binary.BigEndian,
		uint32(maxWALConfigurationVoters+1),
	); err != nil {
		t.Fatalf("encode voter count: %v", err)
	}

	if err := statePayload.WriteByte(0); err != nil {
		t.Fatalf("encode joint flag: %v", err)
	}

	record, err := encodeRecord(recordState, statePayload.Bytes())
	if err != nil {
		_ = storage.Close()
		t.Fatalf("encode record: %v", err)
	}

	if err := storage.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if err := os.WriteFile(path, record, 0o600); err != nil {
		t.Fatalf("write corrupted WAL: %v", err)
	}

	if _, err := OpenWAL(path); err == nil {
		t.Fatal("OpenWAL() error = nil, want oversized voter count error")
	}
}

func TestWALCompactRejectsClosedStorage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raftiq.wal")

	storage, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}

	if err := storage.Close(); err != nil {
		t.Fatalf("close WAL: %v", err)
	}

	err = storage.Compact(model.Snapshot{
		LastIncludedIndex: 1,
		LastIncludedTerm:  1,
	})

	if !errors.Is(err, ErrClosedStorage) {
		t.Fatalf("Compact() error = %v, want ErrClosedStorage", err)
	}
}

func TestWALCompactRejectsMissingPersistedSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raftiq.wal")

	storage, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}
	defer func() {
		if err := storage.Close(); err != nil {
			t.Errorf("close WAL: %v", err)
		}
	}()

	snapshot := model.Snapshot{
		LastIncludedIndex: 5,
		LastIncludedTerm:  1,
		Data:              []byte("snapshot"),
	}

	err = storage.Compact(snapshot)
	if err == nil {
		t.Fatal("Compact() error = nil, want missing persisted snapshot error")
	}

	if !strings.Contains(err.Error(), "without persisted snapshot") {
		t.Fatalf(
			"Compact() error = %v, want missing persisted snapshot error",
			err,
		)
	}
}

func TestWALCompactRejectsSnapshotMismatchWithoutChangingState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raftiq.wal")

	storage, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}
	defer func() {
		if err := storage.Close(); err != nil {
			t.Errorf("close WAL: %v", err)
		}
	}()

	state := model.PersistentState{
		CurrentTerm: 3,
		VotedFor:    "node-1",
	}

	if err := storage.SaveState(state); err != nil {
		t.Fatalf("save state: %v", err)
	}

	entries := []model.LogEntry{
		{
			Index: 1,
			Term:  3,
			Data:  []byte("entry-1"),
		},
		{
			Index: 2,
			Term:  3,
			Data:  []byte("entry-2"),
		},
		{
			Index: 3,
			Term:  3,
			Data:  []byte("entry-3"),
		},
	}

	if err := storage.AppendEntries(entries); err != nil {
		t.Fatalf("append entries: %v", err)
	}

	persistedSnapshot := model.Snapshot{
		LastIncludedIndex: 2,
		LastIncludedTerm:  3,
		Data:              []byte("snapshot"),
	}

	if err := storage.SaveSnapshot(persistedSnapshot); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}

	mismatchedSnapshot := model.Snapshot{
		LastIncludedIndex: 2,
		LastIncludedTerm:  99,
		Data:              []byte("different"),
	}

	err = storage.Compact(mismatchedSnapshot)
	if err == nil {
		t.Fatal("Compact() error = nil, want snapshot term mismatch")
	}

	if !strings.Contains(err.Error(), "snapshot term mismatch") {
		t.Fatalf(
			"Compact() error = %v, want snapshot term mismatch",
			err,
		)
	}

	recoveredEntries, err := storage.LoadEntries()
	if err != nil {
		t.Fatalf("load entries after rejected compaction: %v", err)
	}

	if !reflect.DeepEqual(recoveredEntries, entries) {
		t.Fatalf(
			"entries changed after rejected compaction: got %#v, want %#v",
			recoveredEntries,
			entries,
		)
	}

	recoveredSnapshot, err := storage.LoadSnapshot()
	if err != nil {
		t.Fatalf("load snapshot after rejected compaction: %v", err)
	}

	if !reflect.DeepEqual(recoveredSnapshot, persistedSnapshot) {
		t.Fatalf(
			"snapshot changed after rejected compaction: got %#v, want %#v",
			recoveredSnapshot,
			persistedSnapshot,
		)
	}
}

func TestWALStorageRecoversFromTruncatedRecordHeader(t *testing.T) {
	testCases := []struct {
		name string
		tail []byte
	}{
		{
			name: "partial type",
			tail: []byte{recordState},
		},
		{
			name: "partial length",
			tail: []byte{
				recordState,
				0x00,
				0x00,
			},
		},
		{
			name: "complete header",
			tail: []byte{
				recordState,
				0x00,
				0x00,
				0x00,
				0x01,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "raftiq.wal")

			storage, err := OpenWAL(path)
			if err != nil {
				t.Fatalf("OpenWAL() error = %v", err)
			}

			state := model.PersistentState{
				CurrentTerm: 9,
				VotedFor:    "node-1",
			}

			if err := storage.SaveState(state); err != nil {
				t.Fatalf("SaveState() error = %v", err)
			}

			if err := storage.Sync(); err != nil {
				t.Fatalf("Sync() error = %v", err)
			}

			if err := storage.Close(); err != nil {
				t.Fatalf("Close() error = %v", err)
			}

			file, err := os.OpenFile(
				path,
				os.O_WRONLY|os.O_APPEND,
				0o600,
			)
			if err != nil {
				t.Fatalf("open WAL for partial header: %v", err)
			}

			if _, err := file.Write(tc.tail); err != nil {
				_ = file.Close()
				t.Fatalf("write partial header: %v", err)
			}

			if err := file.Close(); err != nil {
				t.Fatalf("close WAL after partial header: %v", err)
			}

			reopened, err := OpenWAL(path)
			if err != nil {
				t.Fatalf("OpenWAL() error = %v", err)
			}
			defer reopened.Close()

			gotState, err := reopened.LoadState()
			if err != nil {
				t.Fatalf("LoadState() error = %v", err)
			}

			if !reflect.DeepEqual(gotState, state) {
				t.Fatalf(
					"LoadState() = %+v, want %+v",
					gotState,
					state,
				)
			}

			info, err := os.Stat(path)
			if err != nil {
				t.Fatalf("stat WAL: %v", err)
			}

			expectedRecord, err := encodeStateRecord(state)
			if err != nil {
				t.Fatalf("encodeStateRecord() error = %v", err)
			}

			expectedSize := int64(len(expectedRecord))

			if info.Size() != expectedSize {
				t.Fatalf(
					"WAL size = %d, want %d after truncating partial header",
					info.Size(),
					expectedSize,
				)
			}
		})
	}
}

func TestWALStorageRollsBackFailedPartialWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "raftiq.wal")

	storage, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL() error = %v", err)
	}
	defer storage.Close()

	firstEntry := model.LogEntry{
		Index: 1,
		Term:  1,
		Data:  []byte("first"),
	}

	if err := storage.AppendEntries([]model.LogEntry{firstEntry}); err != nil {
		t.Fatalf("AppendEntries(first) error = %v", err)
	}

	originalWriteFn := storage.writeFn
	writeErr := errors.New("injected partial write failure")

	storage.writeFn = func(file *os.File, data []byte) error {
		partial := len(data) / 2
		if partial == 0 {
			partial = 1
		}

		if _, err := file.Write(data[:partial]); err != nil {
			return err
		}

		return writeErr
	}

	secondEntry := model.LogEntry{
		Index: 2,
		Term:  1,
		Data:  []byte("second"),
	}

	err = storage.AppendEntries([]model.LogEntry{secondEntry})
	if !errors.Is(err, writeErr) {
		t.Fatalf("AppendEntries() error = %v, want %v", err, writeErr)
	}

	storage.writeFn = originalWriteFn

	thirdEntry := model.LogEntry{
		Index: 2,
		Term:  1,
		Data:  []byte("third"),
	}

	if err := storage.AppendEntries([]model.LogEntry{thirdEntry}); err != nil {
		t.Fatalf("AppendEntries(third) error = %v", err)
	}

	entries, err := storage.LoadEntries()
	if err != nil {
		t.Fatalf("LoadEntries() error = %v", err)
	}

	want := []model.LogEntry{
		firstEntry,
		thirdEntry,
	}

	if !reflect.DeepEqual(entries, want) {
		t.Fatalf("LoadEntries() = %#v, want %#v", entries, want)
	}

	if err := storage.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reopened, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL() after rollback error = %v", err)
	}
	defer reopened.Close()

	entries, err = reopened.LoadEntries()
	if err != nil {
		t.Fatalf("LoadEntries() after reopen error = %v", err)
	}

	if !reflect.DeepEqual(entries, want) {
		t.Fatalf(
			"LoadEntries() after reopen = %#v, want %#v",
			entries,
			want,
		)
	}
}

func TestWALStorageWriteDiskFull(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "raftiq.wal")

	storage, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL() error = %v", err)
	}
	defer storage.Close()

	storage.writeFn = func(_ *os.File, _ []byte) error {
		return syscall.ENOSPC
	}

	entry := model.LogEntry{
		Index: 1,
		Term:  1,
		Data:  []byte("disk-full"),
	}

	err = storage.AppendEntries([]model.LogEntry{entry})
	if !errors.Is(err, ErrWALDiskFull) {
		t.Fatalf("AppendEntries() error = %v, want ErrWALDiskFull", err)
	}

	if !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("AppendEntries() error = %v, want wrapped syscall.ENOSPC", err)
	}

	entries, err := storage.LoadEntries()
	if err != nil {
		t.Fatalf("LoadEntries() error = %v", err)
	}

	if len(entries) != 0 {
		t.Fatalf("LoadEntries() returned %d entries, want 0", len(entries))
	}
}
