package kv

import (
	"testing"

	"github.com/sanchar127/raftiq/internal/raft"
)

func TestCommandRoundTrip(t *testing.T) {
	original := Command{
		Type:  CommandPut,
		Key:   "name",
		Value: []byte("raftiq"),
	}

	data, err := EncodeCommand(original)
	if err != nil {
		t.Fatal(err)
	}

	decoded, err := DecodeCommand(data)
	if err != nil {
		t.Fatal(err)
	}

	if decoded.Type != original.Type {
		t.Fatalf("expected type %q, got %q", original.Type, decoded.Type)
	}

	if decoded.Key != original.Key {
		t.Fatalf("expected key %q, got %q", original.Key, decoded.Key)
	}

	if string(decoded.Value) != string(original.Value) {
		t.Fatalf("expected value %q, got %q", original.Value, decoded.Value)
	}
}

func TestApplyPut(t *testing.T) {
	store := NewStore()

	command := Command{
		Type:  CommandPut,
		Key:   "name",
		Value: []byte("raftiq"),
	}

	data, err := EncodeCommand(command)
	if err != nil {
		t.Fatal(err)
	}

	entry := raft.LogEntry{
		Index: 1,
		Term:  1,
		Data:  data,
	}

	if err := Apply(store, entry); err != nil {
		t.Fatal(err)
	}

	value, ok := store.Get("name")
	if !ok {
		t.Fatal("expected key to exist")
	}

	if string(value) != "raftiq" {
		t.Fatalf("expected raftiq, got %q", value)
	}
}

func TestApplyDelete(t *testing.T) {
	store := NewStore()

	store.Put("name", []byte("raftiq"))

	command := Command{
		Type: CommandDelete,
		Key:  "name",
	}

	data, err := EncodeCommand(command)
	if err != nil {
		t.Fatal(err)
	}

	entry := raft.LogEntry{
		Index: 2,
		Term:  1,
		Data:  data,
	}

	if err := Apply(store, entry); err != nil {
		t.Fatal(err)
	}

	if _, ok := store.Get("name"); ok {
		t.Fatal("expected key to be deleted")
	}
}

func TestEncodeDecodeLockExpireCommand(t *testing.T) {
	original := Command{
		Type:         CommandLockExpire,
		Key:          "job-1",
		FencingToken: 41,
	}

	data, err := EncodeCommand(original)
	if err != nil {
		t.Fatalf("encode command: %v", err)
	}

	decoded, err := DecodeCommand(data)
	if err != nil {
		t.Fatalf("decode command: %v", err)
	}

	if decoded.Type != CommandLockExpire {
		t.Fatalf(
			"expected command type %q, got %q",
			CommandLockExpire,
			decoded.Type,
		)
	}

	if decoded.Key != original.Key {
		t.Fatalf(
			"expected key %q, got %q",
			original.Key,
			decoded.Key,
		)
	}

	if decoded.FencingToken != original.FencingToken {
		t.Fatalf(
			"expected fencing token %d, got %d",
			original.FencingToken,
			decoded.FencingToken,
		)
	}
}
