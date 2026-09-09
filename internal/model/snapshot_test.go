package model

import (
	"bytes"
	"testing"
)

func TestSnapshot(t *testing.T) {
	data := []byte(`{"name":"raftiq"}`)

	snapshot := Snapshot{
		LastIncludedIndex: 10,
		LastIncludedTerm:  3,
		Data:              append([]byte(nil), data...),
	}

	if snapshot.LastIncludedIndex != 10 {
		t.Fatalf(
			"expected last included index 10, got %d",
			snapshot.LastIncludedIndex,
		)
	}

	if snapshot.LastIncludedTerm != 3 {
		t.Fatalf(
			"expected last included term 3, got %d",
			snapshot.LastIncludedTerm,
		)
	}

	if !bytes.Equal(snapshot.Data, data) {
		t.Fatalf("snapshot data does not match")
	}
}
