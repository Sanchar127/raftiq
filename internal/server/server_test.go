package server

import (
	"testing"

	"github.com/sanchar127/raftiq/internal/kv"
	"github.com/sanchar127/raftiq/internal/raft"
)

func TestServerStartsAndStops(t *testing.T) {
	raftNode := raft.NewRaftNode("A")
	store := kv.NewStore()

	server := NewServer(raftNode, store)

	server.Start()
	server.Stop()
}
