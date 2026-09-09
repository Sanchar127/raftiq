package kv

import (
	"fmt"

	"github.com/sanchar127/raftiq/internal/raft"
)

func Apply(store *Store, entry raft.LogEntry) error {
	command, err := DecodeCommand(entry.Data)
	if err != nil {
		return fmt.Errorf("decode raft command at index %d: %w", entry.Index, err)
	}

	switch command.Type {
	case CommandPut:
		store.Put(command.Key, command.Value)
	case CommandDelete:
		store.Delete(command.Key)

	case CommandReadBarrier:
	
	default:
		return fmt.Errorf("unknown command type %q", command.Type)
	}

	return nil
}
