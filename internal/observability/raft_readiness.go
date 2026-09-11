package observability

import (
	"errors"

	"github.com/sanchar127/raftiq/internal/raft"
)

func RaftReadinessProbe(node *raft.RaftNode) HealthProbe {
	return func() error {
		if node == nil {
			return errors.New("raft node is nil")
		}

		return nil
	}
}
