package observability

import (
	"errors"
	"fmt"

	"github.com/sanchar127/raftiq/internal/raft"
)

func RaftReadinessProbe(node *raft.RaftNode) HealthProbe {
	return func() error {
		if node == nil {
			return errors.New("raft node is nil")
		}

		if !node.IsRunning() {
			return fmt.Errorf("raft node %s is not running", node.ID())
		}

		return nil
	}
}
