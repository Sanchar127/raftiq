package transport

import (
	"context"
	"errors"
	"fmt"

	"github.com/sanchar127/raftiq/internal/raft"
)

func contextError(ctx context.Context) error {
	if ctx == nil {
		return errors.New("transport context is nil")
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func peerServerName(id raft.NodeID) string {
	return fmt.Sprintf("%s.raftiq", id)
}
