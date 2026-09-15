package raft

import (
	"context"
	"errors"
	"fmt"

	"github.com/sanchar127/raftiq/internal/model"
)

func (n *RaftNode) ReadIndex(ctx context.Context) (model.LogIndex, error) {
	if ctx == nil {
		return 0, errors.New("raft: ReadIndex context is nil")
	}

	n.mu.RLock()

	if n.state.Role != Leader {
		n.mu.RUnlock()
		return 0, errors.New("raft: not leader")
	}

	term := n.state.Persistent.CurrentTerm
	commitIndex := n.state.Volatile.CommitIndex
	transport := n.transport
	peerIDs := append([]NodeID(nil), n.peerIDs...)
	membership := n.state.Persistent.Membership

	// Raft §8 requires the leader to have committed an entry from
	// its current term before serving linearizable reads.
	hasCommittedInCurrentTerm := false

	if commitIndex > 0 {
		if entry, ok := n.log.Get(commitIndex); ok {
			hasCommittedInCurrentTerm = entry.Term == term
		}
	}

	n.mu.RUnlock()

	if !hasCommittedInCurrentTerm {
		return 0, errors.New(
			"raft: leader has not committed an entry in the current term yet",
		)
	}

	if transport == nil {
		return 0, errors.New("raft: transport is not configured")
	}

	// The leader counts as one acknowledgement.
	acks := map[NodeID]struct{}{
		n.id: {},
	}

	// A single-node cluster, or a configuration where the leader
	// already satisfies the quorum by itself.
	if membershipHasQuorum(membership, acks) {
		return commitIndex, nil
	}

	type result struct {
		peerID NodeID
		reply  AppendEntriesReply
		err    error
	}

	results := make(chan result, len(peerIDs))

	for _, peerID := range peerIDs {
		peerID := peerID

		go func() {
			n.mu.RLock()

			// ReadIndex is only valid while we remain leader in the
			// same term in which the operation started.
			if n.state.Role != Leader ||
				n.state.Persistent.CurrentTerm != term {
				n.mu.RUnlock()

				select {
				case results <- result{
					peerID: peerID,
					err: errors.New(
						"raft: leadership lost before ReadIndex RPC",
					),
				}:
				case <-ctx.Done():
				}

				return
			}

			args := AppendEntriesArgs{
				Term:         term,
				LeaderID:     n.id,
				LeaderCommit: commitIndex,
			}

			nextIndex, ok := n.state.Leader.NextIndex[peerID]
			if !ok {
				n.mu.RUnlock()

				select {
				case results <- result{
					peerID: peerID,
					err: fmt.Errorf(
						"peer %s has no replication state",
						peerID,
					),
				}:
				case <-ctx.Done():
				}

				return
			}

			// Build a heartbeat with the follower's current
			// replication position. No log entries are sent.
			if nextIndex > 1 {
				prevIndex := nextIndex - 1

				prevEntry, ok := n.log.Get(prevIndex)
				if !ok {
					n.mu.RUnlock()

					select {
					case results <- result{
						peerID: peerID,
						err: fmt.Errorf(
							"previous log entry %d for peer %s not found",
							prevIndex,
							peerID,
						),
					}:
					case <-ctx.Done():
					}

					return
				}

				args.PrevLogIndex = prevIndex
				args.PrevLogTerm = prevEntry.Term
			}

			n.mu.RUnlock()

			rpcCtx, cancel := n.rpcContext()
			defer cancel()

			// Make the RPC respect the caller's context as well.
			go func() {
				select {
				case <-ctx.Done():
					cancel()
				case <-rpcCtx.Done():
				}
			}()

			reply, err := transport.AppendEntries(
				rpcCtx,
				peerID,
				args,
			)

			select {
			case results <- result{
				peerID: peerID,
				reply:  reply,
				err:    err,
			}:
			case <-ctx.Done():
			}
		}()
	}

	pending := len(peerIDs)

	for pending > 0 {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()

		case r := <-results:
			pending--

			if r.err != nil {
				continue
			}

			reply := r.reply

			// A higher term proves that this leader is stale.
			if reply.Term > term {
				if err := n.becomeFollower(reply.Term); err != nil {
					return 0, fmt.Errorf(
						"raft: step down during ReadIndex: %w",
						err,
					)
				}

				return 0, errors.New(
					"raft: leadership lost during ReadIndex",
				)
			}

			// Only a successful response from our original term
			// counts toward the quorum.
			if reply.Term != term || !reply.Success {
				continue
			}

			acks[r.peerID] = struct{}{}

			if !membershipHasQuorum(membership, acks) {
				continue
			}

			// Quorum has confirmed that this leader is still
			// communicating in the same term. Re-check leadership
			// before returning the read index.
			n.mu.RLock()

			stillLeader :=
				n.state.Role == Leader &&
					n.state.Persistent.CurrentTerm == term

			safeIndex := n.state.Volatile.CommitIndex

			n.mu.RUnlock()

			if !stillLeader {
				return 0, errors.New(
					"raft: leadership lost during ReadIndex",
				)
			}

			// Never return an index beyond the commit index observed
			// when this ReadIndex operation started.
			if safeIndex > commitIndex {
				safeIndex = commitIndex
			}

			return safeIndex, nil
		}
	}

	return 0, errors.New("raft: ReadIndex quorum unavailable")
}