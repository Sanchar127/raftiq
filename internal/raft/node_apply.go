package raft

import (
	"context"
	"fmt"
	"time"

	"github.com/sanchar127/raftiq/internal/model"
)

func (n *RaftNode) applyConfigurationEntryLocked(
	entry LogEntry,
) error {
	if len(entry.Data) < 6 {
		return fmt.Errorf(
			"configuration entry at index %d is truncated",
			entry.Index,
		)
	}

	entryType := entry.Data[5]

	var nextMembership model.Membership

	switch entryType {
	case configurationEntryTypeStable:
		configuration, err := DecodeConfigurationEntry(entry.Data)
		if err != nil {
			return fmt.Errorf(
				"decode stable configuration entry at index %d: %w",
				entry.Index,
				err,
			)
		}

		nextMembership = model.Membership{
			Current: configuration,
			Joint:   nil,
		}

	case configurationEntryTypeEnterJoint:
		oldConfiguration, newConfiguration, err :=
			DecodeEnterJointConfigurationEntry(entry.Data)
		if err != nil {
			return fmt.Errorf(
				"decode enter-joint configuration entry at index %d: %w",
				entry.Index,
				err,
			)
		}

		nextMembership = model.Membership{
			Current: oldConfiguration,
			Joint: &model.JointConfiguration{
				Old: oldConfiguration,
				New: newConfiguration,
			},
		}

	case configurationEntryTypeLeaveJoint:
		newConfiguration, err :=
			DecodeLeaveJointConfigurationEntry(entry.Data)
		if err != nil {
			return fmt.Errorf(
				"decode leave-joint configuration entry at index %d: %w",
				entry.Index,
				err,
			)
		}

		nextMembership = model.Membership{
			Current: newConfiguration,
			Joint:   nil,
		}

	default:
		return fmt.Errorf(
			"unknown configuration entry type %d at index %d",
			entryType,
			entry.Index,
		)
	}

	nextState := n.state.Persistent
	nextState.Membership = nextMembership

	if err := n.storage.SaveState(nextState); err != nil {
		return fmt.Errorf(
			"persist membership at index %d: %w",
			entry.Index,
			err,
		)
	}

	if err := n.storage.Sync(); err != nil {
		return fmt.Errorf(
			"sync membership at index %d: %w",
			entry.Index,
			err,
		)
	}

	n.state.Persistent.Membership = nextMembership

	// A leader remains active during joint consensus because the
	// old configuration still includes the leader.
	//
	// Once the final stable configuration no longer contains the
	// local node, the leader must step down.
	if n.state.Role == Leader &&
		nextMembership.Joint == nil &&
		!membershipIsVoter(nextMembership, n.id) {
		if err := n.becomeFollowerLocked(
			n.state.Persistent.CurrentTerm,
		); err != nil {
			return fmt.Errorf(
				"step down after membership removal: %w",
				err,
			)
		}
	}

	return nil
}

func (n *RaftNode) applyCommitted() {
	n.applyMu.Lock()
	defer n.applyMu.Unlock()

	for {
		n.mu.Lock()

		if n.state.Volatile.LastApplied >=
			n.state.Volatile.CommitIndex {
			n.mu.Unlock()
			return
		}

		nextIndex := n.state.Volatile.LastApplied + 1

		entry, ok := n.log.Get(nextIndex)
		if !ok {
			n.mu.Unlock()
			return
		}

		if IsConfigurationEntry(entry.Data) {
			if err := n.applyConfigurationEntryLocked(entry); err != nil {
				n.mu.Unlock()

				n.getLogger().Error(
					"failed to apply raft configuration entry",
					"index", entry.Index,
					"term", entry.Term,
					"error", err,
				)

				return
			}

			n.state.Volatile.LastApplied = nextIndex
			n.updateStateMetricsLocked()

			n.mu.Unlock()

			continue
		}

		n.mu.Unlock()

		n.applyCh <- entry

		n.mu.Lock()

		if n.state.Volatile.LastApplied < nextIndex {
			n.state.Volatile.LastApplied = nextIndex
			n.updateStateMetricsLocked()
		}

		n.mu.Unlock()
	}
}

func (n *RaftNode) WaitApplied(
	ctx context.Context,
	index LogIndex,
) error {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	for {
		n.mu.RLock()
		applied := n.state.Volatile.LastApplied >= index
		if applied {
			n.mu.RUnlock()
			return nil
		}
		n.mu.RUnlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (n *RaftNode) waitForApplied(
	ctx context.Context,
	targetIndex LogIndex,
) error {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	for {
		n.mu.RLock()

		lastApplied := n.state.Volatile.LastApplied
		role := n.state.Role

		n.mu.RUnlock()

		if lastApplied >= targetIndex {
			return nil
		}

		if role != Leader {
			return fmt.Errorf(
				"leader lost while waiting for index %d to apply: role=%v",
				targetIndex,
				role,
			)
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf(
				"timed out waiting for index %d to apply: %w",
				targetIndex,
				ctx.Err(),
			)
		case <-ticker.C:
		}
	}
}
