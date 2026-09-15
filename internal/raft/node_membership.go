package raft

import (
	"context"
	"errors"
	"fmt"

	"github.com/sanchar127/raftiq/internal/model"
)

func (n *RaftNode) BootstrapMembership() error {
	logger := n.getLogger()

	n.mu.Lock()

	// A persisted membership is authoritative. Never overwrite it
	// from the current transport topology.
	if len(n.state.Persistent.Membership.Current.Voters) > 0 ||
		n.state.Persistent.Membership.Joint != nil {
		n.mu.Unlock()
		return nil
	}

	voters := make([]NodeID, 0, len(n.peerIDs)+1)
	seen := make(map[NodeID]struct{}, len(n.peerIDs)+1)

	addVoter := func(id NodeID) {
		if id == "" {
			return
		}
		if _, exists := seen[id]; exists {
			return
		}
		seen[id] = struct{}{}
		voters = append(voters, id)
	}

	addVoter(n.id)
	for _, peerID := range n.peerIDs {
		addVoter(peerID)
	}

	if len(voters) == 0 {
		n.mu.Unlock()
		return errors.New("raft membership cannot be bootstrapped without voters")
	}

	n.state.Persistent.Membership = model.Membership{
		Current: model.Configuration{
			Voters: voters,
		},
	}

	if err := n.persistStateLocked(); err != nil {
		n.mu.Unlock()
		return fmt.Errorf("bootstrap membership: %w", err)
	}

	n.mu.Unlock()

	logger.Info(
		"raft membership bootstrapped",
		"voters", voters,
	)

	return nil
}

func (n *RaftNode) catchUpPeer(
	ctx context.Context,
	peerID NodeID,
	targetIndex LogIndex,
) error {
	for {
		n.mu.RLock()

		if n.state.Role != Leader {
			role := n.state.Role
			n.mu.RUnlock()

			return fmt.Errorf(
				"leader lost while catching up peer %s: role=%v",
				peerID,
				role,
			)
		}

		matchIndex := n.state.Leader.MatchIndex[peerID]

		n.mu.RUnlock()

		if matchIndex >= targetIndex {
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf(
				"timed out catching up peer %s to index %d: %w",
				peerID,
				targetIndex,
				ctx.Err(),
			)
		default:
		}

		n.replicateTo(peerID)
	}
}

func (n *RaftNode) AddMember(
	ctx context.Context,
	peerID NodeID,
) error {
	if peerID == "" {
		return errors.New("raft member ID is required")
	}

	n.mu.Lock()

	if n.state.Role != Leader {
		role := n.state.Role
		n.mu.Unlock()

		return fmt.Errorf(
			"cannot add member %s: node is not leader (role=%v)",
			peerID,
			role,
		)
	}

	if peerID == n.id {
		n.mu.Unlock()

		return fmt.Errorf(
			"cannot add member %s: member is the local node",
			peerID,
		)
	}

	membership := n.state.Persistent.Membership

	if membership.Joint != nil {
		n.mu.Unlock()

		return errors.New(
			"cannot add member while membership is in joint configuration",
		)
	}

	if membershipIsVoter(membership, peerID) {
		n.mu.Unlock()

		return fmt.Errorf(
			"member %s is already a voter",
			peerID,
		)
	}

	registered := false
	for _, existingID := range n.peerIDs {
		if existingID == peerID {
			registered = true
			break
		}
	}

	if !registered {
		n.mu.Unlock()

		return fmt.Errorf(
			"member %s is not registered as a raft peer",
			peerID,
		)
	}

	oldConfiguration := membership.Current

	newVoters := append(
		[]NodeID(nil),
		oldConfiguration.Voters...,
	)
	newVoters = append(newVoters, peerID)

	newConfiguration := model.Configuration{
		Voters: newVoters,
	}

	n.initializeNewPeerReplicationStateLocked(peerID)

	n.mu.Unlock()

	// Capture the current log boundary. The new peer must first catch
	// up with everything that already exists before entering joint
	// consensus.
	n.mu.RLock()
	targetIndex := n.log.LastIndex()
	n.mu.RUnlock()

	if err := n.catchUpPeer(ctx, peerID, targetIndex); err != nil {
		return fmt.Errorf(
			"catch up new member %s: %w",
			peerID,
			err,
		)
	}

	enterJointData, err := EncodeEnterJointConfigurationEntry(
		oldConfiguration,
		newConfiguration,
	)
	if err != nil {
		return fmt.Errorf(
			"encode enter-joint configuration for member %s: %w",
			peerID,
			err,
		)
	}

	enterJointIndex, err := n.Propose(enterJointData)
	if err != nil {
		return fmt.Errorf(
			"propose enter-joint configuration for member %s: %w",
			peerID,
			err,
		)
	}

	if err := n.waitForApplied(ctx, enterJointIndex); err != nil {
		return fmt.Errorf(
			"wait for enter-joint configuration at index %d: %w",
			enterJointIndex,
			err,
		)
	}

	// Propose() may commit EnterJoint after D has already replied to
	// its replication RPC. Send another replication so D receives the
	// updated LeaderCommit and can apply the joint configuration.
	n.replicateTo(peerID)

	n.mu.RLock()
	jointTargetIndex := n.log.LastIndex()
	n.mu.RUnlock()

	if err := n.catchUpPeer(ctx, peerID, jointTargetIndex); err != nil {
		return fmt.Errorf(
			"catch up member %s through joint configuration: %w",
			peerID,
			err,
		)
	}

	leaveJointData, err := EncodeLeaveJointConfigurationEntry(
		newConfiguration,
	)
	if err != nil {
		return fmt.Errorf(
			"encode leave-joint configuration for member %s: %w",
			peerID,
			err,
		)
	}

	leaveJointIndex, err := n.Propose(leaveJointData)
	if err != nil {
		return fmt.Errorf(
			"propose leave-joint configuration for member %s: %w",
			peerID,
			err,
		)
	}

	if err := n.waitForApplied(ctx, leaveJointIndex); err != nil {
		return fmt.Errorf(
			"wait for leave-joint configuration at index %d: %w",
			leaveJointIndex,
			err,
		)
	}

	n.mu.RLock()
	finalMembership := n.state.Persistent.Membership
	n.mu.RUnlock()

	if finalMembership.Joint != nil {
		return fmt.Errorf(
			"member %s added but membership is still joint",
			peerID,
		)
	}

	if !configurationContainsVoter(
		finalMembership.Current,
		peerID,
	) {
		return fmt.Errorf(
			"member %s was not present in final membership",
			peerID,
		)
	}

	return nil
}

func (n *RaftNode) RemoveMember(
	ctx context.Context,
	peerID NodeID,
) error {
	if peerID == "" {
		return errors.New("raft member ID is required")
	}

	if ctx == nil {
		return errors.New("raft: RemoveMember context is nil")
	}

	n.mu.Lock()

	if n.state.Role != Leader {
		role := n.state.Role
		n.mu.Unlock()

		return fmt.Errorf(
			"cannot remove member %s: node is not leader (role=%v)",
			peerID,
			role,
		)
	}

	if peerID == n.id {
		n.mu.Unlock()

		return fmt.Errorf(
			"cannot remove self %s: transfer leadership first",
			peerID,
		)
	}

	membership := n.state.Persistent.Membership

	if membership.Joint != nil {
		n.mu.Unlock()

		return errors.New(
			"cannot remove member while membership is in joint configuration",
		)
	}

	if !membershipIsVoter(membership, peerID) {
		n.mu.Unlock()

		return fmt.Errorf(
			"member %s is not a voter",
			peerID,
		)
	}

	oldConfiguration := membership.Current

	if len(oldConfiguration.Voters) <= 1 {
		n.mu.Unlock()

		return errors.New(
			"cannot remove the last voter",
		)
	}

	newVoters := make([]NodeID, 0, len(oldConfiguration.Voters)-1)

	for _, voterID := range oldConfiguration.Voters {
		if voterID != peerID {
			newVoters = append(newVoters, voterID)
		}
	}

	newConfiguration := model.Configuration{
		Voters: newVoters,
	}

	n.mu.Unlock()

	enterJointData, err := EncodeEnterJointConfigurationEntry(
		oldConfiguration,
		newConfiguration,
	)
	if err != nil {
		return fmt.Errorf(
			"encode enter-joint configuration for member %s: %w",
			peerID,
			err,
		)
	}

	enterJointIndex, err := n.Propose(enterJointData)
	if err != nil {
		return fmt.Errorf(
			"propose enter-joint configuration for member %s: %w",
			peerID,
			err,
		)
	}

	if err := n.waitForApplied(ctx, enterJointIndex); err != nil {
		return fmt.Errorf(
			"wait for enter-joint configuration at index %d: %w",
			enterJointIndex,
			err,
		)
	}

	leaveJointData, err := EncodeLeaveJointConfigurationEntry(
		newConfiguration,
	)
	if err != nil {
		return fmt.Errorf(
			"encode leave-joint configuration for member %s: %w",
			peerID,
			err,
		)
	}

	leaveJointIndex, err := n.Propose(leaveJointData)
	if err != nil {
		return fmt.Errorf(
			"propose leave-joint configuration for member %s: %w",
			peerID,
			err,
		)
	}

	if err := n.waitForApplied(ctx, leaveJointIndex); err != nil {
		return fmt.Errorf(
			"wait for leave-joint configuration at index %d: %w",
			leaveJointIndex,
			err,
		)
	}

	n.mu.RLock()
	finalMembership := n.state.Persistent.Membership
	n.mu.RUnlock()

	if finalMembership.Joint != nil {
		return fmt.Errorf(
			"member %s removal completed with joint configuration still active",
			peerID,
		)
	}

	if membershipIsVoter(finalMembership, peerID) {
		return fmt.Errorf(
			"member %s is still present after removal",
			peerID,
		)
	}

	return nil
}