package raft

import (
	"fmt"
	"time"
)

func (n *RaftNode) Start() error {
	n.runMu.Lock()
	defer n.runMu.Unlock()

	if n.running {
		err := fmt.Errorf(
			"raft node %s is already running",
			n.id,
		)

		n.getLogger().Warn(
			"raft node start rejected",
			"error", err,
		)

		return err
	}

	n.stopCh = make(chan struct{})
	n.doneCh = make(chan struct{})
	n.running = true

	go n.run()

	n.getLogger().Info(
		"raft node started",
		"tick_interval", n.tickInterval.String(),
		"election_timeout", n.electionTimeout,
		"heartbeat_timeout", n.heartbeatTimeout,
	)

	return nil
}

func (n *RaftNode) run() {
	defer close(n.doneCh)

	ticker := time.NewTicker(n.tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.runTick()

		case <-n.stopCh:
			return
		}
	}
}

func (n *RaftNode) runTick() {
	electionDue, heartbeatDue := n.tick()

	if heartbeatDue {
		go n.heartbeat()
	}

	if electionDue {
		n.startElectionIfNeeded()
	}
}

func (n *RaftNode) startElectionIfNeeded() {
	n.runMu.Lock()

	if n.electionInFlight {
		n.runMu.Unlock()
		return
	}

	n.electionInFlight = true

	n.runMu.Unlock()

	go func() {
		defer func() {
			n.runMu.Lock()
			n.electionInFlight = false
			n.runMu.Unlock()
		}()

		n.runElection()
	}()
}

func (n *RaftNode) Stop() {
	n.runMu.Lock()

	if !n.running {
		n.runMu.Unlock()
		return
	}

	close(n.stopCh)
	doneCh := n.doneCh
	n.running = false

	n.runMu.Unlock()

	<-doneCh

	n.getLogger().Info(
		"raft node stopped",
	)
}

func (n *RaftNode) tick() (electionDue, heartbeatDue bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state.Role == Leader {
		n.heartbeatElapsed++

		if n.heartbeatElapsed >= n.heartbeatTimeout {
			n.heartbeatElapsed = 0
			heartbeatDue = true
		}

		return false, heartbeatDue
	}

	n.electionElapsed++

	// Do not reset electionElapsed here.
	//
	// PreVote needs to be able to observe that the election timer
	// actually expired. The timer is reset when:
	//   - a valid AppendEntries is received,
	//   - a vote is granted,
	//   - an actual election starts, or
	//   - a failed PreVote attempt is completed.
	if n.electionElapsed >= n.electionTimeout {
		electionDue = true
	}

	return electionDue, false
}

func (n *RaftNode) Tick() bool {
	electionDue, heartbeatDue := n.tick()

	if heartbeatDue {
		n.heartbeat()
	}

	return electionDue
}

func (n *RaftNode) SetElectionTimeout(timeout int) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.electionTimeout = timeout
}

func (n *RaftNode) onElectionTimeout() {
	state := n.State()

	if state.Role == Leader {
		return
	}

	n.getLogger().Debug(
		"raft election timeout reached",
		"term", state.Persistent.CurrentTerm,
		"role", state.Role,
	)

	n.runElection()
}

func (n *RaftNode) resetElectionTimer() {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.electionElapsed = 0
}
func (n *RaftNode) IsRunning() bool {
	if n == nil {
		return false
	}

	n.runMu.Lock()
	defer n.runMu.Unlock()

	return n.running
}
