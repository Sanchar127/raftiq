package raft

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/sanchar127/raftiq/internal/model"
	"github.com/sanchar127/raftiq/internal/storage"
)

const DefaultRPCTimeout = 2 * time.Second

type RaftNode struct {
	mu      sync.RWMutex
	applyMu sync.Mutex

	runMu   sync.Mutex
	runWG   sync.WaitGroup
	running bool
	stopCh  chan struct{}
	doneCh  chan struct{}

	id     NodeID
	state  State
	log    *Log
	logger *slog.Logger

	transport Transport
	peerIDs   []NodeID

	metrics Metrics

	electionStartedAt time.Time

	applyCh chan LogEntry

	storage storage.Storage

	snapshotRestore func(model.Snapshot) error

	electionElapsed     int
	electionTimeout     int
	electionInFlight    bool
	storageWriteBlocked bool

	heartbeatElapsed int
	heartbeatTimeout int

	tickInterval time.Duration
	rpcTimeout   time.Duration
}

type Peer interface {
	ID() NodeID

	RequestVote(args RequestVoteArgs) RequestVoteReply

	PreVote(args PreVoteArgs) PreVoteReply

	AppendEntries(args AppendEntriesArgs) AppendEntriesReply

	InstallSnapshot(args InstallSnapshotArgs) InstallSnapshotReply
}

func NewRaftNode(id NodeID) *RaftNode {
	node, err := NewRaftNodeWithStorage(id, storage.NewMemoryStorage())
	if err != nil {
		panic(err)
	}

	return node
}

func NewRaftNodeWithStorage(
	id NodeID,
	store storage.Storage,
) (*RaftNode, error) {
	if store == nil {
		return nil, fmt.Errorf("storage must not be nil")
	}

	persistentState, err := store.LoadState()
	if err != nil {
		return nil, fmt.Errorf(
			"load persistent state: %w",
			err,
		)
	}

	entries, err := store.LoadEntries()
	if err != nil {
		return nil, fmt.Errorf(
			"load log entries: %w",
			err,
		)
	}

	snapshot, err := store.LoadSnapshot()
	if err != nil {
		return nil, fmt.Errorf(
			"load snapshot: %w",
			err,
		)
	}

	log := NewLog()

	for _, entry := range entries {
		if err := log.Append(entry); err != nil {
			return nil, fmt.Errorf(
				"restore log entry %d: %w",
				entry.Index,
				err,
			)
		}
	}

	commitIndex := LogIndex(0)
	lastApplied := LogIndex(0)

	if snapshot.LastIncludedIndex > 0 {
		if err := log.RestoreSnapshot(snapshot); err != nil {
			return nil, fmt.Errorf(
				"restore snapshot boundary: %w",
				err,
			)
		}

		commitIndex = snapshot.LastIncludedIndex
		lastApplied = snapshot.LastIncludedIndex
	}

	return &RaftNode{
		id:        id,
		storage:   store,
		transport: NewLocalTransport(),
		peerIDs:   make([]NodeID, 0),
		applyCh:   make(chan LogEntry, 100),
		metrics:   NoopMetrics{},
		logger:    discardRaftLogger(),

		state: State{
			Persistent: persistentState,

			Volatile: VolatileState{
				CommitIndex: commitIndex,
				LastApplied: lastApplied,
			},

			Leader: LeaderState{
				NextIndex:  make(map[NodeID]LogIndex),
				MatchIndex: make(map[NodeID]LogIndex),
			},

			Election: ElectionState{
				VotesReceived: make(map[NodeID]struct{}),
			},

			Role:     Follower,
			LeaderID: "",
		},

		log:              log,
		electionTimeout:  10,
		heartbeatTimeout: 1,
		tickInterval:     100 * time.Millisecond,
		rpcTimeout:       DefaultRPCTimeout,
	}, nil
}

func (n *RaftNode) ID() NodeID {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.id
}

func (n *RaftNode) State() State {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.state
}

func (n *RaftNode) Log() *Log {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.log
}

func (n *RaftNode) ApplyCh() <-chan LogEntry {
	return n.applyCh
}

func (n *RaftNode) Storage() storage.Storage {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.storage
}

func (n *RaftNode) Snapshot() (model.Snapshot, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	snapshot, err := n.storage.LoadSnapshot()
	if err != nil {
		return model.Snapshot{}, fmt.Errorf(
			"load snapshot: %w",
			err,
		)
	}

	snapshot.Data = append([]byte(nil), snapshot.Data...)

	return snapshot, nil
}
