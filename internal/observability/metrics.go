package observability

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/sanchar127/raftiq/internal/raft"
)

type Metrics struct {
	nodeID string

	// Raft state.
	CurrentTerm  *prometheus.GaugeVec
	Role         *prometheus.GaugeVec
	CommitIndex  *prometheus.GaugeVec
	LastApplied  *prometheus.GaugeVec
	LastLogIndex *prometheus.GaugeVec
	LogSize      *prometheus.GaugeVec

	// Raft elections and leadership.
	ElectionsTotal     *prometheus.CounterVec
	ElectionDuration   *prometheus.HistogramVec
	LeaderChangesTotal *prometheus.CounterVec
	VoteRequestsTotal  *prometheus.CounterVec
	VotesGrantedTotal  *prometheus.CounterVec

	// Raft replication.
	AppendEntriesTotal    *prometheus.CounterVec
	AppendEntriesFailures *prometheus.CounterVec
	AppendEntriesDuration *prometheus.HistogramVec

	// Snapshots.
	SnapshotsCreatedTotal   *prometheus.CounterVec
	SnapshotsInstalledTotal *prometheus.CounterVec

	// RPC transport.
	RPCRequestsTotal *prometheus.CounterVec
	RPCErrorsTotal   *prometheus.CounterVec
	RPCDuration      *prometheus.HistogramVec

	// Storage.
	StorageOperationsTotal   *prometheus.CounterVec
	StorageOperationErrors   *prometheus.CounterVec
	StorageOperationDuration *prometheus.HistogramVec
	StorageSyncTotal         *prometheus.CounterVec
	StorageSyncErrors        *prometheus.CounterVec
	StorageSyncDuration      *prometheus.HistogramVec

	// KV.
	KVOperationsTotal *prometheus.CounterVec
	KVOperationErrors *prometheus.CounterVec

	// Scheduler.
	ScheduledJobsTotal     *prometheus.CounterVec
	ExecutedJobsTotal      *prometheus.CounterVec
	JobExecutionFailures   *prometheus.CounterVec
	LeaseAcquisitionsTotal *prometheus.CounterVec
	LeaseLossesTotal       *prometheus.CounterVec
}

// Compile-time verification that Metrics satisfies the Raft metrics
// interface.
//
// RPC metrics are implemented below as well. The transport package owns
// the RPCMetrics interface, so we intentionally avoid importing transport
// here to keep the dependency direction simple.
var _ raft.Metrics = (*Metrics)(nil)

func NewMetrics(
	registerer prometheus.Registerer,
	nodeID string,
) *Metrics {
	metrics := &Metrics{
		nodeID: nodeID,
	}

	metrics.initRaftState()
	metrics.initRaftElection()
	metrics.initRaftReplication()
	metrics.initRaftSnapshot()
	metrics.initRPC()
	metrics.initStorage()
	metrics.initKV()
	metrics.initScheduler()

	registerer.MustRegister(
		// Raft state.
		metrics.CurrentTerm,
		metrics.Role,
		metrics.CommitIndex,
		metrics.LastApplied,
		metrics.LastLogIndex,
		metrics.LogSize,

		// Raft elections and leadership.
		metrics.ElectionsTotal,
		metrics.ElectionDuration,
		metrics.LeaderChangesTotal,
		metrics.VoteRequestsTotal,
		metrics.VotesGrantedTotal,

		// Raft replication.
		metrics.AppendEntriesTotal,
		metrics.AppendEntriesFailures,
		metrics.AppendEntriesDuration,

		// Snapshots.
		metrics.SnapshotsCreatedTotal,
		metrics.SnapshotsInstalledTotal,

		// RPC transport.
		metrics.RPCRequestsTotal,
		metrics.RPCErrorsTotal,
		metrics.RPCDuration,

		// Storage.
		metrics.StorageOperationsTotal,
		metrics.StorageOperationErrors,
		metrics.StorageOperationDuration,
		metrics.StorageSyncTotal,
		metrics.StorageSyncErrors,
		metrics.StorageSyncDuration,

		// KV.
		metrics.KVOperationsTotal,
		metrics.KVOperationErrors,

		// Scheduler.
		metrics.ScheduledJobsTotal,
		metrics.ExecutedJobsTotal,
		metrics.JobExecutionFailures,
		metrics.LeaseAcquisitionsTotal,
		metrics.LeaseLossesTotal,
	)

	return metrics
}
