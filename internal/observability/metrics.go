package observability

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
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

func NewMetrics(registerer prometheus.Registerer) *Metrics {
	metrics := &Metrics{
		// -----------------------------------------------------------------
		// Raft state.
		// -----------------------------------------------------------------

		CurrentTerm: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "raftiq",
				Subsystem: "raft",
				Name:      "current_term",
				Help:      "Current Raft term.",
			},
			[]string{"node_id"},
		),

		Role: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "raftiq",
				Subsystem: "raft",
				Name:      "role",
				Help:      "Current Raft role. Exactly one role is set to 1.",
			},
			[]string{"node_id", "role"},
		),

		CommitIndex: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "raftiq",
				Subsystem: "raft",
				Name:      "commit_index",
				Help:      "Highest Raft log index known to be committed.",
			},
			[]string{"node_id"},
		),

		LastApplied: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "raftiq",
				Subsystem: "raft",
				Name:      "last_applied",
				Help:      "Highest Raft log index applied to the state machine.",
			},
			[]string{"node_id"},
		),

		LastLogIndex: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "raftiq",
				Subsystem: "raft",
				Name:      "last_log_index",
				Help:      "Highest Raft log index currently stored.",
			},
			[]string{"node_id"},
		),

		LogSize: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "raftiq",
				Subsystem: "raft",
				Name:      "log_size",
				Help:      "Number of entries currently retained in the Raft log.",
			},
			[]string{"node_id"},
		),

		// -----------------------------------------------------------------
		// Raft elections and leadership.
		// -----------------------------------------------------------------

		ElectionsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "raftiq",
				Subsystem: "raft",
				Name:      "elections_total",
				Help:      "Total number of Raft elections started.",
			},
			[]string{"node_id"},
		),

		ElectionDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "raftiq",
				Subsystem: "raft",
				Name:      "election_duration_seconds",
				Help:      "Time taken for Raft elections to complete.",
			},
			[]string{"node_id", "result"},
		),

		LeaderChangesTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "raftiq",
				Subsystem: "raft",
				Name:      "leader_changes_total",
				Help:      "Total number of Raft leadership changes.",
			},
			[]string{"node_id"},
		),

		VoteRequestsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "raftiq",
				Subsystem: "raft",
				Name:      "vote_requests_total",
				Help:      "Total number of RequestVote RPCs processed.",
			},
			[]string{"node_id"},
		),

		VotesGrantedTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "raftiq",
				Subsystem: "raft",
				Name:      "votes_granted_total",
				Help:      "Total number of votes granted.",
			},
			[]string{"node_id"},
		),

		// -----------------------------------------------------------------
		// Raft replication.
		// -----------------------------------------------------------------

		AppendEntriesTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "raftiq",
				Subsystem: "raft",
				Name:      "append_entries_total",
				Help:      "Total number of AppendEntries RPCs processed.",
			},
			[]string{"node_id", "peer_id", "result"},
		),

		AppendEntriesFailures: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "raftiq",
				Subsystem: "raft",
				Name:      "append_entries_failures_total",
				Help:      "Total number of failed AppendEntries operations.",
			},
			[]string{"node_id", "peer_id"},
		),

		AppendEntriesDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "raftiq",
				Subsystem: "raft",
				Name:      "append_entries_duration_seconds",
				Help:      "Duration of AppendEntries operations.",
			},
			[]string{"node_id", "peer_id"},
		),

		// -----------------------------------------------------------------
		// Snapshots.
		// -----------------------------------------------------------------

		SnapshotsCreatedTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "raftiq",
				Subsystem: "raft",
				Name:      "snapshots_created_total",
				Help:      "Total number of snapshots created.",
			},
			[]string{"node_id"},
		),

		SnapshotsInstalledTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "raftiq",
				Subsystem: "raft",
				Name:      "snapshots_installed_total",
				Help:      "Total number of snapshots installed.",
			},
			[]string{"node_id"},
		),

		// -----------------------------------------------------------------
		// RPC transport.
		// -----------------------------------------------------------------

		RPCRequestsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "raftiq",
				Subsystem: "rpc",
				Name:      "requests_total",
				Help:      "Total number of RPC requests.",
			},
			[]string{"method"},
		),

		RPCErrorsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "raftiq",
				Subsystem: "rpc",
				Name:      "errors_total",
				Help:      "Total number of RPC errors.",
			},
			[]string{"method"},
		),

		RPCDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "raftiq",
				Subsystem: "rpc",
				Name:      "duration_seconds",
				Help:      "RPC request duration.",
			},
			[]string{"method"},
		),

		// -----------------------------------------------------------------
		// Storage.
		// -----------------------------------------------------------------

		StorageOperationsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "raftiq",
				Subsystem: "storage",
				Name:      "operations_total",
				Help:      "Total number of storage operations.",
			},
			[]string{"operation"},
		),

		StorageOperationErrors: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "raftiq",
				Subsystem: "storage",
				Name:      "operation_errors_total",
				Help:      "Total number of failed storage operations.",
			},
			[]string{"operation"},
		),

		StorageOperationDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "raftiq",
				Subsystem: "storage",
				Name:      "operation_duration_seconds",
				Help:      "Storage operation duration.",
			},
			[]string{"operation"},
		),

		StorageSyncTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "raftiq",
				Subsystem: "storage",
				Name:      "sync_total",
				Help:      "Total number of storage synchronization operations.",
			},
			[]string{"node_id"},
		),

		StorageSyncErrors: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "raftiq",
				Subsystem: "storage",
				Name:      "sync_errors_total",
				Help:      "Total number of failed storage synchronization operations.",
			},
			[]string{"node_id"},
		),

		StorageSyncDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "raftiq",
				Subsystem: "storage",
				Name:      "sync_duration_seconds",
				Help:      "Time spent syncing storage to stable storage.",
			},
			[]string{"node_id"},
		),

		// -----------------------------------------------------------------
		// KV.
		// -----------------------------------------------------------------

		KVOperationsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "raftiq",
				Subsystem: "kv",
				Name:      "operations_total",
				Help:      "Total number of KV operations.",
			},
			[]string{"operation"},
		),

		KVOperationErrors: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "raftiq",
				Subsystem: "kv",
				Name:      "operation_errors_total",
				Help:      "Total number of failed KV operations.",
			},
			[]string{"operation"},
		),

		// -----------------------------------------------------------------
		// Scheduler.
		// -----------------------------------------------------------------

		ScheduledJobsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "raftiq",
				Subsystem: "scheduler",
				Name:      "scheduled_jobs_total",
				Help:      "Total number of jobs scheduled.",
			},
			[]string{"node_id"},
		),

		ExecutedJobsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "raftiq",
				Subsystem: "scheduler",
				Name:      "executed_jobs_total",
				Help:      "Total number of jobs successfully executed.",
			},
			[]string{"node_id"},
		),

		JobExecutionFailures: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "raftiq",
				Subsystem: "scheduler",
				Name:      "job_execution_failures_total",
				Help:      "Total number of failed job executions.",
			},
			[]string{"node_id"},
		),

		LeaseAcquisitionsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "raftiq",
				Subsystem: "scheduler",
				Name:      "lease_acquisitions_total",
				Help:      "Total number of scheduler lease acquisitions.",
			},
			[]string{"node_id"},
		),

		LeaseLossesTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "raftiq",
				Subsystem: "scheduler",
				Name:      "lease_losses_total",
				Help:      "Total number of scheduler lease losses.",
			},
			[]string{"node_id"},
		),
	}

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
