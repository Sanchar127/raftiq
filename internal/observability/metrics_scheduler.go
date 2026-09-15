package observability

import (
	"github.com/prometheus/client_golang/prometheus"
)

func (m *Metrics) initScheduler() {
	m.ScheduledJobsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "raftiq",
			Subsystem: "scheduler",
			Name:      "scheduled_jobs_total",
			Help:      "Total number of jobs scheduled.",
		},
		[]string{"node_id"},
	)

	m.ExecutedJobsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "raftiq",
			Subsystem: "scheduler",
			Name:      "executed_jobs_total",
			Help:      "Total number of jobs successfully executed.",
		},
		[]string{"node_id"},
	)

	m.JobExecutionFailures = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "raftiq",
			Subsystem: "scheduler",
			Name:      "job_execution_failures_total",
			Help:      "Total number of failed job executions.",
		},
		[]string{"node_id"},
	)

	m.LeaseAcquisitionsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "raftiq",
			Subsystem: "scheduler",
			Name:      "lease_acquisitions_total",
			Help:      "Total number of scheduler lease acquisitions.",
		},
		[]string{"node_id"},
	)

	m.LeaseLossesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "raftiq",
			Subsystem: "scheduler",
			Name:      "lease_losses_total",
			Help:      "Total number of scheduler lease losses.",
		},
		[]string{"node_id"},
	)
}
