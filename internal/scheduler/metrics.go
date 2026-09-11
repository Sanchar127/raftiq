package scheduler

type SchedulerMetrics interface {
	IncScheduledJobs()
	IncLeaseAcquisitions()
	IncLeaseLosses()
}

type NoopSchedulerMetrics struct{}

func (NoopSchedulerMetrics) IncScheduledJobs()     {}
func (NoopSchedulerMetrics) IncLeaseAcquisitions() {}
func (NoopSchedulerMetrics) IncLeaseLosses()       {}
