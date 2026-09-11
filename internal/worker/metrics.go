package worker

type WorkerMetrics interface {
	IncExecutedJobs()
	IncJobExecutionFailures()
	IncLeaseLosses()
}

type NoopWorkerMetrics struct{}

func (NoopWorkerMetrics) IncExecutedJobs()         {}
func (NoopWorkerMetrics) IncJobExecutionFailures() {}
func (NoopWorkerMetrics) IncLeaseLosses()          {}
