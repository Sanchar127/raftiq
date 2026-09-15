package worker

import (
	"fmt"
)

// ConfigureLoop attaches the job source and execution-loop configuration.
func (w *Worker) ConfigureLoop(
	source JobSource,
	config Config,
) error {
	if source == nil {
		return fmt.Errorf(
			"%w: missing job source",
			ErrInvalidWorker,
		)
	}

	if config.Interval <= 0 {
		return fmt.Errorf(
			"%w: worker interval must be positive",
			ErrInvalidWorker,
		)
	}

	if config.Raft == nil {
		return fmt.Errorf(
			"%w: missing Raft node",
			ErrInvalidWorker,
		)
	}

	if config.Applier == nil {
		return fmt.Errorf(
			"%w: missing applier",
			ErrInvalidWorker,
		)
	}

	metrics := config.Metrics
	if metrics == nil {
		metrics = NoopWorkerMetrics{}
	}

	w.source = source
	w.interval = config.Interval
	w.raft = config.Raft
	w.applier = config.Applier
	w.metrics = metrics

	return nil
}
