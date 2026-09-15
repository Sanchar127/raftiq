package worker

import (
	"context"
	"fmt"

	"github.com/sanchar127/raftiq/internal/model"
)

// Execute runs the configured handler for a job.
func (w *Worker) Execute(
	ctx context.Context,
	job model.Job,
) error {
	if job.ID == "" {
		return fmt.Errorf(
			"%w: missing job ID",
			ErrInvalidJob,
		)
	}

	if ctx == nil {
		return fmt.Errorf(
			"%w: nil execution context",
			ErrInvalidJob,
		)
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	if err := w.handler.Execute(ctx, job); err != nil {
		return fmt.Errorf(
			"execute job %q: %w",
			job.ID,
			err,
		)
	}

	return nil
}
