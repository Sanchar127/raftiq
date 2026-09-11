package observability

import (
	"io"
	"log/slog"
)

func ComponentLogger(
	logger *slog.Logger,
	component string,
) *slog.Logger {
	if logger == nil {
		logger = slog.New(
			slog.NewTextHandler(io.Discard, nil),
		)
	}

	if component == "" {
		return logger
	}

	return logger.With(
		slog.String("component", component),
	)
}

func NodeLogger(
	logger *slog.Logger,
	component string,
	nodeID string,
) *slog.Logger {
	logger = ComponentLogger(logger, component)

	if nodeID == "" {
		return logger
	}

	return logger.With(
		slog.String("node_id", nodeID),
	)
}
