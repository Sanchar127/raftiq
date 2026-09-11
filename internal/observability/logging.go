package observability

import (
	"io"
	"log/slog"
	"os"
)

type LogFormat string

const (
	LogFormatJSON LogFormat = "json"
	LogFormatText LogFormat = "text"
)

type LoggingConfig struct {
	Level  slog.Level
	Format LogFormat
	Output io.Writer
}

func NewLogger(config LoggingConfig) *slog.Logger {
	output := config.Output
	if output == nil {
		output = os.Stderr
	}

	var handler slog.Handler

	options := &slog.HandlerOptions{
		Level:     config.Level,
		AddSource: false,
	}

	switch config.Format {
	case LogFormatText:
		handler = slog.NewTextHandler(output, options)
	default:
		handler = slog.NewJSONHandler(output, options)
	}

	return slog.New(handler)
}

func NewProductionLogger(output io.Writer) *slog.Logger {
	return NewLogger(LoggingConfig{
		Level:  slog.LevelInfo,
		Format: LogFormatJSON,
		Output: output,
	})
}

func NewDevelopmentLogger(output io.Writer) *slog.Logger {
	return NewLogger(LoggingConfig{
		Level:  slog.LevelDebug,
		Format: LogFormatText,
		Output: output,
	})
}