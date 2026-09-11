package observability

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewLogger_JSON(t *testing.T) {
	var output bytes.Buffer

	logger := NewLogger(LoggingConfig{
		Level:  slog.LevelDebug,
		Format: LogFormatJSON,
		Output: &output,
	})

	logger.Info(
		"leader elected",
		slog.String("node_id", "node-1"),
		slog.Int64("term", 7),
	)

	var record map[string]any

	require.NoError(t, json.Unmarshal(output.Bytes(), &record))

	require.Equal(t, "INFO", record["level"])
	require.Equal(t, "leader elected", record["msg"])
	require.Equal(t, "node-1", record["node_id"])
	require.Equal(t, float64(7), record["term"])
	require.NotEmpty(t, record["time"])
}

func TestNewLogger_Text(t *testing.T) {
	var output bytes.Buffer

	logger := NewLogger(LoggingConfig{
		Level:  slog.LevelDebug,
		Format: LogFormatText,
		Output: &output,
	})

	logger.Info(
		"leader elected",
		slog.String("node_id", "node-1"),
	)

	require.Contains(t, output.String(), "level=INFO")
	require.Contains(t, output.String(), "msg=\"leader elected\"")
	require.Contains(t, output.String(), "node_id=node-1")
}

func TestNewLogger_DefaultOutput(t *testing.T) {
	require.NotPanics(t, func() {
		logger := NewLogger(LoggingConfig{
			Level:  slog.LevelInfo,
			Format: LogFormatJSON,
		})

		logger.Info("test")
	})
}

func TestNewLogger_DefaultFormatIsJSON(t *testing.T) {
	var output bytes.Buffer

	logger := NewLogger(LoggingConfig{
		Level:  slog.LevelInfo,
		Output: &output,
	})

	logger.Info("test")

	var record map[string]any

	require.NoError(t, json.Unmarshal(output.Bytes(), &record))
	require.Equal(t, "test", record["msg"])
}

func TestNewLogger_LevelFiltering(t *testing.T) {
	var output bytes.Buffer

	logger := NewLogger(LoggingConfig{
		Level:  slog.LevelInfo,
		Format: LogFormatJSON,
		Output: &output,
	})

	logger.Debug("should not appear")
	logger.Info("should appear")

	require.NotContains(t, output.String(), "should not appear")
	require.Contains(t, output.String(), "should appear")
}

func TestNewLogger_ContextFields(t *testing.T) {
	var output bytes.Buffer

	logger := NewLogger(LoggingConfig{
		Level:  slog.LevelInfo,
		Format: LogFormatJSON,
		Output: &output,
	})

	nodeLogger := logger.With(
		slog.String("component", "raft"),
		slog.String("node_id", "node-1"),
	)

	nodeLogger.Warn(
		"peer unavailable",
		slog.String("peer_id", "node-2"),
	)

	var record map[string]any

	require.NoError(t, json.Unmarshal(output.Bytes(), &record))

	require.Equal(t, "raft", record["component"])
	require.Equal(t, "node-1", record["node_id"])
	require.Equal(t, "node-2", record["peer_id"])
}

func TestNewProductionLogger(t *testing.T) {
	var output bytes.Buffer

	logger := NewProductionLogger(&output)

	logger.Info("production event")

	var record map[string]any

	require.NoError(t, json.Unmarshal(output.Bytes(), &record))
	require.Equal(t, "production event", record["msg"])
}

func TestNewDevelopmentLogger(t *testing.T) {
	var output bytes.Buffer

	logger := NewDevelopmentLogger(&output)

	logger.Debug("development event")

	require.Contains(t, output.String(), "development event")
}
