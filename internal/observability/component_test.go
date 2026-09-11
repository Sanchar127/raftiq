package observability

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestComponentLogger(t *testing.T) {
	var output bytes.Buffer

	logger := slog.New(
		slog.NewJSONHandler(&output, nil),
	)

	componentLogger := ComponentLogger(logger, "raft")

	componentLogger.Info("event")

	var record map[string]any

	require.NoError(t, json.Unmarshal(output.Bytes(), &record))
	require.Equal(t, "raft", record["component"])
}

func TestNodeLogger(t *testing.T) {
	var output bytes.Buffer

	logger := slog.New(
		slog.NewJSONHandler(&output, nil),
	)

	nodeLogger := NodeLogger(
		logger,
		"raft",
		"node-1",
	)

	nodeLogger.Info("event")

	var record map[string]any

	require.NoError(t, json.Unmarshal(output.Bytes(), &record))

	require.Equal(t, "raft", record["component"])
	require.Equal(t, "node-1", record["node_id"])
}

func TestComponentLogger_NilLogger(t *testing.T) {
	require.NotPanics(t, func() {
		logger := ComponentLogger(nil, "raft")
		logger.Info("event")
	})
}