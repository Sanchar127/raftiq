package observability

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestMetrics_ElectionsTotal(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry)

	metrics.ElectionsTotal.WithLabelValues("node-1").Inc()
	metrics.ElectionsTotal.WithLabelValues("node-1").Inc()

	expected := `
# HELP raftiq_raft_elections_total Total number of Raft elections started.
# TYPE raftiq_raft_elections_total counter
raftiq_raft_elections_total{node_id="node-1"} 2
`

	err := testutil.GatherAndCompare(
		registry,
		strings.NewReader(expected),
		"raftiq_raft_elections_total",
	)

	require.NoError(t, err)
}
