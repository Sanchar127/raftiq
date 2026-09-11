package observability

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func newTestStorageMetrics() *Metrics {
	return &Metrics{
		StorageOperationsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "test_storage_operations_total",
			},
			[]string{"operation"},
		),
		StorageOperationErrors: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "test_storage_operation_errors_total",
			},
			[]string{"operation"},
		),
		StorageOperationDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name: "test_storage_operation_duration_seconds",
			},
			[]string{"operation"},
		),
		StorageSyncTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "test_storage_sync_total",
			},
			[]string{"node_id"},
		),
		StorageSyncErrors: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "test_storage_sync_errors_total",
			},
			[]string{"node_id"},
		),
		StorageSyncDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name: "test_storage_sync_duration_seconds",
			},
			[]string{"node_id"},
		),
	}
}

func TestStorageMetricsOperationCounters(t *testing.T) {
	metrics := newTestStorageMetrics()
	storageMetrics := NewStorageMetrics("node-1", metrics)

	storageMetrics.IncOperation("AppendEntries")
	storageMetrics.IncOperation("AppendEntries")
	storageMetrics.IncOperationError("AppendEntries")

	require.Equal(
		t,
		float64(2),
		testutil.ToFloat64(
			metrics.StorageOperationsTotal.WithLabelValues("AppendEntries"),
		),
	)

	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(
			metrics.StorageOperationErrors.WithLabelValues("AppendEntries"),
		),
	)
}

func TestStorageMetricsSyncCounters(t *testing.T) {
	metrics := newTestStorageMetrics()
	storageMetrics := NewStorageMetrics("node-1", metrics)

	storageMetrics.IncSync()
	storageMetrics.IncSync()
	storageMetrics.IncSyncError()

	require.Equal(
		t,
		float64(2),
		testutil.ToFloat64(
			metrics.StorageSyncTotal.WithLabelValues("node-1"),
		),
	)

	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(
			metrics.StorageSyncErrors.WithLabelValues("node-1"),
		),
	)
}

func TestStorageMetricsOperationDuration(t *testing.T) {
	metrics := newTestStorageMetrics()
	storageMetrics := NewStorageMetrics("node-1", metrics)

	storageMetrics.ObserveOperationDuration(
		"AppendEntries",
		250*time.Millisecond,
	)

	registry := prometheus.NewRegistry()
	require.NoError(t, registry.Register(metrics.StorageOperationDuration))

	families, err := registry.Gather()
	require.NoError(t, err)
	require.Len(t, families, 1)
	require.Len(t, families[0].Metric, 1)

	histogram := families[0].Metric[0].Histogram

	require.Equal(t, uint64(1), histogram.GetSampleCount())
	require.InDelta(t, 0.25, histogram.GetSampleSum(), 0.000001)
}

func TestStorageMetricsSyncDuration(t *testing.T) {
	metrics := newTestStorageMetrics()
	storageMetrics := NewStorageMetrics("node-1", metrics)

	storageMetrics.ObserveSyncDuration(500 * time.Millisecond)

	registry := prometheus.NewRegistry()
	require.NoError(t, registry.Register(metrics.StorageSyncDuration))

	families, err := registry.Gather()
	require.NoError(t, err)
	require.Len(t, families, 1)
	require.Len(t, families[0].Metric, 1)

	histogram := families[0].Metric[0].Histogram

	require.Equal(t, uint64(1), histogram.GetSampleCount())
	require.InDelta(t, 0.5, histogram.GetSampleSum(), 0.000001)
}

func TestNewStorageMetricsRejectsNilMetrics(t *testing.T) {
	require.Panics(t, func() {
		NewStorageMetrics("node-1", nil)
	})
}

func TestNewStorageMetricsRejectsEmptyNodeID(t *testing.T) {
	metrics := newTestStorageMetrics()

	require.Panics(t, func() {
		NewStorageMetrics("", metrics)
	})
}

func TestStorageMetricsUsesNodeIDForSyncMetrics(t *testing.T) {
	metrics := newTestStorageMetrics()
	storageMetrics := NewStorageMetrics("node-7", metrics)

	storageMetrics.IncSync()

	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(
			metrics.StorageSyncTotal.WithLabelValues("node-7"),
		),
	)

	require.Equal(
		t,
		float64(0),
		testutil.ToFloat64(
			metrics.StorageSyncTotal.WithLabelValues("node-1"),
		),
	)
}
