package observability

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestKVMetricsIncOperation(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry, "test-node")
	kvMetrics := NewKVMetrics(metrics)

	kvMetrics.IncOperation("put")
	kvMetrics.IncOperation("put")
	kvMetrics.IncOperation("delete")

	putValue := testutil.ToFloat64(
		metrics.KVOperationsTotal.WithLabelValues("put"),
	)
	if putValue != 2 {
		t.Fatalf("expected put operation count 2, got %v", putValue)
	}

	deleteValue := testutil.ToFloat64(
		metrics.KVOperationsTotal.WithLabelValues("delete"),
	)
	if deleteValue != 1 {
		t.Fatalf("expected delete operation count 1, got %v", deleteValue)
	}
}

func TestKVMetricsIncOperationError(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry, "test-node")
	kvMetrics := NewKVMetrics(metrics)

	kvMetrics.IncOperationError("put")
	kvMetrics.IncOperationError("put")
	kvMetrics.IncOperationError("claim_job")

	putValue := testutil.ToFloat64(
		metrics.KVOperationErrors.WithLabelValues("put"),
	)
	if putValue != 2 {
		t.Fatalf("expected put error count 2, got %v", putValue)
	}

	claimValue := testutil.ToFloat64(
		metrics.KVOperationErrors.WithLabelValues("claim_job"),
	)
	if claimValue != 1 {
		t.Fatalf("expected claim_job error count 1, got %v", claimValue)
	}
}

func TestKVMetricsOperationAndErrorAreIndependent(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry, "test-node")
	kvMetrics := NewKVMetrics(metrics)

	kvMetrics.IncOperation("put")
	kvMetrics.IncOperation("put")
	kvMetrics.IncOperationError("put")

	operations := testutil.ToFloat64(
		metrics.KVOperationsTotal.WithLabelValues("put"),
	)
	errors := testutil.ToFloat64(
		metrics.KVOperationErrors.WithLabelValues("put"),
	)

	if operations != 2 {
		t.Fatalf("expected 2 operations, got %v", operations)
	}

	if errors != 1 {
		t.Fatalf("expected 1 error, got %v", errors)
	}
}

func TestNewKVMetricsPanicsWithNilMetrics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for nil metrics")
		}
	}()

	NewKVMetrics(nil)
}

func TestKVMetricsConcurrent(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry, "test-node")
	kvMetrics := NewKVMetrics(metrics)

	const workers = 10
	const operationsPerWorker = 100

	done := make(chan struct{}, workers)

	for range workers {
		go func() {
			for range operationsPerWorker {
				kvMetrics.IncOperation("put")
				kvMetrics.IncOperationError("put")
			}

			done <- struct{}{}
		}()
	}

	for range workers {
		<-done
	}

	operations := testutil.ToFloat64(
		metrics.KVOperationsTotal.WithLabelValues("put"),
	)
	errors := testutil.ToFloat64(
		metrics.KVOperationErrors.WithLabelValues("put"),
	)

	expected := float64(workers * operationsPerWorker)

	if operations != expected {
		t.Fatalf("expected %v operations, got %v", expected, operations)
	}

	if errors != expected {
		t.Fatalf("expected %v errors, got %v", expected, errors)
	}
}
