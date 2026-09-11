package observability

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestMetricsServer_ExposesMetrics(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry)

	metrics.ElectionsTotal.WithLabelValues("node-1").Add(3)

	server, err := NewMetricsServer(":0", registry)
	require.NoError(t, err)
	require.NotNil(t, server)

	handler := server.server.Handler

	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code)

	body, err := io.ReadAll(recorder.Result().Body)
	require.NoError(t, err)

	require.Contains(
		t,
		string(body),
		`raftiq_raft_elections_total{node_id="node-1"} 3`,
	)
}

func TestMetricsServer_MetricsPathOnly(t *testing.T) {
	registry := prometheus.NewRegistry()
	_, err := NewMetricsServer(":0", registry)
	require.NoError(t, err)

	server, err := NewMetricsServer(":0", registry)
	require.NoError(t, err)

	handler := server.server.Handler

	request := httptest.NewRequest(http.MethodGet, "/health", nil)
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, request)

	require.Equal(t, http.StatusNotFound, recorder.Code)
}

func TestMetricsServer_Shutdown(t *testing.T) {
	registry := prometheus.NewRegistry()

	server, err := NewMetricsServer(":0", registry)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Second,
	)
	defer cancel()

	require.NoError(t, server.Shutdown(ctx))
}

func TestNewMetricsServer_Validation(t *testing.T) {
	registry := prometheus.NewRegistry()

	_, err := NewMetricsServer("", registry)
	require.Error(t, err)

	_, err = NewMetricsServer(":9090", nil)
	require.Error(t, err)
}

func TestMetricsServer_NilServe(t *testing.T) {
	var server *MetricsServer

	require.ErrorIs(t, server.Serve(), ErrMetricsServerClosed)
}

func TestMetricsServer_Lifecycle(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry)

	metrics.ElectionsTotal.WithLabelValues("node-1").Add(3)

	server, err := NewMetricsServer("127.0.0.1:0", registry)
	require.NoError(t, err)
	require.NotNil(t, server)

	errCh := make(chan error, 1)

	go func() {
		errCh <- server.Serve()
	}()

	client := &http.Client{
		Timeout: time.Second,
	}

	require.Eventually(
		t,
		func() bool {
			response, err := client.Get(
				"http://" + server.Address() + "/metrics",
			)
			if err != nil {
				return false
			}
			defer response.Body.Close()

			return response.StatusCode == http.StatusOK
		},
		time.Second,
		10*time.Millisecond,
	)

	response, err := client.Get(
		"http://" + server.Address() + "/metrics",
	)
	require.NoError(t, err)
	defer response.Body.Close()

	require.Equal(t, http.StatusOK, response.StatusCode)

	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)

	require.Contains(
		t,
		string(body),
		`raftiq_raft_elections_total{node_id="node-1"} 3`,
	)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Second,
	)
	defer cancel()

	require.NoError(t, server.Shutdown(ctx))

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("metrics server did not stop")
	}
}
