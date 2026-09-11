package observability

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHealthServerHealthz(t *testing.T) {
	server := NewHealthServer("127.0.0.1:0")

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()

	server.server.Handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.JSONEq(t, `{"status":"ok"}`, rec.Body.String())
	require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
}

func TestHealthServerReadyzWithoutProbes(t *testing.T) {
	server := NewHealthServer("127.0.0.1:0")

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()

	server.server.Handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.JSONEq(t, `{"status":"ready"}`, rec.Body.String())
}

func TestHealthServerReadyzHealthyProbe(t *testing.T) {
	server := NewHealthServer("127.0.0.1:0")

	err := server.RegisterReadinessProbe("raft", func() error {
		return nil
	})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()

	server.server.Handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.JSONEq(
		t,
		`{
			"status": "ready",
			"checks": {
				"raft": "ok"
			}
		}`,
		rec.Body.String(),
	)
}

func TestHealthServerReadyzFailedProbe(t *testing.T) {
	server := NewHealthServer("127.0.0.1:0")

	err := server.RegisterReadinessProbe("raft", func() error {
		return errors.New("raft is not ready")
	})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()

	server.server.Handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.JSONEq(
		t,
		`{
			"status": "not_ready",
			"checks": {
				"raft": "raft is not ready"
			}
		}`,
		rec.Body.String(),
	)
}

func TestHealthServerReadyzMultipleProbes(t *testing.T) {
	server := NewHealthServer("127.0.0.1:0")

	require.NoError(t, server.RegisterReadinessProbe("raft", func() error {
		return nil
	}))

	require.NoError(t, server.RegisterReadinessProbe("storage", func() error {
		return errors.New("storage unavailable")
	}))

	require.NoError(t, server.RegisterReadinessProbe("kv", func() error {
		return errors.New("kv unavailable")
	}))

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()

	server.server.Handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.JSONEq(
		t,
		`{
			"status": "not_ready",
			"checks": {
				"raft": "ok",
				"storage": "storage unavailable",
				"kv": "kv unavailable"
			}
		}`,
		rec.Body.String(),
	)
}

func TestHealthServerRegisterReadinessProbeValidation(t *testing.T) {
	server := NewHealthServer("127.0.0.1:0")

	require.Error(t, server.RegisterReadinessProbe("", func() error {
		return nil
	}))

	require.Error(t, server.RegisterReadinessProbe("raft", nil))
}

func TestHealthServerUnregisterReadinessProbe(t *testing.T) {
	server := NewHealthServer("127.0.0.1:0")

	require.NoError(t, server.RegisterReadinessProbe("raft", func() error {
		return errors.New("raft unavailable")
	}))

	server.UnregisterReadinessProbe("raft")

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()

	server.server.Handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.JSONEq(t, `{"status":"ready"}`, rec.Body.String())
}

func TestHealthServerReadinessProbeCanRegisterWhileChecking(t *testing.T) {
	server := NewHealthServer("127.0.0.1:0")

	started := make(chan struct{})
	release := make(chan struct{})

	require.NoError(t, server.RegisterReadinessProbe("slow", func() error {
		close(started)
		<-release
		return nil
	}))

	done := make(chan struct{})

	go func() {
		req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
		rec := httptest.NewRecorder()

		server.server.Handler.ServeHTTP(rec, req)
		close(done)
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("readiness probe did not start")
	}

	require.NoError(t, server.RegisterReadinessProbe("new", func() error {
		return nil
	}))

	close(release)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("readiness request did not finish")
	}
}

func TestHealthServerServeAndShutdown(t *testing.T) {
	server := NewHealthServer("127.0.0.1:0")

	listener, err := newTestListener()
	require.NoError(t, err)

	server.server.Addr = listener.Addr().String()

	done := make(chan error, 1)

	go func() {
		done <- server.server.Serve(listener)
	}()

	require.Eventually(t, func() bool {
		return server.server.Addr != ""
	}, time.Second, 10*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	require.NoError(t, server.Shutdown(ctx))

	select {
	case err := <-done:
		require.ErrorIs(t, err, http.ErrServerClosed)
	case <-time.After(time.Second):
		t.Fatal("health server did not shut down")
	}
}

func TestHealthServerConcurrentProbeRegistration(t *testing.T) {
	server := NewHealthServer("127.0.0.1:0")

	const workers = 20

	var wg sync.WaitGroup
	wg.Add(workers)

	for i := 0; i < workers; i++ {
		go func(index int) {
			defer wg.Done()

			name := "probe-" + string(rune('a'+index))

			require.NoError(
				t,
				server.RegisterReadinessProbe(name, func() error {
					return nil
				}),
			)
		}(i)
	}

	wg.Wait()
}

func newTestListener() (net.Listener, error) {
	return net.Listen("tcp", "127.0.0.1:0")
}
