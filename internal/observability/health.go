package observability

import (
	"encoding/json"
	"errors"
	"net/http"
	"sync"
)

const (
	healthPath    = "/healthz"
	readinessPath = "/readyz"
)

var ErrHealthServerClosed = errors.New("health server is closed")

type HealthProbe func() error

type HealthServer struct {
	mu sync.RWMutex

	server *http.Server

	readinessProbes map[string]HealthProbe
}

type healthResponse struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks,omitempty"`
}

func NewHealthServer(address string) *HealthServer {
	mux := http.NewServeMux()

	server := &HealthServer{
		readinessProbes: make(map[string]HealthProbe),
		server: &http.Server{
			Addr:              address,
			Handler:           mux,
			ReadHeaderTimeout: 5 * 1e9,
			ReadTimeout:       10 * 1e9,
			WriteTimeout:      10 * 1e9,
			IdleTimeout:       60 * 1e9,
		},
	}

	mux.HandleFunc(healthPath, server.handleHealth)
	mux.HandleFunc(readinessPath, server.handleReadiness)

	return server
}

func (s *HealthServer) RegisterReadinessProbe(
	name string,
	probe HealthProbe,
) error {
	if s == nil {
		return ErrHealthServerClosed
	}

	if name == "" {
		return errors.New("readiness probe name is required")
	}

	if probe == nil {
		return errors.New("readiness probe is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.readinessProbes == nil {
		s.readinessProbes = make(map[string]HealthProbe)
	}

	s.readinessProbes[name] = probe

	return nil
}

func (s *HealthServer) UnregisterReadinessProbe(name string) {
	if s == nil || name == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.readinessProbes, name)
}

func (s *HealthServer) Serve() error {
	if s == nil || s.server == nil {
		return ErrHealthServerClosed
	}

	err := s.server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}

	return err
}

func (s *HealthServer) Shutdown(ctx context.Context) error {
	if s == nil || s.server == nil {
		return nil
	}

	return s.server.Shutdown(ctx)
}

func (s *HealthServer) Address() string {
	if s == nil || s.server == nil {
		return ""
	}

	return s.server.Addr
}

func (s *HealthServer) handleHealth(
	w http.ResponseWriter,
	_ *http.Request,
) {
	writeHealthResponse(
		w,
		http.StatusOK,
		healthResponse{
			Status: "ok",
		},
	)
}

func (s *HealthServer) handleReadiness(
	w http.ResponseWriter,
	_ *http.Request,
) {
	if s == nil {
		writeHealthResponse(
			w,
			http.StatusServiceUnavailable,
			healthResponse{
				Status: "not_ready",
			},
		)

		return
	}

	s.mu.RLock()

	probes := make(map[string]HealthProbe, len(s.readinessProbes))

	for name, probe := range s.readinessProbes {
		probes[name] = probe
	}

	s.mu.RUnlock()

	checks := make(map[string]string, len(probes))

	ready := true

	for name, probe := range probes {
		if err := probe(); err != nil {
			checks[name] = err.Error()
			ready = false
			continue
		}

		checks[name] = "ok"
	}

	if !ready {
		writeHealthResponse(
			w,
			http.StatusServiceUnavailable,
			healthResponse{
				Status: "not_ready",
				Checks: checks,
			},
		)

		return
	}

	writeHealthResponse(
		w,
		http.StatusOK,
		healthResponse{
			Status: "ready",
			Checks: checks,
		},
	)
}

func writeHealthResponse(
	w http.ResponseWriter,
	status int,
	response healthResponse,
) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	_ = json.NewEncoder(w).Encode(response)
}

