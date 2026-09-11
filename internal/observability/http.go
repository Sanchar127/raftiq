package observability

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const defaultMetricsPath = "/metrics"

var ErrMetricsServerClosed = errors.New("metrics server is closed")

type MetricsServer struct {
	server   *http.Server
	listener net.Listener
}

func NewMetricsServer(
	address string,
	gatherer prometheus.Gatherer,
) (*MetricsServer, error) {
	if address == "" {
		return nil, errors.New("metrics server address is required")
	}

	if gatherer == nil {
		return nil, errors.New("metrics gatherer is required")
	}

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("listen for metrics server: %w", err)
	}

	mux := http.NewServeMux()
	mux.Handle(
		defaultMetricsPath,
		promhttp.HandlerFor(
			gatherer,
			promhttp.HandlerOpts{
				EnableOpenMetrics: true,
			},
		),
	)

	return &MetricsServer{
		listener: listener,
		server: &http.Server{
			Addr:              listener.Addr().String(),
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
			IdleTimeout:       60 * time.Second,
		},
	}, nil
}

func (s *MetricsServer) Serve() error {
	if s == nil || s.server == nil || s.listener == nil {
		return ErrMetricsServerClosed
	}

	if err := s.server.Serve(s.listener); err != nil &&
		!errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve metrics: %w", err)
	}

	return nil
}

func (s *MetricsServer) Shutdown(ctx context.Context) error {
	if s == nil || s.server == nil {
		return nil
	}

	if ctx == nil {
		return errors.New("shutdown context is required")
	}

	if err := s.server.Shutdown(ctx); err != nil {
		return fmt.Errorf("shutdown metrics server: %w", err)
	}

	return nil
}

func (s *MetricsServer) Address() string {
	if s == nil || s.listener == nil {
		return ""
	}

	return s.listener.Addr().String()
}
