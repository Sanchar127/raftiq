package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/sanchar127/raftiq/internal/kv"
	"github.com/sanchar127/raftiq/internal/observability"
	"github.com/sanchar127/raftiq/internal/raft"
	"github.com/sanchar127/raftiq/internal/server"
	"github.com/sanchar127/raftiq/internal/storage"
	"github.com/sanchar127/raftiq/internal/transport"
)

type peerFlag map[raft.NodeID]string

func (p peerFlag) String() string {
	if len(p) == 0 {
		return ""
	}

	values := make([]string, 0, len(p))
	for id, address := range p {
		values = append(values, fmt.Sprintf("%s=%s", id, address))
	}

	return strings.Join(values, ",")
}

func (p peerFlag) Set(value string) error {
	if p == nil {
		return errors.New("peer map is not initialized")
	}

	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}

	entries := strings.Split(value, ",")

	for _, entry := range entries {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}

		parts := strings.SplitN(entry, "=", 2)
		if len(parts) != 2 {
			return fmt.Errorf(
				"invalid peer %q: expected id=host:port",
				entry,
			)
		}

		id := strings.TrimSpace(parts[0])
		address := strings.TrimSpace(parts[1])

		if id == "" {
			return fmt.Errorf("invalid peer %q: empty node ID", entry)
		}

		if address == "" {
			return fmt.Errorf(
				"invalid peer %q: empty address",
				entry,
			)
		}

		nodeID := raft.NodeID(id)

		if _, exists := p[nodeID]; exists {
			return fmt.Errorf(
				"duplicate peer node ID %q",
				id,
			)
		}

		p[nodeID] = address
	}

	return nil
}

type config struct {
	nodeID      string
	raftAddr    string
	kvAddr      string
	metricsAddr string
	dataDir     string
	peers       peerFlag
	heartbeat   time.Duration
	election    time.Duration
	logLevel    string
}

func main() {
	cfg := config{
		peers: make(peerFlag),
	}

	flag.StringVar(
		&cfg.nodeID,
		"id",
		"",
		"Unique Raft node ID.",
	)

	flag.StringVar(
		&cfg.raftAddr,
		"raft-addr",
		":7000",
		"Address for the Raft RPC server.",
	)

	flag.StringVar(
		&cfg.kvAddr,
		"kv-addr",
		":8000",
		"Address for the client/KV RPC server.",
	)

	flag.StringVar(
		&cfg.metricsAddr,
		"metrics-addr",
		":9090",
		"Address for the Prometheus metrics HTTP server.",
	)

	flag.StringVar(
		&cfg.dataDir,
		"data-dir",
		"./data",
		"Directory containing persistent node data.",
	)

	flag.Var(
		&cfg.peers,
		"peers",
		"Raft peers as comma-separated id=host:port values.",
	)

	flag.DurationVar(
		&cfg.heartbeat,
		"heartbeat",
		100*time.Millisecond,
		"Raft heartbeat interval.",
	)

	flag.DurationVar(
		&cfg.election,
		"election",
		time.Second,
		"Raft election timeout.",
	)

	flag.StringVar(
		&cfg.logLevel,
		"log-level",
		"info",
		"Log level: debug, info, warn, or error.",
	)

	flag.Parse()

	if err := validateConfig(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "configuration error: %v\n", err)
		os.Exit(2)
	}

	logger := newLogger(cfg.logLevel)

	logger.Info(
		"starting raftiq node",
		"node_id", cfg.nodeID,
		"raft_addr", cfg.raftAddr,
		"kv_addr", cfg.kvAddr,
		"metrics_addr", cfg.metricsAddr,
		"data_dir", cfg.dataDir,
	)

	// -------------------------------------------------------------------------
	// Persistent storage.
	// -------------------------------------------------------------------------

	if err := os.MkdirAll(cfg.dataDir, 0o755); err != nil {
		logger.Error(
			"failed to create data directory",
			"error", err,
		)
		os.Exit(1)
	}

	walPath := fmt.Sprintf(
		"%s/node-%s.wal",
		cfg.dataDir,
		cfg.nodeID,
	)

	store, err := storage.OpenWAL(walPath)
	if err != nil {
		logger.Error(
			"failed to open WAL",
			"path", walPath,
			"error", err,
		)
		os.Exit(1)
	}

	defer func() {
		if err := store.Close(); err != nil {
			logger.Error(
				"failed to close WAL",
				"error", err,
			)
		}
	}()

	// -------------------------------------------------------------------------
	// Prometheus observability.
	// -------------------------------------------------------------------------

	registry := prometheus.NewRegistry()

	metrics := observability.NewMetrics(
		registry,
		cfg.nodeID,
	)

	// -------------------------------------------------------------------------
	// Raft node.
	// -------------------------------------------------------------------------

	node, err := raft.NewRaftNodeWithStorage(
		raft.NodeID(cfg.nodeID),
		store,
	)
	if err != nil {
		logger.Error(
			"failed to create Raft node",
			"error", err,
		)
		os.Exit(1)
	}

	node.SetLogger(logger)
	node.SetMetrics(metrics)

	// -------------------------------------------------------------------------
	// KV state machine.
	// -------------------------------------------------------------------------

	kvStore := kv.NewStore()

	appServer := server.NewServer(
		node,
		kvStore,
	)

	appServer.SetLogger(logger)

	// -------------------------------------------------------------------------
	// Raft transport.
	// -------------------------------------------------------------------------

	raftTransport := transport.NewGRPCTransport()
	raftTransport.SetLogger(logger)

	peerIDs := make([]raft.NodeID, 0, len(cfg.peers))

	for peerID, address := range cfg.peers {
		if peerID == raft.NodeID(cfg.nodeID) {
			continue
		}

		if err := raftTransport.AddPeer(
			peerID,
			address,
		); err != nil {
			logger.Error(
				"failed to add Raft peer",
				"peer_id", peerID,
				"address", address,
				"error", err,
			)
			os.Exit(1)
		}

		peerIDs = append(peerIDs, peerID)
	}

	node.SetTransport(
		raftTransport,
		peerIDs,
	)

	// -------------------------------------------------------------------------
	// Raft gRPC server.
	// -------------------------------------------------------------------------

	raftGRPCServer, err := transport.NewServer(
		cfg.raftAddr,
	)
	if err != nil {
		logger.Error(
			"failed to create Raft gRPC server",
			"address", cfg.raftAddr,
			"error", err,
		)
		os.Exit(1)
	}

	raftGRPCServer.SetLogger(logger)

	raftService, err := transport.NewRaftService(
		node,
	)
	if err != nil {
		logger.Error(
			"failed to create Raft gRPC service",
			"error", err,
		)

		shutdownTransportServer(
			logger,
			raftGRPCServer,
		)

		os.Exit(1)
	}

	raftService.SetLogger(logger)
	raftService.SetMetrics(metrics)

	if err := raftGRPCServer.RegisterRaftService(
		raftService,
	); err != nil {
		logger.Error(
			"failed to register Raft gRPC service",
			"error", err,
		)

		shutdownTransportServer(
			logger,
			raftGRPCServer,
		)

		os.Exit(1)
	}

	// -------------------------------------------------------------------------
	// Client/KV gRPC server.
	// -------------------------------------------------------------------------

	kvGRPCServer, err := transport.NewServer(
		cfg.kvAddr,
	)
	if err != nil {
		logger.Error(
			"failed to create KV gRPC server",
			"address", cfg.kvAddr,
			"error", err,
		)

		shutdownTransportServer(
			logger,
			raftGRPCServer,
		)

		os.Exit(1)
	}

	kvGRPCServer.SetLogger(logger)

	kvService, err := transport.NewKVService(
		appServer,
	)
	if err != nil {
		logger.Error(
			"failed to create KV gRPC service",
			"error", err,
		)

		shutdownTransportServer(
			logger,
			kvGRPCServer,
		)
		shutdownTransportServer(
			logger,
			raftGRPCServer,
		)

		os.Exit(1)
	}

	kvService.SetLogger(logger)

	if err := kvGRPCServer.RegisterKVService(
		kvService,
	); err != nil {
		logger.Error(
			"failed to register KV gRPC service",
			"error", err,
		)

		shutdownTransportServer(
			logger,
			kvGRPCServer,
		)
		shutdownTransportServer(
			logger,
			raftGRPCServer,
		)

		os.Exit(1)
	}

	// -------------------------------------------------------------------------
	// Prometheus HTTP server.
	// -------------------------------------------------------------------------

	metricsMux := http.NewServeMux()

	metricsMux.Handle(
		"/metrics",
		promhttp.HandlerFor(
			registry,
			promhttp.HandlerOpts{},
		),
	)

	metricsServer := &http.Server{
		Addr:              cfg.metricsAddr,
		Handler:           metricsMux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// -------------------------------------------------------------------------
	// Start servers.
	// -------------------------------------------------------------------------

	errCh := make(chan error, 3)

	go func() {
		logger.Info(
			"starting Raft gRPC server",
			"address", raftGRPCServer.Address(),
		)

		if err := raftGRPCServer.Serve(); err != nil {
			errCh <- fmt.Errorf(
				"Raft gRPC server: %w",
				err,
			)
		}
	}()

	go func() {
		logger.Info(
			"starting KV gRPC server",
			"address", kvGRPCServer.Address(),
		)

		if err := kvGRPCServer.Serve(); err != nil {
			errCh <- fmt.Errorf(
				"KV gRPC server: %w",
				err,
			)
		}
	}()

	go func() {
		logger.Info(
			"starting metrics server",
			"address", cfg.metricsAddr,
		)

		if err := metricsServer.ListenAndServe(); err != nil &&
			!errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf(
				"metrics server: %w",
				err,
			)
		}
	}()

	// -------------------------------------------------------------------------
	// Start application server and Raft.
	// -------------------------------------------------------------------------

	if err := appServer.Start(); err != nil {
		logger.Error(
			"failed to start application server",
			"error", err,
		)

		shutdown(
			logger,
			node,
			appServer,
			raftTransport,
			raftGRPCServer,
			kvGRPCServer,
			metricsServer,
		)

		os.Exit(1)
	}

	if err := node.Start(); err != nil {
		logger.Error(
			"failed to start Raft node",
			"error", err,
		)

		shutdown(
			logger,
			node,
			appServer,
			raftTransport,
			raftGRPCServer,
			kvGRPCServer,
			metricsServer,
		)

		os.Exit(1)
	}

	logger.Info(
		"raftiq node started",
		"node_id", cfg.nodeID,
	)

	// -------------------------------------------------------------------------
	// Signal handling.
	// -------------------------------------------------------------------------

	signalCtx, stopSignals := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer stopSignals()

	select {
	case <-signalCtx.Done():
		logger.Info("shutdown signal received")

	case err := <-errCh:
		if err != nil {
			logger.Error(
				"server failed",
				"error", err,
			)
		}
	}

	// -------------------------------------------------------------------------
	// Graceful shutdown.
	// -------------------------------------------------------------------------

	shutdown(
		logger,
		node,
		appServer,
		raftTransport,
		raftGRPCServer,
		kvGRPCServer,
		metricsServer,
	)

	logger.Info("raftiq node stopped")
}

func validateConfig(cfg config) error {
	if strings.TrimSpace(cfg.nodeID) == "" {
		return errors.New("--id is required")
	}

	if cfg.heartbeat <= 0 {
		return errors.New("--heartbeat must be greater than zero")
	}

	if cfg.election <= 0 {
		return errors.New("--election must be greater than zero")
	}

	if cfg.election <= cfg.heartbeat {
		return errors.New(
			"--election must be greater than --heartbeat",
		)
	}

	if strings.TrimSpace(cfg.raftAddr) == "" {
		return errors.New("--raft-addr cannot be empty")
	}

	if strings.TrimSpace(cfg.kvAddr) == "" {
		return errors.New("--kv-addr cannot be empty")
	}

	if strings.TrimSpace(cfg.metricsAddr) == "" {
		return errors.New("--metrics-addr cannot be empty")
	}

	if strings.TrimSpace(cfg.dataDir) == "" {
		return errors.New("--data-dir cannot be empty")
	}

	switch strings.ToLower(strings.TrimSpace(cfg.logLevel)) {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf(
			"invalid --log-level %q: expected debug, info, warn, or error",
			cfg.logLevel,
		)
	}

	return nil
}

func newLogger(level string) *slog.Logger {
	var slogLevel slog.Level

	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		slogLevel = slog.LevelDebug

	case "warn":
		slogLevel = slog.LevelWarn

	case "error":
		slogLevel = slog.LevelError

	default:
		slogLevel = slog.LevelInfo
	}

	handler := slog.NewJSONHandler(
		os.Stdout,
		&slog.HandlerOptions{
			Level: slogLevel,
		},
	)

	return slog.New(handler)
}

func shutdown(
	logger *slog.Logger,
	node *raft.RaftNode,
	appServer *server.Server,
	raftTransport *transport.GRPCTransport,
	raftGRPCServer *transport.Server,
	kvGRPCServer *transport.Server,
	metricsServer *http.Server,
) {
	const shutdownTimeout = 10 * time.Second

	ctx, cancel := context.WithTimeout(
		context.Background(),
		shutdownTimeout,
	)
	defer cancel()

	// Stop accepting new gRPC requests first.
	if raftGRPCServer != nil {
		if err := raftGRPCServer.Shutdown(ctx); err != nil {
			logger.Error(
				"failed to shutdown Raft gRPC server",
				"error", err,
			)
		}
	}

	if kvGRPCServer != nil {
		if err := kvGRPCServer.Shutdown(ctx); err != nil {
			logger.Error(
				"failed to shutdown KV gRPC server",
				"error", err,
			)
		}
	}

	// Stop the metrics HTTP server.
	if metricsServer != nil {
		if err := metricsServer.Shutdown(ctx); err != nil {
			logger.Error(
				"failed to shutdown metrics server",
				"error", err,
			)
		}
	}

	// Stop Raft before closing its transport/storage dependencies.
	if node != nil {
		node.Stop()
	}

	if appServer != nil {
		appServer.Stop()
	}

	if raftTransport != nil {
		if err := raftTransport.Close(); err != nil {
			logger.Error(
				"failed to close Raft transport",
				"error", err,
			)
		}
	}
}

func shutdownTransportServer(
	logger *slog.Logger,
	srv *transport.Server,
) {
	if srv == nil {
		return
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		logger.Error(
			"failed to shutdown transport server",
			"error", err,
		)
	}
}
