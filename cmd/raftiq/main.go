package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/sanchar127/raftiq/internal/kv"
	"github.com/sanchar127/raftiq/internal/model"
	"github.com/sanchar127/raftiq/internal/observability"
	"github.com/sanchar127/raftiq/internal/raft"
	"github.com/sanchar127/raftiq/internal/scheduler"
	"github.com/sanchar127/raftiq/internal/server"
	"github.com/sanchar127/raftiq/internal/storage"
	"github.com/sanchar127/raftiq/internal/transport"
	"github.com/sanchar127/raftiq/internal/worker"
)

type peerFlag map[raft.NodeID]string

func (p peerFlag) String() string {
	if len(p) == 0 {
		return ""
	}

	values := make([]string, 0, len(p))

	for id, address := range p {
		values = append(
			values,
			fmt.Sprintf("%s=%s", id, address),
		)
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
			return fmt.Errorf(
				"invalid peer %q: empty node ID",
				entry,
			)
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

type workerFlag []string

func (w workerFlag) String() string {
	return strings.Join(w, ",")
}

func (w *workerFlag) Set(value string) error {
	value = strings.TrimSpace(value)

	if value == "" {
		return errors.New("worker list cannot be empty")
	}

	entries := strings.Split(value, ",")

	for _, entry := range entries {
		workerID := strings.TrimSpace(entry)

		if workerID == "" {
			return errors.New("worker ID cannot be empty")
		}

		*w = append(*w, workerID)
	}

	return nil
}

type config struct {
	nodeID      string
	raftAddr    string
	kvAddr      string
	metricsAddr string
	healthAddr  string
	dataDir     string
	peers       peerFlag
	workers     workerFlag
	heartbeat   time.Duration
	election    time.Duration
	logLevel    string

	tlsCA   string
	tlsCert string
	tlsKey  string
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
		&cfg.healthAddr,
		"health-addr",
		":8080",
		"Address for the health and readiness HTTP server.",
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

	flag.Var(
		&cfg.workers,
		"workers",
		"Scheduler workers as comma-separated worker IDs.",
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

	flag.StringVar(
		&cfg.tlsCA,
		"tls-ca",
		"",
		"Path to the TLS CA certificate.",
	)

	flag.StringVar(
		&cfg.tlsCert,
		"tls-cert",
		"",
		"Path to the node TLS certificate.",
	)

	flag.StringVar(
		&cfg.tlsKey,
		"tls-key",
		"",
		"Path to the node TLS private key.",
	)

	flag.Parse()

	if err := validateConfig(cfg); err != nil {
		fmt.Fprintf(
			os.Stderr,
			"configuration error: %v\n",
			err,
		)
		os.Exit(2)
	}

	logger := newLogger(cfg.logLevel)

	logger.Info(
		"starting raftiq node",
		"node_id", cfg.nodeID,
		"raft_addr", cfg.raftAddr,
		"kv_addr", cfg.kvAddr,
		"metrics_addr", cfg.metricsAddr,
		"health_addr", cfg.healthAddr,
		"data_dir", cfg.dataDir,
		"workers", []string(cfg.workers),
	)

	// -------------------------------------------------------------------------
	// TLS configuration.
	// -------------------------------------------------------------------------

	clientTLS, err := transport.LoadTLSClientConfig(
		transport.TLSConfig{
			CAFile:   cfg.tlsCA,
			CertFile: cfg.tlsCert,
			KeyFile:  cfg.tlsKey,
		},
	)
	if err != nil {
		logger.Error(
			"failed to load Raft client TLS configuration",
			"error", err,
		)
		os.Exit(1)
	}

	allowedPeerSANs := make(map[string]struct{})

	for peerID := range cfg.peers {
		if peerID == raft.NodeID(cfg.nodeID) {
			continue
		}

		allowedPeerSANs[fmt.Sprintf("%s.raftiq", peerID)] = struct{}{}
	}

	serverTLS, err := transport.LoadTLSServerConfig(
		transport.TLSConfig{
			CAFile:   cfg.tlsCA,
			CertFile: cfg.tlsCert,
			KeyFile:  cfg.tlsKey,
		},
		allowedPeerSANs,
	)
	if err != nil {
		logger.Error(
			"failed to load Raft server TLS configuration",
			"error", err,
		)
		os.Exit(1)
	}

	logger.Info(
		"Raft mutual TLS configured",
		"allowed_peer_sans", len(allowedPeerSANs),
	)

	// -------------------------------------------------------------------------
	// Persistent storage.
	// -------------------------------------------------------------------------

	if err := os.MkdirAll(
		cfg.dataDir,
		0o755,
	); err != nil {
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

	storageMetrics := observability.NewStorageMetrics(
		cfg.nodeID,
		metrics,
	)

	store.SetMetrics(storageMetrics)

	kvMetrics := observability.NewKVMetrics(metrics)

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
	appServer.SetKVMetrics(kvMetrics)

	// -------------------------------------------------------------------------
	// Scheduler.
	// -------------------------------------------------------------------------

	workerSelector, err := scheduler.NewHashWorkerSelector(
		[]string(cfg.workers),
	)
	if err != nil {
		logger.Error(
			"failed to create scheduler worker selector",
			"error", err,
		)
		os.Exit(2)
	}

	jobScheduler, err := scheduler.New(
		node,
		kvStore,
		appServer.Applier(),
		workerSelector,
		scheduler.Config{
			Interval: scheduler.DefaultInterval,
			Lease:    scheduler.DefaultLease,
		},
	)
	if err != nil {
		logger.Error(
			"failed to create scheduler",
			"error", err,
		)
		os.Exit(1)
	}

	jobScheduler.SetLogger(logger)
	jobScheduler.SetMetrics(
		observability.NewSchedulerMetrics(
			cfg.nodeID,
			metrics,
		),
	)

	// -------------------------------------------------------------------------
	// Workers.
	// -------------------------------------------------------------------------

	workers := make([]*worker.Worker, 0, len(cfg.workers))

	jobHandler := worker.HandlerFunc(
		func(
			ctx context.Context,
			job model.Job,
		) error {
			logger.Info(
				"job handler executed",
				"job_id", job.ID,
				"worker_id", job.AssignedWorkerID,
				"execution_id", job.ExecutionID,
				"attempt", job.Attempt,
				"payload_size", len(job.Payload),
			)

			return nil
		},
	)

	for _, workerID := range cfg.workers {
		w, err := worker.New(
			workerID,
			jobHandler,
		)
		if err != nil {
			logger.Error(
				"failed to create worker",
				"worker_id", workerID,
				"error", err,
			)
			os.Exit(1)
		}

		if err := w.ConfigureLoop(
			kvStore,
			worker.Config{
				Interval: scheduler.DefaultInterval,
				Raft:     node,
				Applier:  appServer.Applier(),
				Metrics: observability.NewWorkerMetrics(
					cfg.nodeID,
					metrics,
				),
			},
		); err != nil {
			logger.Error(
				"failed to configure worker",
				"worker_id", workerID,
				"error", err,
			)
			os.Exit(1)
		}

		w.SetLogger(logger)
		workers = append(workers, w)
	}

	// -------------------------------------------------------------------------
	// Raft transport.
	// -------------------------------------------------------------------------

	raftTransport := transport.NewGRPCTransport()
	raftTransport.SetLogger(logger)

	if err := raftTransport.SetTLSConfig(
		clientTLS,
	); err != nil {
		logger.Error(
			"failed to configure Raft transport TLS",
			"error", err,
		)

		raftTransport.Close()
		os.Exit(1)
	}

	peerIDs := make(
		[]raft.NodeID,
		0,
		len(cfg.peers),
	)

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

			raftTransport.Close()
			os.Exit(1)
		}

		peerIDs = append(
			peerIDs,
			peerID,
		)
	}

	node.SetTransport(
		raftTransport,
		peerIDs,
	)

	if err := node.BootstrapMembership(); err != nil {
		logger.Error(
			"failed to bootstrap Raft membership",
			"error", err,
		)

		raftTransport.Close()
		os.Exit(1)
	}

	// -------------------------------------------------------------------------
	// Raft gRPC server.
	// -------------------------------------------------------------------------

	raftGRPCServer, err := transport.NewServer(
		cfg.raftAddr,
		grpc.Creds(
			credentials.NewTLS(serverTLS),
		),
	)
	if err != nil {
		logger.Error(
			"failed to create Raft gRPC server",
			"address", cfg.raftAddr,
			"error", err,
		)

		raftTransport.Close()
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

		raftTransport.Close()
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

		raftTransport.Close()
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

		raftTransport.Close()
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

		raftTransport.Close()
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

		raftTransport.Close()
		os.Exit(1)
	}

	// -------------------------------------------------------------------------
	// Prometheus metrics server.
	// -------------------------------------------------------------------------

	metricsServer, err := observability.NewMetricsServer(
		cfg.metricsAddr,
		registry,
	)
	if err != nil {
		logger.Error(
			"failed to create metrics server",
			"address", cfg.metricsAddr,
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

		raftTransport.Close()
		os.Exit(1)
	}

	// -------------------------------------------------------------------------
	// Health and readiness server.
	// -------------------------------------------------------------------------

	healthServer := observability.NewHealthServer(
		cfg.healthAddr,
	)

	if err := healthServer.RegisterReadinessProbe(
		"raft",
		observability.RaftReadinessProbe(node),
	); err != nil {
		logger.Error(
			"failed to register Raft readiness probe",
			"error", err,
		)

		_ = metricsServer.Shutdown(
			context.Background(),
		)

		shutdownTransportServer(
			logger,
			kvGRPCServer,
		)

		shutdownTransportServer(
			logger,
			raftGRPCServer,
		)

		raftTransport.Close()
		os.Exit(1)
	}

	// -------------------------------------------------------------------------
	// Start servers.
	// -------------------------------------------------------------------------

	errCh := make(chan error, len(workers)+6)

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
			"address", metricsServer.Address(),
		)

		if err := metricsServer.Serve(); err != nil {
			errCh <- fmt.Errorf(
				"metrics server: %w",
				err,
			)
		}
	}()

	go func() {
		logger.Info(
			"starting health server",
			"address", healthServer.Address(),
		)

		if err := healthServer.Serve(); err != nil &&
			!errors.Is(err, observability.ErrHealthServerClosed) {
			errCh <- fmt.Errorf(
				"health server: %w",
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
			healthServer,
			nil,
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
			healthServer,
			nil,
		)

		os.Exit(1)
	}

	// -------------------------------------------------------------------------
	// Start scheduler and workers.
	// -------------------------------------------------------------------------

	executionCtx, executionCancel := context.WithCancel(
		context.Background(),
	)
	defer executionCancel()

	go func() {
		logger.Info(
			"starting scheduler",
			"interval", scheduler.DefaultInterval,
			"lease", scheduler.DefaultLease,
			"workers", []string(cfg.workers),
		)

		if err := jobScheduler.Run(executionCtx); err != nil &&
			!errors.Is(err, context.Canceled) &&
			!errors.Is(err, context.DeadlineExceeded) {
			errCh <- fmt.Errorf(
				"scheduler: %w",
				err,
			)
		}
	}()

	for _, w := range workers {
		go func(w *worker.Worker) {
			logger.Info(
				"starting worker",
				"worker_id", w.ID(),
				"interval", scheduler.DefaultInterval,
			)

			if err := w.Run(executionCtx); err != nil &&
				!errors.Is(err, context.Canceled) &&
				!errors.Is(err, context.DeadlineExceeded) {
				errCh <- fmt.Errorf(
					"worker %s: %w",
					w.ID(),
					err,
				)
			}
		}(w)
	}

	logger.Info(
		"raftiq node started",
		"node_id", cfg.nodeID,
		"workers", len(workers),
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

	executionCancel()

	shutdown(
		logger,
		node,
		appServer,
		raftTransport,
		raftGRPCServer,
		kvGRPCServer,
		metricsServer,
		healthServer,
		jobScheduler,
	)

	logger.Info("raftiq node stopped")
}

func validateConfig(cfg config) error {
	if strings.TrimSpace(cfg.nodeID) == "" {
		return errors.New("--id is required")
	}

	if cfg.heartbeat <= 0 {
		return errors.New(
			"--heartbeat must be greater than zero",
		)
	}

	if cfg.election <= 0 {
		return errors.New(
			"--election must be greater than zero",
		)
	}

	if cfg.election <= cfg.heartbeat {
		return errors.New(
			"--election must be greater than --heartbeat",
		)
	}

	if strings.TrimSpace(cfg.raftAddr) == "" {
		return errors.New(
			"--raft-addr cannot be empty",
		)
	}

	if strings.TrimSpace(cfg.kvAddr) == "" {
		return errors.New(
			"--kv-addr cannot be empty",
		)
	}

	if strings.TrimSpace(cfg.metricsAddr) == "" {
		return errors.New(
			"--metrics-addr cannot be empty",
		)
	}

	if strings.TrimSpace(cfg.healthAddr) == "" {
		return errors.New(
			"--health-addr cannot be empty",
		)
	}

	if strings.TrimSpace(cfg.dataDir) == "" {
		return errors.New(
			"--data-dir cannot be empty",
		)
	}

	switch strings.ToLower(
		strings.TrimSpace(cfg.logLevel),
	) {
	case "debug", "info", "warn", "error":

	default:
		return fmt.Errorf(
			"invalid --log-level %q: expected debug, info, warn, or error",
			cfg.logLevel,
		)
	}

	if strings.TrimSpace(cfg.tlsCA) == "" {
		return errors.New("--tls-ca is required")
	}

	if strings.TrimSpace(cfg.tlsCert) == "" {
		return errors.New("--tls-cert is required")
	}

	if strings.TrimSpace(cfg.tlsKey) == "" {
		return errors.New("--tls-key is required")
	}

	if len(cfg.peers) == 0 {
		return errors.New("--peers requires at least one peer")
	}

	if len(cfg.workers) == 0 {
		return errors.New("--workers requires at least one worker")
	}

	return nil
}

func newLogger(level string) *slog.Logger {
	var slogLevel slog.Level

	switch strings.ToLower(
		strings.TrimSpace(level),
	) {
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
	metricsServer *observability.MetricsServer,
	healthServer *observability.HealthServer,
	jobScheduler *scheduler.Scheduler,
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

	// Scheduler and workers are stopped by cancellation of executionCtx
	// before shutdown() is called.
	_ = jobScheduler

	// Stop Raft before shutting down readiness so the readiness probe
	// can observe the node transitioning out of service.
	if node != nil {
		node.Stop()
	}

	if appServer != nil {
		appServer.Stop()
	}

	// Stop health/readiness endpoint after Raft has stopped.
	if healthServer != nil {
		if err := healthServer.Shutdown(ctx); err != nil {
			logger.Error(
				"failed to shutdown health server",
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

	// Close transport only after Raft has stopped.
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
