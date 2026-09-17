package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"os"
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

type runtime struct {
	logger *slog.Logger
	cfg    config

	clientTLS *tls.Config
	serverTLS *tls.Config

	metricsRegistry *prometheus.Registry
	metrics         *observability.Metrics
	kvMetrics       *observability.KVMetrics

	store *storage.WALStorage
	node  *raft.RaftNode

	kvStore *kv.Store

	appServer *server.Server

	jobScheduler *scheduler.Scheduler
	workers      []*worker.Worker

	raftTransport *transport.GRPCTransport

	raftGRPCServer *transport.Server
	kvGRPCServer   *transport.Server

	metricsServer *observability.MetricsServer
	healthServer  *observability.HealthServer

	executionCtx    context.Context
	executionCancel context.CancelFunc

	errCh chan error

	shutdownOnce bool
}

func newRuntime(
	cfg config,
	logger *slog.Logger,
) (*runtime, error) {
	r := &runtime{
		logger: logger,
		cfg:    cfg,
		errCh:  make(chan error, len(cfg.workers)+6),
	}

	if err := r.initialize(); err != nil {
		r.Shutdown()
		return nil, err
	}

	return r, nil
}

func (r *runtime) initialize() error {
	if err := r.initializeTLS(); err != nil {
		return err
	}

	if err := r.initializeStorage(); err != nil {
		return err
	}

	if err := r.initializeRaft(); err != nil {
		return err
	}

	if err := r.initializeApplication(); err != nil {
		return err
	}

	if err := r.initializeScheduler(); err != nil {
		return err
	}

	if err := r.initializeWorkers(); err != nil {
		return err
	}

	if err := r.initializeTransport(); err != nil {
		return err
	}

	if err := r.initializeServers(); err != nil {
		return err
	}

	return nil
}

func (r *runtime) initializeTLS() error {
	clientTLS, err := transport.LoadTLSClientConfig(
		transport.TLSConfig{
			CAFile:   r.cfg.tlsCA,
			CertFile: r.cfg.tlsCert,
			KeyFile:  r.cfg.tlsKey,
		},
	)
	if err != nil {
		return fmt.Errorf(
			"load Raft client TLS configuration: %w",
			err,
		)
	}

	allowedPeerSANs := make(map[string]struct{})

	for peerID := range r.cfg.peers {
		if peerID == raft.NodeID(r.cfg.nodeID) {
			continue
		}

		allowedPeerSANs[fmt.Sprintf(
			"%s.raftiq",
			peerID,
		)] = struct{}{}
	}

	serverTLS, err := transport.LoadTLSServerConfig(
		transport.TLSConfig{
			CAFile:   r.cfg.tlsCA,
			CertFile: r.cfg.tlsCert,
			KeyFile:  r.cfg.tlsKey,
		},
		allowedPeerSANs,
	)
	if err != nil {
		return fmt.Errorf(
			"load Raft server TLS configuration: %w",
			err,
		)
	}

	r.logger.Info(
		"Raft mutual TLS configured",
		"allowed_peer_sans", len(allowedPeerSANs),
	)

	r.clientTLS = clientTLS
	r.serverTLS = serverTLS

	return nil
}

func (r *runtime) initializeStorage() error {
	if err := os.MkdirAll(
		r.cfg.dataDir,
		0o755,
	); err != nil {
		return fmt.Errorf(
			"create data directory: %w",
			err,
		)
	}

	walPath := fmt.Sprintf(
		"%s/node-%s.wal",
		r.cfg.dataDir,
		r.cfg.nodeID,
	)

	store, err := storage.OpenWAL(walPath)
	if err != nil {
		return fmt.Errorf(
			"open WAL %q: %w",
			walPath,
			err,
		)
	}

	r.store = store

	registry := prometheus.NewRegistry()

	r.metricsRegistry = registry

	r.metrics = observability.NewMetrics(
		registry,
		r.cfg.nodeID,
	)

	storageMetrics := observability.NewStorageMetrics(
		r.cfg.nodeID,
		r.metrics,
	)

	r.store.SetMetrics(storageMetrics)

	r.kvMetrics = observability.NewKVMetrics(
		r.metrics,
	)

	return nil
}

func (r *runtime) initializeRaft() error {
	node, err := raft.NewRaftNodeWithStorage(
		raft.NodeID(r.cfg.nodeID),
		r.store,
	)
	if err != nil {
		return fmt.Errorf(
			"create Raft node: %w",
			err,
		)
	}

	node.SetLogger(r.logger)
	node.SetMetrics(r.metrics)

	r.node = node

	return nil
}

func (r *runtime) initializeApplication() error {
	kvStore := kv.NewStore()

	r.appServer = server.NewServer(
		r.node,
		kvStore,
	)

	r.appServer.SetLogger(r.logger)
	r.appServer.SetKVMetrics(r.kvMetrics)

	r.kvStore = kvStore

	return nil
}

func (r *runtime) initializeScheduler() error {
	workerSelector, err := scheduler.NewHashWorkerSelector(
		[]string(r.cfg.workers),
	)
	if err != nil {
		return fmt.Errorf(
			"create scheduler worker selector: %w",
			err,
		)
	}

	jobScheduler, err := scheduler.New(
		r.node,
		r.kvStore,
		r.appServer.Applier(),
		workerSelector,
		scheduler.Config{
			Interval: scheduler.DefaultInterval,
			Lease:    scheduler.DefaultLease,
		},
	)
	if err != nil {
		return fmt.Errorf(
			"create scheduler: %w",
			err,
		)
	}

	jobScheduler.SetLogger(r.logger)
	jobScheduler.SetMetrics(
		observability.NewSchedulerMetrics(
			r.cfg.nodeID,
			r.metrics,
		),
	)

	r.jobScheduler = jobScheduler

	return nil
}

func (r *runtime) initializeWorkers() error {
	jobHandler := worker.HandlerFunc(
		func(
			ctx context.Context,
			job model.Job,
		) error {
			r.logger.Info(
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

	r.workers = make(
		[]*worker.Worker,
		0,
		len(r.cfg.workers),
	)

	for _, workerID := range r.cfg.workers {
		w, err := worker.New(
			workerID,
			jobHandler,
		)
		if err != nil {
			return fmt.Errorf(
				"create worker %q: %w",
				workerID,
				err,
			)
		}

		if err := w.ConfigureLoop(
			r.kvStore,
			worker.Config{
				Interval: scheduler.DefaultInterval,
				Raft:     r.node,
				Applier:  r.appServer.Applier(),
				Metrics: observability.NewWorkerMetrics(
					r.cfg.nodeID,
					r.metrics,
				),
			},
		); err != nil {
			return fmt.Errorf(
				"configure worker %q: %w",
				workerID,
				err,
			)
		}

		w.SetLogger(r.logger)

		r.workers = append(r.workers, w)
	}

	return nil
}

func (r *runtime) initializeTransport() error {
	raftTransport := transport.NewGRPCTransport()
	raftTransport.SetLogger(r.logger)

	if err := raftTransport.SetTLSConfig(
		r.clientTLS,
	); err != nil {
		raftTransport.Close()

		return fmt.Errorf(
			"configure Raft transport TLS: %w",
			err,
		)
	}

	peerIDs := make(
		[]raft.NodeID,
		0,
		len(r.cfg.peers),
	)

	for peerID, address := range r.cfg.peers {
		if peerID == raft.NodeID(r.cfg.nodeID) {
			continue
		}

		if err := raftTransport.AddPeer(
			peerID,
			address,
		); err != nil {
			raftTransport.Close()

			return fmt.Errorf(
				"add Raft peer %q at %q: %w",
				peerID,
				address,
				err,
			)
		}

		peerIDs = append(peerIDs, peerID)
	}

	r.node.SetTransport(
		raftTransport,
		peerIDs,
	)

	if err := r.node.BootstrapMembership(); err != nil {
		raftTransport.Close()

		return fmt.Errorf(
			"bootstrap Raft membership: %w",
			err,
		)
	}

	r.raftTransport = raftTransport

	return nil
}

func (r *runtime) initializeServers() error {
	if err := r.initializeRaftGRPCServer(); err != nil {
		return err
	}

	if err := r.initializeKVGRPCServer(); err != nil {
		return err
	}

	if err := r.initializeMetricsServer(); err != nil {
		return err
	}

	if err := r.initializeHealthServer(); err != nil {
		return err
	}

	return nil
}

func (r *runtime) initializeRaftGRPCServer() error {
	raftGRPCServer, err := transport.NewServer(
		r.cfg.raftAddr,
		grpc.Creds(
			credentials.NewTLS(r.serverTLS),
		),
	)
	if err != nil {
		return fmt.Errorf(
			"create Raft gRPC server: %w",
			err,
		)
	}

	raftGRPCServer.SetLogger(r.logger)

	raftService, err := transport.NewRaftService(
		r.node,
	)
	if err != nil {
		shutdownTransportServer(
			r.logger,
			raftGRPCServer,
		)

		return fmt.Errorf(
			"create Raft gRPC service: %w",
			err,
		)
	}

	raftService.SetLogger(r.logger)
	raftService.SetMetrics(r.metrics)

	if err := raftGRPCServer.RegisterRaftService(
		raftService,
	); err != nil {
		shutdownTransportServer(
			r.logger,
			raftGRPCServer,
		)

		return fmt.Errorf(
			"register Raft gRPC service: %w",
			err,
		)
	}

	r.raftGRPCServer = raftGRPCServer

	return nil
}

func (r *runtime) initializeKVGRPCServer() error {
	kvGRPCServer, err := transport.NewServer(
		r.cfg.kvAddr,
	)
	if err != nil {
		return fmt.Errorf(
			"create KV gRPC server: %w",
			err,
		)
	}

	kvGRPCServer.SetLogger(r.logger)

	kvService, err := transport.NewKVService(
		r.appServer,
	)
	if err != nil {
		shutdownTransportServer(
			r.logger,
			kvGRPCServer,
		)

		return fmt.Errorf(
			"create KV gRPC service: %w",
			err,
		)
	}

	kvService.SetLogger(r.logger)

	if err := kvGRPCServer.RegisterKVService(
		kvService,
	); err != nil {
		shutdownTransportServer(
			r.logger,
			kvGRPCServer,
		)

		return fmt.Errorf(
			"register KV gRPC service: %w",
			err,
		)
	}

	r.kvGRPCServer = kvGRPCServer

	return nil
}

func (r *runtime) initializeMetricsServer() error {
	metricsServer, err := observability.NewMetricsServer(
		r.cfg.metricsAddr,
		r.metricsRegistry,
	)
	if err != nil {
		return fmt.Errorf(
			"create metrics server: %w",
			err,
		)
	}

	r.metricsServer = metricsServer

	return nil
}

func (r *runtime) initializeHealthServer() error {
	healthServer := observability.NewHealthServer(
		r.cfg.healthAddr,
	)

	if err := healthServer.RegisterReadinessProbe(
		"raft",
		observability.RaftReadinessProbe(r.node),
	); err != nil {
		return fmt.Errorf(
			"register Raft readiness probe: %w",
			err,
		)
	}

	r.healthServer = healthServer

	return nil
}

func (r *runtime) Start(ctx context.Context) error {
	r.executionCtx, r.executionCancel = context.WithCancel(ctx)

	if err := r.startServers(); err != nil {
		r.executionCancel()
		return err
	}

	if err := r.appServer.Start(); err != nil {
		r.executionCancel()

		r.stopServers()
		return fmt.Errorf(
			"start application server: %w",
			err,
		)
	}

	if err := r.node.Start(); err != nil {
		r.executionCancel()

		r.appServer.Stop()
		r.stopServers()

		return fmt.Errorf(
			"start Raft node: %w",
			err,
		)
	}

	r.startScheduler()
	r.startWorkers()

	r.logger.Info(
		"raftiq node started",
		"node_id", r.cfg.nodeID,
		"workers", len(r.workers),
	)

	return nil
}

func (r *runtime) startServers() error {
	go r.serveTransportServer(
		r.raftGRPCServer,
		"Raft gRPC server",
	)

	go r.serveTransportServer(
		r.kvGRPCServer,
		"KV gRPC server",
	)

	go r.serveMetricsServer()

	go r.serveHealthServer()

	return nil
}

func (r *runtime) serveTransportServer(
	srv *transport.Server,
	name string,
) {
	r.logger.Info(
		"starting "+name,
		"address", srv.Address(),
	)

	if err := srv.Serve(); err != nil {
		r.reportError(
			fmt.Errorf("%s: %w", name, err),
		)
	}
}

func (r *runtime) serveMetricsServer() {
	r.logger.Info(
		"starting metrics server",
		"address", r.metricsServer.Address(),
	)

	if err := r.metricsServer.Serve(); err != nil {
		r.reportError(
			fmt.Errorf(
				"metrics server: %w",
				err,
			),
		)
	}
}

func (r *runtime) serveHealthServer() {
	r.logger.Info(
		"starting health server",
		"address", r.healthServer.Address(),
	)

	if err := r.healthServer.Serve(); err != nil &&
		!errors.Is(
			err,
			observability.ErrHealthServerClosed,
		) {
		r.reportError(
			fmt.Errorf(
				"health server: %w",
				err,
			),
		)
	}
}

func (r *runtime) startScheduler() {
	go func() {
		r.logger.Info(
			"starting scheduler",
			"interval", scheduler.DefaultInterval,
			"lease", scheduler.DefaultLease,
			"workers", []string(r.cfg.workers),
		)

		if err := r.jobScheduler.Run(r.executionCtx); err != nil &&
			!isShutdownError(err) {
			r.reportError(
				fmt.Errorf(
					"scheduler: %w",
					err,
				),
			)
		}
	}()
}

func (r *runtime) startWorkers() {
	for _, w := range r.workers {
		go func(w *worker.Worker) {
			r.logger.Info(
				"starting worker",
				"worker_id", w.ID(),
				"interval", scheduler.DefaultInterval,
			)

			if err := w.Run(r.executionCtx); err != nil &&
				!isShutdownError(err) {
				r.reportError(
					fmt.Errorf(
						"worker %s: %w",
						w.ID(),
						err,
					),
				)
			}
		}(w)
	}
}

func (r *runtime) reportError(err error) {
	if err == nil {
		return
	}

	select {
	case r.errCh <- err:
	default:
		r.logger.Error(
			"runtime error channel full",
			"error", err,
		)
	}
}

func (r *runtime) Errors() <-chan error { return r.errCh }

func (r *runtime) Shutdown() {
	if r.shutdownOnce {
		return
	}

	r.shutdownOnce = true

	if r.executionCancel != nil {
		r.executionCancel()
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	if r.raftGRPCServer != nil {
		if err := r.raftGRPCServer.Shutdown(ctx); err != nil {
			r.logger.Error(
				"failed to shutdown Raft gRPC server",
				"error", err,
			)
		}
	}

	if r.kvGRPCServer != nil {
		if err := r.kvGRPCServer.Shutdown(ctx); err != nil {
			r.logger.Error(
				"failed to shutdown KV gRPC server",
				"error", err,
			)
		}
	}

	if r.node != nil {
		r.node.Stop()
	}

	if r.appServer != nil {
		r.appServer.Stop()
	}

	if r.healthServer != nil {
		if err := r.healthServer.Shutdown(ctx); err != nil {
			r.logger.Error(
				"failed to shutdown health server",
				"error", err,
			)
		}
	}

	if r.metricsServer != nil {
		if err := r.metricsServer.Shutdown(ctx); err != nil {
			r.logger.Error(
				"failed to shutdown metrics server",
				"error", err,
			)
		}
	}

	if r.raftTransport != nil {
		if err := r.raftTransport.Close(); err != nil {
			r.logger.Error(
				"failed to close Raft transport",
				"error", err,
			)
		}
	}

	if r.store != nil {
		if err := r.store.Close(); err != nil {
			r.logger.Error(
				"failed to close WAL",
				"error", err,
			)
		}
	}
}

func (r *runtime) stopServers() {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	if r.raftGRPCServer != nil {
		_ = r.raftGRPCServer.Shutdown(ctx)
	}

	if r.kvGRPCServer != nil {
		_ = r.kvGRPCServer.Shutdown(ctx)
	}

	if r.healthServer != nil {
		_ = r.healthServer.Shutdown(ctx)
	}

	if r.metricsServer != nil {
		_ = r.metricsServer.Shutdown(ctx)
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
