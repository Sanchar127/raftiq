package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

func main() {
	cfg := defaultConfig()

	if err := parseConfig(&cfg); err != nil {
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
		"health_addr", cfg.healthAddr,
		"data_dir", cfg.dataDir,
		"workers", []string(cfg.workers),
	)

	runtime, err := newRuntime(cfg, logger)
	if err != nil {
		logger.Error(
			"failed to initialize raftiq node",
			"error", err,
		)
		os.Exit(1)
	}

	signalCtx, stopSignals := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer stopSignals()

	if err := runtime.Start(signalCtx); err != nil {
		logger.Error(
			"raftiq node failed",
			"error", err,
		)

		runtime.Shutdown()

		os.Exit(1)
	}

	select {
	case <-signalCtx.Done():
		logger.Info("shutdown signal received")

	case err := <-runtime.Errors():
		if err != nil {
			logger.Error(
				"raftiq runtime failed",
				"error", err,
			)
		}
	}

	runtime.Shutdown()

	logger.Info("raftiq node stopped")
}

func newLogger(level string) *slog.Logger {
	var slogLevel slog.Level

	switch normalizeLogLevel(level) {
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

func normalizeLogLevel(level string) string {
	level = strings.ToLower(strings.TrimSpace(level))

	switch level {
	case "debug", "info", "warn", "error":
		return level

	default:
		return "info"
	}
}

func isShutdownError(err error) bool {
	return errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded)
}
