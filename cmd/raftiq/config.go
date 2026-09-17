package main

import (
	"errors"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/sanchar127/raftiq/internal/raft"
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

func defaultConfig() config {
	return config{
		raftAddr:    ":7000",
		kvAddr:      ":8000",
		metricsAddr: ":9090",
		healthAddr:  ":8080",
		dataDir:     "./data",
		peers:       make(peerFlag),
		heartbeat:   100 * time.Millisecond,
		election:    time.Second,
		logLevel:    "info",
	}
}

func parseConfig(cfg *config) error {
	flag.StringVar(
		&cfg.nodeID,
		"id",
		cfg.nodeID,
		"Unique Raft node ID.",
	)

	flag.StringVar(
		&cfg.raftAddr,
		"raft-addr",
		cfg.raftAddr,
		"Address for the Raft RPC server.",
	)

	flag.StringVar(
		&cfg.kvAddr,
		"kv-addr",
		cfg.kvAddr,
		"Address for the client/KV RPC server.",
	)

	flag.StringVar(
		&cfg.metricsAddr,
		"metrics-addr",
		cfg.metricsAddr,
		"Address for the Prometheus metrics HTTP server.",
	)

	flag.StringVar(
		&cfg.healthAddr,
		"health-addr",
		cfg.healthAddr,
		"Address for the health and readiness HTTP server.",
	)

	flag.StringVar(
		&cfg.dataDir,
		"data-dir",
		cfg.dataDir,
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
		cfg.heartbeat,
		"Raft heartbeat interval.",
	)

	flag.DurationVar(
		&cfg.election,
		"election",
		cfg.election,
		"Raft election timeout.",
	)

	flag.StringVar(
		&cfg.logLevel,
		"log-level",
		cfg.logLevel,
		"Log level: debug, info, warn, or error.",
	)

	flag.StringVar(
		&cfg.tlsCA,
		"tls-ca",
		cfg.tlsCA,
		"Path to the TLS CA certificate.",
	)

	flag.StringVar(
		&cfg.tlsCert,
		"tls-cert",
		cfg.tlsCert,
		"Path to the node TLS certificate.",
	)

	flag.StringVar(
		&cfg.tlsKey,
		"tls-key",
		cfg.tlsKey,
		"Path to the node TLS private key.",
	)

	flag.Parse()

	return validateConfig(*cfg)
}

func validateConfig(cfg config) error {
	if err := validateIdentity(cfg); err != nil {
		return err
	}

	if err := validateTiming(cfg); err != nil {
		return err
	}

	if err := validateAddresses(cfg); err != nil {
		return err
	}

	if err := validateDataDir(cfg); err != nil {
		return err
	}

	if err := validateLogLevel(cfg); err != nil {
		return err
	}

	if err := validateTLS(cfg); err != nil {
		return err
	}

	if err := validateCluster(cfg); err != nil {
		return err
	}

	return nil
}

func validateIdentity(cfg config) error {
	if strings.TrimSpace(cfg.nodeID) == "" {
		return errors.New("--id is required")
	}

	return nil
}

func validateTiming(cfg config) error {
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

	return nil
}

func validateAddresses(cfg config) error {
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

	return nil
}

func validateDataDir(cfg config) error {
	if strings.TrimSpace(cfg.dataDir) == "" {
		return errors.New(
			"--data-dir cannot be empty",
		)
	}

	return nil
}

func validateLogLevel(cfg config) error {
	switch strings.ToLower(strings.TrimSpace(cfg.logLevel)) {
	case "debug", "info", "warn", "error":
		return nil

	default:
		return fmt.Errorf(
			"invalid --log-level %q: expected debug, info, warn, or error",
			cfg.logLevel,
		)
	}
}

func validateTLS(cfg config) error {
	if strings.TrimSpace(cfg.tlsCA) == "" {
		return errors.New("--tls-ca is required")
	}

	if strings.TrimSpace(cfg.tlsCert) == "" {
		return errors.New("--tls-cert is required")
	}

	if strings.TrimSpace(cfg.tlsKey) == "" {
		return errors.New("--tls-key is required")
	}

	return nil
}

func validateCluster(cfg config) error {
	if len(cfg.peers) == 0 {
		return errors.New("--peers requires at least one peer")
	}

	if len(cfg.workers) == 0 {
		return errors.New("--workers requires at least one worker")
	}

	return nil
}
