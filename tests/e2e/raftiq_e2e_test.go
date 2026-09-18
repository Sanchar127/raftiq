package e2e_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	raftiqv1 "github.com/sanchar127/raftiq/api/proto"
	"github.com/sanchar127/raftiq/internal/transport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const (
	e2eStartupTimeout  = 15 * time.Second
	e2eShutdownTimeout = 5 * time.Second
	e2eElectionTimeout = 10 * time.Second
)

type e2eNode struct {
	id string

	raftAddr    string
	kvAddr      string
	metricsAddr string
	healthAddr  string

	dataDir string

	tlsCert string
	tlsKey  string

	cmd *exec.Cmd
}

type e2eCluster struct {
	t      *testing.T
	binary string
	nodes  []*e2eNode
	tls    *e2eTLS
}

func TestE2EProcessLifecycle(t *testing.T) {
	t.Parallel()

	cluster := newE2ECluster(t)
	cluster.start()

	t.Cleanup(cluster.stop)

	for _, node := range cluster.nodes {
		if err := waitForHTTP(
			"http://"+node.healthAddr+"/healthz",
			e2eStartupTimeout,
		); err != nil {
			dumpE2ELogs(t, cluster)
			t.Fatalf("node %s healthz: %v", node.id, err)
		}

		if err := waitForHTTP(
			"http://"+node.healthAddr+"/readyz",
			e2eStartupTimeout,
		); err != nil {
			dumpE2ELogs(t, cluster)
			t.Fatalf("node %s readyz: %v", node.id, err)
		}
	}
}

func TestE2EKVLeaderFailover(t *testing.T) {
	cluster := newE2ECluster(t)
	cluster.start()

	t.Cleanup(cluster.stop)

	for _, node := range cluster.nodes {
		if err := waitForHTTP(
			"http://"+node.healthAddr+"/readyz",
			e2eStartupTimeout,
		); err != nil {
			dumpE2ELogs(t, cluster)
			t.Fatalf("node %s readyz: %v", node.id, err)
		}
	}

	key := "e2e-failover-key"
	value := []byte("e2e-failover-value")

	clients := make(map[string]*grpc.ClientConn, len(cluster.nodes))

	defer func() {
		for _, conn := range clients {
			_ = conn.Close()
		}
	}()

	// Discover the initial leader by successfully committing a KV Put.
	var leader *e2eNode

	leaderDeadline := time.Now().Add(e2eElectionTimeout)

	for time.Now().Before(leaderDeadline) && leader == nil {
		for _, node := range cluster.nodes {
			conn := clients[node.id]

			if conn == nil {
				var err error

				conn, err = dialKVNode(node, cluster.tls)
				if err != nil {
					t.Logf(
						"node %s KV dial failed: %v",
						node.id,
						err,
					)
					continue
				}

				clients[node.id] = conn
			}

			client := raftiqv1.NewKVServiceClient(conn)

			ctx, cancel := context.WithTimeout(
				context.Background(),
				750*time.Millisecond,
			)

			_, putErr := client.Put(
				ctx,
				&raftiqv1.PutRequest{
					Key:   key,
					Value: value,
				},
			)

			cancel()

			if putErr == nil {
				leader = node
				break
			}

			t.Logf(
				"node %s is not leader yet: %v",
				node.id,
				putErr,
			)
		}

		if leader == nil {
			time.Sleep(200 * time.Millisecond)
		}
	}

	if leader == nil {
		dumpE2ELogs(t, cluster)
		t.Fatal("failed to find Raft leader through KV Put")
	}

	t.Logf("initial leader: %s", leader.id)

	// Kill the actual OS process for the current leader.
	cluster.stopNode(leader)

	// Discover the new leader by committing another KV Put.
	var newLeader *e2eNode

	failoverDeadline := time.Now().Add(e2eElectionTimeout)

	for time.Now().Before(failoverDeadline) && newLeader == nil {
		for _, node := range cluster.nodes {
			if node.id == leader.id {
				continue
			}

			conn := clients[node.id]

			if conn == nil {
				var err error

				conn, err = dialKVNode(node, cluster.tls)
				if err != nil {
					t.Logf(
						"node %s KV dial failed: %v",
						node.id,
						err,
					)
					continue
				}

				clients[node.id] = conn
			}

			client := raftiqv1.NewKVServiceClient(conn)

			ctx, cancel := context.WithTimeout(
				context.Background(),
				750*time.Millisecond,
			)

			_, putErr := client.Put(
				ctx,
				&raftiqv1.PutRequest{
					Key:   "e2e-leader-probe",
					Value: []byte("probe"),
				},
			)

			cancel()

			if putErr == nil {
				newLeader = node
				break
			}

			t.Logf(
				"node %s is not new leader yet: %v",
				node.id,
				putErr,
			)
		}

		if newLeader == nil {
			time.Sleep(200 * time.Millisecond)
		}
	}

	if newLeader == nil {
		dumpE2ELogs(t, cluster)
		t.Fatal("no surviving node became leader after leader process failure")
	}

	t.Logf("new leader after failover: %s", newLeader.id)

	// Read the value written before the leader failure from the new leader.
	conn := clients[newLeader.id]
	client := raftiqv1.NewKVServiceClient(conn)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		3*time.Second,
	)
	defer cancel()

	response, err := client.Get(
		ctx,
		&raftiqv1.GetRequest{
			Key: key,
		},
	)
	if err != nil {
		dumpE2ELogs(t, cluster)
		t.Fatalf("get replicated value from new leader: %v", err)
	}

	if !response.GetFound() {
		t.Fatal("replicated key was not found after leader failover")
	}

	if string(response.GetValue()) != string(value) {
		t.Fatalf(
			"replicated value mismatch: got %q, want %q",
			response.GetValue(),
			value,
		)
	}
}

func newE2ECluster(t *testing.T) *e2eCluster {
	t.Helper()

	root := t.TempDir()
	binary := filepath.Join(root, "raftiq")

	cmd := exec.Command(
		"go",
		"build",
		"-o",
		binary,
		"./cmd/raftiq",
	)

	cmd.Dir = repoRoot(t)

	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf(
			"build raftiq: %v\n%s",
			err,
			output,
		)
	}

	nodes := make([]*e2eNode, 3)
	nodeIDs := make([]string, len(nodes))

	for i := range nodes {
		id := fmt.Sprintf("node-%d", i+1)
		nodeIDs[i] = id

		nodes[i] = &e2eNode{
			id:          id,
			raftAddr:    freeTCPAddress(t),
			kvAddr:      freeTCPAddress(t),
			metricsAddr: freeTCPAddress(t),
			healthAddr:  freeTCPAddress(t),
			dataDir:     filepath.Join(root, id),
		}
	}

	tls := newE2ETLS(t, root, nodeIDs)

	for _, node := range nodes {
		certs := tls.certFiles[node.id]

		node.tlsCert = certs.certFile
		node.tlsKey = certs.keyFile
	}

	return &e2eCluster{
		t:      t,
		binary: binary,
		nodes:  nodes,
		tls:    tls,
	}
}

func (c *e2eCluster) start() {
	c.t.Helper()

	for _, node := range c.nodes {
		peers := peerFlag(c.nodes, node.id)

		args := []string{
			"--id", node.id,
			"--raft-addr", node.raftAddr,
			"--kv-addr", node.kvAddr,
			"--metrics-addr", node.metricsAddr,
			"--health-addr", node.healthAddr,
			"--data-dir", node.dataDir,
			"--peers", peers,
			"--workers", "worker-1",
			"--tls-ca", c.tls.caFile,
			"--tls-cert", node.tlsCert,
			"--tls-key", node.tlsKey,
		}

		cmd := exec.Command(c.binary, args...)

		if err := os.MkdirAll(node.dataDir, 0o755); err != nil {
			c.t.Fatalf(
				"create data directory for %s: %v",
				node.id,
				err,
			)
		}

		logFile, err := os.OpenFile(
			filepath.Join(node.dataDir, "process.log"),
			os.O_CREATE|os.O_WRONLY|os.O_TRUNC,
			0o600,
		)
		if err != nil {
			c.t.Fatalf(
				"open process log for %s: %v",
				node.id,
				err,
			)
		}

		cmd.Stdout = logFile
		cmd.Stderr = logFile

		if err := cmd.Start(); err != nil {
			_ = logFile.Close()

			c.t.Fatalf(
				"start %s: %v",
				node.id,
				err,
			)
		}

		node.cmd = cmd

		c.t.Cleanup(func() {
			_ = logFile.Close()
		})
	}
}

func (c *e2eCluster) stopNode(node *e2eNode) {
	c.t.Helper()

	if node == nil ||
		node.cmd == nil ||
		node.cmd.Process == nil {
		return
	}

	if node.cmd.ProcessState != nil &&
		node.cmd.ProcessState.Exited() {
		return
	}

	_ = node.cmd.Process.Signal(os.Interrupt)

	deadline := time.Now().Add(e2eShutdownTimeout)

	for time.Now().Before(deadline) {
		if node.cmd.ProcessState != nil &&
			node.cmd.ProcessState.Exited() {
			break
		}

		time.Sleep(25 * time.Millisecond)
	}

	if node.cmd.ProcessState == nil ||
		!node.cmd.ProcessState.Exited() {
		_ = node.cmd.Process.Kill()
	}

	_ = node.cmd.Wait()
}

func (c *e2eCluster) stop() {
	c.t.Helper()

	for _, node := range c.nodes {
		c.stopNode(node)
	}
}

func dialKVNode(
	node *e2eNode,
	tlsConfig *e2eTLS,
) (*grpc.ClientConn, error) {
	clientTLS, err := transport.LoadTLSConfig(
		transport.TLSConfig{
			CAFile:         tlsConfig.caFile,
			CertFile:       node.tlsCert,
			KeyFile:        node.tlsKey,
			PeerServerName: node.id + ".raftiq",
		},
	)
	if err != nil {
		return nil, fmt.Errorf(
			"load client TLS for %s: %w",
			node.id,
			err,
		)
	}

	return grpc.NewClient(
		node.kvAddr,
		grpc.WithTransportCredentials(
			credentials.NewTLS(clientTLS),
		),
	)
}

func repoRoot(t *testing.T) string {
	t.Helper()

	root, err := os.Getwd()
	if err != nil {
		t.Fatalf("get working directory: %v", err)
	}

	return filepath.Clean(
		filepath.Join(root, "..", ".."),
	)
}

func freeTCPAddress(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("allocate TCP port: %v", err)
	}

	addr := listener.Addr().String()

	if err := listener.Close(); err != nil {
		t.Fatalf("release TCP port %q: %v", addr, err)
	}

	return addr
}

func peerFlag(nodes []*e2eNode, selfID string) string {
	peers := make([]string, 0, len(nodes)-1)

	for _, node := range nodes {
		if node.id == selfID {
			continue
		}

		peers = append(
			peers,
			fmt.Sprintf("%s=%s", node.id, node.raftAddr),
		)
	}

	return joinPeers(peers)
}

func joinPeers(peers []string) string {
	result := ""

	for i, peer := range peers {
		if i > 0 {
			result += ","
		}

		result += peer
	}

	return result
}

func waitForHTTP(url string, timeout time.Duration) error {
	client := &http.Client{}

	deadline := time.Now().Add(timeout)
	var lastErr error

	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			500*time.Millisecond,
		)

		req, err := http.NewRequestWithContext(
			ctx,
			http.MethodGet,
			url,
			nil,
		)
		if err != nil {
			lastErr = err
			cancel()
			time.Sleep(100 * time.Millisecond)
			continue
		}

		resp, err := client.Do(req)
		if err == nil {
			status := resp.StatusCode

			_ = resp.Body.Close()
			cancel()

			if status == http.StatusOK {
				return nil
			}

			lastErr = fmt.Errorf("HTTP status %d", status)
		} else {
			lastErr = err
			cancel()
		}

		time.Sleep(100 * time.Millisecond)
	}

	return fmt.Errorf(
		"timed out after %s; last error: %v",
		timeout,
		lastErr,
	)
}

func dumpE2ELogs(t *testing.T, cluster *e2eCluster) {
	t.Helper()

	for _, node := range cluster.nodes {
		logPath := filepath.Join(
			node.dataDir,
			"process.log",
		)

		data, err := os.ReadFile(logPath)
		if err != nil {
			t.Logf(
				"--- %s process.log unavailable: %v ---",
				node.id,
				err,
			)
			continue
		}

		t.Logf(
			"--- %s process.log ---\n%s",
			node.id,
			string(data),
		)
	}
}
