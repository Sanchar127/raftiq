package transport

import (
	"crypto/tls"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	raftiqv1 "github.com/sanchar127/raftiq/api/proto"
	"github.com/sanchar127/raftiq/internal/raft"
	"github.com/sanchar127/raftiq/internal/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type testRaftServer struct {
	server   *grpc.Server
	listener net.Listener
}

func startTestRaftServer(
	t *testing.T,
	node *raft.RaftNode,
	tlsConfigs ...*tls.Config,
) *testRaftServer {
	t.Helper()

	var tlsConfig *tls.Config

	if len(tlsConfigs) > 1 {
		t.Fatal("startTestRaftServer accepts at most one TLS config")
	}

	if len(tlsConfigs) == 1 {
		tlsConfig = tlsConfigs[0]
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	service, err := NewRaftService(node)
	if err != nil {
		_ = listener.Close()
		t.Fatalf("create raft service: %v", err)
	}

	var server *grpc.Server

	if tlsConfig != nil {
		server = grpc.NewServer(
			grpc.Creds(credentials.NewTLS(tlsConfig)),
		)
	} else {
		server = grpc.NewServer()
	}

	raftiqv1.RegisterRaftServiceServer(server, service)

	go func() {
		if err := server.Serve(listener); err != nil {
			t.Errorf("gRPC server failed: %v", err)
		}
	}()

	return &testRaftServer{
		server:   server,
		listener: listener,
	}
}

func (s *testRaftServer) close() {
	s.server.GracefulStop()
	_ = s.listener.Close()
}

func registerRaftService(
	server *grpc.Server,
	node *raft.RaftNode,
) {
	service, err := NewRaftService(node)
	if err != nil {
		panic(fmt.Sprintf(
			"create raft service: %v",
			err,
		))
	}

	raftiqv1.RegisterRaftServiceServer(
		server,
		service,
	)
}

func peerIDsExcept(
	ids []raft.NodeID,
	excluded raft.NodeID,
) []raft.NodeID {
	peers := make(
		[]raft.NodeID,
		0,
		len(ids)-1,
	)

	for _, id := range ids {
		if id == excluded {
			continue
		}

		peers = append(peers, id)
	}

	return peers
}

func indexOfNodeID(
	ids []raft.NodeID,
	target raft.NodeID,
) int {
	for i, id := range ids {
		if id == target {
			return i
		}
	}

	return -1
}

func setDeterministicElectionTimeout(
	node *raft.RaftNode,
	index int,
) {
	node.SetElectionTimeout(10 + index*5)
}

func waitForLeader(
	t *testing.T,
	nodes []*raft.RaftNode,
) int {
	t.Helper()

	leaderIndex := -1

	waitForCondition(
		t,
		10*time.Second,
		func() bool {
			leaderIndex = -1

			for i, node := range nodes {
				if node.State().Role != raft.Leader {
					continue
				}

				if leaderIndex != -1 {
					return false
				}

				leaderIndex = i
			}

			return leaderIndex >= 0
		},
	)

	return leaderIndex
}

func waitForCondition(
	t *testing.T,
	timeout time.Duration,
	condition func() bool,
) {
	t.Helper()

	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if condition() {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf(
		"condition not satisfied within %s",
		timeout,
	)
}

type testClusterTLS struct {
	serverConfigs map[raft.NodeID]*tls.Config
	clientConfigs map[raft.NodeID]*tls.Config
}

func newTestClusterTLS(
	t *testing.T,
	ids []raft.NodeID,
) *testClusterTLS {
	t.Helper()

	if len(ids) == 0 {
		t.Fatal("TLS cluster requires at least one node")
	}

	dir := t.TempDir()

	ca := newTestCertificateAuthority(t, dir)

	certs := make(
		map[raft.NodeID]testCertificateFiles,
		len(ids),
	)

	for _, id := range ids {
		certs[id] = writeTestNodeCertificate(
			t,
			ca,
			dir,
			id,
		)
	}

	result := &testClusterTLS{
		serverConfigs: make(
			map[raft.NodeID]*tls.Config,
			len(ids),
		),
		clientConfigs: make(
			map[raft.NodeID]*tls.Config,
			len(ids),
		),
	}

	for _, id := range ids {
		allowedPeerSANs := make(
			map[string]struct{},
			len(ids)-1,
		)

		for _, peerID := range ids {
			if peerID == id {
				continue
			}

			allowedPeerSANs[peerServerName(peerID)] = struct{}{}
		}

		serverFiles := certs[id]

		serverTLS, err := LoadTLSServerConfig(
			TLSConfig{
				CAFile:   serverFiles.caFile,
				CertFile: serverFiles.serverCertFile,
				KeyFile:  serverFiles.serverKeyFile,
			},
			allowedPeerSANs,
		)
		if err != nil {
			t.Fatalf(
				"load server TLS config for %s: %v",
				id,
				err,
			)
		}

		clientTLS, err := LoadTLSClientConfig(
			TLSConfig{
				CAFile:   serverFiles.caFile,
				CertFile: serverFiles.clientCertFile,
				KeyFile:  serverFiles.clientKeyFile,
			},
		)
		if err != nil {
			t.Fatalf(
				"load client TLS config for %s: %v",
				id,
				err,
			)
		}

		result.serverConfigs[id] = serverTLS
		result.clientConfigs[id] = clientTLS
	}

	return result
}
func newPersistentTestNode(
	t *testing.T,
	id raft.NodeID,
	dir string,
) (*raft.RaftNode, *storage.WALStorage) {
	t.Helper()

	path := filepath.Join(
		dir,
		fmt.Sprintf("%s.wal", id),
	)

	store, err := storage.OpenWAL(path)
	if err != nil {
		t.Fatalf(
			"open WAL for node %s: %v",
			id,
			err,
		)
	}

	node, err := raft.NewRaftNodeWithStorage(
		id,
		store,
	)
	if err != nil {
		_ = store.Close()

		t.Fatalf(
			"create persistent node %s: %v",
			id,
			err,
		)
	}

	return node, store
}
