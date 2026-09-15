package transport

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sanchar127/raftiq/internal/raft"
	"github.com/stretchr/testify/require"
)

type testCertificateFiles struct {
	caFile string

	serverCertFile string
	serverKeyFile  string

	clientCertFile string
	clientKeyFile  string
}

type testCertificateAuthority struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	file string
}

func newTestCertificateAuthority(
	t *testing.T,
	dir string,
) testCertificateAuthority {
	t.Helper()

	now := time.Now()

	caKey, err := ecdsa.GenerateKey(
		elliptic.P256(),
		rand.Reader,
	)
	require.NoError(t, err)

	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "RaftIQ Test CA",
		},
		NotBefore: now.Add(-time.Minute),
		NotAfter:  now.Add(time.Hour),

		IsCA:                  true,
		BasicConstraintsValid: true,

		KeyUsage: x509.KeyUsageCertSign |
			x509.KeyUsageDigitalSignature,
	}

	caDER, err := x509.CreateCertificate(
		rand.Reader,
		caTemplate,
		caTemplate,
		&caKey.PublicKey,
		caKey,
	)
	require.NoError(t, err)

	caCert, err := x509.ParseCertificate(caDER)
	require.NoError(t, err)

	caFile := filepath.Join(dir, "ca.crt")

	require.NoError(
		t,
		os.WriteFile(
			caFile,
			pem.EncodeToMemory(&pem.Block{
				Type:  "CERTIFICATE",
				Bytes: caDER,
			}),
			0o600,
		),
	)

	return testCertificateAuthority{
		cert: caCert,
		key:  caKey,
		file: caFile,
	}
}

func writeTestNodeCertificate(
	t *testing.T,
	ca testCertificateAuthority,
	dir string,
	nodeID raft.NodeID,
) testCertificateFiles {
	t.Helper()

	now := time.Now()

	// Generate a unique serial number for this node certificate.
	serialBytes := make([]byte, 16)
	_, err := rand.Read(serialBytes)
	require.NoError(t, err)

	serial := new(big.Int).SetBytes(serialBytes)

	nodeKey, err := ecdsa.GenerateKey(
		elliptic.P256(),
		rand.Reader,
	)
	require.NoError(t, err)

	san := peerServerName(nodeID)

	nodeTemplate := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName: string(nodeID),
		},
		DNSNames: []string{
			san,
		},
		NotBefore: now.Add(-time.Minute),
		NotAfter:  now.Add(time.Hour),

		ExtKeyUsage: []x509.ExtKeyUsage{
			x509.ExtKeyUsageServerAuth,
			x509.ExtKeyUsageClientAuth,
		},

		KeyUsage: x509.KeyUsageDigitalSignature,
	}

	nodeDER, err := x509.CreateCertificate(
		rand.Reader,
		nodeTemplate,
		ca.cert,
		&nodeKey.PublicKey,
		ca.key,
	)
	require.NoError(t, err)

	nodeCertFile := filepath.Join(
		dir,
		fmt.Sprintf("%s.crt", nodeID),
	)

	nodeKeyFile := filepath.Join(
		dir,
		fmt.Sprintf("%s.key", nodeID),
	)

	require.NoError(
		t,
		os.WriteFile(
			nodeCertFile,
			pem.EncodeToMemory(
				&pem.Block{
					Type:  "CERTIFICATE",
					Bytes: nodeDER,
				},
			),
			0o600,
		),
	)

	nodeKeyDER, err := x509.MarshalECPrivateKey(nodeKey)
	require.NoError(t, err)

	require.NoError(
		t,
		os.WriteFile(
			nodeKeyFile,
			pem.EncodeToMemory(
				&pem.Block{
					Type:  "EC PRIVATE KEY",
					Bytes: nodeKeyDER,
				},
			),
			0o600,
		),
	)

	return testCertificateFiles{
		caFile: ca.file,

		serverCertFile: nodeCertFile,
		serverKeyFile:  nodeKeyFile,

		clientCertFile: nodeCertFile,
		clientKeyFile:  nodeKeyFile,
	}
}

func mustPEM(
	blockType string,
	data []byte,
) []byte {
	return pem.EncodeToMemory(&pem.Block{
		Type:  blockType,
		Bytes: data,
	})
}

func writeTestCertificateFiles(
	t *testing.T,
	dir string,
	serverSAN string,
	clientSAN string,
) testCertificateFiles {
	t.Helper()

	now := time.Now()

	// -------------------------------------------------------------------------
	// Test CA
	// -------------------------------------------------------------------------

	caKey, err := ecdsa.GenerateKey(
		elliptic.P256(),
		rand.Reader,
	)
	require.NoError(t, err)

	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "RaftIQ Test CA",
		},
		NotBefore: now.Add(-time.Minute),
		NotAfter:  now.Add(time.Hour),

		IsCA:                  true,
		BasicConstraintsValid: true,

		KeyUsage: x509.KeyUsageCertSign |
			x509.KeyUsageDigitalSignature,
	}

	caDER, err := x509.CreateCertificate(
		rand.Reader,
		caTemplate,
		caTemplate,
		&caKey.PublicKey,
		caKey,
	)
	require.NoError(t, err)

	caCert, err := x509.ParseCertificate(caDER)
	require.NoError(t, err)

	// -------------------------------------------------------------------------
	// Server certificate
	// -------------------------------------------------------------------------

	serverKey, err := ecdsa.GenerateKey(
		elliptic.P256(),
		rand.Reader,
	)
	require.NoError(t, err)

	serverTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			CommonName: "RaftIQ Test Server",
		},

		DNSNames: []string{
			serverSAN,
		},

		NotBefore: now.Add(-time.Minute),
		NotAfter:  now.Add(time.Hour),

		ExtKeyUsage: []x509.ExtKeyUsage{
			x509.ExtKeyUsageServerAuth,
			x509.ExtKeyUsageClientAuth,
		},

		KeyUsage: x509.KeyUsageDigitalSignature,
	}

	serverDER, err := x509.CreateCertificate(
		rand.Reader,
		serverTemplate,
		caCert,
		&serverKey.PublicKey,
		caKey,
	)
	require.NoError(t, err)

	// -------------------------------------------------------------------------
	// Client certificate
	// -------------------------------------------------------------------------

	clientKey, err := ecdsa.GenerateKey(
		elliptic.P256(),
		rand.Reader,
	)
	require.NoError(t, err)

	clientTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject: pkix.Name{
			CommonName: "RaftIQ Test Client",
		},

		DNSNames: []string{
			clientSAN,
		},

		NotBefore: now.Add(-time.Minute),
		NotAfter:  now.Add(time.Hour),

		ExtKeyUsage: []x509.ExtKeyUsage{
			x509.ExtKeyUsageServerAuth,
			x509.ExtKeyUsageClientAuth,
		},

		KeyUsage: x509.KeyUsageDigitalSignature,
	}

	clientDER, err := x509.CreateCertificate(
		rand.Reader,
		clientTemplate,
		caCert,
		&clientKey.PublicKey,
		caKey,
	)
	require.NoError(t, err)

	// -------------------------------------------------------------------------
	// File paths
	// -------------------------------------------------------------------------

	files := testCertificateFiles{
		caFile: filepath.Join(dir, "ca.crt"),

		serverCertFile: filepath.Join(dir, "server.crt"),
		serverKeyFile:  filepath.Join(dir, "server.key"),

		clientCertFile: filepath.Join(dir, "client.crt"),
		clientKeyFile:  filepath.Join(dir, "client.key"),
	}

	// -------------------------------------------------------------------------
	// Write CA
	// -------------------------------------------------------------------------

	require.NoError(
		t,
		os.WriteFile(
			files.caFile,
			pem.EncodeToMemory(&pem.Block{
				Type:  "CERTIFICATE",
				Bytes: caDER,
			}),
			0o600,
		),
	)

	// -------------------------------------------------------------------------
	// Write server certificate
	// -------------------------------------------------------------------------

	require.NoError(
		t,
		os.WriteFile(
			files.serverCertFile,
			pem.EncodeToMemory(&pem.Block{
				Type:  "CERTIFICATE",
				Bytes: serverDER,
			}),
			0o600,
		),
	)

	serverKeyDER, err := x509.MarshalECPrivateKey(serverKey)
	require.NoError(t, err)

	require.NoError(
		t,
		os.WriteFile(
			files.serverKeyFile,
			pem.EncodeToMemory(&pem.Block{
				Type:  "EC PRIVATE KEY",
				Bytes: serverKeyDER,
			}),
			0o600,
		),
	)

	// -------------------------------------------------------------------------
	// Write client certificate
	// -------------------------------------------------------------------------

	require.NoError(
		t,
		os.WriteFile(
			files.clientCertFile,
			pem.EncodeToMemory(&pem.Block{
				Type:  "CERTIFICATE",
				Bytes: clientDER,
			}),
			0o600,
		),
	)

	clientKeyDER, err := x509.MarshalECPrivateKey(clientKey)
	require.NoError(t, err)

	require.NoError(
		t,
		os.WriteFile(
			files.clientKeyFile,
			pem.EncodeToMemory(&pem.Block{
				Type:  "EC PRIVATE KEY",
				Bytes: clientKeyDER,
			}),
			0o600,
		),
	)

	return files
}

func TestLoadTLSConfig(t *testing.T) {
	dir := t.TempDir()

	files := writeTestCertificateFiles(
		t,
		dir,
		"node-1.raftiq",
		"node-1.raftiq",
	)

	cfg, err := LoadTLSConfig(TLSConfig{
		CAFile:         files.caFile,
		CertFile:       files.clientCertFile,
		KeyFile:        files.clientKeyFile,
		PeerServerName: "node-1.raftiq",
	})
	require.NoError(t, err)
	require.NotNil(t, cfg)

	require.EqualValues(
		t,
		tls.VersionTLS13,
		cfg.MinVersion,
	)

	require.Equal(
		t,
		"node-1.raftiq",
		cfg.ServerName,
	)

	require.Equal(
		t,
		tls.RequireAndVerifyClientCert,
		cfg.ClientAuth,
	)

	require.NotNil(t, cfg.RootCAs)
	require.NotNil(t, cfg.ClientCAs)
	require.Len(t, cfg.Certificates, 1)
}

func TestLoadTLSConfigRequiresAllFields(t *testing.T) {
	tests := []struct {
		name string
		cfg  TLSConfig
		want error
	}{
		{
			name: "missing CA",
			cfg: TLSConfig{
				CertFile:       "cert",
				KeyFile:        "key",
				PeerServerName: "node-1.raftiq",
			},
			want: ErrTLSCACertificate,
		},
		{
			name: "missing certificate",
			cfg: TLSConfig{
				CAFile:         "ca",
				KeyFile:        "key",
				PeerServerName: "node-1.raftiq",
			},
			want: ErrTLSCertificate,
		},
		{
			name: "missing private key",
			cfg: TLSConfig{
				CAFile:         "ca",
				CertFile:       "cert",
				PeerServerName: "node-1.raftiq",
			},
			want: ErrTLSPrivateKey,
		},
		{
			name: "missing peer server name",
			cfg: TLSConfig{
				CAFile:   "ca",
				CertFile: "cert",
				KeyFile:  "key",
			},
			want: ErrTLSInvalidPeerName,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := LoadTLSConfig(tt.cfg)
			require.ErrorIs(t, err, tt.want)
		})
	}
}

func TestTLSMutualHandshake(t *testing.T) {
	dir := t.TempDir()

	files := writeTestCertificateFiles(
		t,
		dir,
		"node-1.raftiq",
		"node-1.raftiq",
	)

	serverTLS, err := LoadTLSServerConfig(
		TLSConfig{
			CAFile:   files.caFile,
			CertFile: files.serverCertFile,
			KeyFile:  files.serverKeyFile,
		},
		map[string]struct{}{
			"node-1.raftiq": {},
		},
	)
	require.NoError(t, err)

	clientTLS, err := LoadTLSConfig(TLSConfig{
		CAFile:         files.caFile,
		CertFile:       files.clientCertFile,
		KeyFile:        files.clientKeyFile,
		PeerServerName: "node-1.raftiq",
	})
	require.NoError(t, err)

	listener, err := tls.Listen(
		"tcp",
		"127.0.0.1:0",
		serverTLS,
	)
	require.NoError(t, err)
	defer listener.Close()

	serverErr := make(chan error, 1)

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer conn.Close()

		serverErr <- conn.(*tls.Conn).Handshake()
	}()

	clientConn, err := tls.Dial(
		"tcp",
		listener.Addr().String(),
		&tls.Config{
			Certificates: clientTLS.Certificates,
			RootCAs:      clientTLS.RootCAs,
			ServerName:   "node-1.raftiq",
			MinVersion:   tls.VersionTLS13,
		},
	)
	require.NoError(t, err)
	defer clientConn.Close()

	require.NoError(t, clientConn.Handshake())
	require.NoError(t, <-serverErr)
}

func TestTLSRejectsWrongServerSAN(t *testing.T) {
	dir := t.TempDir()

	files := writeTestCertificateFiles(
		t,
		dir,
		"node-1.raftiq",
		"node-1.raftiq",
	)

	clientTLS, err := LoadTLSConfig(TLSConfig{
		CAFile:         files.caFile,
		CertFile:       files.clientCertFile,
		KeyFile:        files.clientKeyFile,
		PeerServerName: "wrong-node.raftiq",
	})
	require.NoError(t, err)

	serverTLS, err := LoadTLSServerConfig(
		TLSConfig{
			CAFile:   files.caFile,
			CertFile: files.serverCertFile,
			KeyFile:  files.serverKeyFile,
		},
		map[string]struct{}{
			"node-1.raftiq": {},
		},
	)
	require.NoError(t, err)

	listener, err := tls.Listen(
		"tcp",
		"127.0.0.1:0",
		serverTLS,
	)
	require.NoError(t, err)
	defer listener.Close()

	serverErr := make(chan error, 1)

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer conn.Close()

		serverErr <- conn.(*tls.Conn).Handshake()
	}()

	_, err = tls.Dial(
		"tcp",
		listener.Addr().String(),
		&tls.Config{
			Certificates: clientTLS.Certificates,
			RootCAs:      clientTLS.RootCAs,
			ServerName:   clientTLS.ServerName,
			MinVersion:   tls.VersionTLS13,
		},
	)

	require.Error(t, err)
	require.ErrorContains(t, err, "certificate")

	require.Error(t, <-serverErr)
}

func TestTLSRejectsWrongClientSAN(t *testing.T) {
	dir := t.TempDir()

	files := writeTestCertificateFiles(
		t,
		dir,
		"node-1.raftiq",
		"wrong-client.raftiq",
	)

	serverTLS, err := LoadTLSServerConfig(
		TLSConfig{
			CAFile:   files.caFile,
			CertFile: files.serverCertFile,
			KeyFile:  files.serverKeyFile,
		},
		map[string]struct{}{
			"node-1.raftiq": {},
		},
	)
	require.NoError(t, err)

	listener, err := tls.Listen(
		"tcp",
		"127.0.0.1:0",
		serverTLS,
	)
	require.NoError(t, err)
	defer listener.Close()

	serverErr := make(chan error, 1)

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer conn.Close()

		serverErr <- conn.(*tls.Conn).Handshake()
	}()

	clientTLS, err := LoadTLSConfig(TLSConfig{
		CAFile:         files.caFile,
		CertFile:       files.clientCertFile,
		KeyFile:        files.clientKeyFile,
		PeerServerName: "node-1.raftiq",
	})
	require.NoError(t, err)

	clientConn, err := tls.Dial(
		"tcp",
		listener.Addr().String(),
		&tls.Config{
			Certificates: clientTLS.Certificates,
			RootCAs:      clientTLS.RootCAs,
			ServerName:   "node-1.raftiq",
			MinVersion:   tls.VersionTLS13,
		},
	)

	if err == nil {
		defer clientConn.Close()
	}

	require.NoError(t, err)

	err = <-serverErr

	require.Error(t, err)
	require.ErrorIs(t, err, ErrTLSInvalidPeerSAN)
}

func TestTLSAcceptsCorrectClientSAN(t *testing.T) {
	dir := t.TempDir()

	files := writeTestCertificateFiles(
		t,
		dir,
		"node-1.raftiq",
		"node-1.raftiq",
	)

	serverTLS, err := LoadTLSServerConfig(
		TLSConfig{
			CAFile:   files.caFile,
			CertFile: files.serverCertFile,
			KeyFile:  files.serverKeyFile,
		},
		map[string]struct{}{
			"node-1.raftiq": {},
		},
	)
	require.NoError(t, err)

	listener, err := tls.Listen(
		"tcp",
		"127.0.0.1:0",
		serverTLS,
	)
	require.NoError(t, err)
	defer listener.Close()

	serverErr := make(chan error, 1)

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer conn.Close()

		serverErr <- conn.(*tls.Conn).Handshake()
	}()

	clientTLS, err := LoadTLSConfig(TLSConfig{
		CAFile:         files.caFile,
		CertFile:       files.clientCertFile,
		KeyFile:        files.clientKeyFile,
		PeerServerName: "node-1.raftiq",
	})
	require.NoError(t, err)

	clientConn, err := tls.Dial(
		"tcp",
		listener.Addr().String(),
		&tls.Config{
			Certificates: clientTLS.Certificates,
			RootCAs:      clientTLS.RootCAs,
			ServerName:   "node-1.raftiq",
			MinVersion:   tls.VersionTLS13,
		},
	)
	require.NoError(t, err)
	defer clientConn.Close()

	require.NoError(t, clientConn.Handshake())
	require.NoError(t, <-serverErr)
}
