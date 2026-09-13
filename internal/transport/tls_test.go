package transport

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func writeTestCertificateFiles(
	t *testing.T,
	dir string,
) (caFile, certFile, keyFile string) {
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

	nodeKey, err := ecdsa.GenerateKey(
		elliptic.P256(),
		rand.Reader,
	)
	require.NoError(t, err)

	nodeTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			CommonName: "node-1",
		},

		DNSNames: []string{
			"node-1.raftiq",
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
		caCert,
		&nodeKey.PublicKey,
		caKey,
	)
	require.NoError(t, err)

	caFile = filepath.Join(dir, "ca.crt")
	certFile = filepath.Join(dir, "node.crt")
	keyFile = filepath.Join(dir, "node.key")

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

	require.NoError(
		t,
		os.WriteFile(
			certFile,
			pem.EncodeToMemory(&pem.Block{
				Type:  "CERTIFICATE",
				Bytes: nodeDER,
			}),
			0o600,
		),
	)

	nodeKeyDER, err := x509.MarshalECPrivateKey(nodeKey)
	require.NoError(t, err)

	require.NoError(
		t,
		os.WriteFile(
			keyFile,
			pem.EncodeToMemory(&pem.Block{
				Type:  "EC PRIVATE KEY",
				Bytes: nodeKeyDER,
			}),
			0o600,
		),
	)

	return caFile, certFile, keyFile
}

func TestLoadTLSConfig(t *testing.T) {
	dir := t.TempDir()

	caFile, certFile, keyFile := writeTestCertificateFiles(
		t,
		dir,
	)

	cfg, err := LoadTLSConfig(TLSConfig{
		CAFile:         caFile,
		CertFile:       certFile,
		KeyFile:        keyFile,
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

	caFile, certFile, keyFile := writeTestCertificateFiles(t, dir)

	serverTLS, err := LoadTLSConfig(TLSConfig{
		CAFile:         caFile,
		CertFile:       certFile,
		KeyFile:        keyFile,
		PeerServerName: "node-1.raftiq",
	})
	require.NoError(t, err)

	clientTLS, err := LoadTLSConfig(TLSConfig{
		CAFile:         caFile,
		CertFile:       certFile,
		KeyFile:        keyFile,
		PeerServerName: "node-1.raftiq",
	})
	require.NoError(t, err)

	serverTLS = serverTLS.Clone()
	serverTLS.ServerName = ""

	listener, err := tls.Listen(
		"tcp",
		"127.0.0.1:0",
		&tls.Config{
			Certificates: serverTLS.Certificates,
			ClientCAs:    serverTLS.ClientCAs,
			ClientAuth:   tls.RequireAndVerifyClientCert,
			MinVersion:   tls.VersionTLS13,
		},
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

		tlsConn, ok := conn.(*tls.Conn)
		require.True(t, ok)

		serverErr <- tlsConn.Handshake()
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

	caFile, certFile, keyFile := writeTestCertificateFiles(t, dir)

	clientTLS, err := LoadTLSConfig(TLSConfig{
		CAFile:         caFile,
		CertFile:       certFile,
		KeyFile:        keyFile,
		PeerServerName: "wrong-node.raftiq",
	})
	require.NoError(t, err)

	serverTLS, err := LoadTLSConfig(TLSConfig{
		CAFile:         caFile,
		CertFile:       certFile,
		KeyFile:        keyFile,
		PeerServerName: "node-1.raftiq",
	})
	require.NoError(t, err)

	listener, err := tls.Listen(
		"tcp",
		"127.0.0.1:0",
		&tls.Config{
			Certificates: serverTLS.Certificates,
			ClientCAs:    serverTLS.ClientCAs,
			ClientAuth:   tls.RequireAndVerifyClientCert,
			MinVersion:   tls.VersionTLS13,
		},
	)
	require.NoError(t, err)
	defer listener.Close()

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		_ = conn.(*tls.Conn).Handshake()
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
}
