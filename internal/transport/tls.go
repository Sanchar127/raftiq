package transport

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
)

var (
	ErrTLSConfigNil       = errors.New("TLS config is nil")
	ErrTLSCACertificate   = errors.New("TLS CA certificate is required")
	ErrTLSCertificate     = errors.New("TLS certificate is required")
	ErrTLSPrivateKey      = errors.New("TLS private key is required")
	ErrTLSInvalidPeerName = errors.New("TLS peer server name is required")
	ErrTLSInvalidPeerSAN  = errors.New("TLS peer SAN validation failed")
)

// TLSConfig contains the material and policy required for Raft peer mTLS.
type TLSConfig struct {
	CAFile         string
	CertFile       string
	KeyFile        string
	PeerServerName string
}

// LoadTLSConfig loads and validates the TLS configuration.
//
// The returned tls.Config is configured for mutual TLS:
//   - TLS 1.3 minimum
//   - server certificate authentication
//   - client certificate authentication
//   - CA-based peer verification
//   - explicit peer server-name verification
func LoadTLSConfig(cfg TLSConfig) (*tls.Config, error) {
	if cfg.CAFile == "" {
		return nil, ErrTLSCACertificate
	}

	if cfg.CertFile == "" {
		return nil, ErrTLSCertificate
	}

	if cfg.KeyFile == "" {
		return nil, ErrTLSPrivateKey
	}

	if cfg.PeerServerName == "" {
		return nil, ErrTLSInvalidPeerName
	}

	caPEM, err := os.ReadFile(cfg.CAFile)
	if err != nil {
		return nil, fmt.Errorf("read TLS CA certificate: %w", err)
	}

	caPool := x509.NewCertPool()

	if !caPool.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("parse TLS CA certificate")
	}

	certificate, err := tls.LoadX509KeyPair(
		cfg.CertFile,
		cfg.KeyFile,
	)
	if err != nil {
		return nil, fmt.Errorf("load TLS certificate and key: %w", err)
	}

	return &tls.Config{
		MinVersion: tls.VersionTLS13,

		Certificates: []tls.Certificate{
			certificate,
		},

		RootCAs: caPool,

		ClientCAs: caPool,

		ClientAuth: tls.RequireAndVerifyClientCert,

		ServerName: cfg.PeerServerName,
	}, nil
}

func verifyPeerSAN(expectedSAN string) func(tls.ConnectionState) error {
	return func(state tls.ConnectionState) error {
		if expectedSAN == "" {
			return ErrTLSInvalidPeerName
		}

		if len(state.PeerCertificates) == 0 {
			return ErrTLSInvalidPeerSAN
		}

		cert := state.PeerCertificates[0]

		for _, san := range cert.DNSNames {
			if san == expectedSAN {
				return nil
			}
		}

		return fmt.Errorf(
			"%w: expected %q",
			ErrTLSInvalidPeerSAN,
			expectedSAN,
		)
	}
}

func LoadTLSServerConfig(
	cfg TLSConfig,
	allowedPeerSANs map[string]struct{},
) (*tls.Config, error) {
	if cfg.CAFile == "" {
		return nil, ErrTLSCACertificate
	}

	if cfg.CertFile == "" {
		return nil, ErrTLSCertificate
	}

	if cfg.KeyFile == "" {
		return nil, ErrTLSPrivateKey
	}

	if len(allowedPeerSANs) == 0 {
		return nil, ErrTLSInvalidPeerName
	}

	caPEM, err := os.ReadFile(cfg.CAFile)
	if err != nil {
		return nil, fmt.Errorf("read TLS CA certificate: %w", err)
	}

	caPool := x509.NewCertPool()

	if !caPool.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("parse TLS CA certificate")
	}

	certificate, err := tls.LoadX509KeyPair(
		cfg.CertFile,
		cfg.KeyFile,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"load TLS certificate and key: %w",
			err,
		)
	}

	return &tls.Config{
		MinVersion: tls.VersionTLS13,

		Certificates: []tls.Certificate{
			certificate,
		},

		RootCAs:   caPool,
		ClientCAs: caPool,

		ClientAuth: tls.RequireAndVerifyClientCert,

		VerifyConnection: verifyPeerSANs(allowedPeerSANs),
	}, nil
}

func verifyPeerSANs(
	allowedSANs map[string]struct{},
) func(tls.ConnectionState) error {
	return func(state tls.ConnectionState) error {
		if len(state.PeerCertificates) == 0 {
			return ErrTLSInvalidPeerSAN
		}

		cert := state.PeerCertificates[0]

		for _, san := range cert.DNSNames {
			if _, ok := allowedSANs[san]; ok {
				return nil
			}
		}

		return ErrTLSInvalidPeerSAN
	}
}
