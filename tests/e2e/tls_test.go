package e2e_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type e2eTLS struct {
	caFile string

	certFiles map[string]e2eCertFiles
}

type e2eCertFiles struct {
	certFile string
	keyFile  string
}

func newE2ETLS(t *testing.T, dir string, nodeIDs []string) *e2eTLS {
	t.Helper()

	now := time.Now()

	caKey, err := ecdsa.GenerateKey(
		elliptic.P256(),
		rand.Reader,
	)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}

	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "RaftIQ E2E Test CA",
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
	if err != nil {
		t.Fatalf("create CA certificate: %v", err)
	}

	caFile := filepath.Join(dir, "ca.crt")

	if err := os.WriteFile(
		caFile,
		pem.EncodeToMemory(&pem.Block{
			Type:  "CERTIFICATE",
			Bytes: caDER,
		}),
		0o600,
	); err != nil {
		t.Fatalf("write CA certificate: %v", err)
	}

	result := &e2eTLS{
		caFile:    caFile,
		certFiles: make(map[string]e2eCertFiles, len(nodeIDs)),
	}

	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("parse CA certificate: %v", err)
	}

	for i, nodeID := range nodeIDs {
		result.certFiles[nodeID] = writeE2ENodeCertificate(
			t,
			dir,
			caCert,
			caKey,
			nodeID,
			i+2,
		)
	}

	return result
}

func writeE2ENodeCertificate(
	t *testing.T,
	dir string,
	caCert *x509.Certificate,
	caKey *ecdsa.PrivateKey,
	nodeID string,
	serialSeed int,
) e2eCertFiles {
	t.Helper()

	nodeKey, err := ecdsa.GenerateKey(
		elliptic.P256(),
		rand.Reader,
	)
	if err != nil {
		t.Fatalf("generate certificate key for %s: %v", nodeID, err)
	}

	nodeTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(int64(serialSeed)),
		Subject: pkix.Name{
			CommonName: nodeID,
		},
		DNSNames: []string{
			nodeID + ".raftiq",
		},
		NotBefore: time.Now().Add(-time.Minute),
		NotAfter:  time.Now().Add(time.Hour),

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
	if err != nil {
		t.Fatalf("create certificate for %s: %v", nodeID, err)
	}

	certFile := filepath.Join(
		dir,
		fmt.Sprintf("%s.crt", nodeID),
	)
	keyFile := filepath.Join(
		dir,
		fmt.Sprintf("%s.key", nodeID),
	)

	if err := os.WriteFile(
		certFile,
		pem.EncodeToMemory(&pem.Block{
			Type:  "CERTIFICATE",
			Bytes: nodeDER,
		}),
		0o600,
	); err != nil {
		t.Fatalf("write certificate for %s: %v", nodeID, err)
	}

	keyDER, err := x509.MarshalECPrivateKey(nodeKey)
	if err != nil {
		t.Fatalf("marshal private key for %s: %v", nodeID, err)
	}

	if err := os.WriteFile(
		keyFile,
		pem.EncodeToMemory(&pem.Block{
			Type:  "EC PRIVATE KEY",
			Bytes: keyDER,
		}),
		0o600,
	); err != nil {
		t.Fatalf("write private key for %s: %v", nodeID, err)
	}

	return e2eCertFiles{
		certFile: certFile,
		keyFile:  keyFile,
	}
}
