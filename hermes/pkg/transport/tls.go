// pkg/transport/tls.go
package transport

// mTLS (mutual TLS) for Hermes
//
// In production, every Hermes node must:
//   1. Present its own certificate (proves identity)
//   2. Verify the peer's certificate (proves peer identity)
//   3. Reject connections from unknown nodes
//
// Certificate hierarchy:
//   Root CA (your organization's CA)
//     └── Node cert (signed by Root CA)
//           node-1.hermes.internal
//           node-2.hermes.internal
//           ...
//
// Generation (use in production, not self-signed):
//   # Using certstrap:
//   certstrap init --cn "hermes-ca"
//   certstrap request-cert --cn "node-1" --domain "node-1.hermes.internal"
//   certstrap sign node-1 --CA hermes-ca
//
// Or use cert-manager in Kubernetes (automated rotation!)

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"google.golang.org/grpc/credentials"
)

// TLSConfig holds mTLS configuration for a Hermes node
type TLSConfig struct {
	// CertFile is this node's certificate
	CertFile string

	// KeyFile is this node's private key
	KeyFile string

	// CAFile is the CA certificate to verify peers
	CAFile string

	// ServerName is the expected server name in peer certificates
	// Used for hostname verification
	ServerName string

	// InsecureSkipVerify disables cert verification (TESTING ONLY!)
	// NEVER set this in production
	InsecureSkipVerify bool
}

// IsEnabled returns true if TLS is configured
func (c *TLSConfig) IsEnabled() bool {
	return c != nil && c.CertFile != "" && c.KeyFile != ""
}

// LoadServerCredentials creates gRPC server credentials from TLS config
func LoadServerCredentials(cfg *TLSConfig) (credentials.TransportCredentials, error) {
	if cfg == nil || !cfg.IsEnabled() {
		return nil, nil // no TLS
	}

	// Load our certificate + key
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("tls: failed to load server cert/key: %w", err)
	}

	// Load CA certificate to verify client certs
	var certPool *x509.CertPool
	if cfg.CAFile != "" {
		caData, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("tls: failed to read CA file: %w", err)
		}

		certPool = x509.NewCertPool()
		if !certPool.AppendCertsFromPEM(caData) {
			return nil, fmt.Errorf("tls: failed to parse CA certificate")
		}
	}

	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    certPool,

		// Security hardening
		MinVersion: tls.VersionTLS13, // TLS 1.3 minimum
		CipherSuites: []uint16{ // only strong ciphers
			tls.TLS_AES_256_GCM_SHA384,
			tls.TLS_CHACHA20_POLY1305_SHA256,
		},
	}

	return credentials.NewTLS(tlsCfg), nil
}

// LoadClientCredentials creates gRPC client credentials for peer connections
func LoadClientCredentials(cfg *TLSConfig) (credentials.TransportCredentials, error) {
	if cfg == nil || !cfg.IsEnabled() {
		return nil, nil // no TLS
	}

	// Load our certificate (we present this to servers)
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("tls: failed to load client cert/key: %w", err)
	}

	// Load CA to verify server certs
	var certPool *x509.CertPool
	if cfg.CAFile != "" {
		caData, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("tls: failed to read CA file: %w", err)
		}

		certPool = x509.NewCertPool()
		if !certPool.AppendCertsFromPEM(caData) {
			return nil, fmt.Errorf("tls: failed to parse CA certificate")
		}
	}

	tlsCfg := &tls.Config{
		Certificates:       []tls.Certificate{cert},
		RootCAs:            certPool,
		ServerName:         cfg.ServerName,
		InsecureSkipVerify: cfg.InsecureSkipVerify, // ONLY for testing
		MinVersion:         tls.VersionTLS13,
	}

	return credentials.NewTLS(tlsCfg), nil
}

// GenerateTestCerts generates self-signed certs for testing
// DO NOT USE IN PRODUCTION
func GenerateTestCerts(dir string) (*TLSConfig, error) {
	// In a real implementation, use crypto/x509 to generate
	// self-signed certificates for testing.
	//
	// For this demo, we document how to do it:
	fmt.Printf(`
To generate test certificates:

  # Install certstrap
  go install github.com/square/certstrap@latest

  # Create CA
  certstrap --depot-path %s init --cn "hermes-test-ca" --passphrase ""

  # Create node certificate  
  certstrap --depot-path %s request-cert --cn "hermes-node" \
    --domain "localhost,127.0.0.1" --passphrase ""
  certstrap --depot-path %s sign hermes-node --CA hermes-test-ca

  # Use in config:
  --tls-cert=%s/hermes-node.crt
  --tls-key=%s/hermes-node.key
  --tls-ca=%s/hermes-test-ca.crt
`, dir, dir, dir, dir, dir, dir)

	return &TLSConfig{
		CertFile:           dir + "/hermes-node.crt",
		KeyFile:            dir + "/hermes-node.key",
		CAFile:             dir + "/hermes-test-ca.crt",
		InsecureSkipVerify: true, // only for testing!
	}, nil
}
