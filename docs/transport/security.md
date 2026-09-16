# Transport: Security (TLS / mTLS)

**Read [`../AUDIT.md`](../AUDIT.md) and the README's Status section
first.** TLS/mTLS is implemented and unit-tested, but `cmd/raftiq/main.go`
does not use it. Every node started via the stock binary runs both gRPC
listeners in plaintext.

## What's implemented (`internal/transport/tls.go`)

```go
type TLSConfig struct {
	CAFile         string
	CertFile       string
	KeyFile        string
	PeerServerName string
}
```

Three constructors, all requiring TLS 1.3 minimum and mutual TLS
(`ClientAuth: tls.RequireAndVerifyClientCert`):

- `LoadTLSConfig(cfg)` — full mTLS config for peer-to-peer Raft
  connections: loads CA pool, loads the node's own cert/key pair, sets
  `ServerName` to `cfg.PeerServerName` for outbound verification. Requires
  `CAFile`, `CertFile`, `KeyFile`, and `PeerServerName` all non-empty.
- `LoadTLSClientConfig(cfg)` — same certificate/CA loading, without a
  fixed `PeerServerName` (server name is set per-connection by the
  caller). Requires `CAFile`, `CertFile`, `KeyFile`.
- `LoadTLSServerConfig(cfg, ...)` — server-side config, with SAN
  verification callbacks (`verifyPeerSAN`/`verifyPeerSANs`) to check a
  connecting peer's certificate SAN against an expected identity.

All three build the CA pool from a single PEM file
(`x509.NewCertPool().AppendCertsFromPEM`) and load a single cert/key pair
via `tls.LoadX509KeyPair` — there's no support for rotating certs at
runtime or multiple CAs beyond what a single combined PEM file provides.

`GRPCTransport.SetTLSConfig(*tls.Config)` (in `grpc_raft_transport.go`)
applies a loaded config to the client-side peer connections.
`internal/transport/tls_test.go` covers the loading/validation logic.

## What's missing to actually use it

- `cmd/raftiq/main.go` has no `-tls-ca`, `-tls-cert`, `-tls-key`, or
  similar flags.
- `main.go` never calls `LoadTLSConfig`/`LoadTLSServerConfig`/
  `GRPCTransport.SetTLSConfig`/passes TLS credentials to
  `transport.NewServer`.
- There is no flag to require/reject plaintext connections — the binary
  simply never offers TLS as an option today.

## Using TLS today (requires a custom `main`)

Until this is wired in, using TLS/mTLS means writing your own thin `main`
package (or a fork of `cmd/raftiq/main.go`) that:

1. Loads `internal/transport.TLSConfig` from file paths of your choosing.
2. Calls `LoadTLSServerConfig`/`LoadTLSClientConfig` as appropriate.
3. Passes the resulting `*tls.Config` into `transport.NewServer`'s
   `grpc.ServerOption`s (via `grpc.Creds(credentials.NewTLS(cfg))`) for
   both the Raft and KV listeners, and into
   `GRPCTransport.SetTLSConfig` for outbound peer connections.

This is real, tested code you can build on — it's just not exposed as a
CLI feature of the shipped binary.

## Authentication / authorization

There is no authentication or authorization layer beyond whatever mTLS
client-certificate verification you configure yourself. Any client that
can reach the KV port (and pass TLS handshake, if configured) can read and
write any key. There is no per-key or per-client access control.

## Threat model

RaftIQ assumes fail-stop/omission failures and network unreliability
(delay, loss, reordering, duplication), not malicious/Byzantine nodes.
Even with mTLS fully configured, a compromised node holding valid
credentials can still submit arbitrary commands through `Propose` — mTLS
protects the wire, not against a legitimately-authenticated bad actor.
