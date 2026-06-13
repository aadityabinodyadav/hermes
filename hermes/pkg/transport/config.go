package transport

import "time"

type Config struct {
	// ── Server settings ──────────────────────────────────────

	// Address to listen on (e.g., "0.0.0.0:7000")
	ListenAddr string

	// NodeID is this node's unique identifier
	// Format: "hostname:port" — must be unique in cluster
	NodeID string

	// ── Connection settings ───────────────────────────────────

	// MaxConcurrentStreams limits HTTP/2 streams per connection
	// Too high: memory pressure. Too low: throughput bottleneck.
	// Default: 256 (etcd uses 1000)
	MaxConcurrentStreams uint32

	// MaxRecvMsgSize is the largest message we'll accept
	// Protects against memory exhaustion from malicious/buggy peers
	// Default: 64MB (snapshot chunks can be large)
	MaxRecvMsgSize int

	// MaxSendMsgSize limits outgoing message size
	MaxSendMsgSize int

	// ── Timeout settings ──────────────────────────────────────
	// THESE ARE CRITICAL — wrong values = false failure detection

	// DialTimeout: how long to wait when connecting to a peer
	// If a node is starting up, we need time for it to be ready
	// Default: 5s
	DialTimeout time.Duration

	// RequestTimeout: default timeout per RPC call
	// Should be less than Raft election timeout!
	// If RPC takes longer than election timeout, leader might lose election
	// Default: 2s
	RequestTimeout time.Duration

	// KeepAliveTime: how often to send TCP keepalives
	// Detects dead connections faster than TCP default (hours!)
	// Default: 10s
	KeepAliveTime time.Duration

	// KeepAliveTimeout: how long to wait for keepalive ACK
	// After this, connection is declared dead
	// Default: 5s
	KeepAliveTimeout time.Duration

	// ── Connection pool settings ──────────────────────────────

	// MaxConnectionsPerPeer: gRPC connections are multiplexed,
	// but for high throughput you sometimes want multiple
	// Default: 1 (gRPC multiplexing is usually enough)
	MaxConnectionsPerPeer int

	// ── Write buffer settings ─────────────────────────────────

	// WriteBufferSize: gRPC write buffer per stream
	// Larger = better throughput, more memory
	// Default: 32KB
	WriteBufferSize int

	// ReadBufferSize: gRPC read buffer per stream
	ReadBufferSize int
}

func DefaultConfig(listenAddr, nodeID string) Config {
	return Config{
		ListenAddr:            listenAddr,
		NodeID:                nodeID,
		MaxConcurrentStreams:  256,
		MaxRecvMsgSize:        64 * 1024 * 1024, // 64MB
		MaxSendMsgSize:        64 * 1024 * 1024, // 64MB
		DialTimeout:           5 * time.Second,
		RequestTimeout:        2 * time.Second,
		KeepAliveTime:         10 * time.Second,
		KeepAliveTimeout:      5 * time.Second,
		MaxConnectionsPerPeer: 1,
		WriteBufferSize:       32 * 1024, // 32KB
		ReadBufferSize:        32 * 1024, // 32KB
	}
}
