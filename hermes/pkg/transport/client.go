package transport

import (
	"context"
	"fmt"
	"sync"
	"time"

	pb "github.com/aadityabinodyadav/hermes/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

type PeerClient struct {
	peerID  string
	address string
	config  Config

	conn *grpc.ClientConn

	RaftClient       pb.RaftServiceClient
	MembershipClient pb.MembershipServiceClient
	KVClient         pb.HermesKVClient

	mu          sync.RWMutex
	connectedAt time.Time
	lastError   error
}

func newPeerClient(peerID, address string, config Config) (*PeerClient, error) {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),

		grpc.WithBlock(),

		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                config.KeepAliveTime,
			Timeout:             config.KeepAliveTimeout,
			PermitWithoutStream: true, // send keepalive even if no active RPCs
		}),

		grpc.WithUnaryInterceptor(
			ChainClientInterceptors(
				ClientRequestIDInterceptor(),
				RetryInterceptor(3),
			),
		),

		grpc.WithWriteBufferSize(config.WriteBufferSize),
		grpc.WithReadBufferSize(config.ReadBufferSize),

		grpc.WithInitialWindowSize(64 * 1024),
		grpc.WithInitialConnWindowSize(512 * 1024),
	}

	ctx, cancel := context.WithTimeout(context.Background(), config.DialTimeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, address, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to peer %s (%s): %w",
			peerID, address, err)
	}

	client := &PeerClient{
		peerID:           peerID,
		address:          address,
		config:           config,
		conn:             conn,
		RaftClient:       pb.NewRaftServiceClient(conn),
		MembershipClient: pb.NewMembershipServiceClient(conn),
		KVClient:         pb.NewHermesKVClient(conn),
		connectedAt:      time.Now(),
	}

	return client, nil
}

func (c *PeerClient) IsReady() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.conn == nil {
		return false
	}
	return c.conn.GetState() == connectivity.Ready
}

func (c *PeerClient) WaitForReady(ctx context.Context) error {
	for {
		state := c.conn.GetState()
		if state == connectivity.Ready {
			return nil
		}
		if state == connectivity.Shutdown {
			return fmt.Errorf("connection to %s is shutdown", c.peerID)
		}

		if !c.conn.WaitForStateChange(ctx, state) {
			return ctx.Err()
		}
	}
}

func (c *PeerClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *PeerClient) ConnectionState() string {
	if c.conn == nil {
		return "nil"
	}
	return c.conn.GetState().String()
}

func ChainClientInterceptors(interceptors ...grpc.UnaryClientInterceptor) grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		chain := invoker
		for i := len(interceptors) - 1; i >= 0; i-- {
			interceptor := interceptors[i]
			next := chain
			chain = func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
				return interceptor(ctx, method, req, reply, cc, next, opts...)
			}
		}
		return chain(ctx, method, req, reply, cc, opts...)
	}
}

type ConnectionPool struct {
	config Config
	mu     sync.RWMutex
	peers  map[string]*PeerClient
}

func NewConnectionPool(config Config) *ConnectionPool {
	return &ConnectionPool{
		config: config,
		peers:  make(map[string]*PeerClient),
	}
}

func (p *ConnectionPool) AddPeer(peerID, address string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, exists := p.peers[peerID]; exists {
		return nil
	}

	client, err := newPeerClient(peerID, address, p.config)
	if err != nil {
		return fmt.Errorf("failed to add peer %s: %w", peerID, err)
	}

	p.peers[peerID] = client
	fmt.Printf("Connected to peer %s at %s\n", peerID, address)
	return nil
}

func (p *ConnectionPool) RemovePeer(peerID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	client, exists := p.peers[peerID]
	if !exists {
		return nil
	}

	delete(p.peers, peerID)
	return client.Close()
}

func (p *ConnectionPool) GetPeer(peerID string) *PeerClient {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.peers[peerID]
}

func (p *ConnectionPool) GetAllPeers() []*PeerClient {
	p.mu.RLock()
	defer p.mu.RUnlock()

	clients := make([]*PeerClient, 0, len(p.peers))
	for _, c := range p.peers {
		clients = append(clients, c)
	}
	return clients
}

func (p *ConnectionPool) BroadcastAppendEntries(
	ctx context.Context,
	req *pb.AppendEntriesRequest,
) <-chan *AppendEntriesResult {
	peers := p.GetAllPeers()
	resultCh := make(chan *AppendEntriesResult, len(peers))

	for _, peer := range peers {
		go func(c *PeerClient) {
			resp, err := c.RaftClient.AppendEntries(ctx, req)
			resultCh <- &AppendEntriesResult{
				PeerID:   c.peerID,
				Response: resp,
				Err:      err,
			}
		}(peer)
	}

	return resultCh
}

type AppendEntriesResult struct {
	PeerID   string
	Response *pb.AppendEntriesResponse
	Err      error
}

func (p *ConnectionPool) BroadcastRequestVote(
	ctx context.Context,
	req *pb.RequestVoteRequest,
) <-chan *RequestVoteResult {
	peers := p.GetAllPeers()
	resultCh := make(chan *RequestVoteResult, len(peers))

	for _, peer := range peers {
		go func(c *PeerClient) {
			resp, err := c.RaftClient.RequestVote(ctx, req)
			resultCh <- &RequestVoteResult{
				PeerID:   c.peerID,
				Response: resp,
				Err:      err,
			}
		}(peer)
	}

	return resultCh
}

type RequestVoteResult struct {
	PeerID   string
	Response *pb.RequestVoteResponse
	Err      error
}

func (p *ConnectionPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for peerID, client := range p.peers {
		if err := client.Close(); err != nil {
			fmt.Printf("Error closing connection to %s: %v\n", peerID, err)
		}
	}
	p.peers = make(map[string]*PeerClient)
}

func (p *ConnectionPool) HealthCheck(ctx context.Context) map[string]bool {
	peers := p.GetAllPeers()
	results := make(map[string]bool, len(peers))
	var mu sync.Mutex

	var wg sync.WaitGroup
	for _, peer := range peers {
		wg.Add(1)
		go func(c *PeerClient) {
			defer wg.Done()

			// Try a membership ping - lightweight health check
			_, err := c.MembershipClient.Ping(ctx, &pb.PingRequest{
				SenderId:       p.config.NodeID,
				SequenceNumber: uint64(time.Now().UnixNano()),
			})

			mu.Lock()
			results[c.peerID] = (err == nil)
			mu.Unlock()
		}(peer)
	}

	wg.Wait()
	return results
}
