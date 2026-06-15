package transport

import (
	"context"
	"fmt"
	"time"

	pb "github.com/aadityabinodyadav/hermes/proto"
)

type PlaceholderRaftHandler struct {
	pb.UnimplementedRaftServiceServer
	nodeID string
}

func (h *PlaceholderRaftHandler) AppendEntries(
	ctx context.Context,
	req *pb.AppendEntriesRequest,
) (*pb.AppendEntriesResponse, error) {
	fmt.Printf("[%s] Received AppendEntries from %s (term=%d, entries=%d)\n",
		h.nodeID, req.LeaderId, req.Term, len(req.Entries))

	return &pb.AppendEntriesResponse{
		Term:       req.Term,
		Success:    true,
		FollowerId: h.nodeID,
	}, nil
}

func (h *PlaceholderRaftHandler) RequestVote(
	ctx context.Context,
	req *pb.RequestVoteRequest,
) (*pb.RequestVoteResponse, error) {
	fmt.Printf("[%s] Received RequestVote from %s (term=%d)\n",
		h.nodeID, req.CandidateId, req.Term)

	return &pb.RequestVoteResponse{
		Term:        req.Term,
		VoteGranted: true,
		VoterId:     h.nodeID,
	}, nil
}

type PlaceholderMembershipHandler struct {
	pb.UnimplementedMembershipServiceServer
	nodeID string
}

func (h *PlaceholderMembershipHandler) Ping(
	ctx context.Context,
	req *pb.PingRequest,
) (*pb.PingResponse, error) {
	return &pb.PingResponse{
		SenderId:       h.nodeID,
		SequenceNumber: req.SequenceNumber,
	}, nil
}

type PlaceholderKVHandler struct {
	pb.UnimplementedHermesKVServer
	nodeID string
	store  map[string][]byte
}

func NewPlaceholderKVHandler(nodeID string) *PlaceholderKVHandler {
	return &PlaceholderKVHandler{
		nodeID: nodeID,
		store:  make(map[string][]byte),
	}
}

func (h *PlaceholderKVHandler) Put(
	ctx context.Context,
	req *pb.PutRequest,
) (*pb.PutResponse, error) {
	h.store[req.Key] = req.Value
	fmt.Printf("[%s] PUT %s = %s\n", h.nodeID, req.Key, string(req.Value))
	return &pb.PutResponse{Version: 1}, nil
}

func (h *PlaceholderKVHandler) Get(
	ctx context.Context,
	req *pb.GetRequest,
) (*pb.GetResponse, error) {
	val, ok := h.store[req.Key]
	if !ok {
		return &pb.GetResponse{Found: false, Error: pb.ErrorCode_KEY_NOT_FOUND}, nil
	}
	return &pb.GetResponse{
		Found: true,
		Value: val,
	}, nil
}

func RunTransportDemo() {
	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Println("║       HERMES - PHASE 1: TRANSPORT LAYER DEMO                ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")
	fmt.Println()

	fmt.Println("━━━ DEMO 1: Basic gRPC Communication ━━━")
	demoBasicGRPC()

	fmt.Println("\n━━━ DEMO 2: Connection Pool (simulating cluster) ━━━")
	demoConnectionPool()

	fmt.Println("\n━━━ DEMO 3: Network Simulator (chaos testing) ━━━")
	demoNetworkSimulator()

	fmt.Println("\n━━━ DEMO 4: Raft-style Fan-Out (AppendEntries to all peers) ━━━")
	demoConcurrentBroadcast()
}

func demoBasicGRPC() {
	serverConfig := DefaultConfig("127.0.0.1:0", "node-1")
	server, err := NewServer(serverConfig)
	if err != nil {
		fmt.Printf("Failed to create server: %v\n", err)
		return
	}

	server.RegisterRaftService(&PlaceholderRaftHandler{nodeID: "node-1"})
	server.RegisterMembershipService(&PlaceholderMembershipHandler{nodeID: "node-1"})
	server.RegisterKVService(NewPlaceholderKVHandler("node-1"))

	if err := server.Start(); err != nil {
		fmt.Printf("Failed to start server: %v\n", err)
		return
	}
	defer server.Stop(context.Background())

	time.Sleep(50 * time.Millisecond)

	clientConfig := DefaultConfig("", "client")
	pool := NewConnectionPool(clientConfig)
	defer pool.Close()

	if err := pool.AddPeer("node-1", server.Addr()); err != nil {
		fmt.Printf("Failed to connect: %v\n", err)
		return
	}

	peer := pool.GetPeer("node-1")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	fmt.Println("Sending KV operations via gRPC:")

	putResp, err := peer.KVClient.Put(ctx, &pb.PutRequest{
		Key:   "hello",
		Value: []byte("distributed world"),
	})
	if err != nil {
		fmt.Printf("Put failed: %v\n", err)
		return
	}
	fmt.Printf("  PUT hello = 'distributed world' → version=%d\n", putResp.Version)

	getResp, err := peer.KVClient.Get(ctx, &pb.GetRequest{Key: "hello"})
	if err != nil {
		fmt.Printf("Get failed: %v\n", err)
		return
	}
	fmt.Printf("  GET hello → '%s' (found=%v)\n", string(getResp.Value), getResp.Found)

	fmt.Println("\nSending Raft AppendEntries via gRPC:")
	raftResp, err := peer.RaftClient.AppendEntries(ctx, &pb.AppendEntriesRequest{
		Term:     1,
		LeaderId: "leader-node",
		Entries: []*pb.LogEntry{
			{Index: 1, Term: 1, Data: []byte("PUT key1 value1")},
			{Index: 2, Term: 1, Data: []byte("PUT key2 value2")},
		},
		LeaderCommit: 0,
	})
	if err != nil {
		fmt.Printf("AppendEntries failed: %v\n", err)
		return
	}
	fmt.Printf("  AppendEntries → success=%v, term=%d\n",
		raftResp.Success, raftResp.Term)
}

func demoConnectionPool() {
	type nodeServer struct {
		id     string
		server *Server
	}

	var servers []nodeServer
	for i := 1; i <= 3; i++ {
		id := fmt.Sprintf("node-%d", i)
		cfg := DefaultConfig("127.0.0.1:0", id)
		srv, err := NewServer(cfg)
		if err != nil {
			fmt.Printf("Failed to create server %s: %v\n", id, err)
			return
		}
		srv.RegisterRaftService(&PlaceholderRaftHandler{nodeID: id})
		srv.RegisterMembershipService(&PlaceholderMembershipHandler{nodeID: id})
		srv.RegisterKVService(NewPlaceholderKVHandler(id))
		srv.Start()
		servers = append(servers, nodeServer{id: id, server: srv})
	}
	defer func() {
		for _, s := range servers {
			s.server.Stop(context.Background())
		}
	}()

	time.Sleep(50 * time.Millisecond)

	pool := NewConnectionPool(DefaultConfig("", "leader"))
	defer pool.Close()

	for _, s := range servers {
		pool.AddPeer(s.id, s.server.Addr())
	}

	fmt.Printf("Connected to %d peers\n", len(servers))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	fmt.Println("Health checking all peers:")
	health := pool.HealthCheck(ctx)
	for peerID, healthy := range health {
		status := "✅"
		if !healthy {
			status = "❌"
		}
		fmt.Printf("  %s %s\n", status, peerID)
	}
}

func demoNetworkSimulator() {
	sim := NewNetworkSimulator()

	nodes := []string{"node-1", "node-2", "node-3", "node-4", "node-5"}

	fmt.Println("Initial state: perfect network")
	for _, from := range nodes[:2] {
		for _, to := range nodes[2:] {
			result := sim.ShouldDeliver(from, to)
			fmt.Printf("  %s → %s: deliver=%v\n", from, to, result)
		}
	}

	fmt.Println()
	fmt.Println("Applying network partition: [node-1,node-2] ↔ [node-3,node-4,node-5]")
	sim.Partition(nodes[:2], nodes[2:])

	fmt.Println("\nAfter partition:")
	testCases := [][2]string{
		{"node-1", "node-2"},
		{"node-1", "node-3"},
		{"node-3", "node-4"},
		{"node-4", "node-1"},
	}
	for _, tc := range testCases {
		result := sim.ShouldDeliver(tc[0], tc[1])
		icon := "✅"
		if !result {
			icon = "❌"
		}
		fmt.Printf("  %s %s → %s: deliver=%v\n", icon, tc[0], tc[1], result)
	}

	fmt.Println()
	fmt.Println("Adding latency between node-3 and node-4:")
	sim.SetLatency("node-3", "node-4", 50*time.Millisecond, 100*time.Millisecond)

	start := time.Now()
	sim.ShouldDeliver("node-3", "node-4")
	fmt.Printf("  Delivery time with latency: %v\n", time.Since(start))

	fmt.Println()
	sim.HealPartition(nodes[:2], nodes[2:])
	fmt.Printf("\nSimulator stats: %s\n", sim.Stats())
}

func demoConcurrentBroadcast() {

	type follower struct {
		id     string
		server *Server
	}

	var followers []follower
	for i := 1; i <= 4; i++ {
		id := fmt.Sprintf("follower-%d", i)
		cfg := DefaultConfig("127.0.0.1:0", id)
		srv, _ := NewServer(cfg)
		srv.RegisterRaftService(&PlaceholderRaftHandler{nodeID: id})
		srv.RegisterMembershipService(&PlaceholderMembershipHandler{nodeID: id})
		srv.RegisterKVService(NewPlaceholderKVHandler(id))
		srv.Start()
		followers = append(followers, follower{id: id, server: srv})
	}
	defer func() {
		for _, f := range followers {
			f.server.Stop(context.Background())
		}
	}()

	time.Sleep(50 * time.Millisecond)

	pool := NewConnectionPool(DefaultConfig("", "leader"))
	defer pool.Close()

	for _, f := range followers {
		pool.AddPeer(f.id, f.server.Addr())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	req := &pb.AppendEntriesRequest{
		Term:         5,
		LeaderId:     "leader",
		PrevLogIndex: 9,
		PrevLogTerm:  4,
		Entries: []*pb.LogEntry{
			{Index: 10, Term: 5, Data: []byte("PUT user:1 alice")},
		},
		LeaderCommit: 9,
	}

	fmt.Printf("Leader broadcasting AppendEntries (index=10) to %d followers:\n",
		len(followers))

	start := time.Now()
	resultCh := pool.BroadcastAppendEntries(ctx, req)

	quorumNeeded := len(followers)/2 + 1
	acks := 0
	quorumAchieved := false

	for i := 0; i < len(followers); i++ {
		result := <-resultCh
		if result.Err != nil {
			fmt.Printf("  ❌ %s: error=%v\n", result.PeerID, result.Err)
		} else {
			fmt.Printf("  ✅ %s: success=%v (term=%d)\n",
				result.PeerID, result.Response.Success, result.Response.Term)
			if result.Response.Success {
				acks++
				if !quorumAchieved && acks >= quorumNeeded {
					quorumAchieved = true
					fmt.Printf("\n  ⚡ QUORUM REACHED at %v! Entry %d is COMMITTED\n",
						time.Since(start), req.Entries[0].Index)
					fmt.Printf("  (Waiting for remaining acks...)\n\n")
				}
			}
		}
	}

	fmt.Printf("\nFinal: %d/%d acknowledged | Total time: %v\n",
		acks, len(followers), time.Since(start))
}
