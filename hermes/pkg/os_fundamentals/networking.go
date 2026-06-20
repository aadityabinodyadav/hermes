package os_fundamentals

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)



func writeMessage(conn net.Conn, msg string) error {
	data := []byte(msg)

	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(data)))

	if _, err := conn.Write(lenBuf[:]); err != nil {
		return err
	}
	if _, err := conn.Write(data); err != nil {
		return err
	}
	return nil
}

func readMessage(r *bufio.Reader) (string, error) {
	var msgLen uint32
	if err := binary.Read(r, binary.BigEndian, &msgLen); err != nil {
		return "", err
	}

	buf := make([]byte, msgLen)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}



func handleConnection(conn net.Conn) {
	defer conn.Close()

	fmt.Printf("Server: accepted connection from %s\n", conn.RemoteAddr())

	r := bufio.NewReader(conn)
	for {
		msg, err := readMessage(r)
		if err == io.EOF {
			fmt.Println("Server: client disconnected")
			return
		}
		if err != nil {
			fmt.Printf("Server: read error: %v\n", err)
			return
		}

		fmt.Printf("Server: received (%d bytes): %s\n", len(msg), msg)

		if err := writeMessage(conn, "ACK: "+msg); err != nil {
			fmt.Printf("Server: write error: %v\n", err)
			return
		}
	}
}

func runServer(listener net.Listener, wg *sync.WaitGroup) {
	defer wg.Done()

	conn, err := listener.Accept()
	if err != nil {
		return
	}
	handleConnection(conn)
}



func runClient(addr string, wg *sync.WaitGroup) {
	defer wg.Done()

	time.Sleep(50 * time.Millisecond) 

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Printf("Client: dial error: %v\n", err)
		return
	}
	defer conn.Close()

	fmt.Printf("Client: connected to %s\n", addr)

	r := bufio.NewReader(conn)

	
	messages := []string{
		"AppendEntries{term:1, leaderId:node1, entries:[{index:1,cmd:'PUT k1 v1'}]}",
		"AppendEntries{term:1, leaderId:node1, entries:[{index:2,cmd:'PUT k2 v2'}]}",
		"Heartbeat{term:1, leaderId:node1}",
	}

	for _, msg := range messages {
		start := time.Now()

		if err := writeMessage(conn, msg); err != nil {
			fmt.Printf("Client: write error: %v\n", err)
			return
		}

		ack, err := readMessage(r)
		if err != nil {
			fmt.Printf("Client: read error: %v\n", err)
			return
		}

		rtt := time.Since(start)
		preview := msg
		if len(preview) > 40 {
			preview = preview[:40] + "..."
		}
		fmt.Printf("Client: sent %q | got %q | RTT: %v\n", preview, ack, rtt)
	}
}



func RawTCPDemo() {
	fmt.Println("=== RAW TCP: WHAT GRPC IS BUILT ON ===")
	fmt.Println()

	listener, err := net.Listen("tcp", "127.0.0.1:0") 
	if err != nil {
		fmt.Printf("Listen error: %v\n", err)
		return
	}
	defer listener.Close()

	fmt.Printf("Server listening on: %s\n", listener.Addr())

	var wg sync.WaitGroup

	wg.Add(1)
	go runServer(listener, &wg)

	wg.Add(1)
	go runClient(listener.Addr().String(), &wg)

	wg.Wait()
}



func TCPProperties() {
	fmt.Println("\n=== TCP PROPERTIES THAT MATTER FOR DISTRIBUTED SYSTEMS ===")

	properties := []struct {
		name   string
		detail string
	}{
		{
			"ORDERING",
			"Bytes arrive in the order they were sent.\n" +
				"   This is WHY Raft log entries stay ordered over the wire.",
		},
		{
			"RELIABILITY",
			"TCP retransmits lost packets automatically.\n" +
				"   BUT retransmission adds latency - looks like a slow network to Raft.",
		},
		{
			"FLOW CONTROL",
			"Receive window is typically 65KB-4MB.\n" +
				"   A slow follower can block the leader's replication pipeline.\n" +
				"   Hermes handles this via streaming gRPC with flow control.",
		},
		{
			"CONNECTION COST",
			"TCP handshake = 1 RTT (SYN → SYN-ACK → ACK).\n" +
				"   Same-datacenter RTT ≈ 0.5ms, so new connections are expensive.\n" +
				"   Solution: keep connections open. Hermes uses 1 persistent conn per peer.",
		},
		{
			"HALF-OPEN CONNECTIONS",
			"If node B crashes, node A may not notice for hours.\n" +
				"   TCP keepalive is OFF by default.\n" +
				"   Solution: application-level heartbeats. Raft heartbeat = failure detector.",
		},
	}

	for i, p := range properties {
		fmt.Printf("%d. %s\n   %s\n\n", i+1, p.name, p.detail)
	}

	demonstrateTCPKeepAlive()
	demonstrateConnLatency()
}

func demonstrateTCPKeepAlive() {
	fmt.Println("--- TCP Keepalive ---")

	listener, _ := net.Listen("tcp", "127.0.0.1:0")
	defer listener.Close()

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		time.Sleep(100 * time.Millisecond)
	}()

	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		fmt.Printf("Dial error: %v\n", err)
		return
	}
	defer conn.Close()

	tcpConn := conn.(*net.TCPConn)
	tcpConn.SetKeepAlive(true)
	tcpConn.SetKeepAlivePeriod(30 * time.Second)

	fmt.Println("  Keepalive: ENABLED (30s interval)")
	fmt.Println("  Without: dead connections persist for hours")
	fmt.Println("  With:    dead connections detected in ~90s")
	fmt.Printf("  Hermes also sends app-level heartbeats every %dms\n\n", 150)
}

func demonstrateConnLatency() {
	fmt.Println("--- Connection Reuse vs New Connection ---")

	listener, _ := net.Listen("tcp", "127.0.0.1:0")
	defer listener.Close()

	
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			conn.Close()
		}
	}()

	const N = 100

	
	start := time.Now()
	for i := 0; i < N; i++ {
		conn, _ := net.Dial("tcp", listener.Addr().String())
		conn.Close()
	}
	newConnDur := time.Since(start)

	
	conn, _ := net.Dial("tcp", listener.Addr().String())
	start = time.Now()
	for i := 0; i < N; i++ {
		_ = conn
		time.Sleep(10 * time.Microsecond) 
	}
	conn.Close()
	reuseDur := time.Since(start)

	fmt.Printf("  %d new connections:    %v (avg %v/req)\n", N, newConnDur, newConnDur/N)
	fmt.Printf("  %d reused connections: %v (avg %v/req)\n", N, reuseDur, reuseDur/N)
	fmt.Println()
	fmt.Println("  LESSON: Pool connections. Hermes keeps 1 persistent conn per peer.")
}



type NetworkPartitionSimulator struct {
	mu          sync.RWMutex
	partitioned map[string]map[string]bool
}

func NewNetworkPartitionSimulator() *NetworkPartitionSimulator {
	return &NetworkPartitionSimulator{
		partitioned: make(map[string]map[string]bool),
	}
}

func (s *NetworkPartitionSimulator) Partition(a, b string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.partitioned[a] == nil {
		s.partitioned[a] = make(map[string]bool)
	}
	if s.partitioned[b] == nil {
		s.partitioned[b] = make(map[string]bool)
	}
	s.partitioned[a][b] = true
	s.partitioned[b][a] = true

	fmt.Printf("🔴 PARTITION: %s ←✗→ %s\n", a, b)
}

func (s *NetworkPartitionSimulator) Heal(a, b string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.partitioned[a], b)
	delete(s.partitioned[b], a)

	fmt.Printf("🟢 HEALED:    %s ←→ %s\n", a, b)
}

func (s *NetworkPartitionSimulator) CanCommunicate(from, to string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return !s.partitioned[from][to]
}



func NetworkConceptsDemo() {
	fmt.Println("\n=== NETWORK PARTITION SIMULATION ===")
	fmt.Println()
	fmt.Println("Most important concept in distributed systems:")
	fmt.Println("Nodes can lose connectivity while BOTH are still running.")
	fmt.Println()

	sim := NewNetworkPartitionSimulator()

	
	minority := []string{"node-1", "node-2"}
	majority := []string{"node-3", "node-4", "node-5"}

	fmt.Println("EVENT: datacenter switch fails!")
	fmt.Println()
	for _, a := range minority {
		for _, b := range majority {
			sim.Partition(a, b)
		}
	}

	fmt.Println()
	fmt.Println("What happens in a 5-node Raft cluster:")
	fmt.Println("  Minority side [node-1, node-2] → 2 nodes, below quorum of 3 → STALLED")
	fmt.Println("  Majority side [node-3, node-4, node-5] → 3 nodes, has quorum → ELECTS LEADER")
	fmt.Println()
	fmt.Println("  If node-1 was the old leader:")
	fmt.Println("    It loses quorum and cannot commit new entries.")
	fmt.Println("    Majority side elects a new leader.")
	fmt.Println("    node-1 still thinks it's leader - but its writes DON'T commit.")
	fmt.Println("    This is why lease-based reads exist (Phase 8).")
	fmt.Println()

	time.Sleep(100 * time.Millisecond)

	for _, a := range minority {
		for _, b := range majority {
			sim.Heal(a, b)
		}
	}

	fmt.Println()
	fmt.Println("After healing:")
	fmt.Println("  node-1 receives AppendEntries from new leader with higher term.")
	fmt.Println("  node-1 sees higher term → steps down immediately.")
	fmt.Println("  Any uncommitted entries from node-1 are discarded.")
	fmt.Println("  Raft guarantee: committed entries are NEVER lost.")
}
