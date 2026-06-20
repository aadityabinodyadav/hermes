// pkg/membership/swim.go
package membership

// SWIM: Scalable Weakly-consistent Infection-style Membership
//
// Paper: "SWIM: Scalable Weakly-consistent Infection-style Process
//         Group Membership Protocol" (Das, Gupta, Motivala 2002)
//
// Three components:
//   1. Failure Detection: ping + indirect ping
//   2. Dissemination: piggyback on failure detection messages
//   3. Membership: apply updates, trigger callbacks
//
// PROTOCOL ROUND (every protocolPeriod ms):
//   1. Pick ONE random member M
//   2. Send PING to M
//   3. If ACK received within pingTimeout: done (M is alive)
//   4. If no ACK: pick K random members, send PING-REQ(M)
//      "Hey, can you ping M for me?"
//   5. If any PING-REQ returns ACK: done (M is alive, we had a routing issue)
//   6. If no PING-REQ returns ACK within indirectTimeout:
//      SUSPECT M (add to member list as SUSPECTED)
//   7. If M doesn't refute within suspicionTimeout:
//      DEAD M (add to member list as DEAD)
//
// PIGGYBACKING:
//   Every PING and PING-REQ carries recent GossipUpdates
//   These updates are applied to receiver's member list
//   Information spreads O(log N) rounds

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// SWIMConfig configures the SWIM protocol
type SWIMConfig struct {
	// NodeID is this node's unique ID
	NodeID string

	// BindAddr is the address to listen on for SWIM messages
	BindAddr string

	// ProtocolPeriod is how often to probe a random member
	// Lower = faster detection, higher bandwidth
	ProtocolPeriod time.Duration

	// PingTimeout is how long to wait for direct ping response
	PingTimeout time.Duration

	// IndirectPingCount (K) is how many nodes to ask for indirect pings
	IndirectPingCount int

	// IndirectTimeout is how long to wait for indirect pings
	IndirectTimeout time.Duration

	// SuspicionTimeout before declaring dead
	// Longer = fewer false positives but slower detection
	SuspicionTimeout time.Duration

	// GossipFanout is how many updates to piggyback per message
	GossipFanout int
}

// DefaultSWIMConfig returns sensible defaults
func DefaultSWIMConfig(nodeID, bindAddr string) SWIMConfig {
	return SWIMConfig{
		NodeID:            nodeID,
		BindAddr:          bindAddr,
		ProtocolPeriod:    200 * time.Millisecond,
		PingTimeout:       200 * time.Millisecond,
		IndirectPingCount: 3,
		IndirectTimeout:   600 * time.Millisecond,
		SuspicionTimeout:  3 * time.Second,
		GossipFanout:      8,
	}
}

// SWIMMessage is a SWIM protocol message
type SWIMMessage struct {
	Type    SWIMMessageType
	From    string
	To      string
	Target  string         // for PING_REQ: who to ping
	SeqNum  uint64         // for matching responses
	Updates []GossipUpdate // piggybacked membership updates
}

type SWIMMessageType uint8

const (
	MsgPing       SWIMMessageType = 0
	MsgPingAck    SWIMMessageType = 1
	MsgPingReq    SWIMMessageType = 2
	MsgPingReqAck SWIMMessageType = 3
)

func (t SWIMMessageType) String() string {
	switch t {
	case MsgPing:
		return "PING"
	case MsgPingAck:
		return "PING_ACK"
	case MsgPingReq:
		return "PING_REQ"
	case MsgPingReqAck:
		return "PING_REQ_ACK"
	}
	return "UNKNOWN"
}

// Transport is the network interface for SWIM messages
type Transport interface {
	// Send sends a SWIM message to a node
	Send(ctx context.Context, nodeID string, msg SWIMMessage) error

	// Recv returns a channel for incoming messages
	Recv() <-chan SWIMMessage
}

// ─────────────────────────────────────────────────────────────────────────────

// SWIM is the core SWIM protocol implementation
type SWIM struct {
	config     SWIMConfig
	memberList *MemberList
	detector   *PhiAccrualDetector
	transport  Transport

	// State
	mu              sync.Mutex
	seqNum          uint64                 // monotonically increasing sequence numbers
	pendingPings    map[uint64]chan bool   // seq → ack channel
	suspicionTimers map[string]*time.Timer // nodeID → suspicion timer

	// Random source
	rng *rand.Rand

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewSWIM creates a new SWIM instance
func NewSWIM(
	config SWIMConfig,
	memberList *MemberList,
	transport Transport,
) *SWIM {
	ctx, cancel := context.WithCancel(context.Background())

	s := &SWIM{
		config:          config,
		memberList:      memberList,
		detector:        NewPhiAccrualDetector(8.0), // φ=8 default
		transport:       transport,
		pendingPings:    make(map[uint64]chan bool),
		suspicionTimers: make(map[string]*time.Timer),
		rng:             rand.New(rand.NewSource(time.Now().UnixNano())),
		ctx:             ctx,
		cancel:          cancel,
	}

	// When phi detector fires, suspect the node
	s.detector.OnSuspect(func(nodeID string, phi float64) {
		fmt.Printf("[%s] PhiAccrual: %s has φ=%.1f (threshold=8), suspecting\n",
			config.NodeID, nodeID, phi)
		s.suspectNode(nodeID)
	})

	// When suspected node comes back alive
	s.detector.OnAlive(func(nodeID string, phi float64) {
		fmt.Printf("[%s] PhiAccrual: %s is back alive (φ=%.1f)\n",
			config.NodeID, nodeID, phi)
		s.memberList.ApplyUpdate(GossipUpdate{
			NodeID: nodeID,
			State:  StateAlive,
		})
	})

	return s
}

// Start begins the SWIM protocol loops
func (s *SWIM) Start() {
	// Main protocol loop
	s.wg.Add(1)
	go s.protocolLoop()

	// Message handler
	s.wg.Add(1)
	go s.receiveLoop()

	// Phi accrual check loop
	s.wg.Add(1)
	go s.phiCheckLoop()

	// GC loop
	s.wg.Add(1)
	go s.gcLoop()

	fmt.Printf("[%s] SWIM started\n", s.config.NodeID)
}

// Stop stops the SWIM protocol
func (s *SWIM) Stop() {
	s.cancel()
	s.wg.Wait()
	fmt.Printf("[%s] SWIM stopped\n", s.config.NodeID)
}

// ─────────────────────────────────────────────────────────────────────────────
// PROTOCOL LOOP — one probe per ProtocolPeriod
// ─────────────────────────────────────────────────────────────────────────────

func (s *SWIM) protocolLoop() {
	defer s.wg.Done()

	ticker := time.NewTicker(s.config.ProtocolPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.probe()
		}
	}
}

// probe picks a random alive member and pings it
func (s *SWIM) probe() {
	// Pick a random member (not ourselves)
	target := s.pickRandomMember()
	if target == nil {
		return // no other members
	}

	seq := s.nextSeq()

	// Record phi accrual: we're ABOUT to send ping
	// The response will update the heartbeat window

	fmt.Printf("[%s] SWIM PING → %s (seq=%d)\n",
		s.config.NodeID, target.NodeID, seq)

	// Create pending ping channel
	ackCh := make(chan bool, 1)
	s.mu.Lock()
	s.pendingPings[seq] = ackCh
	s.mu.Unlock()

	// Send direct ping with piggybacked gossip
	pingCtx, cancel := context.WithTimeout(s.ctx, s.config.PingTimeout)
	defer cancel()

	msg := SWIMMessage{
		Type:    MsgPing,
		From:    s.config.NodeID,
		To:      target.NodeID,
		SeqNum:  seq,
		Updates: s.memberList.RecentUpdates(s.config.GossipFanout),
	}

	err := s.transport.Send(pingCtx, target.NodeID, msg)

	if err != nil {
		// Couldn't even send — try indirect
		s.mu.Lock()
		delete(s.pendingPings, seq)
		s.mu.Unlock()
		s.indirectProbe(target.NodeID, seq)
		return
	}

	// Wait for direct ACK
	select {
	case <-ackCh:
		// Success! Update heartbeat window
		s.detector.Heartbeat(target.NodeID, time.Now())
		fmt.Printf("[%s] PING ACK ← %s (seq=%d) ✅\n",
			s.config.NodeID, target.NodeID, seq)

	case <-pingCtx.Done():
		// Timeout — try indirect pings
		s.mu.Lock()
		delete(s.pendingPings, seq)
		s.mu.Unlock()
		fmt.Printf("[%s] PING TIMEOUT → %s (seq=%d), trying indirect\n",
			s.config.NodeID, target.NodeID, seq)
		s.indirectProbe(target.NodeID, seq)
	}
}

// indirectProbe asks K other nodes to ping the target
// This distinguishes: target dead vs. us having a network issue to target
func (s *SWIM) indirectProbe(targetNodeID string, originalSeq uint64) {
	// Pick K random members (not us, not the target)
	helpers := s.pickKRandomMembers(s.config.IndirectPingCount,
		s.config.NodeID, targetNodeID)

	if len(helpers) == 0 {
		// No helpers available — suspect directly
		s.suspectNode(targetNodeID)
		return
	}

	fmt.Printf("[%s] Indirect PING for %s via %d helpers\n",
		s.config.NodeID, targetNodeID, len(helpers))

	// Channel to collect indirect acks
	ackCh := make(chan bool, len(helpers))

	indirectCtx, cancel := context.WithTimeout(s.ctx, s.config.IndirectTimeout)
	defer cancel()

	// Ask all helpers concurrently
	for _, helper := range helpers {
		go func(helperID string) {
			seq := s.nextSeq()

			pendingCh := make(chan bool, 1)
			s.mu.Lock()
			s.pendingPings[seq] = pendingCh
			s.mu.Unlock()

			msg := SWIMMessage{
				Type:    MsgPingReq,
				From:    s.config.NodeID,
				To:      helperID,
				Target:  targetNodeID,
				SeqNum:  seq,
				Updates: s.memberList.RecentUpdates(s.config.GossipFanout),
			}

			sendCtx, sendCancel := context.WithTimeout(indirectCtx, s.config.IndirectTimeout)
			defer sendCancel()

			if err := s.transport.Send(sendCtx, helperID, msg); err != nil {
				s.mu.Lock()
				delete(s.pendingPings, seq)
				s.mu.Unlock()
				ackCh <- false
				return
			}

			select {
			case acked := <-pendingCh:
				ackCh <- acked
			case <-indirectCtx.Done():
				s.mu.Lock()
				delete(s.pendingPings, seq)
				s.mu.Unlock()
				ackCh <- false
			}
		}(helper.NodeID)
	}

	// Wait for ANY positive response or all to fail
	for i := 0; i < len(helpers); i++ {
		select {
		case acked := <-ackCh:
			if acked {
				// At least one helper reached the target
				// Target is alive, we just had a routing issue
				s.detector.Heartbeat(targetNodeID, time.Now())
				fmt.Printf("[%s] Indirect PING SUCCESS for %s ✅\n",
					s.config.NodeID, targetNodeID)
				return
			}
		case <-indirectCtx.Done():
			break
		}
	}

	// All indirect pings failed — suspect the node
	fmt.Printf("[%s] Indirect PING FAILED for %s — suspecting\n",
		s.config.NodeID, targetNodeID)
	s.suspectNode(targetNodeID)
}

// suspectNode marks a node as suspected and starts the suspicion timer
func (s *SWIM) suspectNode(nodeID string) {
	s.memberList.SuspectNode(nodeID)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Start suspicion timer — if not refuted, declare dead
	if _, exists := s.suspicionTimers[nodeID]; exists {
		return // already suspicious
	}

	timer := time.AfterFunc(s.config.SuspicionTimeout, func() {
		s.mu.Lock()
		delete(s.suspicionTimers, nodeID)
		s.mu.Unlock()

		// Check if still suspected (might have been refuted)
		m, exists := s.memberList.Get(nodeID)
		if exists && m.State == StateSuspected {
			fmt.Printf("[%s] Suspicion timeout for %s — declaring DEAD\n",
				s.config.NodeID, nodeID)
			s.memberList.ConfirmDead(nodeID)
		}
	})

	s.suspicionTimers[nodeID] = timer
}

// ─────────────────────────────────────────────────────────────────────────────
// RECEIVE LOOP — handle incoming SWIM messages
// ─────────────────────────────────────────────────────────────────────────────

func (s *SWIM) receiveLoop() {
	defer s.wg.Done()

	recvCh := s.transport.Recv()

	for {
		select {
		case <-s.ctx.Done():
			return
		case msg := <-recvCh:
			s.handleMessage(msg)
		}
	}
}

func (s *SWIM) handleMessage(msg SWIMMessage) {
	// Apply any piggybacked gossip updates first
	for _, update := range msg.Updates {
		if update.NodeID == s.config.NodeID {
			// Someone is gossiping about US
			m, _ := s.memberList.Get(s.config.NodeID)
			if m != nil && m.State == StateSuspected {
				// We need to refute!
				refutation := s.memberList.RefuteSuspicion()
				// Broadcast refutation in next PING
				s.memberList.ApplyUpdate(refutation)
			}
		} else {
			s.memberList.ApplyUpdate(update)
		}
	}

	// Handle the message itself
	switch msg.Type {
	case MsgPing:
		s.handlePing(msg)

	case MsgPingAck:
		s.handlePingAck(msg)

	case MsgPingReq:
		s.handlePingReq(msg)

	case MsgPingReqAck:
		s.handlePingReqAck(msg)
	}
}

// handlePing responds to a direct ping
func (s *SWIM) handlePing(msg SWIMMessage) {
	// Update heartbeat for sender (they're alive!)
	s.detector.Heartbeat(msg.From, time.Now())

	// Send ACK with our own gossip updates
	ack := SWIMMessage{
		Type:    MsgPingAck,
		From:    s.config.NodeID,
		To:      msg.From,
		SeqNum:  msg.SeqNum,
		Updates: s.memberList.RecentUpdates(s.config.GossipFanout),
	}

	ctx, cancel := context.WithTimeout(s.ctx, 100*time.Millisecond)
	defer cancel()
	s.transport.Send(ctx, msg.From, ack)
}

// handlePingAck processes an ACK for one of our pings
func (s *SWIM) handlePingAck(msg SWIMMessage) {
	s.mu.Lock()
	ch, exists := s.pendingPings[msg.SeqNum]
	if exists {
		delete(s.pendingPings, msg.SeqNum)
	}
	s.mu.Unlock()

	if exists {
		select {
		case ch <- true:
		default:
		}
	}

	// Update heartbeat for the acking node
	s.detector.Heartbeat(msg.From, time.Now())
}

// handlePingReq acts as an intermediary: ping TARGET on behalf of REQUESTER
func (s *SWIM) handlePingReq(msg SWIMMessage) {
	target := msg.Target
	if target == "" {
		return
	}

	// Try to ping the target
	seq := s.nextSeq()
	ackCh := make(chan bool, 1)
	s.mu.Lock()
	s.pendingPings[seq] = ackCh
	s.mu.Unlock()

	pingMsg := SWIMMessage{
		Type:    MsgPing,
		From:    s.config.NodeID,
		To:      target,
		SeqNum:  seq,
		Updates: s.memberList.RecentUpdates(s.config.GossipFanout),
	}

	pingCtx, cancel := context.WithTimeout(s.ctx, s.config.IndirectTimeout/2)
	defer cancel()

	reached := false
	if err := s.transport.Send(pingCtx, target, pingMsg); err == nil {
		select {
		case acked := <-ackCh:
			reached = acked
		case <-pingCtx.Done():
		}
	}

	s.mu.Lock()
	delete(s.pendingPings, seq)
	s.mu.Unlock()

	// Report back to requester
	ackType := MsgPingReqAck
	replyMsg := SWIMMessage{
		Type:    ackType,
		From:    s.config.NodeID,
		To:      msg.From,
		Target:  target,
		SeqNum:  msg.SeqNum,
		Updates: s.memberList.RecentUpdates(s.config.GossipFanout),
	}

	if !reached {
		// Signal failure by using a "reject" approach
		// (in a real implementation, we'd have a separate message type)
		replyMsg.Updates = append(replyMsg.Updates, GossipUpdate{
			NodeID: target,
			State:  StateSuspected,
		})
	}

	replyCtx, replyCancel := context.WithTimeout(s.ctx, 100*time.Millisecond)
	defer replyCancel()
	s.transport.Send(replyCtx, msg.From, replyMsg)
}

// handlePingReqAck processes the result of an indirect ping
func (s *SWIM) handlePingReqAck(msg SWIMMessage) {
	s.mu.Lock()
	ch, exists := s.pendingPings[msg.SeqNum]
	if exists {
		delete(s.pendingPings, msg.SeqNum)
	}
	s.mu.Unlock()

	if exists {
		// Check if target was reached
		reached := true
		for _, update := range msg.Updates {
			if update.NodeID == msg.Target &&
				update.State == StateSuspected {
				reached = false
				break
			}
		}

		select {
		case ch <- reached:
		default:
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// BACKGROUND LOOPS
// ─────────────────────────────────────────────────────────────────────────────

func (s *SWIM) phiCheckLoop() {
	defer s.wg.Done()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			// Check all peers for suspicious phi values
			s.detector.Check()
		}
	}
}

func (s *SWIM) gcLoop() {
	defer s.wg.Done()

	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			removed := s.memberList.GC()
			if removed > 0 {
				fmt.Printf("[%s] SWIM GC: removed %d dead nodes\n",
					s.config.NodeID, removed)
			}
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────────────────────────────────────

func (s *SWIM) nextSeq() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seqNum++
	return s.seqNum
}

func (s *SWIM) pickRandomMember() *Member {
	alive := s.memberList.Alive()
	// Filter out ourselves
	others := make([]*Member, 0, len(alive))
	for _, m := range alive {
		if m.NodeID != s.config.NodeID {
			others = append(others, m)
		}
	}

	if len(others) == 0 {
		return nil
	}

	return others[s.rng.Intn(len(others))]
}

func (s *SWIM) pickKRandomMembers(k int, exclude ...string) []*Member {
	alive := s.memberList.Alive()

	excludeSet := make(map[string]bool)
	for _, e := range exclude {
		excludeSet[e] = true
	}

	candidates := make([]*Member, 0, len(alive))
	for _, m := range alive {
		if !excludeSet[m.NodeID] {
			candidates = append(candidates, m)
		}
	}

	// Shuffle and take first k
	s.rng.Shuffle(len(candidates), func(i, j int) {
		candidates[i], candidates[j] = candidates[j], candidates[i]
	})

	if k > len(candidates) {
		k = len(candidates)
	}
	return candidates[:k]
}

// JoinCluster adds seed nodes and sends Join requests
func (s *SWIM) JoinCluster(seedNodes []string) error {
	for _, seed := range seedNodes {
		// Add seed as a potential member
		s.memberList.ApplyUpdate(GossipUpdate{
			NodeID:  seed,
			State:   StateAlive,
			Address: seed,
		})

		// Send a ping to get their member list
		seq := s.nextSeq()
		ackCh := make(chan bool, 1)
		s.mu.Lock()
		s.pendingPings[seq] = ackCh
		s.mu.Unlock()

		msg := SWIMMessage{
			Type:    MsgPing,
			From:    s.config.NodeID,
			To:      seed,
			SeqNum:  seq,
			Updates: s.memberList.RecentUpdates(8),
		}

		ctx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
		if err := s.transport.Send(ctx, seed, msg); err != nil {
			cancel()
			s.mu.Lock()
			delete(s.pendingPings, seq)
			s.mu.Unlock()
			continue
		}

		select {
		case <-ackCh:
			fmt.Printf("[%s] Successfully joined via %s\n", s.config.NodeID, seed)
			cancel()
			return nil
		case <-ctx.Done():
			cancel()
		}

		s.mu.Lock()
		delete(s.pendingPings, seq)
		s.mu.Unlock()
	}

	return fmt.Errorf("swim: failed to join cluster via any seed node")
}
