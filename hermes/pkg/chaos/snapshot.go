// pkg/chaos/snapshot.go
package chaos

// ChandyLamportSnapshot implements the Chandy-Lamport global snapshot algorithm
//
// Problem: Take a consistent snapshot of a distributed system
// (state that could actually have occurred at some moment)
//
// Algorithm:
//   1. Initiator saves local state, sends MARKER on all channels
//   2. Non-initiator receiving MARKER for first time:
//      - Save local state
//      - Record empty set for this channel
//      - Send MARKER on all other channels
//   3. Non-initiator receiving MARKER on channel C after recording state:
//      - Record all messages received on C since state recording
//   4. Snapshot complete when all channels have received MARKER back

import (
	"fmt"
	"sync"
	"time"
)

// SnapshotState is the state of one node at snapshot time
type SnapshotState struct {
	NodeID     string
	State      map[string]interface{} // application state
	CapturedAt time.Time
}

// ChannelState captures messages in-flight during snapshot
type ChannelState struct {
	From     string
	To       string
	Messages []interface{}
}

// GlobalSnapshot is the complete distributed snapshot
type GlobalSnapshot struct {
	SnapshotID  string
	Initiator   string
	StartedAt   time.Time
	CompletedAt time.Time

	// State of each node at snapshot time
	NodeStates map[string]*SnapshotState

	// Messages in-flight between nodes
	ChannelStates map[string]*ChannelState // "from→to" → messages
}

// SnapshotCoordinator manages the Chandy-Lamport algorithm
type SnapshotCoordinator struct {
	mu     sync.Mutex
	nodeID string
	nodes  []string

	// Current snapshot in progress
	currentSnapshot *GlobalSnapshot

	// Whether we've recorded our state for this snapshot
	stateRecorded bool

	// Channels for which we've received MARKER
	markerReceived map[string]bool // "from" → received

	// Messages recorded after our state was recorded
	recordingFor map[string][]interface{} // "from" → messages

	// Callbacks
	onStateRequest func() map[string]interface{}
	onComplete     func(snapshot *GlobalSnapshot)
	sendMarker     func(to string, snapshotID string)
}

// NewSnapshotCoordinator creates a new coordinator
func NewSnapshotCoordinator(
	nodeID string,
	nodes []string,
	onStateRequest func() map[string]interface{},
	sendMarker func(to string, snapshotID string),
	onComplete func(snapshot *GlobalSnapshot),
) *SnapshotCoordinator {
	return &SnapshotCoordinator{
		nodeID:         nodeID,
		nodes:          nodes,
		onStateRequest: onStateRequest,
		sendMarker:     sendMarker,
		onComplete:     onComplete,
	}
}

// InitiateSnapshot starts a global snapshot (called on one node)
func (c *SnapshotCoordinator) InitiateSnapshot(snapshotID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	fmt.Printf("[%s] Initiating snapshot %s\n", c.nodeID, snapshotID)

	c.currentSnapshot = &GlobalSnapshot{
		SnapshotID:    snapshotID,
		Initiator:     c.nodeID,
		StartedAt:     time.Now(),
		NodeStates:    make(map[string]*SnapshotState),
		ChannelStates: make(map[string]*ChannelState),
	}

	c.markerReceived = make(map[string]bool)
	c.recordingFor = make(map[string][]interface{})

	// Record our own state
	c.recordLocalState()

	// Send MARKER on all outgoing channels
	for _, node := range c.nodes {
		if node != c.nodeID {
			fmt.Printf("[%s] Sending MARKER to %s\n", c.nodeID, node)
			c.sendMarker(node, snapshotID)
		}
	}
}

// ReceiveMarker handles a MARKER message from another node
func (c *SnapshotCoordinator) ReceiveMarker(from string, snapshotID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.currentSnapshot == nil || c.currentSnapshot.SnapshotID != snapshotID {
		// We haven't started this snapshot yet — start now
		c.currentSnapshot = &GlobalSnapshot{
			SnapshotID:    snapshotID,
			Initiator:     from,
			StartedAt:     time.Now(),
			NodeStates:    make(map[string]*SnapshotState),
			ChannelStates: make(map[string]*ChannelState),
		}
		c.markerReceived = make(map[string]bool)
		c.recordingFor = make(map[string][]interface{})

		// Record our state (first marker)
		c.recordLocalState()

		// Propagate MARKER to all other nodes
		for _, node := range c.nodes {
			if node != c.nodeID && node != from {
				c.sendMarker(node, snapshotID)
			}
		}
	}

	// Record this channel's state
	c.markerReceived[from] = true
	channelKey := fmt.Sprintf("%s→%s", from, c.nodeID)
	c.currentSnapshot.ChannelStates[channelKey] = &ChannelState{
		From:     from,
		To:       c.nodeID,
		Messages: c.recordingFor[from],
	}

	fmt.Printf("[%s] Received MARKER from %s (channel state recorded: %d msgs)\n",
		c.nodeID, from, len(c.recordingFor[from]))

	// Check if snapshot is complete
	c.checkCompletion()
}

// RecordMessage records a message received during snapshot
// Call this for every message received while snapshot is in progress
func (c *SnapshotCoordinator) RecordMessage(from string, msg interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.currentSnapshot == nil {
		return
	}

	if c.stateRecorded && !c.markerReceived[from] {
		// We've recorded our state but not received MARKER from this channel yet
		// Record this message as in-transit
		c.recordingFor[from] = append(c.recordingFor[from], msg)
	}
}

// recordLocalState saves the current node state
func (c *SnapshotCoordinator) recordLocalState() {
	state := c.onStateRequest()

	c.currentSnapshot.NodeStates[c.nodeID] = &SnapshotState{
		NodeID:     c.nodeID,
		State:      state,
		CapturedAt: time.Now(),
	}

	c.stateRecorded = true
	fmt.Printf("[%s] Local state recorded: %d keys\n", c.nodeID, len(state))
}

// checkCompletion checks if we've received MARKERs from all channels
func (c *SnapshotCoordinator) checkCompletion() {
	// Check if we've received MARKERs from all other nodes
	for _, node := range c.nodes {
		if node != c.nodeID && !c.markerReceived[node] {
			return // still waiting
		}
	}

	// Snapshot complete!
	c.currentSnapshot.CompletedAt = time.Now()

	fmt.Printf("[%s] Snapshot %s COMPLETE (%d node states, %d channel states)\n",
		c.nodeID,
		c.currentSnapshot.SnapshotID,
		len(c.currentSnapshot.NodeStates),
		len(c.currentSnapshot.ChannelStates))

	if c.onComplete != nil {
		go c.onComplete(c.currentSnapshot)
	}

	// Reset for next snapshot
	c.currentSnapshot = nil
	c.stateRecorded = false
	c.markerReceived = nil
	c.recordingFor = nil
}
