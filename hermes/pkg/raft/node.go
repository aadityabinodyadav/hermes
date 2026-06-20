package raft

// RaftNode is the goroutine-safe wrapper around the Raft state machine
//
// Architecture:
//
//   External world                RaftNode                  Raft SM
//   ──────────────                ────────                  ───────
//   network recv    ──Step()──▶   recvCh
//   tick timer      ──Tick()──▶   tickCh        ──Step()──▶ state machine
//   client propose  ──Propose()─▶ proposeCh     ──Tick()──▶
//                                 run()          ◀──Ready()──
//                   ◀──Ready()──  readyCh
//                                    │
//                           caller processes Ready:
//                           1. Write entries to WAL
//                           2. Send messages to peers
//                           3. Apply entries to storage engine

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aadityabinodyadav/hermes/pkg/clock"
)

type StateMachine interface {
	Apply(entry LogEntry) error

	Snapshot() ([]byte, error)

	Restore(data []byte) error
}

type RaftNode struct {
	mu     sync.Mutex
	raft   *Raft
	config Config

	tickCh    chan struct{}
	proposeCh chan proposeRequest
	stepCh    chan Message
	readyCh   chan Ready
	stopCh    chan struct{}

	storage StateMachine

	wal []LogEntry

	hlc *clock.HLC

	transport Transport

	hardState HardState

	appliedIndex uint64

	leaderChangeCh chan string

	done chan struct{}
}

type proposeRequest struct {
	entries []LogEntry
	result  chan error
}

type Transport interface {
	Send(msgs []Message)
	AddPeer(id, addr string)
	RemovePeer(id string)
}

func NewRaftNode(
	config Config,
	sm StateMachine,
	hlc *clock.HLC,
	transport Transport,
) *RaftNode {
	n := &RaftNode{
		raft:           NewRaft(config),
		config:         config,
		tickCh:         make(chan struct{}, 128),
		proposeCh:      make(chan proposeRequest, 256),
		stepCh:         make(chan Message, 256),
		readyCh:        make(chan Ready, 1),
		stopCh:         make(chan struct{}),
		storage:        sm,
		hlc:            hlc,
		transport:      transport,
		leaderChangeCh: make(chan string, 1),
		done:           make(chan struct{}),
	}
	return n
}

func (n *RaftNode) Start() {
	go n.run()
	go n.ticker()
}

func (n *RaftNode) ticker() {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			select {
			case n.tickCh <- struct{}{}:
			default:
			}
		case <-n.stopCh:
			return
		}
	}
}

func (n *RaftNode) run() {
	defer close(n.done)

	var prevLead string

	for {
		ready := n.raft.TakeReady()
		if !ready.IsEmpty() {
			n.processReady(ready)

			if n.raft.lead != prevLead {
				prevLead = n.raft.lead
				select {
				case n.leaderChangeCh <- n.raft.lead:
				default:
				}
			}
		}

		select {
		case <-n.tickCh:
			n.raft.Tick()

		case prop := <-n.proposeCh:
			err := n.raft.Step(Message{
				Type:    MsgPropose,
				From:    n.config.NodeID,
				Entries: prop.entries,
			})
			if err == nil {
				ready := n.raft.TakeReady()
				if !ready.IsEmpty() {
					n.processReady(ready)

					if n.raft.lead != prevLead {
						prevLead = n.raft.lead
						select {
						case n.leaderChangeCh <- n.raft.lead:
						default:
						}
					}
				}
			}
			prop.result <- err

		case msg := <-n.stepCh:
			n.raft.Step(msg)

		case <-n.stopCh:
			fmt.Printf("[%s] RaftNode stopping\n", n.config.NodeID)
			return
		}
	}
}

func (n *RaftNode) processReady(ready Ready) {
	if len(ready.Entries) > 0 {
		n.persistEntries(ready.Entries)
	}

	if ready.HardState != nil {
		n.persistHardState(*ready.HardState)
	}

	if ready.Snapshot != nil {
		n.saveSnapshot(ready.Snapshot)
	}

	if len(ready.Messages) > 0 {
		n.transport.Send(ready.Messages)
	}

	if len(ready.CommittedEntries) > 0 {
		n.applyEntries(ready.CommittedEntries)
	}
}

func (n *RaftNode) persistEntries(entries []LogEntry) {
	n.wal = append(n.wal, entries...)
}

func (n *RaftNode) persistHardState(hs HardState) {
	n.hardState = hs
}

func (n *RaftNode) saveSnapshot(snap *Snapshot) {
	if n.storage != nil {
		n.storage.Restore(snap.Data)
	}
}

func (n *RaftNode) applyEntries(entries []LogEntry) {
	for _, entry := range entries {
		if entry.Type == EntryNoop {
			n.appliedIndex = entry.Index
			continue
		}

		if n.storage != nil {
			if err := n.storage.Apply(entry); err != nil {
				fmt.Printf("[%s] FATAL: failed to apply entry %d: %v\n",
					n.config.NodeID, entry.Index, err)
			}
		}

		n.appliedIndex = entry.Index
	}

	n.raft.log.AppliedTo(n.appliedIndex)
}

func (n *RaftNode) Propose(ctx context.Context, data []byte) error {
	result := make(chan error, 1)

	select {
	case n.proposeCh <- proposeRequest{
		entries: []LogEntry{{Type: EntryNormal, Data: data}},
		result:  result,
	}:
	case <-ctx.Done():
		return ctx.Err()
	case <-n.stopCh:
		return fmt.Errorf("raft node stopped")
	}

	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (n *RaftNode) Step(ctx context.Context, msg Message) error {
	select {
	case n.stepCh <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-n.stopCh:
		return fmt.Errorf("raft node stopped")
	}
}

func (n *RaftNode) IsLeader() bool {
	return n.raft.state.Load() == Leader
}

func (n *RaftNode) Leader() string {
	return n.raft.lead
}

func (n *RaftNode) State() NodeState {
	return n.raft.state
}

func (n *RaftNode) Status() Status {
	return Status{
		NodeID:       n.config.NodeID,
		State:        n.raft.state,
		Term:         n.raft.term,
		Lead:         n.raft.lead,
		CommitIndex:  n.raft.log.Stats().CommitIndex,
		AppliedIndex: n.appliedIndex,
		LastIndex:    n.raft.log.LastIndex(),
		Progress:     n.raft.progress,
	}
}

func (n *RaftNode) LeaderChanges() <-chan string {
	return n.leaderChangeCh
}

// Tick sends a tick signal to the Raft state machine.
// This is used by external callers (e.g. ReadIndexTracker) to trigger
// heartbeats or election timeouts directly.
func (n *RaftNode) Tick() {
	select {
	case n.tickCh <- struct{}{}:
	default:
	}
}

func (n *RaftNode) Stop() {
	select {
	case <-n.stopCh:
	default:
		close(n.stopCh)
	}
	<-n.done
}

type Status struct {
	NodeID       string
	State        NodeState
	Term         uint64
	Lead         string
	CommitIndex  uint64
	AppliedIndex uint64
	LastIndex    uint64
	Progress     map[string]*Progress
}

func (s Status) String() string {
	return fmt.Sprintf(
		"[%s] state=%s term=%d lead=%s commit=%d applied=%d lastIndex=%d",
		s.NodeID, s.State, s.Term, s.Lead,
		s.CommitIndex, s.AppliedIndex, s.LastIndex,
	)
}
