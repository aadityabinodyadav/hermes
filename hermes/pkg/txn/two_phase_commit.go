// pkg/txn/two_phase_commit.go
package txn

// TwoPhaseCommitCoordinator implements the 2PC protocol
//
// This is the CLASSIC approach to distributed transactions.
// It's simple, well-understood, but has the blocking problem.
//
// USE CASES:
//   - Small number of participants (2-5 shards)
//   - Low contention environments
//   - When simplicity is more important than availability
//
// NOT SUITABLE FOR:
//   - High-throughput systems (coordinator bottleneck)
//   - Geo-distributed (latency amplification)
//   - When coordinator failure tolerance is critical

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aadityabinodyadav/hermes/pkg/clock"
)

// PrepareVote, VoteCommit, VoteAbort, and Participant are defined in twopc.go

// TwoPhaseCommitCoordinator manages 2PC transactions
type TwoPhaseCommitCoordinator struct {
	mu sync.Mutex

	// transactions maps txnID → transaction state
	transactions map[TxnID]*TwoPhaseCommitState

	// participants is the set of all possible participants
	participants map[uint64]*Participant

	// prepareTimeout is how long to wait for prepare responses
	prepareTimeout time.Duration

	// commitTimeout is how long to wait for commit ACKs
	commitTimeout time.Duration

	// Clock for timestamps
	hlc *clock.HLC
}

// TwoPhaseCommitState tracks the state of one 2PC transaction
type TwoPhaseCommitState struct {
	Txn       *Transaction
	Votes     map[string]Vote // participant → vote
	Prepared  bool                   // all votes received
	Committed bool                   // commit decision made
	Decision  Vote                   // final commit/abort decision
}

// NewTwoPhaseCommitCoordinator creates a new 2PC coordinator
func NewTwoPhaseCommitCoordinator(hlc *clock.HLC) *TwoPhaseCommitCoordinator {
	return &TwoPhaseCommitCoordinator{
		transactions:   make(map[TxnID]*TwoPhaseCommitState),
		participants:   make(map[uint64]*Participant),
		prepareTimeout: 5 * time.Second,
		commitTimeout:  10 * time.Second,
		hlc:            hlc,
	}
}

// AddParticipant registers a shard as a 2PC participant
func (c *TwoPhaseCommitCoordinator) AddParticipant(p *Participant) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.participants[p.ShardID] = p
}

// Begin starts a new 2PC transaction
func (c *TwoPhaseCommitCoordinator) Begin(
	ctx context.Context,
	isolation IsolationLevel,
	participants []uint64,
) (*Transaction, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Generate transaction ID
	txnID := c.generateTxnID()

	// Get participant details
	participantList := make([]string, 0, len(participants))
	for _, shardID := range participants {
		if p, exists := c.participants[shardID]; exists {
			participantList = append(participantList, p.NodeID)
		}
	}

	if len(participantList) == 0 {
		return nil, fmt.Errorf("txn: no valid participants")
	}

	txn := &Transaction{
		ID:             txnID,
		State:          TxnActive,
		IsolationLevel: isolation,
		ReadTimestamp:  c.hlc.Now(),
		StartTime:      time.Now(),
		Coordinator:    c.hlc.PhysicalNow().String(), // simplified
		Participants:   participantList,
		WriteSet:       make(map[string]*WriteIntent),
		ReadSet:        make(map[string]clock.HLCTimestamp),
		Locks:          make(map[string]*Lock),
	}

	state := &TwoPhaseCommitState{
		Txn:      txn,
		Votes:    make(map[string]Vote),
		Prepared: false,
	}

	c.transactions[txnID] = state

	return txn, nil
}

// Prepare sends prepare requests to all participants
// Returns true if ALL participants voted to commit
func (c *TwoPhaseCommitCoordinator) Prepare(
	ctx context.Context,
	txnID TxnID,
) (bool, error) {
	c.mu.Lock()
	state, exists := c.transactions[txnID]
	if !exists {
		c.mu.Unlock()
		return false, fmt.Errorf("txn: unknown transaction %s", txnID)
	}
	state.Txn.State = TxnPreparing
	c.mu.Unlock()

	txn := state.Txn

	// Send prepare to all participants
	type voteResult struct {
		participant string
		vote        Vote
		err         error
	}

	voteCh := make(chan voteResult, len(txn.Participants))
	prepareCtx, cancel := context.WithTimeout(ctx, c.prepareTimeout)
	defer cancel()

	for _, participant := range txn.Participants {
		go func(nodeID string) {
			vote, err := c.sendPrepare(prepareCtx, nodeID, txn)
			voteCh <- voteResult{
				participant: nodeID,
				vote:        vote,
				err:         err,
			}
		}(participant)
	}

	// Collect all votes
	allCommit := true
	for i := 0; i < len(txn.Participants); i++ {
		result := <-voteCh

		if result.err != nil {
			fmt.Printf("[%s] 2PC: participant %s prepare error: %v\n",
				txnID, result.participant, result.err)
			allCommit = false
			continue
		}

		state.Votes[result.participant] = result.vote
		if result.vote == VoteAbort {
			allCommit = false
		}
	}

	c.mu.Lock()
	state.Prepared = true
	if allCommit {
		state.Decision = VoteCommit
	} else {
		state.Decision = VoteAbort
	}
	c.mu.Unlock()

	return allCommit, nil
}

// Commit sends commit decision to all participants
func (c *TwoPhaseCommitCoordinator) Commit(
	ctx context.Context,
	txnID TxnID,
) error {
	c.mu.Lock()
	state, exists := c.transactions[txnID]
	if !exists {
		c.mu.Unlock()
		return fmt.Errorf("txn: unknown transaction %s", txnID)
	}

	if !state.Prepared {
		c.mu.Unlock()
		return fmt.Errorf("txn: cannot commit before prepare")
	}

	state.Txn.State = TxnCommitting
	c.mu.Unlock()

	// Send commit to all participants
	commitCtx, cancel := context.WithTimeout(ctx, c.commitTimeout)
	defer cancel()

	for _, participant := range state.Txn.Participants {
		go func(nodeID string) {
			err := c.sendCommit(commitCtx, nodeID, txnID, true)
			if err != nil {
				fmt.Printf("[%s] 2PC: commit to %s failed: %v\n",
					txnID, nodeID, err)
				// In production: retry, or mark for recovery
			}
		}(participant)
	}

	c.mu.Lock()
	state.Committed = true
	state.Txn.State = TxnCommitted
	state.Txn.CommitTimestamp = c.hlc.Now()
	c.mu.Unlock()

	return nil
}

// Abort sends abort decision to all participants
func (c *TwoPhaseCommitCoordinator) Abort(
	ctx context.Context,
	txnID TxnID,
) error {
	c.mu.Lock()
	state, exists := c.transactions[txnID]
	if !exists {
		c.mu.Unlock()
		return fmt.Errorf("txn: unknown transaction %s", txnID)
	}

	state.Txn.State = TxnAborted
	c.mu.Unlock()

	// Send abort to all participants
	for _, participant := range state.Txn.Participants {
		c.sendAbort(ctx, participant, txnID)
	}

	c.mu.Lock()
	state.Txn.State = TxnAborted
	c.mu.Unlock()

	return nil
}

// sendPrepare sends a prepare request to a participant
// In production: this is a gRPC call to the participant shard
func (c *TwoPhaseCommitCoordinator) sendPrepare(
	ctx context.Context,
	nodeID string,
	txn *Transaction,
) (Vote, error) {
	// SIMULATED: In production, this is:
	// client, _ := c.getParticipantClient(nodeID)
	// resp, err := client.Prepare(ctx, &PrepareRequest{...})
	//
	// For demo, we simulate success
	fmt.Printf("[%s] 2PC PREPARE → %s\n", txn.ID, nodeID)

	// Simulate network latency
	time.Sleep(10 * time.Millisecond)

	// Simulate participant checking for conflicts
	// In production: participant checks locks, writes prepare record to WAL
	hasConflict := false // simplified

	if hasConflict {
		return VoteAbort, nil
	}

	return VoteCommit, nil
}

// sendCommit sends a commit decision to a participant
func (c *TwoPhaseCommitCoordinator) sendCommit(
	ctx context.Context,
	nodeID string,
	txnID TxnID,
	commit bool,
) error {
	fmt.Printf("[%s] 2PC COMMIT → %s\n", txnID, nodeID)

	// Simulate network latency
	time.Sleep(10 * time.Millisecond)

	// In production: participant applies changes, releases locks,
	// writes commit record to WAL

	return nil
}

// sendAbort sends an abort decision to a participant
func (c *TwoPhaseCommitCoordinator) sendAbort(
	ctx context.Context,
	nodeID string,
	txnID TxnID,
) error {
	fmt.Printf("[%s] 2PC ABORT → %s\n", txnID, nodeID)

	// In production: participant releases locks, discards changes

	return nil
}

// generateTxnID creates a unique transaction ID
func (c *TwoPhaseCommitCoordinator) generateTxnID() TxnID {
	ts := c.hlc.Now()
	return TxnID(fmt.Sprintf("txn-%d-%d", ts, time.Now().UnixNano()))
}

// GetState returns the state of a transaction
func (c *TwoPhaseCommitCoordinator) GetState(txnID TxnID) (*TwoPhaseCommitState, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	state, exists := c.transactions[txnID]
	return state, exists
}

// Cleanup removes completed transactions from memory
func (c *TwoPhaseCommitCoordinator) Cleanup() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for txnID, state := range c.transactions {
		if state.Committed || state.Txn.State == TxnAborted {
			// Keep for a while for recovery, then remove
			if time.Since(state.Txn.StartTime) > 5*time.Minute {
				delete(c.transactions, txnID)
			}
		}
	}
}
