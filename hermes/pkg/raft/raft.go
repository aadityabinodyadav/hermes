package raft

import (
	"fmt"
	"math/rand"
	"time"
)

type Config struct {
	NodeID string

	Peers []string

	HeartbeatTick int

	ElectionTick int

	MaxEntriesToSend int

	MaxInflight int
}

func DefaultConfig(nodeID string, peers []string) Config {
	return Config{
		NodeID:           nodeID,
		Peers:            peers,
		HeartbeatTick:    5,
		ElectionTick:     10,
		MaxEntriesToSend: 100,
		MaxInflight:      16,
	}
}

type Message struct {
	Type MsgType
	To   string
	From string
	Term uint64

	Index       uint64
	LogTerm     uint64
	LogIndex    uint64
	Entries     []LogEntry
	CommitIndex uint64

	Success       bool
	ConflictIndex uint64
	ConflictTerm  uint64

	LastLogIndex uint64
	LastLogTerm  uint64

	VoteGranted bool

	PreVote bool

	Snapshot *Snapshot

	Reject     bool
	RejectHint uint64
}

type MsgType uint8

const (
	MsgHeartbeat    MsgType = 0
	MsgHeartbeatRsp MsgType = 1

	MsgAppend    MsgType = 2
	MsgAppendRsp MsgType = 3

	MsgVote    MsgType = 4
	MsgVoteRsp MsgType = 5

	MsgPreVote    MsgType = 6
	MsgPreVoteRsp MsgType = 7

	MsgPropose MsgType = 8

	MsgSnapshot    MsgType = 9
	MsgSnapshotRsp MsgType = 10

	MsgTimeoutNow MsgType = 11

	MsgReadIndex    MsgType = 12
	MsgReadIndexRsp MsgType = 13

	MsgTick MsgType = 14

	MsgUnreachable MsgType = 15
)

func (m MsgType) String() string {
	names := map[MsgType]string{
		MsgHeartbeat: "Heartbeat", MsgHeartbeatRsp: "HeartbeatRsp",
		MsgAppend: "Append", MsgAppendRsp: "AppendRsp",
		MsgVote: "Vote", MsgVoteRsp: "VoteRsp",
		MsgPreVote: "PreVote", MsgPreVoteRsp: "PreVoteRsp",
		MsgPropose: "Propose", MsgSnapshot: "Snapshot",
		MsgTimeoutNow: "TimeoutNow", MsgReadIndex: "ReadIndex",
		MsgUnreachable: "Unreachable",
	}
	if name, ok := names[m]; ok {
		return name
	}
	return fmt.Sprintf("MsgType(%d)", m)
}

type Snapshot struct {
	Index uint64
	Term  uint64
	Data  []byte
}

type Ready struct {
	Entries []LogEntry

	Messages []Message

	CommittedEntries []LogEntry

	Snapshot *Snapshot

	SoftState *SoftState

	HardState *HardState

	ReadStates []ReadState
}

type HardState struct {
	Term   uint64
	Vote   string
	Commit uint64
}

type SoftState struct {
	Lead      string
	RaftState NodeState
}

type ReadState struct {
	Index      uint64
	RequestCtx []byte
}

func (r *Ready) IsEmpty() bool {
	return len(r.Entries) == 0 &&
		len(r.Messages) == 0 &&
		len(r.CommittedEntries) == 0 &&
		r.Snapshot == nil &&
		r.SoftState == nil &&
		r.HardState == nil &&
		len(r.ReadStates) == 0
}

type Raft struct {
	config Config
	id     string

	term uint64
	vote string

	log *RaftLog

	state NodeState

	lead string

	electionElapsed  int
	electionTimeout  int
	heartbeatElapsed int

	progress map[string]*Progress

	votes map[string]bool

	msgs []Message

	readOnly *readOnly

	rng *rand.Rand

	tick func()

	step func(m Message) error
}

func NewRaft(config Config) *Raft {
	r := &Raft{
		config:   config,
		id:       config.NodeID,
		log:      newRaftLog(),
		progress: make(map[string]*Progress),
		votes:    make(map[string]bool),
		readOnly: newReadOnly(),
		rng:      rand.New(rand.NewSource(time.Now().UnixNano())),
	}

	r.becomeFollower(0, "")
	return r
}

func (r *Raft) becomeFollower(term uint64, lead string) {
	r.step = r.stepFollower
	r.tick = r.tickElection

	if term > r.term {
		r.term = term
		r.vote = ""
	}

	r.state = Follower
	r.lead = lead
	r.resetElectionTimeout()

	if lead != "" {
		fmt.Printf("[%s] became FOLLOWER (term=%d, leader=%s)\n",
			r.id, r.term, lead)
	} else {
		fmt.Printf("[%s] became FOLLOWER (term=%d, no leader)\n",
			r.id, r.term)
	}
}

func (r *Raft) becomeCandidate() {
	if r.state == Leader {
		panic("raft: leader cannot become candidate")
	}

	r.step = r.stepCandidate
	r.tick = r.tickElection

	r.term++
	r.vote = r.id
	r.state = Candidate
	r.lead = ""
	r.votes = make(map[string]bool)
	r.votes[r.id] = true

	r.resetElectionTimeout()

	fmt.Printf("[%s] became CANDIDATE (term=%d)\n", r.id, r.term)
}

func (r *Raft) becomeLeader() {
	if r.state != Candidate {
		panic("raft: only candidate can become leader")
	}

	r.step = r.stepLeader
	r.tick = r.tickHeartbeat

	r.state = Leader
	r.lead = r.id
	r.heartbeatElapsed = 0

	lastIndex := r.log.LastIndex()
	r.progress[r.id] = &Progress{
		NextIndex:    lastIndex + 1,
		MatchIndex:   lastIndex,
		State:        ProgressReplicate,
		RecentActive: true,
	}
	for _, peer := range r.config.Peers {
		r.progress[peer] = &Progress{
			NextIndex:  lastIndex + 1,
			MatchIndex: 0,
			State:      ProgressProbe,
		}
	}

	fmt.Printf("[%s] ✅ became LEADER (term=%d, lastIndex=%d)\n",
		r.id, r.term, lastIndex)

	r.appendEntry(LogEntry{Type: EntryNoop})

	r.broadcastAppendEntries(true)
}

func (r *Raft) Tick() {
	r.tick()
}

func (r *Raft) tickElection() {
	r.electionElapsed++

	if r.electionElapsed >= r.electionTimeout {
		r.electionElapsed = 0
		r.campaign()
	}
}

func (r *Raft) tickHeartbeat() {
	r.heartbeatElapsed++

	if r.heartbeatElapsed >= r.config.HeartbeatTick {
		r.heartbeatElapsed = 0

		r.broadcastAppendEntries(true)
	}
}

func (r *Raft) resetElectionTimeout() {
	r.electionElapsed = 0
	r.electionTimeout = r.config.ElectionTick +
		r.rng.Intn(r.config.ElectionTick)
}

func (r *Raft) Step(m Message) error {

	switch {
	case m.Term == 0:

	case m.Term > r.term:

		if m.Type == MsgVote || m.Type == MsgPreVote {

		} else {

			lead := m.From
			if m.Type == MsgVoteRsp || m.Type == MsgPreVoteRsp {
				lead = ""
			}
			r.becomeFollower(m.Term, lead)
		}

	case m.Term < r.term:

		fmt.Printf("[%s] ignoring stale message %s from %s (msg_term=%d, our_term=%d)\n",
			r.id, m.Type, m.From, m.Term, r.term)
		return nil
	}

	return r.step(m)
}

func (r *Raft) stepFollower(m Message) error {
	switch m.Type {
	case MsgHeartbeat:

		r.electionElapsed = 0
		r.lead = m.From
		r.handleHeartbeat(m)

	case MsgAppend:

		r.electionElapsed = 0
		r.lead = m.From
		r.handleAppendEntries(m)

	case MsgSnapshot:

		r.electionElapsed = 0
		r.lead = m.From
		r.handleInstallSnapshot(m)

	case MsgVote, MsgPreVote:
		r.handleVoteRequest(m)

	case MsgTimeoutNow:

		fmt.Printf("[%s] received TimeoutNow — starting election\n", r.id)
		r.campaign()

	case MsgPropose:

		if r.lead == "" {
			return fmt.Errorf("raft: no leader known, cannot propose")
		}

		m.To = r.lead
		r.send(m)
	}
	return nil
}

func (r *Raft) stepCandidate(m Message) error {
	switch m.Type {
	case MsgVoteRsp, MsgPreVoteRsp:
		r.handleVoteResponse(m)

	case MsgAppend, MsgHeartbeat:

		r.becomeFollower(m.Term, m.From)
		if m.Type == MsgAppend {
			r.handleAppendEntries(m)
		} else {
			r.handleHeartbeat(m)
		}

	case MsgPropose:
		return fmt.Errorf("raft: cannot propose during election")

	case MsgVote:

		r.handleVoteRequest(m)
	}
	return nil
}

func (r *Raft) stepLeader(m Message) error {
	switch m.Type {
	case MsgPropose:

		if len(m.Entries) == 0 {
			return fmt.Errorf("raft: empty proposal")
		}
		r.handleProposal(m)

	case MsgAppendRsp:

		r.handleAppendResponse(m)

	case MsgHeartbeatRsp:

		prog := r.progress[m.From]
		if prog != nil {
			prog.RecentActive = true

			if prog.MatchIndex < r.log.LastIndex() {
				r.sendAppendEntries(m.From)
			}
		}

	case MsgVote, MsgPreVote:

		r.handleVoteRequest(m)

	case MsgUnreachable:

		prog := r.progress[m.From]
		if prog != nil && prog.State == ProgressReplicate {
			prog.BecomeProbe()
		}

	case MsgReadIndex:

		r.handleReadIndex(m)
	}
	return nil
}

func (r *Raft) handleProposal(m Message) {
	for i := range m.Entries {

		m.Entries[i].Index = r.log.LastIndex() + 1 + uint64(i)
		m.Entries[i].Term = r.term
	}

	for _, entry := range m.Entries {
		r.log.AppendOne(entry)
	}

	if prog, ok := r.progress[r.id]; ok {
		prog.MaybeUpdate(r.log.LastIndex())
	}

	r.broadcastAppendEntries(false)

	r.maybeCommit()
}

func (r *Raft) handleAppendEntries(m Message) {

	if m.LogIndex < r.log.Stats().CommitIndex {

		r.send(Message{
			Type:  MsgAppendRsp,
			To:    m.From,
			Term:  r.term,
			Index: r.log.Stats().CommitIndex,
		})
		return
	}

	if !r.logConsistencyCheck(m.LogIndex, m.LogTerm) {

		conflictIndex, conflictTerm := r.findConflict(m.LogIndex)
		r.send(Message{
			Type:          MsgAppendRsp,
			To:            m.From,
			Term:          r.term,
			Reject:        true,
			ConflictIndex: conflictIndex,
			ConflictTerm:  conflictTerm,
		})
		return
	}

	if len(m.Entries) > 0 {
		r.log.Append(m.Entries...)
	}

	if m.CommitIndex > r.log.Stats().CommitIndex {
		newCommit := m.CommitIndex
		if lastIdx := r.log.LastIndex(); lastIdx < newCommit {
			newCommit = lastIdx
		}
		r.log.CommitTo(newCommit)
	}

	r.send(Message{
		Type:  MsgAppendRsp,
		To:    m.From,
		Term:  r.term,
		Index: r.log.LastIndex(),
	})
}

func (r *Raft) handleAppendResponse(m Message) {
	prog := r.progress[m.From]
	if prog == nil {
		return
	}

	prog.RecentActive = true

	if m.Reject {

		fmt.Printf("[%s] AppendEntries rejected by %s (conflictIndex=%d, conflictTerm=%d)\n",
			r.id, m.From, m.ConflictIndex, m.ConflictTerm)

		if prog.MaybeDecrTo(m.Index, m.ConflictIndex) {
			r.sendAppendEntries(m.From)
		}
		return
	}

	if prog.MaybeUpdate(m.Index) {

		if prog.State != ProgressReplicate {
			prog.BecomeReplicate()
		}

		r.maybeCommit()

		if prog.MatchIndex < r.log.LastIndex() {
			r.sendAppendEntries(m.From)
		}
	}
}

func (r *Raft) handleHeartbeat(m Message) {

	if m.CommitIndex > r.log.Stats().CommitIndex {
		r.log.CommitTo(m.CommitIndex)
	}

	r.send(Message{
		Type:        MsgHeartbeatRsp,
		To:          m.From,
		Term:        r.term,
		CommitIndex: r.log.Stats().CommitIndex,
	})
}

func (r *Raft) handleVoteRequest(m Message) {

	canVote := (r.vote == "" && r.lead == "") ||
		r.vote == m.From ||
		(m.PreVote && m.Term > r.term)

	logOK := r.log.IsUpToDate(m.LastLogIndex, m.LastLogTerm)

	if canVote && logOK {

		if !m.PreVote {
			r.vote = m.From
			r.term = m.Term
		}
		r.electionElapsed = 0

		fmt.Printf("[%s] GRANTING vote to %s (term=%d)\n",
			r.id, m.From, m.Term)

		r.send(Message{
			Type:        MsgVoteRsp,
			To:          m.From,
			Term:        m.Term,
			VoteGranted: true,
			PreVote:     m.PreVote,
		})
	} else {

		reason := ""
		if !canVote {
			reason = fmt.Sprintf("already voted for %s", r.vote)
		} else {
			reason = "candidate log not up-to-date"
		}
		fmt.Printf("[%s] REJECTING vote from %s: %s\n",
			r.id, m.From, reason)

		r.send(Message{
			Type:        MsgVoteRsp,
			To:          m.From,
			Term:        r.term,
			VoteGranted: false,
			PreVote:     m.PreVote,
		})
	}
}

func (r *Raft) handleVoteResponse(m Message) {
	if m.VoteGranted {
		r.votes[m.From] = true
		fmt.Printf("[%s] received VOTE from %s (total=%d)\n",
			r.id, m.From, len(r.votes))

		if r.hasQuorum(r.votes) {
			if m.PreVote {

				r.campaign()
			} else {

				r.becomeLeader()
			}
		}
	} else {
		r.votes[m.From] = false
		fmt.Printf("[%s] vote DENIED by %s\n", r.id, m.From)
	}
}

func (r *Raft) handleInstallSnapshot(m Message) {
	if m.Snapshot == nil {
		return
	}

	fmt.Printf("[%s] installing snapshot from %s (index=%d, term=%d)\n",
		r.id, m.From, m.Snapshot.Index, m.Snapshot.Term)

	r.log.Compact(m.Snapshot.Index, m.Snapshot.Term)

	r.send(Message{
		Type:  MsgSnapshotRsp,
		To:    m.From,
		Term:  r.term,
		Index: m.Snapshot.Index,
	})
}

func (r *Raft) handleReadIndex(m Message) {

	r.readOnly.addRequest(r.log.Stats().CommitIndex, m.From, m.Entries[0].Data)

	r.broadcastHeartbeat()
}

func (r *Raft) campaign() {
	r.becomeCandidate()

	if len(r.config.Peers) == 0 {
		r.becomeLeader()
		return
	}

	lastIndex := r.log.LastIndex()
	lastTerm := r.log.LastTerm()

	for _, peer := range r.config.Peers {
		fmt.Printf("[%s] sending RequestVote to %s (term=%d)\n",
			r.id, peer, r.term)

		r.send(Message{
			Type:         MsgVote,
			To:           peer,
			Term:         r.term,
			LogIndex:     lastIndex,
			LogTerm:      lastTerm,
			LastLogIndex: lastIndex,
			LastLogTerm:  lastTerm,
		})
	}
}

func (r *Raft) broadcastAppendEntries(heartbeat bool) {
	for _, peer := range r.config.Peers {
		if heartbeat {
			r.sendHeartbeat(peer)
		} else {
			r.sendAppendEntries(peer)
		}
	}
}

func (r *Raft) broadcastHeartbeat() {
	for _, peer := range r.config.Peers {
		r.sendHeartbeat(peer)
	}
}

func (r *Raft) sendHeartbeat(to string) {
	commit := r.log.Stats().CommitIndex
	r.send(Message{
		Type:        MsgHeartbeat,
		To:          to,
		Term:        r.term,
		CommitIndex: commit,
	})
}

func (r *Raft) sendAppendEntries(to string) {
	prog := r.progress[to]
	if prog == nil {
		return
	}

	if prog.State == ProgressSnapshot {
		return
	}

	nextIndex := prog.NextIndex
	prevIndex := nextIndex - 1
	prevTerm := r.log.Term(prevIndex)

	entries := r.log.Entries(nextIndex,
		nextIndex+uint64(r.config.MaxEntriesToSend))

	msg := Message{
		Type:        MsgAppend,
		To:          to,
		Term:        r.term,
		LogIndex:    prevIndex,
		LogTerm:     prevTerm,
		Entries:     entries,
		CommitIndex: r.log.Stats().CommitIndex,
	}

	r.send(msg)
}

func (r *Raft) appendEntry(entry LogEntry) {
	entry.Index = r.log.LastIndex() + 1
	entry.Term = r.term
	r.log.AppendOne(entry)
}

func (r *Raft) maybeCommit() {

	matchIndexes := make([]uint64, 0, len(r.progress))
	for _, prog := range r.progress {
		matchIndexes = append(matchIndexes, prog.MatchIndex)
	}

	committed := r.log.MaybeCommit(matchIndexes, r.log.LastTerm(), r.term)
	if committed {

		stats := r.log.Stats()
		fmt.Printf("[%s] ✅ COMMITTED up to index=%d\n",
			r.id, stats.CommitIndex)
	}
}

func (r *Raft) logConsistencyCheck(prevIndex, prevTerm uint64) bool {
	if prevIndex == 0 {
		return true
	}
	myTerm := r.log.Term(prevIndex)
	return myTerm == prevTerm
}

func (r *Raft) findConflict(rejectedIndex uint64) (uint64, uint64) {
	conflictTerm := r.log.Term(rejectedIndex)
	if conflictTerm == 0 {

		return r.log.LastIndex(), 0
	}

	index := rejectedIndex
	for index > r.log.Stats().SnapshotIndex &&
		r.log.Term(index-1) == conflictTerm {
		index--
	}

	return index, conflictTerm
}

func (r *Raft) hasQuorum(votes map[string]bool) bool {
	granted := 0
	for _, v := range votes {
		if v {
			granted++
		}
	}

	total := 1 + len(r.config.Peers)
	return granted*2 > total
}

func (r *Raft) send(m Message) {
	if m.From == "" {
		m.From = r.id
	}
	if m.Term == 0 {
		m.Term = r.term
	}
	r.msgs = append(r.msgs, m)
}

func (r *Raft) TakeMessages() []Message {
	msgs := r.msgs
	r.msgs = nil
	return msgs
}

func (r *Raft) TakeReady() Ready {
	ready := Ready{}

	stats := r.log.Stats()
	if stats.LastIndex > stats.AppliedIndex {
		ready.Entries = r.log.Entries(stats.AppliedIndex+1, stats.LastIndex+1)
	}

	if stats.CommitIndex > stats.AppliedIndex {
		ready.CommittedEntries = r.log.Entries(stats.AppliedIndex+1, stats.CommitIndex+1)
	}

	ready.Messages = r.TakeMessages()

	ready.ReadStates = r.readOnly.takeReadStates()

	return ready
}

type readIndexState struct {
	index uint64
	from  string
	ctx   []byte
	acks  map[string]bool
}

type readOnly struct {
	pendingReadIndex map[string]*readIndexState
	readIndexQueue   []string
}

func newReadOnly() *readOnly {
	return &readOnly{
		pendingReadIndex: make(map[string]*readIndexState),
	}
}

func (r *readOnly) addRequest(index uint64, from string, ctx []byte) {
	key := string(ctx)
	if _, ok := r.pendingReadIndex[key]; ok {
		return
	}
	r.pendingReadIndex[key] = &readIndexState{
		index: index,
		from:  from,
		ctx:   ctx,
		acks:  make(map[string]bool),
	}
	r.readIndexQueue = append(r.readIndexQueue, key)
}

func (r *readOnly) takeReadStates() []ReadState {
	return nil
}
