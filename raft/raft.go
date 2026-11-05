// Package raft is Lab 2: leader election, log replication, commitment, and
// recovery from stable storage. It is an independent implementation written
// against the Raft paper's Figure 2, not the course skeleton.
package raft

import (
	"bytes"
	"encoding/gob"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lvdousha26/mit-6.5840-labs/labrpc"
)

// NoOp is appended by a fresh leader. Because Raft may only commit an entry
// from its own term by counting replicas, a new leader needs one such entry
// before it can push out entries it inherited from an earlier term.
type NoOp struct{}

// Commands travel through the log as interface{} values, and gob needs their
// concrete types registered before it can put them in a persisted snapshot.
// Callers that log their own struct should call gob.Register in their init.
func init() {
	gob.Register(int(0))
	gob.Register("")
	gob.Register(NoOp{})
}

const (
	heartbeatInterval = 50 * time.Millisecond
	tickerInterval    = 10 * time.Millisecond
	minElectionMS     = 260
	electionJitterMS  = 180
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

type ApplyMsg struct {
	CommandValid bool
	Command      any
	CommandIndex int
}

type LogEntry struct {
	Term    int
	Command any
}

type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	// ConflictIndex is 0 when the leader should simply step back one entry.
	// Otherwise it is the first index the follower can accept.
	ConflictIndex int
	Success       bool
}

type persistentState struct {
	CurrentTerm int
	VotedFor    int
	Log         []LogEntry
}

type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int
	dead      int32
	killOnce  sync.Once
	done      chan struct{}
	applyCh   chan ApplyMsg
	applyCond *sync.Cond

	// persistent state
	currentTerm int
	votedFor    int // -1 means no vote cast in this term
	log         []LogEntry

	// volatile state
	commitIndex int
	lastApplied int
	role        Role
	electionAt  time.Time
	lastBeat    time.Time
	rnd         *rand.Rand

	// leader-only volatile state
	nextIndex  []int
	matchIndex []int
}

func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:      peers,
		persister:  persister,
		me:         me,
		applyCh:    applyCh,
		done:       make(chan struct{}),
		votedFor:   -1,
		log:        []LogEntry{{Term: 0}}, // index 0 is a sentinel
		role:       Follower,
		rnd:        rand.New(rand.NewSource(time.Now().UnixNano() + int64(me)*7919)),
		nextIndex:  make([]int, len(peers)),
		matchIndex: make([]int, len(peers)),
	}
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.readPersist(persister.Read())
	rf.resetElectionLocked()
	go rf.ticker()
	go rf.applier()
	return rf
}

// Start appends a command to the leader's log. It does not wait for the
// command to be committed -- the caller learns about that via applyCh.
func (rf *Raft) Start(command any) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Leader || rf.killed() {
		return -1, rf.currentTerm, false
	}
	index := rf.lastIndexLocked() + 1
	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
	rf.matchIndex[rf.me] = index
	rf.persistLocked()
	go rf.replicate()
	return index, rf.currentTerm, true
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

func (rf *Raft) Kill() {
	rf.killOnce.Do(func() {
		atomic.StoreInt32(&rf.dead, 1)
		close(rf.done)
		rf.mu.Lock()
		rf.applyCond.Broadcast()
		rf.mu.Unlock()
	})
}

func (rf *Raft) killed() bool { return atomic.LoadInt32(&rf.dead) == 1 }

func (rf *Raft) lastIndexLocked() int { return len(rf.log) - 1 }

func (rf *Raft) lastTermLocked() int { return rf.log[len(rf.log)-1].Term }

func (rf *Raft) resetElectionLocked() {
	ms := minElectionMS + rf.rnd.Intn(electionJitterMS)
	rf.electionAt = time.Now().Add(time.Duration(ms) * time.Millisecond)
}

func (rf *Raft) termAtLocked(index int) int {
	if index < 0 || index >= len(rf.log) {
		return -1
	}
	return rf.log[index].Term
}

// becomeFollowerLocked steps down. Only a strictly newer term clears the
// vote, which is what makes a server vote at most once per term.
func (rf *Raft) becomeFollowerLocked(term int) {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
		rf.persistLocked()
	}
	rf.role = Follower
	rf.resetElectionLocked()
}

func (rf *Raft) becomeLeaderLocked() {
	rf.role = Leader
	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: NoOp{}})
	rf.persistLocked()
	last := rf.lastIndexLocked()
	for i := range rf.peers {
		rf.nextIndex[i] = last + 1
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = last
	rf.lastBeat = time.Now()
}

// ---------------------------------------------------------------------------
// Persistence
// ---------------------------------------------------------------------------

func (rf *Raft) persistLocked() {
	var buf bytes.Buffer
	st := persistentState{
		CurrentTerm: rf.currentTerm,
		VotedFor:    rf.votedFor,
		Log:         append([]LogEntry(nil), rf.log...),
	}
	if err := gob.NewEncoder(&buf).Encode(st); err != nil {
		// A command type that the caller forgot to register with gob ends up
		// here. Panic rather than silently running with an empty log.
		panic("raft: cannot encode persistent state: " + err.Error())
	}
	rf.persister.Save(buf.Bytes())
}

func (rf *Raft) readPersist(data []byte) {
	if len(data) == 0 {
		return
	}
	var st persistentState
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&st); err != nil {
		panic("raft: cannot decode persistent state: " + err.Error())
	}
	rf.currentTerm = st.CurrentTerm
	rf.votedFor = st.VotedFor
	if len(st.Log) > 0 {
		rf.log = append([]LogEntry(nil), st.Log...)
	}
	// commitIndex and lastApplied stay at 0 after a restart. The state
	// machine will replay committed entries once a leader tells us the
	// commit point again; nothing is lost because the log is durable.
}

// ---------------------------------------------------------------------------
// Ticker and election
// ---------------------------------------------------------------------------

func (rf *Raft) ticker() {
	for !rf.killed() {
		time.Sleep(tickerInterval)
		rf.mu.Lock()
		now := time.Now()
		switch {
		case rf.role == Leader && now.Sub(rf.lastBeat) >= heartbeatInterval:
			rf.lastBeat = now
			rf.mu.Unlock()
			rf.replicate()
		case rf.role != Leader && now.After(rf.electionAt):
			rf.mu.Unlock()
			rf.startElection()
		default:
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	if rf.role == Leader || !time.Now().After(rf.electionAt) || rf.killed() {
		rf.mu.Unlock()
		return
	}
	rf.role = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persistLocked()
	rf.resetElectionLocked()

	term := rf.currentTerm
	args := RequestVoteArgs{
		Term:         term,
		CandidateID:  rf.me,
		LastLogIndex: rf.lastIndexLocked(),
		LastLogTerm:  rf.lastTermLocked(),
	}
	rf.mu.Unlock()

	var votes atomic.Int32
	var won atomic.Bool
	votes.Store(1)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(peer int) {
			var reply RequestVoteReply
			if !rf.peers[peer].Call("Raft.RequestVote", &args, &reply) {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.currentTerm != term || rf.role != Candidate {
				return
			}
			if reply.Term > rf.currentTerm {
				rf.becomeFollowerLocked(reply.Term)
				return
			}
			if !reply.VoteGranted {
				return
			}
			if votes.Add(1) > int32(len(rf.peers)/2) && won.CompareAndSwap(false, true) {
				rf.becomeLeaderLocked()
				// Establish authority immediately; waiting for the next
				// heartbeat would give followers time to start an election.
				go rf.replicate()
			}
		}(i)
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return nil
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
		reply.Term = rf.currentTerm
	}
	lastIndex, lastTerm := rf.lastIndexLocked(), rf.lastTermLocked()
	upToDate := args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)
	if !upToDate || (rf.votedFor != -1 && rf.votedFor != args.CandidateID) {
		return nil
	}
	rf.votedFor = args.CandidateID
	rf.persistLocked()
	rf.resetElectionLocked()
	reply.VoteGranted = true
	return nil
}

// ---------------------------------------------------------------------------
// Log replication
// ---------------------------------------------------------------------------

// replicate takes a snapshot of the log under the lock and sends one RPC per
// peer in its own goroutine. No network call ever happens while rf.mu is held.
func (rf *Raft) replicate() {
	rf.mu.Lock()
	if rf.role != Leader || rf.killed() {
		rf.mu.Unlock()
		return
	}
	term := rf.currentTerm
	commit := rf.commitIndex
	me := rf.me
	type job struct {
		peer int
		args *AppendEntriesArgs
	}
	jobs := make([]job, 0, len(rf.peers))
	for i := range rf.peers {
		if i == me {
			continue
		}
		next := rf.nextIndex[i]
		if next < 1 {
			next = 1
		}
		if next > len(rf.log) {
			next = len(rf.log)
		}
		prev := next - 1
		entries := append([]LogEntry(nil), rf.log[next:]...)
		jobs = append(jobs, job{peer: i, args: &AppendEntriesArgs{
			Term:         term,
			LeaderID:     me,
			PrevLogIndex: prev,
			PrevLogTerm:  rf.log[prev].Term,
			Entries:      entries,
			LeaderCommit: commit,
		}})
	}
	rf.mu.Unlock()

	for _, j := range jobs {
		go rf.sendAppendEntries(j.peer, j.args)
	}
}

func (rf *Raft) sendAppendEntries(peer int, args *AppendEntriesArgs) {
	var reply AppendEntriesReply
	if !rf.peers[peer].Call("Raft.AppendEntries", args, &reply) {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm != args.Term || rf.role != Leader || rf.killed() {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.becomeFollowerLocked(reply.Term)
		return
	}
	if !reply.Success {
		next := rf.nextIndex[peer]
		if reply.ConflictIndex > 0 {
			next = reply.ConflictIndex
		} else {
			next--
		}
		if next < 1 {
			next = 1
		}
		rf.nextIndex[peer] = next
		return
	}

	match := args.PrevLogIndex + len(args.Entries)
	if match > rf.matchIndex[peer] {
		rf.matchIndex[peer] = match
	}
	if match+1 > rf.nextIndex[peer] {
		rf.nextIndex[peer] = match + 1
	}

	// Figure 8: only an entry from the current term can be committed by
	// counting replicas. Older entries become committed implicitly because
	// committing a later index commits everything before it.
	for n := rf.lastIndexLocked(); n > rf.commitIndex; n-- {
		if rf.log[n].Term != rf.currentTerm {
			continue
		}
		copies := 1
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= n {
				copies++
			}
		}
		if copies > len(rf.peers)/2 {
			rf.commitIndex = n
			rf.applyCond.Signal()
			break
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return nil
	}
	if args.Term > rf.currentTerm || rf.role != Follower {
		rf.becomeFollowerLocked(args.Term)
		reply.Term = rf.currentTerm
	}
	rf.resetElectionLocked()
	reply.Success = false

	last := rf.lastIndexLocked()
	if args.PrevLogIndex > last {
		reply.ConflictIndex = last + 1
		return nil
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		conflictTerm := rf.log[args.PrevLogIndex].Term
		i := args.PrevLogIndex
		for i > 0 && rf.log[i].Term == conflictTerm {
			i--
		}
		reply.ConflictIndex = i + 1
		return nil
	}

	// Copy entries into our own log. args is shallow-copied by labrpc, so
	// retaining the caller's slice directly would share storage.
	for k, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + k
		if idx > rf.lastIndexLocked() {
			rf.log = append(rf.log, args.Entries[k:]...)
			rf.persistLocked()
			break
		}
		if rf.log[idx].Term != entry.Term {
			rf.log = rf.log[:idx]
			rf.log = append(rf.log, args.Entries[k:]...)
			rf.persistLocked()
			break
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastIndexLocked())
		rf.applyCond.Signal()
	}
	reply.Success = true
	return nil
}

// ---------------------------------------------------------------------------
// Applying committed entries
// ---------------------------------------------------------------------------

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex && !rf.killed() {
			rf.applyCond.Wait()
		}
		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		start, end := rf.lastApplied+1, rf.commitIndex
		msgs := make([]ApplyMsg, 0, end-start+1)
		for i := start; i <= end; i++ {
			msgs = append(msgs, ApplyMsg{
				CommandValid: true, Command: rf.log[i].Command, CommandIndex: i,
			})
		}
		rf.lastApplied = end
		rf.mu.Unlock()

		// Sending outside the lock is essential: an applyCh with a full
		// buffer must never stall an RPC handler that holds rf.mu.
		for _, m := range msgs {
			select {
			case rf.applyCh <- m:
			case <-rf.done:
				return
			}
		}
	}
}
