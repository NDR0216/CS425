package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"raft/labrpc"
	"sync"
	"sync/atomic"
	"time"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu    sync.Mutex          // Lock to protect shared access to this peer's state
	peers []*labrpc.ClientEnd // RPC end points of all peers
	me    int                 // this peer's index into peers[]
	dead  int32               // set by Kill()

	// Your data here (2A, 2B).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// You may also need to add other state, as per your implementation.
	currentTerm int        // latest term server has seen(initialized to 0 on first boot, increases monotonically)
	votedFor    int        // candidateId that received vote incurrent term (or null if none)
	log         []LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	commitIndex int        // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	// lastApplied
	nextIndex []int // for each server, index of the next log entry to send to that server (initialized to leader lastlogindex + 1)
	// matchIndex []int

	isleader bool
	stepDown bool
	ch_reset chan bool

	applyCh chan ApplyMsg

	logRevCount map[int]int // count how many servers have appended an entry, used for committing
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.isleader
	rf.mu.Unlock()

	return term, isleader
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// Read the fields in "args",
	// and accordingly assign the values for fields in "reply".
	rf.mu.Lock()

	if args.Term < rf.currentTerm { // Reply false if term < currentTerm
		reply.Term = rf.currentTerm
		reply.VoteGranted = false

		rf.mu.Unlock()
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1 // -1 means null
		rf.stepDown = true
	}

	reply.Term = rf.currentTerm

	// If voted For is null or candidateId, and candidate’s log is at least as
	// up-to-date as receiver’s log, grant vote
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > rf.log[len(rf.log)-1].Term || (args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log)-1)) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}

	rf.mu.Unlock()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term int // leader’s term
	// leaderId int
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store(empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm

	FirstConflictingTermIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()

	if args.Term < rf.currentTerm { // Reply false if term < currentTerm
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.FirstConflictingTermIndex = -1
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	rf.ch_reset <- true
	rf.mu.Lock()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.stepDown = true
	}

	if args.PrevLogIndex > len(rf.log)-1 {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.FirstConflictingTermIndex = -1
		rf.mu.Unlock()
		return
	}

	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false

		for i := args.PrevLogIndex - 1; i >= 0; i-- {
			if rf.log[i].Term != rf.log[args.PrevLogIndex].Term {
				reply.FirstConflictingTermIndex = i + 1
			}
		}
		rf.log = rf.log[0 : args.PrevLogIndex+1]
		rf.mu.Unlock()
		return
	}

	// If an existing entry conflicts with a new one (sameindex but different terms), delete the existing entry and all that follow it
	if len(rf.log) > args.PrevLogIndex {
		rf.log = rf.log[0 : args.PrevLogIndex+1]
	}

	// Append any new entries not already in the log
	for _, e := range args.Entries {
		rf.log = append(rf.log, e)
	}

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	for rf.commitIndex < args.LeaderCommit && rf.commitIndex < len(rf.log) {
		rf.commitIndex += 1
		rf.applyCh <- ApplyMsg{
			true,
			rf.log[rf.commitIndex].Command,
			rf.commitIndex}
	}

	reply.Term = rf.currentTerm
	rf.mu.Unlock()

	reply.Success = true
	reply.FirstConflictingTermIndex = -1
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func elapse(ch chan bool, t time.Duration) {
	time.Sleep(t)
	ch <- true
}

func (rf *Raft) sendRequestVoteCaller(i int, currentTerm int, ch_reply chan RequestVoteReply) {
	rf.mu.Lock()
	lastLog := rf.log[len(rf.log)-1]
	args := RequestVoteArgs{rf.currentTerm, rf.me, len(rf.log) - 1, lastLog.Term}
	rf.mu.Unlock()

	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(i, &args, &reply) // wait for reply

	if ok {
		ch_reply <- reply
	}
}

func (rf *Raft) sendAppendEntriesCaller(i int, currentTerm int, tmpNextIndex int) {
	rf.mu.Lock()
	lastIdx := len(rf.log)

	args := AppendEntriesArgs{
		currentTerm,
		tmpNextIndex - 1,
		rf.log[tmpNextIndex-1].Term,
		rf.log[tmpNextIndex:],
		rf.commitIndex}
	rf.mu.Unlock()

	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(i, &args, &reply)

	rf.mu.Lock()
	if ok && reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.stepDown = true
		rf.mu.Unlock()
		return
	}

	if ok && !reply.Success { // If AppendEntries fails because of log inconsistency: decrement nextIndex and retry
		// rf.nextIndex[i] = tmpNextIndex - 1
		if reply.FirstConflictingTermIndex != -1 {
			rf.nextIndex[i] = reply.FirstConflictingTermIndex
		} else {
			rf.nextIndex[i] = tmpNextIndex - 1
		}
		rf.mu.Unlock()
		return
	}

	if ok && reply.Success {
		// rf.nextIndex[i] = len(rf.log)
		// rf.matchIndex[i] = len(rf.log)
		// lastIdx = len(rf.log)
		for rf.nextIndex[i] < lastIdx {
			rf.logRevCount[rf.nextIndex[i]]++
			if rf.nextIndex[i] > rf.commitIndex && float64(rf.logRevCount[rf.nextIndex[i]]) > float64(len(rf.peers))/2.0 && rf.log[rf.nextIndex[i]].Term == rf.currentTerm {
				for rf.commitIndex < rf.nextIndex[i] {
					rf.commitIndex += 1
					rf.applyCh <- ApplyMsg{
						true,
						rf.log[rf.commitIndex].Command,
						rf.commitIndex}
				}
			}
			rf.nextIndex[i]++
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) election() bool {
	rf.mu.Lock()
	rf.currentTerm++ // Increment currentTerm
	currentTerm := rf.currentTerm
	rf.votedFor = rf.me // Vote for self

	votes := 1 // including self-vote
	rf.mu.Unlock()

	// Reset election timer
	ch_elapse := make(chan bool)
	timeout := time.Duration(300+rand.Float64()*400) * time.Millisecond
	go elapse(ch_elapse, timeout)

	ch_reply := make(chan RequestVoteReply)

	for i := range rf.peers { // Send Request Vote RPCs to all other servers
		if i != rf.me {
			go rf.sendRequestVoteCaller(i, currentTerm, ch_reply)
		}
	}

	// check majority whenever receive a reply
	for {
		select {
		case <-ch_elapse:
			return false // If election timeout elapses: start new election
		case reply := <-ch_reply:
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.stepDown = true
				rf.mu.Unlock()
				return false
			}
			rf.mu.Unlock()

			rf.mu.Lock()
			if currentTerm == rf.currentTerm { // check that is still being candidate
				if reply.VoteGranted {
					votes++
				}

				// If votes received from majority of servers: become leader
				if float64(votes) > float64(len(rf.peers))/2.0 {
					rf.mu.Unlock()
					return true
				}
			}
			rf.mu.Unlock()
		}

	}
}

func (rf *Raft) followers() {
	rf.mu.Lock()
	rf.isleader = false
	rf.mu.Unlock()

	for {
		ch_elapse := make(chan bool)

		timeout := time.Duration(300+rand.Float64()*400) * time.Millisecond
		go elapse(ch_elapse, timeout)

		select {
		case <-ch_elapse: // elapse
			rf.mu.Lock()
			rf.stepDown = false
			rf.mu.Unlock()
			defer rf.candidates()
			return
		case <-rf.ch_reset: // reset
			continue
		}
	}
}

func (rf *Raft) candidates() {
	rf.mu.Lock()

	if rf.stepDown == true {
		rf.mu.Unlock()
		defer rf.followers()
		return
	}
	rf.mu.Unlock()

	if rf.killed() {
		return
	}

	for {
		b := rf.election()

		rf.mu.Lock()

		if rf.stepDown == true {
			rf.mu.Unlock()
			defer rf.followers()
			return
		}
		rf.mu.Unlock()
		if b {
			defer rf.leaders()
			return
		}
	}
}

func (rf *Raft) leaders() {
	rf.mu.Lock()
	rf.isleader = true
	currentTerm := rf.currentTerm

	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.log)
	}

	rf.mu.Unlock()

	for {
		rf.mu.Lock()

		if rf.stepDown == true {
			rf.mu.Unlock()
			defer rf.followers()
			return
		}

		rf.mu.Unlock()
		for i := range rf.peers {
			if i != rf.me {
				rf.mu.Lock()
				tmpNextIndex := rf.nextIndex[i]
				rf.mu.Unlock()
				go rf.sendAppendEntriesCaller(i, currentTerm, tmpNextIndex)
			}
		}

		time.Sleep(100 * time.Millisecond)

		if rf.killed() {
			return
		}
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()

	index := len(rf.log)
	term := rf.currentTerm
	isLeader := rf.isleader

	if !isLeader {
		rf.mu.Unlock()
		return index, term, false
	}

	rf.logRevCount[len(rf.log)] = 1

	newLogEntry := LogEntry{rf.currentTerm, command}
	rf.log = append(rf.log, newLogEntry)

	rf.mu.Unlock()

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.me = me

	// Your initialization code here (2A, 2B).
	rf.currentTerm = 0
	rf.votedFor = -1 // -1 means null
	rf.isleader = false

	rf.ch_reset = make(chan bool)

	//---2B---
	rf.log = []LogEntry{{0, 0}}
	rf.logRevCount = make(map[int]int)

	rf.stepDown = true
	rf.applyCh = applyCh
	rf.nextIndex = make([]int, len(peers))
	rf.commitIndex = 0

	go rf.followers()

	return rf
}
