package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import (
	"labrpc"
	"time"
	"math/rand"
	"log"
	"fmt"
	"encoding/json"
	"sync/atomic"
	"bytes"
	"encoding/gob"
	"strings"
)

// import "bytes"
// import "encoding/gob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}
const  kVotedNone = -1;

type ServerRole int;
const (
	LEADER = iota
	FOLLOWER
	CANDIDATE
)

type ApplyState int;

const (
	NEW_INSERT = iota
	COMMITED
	APPLIES
)


type LogEntry struct {
	Term		int				// Term of this log
	Command		interface{}		// applied command
	State		ApplyState
	Index		int 			// index at the whole log
}

type PutAppendCmd struct {
	Key string
	Value string
	Op	string
}


//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role		ServerRole				// role of server: leader, candiate, follower
	currentTerm	int				 // latest Term server has, increase monotonically;
	votedFor	int 			 //	candidatedId that received vote in current Term, -1 if none
	log			[]LogEntry		// log enteries
								// transient fields =====
	commitIndex 	int			// index of highest log entry known to be commited
	lastApplied	int				// index of highest log entry applied to state machine

	nextIndex	[]int			// for each server, index of next log entry to send that server
	matchedIndex	[]int		// for each server, index of highest log entry known to be replicated on server

	heartbeatTimeout	time.Duration		// the last time received a heartbeat message
	electionTimeout		time.Duration

	firstHeartbeat	bool

	stateMachine	map[string]string

	applyCh			chan ApplyMsg
	quitCh			chan int
	heartbeatCh		chan int

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm;
	isleader = rf.role == LEADER
	return term, isleader
}

func (rf *Raft) GetLeaderId() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.votedFor
}

func (rf *Raft) GetSavedStates() int {
	return rf.persister.RaftStateSize()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate Term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // Term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term		int			// current Term of receiver
	VoteGranted	bool		// true means candidate recieved voted ballot
}

type AppendEntriesArgs struct {
	Term         int        // leader’s Term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // Term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  //	currentTerm, for leader to update itself
	Success bool //	true if follower contained entry matching prevLogIndex and PrevLogTerm
}

type InstallSnapshotArgs struct {
	Term	int // leader’s term
	LeaderId	int
	LastIncludedIndex	int // the snapshot replaces all entries up through and including this index
	LastIncludedTerm	int // term of lastIncludedIndex
	Data	[]byte		// raw bytes of the snapshot thunk
}

type InstallSnapshotReply struct {
	Term	int // currentTerm, for leader to update its term
}


const kHearbeat = 1;

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func max(x,y int) int  {
	if x > y {
		return x
	}
	return y
}

//
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply)  {
	//log.Printf("%d received appendEntires %s", rf.me, toJsonString(args))
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer dropAndSet(rf.heartbeatCh, kHearbeat)
	defer rf.persist()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	if rf.role == CANDIDATE {
		rf.becomeFollower(args.Term)
		rf.votedFor = args.LeaderId
	}


	reply.Term = rf.currentTerm
	baseLogIdx := rf.log[0].Index
	if args.PrevLogIndex < baseLogIdx ||
		(rf.log[args.PrevLogIndex - baseLogIdx].Term != args.PrevLogTerm) {
			reply.Success = false
			return
	}


	reply.Success = true
	matchedLogIndex := args.PrevLogIndex - baseLogIdx
	maxReservedIdx := matchedLogIndex + len(args.Entries)
	rf.log = append(rf.log[ : matchedLogIndex + 1], args.Entries...)
	//log.Printf("%d concatenate result:%s",rf.me, toJsonString(rf.log))
	oldCommitIdx := rf.commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, maxReservedIdx)
	}

	rf.applyLogEntry(oldCommitIdx)
	return
}

func (rf *Raft) InstallSnapshot(args InstallSnapshotArgs, reply *InstallSnapshotReply)  {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer dropAndSet(rf.heartbeatCh, kHearbeat)
	defer rf.persist()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if rf.currentTerm < args.Term {
		rf.becomeFollower(args.Term)
	}
	reply.Term = args.Term

	baseLogIdx := rf.log[0].Index
	if args.LastIncludedIndex < baseLogIdx {
		return
	}


	rf.log = rf.truncateLog(args.LastIncludedIndex, args.LastIncludedTerm)
	rf.persister.SaveSnapshot(args.Data) // replace previous snapshot data
	rf.lastApplied = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex
	rf.applyCh <- ApplyMsg{UseSnapshot:true, Snapshot:args.Data}

}

func (rf *Raft) getLastLogTerm() int {
	return rf.log[len(rf.log) - 1].Term
}

func (rf *Raft)  getLastLogIdx() int {
	return rf.log[len(rf.log) - 1].Index;
}

func (rf *Raft) ProcessGet(key string) string {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.stateMachine[key]
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	log.Printf("%d recieved vote request %s", rf.me, toJsonString(args))
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer dropAndSet(rf.heartbeatCh, kHearbeat)
	defer rf.persist()

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	reply.VoteGranted = false
	if rf.currentTerm == args.Term && (rf.votedFor == kVotedNone || rf.votedFor == args.CandidateId) &&
		(rf.getLastLogTerm() < args.LastLogTerm || (rf.getLastLogTerm() == args.LastLogTerm && rf.getLastLogIdx() <= args.LastLogIndex)) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId

		log.Printf("vote to %d", args.CandidateId)
	}
	reply.Term = rf.currentTerm
	return
}

func dropAndSet(ch chan int, v int) {
	select {
	case <-ch:
	default:
	}
	ch <- v
}

func (rf *Raft) becomeFollower(term int) {
	defer rf.persist()
	log.Printf("%d become follower", rf.me)
	rf.role = FOLLOWER
	rf.currentTerm = term
	rf.votedFor = kVotedNone
	rf.firstHeartbeat = false
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	isLeader = rf.role == LEADER
	if rf.role != LEADER {
		return index, term, isLeader
	}

	newLogEntry := LogEntry{rf.currentTerm, command, NEW_INSERT,
	rf.log[len(rf.log) - 1].Index + 1}
	rf.log = append(rf.log, newLogEntry)
	return rf.getLastLogIdx() + 1, rf.getLastLogTerm(), isLeader
}

func (rf *Raft) applyLogEntry(oldCommitIdx int)  {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	for i:= oldCommitIdx + 1;  i <= rf.commitIndex; i++  {
		rf.log[i].State = APPLIES
		cmd := rf.log[i].Command
		rf.applyCh <- ApplyMsg{i + 1, cmd, false, make([]byte, 0)}
		log.Printf("%d send applyMsg %s at index %d", rf.me, toJsonString(rf.log[i]), i)
		putAppendArg, ok := cmd.(PutAppendCmd)
		if ok && strings.EqualFold("Put", putAppendArg.Op) {
			rf.stateMachine[putAppendArg.Key] = putAppendArg.Value
		} else if ok && strings.EqualFold("Append", putAppendArg.Op) {
			rf.stateMachine[putAppendArg.Key] += putAppendArg.Value
		}

		rf.lastApplied = i;
	}

}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

const kLeaderHeartPeriod  = 40 * time.Millisecond
const kMinElectTime = 300
const kMaxElectTime = 500

func toJsonString(v interface{}) string{
	b, err := json.Marshal(v)
	if err != nil {
		fmt.Println(err)
		return ""
	}
	return string(b)
}

func (rf *Raft) startElection()  {
	rf.mu.Lock();
	rf.currentTerm++
	lastLogIdx := rf.getLastLogIdx()
	lastLogTerm := rf.getLastLogTerm()
	winBallot := len(rf.peers) / 2 + len(rf.peers)%2;
	rf.votedFor = rf.me
	requestVoteArgs := RequestVoteArgs{rf.currentTerm, rf.me, lastLogIdx, lastLogTerm}
	rf.mu.Unlock()

	var collectBallot int32 = 1
	for idx, _ := range rf.peers {
		if idx == rf.me {
			continue
		}
		go func(idx int) {
			reply := &RequestVoteReply{};
			ok := rf.sendRequestVote(idx, requestVoteArgs, reply)

			if ok {
				log.Printf("%d receive reply:%s from node %d", rf.me, toJsonString(reply), idx)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.role = FOLLOWER
					return
				}
				if reply.VoteGranted && rf.role == CANDIDATE {
					atomic.AddInt32(&collectBallot, 1)
					if int32(winBallot) <= atomic.LoadInt32(&collectBallot) {
						log.Printf("%d become leader", rf.me)
						rf.role = LEADER
						rf.firstHeartbeat = true
						dropAndSet(rf.heartbeatCh, kHearbeat)
						return
					}
				}
			}
		}(idx)

	}
}

func (rf *Raft) getBaseLogIdx() int  {
	baseLogIndex := 0
	if 0 < len(rf.log) {
		baseLogIndex = rf.log[0].Index
	}
	return baseLogIndex
}

func (rf *Raft) startAppendEntries()  {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			go rf.updateLeaderCommit()
		} else {
			go func(i int) {
				rf.mu.Lock()
				logs := make([]LogEntry, 0)
				nextIdx := rf.matchedIndex[i] + 1
				baseLogIdx := rf.log[0].Index
				if nextIdx <= baseLogIdx {
					rf.mu.Unlock()
					rf.doInstallSnapshot(i, rf.persister.snapshot)
				} else  {
					mayNextIdx := rf.log[len(rf.log) - 1].Index
					if rf.matchedIndex[i] < mayNextIdx {
						logs = rf.log[rf.matchedIndex[i] + 1:]
					}
					prevLogIdx := rf.matchedIndex[i]
					prevLogTerm := rf.log[prevLogIdx - baseLogIdx].Term
					heartBeatArgs := AppendEntriesArgs{rf.currentTerm, rf.me,
						prevLogIdx, prevLogTerm, logs, rf.commitIndex};
					rf.mu.Unlock()

					reply := &AppendEntriesReply{}
					ok := rf.sendAppendEntries(i, heartBeatArgs, reply)
					if ok {
						rf.mu.Lock()
						if reply.Term > rf.currentTerm {
							rf.becomeFollower(reply.Term)
							dropAndSet(rf.heartbeatCh, kHearbeat);
						}

						if reply.Success {
							rf.nextIndex[i] = mayNextIdx + 1
							rf.matchedIndex[i] = mayNextIdx
						} else {
							if 0 < rf.matchedIndex[i] {
								rf.nextIndex[i]--
								rf.matchedIndex[i]--
							}
						}
						rf.mu.Unlock()
					}
				}
			}(i)
		}
	}
}

func (rf *Raft) runAsLeader(firstHeartBeat bool) bool {
	if firstHeartBeat {
		rf.mu.Lock()
		rf.firstHeartbeat = false
		for idx, _ := range rf.matchedIndex {
			rf.matchedIndex[idx] = 0
		}
		rf.mu.Unlock()
		rf.startAppendEntries()
		return true
	}

	select {
	case <-rf.quitCh:
		return false
	case <-time.After(rf.heartbeatTimeout):
		rf.startAppendEntries()
	case <-rf.heartbeatCh:

	}
	return true
}

func (rf *Raft) runAsFollowerCandidate() bool{
	select {
		case <-rf.quitCh:
			return false
		case <-time.After(rf.electionTimeout):
			rf.mu.Lock()
			if rf.role == FOLLOWER {
				rf.role = CANDIDATE
			}
			rf.mu.Unlock()
			go rf.startElection()
			rf.electionTimeout = time.Millisecond * time.Duration(random(kMinElectTime, kMaxElectTime))
		case <- rf.heartbeatCh:
			// do nothing to skip electionTimer
	}
	return true
}

func random(min, max int) int {
	return rand.Intn(max - min) + min
}

func (rf *Raft) heartbeatBackgroud() {
	for {
		isExited := true
		rf.mu.Lock()
		role := rf.role
		firstHeartbeat := rf.firstHeartbeat
		rf.mu.Unlock()
		if role == LEADER {
			isExited = rf.runAsLeader(firstHeartbeat)
		} else if role== FOLLOWER || role == CANDIDATE {
			isExited = rf.runAsFollowerCandidate()
		}

		if !isExited {
			return
		}
	}
}

func (rf *Raft) StartSnapshot(snapshot []byte, index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.log[0].Index || rf.log[len(rf.log) - 1].Index < index {
		// installed or out of log table
		return
	}

	rf.log = rf.log[index - rf.log[0].Index : ]
	rf.persist()

	w := new(bytes.Buffer)
	enc := gob.NewEncoder(w)
	enc.Encode(rf.log[0].Index)
	enc.Encode(rf.log[0].Term)
	data := w.Bytes()
	data = append(data, snapshot...)
	rf.persister.SaveSnapshot(data)

}

func (rf *Raft) readSnapshot(snapshot []byte)  {
	if len(snapshot) == 0 {
		return
	}
	var lastIncludedIndex int
	var lastIncludedTerm int
	r := bytes.NewBuffer(snapshot)
	dec := gob.NewDecoder(r)
	dec.Decode(&lastIncludedIndex)
	dec.Decode(&lastIncludedTerm)
	rf.lastApplied = lastIncludedIndex
	rf.commitIndex = lastIncludedIndex
	rf.log = rf.truncateLog(lastIncludedIndex, lastIncludedTerm)
	rf.applyCh <- ApplyMsg{UseSnapshot:true, Snapshot:snapshot}

}

func (rf *Raft) truncateLog(index int, term int) []LogEntry {
	var newLogEntries []LogEntry
	newLogEntries = append(newLogEntries, LogEntry{Index: index, Term: term})
	//!!! Be careful of variable name overwrite
	for  i:= len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].Index == index && rf.log[i].Term == term {
			newLogEntries = append(newLogEntries, rf.log[i+1:]...)
			break
		}
	}
	return newLogEntries
}



func (rf *Raft) doInstallSnapshot(serverId int, snapshot []byte) {
	rf.mu.Lock()
	args := InstallSnapshotArgs{rf.currentTerm, rf.me, rf.log[0].Index,
			rf.log[0].Term, snapshot}
	rf.mu.Unlock()

	var reply InstallSnapshotReply
	if ok := rf.sendInstallSnapshot(serverId, args, &reply); ok {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term)
			dropAndSet(rf.heartbeatCh, kHearbeat);
		} else {
			rf.matchedIndex[serverId] = args.LastIncludedIndex
			rf.nextIndex[serverId] = args.LastIncludedIndex + 1
		}
		rf.mu.Unlock()
	}
}
func (rf *Raft) updateLeaderCommit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	oldCommitIdx := rf.commitIndex
	newCommitIdx := oldCommitIdx
	for i:=len(rf.log)-1; rf.log[i].Index > oldCommitIdx && rf.log[i].Term == rf.currentTerm; i-- {
		commitServer := 1
		for server := range rf.peers {
			if server != rf.me && rf.matchedIndex[server] >= rf.log[i].Index {
				commitServer++
			}
		}

		if len(rf.peers) <= commitServer*2  {
			newCommitIdx = i
			break
		}
	}

	if oldCommitIdx == newCommitIdx {
		return
	}

	rf.commitIndex = newCommitIdx
	rf.applyLogEntry(oldCommitIdx)
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = kVotedNone
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchedIndex = make([]int, len(peers))
	for idx, _ := range rf.matchedIndex {
		rf.matchedIndex[idx] = 0
	}
	rf.role = FOLLOWER
	rf.heartbeatTimeout = kLeaderHeartPeriod
	rand.Seed(time.Now().UnixNano())
	rf.electionTimeout = time.Millisecond * time.Duration(random(kMinElectTime, kMaxElectTime))
	rf.applyCh = applyCh;
	rf.quitCh = make(chan int, 1)
	rf.heartbeatCh = make(chan int, 1)
	rf.firstHeartbeat = false
	rf.log = append(rf.log, LogEntry{0, nil, COMMITED,0})
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())


	go rf.heartbeatBackgroud()

	return rf
}
