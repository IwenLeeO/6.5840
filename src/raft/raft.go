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
	//	"bytes"
	"6.5840/labrpc"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type RaftStatus string

const (
	Follower   RaftStatus = "follower"
	Candidator RaftStatus = "candidator"
	Leader     RaftStatus = "leader"
)

type MillisecondTime int

const (
	HeartBeatTimeOut MillisecondTime = 200
)

type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	heartBeatTimer  time.Ticker
	electionTimeOut time.Timer

	leaderedTick time.Ticker
	leadered     bool

	status RaftStatus
	// Persistent state on all servers
	currentTerm int
	votedFor    *int
	logs        []interface{}

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	status := rf.status
	if status == Leader {
		isleader = true
	} else {
		isleader = false
	}
	// Your code here (2A).
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm && rf.checkUpdate() {
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.currentTerm = args.Term
		rf.status = Follower
		return
	}

	if (rf.votedFor == nil || rf.votedFor == &args.CandidateId) && rf.checkUpdate() {
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.currentTerm = args.Term
		rf.status = Follower
		return
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
}

func (rf *Raft) checkUpdate() bool {
	return true
}

type RequestAppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int

	PrevLogTerm int
	Entries     []interface{}
	LederCommit int
}

type RequestAppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) RequestAppendEntries(args *RequestAppendEntriesArgs,
	reply *RequestAppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
	}

	if len(args.Entries) == 0 {
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		reply.Success = true
		rf.status = Follower
		rf.votedFor = nil
		rf.leadered = true
		return
	}
	reply.Success = true
	reply.Term = rf.currentTerm
	rf.votedFor = nil
	rf.status = Follower
	rf.currentTerm = args.Term
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

func (rf *Raft) sendRequestAppendEntries(server int, args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
	return ok
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// fmt.Printf("[%s] ---------- raft %d check need election ------- \n", rf.me)
		// Your code here (2A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.

		// switch rf.status {
		// case Follower:
		// case Candidator:
		// rf.startElection()
		// case Leader:
		// case Candidator:
		// rf.startElection()
		// }
		// rf.startElection()
		// ms := 50 + (rand.Int63() % 300)
		// time.Sleep(time.Duration(ms) * time.Millisecond)
		select {
		case <-rf.electionTimeOut.C:
			fmt.Printf("[%s] raft %d is start election \n", time.Now().Format("2006-01-02 15:04:00.000"), rf.me)
			rf.startElection()
		case <-rf.heartBeatTimer.C:
			rf.heartBeat()
		}
	}
}

func Max(a, b int) int {
	if a >= b {
		return a
	}
	return b
}

func (rf *Raft) tickInPowerByLeader() {
	for t := range rf.leaderedTick.C {
		fmt.Sprintf("is leadered ticked at %v .. \n", t)
		rf.mu.Lock()
		rf.leadered = false
		rf.mu.Unlock()
	}
}

func (rf *Raft) startElection() {
	finalChan := make(chan bool, 1)
	rfCp := Raft{}
	args := RequestVoteArgs{}

	rf.mu.Lock()
	rafts := len(rf.peers)
	if rf.leadered {
		rf.electionTimeOut = *time.NewTimer(time.Duration(50+rand.Int63()%300) * time.Millisecond)
		fmt.Printf("[%s] Raft %d still be follower at term %d \n", time.Now().Format("2006-01-02 15:04:00.000"), rf.me, rf.currentTerm)
		rf.mu.Unlock()
		return
	}

	if rf.status == Leader {
		fmt.Printf("[%s] Raft %d still be leader at term %d \n", time.Now().Format("2006-01-02 15:04:00.000"), rf.me, rf.currentTerm)
		rf.electionTimeOut = *time.NewTimer(time.Duration(50+rand.Int63()%300) * time.Millisecond)
		rf.mu.Unlock()
		return
	}
	rf.currentTerm = rf.currentTerm + 1
	rf.votedFor = &rf.me
	fmt.Printf("[%s] Raft %d status change to %s and votefor %d at term %d \n", time.Now().Format("2006-01-02 15:04:00.000"), rf.me, rf.status, *rf.votedFor, rf.currentTerm)
	rf.status = Candidator
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	DeepCopy(rf, &rfCp)
	rf.mu.Unlock()

	electionTimeOut := time.NewTimer(time.Duration(1000) * time.Millisecond)
	defer electionTimeOut.Stop()
	resChan := make([]RequestVoteReply, 0)
	var voteCount int32
	var localMutex sync.Mutex

	fmt.Printf("[%s] Raft %d status is %s and votedFor is %v at term %d \n", time.Now().Format("2006-01-02 15:04:00.000"), rfCp.me, rfCp.status, rfCp.votedFor, rfCp.currentTerm)
	fmt.Printf("[%s] Raft: %d start election at term %d \n", time.Now().Format("2006-01-02 15:04:00.000"), rfCp.me, rfCp.currentTerm)
	for index := range rfCp.peers {
		if index == rfCp.me {
			continue
		}
		fmt.Printf("[%s] raft %d sending requestvote to raft %d and {args.Term: %v, args.CandidateId: %v, args.LastLogIndex: %v, args.LastLogTerm: %d}\n",
			time.Now().Format("2006-01-02 15:04:00.000"), rfCp.me, index, args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm)

		// wg.Add(1)
		go func(index int, rafts int) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(index, &args, &reply)
			if !ok {
				fmt.Printf("[%s] raft %d receive no reply from raft %d at term %d \n", time.Now().Format("2006-01-02 15:04:00.000"), rfCp.me, index, rfCp.currentTerm)
			}
			fmt.Printf("[%s] Raft %d Receive Reply from raft %d at term %d and Reply {reply.Term: %d, reply.VoteGranted: %v} \n",
				time.Now().Format("2006-01-02 15:04:00.000"), rfCp.me, index, rfCp.currentTerm, reply.Term, reply.VoteGranted)
			localMutex.Lock()
			resChan = append(resChan, reply)
			if len(resChan) == rafts-1 {
        finalChan <-true
			}
			localMutex.Unlock()
		}(index, rafts)
	}

	for {
		select {
		case <-electionTimeOut.C:
		case <-finalChan:
			rf.mu.Lock()
			rf.electionTimeOut = *time.NewTimer(time.Duration((50 + (rand.Int63() % 300))) * time.Millisecond)
			maxTerm := rf.currentTerm
			rf.mu.Unlock()
			localMutex.Lock()
			for _, reply_ := range resChan {
				rf.currentTerm = Max(reply_.Term, maxTerm)
				if reply_.VoteGranted {
					fmt.Printf("[%s] Raft %d Receive Reply from Reply {reply.Term: %d, reply.VoteGranted: %v} \n",
						time.Now().Format("2006-01-02 15:04:00.000"), rfCp.me, reply_.Term, reply_.VoteGranted)
					atomic.AddInt32(&voteCount, 1)
				}
			}
			localMutex.Unlock()
			rf.mu.Lock()
			if maxTerm > rf.currentTerm {
				rf.status = Follower
				rf.votedFor = nil
				rf.mu.Unlock()
				return
			}
			fmt.Printf("[%s] Result of vote request raft: %v get %v votes \n", time.Now().Format("2006-01-02 15:04:00.000"), rf.me, voteCount)
			if voteCount >= (int32)(len(rf.peers)/2) {
				rf.status = Leader
				rf.votedFor = nil
				fmt.Printf("[%s] Raft %d election succeed and  status become %s at term %d \n", time.Now().Format("2006-01-02 15:04:00.000"), rf.me, rf.status, rf.currentTerm)
				rf.mu.Unlock()
				return
			} else {
				rf.status = Follower
				rf.votedFor = nil
				fmt.Printf("[%s] Raft %d failed in election and status become to %s \n", time.Now().Format("2006-01-02 15:04:00.000"), rf.me, rf.status)
				rf.mu.Unlock()
				return
			}
		}
	}
}

func (rf *Raft) heartBeat() {
	raftCopy := Raft{}
	rf.mu.Lock()
	DeepCopy(rf, &raftCopy)
	rf.mu.Unlock()
	if raftCopy.status == Leader {
		// fmt.Printf("[%s] raft %d is %s and still send heartbeat.... \n", time.Now().Format("2006-01-02 15:04:00.000"), rf.me, rf.status)
		for index := range rf.peers {
			if index == rf.me {
				continue
			}
			args := RequestAppendEntriesArgs{
				Term:        raftCopy.currentTerm,
				LeaderId:    raftCopy.me,
				Entries:     make([]interface{}, 0),
				LederCommit: raftCopy.commitIndex,
			}
			go func(index int) {
				reply := RequestAppendEntriesReply{}
				fmt.Printf("[%s] Raft %d start sending heartBeat to Raft %d and args is {Term: %d, LeaderId: %d } \n", time.Now().Format("2006-01-02 15:04:00.000"), rf.me, index, args.Term, args.LeaderId)
				ok := rf.sendRequestAppendEntries(index, &args, &reply)
				if !ok {
					fmt.Printf("[%s] Raft %d recevive heartBeat from raft %d occur error \n", time.Now().Format("2006-01-02 15:04:00.000"), rf.me, index)
					return
				}
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					if rf.status == Leader {
						rf.status = Follower
					}
					rf.currentTerm = Max(reply.Term, rf.currentTerm)
					rf.votedFor = nil
				}
				rf.mu.Unlock()
			}(index)
		}
	}
}

func DeepCopy(rf *Raft, resRaft *Raft) {
	resRaft.status = rf.status
	resRaft.currentTerm = rf.currentTerm
	resRaft.me = rf.me
	// resRaft.peers = rf.peers
	peers := make([]*labrpc.ClientEnd, len(rf.peers))
	for index, peer := range rf.peers {
		peers[index] = peer
	}
	resRaft.peers = peers
	return
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	// initialization 2A
	rf.status = Follower
	rf.currentTerm = 0
	rf.votedFor = nil
	rf.logs = make([]interface{}, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, 0)
	rf.matchIndex = make([]int, 0)
	rf.leaderedTick = *time.NewTicker(time.Duration(HeartBeatTimeOut) * time.Millisecond)
	rf.heartBeatTimer = *time.NewTicker(time.Duration(HeartBeatTimeOut) * time.Millisecond)
	rf.electionTimeOut = *time.NewTimer(time.Duration(50+rand.Int63()%300) * time.Millisecond)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.leadered = false

	go rf.heartBeat()
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.tickInPowerByLeader()

	return rf
}
