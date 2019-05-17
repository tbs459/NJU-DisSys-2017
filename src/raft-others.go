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

import "sync"
import "labrpc"

import "bytes"
import "encoding/gob"
import "time"
import "math/rand"



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

type LogEntry struct {
	Term 		int
	Command		interface{}
}

type ServerState string

const(
	Leader ServerState = "Leader"
	Follower ServerState = "Follower"
	Candidate ServerState = "Candidate"
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	
	state	ServerState
	currentTerm	int
	votedFor		int
	log	[]LogEntry

	commitIndex	int
	lastApplied	int

	nextIndex	[]int
	matchIndex	[]int

	heartbeat	bool
	applyCh 	chan ApplyMsg

	votes		int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool
	// Your code here.
	term = rf.currentTerm
	if rf.state == Leader {
		isLeader = true
	} else {
		isLeader = false
	}
	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
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
	// Your code here.
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	isLogMatch := rf.isLogMatch(args)
	switch rf.state {
	case Follower:
		if args.Term > rf.currentTerm {
			// If this peer has a lower election term than that of the candidate, update peer's term
			rf.currentTerm = args.Term
			rf.votedFor = -1
			if isLogMatch {
				// grant vote if the candidate’s log is at least as up-to-date as receiving peer's log
				rf.votedFor = args.CandidateId
				rf.heartbeat = true
				reply.VoteGranted = true
			}
		} else if args.Term == rf.currentTerm {
			// If votedFor is null or candidateId, and candidate’s log is at least as up-to-date 
			// as receiver’s log, grant vote
			if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && isLogMatch {
				rf.votedFor = args.CandidateId
				rf.heartbeat = true
				reply.VoteGranted = true
			}
		}
	case Candidate, Leader:
		if args.Term > rf.currentTerm {
			// If the term maintained by a 'non-follower' peer is lower than the candidate, 
			// the peer will update its term to match the candidate's and will then become a follower
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.RunServerLoopAsFollower()
		}
	}
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
// returns true if labrpc says the RPC was delivered.
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

func (rf *Raft) handleSendRequestVote(i int) {
	args := RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.LastLogIndex = len(rf.log) -1
	args.LastLogTerm = rf.log[len(rf.log)-1].Term
	args.CandidateId = rf.me
	reply := &RequestVoteReply{}
	if rf.sendRequestVote(i, args, reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.state == Candidate {
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.persist()
				rf.RunServerLoopAsFollower()
				return
			} else if reply.VoteGranted {
				rf.votes++
			}
			if rf.votes >= len(rf.peers)/2+1 {
				rf.RunServerLoopAsLeader()
				return
			}
		}
	}
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
	rf.mu.Lock()
	index := -1
	term,isLeader := rf.GetState()
	if isLeader {
		entry := LogEntry{term,command}
		rf.log = append(rf.log, entry)
		rf.persist()
		index = len(rf.log) - 1
	}
	rf.mu.Unlock()
	return index, term, isLeader
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

	// Your initialization code here.
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{LogEntry{0,nil}}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = nil
	rf.matchIndex = nil
	rf.heartbeat = false
	rf.applyCh = applyCh
	rf.votes = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.ServerLoop()
	return rf
}

func (rf *Raft) ServerLoop() {
	// Server loop contains switch statement, checking whether server state is 'follower', 'candidate' or 'leader'
	switch rf.state {
		case Follower:
			for rf.state == Follower {
				// Follower sets a timer. If it hasn't received a heartbeat from the leader before the timeour, it will
				// become a candidate. Timeout is set randomly between 150 and 300 ms by the randomNumberGenerator() function.
				timer := time.NewTimer(time.Duration(randomNumberGenerator()) * time.Millisecond)
				// wait for timer to timeout
				<-timer.C
				// if no heartbeat received from leader within timeout period, server switches state to candidate.
				// it will then exit the switch statement and will re-enter with the state of 'Candidate'
				if rf.state == Follower && !rf.heartbeat {
					rf.RunServerLoopAsCandidate()
				}
				rf.heartbeat = false
			}
		case Candidate:
			for rf.state == Candidate {
				// Candidate starts new election term and votes for itself
				rf.mu.Lock()
				rf.currentTerm++
				rf.votedFor = rf.me
				rf.persist()
				rf.votes = 1
				// Candidate sends vote requests to other peers (except self)
				for i := 0; i < len(rf.peers); i++ {
					if i != rf.me {
						go rf.handleSendRequestVote(i)
					}
				}
				rf.mu.Unlock()
				// Election timer is set
				timer := time.NewTimer(time.Duration(randomNumberGenerator()) * time.Millisecond)
				// Wait for timer to timeout before running new election
				<-timer.C
			}
		case Leader:
			for rf.state == Leader {
				// Upon election: send initial empty AppendEntries RPCs (heartbeat) 
				// to each server; repeat during idle periods to prevent election timeouts 
				for i := 0; i < len(rf.peers); i++ {
					if i != rf.me {
						go rf.SendHeartBeat(i)
					}
				}
				rf.CalulateCommitIndex()
				timer := time.NewTimer(time.Duration(200) * time.Millisecond)
				go rf.CommitLogs()
				<-timer.C
			}
		}
}

// If there exists an N such that N > commitIndex, a majority
// of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
func (rf *Raft) CalulateCommitIndex() {
	for N := len(rf.log) - 1; N > rf.commitIndex; N-- {
		if rf.log[N].Term == rf.currentTerm {
			count := 0
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me && rf.matchIndex[i] >= N {
					count++
				}
			}
			if count >= len(rf.peers)/2 {
				rf.commitIndex = N
				return
			}
		}
	}
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm	int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success	bool
	NextIndex	int
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.RunServerLoopAsFollower()
	}
	// Reply false if term < currentTerm
	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	reply.Success = false
	rf.heartbeat = true
	if args.Term < rf.currentTerm {
		reply.NextIndex = rf.log[len(rf.log)-1].Term
	} else if args.PrevLogIndex >= len(rf.log) {
		reply.NextIndex = len(rf.log)
	} else if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		for i := args.PrevLogIndex -1; i >= 0; i-- {
			if rf.log[i].Term != rf.log[args.PrevLogIndex].Term {
				reply.NextIndex = i + 1
				break
			}
		}
	} else {
		if len(rf.log) > args.PrevLogIndex +1 {
			rf.log = rf.log[:args.PrevLogIndex+1]
		}
		// Append any new entries not already in the log
		rf.log = append(rf.log, args.Entries...)
		// term is not < currentTerm, and log containts entry at prevLogIndex whose term matches prevLogTerm.
		// Therefore, reply true.
		reply.Success = true
		reply.NextIndex = len(rf.log)

		// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit < len(rf.log)-1 {
				rf.commitIndex = args.LeaderCommit
			} else { rf.commitIndex = len(rf.log)-1 }
		}
		// Commit logs once commit index has been updated
		go rf.CommitLogs()
	}
	reply.Term = rf.currentTerm
}

func (rf *Raft) CommitLogs() {
	rf.mu.Lock()
	commitIndex := rf.commitIndex
	for i := rf.lastApplied + 1; i <= commitIndex; i++ {
		msg := ApplyMsg{Index: i, Command: rf.log[i].Command}
		rf.applyCh <- msg
		rf.lastApplied = i
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) SendHeartBeat(i int) {
	payload := AppendEntriesArgs{}
	payload.Term = rf.currentTerm
	payload.LeaderId = rf.me
	payload.PrevLogIndex = rf.nextIndex[i] - 1
	payload.PrevLogTerm = rf.log[payload.PrevLogIndex].Term
	payload.Entries = rf.log[rf.nextIndex[i]:]
	payload.LeaderCommit = rf.commitIndex
	reply := &AppendEntriesReply{}
	
	if rf.sendAppendEntries(i, payload, reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.state == Leader {
			// Update index if leaders term is at least that of reply
			if reply.Term <= rf.currentTerm {
				rf.nextIndex[i] = reply.NextIndex
				if reply.Success {
					rf.matchIndex[i] = reply.NextIndex - 1
				}
			} else {
				// Leader becomes follower if its term is behind responding peer. It updates term, persists 
				// and becomes a follower
				rf.currentTerm = reply.Term
				rf.persist()
				rf.RunServerLoopAsFollower()
			}
		}
	}
}

// Utility Methods
func (rf *Raft) isLogMatch(args RequestVoteArgs) bool {
	if rf.log[len(rf.log)-1].Term != args.LastLogTerm {
		if rf.log[len(rf.log)-1].Term < args.LastLogTerm {
			return true
		}
		return false
	} else {
		if len(rf.log) - 1 <= args.LastLogIndex {
			return true
		}
		return false
	}
}

func randomNumberGenerator() int {
	return rand.Intn(500) + 500
}

func (rf *Raft) RunServerLoopAsFollower(){
	rf.state = Follower
	rf.heartbeat = false
	go rf.ServerLoop()
}

func (rf *Raft) RunServerLoopAsCandidate(){
	rf.state = Candidate
	go rf.ServerLoop()
}

func (rf *Raft) RunServerLoopAsLeader(){
	rf.state = Leader
	rf.nextIndex = make([]int,len(rf.peers))
	rf.matchIndex = make([]int,len(rf.peers))
	for i:=0; i<len(rf.peers); i++ {
		if i != rf.me {
			rf.nextIndex[i] = len(rf.log)
			rf.matchIndex[i] = 0
		}
	}
	go rf.ServerLoop()
}





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

import "sync"
import "labrpc"
import "bytes"
import "encoding/gob"
import "time"
import "math/rand"


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

type LogEntry struct {
	Term 		int
	Command		interface{}
}
type ServerState string
const(
	Leader ServerState = "Leader"
	Follower ServerState = "Follower"
	Candidate ServerState = "Candidate"
)
//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]
	state	ServerState
	currentTerm	int
	votedFor		int
	heartbeat	bool
	votes		int
	//currentLeader  int
	log	    []LogEntry
	nextIndex	[]int
	matchIndex	[]int
	commitIndex	int
	lastApplied	int
	applyCh 	chan ApplyMsg
	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool
	// Your code here.
	term=rf.currentTerm
	if rf.state == Leader {
		isLeader = true
	} else {
		isLeader = false
	}
	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
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
	// Your code here.
	// Example:
	 r := bytes.NewBuffer(data)
	 d := gob.NewDecoder(r)
	 d.Decode(&rf.currentTerm)
	 d.Decode(&rf.votedFor)
	 d.Decode(&rf.log)
}




//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term int
	VoteGranted bool   
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	isLogMatch := rf.isLogMatch(args)
    if rf.state==Follower{
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = -1
			if isLogMatch {
				rf.votedFor = args.CandidateId
				rf.heartbeat = true
				reply.VoteGranted = true
				}
		} else if args.Term == rf.currentTerm {
			if (rf.votedFor == -1 || rf.votedFor == args.CandidateId)&& isLogMatch{
				rf.votedFor = args.CandidateId
				rf.heartbeat = true
				reply.VoteGranted = true
			}
		}
	}
	if (rf.state==Candidate)||(rf.state==Leader){
	    if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = -1
			//rf.RunServerLoopAsFollower()
			rf.state = Follower
			rf.heartbeat = false
			go rf.ServerLoop()
		}
		}	
}		
  // Your code here.
 // currentTerm, _ := rf.GetState()
 // if args.Term < currentTerm {
 //   reply.Term = currentTerm
   // reply.VoteGranted = false
  //  return      
//  }             
 // if rf.votedFor != -1 && args.Term <= rf.currentTerm {
 //   reply.VoteGranted = false
 //   rf.mu.Lock()
  //  if args.Term>currentTerm{
//	rf.currentTerm=args.Term
	//}else{
	//rf.currentTerm=currentTerm
	//}
  //  reply.Term = rf.currentTerm
  //  rf.mu.Unlock()
 // } else {      
  //  rf.mu.Lock()
//	if args.Term>currentTerm{
//	rf.currentTerm=args.Term
//	}else{
//	rf.currentTerm=currentTerm
//	}
//	rf.votedFor=args.CandidateId
  //  rf.mu.Unlock()
 //   reply.VoteGranted = true
 // }             
	


//心跳
type AppendEntriesArgs struct {
	Term int
	LeaderId int
	Entries []LogEntry
	PrevLogIndex int
	PrevLogTerm	int
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success	bool
	NextIndex	int
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		//rf.RunServerLoopAsFollower()
		rf.state = Follower
		rf.heartbeat = false
		go rf.ServerLoop()
	}
	reply.Success = false
	rf.heartbeat = true
	
	if args.Term < rf.currentTerm {
		reply.NextIndex = rf.log[len(rf.log)-1].Term
	} else if args.PrevLogIndex >= len(rf.log) {
		reply.NextIndex = len(rf.log)
	} else if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		for i := args.PrevLogIndex -1; i >= 0; i-- {
			if rf.log[i].Term != rf.log[args.PrevLogIndex].Term {
				reply.NextIndex = i + 1
				break
			}
		}
	} else {
		if len(rf.log) > args.PrevLogIndex +1 {
			rf.log = rf.log[:args.PrevLogIndex+1]
		}
		// Append any new entries not already in the log
		rf.log = append(rf.log, args.Entries...)
		// term is not < currentTerm, and log containts entry at prevLogIndex whose term matches prevLogTerm.
		// Therefore, reply true.
		reply.Success = true
		reply.NextIndex = len(rf.log)
		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit < len(rf.log)-1 {
				rf.commitIndex = args.LeaderCommit
			} else { rf.commitIndex = len(rf.log)-1 }
		}
		// Commit logs once commit index has been updated
		go rf.CommitLogs()
	}
	reply.Term = rf.currentTerm
}
func (rf *Raft) CommitLogs() {
	rf.mu.Lock()
	commitIndex := rf.commitIndex
	for i := rf.lastApplied + 1; i <= commitIndex; i++ {
		msg := ApplyMsg{Index: i, Command: rf.log[i].Command}
		rf.applyCh <- msg
		rf.lastApplied = i
	}
	rf.mu.Unlock()
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
// returns true if labrpc says the RPC was delivered.
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
func (rf *Raft) handleSendRequestVote(i int) {
	args := RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.LastLogIndex = len(rf.log) -1
	args.LastLogTerm = rf.log[len(rf.log)-1].Term
	args.CandidateId = rf.me
	reply := &RequestVoteReply{}
	if rf.sendRequestVote(i, args, reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.state == Candidate {
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.persist()
				//rf.RunServerLoopAsFollower()
				rf.state = Follower
	            rf.heartbeat = false
				go rf.ServerLoop()
				return
			} else if reply.VoteGranted {
				rf.votes++
			}
			if rf.votes >= len(rf.peers)/2+1 {
				//rf.RunServerLoopAsLeader()
				rf.state = Leader
				rf.nextIndex = make([]int,len(rf.peers))
				rf.matchIndex = make([]int,len(rf.peers))
				for i:=0; i<len(rf.peers); i++ {
					if i != rf.me {
					rf.nextIndex[i] = len(rf.log)
					rf.matchIndex[i] = 0
					}
				}
				//rf.heartbeat=true
				go rf.ServerLoop()
				return
			}
		}
	}
}
func (rf *Raft) SendHeartBeat(i int) {
	payload := AppendEntriesArgs{}
	payload.Term = rf.currentTerm
	payload.LeaderId = rf.me
	payload.PrevLogIndex = rf.nextIndex[i] - 1
	payload.PrevLogTerm = rf.log[payload.PrevLogIndex].Term
	payload.Entries = rf.log[rf.nextIndex[i]:]
	payload.LeaderCommit = rf.commitIndex
	reply := &AppendEntriesReply{}
	if rf.sendAppendEntries(i, payload, reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.state == Leader {
			if reply.Term >rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.persist()
				//rf.RunServerLoopAsFollower()
				rf.state = Follower
	            rf.heartbeat = false
	            go rf.ServerLoop()
			}else{
				rf.nextIndex[i] = reply.NextIndex
				if reply.Success {
					rf.matchIndex[i] = reply.NextIndex - 1
				}
			}
		}
	}
}
func (rf *Raft) isLogMatch(args RequestVoteArgs) bool {
	if rf.log[len(rf.log)-1].Term != args.LastLogTerm {
		if rf.log[len(rf.log)-1].Term < args.LastLogTerm {
			return true
		}
		return false
	} else {
		if len(rf.log) - 1 <= args.LastLogIndex {
			return true
		}
		return false
	}
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
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
    rf.mu.Lock()
	index := -1
	term,isLeader := rf.GetState()
	if isLeader {
		entry := LogEntry{term,command}
		rf.log = append(rf.log, entry)
		rf.persist()
		index = len(rf.log) - 1
	}
    rf.mu.Unlock()
	return index, term, isLeader
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

	// Your initialization code here.
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.heartbeat = false
	rf.votes = 0
	//rf.currentLeader=-1
    rf.log = []LogEntry{LogEntry{0,nil}}
	rf.nextIndex = nil
	rf.matchIndex = nil
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

    go rf.ServerLoop()
	return rf
}
func (rf *Raft) ServerLoop() {
	if rf.state==Follower{
			for rf.state == Follower {
				timer := time.NewTimer(time.Duration(rand.Intn(500) + 500) * time.Millisecond)
				<-timer.C
				if rf.state == Follower && !rf.heartbeat {
					//rf.RunServerLoopAsCandidate()
					rf.state = Candidate
					//rf.heartbeat=true
					go rf.ServerLoop()
				}
				rf.heartbeat = false
			}
		}
	if rf.state==Candidate{
			for rf.state == Candidate {
				rf.mu.Lock()
				rf.currentTerm++
				rf.votedFor = rf.me
				rf.persist()
				rf.votes = 1
				for i := 0; i < len(rf.peers); i++ {
					if i != rf.me {
						go rf.handleSendRequestVote(i)
					}
				}
				rf.mu.Unlock()
				timer := time.NewTimer(time.Duration(rand.Intn(500) + 500) * time.Millisecond)
				<-timer.C
			}
		}
	if rf.state==Leader{
			for rf.state == Leader {
				for i := 0; i < len(rf.peers); i++ {
					if i != rf.me {
						go rf.SendHeartBeat(i)
					}
				}
				rf.CalulateCommitIndex()
				timer := time.NewTimer(time.Duration(200) * time.Millisecond)
				go rf.CommitLogs()
				<-timer.C
			}
		}	
}
func (rf *Raft) CalulateCommitIndex() {
	for N := len(rf.log) - 1; N > rf.commitIndex; N-- {
		if rf.log[N].Term == rf.currentTerm {
			count := 0
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me && rf.matchIndex[i] >= N {
					count++
				}
			}
			if count >= len(rf.peers)/2 {
				rf.commitIndex = N
				return
			}
		}
	}
}
