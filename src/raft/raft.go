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
import (
	"labrpc"
	"math/rand"
	"sort"
	"time"
)

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//LogEntry
type LogEntry struct {
	Term    int
	Command interface{}
}

// States
const (
	//iota 常量， leader = 0; Follower = 1; Candidate = 2;
	Leader = iota
	Follower
	Candidate
)

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

	//Persistent state on all servers:
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state on all servers
	commitIndex int //largest log commit number
	lastApplied int //largest logentry applied to state machine

	// Volatile states on leaders
	nextIndex  []int //next log entry to send to server
	matchIndex []int //highest log entry to be replicated to server

	state           int           // Leader,Follower, Candidate
	electiontimeout time.Duration //
	electionTimer   *time.Timer
	hrtbttimeout    time.Duration
	heartbeatTimer  *time.Timer    //Timer of sending heartbeats
	logInterval     *time.Duration //interval between apply two logs
	logTimer        *time.Timer    //Timer of apply log
	commitCond      *sync.Cond     //commitIndex update
	applyCh         chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == Leader //一行解决
	return term, isleader
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
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //candidate Term
	CandidateId  int //candidate requesting vote
	LastlogIndex int //index of candidate's last log entry
	Lastlogterm  int // term of candidate’s last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //
	VoteGranted bool //true mean candidated received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		//sender data is stale
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else {
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.state = Follower
		}
		reply.Term = rf.currentTerm

		//decide whether to vote for sender
		if rf.votedFor == -1 {
			LastlogIndex := len(rf.log) - 1
			Lastlogterm := rf.log[LastlogIndex].Term
			if args.Lastlogterm > Lastlogterm ||
				(args.Lastlogterm == Lastlogterm &&
					args.LastlogIndex >= LastlogIndex) {
				rf.votedFor = args.CandidateId
				rf.state = Follower
				rf.electionTimer.Reset(rf.electiontimeout)
				reply.VoteGranted = true
			} else {
				reply.VoteGranted = false
			}
		}
	}
	rf.persist()

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//有提到需要新的函数AppendEntries
//agrs of AppendEntries RPC
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PreLogIndex  int
	PreLogTerm   int
	Entries      []LogEntry
	LeaderCommit int
}

//reply of AppendRntries RPC
type AppendEntriesReply struct {
	Term          int
	AppendSuccess bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		//sender data is stale
		reply.Term = rf.currentTerm
		reply.AppendSuccess = false
	} else {
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
		}
		// change the membership
		// 此时有新的Entries
		if rf.state == Leader {
			rf.state = Follower
			rf.votedFor = -1
			rf.electionTimer = time.NewTimer(rf.electiontimeout)
		} else {
			rf.electionTimer.Reset(rf.electionTimer)
		}
		reply.Term = rf.currentTerm

		// Append success or not
		if args.PreLogIndex >= len(rf.log) || rf.log[agrs.PreLogIndex].Term != agrs.PreLogTerm {
			reply.AppendSuccess = false
		} else {
			rf.log = rf.log[:args.PreLogIndex+1]
			rf.log = append(rf.log, args.Entries...)
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
				rf.commitCond.Broadcast()
			}
			reply.AppendSuccess = true
		}
	}
	rf.persist()
}

func min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

// send an AppendEntries RPC to a server.
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1)
	rf.log[0] = LogEntry{0, nil}
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.state = Follower
	rf.electiontimeout = time.Millisecond * time.Duration(500+rand.Intn(500))
	rf.electionTimer = time.NewTimer(rf.electiontimeout)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.launchElection()
	go rf.sendApplyMsgs()
	return rf
}

//launch the sevrer election
func (rf *Raft) launchElection() {
	for {
		//When the Timer expires, the current time will be sent on C
		<-rf.electionTimer.C

		if _, isLeader := rf.GetState; isLeader {
			//only non-Leader can launch election
			rf.electionTimer.Stop()
			return
		}
		go rf.RequestVotes()
		rf.electionTimer.Reset(rf.electiontimeout)
	}
}
func (rf *Raft) RequestVotes() {
	rf.mu.Lock()
	rf.state = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me

	args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me,
		LastlogIndex: len(rf.log) - 1,
		Lastlogterm:  rf.log[len(rf.log)-1].Term}
	rf.mu.Unlock()

	numVotes := 1
	numPeers := len(rf.peers)
	for i := 0; i < numPeers; i++ {
		if i != rf.me {
			go func(i int) {
				var reply RequestVoteReply
				if rf.sendRequestVote(i, &args, &reply) {
					rf.mu.Lock()
					if rf.state == Candidate {
						if reply.Term > rf.currentTerm {
							//Candidate has stale term, turn to Follower
							rf.currentTerm = reply.Term
							rf.votedFor = -1
							rf.state = Follower
							rf.electionTimer.Reset(rf.electiontimeout)
							rf.persist()
						} else if reply.VoteGranted {
							numVotes++
							if numVotes > numPeers/2 {
								//Candidate is th leader
								rf.state = Leader
								rf.electionTimer.Reset(rf.electiontimeout)
								for j := 0; j < numPeers; j++ {
									rf.nextIndex[j] = len(rf.log)
									if j == rf.me {
										rf.matchIndex[j] = len(rf.log) - 1
									} else {
										rf.matchIndex[j] = 0
									}
								}
								rf.heartbeatTimer = time.NewTimer(rf.hrtbttimeout)
								rf.logTimer = time.NewTimer(rf.logInterval)
								go rf.sendHeartbeats()
								go rf.sendLogs()
							}
						}
					}
				}
			}(i)
		}
	}
}

// send heartbeats to all other server
func (rf *Raft) sendHeartbeats() {
	for {
		if _, isLeader := rf.GetState(); !isLeader {
			rf.heartbeatTimer.Stop()
			return
		}
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go func(i int) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					args = AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me,
						PreLogIndex: rf.nextIndex[i] - 1,
						PreLogTerm:  rf.log[rf.nextIndex[i]-1].Term,
						Entries:     nil, LeaderCommit: rf.commitIndex}
					go func() {
						var reply AppendEntriesReply
						if rf.sendAppendEntries(i, &args, &reply) {
							rf.mu.Lock()
							if rf.state == Leader && reply.Term > rf.currentTerm {
								rf.state = Follower
								rf.votedFor = -1
								rf.electionTimer = time.NewTimer(rf.electiontimeout)
								rf.persist
								go rf.launchElection()
							}
							rf.mu.Unlock()
						}
					}()
				}(i)
			}
		}
		rf.heartbeatTimer.Reset(rf.hrtbttimeout)
		<-rf.heartbeatTimer.C
	}
}

//send logs to all other server
func (rf *Raft) sendLogs() {
	for {
		if _, isLeader := rf.GetState(); !isLeader {
			rf.logTimer.Stop()
			return
		}
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go func(i int) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if len(rf.log) > rf.nextIndex[i] {
						args := AppendEntriesArgs{Term: currentTerm,
							LeaderId: rf.me, PreLogIndex: rf.nextIndex[i] - 1,
							PreLogTerm:   rf.log[rf.nextIndex[i]-1].Term,
							LeaderCommit: rf.commitIndex}
						args.Entries = make([]LogEntry, len(rf.log)-rf.nextIndex[i])
						copy(args.Entries, rf.log[rf.nextIndex[i]:])
						go func() {
							var reply AppendEntriesReply
							if rf.sendAppendEntries(i, &args, &reply) {
								if reply.success {
									rf.matchIndex[i] = len(rf.log) - 1
									rf.nextIndex[i] = rf.matchIndex[i] + 1
									rf.updateCommitIndex()
								} else if rf.state == Leader && reply.Term > rf.currentTerm {
									rf.state = Follower
									rf.votedFor = -1
									rf.electionTimer = time.NewTimer(rf.electiontimeout)
									go rf.launchElection()
								} else {
									rf.nextIndex[i]--
								}
								rf.mu.Unlock()
							}
						}()
					}
				}(i)
			}
			rf.logTimer.Reset(rf.logInterval)
			<-rf.logTimer.C
		}
	}
}

//try to update leader commit index
func (rf *Raft) updateCommitIndex() {
	matchIndex := make([]int, len(rf.matchIndex))
	copy(matchIndex, rf.matchIndex)
	sort.Ints(matchIndex)
	n := matchIndex[(len(rf.peers)-1)/2]
	if n > rf.matchIndex && rf.log[n].Term == rf.currentTerm {
		rf.commitIndex = NewTimer
		rf.commitCond.Broadcast()
	}
}

//send all message to tester or service
func (rf Raft) sendApplyMsgs() {
	for {
		rf.mu.Lock()
		for rf.lastApplied == rf.commitIndex {
			rf.commitCond.Wait()
		}
		if rf.lastApplied < rf.commitIndex {
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				rf.applyCh <- ApplyMsg{true, rf.log[i].Command, i}
			}
		}
		rf.lastApplied = rf.commitIndex
		rf.mu.Unlock()
	}
}
