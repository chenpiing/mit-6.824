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
	"bytes"
	"labgob"
	"labrpc"
	"math/rand"
	"sync"
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

type Status int

const (
	FOLLOWER  Status = 0000
	CANDIDATE Status = 1000
	LEADER    Status = 2000

	TIMEOUT_MIN        int64 = 200
	TIMEOUT_INTERVAL   int64 = 300
	HEARTBEAT_INTERVAL int64 = 60
)

type LogEntry struct {
	Command      interface{}
	CommandTerm  int
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// persistent state
	currentTerm int
	votedFor    int
	log         []*LogEntry

	// volatile state
	role      Status // 0-follower 1-candidate 2-leader
	commitIndex int
	lastApplied int

	nextIndex       []int
	matchIndex      []int
	gatherVotes     int
	cond            *sync.Cond
	elapse          int64
	wakeupFromSleep bool

	timeout   int64
	sleeping  bool
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.Lock()
	term := rf.currentTerm
	isleader := (rf.role == LEADER && rf.votedFor == rf.me)
	rf.Unlock()

	// Your code here (2A).
	return term, isleader
}

func (rf *Raft) GetIndex() int {
	rf.Lock()
	defer rf.Unlock()
	return len(rf.log)
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	state := w.Bytes()

	wLog := new(bytes.Buffer)
	eLog := labgob.NewEncoder(wLog)
	eLog.Encode(rf.log)
	log := wLog.Bytes()

	rf.persister.SaveStateAndSnapshot(state, log)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte, snapshot []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []*LogEntry
	if d.Decode(&currentTerm) == nil && d.Decode(&votedFor) == nil && d.Decode(&log) == nil {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

func (rf *Raft) Lock() {
	rf.mu.Lock()
}

func (rf *Raft) Unlock() {
	rf.mu.Unlock()
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.Lock()
	// DPrintf("node=%v, term=%v receive rv req from node=%v term=%v", rf.me, rf.currentTerm, args.CandidateId, args.Term)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = FOLLOWER
		rf.votedFor = -1
	} else if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		DPrintf("node=%v, term=%v receive rv req from node=%v term=%v, reply=(%v, %v)", rf.me, rf.currentTerm, args.CandidateId, args.Term, reply.Term, reply.VoteGranted)
		rf.Unlock()
		return
	}

	ok := (rf.votedFor == -1 || rf.votedFor == args.CandidateId)
	if ok {
		rf.votedFor = args.CandidateId
		rf.role = FOLLOWER
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.interrupteSleepNoLock()
		rf.Unlock()
		DPrintf("node=%v, term=%v receive rv req from node=%v term=%v, reply=(%v, %v)", rf.me, rf.currentTerm, args.CandidateId, args.Term, reply.Term, reply.VoteGranted)
		return
	}
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	DPrintf("node=%v, term=%v receive rv req from node=%v term=%v, reply=(%v, %v)", rf.me, rf.currentTerm, args.CandidateId, args.Term, reply.Term, reply.VoteGranted)
	rf.Unlock()
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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	Commands     []*LogEntry
	PreLogIndex  int
	PreLogTerm   int
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.Lock()
	DPrintf("node=%v term=%v receive ac req from node=%v term=%v", rf.me, rf.currentTerm, args.LeaderId, args.Term)
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
	} else {
		reply.Term = rf.currentTerm
		rf.role = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
		if args.PreLogIndex < 0 {
			rf.log = args.Commands
			reply.Success = true
			rf.interrupteSleepNoLock()
			rf.Unlock()
			return
		}

		if len(rf.log)-1 < args.PreLogIndex {
			reply.Success = false
			rf.interrupteSleepNoLock()
			rf.Unlock()
			return
		}

		if rf.log[args.PreLogIndex].CommandTerm != args.PreLogTerm {
			reply.Success = false
			// 删掉当前server的log的第 args.PreLogIndex 及其之后的记录
			if args.PreLogIndex == 0 {
				rf.log = nil
			} else {
				rf.log = rf.log[0 : args.PreLogIndex-1]
			}
			rf.interrupteSleepNoLock()
			rf.Unlock()
			return
		}

		rf.log = append(rf.log[0:args.PreLogIndex], args.Commands...)
		reply.Success = true
		rf.interrupteSleepNoLock()
	}
	rf.Unlock()
}

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
	term, isLeader := rf.GetState()
	index := rf.GetIndex()
	if !isLeader {
		return index, term, isLeader
	}

	// Your code here (2B).
	// send AppendEntries rpc
	rf.Lock()
	id := len(rf.log)
	rf.log = append(rf.log, &LogEntry{command, rf.currentTerm, id})
	rf.doAppendEntries()
	rf.cond.Wait()
	rf.Unlock()
	return id, term, isLeader
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

func initList(len int, value int) []int {
	arr := make([]int, len)
	for i := 0; i < len; i++ {
		arr[i] = value
	}
	return arr
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
	rf.role = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.cond = sync.NewCond(&rf.mu)
	rf.nextIndex = initList(len(peers), 0)
	rf.matchIndex = initList(len(peers), -1)

	DPrintf("Make Raft instance, peers=%v, me=%v, role=%v", peers, me, rf.role)

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	// rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())

	// timer routine

	go func() {
		for {
			sleepAWhile := true
			rf.Lock()
			if rf.elapse < time.Now().UnixNano() / 1e6 {
				if rf.sleeping {
					rf.sleeping = false
					rf.wakeupFromSleep = true
					rf.cond.Broadcast()
				}
				sleepAWhile = false
			}
			rf.Unlock()
			if sleepAWhile {
				time.Sleep(time.Millisecond * 20)
			} else {
				time.Sleep(time.Millisecond * 5)
			}
		}
	} ()

	// worker routine
	go func() {
		for {
			needResetTimer := true
			rf.Lock()
			// DPrintf("[0] node=%v, term=%v, role=%v", rf.me, rf.currentTerm, rf.role)
			if rf.role == FOLLOWER {
				timeout := rand.Int63n(TIMEOUT_INTERVAL) + TIMEOUT_MIN
				// DPrintf("follwer node=%v, term=%v, role=%v will sleep %v mills", rf.me, rf.currentTerm, rf.role, timeout)
				// ok := rf.ResetTimer(timeout)
				rf.sleepNoLock(timeout)
				// DPrintf("[0] node=%v, term=%v, role=%v after sleeping", rf.me, rf.currentTerm, rf.role)
				if rf.wakeupFromSleep && rf.votedFor == -1 {
					rf.role = CANDIDATE
					needResetTimer = false
					DPrintf("node=%v, term=%v, role=%v, follower convert to candidate", rf.me, rf.currentTerm, rf.role)
				}
			}

			// DPrintf("[1] node=%v, term=%v, role=%v", rf.me, rf.currentTerm, rf.role)
			if rf.role == CANDIDATE {
				if needResetTimer {
					// reset election timeout
					timeout := rand.Int63n(TIMEOUT_INTERVAL) + TIMEOUT_MIN
					// rf.ResetTimer(timeout)
					rf.sleepNoLock(timeout)
				}
				rf.currentTerm++
				rf.votedFor = rf.me
				DPrintf("node=%v, term=%v is candidate", rf.me, rf.currentTerm)
				
				curTerm := rf.currentTerm
				votes := 0
				wg := &sync.WaitGroup{}
				wg.Add(1)

				// send requestVote rpc to all other servers
				go func(curTerm *int, votes *int) {
					wg0 := &sync.WaitGroup{}
					l := sync.Mutex{}
					maxTerm := 0
					for i := 0; i < len(peers); i++ {
						if i == me {
							continue
						}
						wg0.Add(1)
						go func(other int) {
							args := RequestVoteArgs{*curTerm, me}
							reply := RequestVoteReply{}
							ok := rf.sendRequestVote(other, &args, &reply)
							if ok {
								l.Lock()
								if reply.VoteGranted {
									*votes++
								} else if reply.Term > *curTerm {
									if reply.Term > maxTerm {
										maxTerm = reply.Term
									}
								}
								l.Unlock()
							}
							wg0.Done()
						}(i)
					}
					wg0.Wait()
					if maxTerm > *curTerm {
						*curTerm = maxTerm
					}
					wg.Done()
				} (&curTerm, &votes)
				
				// DPrintf("candidate node=%v term=%v waiting for requestVote rpc result", rf.me, rf.currentTerm)
				wg.Wait()
				// DPrintf("node=%v term=%v gathered votes=%v", rf.me, rf.currentTerm, votes)
				if curTerm > rf.currentTerm {
					rf.currentTerm = curTerm
					rf.role = FOLLOWER
					rf.votedFor = -1
				} else if votes >= len(rf.peers)/2 {
					rf.role = LEADER
					// DPrintf("node=%v term=%v gathered majority votes=%v, becomes leader", rf.me, rf.currentTerm, votes)
				}
			}

			// DPrintf("[2] node=%v, term=%v, role=%v", rf.me, rf.currentTerm, rf.role)
			if rf.role == LEADER {
				// DPrintf("node=%v term=%v is leader", rf.me, rf.currentTerm)
				// send AppendEntries rpc
				rf.doAppendEntries()
				rf.sleepNoLock(HEARTBEAT_INTERVAL)
			}
			rf.Unlock()
		}
	}()

	return rf
}

func (rf *Raft) interrupteSleepNoLock() {
	if !rf.sleeping {
		return
	}
	rf.sleeping = false
	rf.wakeupFromSleep = false
	rf.cond.Broadcast()
}

func (rf *Raft) sleepNoLock(timeout int64) {
	if rf.sleeping {
		return
	}
	rf.sleeping = true
	rf.wakeupFromSleep = false
	rf.elapse = timeout + int64(time.Now().UnixNano() / 1e6)
	// DPrintf("node=%v term=%v will sleep for %v mills", rf.me, rf.currentTerm, timeout)
	rf.cond.Wait()
}

// todo count successful rpc call, if receive majority success, return true
// problem: what if majority rpc failed, what if another new client req comes when cur log hasn't been replicated by majority servers
// possible solution: 	if majority rpc failed, gives up cur try, leave it to next heartbeat rpc;
// 						if majority rpc succeeded, wakeup go chan blocking on Start method, then that go chan can just exam state and decided whether
//							reply to client
func (rf *Raft) doAppendEntries() int {
	DPrintf("leader node=%v term=%v send ae rpc to servers", rf.me, rf.currentTerm)
	wg := &sync.WaitGroup{}
	l := sync.Mutex{}
	cnt := 0
	maxTerm := rf.currentTerm
	nextIndex := make([]int, len(rf.peers))
	matchIndex := make([]int, len(rf.peers))
	finishSends := 0

	for i := 0; i < len(rf.peers); i++ {
		nextIndex[i] = rf.nextIndex[i]
	}
	for i := 0; i < len(rf.peers); i++ {
		matchIndex[i] = rf.matchIndex[i]
	}

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		lastLogIndex := len(rf.log)
		args := rf.BuildAppendEntriesArgs(i)
		go func(server int, args AppendEntriesArgs, lastLogIndex int, me int, term int, nextIndex []int, matchIndex []int) {
			// DPrintf("leader node=%v term=%v send ae rpc to node=%v", me, term, server)
			// mark done once firing up go routine
			reply := AppendEntriesReply{-1, false}
			ok := rf.sendAppendEntries(server, &args, &reply)
			// DPrintf("leader node=%v term=%v finish sending ae rpc to node=%v", me, term, server)
			l.Lock()
			if finishSends < len(rf.peers) - 1 {
				if ok {
					if !reply.Success {
						if reply.Term > maxTerm {
							maxTerm = reply.Term
						}
					} else if reply.Success {
						nextIndex[server] = lastLogIndex
						matchIndex[server] = lastLogIndex - 1
						cnt++
					}
					// DPrintf("leader node=%v term=%v receive ae rpc from node=%v, success=%v", me, term, server, reply.Success)
				} else {
					// DPrintf("node=%v term=%v send ae rpc to node=%v failed", me, term, server)
				}
				finishSends++
				wg.Done()
			}
			l.Unlock()
		} (i, args, lastLogIndex, rf.me, rf.currentTerm, nextIndex, matchIndex)
	}
	// timeout
	go func() {
		time.Sleep(time.Millisecond * 60)
		l.Lock()
		for i := finishSends; i < len(rf.peers) - 1; i++ {
			finishSends++
			wg.Done()
		}
		l.Unlock()
	} ()
	wg.Wait()
	l.Lock()
	// stoping waiting and start processing
	if maxTerm > rf.currentTerm {
		// DPrintf("leader node=%v term=%v becomes follower, new term=%v", rf.me, rf.currentTerm, maxTerm)
		rf.currentTerm = maxTerm
		rf.role = FOLLOWER
		rf.votedFor = -1
	}
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = nextIndex[i]
	}
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = matchIndex[i]
	}
	l.Unlock()
	return cnt
}

func (rf *Raft) BuildAppendEntriesArgs(server int) AppendEntriesArgs {
	nextIndex := rf.nextIndex[server]
	var appendLog []*LogEntry
	if len(rf.log) > 0 && nextIndex >= 0 {
		appendLog = rf.log[nextIndex:]
	}
	commandTerm := rf.currentTerm
	if nextIndex > 0 {
		commandTerm = rf.log[nextIndex-1].CommandTerm
	}
	arg := AppendEntriesArgs{rf.currentTerm, rf.me, appendLog, nextIndex - 1, commandTerm, rf.commitIndex}
	return arg
}
