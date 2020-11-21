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
	"labrpc"
	"log"
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

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
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

	// persistent variables
	votedFor int
	term     int
	log      []LogEntry

	// volatile state
	logStartIndex int
	commitIndex   int
	lastApplied   int
	status        int

	nextIndex  []int
	matchIndex []int

	timeChan   chan int64
	wakeupChan chan int
	resChan    chan bool
	t          int64
}

const (
	FOLLOWER  = 0
	CANDIDATE = 1
	LEADER    = 2

	MIN_SLEEP_TIME = 400
	MAX_SLEEP_TIME = 800
	HEARTBEAT_TIME = 100
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.Lock("GetState")
	defer rf.Unlock("GetState")
	// Your code here (2A).
	return rf.term, rf.status == LEADER
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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []LogEntry
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("server[%v][%v] receive ae rpc, args=%v", rf.me, rf.term, *args)
	rf.Lock("AppendEntries")
	defer rf.Unlock("AppendEntries")
	if args.Term < rf.term {
		reply.Success = false
		reply.Term = rf.term
		return
	} else if args.Term >= rf.term {
		rf.term = args.Term
		rf.status = FOLLOWER
		rf.votedFor = args.LeaderId
	}
	index, entry := rf.getEntryNonLocking(args.PrevLogIndex)
	if entry.Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.term
		if index >= 0 {
			rf.log = rf.log[0:index]
		}
	} else {
		rf.log = append(rf.log[0:index+1], args.Entries...)
		rf.commitIndex = args.LeaderCommit
		reply.Success = true
		reply.Term = rf.term
	}
	rf.StopSleep()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogTerm  int
	LastLogIndex int
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

	rf.Lock("RequestVote")
	defer rf.Unlock("RequestVote")
	if args.Term > rf.term {
		rf.term = args.Term
		rf.votedFor = -1
		rf.status = FOLLOWER
		rf.StopSleep()
	} else if args.Term < rf.term {
		reply.Term = rf.term
		reply.VoteGranted = false
		return
	}

	lastLog := rf.GetLastLogNonLocking()
	logUpToDate := args.LastLogTerm > lastLog.Term || ((lastLog.Term == args.LastLogTerm) && (args.LastLogIndex >= lastLog.Index))
	reply.VoteGranted = (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && logUpToDate
	reply.Term = rf.term
	if reply.VoteGranted && rf.votedFor == -1 {
		rf.votedFor = args.CandidateId
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
	// Your code here (2B).
	rf.Lock("Start")
	defer rf.Unlock("Start")

	if rf.status != LEADER {
		return -1, -1, false
	}
	DPrintf("leader[%v][%v] receive log=%v", rf.me, rf.term, command)
	lastLog := rf.GetLastLogNonLocking()
	entry := LogEntry{lastLog.Index + 1, rf.term, command}
	rf.log = append(rf.log, entry)

	return entry.Index, entry.Term, true
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

func (rf *Raft) initNextIndex() {
	rf.nextIndex = make([]int, len(rf.peers))
	logLen := len(rf.log)
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.logStartIndex + logLen
	}
}

func (rf *Raft) initMatchIndex() {
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) StartTimer() {
	rf.t = 0
	for {
		select {
		case rf.t = <-rf.timeChan:
			if rf.t <= 0 {
				rf.t = 0
				rf.resChan <- false
			}
		case <-rf.wakeupChan:
			rf.t = 0
			rf.resChan <- false
		default:
			if rf.t > 0 && time.Now().UnixNano()/1e6 >= rf.t {
				rf.t = 0
				rf.resChan <- true
			}
			time.Sleep(time.Duration(20) * time.Millisecond)
		}
	}
}

func (rf *Raft) StartSleep(t int64) bool {
	if t <= 0 || rf.timeChan == nil || rf.resChan == nil {
		return false
	}
	now := time.Now().UnixNano() / 1e6
	elapseTime := now + t

	rf.timeChan <- elapseTime
	// DPrintf("server[%v][%v] will sleep %v mills", rf.me, rf.term, t)
	return <-rf.resChan
}

func (rf *Raft) StopSleep() {
	if rf.wakeupChan == nil || rf.t == 0 {
		return
	}
	rf.wakeupChan <- 0
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
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.votedFor = -1
	rf.term = 0
	rf.status = FOLLOWER

	rf.timeChan = make(chan int64)
	rf.wakeupChan = make(chan int)
	rf.resChan = make(chan bool)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	if len(rf.log) == 0 {
		rf.logStartIndex = 1
	} else {
		rf.logStartIndex = rf.log[0].Term
	}

	rf.initNextIndex()
	rf.initMatchIndex()

	go rf.StartTimer()

	go func() {
		for {
			switch rf.status {
			case FOLLOWER:
				DPrintf("server[%v][%v] as follower, log=%v, lastApplied=%v, commitIndex=%v", rf.me, rf.term, rf.log, rf.lastApplied, rf.commitIndex)
				t := rand.Int63n(int64(MAX_SLEEP_TIME-MIN_SLEEP_TIME)) + int64(MIN_SLEEP_TIME)
				res := rf.StartSleep(t)
				if res {
					rf.Lock("Make.FOLLOWER case")
					rf.status = CANDIDATE
					rf.term++
					rf.Unlock("Make.FOLLOWER case")
				}
			case CANDIDATE:
				DPrintf("server[%v][%v] as candidate", rf.me, rf.term)
				// setup request votes process
				go rf.MakeRequestVotes()
				t := rand.Int63n(int64(MAX_SLEEP_TIME-MIN_SLEEP_TIME)) + int64(MIN_SLEEP_TIME)
				res := rf.StartSleep(t)
				if res {
					DPrintf("server[%v][%v] elect leader failed, timeout", rf.me, rf.term)
					rf.Lock("Make.CANDIDATE case")
					rf.status = CANDIDATE
					rf.term++
					rf.Unlock("Make.CANDIDATE case")
				}
			case LEADER:
				DPrintf("server[%v][%v] as leader", rf.me, rf.term)
				rf.MakeAppendEntries()
				t := int64(HEARTBEAT_TIME)
				rf.StartSleep(t)
			}
		}
	}()

	go func() {
		for true {
			if rf.status == LEADER {
				rf.Lock("leader count replicas")
				lastLog := rf.GetLastLogNonLocking()
				if lastLog.Term == rf.term && lastLog.Index > rf.commitIndex {
					cnt := 0
					for i := 0; i < len(rf.peers); i++ {
						if rf.matchIndex[i] == lastLog.Index {
							cnt++
						}
					}
					if cnt >= (len(rf.peers)+1)/2 {
						rf.commitIndex = lastLog.Index
						log.Printf("leader[%v][%v] update commitIndex=%v", rf.me, rf.term, rf.commitIndex)
					}
				}
				rf.Unlock("leader count replicas")
			}
			time.Sleep(time.Duration(50) * time.Millisecond)
		}
	}()

	go func() {
		for true {
			rf.Lock("check apply msg")
			if rf.commitIndex > rf.lastApplied {
				for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
					_, entry := rf.getEntryNonLocking(i)
					log.Printf("server[%v][%v] apply msg[%v]=%v", rf.me, rf.term, i, entry)
					applyCh <- ApplyMsg{true, entry.Command, entry.Index}
					rf.lastApplied++
				}
			}
			rf.Unlock("check apply msg")
			time.Sleep(time.Duration(50) * time.Millisecond)
		}
	}()

	return rf
}

func (rf *Raft) GetLastLogNonLocking() LogEntry {
	if len(rf.log) == 0 {
		return LogEntry{0, 0, nil}
	}
	return rf.log[len(rf.log)-1]
}

func (rf *Raft) GetLastLog() LogEntry {
	rf.Lock("GetLastLog")
	defer rf.Unlock("GetLastLog")
	if len(rf.log) == 0 {
		return LogEntry{0, 0, nil}
	}
	return rf.log[len(rf.log)-1]
}

func (rf *Raft) MakeRequestVotes() {
	wg := sync.WaitGroup{}
	votes := 0
	doneSends := 0
	mutex := sync.Mutex{}
	rf.Lock("MakeRequestVotes")
	curTerm := rf.term
	rf.votedFor = rf.me
	rf.Unlock("MakeRequestVotes")
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(server int) {
			lastLog := rf.GetLastLog()
			rf.Lock("MakeRequestVotes 0")
			args := RequestVoteArgs{rf.term, rf.me, lastLog.Index, lastLog.Term}
			// release lock before send rpc
			rf.Unlock("MakeRequestVotes 0")
			reply := RequestVoteReply{0, false}
			DPrintf("server[%v][%v] send rv rpc to server[%v]", rf.me, rf.term, server)
			ok := rf.sendRequestVote(server, &args, &reply)
			DPrintf("server[%v][%v] send rv rpc to server[%v], network=%v, reply=%v", rf.me, rf.term, server, ok, reply.VoteGranted)

			mutex.Lock()
			if ok {
				rf.Lock("MakeRequestVotes 1")
				if reply.VoteGranted {
					DPrintf("server[%v][%v] receive vote from server[%v]", rf.me, rf.term, server)
					votes++
					if votes >= len(rf.peers)/2 && rf.status == CANDIDATE && reply.Term == rf.term {
						// second condition check whether this reply outdated
						rf.status = LEADER
					}
				} else if reply.Term > rf.term {
					if reply.Term > rf.term {
						rf.term = reply.Term
						rf.status = FOLLOWER
					}
				}
				rf.Unlock("MakeRequestVotes 1")
			}
			if doneSends < len(rf.peers)-1 {
				wg.Done()
				doneSends++
			}
			mutex.Unlock()
		}(i)
	}
	go func() {
		time.Sleep(time.Duration(int64(MIN_SLEEP_TIME)) * time.Millisecond)
		mutex.Lock()
		for ; doneSends < len(rf.peers)-1; doneSends++ {
			wg.Done()
		}
		mutex.Unlock()
	}()
	wg.Wait()
	rf.Lock("MakeRequestVotes 2")
	if curTerm == rf.term && rf.status == LEADER {
		DPrintf("server[%v][%v] gather majority votes, stop sleep", rf.me, rf.term)
		rf.StopSleep()
	}
	rf.Unlock("MakeRequestVotes 2")
}

func (rf *Raft) getEntryNonLocking(index int) (int, LogEntry) {
	id := index - rf.logStartIndex
	if index < 0 || len(rf.log) == 0 || id >= len(rf.log) || id < 0 {
		return -1, LogEntry{0, 0, nil}
	}
	return id, rf.log[id]
}

func (rf *Raft) getEntry(index int) (int, LogEntry) {
	rf.Lock("getEntry")
	defer rf.Unlock("getEntry")
	if index < 1 || len(rf.log) == 0 || index-rf.logStartIndex >= len(rf.log) {
		return -1, LogEntry{0, 0, nil}
	}
	return index - rf.logStartIndex, rf.log[index-rf.logStartIndex]
}

func (rf *Raft) getPrevLog(server int) LogEntry {
	rf.Lock("getPrevLog")
	defer rf.Unlock("getPrevLog")
	if server < 0 || server >= len(rf.peers) {
		return LogEntry{0, 0, nil}
	}
	id := rf.nextIndex[server] - rf.logStartIndex - 1
	if id >= 0 && id < len(rf.log) {
		return rf.log[id]
	}
	return LogEntry{0, 0, nil}
}

func (rf *Raft) getNextIndex(server int) int {
	rf.Lock("getNextIndex")
	defer rf.Unlock("getNextIndex")
	if server < 0 || server >= len(rf.peers) {
		return 0
	}
	return rf.nextIndex[server]
}

func (rf *Raft) getAppendEntries(server int) []LogEntry {
	rf.Lock("getAppendEntries")
	defer rf.Unlock("getAppendEntries")
	if server < 0 || server >= len(rf.peers) || len(rf.log) == 0 {
		return nil
	}
	nid := rf.nextIndex[server] - rf.logStartIndex
	if nid >= len(rf.log) {
		return nil
	}
	return rf.log[nid:]
}

func (rf *Raft) MakeAppendEntries() {
	wg := sync.WaitGroup{}
	doneSends := 0
	mutex := sync.Mutex{}
	rf.Lock("MakeAppendEntries")
	curTerm := rf.term
	rf.Unlock("MakeAppendEntries")
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		prevLog := rf.getPrevLog(i)
		entries := rf.getAppendEntries(i)
		go func(server int, prevLogIndex int, prevLogTerm int, entries []LogEntry) {
			rf.Lock("MakeAppendEntries 0")
			args := AppendEntriesArgs{rf.term, rf.me, prevLogIndex, prevLogTerm, rf.commitIndex, entries}
			// release lock before send rpc
			rf.Unlock("MakeAppendEntries 0")
			reply := AppendEntriesReply{0, false}
			ok := rf.sendAppendEntries(server, &args, &reply)
			mutex.Lock()
			rf.Lock("MakeAppendEntries 1")
			if ok && rf.status == LEADER {
				if reply.Success && reply.Term == curTerm && len(entries) > 0 {
					rf.nextIndex[server] = entries[len(entries)-1].Index + 1
					rf.matchIndex[server] = entries[len(entries)-1].Index
				} else if !reply.Success && reply.Term > curTerm && rf.term < reply.Term {
					rf.term = reply.Term
					rf.status = FOLLOWER
				} else if !reply.Success && reply.Term == curTerm && rf.term == curTerm {
					rf.nextIndex[server] = Max(1, rf.nextIndex[server]-1)
				}
			}
			rf.Unlock("MakeAppendEntries 1")
			if doneSends < len(rf.peers)-1 {
				wg.Done()
				doneSends++
			}
			mutex.Unlock()
		}(i, prevLog.Index, prevLog.Term, entries)
	}
	go func() {
		time.Sleep(time.Duration(int64(MIN_SLEEP_TIME)) * time.Millisecond)
		mutex.Lock()
		for ; doneSends < len(rf.peers)-1; doneSends++ {
			wg.Done()
		}
		mutex.Unlock()
	}()
	wg.Wait()
}

func (rf *Raft) Lock(funcName string) {
	DPrintf("server[%v][%v] try acquire lock in [%v]", rf.me, rf.term, funcName)
	rf.mu.Lock()
	DPrintf("server[%v][%v] acquire lock in [%v]", rf.me, rf.term, funcName)
}

func (rf *Raft) Unlock(funcName string) {
	DPrintf("server[%v][%v] release lock in [%v]", rf.me, rf.term, funcName)
	rf.mu.Unlock()
}

func Max(x, y int) int {
	if x > y {
		return x
	}
	return y
}
