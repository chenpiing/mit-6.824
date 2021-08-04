package raftkv

import (
	"time"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType string
	Key    string
	Value  string
	CmdId  int64
	Leader int
	Term   int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	storage          map[string]string
	applyFinishChan  chan CmdInfo
	cmdIdMap map[int64]bool
}

type CmdInfo struct {
	cmdId int64
	leader int
	term   int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if args == nil {
		reply.Err = ErrInvalidParam
		return
	}
	DPrintf("[KVServer %v] Get args=%v", kv.me, *args)
	reply.Err = OK
	reply.WrongLeader = !kv.isLeader()
	kv.mu.Lock()
	reply.Value = kv.storage[args.Key]
	kv.mu.Unlock()
	DPrintf("[KVServer %v] Get args=%v, reply=%v", kv.me, *args, *reply)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("[KVServer %v] PutAppend after calling start, args=%v", kv.me, *args)
	// Your code here.
	if args == nil {
		reply.Err = ErrInvalidParam
		return
	}
	if args.Op != PUT && args.Op != APPEND {
		reply.Err = ErrInvalidOpType
		return
	}
	kv.mu.Lock()
	_, ok := kv.cmdIdMap[args.Id]
	kv.mu.Unlock()
	if ok {
		reply.WrongLeader = false
		reply.Err = OK
		return
	}

	term, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	_, _, isLeader = kv.rf.Start(Op{args.Op, args.Key, args.Value, args.Id, kv.me, term})
	DPrintf("[KVServer %v] PutAppend after calling start, args=%v, isLeader=%v", kv.me, *args, isLeader)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	kv.mu.Lock()
	kv.cmdIdMap[args.Id] = true
	kv.mu.Unlock()
	// if cur server is leader, waiting for cmd applied
	flag : for true {
		select {
		case cmdInfo := <- kv.applyFinishChan:
			DPrintf("[KVServer %v] PutAppend, args=%v receive cmd=%v", kv.me, *args, cmdInfo)
			if cmdInfo.term > term {
				reply.WrongLeader = true
				reply.Err = OK
				DPrintf("[KVServer %v] PutAppend failed, args=%v", kv.me, *args)
				return
			}
			if cmdInfo.leader != kv.me {
				continue flag
			}
			kv.mu.Lock()
			_, ok := kv.cmdIdMap[cmdInfo.cmdId]
			kv.mu.Unlock()
			if !ok {
				continue flag
			}
			if cmdInfo.cmdId != args.Id {
				DPrintf("[KVServer %v] PutAppend not my cmd, args=%v receive cmdId=%v", kv.me, *args, cmdInfo.cmdId)
				go func(msgId int64) {
					kv.applyFinishChan <- cmdInfo
				}(cmdInfo.cmdId)
				time.Sleep(time.Duration(50) * time.Millisecond)
				continue
			}
			reply.WrongLeader = false
			reply.Err = OK
			DPrintf("[KVServer %v] PutAppend success, args=%v", kv.me, *args)
			return
		}
	}
}

func (kv *KVServer) isLeader() bool {
	_, isLeader := kv.rf.GetState()
	return isLeader
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.storage = make(map[string]string)
	kv.applyFinishChan = make(chan CmdInfo)
	kv.cmdIdMap = make(map[int64]bool)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go func() {
		for true {
			select {
			case applyMsg := <-kv.applyCh:
				DPrintf("KVServer[ %v] receive applyMsg=%v", kv.me, applyMsg)
				if applyMsg.CommandValid {
					op := applyMsg.Command.(Op)
					kv.mu.Lock()
					switch op.OpType {
					case PUT:
						kv.storage[op.Key] = op.Value
					case APPEND:
						v, ok := kv.storage[op.Key]
						if ok {
							kv.storage[op.Key] = v + op.Value
						} else {
							kv.storage[op.Key] = op.Value
						}
					}
					kv.mu.Unlock()

					if op.Leader == kv.me {
						kv.applyFinishChan <- CmdInfo{op.CmdId, op.Leader, op.Term}
					} else {
						select {
						case kv.applyFinishChan <- CmdInfo{op.CmdId, op.Leader, op.Term}:
							DPrintf("KVServer[ %v] is leader, finish applyMsg, op=%v", kv.me, op)
						default:
							DPrintf("KVServer[ %v] not leader, finish applyMsg", kv.me)
						}
					}
				}
			}
		}
	}()

	return kv
}
