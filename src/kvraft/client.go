package raftkv

import (
	"labrpc"
	"sync"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader int
	mu     sync.Mutex
	id int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.leader = 0
	ck.mu = sync.Mutex{}
	ck.id = nrand()
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	DPrintf("ck.Get key=%v", key)
	for true {
		resMap := make(map[string]int)
		leaderRes := ""
		for i := 0; i < len(ck.servers); i++ {
			args := GetArgs{key}
			reply := GetReply{false, OK, ""}
			ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
			if ok {
				DPrintf("ck.Get from %v key=%v value=%v", i, key, reply.Value)
				_ , exist := resMap[reply.Value]
				if exist {
					resMap[reply.Value]++
				} else {
					resMap[reply.Value] = 1
				}
			}
			if ok && reply.Err == OK && !reply.WrongLeader {
				//return reply.Value
				leaderRes = reply.Value
				ck.mu.Lock()
				ck.leader = i
				ck.mu.Unlock()
				DPrintf("ck.Get from leader %v key=%v value=%v", i, key, reply.Value)
			}
		}
		DPrintf("ck.Get key=%v map=%v", key, resMap)
		for v, cnt := range resMap {
			if cnt >= len(ck.servers) / 2 + 1 && v == leaderRes {
				return leaderRes
			}
		}
		if ck.leader == len(ck.servers) - 1 {
			time.Sleep(time.Duration(20) * time.Millisecond)
		}
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	DPrintf("ck.PutAppend key=%v value=%v op=%v", key, value, op)
	cmdId := nrand() + ck.id
	for true {
		args := PutAppendArgs{key, value, op, cmdId}
		reply := PutAppendReply{false, OK}
		ok := ck.servers[ck.leader].Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.Err == OK && !reply.WrongLeader {
			DPrintf("ck.PutAppend key=%v value=%v op=%v finished", key, value, op)
			return
		} else {
			DPrintf("ck.PutAppend key=%v value=%v op=%v failed, ok=%v, reply=%v, will keep retry", key, value, op, ok, reply)
		}
		ck.mu.Lock()
		ck.leader = (ck.leader + 1) % len(ck.servers)
		ck.mu.Unlock()
		if ck.leader == len(ck.servers)-1 {
			time.Sleep(time.Duration(30) * time.Millisecond)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
