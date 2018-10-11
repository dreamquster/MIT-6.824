package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
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
	OpType	string
	Args	interface{}
}

type Result struct {
	opType	string
	args	interface{}
	reply	interface{}
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	database	map[string]string
	clientsCommited	map[int64]int64
	messages	map[int]chan Result
	persister	*raft.Persister
}

func (kv *RaftKV) GetLeader(args *GetLeaderArgs, reply *GetLeaderReply) {
	term, isLeader := kv.rf.GetState()
	reply.WrongLeader = !isLeader
	reply.Term = term
	reply.LeaderId = kv.me

	return
}



func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	index, _, isLeader := kv.rf.Start(Op{ OpType: Get, Args: *args })
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	kv.mu.Lock()
	if _, ok := kv.messages[index]; !ok {
		kv.messages[index] = make(chan Result, 1)
	}
	chanMsg := kv.messages[index]
	kv.mu.Unlock()

	select {
	case msg := <- chanMsg:
		if recvArgs, ok := msg.args.(GetArgs); !ok {
			reply.WrongLeader = true
		} else {
			if args.ClientId != recvArgs.ClientId || args.RequestId != recvArgs.RequestId {
				reply.WrongLeader = true
			} else {
				*reply = msg.reply.(GetReply)
				reply.WrongLeader = false
			}
		}
	case <-time.After(time.Second * 1): // 超时服务端控制
		reply.WrongLeader = true
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	index, _, isLeader := kv.rf.Start(Op{OpType:PutAppend, Args: *args})
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	kv.mu.Lock()
	if _, ok := kv.messages[index]; !ok {
		kv.messages[index] = make(chan Result, 1)
	}
	chanMsg := kv.messages[index]
	kv.mu.Unlock()

	select {
	case msg := <- chanMsg:
		if recvArgs, ok := msg.args.(PutAppendArgs); !ok {
			reply.WrongLeader = true
		} else {
			if args.ClientId != recvArgs.ClientId || args.RequestId != recvArgs.RequestId {
				reply.WrongLeader = true
			} else {
				*reply = msg.reply.(PutAppendReply)
				reply.WrongLeader = false
			}
		}
	case <-time.After(time.Second * 1): // 超时服务端控制
		reply.WrongLeader = true
	}

	return
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.database = make(map[string]string)

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	return kv
}
