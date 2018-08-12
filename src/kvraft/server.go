package raftkv

import (
	"encoding/gob"
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
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	clientsCommited	map[int64]int64
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
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, isLeader := kv.rf.GetState()
	reply.WrongLeader = !isLeader
	reply.LeaderId = kv.rf.GetLeaderId()
	if args.Key == "" {
		return
	}

	if isLeader {
		value := kv.rf.ProcessGet(args.Key)
		reply.Value = value
	}

	return
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	defer func() {
		kv.mu.Lock()
		reply.AppendId = kv.clientsCommited[args.ClientId]
		kv.mu.Unlock()
	}()

	if (args.Op == "Put" || args.Op == "Append") &&
			args.RequestId <= kv.clientsCommited[args.ClientId]{
		reply.Err = OK
		return
	}

	index, _, isLeader  := kv.rf.Start(args)
	reply.WrongLeader = !isLeader
	if reply.WrongLeader {
		return
	}

	reply.AppendId = int64(index)
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
	kv.clientsCommited = make(map[int64]int64)

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	return kv
}
