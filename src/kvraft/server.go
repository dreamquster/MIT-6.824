package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
	"bytes"
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
	clientsCommit	map[int64]int64
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

func (kv *RaftKV) DoUpdate()  {
	for true  {
		applyMsg := <- kv.applyCh
		if applyMsg.UseSnapshot {
			kv.UseSnapshot(applyMsg.Snapshot)
		} else  {
			request := applyMsg.Command.(Op)

			var result Result
			var clientId int64
			var requestId int64
			if request.OpType == Get {
				args := request.Args.(GetArgs)
				clientId = args.ClientId
				requestId = args.RequestId
				result.args = args
			} else  {
				args := request.Args.(PutAppendArgs)
				clientId = args.ClientId
				requestId = args.RequestId
				result.args = args
			}

			result.opType = request.OpType
			result.reply = kv.Apply(request, kv.IsDuplicate(clientId, requestId))
			kv.SendResult(applyMsg.Index, result);
			kv.CheckSnapshot(applyMsg.Index)
		}

	}
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
func (kv *RaftKV) Apply(op Op, duplicate bool) interface{} {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch op.Args.(type) {
	case GetArgs:
		var reply  GetReply
		args := op.Args.(GetArgs)
		if value, ok := kv.database[args.Key]; ok {
			reply.Err = OK
			reply.Value = value
		} else {
			reply.Err = ErrNoKey
		}
		if kv.clientsCommit[args.ClientId] < args.RequestId {
			kv.clientsCommit[args.ClientId] = args.RequestId
		}
		return reply
	case PutAppendArgs:
		var reply PutAppendReply
		args := op.Args.(PutAppendArgs)
		if !duplicate {
			if args.Op == Put {
				kv.database[args.Key] = args.Value
			} else {
				kv.database[args.Key] += args.Value
			}
			kv.clientsCommit[args.ClientId] = args.RequestId
		}
		reply.Err = OK
		return  reply
	}

	return nil
}
func (kv *RaftKV) IsDuplicate(clientId int64, requestId int64) bool {
	if maxRequest, ok := kv.clientsCommit[clientId]; ok {
		if maxRequest >= requestId {
			return true
		}
	}
	return false
}
func (kv *RaftKV) SendResult(msgIdx int, result Result) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.messages[msgIdx]; !ok {
		kv.messages[msgIdx] = make(chan Result, 1)
	} else {
		// 防止阻塞，未读数据
		select {
		case <-kv.messages[msgIdx]:
		default:
		}
	}
	kv.messages[msgIdx] <- result
}
func (kv *RaftKV) CheckSnapshot(index int) {
	if kv.maxraftstate != -1 && float64(kv.maxraftstate)*0.8 < float64(kv.rf.GetSavedStates()) {
		w := new(bytes.Buffer)
		enc := gob.NewEncoder(w)
		enc.Encode(kv.database)
		enc.Encode(kv.clientsCommit)
		data := w.Bytes()
		go kv.rf.StartSnapshot(data, index)
	}
}
func (kv *RaftKV) UseSnapshot(snapshot []byte)  {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	var lastIncludedIndex int
	var lastIncludedTerm int
	kv.database = make(map[string]string)

	r := bytes.NewBuffer(snapshot)
	dec := gob.NewDecoder(r)
	dec.Decode(&lastIncludedIndex)
	dec.Decode(&lastIncludedTerm)
	dec.Decode(&kv.database)
	dec.Decode(&kv.clientsCommit)
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

	gob.Register(PutAppendArgs{})
	gob.Register(GetArgs{})
	gob.Register(PutAppendReply{})
	gob.Register(GetReply{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.database = make(map[string]string)
    kv.clientsCommit = make(map[int64]int64)
	kv.persister = persister
	kv.messages = make(map[int]chan Result)

	go kv.DoUpdate()
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	return kv
}
