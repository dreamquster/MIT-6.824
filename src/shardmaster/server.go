package shardmaster


import "raft"
import "labrpc"
import "sync"
import (
	"encoding/gob"
	"time"
)

type Result struct {
	opType string
	args 	interface{}
	reply	interface{}
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num
	clientsCommit 	map[int64]int64
	messages	map[int]chan Result
}


type Op struct {
	// Your data here.
	OpType	string
	Args	interface{}
}


func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	index, _, isLeader := sm.rf.Start(Op{OpType:JOIN, Args: *args})
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	sm.mu.Lock()
	if _, ok := sm.messages[index]; !ok {
		sm.messages[index] = make(chan Result, 1)
	}
	chanMsg := sm.messages[index]
	sm.mu.Unlock()

	select {
	case msg := <-chanMsg:
		if recvArgs, ok := msg.args.(JoinArgs); !ok {
			reply.WrongLeader = true
		} else {
			if args.ClientId != recvArgs.ClientId || args.RequestId != recvArgs.RequestId {
				reply.WrongLeader = true
			} else {
				*reply = msg.reply.(JoinReply)
				reply.WrongLeader = false
			}
		}
	case <-time.After(1 * time.Second):
		break
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
}


//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	gob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.

	return sm
}


func (sm *ShardMaster) IsDuplicate(clientId int64, requestId int64) bool {
	if maxRequest, ok := sm.clientsCommit[clientId]; ok {
		if maxRequest >= requestId {
			return true
		}
	}
	sm.clientsCommit[clientId] = requestId
	return false
}