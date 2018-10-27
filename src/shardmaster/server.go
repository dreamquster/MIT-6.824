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

	reply.WrongLeader = true
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
	index, _, isLeader := sm.rf.Start(Op{OpType:LEAVE, Args: *args})
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

	reply.WrongLeader = true
	select {
	case msg := <-chanMsg:
		if recvArgs, ok := msg.args.(LeaveArgs); !ok {
			reply.WrongLeader = true
		} else {
			if args.ClientId != recvArgs.ClientId || args.RequestId != recvArgs.RequestId {
				reply.WrongLeader = true
			} else {
				*reply = msg.reply.(LeaveReply)
				reply.WrongLeader = false
			}
		}
	case <-time.After(1 * time.Second):
		break
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	index, _, isLeader := sm.rf.Start(Op{OpType:MOVE, Args: *args})
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

	reply.WrongLeader = true
	select {
	case msg := <-chanMsg:
		if recvArgs, ok := msg.args.(MoveArgs); !ok {
			reply.WrongLeader = true
		} else {
			if args.ClientId != recvArgs.ClientId || args.RequestId != recvArgs.RequestId {
				reply.WrongLeader = true
			} else {
				*reply = msg.reply.(MoveReply)
				reply.WrongLeader = false
			}
		}
	case <-time.After(1 * time.Second):
		break
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	index, _, isLeader := sm.rf.Start(Op{OpType:QUERY, Args: *args})
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

	reply.WrongLeader = true
	select {
	case msg := <-chanMsg:
		if recvArgs, ok := msg.args.(QueryArgs); !ok {
			reply.WrongLeader = true
		} else {
			if args.ClientId != recvArgs.ClientId || args.RequestId != recvArgs.RequestId {
				reply.WrongLeader = true
			} else {
				*reply = msg.reply.(QueryReply)
				reply.WrongLeader = false
			}
		}
	case <-time.After(1 * time.Second):
		break
	}
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

func (sm *ShardMaster) DoUpdate()  {
	for true {
		applyMsg := <- sm.applyCh
		if applyMsg.UseSnapshot {
		} else {
			request := applyMsg.Command.(Op)

			var result Result
			var clientId int64
			var requestId int64
			switch request.OpType {
			case QUERY:
				args := request.Args.(QueryArgs)
				clientId = args.ClientId
				requestId = args.RequestId
			case JOIN:
				args := request.Args.(JoinArgs)
				clientId = args.ClientId
				requestId = args.RequestId
			case LEAVE:
				args := request.Args.(LeaveArgs)
				clientId = args.ClientId
				requestId = args.RequestId
			case MOVE:
				args := request.Args.(MoveArgs)
				clientId = args.ClientId
				requestId = args.RequestId
			}
			result.reply = sm.Apply(request, sm.IsDuplicate(clientId, requestId))
			sm.SendResult(applyMsg.Index, result)

		}

	}
}

func (sm *ShardMaster) SendResult(msgIdx int, result Result) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if _, ok := sm.messages[msgIdx]; !ok {
		sm.messages[msgIdx] = make(chan Result, 1)
	} else {
		// 防止阻塞，未读数据
		select {
		case <-sm.messages[msgIdx]:
		default:
		}
	}
	sm.messages[msgIdx] <- result
}

func (sm *ShardMaster) Apply(op Op, duplicate bool) interface{}   {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	switch op.Args.(type) {
	case JoinArgs:
		var reply JoinReply
		reply.Err = OK
		if !duplicate {
			newConfig := sm.copyLastConfig()
			args := op.Args.(JoinArgs)
			unionMaps(newConfig.Groups, args.Servers)
			sm.configs = append(sm.configs, newConfig)
		}
		return reply
	case LeaveArgs:
		var reply LeaveReply
		reply.Err = OK
		if !duplicate {
			newConfig := sm.copyLastConfig()
			args := op.Args.(LeaveArgs)
			for gid := range args.GIDs {
				delete(newConfig.Groups, gid)
			}
			sm.configs = append(sm.configs, newConfig)
		}
		return reply
	case MoveArgs:
		var reply MoveReply
		reply.Err = OK
		if !duplicate {
			newConfig := sm.copyLastConfig()
			args := op.Args.(MoveArgs)
			newConfig.Shards[args.Shard] = args.GID
			sm.configs = append(sm.configs, newConfig)
		}
		return reply
	case QueryArgs:
		var reply QueryReply
		reply.Err = OK
		args := op.Args.(QueryArgs)
		if args.Num == -1 || len(sm.configs) < args.Num {
			reply.Config = sm.configs[len(sm.configs) - 1]
		} else {
			reply.Config = sm.configs[args.Num]
		}
		return reply
	}
	return nil
}

func unionMaps(dst map[int][]string, src map[int][]string)  {
	for key, value := range src {
		newValue := make([]string, len(value))
		copy(newValue, value)
		dst[key] = newValue
	}
}

func (sm *ShardMaster) copyLastConfig() Config {
	var newConfig Config
	lastConfig := sm.configs[len(sm.configs)-1]
	newConfig.Num = lastConfig.Num + 1
	copy(newConfig.Shards[:NShards], lastConfig.Shards[:NShards])
	newConfig.Groups = make(map[int][]string)
	unionMaps(newConfig.Groups, lastConfig.Groups)

	return newConfig
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
	sm.configs[0].Num = 0

	gob.Register(Op{})
	gob.Register(JoinArgs{})
	gob.Register(JoinReply{})
	gob.Register(LeaveArgs{})
	gob.Register(LeaveReply{})
	gob.Register(MoveArgs{})
	gob.Register(MoveReply{})
	gob.Register(QueryArgs{})
	gob.Register(QueryReply{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.clientsCommit = make(map[int64]int64)
	sm.messages = make(map[int]chan Result)
	go sm.DoUpdate()
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