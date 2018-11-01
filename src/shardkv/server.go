package shardkv


// import "shardmaster"
import "labrpc"
import "raft"
import "sync"
import (
	"encoding/gob"
	"shardmaster"
	"time"
	"fmt"
	"encoding/json"
	"log"
)

func toJsonString(v interface{}) string{
	b, err := json.Marshal(v)
	if err != nil {
		fmt.Println(err)
		return ""
	}
	return string(b)
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType string
	Args interface{}
}

type Result struct {
	opType	string
	args	interface{}
	reply	interface{}
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	mck           *shardmaster.Clerk
	clientsCommit map[int64]int64
	messages      map[int]chan Result
	shardDatabase []map[string]string
	config        shardmaster.Config
	requestId     int64

}

func (kv *ShardKV) containsShard(shardId int) bool  {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.config.Shards[shardId] == kv.gid
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if !kv.containsShard(key2shard(args.Key)) {
		reply.WrongLeader = true
		reply.Err = ErrWrongGroup
		return
	}

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
	return
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if !kv.containsShard(key2shard(args.Key)) {
		reply.WrongLeader = true
		reply.Err = ErrWrongGroup
		return
	}

	index, _, isLeader := kv.rf.Start(Op{OpType: PutAppend, Args: *args})
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
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}


//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots with
// persister.SaveSnapshot(), and Raft should save its state (including
// log) with persister.SaveRaftState().
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})
	gob.Register(GetArgs{})
	gob.Register(GetReply{})
	gob.Register(PutAppendArgs{})
	gob.Register(PutAppendReply{})
	gob.Register(PullShardDataArgs{})
	gob.Register(PullShardDataReply{})
	gob.Register(ReconfigArgs{})
	gob.Register(ReconfigReply{})
	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters
	kv.config = shardmaster.Config{Num:0}
	kv.config.Groups = map[int][]string{}


	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.messages = make(map[int]chan Result)
	kv.shardDatabase = make([]map[string]string, shardmaster.NShards)
	for i:=0; i < shardmaster.NShards; i++ {
		kv.shardDatabase[i] = make(map[string]string)
	}
	kv.clientsCommit = make(map[int64]int64)
	kv.requestId = 0
	go kv.DoUpdate()
	go kv.DetectConfigChange()
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	return kv
}

func (kv *ShardKV) DoUpdate()  {
	for true  {
		applyMsg := <- kv.applyCh
		if applyMsg.UseSnapshot {

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
			} else if request.OpType == PutAppend  {
				args := request.Args.(PutAppendArgs)
				clientId = args.ClientId
				requestId = args.RequestId
				result.args = args
			} else if request.OpType == PullShard {
				args := request.Args.(PullShardDataArgs)
				clientId = args.ClientId
				requestId = args.RequestId
				result.args = args
			}


			result.opType = request.OpType
			result.reply = kv.Apply(request, kv.IsDuplicate(clientId, requestId))
			kv.SendResult(applyMsg.Index, result);

		}

	}
}

func (kv *ShardKV) Apply(op Op, duplicate bool) interface{} {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch op.Args.(type) {
	case GetArgs:
		var reply  GetReply
		args := op.Args.(GetArgs)
		sid := key2shard(args.Key)
		if value, ok := kv.shardDatabase[sid][args.Key]; ok {
			reply.Err = OK
			reply.Value = value
		} else {
			reply.Err = ErrNoKey
		}
		return reply
	case PutAppendArgs:
		var reply PutAppendReply
		args := op.Args.(PutAppendArgs)
		sid := key2shard(args.Key)
		if !duplicate {
			if args.Op == Put {
				kv.shardDatabase[sid][args.Key] = args.Value
			} else {
				kv.shardDatabase[sid][args.Key] += args.Value
			}
		}
		reply.Err = OK
		return  reply
	case PullShardDataArgs:
		return kv.applyPullShard(op)
	case ReconfigArgs:
		return kv.applyReconfig(op)
	}

	return nil
}

func (kv *ShardKV) applyPullShard(op Op) PullShardDataReply {
	var reply PullShardDataReply
	args := op.Args.(PullShardDataArgs)
	if args.ConfigNum > kv.config.Num {
		reply.WrongLeader = true
		reply.Err = ErrWrongConfigNum
		return reply
	}

	reply.Err = OK
	reply.WrongLeader = false
	for _, sid := range args.Shards {
		unionMaps(reply.StoredShards[sid], kv.shardDatabase[sid])
		for key, value := range kv.clientsCommit {
			reply.ClientCommit[key] = value
		}
	}
	return reply
}

func (kv *ShardKV) applyReconfig(op Op) interface{} {
	var reply ReconfigReply
	args := op.Args.(ReconfigArgs)
	if kv.config.Num >= args.Config.Num {
		reply.Err = ErrWrongConfigNum
		return reply;
	}

	for sid, _ := range args.StoredShards {
		kv.shardDatabase[sid] = make(map[string]string)

		unionMaps(kv.shardDatabase[sid], args.StoredShards[sid])
	}
	unionCommitMaps(kv.clientsCommit, args.ClientCommit)
	kv.config = args.Config
	log.Printf("%d update to config %d", kv.gid, toJsonString(kv.config))
	reply.Err = OK
	return reply
}

func (kv *ShardKV) IsDuplicate(clientId int64, requestId int64) bool {
	if maxRequest, ok := kv.clientsCommit[clientId]; ok {
		if maxRequest >= requestId {
			return true
		}
	}
	kv.clientsCommit[clientId] = requestId
	return false
}
func (kv *ShardKV) SendResult(msgIdx int, result Result) {
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

func (kv *ShardKV) DetectConfigChange()  {
	ticker := time.NewTicker(time.Millisecond * 100)
	for range ticker.C {
		if _, isLeader := kv.rf.GetState(); isLeader {
			config := kv.mck.Query(kv.config.Num + 1)
			kv.handleConfig(config)
		} else {
			// do nothing
			time.Sleep(time.Second * 1)
		}
	}
}
func (kv *ShardKV) handleConfig(newConfig shardmaster.Config) {

	if kv.config.Num < newConfig.Num {

		pullShards := make(map[int][]int)
		for idx := range newConfig.Shards {
			if newConfig.Shards[idx] == kv.gid && kv.config.Shards[idx] != kv.gid {
				pullShards[kv.config.Shards[idx]] = append(pullShards[kv.config.Shards[idx]], idx)
			}
		}


		if 0 < len(pullShards) {
			var reconfigArgs ReconfigArgs
			var wg sync.WaitGroup
			wg.Add(len(pullShards))
			for gid, shards := range pullShards {
				go func(gid int, shards []int) {
					defer wg.Done()
					kv.pullShardDataFromGid(newConfig, shards, gid, reconfigArgs)
				}(gid, shards)
			}

			wg.Wait()
			// 不成功下一轮重试
			reconfigArgs.Config = newConfig
			kv.rf.Start(Op{OpType:Reconfiguration, Args:reconfigArgs})
		}

	}
}
func (kv *ShardKV) pullShardDataFromGid(newConfig shardmaster.Config, shards []int, gid int, reconfigArgs ReconfigArgs) {
	args := PullShardDataArgs{newConfig.Num, shards, nrand(), kv.requestId}
	gservers := newConfig.Groups[gid]
	var reply PullShardDataReply
	kv.callPullShardDataArgs(gservers, args, &reply)
	for _, sid := range shards {
		unionMaps(reconfigArgs.StoredShards[sid], reply.StoredShards[sid])
		unionCommitMaps(reconfigArgs.ClientCommit, reply.ClientCommit)
	}
}


func (kv *ShardKV) PullShardData(args *PullShardDataArgs, reply *PullShardDataReply)  {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	index, _, isLeader := kv.rf.Start(Op{OpType: PullShard, Args: *args})
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
				*reply = msg.reply.(PullShardDataReply)
				reply.WrongLeader = false
			}
		}
	case <-time.After(time.Second * 1): // 超时服务端控制
		reply.WrongLeader = true
	}

	return
}

func unionMaps(dst map[string]string, src map[string]string)  {
	for key, value := range src {
		if dst[key] < value {
			dst[key] = value
		}
	}
}

func unionCommitMaps(dst map[int64]int64, src map[int64]int64)  {
	for key, value := range src {
		dst[key] = value
	}
}

func (kv *ShardKV) callPullShardDataArgs(gservers []string, args PullShardDataArgs, reply *PullShardDataReply) {
	for _, server := range gservers {
		srv := kv.make_end(server)
		ok := srv.Call("ShardKV.PullShardData", &args, &reply)
		if ok && reply.WrongLeader == false && (reply.Err == OK ) {
			// fetched data
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}
