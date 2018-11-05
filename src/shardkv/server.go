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
	"bytes"
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

	requestId	 int64
	id			 int64
	// Your definitions here.
	mck           *shardmaster.Clerk
	clientsCommit map[int64]int64
	messages      map[int]chan Result
	shardDatabase [shardmaster.NShards]map[string]string
	config        shardmaster.Config

}

func (kv *ShardKV) containsShard(shardId int) bool  {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.config.Shards[shardId] == kv.gid
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	//defer func() {
	//	log.Printf("%d reply Get query key:%s with reply:%s", kv.id, args.Key, toJsonString(reply))
	//}()
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

func (kv *ShardKV) PullShardData(args *PullShardDataArgs, reply *PullShardDataReply) {
	// Your code here.
	index, _, isLeader := kv.rf.Start(Op{ OpType: PullShardData, Args: *args })
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
		if recvArgs, ok := msg.args.(PullShardDataArgs); !ok {
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

func (kv *ShardKV) DeleteShards(args * DeleteShardsArgs, reply *DeleteShardsReply)  {
	index, _, isLeader := kv.rf.Start(Op{ OpType: DeleteShards, Args: *args })
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
		if _, ok := msg.args.(DeleteShardsArgs); !ok {
			reply.WrongLeader = true
		} else {
			*reply = msg.reply.(DeleteShardsReply)
			reply.WrongLeader = false
		}
	case <-time.After(time.Second * 1): // 超时服务端控制
		reply.WrongLeader = true
	}
	return
}

func (kv *ShardKV) Reconfig(args *ReconfigArgs, reply *ReconfigReply) {
	// Your code here.
	index, _, isLeader := kv.rf.Start(Op{ OpType: Reconfiguration, Args: *args })
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
		if _, ok := msg.args.(ReconfigArgs); !ok {
			reply.WrongLeader = true
		} else {
			*reply = msg.reply.(ReconfigReply)
			reply.WrongLeader = false
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
	gob.Register(DeleteShardsArgs{})
	gob.Register(DeleteShardsReply{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	kv.id = nrand()
	kv.requestId = 0
	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.messages = make(map[int]chan Result)
	for i := 0; i < shardmaster.NShards; i++ {
		kv.shardDatabase[i] = make(map[string]string)
	}
	kv.clientsCommit = make(map[int64]int64)
	kv.config.Num = 0
	kv.config.Groups = make(map[int][]string)

	go kv.DoUpdate()
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	go kv.DetectConfigChange()
	return kv
}

func (kv *ShardKV) DoUpdate()  {
	for true  {
		applyMsg := <- kv.applyCh
		if applyMsg.UseSnapshot {
			kv.UseSnapshot(applyMsg.Snapshot)
		} else  {
			request := applyMsg.Command.(Op)

			var result Result
			var clientId int64
			var requestId int64
			result.args = request.Args
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
			} else if request.OpType == PullShardData {
				args := request.Args.(PullShardDataArgs)
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

func (kv *ShardKV) Apply(op Op, duplicate bool) interface{} {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch op.Args.(type) {
	case GetArgs:
		return kv.applyGet(op)
	case PutAppendArgs:
		return kv.applyPutAppend(op, duplicate)
	case PullShardDataArgs:
		return kv.applyPullShardData(op)
	case ReconfigArgs:
		return kv.applyReconfig(op)
	case DeleteShardsArgs:
		return kv.applyDeleteShards(op, duplicate)
	}

	return nil
}

func (kv *ShardKV) CheckSnapshot(index int) {
	if kv.maxraftstate != -1 && float64(kv.maxraftstate)*0.8 < float64(kv.rf.GetSavedStates()) {
		w := new(bytes.Buffer)
		enc := gob.NewEncoder(w)
		enc.Encode(kv.shardDatabase)
		enc.Encode(kv.clientsCommit)
		enc.Encode(kv.config)
		data := w.Bytes()
		go kv.rf.StartSnapshot(data, index)
	}
}

func (kv *ShardKV) UseSnapshot(snapshot []byte)  {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	var lastIncludedIndex int
	var lastIncludedTerm int
	for i := 0; i < shardmaster.NShards; i ++ {
		kv.shardDatabase[i] = make(map[string]string)
	}

	r := bytes.NewBuffer(snapshot)
	dec := gob.NewDecoder(r)
	dec.Decode(&lastIncludedIndex)
	dec.Decode(&lastIncludedTerm)
	dec.Decode(&kv.shardDatabase)
	dec.Decode(&kv.clientsCommit)
	dec.Decode(&kv.config)
}


func (kv *ShardKV) applyPullShardData(op Op) interface{} {
	var reply PullShardDataReply
	args := op.Args.(PullShardDataArgs)
	reply.WrongLeader = false
	if kv.config.Num < args.ConfigNum {
		reply.Err = ErrConfigNum
		return reply

	}
	reply.ClientsCommit = make(map[int64]int64)

	for _, sid := range args.Shards {
		if nil == reply.StoredShards[sid] {
			reply.StoredShards[sid] = make(map[string]string)
		}
		unionMap(reply.StoredShards[sid], kv.shardDatabase[sid])
		unionCommits(reply.ClientsCommit, kv.clientsCommit)
	}
	reply.Err = OK
	kv.clientsCommit[args.ClientId] = args.RequestId
	return reply
}

func (kv *ShardKV) applyReconfig(op Op) interface{} {
	var reply ReconfigReply
	args := op.Args.(ReconfigArgs)
	if kv.config.Num >= args.Config.Num {
		reply.Err = ErrConfigNum
		return reply
	}

	if args.PartialUpdate {
		for _, sid := range args.ShardIds {
			if args.Config.Shards[sid] == kv.gid &&
				kv.config.Shards[sid] != kv.gid {
				database := args.StoredShards[sid]
				if 0 < len(kv.shardDatabase[sid]) {
					kv.shardDatabase[sid] = make(map[string]string)
				}
				unionMap(kv.shardDatabase[sid], database)
				kv.config.Shards[sid] = kv.gid
			}
		}
		unionCommits(kv.clientsCommit, args.ClientsCommit)
	} else {
		kv.config = args.Config
		log.Printf("group %d update %d config to %s", kv.gid, kv.id,toJsonString(kv.config))
	}
	reply.Err = OK
	return reply
}

func unionMap(dst map[string]string, src map[string]string)  {
	for key, value := range src {
		dst[key] = value
	}
}

func unionCommits(dst map[int64]int64, src map[int64]int64)  {
	for key, value := range src {
		if dvalue, ok := dst[key]; !ok || dvalue < value {
			dst[key] = value
		}
	}
}

func (kv *ShardKV) IsDuplicate(clientId int64, requestId int64) bool {
	if maxRequest, ok := kv.clientsCommit[clientId]; ok {
		if maxRequest >= requestId {
			return true
		}
	}
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
	for true {
		if _,  isLeader := kv.rf.GetState(); isLeader {
			config := kv.mck.Query(kv.config.Num + 1)
			kv.handleConfig(config)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
func (kv *ShardKV) handleConfig(newConfig shardmaster.Config) {
	if kv.config.Num < newConfig.Num {
		gshards := make(map[int][]int);
		for sid, gid := range newConfig.Shards {
			kvsgid := kv.config.Shards[sid]
			if 0 < kvsgid && kv.gid == gid &&  kvsgid != gid {
				gshards[kvsgid] = append(gshards[kvsgid], sid)
			}
		}

		var wg sync.WaitGroup
		allPull := true
		for gid, shards := range gshards {
			wg.Add(1)
			go func(gid int, shards []int) {
				defer wg.Done()
				ok, reply := kv.callPullShardData(newConfig.Num, gid, shards)
				log.Printf("%d received ok:%s, %s", kv.id, ok,  toJsonString(reply))
				if ok {
					var partialReply ReconfigReply
					partialArgs := ReconfigArgs{newConfig, reply.StoredShards,
					reply.ClientsCommit, shards,true}
					kv.Reconfig(&partialArgs, &partialReply)
					if OK == reply.Err {
						args := DeleteShardsArgs{kv.config.Num, shards, kv.id, kv.requestId}
						kv.requestId++
						go kv.callDeleteShards(kv.config, &args, gid)
					} else {
						allPull = false
					}
				} else {
					allPull = false
				}
			}(gid, shards)

		}

		timeout:= waitTimeout(&wg, 10 *time.Millisecond)

		if allPull && !timeout {
			var reconfigArgs ReconfigArgs
			reconfigArgs.Config = newConfig
			reconfigArgs.PartialUpdate = false
			reconfigArgs.ClientsCommit = make(map[int64]int64)
			initialShards(&reconfigArgs.StoredShards)
			var reply ReconfigReply
			kv.Reconfig(&reconfigArgs, &reply)
		}
	}
}
// waitTimeout waits for the waitgroup for the specified max timeout.
// Returns true if waiting timed out.
func waitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return false // completed normally
	case <-time.After(timeout):
		return true // timed out
	}
}


func initialShards(shardDb *[shardmaster.NShards]map[string]string) {
	for i := 0; i < shardmaster.NShards; i++ {
		shardDb[i] = make(map[string]string)
	}
}
func (kv *ShardKV) callPullShardData(newConfigNum int, gid int, shards []int) (bool, PullShardDataReply) {
	args := PullShardDataArgs{newConfigNum, shards, kv.id, kv.requestId}
	kv.requestId++
	for i := 0; i < 3; i++ {
		if servers, ok := kv.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := kv.make_end(servers[si])
				for j := 0; j < 3; j++ {
					var reply PullShardDataReply
					ok := srv.Call("ShardKV.PullShardData", &args, &reply)
					if ok && reply.WrongLeader == false && (reply.Err == OK) {
						return true, reply
					} else if ok && reply.WrongLeader == false && reply.Err == ErrConfigNum {
						time.Sleep(10*time.Millisecond)
					} else {
						break
					}
				}

			}
		}
	}
	return false, PullShardDataReply{}
}

func (kv *ShardKV) callDeleteShards(oldConfig shardmaster.Config, args *DeleteShardsArgs, gid int) (bool, interface{}) {
	for i := 0; i < 3; i++ {
		if servers, ok := oldConfig.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := kv.make_end(servers[si])
				var reply DeleteShardsReply
				ok := srv.Call("ShardKV.DeleteShards", args, &reply)
				if ok && reply.WrongLeader == false && (reply.Err == OK) {
					return true, reply
				}
			}
		}
	}
	return false, DeleteShardsReply{}
}

func (kv *ShardKV) applyPutAppend(op Op, duplicate bool) interface{} {
	var reply PutAppendReply
	args := op.Args.(PutAppendArgs)
	sid := key2shard(args.Key)
	if kv.config.Shards[sid] != kv.gid  {
		reply.Err = ErrWrongGroup
		return reply
	}

	if !duplicate {
		if args.Op == Put {
			kv.shardDatabase[sid][args.Key] = args.Value
		} else {
			kv.shardDatabase[sid][args.Key] += args.Value
		}
		log.Printf("group %d %d set key:%s to value:%s",kv.gid, kv.id, args.Key, kv.shardDatabase[sid][args.Key])
	}
	reply.Err = OK
	kv.clientsCommit[args.ClientId] = args.RequestId
	return  reply
}
func (kv *ShardKV) applyGet(op Op) interface{} {
	var reply  GetReply
	args := op.Args.(GetArgs)
	sid := key2shard(args.Key)
	if kv.config.Shards[sid] != kv.gid {
		reply.Err = ErrWrongGroup
		return reply
	}

	if value, ok := kv.shardDatabase[sid][args.Key]; ok {
		reply.Err = OK
		reply.Value = value
	} else {
		reply.Err = ErrNoKey
	}
	kv.clientsCommit[args.ClientId] = args.RequestId
	//log.Printf("%d reply Get query key:%s", kv.id, args.Key)
	return reply
}

func (kv *ShardKV) applyDeleteShards(op Op, duplicate bool) interface{} {
	var reply DeleteShardsReply
	args := op.Args.(DeleteShardsArgs)
	if kv.config.Num < args.ConfigNum {
		reply.Err = ErrConfigNum
		return reply
	}

	if !duplicate {
		for _, sid := range args.DelShards {
			if kv.config.Shards[sid] != kv.gid {
				kv.shardDatabase[sid] = make(map[string]string)
			}
		}
	}
	reply.Err = OK
	kv.clientsCommit[args.ClientId] = args.RequestId
	return reply
}


