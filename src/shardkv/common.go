package shardkv

import "shardmaster"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
	ErrConfigNum  = "ErrConfigNum"
)
const (
	Get             = "Get"
	Put             = "Put"
	Append          = "Append"
	PutAppend       = "PutAppend"
	Reconfiguration = "Reconfiguration"
	PullShardData   = "PullShardData"
	DeleteShards   = "DeleteShards"
)
type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId	int64
	RequestId	int64
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId	int64
	RequestId	int64
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

type PullShardDataArgs struct {
	ConfigNum	int
	Shards		[]int
	ClientId	int64
	RequestId	int64
}

type PullShardDataReply struct {
	WrongLeader bool
	Err         Err
	StoredShards	[shardmaster.NShards]map[string]string
	ClientsCommit	map[int64]int64
}

type ReconfigArgs struct {
	Config		shardmaster.Config
	StoredShards	[shardmaster.NShards]map[string]string
	ClientsCommit	map[int64]int64
}

type ReconfigReply struct {
	WrongLeader bool
	Err			Err
}

type DeleteShardsArgs struct {
	ConfigNum	int
	DelShards []int
	ClientId	int64
	RequestId	int64
}

type DeleteShardsReply struct {
	WrongLeader bool
	Err         Err
}
