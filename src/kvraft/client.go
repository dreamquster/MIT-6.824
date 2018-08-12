package raftkv

import "labrpc"
import "crypto/rand"
import (
	"math/big"
	"time"
	"log"
)

const NO_LEADER int = -1;

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId	int
	term        int
	id			int64
	latestSeq	int64
	commitId	int64
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
	// You'll have to add code here.
	ck.leaderId = NO_LEADER;
	ck.id = nrand()
	ck.CheckOneLeader()
	return ck
}

func (ck *Clerk) GetLeader() {
	getLeaderArgs := &GetLeaderArgs{0}
	replys := make([]GetLeaderReply, len(ck.servers))
	termLeaders := make(map[int][]int, 0)

	for idx, server := range ck.servers {
		ok := server.Call("RaftKV.GetLeader", getLeaderArgs, &replys[idx]);
		if !ok {
			continue
		}

		if !replys[idx].WrongLeader {
			t := replys[idx].Term
			termLeaders[t] = append(termLeaders[t], replys[idx].LeaderId)
		}
	}

	lastTermHasLeader := -1
	for t, leaderInTerm := range termLeaders {
		if 1 < len(leaderInTerm) {
			log.Fatalf("in term %d has %d leaders", t, len(leaderInTerm))
			ck.leaderId = NO_LEADER
			return
		}

		if lastTermHasLeader < t {
			lastTermHasLeader = t
		}
	}

	if -1 < lastTermHasLeader {
		ck.leaderId = termLeaders[lastTermHasLeader][0]
		ck.term = lastTermHasLeader
		return
	}
	ck.leaderId = NO_LEADER
	return
}

func (ck *Clerk) CheckOneLeader()  {
	ck.GetLeader()
	for ck.leaderId == NO_LEADER {
		time.Sleep(50 * time.Millisecond)
		ck.GetLeader()
	}
	log.Printf("find leader is %d", ck.leaderId)
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	getArgs := GetArgs{key, ck.id}

	server := ck.servers[ck.leaderId]
	reply := &GetReply{}
	ok := server.Call("RaftKV.Get", &getArgs, &reply)
	if ok && reply.Err == OK &&!reply.WrongLeader {
		return reply.Value
	}

	if ok && reply.WrongLeader {
		ck.CheckOneLeader()
	}

	return ck.Get(key)
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	requestId := ck.latestSeq
	ck.latestSeq++
	args := PutAppendArgs{key, value, op, ck.id, requestId}
	for {

		server := ck.servers[ck.leaderId]
		reply := &PutAppendReply{}
		ok := server.Call("RaftKV.PutAppend", &args, &reply)
		if ok && !reply.WrongLeader {
			ck.commitId = requestId
			return
		}
		if reply.WrongLeader {
			ck.CheckOneLeader()
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
