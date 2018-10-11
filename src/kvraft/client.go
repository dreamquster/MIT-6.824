package raftkv

import "labrpc"
import "crypto/rand"
import (
	"math/big"
	"time"
	"log"
	"sync"
)

const NO_LEADER int = -1;

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu        sync.Mutex
	leaderId  int
	term      int
	id        int64
	requestId int64
	commitId  int64
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
	ck.leaderId = 0;
	ck.id = nrand()
	ck.CheckOneLeader()
	return ck
}

func (ck *Clerk) GetLeader() {

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
	ck.mu.Lock()
	defer ck.mu.Unlock()
	getLeaderArgs := &GetLeaderArgs{0}
	ck.requestId++
	ret := ""
	for i := ck.leaderId; true; i = (i + 1) % len(ck.servers) {
		server := ck.servers[i]
		reply := GetReply{}
		if server.Call("RaftKV.GetLeader", getLeaderArgs, &reply) {
			if !reply.WrongLeader {
				ck.leaderId = i
				if reply.Err == OK {
					ret = reply.Value
				} else {
					ret = ""
				}
				break
			}
		}
	}

	return ret
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
	ck.mu.Lock()
	defer ck.mu.Unlock()
	requestId := ck.requestId
	ck.requestId++
	args := PutAppendArgs{key, value, op, ck.id, requestId}
	for i := ck.leaderId; true; i = (i + 1) % len(ck.servers) {
		server := ck.servers[i]
		reply := &PutAppendReply{}
		if server.Call("RaftKV.PutAppend", &args, &reply) {
			if !reply.WrongLeader {
				ck.leaderId = i
				return
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
