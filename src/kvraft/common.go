package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
	Get		 = "Get"
	Put		 = "Put"
	Append	 = "Append"
	PutAppend= "PutAppend"
)

type Err string

// Put or Append
type PutAppendArgs struct {
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
	LeaderId	int
	AppendId	int64
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
	LeaderId	int
}

type GetLeaderArgs struct {
	ClientId	int64
}

type GetLeaderReply struct {
	WrongLeader	bool
	Term		int
	LeaderId	int
}