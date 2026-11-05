package kvserver

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/lvdousha26/mit-6.5840-labs/labrpc"
)

var nextClientID atomic.Int64

func init() { nextClientID.Store(time.Now().UnixNano() % 1_000_000_000) }

// Clerk is a client handle. It remembers the last server that answered so
// the common case costs one RPC, and walks the server list on a wrong-leader
// reply. A retry reuses the same sequence number: the server's dedup table
// makes that safe even if the first request actually committed.
type Clerk struct {
	servers  []*labrpc.ClientEnd
	clientID int64
	seq      atomic.Int64
	mu       sync.Mutex
	leader   int
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	return &Clerk{servers: servers, clientID: nextClientID.Add(1)}
}

func (ck *Clerk) currentLeader() int {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	return ck.leader
}

func (ck *Clerk) nextLeader() {
	ck.mu.Lock()
	ck.leader = (ck.leader + 1) % len(ck.servers)
	ck.mu.Unlock()
}

func (ck *Clerk) Get(key string) string {
	args := GetArgs{Key: key, ClientID: ck.clientID, SeqNo: ck.seq.Add(1)}
	for attempt := 0; attempt < 300; attempt++ {
		var reply GetReply
		ok := ck.servers[ck.currentLeader()].Call("KVServer.Get", &args, &reply)
		if ok && reply.Err != ErrWrongLeader {
			if reply.Err == ErrNoKey {
				return ""
			}
			return reply.Value
		}
		ck.nextLeader()
		time.Sleep(12 * time.Millisecond)
	}
	return ""
}

func (ck *Clerk) Put(key, value string)    { ck.PutAppend(key, value, OpPut) }
func (ck *Clerk) Append(key, value string) { ck.PutAppend(key, value, OpAppend) }

func (ck *Clerk) PutAppend(key, value, op string) {
	args := PutAppendArgs{
		Key: key, Value: value, Op: op,
		ClientID: ck.clientID, SeqNo: ck.seq.Add(1),
	}
	for attempt := 0; attempt < 300; attempt++ {
		var reply PutAppendReply
		ok := ck.servers[ck.currentLeader()].Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.Err != ErrWrongLeader {
			return
		}
		ck.nextLeader()
		time.Sleep(12 * time.Millisecond)
	}
}
