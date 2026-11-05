package kvserver

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/lvdousha26/mit-6.5840-labs/labrpc"
	"github.com/lvdousha26/mit-6.5840-labs/raft"
)

type result struct {
	Value    string
	Err      Err
	ClientID int64
	SeqNo    int64
}

// KVServer owns the state machine and one Raft peer. It applies commands in
// Raft log order and hands results back to the RPC that submitted them.
type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	kv         map[string]string
	dedup      map[int64]int64  // client id -> highest sequence number applied
	lastResult map[int64]result // reply to replay for a duplicate request
	results    map[int]result   // raft index -> result, consumed by execute

	dead     int32
	killOnce sync.Once
	done     chan struct{}
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *KVServer {
	kv := &KVServer{
		me:         me,
		applyCh:    make(chan raft.ApplyMsg, 4096),
		kv:         make(map[string]string),
		dedup:      make(map[int64]int64),
		lastResult: make(map[int64]result),
		results:    make(map[int]result),
		done:       make(chan struct{}),
	}
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	go kv.applyLoop()
	return kv
}

// Raft lets the test harness register both services on the same server.
func (kv *KVServer) Raft() *raft.Raft { return kv.rf }

func (kv *KVServer) killed() bool { return atomic.LoadInt32(&kv.dead) == 1 }

func (kv *KVServer) Kill() {
	kv.killOnce.Do(func() {
		atomic.StoreInt32(&kv.dead, 1)
		close(kv.done)
		kv.rf.Kill()
	})
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) error {
	value, err := kv.execute(Op{
		Type: OpGet, Key: args.Key, ClientID: args.ClientID, SeqNo: args.SeqNo,
	})
	reply.Value, reply.Err = value, err
	return nil
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	_, err := kv.execute(Op{
		Type: args.Op, Key: args.Key, Value: args.Value,
		ClientID: args.ClientID, SeqNo: args.SeqNo,
	})
	reply.Err = err
	return nil
}

// execute submits an op to Raft and waits for that exact index to be applied.
// If leadership is lost first, the client is told to try another server.
func (kv *KVServer) execute(op Op) (string, Err) {
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		return "", ErrWrongLeader
	}
	kv.mu.Lock()
	for {
		if r, ok := kv.results[index]; ok {
			delete(kv.results, index)
			kv.mu.Unlock()
			// A different leader may have overwritten this index with an
			// unrelated entry. Never return that result to this client.
			if r.ClientID != op.ClientID || r.SeqNo != op.SeqNo {
				return "", ErrWrongLeader
			}
			return r.Value, r.Err
		}
		if curTerm, leader := kv.rf.GetState(); !leader || curTerm != term {
			kv.mu.Unlock()
			return "", ErrWrongLeader
		}
		kv.mu.Unlock()
		time.Sleep(4 * time.Millisecond)
		kv.mu.Lock()
		if kv.killed() {
			kv.mu.Unlock()
			return "", ErrWrongLeader
		}
	}
}

func (kv *KVServer) applyLoop() {
	for {
		select {
		case <-kv.done:
			return
		case msg := <-kv.applyCh:
			if !msg.CommandValid {
				continue
			}
			kv.mu.Lock()
			// raft.NoOp entries only advance the log; they carry no client
			// request and therefore no one is waiting for a result.
			if op, ok := msg.Command.(Op); ok {
				kv.results[msg.CommandIndex] = kv.applyLocked(op)
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) applyLocked(op Op) result {
	if last, seen := kv.dedup[op.ClientID]; seen && op.SeqNo <= last {
		return kv.lastResult[op.ClientID]
	}
	r := result{ClientID: op.ClientID, SeqNo: op.SeqNo, Err: OK}
	switch op.Type {
	case OpPut:
		kv.kv[op.Key] = op.Value
	case OpAppend:
		kv.kv[op.Key] += op.Value
	case OpGet:
		v, ok := kv.kv[op.Key]
		if !ok {
			r.Err = ErrNoKey
		} else {
			r.Value = v
		}
	default:
		r.Err = ErrWrongLeader
	}
	kv.dedup[op.ClientID] = op.SeqNo
	kv.lastResult[op.ClientID] = r
	return r
}
