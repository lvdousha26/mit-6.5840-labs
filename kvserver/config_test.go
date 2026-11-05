package kvserver

import (
	"fmt"
	"testing"
	"time"

	"github.com/lvdousha26/mit-6.5840-labs/labrpc"
	"github.com/lvdousha26/mit-6.5840-labs/raft"
)

type config struct {
	t          *testing.T
	net        *labrpc.Network
	n          int
	kvs        []*KVServer
	persisters []*raft.Persister
	rends      [][]*labrpc.ClientEnd // raft endpoints: server i -> server j
	kends      []*labrpc.ClientEnd   // clerk endpoints: one per server
	connected  []bool
}

func sname(i int) string    { return fmt.Sprintf("s%d", i) }
func rname(i, j int) string { return fmt.Sprintf("r%d-%d", i, j) }
func kname(i int) string    { return fmt.Sprintf("k%d", i) }

func makeConfig(t *testing.T, n int) *config {
	t.Helper()
	cfg := &config{
		t:          t,
		net:        labrpc.MakeNetwork(),
		n:          n,
		kvs:        make([]*KVServer, n),
		persisters: make([]*raft.Persister, n),
		rends:      make([][]*labrpc.ClientEnd, n),
		kends:      make([]*labrpc.ClientEnd, n),
		connected:  make([]bool, n),
	}
	for i := 0; i < n; i++ {
		cfg.persisters[i] = raft.MakePersister()
		cfg.connected[i] = true
		cfg.rends[i] = make([]*labrpc.ClientEnd, n)
	}
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			cfg.rends[i][j] = cfg.net.MakeEnd(rname(i, j))
			cfg.net.Connect(rname(i, j), sname(j))
		}
	}
	for j := 0; j < n; j++ {
		cfg.kends[j] = cfg.net.MakeEnd(kname(j))
		cfg.net.Connect(kname(j), sname(j))
	}
	t.Cleanup(cfg.cleanup)
	return cfg
}

func (cfg *config) start(i int) {
	kv := StartKVServer(cfg.rends[i], i, cfg.persisters[i])
	srv := labrpc.MakeServer()
	// Both services live on the same simulated server. An endpoint chooses
	// which one to talk to by the "Raft." / "KVServer." prefix on the call.
	srv.RegisterName("Raft", kv.Raft())
	srv.RegisterName("KVServer", kv)
	cfg.kvs[i] = kv
	cfg.net.AddServer(sname(i), srv)
}

func (cfg *config) restart(i int) {
	if cfg.kvs[i] != nil {
		cfg.kvs[i].Kill()
	}
	cfg.net.DeleteServer(sname(i))
	cfg.start(i)
}

func (cfg *config) cleanup() {
	for i := 0; i < cfg.n; i++ {
		if cfg.kvs[i] != nil {
			cfg.kvs[i].Kill()
		}
	}
}

func (cfg *config) clerk() *Clerk { return MakeClerk(cfg.kends) }

func (cfg *config) disconnect(i int) {
	cfg.connected[i] = false
	for j := 0; j < cfg.n; j++ {
		if j == i {
			continue
		}
		cfg.net.Enable(rname(i, j), false)
		cfg.net.Enable(rname(j, i), false)
	}
	cfg.net.Enable(kname(i), false)
}

func (cfg *config) connect(i int) {
	cfg.connected[i] = true
	for j := 0; j < cfg.n; j++ {
		cfg.net.Enable(rname(i, j), true)
		cfg.net.Enable(rname(j, i), true)
	}
	cfg.net.Enable(kname(i), true)
}

func (cfg *config) connectAll() {
	for i := 0; i < cfg.n; i++ {
		cfg.connect(i)
	}
}

func (cfg *config) leader() int {
	for i := 0; i < cfg.n; i++ {
		if cfg.kvs[i] == nil || !cfg.connected[i] {
			continue
		}
		if _, ok := cfg.kvs[i].Raft().GetState(); ok {
			return i
		}
	}
	return -1
}

func (cfg *config) waitLeader() int {
	cfg.t.Helper()
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		if l := cfg.leader(); l >= 0 {
			return l
		}
		time.Sleep(30 * time.Millisecond)
	}
	cfg.t.Fatal("no leader elected")
	return -1
}

// callDirect sends a single RPC to one server without the clerk's retry loop.
// Tests use it to submit a deliberately duplicated sequence number.
func (cfg *config) callDirect(server int, args *PutAppendArgs) (Err, bool) {
	var reply PutAppendReply
	ok := cfg.kends[server].Call("KVServer.PutAppend", args, &reply)
	if !ok {
		return "", false
	}
	return reply.Err, true
}
