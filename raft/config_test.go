package raft

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/lvdousha26/mit-6.5840-labs/labrpc"
)

// config is a minimal version of the course's test harness: every server has
// an endpoint to every other server, so cutting an edge is just disabling
// one endpoint. Nothing binds a port.
type config struct {
	t          *testing.T
	net        *labrpc.Network
	n          int
	rafts      []*Raft
	persisters []*Persister
	servers    []*labrpc.Server
	ends       [][]*labrpc.ClientEnd
	applyChs   []chan ApplyMsg
	connected  []bool

	mu      sync.Mutex
	applied []map[int]any // server -> raft index -> command
	stop    chan struct{}
}

func serverName(i int) string { return fmt.Sprintf("s%d", i) }
func endName(i, j int) string { return fmt.Sprintf("c%d-%d", i, j) }

func makeConfig(t *testing.T, n int) *config {
	t.Helper()
	cfg := &config{
		t:          t,
		net:        labrpc.MakeNetwork(),
		n:          n,
		rafts:      make([]*Raft, n),
		persisters: make([]*Persister, n),
		servers:    make([]*labrpc.Server, n),
		ends:       make([][]*labrpc.ClientEnd, n),
		applyChs:   make([]chan ApplyMsg, n),
		applied:    make([]map[int]any, n),
		connected:  make([]bool, n),
		stop:       make(chan struct{}),
	}
	for i := 0; i < n; i++ {
		cfg.connected[i] = true
		cfg.persisters[i] = MakePersister()
		cfg.applyChs[i] = make(chan ApplyMsg, 4096)
		cfg.applied[i] = make(map[int]any)
		cfg.ends[i] = make([]*labrpc.ClientEnd, n)
	}
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			name := endName(i, j)
			cfg.ends[i][j] = cfg.net.MakeEnd(name)
			cfg.net.Connect(name, serverName(j))
		}
	}
	for i := 0; i < n; i++ {
		go cfg.readApplies(i)
	}
	t.Cleanup(cfg.cleanup)
	return cfg
}

func (cfg *config) readApplies(i int) {
	for {
		select {
		case m := <-cfg.applyChs[i]:
			if !m.CommandValid {
				continue
			}
			cfg.mu.Lock()
			cfg.applied[i][m.CommandIndex] = m.Command
			cfg.mu.Unlock()
		case <-cfg.stop:
			return
		}
	}
}

// start boots server i for the first time.
func (cfg *config) start(i int) {
	rf := Make(cfg.ends[i], i, cfg.persisters[i], cfg.applyChs[i])
	srv := labrpc.MakeServer()
	srv.RegisterName("Raft", rf)
	cfg.rafts[i], cfg.servers[i] = rf, srv
	cfg.net.AddServer(serverName(i), srv)
}

// restart keeps the persister, which is the whole point of Lab 2C.
func (cfg *config) restart(i int) {
	if cfg.rafts[i] != nil {
		cfg.rafts[i].Kill()
	}
	cfg.net.DeleteServer(serverName(i))
	cfg.start(i)
}

func (cfg *config) cleanup() {
	for i := 0; i < cfg.n; i++ {
		if cfg.rafts[i] != nil {
			cfg.rafts[i].Kill()
		}
	}
	close(cfg.stop)
}

func (cfg *config) setEdge(i, j int, enabled bool) {
	cfg.net.Enable(endName(i, j), enabled)
}

func (cfg *config) disconnect(i int) {
	// A fully isolated server keeps believing it is leader until it hears a
	// higher term. The harness must exclude it from leader checks, just as
	// the course harness does with cfg.connected.
	cfg.connected[i] = false
	for j := 0; j < cfg.n; j++ {
		if j == i {
			continue
		}
		cfg.setEdge(i, j, false)
		cfg.setEdge(j, i, false)
	}
}

func (cfg *config) connect(i int) {
	cfg.connected[i] = true
	for j := 0; j < cfg.n; j++ {
		cfg.setEdge(i, j, true)
		cfg.setEdge(j, i, true)
	}
}

func (cfg *config) connectAll() {
	for i := 0; i < cfg.n; i++ {
		cfg.connect(i)
	}
}

// partition cuts every edge between the two sets but keeps each set intact,
// so the minority can still elect among itself (and must fail to commit).
func (cfg *config) partition(a, b []int) {
	for _, i := range a {
		for _, j := range b {
			cfg.setEdge(i, j, false)
			cfg.setEdge(j, i, false)
		}
	}
}

func (cfg *config) leaders() map[int][]int {
	out := map[int][]int{}
	for i := 0; i < cfg.n; i++ {
		if cfg.rafts[i] == nil || !cfg.connected[i] {
			continue
		}
		term, isLeader := cfg.rafts[i].GetState()
		if isLeader {
			out[term] = append(out[term], i)
		}
	}
	return out
}

// checkOneLeader waits for a *steady* answer. Right after a partition the old
// leader is still reporting itself as leader until its election timer fires;
// accepting that first sighting would hand a command to a server that can
// never commit it.
func (cfg *config) checkOneLeader() int {
	cfg.t.Helper()
	deadline := time.Now().Add(6 * time.Second)
	last, streak := "", 0
	for time.Now().Before(deadline) {
		time.Sleep(40 * time.Millisecond)
		byTerm := cfg.leaders()
		bad, newest := false, -1
		for term, ids := range byTerm {
			if len(ids) > 1 {
				bad = true
				break
			}
			if term > newest {
				newest = term
			}
		}
		if bad || newest < 0 {
			last, streak = "", 0
			continue
		}
		key := fmt.Sprintf("%d/%d", newest, byTerm[newest][0])
		if key == last {
			streak++
		} else {
			last, streak = key, 1
		}
		if streak >= 3 {
			return byTerm[newest][0]
		}
	}
	cfg.t.Fatalf("no steady single leader: %v", cfg.leaders())
	return -1
}

func (cfg *config) checkNoLeader() {
	cfg.t.Helper()
	for rep := 0; rep < 6; rep++ {
		time.Sleep(80 * time.Millisecond)
		if len(cfg.leaders()) != 0 {
			cfg.t.Fatalf("a leader appeared without a majority: %v", cfg.leaders())
		}
	}
}

// checkStableLeader verifies that the same term stays in charge for a while.
func (cfg *config) checkStableLeader(leader, term int) {
	cfg.t.Helper()
	for rep := 0; rep < 12; rep++ {
		time.Sleep(60 * time.Millisecond)
		byTerm := cfg.leaders()
		if len(byTerm) != 1 || len(byTerm[term]) != 1 || byTerm[term][0] != leader {
			cfg.t.Fatalf("leadership changed: want %d term %d, got %v", leader, term, byTerm)
		}
	}
}

func (cfg *config) waitApplied(index int, cmd any, servers int, timeout time.Duration) bool {
	cfg.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		cfg.mu.Lock()
		matched := 0
		for i := 0; i < cfg.n; i++ {
			if v, ok := cfg.applied[i][index]; ok && reflect.DeepEqual(v, cmd) {
				matched++
			}
		}
		cfg.mu.Unlock()
		if matched >= servers {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

func (cfg *config) checkNoApplyAt(index int) {
	cfg.t.Helper()
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	for i := 0; i < cfg.n; i++ {
		if v, ok := cfg.applied[i][index]; ok {
			cfg.t.Fatalf("server %d applied %v at index %d without a majority", i, v, index)
		}
	}
}

// startOnLeader finds the current leader and submits one command. Going
// through checkOneLeader matters: an isolated old leader would happily accept
// a command that can never be committed.
func (cfg *config) startOnLeader(cmd any) (int, int) {
	cfg.t.Helper()
	for attempt := 0; attempt < 4; attempt++ {
		leader := cfg.checkOneLeader()
		if index, term, ok := cfg.rafts[leader].Start(cmd); ok {
			return index, term
		}
		time.Sleep(100 * time.Millisecond)
	}
	cfg.t.Fatal("no leader accepted a command")
	return -1, -1
}

// checkLogsAgree compares every index that any server has applied. Divergent
// commands at the same index are the failure mode log backtracking prevents.
func (cfg *config) checkLogsAgree() {
	cfg.t.Helper()
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	max := 0
	for i := 0; i < cfg.n; i++ {
		for idx := range cfg.applied[i] {
			if idx > max {
				max = idx
			}
		}
	}
	for idx := 1; idx <= max; idx++ {
		var first any
		have := false
		for i := 0; i < cfg.n; i++ {
			if v, ok := cfg.applied[i][idx]; ok {
				if !have {
					first, have = v, true
				} else if !reflect.DeepEqual(first, v) {
					cfg.t.Fatalf("log divergence at index %d: server %d has %v, another has %v", idx, i, v, first)
				}
			}
		}
	}
}
