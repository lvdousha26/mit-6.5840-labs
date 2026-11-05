package raft

import (
	"fmt"
	"testing"
	"time"
)

// Lab 2A -------------------------------------------------------------------

func TestInitialElection(t *testing.T) {
	cfg := makeConfig(t, 3)
	for i := 0; i < 3; i++ {
		cfg.start(i)
	}
	leader := cfg.checkOneLeader()
	term, _ := cfg.rafts[leader].GetState()
	cfg.checkStableLeader(leader, term)
}

func TestReElectionAfterLeaderIsIsolated(t *testing.T) {
	cfg := makeConfig(t, 5)
	for i := 0; i < 5; i++ {
		cfg.start(i)
	}
	old := cfg.checkOneLeader()
	cfg.disconnect(old)

	newLeader := cfg.checkOneLeader()
	if newLeader == old {
		t.Fatal("isolated server was still reported as the only leader")
	}
	cfg.connect(old)
	final := cfg.checkOneLeader()
	term, _ := cfg.rafts[final].GetState()
	cfg.checkStableLeader(final, term)
}

// Lab 2B -------------------------------------------------------------------

func TestBasicAgree(t *testing.T) {
	cfg := makeConfig(t, 5)
	for i := 0; i < 5; i++ {
		cfg.start(i)
	}
	cfg.checkOneLeader()
	for it := 0; it < 5; it++ {
		cmd := 100 + it
		index, _ := cfg.startOnLeader(cmd)
		if !cfg.waitApplied(index, cmd, 5, 5*time.Second) {
			t.Fatalf("command %d at index %d did not reach all servers", cmd, index)
		}
	}
	cfg.checkLogsAgree()
}

func TestNoCommitWithoutMajority(t *testing.T) {
	cfg := makeConfig(t, 3)
	for i := 0; i < 3; i++ {
		cfg.start(i)
	}
	leader := cfg.checkOneLeader()
	for i := 0; i < 3; i++ {
		if i != leader {
			cfg.disconnect(i)
		}
	}

	var indices []int
	for it := 0; it < 3; it++ {
		index, _, ok := cfg.rafts[leader].Start(500 + it)
		if !ok {
			t.Fatal("the isolated leader stepped down unexpectedly")
		}
		indices = append(indices, index)
	}
	time.Sleep(700 * time.Millisecond)
	for _, index := range indices {
		cfg.checkNoApplyAt(index)
	}

	cfg.connectAll()
	cfg.checkOneLeader()
	index, _ := cfg.startOnLeader(999)
	if !cfg.waitApplied(index, 999, 3, 6*time.Second) {
		t.Fatal("the cluster did not make progress after the partition healed")
	}
	cfg.checkLogsAgree()
}

func TestOldLeaderRejoins(t *testing.T) {
	cfg := makeConfig(t, 5)
	for i := 0; i < 5; i++ {
		cfg.start(i)
	}
	old := cfg.checkOneLeader()

	minority := []int{old, (old + 1) % 5}
	inMinority := map[int]bool{minority[0]: true, minority[1]: true}
	var majority []int
	for i := 0; i < 5; i++ {
		if !inMinority[i] {
			majority = append(majority, i)
		}
	}
	cfg.partition(minority, majority)
	// Let one full election timeout pass so the isolated leader has stepped
	// down into candidate state before we look for the new leader.
	time.Sleep(600 * time.Millisecond)

	// The majority must elect a leader and commit without the old leader.
	cfg.checkOneLeader()
	for it := 0; it < 3; it++ {
		cmd := 200 + it
		index, _ := cfg.startOnLeader(cmd)
		if !cfg.waitApplied(index, cmd, 3, 5*time.Second) {
			t.Fatalf("majority failed to commit command %d", cmd)
		}
	}
	// Meanwhile the isolated old leader keeps accepting writes that go nowhere.
	if _, _, ok := cfg.rafts[old].Start(777); !ok {
		t.Fatal("old leader stepped down without hearing a higher term")
	}

	cfg.connectAll()
	cfg.checkOneLeader()
	index, _ := cfg.startOnLeader(888)
	if !cfg.waitApplied(index, 888, 5, 8*time.Second) {
		t.Fatal("servers did not converge after the old leader rejoined")
	}
	cfg.checkLogsAgree()
}

// Lab 2C -------------------------------------------------------------------

func TestPersistAndRestart(t *testing.T) {
	cfg := makeConfig(t, 5)
	for i := 0; i < 5; i++ {
		cfg.start(i)
	}
	cfg.checkOneLeader()
	for it := 0; it < 3; it++ {
		cmd := 300 + it
		index, _ := cfg.startOnLeader(cmd)
		if !cfg.waitApplied(index, cmd, 5, 5*time.Second) {
			t.Fatalf("command %d not committed", cmd)
		}
	}
	before := cfg.persisters[0].RaftStateSize()
	if before == 0 {
		t.Fatal("nothing was persisted")
	}

	// Restart one at a time so the cluster never loses its majority.
	for i := 0; i < 5; i++ {
		cfg.restart(i)
		time.Sleep(120 * time.Millisecond)
	}
	cfg.checkOneLeader()
	cmd := 333
	index, _ := cfg.startOnLeader(cmd)
	if !cfg.waitApplied(index, cmd, 5, 6*time.Second) {
		t.Fatal("no progress after every server restarted")
	}
	// The log, not just the term, has to survive the restart.
	for i := 0; i < 5; i++ {
		if cfg.persisters[i].RaftStateSize() < before {
			t.Fatalf("server %d lost persistent state: %d < %d",
				i, cfg.persisters[i].RaftStateSize(), before)
		}
	}
	cfg.checkLogsAgree()
}

// A light lossy network: the point is that retries and term checks eventually
// converge without any special-casing in the implementation.
func TestUnreliableAgree(t *testing.T) {
	cfg := makeConfig(t, 5)
	for i := 0; i < 5; i++ {
		cfg.start(i)
	}
	cfg.checkOneLeader()
	cfg.net.Reliable(false)
	cfg.net.SetDropRate(12)

	for it := 0; it < 3; it++ {
		cmd := fmt.Sprintf("cmd-%d", it)
		index, _ := cfg.startOnLeader(cmd)
		if !cfg.waitApplied(index, cmd, 5, 8*time.Second) {
			t.Fatalf("command %q did not commit over a lossy network", cmd)
		}
	}
	cfg.net.Reliable(true)
	cfg.checkLogsAgree()
}
