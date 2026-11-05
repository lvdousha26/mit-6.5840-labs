package kvserver

import (
	"strings"
	"sync"
	"testing"
	"time"
)

func startAll(cfg *config) {
	for i := 0; i < cfg.n; i++ {
		cfg.start(i)
	}
}

func TestBasicKV(t *testing.T) {
	cfg := makeConfig(t, 5)
	startAll(cfg)
	ck := cfg.clerk()
	cfg.waitLeader()

	ck.Put("a", "1")
	if got := ck.Get("a"); got != "1" {
		t.Fatalf("Put/Get: got %q, want %q", got, "1")
	}
	ck.Append("a", "2")
	if got := ck.Get("a"); got != "12" {
		t.Fatalf("Append: got %q, want %q", got, "12")
	}
	ck.Put("b", "hello")
	if got := ck.Get("b"); got != "hello" {
		t.Fatalf("second key: got %q", got)
	}
	if got := ck.Get("missing"); got != "" {
		t.Fatalf("missing key returned %q", got)
	}
}

func TestConcurrentAppend(t *testing.T) {
	cfg := makeConfig(t, 5)
	startAll(cfg)
	cfg.waitLeader()

	const clerks, ops = 5, 20
	var wg sync.WaitGroup
	for c := 0; c < clerks; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ck := cfg.clerk()
			for i := 0; i < ops; i++ {
				ck.Append("k", "x")
			}
		}()
	}
	wg.Wait()

	want := strings.Repeat("x", clerks*ops)
	if got := cfg.clerk().Get("k"); got != want {
		t.Fatalf("concurrent appends: got %d chars, want %d", len(got), len(want))
	}
}

func TestLeaderChangeKeepsState(t *testing.T) {
	cfg := makeConfig(t, 5)
	startAll(cfg)
	ck := cfg.clerk()
	cfg.waitLeader()
	ck.Put("k", "v1")

	old := cfg.leader()
	if old < 0 {
		t.Fatal("no leader before the partition")
	}
	cfg.disconnect(old)
	// Give the remaining four servers time to elect a new leader.
	time.Sleep(650 * time.Millisecond)

	ck.Put("k", "v2")
	if got := ck.Get("k"); got != "v2" {
		t.Fatalf("write after leader change: got %q, want v2", got)
	}

	cfg.connect(old)
	time.Sleep(400 * time.Millisecond)
	if got := ck.Get("k"); got != "v2" {
		t.Fatalf("stale value after the old leader rejoined: got %q", got)
	}
}

// A client whose reply was lost resends the same sequence number. The
// server must not run Append twice.
func TestDuplicateRequestIsIdempotent(t *testing.T) {
	cfg := makeConfig(t, 3)
	startAll(cfg)
	cfg.waitLeader()
	leader := cfg.leader()

	args := &PutAppendArgs{
		Key: "k", Value: "x", Op: OpAppend, ClientID: 90210, SeqNo: 7,
	}
	for i := 0; i < 4; i++ {
		_, _ = cfg.callDirect(leader, args)
		time.Sleep(60 * time.Millisecond)
	}
	if got := cfg.clerk().Get("k"); got != "x" {
		t.Fatalf("duplicate request appended more than once: %q", got)
	}
}

func TestRestartAllServers(t *testing.T) {
	cfg := makeConfig(t, 5)
	startAll(cfg)
	ck := cfg.clerk()
	cfg.waitLeader()
	ck.Put("k", "persisted")

	for i := 0; i < cfg.n; i++ {
		cfg.restart(i)
		time.Sleep(120 * time.Millisecond)
	}
	cfg.waitLeader()
	if got := ck.Get("k"); got != "persisted" {
		t.Fatalf("state lost after every server restarted: %q", got)
	}
	ck.Append("k", "+")
	if got := ck.Get("k"); got != "persisted+" {
		t.Fatalf("no progress after restarts: %q", got)
	}
}
