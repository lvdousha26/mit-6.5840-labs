package labrpc

import (
	"sync/atomic"
	"testing"
	"time"
)

type echoArgs struct{ Value int }
type echoReply struct{ Value int }

type Echo struct{}

func (e *Echo) Double(args *echoArgs, reply *echoReply) error {
	reply.Value = args.Value * 2
	return nil
}

func makeEchoNetwork(t *testing.T) (*Network, *ClientEnd, *Server) {
	t.Helper()
	n := MakeNetwork()
	s := MakeServer()
	s.RegisterName("Echo", &Echo{})
	n.AddServer("s0", s)
	end := n.MakeEnd("c0")
	n.Connect("c0", "s0")
	end.SetTimeout(40 * time.Millisecond)
	return n, end, s
}

func TestCall(t *testing.T) {
	_, end, _ := makeEchoNetwork(t)
	var reply echoReply
	if !end.Call("Echo.Double", &echoArgs{Value: 21}, &reply) {
		t.Fatal("call failed")
	}
	if reply.Value != 42 {
		t.Fatalf("got %d, want 42", reply.Value)
	}
}

func TestDisconnectedCallTimesOut(t *testing.T) {
	n, end, _ := makeEchoNetwork(t)
	n.Enable("c0", false)
	start := time.Now()
	var reply echoReply
	if end.Call("Echo.Double", &echoArgs{Value: 5}, &reply) {
		t.Fatal("call on a disabled endpoint succeeded")
	}
	if time.Since(start) < 20*time.Millisecond {
		t.Fatal("disconnected call returned instantly instead of timing out")
	}
	if reply.Value != 0 {
		t.Fatalf("reply was written on a failed call: %d", reply.Value)
	}
	n.Enable("c0", true)
	if !end.Call("Echo.Double", &echoArgs{Value: 5}, &reply) || reply.Value != 10 {
		t.Fatal("call did not recover after re-enabling")
	}
}

func TestDroppedPackets(t *testing.T) {
	n, end, _ := makeEchoNetwork(t)
	n.Reliable(false)
	n.SetDropRate(100)
	var reply echoReply
	for i := 0; i < 5; i++ {
		if end.Call("Echo.Double", &echoArgs{Value: 1}, &reply) {
			t.Fatal("call survived a 100% drop rate")
		}
	}
	n.Reliable(true)
	if !end.Call("Echo.Double", &echoArgs{Value: 3}, &reply) || reply.Value != 6 {
		t.Fatal("call did not recover after the network became reliable")
	}
}

// Flip simulates the nasty case: the handler runs and changes server state,
// but the partition appears before the reply gets back. The caller has to
// treat it as a failed RPC even though the request was processed.
type Flip struct {
	n     *Network
	calls int32
}

func (f *Flip) Do(args *echoArgs, reply *echoReply) error {
	reply.Value = args.Value * 2
	if atomic.AddInt32(&f.calls, 1) == 1 {
		f.n.Enable("c0", false)
	}
	return nil
}

func TestReplyLostAfterHandlerRan(t *testing.T) {
	n := MakeNetwork()
	f := &Flip{n: n}
	s := MakeServer()
	s.RegisterName("Flip", f)
	n.AddServer("s0", s)
	end := n.MakeEnd("c0")
	n.Connect("c0", "s0")
	end.SetTimeout(40 * time.Millisecond)

	var reply echoReply
	if end.Call("Flip.Do", &echoArgs{Value: 7}, &reply) {
		t.Fatal("call succeeded even though the reply was dropped")
	}
	if atomic.LoadInt32(&f.calls) != 1 {
		t.Fatal("handler did not run")
	}
	if reply.Value != 0 {
		t.Fatalf("reply was written despite the lost reply: %d", reply.Value)
	}
	n.Enable("c0", true)
	if !end.Call("Flip.Do", &echoArgs{Value: 7}, &reply) || reply.Value != 14 {
		t.Fatal("round trip did not recover")
	}
}

func TestUnknownMethodTimesOut(t *testing.T) {
	n, end, _ := makeEchoNetwork(t)
	_ = n
	var reply echoReply
	start := time.Now()
	if end.Call("Echo.Missing", &echoArgs{Value: 1}, &reply) {
		t.Fatal("unknown method succeeded")
	}
	if time.Since(start) < 20*time.Millisecond {
		t.Fatal("unknown method returned instantly")
	}
}

func TestServerReplacement(t *testing.T) {
	n, end, _ := makeEchoNetwork(t)
	var reply echoReply
	if !end.Call("Echo.Double", &echoArgs{Value: 2}, &reply) || reply.Value != 4 {
		t.Fatal("initial server not reachable")
	}
	n.DeleteServer("s0")
	if end.Call("Echo.Double", &echoArgs{Value: 2}, &reply) {
		t.Fatal("call succeeded after the server was removed")
	}
	s2 := MakeServer()
	s2.RegisterName("Echo", &Echo{})
	n.AddServer("s0", s2)
	if !end.Call("Echo.Double", &echoArgs{Value: 2}, &reply) || reply.Value != 4 {
		t.Fatal("call did not reach the replacement server")
	}
}
