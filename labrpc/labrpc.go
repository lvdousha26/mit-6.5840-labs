// Package labrpc is a tiny in-process RPC network with injectable faults.
//
// The Raft and KV labs need to create partitions, drop packets, restart
// servers and delay replies. Doing that over real sockets would make the
// tests depend on ports and timing of the host kernel, so everything here
// runs in goroutines connected by channels. A call still goes through a
// lookup, a possible drop, an asynchronous dispatch, and a deadline.
//
// Two deliberate simplifications, both called out in the README:
//   - arguments are shallow-copied, not serialized. Handlers must treat
//     args as read-only and copy any slice they intend to retain.
//   - a disconnected endpoint sleeps until its deadline instead of
//     blocking until the network heals, which keeps callers from spinning.
package labrpc

import (
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const defaultTimeout = 120 * time.Millisecond

var errorType = reflect.TypeOf((*error)(nil)).Elem()

// ---------------------------------------------------------------------------
// Network
// ---------------------------------------------------------------------------

// Network owns endpoints, servers, and the failure switches.
// Every exported method is safe to call concurrently with an in-flight RPC.
type Network struct {
	mu          sync.Mutex
	reliable    bool
	dropPercent int // 0..100, applied to requests and replies independently
	longDelay   bool
	rnd         *rand.Rand
	ends        map[string]*ClientEnd
	enabled     map[string]bool
	servers     map[string]*Server
	conn        map[string]string // endpoint name -> server name
}

func MakeNetwork() *Network {
	return &Network{
		reliable: true,
		rnd:      rand.New(rand.NewSource(time.Now().UnixNano())),
		ends:     make(map[string]*ClientEnd),
		enabled:  make(map[string]bool),
		servers:  make(map[string]*Server),
		conn:     make(map[string]string),
	}
}

// MakeEnd creates an endpoint. It is initially enabled but not connected.
func (n *Network) MakeEnd(name string) *ClientEnd {
	n.mu.Lock()
	defer n.mu.Unlock()
	e := &ClientEnd{name: name, net: n}
	e.timeoutNs.Store(int64(defaultTimeout))
	n.ends[name] = e
	n.enabled[name] = true
	return e
}

// Connect wires an endpoint to a server. An empty server name disconnects it.
func (n *Network) Connect(endname, servername string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.conn[endname] = servername
}

// Enable toggles a single direction of communication. This is how a test
// builds a partition: disable every endpoint crossing the cut.
func (n *Network) Enable(endname string, enabled bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.enabled[endname] = enabled
}

func (n *Network) IsEnabled(endname string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.enabled[endname]
}

func (n *Network) AddServer(name string, s *Server) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.servers[name] = s
}

func (n *Network) DeleteServer(name string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.servers, name)
}

// Reliable(false) turns on random drops at the configured rate.
func (n *Network) Reliable(yes bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.reliable = yes
}

func (n *Network) SetDropRate(percent int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if percent < 0 {
		percent = 0
	}
	if percent > 100 {
		percent = 100
	}
	n.dropPercent = percent
}

// LongDelays adds a small random delay before each dispatch.
func (n *Network) LongDelays(yes bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.longDelay = yes
}

// lookup returns the target server and the fault decisions for one call.
// All random choices are made under the lock so they are race-free.
func (n *Network) lookup(endname string) (*Server, bool, bool, time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()

	dropRequest := !n.reliable && n.rnd.Intn(100) < n.dropPercent
	dropReply := !n.reliable && n.rnd.Intn(100) < n.dropPercent
	if !n.enabled[endname] || n.conn[endname] == "" || dropRequest {
		return nil, false, false, 0
	}
	srv := n.servers[n.conn[endname]]
	var delay time.Duration
	if n.longDelay {
		delay = time.Duration(n.rnd.Intn(27)) * time.Millisecond
	}
	return srv, true, dropReply, delay
}

func (n *Network) replyAllowed(endname string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.enabled[endname]
}

// ---------------------------------------------------------------------------
// ClientEnd
// ---------------------------------------------------------------------------

type ClientEnd struct {
	name      string
	net       *Network
	timeoutNs atomic.Int64
}

func (e *ClientEnd) SetTimeout(d time.Duration) {
	if d <= 0 {
		d = defaultTimeout
	}
	e.timeoutNs.Store(int64(d))
}

func (e *ClientEnd) timeout() time.Duration {
	return time.Duration(e.timeoutNs.Load())
}

// Call sends "Service.Method" to the connected server and waits for a reply.
// It returns false on a drop, a disconnection, a handler error, or a timeout.
// args and reply must both be pointers; reply is only written on success.
func (e *ClientEnd) Call(svcMeth string, args any, reply any) bool {
	parts := strings.SplitN(svcMeth, ".", 2)
	if len(parts) != 2 {
		panic("labrpc: service method must look like \"Service.Method\": " + svcMeth)
	}
	av, rv := reflect.ValueOf(args), reflect.ValueOf(reply)
	if av.Kind() != reflect.Ptr || rv.Kind() != reflect.Ptr || av.IsNil() || rv.IsNil() {
		panic("labrpc: args and reply must be non-nil pointers")
	}

	srv, reachable, dropReply, delay := e.net.lookup(e.name)
	deadline := e.timeout()
	if !reachable || srv == nil {
		// Behave like a timed-out RPC rather than an instant failure, so a
		// partitioned leader does not busy-loop on its retry path.
		time.Sleep(deadline)
		return false
	}
	if delay > 0 {
		time.Sleep(delay)
	}

	argv := shallowCopy(av)
	replyv := reflect.New(rv.Elem().Type())
	ch := make(chan bool, 1) // buffered: a late handler never blocks forever
	go srv.dispatch(parts[0], parts[1], argv.Interface(), replyv.Interface(), e, dropReply, ch)

	select {
	case <-ch:
		rv.Elem().Set(replyv.Elem())
		return true
	case <-time.After(deadline):
		return false
	}
}

// shallowCopy gives the handler its own top-level struct. Slices and maps
// still share storage, which is why Raft copies AppendEntries before keeping it.
func shallowCopy(v reflect.Value) reflect.Value {
	c := reflect.New(v.Elem().Type())
	c.Elem().Set(v.Elem())
	return c
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

type service struct {
	methods map[string]reflect.Value
}

type Server struct {
	mu       sync.RWMutex
	services map[string]*service
}

func MakeServer() *Server {
	return &Server{services: make(map[string]*service)}
}

// RegisterName registers every exported method with the signature
//
//	func (rcvr *T) Method(args *Args, reply *Reply) error
//
// under the given service name. rcvr must be a pointer.
func (s *Server) RegisterName(name string, rcvr any) {
	rv := reflect.ValueOf(rcvr)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		panic("labrpc: RegisterName needs a non-nil pointer")
	}
	svc := &service{methods: make(map[string]reflect.Value)}
	rt := rv.Type()
	for i := 0; i < rt.NumMethod(); i++ {
		m := rt.Method(i)
		if !m.IsExported() {
			continue
		}
		mv := rv.MethodByName(m.Name)
		mt := mv.Type()
		if mt.NumIn() != 2 || mt.NumOut() != 1 || mt.Out(0) != errorType {
			continue
		}
		if mt.In(0).Kind() != reflect.Ptr || mt.In(1).Kind() != reflect.Ptr {
			continue
		}
		svc.methods[m.Name] = mv
	}
	if len(svc.methods) == 0 {
		panic(fmt.Sprintf("labrpc: %s has no RPC methods", name))
	}
	s.mu.Lock()
	s.services[name] = svc
	s.mu.Unlock()
}

func (s *Server) method(svcName, methName string) reflect.Value {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if svc := s.services[svcName]; svc != nil {
		return svc.methods[methName]
	}
	return reflect.Value{}
}

type dispatchJob struct {
	svcName, methName string
	args, reply       any
	end               *ClientEnd
	dropReply         bool
	ch                chan<- bool
}

func (s *Server) dispatch(svcName, methName string, args, reply any, end *ClientEnd, dropReply bool, ch chan<- bool) {
	// Each call runs in its own goroutine. Deliberately no global lock here:
	// all peers share a service name ("Raft"), and serializing on it would
	// serialize unrelated servers and turn one slow call into a stuck cluster.
	// Handlers are responsible for guarding their own state.
	runDispatch(s, dispatchJob{svcName, methName, args, reply, end, dropReply, ch})
}

func runDispatch(s *Server, job dispatchJob) {
	m := s.method(job.svcName, job.methName)
	if !m.IsValid() {
		return // unknown method: let the caller time out
	}
	ok := true
	func() {
		defer func() {
			if recover() != nil {
				ok = false
			}
		}()
		out := m.Call([]reflect.Value{reflect.ValueOf(job.args), reflect.ValueOf(job.reply)})
		if !out[0].IsNil() {
			ok = false
		}
	}()
	if !ok {
		return
	}
	// The reply still has to travel back over the network. A packet dropped
	// here, or a partition created while the handler ran, loses the reply
	// even though the request was processed -- exactly the ambiguity Raft
	// has to live with.
	if job.dropReply || !job.end.net.replyAllowed(job.end.name) {
		return
	}
	select {
	case job.ch <- true:
	default:
	}
}
