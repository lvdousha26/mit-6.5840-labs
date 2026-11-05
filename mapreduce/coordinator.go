package mapreduce

import (
	"net"
	"net/rpc"
	"sync"
	"time"
)

type Phase int

const (
	PhaseMap Phase = iota
	PhaseReduce
	PhaseDone
)

type taskState struct {
	assigned bool
	done     bool
	version  int
	deadline time.Time
}

// Coordinator is the single point that assigns work. All mutable state is
// under mu; each RPC takes it once, so a burst of workers cannot observe
// two different views of the phase.
type Coordinator struct {
	mu      sync.Mutex
	dir     string
	files   []string
	nReduce int
	timeout time.Duration
	phase   Phase

	maps    []taskState
	reduces []taskState

	// mapOutputs[id] holds the nReduce intermediate filenames, in reduce-id
	// order, from the worker whose report was accepted for map task id.
	mapOutputs  map[int][]string
	outputFiles map[int]string

	listener net.Listener
	rpcSrv   *rpc.Server
}

func MakeCoordinator(files []string, nReduce int, dir string, timeout time.Duration) *Coordinator {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &Coordinator{
		dir:         dir,
		files:       append([]string(nil), files...),
		nReduce:     nReduce,
		timeout:     timeout,
		phase:       PhaseMap,
		maps:        make([]taskState, len(files)),
		reduces:     make([]taskState, nReduce),
		mapOutputs:  make(map[int][]string),
		outputFiles: make(map[int]string),
	}
}

// Start listens on a loopback port and serves RPCs in the background.
func (c *Coordinator) Start() (string, error) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	srv := rpc.NewServer()
	// Register only the two RPC methods. Registering *Coordinator directly
	// would make net/rpc log complaints about Start/Stop/Done.
	if err := srv.RegisterName("Coordinator", &coordinatorRPC{c: c}); err != nil {
		_ = lis.Close()
		return "", err
	}
	c.listener, c.rpcSrv = lis, srv
	// Use our own accept loop instead of srv.Accept: the latter logs
	// "use of closed network connection" every time a test stops a job.
	go func() {
		for {
			conn, err := lis.Accept()
			if err != nil {
				return
			}
			go srv.ServeConn(conn)
		}
	}()
	return lis.Addr().String(), nil
}

func (c *Coordinator) Stop() {
	if c.listener != nil {
		_ = c.listener.Close()
	}
}

func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.phase == PhaseDone
}

// expireLocked releases tasks whose worker stopped reporting. The version is
// deliberately not changed here: it is bumped on the next assignment, which
// is what invalidates the late report from the presumed-dead worker.
func (c *Coordinator) expireLocked(now time.Time) {
	for i := range c.maps {
		if c.maps[i].assigned && !c.maps[i].done && now.After(c.maps[i].deadline) {
			c.maps[i].assigned = false
		}
	}
	for i := range c.reduces {
		if c.reduces[i].assigned && !c.reduces[i].done && now.After(c.reduces[i].deadline) {
			c.reduces[i].assigned = false
		}
	}
}

func allDone(states []taskState) bool {
	for i := range states {
		if !states[i].done {
			return false
		}
	}
	return true
}

func (c *Coordinator) nextTask() Task {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	c.expireLocked(now)

	if c.phase == PhaseMap && allDone(c.maps) {
		c.phase = PhaseReduce
	}
	if c.phase == PhaseReduce && allDone(c.reduces) {
		c.phase = PhaseDone
	}

	if c.phase == PhaseDone {
		return Task{Type: ExitTask}
	}

	if c.phase == PhaseMap {
		for i := range c.maps {
			s := &c.maps[i]
			if !s.done && !s.assigned {
				s.assigned = true
				s.version++
				s.deadline = now.Add(c.timeout)
				return Task{
					Type: MapTask, ID: i, Version: s.version,
					NReduce: c.nReduce, Dir: c.dir, Files: []string{c.files[i]},
				}
			}
		}
		return Task{Type: WaitTask}
	}

	for i := range c.reduces {
		s := &c.reduces[i]
		if !s.done && !s.assigned {
			s.assigned = true
			s.version++
			s.deadline = now.Add(c.timeout)
			return Task{
				Type: ReduceTask, ID: i, Version: s.version,
				NReduce: c.nReduce, Dir: c.dir, Files: c.reduceInputsLocked(i),
			}
		}
	}
	return Task{Type: WaitTask}
}

func (c *Coordinator) reduceInputsLocked(reduceID int) []string {
	files := make([]string, 0, len(c.maps))
	for i := range c.maps {
		outs := c.mapOutputs[i]
		if reduceID < len(outs) {
			files = append(files, outs[reduceID])
		}
	}
	return files
}

// report accepts a completion only if the task is still assigned to exactly
// the version the worker was given. Everything else is a stale report from a
// worker that was timed out, and its files must not enter the reduce inputs.
func (c *Coordinator) report(t TaskType, id, version int, files []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// A report may be the first RPC to arrive after a deadline passed, so the
	// expiry check cannot live only in nextTask.
	c.expireLocked(time.Now())

	var s *taskState
	switch t {
	case MapTask:
		if id >= 0 && id < len(c.maps) {
			s = &c.maps[id]
		}
	case ReduceTask:
		if id >= 0 && id < len(c.reduces) {
			s = &c.reduces[id]
		}
	default:
		return
	}
	if s == nil || s.done || !s.assigned || s.version != version {
		return
	}
	s.done = true
	s.assigned = false

	switch t {
	case MapTask:
		c.mapOutputs[id] = append([]string(nil), files...)
	case ReduceTask:
		if len(files) > 0 {
			c.outputFiles[id] = files[0]
		}
		if len(c.outputFiles) == c.nReduce {
			c.phase = PhaseDone
		}
	}
}

// MapVersions is exposed for the crash test, which checks that a task really
// was handed out twice.
func (c *Coordinator) MapVersions() []int {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]int, len(c.maps))
	for i := range c.maps {
		out[i] = c.maps[i].version
	}
	return out
}
