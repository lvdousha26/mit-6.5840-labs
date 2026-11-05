package raft

import "sync"

// Persister stands in for stable storage. It is shared by the old and the new
// Raft instance when a test restarts a server, which is why Save copies the
// bytes: a caller that reuses its buffer must not be able to rewrite history.
type Persister struct {
	mu    sync.Mutex
	state []byte
}

func MakePersister() *Persister { return &Persister{} }

func (p *Persister) Copy() *Persister {
	p.mu.Lock()
	defer p.mu.Unlock()
	q := &Persister{}
	q.state = append([]byte(nil), p.state...)
	return q
}

func (p *Persister) Save(state []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.state = append([]byte(nil), state...)
}

func (p *Persister) Read() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]byte(nil), p.state...)
}

func (p *Persister) RaftStateSize() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.state)
}
