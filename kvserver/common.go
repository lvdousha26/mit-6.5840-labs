// Package kvserver is Lab 3: a linearizable key/value store replicated by
// Raft. Clients tag every request with (client id, sequence number) so a
// retry after a lost reply cannot apply the same Append twice.
package kvserver

import "encoding/gob"

type Err string

const (
	OK             Err = "OK"
	ErrNoKey       Err = "ErrNoKey"
	ErrWrongLeader Err = "ErrWrongLeader"
	ErrTimeout     Err = "ErrTimeout"
)

const (
	OpGet    = "Get"
	OpPut    = "Put"
	OpAppend = "Append"
)

type Op struct {
	Type     string
	Key      string
	Value    string
	ClientID int64
	SeqNo    int64
}

type GetArgs struct {
	Key      string
	ClientID int64
	SeqNo    int64
}

type GetReply struct {
	Err   Err
	Value string
}

type PutAppendArgs struct {
	Key      string
	Value    string
	Op       string
	ClientID int64
	SeqNo    int64
}

type PutAppendReply struct {
	Err Err
}

func init() {
	// The op travels inside a persisted Raft log entry, so gob needs to know
	// its concrete type on the way back out of stable storage.
	gob.Register(Op{})
}
