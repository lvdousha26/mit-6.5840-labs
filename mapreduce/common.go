// Package mapreduce is Lab 1: a coordinator hands map and reduce tasks to
// workers over net/rpc, re-issues tasks whose worker stopped reporting, and
// ignores reports from workers that were superseded.
package mapreduce

import (
	"fmt"
	"hash/fnv"
)

type KeyValue struct {
	Key   string
	Value string
}

type MapFunc func(filename, contents string) []KeyValue
type ReduceFunc func(key string, values []string) string

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask // all tasks are in flight; come back later
	ExitTask // the whole job is finished
)

type Task struct {
	Type    TaskType
	ID      int
	Version int // bumped on every (re)assignment
	NReduce int
	Dir     string // where intermediate and output files live
	Files   []string
}

// ihash picks the reduce partition for a key. It has to be a stable hash of
// the key bytes, not Go's randomized map iteration or a pointer-dependent
// hash, or map and reduce workers would disagree about where a key lives.
func ihash(key string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// The version is part of the intermediate filename: a worker that crashed
// and was replaced writes to a different path, so a late write cannot
// clobber the output of the worker that took over.
func MapFileName(dir string, mapID, reduceID, version int) string {
	return fmt.Sprintf("%s/mr-%d-%d-%d", dir, mapID, reduceID, version)
}

func ReduceFileName(dir string, reduceID int) string {
	return fmt.Sprintf("%s/mr-out-%d", dir, reduceID)
}
