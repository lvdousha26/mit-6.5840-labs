package mapreduce

import (
	"encoding/json"
	"fmt"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Worker runs the ask/work/report loop until the coordinator says the job is
// done. crash, when non-nil, is called after a task's output files are written
// but before the report is sent; returning true makes this worker exit. The
// tests use it to simulate a process dying at the worst possible moment.
func Worker(mapf MapFunc, reducef ReduceFunc, addr string, crash func(Task) bool) {
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		return
	}
	defer client.Close()

	consecutiveErrors := 0
	for {
		var reply GetTaskReply
		if err := client.Call("Coordinator.GetTask", &GetTaskArgs{}, &reply); err != nil {
			consecutiveErrors++
			if consecutiveErrors > 25 {
				return
			}
			time.Sleep(20 * time.Millisecond)
			continue
		}
		consecutiveErrors = 0
		task := reply.Task

		switch task.Type {
		case ExitTask:
			return
		case WaitTask:
			time.Sleep(20 * time.Millisecond)
			continue
		}

		var files []string
		switch task.Type {
		case MapTask:
			files, err = doMap(mapf, task)
		case ReduceTask:
			files, err = doReduce(reducef, task)
		default:
			continue
		}
		if err != nil {
			// A local failure is reported by doing nothing: the coordinator's
			// deadline expires and somebody else retries the task.
			continue
		}
		if crash != nil && crash(task) {
			return
		}

		var out ReportReply
		_ = client.Call("Coordinator.ReportTask", &ReportArgs{
			Type: task.Type, ID: task.ID, Version: task.Version, Files: files,
		}, &out)
	}
}

func doMap(mapf MapFunc, task Task) ([]string, error) {
	content, err := os.ReadFile(task.Files[0])
	if err != nil {
		return nil, err
	}
	buckets := make([][]KeyValue, task.NReduce)
	for _, kv := range mapf(task.Files[0], string(content)) {
		r := ihash(kv.Key) % task.NReduce
		buckets[r] = append(buckets[r], kv)
	}

	files := make([]string, task.NReduce)
	for r := range buckets {
		path := MapFileName(task.Dir, task.ID, r, task.Version)
		if err := writeAtomically(path, buckets[r]); err != nil {
			return nil, err
		}
		files[r] = path
	}
	return files, nil
}

func doReduce(reducef ReduceFunc, task Task) ([]string, error) {
	var kvs []KeyValue
	for _, path := range task.Files {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		var part []KeyValue
		if err := json.Unmarshal(data, &part); err != nil {
			return nil, err
		}
		kvs = append(kvs, part...)
	}
	sort.Slice(kvs, func(i, j int) bool { return kvs[i].Key < kvs[j].Key })

	path := ReduceFileName(task.Dir, task.ID)
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(kvs); {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		values := make([]string, 0, j-i)
		for _, kv := range kvs[i:j] {
			values = append(values, kv.Value)
		}
		if _, err := fmt.Fprintf(f, "%v %v\n", kvs[i].Key, reducef(kvs[i].Key, values)); err != nil {
			_ = f.Close()
			return nil, err
		}
		i = j
	}
	if err := f.Close(); err != nil {
		return nil, err
	}
	if err := os.Rename(tmp, path); err != nil {
		return nil, err
	}
	return []string{path}, nil
}

// writeAtomically makes a reader see either a complete partition file or no
// file at all. A worker killed mid-write leaves only the .tmp path, which no
// report ever points at.
func writeAtomically(path string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
