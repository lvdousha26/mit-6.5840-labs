package mapreduce

import (
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func mapWC(_ string, contents string) []KeyValue {
	var out []KeyValue
	for _, w := range strings.Fields(contents) {
		out = append(out, KeyValue{Key: w, Value: "1"})
	}
	return out
}

func reduceWC(_ string, values []string) string {
	return strconv.Itoa(len(values))
}

func makeInputs(t *testing.T) (string, []string) {
	t.Helper()
	dir := t.TempDir()
	texts := []string{
		"a b c a b a",
		"b c d d e",
		"a e f f f g",
		"the quick brown fox jumps over the lazy dog the",
	}
	files := make([]string, len(texts))
	for i, s := range texts {
		p := filepath.Join(dir, "pg-"+strconv.Itoa(i)+".txt")
		if err := os.WriteFile(p, []byte(s), 0o644); err != nil {
			t.Fatal(err)
		}
		files[i] = p
	}
	return dir, files
}

func sequentialWC(t *testing.T, files []string) map[string]string {
	t.Helper()
	counts := map[string]int{}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for _, w := range strings.Fields(string(b)) {
			counts[w]++
		}
	}
	out := make(map[string]string)
	for k, v := range counts {
		out[k] = strconv.Itoa(v)
	}
	return out
}

func readOutputs(t *testing.T, dir string, nReduce int) map[string]string {
	t.Helper()
	out := make(map[string]string)
	for r := 0; r < nReduce; r++ {
		b, err := os.ReadFile(ReduceFileName(dir, r))
		if err != nil {
			t.Fatalf("reduce %d produced no output: %v", r, err)
		}
		for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
			if line == "" {
				continue
			}
			parts := strings.Fields(line)
			if len(parts) != 2 {
				t.Fatalf("malformed output line %q", line)
			}
			if _, dup := out[parts[0]]; dup {
				t.Fatalf("key %q appeared in more than one reduce output", parts[0])
			}
			out[parts[0]] = parts[1]
		}
	}
	return out
}

func runJob(t *testing.T, dir string, files []string, nReduce, nWorkers int, timeout time.Duration, crash func(Task) bool) *Coordinator {
	t.Helper()
	c := MakeCoordinator(files, nReduce, dir, timeout)
	addr, err := c.Start()
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < nWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			Worker(mapWC, reduceWC, addr, crash)
		}()
	}

	deadline := time.Now().Add(20 * time.Second)
	for !c.Done() {
		if time.Now().After(deadline) {
			c.Stop()
			t.Fatal("job did not finish")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Let workers pick up their ExitTask before closing the listener.
	time.Sleep(80 * time.Millisecond)
	c.Stop()
	wg.Wait()
	return c
}

func TestWordCount(t *testing.T) {
	dir, files := makeInputs(t)
	want := sequentialWC(t, files)
	runJob(t, dir, files, 5, 4, time.Second, nil)
	got := readOutputs(t, dir, 5)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("output mismatch\n got: %v\nwant: %v", got, want)
	}
}

func TestWorkerCrashForcesReassignment(t *testing.T) {
	dir, files := makeInputs(t)
	want := sequentialWC(t, files)

	var once atomic.Int32
	crash := func(task Task) bool {
		// One worker dies after writing its map output but before reporting.
		// The coordinator must time that task out and hand it to someone else.
		return task.Type == MapTask && once.CompareAndSwap(0, 1)
	}
	c := runJob(t, dir, files, 3, 4, 300*time.Millisecond, crash)

	versions := c.MapVersions()
	redispatched := false
	for _, v := range versions {
		if v >= 2 {
			redispatched = true
		}
	}
	if !redispatched {
		t.Fatalf("no map task was reassigned: versions %v", versions)
	}
	if got := readOutputs(t, dir, 3); !reflect.DeepEqual(got, want) {
		t.Fatalf("output mismatch after a worker crash\n got: %v\nwant: %v", got, want)
	}
}

func TestStaleReportIsRejected(t *testing.T) {
	dir, files := makeInputs(t)
	c := MakeCoordinator(files, 3, dir, 40*time.Millisecond)

	first := c.nextTask()
	if first.Type != MapTask || first.Version != 1 {
		t.Fatalf("unexpected first task: %+v", first)
	}
	time.Sleep(60 * time.Millisecond) // let the deadline lapse
	c.report(MapTask, first.ID, first.Version, []string{"old-worker-file"})

	retry := c.nextTask()
	if retry.ID != first.ID || retry.Version != 2 {
		t.Fatalf("expected task %d version 2, got %+v", first.ID, retry)
	}
	c.report(MapTask, retry.ID, retry.Version, []string{"new-0", "new-1", "new-2"})

	// Finish the remaining map tasks, then check what reduce worker 0 is told
	// to read. Only the accepted version may show up.
	for i := 1; i < len(files); i++ {
		task := c.nextTask()
		if task.Type != MapTask || task.ID != i {
			t.Fatalf("expected map task %d, got %+v", i, task)
		}
		c.report(MapTask, task.ID, task.Version, []string{"x-0", "x-1", "x-2"})
	}
	reduce := c.nextTask()
	if reduce.Type != ReduceTask {
		t.Fatalf("expected a reduce task, got %+v", reduce)
	}
	if len(reduce.Files) != len(files) {
		t.Fatalf("expected %d reduce inputs, got %v", len(files), reduce.Files)
	}
	if reduce.Files[0] != "new-0" {
		t.Fatalf("stale files leaked into reduce inputs: %v", reduce.Files)
	}
}
