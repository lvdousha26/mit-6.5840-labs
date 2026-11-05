# MIT 6.5840 labs, from scratch

An independent Go implementation of the MIT distributed systems course labs
(6.5840, formerly 6.824): MapReduce, Raft, and a fault-tolerant KV store on
top of Raft. Standard library only, no module dependencies.

The course asks students not to publish their lab solutions. Nothing here
comes from the official skeleton or was submitted to the course grader —
this is written from the Raft paper and the lab handouts' descriptions. If
you are taking 6.5840, treat it as a reference, not as something to hand in.

## Running it

```
go test ./... -count=1          # all four packages
go test -race ./... -count=1    # what CI runs
make check                      # same as the line above
```

Go 1.22 or newer. Locally verified on Windows with Go 1.25; CI runs the
Ubuntu and macOS jobs with `-race`.

| Package     | Lab | What it does |
|-------------|-----|--------------|
| `labrpc/`   | —   | In-process RPC network with partitions, drops and server restarts |
| `mapreduce/`| 1   | Coordinator/worker word count over `net/rpc` |
| `raft/`     | 2   | Election, log replication, persistence (2A/2B/2C) |
| `kvserver/` | 3   | Linearizable Get/Put/Append with client-side retries |

`labrpc` is worth a look before the other packages. Raft and KV need to cut
a specific network edge, lose a reply *after* the handler ran, and replace a
server without changing any client's view of it. Doing that with real sockets
would make the tests depend on host ports and kernel timing. So calls travel
through channels instead: `ClientEnd.Call` looks up its endpoint, may drop
the request, dispatches the handler in a goroutine, may drop the reply, and
has a deadline. Two deliberate simplifications are called out in the source:
arguments are shallow-copied rather than serialized (so handlers must copy
any slice they keep), and a disconnected endpoint sleeps until its deadline
instead of blocking until the network heals.

## Lab 1 — MapReduce

`mapreduce/` uses real `net/rpc` over a loopback TCP listener, matching the
lab's coordinator/worker split. Workers are goroutines rather than processes;
everything else — task assignment, intermediate files, the phase change —
follows the handout.

The part worth reading is the version number. Each task carries a `version`
that increments on every assignment, and intermediate filenames include it.
When a worker dies, the coordinator times the task out and hands it to
someone else with a higher version. A late report from the dead worker fails
the version check, so its files never enter the reduce inputs. Without this,
a worker that finished the map but lost its report would race the replacement
and could silently win.

Three smaller decisions:

- `report()` runs the expiry check too, not just `nextTask()`. Otherwise a
  report arriving as the first RPC after a deadline would be accepted.
- Intermediate and output files are written to a `.tmp` path and renamed, so
  a worker killed mid-write leaves nothing a reader will pick up.
- The accept loop is handwritten instead of `rpc.Server.Accept`, because the
  latter logs "use of closed network connection" every time a test stops a job.

The tests cover plain word count, a worker that dies after writing its map
output but before reporting it (the task must be reassigned and the final
answer must still be right), and a stale report being rejected at the
coordinator level.

## Lab 2 — Raft

`raft/raft.go` covers 2A (election), 2B (log replication) and 2C
(persistence). The state follows Figure 2; RPCs are `RequestVote` and
`AppendEntries`.

Things that are easy to get wrong and are handled explicitly:

- **Commit only by counting replicas of a current-term entry.** Scanning
  downward from `lastIndex` and skipping entries whose term is not
  `currentTerm` is the Figure 8 rule. Committing an old-term entry just
  because it is on a majority is the classic way to overwrite a committed
  value later.
- **A new leader appends a no-op.** Since the rule above needs a
  current-term entry, a leader that inherits uncommitted entries would
  otherwise sit on them indefinitely until a client writes something.
- **No network call while holding `rf.mu`.** `replicate()` snapshots the log
  into per-peer argument structs under the lock, then sends from separate
  goroutines. `applier()` likewise batches the messages under the lock and
  writes to `applyCh` outside it — a full apply channel must never stall an
  RPC handler that owns the state mutex.
- **Fast log backtracking.** On a term mismatch the follower reports the
  first index of the conflicting term, so the leader jumps back a whole term
  instead of one entry per round trip.
- **Restart replays the log.** `commitIndex` and `lastApplied` start at zero
  after a restart; the leader's `LeaderCommit` brings a follower back up to
  date and the state machine reapplies from index 1.

`Persister` stands in for stable storage and copies the bytes on `Save`, so
a caller reusing its buffer cannot rewrite history. Commands travel as
`interface{}` values, which means gob needs their concrete types registered;
`raft` registers `int`, `string` and `NoOp`, and `kvserver` registers `Op` in
its own `init`. Forgetting this panics in `persistLocked` rather than
silently persisting an empty log.

Tests: initial election and leadership stability, re-election after the
leader is isolated, five sequential agreements, no commit without a
majority, an old leader rejoining after a 2/3 partition, all five servers
restarting, and agreement over a 12%-loss network.

## Lab 3 — fault-tolerant KV

`kvserver/` puts a `Get`/`Put`/`Append` state machine behind Raft. Every
operation, including `Get`, goes through `Start()` — a read served from a
follower's local map could return a value the majority had already replaced.

Clients stamp requests with `(clientID, seqNo)`. The server keeps the highest
applied sequence number per client and replays the cached result for a
duplicate, so an `Append` whose reply was lost is not applied twice when the
client retries. The RPC waits on `results[raftIndex]`; because a different
leader can overwrite that index with an unrelated entry, the result carries
its own client id and sequence number and `execute()` checks them before
returning anything. Returning the wrong client's result here is a subtle way
to break linearizability.

Tests cover the basic ops, 100 concurrent appends from five clients, a write
and read across a leader change, a deliberately duplicated sequence number,
and losing and restarting every server.

## What is not here

Lab 4 (sharded KV) is not implemented. It needs a configuration service,
shard migration between groups, and duplicate suppression that survives a
shard moving mid-request — a different order of magnitude from the first
three, and I would rather stop at a Lab 3 that actually passes its tests
than ship a Lab 4 that only compiles.

Other known limits, all of which are visible in the code:

- No snapshots (Raft 2D). The log grows without bound and `kvserver`'s
  `results` map has no compaction.
- No membership changes, no `InstallSnapshot` RPC, no `CondInstallSnapshot`.
- No `checkQuorum`: a leader cut off from the majority keeps calling itself
  leader until it hears a higher term. The test harness excludes fully
  isolated servers from leader checks for exactly this reason.
- `labrpc` does not serialize arguments and does not simulate reordering.
- The KV store has no persistence of its own beyond the Raft log.

## Layout

```
labrpc/     simulated network + its own unit tests
mapreduce/  common.go coordinator.go rpc.go worker.go mr_test.go
raft/       raft.go persister.go config_test.go raft_test.go
kvserver/   common.go client.go server.go config_test.go kv_test.go
.github/    Ubuntu + macOS CI, go vet and go test -race
Makefile    test / race / vet / check / clean
```

MIT licensed.
