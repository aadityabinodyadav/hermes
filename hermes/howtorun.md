# Developer Commands

Quick reference for building, running, testing, and profiling Hermes locally. For cluster setup and the client API, see the [top-level README](../README.md).

## Build

```bash
go build ./...
```

## Run a single subsystem demo

Each core algorithm has a narrated, standalone demo — useful when you want to exercise one subsystem without standing up a cluster (see [`README.md`](../README.md#explore-individual-subsystems-in-isolation) for the full list of phases):

```bash
go run ./cmd/hermes-server os-fundamentals
go run ./cmd/hermes-server raft
go run ./cmd/hermes-server storage
go run ./cmd/hermes-server partition
go run ./cmd/hermes-server membership
go run ./cmd/hermes-server txn
go run ./cmd/hermes-server consistency
go run ./cmd/hermes-server chaos
go run ./cmd/hermes-server observability
go run ./cmd/hermes-server advanced
go run ./cmd/hermes-server all-demos     # run every phase in sequence
```

## Always develop with the race detector on

Go's race detector catches data races that only show up under real concurrency — with a system this concurrency-heavy (Raft, SWIM, compaction, background GC all running as goroutines), this should be the default, not an occasional check:

```bash
go run -race ./cmd/hermes-server <phase>
go test -race ./...
```

## Testing

```bash
go test ./...                                     # unit tests, all packages
go test ./test/integration/... -run TestCluster    # multi-node integration tests
go test -bench=. -benchmem ./pkg/os_fundamentals/...   # benchmarks with allocation stats
```

Swap `os_fundamentals` for any package path to benchmark that package specifically.

## Profiling

Start a phase with pprof's HTTP endpoint running in the background, then sample it:

```bash
go run ./cmd/hermes-server os-fundamentals &
go tool pprof http://localhost:6060/debug/pprof/heap
```

## Escape analysis

Shows which variables the compiler moves to the heap instead of the stack — useful for tracking down unexpected allocations in hot paths (e.g. the Raft log append path, the LSM write path):

**bash/zsh:**
```bash
go build -gcflags='-m -m' ./pkg/os_fundamentals/... 2>&1 | grep escape
```

**PowerShell:**
```powershell
go build -gcflags="-m -m" ./pkg/os_fundamentals/... 2>&1 | Select-String "escape"
```
