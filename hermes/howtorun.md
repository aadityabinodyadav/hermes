# Run Phase 0 (all of it)
go run ./cmd/hermes-server os-fundamentals

# Run with race detector ON (always develop with this)
go run -race ./cmd/hermes-server os-fundamentals

# Profile memory allocations
go run ./cmd/hermes-server os-fundamentals &
go tool pprof http://localhost:6060/debug/pprof/heap

# Benchmark specific functions
go test -bench=. -benchmem ./pkg/os_fundamentals/...

# Check for escape analysis (see what goes to heap)
go build -gcflags='-m -m' ./pkg/os_fundamentals/ 2>&1 | head -50

go build -gcflags="-m -m" ./pkg/os_fundamentals 2>&1 |
Select-String "escape"