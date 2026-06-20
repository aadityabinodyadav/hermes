# Makefile — builds everything in the correct order

.PHONY: all proto build test test-unit test-integration test-e2e \
        run-demo run-all-demos run-server clean lint bench

# ── Proto generation ──────────────────────────────────────────────────────────
proto:
	@echo "Generating protobuf code..."
	protoc \
		--go_out=proto \
		--go_opt=paths=source_relative \
		--go-grpc_out=proto \
		--go-grpc_opt=paths=source_relative \
		-I proto \
		proto/common.proto \
		proto/hermes.proto \
		proto/raft.proto \
		proto/membership.proto
	@echo "✅ Proto generation complete"

# ── Build ─────────────────────────────────────────────────────────────────────
build: proto
	@echo "Building Hermes..."
	go build -race ./...
	go build -o bin/hermes-server ./cmd/hermes-server/
	go build -o bin/hermes-cli    ./cmd/hermes-cli/
	@echo "✅ Build complete"

# ── Tests ─────────────────────────────────────────────────────────────────────
test: test-unit test-integration

test-unit:
	@echo "Running unit tests..."
	go test -race -v -timeout 60s \
		./pkg/clock/... \
		./pkg/storage/... \
		./pkg/storage/wal/... \
		./pkg/storage/bloom/... \
		./pkg/partition/... \
		./pkg/membership/... \
		./pkg/txn/... \
		./pkg/consistency/... \
		./pkg/ratelimit/... \
		./pkg/observability/...
	@echo "✅ Unit tests passed"

test-integration:
	@echo "Running integration tests..."
	go test -race -v -timeout 120s ./test/integration/...
	@echo "✅ Integration tests passed"

test-e2e:
	@echo "Running end-to-end proof tests..."
	go test -race -v -timeout 300s ./test/e2e/...
	@echo "✅ E2E proof tests passed"

test-chaos:
	@echo "Running chaos tests (slow)..."
	go test -race -v -timeout 600s ./test/chaos/...
	@echo "✅ Chaos tests passed"

test-all: test-unit test-integration test-e2e
	@echo "✅ ALL TESTS PASSED"

# ── Benchmarks ────────────────────────────────────────────────────────────────
bench:
	go test -bench=. -benchmem -benchtime=10s \
		./pkg/storage/... \
		./pkg/clock/... \
		./pkg/partition/...

# ── Demo runner ───────────────────────────────────────────────────────────────
run-phase-%: build
	./bin/hermes-server $*

run-all-demos: build
	./bin/hermes-server all-demos

# ── Production server ─────────────────────────────────────────────────────────
run-server: build
	HERMES_NODE_ID=hermes-0 \
	HERMES_LISTEN_ADDR=0.0.0.0:7000 \
	HERMES_DATA_DIR=/tmp/hermes-0 \
	./bin/hermes-server server

# ── Local cluster (3 nodes) ───────────────────────────────────────────────────
cluster: build
	mkdir -p /tmp/hermes-{0,1,2}
	HERMES_NODE_ID=hermes-0 HERMES_LISTEN_ADDR=0.0.0.0:7000 \
		HERMES_DATA_DIR=/tmp/hermes-0 ./bin/hermes-server server &
	sleep 1
	HERMES_NODE_ID=hermes-1 HERMES_LISTEN_ADDR=0.0.0.0:7001 \
		HERMES_DATA_DIR=/tmp/hermes-1 ./bin/hermes-server server &
	sleep 1
	HERMES_NODE_ID=hermes-2 HERMES_LISTEN_ADDR=0.0.0.0:7002 \
		HERMES_DATA_DIR=/tmp/hermes-2 ./bin/hermes-server server &
	@echo "✅ 3-node cluster started"
	@echo "   hermes-0: localhost:7000"
	@echo "   hermes-1: localhost:7001"
	@echo "   hermes-2: localhost:7002"
	@echo "   Press Ctrl+C to stop"

cluster-stop:
	pkill hermes-server || true
	@echo "✅ Cluster stopped"

# ── Kubernetes ────────────────────────────────────────────────────────────────
k8s-deploy:
	kubectl apply -f deploy/kubernetes/namespace.yaml
	kubectl apply -f deploy/kubernetes/statefulset.yaml
	kubectl apply -f deploy/kubernetes/services.yaml
	kubectl apply -f deploy/kubernetes/prometheus.yaml
	kubectl -n hermes rollout status statefulset/hermes

k8s-status:
	kubectl -n hermes get pods
	kubectl -n hermes exec hermes-0 -- \
		curl -s http://localhost:9090/debug/status

k8s-destroy:
	kubectl delete namespace hermes

# ── Profiling ─────────────────────────────────────────────────────────────────
profile-cpu:
	go tool pprof http://localhost:6060/debug/pprof/profile?seconds=30

profile-heap:
	go tool pprof http://localhost:6060/debug/pprof/heap

profile-goroutines:
	go tool pprof http://localhost:6060/debug/pprof/goroutine

# ── Code quality ─────────────────────────────────────────────────────────────
lint:
	golangci-lint run ./...

vet:
	go vet ./...

# ── Clean ────────────────────────────────────────────────────────────────────
clean:
	rm -rf bin/
	rm -f proto/*.pb.go proto/*_grpc.pb.go
	go clean -testcache
	@echo "✅ Clean complete"

# ── Setup (first time) ───────────────────────────────────────────────────────
setup:
	@echo "Installing tools..."
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	go install github.com/square/certstrap@latest
	@echo "✅ Tools installed"
	@echo ""
	@echo "On Mac: brew install protobuf"
	@echo "On Linux: apt install -y protobuf-compiler"
	@echo ""
	make proto
	make build
	@echo ""
	@echo "✅ Hermes setup complete!"
	@echo "   Run: make run-all-demos"
	@echo "   Or:  make run-phase-raft"
	@echo "   Or:  make cluster"