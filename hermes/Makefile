PROTO_DIR=proto
OUT_DIR=proto

.PHONY: proto
proto:
	protoc \
		--go_out=$(OUT_DIR) \
		--go_opt=paths=source_relative \
		--go-grpc_out=$(OUT_DIR) \
		--go-grpc_opt=paths=source_relative \
		-I $(PROTO_DIR) \
		$(PROTO_DIR)/common.proto \
		$(PROTO_DIR)/hermes.proto \
		$(PROTO_DIR)/raft.proto \
		$(PROTO_DIR)/membership.proto

.PHONY: test
test:
	go test -race -v ./...

.PHONY: build
build:
	go build ./cmd/hermes-server/...

.PHONY: run-phase0
run-phase0:
	go run ./cmd/hermes-server os-fundamentals