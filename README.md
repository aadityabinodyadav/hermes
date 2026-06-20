# Hermes

Hermes is a highly-available, distributed key-value store built in Go. It uses the **Raft consensus algorithm** for distributed coordination and an **LSM-Tree** (Log-Structured Merge Tree) for the underlying storage engine.

## Features

- **Distributed Consensus**: Built on the Raft protocol to ensure high availability and data consistency.
- **LSM-Tree Storage Engine**: Optimized for high write throughput using Write-Ahead Logging (WAL), MemTables, and SSTables.
- **Multi-Protocol Support**: Exposes APIs via **gRPC** (for internal/fast communication) and **HTTP/JSON** (for easy integration).
- **Interactive CLI**: Includes `hermes-cli` with both a command mode and an interactive REPL shell.

## Getting Started

### 1. Start the Hermes Server
You can start a single-node Hermes cluster by running the `hermes-server` command. By default, it runs gRPC on port `:7001` and the HTTP API on port `:7000`.

```bash
go run ./cmd/hermes-server server
```

*(Note: Data is persisted in the `\data` directory by default.)*

### 2. Using the Hermes CLI

The `hermes-cli` is the easiest way to interact with your Hermes cluster. 

#### Command Mode

You can run individual commands directly from your terminal:

- **Put a value**:
  ```bash
  go run ./cmd/hermes-cli put user:alice 1000
  ```

- **Get a value**:
  ```bash
  go run ./cmd/hermes-cli get user:alice
  ```

- **Delete a value**:
  ```bash
  go run ./cmd/hermes-cli delete user:alice
  ```

- **Scan by prefix**:
  ```bash
  go run ./cmd/hermes-cli scan user:
  ```

- **Check cluster status**:
  ```bash
  go run ./cmd/hermes-cli cluster status
  ```

#### Interactive REPL Mode

If you run the CLI without any arguments, it drops you into an interactive REPL mode where you can execute commands consecutively:

```bash
$ go run ./cmd/hermes-cli
hermes> put user:bob 500
OK
hermes> get user:bob
500
hermes> scan user:
user:bob = 500
hermes> cluster status
Leader: hermes-0 (you)
Nodes:  1/1 alive
Shards: 2
hermes> exit
```

### 3. Using the HTTP API

You can also interact with the Hermes cluster programmatically using the HTTP API:

- **Put a value**:
  ```bash
  curl -X POST http://localhost:7000/put -d '{"key":"user:alice","value":"1000"}'
  ```

- **Get a value**:
  ```bash
  curl http://localhost:7000/get?key=user:alice
  ```
