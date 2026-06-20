# Hermes Distributed Key-Value Store

Hermes is a highly-available, distributed key-value store built in Go. It uses the **Raft consensus algorithm** for distributed coordination, **SWIM gossip protocol** for cluster membership, and an **LSM-Tree** (Log-Structured Merge Tree) for the underlying storage engine.

## Features

- **Distributed Consensus**: Built on the Raft protocol to ensure high availability, leader election, and data consistency across multiple nodes.
- **SWIM Cluster Membership**: Decentralized, scalable peer discovery and failure detection via the SWIM gossip protocol.
- **LSM-Tree Storage Engine**: Optimized for high write throughput using Write-Ahead Logging (WAL), MemTables, and SSTables.
- **Multi-Protocol Support**: Exposes internal APIs via **gRPC** and user-facing APIs via both **gRPC** and **HTTP/JSON**.
- **Interactive CLI**: Includes `hermes-cli` with both a command-line mode and an interactive REPL shell.

---

## 🚀 Running a Multi-Node Cluster

To experience the distributed nature of Hermes, you can run a local 3-node cluster. Each node requires its own ports and data directory, which are configured via environment variables.

### 1. Start Node 0 (The Seed Node)
Open your first terminal and start the initial node. This node acts as the seed node for others to join.
```powershell
$env:HERMES_NODE_ID="hermes-0"
$env:HERMES_LISTEN_ADDR="127.0.0.1:7001"
$env:HERMES_HTTP_ADDR="127.0.0.1:7000"
$env:HERMES_METRICS_ADDR="127.0.0.1:9000"
$env:HERMES_DATA_DIR="/tmp/hermes-0"
$env:HERMES_SEED_NODES=""
go run ./cmd/hermes-server server
```

### 2. Start Node 1
Open a second terminal and start Node 1, pointing it to Node 0 to join the cluster.
```powershell
$env:HERMES_NODE_ID="hermes-1"
$env:HERMES_LISTEN_ADDR="127.0.0.1:7011"
$env:HERMES_HTTP_ADDR="127.0.0.1:7010"
$env:HERMES_METRICS_ADDR="127.0.0.1:9010"
$env:HERMES_DATA_DIR="/tmp/hermes-1"
$env:HERMES_SEED_NODES="127.0.0.1:7001"
go run ./cmd/hermes-server server
```

### 3. Start Node 2
Open a third terminal and start Node 2, also pointing it to Node 0.
```powershell
$env:HERMES_NODE_ID="hermes-2"
$env:HERMES_LISTEN_ADDR="127.0.0.1:7021"
$env:HERMES_HTTP_ADDR="127.0.0.1:7020"
$env:HERMES_METRICS_ADDR="127.0.0.1:9020"
$env:HERMES_DATA_DIR="/tmp/hermes-2"
$env:HERMES_SEED_NODES="127.0.0.1:7001"
go run ./cmd/hermes-server server
```

*Note: You can easily automate this by running the provided `start_cluster.ps1` script.*

---

## 🛠️ Interacting with the Cluster

You can interact with any node in the cluster. Changes sent to the Raft leader will be replicated across the cluster.

### Using the Hermes CLI

The `hermes-cli` provides an interactive shell (REPL) and standard command-line execution. By default, the CLI connects to `127.0.0.1:7001`. To connect to a different node, set `$env:HERMES_ADDR="127.0.0.1:7011"`.

#### Interactive REPL Mode
```powershell
go run ./cmd/hermes-cli
```
Once inside the REPL (`hermes> `), you can run:

- **Check Cluster Status:**
  ```text
  hermes> cluster status
  Leader: hermes-0
  Nodes:  3/3 alive
  Shards: 3
  ```
- **Insert a Key:**
  ```text
  hermes> put user:alice 1000
  OK
  ```
- **Retrieve a Key:**
  ```text
  hermes> get user:alice
  1000
  ```
- **Delete a Key:**
  ```text
  hermes> delete user:alice
  OK
  ```
- **Scan by Prefix:**
  ```text
  hermes> scan user:
  user:alice = 1000
  ```

#### Single-Command Mode
Execute operations directly from your shell:
```powershell
go run ./cmd/hermes-cli put mykey myvalue
go run ./cmd/hermes-cli get mykey
go run ./cmd/hermes-cli delete mykey
go run ./cmd/hermes-cli scan my
go run ./cmd/hermes-cli cluster status
```

---

### Using the HTTP API

Hermes exposes a REST-like API using JSON over HTTP. You can point your HTTP requests to any node's `$env:HERMES_HTTP_ADDR` (e.g., `127.0.0.1:7000`).

- **Put a value:**
  ```powershell
  curl -X POST http://127.0.0.1:7000/put -d '{"key":"user:alice","value":"1000"}'
  ```

- **Get a value:**
  ```powershell
  curl http://127.0.0.1:7000/get?key=user:alice
  ```

- **Delete a value:**
  ```powershell
  curl -X POST http://127.0.0.1:7000/delete -d '{"key":"user:alice"}'
  ```

- **Cluster Status:**
  ```powershell
  curl http://127.0.0.1:7000/cluster/status
  ```

---

## 📊 Observability


Hermes exposes Prometheus metrics on the address specified by `$env:HERMES_METRICS_ADDR`.

- **View raw metrics:**
  ```powershell
  curl http://127.0.0.1:9000/metrics
  ```
- **Grafana Dashboards:**
  A predefined Grafana dashboard is available in `deploy/kubernetes/grafana-dashboard.json`. Import this into Grafana to visualize Raft leader changes, replication lag, and LSM-tree compaction stats.


┌─────────────┐     gRPC      ┌─────────────────────────────┐
│  CLI / HTTP │ ────────────▶ │         Hermes Node          │
└─────────────┘               │  ┌──────────┐ ┌──────────┐  │
                               │  │  Raft    │ │  LSM     │  │
                               │  │ Consensus│ │  Tree    │  │
                               │  └──────────┘ └──────────┘  │
                               └─────────────────────────────┘
                                        │ gossip (SWIM)
                               ┌────────┴────────┐
                               │   Peer Nodes    │
                               └─────────────────┘
