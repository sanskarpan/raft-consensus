# Raft Consensus

A production-ready Raft consensus implementation in Go, following the design patterns from hashicorp/raft and etcd/raft.

## Features

- **Leader Election**: Full Raft leader election with pre-vote protocol
- **Log Replication**: AppendEntries RPC with batching
- **Snapshots**: Periodic and size-triggered snapshots with InstallSnapshot RPC
- **Membership Changes**: Joint consensus for cluster reconfiguration
- **Learners**: Non-voting replicas for safe scaling
- **Transport**: TCP-based RPC with JSON encoding (gRPC available)
- **Observability**: Prometheus metrics, structured logging

## Project Structure

```
pkg/
  raft/           - Core Raft implementation
  storage/        - WAL and snapshot storage
  transport/      - Network transport layer
  client/         - Client library
  fsm/            - Example FSM (KV store)
cmd/
  raftd/          - Server binary
```

## Quick Start

### Build

```bash
go build ./cmd/raftd
```

### Run a 3-node cluster

Create config files for each node:

```yaml
# config-node1.yaml
node_id: node1
listen_addr: :8001
data_dir: ./data
cluster:
  - id: node1
    address: localhost:8001
  - id: node2
    address: localhost:8002
  - id: node3
    address: localhost:8003
```

Start nodes:

```bash
./raftd -config config-node1.yaml &
./raftd -config config-node2.yaml &
./raftd -config config-node3.yaml &
```

### Using the Client

```go
client := client.NewClient(
    client.WithAddresses([]string{"localhost:8001", "localhost:8002", "localhost:8003"}),
)

// Submit a command
err := client.SubmitCommand("key", "value")

// Read a value
value, err := client.GetValue("key")
```

## API Endpoints

- `GET /health` - Health check
- `GET /ready` - Readiness check
- `GET /admin/cluster` - Cluster information
- `POST /admin/snapshot` - Create snapshot
- `GET /metrics` - Prometheus metrics

## Architecture

The implementation follows the Raft consensus algorithm as described in the Raft paper and Diego Ongaro's thesis.

### Storage Layer

- **WAL**: Segment-based write-ahead log with 64MB segments
- **Stable Store**: BoltDB for term, votedFor, and configuration
- **Snapshots**: Atomic file-based snapshots

### Transport Layer

- **TCP Transport**: JSON-based RPC
- **gRPC Transport**: Available (requires protobuf generation)

### Consistency

- **Linearizable Reads**: Quorum read
- **Stale Reads**: Read from any node

## Testing

```bash
go test ./...
```

## Roadmap

- [x] Core Raft implementation
- [x] WAL and snapshot storage
- [x] TCP transport
- [x] Basic server with admin API
- [ ] gRPC transport
- [ ] Membership changes
- [ ] Pre-vote protocol
- [ ] Learner support
- [ ] OpenTelemetry tracing
- [ ] Admin UI

## References

- [Raft Paper](https://raft.github.io/raft.pdf)
- [hashicorp/raft](https://github.com/hashicorp/raft)
- [etcd/raft](https://github.com/etcd-io/raft)
