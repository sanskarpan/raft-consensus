# Examples

Runnable programs demonstrating the raft-consensus Go client and server.

## kvclient

A tour of the client API — Put/Get, atomic `Increment`, a compare-and-swap
`Txn`, cursor-paginated `RangePage`, and a `Watch` — against a running cluster.

```bash
# Start a local cluster first (see the repo README / scripts/docker), then:
go run ./examples/kvclient -endpoints localhost:8002,localhost:8004,localhost:8006
```

See also the godoc examples in `pkg/client` (rendered on pkg.go.dev).
