# Contributing to raft-consensus

Thanks for your interest in contributing! This document explains how to build,
test, and submit changes.

## Prerequisites

- Go **1.26.5** or newer (the module pins `toolchain go1.26.5`).
- `make`, `git`, and optionally `docker` and `protoc` (only for `make proto`).
- Linters/scanners are installed on demand by the Makefile targets, or you can
  install them yourself:
  - [`golangci-lint`](https://golangci-lint.run/) v1.64.x
  - [`staticcheck`](https://staticcheck.dev/)
  - [`govulncheck`](https://pkg.go.dev/golang.org/x/vuln/cmd/govulncheck)

## Build, test, and lint

All common workflows are wrapped by the [`Makefile`](./Makefile):

```sh
make build       # build ./bin/raftd and ./bin/kvctl
make test        # unit tests
make test-race   # tests under the race detector
make cover       # coverage profile + coverage.html
make bench       # benchmarks
make lint        # golangci-lint + staticcheck
make vuln        # govulncheck vulnerability scan
make docker      # build the container image
make run         # build and run raftd with config.yaml
make proto       # regenerate protobuf/gRPC stubs
make tidy        # go mod tidy
make clean       # remove build/test artifacts
```

Run `make help` for the full list.

Before opening a pull request, please make sure the following all pass locally:

```sh
make tidy
make test-race
make lint
make vuln
```

## Regenerating protobuf code

The generated stubs in `proto/*.pb.go` are committed. If you change
`proto/raft.proto`, regenerate them with `make proto` (requires `protoc`,
`protoc-gen-go`, and `protoc-gen-go-grpc` on your `PATH`) and commit the result.

## Pull request process

1. Fork the repository and create a topic branch off `main`.
2. Keep changes focused; unrelated cleanups belong in separate PRs.
3. Add or update tests for behavior changes.
4. Ensure `make test-race`, `make lint`, and `make vuln` are green.
5. Update `CHANGELOG.md` and any relevant docs.
6. Open a PR against `main` and fill in the pull request template. Link any
   related issues.
7. A maintainer will review; address feedback by pushing additional commits
   (avoid force-pushing during active review when possible).

## Developer Certificate of Origin (DCO)

This project requires that all commits be signed off under the
[Developer Certificate of Origin](https://developercertificate.org/). Sign off
by adding a `Signed-off-by` trailer to each commit, which you can do
automatically with the `-s` flag:

```sh
git commit -s -m "raft: fix snapshot install on lagging follower"
```

This adds a line like:

```
Signed-off-by: Jane Developer <jane@example.com>
```

certifying that you wrote the patch or otherwise have the right to submit it
under the project's Apache-2.0 license. PRs with unsigned commits will be asked
to amend and re-sign.

## Reporting security issues

Please do **not** open public issues for security vulnerabilities. See
[SECURITY.md](./SECURITY.md) for private disclosure instructions.

## Code of conduct

Participation in this project is governed by our
[Code of Conduct](./CODE_OF_CONDUCT.md).
