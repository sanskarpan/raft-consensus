# Makefile for raft-consensus
#
# Common developer and CI targets. Run `make help` for a summary.

# ---- variables ----------------------------------------------------------
MODULE      := github.com/raft-consensus
VERSION     ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT      ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
DATE        ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS     := -s -w \
	-X $(MODULE)/pkg/version.Version=$(VERSION) \
	-X $(MODULE)/pkg/version.GitCommit=$(COMMIT) \
	-X $(MODULE)/pkg/version.BuildDate=$(DATE)

BIN_DIR     := bin
GO          ?= go
DOCKER_IMAGE ?= raftd:latest

# Binaries built by `make build`.
BINARIES    := raftd kvctl

.DEFAULT_GOAL := help

# ---- help ---------------------------------------------------------------
.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) | \
		sort | awk 'BEGIN {FS = ":.*?## "} {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

# ---- build --------------------------------------------------------------
.PHONY: build
build: ## Build all binaries into ./bin
	@mkdir -p $(BIN_DIR)
	@for b in $(BINARIES); do \
		echo "building $$b"; \
		CGO_ENABLED=0 $(GO) build -trimpath -ldflags="$(LDFLAGS)" \
			-o $(BIN_DIR)/$$b ./cmd/$$b || exit 1; \
	done

# ---- test ---------------------------------------------------------------
.PHONY: test
test: ## Run unit tests
	$(GO) test -timeout 120s ./...

.PHONY: test-race
test-race: ## Run tests with the race detector
	$(GO) test -race -timeout 120s ./...

.PHONY: cover
cover: ## Run tests with coverage and produce coverage.html
	$(GO) test -coverprofile=coverage.out -timeout 120s ./...
	$(GO) tool cover -html=coverage.out -o coverage.html
	$(GO) tool cover -func=coverage.out | tail -n 1

.PHONY: bench
bench: ## Run benchmarks
	$(GO) test -run '^$$' -bench=. -benchmem ./...

# ---- lint / static analysis --------------------------------------------
.PHONY: lint
lint: ## Run golangci-lint and staticcheck
	@# .golangci.yml is schema v2; require golangci-lint v2 (module path has /v2).
	@golangci-lint version 2>/dev/null | grep -q 'version 2' || { \
		echo "installing golangci-lint v2..."; \
		$(GO) install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2; }
	golangci-lint run
	@command -v staticcheck >/dev/null 2>&1 || { \
		echo "installing staticcheck..."; \
		$(GO) install honnef.co/go/tools/cmd/staticcheck@latest; }
	staticcheck ./...

.PHONY: vuln
vuln: ## Run govulncheck vulnerability scan
	@command -v govulncheck >/dev/null 2>&1 || { \
		echo "installing govulncheck..."; \
		$(GO) install golang.org/x/vuln/cmd/govulncheck@latest; }
	govulncheck ./...

# ---- proto --------------------------------------------------------------
.PHONY: proto
proto: ## Regenerate protobuf/gRPC stubs from proto/raft.proto
	@command -v protoc >/dev/null 2>&1 || { \
		echo "protoc not found; install the Protocol Buffers compiler"; exit 1; }
	protoc \
		--go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		proto/raft.proto

# ---- docker / run -------------------------------------------------------
.PHONY: docker
docker: ## Build the Docker image
	docker build --build-arg VERSION=$(VERSION) -t $(DOCKER_IMAGE) .

.PHONY: run
run: build ## Build and run raftd with the default config
	$(BIN_DIR)/raftd -config config.yaml

# ---- housekeeping -------------------------------------------------------
.PHONY: tidy
tidy: ## Run go mod tidy
	$(GO) mod tidy

.PHONY: clean
clean: ## Remove build and test artifacts
	rm -rf $(BIN_DIR) dist
	rm -f coverage.out coverage.html *.prof *.trace
