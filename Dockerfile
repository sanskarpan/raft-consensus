# syntax=docker/dockerfile:1

# ---- build stage ----
# Go toolchain note: go.mod pins `toolchain go1.26.5`; keep this base image's
# minor version >= 1.26 so the module toolchain is satisfied without a download.
#
# Supply-chain note: for reproducible, tamper-evident builds these base images
# should be pinned by digest, e.g.
#   FROM golang:1.26.5-alpine@sha256:<digest> AS builder
# The digest must be resolved from the registry at pin time (e.g.
# `docker buildx imagetools inspect golang:1.26.5-alpine`) and is intentionally
# left as a specific minor-version tag here rather than a guessed digest.
# Dependabot's docker ecosystem (see .github/dependabot.yml) keeps this current.
# Run the builder on the native BUILDPLATFORM and cross-compile to the requested
# TARGETOS/TARGETARCH, so multi-arch (amd64+arm64) images build fast without QEMU.
FROM --platform=$BUILDPLATFORM golang:1.26.5-alpine AS builder
WORKDIR /app

# Download dependencies first (better layer caching)
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build
COPY . .
ARG VERSION=dev
# TARGETOS/TARGETARCH are provided automatically by buildkit per target platform.
ARG TARGETOS=linux
ARG TARGETARCH=amd64
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath \
    -ldflags="-s -w -X github.com/sanskarpan/raft-consensus/pkg/version.Version=${VERSION}" \
    -o /raftd ./cmd/raftd \
 && CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -o /kvctl ./cmd/kvctl

# Pre-create the data directory. A named/anonymous volume mounted at /data
# inherits this directory's ownership on first creation, so the distroless
# runtime user (uid 65532, "nonroot") can write here. Without this, the
# root-owned mount makes `mkdir /data/<node_id>` fail with EACCES.
RUN mkdir -p /data

# ---- runtime stage ----
# Pin by digest in production, e.g.
#   FROM gcr.io/distroless/static-debian12:nonroot@sha256:<digest>
# resolved via `crane digest gcr.io/distroless/static-debian12:nonroot`.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /raftd /raftd
COPY --from=builder /kvctl /kvctl
COPY --from=builder --chown=65532:65532 /data /data
VOLUME ["/data"]

# Inter-node Raft (8001/8003/8005) and HTTP API (8002/8004/8006) ports are set
# per node in the config; these are representative defaults for documentation.
EXPOSE 8001 8002
ENTRYPOINT ["/raftd"]
CMD ["-config", "/etc/raftd/config.yaml"]
