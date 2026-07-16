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
FROM golang:1.26.5-alpine AS builder
WORKDIR /app

# Download dependencies first (better layer caching)
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath \
    -ldflags="-s -w -X github.com/raft-consensus/pkg/version.Version=${VERSION}" \
    -o /raftd ./cmd/raftd

# ---- runtime stage ----
# Pin by digest in production, e.g.
#   FROM gcr.io/distroless/static-debian12:nonroot@sha256:<digest>
# resolved via `crane digest gcr.io/distroless/static-debian12:nonroot`.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /raftd /raftd

EXPOSE 8080 8081
ENTRYPOINT ["/raftd"]
CMD ["-config", "/etc/raftd/config.yaml"]
