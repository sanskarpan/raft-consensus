# syntax=docker/dockerfile:1

# ---- build stage ----
FROM golang:1.24-alpine AS builder
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
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /raftd /raftd

EXPOSE 8080 8081
ENTRYPOINT ["/raftd"]
CMD ["-config", "/etc/raftd/config.yaml"]
