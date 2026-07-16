#!/bin/bash
# Generate self-signed TLS certificates for raftd development
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CERTS_DIR="$DIR/generated"
mkdir -p "$CERTS_DIR"
# L10: private keys must never be world-readable. Create the directory 0700 and
# use a restrictive umask so keys land as 0600.
chmod 700 "$CERTS_DIR"
umask 077

# Generate CA key and certificate
openssl genrsa -out "$CERTS_DIR/ca.key" 4096
openssl req -new -x509 -days 3650 -key "$CERTS_DIR/ca.key" \
    -out "$CERTS_DIR/ca.crt" \
    -subj "/C=US/O=Raft/CN=Raft CA"

# Generate server key and CSR
openssl genrsa -out "$CERTS_DIR/server.key" 4096
openssl req -new -key "$CERTS_DIR/server.key" \
    -out "$CERTS_DIR/server.csr" \
    -subj "/C=US/O=Raft/CN=raft-server"

# Create SAN extension file
cat > "$CERTS_DIR/server.ext" <<EOF
subjectAltName=DNS:localhost,DNS:*.raft.local,IP:127.0.0.1
EOF

# Sign server certificate with CA
openssl x509 -req -days 365 \
    -in "$CERTS_DIR/server.csr" \
    -CA "$CERTS_DIR/ca.crt" \
    -CAkey "$CERTS_DIR/ca.key" \
    -CAcreateserial \
    -extfile "$CERTS_DIR/server.ext" \
    -out "$CERTS_DIR/server.crt"

# Generate client key and CSR (for mTLS)
openssl genrsa -out "$CERTS_DIR/client.key" 4096
openssl req -new -key "$CERTS_DIR/client.key" \
    -out "$CERTS_DIR/client.csr" \
    -subj "/C=US/O=Raft/CN=raft-client"

openssl x509 -req -days 365 \
    -in "$CERTS_DIR/client.csr" \
    -CA "$CERTS_DIR/ca.crt" \
    -CAkey "$CERTS_DIR/ca.key" \
    -CAcreateserial \
    -out "$CERTS_DIR/client.crt"

# L10: ensure private keys are owner-read-only regardless of umask inheritance.
chmod 600 "$CERTS_DIR"/*.key

echo "Certificates generated in $CERTS_DIR"
echo "  ca.crt     - CA certificate"
echo "  server.key - Server private key"
echo "  server.crt - Server certificate"
echo "  client.key - Client private key"
echo "  client.crt - Client certificate (for mTLS)"
