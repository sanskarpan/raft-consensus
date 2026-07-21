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

# Generate per-node certs with node-ID SANs (for mTLS SAN validation)
for NODE in node1 node2 node3; do
    openssl genrsa -out "$CERTS_DIR/${NODE}.key" 4096

    cat > "$CERTS_DIR/${NODE}.ext" <<EXTEOF
subjectAltName=DNS:${NODE},DNS:${NODE}.cluster.local,DNS:localhost,IP:127.0.0.1
extendedKeyUsage=serverAuth,clientAuth
EXTEOF

    openssl req -new -key "$CERTS_DIR/${NODE}.key" \
        -out "$CERTS_DIR/${NODE}.csr" \
        -subj "/C=US/O=Raft/CN=${NODE}"

    openssl x509 -req -days 3650 \
        -in "$CERTS_DIR/${NODE}.csr" \
        -CA "$CERTS_DIR/ca.crt" \
        -CAkey "$CERTS_DIR/ca.key" \
        -CAcreateserial \
        -extfile "$CERTS_DIR/${NODE}.ext" \
        -out "$CERTS_DIR/${NODE}.crt"

    echo "  ${NODE}.crt / ${NODE}.key - per-node cert for mTLS SAN validation"
done

# L10: ensure private keys are owner-read-only regardless of umask inheritance.
chmod 600 "$CERTS_DIR"/*.key

echo "Certificates generated in $CERTS_DIR"
echo "  ca.crt     - CA certificate"
echo "  server.key - Server private key"
echo "  server.crt - Server certificate"
echo "  client.key - Client private key"
echo "  client.crt - Client certificate (for mTLS)"
echo ""
echo "Per-node certs for mTLS:"
echo "  Config example (node1):"
echo "    tls_cert: $CERTS_DIR/node1.crt"
echo "    tls_key:  $CERTS_DIR/node1.key"
echo "    tls_ca:   $CERTS_DIR/ca.crt"
echo "    require_tls: true"
