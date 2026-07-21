#!/usr/bin/env bash
# Generates self-signed mTLS certificates for local development.
# Uses ECDSA P-256 (smaller and faster than RSA-4096).
# Usage: ./scripts/gen-dev-certs.sh [output-dir]
set -euo pipefail
DIR="${1:-./certs}"
mkdir -p "$DIR"
chmod 700 "$DIR"
umask 077

echo "Generating CA..."
openssl req -new -x509 -days 3650 -nodes \
  -newkey ec -pkeyopt ec_paramgen_curve:P-256 \
  -out "$DIR/ca.crt" -keyout "$DIR/ca.key" \
  -subj "/CN=raft-ca/O=raft-cluster" \
  -addext "basicConstraints=critical,CA:true" \
  -addext "keyUsage=critical,keyCertSign,cRLSign"
chmod 600 "$DIR/ca.key"

for NODE in node1 node2 node3; do
  echo "Generating cert for $NODE..."
  openssl req -new -nodes \
    -newkey ec -pkeyopt ec_paramgen_curve:P-256 \
    -out "$DIR/$NODE.csr" -keyout "$DIR/$NODE.key" \
    -subj "/CN=$NODE/O=raft-cluster"
  chmod 600 "$DIR/$NODE.key"

  openssl x509 -req -days 3650 \
    -CA "$DIR/ca.crt" -CAkey "$DIR/ca.key" \
    -CAcreateserial \
    -in "$DIR/$NODE.csr" \
    -out "$DIR/$NODE.crt" \
    -extfile <(printf "subjectAltName=DNS:%s,DNS:%s.cluster.local,DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth,clientAuth" "$NODE" "$NODE")
done

echo ""
echo "Generated certs in $DIR/"
echo ""
echo "Config example (node1):"
echo "  tls_cert: $DIR/node1.crt"
echo "  tls_key:  $DIR/node1.key"
echo "  tls_ca:   $DIR/ca.crt"
echo "  require_tls: true"
echo ""
echo "Or for zero-config encrypted dev:"
echo "  auto_tls: true"
