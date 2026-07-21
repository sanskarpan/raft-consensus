#!/usr/bin/env bash
# rotate-node-cert.sh — zero-downtime TLS certificate rotation for a raft node
#
# Rotates the TLS certificate for a specific raft node without cluster downtime.
# The rotation is:
#   1. Validate the new cert/key pair
#   2. Verify the new cert is signed by the expected CA
#   3. Copy the new cert/key into the node's data directory (atomic rename)
#   4. Send SIGHUP to the raftd process so the cert reloader picks up the new files
#   5. Verify the node is still serving traffic after rotation
#
# raftd uses a GetCertificate/GetClientCertificate-backed atomic reloader
# (pkg/transport/certreload.go) — SIGHUP triggers a live reload with zero
# connection interruption.
#
# Usage:
#   ./rotate-node-cert.sh <node-id> <new-cert-pem> <new-key-pem>
#
# Examples:
#   # Rotate node1 with certs issued from Vault:
#   vault write -format=json pki_int/issue/raft-node \
#     common_name="node1.cluster.local" alt_names="node1,localhost" \
#     ip_sans="127.0.0.1" > /tmp/vault-cert.json
#   jq -r '.data.certificate' /tmp/vault-cert.json > /tmp/node1.crt
#   jq -r '.data.private_key' /tmp/vault-cert.json > /tmp/node1.key
#   ./rotate-node-cert.sh node1 /tmp/node1.crt /tmp/node1.key
#
#   # Rotate node2 with a manually-generated cert:
#   ./rotate-node-cert.sh node2 /etc/pki/node2.crt /etc/pki/node2.key
#
# Optional environment variables:
#   RAFT_DATA_DIR   — directory where raftd stores certs (default: ./data/<node-id>)
#   RAFT_CA_CERT    — path to the CA cert for validation (default: <data-dir>/ca.crt)
#   RAFTD_PID_FILE  — path to raftd PID file (default: /var/run/raftd-<node-id>.pid)
#   RAFTD_HTTP_ADDR — HTTP address for post-rotation health check (default: http://127.0.0.1:8080)
#   DRY_RUN         — set to "true" to validate without making changes (default: false)
#   BACKUP_OLD_CERT — set to "false" to skip backing up old cert (default: true)

set -euo pipefail

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
if [[ $# -lt 3 ]]; then
  cat >&2 <<'USAGE'
Usage: rotate-node-cert.sh <node-id> <new-cert-pem> <new-key-pem>

  node-id       — ID of the raft node whose cert is being rotated (e.g. "node1")
  new-cert-pem  — path to the new PEM-encoded certificate file
  new-key-pem   — path to the new PEM-encoded private key file

Environment variables (all optional):
  RAFT_DATA_DIR   — cert storage dir (default: ./data/<node-id>)
  RAFT_CA_CERT    — CA cert for validation (default: <data-dir>/ca.crt)
  RAFTD_PID_FILE  — raftd PID file (default: /var/run/raftd-<node-id>.pid)
  RAFTD_HTTP_ADDR — health check URL (default: http://127.0.0.1:8080)
  DRY_RUN         — "true" to validate only (default: false)
  BACKUP_OLD_CERT — "false" to skip backup (default: true)
USAGE
  exit 1
fi

NODE_ID="$1"
NEW_CERT="$2"
NEW_KEY="$3"

RAFT_DATA_DIR="${RAFT_DATA_DIR:-./data/${NODE_ID}}"
RAFT_CA_CERT="${RAFT_CA_CERT:-${RAFT_DATA_DIR}/ca.crt}"
RAFTD_PID_FILE="${RAFTD_PID_FILE:-/var/run/raftd-${NODE_ID}.pid}"
RAFTD_HTTP_ADDR="${RAFTD_HTTP_ADDR:-http://127.0.0.1:8080}"
DRY_RUN="${DRY_RUN:-false}"
BACKUP_OLD_CERT="${BACKUP_OLD_CERT:-true}"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
info()  { printf '\033[1;34m[INFO]\033[0m  %s\n' "$*"; }
ok()    { printf '\033[1;32m[OK]\033[0m    %s\n' "$*"; }
warn()  { printf '\033[1;33m[WARN]\033[0m  %s\n' "$*"; }
die()   { printf '\033[1;31m[ERROR]\033[0m %s\n' "$*" >&2; exit 1; }

require_cmd() { command -v "$1" >/dev/null 2>&1 || die "Required command not found: $1 — install it and try again."; }

# ---------------------------------------------------------------------------
# Prerequisites
# ---------------------------------------------------------------------------
require_cmd openssl

[[ -f "${NEW_CERT}" ]] || die "New certificate file not found: ${NEW_CERT}"
[[ -f "${NEW_KEY}"  ]] || die "New private key file not found: ${NEW_KEY}"

# ---------------------------------------------------------------------------
# Step 1: Validate new certificate PEM
# ---------------------------------------------------------------------------
info "Validating new certificate at ${NEW_CERT} …"

# Parse the certificate to get subject, validity, and SANs.
CERT_TEXT=$(openssl x509 -in "${NEW_CERT}" -noout -text 2>&1) \
  || die "Failed to parse certificate: ${NEW_CERT}"

# Extract and display key fields for operator review.
SUBJECT=$(openssl x509 -in "${NEW_CERT}" -noout -subject 2>/dev/null | sed 's/subject=//')
NOT_BEFORE=$(openssl x509 -in "${NEW_CERT}" -noout -startdate 2>/dev/null | sed 's/notBefore=//')
NOT_AFTER=$(openssl x509 -in "${NEW_CERT}" -noout -enddate   2>/dev/null | sed 's/notAfter=//')

info "  Subject    : ${SUBJECT}"
info "  Valid from : ${NOT_BEFORE}"
info "  Valid until: ${NOT_AFTER}"

# Check the cert is not already expired.
openssl x509 -in "${NEW_CERT}" -noout -checkend 0 \
  || die "New certificate is already expired (NotAfter: ${NOT_AFTER})."

# Check the cert has at least 30 days remaining.
THIRTY_DAYS=$((30 * 24 * 3600))
if ! openssl x509 -in "${NEW_CERT}" -noout -checkend "${THIRTY_DAYS}" 2>/dev/null; then
  warn "New certificate expires in fewer than 30 days (${NOT_AFTER})."
  warn "Consider issuing a longer-lived certificate before rotating."
fi

ok "Certificate is valid."

# ---------------------------------------------------------------------------
# Step 2: Verify cert/key pair match
# ---------------------------------------------------------------------------
info "Verifying cert/key pair consistency …"
CERT_PUBKEY=$(openssl x509 -in "${NEW_CERT}" -noout -pubkey 2>/dev/null)
KEY_PUBKEY=$(openssl pkey -in "${NEW_KEY}" -pubout 2>/dev/null)

if [[ "${CERT_PUBKEY}" != "${KEY_PUBKEY}" ]]; then
  die "Certificate and private key do not match — aborting rotation."
fi
ok "Cert/key pair match confirmed."

# ---------------------------------------------------------------------------
# Step 3: Verify the new cert is signed by the expected CA
# ---------------------------------------------------------------------------
if [[ -f "${RAFT_CA_CERT}" ]]; then
  info "Verifying new cert is signed by CA at ${RAFT_CA_CERT} …"
  if openssl verify -CAfile "${RAFT_CA_CERT}" "${NEW_CERT}" >/dev/null 2>&1; then
    ok "Certificate verified against CA."
  else
    warn "Certificate does NOT verify against ${RAFT_CA_CERT}."
    warn "If you are rotating the CA as well, this is expected — continuing."
    warn "Otherwise, ensure the new cert is signed by the correct CA."
  fi
else
  warn "CA cert not found at ${RAFT_CA_CERT} — skipping CA verification."
fi

# ---------------------------------------------------------------------------
# DRY RUN gate
# ---------------------------------------------------------------------------
if [[ "${DRY_RUN}" == "true" ]]; then
  ok "DRY_RUN=true — validation passed. No files were modified."
  exit 0
fi

# ---------------------------------------------------------------------------
# Step 4: Backup current certs
# ---------------------------------------------------------------------------
DEST_CERT="${RAFT_DATA_DIR}/${NODE_ID}.crt"
DEST_KEY="${RAFT_DATA_DIR}/${NODE_ID}.key"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)

if [[ "${BACKUP_OLD_CERT}" == "true" ]]; then
  if [[ -f "${DEST_CERT}" ]]; then
    BACKUP_CERT="${DEST_CERT}.bak.${TIMESTAMP}"
    cp "${DEST_CERT}" "${BACKUP_CERT}"
    info "Backed up old cert to ${BACKUP_CERT}"
  fi
  if [[ -f "${DEST_KEY}" ]]; then
    BACKUP_KEY="${DEST_KEY}.bak.${TIMESTAMP}"
    cp "${DEST_KEY}" "${BACKUP_KEY}"
    info "Backed up old key to ${BACKUP_KEY}"
  fi
fi

# ---------------------------------------------------------------------------
# Step 5: Atomically replace cert and key
# ---------------------------------------------------------------------------
# Write to temp files first, then rename atomically to avoid a window where
# the cert exists but the key doesn't (or vice versa).
info "Installing new cert/key into ${RAFT_DATA_DIR} …"
mkdir -p "${RAFT_DATA_DIR}"

TMP_CERT=$(mktemp "${RAFT_DATA_DIR}/.rotate-cert.XXXXXX")
TMP_KEY=$(mktemp  "${RAFT_DATA_DIR}/.rotate-key.XXXXXX")

# Ensure temp files are cleaned up on failure.
cleanup_tmps() { rm -f "${TMP_CERT}" "${TMP_KEY}" 2>/dev/null || true; }
trap cleanup_tmps EXIT

cp "${NEW_CERT}" "${TMP_CERT}"
cp "${NEW_KEY}"  "${TMP_KEY}"
chmod 600 "${TMP_CERT}" "${TMP_KEY}"

# Rename key first, then cert (raftd reloader watches the cert).
mv "${TMP_KEY}"  "${DEST_KEY}"
mv "${TMP_CERT}" "${DEST_CERT}"

# Remove trap since files are now in place.
trap - EXIT

ok "New cert/key installed:
    ${DEST_CERT}
    ${DEST_KEY}"

# ---------------------------------------------------------------------------
# Step 6: Send SIGHUP to reload the certificate
# ---------------------------------------------------------------------------
RAFTD_PID=""

if [[ -f "${RAFTD_PID_FILE}" ]]; then
  RAFTD_PID=$(cat "${RAFTD_PID_FILE}")
elif command -v pgrep >/dev/null 2>&1; then
  RAFTD_PID=$(pgrep -f "raftd.*${NODE_ID}" 2>/dev/null | head -1 || true)
fi

if [[ -n "${RAFTD_PID}" ]]; then
  info "Sending SIGHUP to raftd (PID ${RAFTD_PID}) to trigger cert reload …"
  if kill -HUP "${RAFTD_PID}" 2>/dev/null; then
    ok "SIGHUP sent to PID ${RAFTD_PID}."
  else
    warn "Failed to send SIGHUP to PID ${RAFTD_PID}. Process may have exited. Cert files are updated."
  fi
else
  warn "Could not determine raftd PID for ${NODE_ID}."
  warn "Send SIGHUP manually: kill -HUP \$(pgrep -f 'raftd.*${NODE_ID}')"
  warn "Or restart raftd to load the new certificate."
fi

# ---------------------------------------------------------------------------
# Step 7: Post-rotation health check
# ---------------------------------------------------------------------------
info "Waiting 3s for reload to complete, then checking node health …"
sleep 3

if command -v curl >/dev/null 2>&1; then
  HTTP_STATUS=$(curl -sk -o /dev/null -w '%{http_code}' \
    --max-time 5 "${RAFTD_HTTP_ADDR}/v1/status" 2>/dev/null || echo "000")
  if [[ "${HTTP_STATUS}" =~ ^2 ]]; then
    ok "Health check passed (HTTP ${HTTP_STATUS}) — node is serving requests."
  else
    warn "Health check returned HTTP ${HTTP_STATUS} from ${RAFTD_HTTP_ADDR}/v1/status."
    warn "Check raftd logs: journalctl -u raftd-${NODE_ID} -n 50"
  fi
else
  warn "curl not found — skipping post-rotation health check."
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
cat <<SUMMARY

+---------------------------------------------------------------+
|   Certificate rotation for ${NODE_ID} DONE                    |
+---------------------------------------------------------------+
  New cert     : ${DEST_CERT}
  New key      : ${DEST_KEY}
  Valid until  : ${NOT_AFTER}
  SIGHUP sent  : ${RAFTD_PID:-"(manual required)"}

  To verify the new cert is live, inspect the TLS handshake:
    openssl s_client -connect 127.0.0.1:<raft-port> -servername ${NODE_ID} </dev/null 2>&1 | head -20

  To revert to the old cert (if backup enabled):
    mv ${DEST_CERT}.bak.${TIMESTAMP} ${DEST_CERT}
    mv ${DEST_KEY}.bak.${TIMESTAMP} ${DEST_KEY}
    kill -HUP ${RAFTD_PID:-"<pid>"}
SUMMARY
