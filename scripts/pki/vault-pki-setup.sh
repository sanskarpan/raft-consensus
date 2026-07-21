#!/usr/bin/env bash
# vault-pki-setup.sh — HashiCorp Vault PKI engine setup for raft-consensus
#
# This script bootstraps a Vault PKI secrets engine with:
#   1. A root CA (generated internally or imported from an external PEM)
#   2. An intermediate CA for raft nodes
#   3. A "raft-node" role with 1-year TTL and appropriate SANs
#   4. A Vault policy that lets raft nodes issue their own certificates
#
# Usage:
#   VAULT_ADDR=http://127.0.0.1:8200 VAULT_TOKEN=root ./vault-pki-setup.sh
#
# Optional environment variables:
#   VAULT_ADDR          — Vault server URL (default: http://127.0.0.1:8200)
#   VAULT_TOKEN         — Vault authentication token (default: root)
#   PKI_MOUNT           — Secrets engine mount path (default: pki)
#   PKI_INT_MOUNT       — Intermediate CA mount path (default: pki_int)
#   ROOT_CA_TTL         — Root CA validity (default: 87600h = 10 years)
#   INT_CA_TTL          — Intermediate CA validity (default: 43800h = 5 years)
#   RAFT_CERT_TTL       — Issued node cert TTL (default: 8760h = 1 year)
#   RAFT_CLUSTER_DOMAIN — Base domain for DNS SANs (default: cluster.local)
#   EXTERNAL_ROOT_CA    — Path to external root CA bundle (PEM); if set,
#                         the root CA generation step is skipped and this
#                         bundle is used to sign the intermediate CSR instead.
#
# Requirements: vault CLI must be installed and PATH-accessible.
#
# Security notes:
#   - The root CA private key stays inside Vault and is never exported.
#   - The intermediate CA signs a CSR; its key also stays in Vault.
#   - Node certs are issued via the role and downloaded only at issuance time.
#   - Rotate node certs by re-running `vault write pki_int/issue/raft-node …`
#     and reloading raftd via SIGHUP (cert reloader picks them up automatically).

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
VAULT_ADDR="${VAULT_ADDR:-http://127.0.0.1:8200}"
VAULT_TOKEN="${VAULT_TOKEN:-root}"
PKI_MOUNT="${PKI_MOUNT:-pki}"
PKI_INT_MOUNT="${PKI_INT_MOUNT:-pki_int}"
ROOT_CA_TTL="${ROOT_CA_TTL:-87600h}"
INT_CA_TTL="${INT_CA_TTL:-43800h}"
RAFT_CERT_TTL="${RAFT_CERT_TTL:-8760h}"
RAFT_CLUSTER_DOMAIN="${RAFT_CLUSTER_DOMAIN:-cluster.local}"
EXTERNAL_ROOT_CA="${EXTERNAL_ROOT_CA:-}"

export VAULT_ADDR VAULT_TOKEN

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
info()  { printf '\033[1;34m[INFO]\033[0m  %s\n' "$*"; }
ok()    { printf '\033[1;32m[OK]\033[0m    %s\n' "$*"; }
warn()  { printf '\033[1;33m[WARN]\033[0m  %s\n' "$*"; }
die()   { printf '\033[1;31m[ERROR]\033[0m %s\n' "$*" >&2; exit 1; }

require_cmd() { command -v "$1" >/dev/null 2>&1 || die "Required command not found: $1"; }

require_cmd vault

# Verify connectivity to Vault.
info "Checking Vault connectivity at ${VAULT_ADDR} …"
vault status -format=json >/dev/null || die "Cannot reach Vault at ${VAULT_ADDR}. Is it running and VAULT_TOKEN correct?"
ok "Vault is reachable."

# ---------------------------------------------------------------------------
# Step 1: Enable root PKI secrets engine
# ---------------------------------------------------------------------------
if vault secrets list -format=json | grep -q "\"${PKI_MOUNT}/\""; then
  warn "PKI engine already mounted at ${PKI_MOUNT}/ — skipping enable."
else
  info "Enabling PKI secrets engine at ${PKI_MOUNT}/ …"
  vault secrets enable -path="${PKI_MOUNT}" pki
  vault secrets tune -max-lease-ttl="${ROOT_CA_TTL}" "${PKI_MOUNT}/"
  ok "PKI engine enabled at ${PKI_MOUNT}/."
fi

# ---------------------------------------------------------------------------
# Step 2: Generate or import root CA
# ---------------------------------------------------------------------------
if [[ -n "${EXTERNAL_ROOT_CA}" ]]; then
  # Import an external root CA bundle (the private key remains offline/HSM).
  info "Importing external root CA from ${EXTERNAL_ROOT_CA} …"
  [[ -f "${EXTERNAL_ROOT_CA}" ]] || die "EXTERNAL_ROOT_CA file not found: ${EXTERNAL_ROOT_CA}"
  vault write "${PKI_MOUNT}/config/ca" \
    pem_bundle=@"${EXTERNAL_ROOT_CA}"
  ok "External root CA imported."
else
  # Generate a new internal root CA.  Key never leaves Vault.
  ROOT_CN="raft-cluster Root CA"
  info "Generating internal root CA: '${ROOT_CN}' (TTL=${ROOT_CA_TTL}) …"
  vault write -field=certificate "${PKI_MOUNT}/root/generate/internal" \
    common_name="${ROOT_CN}"             \
    ttl="${ROOT_CA_TTL}"                 \
    key_type="ec"                        \
    key_bits=384                         \
    organization="raft-cluster"          \
    ou="Infrastructure"                  \
    > /tmp/root-ca.crt
  ok "Root CA generated. Certificate saved to /tmp/root-ca.crt."
fi

# Configure CRL and issuing URLs so issued certs embed valid CDP/AIA URIs.
info "Configuring root CA CRL and issuing certificate URLs …"
vault write "${PKI_MOUNT}/config/urls"                         \
  issuing_certificates="${VAULT_ADDR}/v1/${PKI_MOUNT}/ca"     \
  crl_distribution_points="${VAULT_ADDR}/v1/${PKI_MOUNT}/crl"
ok "Root CA URLs configured."

# ---------------------------------------------------------------------------
# Step 3: Enable intermediate PKI engine
# ---------------------------------------------------------------------------
if vault secrets list -format=json | grep -q "\"${PKI_INT_MOUNT}/\""; then
  warn "Intermediate PKI engine already mounted at ${PKI_INT_MOUNT}/ — skipping enable."
else
  info "Enabling intermediate PKI secrets engine at ${PKI_INT_MOUNT}/ …"
  vault secrets enable -path="${PKI_INT_MOUNT}" pki
  vault secrets tune -max-lease-ttl="${INT_CA_TTL}" "${PKI_INT_MOUNT}/"
  ok "Intermediate PKI engine enabled."
fi

# ---------------------------------------------------------------------------
# Step 4: Generate intermediate CSR and sign it with the root CA
# ---------------------------------------------------------------------------
INT_CN="raft-cluster Intermediate CA"
info "Generating intermediate CA CSR: '${INT_CN}' …"
vault write -format=json "${PKI_INT_MOUNT}/intermediate/generate/internal" \
  common_name="${INT_CN}"   \
  ttl="${INT_CA_TTL}"       \
  key_type="ec"             \
  key_bits=384              \
  organization="raft-cluster" \
  | jq -r '.data.csr' > /tmp/pki_int.csr

info "Signing intermediate CSR with root CA …"
vault write -format=json "${PKI_MOUNT}/root/sign-intermediate" \
  csr=@/tmp/pki_int.csr                 \
  common_name="${INT_CN}"               \
  ttl="${INT_CA_TTL}"                   \
  format=pem_bundle                     \
  | jq -r '.data.certificate' > /tmp/pki_int.crt

info "Setting intermediate CA signed certificate …"
vault write "${PKI_INT_MOUNT}/intermediate/set-signed" \
  certificate=@/tmp/pki_int.crt
ok "Intermediate CA signed and installed."

# Configure CRL/AIA URLs on the intermediate.
vault write "${PKI_INT_MOUNT}/config/urls"                         \
  issuing_certificates="${VAULT_ADDR}/v1/${PKI_INT_MOUNT}/ca"     \
  crl_distribution_points="${VAULT_ADDR}/v1/${PKI_INT_MOUNT}/crl"
ok "Intermediate CA URLs configured."

# ---------------------------------------------------------------------------
# Step 5: Create "raft-node" role
# ---------------------------------------------------------------------------
info "Creating raft-node role on ${PKI_INT_MOUNT}/ …"
vault write "${PKI_INT_MOUNT}/roles/raft-node"    \
  allowed_domains="raft-node,${RAFT_CLUSTER_DOMAIN},localhost" \
  allow_subdomains=true                            \
  allow_bare_domains=true                          \
  allow_localhost=true                             \
  allow_ip_sans=true                               \
  key_type="ec"                                    \
  key_bits=256                                     \
  ttl="${RAFT_CERT_TTL}"                           \
  max_ttl="${RAFT_CERT_TTL}"                       \
  client_flag=true                                 \
  server_flag=true                                 \
  require_cn=true
ok "Role raft-node created (TTL=${RAFT_CERT_TTL})."

# ---------------------------------------------------------------------------
# Step 6: Create Vault policy for raft nodes
# ---------------------------------------------------------------------------
info "Writing Vault policy 'raft-pki' …"
vault policy write raft-pki - <<'POLICY'
# raft-pki — allows a raft node to issue its own certificate and read the CA.
# Attach this policy to the AppRole / Kubernetes auth role used by raftd.

path "pki_int/issue/raft-node" {
  capabilities = ["create", "update"]
}

path "pki_int/ca" {
  capabilities = ["read"]
}

path "pki_int/ca_chain" {
  capabilities = ["read"]
}

path "pki_int/crl" {
  capabilities = ["read"]
}
POLICY
ok "Policy 'raft-pki' written."

# ---------------------------------------------------------------------------
# Step 7: (Optional) Test issuance for a sample node
# ---------------------------------------------------------------------------
info "Test-issuing a certificate for 'node1.${RAFT_CLUSTER_DOMAIN}' …"
vault write -format=json "${PKI_INT_MOUNT}/issue/raft-node"        \
  common_name="node1.${RAFT_CLUSTER_DOMAIN}"                       \
  alt_names="node1,localhost"                                       \
  ip_sans="127.0.0.1,::1"                                          \
  ttl="${RAFT_CERT_TTL}"                                            \
  | jq -r '{
      serial:      .data.serial_number,
      not_after:   .data.expiration,
      common_name: .data.certificate | (ltrimstr("-----BEGIN CERTIFICATE-----\n"))
    }'
ok "Test certificate issued successfully."

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
cat <<SUMMARY

+---------------------------------------------------------------+
|            Vault PKI setup for raft-consensus DONE           |
+---------------------------------------------------------------+
  Root CA mount     : ${PKI_MOUNT}/
  Intermediate mount: ${PKI_INT_MOUNT}/
  Role              : ${PKI_INT_MOUNT}/roles/raft-node
  Policy            : raft-pki

  To issue a node cert at runtime:
    vault write ${PKI_INT_MOUNT}/issue/raft-node \\
      common_name="<nodeID>.${RAFT_CLUSTER_DOMAIN}" \\
      alt_names="<nodeID>,localhost"                 \\
      ip_sans="127.0.0.1,::1"

  To rotate a node cert (raftd will pick it up via SIGHUP):
    ./scripts/pki/rotate-node-cert.sh <node-id> <new-cert> <new-key>

  CA chain for node trust pool:
    curl -s ${VAULT_ADDR}/v1/${PKI_INT_MOUNT}/ca_chain > ca-chain.pem
SUMMARY
