#!/usr/bin/env bash
# certmanager-install.sh — cert-manager installation check and ClusterIssuer setup
#
# This script:
#   1. Verifies that cert-manager is installed in the target cluster
#   2. Waits for the cert-manager webhook to become ready
#   3. Creates a self-signed ClusterIssuer suitable for raft-consensus development
#   4. Creates a self-signed CA Certificate (bootstrap CA) to back an Issuer
#   5. Creates an Issuer backed by that CA, which the Helm chart uses to issue
#      per-node certificates
#
# For production: replace the SelfSigned / CA Issuer with one of:
#   - ACME (Let's Encrypt) for public endpoints
#   - Vault Issuer (requires vault.io/v1alpha1 CRDs)
#   - CFSSL Issuer
#   - AWS/GCP/Azure certificate issuers
#
# Usage:
#   NAMESPACE=raft ./certmanager-install.sh
#
# Optional environment variables:
#   CERTMANAGER_VERSION  — version to install if not present (default: v1.14.5)
#   NAMESPACE            — Kubernetes namespace for raft-cluster (default: raft)
#   INSTALL_IF_MISSING   — set to "true" to auto-install cert-manager (default: false)
#   KUBECONFIG           — path to kubeconfig (default: ~/.kube/config)
#
# Requirements: kubectl, helm (only if INSTALL_IF_MISSING=true)

set -euo pipefail

CERTMANAGER_VERSION="${CERTMANAGER_VERSION:-v1.14.5}"
NAMESPACE="${NAMESPACE:-raft}"
INSTALL_IF_MISSING="${INSTALL_IF_MISSING:-false}"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
info()  { printf '\033[1;34m[INFO]\033[0m  %s\n' "$*"; }
ok()    { printf '\033[1;32m[OK]\033[0m    %s\n' "$*"; }
warn()  { printf '\033[1;33m[WARN]\033[0m  %s\n' "$*"; }
die()   { printf '\033[1;31m[ERROR]\033[0m %s\n' "$*" >&2; exit 1; }

require_cmd() { command -v "$1" >/dev/null 2>&1 || die "Required command not found: $1"; }

require_cmd kubectl

# ---------------------------------------------------------------------------
# Step 1: Check if cert-manager is installed
# ---------------------------------------------------------------------------
info "Checking for cert-manager installation …"

CM_NS=""
for ns in cert-manager kube-system default; do
  if kubectl get deployment -n "${ns}" cert-manager >/dev/null 2>&1; then
    CM_NS="${ns}"
    break
  fi
done

if [[ -z "${CM_NS}" ]]; then
  warn "cert-manager deployment not found in common namespaces."
  if [[ "${INSTALL_IF_MISSING}" == "true" ]]; then
    require_cmd helm
    info "INSTALL_IF_MISSING=true — installing cert-manager ${CERTMANAGER_VERSION} via Helm …"
    helm repo add jetstack https://charts.jetstack.io --force-update
    helm repo update
    helm install cert-manager jetstack/cert-manager \
      --namespace cert-manager                      \
      --create-namespace                            \
      --version "${CERTMANAGER_VERSION}"            \
      --set installCRDs=true
    CM_NS="cert-manager"
    ok "cert-manager ${CERTMANAGER_VERSION} installed."
  else
    die "cert-manager is not installed. Set INSTALL_IF_MISSING=true to auto-install, or install manually:
  kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/${CERTMANAGER_VERSION}/cert-manager.yaml"
  fi
else
  ok "cert-manager found in namespace '${CM_NS}'."
fi

# ---------------------------------------------------------------------------
# Step 2: Wait for cert-manager webhook to be ready
# ---------------------------------------------------------------------------
info "Waiting for cert-manager webhook to be Ready (up to 120s) …"
kubectl rollout status deployment/cert-manager-webhook \
  -n "${CM_NS}" --timeout=120s
ok "cert-manager webhook is ready."

# ---------------------------------------------------------------------------
# Step 3: Ensure target namespace exists
# ---------------------------------------------------------------------------
if ! kubectl get namespace "${NAMESPACE}" >/dev/null 2>&1; then
  info "Creating namespace '${NAMESPACE}' …"
  kubectl create namespace "${NAMESPACE}"
fi
ok "Namespace '${NAMESPACE}' exists."

# ---------------------------------------------------------------------------
# Step 4: Create a self-signed ClusterIssuer (bootstrap issuer)
# ---------------------------------------------------------------------------
# The SelfSigned ClusterIssuer is the bootstrap root — it can only sign
# Certificate resources marked isCA: true. We use it to create a CA cert,
# then create a second Issuer backed by that CA cert.
info "Creating SelfSigned ClusterIssuer 'selfsigned-cluster-issuer' …"
kubectl apply -f - <<'EOF'
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata:
  name: selfsigned-cluster-issuer
spec:
  selfSigned: {}
EOF
ok "SelfSigned ClusterIssuer created."

# ---------------------------------------------------------------------------
# Step 5: Create the bootstrap CA Certificate
# ---------------------------------------------------------------------------
# This certificate will be stored in the Secret 'raft-ca-secret' and used as
# the trust anchor (ca.crt) that all raft nodes mount.
info "Creating bootstrap CA Certificate 'raft-ca' in namespace '${NAMESPACE}' …"
kubectl apply -f - <<EOF
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: raft-ca
  namespace: ${NAMESPACE}
spec:
  isCA: true
  commonName: raft-cluster Root CA
  subject:
    organizations:
      - raft-cluster
  secretName: raft-ca-secret
  duration: 87600h    # 10 years
  renewBefore: 720h   # 30 days
  privateKey:
    algorithm: ECDSA
    size: 384
  issuerRef:
    name: selfsigned-cluster-issuer
    kind: ClusterIssuer
    group: cert-manager.io
EOF
ok "Bootstrap CA Certificate 'raft-ca' applied."

# Wait for the CA Secret to be populated.
info "Waiting for CA secret 'raft-ca-secret' to be created …"
for i in $(seq 1 30); do
  if kubectl get secret raft-ca-secret -n "${NAMESPACE}" >/dev/null 2>&1; then
    ok "CA secret 'raft-ca-secret' is ready."
    break
  fi
  sleep 2
  if [[ "${i}" -eq 30 ]]; then
    die "Timed out waiting for raft-ca-secret to be created."
  fi
done

# ---------------------------------------------------------------------------
# Step 6: Create an Issuer backed by the CA
# ---------------------------------------------------------------------------
# This Issuer is what the Helm chart's cert-manager templates use to issue
# per-node certificates (one Certificate per raft node).
info "Creating CA-backed Issuer 'raft-cluster-issuer' in namespace '${NAMESPACE}' …"
kubectl apply -f - <<EOF
apiVersion: cert-manager.io/v1
kind: Issuer
metadata:
  name: raft-cluster-issuer
  namespace: ${NAMESPACE}
spec:
  ca:
    secretName: raft-ca-secret
EOF
ok "Issuer 'raft-cluster-issuer' created."

# Wait for the Issuer to be Ready.
info "Waiting for Issuer 'raft-cluster-issuer' to become Ready …"
for i in $(seq 1 20); do
  READY=$(kubectl get issuer raft-cluster-issuer -n "${NAMESPACE}" \
    -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || echo "")
  if [[ "${READY}" == "True" ]]; then
    ok "Issuer 'raft-cluster-issuer' is Ready."
    break
  fi
  sleep 3
  if [[ "${i}" -eq 20 ]]; then
    warn "Issuer not Ready after 60s — continuing anyway. Check: kubectl describe issuer raft-cluster-issuer -n ${NAMESPACE}"
  fi
done

# ---------------------------------------------------------------------------
# Step 7: Test — issue a sample certificate
# ---------------------------------------------------------------------------
info "Issuing a test Certificate to verify the Issuer works …"
kubectl apply -f - <<EOF
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: raft-test-cert
  namespace: ${NAMESPACE}
spec:
  secretName: raft-test-cert-secret
  commonName: node-test
  dnsNames:
    - node-test
    - node-test.raft.svc.cluster.local
    - localhost
  ipAddresses:
    - 127.0.0.1
  duration: 8760h
  renewBefore: 720h
  privateKey:
    algorithm: ECDSA
    size: 256
  usages:
    - server auth
    - client auth
  issuerRef:
    name: raft-cluster-issuer
    kind: Issuer
    group: cert-manager.io
EOF

# Wait up to 30s for the certificate to be Ready.
for i in $(seq 1 15); do
  CERT_READY=$(kubectl get certificate raft-test-cert -n "${NAMESPACE}" \
    -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || echo "")
  if [[ "${CERT_READY}" == "True" ]]; then
    ok "Test certificate issued successfully."
    break
  fi
  sleep 2
  if [[ "${i}" -eq 15 ]]; then
    warn "Test certificate not Ready after 30s. Check: kubectl describe certificate raft-test-cert -n ${NAMESPACE}"
  fi
done

# Clean up test cert.
kubectl delete certificate raft-test-cert -n "${NAMESPACE}" --ignore-not-found=true >/dev/null
kubectl delete secret raft-test-cert-secret -n "${NAMESPACE}" --ignore-not-found=true >/dev/null

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
cat <<SUMMARY

+---------------------------------------------------------------+
|       cert-manager setup for raft-consensus DONE             |
+---------------------------------------------------------------+
  Namespace         : ${NAMESPACE}
  ClusterIssuer     : selfsigned-cluster-issuer  (bootstrap)
  Issuer            : raft-cluster-issuer         (used by Helm chart)
  CA Secret         : raft-ca-secret              (contains ca.crt)

  To use with the Helm chart, set in values.yaml:
    tls:
      mode: "cert-manager"
      certManager:
        enabled: true
        issuerRef:
          name: raft-cluster-issuer
          kind: Issuer

  To retrieve the CA cert for external clients:
    kubectl get secret raft-ca-secret -n ${NAMESPACE} \\
      -o jsonpath='{.data.ca\.crt}' | base64 -d > ca.crt

  For production: replace 'raft-cluster-issuer' with a Vault or ACME issuer.
SUMMARY
