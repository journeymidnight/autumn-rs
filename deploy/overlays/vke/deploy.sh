#!/usr/bin/env bash
# Deploy the vke overlay.
#
# The autumn-rs image (registry AND tag) is deliberately NOT committed —
# the registry/namespace is a private account (this is a public repo) and the
# tag is a per-build git SHA. Both come from the environment, so a deploy never
# dirties git:
#
#   export AUTUMN_IMAGE_REPO=<registry>/<namespace>/autumn-rs
#   export AUTUMN_IMAGE_TAG=<git-sha>          # optional; defaults to HEAD
#   deploy/overlays/vke/deploy.sh              # apply
#   deploy/overlays/vke/deploy.sh --restart    # apply + stop-the-world restart
#
# The manifests carry the base placeholder `autumn-rs:latest`; this script
# rewrites it at apply time.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

: "${AUTUMN_IMAGE_REPO:?set AUTUMN_IMAGE_REPO, e.g. <registry>/<namespace>/autumn-rs}"
: "${AUTUMN_IMAGE_TAG:=$(git -C "$here" rev-parse HEAD)}"
image="${AUTUMN_IMAGE_REPO}:${AUTUMN_IMAGE_TAG}"

# data-plane authz is ON by default. Provision the signing-key
# + admin-token Secret ONCE (rotating it invalidates every minted credential, so
# we never overwrite an existing one). The signing-key file format is
# `<kid> <hex-32-byte-seed>` — same as `autumn-op gen-signing-key`, generated
# here with shell so the deploy host needs no autumn binary.
#
# To actually DISABLE authz on k8s, set `AUTUMN_AUTH_DISABLE: "1"` in
# configmap.yaml — the manager entrypoint reads the POD env, not this script.
# `AUTUMN_AUTH_DISABLE=1` HERE only SKIPS key generation; it does NOT unmount an
# already-created `autumn-authz` Secret (which the manager would still mount). So
# a real rollback = configmap flag (or delete the Secret + redeploy).
if [[ "${AUTUMN_AUTH_DISABLE:-0}" != "1" ]]; then
    kubectl create namespace autumn >/dev/null 2>&1 || true
    if kubectl -n autumn get secret autumn-authz >/dev/null 2>&1; then
        echo ">>> authz Secret autumn-authz exists (reused — never rotated here)"
    else
        echo ">>> generating authz signing key + admin token → Secret autumn-authz"
        # Restricted temp files + --from-file (NOT --from-literal) so the seed +
        # admin token never land in kubectl's argv / /proc/<pid>/cmdline on a
        # shared deploy host (coco P2 security).
        authz_tmp="$(umask 077 && mktemp -d)"
        printf '1 %s\n' "$(head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n')" > "$authz_tmp/signing.key"
        head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n' > "$authz_tmp/admin.token"
        kubectl -n autumn create secret generic autumn-authz \
            --from-file=signing.key="$authz_tmp/signing.key" \
            --from-file=admin.token="$authz_tmp/admin.token"
        rm -rf "$authz_tmp"
    fi
fi

echo ">>> deploying image: ${image}"
kubectl kustomize "$here" \
    | sed -E "s#image: autumn-rs:latest#image: ${image}#g" \
    | kubectl apply -f -

if [[ "${1:-}" == "--restart" ]]; then
    # Stop-the-world restart: every role except etcd. WIRE-version bumps require
    # this (rkyv has no cross-version compat); it also forces a full recovery,
    # which is what you measure PS reopen time with.
    echo ">>> $(date +%T) stop-the-world restart (all roles except etcd)"
    kubectl delete pod -n autumn -l 'component!=etcd'
    kubectl get pods -n autumn -w
fi
