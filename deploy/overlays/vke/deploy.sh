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
