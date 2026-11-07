#!/usr/bin/env bash
# Real isolated cluster contract; portable, fail-fast, cleans up only its children.
set -euo pipefail
exec python3 "$(dirname "$0")/api_contract.py" "$@"
