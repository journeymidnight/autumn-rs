#!/usr/bin/env bash
# perf_ucx_baseline.sh — F100-UCX A/B perf capture (transport-level + cluster-level).
#
# TRANSPORT-LEVEL (no cluster needed; runs locally):
#   ./scripts/perf_ucx_baseline.sh transport
#
# CLUSTER-LEVEL (requires running cluster.sh first; multi-host for real RDMA):
#   bash cluster.sh start 3                                    # TCP, 127.0.0.1
#   AUTUMN_TRANSPORT=ucx bash cluster.sh start 3               # UCX, RoCE-attached
#   ./scripts/perf_ucx_baseline.sh cluster [--manager addr]
#
# Output: perf_baseline_ucx.json with TCP/UCX numbers per scenario.

set -uo pipefail

mode=${1:-transport}
out=${2:-perf_baseline_ucx.json}
manager=${MANAGER:-127.0.0.1:9001}

cd "$(dirname "$0")/.."

case "$mode" in
  transport)
    echo "=== Transport-level micro-bench (no cluster needed) ==="
    out_lines=$(cargo bench -p autumn-transport --features ucx --bench transport_bench 2>&1 \
                | grep -E "TCP|UCX")
    cat <<EOF > "$out"
{
  "kind": "transport_micro_bench",
  "host": "$(hostname)",
  "date": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "scenarios": [
$(echo "$out_lines" | sed 's/.*/    "&"/' | paste -sd ',\n')
  ]
}
EOF
    echo "wrote $out"
    cat "$out"
    ;;
  cluster)
    echo "=== Cluster-level ps_bench A/B (cluster must be running) ==="
    declare -a results=()
    for transport in tcp ucx; do
      for sz in 64 4096 65536; do
        echo "--- $transport ${sz}B ---"
        line=$(cargo bench -q -p autumn-partition-server --bench ps_bench -- \
                 --transport "$transport" --manager "$manager" \
                 --value-size "$sz" --duration 10 2>&1 \
               | grep -E "ops/s|MB/s|throughput" | tail -1 || true)
        results+=("    {\"transport\":\"$transport\",\"value_size\":$sz,\"line\":\"${line//\"/\\\"}\"}")
      done
    done
    {
      echo "{"
      echo "  \"kind\": \"cluster_ps_bench\","
      echo "  \"host\": \"$(hostname)\","
      echo "  \"date\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\","
      echo "  \"manager\": \"$manager\","
      echo "  \"scenarios\": ["
      printf "%s\n" "${results[@]}" | paste -sd ',\n'
      echo "  ]"
      echo "}"
    } > "$out"
    echo "wrote $out"
    ;;
  *)
    echo "Usage: $0 {transport|cluster} [out-file]" >&2
    exit 64
    ;;
esac
