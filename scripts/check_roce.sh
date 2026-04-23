#!/usr/bin/env bash
# check_roce.sh — quick RoCEv2 sanity check for autumn-rs UCX transport (F100-UCX).
#
# Walks every InfiniBand-class device in /sys/class/infiniband/ and reports:
#   - port state + link layer (Ethernet = RoCE, InfiniBand = IB)
#   - GID table entries with type RoCE v2 + bound netdev with a routable IP
#   - whether any RoCE-attached netdev has an IPv4/IPv6 address ucp_listener can bind
#
# Exit code:
#   0 — at least one HCA has a RoCE v2 GID on a netdev with a non-link-local IP
#   1 — no usable RoCEv2 endpoint found (autumn-rs UCX mode would fall back / fail)
#   2 — sysfs not present (no RDMA stack at all)
#
# Usage:
#   scripts/check_roce.sh                   # human-readable output, exit code as above
#   scripts/check_roce.sh --quiet           # exit code only, no stdout
#   scripts/check_roce.sh --listen-candidates   # print "<dev> <ip>" pairs only

set -u

QUIET=0
LIST_ONLY=0
for arg in "$@"; do
  case "$arg" in
    --quiet)              QUIET=1 ;;
    --listen-candidates)  LIST_ONLY=1; QUIET=1 ;;
    -h|--help)
      sed -n '2,20p' "$0"; exit 0 ;;
    *) echo "unknown arg: $arg" >&2; exit 64 ;;
  esac
done

log() { [ "$QUIET" = 1 ] || echo "$@"; }

IB_ROOT=/sys/class/infiniband
[ -d "$IB_ROOT" ] || { log "no $IB_ROOT — kernel has no RDMA stack"; exit 2; }

usable=0
declare -a candidates=()

for dev_path in "$IB_ROOT"/*; do
  [ -d "$dev_path" ] || continue
  dev=$(basename "$dev_path")

  for port_path in "$dev_path"/ports/*; do
    [ -d "$port_path" ] || continue
    port=$(basename "$port_path")
    state=$(cat "$port_path/state" 2>/dev/null || echo unknown)
    link=$(cat "$port_path/link_layer" 2>/dev/null || echo unknown)

    log "=== $dev port $port ==="
    log "    state=$state  link_layer=$link"

    [[ "$state" == *ACTIVE* ]] || { log "    SKIP: port not active"; continue; }

    for i in $(seq 0 31); do
      gid_file=$port_path/gids/$i
      type_file=$port_path/gid_attrs/types/$i
      ndev_file=$port_path/gid_attrs/ndevs/$i
      [ -r "$gid_file" ] || continue
      gid=$(cat "$gid_file" 2>/dev/null)
      # Skip empty GID slots (all zeros)
      [ "${gid//[0:]/}" = "" ] && continue
      type=$(cat "$type_file" 2>/dev/null || echo "")
      ndev=$(cat "$ndev_file" 2>/dev/null || echo "")

      # Only RoCE v2 entries are useful for autumn-rs UCX deployment
      [ "$type" = "RoCE v2" ] || continue

      # Skip link-local fe80:: GIDs — not routable, not usable for ucp_listener bind
      if [[ "$gid" == fe80:* ]]; then
        log "    gid[$i] $type -> $ndev $gid  (link-local, skip)"
        continue
      fi

      # The GID byte pattern often equals one of the netdev IPs but in
      # expanded form (sysfs) vs canonical form (ip addr). Rather than
      # normalize, just verify the netdev has at least one routable IP
      # — that's enough for ucp_listener bind via rdmacm.
      routable_ip=""
      if [ -n "$ndev" ]; then
        while read -r addr _; do
          [ -z "$addr" ] && continue
          ip_only=${addr%%/*}
          # Skip link-local (fe80::), unspecified (::), loopback, docker
          case "$ip_only" in
            fe80:*|::|::1|127.*|169.254.*) continue ;;
          esac
          routable_ip=$ip_only; break
        done < <(ip -o addr show dev "$ndev" 2>/dev/null | awk '{print $4}')
      fi

      if [ -n "$routable_ip" ]; then
        log "    gid[$i] $type -> $ndev $gid  ✓ usable (bind ip=$routable_ip)"
        usable=$((usable+1))
        candidates+=("$ndev $routable_ip")
      else
        log "    gid[$i] $type -> $ndev $gid  (no routable IP on $ndev)"
      fi
    done
  done
done

if [ "$LIST_ONLY" = 1 ]; then
  for c in "${candidates[@]+"${candidates[@]}"}"; do echo "$c"; done
  [ "$usable" -gt 0 ] && exit 0 || exit 1
fi

log ""
log "summary: $usable usable RoCEv2 GID(s)"
if [ "$usable" -gt 0 ]; then
  log "autumn-rs F100-UCX: AUTUMN_TRANSPORT=ucx is viable on this host"
  exit 0
else
  log "autumn-rs F100-UCX: no usable RoCEv2 endpoint — UCX mode will fall back / fail"
  exit 1
fi
