#!/usr/bin/env bash
# LIVE check: a cluster whose etcd requires mutual TLS and a password, with JWT
# tokens that expire every 5 s.
#
# 1. A wrong password and an untrusted CA each stop the manager at start.
# 2. Manager + 3 ENs + PS come up, a key is written and read back, the
#    cluster runs on for 30 s (a token expires ~6 times; every refusal is
#    re-authenticated), the manager restarts (replays from etcd) and the key
#    reads back again.
#
# Expected tail:
#     wrong password: rc=1 ... authentication failed, invalid user ID or password
#     untrusted CA:   rc=1 ... invalid peer certificate: UnknownIssuer
#     PASS: key read back after the manager restart; re-authentications: N (N > 0)
#
#   cargo build --workspace
#   bash scripts/etcd_tls_auth.sh
set -u
cd "$(dirname "${BASH_SOURCE[0]}")/.." || exit 1
BIN=$PWD/target/debug
W=/tmp/etcd-tls-auth; PB=24000; SECRET="$W/cluster.secret"
MGR="127.0.0.1:$((PB+1))"; PS=$((PB+201))
ETCD=$((PB+401)); ETCD_PEER=$((PB+402)); ETCD_PLAIN=$((PB+403))
AO="$BIN/autumn-op --cluster-secret-file $SECRET --manager $MGR"
rm -rf "$W"; mkdir -p "$W/pki"
( umask 077; "$BIN/autumn-op" gen-cluster-secret >"$SECRET" ) || { echo FAIL-secret; exit 1; }
# Only this script's own processes are stopped. The work dir stays after a
# failure (logs) and is removed after a PASS.
PIDS=(); cleanup(){ for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done
  for p in "${PIDS[@]:-}"; do wait "$p" 2>/dev/null; done; }
trap cleanup EXIT
wp(){ for _ in $(seq 1 30); do ss -ltn 2>/dev/null | grep -q ":$1\b" && return 0; sleep 1; done; return 1; }

# --- PKI: a CA, etcd's certificate (localhost + 127.0.0.1), a client
# certificate, an unrelated CA, and etcd's JWT signing key.
cd "$W/pki"
ossl(){ openssl "$@" >/dev/null 2>&1 || { echo "FAIL-openssl $1"; exit 1; }; }
ossl req -x509 -newkey rsa:2048 -nodes -keyout ca-key.pem -out ca.pem -days 2 -subj /CN=test-ca
ossl req -x509 -newkey rsa:2048 -nodes -keyout other-key.pem -out other-ca.pem -days 2 -subj /CN=other
printf 'subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth,clientAuth\n' >server.ext
printf 'extendedKeyUsage=clientAuth\n' >client.ext
ossl req -newkey rsa:2048 -nodes -keyout server-key.pem -out server.csr -subj /CN=etcd
ossl x509 -req -in server.csr -CA ca.pem -CAkey ca-key.pem -CAcreateserial -out server.pem -days 2 -extfile server.ext
ossl req -newkey rsa:2048 -nodes -keyout client-key.pem -out client.csr -subj /CN=autumn-manager
ossl x509 -req -in client.csr -CA ca.pem -CAkey ca-key.pem -CAcreateserial -out client.pem -days 2 -extfile client.ext
ossl genrsa -out jwt.key 2048
ossl rsa -in jwt.key -pubout -out jwt.pub
cd - >/dev/null
printf 'r00t-pw\n' >"$W/etcd-pass"; printf 'wrong\n' >"$W/bad-pass"

# --- etcd: clients on TLS (certificates required); a cleartext port only for
# this script's curl, auth applies there too.
etcd --name tls --data-dir "$W/etcd" --log-level warn \
  --listen-client-urls "https://127.0.0.1:$ETCD,http://127.0.0.1:$ETCD_PLAIN" \
  --advertise-client-urls "https://127.0.0.1:$ETCD" \
  --listen-peer-urls "http://127.0.0.1:$ETCD_PEER" --initial-advertise-peer-urls "http://127.0.0.1:$ETCD_PEER" \
  --initial-cluster "tls=http://127.0.0.1:$ETCD_PEER" \
  --cert-file "$W/pki/server.pem" --key-file "$W/pki/server-key.pem" \
  --trusted-ca-file "$W/pki/ca.pem" --client-cert-auth \
  --auth-token "jwt,pub-key=$W/pki/jwt.pub,priv-key=$W/pki/jwt.key,sign-method=RS256,ttl=5s" \
  >"$W/etcd.log" 2>&1 & PIDS+=($!)
wp $ETCD_PLAIN || { echo FAIL-etcd; tail -5 "$W/etcd.log"; exit 1; }
E="curl -s http://127.0.0.1:$ETCD_PLAIN/v3"
$E/auth/user/add -d '{"name":"root","password":"r00t-pw"}' >/dev/null
$E/auth/user/grant -d '{"user":"root","role":"root"}' >/dev/null
$E/auth/enable -d '{}' | grep -q header || { echo FAIL-auth-enable; exit 1; }
$E/kv/put -d '{"key":"eA==","value":"eQ=="}' | grep -q 'user name is empty' \
  || { echo "FAIL: etcd auth is not enforced"; exit 1; }

mgr_args(){ # $1 = password file, $2 = CA file
  MA=(--port $((PB+1)) --listen 127.0.0.1 --cluster-secret-file "$SECRET"
      --manager-id 1 --etcd "localhost:$ETCD"
      --etcd-user root --etcd-password-file "$1"
      --etcd-cacert "$2" --etcd-cert "$W/pki/client.pem" --etcd-key "$W/pki/client-key.pem")
}
refused(){ # $1 = label, $2 = password file, $3 = CA file
  mgr_args "$2" "$3"
  timeout 20 "$BIN/autumn-manager-server" "${MA[@]}" >"$W/refused.log" 2>&1; local rc=$?
  echo "$1: rc=$rc $(sed -E 's/\x1b\[[0-9;]*m//g' "$W/refused.log" | grep -A1 'Caused by' | tail -1 | grep -oE '[^:]+: [^:]+$')"
  (( rc == 1 )) || { echo "FAIL: $1 did not stop the manager at start"; exit 1; }
}
refused "wrong password" "$W/bad-pass" "$W/pki/ca.pem"
refused "untrusted CA  " "$W/etcd-pass" "$W/pki/other-ca.pem"

start_manager(){
  mgr_args "$W/etcd-pass" "$W/pki/ca.pem"
  "$BIN/autumn-manager-server" "${MA[@]}" >>"$W/mgr.log" 2>&1 & MGR_PID=$!; PIDS+=($MGR_PID)
}
start_manager
wp $((PB+1)) || { echo FAIL-mgr; tail -5 "$W/mgr.log"; exit 1; }
for i in 1 2 3; do
  mkdir -p "$W/en$i"
  $AO format "$W/en$i" >"$W/fmt$i.log" 2>&1 || { echo "FAIL-format$i"; cat "$W/fmt$i.log"; exit 1; }
  "$BIN/autumn-extent-node" --cluster-secret-file "$SECRET" \
    --data "$W/en$i" --port $((PB+100+i)) --manager "$MGR" --cpuset $((i-1)) \
    --advertise "127.0.0.1:$((PB+100+i))" --listen 127.0.0.1 >"$W/en$i.log" 2>&1 & PIDS+=($!)
  wp $((PB+100+i)) || { echo "FAIL-en$i"; tail -5 "$W/en$i.log"; exit 1; }
done
sleep 4
$AO bootstrap --replication 3+0 >"$W/boot.log" 2>&1 || { echo FAIL-boot; cat "$W/boot.log"; exit 1; }
"$BIN/autumn-ps" --cluster-secret-file "$SECRET" --psid 1 --port $PS --manager "$MGR" \
  --listen 127.0.0.1 --advertise "127.0.0.1:$PS" >"$W/ps.log" 2>&1 & PIDS+=($!)
wp $PS || { echo FAIL-ps; tail -5 "$W/ps.log"; exit 1; }
sleep 5
$AO namespace-create --name tls >"$W/ns.log" 2>&1 || { echo FAIL-ns; cat "$W/ns.log"; exit 1; }
AC="$BIN/autumn-client --manager $MGR --namespace tls"
head -c 65536 /dev/urandom >"$W/value"
$AC put k1 "$W/value" >"$W/put.log" 2>&1 || { echo FAIL-put; cat "$W/put.log"; exit 1; }
$AC get k1 >"$W/got1" 2>/dev/null && cmp -s "$W/value" "$W/got1" || { echo FAIL-get; exit 1; }
echo "written and read back; running 30 s on expiring tokens"
sleep 30
kill -0 $MGR_PID 2>/dev/null || { echo "FAIL: manager died"; tail -20 "$W/mgr.log"; exit 1; }

kill $MGR_PID; wait $MGR_PID 2>/dev/null
P=(); for p in "${PIDS[@]}"; do [[ $p == "$MGR_PID" ]] || P+=("$p"); done; PIDS=("${P[@]}")
start_manager
for _ in $(seq 1 60); do $AO info >/dev/null 2>&1 && break; sleep 1; done
$AO info >/dev/null 2>&1 || { echo "FAIL: restarted manager never led"; tail -20 "$W/mgr.log"; exit 1; }
$AC put k2 "$W/value" >"$W/put2.log" 2>&1 || { echo FAIL-put-after-restart; cat "$W/put2.log"; exit 1; }
$AC get k1 >"$W/got2" 2>/dev/null && cmp -s "$W/value" "$W/got2" || { echo FAIL-get-after-restart; exit 1; }
N=$(grep -c 'authenticating again' "$W/mgr.log")
echo "manager WARN/ERROR lines:"
sed -E 's/\x1b\[[0-9;]*m//g' "$W/mgr.log" | grep -E ' (WARN|ERROR) ' | cut -c29-170 | sort | uniq -c | sort -rn | head
(( N > 0 )) || { echo "FAIL: no token expired in 30 s (re-authentications: 0)"; exit 1; }
echo "PASS: key read back after the manager restart; re-authentications: $N"
cleanup; trap - EXIT; rm -rf "$W"
