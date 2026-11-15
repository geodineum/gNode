#!/bin/bash
# on any dev box, no sudo: the daemon on a throwaway ValKey reads an announced site's stream within
# a second, and an unannounced one's at its next discovery poll (within ~60 s).
#   bash tests/broadcast-refresh/run.sh <daemon binary> [port]
set -uo pipefail
TREE=$(cd "$(dirname "$0")/../.." && pwd); BIN=${1:?daemon binary}; PORT=${2:-47992}
W=$(mktemp -d); chmod 0750 "$W"; umask 077
fail=0; D=""
cleanup() { [[ -n "$D" ]] && kill "$D" 2>/dev/null; wait 2>/dev/null
            valkey-cli -p "$PORT" --no-auth-warning shutdown nosave >/dev/null 2>&1; [[ -n "${KEEP:-}" ]] && echo "kept $W" || rm -rf "$W"; }
trap cleanup EXIT
check() { if [[ "$2" == "$3" ]]; then echo "  ok   $1"; else echo "  FAIL $1: expected '$3', got '$2'"; fail=1; fi; }
vk() { valkey-cli -p "$PORT" --no-auth-warning --raw "$@"; }
ms() { date +%s%3N; }
# A reader, not a declaration: the worker's consumer exists only once it has read the stream.
subscribed() { [[ -n "$(vk XINFO CONSUMERS "{$1}:gnode:unified:production" gnode-workers 2>/dev/null | grep -x 'gnode-d1')" ]]; }
provision() { vk FCALL GNODE_PROVISION_SERVICE 0 "$1" '["production"]' geodineum '' >/dev/null; }
announce() { vk XADD '{geodineum}:gnode:broadcast:global' MAXLEN '~' 1000 '*' type site_registered \
    data "{\"site_id\":\"$1\",\"environment\":\"production\",\"constellation\":\"geodineum\",\"ts\":$(date +%s)}" >/dev/null; }
wait_sub() { local site=$1 limit_ms=$2 t0; t0=$(ms)
    while (( $(ms) - t0 < limit_ms )); do subscribed "$site" && { echo $(( $(ms) - t0 )); return 0; }; sleep 0.02; done; echo none; }

valkey-server --port "$PORT" --bind 127.0.0.1 --save '' --appendonly no --daemonize yes --dir "$W" --logfile "$W/v.log" >/dev/null
for _ in $(seq 20); do vk ping >/dev/null 2>&1 && break; sleep 0.2; done
for f in "$TREE"/daemon/functions/gnode_*.lua; do [[ "$f" == *gnode_test* ]] || vk -x FUNCTION LOAD REPLACE < "$f" >/dev/null; done
vk ACL SETUSER gnode_daemon on '>pw' resetkeys '~*' resetchannels '&*' +@all -@dangerous >/dev/null
mkdir -p "$W/creds" "$W/nodes"; printf pw > "$W/creds/valkey_daemon.password"
printf 'VALKEY_HOST=127.0.0.1\nVALKEY_PORT=%s\nVALKEY_CREDS_PATH=%s\n' "$PORT" "$W/creds" > "$W/boot.env"

( cd "$TREE" && exec env -i PATH="$PATH" HOME="$HOME" GEODINEUM_BOOTSTRAP_DEV=1 GEODINEUM_BOOTSTRAP_FILE="$W/boot.env" \
    VALKEY_HOST=127.0.0.1 VALKEY_PORT="$PORT" VALKEY_USER=gnode_daemon VALKEY_PASSWORD_FILE="$W/creds/valkey_daemon.password" \
    GNODE_REDIS_AUTH_FILE="$W/creds/valkey_daemon.password" GEODINEUM_CREDENTIALS_DIR="$W/creds" GNODE_RECEIPT_KEY_FILE="$W/receipt_signing.key" \
    GNODE_FUNCTIONS_DIR="$TREE/daemon/functions" \
    GNODE_TOPOLOGY_SCHEMA="$TREE/daemon/config/topology_schema.yaml" GNODE_SCHEMAS_DIR="$TREE/config/schemas" \
    GNODE_CONFIG_SCHEMA_FILE="$TREE/daemon/config/config_schema.yaml" GNODE_EXT_DIR="$W/noext" \
    GNODE_TOPOLOGY_NAMESPACE=geodineum GNODE_CONSTELLATIONS=geodineum GNODE_NODE_CONFIG_DIR="$W/nodes" \
    GNODE_NODE_ID=d1 GNODE_LOG_LEVEL=info "$BIN" --node-id d1 --node-type general start ) > "$W/d.log" 2>&1 & D=$!
for _ in $(seq 120); do grep -q 'Broadcast reader worker thread started' "$W/d.log" && grep -q 'Consumer groups initialized' "$W/d.log" && break; sleep 0.5; done
sleep 2
check "daemon up, broadcast reader started" "$(kill -0 "$D" 2>/dev/null && grep -c 'Broadcast reader worker thread started' "$W/d.log")" 1

echo "Announced: provision, then site_registered on {geodineum}:gnode:broadcast:global"
provision a_com; announce a_com
t=$(wait_sub a_com 1000)
check "subscribed within 1 s" "$([[ "$t" != none ]] && echo yes)" yes
echo "       (after ${t} ms)"
check "one re-read named the event" "$(grep -c 'Stream discovery refreshed on broadcast (site_registered a_com)' "$W/d.log")" 1

echo "Lost announcement: provision only; the 60 s poll catches it"
provision b_com
t0=$(ms)
check "not subscribed after 2 s (no announcement)" "$(wait_sub b_com 2000)" none
t=$(wait_sub b_com 75000)
check "subscribed by the poll within 75 s" "$([[ "$t" != none ]] && echo yes)" yes
echo "       (after $(( $(ms) - t0 )) ms)"

check "still running" "$(kill -0 "$D" 2>/dev/null && echo yes)" yes
echo "  log: $(grep -c ' WARN ' "$W/d.log") WARN, $(grep -c ' ERROR ' "$W/d.log") ERROR"
grep -E ' (WARN|ERROR) ' "$W/d.log" | sed 's/^/    /' | head -10
[[ $fail == 0 ]] && echo "ALL PASSED" || echo "FAILED"
exit $fail
