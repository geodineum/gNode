#!/bin/bash
# on any dev box, no sudo: the joined-node tier and the client command set on a throwaway ValKey.
#   bash tests/acl-tiers/run.sh [daemon binary] [port]
# With a daemon binary the daemon also starts as a joined node under the node tier
# (GNODE_EXT_DIR, when set, is the extension directory it serves).
set -uo pipefail
TREE=$(cd "$(dirname "$0")/../.." && pwd); BIN=${1:-}; PORT=${2:-47983}
# shellcheck source=../../scripts/acl-tiers.sh
source "$TREE/scripts/acl-tiers.sh"
W=$(mktemp -d); chmod 0750 "$W"; umask 077
fail=0; D=""
cleanup() { [[ -n "$D" ]] && kill "$D" 2>/dev/null; wait 2>/dev/null
            valkey-cli -p "$PORT" --no-auth-warning shutdown nosave >/dev/null 2>&1; [[ -n "${KEEP:-}" ]] && echo "kept $W" || rm -rf "$W"; }
trap cleanup EXIT
check() { if [[ "$2" == "$3" ]]; then echo "  ok   $1"; else echo "  FAIL $1: expected '$3', got '$2'"; fail=1; fi; }
vk() { valkey-cli -p "$PORT" --no-auth-warning --raw "$@"; }
as() { local u=$1; shift; REDISCLI_AUTH=pw valkey-cli -p "$PORT" --no-auth-warning --raw --user "$u" "$@" 2>&1; }
verdict() { case "$1" in *NOPERM*|*"no permissions"*) echo NOPERM ;; *ERR*|*WRONGTYPE*) echo "ERR: $1" ;; *) echo OK ;; esac; }

valkey-server --port "$PORT" --bind 127.0.0.1 --save '' --appendonly no --daemonize yes --dir "$W" --logfile "$W/v.log" >/dev/null
for _ in $(seq 20); do vk ping >/dev/null 2>&1 && break; sleep 0.2; done
for f in "$TREE"/daemon/functions/gnode_*.lua; do [[ "$f" == *gnode_test* ]] || vk -x FUNCTION LOAD REPLACE < "$f" >/dev/null; done
vk FUNCTION LOAD "#!lua name=gnode_x
server.register_function('GNODE_X', function(k, a) return 'admin' end)" >/dev/null
REPLACE="#!lua name=gnode_x
server.register_function('GNODE_X', function(k, a) return 'replaced' end)"
vk SET '{other}:secret' x >/dev/null

# The daemon tier exactly as the installer writes acl-daemon-tier.rule.
printf '%s\n' 'on resetkeys ~* resetchannels &* +@all -@dangerous -function|flush -function|restore' > "$W/acl-daemon-tier.rule"
set -f
# shellcheck disable=SC2046
vk ACL SETUSER gnode_daemon resetpass '>pw' $(cat "$W/acl-daemon-tier.rule") >/dev/null
# shellcheck disable=SC2046
vk ACL SETUSER gnode_node_t resetpass '>pw' $(acl_node_tier "$W/acl-daemon-tier.rule") >/dev/null
set +f

echo "Master daemon tier (gnode_daemon): unchanged"
check "FUNCTION LOAD REPLACE allowed" "$(verdict "$(as gnode_daemon FUNCTION LOAD REPLACE "$REPLACE")")" OK
vk FUNCTION LOAD REPLACE "#!lua name=gnode_x
server.register_function('GNODE_X', function(k, a) return 'admin' end)" >/dev/null

echo "Joined-node tier (gnode_node_t): no writes to the library namespace"
check "FUNCTION LOAD REPLACE refused" "$(verdict "$(as gnode_node_t FUNCTION LOAD REPLACE "$REPLACE")")" NOPERM
check "FCALL still runs the admin's library" "$(as gnode_node_t FCALL GNODE_X 0)" admin
check "FUNCTION DELETE refused" "$(verdict "$(as gnode_node_t FUNCTION DELETE gnode_x)")" NOPERM
check "FUNCTION FLUSH refused" "$(verdict "$(as gnode_node_t FUNCTION FLUSH)")" NOPERM
check "FUNCTION RESTORE refused" "$(verdict "$(vk ACL DRYRUN gnode_node_t FUNCTION RESTORE x)")" NOPERM
check "library still loaded" "$(vk FCALL GNODE_X 0)" admin
check "FUNCTION LIST (worker verify)" "$(verdict "$(as gnode_node_t FUNCTION LIST LIBRARYNAME gnode_x)")" OK
S='{site_n}:gnode:unified:production'
check "XGROUP CREATE MKSTREAM" "$(as gnode_node_t XGROUP CREATE "$S" gnode-daemon '$' MKSTREAM)" OK
check "XADD" "$(verdict "$(as gnode_node_t XADD "$S" '*' t c c ping)")" OK
check "XREADGROUP" "$(as gnode_node_t XREADGROUP GROUP gnode-daemon n1 COUNT 1 STREAMS "$S" '>' | sed -n 1p)" "$S"
ID=$(vk XRANGE "$S" - + | sed -n 1p)
check "XACK" "$(as gnode_node_t XACK "$S" gnode-daemon "$ID")" 1
check "HSET the topology" "$(verdict "$(as gnode_node_t HSET '{geodineum}:gnode:constellation:entities' n1 '{}')")" OK
check "SET its heartbeat" "$(as gnode_node_t SET '{geodineum}:gnode:heartbeat:all:gnode-daemon:n1' 1 EX 75)" OK
check "FCALL a shipped library" "$(verdict "$(as gnode_node_t FCALL GNODE_CACHE_STATS 0 site_n)")" OK

echo "Client command set (gnode_client_svc): no SCAN unless the manifest opts in or the entity is a site"
printf 'consumes: []\nproduces: ["{svc}:gnode:comms:*"]\n' > "$W/svc.yaml"
printf 'keyspace_scan: true\nconsumes: []\nproduces: ["{scanner}:gnode:comms:*"]\n' > "$W/scanner.yaml"
printf 'keyspace_scan: false\nconsumes: []\nproduces: ["{quiet}:*"]\n' > "$W/quiet.yaml"
check "daemon manifest without the flag" "$(acl_client_scan "$W/svc.yaml" daemon)" -scan
check "app manifest without the flag" "$(acl_client_scan "$W/svc.yaml" app)" -scan
check "daemon manifest with keyspace_scan: true" "$(acl_client_scan "$W/scanner.yaml" daemon)" +scan
check "site manifest without the flag keeps SCAN" "$(acl_client_scan "$W/svc.yaml" site)" +scan
check "site manifest with keyspace_scan: false" "$(acl_client_scan "$W/quiet.yaml" site)" -scan
check "unreadable manifest, no shape" "$(acl_client_scan "$W/missing.yaml")" -scan
for id in svc scanner; do
    vk ACL SETUSER "gnode_client_$id" on '>pw' clearselectors resetkeys "~$id:*" "~{$id}:*" \
        "~{geodineum}:gnode:heartbeat:*:$id:*" '%R~{geodineum}:gnode:*' resetchannels '&*' >/dev/null
    vk ACL SETUSER "gnode_client_$id" nocommands "${ACL_CLIENT_COMMANDS[@]}" "$(acl_client_scan "$W/$id.yaml" daemon)" >/dev/null
done
C=gnode_client_svc
check "SCAN refused" "$(verdict "$(as $C SCAN 0)")" NOPERM
check "KEYS refused" "$(verdict "$(as $C KEYS '*')")" NOPERM
check "GNODE_KEYS_PATTERN (SCAN inside) refused" "$(verdict "$(as $C FCALL GNODE_KEYS_PATTERN 0 '{svc}:*' svc)")" NOPERM
check "GNODE_CACHE_STATS (SCAN in pcall) still answers" "$(verdict "$(as $C FCALL GNODE_CACHE_STATS 0 svc)")" OK
check "GET another tenant refused" "$(verdict "$(as $C GET '{other}:secret')")" NOPERM
check "XGROUP CREATE own stream" "$(as $C XGROUP CREATE '{svc}:gnode:unified:production' svc '$' MKSTREAM)" OK
check "XADD own stream" "$(verdict "$(as $C XADD '{svc}:gnode:unified:production' '*' t c)")" OK
check "XREADGROUP own stream" "$(as $C XREADGROUP GROUP svc w1 COUNT 1 STREAMS '{svc}:gnode:unified:production' '>' | sed -n 1p)" '{svc}:gnode:unified:production'
check "SET/GET own key" "$(as $C SET '{svc}:k' v)$(as $C GET '{svc}:k')" OKv
check "HSET/HGETALL own hash" "$(as $C HSET '{svc}:h' f v)$(as $C HGETALL '{svc}:h' | tr -d '\n')" 1fv
check "SET own heartbeat" "$(as $C SET '{geodineum}:gnode:heartbeat:all:app:svc:production' 1 EX 75)" OK
check "read the topology" "$(verdict "$(as $C HGETALL '{geodineum}:gnode:constellation:entities')")" OK
check "FCALL" "$(as $C FCALL GNODE_X 0)" admin
check "PING" "$(as $C PING)" PONG
check "MULTI/EXEC" "$(printf 'MULTI\nINCR {svc}:n\nEXEC\n' | REDISCLI_AUTH=pw valkey-cli -p "$PORT" --no-auth-warning --raw --user $C | tail -1)" 1
check "opted-in client may SCAN" "$(verdict "$(as gnode_client_scanner SCAN 0)")" OK

if [[ -n "$BIN" ]]; then
    echo "The daemon as a joined node under the node tier"
    mkdir -p "$W/creds" "$W/nodes"; printf pw > "$W/creds/valkey_node.password"
    if [[ -n "${GNODE_EXT_DIR:-}" ]]; then
        for f in "$GNODE_EXT_DIR"/*/functions/*.lua; do [[ -f "$f" ]] && vk -x FUNCTION LOAD REPLACE < "$f" >/dev/null; done
    fi
    printf 'VALKEY_HOST=127.0.0.1\nVALKEY_PORT=%s\nVALKEY_CREDS_PATH=%s\n' "$PORT" "$W/creds" > "$W/boot.env"
    LIBS_BEFORE=$(vk FUNCTION DUMP | sha256sum)
    start_daemon() { ( cd "$TREE" && exec env -i PATH="$PATH" HOME="$HOME" GEODINEUM_BOOTSTRAP_DEV=1 GEODINEUM_BOOTSTRAP_FILE="$W/boot.env" \
        VALKEY_HOST=127.0.0.1 VALKEY_PORT="$PORT" VALKEY_USER=gnode_node_t VALKEY_PASSWORD_FILE="$W/creds/valkey_node.password" GNODE_REDIS_AUTH_FILE="$W/creds/valkey_node.password" \
        GEODINEUM_CREDENTIALS_DIR="$W/creds" GNODE_FUNCTIONS_DIR="$TREE/daemon/functions" \
        GNODE_TOPOLOGY_SCHEMA="$TREE/daemon/config/topology_schema.yaml" GNODE_SCHEMAS_DIR="$TREE/config/schemas" \
        GNODE_CONFIG_SCHEMA_FILE="$TREE/daemon/config/config_schema.yaml" GNODE_EXT_DIR="${GNODE_EXT_DIR:-$W/noext}" \
        GNODE_TOPOLOGY_NAMESPACE=geodineum GNODE_CONSTELLATIONS=geodineum GNODE_NODE_CONFIG_DIR="$W/nodes" \
        GNODE_NODE_ID=d1 GNODE_LOG_LEVEL=info "$BIN" --node-id d1 --node-type general start ) > "$W/$1" 2>&1 & D=$!; }
    start_daemon d.log
    for _ in $(seq 120); do [[ "$(vk HEXISTS '{geodineum}:gnode:constellation:entities' d1)" == 1 && -n "$(vk GET '{geodineum}:gnode:heartbeat:all:gnode-daemon:d1')" ]] && break; sleep 0.5; done
    check "registered its node and heartbeats" "$(vk HEXISTS '{geodineum}:gnode:constellation:entities' d1)$( [[ -n "$(vk GET '{geodineum}:gnode:heartbeat:all:gnode-daemon:d1')" ]] && echo 1)" 11
    check "connected as gnode_node_t" "$(vk CLIENT LIST | grep -c 'user=gnode_node_t' | awk '{print ($1>0)?"yes":"no"}')" yes
    sleep 3
    check "still running" "$(kill -0 "$D" 2>/dev/null && echo yes)" yes
    check "no FUNCTION LOAD warning per library" "$(grep -c 'FUNCTION LOAD failed' "$W/d.log")" 0
    check "no missing-library warning" "$(grep -c 'missing extension librar\|missing .* function librar' "$W/d.log")" 0
    check "libraries untouched by the node" "$(vk FUNCTION DUMP | sha256sum)" "$LIBS_BEFORE"
    echo "  log: $(grep -c . "$W/d.log") lines, $(grep -c ' WARN ' "$W/d.log") WARN, $(grep -c ' ERROR ' "$W/d.log") ERROR; library lines:"
    grep -E 'Lua librar|FUNCTION LOAD|function librar|Function initialization' "$W/d.log" | sed 's/^/    /'
    EXT_LIBS=$(for f in "${GNODE_EXT_DIR:-$W/noext}"/*/functions/*.lua; do [[ -f "$f" ]] && basename "$f" .lua; done)
    if [[ -n "$EXT_LIBS" ]]; then
        kill "$D"; wait "$D" 2>/dev/null
        for l in $EXT_LIBS; do vk FUNCTION DELETE "$l" >/dev/null; done
        start_daemon d2.log
        for _ in $(seq 60); do grep -q 'Function initialization complete' "$W/d2.log" && break; sleep 0.5; done
        check "an extension library gone from ValKey is named once" "$(grep -c 'missing extension librar' "$W/d2.log")" 1
        check "and not loaded by the node" "$(vk FUNCTION LIST | grep -cx "$(echo "$EXT_LIBS" | head -1)")" 0
        check "still running" "$(kill -0 "$D" 2>/dev/null && echo yes)" yes
        grep 'missing extension librar' "$W/d2.log" | sed 's/^/    /'
    fi
fi

[[ $fail == 0 ]] && echo "ALL PASSED" || echo "FAILED"
exit $fail
