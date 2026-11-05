#!/bin/bash
# on aesir, no sudo: one daemon binary at a time on a throwaway ValKey with N sites: round trip of a
# money command in the client's own pattern, ValKey ops per second and daemon CPU while idle.
#   bash tests/pickup-latency/compare.sh <old-binary> <new-binary> [sites=16] [port=6394]
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd); TREE=$(cd "$HERE/../.." && pwd)
OLD=$1; NEW=$2; SITES=${3:-16}; PORT=${4:-6394}
W=$(mktemp -d); chmod 0750 "$W"; umask 077
vk() { valkey-cli -p "$PORT" "$@"; }
cleanup() { [[ -n ${DP:-} ]] && kill "$DP" 2>/dev/null; vk shutdown nosave >/dev/null 2>&1; rm -rf "$W"; }
trap cleanup EXIT
measure() { # <label> <binary>
    local label=$1 bin=$2
    vk flushall >/dev/null; vk function flush >/dev/null
    for f in "$TREE"/daemon/functions/*.lua; do vk -x function load replace < "$f" >/dev/null; done
    for ((i=0;i<SITES;i++)); do s=site_$i; vk sadd gnode:sites:registry "$s" >/dev/null
        vk xadd "{$s}:gnode:unified:production" '*' t init >/dev/null; vk xadd "{$s}:gnode:health" '*' t init >/dev/null; done
    ( cd "$TREE" && exec env GEODINEUM_BOOTSTRAP_DEV=1 GEODINEUM_BOOTSTRAP_FILE="$W/boot.env" VALKEY_HOST=127.0.0.1 VALKEY_PORT="$PORT" VALKEY_USER=gnode_daemon \
        GNODE_REDIS_AUTH_FILE="$W/creds/valkey_daemon.password" GEODINEUM_CREDENTIALS_DIR="$W/creds" \
        GNODE_FUNCTIONS_DIR="$TREE/daemon/functions" GNODE_TOPOLOGY_SCHEMA="$TREE/daemon/config/topology_schema.yaml" GNODE_SCHEMAS_DIR="$TREE/config/schemas" \
        GNODE_CONFIG_SCHEMA_FILE="$TREE/daemon/config/config_schema.yaml" GNODE_EXT_DIR="${GNODE_EXT_DIR:-$HOME/gh/pro/gNode}" GNODE_TOPOLOGY_NAMESPACE=geodineum \
        GNODE_NODE_CONFIG_DIR="$W/nodes" GNODE_LOG_LEVEL=warn "$bin" --node-id bench --node-type general start ) >"$W/$label.log" 2>&1 &
    DP=$!
    for _ in $(seq 100); do [[ $(vk xinfo groups "{site_0}:gnode:unified:production" 2>/dev/null | wc -l) -gt 1 ]] && break; sleep 1; done
    sleep 3
    echo "== $label ($(basename "$(dirname "$bin")")) consumers on site_0: $(vk xinfo groups '{site_0}:gnode:unified:production' 2>&1 | sed -n 2p)"
    php -d display_errors=1 "$HERE/driver.php" "$PORT" site_3 200
    vk config resetstat >/dev/null; a=$(awk '{print $14+$15}' "/proc/$DP/stat"); sleep 20; b=$(awk '{print $14+$15}' "/proc/$DP/stat")
    echo "idle 20 s: valkey ops/s $(vk info stats | awk -F: '/instantaneous_ops_per_sec/{print $2}' | tr -d '\r'), total $(( $(vk info stats | awk -F: '/total_commands_processed/{print $2}' | tr -d '\r') / 20 ))/s, daemon cpu $(( (b-a) )) ticks of 2000"
    php "$HERE/driver.php" "$PORT" site_15 200 2>/dev/null
    kill "$DP" 2>/dev/null; wait "$DP" 2>/dev/null; DP=
    tail -3 "$W/$label.log" | cut -c1-160
}
valkey-server --port "$PORT" --save '' --appendonly no --bind 127.0.0.1 --daemonize yes --dir "$W" --logfile "$W/v.log" >/dev/null
for _ in $(seq 20); do vk ping >/dev/null 2>&1 && break; sleep 0.3; done
mkdir -p "$W/creds"; head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n' > "$W/creds/valkey_daemon.password"; cp "$W/creds/valkey_daemon.password" "$W/creds/valkey.password"
vk acl setuser gnode_daemon on ">$(<"$W/creds/valkey_daemon.password")" '~*' '&*' +@all >/dev/null
printf 'VALKEY_HOST=127.0.0.1\nVALKEY_PORT=%s\nVALKEY_CREDS_PATH=%s\n' "$PORT" "$W/creds" > "$W/boot.env"; chmod 600 "$W/boot.env"
start_one() { # <id> <binary>: a second competing consumer, as squad is
    ( cd "$TREE" && exec env GEODINEUM_BOOTSTRAP_DEV=1 GEODINEUM_BOOTSTRAP_FILE="$W/boot.env" VALKEY_HOST=127.0.0.1 VALKEY_PORT="$PORT" VALKEY_USER=gnode_daemon \
        GNODE_REDIS_AUTH_FILE="$W/creds/valkey_daemon.password" GEODINEUM_CREDENTIALS_DIR="$W/creds" \
        GNODE_FUNCTIONS_DIR="$TREE/daemon/functions" GNODE_TOPOLOGY_SCHEMA="$TREE/daemon/config/topology_schema.yaml" GNODE_SCHEMAS_DIR="$TREE/config/schemas" \
        GNODE_CONFIG_SCHEMA_FILE="$TREE/daemon/config/config_schema.yaml" GNODE_EXT_DIR="${GNODE_EXT_DIR:-$HOME/gh/pro/gNode}" GNODE_TOPOLOGY_NAMESPACE=geodineum \
        GNODE_NODE_CONFIG_DIR="$W/nodes" GNODE_LOG_LEVEL=warn "$2" --node-id "$1" --node-type general start ) >"$W/$1.log" 2>&1 &
    echo $!
}
pair() { # <label> <binary a> <binary b>
    vk flushall >/dev/null; vk function flush >/dev/null
    for f in "$TREE"/daemon/functions/*.lua; do vk -x function load replace < "$f" >/dev/null; done
    for ((i=0;i<SITES;i++)); do s=site_$i; vk sadd gnode:sites:registry "$s" >/dev/null
        vk xadd "{$s}:gnode:unified:production" '*' t init >/dev/null; vk xadd "{$s}:gnode:health" '*' t init >/dev/null; done
    A=$(start_one aa "$2"); B=$(start_one bb "$3")
    for _ in $(seq 100); do [[ $(vk xinfo consumers "{site_0}:gnode:unified:production" gnode-daemon 2>/dev/null | grep -c '^daemon-') -ge 2 ]] && break; sleep 1; done
    sleep 3
    echo "== $1: consumers $(vk xinfo consumers '{site_0}:gnode:unified:production' gnode-daemon 2>&1 | grep -c '^daemon-')"
    php "$HERE/driver.php" "$PORT" site_7 300 2>/dev/null
    php "$HERE/driver.php" "$PORT" site_1 100 2>/dev/null & P1=$!; php "$HERE/driver.php" "$PORT" site_2 100 2>/dev/null & P2=$!; php "$HERE/driver.php" "$PORT" site_9 100 2>/dev/null; wait $P1 $P2
    echo "pending on site_7: $(vk xpending '{site_7}:gnode:unified:production' gnode-daemon | head -1)"
    vk xinfo consumers "{site_7}:gnode:unified:production" gnode-daemon | paste -sd" " | cut -c1-200
    kill "$A" "$B" 2>/dev/null; wait "$A" "$B" 2>/dev/null
}
if [[ ${PAIRS:-} == 1 ]]; then pair "new+new" "$NEW" "$NEW"; pair "old+new (rollout)" "$OLD" "$NEW"; exit 0; fi
measure old "$OLD"; measure new "$NEW"
