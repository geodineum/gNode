#!/bin/bash
# on aesir, no sudo: the real daemon on a throwaway ValKey, one tenant against another through its own stream.
#   bash tests/tenant-guard/attack.sh <daemon binary>   (old binary: FAILs are the hole; this branch: ALL PASSED)
BIN=${1:?daemon binary}; TREE=$(cd "$(dirname "$0")/../.." && pwd); PORT=${2:-6395}
W=$(mktemp -d /dev/shm/gn.XXXX); chmod 0750 "$W"; umask 077
vk() { valkey-cli -p $PORT "$@"; }
valkey-server --port $PORT --save '' --appendonly no --bind 127.0.0.1 --daemonize yes --dir "$W" --logfile "$W/v.log" >/dev/null; sleep 0.4
mkdir -p "$W/creds"; head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n' > "$W/creds/valkey_daemon.password"; cp "$W/creds/valkey_daemon.password" "$W/creds/valkey.password"
vk acl setuser gnode_daemon on ">$(<"$W/creds/valkey_daemon.password")" '~*' '&*' +@all >/dev/null
printf 'VALKEY_HOST=127.0.0.1\nVALKEY_PORT=%s\nVALKEY_CREDS_PATH=%s\n' $PORT "$W/creds" > "$W/boot.env"; chmod 600 "$W/boot.env"
for f in "$TREE"/daemon/functions/*.lua; do vk -x function load replace < "$f" >/dev/null; done
for s in geodineum site_1 site_2; do vk sadd gnode:sites:registry $s >/dev/null; vk xadd "{$s}:gnode:unified:production" '*' t init >/dev/null; vk xadd "{$s}:gnode:health" '*' t init >/dev/null; done
( cd "$TREE" && exec env GEODINEUM_BOOTSTRAP_DEV=1 GEODINEUM_BOOTSTRAP_FILE="$W/boot.env" VALKEY_HOST=127.0.0.1 VALKEY_PORT=$PORT VALKEY_USER=gnode_daemon GNODE_REDIS_AUTH_FILE="$W/creds/valkey_daemon.password" GEODINEUM_CREDENTIALS_DIR="$W/creds" GNODE_FUNCTIONS_DIR="$TREE/daemon/functions" GNODE_TOPOLOGY_SCHEMA="$TREE/daemon/config/topology_schema.yaml" GNODE_SCHEMAS_DIR="$TREE/config/schemas" GNODE_CONFIG_SCHEMA_FILE="$TREE/daemon/config/config_schema.yaml" GNODE_EXT_DIR="$HOME/gh/pro/gNode" GNODE_TOPOLOGY_NAMESPACE=geodineum GNODE_NODE_CONFIG_DIR="$W/nodes" GNODE_LOG_LEVEL=warn "$BIN" --node-id bench --node-type general start ) >"$W/d.log" 2>&1 &
D=$!
for _ in $(seq 90); do [[ $(vk xinfo groups "{site_1}:gnode:unified:production" 2>/dev/null | wc -l) -gt 1 ]] && break; sleep 1; done; sleep 2
timeout 150 php "$TREE/tests/tenant-guard/attack.php" $PORT; RC=$?
kill $D; wait $D 2>/dev/null; vk shutdown nosave >/dev/null 2>&1; rm -rf "$W"
exit ${RC:-1}
