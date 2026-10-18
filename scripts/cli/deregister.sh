#!/bin/bash
# geodineum topology deregister — remove an entity from the one topology.
#
#   sudo geodineum topology deregister <id>                   the entity, its indexes, edges
#                                                             and placements (cascade)
#   sudo geodineum topology deregister --shape <shape> --confirm
#                                                             every entity of one shape
#                                                             (site|app|daemon|component|node)
#
# Wraps FCALL GNODE_TOPO_DEREGISTER on {<c>}:gnode:constellation: by id, the served
# constellation that holds the entity (or the one --constellation names). Under one key a
# blanket "--all" would empty the constellation, so removal is by id or by one
# shape with an explicit --confirm. Reads the admin credential → requires_sudo.
set -uo pipefail

PORT="${VALKEY_PORT:-47445}"
CRED_DIR="${GEODINEUM_CREDENTIALS_DIR:-/etc/geodineum/credentials}"
source "$(dirname "${BASH_SOURCE[0]}")/constellation.sh"
NS="$(gnode_cli_constellation)"
TK="{${NS}}:gnode:constellation"

AUTH=""
for f in "$CRED_DIR/valkey_admin.password" "$CRED_DIR/valkey.password"; do
    [[ -r "$f" ]] && { AUTH="$(cat "$f")"; break; }
done
[[ -n "$AUTH" ]] || { echo "deregister: cannot read admin credential under $CRED_DIR — run with sudo" >&2; exit 1; }
vc() { REDISCLI_AUTH="$AUTH" redis-cli -p "$PORT" "$@"; }

dereg_one() { vc FCALL GNODE_TOPO_DEREGISTER 1 "$TK" "$1"; }

case "${1:-}" in
    ""|-h|--help)
        echo "Usage: geodineum topology deregister <id> | --shape <shape> --confirm" >&2
        exit 1 ;;
    --shape)
        shape="${2:-}"; confirm="${3:-}"
        declare -A keys=([site]=3.1 [app]=3.2 [daemon]=3.3 [component]=2 [node]=1)
        sk="${keys[$shape]:-$shape}"
        [[ "$confirm" == "--confirm" ]] || { echo "deregister: --shape removes every $shape; add --confirm" >&2; exit 2; }
        n=0
        while IFS=$'\t' read -r id sh; do
            [[ "$sh" == "$sk" ]] || continue
            echo "  - $id"
            dereg_one "$id" >/dev/null
            n=$((n + 1))
        done < <(vc --raw HGETALL "${TK}:entities" | python3 -c '
import json, sys
l = sys.stdin.read().split("\n")
for i in range(0, len(l) - 1, 2):
    try: print("%s\t%s" % (l[i], json.loads(l[i + 1]).get("sh", "")))
    except ValueError: pass')
        echo "deregistered $n entit$([[ $n -eq 1 ]] && echo y || echo ies) of shape $shape"
        ;;
    *)
        # By id: the constellation that holds it (--constellation names one explicitly).
        if [[ -n "${GNODE_TOPOLOGY_NAMESPACE:-}" ]]; then dereg_one "$1"; exit $?; fi
        found=0
        while IFS= read -r c; do
            [[ "$(vc --raw HEXISTS "{$c}:gnode:constellation:entities" "$1")" == 1 ]] || continue
            found=1
            TK="{$c}:gnode:constellation" dereg_one "$1" || exit 1
        done < <(gnode_cli_constellations)
        [[ $found -eq 1 ]] || { echo "deregister: no entity '$1' in $(gnode_cli_constellations | tr '\n' ' ')" >&2; exit 1; }
        ;;
esac
