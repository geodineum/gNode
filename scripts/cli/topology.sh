#!/bin/bash
# geodineum topology — inspect the one topology of a constellation.
#
#   sudo geodineum topology                    counts per layer + the custom topologies
#   sudo geodineum topology all                every entity, grouped by layer
#   sudo geodineum topology show <id>          one entity's stored record
#   sudo geodineum topology stats              the constellation's stats
#   sudo geodineum topology sites              registered sites
#   sudo geodineum topology load [id]          what the sampler measured: band, coordinate,
#                                              and how old the reading is (shapes with a load slot)
#   sudo geodineum topology verify             the library's own invariant check
#
# The constellation is GNODE_TOPOLOGY_NAMESPACE (default geodineum). Reads the
# admin credential (root-readable) → requires_sudo. Read-only.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PORT="${VALKEY_PORT:-47445}"
CRED_DIR="${GEODINEUM_CREDENTIALS_DIR:-/etc/geodineum/credentials}"
NS="${GNODE_TOPOLOGY_NAMESPACE:-geodineum}"
TK="{${NS}}:gnode:constellation"

AUTH=""
for f in "$CRED_DIR/valkey_admin.password" "$CRED_DIR/valkey.password"; do
    [[ -r "$f" ]] && { AUTH="$(cat "$f")"; break; }
done
if [[ -z "$AUTH" ]]; then
    echo "topology: cannot read admin credential under $CRED_DIR — run with sudo" >&2
    exit 1
fi
vc() { REDISCLI_AUTH="$AUTH" redis-cli -p "$PORT" "$@"; }

# Every entity, decoded: "<layer>\t<shape>\t<id>\t<env>\t<load>\t<health>\t<of>\t<on>"
entities() {
    vc --raw HGETALL "${TK}:entities" | python3 -c '
import json, sys
l = [x for x in sys.stdin.read().split("\n")]
names = {"1": "node", "2": "component", "3.1": "site", "3.2": "app", "3.3": "daemon", "4.1": "component@node", "4.2": "service@node"}
rows = []
for i in range(0, len(l) - 1, 2):
    try:
        e = json.loads(l[i + 1])
    except ValueError:
        continue
    sh = str(e.get("sh", "")); pd = e.get("pd") or []
    rows.append((sh, names.get(sh, sh), l[i], e.get("env", ""), pd[16] if len(pd) > 16 else "", pd[17] if len(pd) > 17 else "", e.get("of", ""), e.get("on", ""), (e.get("m") or {}).get("ds", "")))
for r in sorted(rows):
    print("\t".join(str(x) for x in r))
'
}

sub="${1:-list}"
[[ $# -gt 0 ]] && shift

case "$sub" in
    list)
        printf 'Constellation %s (%s)\n' "$NS" "$TK"
        printf '  schema version %s, %s entities\n' "$(vc --raw HGET "${TK}:meta" sv)" "$(vc --raw HLEN "${TK}:entities")"
        entities | awk -F'\t' '{ n[$2]++ } END { for (k in n) printf "  %-16s %s\n", k, n[k] }' | sort
        echo
        echo "Custom topologies (Pro):"
        found=0
        while IFS= read -r metakey; do
            [[ -n "$metakey" ]] || continue
            found=1
            base="${metakey%:meta}"
            printf '  %-48s %s entities\n' "$base" "$(vc --raw HLEN "${base}:entities")"
        done < <(vc --scan --pattern "{${NS}}:gnode:topo:*:meta")
        [[ "$found" -eq 0 ]] && echo "  (none)"
        echo
        printf 'Registered sites: %s\n' "$(vc --raw SMEMBERS gnode:sites:registry | tr '\n' ' ')"
        ;;
    all|dump)
        printf '%-16s %-30s %-12s %-8s %-8s %s\n' SHAPE ID ENV LOAD HEALTH PLACEMENT
        entities | awk -F'\t' '{ pl = ($7 != "") ? $7 "@" $8 : ""; printf "%-16s %-30s %-12s %-8s %-8s %s\n", $2, $3, $4, $5, $6, pl }'
        ;;
    show|entities)
        if [[ $# -lt 1 ]]; then echo "usage: geodineum topology show <id>" >&2; exit 2; fi
        rec="$(vc --raw HGET "${TK}:entities" "$1")"
        if [[ -n "$rec" ]]; then
            printf '%s' "$rec" | python3 -m json.tool
        else
            echo "  (no entity '$1' in $TK)"
        fi
        ;;
    stats)
        vc FCALL_RO GNODE_TOPO_STATS 1 "$TK"
        ;;
    verify)
        vc FCALL_RO GNODE_TOPO_VERIFY 1 "$TK"
        ;;
    sites)
        vc SMEMBERS gnode:sites:registry
        ;;
    load)
        # Load (slot 16) is derived by the sampler for nodes, services and
        # service@node placements; a service reads the max of its placements.
        printf 'writer lease: %s (ttl %s)\n\n' \
            "$(vc --raw GET "${TK%:constellation}:cluster:writer")" "$(vc --raw TTL "${TK%:constellation}:cluster:writer")"
        printf '%-16s %-30s %-10s %8s  %s\n' SHAPE ENTITY BAND COORD 'MEASURED'
        entities | python3 -c '
import sys, time
want = sys.argv[1] if len(sys.argv) > 1 else ""
bands = [(0.0, "unknown"), (0.2, "idle"), (0.4, "light"), (0.6, "moderate"), (0.8, "heavy"), (1.0, "saturated")]
now = int(time.time())
for line in sys.stdin:
    sh, name, eid, env, load, health, of, on, ds = line.rstrip("\n").split("\t")
    if sh not in ("1", "3.1", "3.2", "3.3", "4.2") or load == "":
        continue
    if want and eid != want and of != want:
        continue
    v = float(load)
    band = [n for edge, n in bands if v + 1e-9 >= edge][-1]
    age = "never" if not ds else "%ds ago" % (now - int(ds))
    print("%-16s %-30s %-10s %8.4f  %s" % (name, eid, band, v, age))
' "${1:-}"
        ;;
    register)
        exec "$SCRIPT_DIR/register.sh" "$@"
        ;;
    deregister)
        exec "$SCRIPT_DIR/deregister.sh" "$@"
        ;;
    help | -h | --help)
        echo "geodineum topology {list | all | show <id> | stats | verify | sites | load [id]}"
        echo "                   register {components | service <site> [profile] [--env <dtap>]}"
        echo "                   deregister <id> | deregister --shape <shape> --confirm"
        ;;
    *)
        echo "usage: geodineum topology {list|all|show <id>|stats|verify|sites|load [id]|register ...|deregister ...}" >&2
        exit 2
        ;;
esac
