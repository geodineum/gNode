#!/bin/bash
# geodineum topology — inspect the live 30-dim service topology.
#
#   sudo geodineum topology                  list topologies + entity counts
#   sudo geodineum topology all              dump EVERY registered service in every topology
#   sudo geodineum topology show <site_id>   entity IDs placed in <site_id>
#   sudo geodineum topology stats <site_id>  daemon stats for <site_id>'s topology
#   sudo geodineum topology sites            registered sites
#   sudo geodineum topology load [site]      what the sampler measured: band, coordinate,
#                                            baseline per command, and how old the reading is
#
# Backs registration verification: after `register service`, confirm the
# component actually LANDED in the topology (daemon discovery-ingest). Reads the
# admin credential (root-readable) → requires_sudo. Read-only.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PORT="${VALKEY_PORT:-47445}"
CRED_DIR="${GEODINEUM_CREDENTIALS_DIR:-/etc/geodineum/credentials}"

AUTH=""
for f in "$CRED_DIR/valkey_admin.password" "$CRED_DIR/valkey.password"; do
    [[ -r "$f" ]] && { AUTH="$(cat "$f")"; break; }
done
if [[ -z "$AUTH" ]]; then
    echo "topology: cannot read admin credential under $CRED_DIR — run with sudo" >&2
    exit 1
fi
vc() { REDISCLI_AUTH="$AUTH" redis-cli -p "$PORT" "$@"; }

# Topology key for a site — matches the daemon (tool_registration.rs):
#   {<site_id>}:gnode:services    (braces are literal ValKey hash-tags)
topo_key() { printf '{%s}:gnode:services' "$1"; }

sub="${1:-list}"
[[ $# -gt 0 ]] && shift

case "$sub" in
    list)
        echo "Service topologies (key → entities):"
        found=0
        while IFS= read -r metakey; do
            [[ -n "$metakey" ]] || continue
            found=1
            base="${metakey%:meta}"
            n="$(vc HLEN "${base}:entities" 2>/dev/null)"
            printf '  %-48s %s entities\n' "$base" "${n:-?}"
        done < <(vc --scan --pattern '*:gnode:services:meta'; vc --scan --pattern '*:topology:meta')
        if [[ "$found" -eq 0 ]]; then
            echo "  (no topologies registered yet)"
        fi
        echo
        printf 'Registered sites: %s\n' "$(vc SMEMBERS gnode:sites:registry | tr '\n' ' ')"
        ;;
    all|dump)
        # Every entity in every service topology — the full picture.
        any=0
        while IFS= read -r metakey; do
            [[ -n "$metakey" ]] || continue
            any=1
            base="${metakey%:meta}"
            out="$(vc HKEYS "${base}:entities")"
            cnt="$(printf '%s' "$out" | grep -c . 2>/dev/null)"
            printf '\n── %s  (%s) ──\n' "$base" "${cnt:-0} entities"
            if [[ -n "$out" ]]; then
                printf '%s\n' "$out" | sed 's/^/  /'
            else
                echo "  (empty)"
            fi
        done < <(vc --scan --pattern '*:gnode:services:meta'; vc --scan --pattern '*:topology:meta')
        if [[ "$any" -eq 0 ]]; then
            echo "(no topologies registered yet)"
        fi
        ;;
    show|entities)
        if [[ $# -lt 1 ]]; then echo "usage: geodineum topology show <site_id>" >&2; exit 2; fi
        key="$(topo_key "$1")"
        echo "Entities in ${key}:"
        out="$(vc HKEYS "${key}:entities")"
        if [[ -n "$out" ]]; then
            printf '%s\n' "$out" | sed 's/^/  /'
        else
            echo "  (none placed — not in topology; check daemon discovery / capabilities YAML)"
        fi
        ;;
    stats)
        if [[ $# -lt 1 ]]; then echo "usage: geodineum topology stats <site_id>" >&2; exit 2; fi
        vc FCALL GNODE_TOPO_STATS 1 "$(topo_key "$1")"
        ;;
    sites)
        vc SMEMBERS gnode:sites:registry
        ;;
    load)
        # Read the derived load axis and the baselines it was measured against.
        # A human has to be able to check these numbers before anything ranks on
        # them, which is why this exists before the engine does.
        printf 'writer lease: %s (ttl %s)\n\n' \
            "$(vc --no-raw GET gnode:cluster:writer | tr -d '"')" "$(vc TTL gnode:cluster:writer)"
        printf '%-22s %-26s %-10s %8s  %s\n' SITE ENTITY BAND COORD 'MEASURED'
        while IFS= read -r site; do
            [[ -n "$site" ]] || continue
            [[ $# -ge 1 && "$site" != "$1" ]] && continue
            while IFS= read -r eid; do
                [[ -n "$eid" ]] || continue
                json=$(vc HGET "$(topo_key "$site"):entities" "$eid")
                printf '%s' "$json" | python3 -c '
import json, sys, time
site, eid = sys.argv[1], sys.argv[2]
e = json.loads(sys.stdin.read() or "{}")
pd = e.get("pd") or []
if len(pd) < 17:
    sys.exit()
v = pd[16]
bands = [(0.0, "unknown"), (0.2, "idle"), (0.4, "light"),
         (0.6, "moderate"), (0.8, "heavy"), (1.0, "saturated")]
band = [name for edge, name in bands if v + 1e-9 >= edge][-1]
ds = (e.get("m") or {}).get("ds")
age = "never" if not ds else "%ds ago" % (int(time.time()) - int(ds))
print("%-22s %-26s %-10s %8.4f  %s" % (site, eid, band, v, age))
' "$site" "$eid"
            done < <(vc --no-raw HKEYS "$(topo_key "$site"):entities" | tr -d '"')
        done < <(vc --no-raw SMEMBERS gnode:sites:registry | tr -d '"' | sort)
        echo
        echo "Baselines (p50 ms the inflation is measured against):"
        while IFS= read -r site; do
            [[ -n "$site" ]] || continue
            [[ $# -ge 1 && "$site" != "$1" ]] && continue
            n=$(vc HLEN "{$site}:gnode:baseline")
            [[ "$n" == "0" || -z "$n" ]] && continue
            printf '  %s\n' "$site"
            vc --no-raw HGETALL "{$site}:gnode:baseline" | tr -d '"' | paste - - | sed 's/^/    /'
        done < <(vc --no-raw SMEMBERS gnode:sites:registry | tr -d '"' | sort)
        ;;
    register)
        # geodineum topology register {tool | service <site> [profile]}
        exec "$SCRIPT_DIR/register.sh" "$@"
        ;;
    deregister)
        # geodineum topology deregister <site> {<entity> | --all}
        exec "$SCRIPT_DIR/deregister.sh" "$@"
        ;;
    help | -h | --help)
        echo "geodineum topology {list | all | show <site> | stats <site> | sites | load [site]}"
        echo "                   register {tool | service <site> [profile]}"
        echo "                   deregister <site> {<entity> | --all}"
        ;;
    *)
        echo "usage: geodineum topology {list|all|show <site>|stats <site>|sites|register ...|deregister ...}" >&2
        exit 2
        ;;
esac
