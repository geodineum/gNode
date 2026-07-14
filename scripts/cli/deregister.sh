#!/bin/bash
# geodineum deregister — remove entities from a (C) topology + the (B) snapshot.
#
#   sudo geodineum deregister <site> <entity>   remove one entity
#   sudo geodineum deregister <site> --all      remove ALL entities in <site>'s topology
#
# Wraps FCALL GNODE_DEREGISTER_CAPABILITY_VECTOR, which removes the entity (plus
# its edges + voxel index) and the matching row from the global (B) snapshot.
# Reads the admin credential (root-only) → requires_sudo.
set -uo pipefail

PORT="${VALKEY_PORT:-47445}"
CRED_DIR="${GEODINEUM_CREDENTIALS_DIR:-/etc/geodineum/credentials}"
SNAPSHOT_KEY="{geodineum}:gnode:topology:services"

AUTH=""
for f in "$CRED_DIR/valkey_admin.password" "$CRED_DIR/valkey.password"; do
    [[ -r "$f" ]] && { AUTH="$(cat "$f")"; break; }
done
[[ -n "$AUTH" ]] || { echo "deregister: cannot read admin credential under $CRED_DIR — run with sudo" >&2; exit 1; }

vc() { REDISCLI_AUTH="$AUTH" redis-cli -p "$PORT" "$@"; }
# Topology key — matches the daemon (tool_registration.rs): {<site>}:gnode:services
topo_key() { printf '{%s}:gnode:services' "$1"; }

site="${1:-}"; target="${2:-}"
if [[ -z "$site" || -z "$target" ]]; then
    echo "Usage: geodineum deregister <site> <entity|--all>" >&2
    exit 1
fi
TK="$(topo_key "$site")"

dereg_one() { vc FCALL GNODE_DEREGISTER_CAPABILITY_VECTOR 1 "$TK" "$1" "$SNAPSHOT_KEY"; }

if [[ "$target" == "--all" ]]; then
    mapfile -t eids < <(vc HKEYS "${TK}:entities")
    if [[ ${#eids[@]} -eq 0 || ( ${#eids[@]} -eq 1 && -z "${eids[0]}" ) ]]; then
        echo "deregister: '$site' topology is already empty"
        exit 0
    fi
    n=0
    for eid in "${eids[@]}"; do
        [[ -n "$eid" ]] || continue
        echo "  - $eid"
        dereg_one "$eid" >/dev/null
        n=$((n + 1))
    done
    echo "deregistered $n entit$([[ $n -eq 1 ]] && echo y || echo ies) from '$site'"
else
    dereg_one "$target"
fi
