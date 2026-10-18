#!/bin/bash
#
# The ValKey half of `geodineum deregister`: removes what registration wrote into
# ValKey for one service, in every constellation this node serves.
#
#   1. its registration intent ({c}:gnode:registrations)
#   2. its topology entity and placements, in the constellation that holds it
#   3. its keys: streams, metadata, {id}:* (GNODE_DEPROVISION_SERVICE; a constellation
#      that shares the namespace keeps its own keys)
#   4. its identity: ACL user gnode_client_<id> and its credential files
#
# The host half (units, vhost, files registration created) is `geodineum deregister`,
# which calls this. Run this directly only for a service whose host is gone.
#
# Usage: deregister-service.sh <service_id> [--dry-run] [--keep-acl] [--keep-cache] [--force]
#
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
source "$SCRIPT_DIR/cli/constellation.sh"

PASSWORD_DIR="${GNODE_PASSWORD_DIR:-$PROJECT_ROOT/.gnode}"
VALKEY_PORT="${VALKEY_PORT:-47445}"
VALKEY_HOST="${VALKEY_HOST:-127.0.0.1}"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'
log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[OK]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1" >&2; }

usage() {
    sed -n 3,15p "$0" | sed 's/^# \{0,1\}//'
    exit "${1:-0}"
}

SERVICE_ID="" DRY_RUN=false KEEP_ACL=false KEEP_CACHE=false FORCE=false
while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run)    DRY_RUN=true ;;
        --keep-acl)   KEEP_ACL=true ;;
        --keep-cache) KEEP_CACHE=true ;;
        --force)      FORCE=true ;;
        --remove-acl) log_error "--remove-acl is gone: the identity is removed by default (--keep-acl keeps it)"; exit 2 ;;
        -h|--help)    usage 0 ;;
        -*)           log_error "Unknown option: $1"; usage 2 ;;
        *)            [[ -z "$SERVICE_ID" ]] || { log_error "Unexpected argument: $1"; usage 2; }; SERVICE_ID="$1" ;;
    esac
    shift
done
[[ -n "$SERVICE_ID" ]] || { log_error "Service ID required"; usage 2; }
[[ "$SERVICE_ID" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]*$ ]] || { log_error "Not a service id: '$SERVICE_ID'"; exit 2; }

ADMIN_FILE="$PASSWORD_DIR/valkey.password"
[[ -r "$ADMIN_FILE" ]] || { log_error "Cannot read $ADMIN_FILE — run with sudo"; exit 1; }
vk() { REDISCLI_AUTH="$(<"$ADMIN_FILE")" valkey-cli --no-auth-warning --raw -h "$VALKEY_HOST" -p "$VALKEY_PORT" "$@"; }
[[ "$(vk PING 2>&1)" == PONG ]] || { log_error "ValKey not reachable on ${VALKEY_HOST}:${VALKEY_PORT}"; exit 1; }

mapfile -t CONSTELLATIONS < <(gnode_cli_constellations)
ACL_USER="gnode_client_${SERVICE_ID}"
CRED_FILES=("$PASSWORD_DIR/valkey_client_${SERVICE_ID}.password" "$PASSWORD_DIR/valkey_client_${SERVICE_ID}.password.owner")
OPTIONS_JSON="{\"include_cache\":$([[ "$KEEP_CACHE" == true ]] && echo false || echo true)}"
DRY_JSON="{\"dry_run\":true,${OPTIONS_JSON#\{}"

# --- what exists --------------------------------------------------------------
intent_in=() entity_in=()
for c in "${CONSTELLATIONS[@]}"; do
    [[ "$(vk HEXISTS "{$c}:gnode:registrations" "$SERVICE_ID")" == 1 ]] && intent_in+=("$c")
    [[ "$(vk HEXISTS "{$c}:gnode:constellation:entities" "$SERVICE_ID")" == 1 ]] && entity_in+=("$c")
done
acl_exists=false
[[ -n "$(vk ACL GETUSER "$ACL_USER" 2>/dev/null)" ]] && acl_exists=true

preview="$(vk FCALL GNODE_DEPROVISION_SERVICE 0 "$SERVICE_ID" "$DRY_JSON" 2>&1)"
doomed_raw="$(printf '%s' "$preview" | python3 -I -c '
import json, sys
try:
    d = json.load(sys.stdin)
except ValueError:
    sys.exit(3)
for k in d.get("deleted_keys", []):
    print(k["key"])
')" || { log_error "GNODE_DEPROVISION_SERVICE preview failed: $preview"; exit 1; }
doomed=()
[[ -n "$doomed_raw" ]] && mapfile -t doomed < <(printf '%s\n' "$doomed_raw" | sort -u)

# A deprovision that would touch a constellation's own keys is the pre-guard Lua.
for k in "${doomed[@]}"; do
    if [[ "$k" =~ ^\{[^}]+\}:gnode:(constellation|heartbeat|registrations|cluster|grants|schema|topo)(:|$) ]]; then
        log_error "Refusing: deprovisioning '$SERVICE_ID' would delete the constellation key $k"
        log_error "  The loaded Lua predates the shared-namespace guard. Run: sudo geodineum daemon reload-lua"
        exit 1
    fi
done

echo ""
log_info "Deregister '$SERVICE_ID' from ValKey (constellations: ${CONSTELLATIONS[*]})"
echo "  intent        ${intent_in[*]:-(none)}"
echo "  entity        ${entity_in[*]:-(none)}"
echo "  keys          ${#doomed[@]} to delete"
if [[ "$KEEP_ACL" == true ]]; then
    echo "  identity      kept (--keep-acl)"
else
    echo "  identity      ACL user ${ACL_USER}: $([[ "$acl_exists" == true ]] && echo present || echo absent)"
fi

nothing=false
[[ ${#intent_in[@]} -eq 0 && ${#entity_in[@]} -eq 0 && ${#doomed[@]} -eq 0 && "$acl_exists" == false ]] && nothing=true
for f in "${CRED_FILES[@]}"; do [[ -e "$f" && "$KEEP_ACL" == false ]] && nothing=false; done
if [[ "$nothing" == true ]]; then
    log_success "Nothing in ValKey for '$SERVICE_ID'"
    exit 0
fi
if [[ "$DRY_RUN" == true ]]; then
    printf '    %s\n' "${doomed[@]}"
    log_info "Dry run — no changes made"
    exit 0
fi
if [[ "$FORCE" != true ]]; then
    read -r -p "Deregister '$SERVICE_ID'? (y/N) " reply
    [[ "$reply" =~ ^[Yy] ]] || { log_info "Cancelled"; exit 0; }
fi

# --- remove, intent first so nothing re-derives what is being removed ---------
failed=0
for c in "${intent_in[@]}"; do
    if [[ "$(vk HDEL "{$c}:gnode:registrations" "$SERVICE_ID")" == 1 ]]; then log_success "Intent removed from {$c}"
    else log_error "Could not remove the intent from {$c}"; failed=1; fi
done
for c in "${entity_in[@]}"; do
    if out="$(GNODE_TOPOLOGY_NAMESPACE="$c" "$SCRIPT_DIR/cli/deregister.sh" "$SERVICE_ID" 2>&1)" && [[ "$out" == *'"ok":true'* ]]; then
        log_success "Topology entity and placements removed from {$c}"
    else
        log_error "Topology removal in {$c} failed: $out"; failed=1
    fi
done
result="$(vk FCALL GNODE_DEPROVISION_SERVICE 0 "$SERVICE_ID" "$OPTIONS_JSON" 2>&1)"
if summary="$(printf '%s' "$result" | python3 -I -c '
import json, sys
d = json.load(sys.stdin)
kept = [k["key"] for k in d.get("skipped_keys", []) if "shares the namespace" in k.get("reason", "")]
print("%d key(s) deleted%s" % (d.get("deleted_count", 0), ", %d kept for the constellation that shares the namespace" % len(kept) if kept else ""))
sys.exit(0 if d.get("success") else 1)
')"; then
    log_success "Keys: $summary"
else
    log_error "Deprovision failed: $result"; failed=1
fi
if [[ "$KEEP_ACL" != true ]]; then
    if [[ "$acl_exists" == true ]]; then
        if [[ "$(vk ACL DELUSER "$ACL_USER")" == 1 ]]; then
            log_success "ACL user $ACL_USER removed"
            [[ "$(vk ACL SAVE 2>&1)" == OK ]] || log_warn "ACL SAVE failed — the removal holds until ValKey restarts"
        else
            log_error "Could not remove ACL user $ACL_USER"; failed=1
        fi
    fi
    for f in "${CRED_FILES[@]}"; do
        [[ -e "$f" ]] || continue
        rm -f "$f" && log_success "Removed $f" || { log_error "Could not remove $f"; failed=1; }
    done
fi

[[ $failed -eq 0 ]] && log_success "'$SERVICE_ID' removed from ValKey" || log_error "'$SERVICE_ID' only partly removed — re-run to finish"
exit $failed
