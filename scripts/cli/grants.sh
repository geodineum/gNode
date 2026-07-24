#!/bin/bash
# geodineum grants — the grant-request approval loop (v0 of the SB-8.92
# lifecycle; CONTRACTS/access-grants.scn.md tier-3).
#
#   geodineum grants request <service> <pattern...> [--reason "…"] [--ttl-hours N]
#   geodineum grants pending
#   geodineum grants approve <request_id> [--patterns "…"]     (master, admin cred)
#   geodineum grants deny    <request_id> [--reason "…"]        (master, admin cred)
#   geodineum grants show    <service>
#   geodineum grants sweep                                       (timeout auto-deny)
#
# Requests are DATA on {ns}:gnode:grants:requests; every decision appends to
# the {ns}:gnode:grants:ledger BEFORE any ACL change (ledger-then-apply).
# Approval ADDS ~patterns to gnode_client_<service> (ACL is additive; revoke =
# recompose via re-onboard). Notification rides COMMS (a working transport):
# one email per request + per timeout-deny, carrying the approve/deny CLI
# one-liners. v0 ledger entries are unsigned; the provisioner (SB-8.83 3-4)
# takes over signing + auto-grant matching when it lands.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GNODE_SCRIPTS="$(dirname "$SCRIPT_DIR")"
VCLI="$GNODE_SCRIPTS/valkey-cli-secure.sh"

NS="${GNODE_TOPOLOGY_NAMESPACE:-geodineum}"
REQ_STREAM="{${NS}}:gnode:grants:requests"
LEDGER_STREAM="{${NS}}:gnode:grants:ledger"
NOTIFY_SITE="${GEODINEUM_GRANTS_SITE:-geodineum_com}"
DEFAULT_TTL_HOURS="${GEODINEUM_GRANTS_TTL_HOURS:-72}"
CRED_DIR="${GNODE_CREDENTIAL_DIR:-/etc/geodineum/credentials}"

log()  { echo "[grants] $*"; }
die()  { echo "[grants] ERROR: $*" >&2; exit 1; }

# Daemon-tier for reads/request-writes (every node); admin for ACL mutation.
vk()       { "$VCLI" --user gnode_daemon "$@"; }
vk_admin() {
    [[ -f "$CRED_DIR/valkey.password" ]] || die "admin credential absent — approve/deny run on the constellation master only"
    REDISCLI_AUTH="$(cat "$CRED_DIR/valkey.password")" \
        valkey-cli -h "${VALKEY_HOST:-127.0.0.1}" -p "${VALKEY_PORT:-47445}" "$@"
}

# One email through COMMS (proven transport; site routing delivers to the
# site's configured recipients). Sentinel-safe: explicit email channel.
notify() {
    local subject="$1" body="$2"
    vk XADD "{${NOTIFY_SITE}}:gnode:comms:production" '*' \
        id "grants-$(date +%s)-$RANDOM" \
        type system \
        timestamp "$(date -Iseconds)" \
        environment production \
        priority 2 \
        content "{\"subject\":$(printf '%s' "$subject" | python3 -c 'import json,sys;print(json.dumps(sys.stdin.read()))'),\"body\":$(printf '%s' "$body" | python3 -c 'import json,sys;print(json.dumps(sys.stdin.read()))')}" \
        dispatch '{"channels":["email"]}' >/dev/null \
        && log "notification queued via COMMS ({${NOTIFY_SITE}})" \
        || log "WARN: COMMS notification failed (loop still functional via CLI)"
}

# Decision lookup: newest ledger action for a request id, empty if none.
decision_for() {
    local rid="$1"
    vk XRANGE "$LEDGER_STREAM" - + | awk -v want="$rid" '
        /^[0-9]+-[0-9]+$/ { flush() ; delete f; next }
        { if (prev != "") { f[prev] = $0; prev = "" } else { prev = $0 } }
        function flush() { if (f["req"] == want) last = f["action"] }
        END { flush(); print last }'
}

cmd="${1:-}"; shift || true
case "$cmd" in

request)
    SERVICE="${1:-}"; shift || true
    [[ -n "$SERVICE" ]] || die "usage: grants request <service> <pattern...> [--reason ...] [--ttl-hours N]"
    PATTERNS=(); REASON=""; TTL="$DEFAULT_TTL_HOURS"
    while [[ $# -gt 0 ]]; do case "$1" in
        --reason) REASON="$2"; shift 2;;
        --ttl-hours) TTL="$2"; shift 2;;
        *) PATTERNS+=("$1"); shift;;
    esac; done
    [[ ${#PATTERNS[@]} -gt 0 ]] || die "at least one key pattern required"
    RID="gr-$(date +%s)-$RANDOM"
    vk XADD "$REQ_STREAM" '*' \
        req "$RID" svc "$SERVICE" patterns "${PATTERNS[*]}" \
        reason "${REASON:-none given}" ttl_hours "$TTL" \
        ts "$(date -Iseconds)" requester "${SUDO_USER:-$(whoami)}@$(hostname)" >/dev/null
    log "request $RID filed: $SERVICE → ${PATTERNS[*]} (auto-deny after ${TTL}h)"
    notify "[geodineum] grant request $RID: $SERVICE" \
"Service '$SERVICE' requests ValKey access:

  patterns: ${PATTERNS[*]}
  reason:   ${REASON:-none given}
  filed:    $(date -Iseconds) by ${SUDO_USER:-$(whoami)}@$(hostname)
  timeout:  auto-DENY after ${TTL}h

Decide on the constellation master:
  sudo geodineum grants approve $RID
  sudo geodineum grants deny $RID --reason \"...\"
Inspect first:
  sudo geodineum grants pending"
    ;;

pending)
    vk XRANGE "$REQ_STREAM" - + | awk '
        /^[0-9]+-[0-9]+$/ { flush(); delete f; next }
        { if (prev != "") { f[prev] = $0; prev = "" } else { prev = $0 } }
        function flush() { if (f["req"] != "") printf "%s  svc=%s  patterns=[%s]  ttl=%sh  filed=%s  reason=%s\n", f["req"], f["svc"], f["patterns"], f["ttl_hours"], f["ts"], f["reason"] }
        END { flush() }' | while IFS= read -r line; do
        rid="${line%% *}"
        d=$("$0" __decision "$rid")
        [[ -z "$d" ]] && echo "PENDING  $line" || echo "$(echo "$d" | tr a-z A-Z)  $line"
    done
    ;;

__decision) decision_for "${1:?}";;

approve|deny)
    ACTION="$cmd"; RID="${1:-}"; shift || true
    [[ -n "$RID" ]] || die "usage: grants $ACTION <request_id>"
    OVERRIDE=""; REASON=""
    while [[ $# -gt 0 ]]; do case "$1" in
        --patterns) OVERRIDE="$2"; shift 2;;
        --reason) REASON="$2"; shift 2;;
        *) shift;;
    esac; done
    ENTRY=$(vk XRANGE "$REQ_STREAM" - + | awk -v want="$RID" '
        /^[0-9]+-[0-9]+$/ { flush(); delete f; next }
        { if (prev != "") { f[prev] = $0; prev = "" } else { prev = $0 } }
        function flush() { if (f["req"] == want) printf "%s\t%s", f["svc"], f["patterns"] }
        END { flush() }')
    [[ -n "$ENTRY" ]] || die "request $RID not found on $REQ_STREAM"
    SVC="${ENTRY%%$'\t'*}"; REQ_PATTERNS="${ENTRY#*$'\t'}"
    [[ -z "$(decision_for "$RID")" ]] || die "request $RID already decided (ledger)"
    PATTERNS="${OVERRIDE:-$REQ_PATTERNS}"
    ACL_USER="gnode_client_${SVC}"

    # LEDGER-THEN-APPLY: the decision is recorded before any ACL mutation.
    vk_admin XADD "$LEDGER_STREAM" '*' \
        req "$RID" svc "$SVC" action "$ACTION" patterns "$PATTERNS" \
        decider "operator:${SUDO_USER:-$(whoami)}" reason "${REASON:-—}" \
        ts "$(date -Iseconds)" >/dev/null

    if [[ "$ACTION" == "approve" ]]; then
        GRANT_ARGS=(); for p in $PATTERNS; do GRANT_ARGS+=("~${p#\~}"); done
        vk_admin ACL SETUSER "$ACL_USER" "${GRANT_ARGS[@]}" >/dev/null \
            || die "ACL SETUSER failed (ledger already records the intent — investigate)"
        vk_admin ACL SAVE >/dev/null
        log "APPROVED $RID — added to $ACL_USER: ${GRANT_ARGS[*]}"
    else
        log "DENIED $RID (${REASON:-no reason given})"
    fi
    notify "[geodineum] grant $RID ${ACTION}d" \
"Request $RID for '$SVC' was ${ACTION}d by operator:${SUDO_USER:-$(whoami)}.
patterns: $PATTERNS
reason: ${REASON:-—}"
    ;;

show)
    SVC="${1:-}"; [[ -n "$SVC" ]] || die "usage: grants show <service>"
    echo "== ledger decisions for $SVC:"
    vk XRANGE "$LEDGER_STREAM" - + | awk -v want="$SVC" '
        /^[0-9]+-[0-9]+$/ { flush(); delete f; next }
        { if (prev != "") { f[prev] = $0; prev = "" } else { prev = $0 } }
        function flush() { if (f["svc"] == want) printf "  %s  %s  [%s]  by %s  (%s)\n", f["ts"], f["action"], f["patterns"], f["decider"], f["reason"] }
        END { flush() }'
    echo "== effective ACL (gnode_client_${SVC}):"
    vk_admin ACL GETUSER "gnode_client_${SVC}" 2>/dev/null | sed 's/^/  /' || echo "  (admin credential required — run on the master)"
    ;;

sweep)
    NOW=$(date +%s)
    vk XRANGE "$REQ_STREAM" - + | awk '
        /^[0-9]+-[0-9]+$/ { flush(); delete f; id=$0; next }
        { if (prev != "") { f[prev] = $0; prev = "" } else { prev = $0 } }
        function flush() { if (f["req"] != "") printf "%s|%s|%s|%s\n", f["req"], f["svc"], f["ttl_hours"], f["ts"] }
        END { flush() }' | while IFS='|' read -r rid svc ttl ts; do
        [[ -n "$(decision_for "$rid")" ]] && continue
        DEADLINE=$(( $(date -d "$ts" +%s 2>/dev/null || echo 0) + ${ttl:-72} * 3600 ))
        if [[ "$NOW" -gt "$DEADLINE" ]]; then
            vk_admin XADD "$LEDGER_STREAM" '*' \
                req "$rid" svc "$svc" action deny patterns "-" \
                decider "timeout" reason "auto-deny after ${ttl:-72}h" \
                ts "$(date -Iseconds)" >/dev/null
            log "auto-DENIED $rid ($svc) — ${ttl:-72}h timeout"
            notify "[geodineum] grant $rid auto-denied (timeout)" \
"Request $rid for '$svc' expired undecided after ${ttl:-72}h and was auto-denied.
Re-file with: geodineum grants request $svc <patterns> --reason ..."
        fi
    done
    log "sweep complete"
    ;;

*)
    sed -n '3,12p' "$0" | sed 's/^# \?//'
    exit 1
    ;;
esac
