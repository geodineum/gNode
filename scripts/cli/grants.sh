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
# The wrapper resolves the daemon TIER from VALKEY_USER (env), loading that
# user's password. Passing `--user` as an ARG appends a second flag while the
# wrapper still loads the DEFAULT user's password → NOAUTH on every call.
vk()       { VALKEY_USER=gnode_daemon "$VCLI" "$@"; }
vk_admin() {
    [[ -f "$CRED_DIR/valkey.password" ]] || die "admin credential absent — approve/deny run on the constellation master only"
    REDISCLI_AUTH="$(cat "$CRED_DIR/valkey.password")" \
        valkey-cli -h "${VALKEY_HOST:-127.0.0.1}" -p "${VALKEY_PORT:-47445}" "$@"
}

# One email through COMMS (proven transport; site routing delivers to the
# site's configured recipients). Sentinel-safe: explicit email channel.
notify() {
    local subject="$1" body="$2"
    # type=alert, NOT system. COMMS acks and DROPS system messages without
    # dispatching them (Geodineum-COMMS/src/main.rs:566-570), so every grant
    # request since this loop shipped was queued successfully and then silently
    # discarded — the CLI logged "notification queued via COMMS", which was
    # true and useless. An approval loop whose notification never arrives is an
    # approval loop that auto-denies everything after 72h.
    vk XADD "{${NOTIFY_SITE}}:gnode:comms:production" '*' \
        id "grants-$(date +%s)-$RANDOM" \
        type alert \
        timestamp "$(date -Iseconds)" \
        environment production \
        priority 2 \
        content "{\"subject\":$(printf '%s' "$subject" | python3 -c 'import json,sys;print(json.dumps(sys.stdin.read()))'),\"body\":$(printf '%s' "$body" | python3 -c 'import json,sys;print(json.dumps(sys.stdin.read()))')}" \
        dispatch "$(python3 -c '
import json, sys
# NO "channels" key on purpose. Naming channels OVERRIDES the site routing
# (Geodineum-COMMS/src/router/dispatcher.rs:181-185), so asking for telegram
# on a site that has not configured it turns every grant notification into a
# per-channel failure and retry churn. The dispatch block exists here only to
# carry the buttons; where the message goes stays the site operator|s call.
d = {}
m = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else ""
if m:
    d["reply_markup"] = json.loads(m)
print(json.dumps(d))' "${NOTIFY_MARKUP:-}")" >/dev/null \
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

    # Refuse an identical request that is already pending. Filing used to be
    # unconditional, so a provisioning script run three times left six requests
    # for two actual needs. Nothing broke — they all auto-deny — but an operator
    # facing six near-identical entries has to work out which are the same ask,
    # and that is how a security review becomes a formality.
    _dupe=$("$0" pending 2>/dev/null \
        | grep "^PENDING" \
        | grep -F "svc=${SERVICE}  patterns=[${PATTERNS[*]}]" \
        | head -1 || true)
    if [[ -n "$_dupe" ]]; then
        log "identical request already pending: $(awk '{print $2}' <<< "$_dupe") — not filing a duplicate"
        log "  (use 'grants deny <id>' first if you want to re-file with a different reason)"
        exit 0
    fi

    RID="gr-$(date +%s)-$RANDOM"
    vk XADD "$REQ_STREAM" '*' \
        req "$RID" svc "$SERVICE" patterns "${PATTERNS[*]}" \
        reason "${REASON:-none given}" ttl_hours "$TTL" \
        ts "$(date -Iseconds)" requester "${SUDO_USER:-$(whoami)}@$(hostname)" >/dev/null
    log "request $RID filed: $SERVICE → ${PATTERNS[*]} (auto-deny after ${TTL}h)"
    NOTIFY_MARKUP=$(python3 -c '
import json, sys
rid = sys.argv[1]
print(json.dumps({"inline_keyboard": [[
    {"text": "\u2713 Approve", "callback_data": "grant:approve:" + rid},
    {"text": "\u2717 Deny",    "callback_data": "grant:deny:" + rid},
]]}))' "$RID")
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

watch)
    # Consume Telegram approve/deny callbacks and apply them.
    #
    #   geodineum grants watch [--once]
    #
    # MASTER ONLY, because applying a decision needs the ACL admin credential.
    #
    # WHY A BUTTON AND NOT A LINK. A token in a URL is a bearer capability in
    # a medium that forwards, indexes and archives itself — and mail and chat
    # clients FETCH urls to build previews, so a state-changing GET can fire
    # with nobody clicking. A callback carries no capability at all: Telegram
    # reports the pressing user's id and that id is checked here.
    #
    # THE ALLOWLIST IS CHECKED TWICE, DELIBERATELY. The receiver rejects
    # unauthorized users before writing to the inbound stream, but that stream
    # is a ValKey key — anything holding a grant on it could inject an entry
    # that never passed through Telegram at all. Trusting an upstream check
    # you cannot see from here is how a defence becomes decorative.
    ONCE=false
    [[ "${1:-}" == "--once" ]] && ONCE=true
    # Reuse COMMS_ADMIN_IDS — the allowlist that already gates who may talk to
    # the bot at all (Geodineum-COMMS/src/config.rs:285). A SECOND list would
    # drift from it, and two allowlists that disagree is worse than one:
    # whichever is looser becomes the real policy while the other reads as
    # protection.
    #
    # GEODINEUM_GRANT_ADMINS overrides it, for the case where deciding grants
    # should be NARROWER than chatting with the bot. It can only narrow in
    # practice: an id absent from COMMS_ADMIN_IDS never reaches the inbound
    # stream, so listing it here grants nothing.
    COMMS_ENV=/etc/geodineum/components/geodineum-comms/geodineum-comms.env
    comms_env() {
        [[ -r "$COMMS_ENV" ]] || return 0
        grep -E "^${1}=" "$COMMS_ENV" 2>/dev/null | tail -1 | cut -d= -f2- | tr -d '"'"'"''
    }
    ADMINS="${GEODINEUM_GRANT_ADMINS:-${COMMS_ADMIN_IDS:-$(comms_env COMMS_ADMIN_IDS)}}"
    if [[ -z "$ADMINS" ]]; then
        die "No admin allowlist — refusing to apply callbacks.
  Looked at: GEODINEUM_GRANT_ADMINS, COMMS_ADMIN_IDS, and ${COMMS_ENV}
  Defaulting to permissive here would let anyone who can write the inbound
  stream approve their own ACL grant, so this refuses instead."
    fi

    # The inbound stream is NOT under NOTIFY_SITE. Outbound notification is
    # per-site; inbound is ONE bot bound to ONE site by COMMS_INBOUND_SITE,
    # which defaults to "geodine" (Geodineum-COMMS/src/config.rs:238,266) —
    # so watching the notifying site would block forever on a stream nothing
    # ever writes, and every press would look like it did nothing.
    INBOUND_SITE="${COMMS_INBOUND_SITE:-$(comms_env COMMS_INBOUND_SITE)}"
    INBOUND_SITE="${INBOUND_SITE:-geodine}"
    STREAM="{${INBOUND_SITE}}:gnode:comms:inbound:production"
    GROUP=grants-watch
    # From 0, not $: a press that lands before this loop starts must still be
    # honoured. Replay is safe — approve/deny refuses a request the ledger has
    # already decided, so a re-read decides nothing twice.
    vk XGROUP CREATE "$STREAM" "$GROUP" 0 MKSTREAM >/dev/null 2>&1 || true
    if [[ "$(vk EXISTS "$STREAM" 2>/dev/null | tr -d '[:space:]')" != "1" ]]; then
        log "WARN: ${STREAM} does not exist yet — no inbound message has ever"
        log "      been written to it. If presses do nothing, COMMS_INBOUND_SITE"
        log "      is not '${INBOUND_SITE}'; check ${COMMS_ENV}."
    fi
    log "watching ${STREAM} (admins: ${ADMINS})"
    while :; do
        # One entry at a time: a decision that applies an ACL change should be
        # readable in the log as one line per decision.
        RAW=$(vk XREADGROUP GROUP "$GROUP" "$(hostname)" COUNT 1 BLOCK 5000 STREAMS "$STREAM" '>' 2>/dev/null)
        if [[ -n "$RAW" ]]; then
            EID=$(grep -oE '^[0-9]+-[0-9]+$' <<< "$RAW" | head -1)
            FIELDS=$(printf '%s\n' "$RAW")
            is_cb=$(awk '/^is_callback$/{getline; print}' <<< "$FIELDS")
            txt=$(awk '/^text$/{getline; print}'        <<< "$FIELDS")
            op=$(awk '/^operator_id$/{getline; print}'  <<< "$FIELDS")
            if [[ "$is_cb" == "true" && "$txt" == grant:* ]]; then
                action="${txt#grant:}"; action="${action%%:*}"
                rid="${txt##*:}"
                if [[ ",${ADMINS}," != *",${op},"* ]]; then
                    log "REFUSED ${action} ${rid}: operator ${op} is not in GEODINEUM_GRANT_ADMINS"
                elif [[ "$action" != "approve" && "$action" != "deny" ]]; then
                    log "ignored callback with unknown action: ${txt}"
                else
                    log "operator ${op} pressed ${action} for ${rid}"
                    "$0" "$action" "$rid" --reason "via Telegram by operator ${op}" || \
                        log "WARN: ${action} ${rid} failed"
                fi
            fi
            [[ -n "$EID" ]] && vk XACK "$STREAM" "$GROUP" "$EID" >/dev/null 2>&1
        fi
        [[ "$ONCE" == "true" ]] && break
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
