#!/bin/bash
#
# Show or set this node's EXPOSURE — the set of work classes it will process.
#
# Exposure is written to daemon.env as VALKEY-independent daemon config
# (GNODE_NODE_TYPE), a comma-separated set of routing hints. The daemon reads
# it at start and processes an entry if ANY member of the set covers the
# entry's _gh hint. Changing it takes effect on the next daemon restart.
#
# Usage:
#   node-exposure.sh show
#   node-exposure.sh set <hint[,hint...]>
#   node-exposure.sh add <hint>
#   node-exposure.sh remove <hint>
#
# This is the mechanism. The guided `geodineum node expose` wraps it and hints
# the direct form; either reaches the same place.
set -uo pipefail

ACTION="${1:-show}"
ARG="${2:-}"

CONFIG_ROOT="${GEODINEUM_CONFIG_ROOT:-/etc/geodineum}"
DAEMON_ENV="${CONFIG_ROOT}/components/gnode-daemon/daemon.env"
KEY="GNODE_NODE_TYPE"
SERVICE="gnode-daemon"

# Known built-in exposures, for guidance only — custom hints are allowed, since
# routing configs are operator-definable (daemon/config/nodes/*.yaml).
KNOWN="general inference gpu_compute all"

err()  { echo "ERROR: $*" >&2; }
note() { echo "$*"; }

read_current() {
    if [[ -r "$DAEMON_ENV" ]] && grep -qE "^${KEY}=" "$DAEMON_ENV"; then
        grep -E "^${KEY}=" "$DAEMON_ENV" | tail -1 | cut -d= -f2- | tr -d '"'
    else
        echo "general"
    fi
}

# Normalise a comma list: trim, drop empties and duplicates, preserve order.
normalise() {
    printf '%s' "$1" | tr ',' '\n' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//' \
        | awk 'NF && !seen[$0]++' | paste -sd, -
}

write_current() {
    local value="$1"
    [[ -n "$value" ]] || { err "refusing to write an empty exposure (would expose to nothing)"; return 1; }
    if [[ ! -w "$DAEMON_ENV" && ! -w "$(dirname "$DAEMON_ENV")" ]]; then
        err "cannot write ${DAEMON_ENV} — run with sudo"
        return 1
    fi
    install -d -m 0755 "$(dirname "$DAEMON_ENV")" 2>/dev/null || true
    if [[ -f "$DAEMON_ENV" ]] && grep -qE "^${KEY}=" "$DAEMON_ENV"; then
        sed -i "s|^${KEY}=.*|${KEY}=\"${value}\"|" "$DAEMON_ENV"
    else
        printf '%s="%s"\n' "$KEY" "$value" >> "$DAEMON_ENV"
    fi
}

warn_unknown() {
    local hint
    for hint in $(printf '%s' "$1" | tr ',' ' '); do
        case " $KNOWN " in
            *" $hint "*) : ;;
            *) note "  note: '${hint}' is not a built-in exposure; it needs a routing config"
               note "        (daemon/config/nodes/${hint}.yaml on the master) or the daemon"
               note "        will treat it as matching nothing." ;;
        esac
    done
}

restart_hint() {
    note ""
    note "Takes effect on the next daemon restart:"
    note "  sudo systemctl restart ${SERVICE}"
}

case "$ACTION" in
    show)
        cur="$(read_current)"
        note "Exposure: ${cur}"
        note ""
        note "This node processes an entry if any of these covers its _gh hint."
        note "Built-in exposures: ${KNOWN}"
        active="$(systemctl is-active "$SERVICE" 2>/dev/null || echo unknown)"
        note "Daemon: ${active}"
        ;;
    set)
        [[ -n "$ARG" ]] || { err "usage: node-exposure.sh set <hint[,hint...]>"; exit 1; }
        value="$(normalise "$ARG")"
        warn_unknown "$value"
        write_current "$value" || exit 1
        note "Exposure set: ${value}"
        restart_hint
        ;;
    add)
        [[ -n "$ARG" ]] || { err "usage: node-exposure.sh add <hint>"; exit 1; }
        value="$(normalise "$(read_current),${ARG}")"
        warn_unknown "$ARG"
        write_current "$value" || exit 1
        note "Exposure now: ${value}"
        restart_hint
        ;;
    remove)
        [[ -n "$ARG" ]] || { err "usage: node-exposure.sh remove <hint>"; exit 1; }
        value="$(read_current | tr ',' '\n' | grep -vxF "$ARG" | paste -sd, -)"
        value="$(normalise "$value")"
        [[ -n "$value" ]] || value="general"   # never leave a node exposed to nothing
        write_current "$value" || exit 1
        note "Exposure now: ${value}"
        restart_hint
        ;;
    *)
        err "unknown action '${ACTION}' (show|set|add|remove)"
        exit 1
        ;;
esac
