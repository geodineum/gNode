#!/bin/bash
# The grant ledger as the record of what a service holds beyond its manifest.
# Sourced by cli/grants.sh (decisions) and onboard-service.sh (every regrant replays it).

[[ -n "${_GNODE_GRANTS_LEDGER_SH:-}" ]] && return 0
_GNODE_GRANTS_LEDGER_SH=1

# One requested pattern -> one ACL selector. Only the key-permission prefixes pass
# through; anything else becomes ~<keys>, so a pattern can never carry another ACL
# rule (+@all, on, >password).
grants_acl_selector() {
    local p="$1"
    [[ "$p" =~ ^(%(R|W|RW)~)?~?[^[:space:]%~]+$ ]] || return 1
    case "$p" in
        %R~*|%W~*|%RW~*) printf '%s' "$p" ;;
        *) printf '~%s' "${p#\~}" ;;
    esac
}

# Selectors in force for one service, from `XRANGE <ledger> - +` on stdin: per
# request the newest action decides, and only `approve` keeps its patterns.
# Usage: <xrange output> | grants_in_force <service>
grants_in_force() {
    local svc="$1" p sel
    awk -v want="$svc" '
        /^[0-9]+-[0-9]+$/ { flush(); delete f; next }
        { if (prev != "") { f[prev] = $0; prev = "" } else { prev = $0 } }
        function flush() { if (f["svc"] == want && f["req"] != "") { act[f["req"]] = f["action"]; pat[f["req"]] = f["patterns"] } }
        END { flush(); for (r in act) if (act[r] == "approve") print pat[r] }' |
    tr ' ' '\n' | while IFS= read -r p; do
        [[ -n "$p" ]] || continue
        sel="$(grants_acl_selector "$p")" && printf '%s\n' "$sel"
    done | sort -u
}
