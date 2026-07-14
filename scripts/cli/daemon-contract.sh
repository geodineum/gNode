#!/bin/bash
set -euo pipefail

# `geodineum daemon contract [<extension>]`
#   (no arg)   → the daemon command/function reference + canonical wire format
#                (COMMAND_SCHEMA.md) + a list of available signed-extension
#                contracts.
#   <extension>→ that signed extension's CONTRACT.md (e.g. cms, broker, observe,
#                topo, signals). Extensions are not standalone components — they
#                compile into the daemon via GNODE_EXT_DIR — so their contracts
#                are surfaced through the daemon (their host) rather than as
#                separate `geodineum <component> contract` verbs.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GNODE_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"

# Resolve where signed extensions are deployed: GNODE_EXT_DIR, else the canonical
# prod path, else a sibling pro/gNode next to the gNode repo.
EXT_ROOT="${GNODE_EXT_DIR:-}"
[[ -n "$EXT_ROOT" && -d "$EXT_ROOT" ]] || EXT_ROOT="/opt/geodineum/pro/gNode"
[[ -d "$EXT_ROOT" ]] || EXT_ROOT="$(dirname "$GNODE_ROOT")/pro/gNode"

# Map an extension dir (gNode-CMS) to its short verb (cms).
_ext_short() { basename "$1" | sed 's/^gNode-//' | tr '[:upper:]' '[:lower:]'; }

arg="${1:-}"

if [[ -z "$arg" ]]; then
    CONTRACT="${GNODE_ROOT}/COMMAND_SCHEMA.md"
    if [[ -r "$CONTRACT" ]]; then
        cat "$CONTRACT"
    else
        echo "Error: contract not found at ${CONTRACT}" >&2
        echo "The gNode component may be incompletely deployed." >&2
        exit 1
    fi
    if [[ -d "$EXT_ROOT" ]]; then
        echo
        echo "---"
        echo
        echo "## Signed extensions — \`geodineum daemon contract <name>\`"
        echo
        found=0
        for d in "$EXT_ROOT"/gNode-*/; do
            [[ -d "$d" && -r "${d}CONTRACT.md" ]] || continue
            printf '  - %s\n' "$(_ext_short "$d")"
            found=1
        done
        [[ "$found" -eq 1 ]] || echo "  (none deployed with a CONTRACT.md under ${EXT_ROOT})"
    fi
    exit 0
fi

# An extension was named — print its CONTRACT.md.
name="$(echo "$arg" | tr '[:upper:]' '[:lower:]')"
if [[ -d "$EXT_ROOT" ]]; then
    for d in "$EXT_ROOT"/gNode-*/; do
        [[ -d "$d" ]] || continue
        if [[ "$(_ext_short "$d")" == "$name" ]]; then
            if [[ -r "${d}CONTRACT.md" ]]; then
                cat "${d}CONTRACT.md"
                exit 0
            fi
            echo "Extension '${arg}' has no CONTRACT.md at ${d}" >&2
            exit 1
        fi
    done
fi

echo "Unknown extension '${arg}'. Available under ${EXT_ROOT}:" >&2
for d in "$EXT_ROOT"/gNode-*/; do
    [[ -d "$d" && -r "${d}CONTRACT.md" ]] && printf '  %s\n' "$(_ext_short "$d")" >&2
done
exit 1
