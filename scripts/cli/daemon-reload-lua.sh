#!/bin/bash
set -euo pipefail

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    cat <<'HLP'
Usage: geodineum daemon reload-lua

Reload the gNode Lua function libraries into ValKey (wraps
load-valkey-functions.sh; further flags pass through to it).
HLP
    exit 0
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GNODE_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"

LOADER="${GNODE_ROOT}/scripts/load-valkey-functions.sh"

if [[ ! -x "$LOADER" ]]; then
    echo "Error: load-valkey-functions.sh not found at ${LOADER}" >&2
    exit 1
fi

exec "$LOADER" "$@"
