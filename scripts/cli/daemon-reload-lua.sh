#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GNODE_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"

LOADER="${GNODE_ROOT}/scripts/load-valkey-functions.sh"

if [[ ! -x "$LOADER" ]]; then
    echo "Error: load-valkey-functions.sh not found at ${LOADER}" >&2
    exit 1
fi

exec "$LOADER" "$@"
