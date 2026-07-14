#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GNODE_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"
DAEMON_DIR="${GNODE_ROOT}/daemon"

restart=false
while [[ $# -gt 0 ]]; do
    case "$1" in
        --restart) restart=true; shift ;;
        --help|-h)
            echo "Usage: geodineum daemon rebuild [--restart]"
            echo "  --restart    Restart gnode-daemon after successful build"
            exit 0
            ;;
        *) echo "Unknown option: $1" >&2; exit 2 ;;
    esac
done

if [[ ! -d "$DAEMON_DIR" ]]; then
    echo "Error: daemon source not found at ${DAEMON_DIR}" >&2
    exit 1
fi

# Route through the canonical build script so signed extensions (CMS et al.)
# are discovered + staged. A bare `cargo build` here builds lean and silently
# drops every extension. --force because an explicit rebuild must not be
# short-circuited by the freshness check.
echo "Building gNode daemon..."
bash "${GNODE_ROOT}/scripts/build.sh" --force || { echo "Build failed" >&2; exit 1; }
echo "Build complete: ${DAEMON_DIR}/target/release/gnode-daemon"

if [[ "$restart" == "true" ]]; then
    echo "Restarting gnode-daemon..."
    systemctl restart gnode-daemon
    echo "Daemon restarted"
fi
