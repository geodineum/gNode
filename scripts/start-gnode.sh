#!/bin/bash
# =============================================================================
# gNode Daemon Start Script
# =============================================================================
# Starts the gNode daemon with proper configuration for multi-tenant operation.
# Uses ACL authentication and listens to ALL registered sites and environments.
#
# Usage:
#   ./scripts/start-gnode.sh              # Start with defaults (foreground)
#   ./scripts/start-gnode.sh --background # Start in background
#   ./scripts/start-gnode.sh --debug      # Start with debug logging
#   ./scripts/start-gnode.sh --help       # Show help
#
# For production use systemd:
#   sudo systemctl start gnode-daemon
# =============================================================================

set -euo pipefail  # Exit on error, unset vars, and pipe failures

# Determine script and project directories
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Source environment file if it exists
if [ -f "$PROJECT_ROOT/.env" ]; then
    source "$PROJECT_ROOT/.env"
fi

# Set defaults (can be overridden by .env)
GNODE_DIR="${GNODE_DIR:-$PROJECT_ROOT}"
GNODE_PASSWORD_DIR="${GNODE_PASSWORD_DIR:-$GNODE_DIR/.gnode}"
GNODE_DAEMON_BIN="${GNODE_DAEMON_BIN:-$GNODE_DIR/daemon/target/release/gnode-daemon}"
VALKEY_HOST="${VALKEY_HOST:-127.0.0.1}"
VALKEY_PORT="${VALKEY_PORT:-47445}"

# Default daemon settings
TOPOLOGY_NAMESPACE="${GNODE_TOPOLOGY_NAMESPACE:-geodineum}"
NODE_ID="${GNODE_NODE_ID:-master}"
NODE_TYPE="${GNODE_NODE_TYPE:-general}"
STREAM_PREFIX="${GNODE_STREAM_PREFIX:-gnode}"
DIMENSIONS="${GNODE_DIMENSIONS:-17}"
LOG_LEVEL="${GNODE_LOG_LEVEL:-info}"

# Parse command line arguments
BACKGROUND=false
DEBUG=false
EXTRA_ARGS=()

show_help() {
    cat << EOF
gNode Daemon Start Script

Usage: $0 [OPTIONS]

Options:
  --background, -b    Run daemon in background
  --debug, -d         Enable debug logging
  --node-id ID        Set node ID (default: master)
  --node-type TYPE    Set node type: general|inference|gpu_compute|all (default: general)
  --topology-namespace NS  Set topology namespace (default: geodineum)
  --dimensions N      Set number of dimensions (default: 17)
  --help, -h          Show this help message

Environment Variables (can be set in .env):
  GNODE_TOPOLOGY_NAMESPACE  Topology namespace (default: geodineum)
  GNODE_NODE_ID             Node identifier (default: master)
  GNODE_NODE_TYPE           Node type for routing (default: general)
  GNODE_DIMENSIONS          Number of topology dimensions (default: 17)
  GNODE_LOG_LEVEL           Log level: error|warn|info|debug|trace (default: info)
  VALKEY_HOST             ValKey host (default: 127.0.0.1)
  VALKEY_PORT             ValKey port (default: 47445)

Architecture Notes:
  - Daemon listens to ALL DTAP environments (testing, staging, acceptance, production)
  - Daemon discovers ALL registered sites from topology automatically
  - No site_id or environment needed - daemon is multi-tenant infrastructure
  - Uses shared topology at {namespace}:gnode:topology

Examples:
  $0                           # Start in foreground (master node)
  $0 --background              # Start in background
  $0 --debug                   # Start with debug logging
  $0 --node-id worker1         # Start as worker node
  $0 --node-type inference     # Start as inference-only node

For production, use systemd:
  sudo systemctl start gnode-daemon
  sudo systemctl status gnode-daemon
  journalctl -u gnode-daemon -f
EOF
    exit 0
}

while [[ $# -gt 0 ]]; do
    case $1 in
        --background|-b)
            BACKGROUND=true
            shift
            ;;
        --debug|-d)
            DEBUG=true
            LOG_LEVEL="debug"
            shift
            ;;
        --node-id)
            NODE_ID="$2"
            shift 2
            ;;
        --node-type)
            NODE_TYPE="$2"
            shift 2
            ;;
        --topology-namespace)
            TOPOLOGY_NAMESPACE="$2"
            shift 2
            ;;
        --dimensions)
            DIMENSIONS="$2"
            shift 2
            ;;
        --help|-h)
            show_help
            ;;
        *)
            EXTRA_ARGS+=("$1")
            shift
            ;;
    esac
done

# Verify daemon binary exists
if [ ! -f "$GNODE_DAEMON_BIN" ]; then
    echo "[ERROR] Daemon binary not found at: $GNODE_DAEMON_BIN"
    echo "[INFO] Building daemon..."
    cd "$GNODE_DIR/daemon" && cargo build --release
    if [ ! -f "$GNODE_DAEMON_BIN" ]; then
        echo "[ERROR] Build failed. Cannot start daemon."
        exit 1
    fi
fi

# Verify password file exists
DAEMON_PASSWORD_FILE="$GNODE_PASSWORD_DIR/valkey_daemon.password"
if [ ! -f "$DAEMON_PASSWORD_FILE" ]; then
    echo "[ERROR] ValKey daemon password file not found: $DAEMON_PASSWORD_FILE"
    echo "[INFO] Run ./setup-gnode.sh or scripts/setup-gnode-stack.sh to set up the gNode daemon"
    exit 1
fi

# Read password via env var (not CLI arg) to avoid exposure in `ps aux`
GNODE_REDIS_AUTH=$(cat "$DAEMON_PASSWORD_FILE")

# Guard against placeholder or empty passwords
if [[ "$GNODE_REDIS_AUTH" == "CHANGE_ME_TO_STRONG_PASSWORD" || -z "$GNODE_REDIS_AUTH" ]]; then
    echo "[ERROR] Password file contains placeholder or is empty: $DAEMON_PASSWORD_FILE"
    echo "[INFO] Run ./setup-gnode.sh to generate proper credentials"
    exit 1
fi

export GNODE_REDIS_AUTH

# Check if daemon is already running
if pgrep -f "gnode-daemon.*start" > /dev/null 2>&1; then
    echo "[WARN] gNode daemon appears to be already running"
    echo "[INFO] Use 'pkill -f gnode-daemon' to stop it, or 'systemctl stop gnode-daemon'"
    exit 1
fi

# Build command
DAEMON_CMD=(
    "$GNODE_DAEMON_BIN"
    --redis-user gnode_daemon
    --redis-host "$VALKEY_HOST"
    --redis-port "$VALKEY_PORT"
    --topology-namespace "$TOPOLOGY_NAMESPACE"
    --node-id "$NODE_ID"
    --node-type "$NODE_TYPE"
    --stream-prefix "$STREAM_PREFIX"
    --dimensions "$DIMENSIONS"
    --threads auto
    --max-threads 16
    --log-level "$LOG_LEVEL"
    --initial-batch-size 250
    --max-batch-size 500
)

# Add debug flag if requested
if [ "$DEBUG" = true ]; then
    DAEMON_CMD+=(--debug)
fi

# Add any extra arguments
DAEMON_CMD+=("${EXTRA_ARGS[@]}")

# Add start subcommand
DAEMON_CMD+=(start)

echo "=============================================="
echo "Starting gNode Daemon"
echo "=============================================="
echo "  Topology Namespace: $TOPOLOGY_NAMESPACE"
echo "  Node ID:            $NODE_ID"
echo "  Node Type:          $NODE_TYPE"
echo "  Dimensions:         $DIMENSIONS"
echo "  Log Level:          $LOG_LEVEL"
echo "  ValKey:             $VALKEY_HOST:$VALKEY_PORT"
echo "  Environment:        ALL (multi-tenant)"
echo "=============================================="

if [ "$BACKGROUND" = true ]; then
    echo "[INFO] Starting daemon in background..."
    nohup "${DAEMON_CMD[@]}" > "$GNODE_DIR/logs/gnode-daemon.log" 2>&1 &
    DAEMON_PID=$!
    echo "[INFO] Daemon started with PID: $DAEMON_PID"
    echo "[INFO] Logs: $GNODE_DIR/logs/gnode-daemon.log"
    echo "[INFO] To stop: pkill -f gnode-daemon"

    # Wait a moment and verify it started
    sleep 2
    if ps -p $DAEMON_PID > /dev/null 2>&1; then
        echo "[SUCCESS] Daemon is running"
    else
        echo "[ERROR] Daemon failed to start. Check logs for details."
        exit 1
    fi
else
    echo "[INFO] Starting daemon in foreground (Ctrl+C to stop)..."
    exec "${DAEMON_CMD[@]}"
fi
