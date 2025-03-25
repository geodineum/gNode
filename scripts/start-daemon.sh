#!/bin/bash
# Start the GSD daemon

GSD_HOME=$(dirname "$(dirname "$(readlink -f "$0")")")
GSD_DAEMON="$GSD_HOME/daemon/target/release/gsd-daemon"

if [ ! -f "$GSD_DAEMON" ]; then
    echo "GSD daemon binary not found. Please build it first with ./build.sh"
    exit 1
fi

# Default settings
REDIS_HOST=${REDIS_HOST:-127.0.0.1}
REDIS_PORT=${REDIS_PORT:-6379}
SITE_ID=${SITE_ID:-default}
NODE_ID=${NODE_ID:-default}
STREAM_PREFIX=${STREAM_PREFIX:-gsd}
DIMENSIONS=${DIMENSIONS:-8}
LOG_FILE=${LOG_FILE:-/var/log/gsd/daemon.log}
PID_FILE=${PID_FILE:-/var/run/gsd-daemon.pid}

# Create log directory if it doesn't exist
mkdir -p "$(dirname "$LOG_FILE")"

# Start the daemon
echo "Starting GSD daemon..."
RUST_LOG=info "$GSD_DAEMON" \
    --redis-host "$REDIS_HOST" \
    --redis-port "$REDIS_PORT" \
    --site-id "$SITE_ID" \
    --node-id "$NODE_ID" \
    --stream-prefix "$STREAM_PREFIX" \
    --dimensions "$DIMENSIONS" \
    --debug > "$LOG_FILE" 2>&1 &

echo $! > "$PID_FILE"
echo "GSD daemon started with PID $(cat "$PID_FILE")"
