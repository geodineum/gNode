#!/bin/bash
# Stop the GSD daemon

PID_FILE=${PID_FILE:-/var/run/gsd-daemon.pid}

if [ ! -f "$PID_FILE" ]; then
    echo "PID file not found. Daemon may not be running."
    exit 0
fi

PID=$(cat "$PID_FILE")

if [ -z "$PID" ]; then
    echo "No PID found in $PID_FILE"
    exit 1
fi

echo "Stopping GSD daemon with PID $PID..."
kill "$PID" || true

# Wait for process to stop
MAX_WAIT=10
for i in $(seq 1 $MAX_WAIT); do
    if ! ps -p "$PID" > /dev/null; then
        echo "GSD daemon stopped"
        rm -f "$PID_FILE"
        exit 0
    fi
    echo "Waiting for daemon to stop... ($i/$MAX_WAIT)"
    sleep 1
done

# Force kill if still running
echo "Force killing daemon..."
kill -9 "$PID" || true
rm -f "$PID_FILE"
echo "GSD daemon killed"
