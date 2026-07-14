#!/bin/bash
#
# Uninstall gNode daemon systemd service
#

set -euo pipefail  # Exit on error, unset vars, and pipe failures

SERVICE_DEST="/etc/systemd/system/gnode-daemon.service"

echo "=== gNode Daemon Service Uninstallation ==="
echo

# Check if we need sudo
if [ "$EUID" -ne 0 ]; then
    echo "This script must be run with sudo privileges to uninstall systemd services."
    echo "Usage: sudo $0"
    exit 1
fi

# Check if service exists
if [ ! -f "$SERVICE_DEST" ]; then
    echo "gNode daemon service is not installed."
    exit 0
fi

# Stop service if running
if systemctl is-active --quiet gnode-daemon.service; then
    echo "Stopping gNode daemon service..."
    systemctl stop gnode-daemon.service
    echo "✓ Service stopped"
fi

# Disable service
if systemctl is-enabled --quiet gnode-daemon.service; then
    echo "Disabling gNode daemon service..."
    systemctl disable gnode-daemon.service
    echo "✓ Service disabled"
fi

# Remove service file
echo "Removing service file..."
rm -f "$SERVICE_DEST"
echo "✓ Service file removed"

# Reload systemd
echo "Reloading systemd daemon..."
systemctl daemon-reload
systemctl reset-failed
echo "✓ Systemd daemon reloaded"

echo
echo "=== Uninstallation Complete ==="
echo "gNode daemon service has been removed from systemd."
echo
