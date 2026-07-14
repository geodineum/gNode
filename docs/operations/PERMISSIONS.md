# gNode Permission System

This document explains how the gNode daemon's permission system works and how to properly set it up.

## Overview

gNode uses a dedicated user and group (`gnode:gnode`) for security and proper permission management. This ensures:

1. Proper isolation of the daemon process
2. Security through principle of least privilege
3. Consistent access to logs and runtime files
4. Multi-user access via group membership

## Setup Instructions

### One-time Setup (as root/sudo)

To set up the gNode permission system correctly, run the main setup script:

```bash
./setup-gnode.sh
```

This script handles all permissions automatically:
- Sets proper ownership and permissions on logs and run directories
- Ensures your user has access to all required files
- Installs and configures systemd services

### Verifying Setup

To verify the setup worked correctly, run:

```bash
# Check if you are in the gnode group
groups | grep gnode

# Verify permissions on log and run directories
ls -la logs/
ls -la run/
```

## Common Issues

### Permission Denied Errors

If you see `Permission denied` errors when starting or stopping the daemon:

1. Make sure you've run `./setup-gnode.sh`
2. Verify the systemd service is properly installed: `systemctl status gnode-daemon`
3. Check file permissions in `.gnode/` directory

### Daemon Can't Write to Logs

If the daemon cannot write to log files:

1. Verify ownership of log directory: `ls -la logs/`
2. Ensure permissions are set correctly: `sudo chmod -R 750 logs/`
3. Check if your user is in the gnode group: `groups | grep gnode` 

### Stopping a Daemon Started by Another User

If you need to stop a daemon started by another user:

```bash
# View running daemon processes
ps aux | grep gnode-daemon

# Stop with sudo (if needed)
sudo kill <PID>
```

## Best Practices

1. **Always run the setup script first**: `./setup-gnode.sh`
2. **Use systemd for service management**: `systemctl start/stop gnode-daemon`
3. **Check status with**: `./scripts/check-gnode-status.sh`
4. **Avoid running as root**: The daemon runs under its configured user

## Technical Details

The permission model works as follows:

- Directories:
  - `logs/`: owned by `gnode:gnode` with `750` permissions
  - `run/`: owned by `gnode:gnode` with `750` permissions

- User membership:
  - Regular users are added to the `gnode` group
  - Group membership provides write access to logs and run directories

- Daemon execution:
  - The daemon runs as the current user (in gnode group)
  - Writes to logs and run directories are permitted via group permissions
  - PID files are stored with proper access control

## Fallback Mechanisms

The scripts include several fallback mechanisms:

1. If log directory isn't writable, logs go to `/tmp/gnode-daemon.log`
2. If PID file location isn't writable, PID is stored in `/tmp/gnode-daemon.pid`
3. Stop script will check multiple locations for PID files