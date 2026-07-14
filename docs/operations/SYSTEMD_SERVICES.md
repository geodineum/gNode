# gNode Systemd Services Guide

Complete reference for managing gNode and ValKey systemd services.

---

## Table of Contents

- [Service Overview](#service-overview)
- [Quick Reference](#quick-reference)
- [Service Installation](#service-installation)
- [Service Management](#service-management)
- [Log Viewing](#log-viewing)
- [Service Status](#service-status)
- [Troubleshooting](#troubleshooting)
- [Service Configuration](#service-configuration)
- [Advanced Operations](#advanced-operations)

---

## Service Overview

gNode uses two systemd services that work together:

### valkey-gnode.service
**Purpose**: ValKey database server
**Port**: 47445
**Config**: `/etc/valkey/valkey-gnode.conf`
**Data**: `/var/lib/valkey-gnode/`
**Logs**: `journalctl -u valkey-gnode`

**Features**:
- Secure authentication (requirepass)
- Production-optimized configuration
- Auto-restart on failure
- Memory management (2GB default)
- AOF persistence + RDB snapshots

### gnode-daemon.service
**Purpose**: gNode (Geodineum Service Daemon)
**Dependencies**: Requires valkey-gnode.service
**User**: gnode (system user)
**Logs**: `journalctl -u gnode-daemon`

**Features**:
- Starts after ValKey automatically
- Auto-restart on failure (5s delay, 3 burst limit)
- Security hardening (NoNewPrivileges, ProtectSystem)
- Automatic password loading from `/etc/geodineum/credentials/valkey_daemon.password`
- Journald logging integration

---

## Quick Reference

### Common Commands

```bash
# Start services
sudo systemctl start valkey-gnode
sudo systemctl start gnode-daemon

# Stop services
sudo systemctl stop gnode-daemon
sudo systemctl stop valkey-gnode

# Restart services
sudo systemctl restart valkey-gnode
sudo systemctl restart gnode-daemon

# Check status
sudo systemctl status valkey-gnode
sudo systemctl status gnode-daemon

# Enable auto-start on boot
sudo systemctl enable valkey-gnode
sudo systemctl enable gnode-daemon

# Disable auto-start
sudo systemctl disable valkey-gnode
sudo systemctl disable gnode-daemon

# View logs in real-time
sudo journalctl -u valkey-gnode -f
sudo journalctl -u gnode-daemon -f

#status check
./scripts/check-gnode-status.sh
```

---

## Service Installation

### Installing ValKey Service

**Option 1: Production Setup (Recommended)**
```bash
# Automated installation with systemd service
./scripts/setup-valkey-smart.sh
```

This creates:
- `/etc/systemd/system/valkey-gnode.service`
- `/etc/valkey/valkey-gnode.conf`
- `/var/lib/valkey-gnode/` (data directory)
- Password saved to `/etc/geodineum/credentials/valkey.password`

**Option 2: Complete Setup**
```bash
# One-command setup (includes ValKey + gNode)
./setup-gnode.sh --valkey-production
```

### Installing gNode Daemon Service

```bash
# Install gNode as systemd service
sudo ./scripts/install-gnode-service.sh
```

This creates:
- `/etc/systemd/system/gnode-daemon.service`
- Auto-start after ValKey
- Security hardening configuration

**Verify Installation**
```bash
# Check if services are installed
systemctl list-unit-files | grep -E "valkey-gnode|gnode-daemon"

# Should show:
# valkey-gnode.service                enabled
# gnode-daemon.service                enabled
```

---

## Service Management

### Starting Services

**Important**: Always start ValKey before gNode daemon.

```bash
# Start ValKey first
sudo systemctl start valkey-gnode

# Verify ValKey is running
sudo systemctl is-active valkey-gnode
# Output: active

# Then start gNode daemon
sudo systemctl start gnode-daemon

# Verify daemon is running
sudo systemctl is-active gnode-daemon
# Output: active
```

**Start Both Services**
```bash
# Start in correct order
sudo systemctl start valkey-gnode gnode-daemon

# Or let dependency management handle it
sudo systemctl start gnode-daemon
# This automatically starts valkey-gnode.service first
```

### Stopping Services

**Important**: Stop gNode daemon before ValKey to avoid connection errors.

```bash
# Stop gNode daemon first
sudo systemctl stop gnode-daemon

# Then stop ValKey
sudo systemctl stop valkey-gnode
```

**Stop Both Services**
```bash
# Stop in correct order
sudo systemctl stop gnode-daemon valkey-gnode
```

### Restarting Services

```bash
# Restart ValKey (will restart gNode automatically due to dependency)
sudo systemctl restart valkey-gnode

# Restart only gNode daemon
sudo systemctl restart gnode-daemon

# Restart both
sudo systemctl restart valkey-gnode gnode-daemon
```

### Reloading Configuration

```bash
# Reload systemd daemon after editing service files
sudo systemctl daemon-reload

# Then restart affected services
sudo systemctl restart valkey-gnode
sudo systemctl restart gnode-daemon
```

### Auto-Start Configuration

**Enable auto-start on boot**
```bash
sudo systemctl enable valkey-gnode
sudo systemctl enable gnode-daemon
```

**Disable auto-start**
```bash
sudo systemctl disable gnode-daemon
sudo systemctl disable valkey-gnode
```

**Check if enabled**
```bash
systemctl is-enabled valkey-gnode
systemctl is-enabled gnode-daemon
# Output: enabled or disabled
```

---

## Log Viewing

### Real-Time Log Monitoring

**Follow logs (like tail -f)**
```bash
# ValKey logs
sudo journalctl -u valkey-gnode -f

# gNode daemon logs
sudo journalctl -u gnode-daemon -f

# Both services
sudo journalctl -u valkey-gnode -u gnode-daemon -f
```

### Historical Logs

**Last N lines**
```bash
# Last 50 lines
sudo journalctl -u valkey-gnode -n 50
sudo journalctl -u gnode-daemon -n 50

# Last 100 lines
sudo journalctl -u gnode-daemon -n 100
```

**Time-based logs**
```bash
# Today's logs
sudo journalctl -u gnode-daemon --since today

# Last hour
sudo journalctl -u gnode-daemon --since "1 hour ago"

# Last 30 minutes
sudo journalctl -u gnode-daemon --since "30 minutes ago"

# Specific time range
sudo journalctl -u gnode-daemon --since "2025-10-24 14:00:00" --until "2025-10-24 15:00:00"

# Yesterday's logs
sudo journalctl -u gnode-daemon --since yesterday --until today
```

**Filter by priority**
```bash
# Only errors
sudo journalctl -u gnode-daemon -p err

# Warnings and above
sudo journalctl -u gnode-daemon -p warning

# Info and above
sudo journalctl -u gnode-daemon -p info
```

### Log Output Formats

**Verbose output (full details)**
```bash
sudo journalctl -u gnode-daemon -o verbose
```

**JSON output (for parsing)**
```bash
sudo journalctl -u gnode-daemon -o json-pretty
```

**Short output (one line per message)**
```bash
sudo journalctl -u gnode-daemon -o short
```

**Export logs to file**
```bash
# Export to text file
sudo journalctl -u gnode-daemon --since today > gnode-logs-$(date +%Y%m%d).txt

# Export to JSON
sudo journalctl -u gnode-daemon --since today -o json > gnode-logs-$(date +%Y%m%d).json
```

---

## Service Status

### Quick Status Check

```bash
# Brief status
sudo systemctl is-active valkey-gnode
sudo systemctl is-active gnode-daemon

# Detailed status
sudo systemctl status valkey-gnode
sudo systemctl status gnode-daemon
```

### Status

**Using check-gnode-status.sh (Recommended)**
```bash
./scripts/check-gnode-status.sh
```

This shows:
1. Systemd service status (if installed)
2. Process status (PID, memory, CPU)
3. ValKey connection test
4. Daemon internal status
5. ValKey streams (unified, health, broadcast)
6. ValKey functions (loaded count + test)
7. Resource usage
8. Recent logs

### Service Status Details

**Full status output**
```bash
sudo systemctl status gnode-daemon --no-pager --full
```

**Status with lines of log context**
```bash
# Show status with last 20 log lines
sudo systemctl status gnode-daemon -n 20

# Show status with last 50 log lines
sudo systemctl status gnode-daemon -n 50
```

### Service Properties

**View all service properties**
```bash
sudo systemctl show gnode-daemon
sudo systemctl show valkey-gnode
```

**View specific properties**
```bash
# Main PID
sudo systemctl show gnode-daemon -p MainPID

# Memory usage
sudo systemctl show gnode-daemon -p MemoryCurrent

# Active state
sudo systemctl show gnode-daemon -p ActiveState

# Restart count
sudo systemctl show gnode-daemon -p NRestarts
```

---

## Troubleshooting

### Service Won't Start

**Check why service failed**
```bash
# View failure reason
sudo systemctl status gnode-daemon

# View detailed logs
sudo journalctl -u gnode-daemon -n 100

# Check for errors
sudo journalctl -u gnode-daemon -p err --since today
```

**Common issues and fixes**

1. **ValKey not running**
```bash
# Error: "connection refused"
# Fix: Start ValKey first
sudo systemctl start valkey-gnode
sudo systemctl start gnode-daemon
```

2. **Password file missing**
```bash
# Error: "Valkey password not found"
# Fix: Check password file exists
ls -la /etc/geodineum/credentials/valkey_daemon.password

# If missing, run setup
./setup-gnode.sh
```

3. **Daemon binary not found**
```bash
# Error: "gnode-daemon: No such file"
# Fix: Build the daemon
cd /opt/gNode/daemon
cargo build --release
```

4. **Permission denied**
```bash
# Error: "Permission denied" on password file
# Fix: Check file permissions
ls -la /etc/geodineum/credentials/valkey_daemon.password
# Should be: -rw------- owned by gnode:gnode

# Fix permissions
sudo chown gnode:gnode /etc/geodineum/credentials/valkey_daemon.password
sudo chmod 600 /etc/geodineum/credentials/valkey_daemon.password
```

### Service Keeps Restarting

**Check restart loop**
```bash
# View restart count
sudo systemctl show gnode-daemon -p NRestarts

# View recent failures
sudo journalctl -u gnode-daemon --since "10 minutes ago"
```

**Stop restart loop**
```bash
# Stop service
sudo systemctl stop gnode-daemon

# Check logs for root cause
sudo journalctl -u gnode-daemon -n 200

# Fix the issue, then start again
sudo systemctl start gnode-daemon
```

### Service Dependency Issues

**Check dependencies**
```bash
# View what gnode-daemon depends on
systemctl list-dependencies gnode-daemon

# View what depends on valkey-gnode
systemctl list-dependencies --reverse valkey-gnode
```

**Reset failed state**
```bash
# Reset failed services
sudo systemctl reset-failed valkey-gnode
sudo systemctl reset-failed gnode-daemon

# Then try starting again
sudo systemctl start valkey-gnode gnode-daemon
```

### Performance Issues

**Check resource usage**
```bash
# Memory usage
sudo systemctl status gnode-daemon | grep Memory

# CPU usage (via top/htop)
top -p $(systemctl show gnode-daemon -p MainPID --value)

# Detailed process info
ps aux | grep gnode-daemon
```

**Check for memory leaks**
```bash
# Watch memory over time
watch -n 5 'systemctl show gnode-daemon -p MemoryCurrent'
```

### Logs Not Appearing

**Check journal is running**
```bash
sudo systemctl status systemd-journald
```

**Check journal space**
```bash
journalctl --disk-usage

# Clean old logs if needed
sudo journalctl --vacuum-time=7d  # Keep last 7 days
sudo journalctl --vacuum-size=500M  # Keep max 500MB
```

---

## Service Configuration

### Viewing Service Files

**View installed service files**
```bash
sudo systemctl cat valkey-gnode
sudo systemctl cat gnode-daemon
```

**View service file locations**
```bash
systemctl show valkey-gnode -p FragmentPath
systemctl show gnode-daemon -p FragmentPath
```

### Editing Service Files

**Edit service configuration**
```bash
# Edit gNode daemon service
sudo systemctl edit --full gnode-daemon

# Edit ValKey service
sudo systemctl edit --full valkey-gnode
```

**After editing, reload and restart**
```bash
sudo systemctl daemon-reload
sudo systemctl restart gnode-daemon
```

### Service Override Files

**Create override (recommended)**
```bash
# Create drop-in override
sudo systemctl edit gnode-daemon
```

This creates: `/etc/systemd/system/gnode-daemon.service.d/override.conf`

**Example override** (change user):
```ini
[Service]
User=myuser
Group=myuser
```

**View overrides**
```bash
sudo systemctl cat gnode-daemon
# Shows both main file and overrides
```

### Common Configuration Changes

**Change daemon user**
```bash
sudo systemctl edit gnode-daemon
```
Add:
```ini
[Service]
User=youruser
Group=youruser
```

**Change restart policy**
```bash
sudo systemctl edit gnode-daemon
```
Add:
```ini
[Service]
Restart=always
RestartSec=10s
```

**Change resource limits**
```bash
sudo systemctl edit gnode-daemon
```
Add:
```ini
[Service]
LimitNOFILE=100000
MemoryMax=1G
CPUQuota=200%
```

---

## Advanced Operations

### Service State

**Check service load state**
```bash
systemctl show gnode-daemon -p LoadState
# Output: LoadState=loaded
```

**Check if service is masked**
```bash
systemctl is-enabled gnode-daemon
# Output: enabled, disabled, or masked
```

**Mask service (prevent start)**
```bash
sudo systemctl mask gnode-daemon
# Creates symlink to /dev/null

# Unmask
sudo systemctl unmask gnode-daemon
```

### Analyzing Service Startup Time

**Show boot timing**
```bash
systemd-analyze blame | grep -E "valkey-gnode|gnode-daemon"
```

**Show service startup critical chain**
```bash
systemd-analyze critical-chain gnode-daemon
```

### Service Environment

**View service environment variables**
```bash
sudo systemctl show gnode-daemon -p Environment
```

**Set environment variable**
```bash
sudo systemctl edit gnode-daemon
```
Add:
```ini
[Service]
Environment="DEBUG=true"
Environment="LOG_LEVEL=debug"
```

### Service Isolation

**View isolation settings**
```bash
sudo systemctl show gnode-daemon -p PrivateTmp -p ProtectSystem -p ProtectHome
```

These show gNode's security hardening:
- `PrivateTmp=yes` - Isolated /tmp
- `ProtectSystem=strict` - Read-only system directories
- `ProtectHome=read-only` - Limited home access

### Monitoring Multiple Services

**Watch both services**
```bash
watch -n 2 'systemctl status valkey-gnode gnode-daemon --no-pager | head -30'
```

**Check if both are running**
```bash
systemctl is-active valkey-gnode gnode-daemon
# Output: active active (if both running)
```

### Graceful Shutdown

**Stop services in correct order**
```bash
# Create shutdown script
cat > /usr/local/bin/gnode-shutdown.sh << 'EOF'
#!/bin/bash
echo "Stopping gNode services gracefully..."
sudo systemctl stop gnode-daemon
echo "gNode daemon stopped"
sleep 2
sudo systemctl stop valkey-gnode
echo "ValKey stopped"
echo "Shutdown complete"
EOF

sudo chmod +x /usr/local/bin/gnode-shutdown.sh
```

---

## Complete Workflow Examples

### Fresh Installation

```bash
# 1. Install ValKey service
./scripts/setup-valkey-smart.sh

# 2. Build daemon
cd daemon && cargo build --release && cd ..

# 3. Load ValKey functions
./scripts/load-valkey-functions.sh

# 4. Install gNode service
sudo ./scripts/install-gnode-service.sh

# 5. Enable auto-start
sudo systemctl enable valkey-gnode gnode-daemon

# 6. Start services
sudo systemctl start valkey-gnode gnode-daemon

# 7. Verify everything works
./scripts/check-gnode-status.sh
```

### Daily Operations

```bash
# Morning: Check if services are running
./scripts/check-gnode-status.sh

# View overnight logs
sudo journalctl -u gnode-daemon --since yesterday

# Restart if needed
sudo systemctl restart gnode-daemon

# Monitor in real-time
sudo journalctl -u gnode-daemon -f
```

### After Code Changes

```bash
# 1. Stop daemon
sudo systemctl stop gnode-daemon

# 2. Rebuild
cd daemon && cargo build --release && cd ..

# 3. Reload functions (if changed)
./scripts/load-valkey-functions.sh

# 4. Start daemon
sudo systemctl start gnode-daemon

# 5. Check logs for errors
sudo journalctl -u gnode-daemon -n 50
```

### Debugging Session

```bash
# 1. Stop service to run manually
sudo systemctl stop gnode-daemon

# 2. Run daemon in foreground with debug logging
./daemon/target/release/gnode-daemon \
  --redis-auth "$(cat .gnode/valkey_daemon.password)" \
  --log-level debug \
  --debug \
  start

# 3. When done debugging, start service again
# (Press Ctrl+C to stop foreground daemon)
sudo systemctl start gnode-daemon
```

---

## Related Documentation

- **[README.md](../../README.md)** - Main installation guide
- **[daemon/config/gnode-daemon.service](../../daemon/config/gnode-daemon.service)** - gNode service file
- **[daemon/config/valkey-gnode.conf](../../daemon/config/valkey-gnode.conf)** - ValKey configuration

---

## Quick Tips

- **Always checkstatus first**: `./scripts/check-gnode-status.sh`

- **Use journalctl for logs**: richer than log files

- **Follow logs in real-time during debugging**: `sudo journalctl -u gnode-daemon -f`

- **Check service dependencies**: `systemctl list-dependencies gnode-daemon`

- **Reset failed state before retrying**: `sudo systemctl reset-failed gnode-daemon`

- **Use overrides instead of editing service files**: `sudo systemctl edit gnode-daemon`

- **Enable services for auto-start**: `sudo systemctl enable gnode-daemon`

- **Stop gNode before ValKey**: Prevents connection errors

- **Check resource usage**: `systemctl show gnode-daemon -p MemoryCurrent`

- **Export logs for analysis**: `sudo journalctl -u gnode-daemon --since today > logs.txt`

---

**Last Updated**: 2026-03-11
