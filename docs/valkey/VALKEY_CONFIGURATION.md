# gNode ValKey Configuration Guide

## Overview

gNode uses ValKey as its core data store for:
- **Stream-based communication** (unified, health, broadcast streams)
- **Service topology storage** (geometric capability matching)
- **ValKey functions** (server-side Lua scripts for atomic operations)
- **Format definitions** (custom message format schemas)
- **Template storage** (Tera template rendering)

This document describes the production-ready, security-hardened ValKey configuration specifically designed for gNode.

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Security Features](#security-features)
3. [Performance Tuning](#performance-tuning)
4. [Configuration Reference](#configuration-reference)
5. [Monitoring & Maintenance](#monitoring--maintenance)
6. [Troubleshooting](#troubleshooting)

---

## Quick Start

### Automated Setup (Recommended)

```bash
# 1. Navigate to gNode project root
cd /path/to/gNode

# 2. Run smart setup script
./scripts/setup-valkey-smart.sh

# 3. Verify installation
./scripts/valkey-cli-secure.sh PING
# Expected output: PONG
```

The automated setup will:
- Generate a cryptographically secure password
- Install the gNode-optimized configuration
- Create necessary directories with proper permissions
- Start the `valkey-gnode.service` systemd unit
- Test authentication and connectivity

### Manual Setup

```bash
# 1. Generate password
sudo mkdir -p /etc/geodineum/credentials
valkey-cli ACL GENPASS | sudo tee /etc/geodineum/credentials/valkey.password > /dev/null
sudo chmod 600 /etc/geodineum/credentials/valkey.password
sudo chown gnode:gnode /etc/geodineum/credentials/valkey.password

# 2. Copy and customize config
sudo cp daemon/config/valkey-gnode.conf /etc/valkey/valkey.conf

# 3. Update password in config
PASS=$(sudo cat /etc/geodineum/credentials/valkey.password)
sudo sed -i "s/requirepass CHANGE_ME_TO_STRONG_PASSWORD/requirepass $PASS/" /etc/valkey/valkey.conf

# 4. Setup directories
sudo mkdir -p /var/lib/valkey /var/log/valkey
sudo chown -R valkey:valkey /var/lib/valkey /var/log/valkey

# 5. Restart service
sudo systemctl restart valkey-gnode
```

---

## Security Features

### Network Isolation

```conf
bind 127.0.0.1 ::1
protected-mode yes
port 47445
```

**What this means:**
- ValKey **only** accepts connections from localhost
- No external network access possible (even on same LAN)
- Protected mode prevents unauthenticated connections
- Standard port 47445 (change if needed)

### Strong Authentication

```conf
requirepass <32-byte-secure-password>
```

**Password Security:**
- Generated using `ACL GENPASS` (256-bit cryptographic randomness)
- Stored in `/etc/geodineum/credentials/valkey.password` with mode 600 (owner read-only)
- Never committed to version control (`.gitignore` entry required)
- Rotatable without downtime using `CONFIG SET requirepass`

### Command Restrictions

```conf
rename-command FLUSHDB ""
rename-command FLUSHALL ""
rename-command CONFIG ""
rename-command SHUTDOWN SHUTDOWN_GNODE_SECURE
```

**Protection against:**
- Accidental data deletion (`FLUSHDB`, `FLUSHALL` disabled)
- Runtime config tampering (`CONFIG` disabled)
- Graceful shutdown (renamed to `SHUTDOWN_GNODE_SECURE`)

### Data Privacy

```conf
hide-user-data-from-log yes
```

- Prevents passwords, keys, and values from appearing in logs
- Important for compliance (GDPR, HIPAA, etc.)
- Logs only show command names and metadata

---

## Performance Tuning

### Multi-threaded I/O

```conf
io-threads 4
io-threads-do-reads yes
```

**Tuning Guidelines:**

| Server Cores | Recommended io-threads | Expected Throughput |
|--------------|------------------------|---------------------|
| 2-4 cores    | 2-3 threads            | ~500K ops/sec       |
| 4-8 cores    | 4 threads              | ~800K ops/sec       |
| 8-16 cores   | 6 threads              | ~1.2M ops/sec       |
| 16+ cores    | 8 threads              | ~1.5M+ ops/sec      |

**Notes:**
- Includes main thread + I/O threads (4 = 1 main + 3 I/O)
- Diminishing returns beyond 8 threads
- Test with `valkey-benchmark` to find optimal value

### Memory Management

```conf
maxmemory 2gb
maxmemory-policy volatile-lru
```

**gNode Memory Usage Estimates:**

| Services | Topology | Streams | Formats | Total   |
|----------|----------|---------|---------|---------|
| 1K       | ~50MB    | ~100MB  | ~10MB   | ~200MB  |
| 10K      | ~500MB   | ~500MB  | ~50MB   | ~1GB    |
| 50K      | ~2.5GB   | ~1GB    | ~100MB  | ~4GB    |
| 100K     | ~5GB     | ~2GB    | ~200MB  | ~8GB    |

**Eviction Strategy:**
- `volatile-lru` evicts least recently used keys **with TTL**
- Protects critical data (topology, formats) without TTL
- Allows streams (with XTRIM/TTL) to be evicted
- Health metrics naturally expire (30s TTL)

### Persistence Strategy

```conf
# RDB: Point-in-time snapshots
save 900 1      # Save if 1+ changes in 15 minutes
save 300 10     # Save if 10+ changes in 5 minutes
save 60 10000   # Save if 10K+ changes in 1 minute

# AOF: Append-only file
appendonly yes
appendfsync everysec
aof-use-rdb-preamble yes
```

**What persists:**
- Service topology (critical - must survive restarts)
- Format definitions (critical - must survive restarts)
- Template content (important - should persist)
- ValKey functions (reloaded on startup by gNode)
- Streams (ephemeral - trimmed regularly, transient data)
- Health metrics (transient - 30s TTL, rebuilt on daemon start)

**Recovery:**
- Restart time: Fast (~100ms for 10K services with RDB-AOF hybrid)
- Data loss window: <1 second (AOF everysec)
- Crash recovery: Automatic (AOF replay + RDB base)

---

## Configuration Reference

### Full Configuration Sections

#### 1. Network & Security
- **bind**: Network interfaces (127.0.0.1 ::1)
- **protected-mode**: Refuse unauthenticated connections (yes)
- **port**: ValKey port (47445)
- **timeout**: Close idle connections (300s)
- **maxclients**: Max simultaneous connections (1000)

#### 2. Authentication
- **requirepass**: Password for default user (32-byte secure)
- **aclfile**: Optional ACL user file (disabled by default)
- **acllog-max-len**: Track failed auth attempts (256)

#### 3. Data Persistence
- **save**: RDB snapshot triggers (900s/1, 300s/10, 60s/10000)
- **appendonly**: Enable AOF (yes)
- **appendfsync**: AOF durability (everysec)
- **dir**: Data directory (/var/lib/valkey)

#### 4. Memory Management
- **maxmemory**: Memory limit (2gb, adjust as needed)
- **maxmemory-policy**: Eviction strategy (volatile-lru)
- **lazyfree-***: Async memory freeing (all enabled)

#### 5. Performance
- **io-threads**: Multi-threaded I/O (4 threads)
- **io-threads-do-reads**: Enable threaded reads (yes)
- **lua-time-limit**: Script timeout (5000ms)
- **client-output-buffer-limit**: Per-client memory limits

#### 6. Logging
- **loglevel**: Verbosity (notice)
- **logfile**: Log destination (/var/log/valkey/gnode-valkey.log)
- **slowlog-log-slower-than**: Slow query threshold (10ms)

#### 7. Advanced
- **activedefrag**: Memory defragmentation (yes)
- **latency-monitor-threshold**: Latency tracking (100ms)
- **hash/list/set/zset-max-***: Data structure optimizations

---

## Monitoring & Maintenance

### Health Checks

```bash
# Basic connectivity
./scripts/valkey-cli-secure.sh PING

# Server info
./scripts/valkey-cli-secure.sh INFO

# Memory usage
./scripts/valkey-cli-secure.sh INFO memory

# Replication status (if configured)
./scripts/valkey-cli-secure.sh INFO replication

# Client connections
./scripts/valkey-cli-secure.sh CLIENT LIST

# Slow queries
./scripts/valkey-cli-secure.sh SLOWLOG GET 10
```

### Performance Metrics

```bash
# Real-time stats
./scripts/valkey-cli-secure.sh --stat

# Latency doctor
./scripts/valkey-cli-secure.sh --latency
./scripts/valkey-cli-secure.sh LATENCY DOCTOR

# Memory doctor
./scripts/valkey-cli-secure.sh MEMORY DOCTOR

# Benchmark (requires REDISCLI_AUTH for valkey-benchmark)
REDISCLI_AUTH="$(cat /etc/geodineum/credentials/valkey.password)" valkey-benchmark -p 47445 -t get,set -n 100000 -q
```

### Monitoring Key Metrics

| Metric | Command | Healthy Range | Alert If |
|--------|---------|---------------|----------|
| Memory usage | `INFO memory` → `used_memory_human` | <80% of maxmemory | >90% |
| Connected clients | `INFO clients` → `connected_clients` | <800 (of 1000 max) | >900 |
| Ops/sec | `INFO stats` → `instantaneous_ops_per_sec` | <50K | Spikes/drops |
| Evicted keys | `INFO stats` → `evicted_keys` | Growing slowly | Rapid growth |
| Rejected connections | `INFO stats` → `rejected_connections` | 0 | >0 |
| AOF last write status | `INFO persistence` → `aof_last_write_status` | ok | error |

### Log Monitoring

```bash
# Follow logs in real-time
sudo journalctl -u valkey-gnode -f

# Recent errors
sudo journalctl -u valkey-gnode -p err -n 50

# Search for specific pattern
sudo journalctl -u valkey-gnode | grep -i "authentication"

# ValKey log file (if configured)
tail -f /var/log/valkey/gnode-valkey.log
```

### Backup Strategy

```bash
# Manual backup
sudo systemctl stop valkey-gnode
sudo cp -a /var/lib/valkey /backup/valkey-$(date +%Y%m%d)
sudo systemctl start valkey-gnode

# Automated backup (add to cron)
#!/bin/bash
BACKUP_DIR="/backup/valkey"
DATE=$(date +%Y%m%d-%H%M%S)
REDISCLI_AUTH="$(cat /etc/geodineum/credentials/valkey.password)" valkey-cli -p 47445 BGSAVE
sleep 5  # Wait for BGSAVE to complete
cp /var/lib/valkey/gnode-dump.rdb "$BACKUP_DIR/dump-$DATE.rdb"
cp /var/lib/valkey/gnode-appendonly.aof "$BACKUP_DIR/aof-$DATE.aof"
find "$BACKUP_DIR" -name "*.rdb" -mtime +7 -delete  # Keep 7 days
```

---

## Troubleshooting

### Common Issues

#### 1. "Connection refused"

**Symptoms:**
```
Error: Connection refused
```

**Diagnosis:**
```bash
# Check if ValKey is running
sudo systemctl status valkey-gnode

# Check port binding
sudo netstat -tlnp | grep 47445

# Check logs
sudo journalctl -u valkey-gnode -n 50
```

**Solutions:**
- Start service: `sudo systemctl start valkey-gnode`
- Check bind address in config
- Verify firewall rules

---

#### 2. "Authentication required" / "NOAUTH"

**Symptoms:**
```
(error) NOAUTH Authentication required
```

**Diagnosis:**
```bash
# Check if password is set
sudo grep requirepass /etc/valkey/valkey.conf

# Verify password file
cat /etc/geodineum/credentials/valkey.password
```

**Solutions:**
- Use `REDISCLI_AUTH`: `REDISCLI_AUTH="$PASSWORD" valkey-cli -p 47445 PING`
- Or use wrapper: `./scripts/valkey-cli-secure.sh PING`
- Check password matches config
- Regenerate password if lost

---

#### 3. Out of Memory

**Symptoms:**
```
(error) OOM command not allowed when used memory > 'maxmemory'
```

**Diagnosis:**
```bash
# Check memory usage
./scripts/valkey-cli-secure.sh INFO memory | grep used_memory_human
./scripts/valkey-cli-secure.sh INFO memory | grep maxmemory_human

# Check eviction stats
./scripts/valkey-cli-secure.sh INFO stats | grep evicted_keys
```

**Solutions:**
- Increase maxmemory in config
- Reduce data retention (stream TTL, XTRIM)
- Enable more aggressive eviction
- Add more RAM or scale horizontally

---

#### 4. High Latency

**Symptoms:**
- Slow responses (>10ms)
- Timeout errors

**Diagnosis:**
```bash
# Latency monitoring
./scripts/valkey-cli-secure.sh --latency
./scripts/valkey-cli-secure.sh LATENCY DOCTOR

# Slow queries
./scripts/valkey-cli-secure.sh SLOWLOG GET 10

# Check CPU usage
top -p $(pgrep valkey-server)
```

**Solutions:**
- Increase io-threads if CPU allows
- Enable activedefrag
- Optimize slow queries (use pipelining)
- Check disk I/O (AOF fsync)
- Upgrade hardware

---

#### 5. Service Won't Start

**Symptoms:**
```
valkey.service: Main process exited, code=exited, status=1/FAILURE
```

**Diagnosis:**
```bash
# Get detailed logs
sudo journalctl -u valkey-gnode -xe

# Try manual start to see error
sudo -u valkey /usr/local/bin/valkey-server /etc/valkey/valkey.conf

# Check config syntax
valkey-server /etc/valkey/valkey.conf --test-memory 0
```

**Common Causes:**
- Log file permissions: `sudo chown valkey:valkey /var/log/valkey`
- Data directory permissions: `sudo chown valkey:valkey /var/lib/valkey`
- Config file syntax error: Fix and retry
- Port already in use: `sudo netstat -tlnp | grep 47445`

---

## Advanced Topics

### Password Rotation

```bash
# 1. Generate new password
NEW_PASS=$(valkey-cli ACL GENPASS)

# 2. Set new password (hot reload)
./scripts/valkey-cli-secure.sh CONFIG SET requirepass "$NEW_PASS"

# 3. Update password file
echo -n "$NEW_PASS" | sudo tee /etc/geodineum/credentials/valkey.password > /dev/null

# 4. Update config file permanently
sudo sed -i "s/^requirepass .*/requirepass $NEW_PASS/" /etc/valkey/valkey.conf

# 5. Restart gNode daemon with new password
sudo systemctl restart gnode-daemon
```

### Multi-Instance Setup

To run multiple ValKey instances (e.g., production + development):

```bash
# Copy config
sudo cp /etc/valkey/valkey.conf /etc/valkey/valkey-dev.conf

# Modify port and directories
sudo sed -i 's/port 47445/port 6380/' /etc/valkey/valkey-dev.conf
sudo sed -i 's|dir /var/lib/valkey|dir /var/lib/valkey-dev|' /etc/valkey/valkey-dev.conf

# Create systemd service
sudo cp /etc/systemd/system/valkey.service /etc/systemd/system/valkey-dev.service
sudo sed -i 's|valkey.conf|valkey-dev.conf|' /etc/systemd/system/valkey-dev.service

# Start
sudo systemctl daemon-reload
sudo systemctl start valkey-gnode-dev
```

### Replication Setup (High Availability)

```conf
# On replica server: /etc/valkey/valkey.conf
replicaof <master-ip> 47445
masterauth <master-password>
```

```bash
# Check replication status
./scripts/valkey-cli-secure.sh INFO replication
```

---

## References

- **ValKey Official Docs**: https://valkey.io/topics/valkey.conf/
- **gNode Architecture**: See `CLAUDE.md` in project root
- **Security Guide**: https://valkey.io/topics/security/
- **ACL Documentation**: https://valkey.io/topics/acl/
- **Performance Tuning**: https://valkey.io/blog/unlock-one-million-rps/

---

## Quick Reference

### Essential Commands

```bash
# Connection test
./scripts/valkey-cli-secure.sh PING

# Server info
./scripts/valkey-cli-secure.sh INFO

# Memory usage
./scripts/valkey-cli-secure.sh INFO memory | grep used_memory_human

# Client count
./scripts/valkey-cli-secure.sh CLIENT LIST | wc -l

# Stream lengths (gNode)
./scripts/valkey-cli-secure.sh XLEN "default:gnode:unified:default"
./scripts/valkey-cli-secure.sh XLEN "default:gnode:broadcast:global"

# Loaded functions (gNode)
./scripts/valkey-cli-secure.sh FUNCTION LIST

# Restart service
sudo systemctl restart valkey-gnode

# View logs
sudo journalctl -u valkey-gnode -f
```

### Configuration Files

| File | Purpose | Permissions |
|------|---------|-------------|
| `/etc/valkey/valkey.conf` | Main config | 640 (valkey:valkey) |
| `/etc/geodineum/credentials/valkey.password` | Authentication password | 600 (gnode:gnode) |
| `/var/lib/valkey/` | Data directory | 755 (valkey:valkey) |
| `/var/log/valkey/` | Log directory | 755 (valkey:valkey) |
| `/etc/geodineum/components/gnode-daemon/daemon.env` | Daemon tuning | 640 (gnode:gnode) |

---

**Last Updated**: 2025-10-24
**gNode Version**: All versions
**ValKey Version**: 8.0+
