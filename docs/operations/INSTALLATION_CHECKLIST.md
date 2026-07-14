# gNode Installation Checklist

Complete checklist for installing gNode on a fresh system.

## Pre-Installation Checklist

### System Requirements
- [ ] Linux OS (Ubuntu 20.04+, Debian 11+, or equivalent)
- [ ] Minimum 512MB RAM (2GB+ recommended)
- [ ] 500MB+ free disk space
- [ ] Root/sudo access

### Required Software
- [ ] Git installed
- [ ] OpenSSL installed
- [ ] Build tools (gcc, make, pkg-config, libssl-dev)
- [ ] Rust 1.50+ (or will be auto-installed by setup script)

### User Permissions
- [ ] User has sudo privileges

## Installation Steps

### Step 1: Clone Repository
```bash
git clone https://github.com/geodineum/gNode.git
cd gNode
```
- [ ] Repository cloned successfully
- [ ] Changed to gNode directory

### Step 2: Run Initialization Script
```bash
./setup-gnode.sh
```

**What happens during initialization:**
- [ ] Prerequisites checked (Rust, OpenSSL, Git)
- [ ] **SUDO PASSWORD REQUESTED** for permission setup
- [ ] Log and run directories created with proper permissions
- [ ] gNode daemon built (release mode)
- [ ] ValKey configured with authentication (systemd service)
- [ ] Secure password generated and stored in `/etc/geodineum/credentials/`
- [ ] 237+ ValKey functions loaded (22 base libraries)
- [ ] Systemd services installed and started
- [ ] Connectivity tests run successfully

### Step 3: Activate gNode Group Membership

**CRITICAL**: You must activate the `gnode` group before using gNode!

**Option A: Activate for current session only**
```bash
newgrp gnode
```
- [ ] Command run successfully
- [ ] `groups` command shows `gnode` in output

**Option B: Activate permanently** (recommended)
```bash
# Log out and log back in
# Then verify:
groups | grep gnode
```
- [ ] Logged out and back in
- [ ] `gnode` group appears in groups output

### Step 4: Verify Installation

```bash
# 1. Check ValKey is running
systemctl status valkey-gnode
```
- [ ] ValKey service is active

```bash
# 2. Test ValKey authentication
./scripts/valkey-cli-secure.sh PING
```
- [ ] Returns "PONG"

```bash
# 3. Count loaded functions
./scripts/valkey-cli-secure.sh FUNCTION LIST | grep -c GNODE_
```
- [ ] Returns 237+

```bash
# 4. Check daemon binary
ls -lh daemon/target/release/gnode-daemon
```
- [ ] File exists and is 10-15MB

```bash
# 5. Verify permissions
ls -la logs/
ls -la run/
```
- [ ] Directories owned by `gnode:gnode`
- [ ] Permissions are `drwxrwxr-x` (775)

```bash
# 6. Check group membership
groups | grep gnode
```
- [ ] `gnode` appears in output

### Step 5: Start Daemon

```bash
sudo systemctl start gnode-daemon
```
- [ ] Daemon starts without errors
- [ ] Log file created (view with `journalctl -u gnode-daemon`)

```bash
# Verify daemon is running
ps aux | grep gnode-daemon
```
- [ ] Process appears in output
- [ ] RSS memory is ~4.6MB

### Step 6: Test Basic Functionality

```bash
# Check daemon status
./scripts/check-gnode-status.sh
```
- [ ] Daemon reported as running
- [ ] Stream exists: `{default}:gnode:unified:default`
- [ ] Consumer groups configured

```bash
# Send test command
./scripts/valkey-cli-secure.sh XADD "default:gnode:unified:production" '*' \
  id "test-1" cmd "ping" params '{}'
```
- [ ] Message ID returned
- [ ] No errors in daemon logs (`journalctl -u gnode-daemon`)

## Post-Installation Checklist

### Security
- [ ] Password files have 600 permissions: `ls -la /etc/geodineum/credentials/`
- [ ] `.gnode` directory is in `.gitignore`
- [ ] ValKey only binds to localhost (verify: `ss -tlnp | grep 47445`)

### Files Created
- [ ] `/etc/geodineum/credentials/valkey_daemon.password` - Daemon ACL password (600)
- [ ] `daemon/target/release/gnode-daemon` - Compiled daemon binary
- [ ] `logs/` - Log directory (owned by gnode:gnode)
- [ ] `run/` - Runtime files directory (owned by gnode:gnode)

### Configuration
- [ ] Credentials centralized at `/etc/geodineum/credentials/`
- [ ] Always use `REDISCLI_AUTH` env var for CLI auth (never `--pass` flag)

### Optional: Run Test Suite
```bash
# Test all ValKey functions
./scripts/test-all-valkey-functions.sh
```
- [ ] Tests pass successfully

```bash
# Check gNode status
./scripts/check-gnode-status.sh
```
- [ ] Status check completes without errors

## Troubleshooting Checklist

### If Installation Fails

#### Permission Issues
- [ ] Run: `./setup-gnode.sh` (handles permissions automatically)
- [ ] Verify gnode group exists: `getent group gnode`
- [ ] User in gnode group: `groups | grep gnode`
- [ ] Activated group: `newgrp gnode` or logout/login

#### Build Failures
- [ ] Rust installed: `cargo --version`
- [ ] Build dependencies installed: `sudo apt-get install build-essential pkg-config libssl-dev`
- [ ] Sufficient disk space: `df -h`
- [ ] Clean build: `cd daemon && cargo clean && cargo build --release`

#### ValKey Issues
- [ ] Service running: `systemctl status valkey-gnode`
- [ ] Password file exists: `ls -la /etc/geodineum/credentials/valkey_daemon.password`
- [ ] Can connect: `./scripts/valkey-cli-secure.sh PING`
- [ ] Port 47445 not already in use: `ss -tlnp | grep 47445`

#### Function Loading Issues
- [ ] Functions loaded: `./scripts/valkey-cli-secure.sh FUNCTION LIST`
- [ ] Reload if needed: `./scripts/load-valkey-functions.sh`
- [ ] Check for errors: `journalctl -u valkey-gnode | grep -i error`

## Common Mistakes

### Mistake 1: Not Running with Sudo
**Problem**: Permission setup script fails
**Solution**: The init script automatically calls sudo when needed

### Mistake 2: Not Activating gNode Group
**Problem**: "Permission denied" when writing logs
**Solution**: Run `newgrp gnode` or logout/login after installation

### Mistake 3: Forgetting Password Location
**Problem**: Can't connect to ValKey
**Solution**: Password is in `/etc/geodineum/credentials/valkey_daemon.password` (use `REDISCLI_AUTH` env var or `./scripts/valkey-cli-secure.sh`)

### Mistake 4: Running Daemon Before Group Activation
**Problem**: Logs not written, PID file fails
**Solution**: Always run `newgrp gnode` first, or logout/login

## Success Indicators

If all these are true, your installation is complete:

- `systemctl status valkey-gnode` shows service active
- `systemctl status gnode-daemon` shows service active
- `ps aux | grep gnode-daemon` shows process running (~4.6MB RSS)
- `./scripts/check-gnode-status.sh` reports healthy status
- Test command via XADD succeeds

## Next Steps After Installation

1. **Read Documentation**
   - [ ] Read `CLAUDE.md` for architecture details
   - [ ] Check `COMMAND_SCHEMA.md` for command reference

2. **Integrate with Application**
   - [ ] Install gNode-Client: `composer require geodineum/gnode-client`
   - [ ] Configure application with credentials from `/etc/geodineum/credentials/`
   - [ ] Test basic commands from application

3. **Production Preparation**
   - [ ] Configure monitoring and logging
   - [ ] Set up automated backups of ValKey data (`./scripts/backup-valkey.sh`)

4. **Performance Tuning**
   - [ ] Adjust thread count if needed
   - [ ] Configure batch sizes for workload
   - [ ] Monitor resource usage
   - [ ] Run benchmarks with production data

## Quick Reference Commands

```bash
# Start daemon
sudo systemctl start gnode-daemon

# Stop daemon
sudo systemctl stop gnode-daemon

# Check status
./scripts/check-gnode-status.sh

# View logs
sudo journalctl -u gnode-daemon -f

# Test connection
./scripts/valkey-cli-secure.sh PING

# Reload functions
./scripts/load-valkey-functions.sh

# Full setup/repair
./setup-gnode.sh
```

## Support Resources

- **Architecture Guide**: `CLAUDE.md`
- **Command Reference**: `COMMAND_SCHEMA.md`
- **Permissions Guide**: `docs/operations/PERMISSIONS.md`
- **GitHub Issues**: https://github.com/geodineum/gNode/issues

---

**Remember**: The most common issue is forgetting to activate the `gnode` group membership. Always run `newgrp gnode` after installation or logout/login for permanent effect!
