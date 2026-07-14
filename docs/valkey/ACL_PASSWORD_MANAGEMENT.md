# ValKey ACL Password Management - Definitive Guide

## TL;DR - What Actually Works

```bash
# OK:CORRECT - Always use REDISCLI_AUTH
REDISCLI_AUTH="$(cat /opt/gNode/.gnode/valkey_client_staging_my_app.password)" \
  valkey-cli --user gnode_client_staging_my_app PING

# WRONG:WRONG - --pass flag is unreliable
valkey-cli --user gnode_client_staging_my_app \
  --pass "$(cat /opt/gNode/.gnode/valkey_client_staging_my_app.password)" PING
```

## Password Lifecycle

### 1. Generation (register-site.sh)

```bash
/opt/gNode/scripts/register-site.sh staging_my_app
```

**What it does:**
1. Generates 64-char hex password: `openssl rand -hex 32 | cut -c1-65`
2. Stores in `/opt/gNode/.gnode/valkey_client_staging_my_app.password` (NO newline)
3. Creates ValKey ACL user: `gnode_client_staging_my_app`
4. Sets keyspace: `~error:staging_my_app:*`, `~cache:staging_my_app:*`, etc.
5. Saves to `/etc/valkey/users.acl` (DURABLE - persists across restarts)

### 2. Storage Locations

```
/opt/gNode/.gnode/
├── valkey_daemon.password                      # gNode Daemon (omnipotent)
├── valkey_client.password                      # Legacy shared (DEPRECATED)
└── valkey_client_{service_id}.password            # Per-site (NEW - use this)
```

**Permissions:** `600` (owner read/write only)
**Owner:** `gnode` (system user)
**Format:** 64 hex characters, NO newline

### 3. How Components Access Passwords

#### A. gNode Daemon (Rust)
**File:** `daemon/config/gnode-daemon.service`
```ini
ExecStart=/opt/gNode/daemon/target/release/gnode-daemon \
  --redis-user gnode_daemon \
  --redis-auth "$(cat /opt/gNode/.gnode/valkey_daemon.password)"
```
**Status:** Working

#### B. WordPress/gCore (PHP)
**File:** `/var/www/*/wp-content/mu-plugins/gcore-loader.php`
```php
$site_id = str_replace(['.', '-'], '_', parse_url(get_site_url(), PHP_URL_HOST));
$valkey_user = 'gnode_client_' . $site_id;
$password_file = "/opt/gNode/.gnode/valkey_client_{$site_id}.password";
$valkey_password = trim(file_get_contents($password_file));

putenv("VALKEY_USER=$valkey_user");
putenv("VALKEY_AUTH=$valkey_password");
```
**Status:** Working

#### C. CLI Scripts (valkey-cli-secure.sh)
**File:** `/opt/gNode/scripts/valkey-cli-secure.sh`
```bash
VALKEY_USER="${VALKEY_USER:-gnode_client}"
PASSWORD_FILE="$CONFIG_DIR/valkey_${VALKEY_USER#gnode_}.password"
VALKEY_PASSWORD=$(cat "$PASSWORD_FILE")

REDISCLI_AUTH="$VALKEY_PASSWORD" valkey-cli --user "$VALKEY_USER" "$@"
```
**Status:** Working

#### D. Manual CLI Usage
```bash
# Export password first
export REDISCLI_AUTH="$(cat /opt/gNode/.gnode/valkey_client_staging_my_app.password)"

# Then run commands
valkey-cli --user gnode_client_staging_my_app KEYS "error:staging_my_app:*"
valkey-cli --user gnode_client_staging_my_app SET "cache:staging_my_app:test" "value"
```

## Why `--pass` Flag Fails

**ValKey/Redis CLI has a known limitation:**
The `--pass` flag doesn't properly handle:
- Long passwords (>32 chars)
- Hex-only passwords
- Special characters in certain contexts

**Solution:** Always use `REDISCLI_AUTH` environment variable.

## Verification Tests

### Test 1: Check ACL user exists
```bash
VALKEY_USER=gnode_daemon ./scripts/valkey-cli-secure.sh ACL GETUSER gnode_client_staging_my_app
```

### Test 2: Verify password hash matches
```bash
# Get hash from ACL
VALKEY_USER=gnode_daemon ./scripts/valkey-cli-secure.sh ACL GETUSER gnode_client_staging_my_app | grep -A1 "passwords"

# Hash password file
cat /opt/gNode/.gnode/valkey_client_staging_my_app.password | sha256sum
```

### Test 3: Test authentication
```bash
REDISCLI_AUTH="$(cat /opt/gNode/.gnode/valkey_client_staging_my_app.password)" \
  valkey-cli --user gnode_client_staging_my_app PING
# Expected: PONG
```

### Test 4: Test keyspace isolation
```bash
REDISCLI_AUTH="$(cat /opt/gNode/.gnode/valkey_client_staging_my_app.password)" \
  valkey-cli --user gnode_client_staging_my_app SET "error:staging_my_app:test" "allowed"
# Expected: OK

REDISCLI_AUTH="$(cat /opt/gNode/.gnode/valkey_client_staging_my_app.password)" \
  valkey-cli --user gnode_client_staging_my_app SET "error:other_site:test" "denied"
# Expected: NOPERM No permissions to access a key
```

### Test 5: Verify WordPress is using ValKey
```bash
# Check if WordPress wrote data
VALKEY_USER=gnode_daemon ./scripts/valkey-cli-secure.sh KEYS "*:staging_my_app:*"
```

## Troubleshooting

### Problem: AUTH failed: WRONGPASS

**Cause 1:** Multiple passwords in ACL user
```bash
# Check password count
VALKEY_USER=gnode_daemon ./scripts/valkey-cli-secure.sh ACL GETUSER gnode_client_staging_my_app | grep -A5 "passwords"

# Fix: Re-run creation script (now uses resetpass)
/opt/gNode/scripts/register-site.sh staging_my_app
```

**Cause 2:** Password file has newline
```bash
# Check file size (should be 64 bytes)
wc -c /opt/gNode/.gnode/valkey_client_staging_my_app.password

# Fix: Script now uses echo -n (already fixed)
```

**Cause 3:** Using `--pass` flag instead of `REDISCLI_AUTH`
```bash
# Don't do this:
valkey-cli --user ... --pass "..."

# Do this instead:
REDISCLI_AUTH="..." valkey-cli --user ...
```

### Problem: NOPERM errors in WordPress

**Cause:** WordPress using wrong keyspace prefix

**Check:**
```bash
tail -100 /var/www/*/wp-content/gcore-logs/gcore-core.log | grep NOPERM
```

**Expected keyspace:**
- OK: `error:staging_my_app:*`
- WRONG: `error:default:*` (wrong site_id)

**Fix:** Verify gcore-loader.php sets correct `site_id` in config

## ACL Persistence

**ValKey ACL is DURABLE:**
1. Changes saved to: `/etc/valkey/users.acl`
2. Loaded on startup via: `/etc/valkey/valkey-gnode.conf`
3. Survives restarts: Yes
4. Survives system reboot: Yes

**To persist changes:**
```bash
VALKEY_USER=gnode_daemon ./scripts/valkey-cli-secure.sh ACL SAVE
```

## Quick Reference Card

| Task | Command |
|------|---------|
| Create site ACL | `/opt/gNode/scripts/register-site.sh {service_id}` |
| Test auth | `REDISCLI_AUTH="$(cat /opt/gNode/.gnode/valkey_client_{service_id}.password)" valkey-cli --user gnode_client_{service_id} PING` |
| List users | `VALKEY_USER=gnode_daemon ./scripts/valkey-cli-secure.sh ACL LIST` |
| Check user perms | `VALKEY_USER=gnode_daemon ./scripts/valkey-cli-secure.sh ACL GETUSER {user}` |
| View site data | `VALKEY_USER=gnode_daemon ./scripts/valkey-cli-secure.sh KEYS "*:{service_id}:*"` |

## Migration from Legacy

**Old system:** Single `gnode_client` user for all sites
**New system:** Per-site `gnode_client_{service_id}` users

**Migration steps:**
1. Create per-site ACL user: `/opt/gNode/scripts/register-site.sh {service_id}`
2. WordPress auto-detects and uses new user (gcore-loader.php)
3. Verify: Check logs for `"site_id":"{service_id}"`
4. After all sites migrated: Deprecate `gnode_client`

## Security Benefits

**Before:** Single password → all sites accessible
**After:** Per-site passwords → isolated keyspaces

**Attack surface reduction:**
- Client compromise: 1 site affected (not all sites)
- Keyspace isolation: `~error:{service_id}:*` only
- Audit trail: Per-user ACL logs

## Summary

- **ACL is durable** - passwords persist across restarts
- **All components work** - WordPress, gNode daemon, scripts
- **Isolation works** - verified with NOPERM tests
- **Manual CLI quirk** - must use `REDISCLI_AUTH`, not `--pass`

**Remember:** ALWAYS use `REDISCLI_AUTH` environment variable for authentication.
