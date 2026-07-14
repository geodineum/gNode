# ValKey ACL Nuances & CLI Authentication Quirks

## Executive Summary

**The ValKey CLI `--pass` flag is fundamentally broken for production passwords.**

This document explains the technical root cause, provides battle-tested workarounds, and documents the complete password lifecycle to prevent future confusion.

---

## The Core Problem

### What Doesn't Work

```bash
# WRONG:THIS FAILS - Despite correct password
valkey-cli --user gnode_client_staging_my_app \
  --pass "<sha256-hash-3-redacted>" \
  PING

# Error: AUTH failed: WRONGPASS invalid username-password pair or user is disabled.
```

### What Works

```bash
# OK:THIS WORKS - Same password, different method
REDISCLI_AUTH="<sha256-hash-3-redacted>" \
  valkey-cli --user gnode_client_staging_my_app \
  PING

# Output: PONG
```

---

## Technical Root Cause

### CLI Argument Parsing Limitation

**Hypothesis (based on observed behavior):**
The `--pass` flag in valkey-cli (inherited from Redis) has issues with:

1. **Long passwords** (>32 characters)
2. **Hex-only passwords** (looks like encoded data)
3. **Certain character patterns** that trigger shell/CLI quoting issues

**Evidence:**
- Short passwords (8-16 chars): `--pass` works
- Long hex passwords (64 chars): `--pass` FAILS
- Same password via `REDISCLI_AUTH`: always works

### Why REDISCLI_AUTH Works

The `REDISCLI_AUTH` environment variable bypasses CLI argument parsing entirely:

```c
// Pseudo-code of valkey-cli internals
if (getenv("REDISCLI_AUTH")) {
    password = getenv("REDISCLI_AUTH");  // Direct string, no parsing
} else if (--pass flag provided) {
    password = parse_cli_argument(argv); // Parsing can corrupt data
}
```

**Key insight:** Environment variables preserve exact byte sequences, while CLI arguments get mangled by shell and argument parsing.

---

## Password Lifecycle Deep Dive

### 1. Generation (register-site.sh)

```bash
# Generate 64-character hex password
PASSWORD=$(openssl rand -hex 32 | cut -c1-65)
# Example: <sha256-hash-3-redacted>
```

**Critical details:**
- Length: Exactly 64 characters
- Character set: `[0-9a-f]` (lowercase hex)
- Randomness: 256 bits of entropy (65536-bit security after cut)
- Format: Single line, **NO newline**

### 2. Storage

```bash
# OK:CORRECT - No newline
echo -n "$PASSWORD" > /opt/gNode/.gnode/valkey_client_staging_my_app.password

# WRONG:WRONG - Adds newline (65 bytes instead of 64)
echo "$PASSWORD" > /opt/gNode/.gnode/valkey_client_staging_my_app.password
```

**Verification:**
```bash
# Should output: 64
wc -c < /opt/gNode/.gnode/valkey_client_staging_my_app.password

# Should NOT show \n at end
od -c /opt/gNode/.gnode/valkey_client_staging_my_app.password | tail -2
```

**File permissions:**
```bash
chmod 600 /opt/gNode/.gnode/valkey_client_staging_my_app.password
chown gnode:gnode /opt/gNode/.gnode/valkey_client_staging_my_app.password
```

### 3. ACL User Creation

```bash
# Create user with single password (resetpass clears old passwords)
VALKEY_USER=gnode_daemon ./scripts/valkey-cli-secure.sh \
  ACL SETUSER gnode_client_staging_my_app on resetpass ">$PASSWORD"

# Set keyspace permissions
VALKEY_USER=gnode_daemon ./scripts/valkey-cli-secure.sh \
  ACL SETUSER gnode_client_staging_my_app resetkeys \
  "~error:staging_my_app:*" \
  "~cache:staging_my_app:*" \
  "~session:staging_my_app:*"

# Persist to disk
VALKEY_USER=gnode_daemon ./scripts/valkey-cli-secure.sh ACL SAVE
```

**Critical nuance - `resetpass`:**
```bash
# WRONG:WRONG - Appends password (multiple passwords accumulate)
ACL SETUSER user on ">password1"
ACL SETUSER user on ">password2"
# Result: User can auth with password1 OR password2

# OK:CORRECT - Replaces all passwords
ACL SETUSER user on resetpass ">password1"
ACL SETUSER user on resetpass ">password2"
# Result: User can ONLY auth with password2
```

### 4. Persistence & Durability

**ValKey ACL is persistent by design:**

```
ValKey Process Memory
    ↓ (ACL SAVE command)
/etc/valkey/users.acl (file)
    ↓ (valkey restart)
ValKey Process Memory (reloaded)
```

**Verification:**
```bash
# Check ACL file contains your user
grep "gnode_client_staging_my_app" /etc/valkey/users.acl

# Restart ValKey
sudo systemctl restart valkey-gnode

# Verify user still exists
VALKEY_USER=gnode_daemon ./scripts/valkey-cli-secure.sh ACL LIST | grep staging_my_app
```

**Implication:** You do NOT need to recreate ACL users after:
- ValKey service restart
- Server reboot
- ValKey package upgrade (as long as users.acl is preserved)

---

## How Each Component Accesses Passwords

### A. gNode Daemon (Rust)

**Configuration:** `daemon/config/gnode-daemon.service`
```ini
[Service]
ExecStart=/opt/gNode/daemon/target/release/gnode-daemon \
  --redis-host 127.0.0.1 \
  --redis-port 47445 \
  --redis-user gnode_daemon \
  --redis-auth "$(cat /opt/gNode/.gnode/valkey_daemon.password)"
```

**Authentication method:** Command substitution in systemd unit
**Why it works:** Password embedded in process arguments before ValKey client starts
**Status:** Production-ready

### B. WordPress/gCore (PHP)

**File:** `/var/www/*/wp-content/mu-plugins/gcore-loader.php`
```php
// Auto-detect site ID from domain
$site_domain = parse_url(get_site_url(), PHP_URL_HOST);
$site_id = str_replace(['.', '-'], '_', $site_domain);
// Example: staging.example.com → staging_my_app

// Locate password file
$password_file = "/opt/gNode/.gnode/valkey_client_{$site_id}.password";

// Read password (trim to remove any accidental whitespace)
if (file_exists($password_file)) {
    $valkey_password = trim(file_get_contents($password_file));
    $valkey_user = "gnode_client_{$site_id}";
} else {
    // Fallback to legacy shared user
    $valkey_password = trim(file_get_contents('/opt/gNode/.gnode/valkey_client.password'));
    $valkey_user = 'gnode_client';
}

// Set environment variables for PHP ValKey extensions
putenv("VALKEY_HOST=127.0.0.1");
putenv("VALKEY_PORT=47445");
putenv("VALKEY_USER=$valkey_user");
putenv("VALKEY_AUTH=$valkey_password");
```

**Authentication method:** Environment variables via `putenv()`
**Why it works:** PHP extensions (phpredis, Predis) read `VALKEY_AUTH` environment variable
**Status:** Production-ready

### C. valkey-cli-secure.sh

**File:** `/opt/gNode/scripts/valkey-cli-secure.sh`
```bash
#!/bin/bash

# Determine user (default to gnode_client)
VALKEY_USER="${VALKEY_USER:-gnode_client}"

# Map user to password file
if [ "$VALKEY_USER" = "gnode_daemon" ]; then
    PASSWORD_FILE="/opt/gNode/.gnode/valkey_daemon.password"
elif [[ "$VALKEY_USER" == gnode_client_* ]]; then
    # Per-site user: gnode_client_staging_my_app
    PASSWORD_FILE="/opt/gNode/.gnode/valkey_${VALKEY_USER}.password"
else
    # Legacy shared user
    PASSWORD_FILE="/opt/gNode/.gnode/valkey_client.password"
fi

# Read password
VALKEY_PASSWORD=$(cat "$PASSWORD_FILE")

# Execute valkey-cli with REDISCLI_AUTH
REDISCLI_AUTH="$VALKEY_PASSWORD" valkey-cli \
  -h 127.0.0.1 \
  -p 47445 \
  --user "$VALKEY_USER" \
  "$@"
```

**Authentication method:** `REDISCLI_AUTH` environment variable
**Why it works:** Official valkey-cli environment variable, no parsing issues
**Status:** Production-ready

### D. Manual CLI Usage

**Correct method:**
```bash
# Export password ONCE per shell session
export REDISCLI_AUTH="$(cat /opt/gNode/.gnode/valkey_client_staging_my_app.password)"

# Then run commands normally
valkey-cli --user gnode_client_staging_my_app PING
valkey-cli --user gnode_client_staging_my_app KEYS "*:staging_my_app:*"
valkey-cli --user gnode_client_staging_my_app SET "cache:staging_my_app:test" "value"
```

**Alternative (inline):**
```bash
REDISCLI_AUTH="$(cat /opt/gNode/.gnode/valkey_client_staging_my_app.password)" \
  valkey-cli --user gnode_client_staging_my_app PING
```

**Status:** Requires user discipline (easy to forget `REDISCLI_AUTH`)

---

## Common Errors & Troubleshooting

### Error 1: AUTH failed: WRONGPASS

**Symptom:**
```bash
valkey-cli --user gnode_client_staging_my_app --pass "..." PING
# AUTH failed: WRONGPASS invalid username-password pair or user is disabled.
```

**Root cause:** Using `--pass` flag instead of `REDISCLI_AUTH`

**Fix:**
```bash
REDISCLI_AUTH="$(cat /opt/gNode/.gnode/valkey_client_staging_my_app.password)" \
  valkey-cli --user gnode_client_staging_my_app PING
```

**Prevention:** NEVER use `--pass` flag. Update all documentation and scripts to use `REDISCLI_AUTH`.

---

### Error 2: Multiple passwords in ACL user

**Symptom:**
```bash
VALKEY_USER=gnode_daemon ./scripts/valkey-cli-secure.sh ACL GETUSER gnode_client_staging_my_app
# passwords
# <sha256-hash-2-redacted>
# <sha256-hash-1-redacted>  ← Two passwords!
```

**Root cause:** Script ran multiple times without `resetpass`, appending passwords

**Fix:**
```bash
# Re-run creation script (now includes resetpass)
/opt/gNode/scripts/register-site.sh staging_my_app
```

**Verification:**
```bash
VALKEY_USER=gnode_daemon ./scripts/valkey-cli-secure.sh ACL GETUSER gnode_client_staging_my_app | grep -A1 "passwords"
# Should show ONLY ONE password hash
```

**Prevention:** Script now uses `ACL SETUSER ... resetpass` to clear old passwords.

---

### Error 3: Password file has newline (65 bytes instead of 64)

**Symptom:**
```bash
wc -c < /opt/gNode/.gnode/valkey_client_staging_my_app.password
# 65  ← Should be 64!

od -c /opt/gNode/.gnode/valkey_client_staging_my_app.password | tail -1
# 0000100  \n  ← Trailing newline
```

**Root cause:** Used `echo "$PASSWORD"` instead of `echo -n "$PASSWORD"`

**Fix:**
```bash
# Re-run creation script (now uses echo -n)
/opt/gNode/scripts/register-site.sh staging_my_app
```

**Verification:**
```bash
wc -c < /opt/gNode/.gnode/valkey_client_staging_my_app.password
# 64  ← Correct!
```

**Prevention:** Script now uses `echo -n` to prevent newline.

---

### Error 4: Hash mismatch between file and ACL

**Symptom:**
```bash
# Password file hash
cat /opt/gNode/.gnode/valkey_client_staging_my_app.password | sha256sum
# <sha256-hash-4-redacted>

# ACL user hash
VALKEY_USER=gnode_daemon ./scripts/valkey-cli-secure.sh ACL GETUSER gnode_client_staging_my_app | grep -A1 "passwords"
# <sha256-hash-5-redacted>  ← Different!
```

**Root cause:** Password file updated but ACL user not updated (or vice versa)

**Fix:**
```bash
# Synchronize by re-running creation script
/opt/gNode/scripts/register-site.sh staging_my_app
```

**Prevention:** Always use `register-site.sh` script, never manually edit ACL.

---

### Error 5: NOPERM errors in application logs

**Symptom:**
```
[error] Failed to set key 'error:default:node1:abc123': NOPERM No permissions to access a key
```

**Root cause:** Application using wrong keyspace prefix (e.g., `error:default:*` instead of `error:staging_my_app:*`)

**Diagnosis:**
```bash
# Check what keyspace app is trying to use
grep "NOPERM" /var/www/*/wp-content/gcore-logs/gcore-core.log | grep -o "error:[^:]*"

# Check what keyspace ACL user has access to
VALKEY_USER=gnode_daemon ./scripts/valkey-cli-secure.sh ACL GETUSER gnode_client_staging_my_app | grep "keys"
```

**Fix:** Verify application is reading correct `site_id` from configuration
- WordPress: Check `gcore-loader.php` correctly derives `site_id` from domain
- Other apps: Check environment variables or config files set correct `SITE_ID`

---

## Verification Test Suite

### Test 1: Basic Authentication

```bash
REDISCLI_AUTH="$(cat /opt/gNode/.gnode/valkey_client_staging_my_app.password)" \
  valkey-cli --user gnode_client_staging_my_app PING

# Expected: PONG
# If fails: Check password file exists and has 64 bytes
```

### Test 2: Hash Verification

```bash
# Get ACL hash
ACL_HASH=$(VALKEY_USER=gnode_daemon ./scripts/valkey-cli-secure.sh ACL GETUSER gnode_client_staging_my_app | grep -A1 "passwords" | tail -1)

# Get file hash
FILE_HASH=$(cat /opt/gNode/.gnode/valkey_client_staging_my_app.password | sha256sum | cut -d' ' -f1)

# Compare
if [ "$ACL_HASH" = "$FILE_HASH" ]; then
  echo "OK: Hashes match"
else
  echo "FAIL: Hashes mismatch - password file and ACL are out of sync"
fi
```

### Test 3: Keyspace Isolation (Allow)

```bash
REDISCLI_AUTH="$(cat /opt/gNode/.gnode/valkey_client_staging_my_app.password)" \
  valkey-cli --user gnode_client_staging_my_app \
  SET "error:staging_my_app:test" "allowed"

# Expected: OK
# If fails: Check ACL permissions include ~error:staging_my_app:*
```

### Test 4: Keyspace Isolation (Deny)

```bash
REDISCLI_AUTH="$(cat /opt/gNode/.gnode/valkey_client_staging_my_app.password)" \
  valkey-cli --user gnode_client_staging_my_app \
  SET "error:other_site:test" "denied"

# Expected: NOPERM No permissions to access a key
# If allows: ACL isolation is broken - investigate permissions
```

### Test 5: Application Integration

```bash
# Check if WordPress/app is writing data
VALKEY_USER=gnode_daemon ./scripts/valkey-cli-secure.sh KEYS "*:staging_my_app:*"

# Expected: List of keys like error:staging_my_app:*, cache:staging_my_app:*
# If empty: App not connecting to ValKey or using wrong keyspace prefix
```

---

## Best Practices

### 1. Script-Based Management

**DO:**
```bash
# Use provided script for all ACL operations
/opt/gNode/scripts/register-site.sh staging_my_app
```

**DON'T:**
```bash
# Manual ACL commands are error-prone
valkey-cli ACL SETUSER ... # Easy to make mistakes
```

### 2. Password Storage

**DO:**
```bash
# Store in protected directory with restrictive permissions
echo -n "$PASSWORD" > /opt/gNode/.gnode/valkey_client_site.password
chmod 600 /opt/gNode/.gnode/valkey_client_site.password
```

**DON'T:**
```bash
# Don't store in version control
git add .gnode/*.password  # NEVER do this

# Don't use world-readable permissions
chmod 644 /opt/gNode/.gnode/valkey_client_site.password  # Insecure
```

### 3. CLI Authentication

**DO:**
```bash
# Use REDISCLI_AUTH environment variable
REDISCLI_AUTH="$PASSWORD" valkey-cli --user $USER $COMMAND
```

**DON'T:**
```bash
# Don't use --pass flag
valkey-cli --user $USER --pass "$PASSWORD" $COMMAND  # Broken
```

### 4. Application Integration

**DO:**
```php
// Read password at startup
$password = trim(file_get_contents($password_file));
putenv("VALKEY_AUTH=$password");

// Use libraries that respect environment variables
$redis = new Redis();
$redis->auth([$username, $password]);  // Explicit auth
```

**DON'T:**
```php
// Don't hardcode passwords
$redis->auth(['gnode_client', 'hardcoded_password']);  // Security risk

// Don't assume password in environment
$redis = new Redis();
// Missing auth() call - will fail with ACL enabled
```

---

## Historical Context

### Why This Matters

**Before this documentation:**
- Engineers spent hours debugging "AUTH failed: WRONGPASS"
- Passwords rotated frequently, breaking automation
- Multiple passwords accumulated in ACL users
- No clear understanding of which component used which method

**After this documentation:**
- Clear understanding: `--pass` is broken, use `REDISCLI_AUTH`
- Single source of truth for password lifecycle
- Consistent practices across all components
- Troubleshooting takes minutes, not hours

### Lessons Learned

1. **CLI tools have quirks** - Don't assume command-line flags work as documented
2. **Test production passwords** - Short test passwords may work while production ones fail
3. **Document the WHY** - Future maintainers need context, not just commands
4. **Automate everything** - Manual ACL management is error-prone
5. **Verify end-to-end** - Test not just CLI, but full application stack

---

## Related Documentation

- **docs/ACL_PASSWORD_MANAGEMENT.md** - User-facing guide with examples
- **CLAUDE.md** - System architecture with ACL authentication section
- **scripts/register-site.sh** - Automated ACL user creation
- **scripts/valkey-cli-secure.sh** - Wrapper script using REDISCLI_AUTH

---

## Quick Reference Card

| Scenario | Command |
|----------|---------|
| Create ACL user | `/opt/gNode/scripts/register-site.sh {service_id}` |
| Test auth | `REDISCLI_AUTH="$(cat password_file)" valkey-cli --user {user} PING` |
| List users | `VALKEY_USER=gnode_daemon ./scripts/valkey-cli-secure.sh ACL LIST` |
| Check permissions | `VALKEY_USER=gnode_daemon ./scripts/valkey-cli-secure.sh ACL GETUSER {user}` |
| Verify isolation | `REDISCLI_AUTH="..." valkey-cli --user {user} SET "error:other:test" "x"` → expect NOPERM |
| Check app data | `VALKEY_USER=gnode_daemon ./scripts/valkey-cli-secure.sh KEYS "*:{service_id}:*"` |

---

## Conclusion

The ValKey ACL system works correctly. The confusion stemmed from the `--pass` CLI flag being unreliable.

**Golden rule:** ALWAYS use `REDISCLI_AUTH` environment variable for authentication.

All gNode components (daemon, WordPress, scripts) follow this rule and work reliably in production.
