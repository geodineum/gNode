#!/bin/bash
#
# ValKey function loader for gNode.
#
# Loads every `gnode_*.lua` library in `daemon/functions/` plus any
# library provided by a registered extension directory.
#
# Usage:
#   ./load-valkey-functions.sh [password]
#
# Environment:
#   VALKEY_HOST              - ValKey host (default: 127.0.0.1)
#   VALKEY_PORT              - ValKey port (default: 47445)
#   VALKEY_USER              - ACL username (default: gnode_daemon)
#   GNODE_EXT_<NAME>_PATH    - explicit path to an extension repo
#                              (scanned from the environment; any env var
#                              matching the pattern is checked)
#   GNODE_EXT_DIR            - directory of signed extension subdirs.
#                              Each subdir must carry a manifest.yaml +
#                              manifest.sig verified against the public
#                              key at $AUTHOR_PUBKEY_PEM. Unverified
#                              subdirs are skipped with a warning.
#   AUTHOR_PUBKEY_PEM        - override the default pubkey path
#                              (default: /etc/geodineum/gnode/ext-author.pub)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# ---------------------------------------------------------------------------
# Credentials + connection (unchanged from prior loader)
# ---------------------------------------------------------------------------
GEODINEUM_LIB="${GEODINEUM_LIB:-/usr/local/lib/geodineum}"
if [ ! -r "$GEODINEUM_LIB/bootstrap-loader.sh" ]; then
    echo "FATAL: $GEODINEUM_LIB/bootstrap-loader.sh not found. Run installer first." >&2
    exit 1
fi
# shellcheck source=/usr/local/lib/geodineum/bootstrap-loader.sh
source "$GEODINEUM_LIB/bootstrap-loader.sh"
load_ecosystem_config

CENTRALIZED_CREDS="${GEODINEUM_CREDENTIALS_DIR:-/etc/geodineum/credentials}"
STANDARD_CREDS="$PROJECT_ROOT/.gnode"
LEGACY_CREDS="/opt/gNode/.gnode"

VALKEY_HOST="${VALKEY_HOST:-127.0.0.1}"
VALKEY_PORT="${VALKEY_PORT:-47445}"
VALKEY_USER="${VALKEY_USER:-gnode_daemon}"

if [ "$VALKEY_USER" = "gnode_daemon" ]; then
    PASSWORD_FILENAME="valkey_daemon.password"
elif [ "$VALKEY_USER" = "gnode_client" ]; then
    PASSWORD_FILENAME="valkey_client.password"
else
    PASSWORD_FILENAME="valkey.password"
fi

if [ -n "${1:-}" ]; then
    VALKEY_PASSWORD="$1"
else
    VALKEY_PASSWORD=""
    for creds_dir in "$CENTRALIZED_CREDS" "$STANDARD_CREDS" "$LEGACY_CREDS"; do
        if [ -f "$creds_dir/$PASSWORD_FILENAME" ]; then
            VALKEY_PASSWORD=$(cat "$creds_dir/$PASSWORD_FILENAME")
            break
        fi
    done
    if [ -z "$VALKEY_PASSWORD" ]; then
        VALKEY_PASSWORD="${REDIS_AUTH:-}"
    fi
fi

VALKEY_CLI_BIN=$(which valkey-cli 2>/dev/null || echo "/usr/local/bin/valkey-cli")
if [ ! -x "$VALKEY_CLI_BIN" ]; then
    echo "Error: valkey-cli not found or not executable" >&2
    exit 1
fi

valkey_exec() {
    REDISCLI_AUTH="$VALKEY_PASSWORD" "$VALKEY_CLI_BIN" \
        -h "$VALKEY_HOST" -p "$VALKEY_PORT" --user "$VALKEY_USER" "$@"
}
valkey_exec_stdin() {
    REDISCLI_AUTH="$VALKEY_PASSWORD" "$VALKEY_CLI_BIN" \
        -h "$VALKEY_HOST" -p "$VALKEY_PORT" --user "$VALKEY_USER" -x "$@"
}

FUNCTIONS_DIR="$PROJECT_ROOT/daemon/functions"

# ---------------------------------------------------------------------------
# Library loading helpers
# ---------------------------------------------------------------------------
ensure_metadata() {
    local file=$1
    local name
    name=$(basename "$file" .lua)
    if ! grep -q "^#!lua name=" "$file"; then
        echo "Adding metadata to $file..."
        local temp_file
        temp_file=$(mktemp)
        cat > "$temp_file" << EOF
#!lua name=$name

--
-- gNode $(echo "$name" | sed 's/gnode_//' | tr '[a-z]' '[A-Z]') Functions
-- A ValKey function library for $(echo "$name" | sed 's/gnode_//' | tr '_' ' ') operations
--
EOF
        grep -v "^#!lua name=" "$file" >> "$temp_file"
        mv "$temp_file" "$file"
    elif grep -q "^#\\\\!lua name=" "$file"; then
        echo "Fixing escaped shebang in $file..."
        sed -i 's/^#\\!lua name=/#!lua name=/' "$file"
    fi
}

load_function() {
    local file=$1
    local name
    name=$(basename "$file" .lua)
    echo "  Loading $name..."
    ensure_metadata "$file"
    local result
    result=$(cat "$file" | valkey_exec_stdin FUNCTION LOAD REPLACE 2>&1)
    local status=$?
    if [ $status -ne 0 ]; then
        echo "  FAILED: $name — $result"
        return 1
    fi
    echo "  OK: $name"
    return 0
}

# ---------------------------------------------------------------------------
# Signature verification — delegate to the daemon's runtime verifier so build
# and load share ONE scheme.
# ---------------------------------------------------------------------------
# The extension's canonical artifacts are `extension.yaml` + `extension.sig`
# (raw Ed25519 over a canonical hash-manifest, key baked into the daemon).
# `gnode-daemon verify-extension <dir>` runs the EXACT same check as build.rs.
# (Previously this used an openssl `manifest.yaml`/`manifest.sig` scheme the
# extensions never carried, so every extension was skipped and no Lua libs
# loaded — see .)
GNODE_DAEMON_BIN="${GNODE_DAEMON_BIN:-$PROJECT_ROOT/daemon/target/release/gnode-daemon}"

verify_signed_manifest() {
    # verify_signed_manifest <extension-dir>
    # returns 0 on success, non-zero on failure (with reason to stderr)
    local dir="$1"
    if [ ! -x "$GNODE_DAEMON_BIN" ]; then
        echo "    daemon binary not found at $GNODE_DAEMON_BIN (cannot verify)" >&2
        return 3
    fi
    if ! "$GNODE_DAEMON_BIN" verify-extension "$dir" >/dev/null 2>&1; then
        echo "    signature verification failed" >&2
        return 4
    fi
    return 0
}

# ---------------------------------------------------------------------------
# Extension directory discovery
# ---------------------------------------------------------------------------
EXTENSION_DIRS=()
EXTENSION_NAMES=()

register_extension() {
    local name="$1" dir="$2"
    for existing in "${EXTENSION_NAMES[@]:-}"; do
        [ "$existing" = "$name" ] && return
    done
    EXTENSION_NAMES+=("$name")
    EXTENSION_DIRS+=("$dir")
    echo "  $name: $dir"
}

echo "=========================================="
echo "gNode ValKey Function Loader"
echo "=========================================="
echo "ValKey:    $VALKEY_HOST:$VALKEY_PORT (user: $VALKEY_USER)"
echo "Functions: $FUNCTIONS_DIR"
echo ""
echo "Extensions:"

# 1. GNODE_EXT_<NAME>_PATH env vars (scan environment)
while IFS='=' read -r key value; do
    [[ "$key" =~ ^GNODE_EXT_.+_PATH$ ]] || continue
    [ -n "$value" ] || continue
    [ -d "$value/functions" ] || continue
    name=$(basename "$value")
    register_extension "$name" "$value/functions"
done < <(env)

# Default GNODE_EXT_DIR to the sibling pro/gNode tree when unset, so a plain
# `geodineum daemon reload-lua` loads signed-extension Lua libs (e.g. CMS's
# gnode_asset) just as build.sh stages their Rust handlers. Without this the
# extension's Lua functions never reach ValKey → "Function not found" on FCALL.
if [ -z "${GNODE_EXT_DIR:-}" ]; then
    _ext_default="$(dirname "$PROJECT_ROOT")/pro/gNode"
    if [ -d "$_ext_default" ]; then
        GNODE_EXT_DIR="$_ext_default"
    fi
fi

# 2. GNODE_EXT_DIR — signed subdirectories
if [ -n "${GNODE_EXT_DIR:-}" ] && [ -d "$GNODE_EXT_DIR" ]; then
    for ext_dir in "$GNODE_EXT_DIR"/*/; do
        [ -d "$ext_dir" ] || continue
        name=$(basename "$ext_dir")
        if verify_signed_manifest "$ext_dir"; then
            if [ -d "$ext_dir/functions" ]; then
                register_extension "$name" "$ext_dir/functions"
            fi
        else
            echo "  SKIP: $name (signature verification failed)"
        fi
    done
fi

if [ ${#EXTENSION_DIRS[@]} -eq 0 ]; then
    echo "  (none registered)"
fi
echo ""

# ---------------------------------------------------------------------------
# Connection + FUNCTION LOAD sanity check
# ---------------------------------------------------------------------------
echo "Testing ValKey connection..."
PING_RESULT=$(valkey_exec PING 2>&1)
if [ "$PING_RESULT" != "PONG" ]; then
    echo "Failed to connect to ValKey: $PING_RESULT"
    echo "Check that ValKey is running and credentials are correct."
    exit 1
fi
echo "Connection OK."

echo "Testing FUNCTION LOAD support..."
TESTFUNC='#!lua name=testlib

redis.register_function("TEST_HELLO", function() return "Hello!" end)'
TEST_RESULT=$(echo "$TESTFUNC" | valkey_exec_stdin FUNCTION LOAD 2>&1)
if [ $? -ne 0 ]; then
    echo "This ValKey instance doesn't support functions!"
    echo "Error: $TEST_RESULT"
    echo "ValKey 7.2+ is required."
    exit 1
fi
valkey_exec FUNCTION DELETE testlib >/dev/null 2>&1
echo "FUNCTION LOAD OK."
echo ""

# ---------------------------------------------------------------------------
# Load base libraries
# ---------------------------------------------------------------------------
if [ -f "$FUNCTIONS_DIR/gnode_test.lua" ]; then
    echo "Loading test library..."
    load_function "$FUNCTIONS_DIR/gnode_test.lua"
    echo ""
fi

echo "Loading base libraries from $FUNCTIONS_DIR..."
LOADED=0
FAILED=0
for file in $(find "$FUNCTIONS_DIR" -name "gnode_*.lua" ! -name "gnode_test*" | sort); do
    if load_function "$file"; then
        LOADED=$((LOADED + 1))
    else
        FAILED=$((FAILED + 1))
    fi
done

# ---------------------------------------------------------------------------
# Load extension libraries
# ---------------------------------------------------------------------------
EXT_LOADED=0
for i in "${!EXTENSION_DIRS[@]}"; do
    ext_name="${EXTENSION_NAMES[$i]}"
    ext_func_dir="${EXTENSION_DIRS[$i]}"
    echo ""
    echo "Loading $ext_name extension libraries..."
    for file in $(find "$ext_func_dir" -name "gnode_*.lua" | sort); do
        lib_name=$(basename "$file" .lua)
        if [ ! -f "$FUNCTIONS_DIR/$lib_name.lua" ]; then
            if load_function "$file"; then
                EXT_LOADED=$((EXT_LOADED + 1))
                LOADED=$((LOADED + 1))
            else
                FAILED=$((FAILED + 1))
            fi
        else
            echo "  SKIP: $lib_name (already loaded from base)"
        fi
    done
done

# ---------------------------------------------------------------------------
# Summary + smoke test
# ---------------------------------------------------------------------------
echo ""
echo "=========================================="
echo "Load Summary"
echo "=========================================="
echo "  Base:       $((LOADED - EXT_LOADED))"
if [ "$EXT_LOADED" -gt 0 ]; then
    echo "  Extensions: $EXT_LOADED (from ${#EXTENSION_DIRS[@]} dir(s))"
fi
echo "  Loaded:  $LOADED"
if [ "$FAILED" -gt 0 ]; then
    echo "  Failed:  $FAILED"
fi
echo "  Total:   $((LOADED + FAILED))"
echo "=========================================="

echo ""
echo "Loaded ValKey function libraries:"
valkey_exec FUNCTION LIST | grep -A1 "^library_name$" | grep "^gnode_" | sed 's/^/  - /' || true

if [ -f "$FUNCTIONS_DIR/gnode_test.lua" ]; then
    echo ""
    echo "Smoke test..."
    TEST_RESULT=$(valkey_exec FCALL GNODE_TEST_HELLO 0 2>&1)
    if echo "$TEST_RESULT" | grep -qi "hello"; then
        echo "  PASS: GNODE_TEST_HELLO → $TEST_RESULT"
    else
        echo "  FAIL: GNODE_TEST_HELLO → $TEST_RESULT"
    fi
fi

if [ "$FAILED" -gt 0 ]; then
    echo ""
    echo "WARNING: $FAILED libraries failed to load. Check errors above."
    exit 1
fi

echo ""
echo "Done."
