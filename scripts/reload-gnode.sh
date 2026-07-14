#!/bin/bash
# Reload gNode Runtime Environment
#
# This script reinitializes the gNode runtime environment without the heavy setup:
# - Does NOT rebuild the daemon
# - Does NOT setup permissions/groups
# - DOES reload Lua functions into ValKey
# - DOES verify connectivity and function count
#
# Use this after editing Lua functions or when functions need reloading.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Canonical ecosystem config loader (installed by Geodineum installer).
GEODINEUM_LIB="${GEODINEUM_LIB:-/usr/local/lib/geodineum}"
if [ ! -r "$GEODINEUM_LIB/bootstrap-loader.sh" ]; then
    echo "FATAL: $GEODINEUM_LIB/bootstrap-loader.sh not found. Run installer first." >&2
    exit 1
fi
# shellcheck source=/usr/local/lib/geodineum/bootstrap-loader.sh
source "$GEODINEUM_LIB/bootstrap-loader.sh"
load_ecosystem_config

# Credential directories (same resolution order as valkey-cli-secure.sh and PHP CredentialResolver)
CENTRALIZED_CREDS="${GEODINEUM_CREDENTIALS_DIR:-/etc/geodineum/credentials}"
STANDARD_CREDS="$PROJECT_ROOT/.gnode"
LEGACY_CREDS="/opt/gNode/.gnode"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[OK]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1" >&2; }
log_step() { echo -e "\n${BLUE}==>${NC} $1"; }

# Resolve daemon password (centralized → standard → legacy)
VALKEY_PASSWORD=""
for creds_dir in "$CENTRALIZED_CREDS" "$STANDARD_CREDS" "$LEGACY_CREDS"; do
    if [ -f "$creds_dir/valkey_daemon.password" ]; then
        VALKEY_PASSWORD=$(cat "$creds_dir/valkey_daemon.password")
        break
    fi
done

if [ -z "$VALKEY_PASSWORD" ]; then
    log_error "ValKey daemon password not found. Searched:"
    echo "  - $CENTRALIZED_CREDS/valkey_daemon.password"
    echo "  - $STANDARD_CREDS/valkey_daemon.password"
    echo "  - $LEGACY_CREDS/valkey_daemon.password"
    exit 1
fi

VALKEY_PORT="${VALKEY_PORT:-47445}"

# Wrapper for authenticated ValKey CLI
valkey_exec() {
    REDISCLI_AUTH="$VALKEY_PASSWORD" valkey-cli -p "$VALKEY_PORT" --user gnode_daemon "$@"
}

# Step 1: Check ValKey connectivity
log_step "Checking ValKey connectivity..."
if valkey_exec PING 2>&1 | grep -q "PONG"; then
    log_info "ValKey connection OK"
else
    log_error "ValKey connection failed. Is valkey-gnode.service running?"
    echo "  sudo systemctl start valkey-gnode"
    exit 1
fi

# Step 2: Reload Lua functions
log_step "Reloading Lua functions..."
if [ -f "$SCRIPT_DIR/load-valkey-functions.sh" ]; then
    bash "$SCRIPT_DIR/load-valkey-functions.sh" "$VALKEY_PASSWORD"
else
    log_error "load-valkey-functions.sh not found"
    exit 1
fi

# Step 3: Verify
log_step "Verifying runtime environment..."

FUNCTION_COUNT=$(valkey_exec FUNCTION LIST 2>&1 | grep -c "library_name" || echo "0")
log_info "Functions loaded: $FUNCTION_COUNT libraries"

SITE_COUNT=$(valkey_exec SCARD gnode:sites:registry 2>&1 || echo "0")
log_info "Registered sites: $SITE_COUNT"

STREAM_COUNT=$(valkey_exec KEYS "*:gnode:unified:*" 2>&1 | grep -v Warning | wc -l || echo "0")
log_info "Active streams: $STREAM_COUNT"

echo
echo -e "${GREEN}gNode runtime environment reloaded.${NC}"
echo
echo "To restart the daemon with the new binary:"
echo "  sudo systemctl restart gnode-daemon"
echo
