#!/bin/bash
#
# Deregister a service from gNode
#
# This script completely removes a service and all its associated data:
# - Registry entry (gnode:sites:registry)
# - All stream keys ({service_id}:gnode:*)
# - Metadata keys (gnode:site:{service_id}:*)
# - Cache and rate-limit keys
# - ACL user (if --remove-acl specified)
#
# Usage: ./deregister-service.sh <service_id> [options]
#
# Options:
#   --dry-run       Preview what would be deleted without making changes
#   --remove-acl    Also remove the ValKey ACL user (requires admin password)
#   --force         Skip confirmation prompt
#   --keep-cache    Keep cache/rate-limit keys (only remove streams and registry)
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Canonical ecosystem config loader (installed by Geodineum installer).
GEODINEUM_LIB="${GEODINEUM_LIB:-/usr/local/lib/geodineum}"
if [[ ! -r "$GEODINEUM_LIB/bootstrap-loader.sh" ]]; then
    echo "FATAL: $GEODINEUM_LIB/bootstrap-loader.sh not found. Run installer first." >&2
    exit 1
fi
# shellcheck source=/usr/local/lib/geodineum/bootstrap-loader.sh
source "$GEODINEUM_LIB/bootstrap-loader.sh"
load_ecosystem_config

PASSWORD_DIR="${GNODE_PASSWORD_DIR:-$PROJECT_ROOT/.gnode}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[OK]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1" >&2; }

# ValKey connection
VALKEY_PORT="${VALKEY_PORT:-47445}"
VALKEY_HOST="${VALKEY_HOST:-127.0.0.1}"

# CLI wrapper using daemon credentials
valkey_cli() {
    if [[ -f "$PASSWORD_DIR/valkey_daemon.password" ]]; then
        REDISCLI_AUTH="$(cat "$PASSWORD_DIR/valkey_daemon.password")" \
            valkey-cli -h "$VALKEY_HOST" -p "$VALKEY_PORT" --user gnode_daemon "$@"
    elif [[ -f "$PASSWORD_DIR/valkey.password" ]]; then
        REDISCLI_AUTH="$(cat "$PASSWORD_DIR/valkey.password")" \
            valkey-cli -h "$VALKEY_HOST" -p "$VALKEY_PORT" "$@"
    else
        log_error "No ValKey password file found"
        exit 1
    fi
}

# Admin CLI for ACL operations (uses default user with legacy password)
valkey_admin_cli() {
    if [[ -f "$PASSWORD_DIR/valkey.password" ]]; then
        REDISCLI_AUTH="$(cat "$PASSWORD_DIR/valkey.password")" \
            valkey-cli -h "$VALKEY_HOST" -p "$VALKEY_PORT" "$@"
    else
        log_error "Admin password file not found: $PASSWORD_DIR/valkey.password"
        return 1
    fi
}

usage() {
    cat << EOF
Usage: $(basename "$0") <service_id> [options]

Completely removes a service from gNode.

Arguments:
  service_id      The service identifier to remove

Options:
  --dry-run       Preview what would be deleted without making changes
  --remove-acl    Also remove the ValKey ACL user for this service
  --force         Skip confirmation prompt
  --keep-cache    Keep cache/rate-limit keys (only remove streams and registry)
  -h, --help      Show this help message

Examples:
  # Preview what would be deleted
  $(basename "$0") my_test_site --dry-run

  # Remove service with confirmation
  $(basename "$0") my_test_site

  # Remove service including ACL user
  $(basename "$0") my_test_site --remove-acl

  # Force remove without confirmation
  $(basename "$0") my_test_site --force
EOF
    exit 0
}

# Parse arguments
SERVICE_ID=""
DRY_RUN=false
REMOVE_ACL=false
FORCE=false
KEEP_CACHE=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --remove-acl)
            REMOVE_ACL=true
            shift
            ;;
        --force)
            FORCE=true
            shift
            ;;
        --keep-cache)
            KEEP_CACHE=true
            shift
            ;;
        -h|--help)
            usage
            ;;
        -*)
            log_error "Unknown option: $1"
            usage
            ;;
        *)
            if [[ -z "$SERVICE_ID" ]]; then
                SERVICE_ID="$1"
            else
                log_error "Unexpected argument: $1"
                usage
            fi
            shift
            ;;
    esac
done

# Validate service ID
if [[ -z "$SERVICE_ID" ]]; then
    log_error "Service ID required"
    usage
fi

# Check if service exists
log_info "Checking if service '$SERVICE_ID' exists..."
EXISTS=$(valkey_cli SISMEMBER gnode:sites:registry "$SERVICE_ID" 2>/dev/null || echo "0")

if [[ "$EXISTS" == "0" ]]; then
    log_warn "Service '$SERVICE_ID' not found in registry"
    log_info "Checking for orphaned keys..."

    # Check for orphaned stream keys
    ORPHAN_COUNT=$(valkey_cli KEYS "{$SERVICE_ID}:*" 2>/dev/null | wc -l || echo "0")
    META_EXISTS=$(valkey_cli EXISTS "gnode:site:${SERVICE_ID}:meta" 2>/dev/null || echo "0")

    if [[ "$ORPHAN_COUNT" == "0" && "$META_EXISTS" == "0" ]]; then
        log_success "No data found for service '$SERVICE_ID' - nothing to clean up"
        exit 0
    fi

    log_warn "Found $ORPHAN_COUNT orphaned keys for service '$SERVICE_ID'"
fi

# Build options JSON
OPTIONS_JSON="{\"dry_run\":$DRY_RUN,\"include_cache\":$([ "$KEEP_CACHE" == "true" ] && echo "false" || echo "true")}"

# Show what will be done
echo ""
if [[ "$DRY_RUN" == "true" ]]; then
    log_info "=== DRY RUN - No changes will be made ==="
else
    log_warn "=== This will PERMANENTLY delete service data ==="
fi
echo ""
log_info "Service ID: $SERVICE_ID"
log_info "Options: $OPTIONS_JSON"
if [[ "$REMOVE_ACL" == "true" ]]; then
    log_info "ACL user 'gnode_client_$SERVICE_ID' will also be removed"
fi
echo ""

# Confirmation (unless --force or --dry-run)
if [[ "$DRY_RUN" == "false" && "$FORCE" == "false" ]]; then
    read -p "Are you sure you want to deregister '$SERVICE_ID'? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        log_info "Cancelled"
        exit 0
    fi
fi

# Call the deprovision function
log_info "Calling GNODE_DEPROVISION_SERVICE..."
RESULT=$(valkey_cli FCALL GNODE_DEPROVISION_SERVICE 0 "$SERVICE_ID" "$OPTIONS_JSON" 2>&1)

if [[ "$RESULT" == *"error"* && "$RESULT" == *"ERR"* ]]; then
    log_error "Deprovision failed: $RESULT"
    exit 1
fi

# Parse and display results
echo ""
log_info "=== Deprovision Results ==="

# Use Python to parse JSON and display nicely
set +e
echo "$RESULT" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    print(f\"Success: {d.get('success', False)}\")
    print(f\"Dry run: {d.get('dry_run', False)}\")
    print(f\"Deleted: {d.get('deleted_count', 0)} keys\")
    print(f\"Skipped: {d.get('skipped_count', 0)} keys\")

    if d.get('deleted_keys'):
        print()
        print('Deleted keys:')
        for k in d['deleted_keys']:
            status = ' (would delete)' if k.get('dry_run') else ''
            print(f\"  - {k['key']}: {k['reason']}{status}\")

    if d.get('skipped_keys'):
        print()
        print('Skipped keys:')
        for k in d['skipped_keys']:
            print(f\"  - {k['key']}: {k['reason']}\")

    if d.get('errors') and len(d['errors']) > 0:
        print()
        print('Errors:')
        for e in d['errors']:
            print(f\"  - {e}\")
except Exception as e:
    print(f'Raw result: {sys.stdin.read()}')
    print(f'Parse error: {e}')
" 2>/dev/null || echo "Raw result: $RESULT"
set -e

# Remove ACL user if requested
if [[ "$REMOVE_ACL" == "true" && "$DRY_RUN" == "false" ]]; then
    echo ""
    ACL_USER="gnode_client_$SERVICE_ID"
    log_info "Removing ACL user '$ACL_USER'..."

    if valkey_admin_cli ACL DELUSER "$ACL_USER" 2>/dev/null | grep -q "1"; then
        log_success "ACL user '$ACL_USER' removed"

        # Also remove the password file
        PASSWORD_FILE="$PASSWORD_DIR/valkey_client_${SERVICE_ID}.password"
        if [[ -f "$PASSWORD_FILE" ]]; then
            rm -f "$PASSWORD_FILE"
            log_success "Password file removed: $PASSWORD_FILE"
        fi

        # Persist ACL changes
        valkey_admin_cli ACL SAVE 2>/dev/null || log_warn "Could not persist ACL changes"
    else
        log_warn "ACL user '$ACL_USER' not found or could not be removed"
    fi
elif [[ "$REMOVE_ACL" == "true" && "$DRY_RUN" == "true" ]]; then
    log_info "(Dry run) Would remove ACL user 'gnode_client_$SERVICE_ID'"
fi

echo ""
if [[ "$DRY_RUN" == "true" ]]; then
    log_success "Dry run complete - no changes were made"
    log_info "Run without --dry-run to apply changes"
else
    log_success "Service '$SERVICE_ID' has been deregistered"
    log_info "The daemon will automatically stop listening to removed streams"
fi
