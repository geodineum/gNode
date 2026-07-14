#!/bin/bash
#
# gNode Site ACL Repair Script
#
# Use this script to REPAIR or UPDATE ACL permissions for an existing site.
# For NEW site registration, use: ./scripts/register-site.sh <site_id>
#
# This script:
#   - Fixes/updates ACL keyspace patterns
#   - Resets command permissions
#   - Optionally regenerates passwords
#
# Usage:
#   sudo ./scripts/repair-site-acl.sh <site_id> [environment]
#
# Examples:
#   sudo ./scripts/repair-site-acl.sh my_app staging
#   sudo ./scripts/repair-site-acl.sh production_example_com production
#

set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
PASSWORD_DIR="$PROJECT_ROOT/.gnode"
VALKEY_CLI_SECURE="$SCRIPT_DIR/valkey-cli-secure.sh"

# ACL operations require the default/admin user — gnode_daemon has
# -@dangerous, which denies the ACL command (NOPERM acl|setuser).
# Mirror register-site.sh's admin helper.
VALKEY_ADMIN_PASSWORD_FILE="$PASSWORD_DIR/valkey.password"
valkey_admin_cli() {
    if [[ -f "$VALKEY_ADMIN_PASSWORD_FILE" ]]; then
        REDISCLI_AUTH="$(cat "$VALKEY_ADMIN_PASSWORD_FILE")" valkey-cli -p "${VALKEY_PORT:-47445}" "$@"
    else
        log_error "Admin password file not found: $VALKEY_ADMIN_PASSWORD_FILE"
        log_error "ACL operations require the default user password"
        return 1
    fi
}

# Logging functions
log_info() { echo -e "${BLUE}ℹ${NC} $1"; }
log_success() { echo -e "${GREEN}✓${NC} $1"; }
log_warning() { echo -e "${YELLOW}⚠${NC} $1"; }
log_error() { echo -e "${RED}✗${NC} $1"; }
log_step() { echo ""; echo -e "${CYAN}==>${NC} ${BOLD}$1${NC}"; }

#######################################
# Argument Parsing
#######################################

if [[ $# -lt 1 ]]; then
    log_error "Missing required argument: site_id"
    echo ""
    echo "Usage: $0 <site_id> [environment]"
    echo ""
    echo "Examples:"
    echo "  $0 my_app staging"
    echo "  $0 production_example_com production"
    echo ""
    echo "Environment (optional): testing, staging, acceptance, production"
    echo "  Default: auto-detect from site_id prefix"
    exit 1
fi

SITE_ID="$1"
ENVIRONMENT="${2:-}"

# Validate site_id format
if [[ ! "$SITE_ID" =~ ^[a-z0-9_]+$ ]]; then
    log_error "Invalid site_id format: $SITE_ID"
    log_info "Must contain only lowercase letters, numbers, and underscores"
    exit 1
fi

# Auto-detect environment from site_id if not specified
if [[ -z "$ENVIRONMENT" ]]; then
    if [[ "$SITE_ID" =~ ^testing_ ]] || [[ "$SITE_ID" =~ ^dev_ ]]; then
        ENVIRONMENT="testing"
    elif [[ "$SITE_ID" =~ ^staging_ ]]; then
        ENVIRONMENT="staging"
    elif [[ "$SITE_ID" =~ ^acceptance_ ]] || [[ "$SITE_ID" =~ ^uat_ ]]; then
        ENVIRONMENT="acceptance"
    else
        ENVIRONMENT="production"
    fi
    log_info "Auto-detected environment: $ENVIRONMENT"
fi

# Validate environment
case "$ENVIRONMENT" in
    testing|staging|acceptance|production)
        ;;
    *)
        log_error "Invalid environment: $ENVIRONMENT"
        log_info "Must be one of: testing, staging, acceptance, production"
        exit 1
        ;;
esac

ACL_USER="gnode_client_${SITE_ID}"
PASSWORD_FILE="$PASSWORD_DIR/valkey_client_${SITE_ID}.password"

#######################################
# Prerequisites
#######################################

check_root() {
    if [[ $EUID -ne 0 ]]; then
        log_error "This script requires root privileges"
        echo ""
        echo "Run with sudo:"
        echo "  sudo $0 $SITE_ID $ENVIRONMENT"
        exit 1
    fi
}

check_valkey() {
    if [[ ! -x "$VALKEY_CLI_SECURE" ]]; then
        log_error "ValKey CLI wrapper not found: $VALKEY_CLI_SECURE"
        exit 1
    fi

    if ! VALKEY_USER=gnode_daemon "$VALKEY_CLI_SECURE" PING &>/dev/null; then
        log_error "Cannot connect to ValKey"
        log_info "Ensure ValKey is running: systemctl status valkey-gnode"
        exit 1
    fi
}

check_password_dir() {
    if [[ ! -d "$PASSWORD_DIR" ]]; then
        log_info "Creating password directory: $PASSWORD_DIR"
        mkdir -p "$PASSWORD_DIR"
        chmod 750 "$PASSWORD_DIR"
        chown "root:${GNODE_GROUP:-gnode}" "$PASSWORD_DIR" 2>/dev/null || true
    fi
}

#######################################
# User Creation
#######################################

check_user_exists() {
    local username="$1"

    # Query ValKey for user info - check output, not just exit code
    # ACL GETUSER returns "(nil)" for non-existent users but exits with code 0
    local result
    result=$(valkey_admin_cli ACL GETUSER "$username" 2>/dev/null)

    if [[ "$result" == "(nil)" ]] || [[ -z "$result" ]]; then
        return 1  # User doesn't exist
    else
        return 0  # User exists
    fi
}

generate_password() {
    # Generate 64-character hex password (matches register-site.sh)
    openssl rand -hex 32 | cut -c1-65
}

create_or_update_user() {
    local username="$1"
    local password="$2"

    log_step "Creating/updating ACL user: $username"

    # Create/reset user with password
    if valkey_admin_cli ACL SETUSER "$username" on resetpass ">${password}" &>/dev/null; then
        log_success "User credentials configured"
    else
        log_error "Failed to set user credentials"
        return 1
    fi

    # Set keyspace permissions (pattern set)
    log_info "Configuring key patterns..."

    # Complete pattern set - must match register-site.sh!
    valkey_admin_cli ACL SETUSER "$username" resetkeys \
        "~error:${SITE_ID}:*" \
        "~cache:${SITE_ID}:*" \
        "~session:${SITE_ID}:*" \
        "~${SITE_ID}:error:*" \
        "~${SITE_ID}:cache:*" \
        "~${SITE_ID}:session:*" \
        "~${SITE_ID}:gnode:*" \
        "~${SITE_ID}:*" \
        "~{${SITE_ID}}:gnode:*" \
        "~{${SITE_ID}}:bundle:*" \
        "~{${SITE_ID}}:cache:*" \
        "~{${SITE_ID}}:metrics:*" \
        "~{${SITE_ID}}:*" \
        "~{testing}:gnode:*" \
        "~{staging}:gnode:*" \
        "~{acceptance}:gnode:*" \
        "~{production}:gnode:*" \
        "~{default}:gnode:*" \
        "~{default}:gcore:*" \
        "~{geodineum}:gnode:*" \
        "~gnode:*" \
        "~gnode:routing:*" \
        "~topology:*" \
        "~template:*" \
        "~membership:*" &>/dev/null || {
        log_error "Failed to set key patterns"
        return 1
    }

    log_success "Key patterns configured"

    # Set channel permissions
    log_info "Configuring channel patterns..."

    valkey_admin_cli ACL SETUSER "$username" resetchannels \
        "&${SITE_ID}:gnode:broadcast:*" \
        "&${SITE_ID}:gnode:events:*" \
        "&{testing}:gnode:broadcast:*" \
        "&{testing}:gnode:unified:*" \
        "&{staging}:gnode:broadcast:*" \
        "&{staging}:gnode:unified:*" \
        "&{acceptance}:gnode:broadcast:*" \
        "&{acceptance}:gnode:unified:*" \
        "&{production}:gnode:broadcast:*" \
        "&{production}:gnode:unified:*" &>/dev/null || {
        log_error "Failed to set channel patterns"
        return 1
    }

    log_success "Channel patterns configured"

    # Set command permissions
    # Note: All permissions must be in a single command call for ValKey to process them correctly
    log_info "Configuring command permissions..."

    local cmd_result
    # Use nocommands to clear, then add specific commands (resetcommands not supported in ValKey)
    cmd_result=$(valkey_admin_cli ACL SETUSER "$username" \
        nocommands \
        +xread +xreadgroup +xadd +xack +xclaim +xpending +xinfo +xlen +xtrim +xrange +xrevrange +xgroup +xdel \
        +fcall +fcall_ro \
        +get +set +setex +setnx +del +exists +ttl +expire +mget +mset +incr +decr +incrby +decrby \
        +hget +hset +hgetall +hdel +hexists +hkeys +hvals +hincrby +hmget +hmset \
        +sadd +smembers +sismember +srem +scard \
        +lpush +rpush +lpop +rpop +lrange +llen +lindex +ltrim \
        +zadd +zrange +zrevrange +zrem +zscore +zcard \
        +keys +scan +ping +publish +auth +select +info +client +multi +exec +discard +time +type +object +debug 2>&1)

    if [[ "$cmd_result" != "OK" ]]; then
        log_error "Failed to set command permissions: $cmd_result"
        return 1
    fi

    log_success "Command permissions configured"

    # Save ACL to file
    log_info "Saving ACL configuration..."

    if valkey_admin_cli ACL SAVE &>/dev/null; then
        log_success "ACL saved to /etc/valkey/users.acl"
    else
        log_warning "Failed to save ACL (changes active but not persisted)"
    fi

    return 0
}

#######################################
# Verification
#######################################

verify_access() {
    local username="$1"
    local password="$2"

    log_step "Verifying user access"

    # Test basic connectivity
    log_info "Testing PING..."
    if timeout 3 env REDISCLI_AUTH="$password" valkey-cli --user "$username" PING &>/dev/null; then
        log_success "Basic connectivity: OK"
    else
        log_error "PING failed"
        return 1
    fi

    # Test topology access
    log_info "Testing topology read access..."
    if timeout 3 env REDISCLI_AUTH="$password" valkey-cli --user "$username" EXISTS "{default}:gnode:topology" &>/dev/null; then
        log_success "Can access {default}:gnode:topology"
    else
        log_warning "Cannot check topology (may not exist yet)"
    fi

    # Test stream access
    log_info "Testing stream access..."
    local stream="{${ENVIRONMENT}}:gnode:unified:default"
    if timeout 3 env REDISCLI_AUTH="$password" valkey-cli --user "$username" EXISTS "$stream" &>/dev/null; then
        log_success "Can access stream: $stream"
    else
        log_info "Stream $stream not yet created (will be created on first use)"
    fi

    return 0
}

#######################################
# Main Script
#######################################

main() {
    echo ""
    echo "╔════════════════════════════════════════════════════════════════════╗"
    echo "║                    gNode Site ACL Setup                              ║"
    echo "╚════════════════════════════════════════════════════════════════════╝"
    echo ""

    log_info "Site ID: $SITE_ID"
    log_info "Environment: $ENVIRONMENT"
    log_info "ACL User: $ACL_USER"
    echo ""

    # Prerequisites
    log_step "Checking prerequisites"
    check_root
    check_valkey
    check_password_dir

    # Check if user already exists
    log_step "Checking existing user"
    if check_user_exists "$ACL_USER"; then
        log_warning "User '$ACL_USER' already exists"

        # Check if password file exists
        if [[ -f "$PASSWORD_FILE" ]]; then
            log_info "Password file exists, will update permissions only"

            read -p "Update ACL permissions for existing user? [Y/n] " -n 1 -r
            echo
            if [[ ! $REPLY =~ ^[Nn]$ ]]; then
                local existing_password
                existing_password=$(cat "$PASSWORD_FILE")

                if create_or_update_user "$ACL_USER" "$existing_password"; then
                    log_success "ACL permissions updated"
                else
                    log_error "Failed to update permissions"
                    exit 1
                fi
            else
                log_info "Skipping permission update"
                exit 0
            fi
        else
            log_error "User exists but password file not found: $PASSWORD_FILE"
            log_info "Cannot proceed without password"
            log_info "Delete user first: VALKEY_USER=gnode_daemon $VALKEY_CLI_SECURE ACL DELUSER $ACL_USER"
            exit 1
        fi
    else
        log_info "User does not exist, will create new user"

        # Generate password
        log_step "Generating password"
        local password
        password=$(generate_password)

        # Store password
        echo -n "$password" > "$PASSWORD_FILE"
        chmod 640 "$PASSWORD_FILE"
        chown "root:${GNODE_GROUP:-gnode}" "$PASSWORD_FILE" 2>/dev/null || chmod 644 "$PASSWORD_FILE"

        log_success "Password stored: $PASSWORD_FILE"

        # Create user with full permissions
        if create_or_update_user "$ACL_USER" "$password"; then
            log_success "User created successfully"
        else
            log_error "Failed to create user"
            exit 1
        fi
    fi

    # Verify access
    local password
    password=$(cat "$PASSWORD_FILE")
    verify_access "$ACL_USER" "$password" || {
        log_warning "Verification failed, but user may still work"
    }

    # Summary
    echo ""
    log_step "Setup Complete!"
    echo ""
    log_success "ACL user: $ACL_USER"
    log_success "Password file: $PASSWORD_FILE"
    log_success "Environment: $ENVIRONMENT"
    echo ""

    log_step "Key Patterns (what this user can access):"
    echo "  Site-specific patterns:"
    echo "    • {${SITE_ID}}:gnode:*       (site streams)"
    echo "    • {${SITE_ID}}:bundle:*    (site bundles)"
    echo "    • ${SITE_ID}:*             (site data)"
    echo ""
    echo "  Environment streams (DTAP):"
    echo "    • {testing}:gnode:*          (testing environment)"
    echo "    • {staging}:gnode:*          (staging environment)"
    echo "    • {acceptance}:gnode:*       (acceptance environment)"
    echo "    • {production}:gnode:*       (production environment)"
    echo ""
    echo "  Shared topology & internal:"
    echo "    • {default}:gnode:*          (daemon topology & shared data)"
    echo "    • gnode:*                    (internal cache keys)"
    echo "    • topology:*               (topology metadata)"
    echo "    • template:*               (template cache)"
    echo ""

    log_step "Next Steps for WordPress Integration:"
    echo ""
    echo "  1. Update WordPress configuration:"
    echo "     File: \${GCUBE_DIR:-/opt/geodineum/gCube}/registration.yaml"
    echo ""
    echo "     valkey:"
    echo "       user: $ACL_USER"
    echo "       password_file: $PASSWORD_FILE"
    echo ""
    echo "     metadata:"
    echo "       environment: $ENVIRONMENT"
    echo ""
    echo "  2. Test WordPress registration:"
    echo "     php \${GCUBE_DIR:-/opt/geodineum/gCube}/bin/register-wordpress-site.php"
    echo ""
    echo "  3. Monitor gNode daemon:"
    echo "     sudo journalctl -u gnode-daemon -f"
    echo ""

    log_step "Testing Access (manual verification):"
    echo ""
    echo "  # Test PING"
    echo "  REDISCLI_AUTH=\"\$(cat $PASSWORD_FILE)\" valkey-cli --user $ACL_USER PING"
    echo ""
    echo "  # Test topology access"
    echo "  REDISCLI_AUTH=\"\$(cat $PASSWORD_FILE)\" valkey-cli --user $ACL_USER GET '{default}:gnode:topology'"
    echo ""
    echo "  # Test stream access"
    echo "  REDISCLI_AUTH=\"\$(cat $PASSWORD_FILE)\" valkey-cli --user $ACL_USER XINFO STREAM '{${ENVIRONMENT}}:gnode:unified:default'"
    echo ""
}

main "$@"
