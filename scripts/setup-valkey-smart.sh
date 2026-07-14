#!/bin/bash

# gNode Smart Valkey Setup Script
# This script detects existing ValKey installations (native or Docker)
# and uses them instead of creating a new Docker container

set -euo pipefail  # Exit on error, unset vars, and pipe failures

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
CONFIG_DIR="$PROJECT_ROOT/.gnode"
VALKEY_PASSWORD_FILE="$CONFIG_DIR/valkey.password"
VALKEY_CONFIG_FILE="$CONFIG_DIR/valkey.conf"
ENV_FILE="$PROJECT_ROOT/.env"
# Source bootstrap.env for canonical port if available, fallback to 47445
_BOOTSTRAP_ENV="/etc/geodineum/bootstrap.env"
if [[ -f "$_BOOTSTRAP_ENV" ]]; then
    # shellcheck disable=SC1090
    source "$_BOOTSTRAP_ENV"
fi
VALKEY_PORT="${VALKEY_PORT:-47445}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Functions
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_step() {
    echo -e "\n${BLUE}==>${NC} $1"
}

# Generate secure password
generate_password() {
    openssl rand -base64 32 | tr -d "=+/" | cut -c1-32
}

# Create configuration directory
create_config_dir() {
    if [ ! -d "$CONFIG_DIR" ]; then
        log_info "Creating configuration directory: $CONFIG_DIR"
        mkdir -p "$CONFIG_DIR"
        chmod 700 "$CONFIG_DIR"
    fi
}

# Detect existing ValKey installations
detect_valkey_installations() {
    # Redirect all log output to stderr so only the final array goes to stdout
    {
    log_step "Detecting existing ValKey/Redis installations..."

    local installations=()
    local found_count=0

    # 1. Check for native valkey-server binary
    if command -v valkey-server &> /dev/null; then
        log_info "Found: Native valkey-server binary"
        installations+=("native-binary")
        ((found_count++))
    fi

    # 2. Check for running native valkey-server process
    if pgrep -x "valkey-server" > /dev/null 2>&1; then
        log_info "Found: Running native valkey-server process"
        installations+=("native-running")
        ((found_count++))

        # Try to get port from process (with numeric validation)
        NATIVE_PORT=$(ps aux | grep valkey-server | grep -oP '\*:\K[0-9]+' | head -1)
        if [ -n "$NATIVE_PORT" ] && [[ "$NATIVE_PORT" =~ ^[0-9]+$ ]] && [ "$NATIVE_PORT" -ge 1 ] && [ "$NATIVE_PORT" -le 65535 ]; then
            log_info "  Port: $NATIVE_PORT"
        else
            NATIVE_PORT=""
        fi
    fi

    # 3. Check for systemd valkey service (any variant: valkey, valkey-server, valkey-gnode, etc.)
    VALKEY_SERVICES=$(systemctl list-units --type=service --all 2>/dev/null | grep -i "valkey" | awk '{print $1}' || true)

    if [ -n "$VALKEY_SERVICES" ]; then
        for service in $VALKEY_SERVICES; do
            if systemctl is-active --quiet "$service" 2>/dev/null; then
                log_info "Found: systemd service $service (active)"
                installations+=("systemd-service")
                ((found_count++))
                break  # Only count once even if multiple services
            fi
        done

        # If no active service found, check for inactive ones
        if [ "$found_count" -eq 0 ]; then
            for service in $VALKEY_SERVICES; do
                if systemctl list-unit-files 2>/dev/null | grep -q "$service"; then
                    log_warn "Found: systemd service $service (inactive)"
                    installations+=("systemd-service-inactive")
                    ((found_count++))
                    break
                fi
            done
        fi
    fi

    # 4. Check for Docker valkey container (with explicit error handling)
    if command -v docker &> /dev/null; then
        if docker ps > /dev/null 2>&1; then
            # We have docker access
            if docker ps --format '{{.Names}}' 2>/dev/null | grep -q "valkey"; then
                log_info "Found: Running Docker valkey container"
                installations+=("docker-running")
                ((found_count++))

                # Get container name
                DOCKER_CONTAINER=$(docker ps --format '{{.Names}}' | grep valkey | head -1)
                log_info "  Container: $DOCKER_CONTAINER"
            elif docker ps -a --format '{{.Names}}' 2>/dev/null | grep -q "valkey"; then
                log_warn "Found: Stopped Docker valkey container"
                installations+=("docker-stopped")
                ((found_count++))
                DOCKER_CONTAINER=$(docker ps -a --format '{{.Names}}' | grep valkey | head -1)
                log_info "  Container: $DOCKER_CONTAINER"
            fi
        else
            log_warn "Docker installed but cannot access (permission issue?)"
        fi
    fi

    # 5. Test connection to localhost:6379
    if command -v valkey-cli &> /dev/null || command -v redis-cli &> /dev/null; then
        CLI_CMD=$(command -v valkey-cli || command -v redis-cli)
        if timeout 2 $CLI_CMD -h 127.0.0.1 -p 6379 ping > /dev/null 2>&1; then
            log_info "Found: ValKey/Redis responding on 127.0.0.1:6379 (no auth)"
            installations+=("localhost-noauth")
            ((found_count++))
        elif timeout 2 $CLI_CMD -h 127.0.0.1 -p 6379 ping 2>&1 | grep -q "NOAUTH"; then
            log_info "Found: ValKey/Redis responding on 127.0.0.1:6379 (requires auth)"
            installations+=("localhost-auth")
            ((found_count++))
        fi
    fi

    log_info "Total installations detected: $found_count"

    # Close stderr redirect block
    } >&2

    # Return the array as space-separated string (only if not empty)
    # This goes to stdout for capture by the caller
    if [ ${#installations[@]} -gt 0 ]; then
        echo "${installations[@]}"
    fi
}

# Test connection with authentication
test_valkey_connection() {
    local host=${1:-127.0.0.1}
    local port=${2:-6379}
    local password=$3

    CLI_CMD=$(command -v valkey-cli || command -v redis-cli || echo "")

    if [ -z "$CLI_CMD" ]; then
        log_error "Neither valkey-cli nor redis-cli found"
        return 1
    fi

    if [ -n "$password" ]; then
        timeout 2 $CLI_CMD -h "$host" -p "$port" -a "$password" ping > /dev/null 2>&1
    else
        timeout 2 $CLI_CMD -h "$host" -p "$port" ping > /dev/null 2>&1
    fi
}

# Get password from existing installation
get_existing_password() {
    # Redirect all log output to stderr so only the password goes to stdout
    {
    log_step "Attempting to retrieve existing ValKey password..."

    local found_password=""

    # 1. Check for running service config file first
    log_info "Detecting ValKey service configuration..."
    local service_configs=()

    # Find all valkey services
    for service in $(systemctl list-units --type=service --all 2>/dev/null | grep -i valkey | awk '{print $1}'); do
        # Get the ExecStart line to find config file
        local config_path=$(systemctl cat "$service" 2>/dev/null | grep "ExecStart" | grep -oP "/etc/[^ ]*\.conf" | head -1)
        if [ -n "$config_path" ] && [ -f "$config_path" ]; then
            log_info "Found service config: $service → $config_path"
            service_configs+=("$config_path")
        fi
    done

    # 2. Add common config file locations
    local config_files=(
        "${service_configs[@]}"
        "/etc/valkey/valkey-gnode.conf"
        "/etc/valkey/valkey.conf"
        "/etc/redis/redis.conf"
        "/usr/local/etc/valkey/valkey.conf"
        "/opt/valkey/valkey.conf"
        "/var/lib/valkey/valkey.conf"
    )

    # Try each config file
    for config_file in "${config_files[@]}"; do
        if [ -n "$config_file" ] && [ -f "$config_file" ]; then
            log_info "Checking config file: $config_file"
            local temp_pass=$(sudo grep "^requirepass" "$config_file" 2>/dev/null | awk '{print $2}' | tr -d '"' | head -1)
            if [ -n "$temp_pass" ]; then
                log_info "✓ Found password in $config_file"
                found_password="$temp_pass"
                break  # Found it, exit loop
            fi
        fi
    done

    # 2. Check environment variables
    if [ -n "$REDIS_PASSWORD" ]; then
        log_info "Found password in REDIS_PASSWORD environment variable"
        found_password="$REDIS_PASSWORD"
    elif [ -n "$VALKEY_PASSWORD" ]; then
        log_info "Found password in VALKEY_PASSWORD environment variable"
        found_password="$VALKEY_PASSWORD"
    fi

    # Close stderr redirect block before interactive prompt
    } >&2

    # 3. Prompt user if still not found (this needs to be interactive)
    if [ -z "$found_password" ]; then
        echo "[WARN] Could not automatically detect password" >&2
        read -p "Enter existing ValKey password (or press Enter to generate new): " user_password
        if [ -n "$user_password" ]; then
            echo "$user_password"
            return 0
        fi
        return 1
    fi

    # Return password to stdout (for capture)
    echo "$found_password"
    return 0
}

# Configure authentication for existing installation
configure_existing_auth() {
    local install_type=$1
    local password=$2

    log_step "Configuring authentication for existing installation..."

    case $install_type in
        "native-running"|"systemd-service")
            # For native/systemd installations, update config file
            local config_file=$(find /etc /usr/local/etc /opt -name "valkey.conf" 2>/dev/null | head -1)

            if [ -z "$config_file" ]; then
                log_warn "Could not find ValKey config file"
                log_info "You may need to manually add: requirepass $password"
                return 1
            fi

            log_info "Updating config file: $config_file"

            # Backup original
            sudo cp "$config_file" "$config_file.backup.$(date +%Y%m%d_%H%M%S)"

            # Update or add requirepass
            if grep -q "^requirepass" "$config_file"; then
                sudo sed -i "s/^requirepass .*/requirepass $password/" "$config_file"
            else
                echo "requirepass $password" | sudo tee -a "$config_file" > /dev/null
            fi

            # Restart service
            log_info "Restarting ValKey service..."
            if systemctl is-active --quiet valkey 2>/dev/null; then
                sudo systemctl restart valkey
            elif systemctl is-active --quiet valkey-server 2>/dev/null; then
                sudo systemctl restart valkey-server
            else
                log_warn "Could not restart service automatically. Please restart manually."
            fi
            ;;

        "docker-running")
            log_info "Docker container already configured"
            log_warn "If password is incorrect, you may need to recreate the container"
            ;;

        "localhost-noauth")
            log_warn "ValKey is running without authentication"
            log_warn "Consider enabling authentication for security"
            ;;
    esac
}

# Setup using existing installation
setup_existing_installation() {
    local install_type=$1

    log_step "Setting up gNode to use existing ValKey installation..."

    # Try to get existing password
    existing_password=$(get_existing_password) || true

    if [ -n "$existing_password" ]; then
        # Test if password works
        if test_valkey_connection "127.0.0.1" "$VALKEY_PORT" "$existing_password"; then
            log_info "✓ Existing password works!"
            VALKEY_PASSWORD="$existing_password"
        else
            log_warn "Provided password does not work"
            log_info "Generating new password..."
            VALKEY_PASSWORD=$(generate_password)
            configure_existing_auth "$install_type" "$VALKEY_PASSWORD"
        fi
    else
        # No existing password, check if auth is required
        if test_valkey_connection "127.0.0.1" "$VALKEY_PORT" ""; then
            log_warn "ValKey is running WITHOUT authentication"
            read -p "Configure authentication? (recommended) [Y/n]: " -n 1 -r
            echo
            if [[ ! $REPLY =~ ^[Nn]$ ]]; then
                VALKEY_PASSWORD=$(generate_password)
                configure_existing_auth "$install_type" "$VALKEY_PASSWORD"
            else
                log_warn "Continuing without authentication (NOT recommended for production)"
                VALKEY_PASSWORD=""
            fi
        else
            log_error "Cannot connect to ValKey and no password found"
            log_info "Please provide password or fix connection"
            exit 1
        fi
    fi

    # Save password
    if [ -n "$VALKEY_PASSWORD" ]; then
        echo "$VALKEY_PASSWORD" > "$VALKEY_PASSWORD_FILE"
        chmod 600 "$VALKEY_PASSWORD_FILE"
        log_info "Password saved to $VALKEY_PASSWORD_FILE"
    fi
}

# Update .env file
update_env_file() {
    log_info "Updating .env file..."

    # Backup existing .env if it exists
    if [ -f "$ENV_FILE" ]; then
        cp "$ENV_FILE" "$ENV_FILE.backup.$(date +%Y%m%d_%H%M%S)"
    fi

    # Create temp file and preserve non-ValKey/Redis entries
    TEMP_ENV=$(mktemp)

    if [ -f "$ENV_FILE" ]; then
        # Keep only valid, non-ValKey/Redis lines (skip comments, errors, empty lines)
        grep -v "^REDIS_\|^VALKEY_\|^#\|^$\|^(error)" "$ENV_FILE" > "$TEMP_ENV" 2>/dev/null || true
    fi

    # Add ValKey configuration section
    cat >> "$TEMP_ENV" << EOF

# ==============================================================================
# ValKey Configuration (auto-detected by setup-valkey-smart.sh)
# ==============================================================================
# VALKEY_* vars: For clarity and future compatibility
# REDIS_* vars:  Used by PHP client (gCore) and scripts
# gNode Daemon:    Uses --redis-host/port/auth CLI flags
# ==============================================================================

VALKEY_HOST=127.0.0.1
VALKEY_PORT=$VALKEY_PORT
VALKEY_PASSWORD=$VALKEY_PASSWORD

REDIS_HOST=127.0.0.1
REDIS_PORT=$VALKEY_PORT
REDIS_AUTH=$VALKEY_PASSWORD
EOF

    # Move temp file to .env
    mv "$TEMP_ENV" "$ENV_FILE"
    chmod 600 "$ENV_FILE"

    log_info ".env file updated and secured (mode 600)"
}

# Create gNode configuration
create_gnode_config() {
    GNODE_CONFIG_FILE="$CONFIG_DIR/gnode.conf"
    log_info "Creating gNode configuration file..."

    cat > "$GNODE_CONFIG_FILE" << EOF
# gNode Configuration
# Auto-generated by setup-valkey-smart.sh

# ValKey connection settings
redis_host=127.0.0.1
redis_port=$VALKEY_PORT
redis_auth=$VALKEY_PASSWORD

# Default gNode settings
site_id=default
node_id=default
stream_prefix=gnode
dimensions=8
threads=4
log_level=info
EOF

    chmod 600 "$GNODE_CONFIG_FILE"
    log_info "gNode configuration saved to $GNODE_CONFIG_FILE"
}

# Main execution
main() {
    echo -e "${BLUE}╔════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║   gNode Smart ValKey Setup (Auto-Detect) ║${NC}"
    echo -e "${BLUE}╚════════════════════════════════════════╝${NC}"
    echo

    # Check prerequisites
    if ! command -v openssl &> /dev/null; then
        log_error "OpenSSL is not installed"
        exit 1
    fi

    # Create config directory
    create_config_dir

    # Detect installations
    installations=$(detect_valkey_installations)

    # Convert to array and validate
    IFS=' ' read -ra install_array <<< "$installations"

    # Debug output
    log_info "Detection returned: '$installations'"
    log_info "Array size: ${#install_array[@]}"

    # Check if we actually found any valid installations
    if [ ${#install_array[@]} -eq 0 ] || [ -z "$installations" ]; then
        log_warn "No existing ValKey installation detected"
        echo
        read -p "Install ValKey via Docker? [Y/n]: " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Nn]$ ]]; then
            log_info "Running standard ValKey setup..."
            exec "$SCRIPT_DIR/setup-valkey.sh"
        else
            log_error "Cannot proceed without ValKey"
            exit 1
        fi
    fi

    # Display found installations
    log_step "Found installations:"
    for i in "${!install_array[@]}"; do
        echo "  $((i+1)). ${install_array[$i]}"
    done

    # Determine which to use
    if [ ${#install_array[@]} -eq 1 ]; then
        log_info "Using: ${install_array[0]}"
        selected_install="${install_array[0]}"
    else
        echo
        read -p "Which installation to use? [1-${#install_array[@]}]: " choice

        # Validate choice
        if [ -z "$choice" ] || [ "$choice" -lt 1 ] || [ "$choice" -gt "${#install_array[@]}" ]; then
            log_error "Invalid selection"
            exit 1
        fi

        selected_install="${install_array[$((choice-1))]}"
        log_info "Using: $selected_install"
    fi

    # Setup with selected installation
    setup_existing_installation "$selected_install"

    # Update configuration files
    update_env_file
    create_gnode_config

    # Final test
    log_step "Testing connection..."
    if test_valkey_connection "127.0.0.1" "$VALKEY_PORT" "$VALKEY_PASSWORD"; then
        log_info "✓ Connection successful!"
    else
        log_error "Connection test failed"
        log_warn "Please verify configuration manually"
        exit 1
    fi

    echo
    echo -e "${GREEN}╔════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║        Setup Complete! 🎉             ║${NC}"
    echo -e "${GREEN}╚════════════════════════════════════════╝${NC}"
    echo
    echo "Using existing ValKey installation"
    echo "Password stored in: $VALKEY_PASSWORD_FILE"
    echo
    echo "gNode is now configured to use your existing ValKey"
    echo
}

# Run main function
main "$@"
