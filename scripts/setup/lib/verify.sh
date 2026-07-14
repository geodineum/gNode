#!/bin/bash
# gNode Setup System - Verification Library

# Source common library if not already loaded
if [[ -z "${COMMON_LIB_LOADED:-}" ]]; then
    source "$(dirname "${BASH_SOURCE[0]}")/common.sh"
    COMMON_LIB_LOADED=1
fi

#######################################
# Component Verification
#######################################

verify_valkey() {
    local valkey_host=${1:-"127.0.0.1"}
    local valkey_port=${2:-"47445"}
    local valkey_password=${3:-""}
    local valkey_user=${4:-"default"}

    info "Verifying ValKey installation..."

    # Check service is running
    if ! systemd_is_running "valkey-gnode.service"; then
        error "ValKey service is not running"
        return 1
    fi
    success "ValKey service is running"

    # Check port is accessible
    if ! check_port "$valkey_host" "$valkey_port" 2; then
        error "ValKey port $valkey_port is not accessible"
        return 1
    fi
    success "ValKey port is accessible"

    # Check authentication
    if [[ -n "$valkey_password" ]]; then
        local auth_test
        if [[ -n "$valkey_user" ]] && [[ "$valkey_user" != "default" ]]; then
            auth_test=$(VALKEY_USER="$valkey_user" VALKEY_PASSWORD="$valkey_password" \
                ${GNODE_SCRIPTS_DIR:-${GNODE_DIR:-/opt/gNode}/scripts}/valkey-cli-secure.sh PING 2>/dev/null || echo "")
        else
            auth_test=$(echo "AUTH $valkey_password"$'\n'"PING" | nc -w 2 "$valkey_host" "$valkey_port" 2>/dev/null | tail -1 || echo "")
        fi

        if [[ "$auth_test" != *"PONG"* ]]; then
            error "ValKey authentication failed"
            return 1
        fi
        success "ValKey authentication successful"
    fi

    return 0
}

verify_gnode_daemon() {
    local daemon_path=${1:-"${GNODE_DAEMON_BIN:-${GNODE_DIR:-/opt/gNode}/daemon/target/release/gnode-daemon}"}
    local valkey_host=${2:-"127.0.0.1"}
    local valkey_port=${3:-"47445"}

    info "Verifying gNode daemon installation..."

    # Check daemon binary exists
    if [[ ! -x "$daemon_path" ]]; then
        error "gNode daemon binary not found or not executable: $daemon_path"
        return 1
    fi
    success "gNode daemon binary exists"

    # Check service is running
    if ! systemd_is_running "gnode-daemon.service"; then
        error "gNode daemon service is not running"
        return 1
    fi
    success "gNode daemon service is running"

    # Check topology is registered
    local topology_key="{default}:gnode:topology"
    local topology_exists=$(VALKEY_USER=gnode_daemon \
        ${GNODE_SCRIPTS_DIR:-${GNODE_DIR:-/opt/gNode}/scripts}/valkey-cli-secure.sh EXISTS "$topology_key" 2>/dev/null || echo "0")

    if [[ "$topology_exists" != "1" ]]; then
        error "gNode topology not found in ValKey"
        return 1
    fi
    success "gNode topology is registered"

    # Check ORCHESTRATOR tier is registered
    local orchestrator_services=$(VALKEY_USER=gnode_daemon \
        ${GNODE_SCRIPTS_DIR:-${GNODE_DIR:-/opt/gNode}/scripts}/valkey-cli-secure.sh FCALL GNODE_GEOMETRIC_DISCOVER \
        1 "$topology_key" '{"requirements":[{"name":"topology_tier","min_value":0.0,"max_value":0.0}]}' \
        2>/dev/null || echo "[]")

    if [[ "$orchestrator_services" == "[]" ]] || [[ "$orchestrator_services" == "" ]]; then
        error "ORCHESTRATOR tier not registered in topology"
        return 1
    fi
    success "ORCHESTRATOR tier is registered"

    # Check ValKey functions are loaded
    local functions_count=$(VALKEY_USER=gnode_daemon \
        ${GNODE_SCRIPTS_DIR:-${GNODE_DIR:-/opt/gNode}/scripts}/valkey-cli-secure.sh FCALL_RO GNODE_TEST_PING 0 2>/dev/null | grep -c "PONG" || echo "0")

    if [[ "$functions_count" -lt 1 ]]; then
        warn "ValKey functions may not be loaded correctly"
    else
        success "ValKey functions are loaded"
    fi

    return 0
}

verify_gnode_client() {
    local client_path=${1:-"${GNODE_CLIENT_DIR:-/opt/gNode-Client}"}

    info "Verifying gNode-Client installation..."

    # Check client directory exists
    if [[ ! -d "$client_path" ]]; then
        error "gNode-Client directory not found: $client_path"
        return 1
    fi
    success "gNode-Client directory exists"

    # Check required files
    local required_files=(
        "$client_path/src/Client.php"
        "$client_path/src/Storage/ValKeyStorage.php"
        "$client_path/src/ConsumerGroupHandler.php"
        "$client_path/composer.json"
    )

    for file in "${required_files[@]}"; do
        if [[ ! -f "$file" ]]; then
            error "Required file not found: $file"
            return 1
        fi
    done
    success "All required files present"

    # Check Composer autoload
    if [[ -f "$client_path/vendor/autoload.php" ]]; then
        success "Composer autoload exists"
    else
        warn "Composer autoload not found (may need to run composer install)"
    fi

    return 0
}

verify_gcore() {
    local gcore_path=${1:-"${GCORE_DIR:-/opt/geodineum/gCore}"}

    info "Verifying gCore installation..."

    # Check gCore directory exists
    if [[ ! -d "$gcore_path" ]]; then
        error "gCore directory not found: $gcore_path"
        return 1
    fi
    success "gCore directory exists"

    # Check required files
    local required_files=(
        "$gcore_path/Modules/Core/gCore.php"
        "$gcore_path/Modules/Core/Client/ExternalgNodeAdapter.php"
        "$gcore_path/config/geometric_topology.yaml"
        "$gcore_path/composer.json"
    )

    for file in "${required_files[@]}"; do
        if [[ ! -f "$file" ]]; then
            error "Required file not found: $file"
            return 1
        fi
    done
    success "All required files present"

    # Check Composer installation
    if [[ ! -f "$gcore_path/vendor/autoload.php" ]]; then
        error "gCore Composer dependencies not installed"
        return 1
    fi
    success "Composer dependencies installed"

    # Check gNode-Client is in Composer
    if [[ ! -d "$gcore_path/vendor/geodineum/gnode-client" ]]; then
        warn "gNode-Client not found in gCore vendor directory"
    else
        success "gNode-Client is linked via Composer"
    fi

    # Check geometric_topology.yaml configuration
    local gnode_client_path=$(grep "path:" "$gcore_path/config/geometric_topology.yaml" 2>/dev/null | grep "gnode-client" | awk '{print $2}' | tr -d '"')

    if [[ "$gnode_client_path" == "${GNODE_CLIENT_DIR:-/opt/gNode-Client}" ]] || [[ "$gnode_client_path" == "" ]]; then
        success "gNode-Client path is correctly configured"
    else
        warn "gNode-Client path may be incorrect: $gnode_client_path"
    fi

    return 0
}

verify_wordpress_integration() {
    local wp_path=$1
    local theme_name=${2:-"gCube"}

    info "Verifying WordPress integration for: $(basename "$wp_path")"

    # Check WordPress installation
    if [[ ! -f "$wp_path/wp-config.php" ]]; then
        error "Not a valid WordPress installation: $wp_path"
        return 1
    fi
    success "Valid WordPress installation"

    # Check theme is deployed
    if [[ -d "$wp_path/wp-content/themes/$theme_name" ]] || [[ -L "$wp_path/wp-content/themes/$theme_name" ]]; then
        success "Theme $theme_name is deployed"
    else
        warn "Theme $theme_name not found in wp-content/themes"
    fi

    # Check gCore integration (look for gCore in functions.php or autoload)
    local has_gcore="false"
    if [[ -f "$wp_path/wp-content/themes/$theme_name/functions.php" ]]; then
        if grep -q "gCore" "$wp_path/wp-content/themes/$theme_name/functions.php" 2>/dev/null; then
            has_gcore="true"
            success "gCore integration found in theme"
        fi
    fi

    if [[ "$has_gcore" == "false" ]]; then
        warn "gCore integration not detected"
    fi

    # Check password file accessibility
    local password_file="${GNODE_PASSWORD_DIR:-${GNODE_DIR:-/opt/gNode}/.gnode}/valkey_client.password"
    if [[ -f "$password_file" ]]; then
        # Test if www-data can read it
        if sudo -u www-data test -r "$password_file" 2>/dev/null; then
            success "Password file is accessible to www-data"
        else
            error "Password file not accessible to www-data"
            info "Run: sudo chmod 640 $password_file"
            return 1
        fi
    else
        error "Password file not found: $password_file"
        return 1
    fi

    return 0
}

#######################################
# Full System Verification
#######################################

verify_full_system() {
    local valkey_host=${1:-"127.0.0.1"}
    local valkey_port=${2:-"47445"}
    local -a failures=()

    info "Running full system verification..."
    echo ""

    # Verify ValKey
    if ! verify_valkey "$valkey_host" "$valkey_port" "" "gnode_daemon"; then
        failures+=("valkey")
    fi
    echo ""

    # Verify gNode Daemon
    if ! verify_gnode_daemon "${GNODE_DAEMON_BIN:-${GNODE_DIR:-/opt/gNode}/daemon/target/release/gnode-daemon}" "$valkey_host" "$valkey_port"; then
        failures+=("gnode-daemon")
    fi
    echo ""

    # Verify gNode-Client
    if ! verify_gnode_client "${GNODE_CLIENT_DIR:-/opt/gNode-Client}"; then
        failures+=("gnode-client")
    fi
    echo ""

    # Verify gCore
    if ! verify_gcore "${GCORE_DIR:-/opt/geodineum/gCore}"; then
        failures+=("gcore")
    fi
    echo ""

    # Summary
    if [[ ${#failures[@]} -eq 0 ]]; then
        success "========================================="
        success "  All system components verified!"
        success "========================================="
        return 0
    else
        error "========================================="
        error "  Verification failed for: ${failures[*]}"
        error "========================================="
        return 1
    fi
}

#######################################
# Integration Tests
#######################################

test_topology_discovery() {
    local topology_key="{default}:gnode:topology"

    info "Testing topology discovery..."

    # Test ORCHESTRATOR tier
    local orchestrator=$(VALKEY_USER=gnode_daemon \
        ${GNODE_SCRIPTS_DIR:-${GNODE_DIR:-/opt/gNode}/scripts}/valkey-cli-secure.sh FCALL GNODE_GEOMETRIC_DISCOVER \
        1 "$topology_key" '{"requirements":[{"name":"topology_tier","min_value":0.0,"max_value":0.0}]}' \
        2>/dev/null || echo "[]")

    if [[ "$orchestrator" == "[]" ]] || [[ "$orchestrator" == "" ]]; then
        error "ORCHESTRATOR tier discovery failed"
        return 1
    fi
    success "ORCHESTRATOR tier discovery: $orchestrator"

    # Test TOOL tier
    local tools=$(VALKEY_USER=gnode_daemon \
        ${GNODE_SCRIPTS_DIR:-${GNODE_DIR:-/opt/gNode}/scripts}/valkey-cli-secure.sh FCALL GNODE_GEOMETRIC_DISCOVER \
        1 "$topology_key" '{"requirements":[{"name":"topology_tier","min_value":0.1,"max_value":0.1}]}' \
        2>/dev/null || echo "[]")

    info "TOOL tier discovery: $tools"

    # Count services
    local service_count=$(VALKEY_USER=gnode_daemon \
        ${GNODE_SCRIPTS_DIR:-${GNODE_DIR:-/opt/gNode}/scripts}/valkey-cli-secure.sh GET "$topology_key" 2>/dev/null | \
        python3 -c "import json, sys; data=json.load(sys.stdin); print(len(data['services']))" 2>/dev/null || echo "0")

    success "Total services registered: $service_count"

    return 0
}

test_wordpress_gnode_access() {
    local wp_path=$1

    info "Testing WordPress gNode access..."

    # Test PHP can initialize gCore and access gNode
    local test_result=$(cd "$wp_path" && php -r "
        require_once 'wp-load.php';
        try {
            \$gCore = \\gCore\\Modules\\Core\\gCore::getInstance();
            if (\$gCore) {
                echo 'SUCCESS: gCore initialized';
            } else {
                echo 'ERROR: gCore getInstance returned null';
            }
        } catch (Exception \$e) {
            echo 'ERROR: ' . \$e->getMessage();
        }
    " 2>&1)

    if [[ "$test_result" == *"SUCCESS"* ]]; then
        success "$test_result"
        return 0
    else
        error "$test_result"
        return 1
    fi
}

#######################################
# Health Check Summary
#######################################

generate_health_report() {
    info "Generating health report..."
    echo ""

    local -A status=(
        [valkey]="unknown"
        [gnode_daemon]="unknown"
        [gnode_client]="unknown"
        [gcore]="unknown"
    )

    # Check each component
    if verify_valkey "127.0.0.1" "47445" "" "gnode_daemon" &>/dev/null; then
        status[valkey]="healthy"
    else
        status[valkey]="unhealthy"
    fi

    if verify_gnode_daemon "${GNODE_DAEMON_BIN:-${GNODE_DIR:-/opt/gNode}/daemon/target/release/gnode-daemon}" "127.0.0.1" "47445" &>/dev/null; then
        status[gnode_daemon]="healthy"
    else
        status[gnode_daemon]="unhealthy"
    fi

    if verify_gnode_client "${GNODE_CLIENT_DIR:-/opt/gNode-Client}" &>/dev/null; then
        status[gnode_client]="healthy"
    else
        status[gnode_client]="unhealthy"
    fi

    if verify_gcore "${GCORE_DIR:-/opt/geodineum/gCore}" &>/dev/null; then
        status[gcore]="healthy"
    else
        status[gcore]="unhealthy"
    fi

    # Display report
    echo "========================================="
    echo "           HEALTH REPORT"
    echo "========================================="
    for component in valkey gnode_daemon gnode_client gcore; do
        local status_str="${status[$component]}"
        if [[ "$status_str" == "healthy" ]]; then
            echo -e "  ${GREEN}✓${NC} $component: $status_str"
        else
            echo -e "  ${RED}✗${NC} $component: $status_str"
        fi
    done
    echo "========================================="
}
