#!/bin/bash
# gNode Stack Setup - Main Orchestrator
# Complete automated setup for gNode → gNode-Client → gCore → WordPress themes
#
# Usage:
#   sudo ./setup-gnode-stack.sh install    # Fresh installation
#   sudo ./setup-gnode-stack.sh repair     # Repair existing installation
#   sudo ./setup-gnode-stack.sh verify     # Verify installation
#   sudo ./setup-gnode-stack.sh status     # Show system status
#   sudo ./setup-gnode-stack.sh wordpress  # Configure WordPress sites only

set -euo pipefail

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GNODE_ROOT="$(dirname "$SCRIPT_DIR")"

# Load environment configuration
source "${GNODE_ROOT}/.env" 2>/dev/null || {
    echo "Warning: .env file not found, using defaults"
    GNODE_DIR="${GNODE_DIR:-$GNODE_ROOT}"
    GNODE_CLIENT_DIR="${GNODE_CLIENT_DIR:-${GNODE_CLIENT_DIR}}"
}
# Ensure GNODE_DIR matches GNODE_ROOT
GNODE_DIR="$GNODE_ROOT"

# Load libraries
source "$SCRIPT_DIR/setup/lib/common.sh"
source "$SCRIPT_DIR/setup/lib/detect.sh"
source "$SCRIPT_DIR/setup/lib/verify.sh"

# Configuration
VERSION="1.0.0"
CONFIG_FILE="${CONFIG_FILE:-$GNODE_ROOT/.setup-config.yaml}"
STATE_FILE="${STATE_FILE:-$GNODE_ROOT/.setup-state.json}"
LOG_FILE="${LOG_FILE:-$GNODE_ROOT/logs/setup-$(date +%Y%m%d_%H%M%S).log}"

#######################################
# Banner
#######################################

show_banner() {
    cat << 'EOF'
╔══════════════════════════════════════════════════════════════╗
║                                                              ║
║   ██████╗ ███████╗██████╗     ███████╗████████╗ █████╗      ║
║  ██╔════╝ ██╔════╝██╔══██╗    ██╔════╝╚══██╔══╝██╔══██╗     ║
║  ██║  ███╗███████╗██║  ██║    ███████╗   ██║   ███████║     ║
║  ██║   ██║╚════██║██║  ██║    ╚════██║   ██║   ██╔══██║     ║
║  ╚██████╔╝███████║██████╔╝    ███████║   ██║   ██║  ██║     ║
║   ╚═════╝ ╚══════╝╚═════╝     ╚══════╝   ╚═╝   ╚═╝  ╚═╝     ║
║                                                              ║
║     Complete Stack Setup & Configuration System             ║
║                                                              ║
╚══════════════════════════════════════════════════════════════╝
EOF
    echo ""
    info "Version: $VERSION"
    info "Log: $LOG_FILE"
    echo ""
}

#######################################
# Setup Modes
#######################################

mode_install() {
    info "========================================="
    info "  FRESH INSTALLATION MODE"
    info "========================================="
    echo ""

    # Step 1: System check
    info "Step 1/7: System Requirements Check"
    check_system_requirements || {
        error "System requirements not met"
        exit 1
    }
    echo ""

    # Step 2: ValKey
    info "Step 2/7: ValKey Setup"
    if ! check_valkey_installed; then
        if ask_yes_no "ValKey not installed. Run setup?" "y"; then
            run_valkey_setup || {
                error "ValKey setup failed"
                exit 1
            }
        else
            error "ValKey is required. Aborting."
            exit 1
        fi
    else
        success "ValKey already installed"
    fi
    echo ""

    # Step 3: gNode Daemon
    info "Step 3/7: gNode Daemon Setup"
    if ! check_gnode_daemon_installed; then
        if ask_yes_no "gNode Daemon not built. Build now?" "y"; then
            run_gnode_daemon_build || {
                error "gNode Daemon build failed"
                exit 1
            }
        else
            error "gNode Daemon is required. Aborting."
            exit 1
        fi
    else
        success "gNode Daemon already installed"
    fi

    if ! check_gnode_daemon_running; then
        if ask_yes_no "gNode Daemon not running. Start now?" "y"; then
            run_gnode_daemon_start || {
                error "gNode Daemon failed to start"
                exit 1
            }
        fi
    else
        success "gNode Daemon is running"
    fi
    echo ""

    # Step 4: gNode-Client
    info "Step 4/7: gNode-Client Setup"
    if ! check_gnode_client_installed; then
        run_gnode_client_setup || {
            error "gNode-Client setup failed"
            exit 1
        }
    else
        success "gNode-Client already installed"
    fi
    echo ""

    # Step 5: gCore
    info "Step 5/7: gCore Setup"
    if ! check_gcore_installed; then
        run_gcore_setup || {
            error "gCore setup failed"
            exit 1
        }
    else
        success "gCore already installed"
    fi

    # Fix gCore configuration
    fix_gcore_config || warn "gCore config fixes had warnings"
    echo ""

    # Step 6: WordPress Integration
    info "Step 6/7: WordPress Integration"
    setup_wordpress_integration
    echo ""

    # Step 7: Verification
    info "Step 7/7: Final Verification"
    verify_full_system || {
        warn "Some verification checks failed. Review the log."
    }
    echo ""

    # Summary
    success "========================================="
    success "  INSTALLATION COMPLETE!"
    success "========================================="
    info "Next steps:"
    info "  1. Review the log: $LOG_FILE"
    info "  2. Test topology: VALKEY_USER=gnode_daemon $SCRIPT_DIR/valkey-cli-secure.sh GET '{default}:gnode:topology'"
    info "  3. Check status: $0 status"
    echo ""
}

mode_repair() {
    info "========================================="
    info "  REPAIR MODE"
    info "========================================="
    echo ""

    # Detect what's broken
    local -a broken_components=()

    info "Scanning system..."

    if ! verify_valkey "127.0.0.1" "47445" "" "gnode_daemon" 2>/dev/null; then
        broken_components+=("ValKey")
    fi

    if ! verify_gnode_daemon "${GNODE_DAEMON_BIN:-$GNODE_DIR/daemon/target/release/gnode-daemon}" "127.0.0.1" "47445" 2>/dev/null; then
        broken_components+=("gNode Daemon")
    fi

    if ! verify_gnode_client "${GNODE_CLIENT_DIR}" 2>/dev/null; then
        broken_components+=("gNode-Client")
    fi

    if ! verify_gcore "${GCORE_DIR:-/opt/geodineum/gCore}" 2>/dev/null; then
        broken_components+=("gCore")
    fi

    if [[ ${#broken_components[@]} -eq 0 ]]; then
        success "All components are healthy!"
        info "Checking WordPress integrations..."
        source "$SCRIPT_DIR/setup/modules/05-wordpress.sh"
        wordpress_repair
        return 0
    fi

    warn "Found issues with: ${broken_components[*]}"
    echo ""

    # Repair each component
    for component in "${broken_components[@]}"; do
        info "Repairing: $component"

        case "$component" in
            "ValKey")
                systemctl restart valkey-gnode.service || error "Failed to restart ValKey"
                ;;
            "gNode Daemon")
                systemctl restart gnode-daemon.service || error "Failed to restart gNode Daemon"
                ;;
            "gNode-Client")
                run_gnode_client_setup || error "Failed to repair gNode-Client"
                ;;
            "gCore")
                fix_gcore_config || error "Failed to repair gCore"
                ;;
        esac

        echo ""
    done

    # Re-verify
    info "Re-verifying system..."
    verify_full_system

    success "Repair complete"
}

mode_verify() {
    info "========================================="
    info "  VERIFICATION MODE"
    info "========================================="
    echo ""

    verify_full_system

    echo ""
    info "Testing integration..."
    test_topology_discovery

    echo ""
    generate_health_report
}

mode_status() {
    info "========================================="
    info "  SYSTEM STATUS"
    info "========================================="
    echo ""

    generate_system_report ""

    echo ""
    generate_health_report
}

mode_wordpress() {
    info "========================================="
    info "  WORDPRESS CONFIGURATION"
    info "========================================="
    echo ""

    setup_wordpress_integration
}

#######################################
# Component Checks
#######################################

check_system_requirements() {
    local -a missing=()

    # Required commands
    local required_commands=(
        "systemctl:systemd"
        "python3:python3"
        "cargo:rust"
        "php:php"
        "composer:composer"
    )

    for cmd_info in "${required_commands[@]}"; do
        local cmd=$(echo "$cmd_info" | cut -d: -f1)
        local pkg=$(echo "$cmd_info" | cut -d: -f2)

        if ! require_command "$cmd" "apt install $pkg" 2>/dev/null; then
            missing+=("$cmd")
        fi
    done

    if [[ ${#missing[@]} -gt 0 ]]; then
        error "Missing required commands: ${missing[*]}"
        return 1
    fi

    success "All system requirements met"
    return 0
}

check_valkey_installed() {
    # exit-code check, not `list-unit-files | grep -q` (SIGPIPE × pipefail
    # fails the pipeline exactly when the unit exists)
    systemctl cat valkey-gnode.service &>/dev/null
}

check_gnode_daemon_installed() {
    [[ -x "${GNODE_DAEMON_BIN:-$GNODE_DIR/daemon/target/release/gnode-daemon}" ]]
}

check_gnode_daemon_running() {
    systemd_is_running "gnode-daemon.service"
}

check_gnode_client_installed() {
    [[ -d "${GNODE_CLIENT_DIR}" ]] && [[ -f "${GNODE_CLIENT_DIR}/src/Client.php" ]]
}

check_gcore_installed() {
    [[ -d "${GCORE_DIR:-/opt/geodineum/gCore}" ]] && [[ -f "${GCORE_DIR:-/opt/geodineum/gCore}/composer.json" ]]
}

#######################################
# Component Setup Functions
#######################################

run_valkey_setup() {
    if [[ ! -f "$SCRIPT_DIR/setup-valkey-smart.sh" ]]; then
        error "ValKey setup script not found: $SCRIPT_DIR/setup-valkey-smart.sh"
        return 1
    fi

    info "Running ValKey setup..."
    bash "$SCRIPT_DIR/setup-valkey-smart.sh"
}

run_gnode_daemon_build() {
    info "Building gNode Daemon..."

    cd "$GNODE_ROOT/daemon"

    if [[ ! -f "Cargo.toml" ]]; then
        error "Not in gNode daemon directory"
        return 1
    fi

    cargo build --release || {
        error "Cargo build failed"
        return 1
    }

    success "gNode Daemon built successfully"

    # Install systemd service
    if [[ -f "$SCRIPT_DIR/install-gnode-service.sh" ]]; then
        info "Installing systemd service..."
        bash "$SCRIPT_DIR/install-gnode-service.sh"
    fi

    return 0
}

run_gnode_daemon_start() {
    info "Starting gNode Daemon..."

    systemctl enable gnode-daemon.service || warn "Failed to enable service"
    systemctl start gnode-daemon.service || {
        error "Failed to start gNode Daemon"
        return 1
    }

    # Wait for daemon to be ready
    sleep 3

    if systemd_is_running "gnode-daemon.service"; then
        success "gNode Daemon is running"
        return 0
    else
        error "gNode Daemon failed to start"
        journalctl -u gnode-daemon -n 20
        return 1
    fi
}

run_gnode_client_setup() {
    info "Setting up gNode-Client..."

    # Check if already exists
    if [[ -d "${GNODE_CLIENT_DIR}" ]]; then
        success "gNode-Client directory exists"
    else
        error "gNode-Client not found. Please clone from repository."
        info "Run: git clone <gnode-client-repo> ${GNODE_CLIENT_DIR}"
        return 1
    fi

    # Run composer install if needed
    if [[ ! -d "${GNODE_CLIENT_DIR}/vendor" ]]; then
        info "Running composer install..."
        cd "${GNODE_CLIENT_DIR}"
        composer install || {
            error "Composer install failed"
            return 1
        }
    fi

    success "gNode-Client setup complete"
    return 0
}

run_gcore_setup() {
    local gcore_path="${GCORE_DIR:-/opt/geodineum/gCore}"

    info "Setting up gCore..."

    if [[ ! -d "$gcore_path" ]]; then
        error "gCore not found at $gcore_path"
        info "Please clone gCore repository first"
        return 1
    fi

    # Run composer install
    if [[ ! -d "$gcore_path/vendor" ]]; then
        info "Running composer install..."
        cd "$gcore_path"
        composer install || {
            error "Composer install failed"
            return 1
        }
    fi

    success "gCore setup complete"
    return 0
}

fix_gcore_config() {
    local config_file="${GCORE_DIR:-/opt/geodineum/gCore}/config/geometric_topology.yaml"

    info "Checking gCore configuration..."

    if [[ ! -f "$config_file" ]]; then
        error "gCore config not found: $config_file"
        return 1
    fi

    # Check external_client path
    local current_path=$(grep "path:" "$config_file" | grep "gnode-client" | awk '{print $2}' | tr -d '"' || echo "")

    if [[ "$current_path" != "${GNODE_CLIENT_DIR}" ]] && [[ -n "$current_path" ]]; then
        warn "gCore config has incorrect gNode-Client path: $current_path"

        if ask_yes_no "Fix to ${GNODE_CLIENT_DIR}?" "y"; then
            backup_file "$config_file"
            sed -i 's|path: ".*/gnode-client"|path: "${GNODE_CLIENT_DIR}"|g' "$config_file"
            success "Config updated"
        fi
    else
        success "gCore config is correct"
    fi

    return 0
}

setup_wordpress_integration() {
    # Load WordPress module
    if [[ ! -f "$SCRIPT_DIR/setup/modules/05-wordpress.sh" ]]; then
        error "WordPress module not found"
        return 1
    fi

    source "$SCRIPT_DIR/setup/modules/05-wordpress.sh"

    # Interactive site selection
    local selected_sites=$(select_wordpress_sites)

    if [[ "$selected_sites" == "[]" ]] || [[ -z "$selected_sites" ]]; then
        info "No sites selected"
        return 0
    fi

    # Run WordPress setup
    wordpress_setup "$selected_sites" "gCube" "${GCUBE_DIR:-/opt/geodineum/gCube}"
}

#######################################
# Main Entry Point
#######################################

main() {
    show_banner

    # Check if running as root for operations that need it
    if [[ $EUID -ne 0 ]] && [[ "${1:-}" != "status" ]] && [[ "${1:-}" != "verify" ]]; then
        error "This script must be run with sudo for installation/repair operations"
        info "Usage: sudo $0 {install|repair|verify|status|wordpress}"
        exit 1
    fi

    local mode="${1:-install}"

    case "$mode" in
        install)
            mode_install
            ;;
        repair)
            mode_repair
            ;;
        verify)
            mode_verify
            ;;
        status)
            mode_status
            ;;
        wordpress)
            mode_wordpress
            ;;
        help|--help|-h)
            cat << EOF
gNode Stack Setup - Complete automated deployment system

Usage:
    sudo $0 install     - Fresh installation of complete stack
    sudo $0 repair      - Repair existing installation
    $0 verify           - Verify installation (no sudo needed)
    $0 status           - Show system status (no sudo needed)
    sudo $0 wordpress   - Configure WordPress sites only

Options:
    --dry-run          - Show what would be done without making changes
    --verbose          - Enable verbose output
    --help             - Show this help message

Examples:
    # Fresh install
    sudo ./setup-gnode-stack.sh install

    # Repair broken components
    sudo ./setup-gnode-stack.sh repair

    # Check system status
    ./setup-gnode-stack.sh status

    # Add WordPress sites
    sudo ./setup-gnode-stack.sh wordpress

Environment:
    DRY_RUN=1         - Enable dry-run mode
    VERBOSE=1         - Enable verbose logging
    LOG_FILE=<path>   - Custom log file location

For more information, see: \${GNODE_DIR}/docs/DEPLOYMENT.md
EOF
            ;;
        *)
            error "Unknown mode: $mode"
            info "Usage: $0 {install|repair|verify|status|wordpress|help}"
            exit 1
            ;;
    esac
}

# Handle script arguments
if [[ "$*" == *"--dry-run"* ]]; then
    DRY_RUN=1
    info "DRY RUN MODE - No changes will be made"
fi

if [[ "$*" == *"--verbose"* ]]; then
    VERBOSE=1
fi

main "$@"
