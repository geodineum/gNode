#!/bin/bash
#
# DEPRECATED: Use setup-geodineum.sh instead
#
# This script is a compatibility wrapper that forwards to the unified
# Geodineum ecosystem installer. It will be removed in a future release.
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo ""
echo -e "\033[1;33m[DEPRECATED]\033[0m setup-gnode.sh is deprecated."
echo -e "             Use \033[1msetup-geodineum.sh\033[0m instead for the unified installer."
echo ""
echo "Forwarding to setup-geodineum.sh..."
echo ""

exec "$SCRIPT_DIR/setup-geodineum.sh" "$@"

# The rest of this file is kept for reference only — exec above never returns.

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# Script directory
# SCRIPT_DIR already set above
cd "$SCRIPT_DIR"

# Configuration
INSTALL_AS_SERVICE=true  # Default to systemd service
SKIP_TESTS=false
VALKEY_SETUP_MODE="smart"  # smart, production, or skip
SKIP_APACHE=false

# Progress tracking
STEP=0
TOTAL_STEPS=9

#=============================================================================
# Helper Functions
#=============================================================================

print_header() {
    echo
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BOLD}$1${NC}"
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo
}

print_step() {
    STEP=$((STEP + 1))
    echo
    echo -e "${BLUE}[Step $STEP/$TOTAL_STEPS]${NC} ${BOLD}$1${NC}"
    echo
}

print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

print_info() {
    echo -e "${CYAN}ℹ${NC} $1"
}

ask_yes_no() {
    local prompt=$1
    local default=${2:-y}

    if [ "$default" = "y" ]; then
        read -p "$prompt [Y/n] " -n 1 -r
    else
        read -p "$prompt [y/N] " -n 1 -r
    fi
    echo

    if [ "$default" = "y" ]; then
        [[ ! $REPLY =~ ^[Nn]$ ]]
    else
        [[ $REPLY =~ ^[Yy]$ ]]
    fi
}

check_command() {
    command -v "$1" &> /dev/null
}

#=============================================================================
# Setup Steps
#=============================================================================

show_welcome() {
    clear
    print_header "gNode (Geodineum Service Daemon) - Complete Setup"

    echo -e "${BOLD}This script will install and configure gNode with:${NC}"
    echo "  • Rust toolchain (if needed)"
    echo "  • Docker (if needed)"
    echo "  • ValKey server (Redis fork)"
    echo "  • gNode daemon compilation"
    echo "  • ~170 ValKey functions"
    echo "  • Systemd service integration (optional)"
    echo "  •verification"
    echo
    echo -e "${YELLOW}Estimated time: 5-10 minutes${NC}"
    echo -e "${YELLOW}Internet connection required for downloads${NC}"
    echo

    if ! ask_yes_no "Continue with installation?"; then
        echo "Installation cancelled."
        exit 0
    fi
}

parse_arguments() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --no-service)
                INSTALL_AS_SERVICE=false
                shift
                ;;
            --skip-tests)
                SKIP_TESTS=true
                shift
                ;;
            --valkey-production)
                VALKEY_SETUP_MODE="production"
                shift
                ;;
            --valkey-skip)
                VALKEY_SETUP_MODE="skip"
                shift
                ;;
            --skip-apache)
                SKIP_APACHE=true
                shift
                ;;
            -h|--help)
                echo "Usage: $0 [OPTIONS]"
                echo
                echo "Options:"
                echo "  --no-service         Don't install as systemd service"
                echo "  --skip-tests         Skip verification tests"
                echo "  --skip-apache        Skip Apache2 optimization"
                echo "  --valkey-production  Use production ValKey setup (creates systemd service)"
                echo "  --valkey-skip        Skip ValKey setup (use existing installation)"
                echo "  -h, --help           Show this help message"
                exit 0
                ;;
            *)
                print_error "Unknown option: $1"
                echo "Use --help for usage information"
                exit 1
                ;;
        esac
    done
}

step_prerequisites() {
    print_step "Checking Prerequisites"

    # Check if init-gnode.sh exists
    if [ ! -f "$SCRIPT_DIR/scripts/init-gnode.sh" ]; then
        print_error "scripts/init-gnode.sh not found"
        exit 1
    fi

    # Run init-gnode.sh for Rust and Docker setup
    print_info "Running prerequisite checks and setup..."

    # Run init script but capture its output
    if bash "$SCRIPT_DIR/scripts/init-gnode.sh"; then
        print_success "Prerequisites installed successfully"
    else
        print_error "Prerequisites installation failed"
        exit 1
    fi
}

step_apache_optimize() {
    print_step "Optimizing Apache2 for gNode Performance"

    # Check if skipped via flag
    if [ "$SKIP_APACHE" = true ]; then
        print_warning "Apache optimization skipped (--skip-apache)"
        return 0
    fi

    # Check if Apache2 is installed
    if ! check_command apachectl && ! check_command apache2ctl; then
        print_warning "Apache2 not detected, skipping optimization"
        print_info "Run scripts/setup/modules/10-apache-optimize.sh manually if needed"
        return 0
    fi

    # Check if we have the optimization script
    APACHE_SCRIPT="$SCRIPT_DIR/scripts/setup/modules/10-apache-optimize.sh"
    if [ ! -f "$APACHE_SCRIPT" ]; then
        print_warning "Apache optimization script not found"
        return 0
    fi

    if ask_yes_no "Optimize Apache2 for gNode performance (recommended)?"; then
        print_info "Running Apache2 optimization..."

        if sudo bash "$APACHE_SCRIPT"; then
            print_success "Apache2 optimized for high-performance gNode operations"
        else
            print_warning "Apache optimization completed with warnings"
        fi
    else
        print_info "Skipping Apache optimization"
        print_info "Run manually later: sudo $APACHE_SCRIPT"
    fi
}

step_valkey_setup() {
    print_step "Setting Up ValKey Server"

    if [ "$VALKEY_SETUP_MODE" = "skip" ]; then
        print_warning "Skipping ValKey setup as requested"

        # Check if ValKey is accessible
        if [ -f "$SCRIPT_DIR/.gnode/valkey.password" ]; then
            VALKEY_PASSWORD=$(cat "$SCRIPT_DIR/.gnode/valkey.password")
            if docker exec valkey valkey-cli -a "$VALKEY_PASSWORD" PING 2>/dev/null | grep -q "PONG"; then
                print_success "ValKey is accessible (Docker)"
            elif valkey-cli -a "$VALKEY_PASSWORD" PING 2>/dev/null | grep -q "PONG"; then
                print_success "ValKey is accessible (Native)"
            else
                print_error "ValKey is not accessible"
                exit 1
            fi
        else
            print_error "ValKey password file not found and setup skipped"
            exit 1
        fi
        return
    fi

    # Choose setup script based on mode
    if [ "$VALKEY_SETUP_MODE" = "production" ]; then
        print_info "Using production ValKey setup (systemd service)..."
        SETUP_SCRIPT="$SCRIPT_DIR/scripts/setup-valkey-production.sh"
    else
        print_info "Using smart ValKey setup (auto-detect existing)..."
        SETUP_SCRIPT="$SCRIPT_DIR/scripts/setup-valkey-smart.sh"
    fi

    if [ ! -f "$SETUP_SCRIPT" ]; then
        print_error "Setup script not found: $SETUP_SCRIPT"
        exit 1
    fi

    # Run ValKey setup
    if bash "$SETUP_SCRIPT"; then
        print_success "ValKey setup completed"
    else
        print_error "ValKey setup failed"
        exit 1
    fi

    # Verify ValKey is accessible
    if [ -f "$SCRIPT_DIR/.gnode/valkey.password" ]; then
        VALKEY_PASSWORD=$(cat "$SCRIPT_DIR/.gnode/valkey.password")
        if docker exec valkey valkey-cli -a "$VALKEY_PASSWORD" PING 2>/dev/null | grep -q "PONG"; then
            print_success "ValKey connection verified (Docker)"
        elif valkey-cli -a "$VALKEY_PASSWORD" PING 2>/dev/null | grep -q "PONG"; then
            print_success "ValKey connection verified (Native)"
        else
            print_warning "ValKey connection could not be verified"
        fi
    fi
}

step_build_daemon() {
    print_step "Building gNode Daemon"

    # Check if already built and recent
    DAEMON_BINARY="$SCRIPT_DIR/daemon/target/release/gnode-daemon"
    if [ -f "$DAEMON_BINARY" ]; then
        # Check if binary is less than 1 hour old
        if [ $(find "$DAEMON_BINARY" -mmin -60 2>/dev/null | wc -l) -gt 0 ]; then
            print_info "Daemon binary is recent, skipping rebuild"
            print_success "Using existing daemon binary"
            return
        fi
    fi

    print_info "Compiling daemon (this may take a few minutes)..."

    cd "$SCRIPT_DIR/daemon"
    if cargo build --release 2>&1 | grep -E "(Compiling|Finished|error)"; then
        if [ -f "$DAEMON_BINARY" ]; then
            print_success "Daemon compiled successfully"
        else
            print_error "Daemon compilation failed - binary not found"
            exit 1
        fi
    else
        print_error "Daemon compilation failed"
        exit 1
    fi

    cd "$SCRIPT_DIR"
}

step_load_functions() {
    print_step "Loading ValKey Functions"

    if [ ! -f "$SCRIPT_DIR/scripts/load-valkey-functions.sh" ]; then
        print_error "load-valkey-functions.sh not found"
        exit 1
    fi

    print_info "Loading ~170 ValKey functions (23 libraries)..."

    if bash "$SCRIPT_DIR/scripts/load-valkey-functions.sh"; then
        print_success "ValKey functions loaded successfully"
    else
        print_error "ValKey function loading failed"
        exit 1
    fi
}

step_install_service() {
    print_step "Installing Systemd Service"

    if [ "$INSTALL_AS_SERVICE" = false ]; then
        print_warning "Skipping systemd service installation (--no-service flag)"
        return
    fi

    if ! check_command systemctl; then
        print_warning "Systemd not available, skipping service installation"
        INSTALL_AS_SERVICE=false
        return
    fi

    # Ask for confirmation
    echo -e "${BOLD}Install gNode as a systemd service?${NC}"
    echo "This will:"
    echo "  • Install gnode-daemon.service to /etc/systemd/system/"
    echo "  • Enable auto-start on boot"
    echo "  • Auto-restart on failures"
    echo "  • Start after valkey-gnode.service"
    echo

    if ask_yes_no "Install systemd service?"; then
        if [ ! -f "$SCRIPT_DIR/scripts/install-gnode-service.sh" ]; then
            print_error "install-gnode-service.sh not found"
            exit 1
        fi

        print_info "Installing systemd service (requires sudo)..."

        if sudo bash "$SCRIPT_DIR/scripts/install-gnode-service.sh"; then
            print_success "Systemd service installed"
        else
            print_error "Systemd service installation failed"
            exit 1
        fi
    else
        print_info "Skipping systemd service installation"
        INSTALL_AS_SERVICE=false
    fi
}

step_start_daemon() {
    print_step "Starting gNode Daemon"

    if [ "$INSTALL_AS_SERVICE" = true ]; then
        # Start via systemd
        print_info "Starting daemon via systemd..."

        if sudo systemctl start gnode-daemon; then
            sleep 2
            if sudo systemctl is-active --quiet gnode-daemon; then
                print_success "Daemon started via systemd"
            else
                print_error "Daemon failed to start"
                sudo systemctl status gnode-daemon --no-pager
                exit 1
            fi
        else
            print_error "Failed to start daemon via systemd"
            exit 1
        fi
    else
        # Start manually
        print_info "Starting daemon manually..."

        if [ ! -f "$SCRIPT_DIR/scripts/start-gnode.sh" ]; then
            print_error "start-gnode.sh not found"
            exit 1
        fi

        # Start in background
        if bash "$SCRIPT_DIR/scripts/start-gnode.sh" &> /tmp/gnode-start.log &
        then
            sleep 3

            # Check if process is running
            if pgrep -f "gnode-daemon" > /dev/null; then
                print_success "Daemon started manually"
            else
                print_error "Daemon failed to start"
                cat /tmp/gnode-start.log
                exit 1
            fi
        else
            print_error "Failed to start daemon"
            exit 1
        fi
    fi
}

step_verify() {
    print_step "Verifying Installation"

    if [ "$SKIP_TESTS" = true ]; then
        print_warning "Skipping verification tests (--skip-tests flag)"
        return
    fi

    # Runstatus check
    if [ -f "$SCRIPT_DIR/scripts/check-gnode-status.sh" ]; then
        print_info "Runningstatus check..."
        echo
        bash "$SCRIPT_DIR/scripts/check-gnode-status.sh"
    else
        print_warning "Status check script not found, skipping"
    fi
}

show_completion() {
    print_header "Installation Complete! 🎉"

    echo -e "${GREEN}${BOLD}gNode is now installed and running!${NC}"
    echo

    if [ "$INSTALL_AS_SERVICE" = true ]; then
        echo -e "${BOLD}Systemd Service Commands:${NC}"
        echo "  sudo systemctl status gnode-daemon       # Check status"
        echo "  sudo systemctl stop gnode-daemon         # Stop daemon"
        echo "  sudo systemctl start gnode-daemon        # Start daemon"
        echo "  sudo systemctl restart gnode-daemon      # Restart daemon"
        echo "  sudo journalctl -u gnode-daemon -f       # Follow logs"
        echo
    else
        echo -e "${BOLD}Manual Control Commands:${NC}"
        echo "  ./scripts/start-gnode.sh                 # Start daemon"
        echo "  ./scripts/stop-gnode.sh                  # Stop daemon"
        echo "  ./scripts/reload-gnode.sh                # Reload (rebuild + restart)"
        echo
    fi

    echo -e "${BOLD}Status & Monitoring:${NC}"
    echo "  ./scripts/check-gnode-status.sh          #status check"
    echo

    echo -e "${BOLD}Development:${NC}"
    echo "  cd daemon && cargo build --release     # Rebuild daemon"
    echo "  ./scripts/load-valkey-functions.sh     # Reload ValKey functions"
    echo "  ./scripts/test-all-valkey-functions.sh # Test all ~170 functions"
    echo "  ./scripts/flush-valkey.sh --force      # Clear all data (dev only!)"
    echo

    echo -e "${BOLD}Configuration Files:${NC}"
    echo "  .gnode/valkey.password                   # ValKey authentication"
    echo "  .env                                   # Environment variables"
    echo "  daemon/config/gnode-daemon.service       # Systemd service file"
    echo

    echo -e "${BOLD}Documentation:${NC}"
    echo "  CLAUDE.md                              # Architecture reference"
    echo "  GNODE_COMMANDS.md                        # Command catalog"
    echo "  VALKEY_FUNCTIONS.md                    # Function reference"
    echo "  docs/SCRIPT_AUDIT.md                   # Script inventory"
    echo

    echo -e "${BOLD}Next Steps:${NC}"
    echo "  1. Review the status output above"
    echo "  2. Check logs: sudo journalctl -u gnode-daemon -f"
    echo "  3. Test a command: scripts/check-gnode-status.sh"
    echo "  4. See CLIENT_HANDOUT.md for integration guide"
    echo

    print_success "Setup completed successfully!"
}

#=============================================================================
# Main Execution
#=============================================================================

main() {
    # Parse command line arguments
    parse_arguments "$@"

    # Show welcome screen
    show_welcome

    # Run setup steps
    step_prerequisites
    step_apache_optimize
    step_valkey_setup
    step_build_daemon
    step_load_functions
    step_install_service
    step_start_daemon
    step_verify

    # Show completion message
    show_completion
}

# Run main function
main "$@"
