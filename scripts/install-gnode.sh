#!/bin/bash
#
# gNode Component Installer
#
# Self-contained installer for the gNode daemon. Handles prerequisites,
# ValKey, daemon build, Lua functions, systemd service, and startup.
#
# All configuration is sourced from /etc/geodineum/bootstrap.env (centralized
# config store). Falls back to config/bootstrap.env template on fresh installs.
#
# Can run standalone or be called from setup-geodineum.sh orchestrator.
#
# Usage:
#   sudo ./scripts/install-gnode.sh [OPTIONS]
#
# Options:
#   --yes, -y           Non-interactive mode
#   --dry-run, -n       Preview changes without executing
#   --skip-build        Skip daemon compilation
#   --skip-valkey       Skip ValKey setup (use existing)
#   --skip-service      Don't install systemd service
#   --skip-start        Install but don't start daemon
#   --help, -h          Show this help message
#

set -euo pipefail

# =============================================================================
# Configuration — sourced from centralized config store
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Flags: orchestrator sets GNODE_INSTALL_* env vars; CLI args override
YES="${GNODE_INSTALL_YES:-false}"
DRY_RUN="${GNODE_INSTALL_DRY_RUN:-false}"
SKIP_BUILD="${GNODE_INSTALL_SKIP_BUILD:-false}"
SKIP_VALKEY="${GNODE_INSTALL_SKIP_VALKEY:-false}"
SKIP_SERVICE="${GNODE_INSTALL_SKIP_SERVICE:-false}"
SKIP_START="${GNODE_INSTALL_SKIP_START:-false}"

# Canonical ecosystem config loader (installed by Geodineum installer).
GEODINEUM_LIB="${GEODINEUM_LIB:-/usr/local/lib/geodineum}"
if [[ ! -r "$GEODINEUM_LIB/bootstrap-loader.sh" ]]; then
    echo "FATAL: $GEODINEUM_LIB/bootstrap-loader.sh not found." >&2
    echo "       Run 'sudo ./install.sh' (from Geodineum repo) first." >&2
    exit 1
fi
# shellcheck source=/usr/local/lib/geodineum/bootstrap-loader.sh
source "$GEODINEUM_LIB/bootstrap-loader.sh"
load_ecosystem_config

# =============================================================================
# Helpers
# =============================================================================

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; BOLD='\033[1m'; NC='\033[0m'

log_info()    { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}  [OK]${NC} $1"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error()   { echo -e "${RED}[ERROR]${NC} $1" >&2; }

step_header() {
    echo ""
    echo -e "${BOLD}  [$1/7] $2${NC}"
}

ask_continue() {
    if [[ "$YES" == "true" ]]; then return 0; fi
    local prompt="${1:-Continue?}"
    read -r -p "  $prompt [Y/n] " reply
    [[ -z "$reply" || "$reply" =~ ^[Yy] ]]
}

# =============================================================================
# Argument Parsing
# =============================================================================

while [[ $# -gt 0 ]]; do
    case $1 in
        --yes|-y)        YES=true; shift ;;
        --dry-run|-n)    DRY_RUN=true; shift ;;
        --skip-build)    SKIP_BUILD=true; shift ;;
        --skip-valkey)   SKIP_VALKEY=true; shift ;;
        --skip-service)  SKIP_SERVICE=true; shift ;;
        --skip-start)    SKIP_START=true; shift ;;
        --help|-h)       head -25 "$0" | tail -21; exit 0 ;;
        *)               log_error "Unknown option: $1"; exit 1 ;;
    esac
done

# =============================================================================
# Installation Steps
# =============================================================================

step_prerequisites() {
    step_header 1 "Prerequisites"

    if [[ ! -f "$SCRIPT_DIR/init-gnode.sh" ]]; then
        log_error "scripts/init-gnode.sh not found"
        return 1
    fi

    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "[DRY-RUN] Would run: scripts/init-gnode.sh"
        return 0
    fi

    bash "$SCRIPT_DIR/init-gnode.sh"
}

step_system_user() {
    step_header 2 "System User"

    if id "gnode" &>/dev/null; then
        log_success "User 'gnode' already exists"
        return 0
    fi

    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "[DRY-RUN] Would create gnode user/group"
        return 0
    fi

    if [[ $EUID -ne 0 ]]; then
        log_error "Root required to create system user"
        return 1
    fi

    bash "$SCRIPT_DIR/install-gnode-service.sh" --user-only
}

step_valkey() {
    step_header 3 "ValKey"

    if [[ "$SKIP_VALKEY" == "true" ]]; then
        log_info "Skipped (--skip-valkey)"
        return 0
    fi

    # Check if ValKey is already running on the configured port
    if pgrep -x "valkey-server" &>/dev/null; then
        local running_port
        running_port=$(ps aux | grep "[v]alkey-server" | grep -oP '\*:\K[0-9]+' | head -1 || true)
        if [[ "$running_port" == "$VALKEY_PORT" ]]; then
            log_success "ValKey already running on port $VALKEY_PORT"
            return 0
        fi
    fi

    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "[DRY-RUN] Would run: scripts/setup-valkey-smart.sh"
        return 0
    fi

    bash "$SCRIPT_DIR/setup-valkey-smart.sh"
}

step_build() {
    step_header 4 "Build Daemon"

    if [[ "$SKIP_BUILD" == "true" ]]; then
        log_info "Skipped (--skip-build)"
        return 0
    fi

    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "[DRY-RUN] Would run: scripts/build.sh"
        return 0
    fi

    bash "$SCRIPT_DIR/build.sh"
}

step_lua_functions() {
    step_header 5 "Lua Functions"

    if [[ ! -f "$SCRIPT_DIR/load-valkey-functions.sh" ]]; then
        log_error "scripts/load-valkey-functions.sh not found"
        return 1
    fi

    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "[DRY-RUN] Would run: scripts/load-valkey-functions.sh"
        return 0
    fi

    bash "$SCRIPT_DIR/load-valkey-functions.sh"
}

step_systemd_service() {
    step_header 6 "Systemd Service"

    if [[ "$SKIP_SERVICE" == "true" ]]; then
        log_info "Skipped (--skip-service)"
        return 0
    fi

    if systemctl list-unit-files 2>/dev/null | grep -q "gnode-daemon.service"; then
        log_success "gnode-daemon.service already installed"
        return 0
    fi

    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "[DRY-RUN] Would run: scripts/install-gnode-service.sh"
        return 0
    fi

    if [[ $EUID -ne 0 ]]; then
        log_error "Root required to install systemd service"
        return 1
    fi

    if [[ "$YES" == "true" ]]; then
        yes y 2>/dev/null | bash "$SCRIPT_DIR/install-gnode-service.sh" || true
    else
        bash "$SCRIPT_DIR/install-gnode-service.sh"
    fi
}

step_start_verify() {
    step_header 7 "Start & Verify"

    if [[ "$SKIP_START" == "true" || "$SKIP_SERVICE" == "true" ]]; then
        log_info "Skipped"
        return 0
    fi

    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "[DRY-RUN] Would start gnode-daemon and run health check"
        return 0
    fi

    if systemctl is-active --quiet gnode-daemon 2>/dev/null; then
        log_success "gnode-daemon already running"
    else
        log_info "Starting gnode-daemon..."
        systemctl start gnode-daemon 2>/dev/null || true
        sleep 2
    fi

    if [[ -f "$SCRIPT_DIR/check-gnode-status.sh" ]]; then
        bash "$SCRIPT_DIR/check-gnode-status.sh" || true
    fi
}

# =============================================================================
# Main
# =============================================================================

main() {
    echo ""
    echo -e "${BOLD}  gNode Component Installer${NC}"
    echo ""

    if [[ "$DRY_RUN" == "true" ]]; then
        echo -e "  ${YELLOW}DRY-RUN MODE${NC}"
        echo ""
    fi

    # Root check (non-dry-run)
    if [[ "$DRY_RUN" != "true" && $EUID -ne 0 ]]; then
        log_error "This installer must be run as root (use sudo)"
        log_info "  For preview: $0 --dry-run"
        exit 1
    fi

    echo ""
    log_info "Config: VALKEY_HOST=$VALKEY_HOST VALKEY_PORT=$VALKEY_PORT"
    echo ""

    step_prerequisites     || { log_error "Prerequisites failed"; exit 1; }
    step_system_user       || { log_error "User setup failed"; exit 1; }
    step_valkey            || { log_error "ValKey setup failed"; exit 1; }
    step_build             || { log_error "Build failed"; exit 1; }
    step_lua_functions     || { log_error "Lua loading failed"; exit 1; }
    step_systemd_service   || { log_error "Service install failed"; exit 1; }
    step_start_verify      || log_warn "Start/verify had warnings"

    echo ""
    log_success "gNode component installation complete"
    echo ""
}

main
