#!/bin/bash
#
# gNode Prerequisite Checker and Installer
#
# Checks for and installs required build dependencies:
#   - Rust/Cargo (via rustup, with 5-method detection for sudo-aware setups)
#   - build-essential, pkg-config, libssl-dev, git
#
# Usage:
#   ./scripts/init-gnode.sh [--check-only]
#
# Options:
#   --check-only    Report status without installing anything
#
# Exit codes:
#   0  All prerequisites satisfied
#   1  Missing prerequisites (with --check-only) or install failure
#

set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info()    { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[OK]${NC} $1"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error()   { echo -e "${RED}[ERROR]${NC} $1" >&2; }

CHECK_ONLY=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --check-only) CHECK_ONLY=true; shift ;;
        *) log_error "Unknown option: $1"; exit 1 ;;
    esac
done

# =============================================================================
# Rust Environment Detection (5 fallback methods, sudo-aware)
# =============================================================================

setup_rust_environment() {
    # If cargo is already available, we're done
    if command -v cargo &>/dev/null; then
        return 0
    fi

    log_info "Cargo not in PATH, searching for Rust installation..."

    # Detect if running under sudo and get the real user's home directory
    local REAL_HOME="$HOME"
    if [[ -n "${SUDO_USER:-}" && "$SUDO_USER" != "root" ]]; then
        REAL_HOME=$(eval echo "~$SUDO_USER")
        log_info "Running as sudo, checking $SUDO_USER's home: $REAL_HOME"
    fi

    # Attempt 1: Source ~/.cargo/env
    if [[ -f "$REAL_HOME/.cargo/env" ]]; then
        log_info "Attempt 1: Sourcing $REAL_HOME/.cargo/env"
        set +e
        # shellcheck disable=SC1091
        source "$REAL_HOME/.cargo/env" 2>/dev/null
        set -e
        if command -v cargo &>/dev/null; then
            log_success "Rust found via ~/.cargo/env"
            return 0
        fi
    fi

    # Attempt 2: Direct PATH to cargo binary
    if [[ -f "$REAL_HOME/.cargo/bin/cargo" ]]; then
        log_info "Attempt 2: Adding $REAL_HOME/.cargo/bin to PATH"
        export PATH="$REAL_HOME/.cargo/bin:$PATH"
        if command -v cargo &>/dev/null; then
            log_success "Rust found in ~/.cargo/bin"
            return 0
        fi
    fi

    # Attempt 3: Check rustup availability
    if [[ -f "$REAL_HOME/.cargo/bin/rustup" ]]; then
        log_info "Attempt 3: Using rustup to configure environment"
        export PATH="$REAL_HOME/.cargo/bin:$PATH"
        if command -v cargo &>/dev/null; then
            log_success "Rust found via rustup"
            return 0
        fi
    fi

    # Attempt 4: Source ~/.profile which might load cargo
    if [[ -f "$REAL_HOME/.profile" ]]; then
        log_info "Attempt 4: Sourcing $REAL_HOME/.profile"
        set +e
        # shellcheck disable=SC1091
        source "$REAL_HOME/.profile" 2>/dev/null
        set -e
        if command -v cargo &>/dev/null; then
            log_success "Rust found via ~/.profile"
            return 0
        fi
    fi

    # Attempt 5: System-wide installation
    if [[ -f "/usr/local/bin/cargo" || -f "/usr/bin/cargo" ]]; then
        log_info "Attempt 5: Checking system-wide installation"
        if command -v cargo &>/dev/null; then
            log_success "Rust found in system path"
            return 0
        fi
    fi

    # Diagnostics on failure
    log_warn "Rust/Cargo not found after 5 detection attempts"
    if [[ -n "${SUDO_USER:-}" ]]; then
        log_info "  SUDO_USER=$SUDO_USER, REAL_HOME=$REAL_HOME"
    fi
    return 1
}

# =============================================================================
# Package Checks
# =============================================================================

MISSING_PACKAGES=()

check_package() {
    local pkg="$1"
    if dpkg -s "$pkg" &>/dev/null; then
        log_success "$pkg installed"
    else
        log_warn "$pkg NOT installed"
        MISSING_PACKAGES+=("$pkg")
    fi
}

check_command() {
    local cmd="$1"
    local label="${2:-$cmd}"
    if command -v "$cmd" &>/dev/null; then
        log_success "$label found: $(command -v "$cmd")"
        return 0
    else
        log_warn "$label not found"
        return 1
    fi
}

# =============================================================================
# Main
# =============================================================================

main() {
    echo ""
    log_info "gNode Prerequisite Check"
    echo ""

    local needs_install=false

    # -- Rust/Cargo --
    log_info "Checking Rust toolchain..."
    if setup_rust_environment; then
        log_success "cargo $(cargo --version 2>/dev/null | awk '{print $2}')"
        log_success "rustc $(rustc --version 2>/dev/null | awk '{print $2}')"
    else
        needs_install=true
    fi
    echo ""

    # -- System packages --
    log_info "Checking system packages..."
    check_package "build-essential"
    check_package "pkg-config"
    check_package "libssl-dev"
    check_command "git" "git" || MISSING_PACKAGES+=("git")
    echo ""

    # -- Check-only mode: report and exit --
    if $CHECK_ONLY; then
        if [[ ${#MISSING_PACKAGES[@]} -gt 0 ]] || $needs_install; then
            log_warn "Missing: ${MISSING_PACKAGES[*]:-} ${needs_install:+rust/cargo}"
            exit 1
        else
            log_success "All prerequisites satisfied"
            exit 0
        fi
    fi

    # -- Install missing system packages --
    if [[ ${#MISSING_PACKAGES[@]} -gt 0 ]]; then
        log_info "Installing missing packages: ${MISSING_PACKAGES[*]}"

        if [[ $EUID -ne 0 ]]; then
            log_error "Root privileges required to install packages. Re-run with sudo."
            exit 1
        fi

        apt-get update -qq
        apt-get install --yes "${MISSING_PACKAGES[@]}"
        log_success "System packages installed"
    fi

    # -- Install Rust if needed --
    if $needs_install; then
        log_info "Installing Rust via rustup..."

        if [[ $EUID -eq 0 && -n "${SUDO_USER:-}" ]]; then
            # Running under sudo — install as the real user
            log_info "Installing Rust for user $SUDO_USER"
            sudo -u "$SUDO_USER" bash -c 'curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y'
            local REAL_HOME
            REAL_HOME=$(eval echo "~$SUDO_USER")
            export PATH="$REAL_HOME/.cargo/bin:$PATH"
        else
            curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
            # shellcheck disable=SC1091
            source "$HOME/.cargo/env" 2>/dev/null || export PATH="$HOME/.cargo/bin:$PATH"
        fi

        if command -v cargo &>/dev/null; then
            log_success "Rust installed: $(cargo --version)"
        else
            log_error "Rust installation failed"
            exit 1
        fi
    fi

    echo ""
    log_success "All prerequisites satisfied"
}

main
