#!/bin/bash
# gNode Setup System - Common Library
# Shared functions for all setup modules

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Logging levels
LOG_ERROR=0
LOG_WARN=1
LOG_INFO=2
LOG_SUCCESS=3
LOG_DEBUG=4

# Global state
VERBOSE=${VERBOSE:-0}
DRY_RUN=${DRY_RUN:-0}
# Use environment variables from .env (loaded by caller) with fallbacks
STATE_FILE="${STATE_FILE:-${GNODE_DIR:-/opt/gNode}/.setup-state.json}"
LOG_FILE="${LOG_FILE:-${GNODE_LOGS_DIR:-${GNODE_DIR:-/opt/gNode}/logs}/setup.log}"

# Ensure log directory exists. Tolerate a non-writable location (e.g. a
# read-only/owned-by-another-user path when a status command runs as a
# non-root operator) — logging must never abort a setup run under set -e.
mkdir -p "$(dirname "$LOG_FILE")" 2>/dev/null || true

#######################################
# Logging Functions
#######################################

log() {
    local level=$1
    shift
    local message="$*"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')

    # Group-redirect so a failed open of $LOG_FILE (e.g. status run by a
    # non-root operator against a gnode-owned log) is fully silent — a
    # trailing 2>/dev/null does NOT catch the redirection-open error.
    { echo "[$timestamp] $message" >> "$LOG_FILE"; } 2>/dev/null || true

    case $level in
        $LOG_ERROR)
            echo -e "${RED}✗${NC} $message" >&2
            ;;
        $LOG_WARN)
            echo -e "${YELLOW}⚠${NC} $message"
            ;;
        $LOG_INFO)
            echo -e "${BLUE}ℹ${NC} $message"
            ;;
        $LOG_SUCCESS)
            echo -e "${GREEN}✓${NC} $message"
            ;;
        $LOG_DEBUG)
            [[ $VERBOSE -eq 1 ]] && echo -e "${CYAN}→${NC} $message"
            ;;
    esac
}

error() { log $LOG_ERROR "$@"; }
warn() { log $LOG_WARN "$@"; }
info() { log $LOG_INFO "$@"; }
success() { log $LOG_SUCCESS "$@"; }
debug() { log $LOG_DEBUG "$@"; }

#######################################
# State Management
#######################################

state_init() {
    if [[ ! -f "$STATE_FILE" ]]; then
        echo '{"installed": {}, "timestamp": "'$(date -Iseconds)'", "version": "1.0"}' > "$STATE_FILE"
        debug "Initialized state file: $STATE_FILE"
    fi
}

state_get() {
    local key=$1
    local default=${2:-null}

    if [[ ! -f "$STATE_FILE" ]]; then
        echo "$default"
        return
    fi

    python3 -c "import json, sys; data=json.load(open('$STATE_FILE')); print(data.get('installed', {}).get('$key', $default))" 2>/dev/null || echo "$default"
}

state_set() {
    local key=$1
    local value=$2

    state_init

    # Convert bash true/false to Python True/False, otherwise quote as string
    local py_value
    if [[ "$value" == "true" ]]; then
        py_value="True"
    elif [[ "$value" == "false" ]]; then
        py_value="False"
    elif [[ "$value" =~ ^[0-9]+$ ]]; then
        py_value="$value"
    elif [[ "$value" =~ ^\[.*\]$ ]] || [[ "$value" =~ ^\{.*\}$ ]]; then
        # JSON array or object - pass as-is
        py_value="$value"
    else
        py_value="\"$value\""
    fi

    python3 -c "
import json
data = json.load(open('$STATE_FILE'))
if 'installed' not in data:
    data['installed'] = {}
data['installed']['$key'] = $py_value
data['timestamp'] = '$(date -Iseconds)'
json.dump(data, open('$STATE_FILE', 'w'), indent=2)
"
    debug "State updated: $key = $value"
}

state_remove() {
    local key=$1

    if [[ ! -f "$STATE_FILE" ]]; then
        return
    fi

    python3 -c "
import json
data = json.load(open('$STATE_FILE'))
if 'installed' in data and '$key' in data['installed']:
    del data['installed']['$key']
    data['timestamp'] = '$(date -Iseconds)'
    json.dump(data, open('$STATE_FILE', 'w'), indent=2)
"
    debug "State removed: $key"
}

#######################################
# Requirement Checking
#######################################

require_root() {
    if [[ $EUID -ne 0 ]]; then
        error "This operation requires root privileges. Please run with sudo."
        return 1
    fi
}

require_user() {
    local user=$1
    if [[ $(whoami) != "$user" ]]; then
        error "This operation must be run as user: $user"
        return 1
    fi
}

require_command() {
    local cmd=$1
    local install_hint=${2:-""}

    if ! command -v "$cmd" &> /dev/null; then
        error "Required command not found: $cmd"
        [[ -n "$install_hint" ]] && info "Install with: $install_hint"
        return 1
    fi

    debug "Required command found: $cmd"
    return 0
}

require_file() {
    local file=$1
    local hint=${2:-""}

    if [[ ! -f "$file" ]]; then
        error "Required file not found: $file"
        [[ -n "$hint" ]] && info "$hint"
        return 1
    fi

    debug "Required file found: $file"
    return 0
}

require_directory() {
    local dir=$1
    local hint=${2:-""}

    if [[ ! -d "$dir" ]]; then
        error "Required directory not found: $dir"
        [[ -n "$hint" ]] && info "$hint"
        return 1
    fi

    debug "Required directory found: $dir"
    return 0
}

#######################################
# User Interaction
#######################################

ask_yes_no() {
    local prompt=$1
    local default=${2:-"n"}

    if [[ $default == "y" ]]; then
        prompt="$prompt [Y/n]: "
    else
        prompt="$prompt [y/N]: "
    fi

    read -p "$prompt" response
    response=${response:-$default}

    [[ "$response" =~ ^[Yy] ]]
}

ask_choice() {
    local prompt=$1
    shift
    local options=("$@")

    echo "$prompt"
    for i in "${!options[@]}"; do
        echo "  $((i+1)). ${options[$i]}"
    done

    local choice
    while true; do
        read -p "Enter choice [1-${#options[@]}]: " choice
        if [[ "$choice" =~ ^[0-9]+$ ]] && [[ $choice -ge 1 ]] && [[ $choice -le ${#options[@]} ]]; then
            echo "${options[$((choice-1))]}"
            return 0
        fi
        error "Invalid choice. Please enter a number between 1 and ${#options[@]}"
    done
}

ask_multiselect() {
    local prompt=$1
    shift
    local options=("$@")

    echo "$prompt"
    echo "  0. All"
    for i in "${!options[@]}"; do
        echo "  $((i+1)). ${options[$i]}"
    done

    local selection
    read -p "Enter choices (comma-separated, or 0 for all): " selection

    if [[ "$selection" == "0" ]]; then
        printf '%s\n' "${options[@]}"
        return 0
    fi

    IFS=',' read -ra choices <<< "$selection"
    for choice in "${choices[@]}"; do
        choice=$(echo "$choice" | xargs) # trim whitespace
        if [[ "$choice" =~ ^[0-9]+$ ]] && [[ $choice -ge 1 ]] && [[ $choice -le ${#options[@]} ]]; then
            echo "${options[$((choice-1))]}"
        fi
    done
}

#######################################
# Service Management
#######################################

systemd_is_running() {
    local service=$1
    systemctl is-active --quiet "$service"
}

systemd_is_enabled() {
    local service=$1
    systemctl is-enabled --quiet "$service"
}

systemd_restart() {
    local service=$1

    if [[ $DRY_RUN -eq 1 ]]; then
        info "[DRY RUN] Would restart service: $service"
        return 0
    fi

    info "Restarting service: $service"
    systemctl restart "$service" || {
        error "Failed to restart $service"
        return 1
    }

    sleep 2

    if systemd_is_running "$service"; then
        success "Service $service is running"
        return 0
    else
        error "Service $service failed to start"
        return 1
    fi
}

#######################################
# File Operations
#######################################

backup_file() {
    local file=$1
    local backup="${file}.backup.$(date +%Y%m%d_%H%M%S)"

    if [[ ! -f "$file" ]]; then
        debug "No file to backup: $file"
        return 0
    fi

    if [[ $DRY_RUN -eq 1 ]]; then
        info "[DRY RUN] Would backup: $file -> $backup"
        return 0
    fi

    cp -a "$file" "$backup"
    success "Backed up: $file -> $backup"
}

safe_symlink() {
    local target=$1
    local link=$2

    if [[ ! -e "$target" ]]; then
        error "Symlink target does not exist: $target"
        return 1
    fi

    if [[ -L "$link" ]]; then
        local current_target=$(readlink "$link")
        if [[ "$current_target" == "$target" ]]; then
            debug "Symlink already correct: $link -> $target"
            return 0
        else
            warn "Updating symlink: $link ($current_target -> $target)"
            [[ $DRY_RUN -eq 0 ]] && rm "$link"
        fi
    elif [[ -e "$link" ]]; then
        error "Path exists but is not a symlink: $link"
        return 1
    fi

    if [[ $DRY_RUN -eq 1 ]]; then
        info "[DRY RUN] Would create symlink: $link -> $target"
        return 0
    fi

    ln -sf "$target" "$link"
    success "Created symlink: $link -> $target"
}

ensure_directory() {
    local dir=$1
    local owner=${2:-""}
    local perms=${3:-"755"}

    if [[ -d "$dir" ]]; then
        debug "Directory exists: $dir"
        return 0
    fi

    if [[ $DRY_RUN -eq 1 ]]; then
        info "[DRY RUN] Would create directory: $dir"
        return 0
    fi

    mkdir -p "$dir"
    [[ -n "$owner" ]] && chown "$owner" "$dir"
    chmod "$perms" "$dir"

    success "Created directory: $dir"
}

#######################################
# Network & Connectivity
#######################################

check_port() {
    local host=$1
    local port=$2
    local timeout=${3:-2}

    timeout "$timeout" bash -c "cat < /dev/null > /dev/tcp/$host/$port" 2>/dev/null
}

wait_for_port() {
    local host=$1
    local port=$2
    local max_attempts=${3:-30}
    local sleep_time=${4:-1}

    info "Waiting for $host:$port to be available..."

    for ((i=1; i<=max_attempts; i++)); do
        if check_port "$host" "$port" 2; then
            success "$host:$port is available"
            return 0
        fi
        debug "Attempt $i/$max_attempts: $host:$port not ready"
        sleep "$sleep_time"
    done

    error "Timeout waiting for $host:$port"
    return 1
}

#######################################
# Validation
#######################################

validate_path() {
    local path=$1
    # Remove any potentially dangerous characters
    echo "$path" | sed 's/[;&|`$()]//g'
}

validate_identifier() {
    local id=$1
    # Only allow alphanumeric, dash, underscore, dot
    if [[ ! "$id" =~ ^[a-zA-Z0-9._-]+$ ]]; then
        error "Invalid identifier: $id (only alphanumeric, dash, underscore, dot allowed)"
        return 1
    fi
    echo "$id"
}

#######################################
# Progress Tracking
#######################################

progress_start() {
    local total=$1
    echo "0:$total" > /tmp/gnode-setup-progress.$$
}

progress_update() {
    local current=$1
    if [[ -f /tmp/gnode-setup-progress.$$ ]]; then
        local total=$(cut -d: -f2 /tmp/gnode-setup-progress.$$)
        local percent=$((current * 100 / total))
        echo -ne "\rProgress: [$current/$total] ${percent}%   "
    fi
}

progress_end() {
    [[ -f /tmp/gnode-setup-progress.$$ ]] && rm -f /tmp/gnode-setup-progress.$$
    echo ""
}

#######################################
# Cleanup on Exit
#######################################

cleanup() {
    progress_end
    debug "Cleanup completed"
}

trap cleanup EXIT

# Initialize state on load
state_init
