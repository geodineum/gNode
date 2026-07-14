#!/bin/bash
#
# gNode Network Hardening Module
# Phase 20: Security hardening for distributed gNode deployment
#
# Creates a "dead zone" trap around the ValKey port:
#   - Non-standard port (47445) for ValKey - IANA unassigned, safe for gNode
#   - Trap zone (47000-48000 except real port) logs and drops
#   - fail2ban watches logs → immediate 24h ban on probe
#   - Only whitelisted IPs can access the real port
#
# Run as: sudo ./scripts/setup/modules/20-network-hardening.sh [ALLOWED_IP...]
#
# This script is idempotent - safe to run multiple times.
# Each phase checks if already completed before running.
#

set -euo pipefail

# Script location
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LIB_DIR="$(dirname "$SCRIPT_DIR")/lib"

# Source common library
if [[ -f "$LIB_DIR/common.sh" ]]; then
    source "$LIB_DIR/common.sh"
else
    echo "ERROR: common.sh not found at $LIB_DIR/common.sh"
    exit 1
fi

# ============================================================================
# Configuration
# ============================================================================

# gNode standard port: 47445 (IANA unassigned range 47101-47556)
VALKEY_PORT="${VALKEY_PORT:-47445}"
# Trap zone: tight band around gNode port (47000-48000)
# Any connection attempt to ports in this range (except 47445) triggers instant ban
TRAP_ZONE_START="${TRAP_ZONE_START:-47000}"
TRAP_ZONE_END="${TRAP_ZONE_END:-48000}"
BAN_TIME="${BAN_TIME:-86400}"  # 24 hours
LOG_PREFIX="GNODE-TRAP"

VALKEY_CONF="/etc/valkey/valkey-gnode.conf"
FAIL2BAN_FILTER="/etc/fail2ban/filter.d/gnode-trap.conf"
FAIL2BAN_JAIL="/etc/fail2ban/jail.d/gnode-trap.conf"
UFW_BEFORE_RULES="/etc/ufw/before.rules"

# State keys for tracking
STATE_KEY_VALKEY_PORT="network_hardening.valkey_port"
STATE_KEY_FAIL2BAN_FILTER="network_hardening.fail2ban_filter"
STATE_KEY_FAIL2BAN_JAIL="network_hardening.fail2ban_jail"
STATE_KEY_UFW_TRAP="network_hardening.ufw_trap"
STATE_KEY_ALLOWED_IPS="network_hardening.allowed_ips"

# ============================================================================
# Helper Functions
# ============================================================================

show_usage() {
    cat << EOF
gNode Network Hardening Module (Phase 20)

Usage: $0 [OPTIONS] [ALLOWED_IP...]

Options:
    -h, --help          Show this help message
    -p, --port PORT     ValKey port (default: $VALKEY_PORT)
    -b, --ban-time SEC  Ban duration in seconds (default: $BAN_TIME)
    -n, --dry-run       Show what would be done without making changes
    -v, --verbose       Enable verbose output
    --reset             Remove all hardening and restore defaults

Arguments:
    ALLOWED_IP          IP addresses allowed to access ValKey (required for first run)

Examples:
    # First-time setup with inference node
    sudo $0 10.0.0.50

    # Add multiple nodes
    sudo $0 10.0.0.50 10.0.0.51 10.0.0.52

    # Check status (no IPs = status check only)
    sudo $0

    # Reset to defaults
    sudo $0 --reset

EOF
}

get_server_ip() {
    hostname -I | awk '{print $1}'
}

# Check if ValKey is configured with hardened port
check_valkey_port_configured() {
    if [[ ! -f "$VALKEY_CONF" ]]; then
        return 1
    fi

    local current_port
    current_port=$(grep "^port " "$VALKEY_CONF" 2>/dev/null | awk '{print $2}' || echo "6379")

    [[ "$current_port" == "$VALKEY_PORT" ]]
}

# Check if ValKey is bound to public IP
check_valkey_bind_configured() {
    if [[ ! -f "$VALKEY_CONF" ]]; then
        return 1
    fi

    local server_ip
    server_ip=$(get_server_ip)

    grep "^bind " "$VALKEY_CONF" 2>/dev/null | grep -q "$server_ip"
}

# Check if fail2ban filter exists
check_fail2ban_filter_exists() {
    [[ -f "$FAIL2BAN_FILTER" ]] && grep -q "GNODE-TRAP\|gnode-trap" "$FAIL2BAN_FILTER" 2>/dev/null
}

# Check if fail2ban jail exists and is enabled
check_fail2ban_jail_exists() {
    [[ -f "$FAIL2BAN_JAIL" ]] && grep -q "enabled = true" "$FAIL2BAN_JAIL" 2>/dev/null
}

# Check if UFW trap rules exist
check_ufw_trap_rules_exist() {
    grep -q "$LOG_PREFIX" "$UFW_BEFORE_RULES" 2>/dev/null
}

# Check if IP is already allowed in UFW
check_ip_allowed() {
    local ip=$1
    ufw status | grep -q "$ip.*$VALKEY_PORT"
}

# ============================================================================
# Phase Functions
# ============================================================================

phase_1_check_dependencies() {
    info "Phase 1: Checking dependencies..."

    local missing=()

    # Check fail2ban
    if ! command -v fail2ban-client &>/dev/null; then
        missing+=("fail2ban")
    fi

    # Check UFW
    if ! command -v ufw &>/dev/null; then
        missing+=("ufw")
    fi

    # Check ValKey config exists
    if [[ ! -f "$VALKEY_CONF" ]]; then
        error "ValKey config not found: $VALKEY_CONF"
        error "Please install and configure ValKey first (run setup-valkey-smart.sh)"
        return 1
    fi

    if [[ ${#missing[@]} -gt 0 ]]; then
        error "Missing dependencies: ${missing[*]}"
        info "Install with: sudo apt-get install ${missing[*]}"
        return 1
    fi

    # Check fail2ban is running
    if ! systemctl is-active --quiet fail2ban; then
        warn "fail2ban is not running. Starting..."
        if [[ $DRY_RUN -eq 0 ]]; then
            systemctl start fail2ban
            systemctl enable fail2ban
        fi
    fi

    # Check UFW is active
    if ! ufw status | grep -q "Status: active"; then
        warn "UFW is not active"
        info "Enable with: sudo ufw enable"
        # Don't fail - UFW might be managed differently
    fi

    success "Phase 1: All dependencies satisfied"
    return 0
}

phase_2_configure_valkey_port() {
    info "Phase 2: Configuring ValKey port..."

    local server_ip
    server_ip=$(get_server_ip)

    # Check if already configured
    if check_valkey_port_configured && check_valkey_bind_configured; then
        success "Phase 2: ValKey already configured (port=$VALKEY_PORT, bind includes $server_ip)"
        state_set "$STATE_KEY_VALKEY_PORT" "true"
        return 0
    fi

    if [[ $DRY_RUN -eq 1 ]]; then
        info "[DRY RUN] Would configure ValKey: port=$VALKEY_PORT, bind=127.0.0.1 $server_ip"
        return 0
    fi

    # Backup config
    backup_file "$VALKEY_CONF"

    # Update port
    if grep -q "^port " "$VALKEY_CONF"; then
        sed -i "s/^port .*/port $VALKEY_PORT/" "$VALKEY_CONF"
    else
        echo "port $VALKEY_PORT" >> "$VALKEY_CONF"
    fi

    # Update bind address
    if grep -q "^bind " "$VALKEY_CONF"; then
        sed -i "s/^bind .*/bind 127.0.0.1 $server_ip/" "$VALKEY_CONF"
    else
        echo "bind 127.0.0.1 $server_ip" >> "$VALKEY_CONF"
    fi

    state_set "$STATE_KEY_VALKEY_PORT" "true"
    success "Phase 2: ValKey configured (port=$VALKEY_PORT, bind=127.0.0.1 $server_ip)"

    # Mark that ValKey needs restart
    VALKEY_NEEDS_RESTART=1
}

phase_3_create_fail2ban_filter() {
    info "Phase 3: Creating fail2ban filter..."

    # Check if already exists
    if check_fail2ban_filter_exists; then
        success "Phase 3: fail2ban filter already exists"
        state_set "$STATE_KEY_FAIL2BAN_FILTER" "true"
        return 0
    fi

    if [[ $DRY_RUN -eq 1 ]]; then
        info "[DRY RUN] Would create fail2ban filter: $FAIL2BAN_FILTER"
        return 0
    fi

    cat > "$FAIL2BAN_FILTER" << 'EOF'
# gNode Network Hardening - Port Trap Filter
# Catches anyone probing the dead zone around the real ValKey port
#
# This filter matches kernel log entries from iptables LOG rules
# that tag connection attempts to trap ports with "GNODE-TRAP"

[Definition]
failregex = ^.*GNODE-TRAP.*SRC=<HOST>.*$
            ^<HOST>.*GNODE-TRAP.*$
ignoreregex =

# DEV Notes:
# - The LOG rule in iptables adds "GNODE-TRAP" prefix
# - SRC=<HOST> captures the attacking IP
# - This triggers on ANY probe to trap ports (single attempt = ban)
EOF

    state_set "$STATE_KEY_FAIL2BAN_FILTER" "true"
    success "Phase 3: Created fail2ban filter"
    FAIL2BAN_NEEDS_RELOAD=1
}

phase_4_create_fail2ban_jail() {
    info "Phase 4: Creating fail2ban jail..."

    # Check if already exists
    if check_fail2ban_jail_exists; then
        success "Phase 4: fail2ban jail already exists and is enabled"
        state_set "$STATE_KEY_FAIL2BAN_JAIL" "true"
        return 0
    fi

    if [[ $DRY_RUN -eq 1 ]]; then
        info "[DRY RUN] Would create fail2ban jail: $FAIL2BAN_JAIL"
        return 0
    fi

    cat > "$FAIL2BAN_JAIL" << EOF
# gNode Network Hardening - Port Trap Jail
# Immediately bans IPs that probe the dead zone around ValKey port
#
# Security design:
#   - maxretry=1: Single probe = immediate ban (no second chances)
#   - bantime=$BAN_TIME: 24 hour ban by default
#   - action=iptables-allports: Ban on ALL ports, not just the trap

[gnode-trap]
enabled = true
filter = gnode-trap
logpath = /var/log/kern.log
          /var/log/syslog
maxretry = 1
findtime = 60
bantime = $BAN_TIME
action = iptables-allports[name=gnode-trap, protocol=all]
EOF

    state_set "$STATE_KEY_FAIL2BAN_JAIL" "true"
    success "Phase 4: Created fail2ban jail (ban time: ${BAN_TIME}s)"
    FAIL2BAN_NEEDS_RELOAD=1
}

phase_5_configure_ufw_trap_rules() {
    info "Phase 5: Configuring UFW trap rules..."

    # Check if already configured
    if check_ufw_trap_rules_exist; then
        success "Phase 5: UFW trap rules already exist"
        state_set "$STATE_KEY_UFW_TRAP" "true"
        return 0
    fi

    if [[ $DRY_RUN -eq 1 ]]; then
        info "[DRY RUN] Would add trap rules to $UFW_BEFORE_RULES"
        return 0
    fi

    # Backup before.rules
    backup_file "$UFW_BEFORE_RULES"

    # Create the trap rules to insert
    local trap_rules="
# ============================================================================
# gNode Network Hardening - Dead Zone Trap Rules
# Generated: $(date -Iseconds)
# Port: $VALKEY_PORT, Trap Zone: $TRAP_ZONE_START-$TRAP_ZONE_END
# ============================================================================
# Log and DROP all connections to trap zone (rate limited to prevent log flood)
-A ufw-before-input -p tcp --dport $TRAP_ZONE_START:$((VALKEY_PORT - 1)) -m limit --limit 5/min -j LOG --log-prefix \"$LOG_PREFIX \"
-A ufw-before-input -p tcp --dport $((VALKEY_PORT + 1)):$TRAP_ZONE_END -m limit --limit 5/min -j LOG --log-prefix \"$LOG_PREFIX \"
-A ufw-before-input -p tcp --dport $TRAP_ZONE_START:$((VALKEY_PORT - 1)) -j DROP
-A ufw-before-input -p tcp --dport $((VALKEY_PORT + 1)):$TRAP_ZONE_END -j DROP
# ============================================================================"

    # Insert before the final COMMIT in the filter section
    # Find the line number of COMMIT and insert before it
    local commit_line
    commit_line=$(grep -n "^COMMIT" "$UFW_BEFORE_RULES" | tail -1 | cut -d: -f1)

    if [[ -n "$commit_line" ]]; then
        # Insert trap rules before COMMIT
        head -n $((commit_line - 1)) "$UFW_BEFORE_RULES" > "${UFW_BEFORE_RULES}.new"
        echo "$trap_rules" >> "${UFW_BEFORE_RULES}.new"
        tail -n +"$commit_line" "$UFW_BEFORE_RULES" >> "${UFW_BEFORE_RULES}.new"
        mv "${UFW_BEFORE_RULES}.new" "$UFW_BEFORE_RULES"
    else
        error "Could not find COMMIT line in $UFW_BEFORE_RULES"
        return 1
    fi

    state_set "$STATE_KEY_UFW_TRAP" "true"
    success "Phase 5: Added UFW trap rules"
    UFW_NEEDS_RELOAD=1
}

phase_6_configure_allowed_ips() {
    local -a allowed_ips=("$@")

    info "Phase 6: Configuring allowed IPs..."

    if [[ ${#allowed_ips[@]} -eq 0 ]]; then
        # Check if we have stored IPs
        local stored_ips
        stored_ips=$(state_get "$STATE_KEY_ALLOWED_IPS" '[]')
        if [[ "$stored_ips" != "[]" && "$stored_ips" != "null" ]]; then
            success "Phase 6: Using previously configured IPs"
            return 0
        fi
        warn "Phase 6: No IPs specified. ValKey will only be accessible from localhost."
        return 0
    fi

    if [[ $DRY_RUN -eq 1 ]]; then
        info "[DRY RUN] Would allow IPs: ${allowed_ips[*]}"
        return 0
    fi

    # Allow localhost first
    if ! check_ip_allowed "127.0.0.1"; then
        ufw allow from 127.0.0.1 to any port "$VALKEY_PORT" proto tcp comment "gNode ValKey localhost" >/dev/null 2>&1 || true
    fi

    # Allow each specified IP
    for ip in "${allowed_ips[@]}"; do
        if check_ip_allowed "$ip"; then
            debug "IP already allowed: $ip"
        else
            ufw allow from "$ip" to any port "$VALKEY_PORT" proto tcp comment "gNode ValKey node: $ip" >/dev/null 2>&1
            success "Allowed IP: $ip → port $VALKEY_PORT"
        fi
    done

    # Store allowed IPs in state
    local ips_json
    ips_json=$(printf '%s\n' "${allowed_ips[@]}" | jq -R . | jq -s .)
    state_set "$STATE_KEY_ALLOWED_IPS" "$ips_json"

    success "Phase 6: Configured ${#allowed_ips[@]} allowed IP(s)"
    UFW_NEEDS_RELOAD=1
}

phase_7_apply_and_verify() {
    info "Phase 7: Applying changes and verifying..."

    if [[ $DRY_RUN -eq 1 ]]; then
        info "[DRY RUN] Would restart services and verify"
        return 0
    fi

    # Restart ValKey if needed
    if [[ "${VALKEY_NEEDS_RESTART:-0}" -eq 1 ]]; then
        info "Restarting ValKey..."
        systemctl restart valkey-gnode
        sleep 2
    fi

    # Reload fail2ban if needed
    if [[ "${FAIL2BAN_NEEDS_RELOAD:-0}" -eq 1 ]]; then
        info "Reloading fail2ban..."
        systemctl reload fail2ban || systemctl restart fail2ban
    fi

    # Reload UFW if needed
    if [[ "${UFW_NEEDS_RELOAD:-0}" -eq 1 ]]; then
        info "Reloading UFW..."
        ufw reload >/dev/null 2>&1 || true
    fi

    # Verify ValKey is listening on correct port
    sleep 2
    if ss -tlnp 2>/dev/null | grep -q ":$VALKEY_PORT"; then
        success "ValKey listening on port $VALKEY_PORT"
    else
        error "ValKey NOT listening on port $VALKEY_PORT"
        warn "Check: systemctl status valkey-gnode"
        return 1
    fi

    # Verify fail2ban jail is active
    if fail2ban-client status gnode-trap &>/dev/null; then
        success "fail2ban gnode-trap jail is active"
    else
        warn "fail2ban gnode-trap jail may need manual activation"
        info "Try: sudo fail2ban-client reload"
    fi

    success "Phase 7: All changes applied and verified"
}

reset_hardening() {
    warn "Resetting network hardening to defaults..."

    if [[ $DRY_RUN -eq 1 ]]; then
        info "[DRY RUN] Would reset all hardening configuration"
        return 0
    fi

    # Remove fail2ban jail
    if [[ -f "$FAIL2BAN_JAIL" ]]; then
        rm -f "$FAIL2BAN_JAIL"
        info "Removed fail2ban jail"
    fi

    # Remove fail2ban filter
    if [[ -f "$FAIL2BAN_FILTER" ]]; then
        rm -f "$FAIL2BAN_FILTER"
        info "Removed fail2ban filter"
    fi

    # Remove trap rules from UFW
    if check_ufw_trap_rules_exist; then
        # Create cleaned version without trap rules
        grep -v "$LOG_PREFIX" "$UFW_BEFORE_RULES" | grep -v "gNode Network Hardening" > "${UFW_BEFORE_RULES}.clean"
        mv "${UFW_BEFORE_RULES}.clean" "$UFW_BEFORE_RULES"
        info "Removed UFW trap rules"
    fi

    # Reset ValKey to default port (optional - comment out if you want to keep custom port)
    # sed -i "s/^port .*/port 6379/" "$VALKEY_CONF"
    # info "Reset ValKey port to 6379"

    # Clear state
    state_remove "$STATE_KEY_VALKEY_PORT"
    state_remove "$STATE_KEY_FAIL2BAN_FILTER"
    state_remove "$STATE_KEY_FAIL2BAN_JAIL"
    state_remove "$STATE_KEY_UFW_TRAP"
    state_remove "$STATE_KEY_ALLOWED_IPS"

    # Reload services
    systemctl reload fail2ban 2>/dev/null || true
    ufw reload 2>/dev/null || true

    success "Network hardening reset to defaults"
    warn "Note: ValKey port was NOT reset. Edit $VALKEY_CONF manually if needed."
}

show_status() {
    echo ""
    echo "=============================================="
    echo "  gNode Network Hardening Status"
    echo "=============================================="
    echo ""

    local server_ip
    server_ip=$(get_server_ip)

    # ValKey port
    if check_valkey_port_configured; then
        echo -e "${GREEN}✓${NC} ValKey port: $VALKEY_PORT (hardened)"
    else
        local current_port
        current_port=$(grep "^port " "$VALKEY_CONF" 2>/dev/null | awk '{print $2}' || echo "6379")
        echo -e "${YELLOW}○${NC} ValKey port: $current_port (standard)"
    fi

    # ValKey bind
    if check_valkey_bind_configured; then
        echo -e "${GREEN}✓${NC} ValKey bind: includes $server_ip"
    else
        echo -e "${YELLOW}○${NC} ValKey bind: localhost only"
    fi

    # fail2ban filter
    if check_fail2ban_filter_exists; then
        echo -e "${GREEN}✓${NC} fail2ban filter: configured"
    else
        echo -e "${YELLOW}○${NC} fail2ban filter: not configured"
    fi

    # fail2ban jail
    if check_fail2ban_jail_exists; then
        echo -e "${GREEN}✓${NC} fail2ban jail: enabled"
        # Show current bans
        local banned
        banned=$(fail2ban-client status gnode-trap 2>/dev/null | grep "Currently banned" | awk '{print $NF}' || echo "0")
        echo "   Currently banned IPs: $banned"
    else
        echo -e "${YELLOW}○${NC} fail2ban jail: not configured"
    fi

    # UFW trap rules
    if check_ufw_trap_rules_exist; then
        echo -e "${GREEN}✓${NC} UFW trap rules: active"
        echo "   Trap zone: $TRAP_ZONE_START-$TRAP_ZONE_END (except $VALKEY_PORT)"
    else
        echo -e "${YELLOW}○${NC} UFW trap rules: not configured"
    fi

    # Allowed IPs
    local stored_ips
    stored_ips=$(state_get "$STATE_KEY_ALLOWED_IPS" '[]')
    if [[ "$stored_ips" != "[]" && "$stored_ips" != "null" ]]; then
        echo -e "${GREEN}✓${NC} Allowed IPs:"
        echo "$stored_ips" | jq -r '.[]' 2>/dev/null | while read -r ip; do
            echo "   - $ip"
        done
    else
        echo -e "${YELLOW}○${NC} Allowed IPs: none configured (localhost only)"
    fi

    echo ""
    echo "Connection string for remote nodes:"
    echo "  --redis-host $server_ip --redis-port $VALKEY_PORT"
    echo ""
}

# ============================================================================
# Main
# ============================================================================

main() {
    local -a allowed_ips=()
    local do_reset=0

    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help)
                show_usage
                exit 0
                ;;
            -p|--port)
                VALKEY_PORT="$2"
                shift 2
                ;;
            -b|--ban-time)
                BAN_TIME="$2"
                shift 2
                ;;
            -n|--dry-run)
                DRY_RUN=1
                shift
                ;;
            -v|--verbose)
                VERBOSE=1
                shift
                ;;
            --reset)
                do_reset=1
                shift
                ;;
            -*)
                error "Unknown option: $1"
                show_usage
                exit 1
                ;;
            *)
                # Assume it's an IP address
                allowed_ips+=("$1")
                shift
                ;;
        esac
    done

    # Require root
    require_root || exit 1

    echo ""
    echo "=============================================="
    echo "  gNode Network Hardening (Phase 20)"
    echo "=============================================="
    echo ""

    if [[ $DRY_RUN -eq 1 ]]; then
        warn "DRY RUN MODE - No changes will be made"
        echo ""
    fi

    # Handle reset
    if [[ $do_reset -eq 1 ]]; then
        reset_hardening
        exit 0
    fi

    # If no IPs provided, just show status
    if [[ ${#allowed_ips[@]} -eq 0 ]]; then
        show_status
        echo ""
        info "To configure hardening, run: $0 <ALLOWED_IP> [ALLOWED_IP2] ..."
        exit 0
    fi

    info "Configuring network hardening..."
    info "ValKey port: $VALKEY_PORT"
    info "Trap zone: $TRAP_ZONE_START-$TRAP_ZONE_END"
    info "Allowed IPs: ${allowed_ips[*]}"
    info "Ban time: ${BAN_TIME}s"
    echo ""

    # Initialize state tracking flags
    VALKEY_NEEDS_RESTART=0
    FAIL2BAN_NEEDS_RELOAD=0
    UFW_NEEDS_RELOAD=0

    # Run phases
    phase_1_check_dependencies || exit 1
    phase_2_configure_valkey_port
    phase_3_create_fail2ban_filter
    phase_4_create_fail2ban_jail
    phase_5_configure_ufw_trap_rules
    phase_6_configure_allowed_ips "${allowed_ips[@]}"
    phase_7_apply_and_verify || exit 1

    echo ""
    echo "=============================================="
    echo "  Network Hardening Complete"
    echo "=============================================="
    show_status
}

main "$@"
