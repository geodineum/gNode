#!/bin/bash
#
# Geodineum Constellation — WireGuard VPN Setup (Chapter 1 Infrastructure)
# ========================================================================
# Sets up a private WireGuard VPN between constellation nodes.
# ValKey binds to the WireGuard IP — never exposed to the public internet.
#
# Architecture:
#   - WireGuard creates a private network (10.66.0.0/24)
#   - Master gets 10.66.0.1, workers get 10.66.0.2+
#   - ValKey binds to 127.0.0.1 + 10.66.0.1 (VPN only)
#   - Only peers with the correct public key can join
#   - Port 47445 never touches the public interface
#   - Fail2ban watches ValKey logs as defense-in-depth
#
# Usage:
#   geodineum constellation init
#   geodineum constellation add-peer <name> <public_key> <endpoint>
#   geodineum constellation show-config
#   geodineum constellation close
#   geodineum constellation status
#
# Direct invocation:
#   ./constellation-setup.sh --init-master
#   ./constellation-setup.sh --add-peer <name> <public_key> <endpoint>
#   ./constellation-setup.sh --show-config
#   ./constellation-setup.sh --disable
#   ./constellation-setup.sh --status
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GNODE_ROOT="$(dirname "$SCRIPT_DIR")"

# common.sh derives LOG_FILE/STATE_FILE from GNODE_DIR; export it so they
# resolve to THIS install's real path instead of the legacy /opt/gNode
# fallback (which doesn't exist — its failed log write killed init under
# set -e, producing silent no-ops).
export GNODE_DIR="${GNODE_DIR:-$GNODE_ROOT}"

# Source common logging if available, otherwise define inline
if [[ -f "${GNODE_ROOT}/scripts/setup/lib/common.sh" ]]; then
    source "${GNODE_ROOT}/scripts/setup/lib/common.sh"
fi
# Ensure logging functions exist (standalone or fallback)
type log_info    &>/dev/null 2>&1 || log_info()    { echo "[INFO] $1"; }
type log_success &>/dev/null 2>&1 || log_success() { echo "[OK]   $1"; }
type log_warning &>/dev/null 2>&1 || log_warning() { echo "[WARN] $1"; }
type log_error   &>/dev/null 2>&1 || log_error()   { echo "[ERR]  $1" >&2; }
type log_step    &>/dev/null 2>&1 || log_step()    { echo ""; echo "==> $1"; }

BOLD="\033[1m"
NC="\033[0m"

# =============================================================================
# Configuration
# =============================================================================

VALKEY_CONF="${VALKEY_CONF:-/etc/valkey/valkey-gnode.conf}"
VALKEY_SERVICE="${VALKEY_SERVICE:-valkey-gnode}"
VALKEY_PORT="${VALKEY_PORT:-47445}"
VALKEY_LOG="${VALKEY_LOG:-/var/log/valkey-gsd/gsd-valkey.log}"

WG_INTERFACE="wg-geodineum"
WG_DIR="/etc/wireguard"
WG_CONF="${WG_DIR}/${WG_INTERFACE}.conf"
WG_PORT="${GEODINEUM_WG_PORT:-51820}"
WG_NETWORK="10.66.0"
WG_MASTER_IP="${WG_NETWORK}.1"

# Resolve the ValKey conf + unit ACTUALLY present. The installer's canonical
# layout is /etc/valkey/valkey.conf under valkey-gnode.service; legacy hosts
# carry valkey-gnode.conf or valkey-server.service. Hard-assuming one layout
# made init's bind step (and its boot-ordering drop-in) silently no-op on
# the others.
if [[ ! -f "$VALKEY_CONF" ]]; then
    for _cand in /etc/valkey/valkey-gnode.conf /etc/valkey/valkey.conf /usr/local/etc/valkey/valkey.conf; do
        if [[ -f "$_cand" ]]; then
            VALKEY_CONF="$_cand"
            break
        fi
    done
fi
# systemctl cat (exit code only) — NOT `list-unit-files | grep -q`: grep -q
# exits at first match, systemctl catches SIGPIPE, and pipefail turns the
# successful match into a failed pipeline (intermittent, match-only flake).
if ! systemctl cat "${VALKEY_SERVICE}.service" &>/dev/null; then
    for _cand in valkey-gnode valkey-server valkey; do
        if systemctl cat "${_cand}.service" &>/dev/null; then
            VALKEY_SERVICE="$_cand"
            break
        fi
    done
fi

VALKEY_DROPIN_DIR="/etc/systemd/system/${VALKEY_SERVICE}.service.d"
VALKEY_DROPIN="${VALKEY_DROPIN_DIR}/wg-ordering.conf"

FAIL2BAN_FILTER_DIR="/etc/fail2ban/filter.d"
FAIL2BAN_JAIL_DIR="/etc/fail2ban/jail.d"

STATE_DIR="/etc/geodineum/components/gnode"
STATE_FILE="${STATE_DIR}/constellation.state"
PEERS_DIR="${STATE_DIR}/peers"

# =============================================================================
# Usage
# =============================================================================

usage() {
    cat << 'EOF'
Usage: constellation-setup.sh <action> [options]

Manages a private WireGuard VPN for secure multi-node ValKey access.
ValKey never binds to the public interface — only localhost + VPN IP.

Typically invoked via: geodineum constellation <action>

Actions:
  --init-master             Initialize this server as constellation master
  --add-peer <name> <pubkey> <endpoint>
                            Add a worker node to the VPN
  --remove-peer <name>      Remove a worker node
  --show-config             Show WireGuard config for a new worker to use
  --disable                 Tear down VPN, revert ValKey to localhost
  --status                  Show current protection and VPN status

Options:
  --wg-port <port>          WireGuard UDP port (default: 51820)
  --dry-run                 Preview without changes

Setup flow:
  1. Master:  sudo geodineum constellation init
  2. Master:  geodineum constellation show-config
  3. Worker:  install WireGuard, use the shown config
  4. Master:  sudo geodineum constellation add-peer worker1 <pubkey> <ip>:<port>

Examples:
  sudo geodineum constellation init
  sudo geodineum constellation add-peer worker1 "aB3d...=" "203.0.113.50:51820"
  geodineum constellation show-config
  geodineum constellation status
EOF
}

# =============================================================================
# Helpers
# =============================================================================

ensure_wireguard() {
    if ! command -v wg &>/dev/null; then
        log_info "Installing WireGuard tools..."
        apt-get update -qq && apt-get install -y -qq wireguard-tools >/dev/null 2>&1 || {
            log_error "Failed to install wireguard-tools"
            log_error "Install manually: sudo apt install wireguard-tools"
            exit 1
        }
        log_success "WireGuard tools installed"
    fi
}

save_state() {
    mkdir -p "$STATE_DIR" "$PEERS_DIR"
    local key="$1" value="$2"
    if [[ -f "$STATE_FILE" ]]; then
        grep -v "^${key}=" "$STATE_FILE" > "${STATE_FILE}.tmp" 2>/dev/null || true
        mv "${STATE_FILE}.tmp" "$STATE_FILE"
    fi
    echo "${key}=${value}" >> "$STATE_FILE"
    chmod 640 "$STATE_FILE" 2>/dev/null || true
}

read_state() {
    local key="$1"
    [[ -f "$STATE_FILE" ]] && grep "^${key}=" "$STATE_FILE" 2>/dev/null | cut -d= -f2- || echo ""
}

next_peer_ip() {
    local count
    count=$(find "$PEERS_DIR" -name "*.conf" 2>/dev/null | wc -l)
    echo "${WG_NETWORK}.$((count + 2))"
}

detect_public_endpoint() {
    local ip
    ip=$(curl -s -4 ifconfig.me 2>/dev/null || hostname -I 2>/dev/null | awk '{print $1}')
    echo "${ip}:${WG_PORT}"
}

# =============================================================================
# ValKey boot ordering — a VPN-bound ValKey must start AFTER WireGuard
# =============================================================================
# valkey.conf hard-binds the VPN IP; at boot the base unit (After=network.target
# only) races wg-quick@. Losing the race fails the bind, and StartLimitBurst=3
# then gives up for good — the whole constellation stays down until a manual
# start. The drop-in turns the race into an ordering. disable_protection
# removes it together with the VPN bind.

ensure_valkey_wg_ordering() {
    if [[ -f "$VALKEY_DROPIN" ]]; then
        log_info "ValKey boot-ordering drop-in already present"
        return 0
    fi
    mkdir -p "$VALKEY_DROPIN_DIR"
    cat > "$VALKEY_DROPIN" << DROPEOF
# Geodineum constellation: ValKey binds the WireGuard VPN IP, so it must
# start after the tunnel interface exists. Managed by constellation-setup.sh.
[Unit]
After=wg-quick@${WG_INTERFACE}.service
Wants=wg-quick@${WG_INTERFACE}.service
StartLimitBurst=5

[Service]
RestartSec=5s
DROPEOF
    chmod 640 "$VALKEY_DROPIN"
    systemctl daemon-reload
    log_success "ValKey ordered after wg-quick@${WG_INTERFACE} (boot drop-in)"
}

remove_valkey_wg_ordering() {
    if [[ -f "$VALKEY_DROPIN" ]]; then
        rm -f "$VALKEY_DROPIN"
        rmdir "$VALKEY_DROPIN_DIR" 2>/dev/null || true
        systemctl daemon-reload
        log_success "ValKey boot-ordering drop-in removed"
    fi
}

# =============================================================================
# Init Master
# =============================================================================

init_master() {
    echo ""
    echo -e "${BOLD}Initializing Constellation Master${NC}"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    if [[ -f "$WG_CONF" ]]; then
        log_warning "WireGuard config already exists: ${WG_CONF}"
        log_info "To start fresh: sudo geodineum constellation close"
        return 0
    fi

    ensure_wireguard

    # ── Generate keypair ──
    log_step "Step 1: Generate WireGuard keypair"
    mkdir -p "$WG_DIR"
    chmod 700 "$WG_DIR"

    local privkey pubkey
    privkey=$(wg genkey)
    pubkey=$(echo "$privkey" | wg pubkey)

    if [[ "$DRY_RUN" == "true" ]]; then
        echo "  [DRY] Would generate keypair"
        echo "  [DRY] Public key: ${pubkey}"
    else
        save_state "master_pubkey" "$pubkey"
        save_state "master_privkey_file" "${WG_DIR}/${WG_INTERFACE}.key"
        echo "$privkey" > "${WG_DIR}/${WG_INTERFACE}.key"
        chmod 600 "${WG_DIR}/${WG_INTERFACE}.key"
        log_success "Keypair generated"
        log_info "Master public key: ${pubkey}"
    fi

    # ── Write WireGuard config ──
    log_step "Step 2: Configure WireGuard interface"

    if [[ "$DRY_RUN" == "true" ]]; then
        echo "  [DRY] Would create ${WG_CONF}"
        echo "  [DRY] Interface: ${WG_INTERFACE}, IP: ${WG_MASTER_IP}/24, Port: ${WG_PORT}"
    else
        cat > "$WG_CONF" << WGEOF
# Geodineum Constellation — Master Node
# Generated by: geodineum constellation init
# Interface: ${WG_INTERFACE}

[Interface]
Address = ${WG_MASTER_IP}/24
ListenPort = ${WG_PORT}
PrivateKey = ${privkey}

# Save/restore iptables rules for VPN routing
PostUp = iptables -A INPUT -p udp --dport ${WG_PORT} -j ACCEPT
PostDown = iptables -D INPUT -p udp --dport ${WG_PORT} -j ACCEPT

# Peers are added below by: geodineum constellation add-peer
WGEOF
        chmod 600 "$WG_CONF"
        log_success "Config written: ${WG_CONF}"
    fi

    # ── Start WireGuard ──
    log_step "Step 3: Start WireGuard"

    if [[ "$DRY_RUN" == "true" ]]; then
        echo "  [DRY] Would enable and start wg-quick@${WG_INTERFACE}"
    else
        systemctl enable "wg-quick@${WG_INTERFACE}" 2>/dev/null || true
        systemctl start "wg-quick@${WG_INTERFACE}" 2>/dev/null && \
            log_success "WireGuard interface ${WG_INTERFACE} is up" || {
                log_error "Failed to start WireGuard"
                log_info "Check: journalctl -u wg-quick@${WG_INTERFACE}"
                return 1
            }
    fi

    # ── Bind ValKey to VPN IP ──
    log_step "Step 4: Bind ValKey to VPN interface"

    if [[ "$DRY_RUN" == "true" ]]; then
        echo "  [DRY] Would add ${WG_MASTER_IP} to ValKey bind"
        echo "  [DRY] Would order ${VALKEY_SERVICE} after wg-quick@${WG_INTERFACE} (boot drop-in)"
    else
        if [[ -f "$VALKEY_CONF" ]]; then
            local current_bind
            current_bind=$(grep "^bind " "$VALKEY_CONF" | head -1)
            save_state "original_bind" "$current_bind"

            if echo "$current_bind" | grep -q "$WG_MASTER_IP"; then
                log_info "ValKey already binds to ${WG_MASTER_IP}"
            else
                sed -i "s/^bind .*/bind 127.0.0.1 ${WG_MASTER_IP}/" "$VALKEY_CONF"
                systemctl restart "$VALKEY_SERVICE" 2>/dev/null
                log_success "ValKey now binds to: 127.0.0.1 ${WG_MASTER_IP} (VPN only)"
            fi

            # VPN-bound ValKey must wait for the tunnel at boot (nightly
            # restarts roll this race every night without it)
            ensure_valkey_wg_ordering
        else
            log_warning "No ValKey conf found (tried valkey-gnode.conf, valkey.conf) — bind + boot-ordering skipped"
            log_info "Bind 127.0.0.1 ${WG_MASTER_IP} manually and re-run init, or set VALKEY_CONF="
        fi
    fi

    # ── Deploy fail2ban (defense-in-depth) ──
    log_step "Step 5: Fail2ban defense-in-depth"
    deploy_fail2ban

    # ── Firewall: WireGuard handshake on the public iface + ValKey ONLY on
    #    the VPN iface. Without the second rule, ufw's default-deny drops a
    #    peer's TCP connection to ValKey over the tunnel (ICMP still passes,
    #    so the link looks up) — the daemon then FATALs "ValKey unreachable
    #    ... connection timed out". ValKey stays closed on the public iface.
    log_step "Step 6: Firewall"

    if command -v ufw &>/dev/null; then
        if [[ "$DRY_RUN" != "true" ]]; then
            ufw allow "${WG_PORT}/udp" comment "Geodineum: WireGuard constellation" 2>/dev/null
            log_success "Allowed UDP ${WG_PORT} (WireGuard handshake)"
            ufw allow in on "${WG_INTERFACE}" to any port "${VALKEY_PORT}" proto tcp \
                comment "Geodineum: ValKey over VPN only" 2>/dev/null
            log_success "Allowed TCP ${VALKEY_PORT} on ${WG_INTERFACE} (VPN peers reach ValKey)"
            log_info "Port ${VALKEY_PORT} remains closed on the public interface"
        fi
    else
        log_info "No ufw — ensure UDP ${WG_PORT} is open and TCP ${VALKEY_PORT} is allowed on ${WG_INTERFACE}"
    fi

    save_state "enabled" "true"
    save_state "role" "master"
    save_state "wg_port" "$WG_PORT"
    save_state "wg_ip" "$WG_MASTER_IP"

    # ── Summary ──
    local endpoint
    endpoint=$(detect_public_endpoint)

    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo -e "${BOLD}Constellation Master Ready${NC}"
    echo ""
    echo "  VPN interface:   ${WG_INTERFACE}"
    echo "  VPN IP:          ${WG_MASTER_IP}/24"
    echo "  Public endpoint: ${endpoint}"
    echo "  Public key:      ${pubkey}"
    echo "  ValKey:          127.0.0.1 + ${WG_MASTER_IP} (port ${VALKEY_PORT})"
    echo "  Internet:        port ${VALKEY_PORT} NOT exposed"
    echo ""
    echo -e "${BOLD}Next:${NC} Add a worker node:"
    echo "  1. On the worker: install WireGuard and generate a keypair"
    echo "     sudo apt install wireguard-tools"
    echo "     wg genkey | tee /tmp/wg-private.key | wg pubkey > /tmp/wg-public.key"
    echo ""
    echo "  2. On this master: add the peer"
    echo "     sudo geodineum constellation add-peer worker1 \$(cat worker-public.key) <worker_ip>:${WG_PORT}"
    echo ""
    echo "  3. Show the config the worker needs:"
    echo "     geodineum constellation show-config"
    echo ""
}

# =============================================================================
# Add Peer
# =============================================================================

add_peer() {
    local name="$1"
    local pubkey="$2"
    local endpoint="$3"

    local peer_ip
    peer_ip=$(next_peer_ip)

    echo ""
    echo -e "${BOLD}Adding Peer: ${name}${NC}"
    echo "  Public key: ${pubkey}"
    echo "  Endpoint:   ${endpoint}"
    echo "  VPN IP:     ${peer_ip}/32"
    echo ""

    if [[ "$DRY_RUN" == "true" ]]; then
        echo "  [DRY] Would add peer to ${WG_CONF}"
        return
    fi

    cat >> "$WG_CONF" << PEEREOF

# Peer: ${name} (added $(date -Iseconds))
[Peer]
PublicKey = ${pubkey}
AllowedIPs = ${peer_ip}/32
Endpoint = ${endpoint}
PersistentKeepalive = 25
PEEREOF

    cat > "${PEERS_DIR}/${name}.conf" << INFOEOF
name=${name}
pubkey=${pubkey}
endpoint=${endpoint}
vpn_ip=${peer_ip}
added=$(date -Iseconds)
INFOEOF

    # Hot-reload WireGuard (no restart needed)
    wg set "$WG_INTERFACE" peer "$pubkey" allowed-ips "${peer_ip}/32" endpoint "$endpoint" persistent-keepalive 25 2>/dev/null \
        && log_success "Peer added and activated (hot-reload)" \
        || { systemctl restart "wg-quick@${WG_INTERFACE}" 2>/dev/null; log_success "Peer added (restarted interface)"; }

    save_state "peer_${name}" "${peer_ip}"

    echo ""
    echo "  Worker '${name}' can now reach ValKey at ${WG_MASTER_IP}:${VALKEY_PORT}"
    echo "  Their bootstrap.env: VALKEY_HOST=\"${WG_MASTER_IP}\""
    echo ""
}

# =============================================================================
# Remove Peer
# =============================================================================

remove_peer() {
    local name="$1"
    local peer_file="${PEERS_DIR}/${name}.conf"

    if [[ ! -f "$peer_file" ]]; then
        log_error "Peer '${name}' not found"
        return 1
    fi

    local pubkey
    pubkey=$(grep "^pubkey=" "$peer_file" | cut -d= -f2)

    if [[ "$DRY_RUN" != "true" ]]; then
        wg set "$WG_INTERFACE" peer "$pubkey" remove 2>/dev/null || true

        local tmp="${WG_CONF}.tmp"
        awk -v pk="$pubkey" '
            /^\[Peer\]/ { block=1; buf=$0; next }
            block && /^PublicKey/ && index($0, pk) { block=2; next }
            block==1 { buf=buf ORS $0; next }
            block==2 && /^\[/ { block=0; print; next }
            block==2 { next }
            !block { if (buf) { print buf; buf="" }; print }
            END { if (buf) print buf }
        ' "$WG_CONF" > "$tmp" && mv "$tmp" "$WG_CONF"

        rm -f "$peer_file"
        log_success "Removed peer: ${name}"
    else
        echo "  [DRY] Would remove peer: ${name} (${pubkey})"
    fi
}

# =============================================================================
# Show Config (for worker to use)
# =============================================================================

show_config() {
    local master_pubkey
    master_pubkey=$(read_state "master_pubkey")
    local master_endpoint
    master_endpoint=$(detect_public_endpoint)

    if [[ -z "$master_pubkey" ]]; then
        log_error "Master not initialized. Run: sudo geodineum constellation init"
        exit 1
    fi

    local peer_ip
    peer_ip=$(next_peer_ip)

    cat << CFGEOF

# ═══════════════════════════════════════════════════════════════
# Geodineum Constellation — Worker Node Config
# ═══════════════════════════════════════════════════════════════
# Save this as /etc/wireguard/wg-geodineum.conf on the worker.
# Replace WORKER_PRIVATE_KEY with the worker's generated private key.
#
# Setup on worker:
#   sudo apt install wireguard-tools
#   wg genkey | sudo tee /etc/wireguard/wg-geodineum.key | wg pubkey
#   # Edit the config below, paste private key
#   sudo systemctl enable --now wg-quick@wg-geodineum
#
# Then on the master:
#   sudo geodineum constellation add-peer <name> <pubkey> <worker_ip>:${WG_PORT}
# ═══════════════════════════════════════════════════════════════

[Interface]
Address = ${peer_ip}/24
ListenPort = ${WG_PORT}
PrivateKey = WORKER_PRIVATE_KEY

[Peer]
# Constellation master
PublicKey = ${master_pubkey}
Endpoint = ${master_endpoint}
AllowedIPs = ${WG_NETWORK}.0/24
PersistentKeepalive = 25

# ═══════════════════════════════════════════════════════════════
# After WireGuard is up, configure gNode on this worker:
#
#   bootstrap.env:
#     VALKEY_HOST="${WG_MASTER_IP}"
#     VALKEY_PORT="${VALKEY_PORT}"
#
#   daemon.env (or systemd override):
#     --node-id worker1
#     --node-type general
#     --redis-host ${WG_MASTER_IP}
# ═══════════════════════════════════════════════════════════════
CFGEOF
}

# =============================================================================
# Deploy fail2ban (defense-in-depth on VPN)
# =============================================================================

deploy_fail2ban() {
    if ! command -v fail2ban-client &>/dev/null; then
        log_warning "fail2ban not installed — skipping (defense-in-depth layer)"
        log_info "Install with: sudo apt install fail2ban"
        return
    fi

    if [[ "$DRY_RUN" == "true" ]]; then
        echo "  [DRY] Would deploy fail2ban filter + jail"
        return
    fi

    # Deploy filter (inline — no external template dependency)
    cat > "${FAIL2BAN_FILTER_DIR}/valkey-auth.conf" << 'FILTEREOF'
# Fail2ban filter for ValKey authentication failures
# Deployed by: geodineum constellation init

[Definition]

failregex = ^\d+:\w\s+\d+\s+\w+\s+\d+\s+\d+:\d+:\d+\.\d+\s+\*\s+User\s+\S+\s+failed\s+authentication.*from\s+<HOST>
            ^\d+:\w\s+\d+\s+\w+\s+\d+\s+\d+:\d+:\d+\.\d+\s+#\s+Client\s+closed\s+connection.*<HOST>

ignoreregex =

datepattern = {^LN-BEG}

journalmatch = _SYSTEMD_UNIT=valkey-gnode.service
FILTEREOF

    # Deploy jail
    cat > "${FAIL2BAN_JAIL_DIR}/valkey-auth.conf" << JAILEOF
# Fail2ban jail for ValKey authentication brute-force protection
#
# 3 failed auth attempts → permanent ban (must be manually removed).
# Defense-in-depth: ValKey is only reachable via WireGuard VPN,
# so any auth failure on the VPN is highly suspicious.
#
# Unban: sudo fail2ban-client set valkey-auth unbanip <ip>
# Deployed by: geodineum constellation init

[valkey-auth]
enabled  = true
port     = ${VALKEY_PORT}
filter   = valkey-auth
logpath  = ${VALKEY_LOG}
maxretry = 3
findtime = 600
bantime  = -1
action   = iptables-allports[name=valkey-auth, protocol=tcp]
JAILEOF

    fail2ban-client reload 2>/dev/null && log_success "Fail2ban: ValKey auth jail active" || true
}

# =============================================================================
# Disable
# =============================================================================

disable_protection() {
    echo ""
    echo -e "${BOLD}Disabling Constellation Network${NC}"
    echo ""

    if [[ "$DRY_RUN" == "true" ]]; then
        echo "  [DRY] Would stop WireGuard, revert ValKey, remove fail2ban"
        return
    fi

    systemctl stop "wg-quick@${WG_INTERFACE}" 2>/dev/null || true
    systemctl disable "wg-quick@${WG_INTERFACE}" 2>/dev/null || true
    log_success "WireGuard stopped"

    if [[ -f "$VALKEY_CONF" ]]; then
        sed -i "s/^bind .*/bind 127.0.0.1/" "$VALKEY_CONF"
        systemctl restart "$VALKEY_SERVICE" 2>/dev/null
        log_success "ValKey reverted to localhost-only"
    fi

    remove_valkey_wg_ordering

    rm -f "${FAIL2BAN_JAIL_DIR}/valkey-auth.conf" "${FAIL2BAN_FILTER_DIR}/valkey-auth.conf"
    fail2ban-client reload 2>/dev/null || true
    log_success "Fail2ban jail removed"

    if command -v ufw &>/dev/null; then
        ufw delete allow "${WG_PORT}/udp" 2>/dev/null || true
        ufw delete allow in on "${WG_INTERFACE}" to any port "${VALKEY_PORT}" proto tcp 2>/dev/null || true
        log_success "Firewall rules removed"
    fi

    rm -f "$STATE_FILE"
    log_success "Protection disabled"
    echo ""
    log_info "WireGuard config preserved at ${WG_CONF} (delete manually if permanent)"
}

# =============================================================================
# Status
# =============================================================================

show_status() {
    echo ""
    echo -e "${BOLD}Constellation Network Status${NC}"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    if ip link show "$WG_INTERFACE" &>/dev/null; then
        echo "  WireGuard:    UP (${WG_INTERFACE})"
        local wg_ip
        wg_ip=$(ip addr show "$WG_INTERFACE" 2>/dev/null | grep "inet " | awk '{print $2}')
        echo "  VPN IP:       ${wg_ip:-unknown}"

        local peer_count
        peer_count=$(wg show "$WG_INTERFACE" peers 2>/dev/null | wc -l)
        echo "  Peers:        ${peer_count}"
        if [[ $peer_count -gt 0 ]]; then
            wg show "$WG_INTERFACE" 2>/dev/null | grep -A3 "^peer:" | while IFS= read -r line; do
                echo "    ${line}"
            done
        fi
    else
        echo "  WireGuard:    DOWN"
    fi

    local current_bind
    current_bind=$(grep "^bind " "$VALKEY_CONF" 2>/dev/null | head -1)
    echo "  ValKey bind:  ${current_bind:-unknown}"
    local listening
    listening=$(ss -tlnp 2>/dev/null | grep ":${VALKEY_PORT} " | awk '{print $4}' | tr '\n' ' ')
    echo "  Listening:    ${listening:-not running}"

    if echo "$listening" | grep -qv "127.0.0.1\|10.66.0\|${WG_NETWORK}"; then
        echo -e "  \033[31mWARNING: ValKey may be exposed on a public interface\033[0m"
    else
        echo "  Public:       NOT EXPOSED (private VPN only)"
    fi

    if command -v fail2ban-client &>/dev/null; then
        local jail_status
        jail_status=$(fail2ban-client status valkey-auth 2>/dev/null | grep "Currently banned" || echo "not active")
        echo "  Fail2ban:     ${jail_status}"
    else
        echo "  Fail2ban:     not installed"
    fi

    if [[ -d "$PEERS_DIR" ]]; then
        local peers
        peers=$(find "$PEERS_DIR" -name "*.conf" 2>/dev/null)
        if [[ -n "$peers" ]]; then
            echo ""
            echo "  Peers:"
            while IFS= read -r pf; do
                local pname pvpn
                pname=$(grep "^name=" "$pf" | cut -d= -f2)
                pvpn=$(grep "^vpn_ip=" "$pf" | cut -d= -f2)
                echo "    ${pname}: ${pvpn}"
            done <<< "$peers"
        fi
    fi

    echo ""
}

# =============================================================================
# Expand — one-shot worker enrollment
# =============================================================================
# Mints a worker keypair, registers the peer (reusing add_peer), and emits a
# single base64 bundle containing the worker's WireGuard config + the master's
# ValKey credentials. Collapses add-peer + show-config + credential copy into
# one master command; the worker pastes the bundle into its installer.
expand_node() {
    local name="$1"
    local endpoint="$2"
    if [[ -z "$name" || -z "$endpoint" ]]; then
        log_error "Usage: --expand <name> <worker_ip:port>"
        exit 1
    fi
    if [[ -z "$(read_state master_pubkey)" ]]; then
        log_error "Master not initialized. Run: sudo geodineum constellation init"
        exit 1
    fi

    # Mint the worker keypair on the master; the private key travels in the bundle.
    local worker_priv worker_pub
    worker_priv="$(wg genkey)"
    worker_pub="$(printf '%s' "$worker_priv" | wg pubkey)"

    # Register the peer (assigns VPN IP, appends to wg conf, hot-reloads).
    add_peer "$name" "$worker_pub" "$endpoint"
    local peer_ip; peer_ip="$(read_state "peer_${name}")"
    [[ -z "$peer_ip" ]] && peer_ip="$(next_peer_ip)"

    local master_pub master_ep
    master_pub="$(read_state master_pubkey)"
    master_ep="$(detect_public_endpoint)"

    local cred_dir="/etc/geodineum/credentials"
    local daemon_pw="" replica_pw=""
    [[ -r "${cred_dir}/valkey_daemon.password" ]]  && daemon_pw="$(cat "${cred_dir}/valkey_daemon.password")"
    [[ -r "${cred_dir}/valkey_replica.password" ]] && replica_pw="$(cat "${cred_dir}/valkey_replica.password")"

    # Per-node ValKey identity. Every daemon sharing one `gnode_daemon` login
    # means the master cannot tell one worker from another — no attribution,
    # no per-node scoping, and no basis for authorising anything a node asks
    # for. Mint this node its own user of the same daemon tier.
    #
    # Grants come from the rule the installer wrote when it provisioned
    # gnode_daemon, never from a copy kept here: one definition of the
    # privilege boundary. Identical grants means this changes identity only,
    # not what a node may do — scoping is a later, separate change.
    local node_user="gnode_node_$(printf '%s' "$name" | tr -c 'a-zA-Z0-9_' '_')"
    local node_pw="" acl_rule_file="/etc/geodineum/components/gnode-daemon/acl-daemon-tier.rule"
    local admin_pwfile="${cred_dir}/valkey.password"
    [[ -r "$admin_pwfile" ]] || admin_pwfile="${cred_dir}/valkey_admin.password"

    if [[ -r "$acl_rule_file" && -r "$admin_pwfile" ]]; then
        node_pw="$(head -c 32 /dev/urandom | base64 | tr -d '/+=' | head -c 32)"
        # Unquoted on purpose: the rule file holds whitespace-separated ACL
        # tokens that must arrive as separate arguments.
        # shellcheck disable=SC2046
        if REDISCLI_AUTH="$(cat "$admin_pwfile")" valkey-cli -p "${VALKEY_PORT:-47445}" \
            ACL SETUSER "$node_user" resetpass ">${node_pw}" $(cat "$acl_rule_file") >/dev/null 2>&1; then
            REDISCLI_AUTH="$(cat "$admin_pwfile")" valkey-cli -p "${VALKEY_PORT:-47445}" ACL SAVE >/dev/null 2>&1 || true
            log_success "Minted per-node ValKey identity: ${node_user}"
        else
            log_warning "Could not mint ${node_user} — the worker will fall back to the shared gnode_daemon login."
            node_pw=""
        fi
    else
        log_warning "Daemon-tier ACL rule or admin credential unreadable — no per-node identity minted."
        log_warning "  rule: ${acl_rule_file}"
        log_warning "  This master predates per-node identities; re-run the installer to write the rule."
    fi
    [[ -z "$node_pw" ]] && node_user=""

    local bundle
    bundle="$(cat <<BUNDLE
===GEODINEUM-CONSTELLATION-BUNDLE-V2===
name=${name}
vpn_ip=${peer_ip}
---WIREGUARD---
[Interface]
Address = ${peer_ip}/24
PrivateKey = ${worker_priv}

[Peer]
PublicKey = ${master_pub}
Endpoint = ${master_ep}
AllowedIPs = ${WG_NETWORK}.0/24
PersistentKeepalive = 25
---VALKEY_DAEMON_PASSWORD---
${daemon_pw}
---VALKEY_REPLICA_PASSWORD---
${replica_pw}
---VALKEY_NODE_USER---
${node_user}
---VALKEY_NODE_PASSWORD---
${node_pw}
===END===
BUNDLE
)"
    local b64; b64="$(printf '%s' "$bundle" | base64 -w0)"

    echo ""
    echo -e "${BOLD}Expansion bundle for '${name}' (VPN IP ${peer_ip})${NC}"
    log_warning "Contains the worker's private key + ValKey credentials — treat as a secret; copy over a trusted channel."
    echo ""
    echo "  On the worker: run the installer, choose 'Join constellation', and paste"
    echo "  this single line at the bundle prompt:"
    echo ""
    echo "$b64"
    echo ""
}

# =============================================================================
# Main
# =============================================================================

ACTION=""
PEER_NAME=""
PEER_PUBKEY=""
PEER_ENDPOINT=""
DRY_RUN=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --init-master)  ACTION="init"; shift ;;
        --add-peer)     ACTION="add-peer"; PEER_NAME="$2"; PEER_PUBKEY="$3"; PEER_ENDPOINT="$4"; shift 4 ;;
        --expand)       ACTION="expand"; PEER_NAME="${2:-}"; PEER_ENDPOINT="${3:-}"; shift $(( $# > 3 ? 3 : $# )) ;;
        --remove-peer)  ACTION="remove-peer"; PEER_NAME="$2"; shift 2 ;;
        --show-config)  ACTION="show-config"; shift ;;
        --disable)      ACTION="disable"; shift ;;
        --status)       ACTION="status"; shift ;;
        --wg-port)      WG_PORT="$2"; shift 2 ;;
        --dry-run)      DRY_RUN=true; shift ;;
        --help|-h)      usage; exit 0 ;;
        *)              log_error "Unknown option: $1"; usage; exit 1 ;;
    esac
done

case "$ACTION" in
    init)
        [[ $EUID -ne 0 ]] && { log_error "Requires root"; exit 1; }
        init_master
        ;;
    add-peer)
        [[ $EUID -ne 0 ]] && { log_error "Requires root"; exit 1; }
        [[ -z "$PEER_NAME" || -z "$PEER_PUBKEY" || -z "$PEER_ENDPOINT" ]] && {
            log_error "Usage: --add-peer <name> <public_key> <endpoint_ip:port>"
            exit 1
        }
        add_peer "$PEER_NAME" "$PEER_PUBKEY" "$PEER_ENDPOINT"
        ;;
    expand)
        [[ $EUID -ne 0 ]] && { log_error "Requires root"; exit 1; }
        expand_node "$PEER_NAME" "$PEER_ENDPOINT"
        ;;
    remove-peer)
        [[ $EUID -ne 0 ]] && { log_error "Requires root"; exit 1; }
        remove_peer "$PEER_NAME"
        ;;
    show-config)
        show_config
        ;;
    disable)
        [[ $EUID -ne 0 ]] && { log_error "Requires root"; exit 1; }
        disable_protection
        ;;
    status)
        show_status
        ;;
    "")
        usage
        exit 0
        ;;
esac
