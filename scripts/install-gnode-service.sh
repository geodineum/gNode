#!/bin/bash
#
# Install gNode daemon as a systemd service
#

set -euo pipefail  # Exit on error, unset vars, and pipe failures

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
SERVICE_FILE="$PROJECT_ROOT/daemon/config/gnode-daemon.service"

# ---------------------------------------------------------------------------
# Yes/No prompt with timeout + non-TTY auto-default.
#
# Pre-fix: each `read -t 30 -p "..." -n 1 -r` call ran under `set -e`. When
# the script was invoked non-interactively (e.g. by Geodineum/install.sh
# which pipes the install through `tee`), stdin wasn't a TTY and the read
# returned EOF immediately → non-zero exit code → set -e killed the script
# at the first prompt. The legacy-valkey-mask prompt at line ~123 was the
# observed crash site on operator's first install run.
#
# Post-fix:
#   - When stdin is not a TTY → use the supplied default, no read attempt
#   - When stdin is a TTY → read with 30s timeout, fall back to default
#     on timeout or EOF (no `set -e` death)
# ---------------------------------------------------------------------------
prompt_yn() {
    local question="$1"
    local default_letter="${2:-Y}"   # one of Y / N (the documented default)
    REPLY=""
    if [ ! -t 0 ]; then
        echo "${question}${default_letter}  (no TTY → using default)"
        REPLY="$default_letter"
        return 0
    fi
    # `|| true` neutralises set -e on timeout/EOF; empty REPLY then falls
    # through to the default below.
    read -t 30 -p "$question" -n 1 -r || true
    echo
    # use if/then/fi (not `&&` short-circuit) so the function's
    # exit code is always 0. Pre-fix `[ -z "$REPLY" ] && REPLY="..."`
    # had a nasty set -e × && interaction: when REPLY was non-empty
    # (the common case — operator typed a key), `[ -z "y" ]` returned
    # 1, the `&&` short-circuited, the line's overall exit was 1,
    # the function returned 1, and the caller's set -e killed the
    # script on a normal Y answer. The no-TTY branch happened to
    # work via the explicit `return 0` — which masked the bug on
    # CI/installer-piped invocations and only surfaced when an
    # actual operator interacted with the prompt. Decision U from
    # CH1_INSTALL_DEBUG_PRIMER (set -e doesn't mix with && chains).
    if [ -z "$REPLY" ]; then
        REPLY="$default_letter"
    fi
    return 0
}

# Detect the canonical Valkey systemd unit. Different install paths use
# different unit names — the source-build installer uses `valkey-server`,
# the legacy gNode setup-valkey-smart.sh used `valkey-gnode`, the apt
# valkey-server package also uses `valkey-server`. Any of these means
# "Valkey is set up"; the script's hard requirement is just that ONE of
# them is reachable.
have_valkey_unit() {
    systemctl cat valkey-gnode.service &>/dev/null \
        || systemctl cat valkey-server.service &>/dev/null
}

# Return the ACTUAL Valkey unit name present on this host. The source-build
# install path creates `valkey-gnode.service`; the apt valkey-server package
# creates `valkey-server.service`. The shipped unit file hardcodes one name
# in After=/Requires=/PartOf=, so the installed copy must be rewritten to
# whichever unit actually exists — otherwise Requires= names a missing unit
# and `systemctl start gnode-daemon` fails with "Unit ... not found" (seen
# on 22.04 source-build hosts, where the apt package is unavailable).
detect_valkey_unit() {
    # systemctl cat (exit code only) — `list-unit-files | grep -q` flakes
    # under pipefail: grep -q exits at first match → systemctl SIGPIPE →
    # pipeline "fails" exactly when the unit EXISTS. A flake here strips
    # the daemon's ValKey dependency on a host that has local ValKey.
    if systemctl cat valkey-gnode.service &>/dev/null; then
        echo "valkey-gnode.service"
    elif systemctl cat valkey-server.service &>/dev/null; then
        echo "valkey-server.service"
    else
        echo ""   # caller already warned via have_valkey_unit
    fi
}
SERVICE_DEST="/etc/systemd/system/gnode-daemon.service"

# Parse flags
USER_ONLY=false
for arg in "$@"; do
    case "$arg" in
        --user-only) USER_ONLY=true ;;
    esac
done

echo "=== gNode Daemon Service Installation ==="
echo

# Check if we need sudo
if [ "$EUID" -ne 0 ]; then
    echo "This script must be run with sudo privileges to install systemd services."
    echo "Usage: sudo $0"
    exit 1
fi

# =============================================================================
# Create gnode user and group if they don't exist
# =============================================================================
GNODE_USER="gnode"
GNODE_GROUP="gnode"

echo "Setting up gNode system user..."

# Create group if it doesn't exist
if ! getent group "$GNODE_GROUP" > /dev/null 2>&1; then
    echo "Creating group: $GNODE_GROUP"
    groupadd --system "$GNODE_GROUP"
    echo "✓ Group '$GNODE_GROUP' created"
else
    echo "✓ Group '$GNODE_GROUP' already exists"
fi

# Create user if it doesn't exist
if ! getent passwd "$GNODE_USER" > /dev/null 2>&1; then
    echo "Creating user: $GNODE_USER"
    useradd --system \
        --gid "$GNODE_GROUP" \
        --home-dir "/opt/geodineum" \
        --no-create-home \
        --shell /usr/sbin/nologin \
        --comment "gNode Service Daemon" \
        "$GNODE_USER"
    echo "✓ User '$GNODE_USER' created"
else
    echo "✓ User '$GNODE_USER' already exists"
fi

# NOTE: We do NOT add www-data to gnode group (principle of least privilege)
# Instead, password files are set to gnode:www-data so PHP can read ONLY those files

# Add gnode user to valkey group (for backup access to RDB files)
if getent group valkey > /dev/null 2>&1; then
    if ! groups "$GNODE_USER" 2>/dev/null | grep -q "\bvalkey\b"; then
        usermod -aG valkey "$GNODE_USER"
        echo "✓ Added '$GNODE_USER' to 'valkey' group (for backup access)"
    fi
fi
echo

# --user-only: exit after user/group setup (for setup-geodineum.sh bootstrap)
if [ "$USER_ONLY" = true ]; then
    echo "✓ User/group setup complete (--user-only mode)"
    exit 0
fi

# Check if daemon binary exists
if [ ! -f "$PROJECT_ROOT/daemon/target/release/gnode-daemon" ]; then
    echo "Error: gNode daemon binary not found at $PROJECT_ROOT/daemon/target/release/gnode-daemon"
    echo "Please build the daemon first with: cd daemon && cargo build --release"
    exit 1
fi

# Check if password file exists (ACL daemon password preferred, fallback to default)
if [ -f "$PROJECT_ROOT/.gnode/valkey_daemon.password" ]; then
    echo "✓ Using ACL daemon credentials (.gnode/valkey_daemon.password)"
elif [ -f "$PROJECT_ROOT/.gnode/valkey.password" ]; then
    echo "⚠ Warning: Using legacy password authentication (.gnode/valkey.password)"
    echo "  For better security, consider migrating to ACL authentication"
else
    echo "Error: ValKey password file not found"
    echo "  Expected: $PROJECT_ROOT/.gnode/valkey_daemon.password (ACL)"
    echo "  Or: $PROJECT_ROOT/.gnode/valkey.password (legacy)"
    echo "Please run scripts/setup-valkey-smart.sh first"
    exit 1
fi

# Check for old valkey.service and offer to retire it.
#
# `systemctl list-unit-files | grep -q "^valkey.service"` is too loose:
# it matches the unit name even when the unit is already masked
# (symlink → /dev/null) from a previous run of THIS script, OR doesn't
# really exist as a separate competing daemon (the modern install path
# uses valkey-server.service; the apt valkey-server package may also
# expose valkey.service as an alias). We only want to act when there's
# a REAL competing service we haven't already retired.
#
# Tighten:
#   - `systemctl is-enabled valkey.service` returns:
#       enabled | enabled-runtime | linked → real unit, may compete
#       static                              → built-in alias, ignore
#       disabled                            → real unit but not active
#       masked                              → already retired, no-op
#       not-found                           → no such unit, no-op
#   We only enter the retire-prompt path when the state is one of the
#   first two real-and-active categories. Everything else is already
#   safe.
#
# Also: `systemctl mask` is wrapped with `|| true` so a benign "already
# masked" failure under `set -e` doesn't kill the install. Pre-fix this
# was the silent-crash site on operator's second install (mask failed
# because valkey.service was already masked from the prior run; uninstall
# didn't unmask, so state accumulated; set -e killed the script;
# Phase 8 reported [FAIL] Services).
valkey_state=$(systemctl is-enabled valkey.service 2>/dev/null || echo "not-found")
case "$valkey_state" in
    enabled|enabled-runtime|linked)
        echo
        echo "🔧 Detected old valkey.service (state: $valkey_state)"

        if systemctl is-active --quiet valkey.service; then
            echo "  Status: RUNNING (conflicts with valkey-server.service)"
            echo "  Action needed: Stop and disable to avoid conflicts"
            echo
            prompt_yn "Stop and disable old valkey.service? (Y/n) " Y
            if [[ ! $REPLY =~ ^[Nn]$ ]]; then
                echo "Stopping old valkey.service..."
                systemctl stop valkey.service || true
                systemctl disable valkey.service 2>/dev/null || true
                systemctl mask valkey.service 2>/dev/null || true
                echo "✓ Old valkey.service stopped, disabled, and masked"
            else
                echo "⚠️  Warning: Leaving old service active may cause conflicts"
            fi
        else
            echo "  Status: Inactive (already stopped)"
            prompt_yn "Disable and mask old valkey.service to prevent conflicts? (Y/n) " Y
            if [[ ! $REPLY =~ ^[Nn]$ ]]; then
                systemctl disable valkey.service 2>/dev/null || true
                systemctl mask valkey.service 2>/dev/null || true
                echo "✓ Old valkey.service disabled and masked"
            fi
        fi
        echo
        ;;
    masked|not-found|static|disabled|"")
        # Already retired, doesn't exist as a separate unit, is a built-in
        # alias, or is disabled-and-out-of-the-way. No action needed.
        : # explicit no-op for clarity
        ;;
    *)
        echo "  (valkey.service in unexpected state: $valkey_state — skipping retire step)"
        ;;
esac

# Check that SOME Valkey systemd unit is present. The source-build
# installer ships valkey-server.service; the apt valkey-server package
# also installs that name; the legacy gNode setup-valkey-smart.sh used
# valkey-gnode.service. Any of these satisfies the runtime dependency.
# Pre-fix only checked for valkey-gnode.service which left current
# installs falsely warning + offering to abort.
# A constellation worker (full/headless tier) has no local ValKey unit by
# design — bootstrap.env points the daemon at the master over the VPN, and
# the install step below strips the local-unit dependency from the unit file.
# Only warn/prompt when bootstrap.env says ValKey is expected on this host.
BOOTSTRAP_VALKEY_HOST="$(grep -s '^VALKEY_HOST=' /etc/geodineum/bootstrap.env | cut -d= -f2)"
if ! have_valkey_unit; then
    case "$BOOTSTRAP_VALKEY_HOST" in
        ""|127.0.0.1|localhost|::1)
            echo "Warning: no valkey systemd unit found (looked for valkey-server.service and valkey-gnode.service)"
            echo "The gNode daemon requires ValKey to be running"
            prompt_yn "Continue anyway? (y/N) " N
            if [[ ! $REPLY =~ ^[Yy]$ ]]; then
                exit 1
            fi
            ;;
        *)
            echo "✓ No local ValKey unit — remote ValKey at ${BOOTSTRAP_VALKEY_HOST} (constellation worker)"
            ;;
    esac
fi

# Check if gnode-daemon service is already installed
if systemctl cat gnode-daemon.service &>/dev/null; then
    echo
    echo "⚠️  WARNING: gnode-daemon.service is already installed"

    # Check if it's running
    if systemctl is-active --quiet gnode-daemon.service; then
        echo "  Status: Currently RUNNING"
        echo
        echo "This installation will:"
        echo "  1. Stop the current daemon"
        echo "  2. Update service file with ACL credentials"
        echo "  3. Reload systemd"
        echo "  4. Restart daemon with new configuration"
        echo
        # Default Y: an installer (re-)run exists to converge state. Under
        # no-TTY (Geodineum installer driver) the old N default silently
        # skipped every unit-file and daemon.env update on running nodes.
        prompt_yn "Proceed with upgrade to ACL authentication? (Y/n) " Y
        if [[ $REPLY =~ ^[Nn]$ ]]; then
            echo "Installation cancelled."
            exit 0
        fi

        # Stop the running service
        echo "Stopping current gnode-daemon service..."
        systemctl stop gnode-daemon.service
        echo "✓ Service stopped"
    else
        echo "  Status: Installed but not running"
        echo
        echo "This will update the service file with ACL authentication."
        prompt_yn "Proceed with upgrade? (Y/n) " Y
        if [[ $REPLY =~ ^[Nn]$ ]]; then
            echo "Installation cancelled."
            exit 0
        fi
    fi
    echo
fi

# Copy service file
echo "Installing service file..."
cp "$SERVICE_FILE" "$SERVICE_DEST"

# Rewrite the Valkey dependency to the unit that ACTUALLY exists on this
# host (source-build → valkey-gnode.service, apt → valkey-server.service).
# The shipped file hardcodes valkey-server.service; without this rewrite a
# source-build host fails to start gnode-daemon (Requires= → missing unit).
_vk_unit="$(detect_valkey_unit)"
if [[ -n "$_vk_unit" ]]; then
    sed -i -E "s/valkey-(server|gnode)\.service/${_vk_unit}/g" "$SERVICE_DEST"
    echo "✓ Service file installed to $SERVICE_DEST (Valkey dependency: ${_vk_unit})"
else
    # No local ValKey unit — this is a constellation worker (full/headless)
    # whose daemon connects to the MASTER's ValKey over the VPN. A
    # Requires=/PartOf= on a nonexistent local unit would block start
    # ("Unit valkey-gnode.service not found"), so strip the ValKey dependency
    # entirely; the daemon reaches ValKey via VALKEY_HOST, not a local unit.
    sed -i -E '/^(Requires|PartOf)=valkey-(server|gnode)\.service$/d' "$SERVICE_DEST"
    _wg_after=""
    if [[ -f /etc/wireguard/wg-geodineum.conf ]]; then
        # Remote ValKey is only reachable over the tunnel — order after it
        _wg_after=" wg-quick@wg-geodineum.service"
    fi
    sed -i -E "s/^After=network\.target valkey-(server|gnode)\.service$/After=network.target network-online.target${_wg_after}/" "$SERVICE_DEST"
    if [[ -n "$_wg_after" ]]; then
        sed -i '/^After=network.target network-online.target/a Wants=wg-quick@wg-geodineum.service' "$SERVICE_DEST"
    fi
    # The remote master may be mid-reboot when this worker boots (nightly
    # restarts on both hosts) — a bounded StartLimit would give up for good
    sed -i -E 's/^StartLimitInterval(Sec)?=.*$/StartLimitIntervalSec=0/' "$SERVICE_DEST"
    echo "✓ Service file installed to $SERVICE_DEST (no local ValKey — ValKey dependency stripped; daemon uses remote ValKey via VALKEY_HOST)"
fi

# Set permissions
chmod 644 "$SERVICE_DEST"
echo "✓ Service file permissions set"

# =============================================================================
# Set ownership of gNode directories
# =============================================================================
echo
echo "Setting gNode directory ownership..."

# Production directory.
# The DEPLOY USER owns source trees — geodeploy and installer re-runs fetch
# and build as that user, and a gnode-owned tree breaks every later pull
# ("cannot open .git/FETCH_HEAD: Permission denied", hit on the squad join).
# The daemon only needs gnode GROUP read/traverse plus its runtime dirs
# (logs, run — re-owned to gnode below).
DEPLOY_OWNER="$(grep -s '^DEPLOY_USER=' /etc/geodineum/deploy.env | cut -d= -f2)"
if [ -z "$DEPLOY_OWNER" ] || ! id -u "$DEPLOY_OWNER" >/dev/null 2>&1; then
    DEPLOY_OWNER="$GNODE_USER"
fi
PROD_DIR="/opt/geodineum/gNode"
if [ -d "$PROD_DIR" ]; then
    chown -R "$DEPLOY_OWNER:$GNODE_GROUP" "$PROD_DIR"
    # 0751 (was 0750). o+x allows www-data (and other non-gnode
    # processes) to TRAVERSE the dir to reach the .gnode/ symlink
    # which resolves to /etc/geodineum/credentials/. PHP's
    # bootstrap-loader needs to fopen() per-site client password
    # files (gnode:www-data 0640) under that symlink. With 0750,
    # the dir denied traversal entirely and gCore logged
    # "no readable ValKey password under /etc/geodineum/credentials"
    # despite the per-file ownership being correct.
    #
    # Listing the dir (ls /opt/geodineum/gNode/) still requires
    # gnode group (no o+r), so non-gnode users see nothing on `ls`.
    # Individual file modes (binary, source, configs) unchanged.
    # Mirrors the /etc/geodineum/credentials/ fix one dir up.
    chmod 0751 "$PROD_DIR"
    echo "✓ Set ownership of $PROD_DIR (0751)"

    # Password directory permissions.
    # when $PROD_DIR/.gnode is a symlink (current layout: points
    # to /etc/geodineum/credentials/), the directory's permissions are
    # managed by Geodineum/install.sh Phase 7 with the narrow group model
    # (root:geodineum-creds 0750). DO NOT chown/chmod through the symlink
    # here — `chown` and `chmod` follow symlinks by default, so doing so
    # mutates the centralized credential dir's permissions away from
    # the canonical setting (gnode:geodineum-creds was observed
    # previously because this block ran AFTER Phase 7 and overrode it).
    #
    # Per-file chown's via the symlink (lines below) follow the link to
    # the actual files in /etc/geodineum/credentials/ — that's OK
    # because the canonical ownership of those files (gnode:* 0640)
    # matches what we want anyway.
    if [ -L "$PROD_DIR/.gnode" ]; then
        echo "✓ Password directory is symlink → centralized store"
        echo "  (perms managed by Geodineum installer Phase 7)"
    elif [ -d "$PROD_DIR/.gnode" ]; then
        # Legacy / standalone install — manage the directory directly.
        chown "$GNODE_USER:geodineum" "$PROD_DIR/.gnode"
        chmod 750 "$PROD_DIR/.gnode"
        echo "✓ Set permissions on password directory"
    fi

    # Per-file ownership (follows symlinks intentionally — acts on the
    # canonical files in /etc/geodineum/credentials/).
    if [ -e "$PROD_DIR/.gnode" ]; then
        # Daemon password: gnode:gnode (daemon only, no PHP access)
        if [ -f "$PROD_DIR/.gnode/valkey_daemon.password" ]; then
            chown "$GNODE_USER:$GNODE_GROUP" "$PROD_DIR/.gnode/valkey_daemon.password"
            chmod 640 "$PROD_DIR/.gnode/valkey_daemon.password"
        fi

        # Client passwords: gnode:www-data (PHP needs read access)
        for pwfile in "$PROD_DIR/.gnode"/valkey_client_*.password; do
            if [ -f "$pwfile" ]; then
                chown "$GNODE_USER:www-data" "$pwfile"
                chmod 640 "$pwfile"
            fi
        done 2>/dev/null || true
        echo "  - valkey_daemon.password: gnode:gnode (daemon only)"
        echo "  - valkey_client_*.password: gnode:www-data (PHP readable)"
    fi

    # Required directories for systemd ProtectSystem=strict + ReadWritePaths
    # These MUST exist before the service starts or namespace setup fails
    for subdir in logs run; do
        mkdir -p "$PROD_DIR/$subdir"
        chown "$GNODE_USER:$GNODE_GROUP" "$PROD_DIR/$subdir"
        chmod 750 "$PROD_DIR/$subdir"
    done
    mkdir -p /var/log/geodineum/gnode
    chown "$GNODE_USER:$GNODE_GROUP" /var/log/geodineum/gnode
    chmod 750 /var/log/geodineum/gnode
    echo "✓ Created required directories (logs, run, /var/log/geodineum/gnode)"
fi

# Development directory (if different from production)
DEV_DIR="/opt/gNode"
if [ -d "$DEV_DIR" ] && [ "$DEV_DIR" != "$PROD_DIR" ]; then
    chown -R "$GNODE_USER:$GNODE_GROUP" "$DEV_DIR"
    chmod 750 "$DEV_DIR"
    echo "✓ Set ownership of $DEV_DIR (750)"

    if [ -d "$DEV_DIR/.gnode" ]; then
        chown "$GNODE_USER:geodineum" "$DEV_DIR/.gnode"
        chmod 750 "$DEV_DIR/.gnode"
        # Daemon password: gnode:gnode
        if [ -f "$DEV_DIR/.gnode/valkey_daemon.password" ]; then
            chown "$GNODE_USER:$GNODE_GROUP" "$DEV_DIR/.gnode/valkey_daemon.password"
            chmod 640 "$DEV_DIR/.gnode/valkey_daemon.password"
        fi
        # Client passwords: gnode:www-data
        for pwfile in "$DEV_DIR/.gnode"/valkey_client_*.password; do
            if [ -f "$pwfile" ]; then
                chown "$GNODE_USER:www-data" "$pwfile"
                chmod 640 "$pwfile"
            fi
        done 2>/dev/null || true
    fi
fi

# gNode extensions directories — deploy-user owned for the same reason as
# PROD_DIR (these are git working trees the deploy user must keep pulling)
for ext_dir in /opt/geodineum/Geodineum-COMMS /opt/geodineum/Geodineum-BAK; do
    if [ -d "$ext_dir" ]; then
        chown -R "$DEPLOY_OWNER:$GNODE_GROUP" "$ext_dir"
        chmod 750 "$ext_dir"
        echo "✓ Set ownership of $ext_dir (750, owner ${DEPLOY_OWNER})"
    fi
done
echo

# install daemon.env to /etc/geodineum/components/gnode-daemon/.
# Previously, this file was treated as "optional override" — but the
# template ships with VALKEY_USER="gnode_daemon" which the daemon
# REQUIRES to construct the correct Redis URL (redis://gnode_daemon:pw@…
# instead of redis://:pw@… — empty user = `default` which has a
# different password). Without daemon.env present, the daemon starts,
# tries to auth as `default` with the daemon password, and gets
# AuthenticationFailed on every connection-pool attempt. Streams then
# go uncomsumed, registration hangs, managers silently degrade.
#
# Copy is idempotent: only writes if the destination is missing.
# Operators who hand-edited daemon.env keep their version.
DAEMON_ENV_SRC="${SCRIPT_DIR:-$(dirname "$0")}/../config/daemon.env"
DAEMON_ENV_DST="/etc/geodineum/components/gnode-daemon/daemon.env"
if [ -f "$DAEMON_ENV_SRC" ] && [ ! -f "$DAEMON_ENV_DST" ]; then
    install -d -m 0755 -o gnode -g gnode /etc/geodineum/components/gnode-daemon
    install -m 0640 -o gnode -g gnode "$DAEMON_ENV_SRC" "$DAEMON_ENV_DST"
    echo "✓ Installed daemon.env to $DAEMON_ENV_DST (sets VALKEY_USER=gnode_daemon)"
elif [ -f "$DAEMON_ENV_DST" ]; then
    # Existing daemon.env — verify VALKEY_USER is set, warn if not.
    if grep -qE '^VALKEY_USER=' "$DAEMON_ENV_DST"; then
        echo "  daemon.env exists with VALKEY_USER set"
    else
        echo "⚠️  WARNING: $DAEMON_ENV_DST exists but has no VALKEY_USER line"
        echo "    Daemon will attempt empty-user auth and fail with AuthenticationFailed"
        echo "    Fix: add 'VALKEY_USER=\"gnode_daemon\"' to $DAEMON_ENV_DST"
    fi
elif [ ! -f "$DAEMON_ENV_SRC" ]; then
    echo "⚠️  WARNING: template missing at $DAEMON_ENV_SRC — daemon.env NOT installed"
    echo "    Daemon will attempt empty-user auth and likely fail."
fi

# Node ID: ensure GNODE_NODE_ID is in daemon.env so the daemon registers a
# UNIQUE consumer name (the unit's --node-id reads it). The installer passes
# GNODE_NODE_ID for a constellation join; unset → the unit defaults to "master"
# (correct for the master/standalone node).
if [ -n "${GNODE_NODE_ID:-}" ] && [ -f "$DAEMON_ENV_DST" ]; then
    if grep -qE '^GNODE_NODE_ID=' "$DAEMON_ENV_DST"; then
        sed -i "s|^GNODE_NODE_ID=.*|GNODE_NODE_ID=\"${GNODE_NODE_ID}\"|" "$DAEMON_ENV_DST"
    else
        echo "GNODE_NODE_ID=\"${GNODE_NODE_ID}\"" >> "$DAEMON_ENV_DST"
    fi
    echo "✓ Node ID set to '${GNODE_NODE_ID}' in daemon.env"
fi

# Per-node ValKey identity: point daemon.env at this node's own user where the
# master minted one (constellation expand, bundle V2). daemon.env ships with
# the shared gnode_daemon login, which is correct for a master or a standalone
# install and correct as a fallback for a pre-V2 join.
#
# Both keys move together on purpose: the username and the password file are
# one credential, and setting one without the other is an auth failure at the
# next restart, not a degraded mode.
NODE_IDENTITY_ENV="/etc/geodineum/components/gnode-daemon/node-identity.env"
NODE_PW_FILE="/etc/geodineum/credentials/valkey_node.password"
if [ -r "$NODE_IDENTITY_ENV" ] && [ -r "$NODE_PW_FILE" ] && [ -f "$DAEMON_ENV_DST" ]; then
    # shellcheck disable=SC1090
    . "$NODE_IDENTITY_ENV"
    if [ -n "${VALKEY_NODE_USER:-}" ]; then
        if grep -qE '^VALKEY_USER=' "$DAEMON_ENV_DST"; then
            sed -i "s|^VALKEY_USER=.*|VALKEY_USER=\"${VALKEY_NODE_USER}\"|" "$DAEMON_ENV_DST"
        else
            echo "VALKEY_USER=\"${VALKEY_NODE_USER}\"" >> "$DAEMON_ENV_DST"
        fi
        if grep -qE '^VALKEY_PASSWORD_FILE=' "$DAEMON_ENV_DST"; then
            sed -i "s|^VALKEY_PASSWORD_FILE=.*|VALKEY_PASSWORD_FILE=\"${NODE_PW_FILE}\"|" "$DAEMON_ENV_DST"
        else
            echo "VALKEY_PASSWORD_FILE=\"${NODE_PW_FILE}\"" >> "$DAEMON_ENV_DST"
        fi
        echo "✓ Daemon authenticates as per-node identity '${VALKEY_NODE_USER}'"
    fi
fi

# Reload systemd
echo "Reloading systemd daemon..."
systemctl daemon-reload
echo "✓ Systemd daemon reloaded"

# Enable service
echo "Enabling gNode daemon service..."
systemctl enable gnode-daemon.service
echo "✓ Service enabled (will start on boot)"

# Check if we should start now. Default Y in interactive runs; non-TTY
# (Geodineum installer driver) also gets Y so a fresh install ends with
# a running daemon rather than a "remember to start it" hint that
# operators have to act on out of band.
#
# capture rc instead of letting set -e kill the script on start
# failure. Pre-fix, when the daemon failed to start (any reason — bad
# config, port collision, missing dep), `systemctl start` returned
# non-zero and set -e exited the script BEFORE printing systemctl
# status / journalctl. The operator saw "[FAIL] Services" with no
# diagnostic. Now we ALWAYS print status + a journalctl tail so the
# actual error message reaches the install log, then propagate the
# failure code so Phase 8 reports [FAIL] honestly.
prompt_yn "Start gNode daemon now? (Y/n) " Y
if [[ ! $REPLY =~ ^[Nn]$ ]]; then
    echo "Starting gNode daemon..."
    start_rc=0
    systemctl start gnode-daemon.service || start_rc=$?
    sleep 2

    echo
    echo "Service status:"
    systemctl status gnode-daemon.service --no-pager --lines=0 || true

    if [[ "$start_rc" -ne 0 ]] \
       || ! systemctl is-active --quiet gnode-daemon.service; then
        echo
        echo "❌ gNode daemon failed to start — recent journalctl output:"
        journalctl -u gnode-daemon.service --no-pager -n 40 \
            --since "1 minute ago" 2>&1 || true
        echo
        echo "Manual recovery:"
        echo "  sudo systemctl start gnode-daemon"
        echo "  sudo journalctl -u gnode-daemon -f"
        exit 1
    fi
fi

echo
echo "=== Installation Complete ==="
echo

# Backup service is provisioned by the Geodineum installer (phase_bak),
# which writes /etc/systemd/system/valkey-backup.{service,timer} and wires
# the ExecStart at ${INSTALL_ROOT}/Geodineum-BAK/scripts/backup-valkey.sh.
# gNode/scripts/backup-valkey.sh and gNode/daemon/config/valkey-backup.*
# were deleted in Commits 0.1.f and 0.6.b — this script no longer creates
# backup units. Report current state for the operator and move on.
echo
if systemctl list-unit-files 2>/dev/null | grep -q "valkey-backup.timer"; then
    echo "ℹ️  ValKey backup timer already installed (provisioned by Geodineum installer)."
    systemctl is-active --quiet valkey-backup.timer \
        && echo "  Status: ACTIVE" \
        || echo "  Status: inactive (operator can: sudo systemctl start valkey-backup.timer)"
else
    echo "ℹ️  ValKey backup not installed. Run the Geodineum installer's phase_bak"
    echo "   (sudo ./install.sh --only phase_bak) to provision the timer from"
    echo "   Geodineum-BAK/scripts/backup-valkey.sh."
fi
echo

# Install log rotation for gCore/WordPress logs
GNODE_BAK_DIR="/opt/geodineum/Geodineum-BAK"
LOGROTATE_SRC="$GNODE_BAK_DIR/config/logrotate"
LOG_ARCHIVE_DIR="$GNODE_BAK_DIR/logs"

if [[ -d "$LOGROTATE_SRC" ]]; then
    echo
    if [[ -f "/etc/logrotate.d/gcore" && -f "/etc/logrotate.d/gcore-wp" ]]; then
        echo "ℹ️  Log rotation configs already installed"
    else
        prompt_yn "Install log rotation for gCore/WordPress logs? (Y/n) " Y
        if [[ ! $REPLY =~ ^[Nn]$ ]]; then
            echo "Installing log rotation configs..."

            # Ensure archive directory exists
            mkdir -p "$LOG_ARCHIVE_DIR"

            if [[ -f "$LOGROTATE_SRC/gcore" ]]; then
                cp "$LOGROTATE_SRC/gcore" /etc/logrotate.d/gcore
                chmod 644 /etc/logrotate.d/gcore
                echo "✓ Installed /etc/logrotate.d/gcore"
            fi

            if [[ -f "$LOGROTATE_SRC/gcore-wp" ]]; then
                cp "$LOGROTATE_SRC/gcore-wp" /etc/logrotate.d/gcore-wp
                chmod 644 /etc/logrotate.d/gcore-wp
                echo "✓ Installed /etc/logrotate.d/gcore-wp"
            fi

            # Check for large existing logs
            if [[ -f /var/log/gcore/bootstrap.log ]]; then
                SIZE=$(stat -c%s /var/log/gcore/bootstrap.log 2>/dev/null || echo 0)
                if [[ $SIZE -gt 10485760 ]]; then  # >10MB
                    echo
                    echo "⚠️  Large log detected: /var/log/gcore/bootstrap.log ($(numfmt --to=iec $SIZE))"
                    prompt_yn "Backup and truncate now? (Y/n) " Y
                    if [[ ! $REPLY =~ ^[Nn]$ ]]; then
                        # Backup before truncating
                        TIMESTAMP=$(date +%Y%m%d_%H%M%S)
                        if gzip -c /var/log/gcore/bootstrap.log > "$LOG_ARCHIVE_DIR/bootstrap_${TIMESTAMP}.log.gz"; then
                            echo "✓ Backed up to $LOG_ARCHIVE_DIR/bootstrap_${TIMESTAMP}.log.gz"
                        fi
                        > /var/log/gcore/bootstrap.log
                        chown www-data:www-data /var/log/gcore/bootstrap.log
                        echo "✓ bootstrap.log truncated"
                    fi
                fi
            fi

            echo "✓ Log rotation configured (daily, 7-day retention)"
            echo "  Archives: $LOG_ARCHIVE_DIR"
        fi
    fi
fi

echo
echo "Service management commands:"
echo "  sudo systemctl start gnode-daemon     # Start the daemon"
echo "  sudo systemctl stop gnode-daemon      # Stop the daemon"
echo "  sudo systemctl restart gnode-daemon   # Restart the daemon"
echo "  sudo systemctl status gnode-daemon    # Check service status"
echo "  sudo systemctl enable gnode-daemon    # Enable auto-start on boot"
echo "  sudo systemctl disable gnode-daemon   # Disable auto-start"
echo
echo "Log viewing:"
echo "  sudo journalctl -u gnode-daemon -f           # Follow logs in real-time"
echo "  sudo journalctl -u gnode-daemon -n 100       # Show last 100 lines"
echo "  sudo journalctl -u gnode-daemon --since today # Show today's logs"
echo
echo "Daemon status check:"
echo "  $PROJECT_ROOT/scripts/check-gnode-status.sh  #status"
echo
