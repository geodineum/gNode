#!/bin/bash
# Secure ValKey CLI Wrapper
# Uses REDISCLI_AUTH environment variable instead of -a flag
# This prevents the password from appearing in process lists

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Credential directories (checked in order: centralized, standard, legacy)
CENTRALIZED_CREDS="/etc/geodineum/credentials"
STANDARD_CREDS="$PROJECT_ROOT/.gnode"
LEGACY_CREDS="/opt/gNode/.gnode"

# Canonical ecosystem config loader (installed by Geodineum installer).
GEODINEUM_LIB="${GEODINEUM_LIB:-/usr/local/lib/geodineum}"
if [ ! -r "$GEODINEUM_LIB/bootstrap-loader.sh" ]; then
    echo "FATAL: $GEODINEUM_LIB/bootstrap-loader.sh not found. Run installer first." >&2
    exit 1
fi
# shellcheck source=/usr/local/lib/geodineum/bootstrap-loader.sh
source "$GEODINEUM_LIB/bootstrap-loader.sh"
load_ecosystem_config

# Host + port from the ecosystem config loaded above. VALKEY_HOST is the
# master's VPN address on every node that does not own the ValKey; defaulting
# it away silently pointed every caller at a localhost that isn't there.
VALKEY_HOST="${VALKEY_HOST:-127.0.0.1}"
VALKEY_PORT="${VALKEY_PORT:-47445}"

# Determine which user to authenticate as (daemon or client)
# Default to client for most operations
VALKEY_USER="${VALKEY_USER:-gnode_client}"

# Resolve the DAEMON TIER to this node's own identity.
#
# `gnode_daemon` names a privilege tier, not a login. Where the master has
# minted this node its own user (constellation expand, bundle V2), that user
# IS the daemon tier here and every caller asking for gnode_daemon means it.
# Resolving centrally keeps ~56 call sites across the ecosystem correct
# without any of them knowing a node identity exists.
#
# Falls through untouched when no identity was minted: masters, pre-V2 joins,
# and standalone installs all keep authenticating as the shared login.
NODE_IDENTITY_ENV="/etc/geodineum/components/gnode-daemon/node-identity.env"
if [ "$VALKEY_USER" = "gnode_daemon" ] && [ -r "$NODE_IDENTITY_ENV" ]; then
    # shellcheck disable=SC1090
    . "$NODE_IDENTITY_ENV"
    if [ -n "${VALKEY_NODE_USER:-}" ] && [ -r "${CENTRALIZED_CREDS}/valkey_node.password" ]; then
        VALKEY_USER="$VALKEY_NODE_USER"
        VALKEY_NODE_IDENTITY_RESOLVED="valkey_node.password"
    fi
fi

# Determine password filename based on user
if [ -n "${VALKEY_NODE_IDENTITY_RESOLVED:-}" ]; then
    PASSWORD_FILENAME="$VALKEY_NODE_IDENTITY_RESOLVED"
elif [ "$VALKEY_USER" = "gnode_daemon" ]; then
    PASSWORD_FILENAME="valkey_daemon.password"
elif [ "$VALKEY_USER" = "gnode_client" ]; then
    PASSWORD_FILENAME="valkey_client.password"
elif [[ "$VALKEY_USER" == gnode_client_* ]]; then
    # Per-site client user (e.g., gnode_client_my_app)
    PASSWORD_FILENAME="valkey_${VALKEY_USER#gnode_}.password"
else
    # Fallback to legacy password
    PASSWORD_FILENAME="valkey.password"
fi

# Search for password file in credential directories (order: centralized, standard, legacy)
PASSWORD_FILE=""
for creds_dir in "$CENTRALIZED_CREDS" "$STANDARD_CREDS" "$LEGACY_CREDS"; do
    if [ -f "$creds_dir/$PASSWORD_FILENAME" ]; then
        PASSWORD_FILE="$creds_dir/$PASSWORD_FILENAME"
        break
    fi
done

if [ -n "$PASSWORD_FILE" ] && [ -f "$PASSWORD_FILE" ]; then
    VALKEY_PASSWORD=$(cat "$PASSWORD_FILE")
else
    echo "Error: ValKey password not found. Searched for $PASSWORD_FILENAME in:" >&2
    echo "  - $CENTRALIZED_CREDS" >&2
    echo "  - $STANDARD_CREDS" >&2
    echo "  - $LEGACY_CREDS" >&2
    exit 1
fi

# Detect valkey-cli location
VALKEY_CLI=$(which valkey-cli 2>/dev/null || echo "/usr/local/bin/valkey-cli")

if [ ! -x "$VALKEY_CLI" ]; then
    echo "Error: valkey-cli not found or not executable" >&2
    exit 1
fi

# Execute valkey-cli with REDISCLI_AUTH environment variable
# Use --user for ACL authentication (ValKey 7.2+)
# This is the recommended secure method that doesn't expose password in ps
REDISCLI_AUTH="$VALKEY_PASSWORD" "$VALKEY_CLI" -h "$VALKEY_HOST" -p "$VALKEY_PORT" --user "$VALKEY_USER" "$@"
