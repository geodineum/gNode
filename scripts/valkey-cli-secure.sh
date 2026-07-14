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

# Use port from environment or default to 47445 (standardized port)
VALKEY_PORT="${VALKEY_PORT:-47445}"

# Determine which user to authenticate as (daemon or client)
# Default to client for most operations
VALKEY_USER="${VALKEY_USER:-gnode_client}"

# Determine password filename based on user
if [ "$VALKEY_USER" = "gnode_daemon" ]; then
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
REDISCLI_AUTH="$VALKEY_PASSWORD" "$VALKEY_CLI" -p "$VALKEY_PORT" --user "$VALKEY_USER" "$@"
