#!/bin/bash
# Flush ValKey database - useful for tests and development

set -euo pipefail  # Exit on error, unset vars, and pipe failures

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Check if valkey-gnode.service is running (systemd native)
if ! systemctl is-active --quiet valkey-gnode.service; then
    echo -e "${RED}[ERROR]${NC} valkey-gnode.service is not running"
    echo -e "${YELLOW}[INFO]${NC} Start it with: sudo systemctl start valkey-gnode"
    exit 1
fi

# Confirm flush (unless --force flag is provided)
if [ "$1" != "--force" ]; then
    echo -e "${YELLOW}[WARNING]${NC} This will delete ALL data in ValKey database!"
    read -p "Are you sure you want to continue? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Aborted."
        exit 0
    fi
fi

# Flush the database (using valkey-cli-secure.sh wrapper with ACL auth)
echo -e "${GREEN}[INFO]${NC} Flushing ValKey database..."

# Use gnode_daemon user for administrative operations
VALKEY_USER=gnode_daemon "$SCRIPT_DIR/valkey-cli-secure.sh" FLUSHALL

if [ $? -eq 0 ]; then
    echo -e "${GREEN}[SUCCESS]${NC} ValKey database flushed successfully"

    # Show database size to confirm
    DBSIZE=$(VALKEY_USER=gnode_daemon "$SCRIPT_DIR/valkey-cli-secure.sh" DBSIZE)
    echo -e "${GREEN}[INFO]${NC} Database size: $DBSIZE keys"

    # Reload runtime environment (functions persist, but streams need recreation)
    echo -e "${GREEN}[INFO]${NC} Reloading gNode runtime environment..."
    "$SCRIPT_DIR/reload-gnode.sh"
else
    echo -e "${RED}[ERROR]${NC} Failed to flush ValKey database"
    exit 1
fi
