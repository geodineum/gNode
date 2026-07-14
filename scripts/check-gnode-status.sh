#!/bin/bash
#
#gNode Daemon Status Checker
# Shows systemd service status, daemon internal status, and ValKey connection
#

set -euo pipefail  # Exit on error, unset vars, and pipe failures

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== gNode Daemon Status Check ===${NC}"
echo

# 1. Check if running as systemd service
echo -e "${BLUE}[1] Systemd Service Status${NC}"
if systemctl is-active --quiet gnode-daemon.service 2>/dev/null; then
    echo -e "${GREEN}✓ gNode daemon service is running${NC}"
    systemctl status gnode-daemon.service --no-pager | head -15
else
    if systemctl cat gnode-daemon.service &>/dev/null; then
        echo -e "${RED}✗ gNode daemon service is installed but not running${NC}"
        echo "  Start with: sudo systemctl start gnode-daemon"
    else
        echo -e "${YELLOW}⚠ gNode daemon service not installed${NC}"
        echo "  Install with: sudo $PROJECT_ROOT/scripts/install-gnode-service.sh"
    fi
fi
echo

# 2. Check for manually running daemon processes
echo -e "${BLUE}[2] Process Status${NC}"
if pgrep -f "gnode-daemon" > /dev/null; then
    echo -e "${GREEN}✓ gNode daemon process found${NC}"
    ps aux | grep "[g]sd-daemon" | grep -v grep
else
    echo -e "${YELLOW}⚠ No gNode daemon process found${NC}"
fi
echo

# 3. Check ValKey connection
echo -e "${BLUE}[3] ValKey Connection${NC}"
if [ -f "$PROJECT_ROOT/.gnode/valkey.password" ]; then
    VALKEY_PASSWORD=$(cat "$PROJECT_ROOT/.gnode/valkey.password")

    # Test connection using REDISCLI_AUTH scoped per-command (password not visible in ps)
    if docker exec -e REDISCLI_AUTH="$VALKEY_PASSWORD" valkey valkey-cli PING 2>/dev/null | grep -q "PONG"; then
        echo -e "${GREEN}✓ ValKey is accessible (Docker)${NC}"
    elif REDISCLI_AUTH="$VALKEY_PASSWORD" valkey-cli PING 2>/dev/null | grep -q "PONG"; then
        echo -e "${GREEN}✓ ValKey is accessible (Native)${NC}"
    else
        echo -e "${RED}✗ Cannot connect to ValKey${NC}"
    fi
else
    echo -e "${RED}✗ ValKey password file not found${NC}"
fi
echo

# 4. Check daemon internal status (if password available)
echo -e "${BLUE}[4] Daemon Internal Status${NC}"
if [ -f "$PROJECT_ROOT/.gnode/valkey.password" ]; then
    VALKEY_PASSWORD=$(cat "$PROJECT_ROOT/.gnode/valkey.password")

    if [ -f "$PROJECT_ROOT/daemon/target/release/gnode-daemon" ]; then
        echo "Querying daemon internals via ValKey..."
        GNODE_REDIS_AUTH="$VALKEY_PASSWORD" "$PROJECT_ROOT/daemon/target/release/gnode-daemon" \
            status 2>&1 || echo -e "${YELLOW}⚠ Daemon status command failed (daemon may not be running)${NC}"
    else
        echo -e "${RED}✗ Daemon binary not found${NC}"
        echo "  Build with: cd $PROJECT_ROOT/daemon && cargo build --release"
    fi
else
    echo -e "${RED}✗ Cannot check internal status (no password file)${NC}"
fi
echo

# 5. Check ValKey streams
echo -e "${BLUE}[5] ValKey Streams Status${NC}"
if [ -f "$PROJECT_ROOT/.gnode/valkey.password" ]; then
    VALKEY_PASSWORD=$(cat "$PROJECT_ROOT/.gnode/valkey.password")

    # Helper function using REDISCLI_AUTH scoped per-command (password not visible in ps)
    valkey_cmd() {
        if docker ps | grep -q valkey; then
            docker exec -e REDISCLI_AUTH="$VALKEY_PASSWORD" valkey valkey-cli "$@" 2>/dev/null
        else
            REDISCLI_AUTH="$VALKEY_PASSWORD" valkey-cli "$@" 2>/dev/null
        fi
    }

    # Check unified stream
    echo "Unified Stream ({default}:gnode:unified:default):"
    valkey_cmd XINFO STREAM '{default}:gnode:unified:default' | grep -E "length|first-entry|last-entry" || echo "  Stream not found or empty"

    echo
    echo "Health Stream ({default}:gnode:health):"
    valkey_cmd XINFO STREAM '{default}:gnode:health' | grep -E "length|first-entry|last-entry" || echo "  Stream not found or empty"

    echo
    echo "Broadcast Stream ({default}:gnode:broadcast:global):"
    valkey_cmd XINFO STREAM '{default}:gnode:broadcast:global' | grep -E "length|first-entry|last-entry" || echo "  Stream not found or empty"
else
    echo -e "${RED}✗ Cannot check streams (no password file)${NC}"
fi
echo

# 6. Check ValKey functions
echo -e "${BLUE}[6] ValKey Functions Status${NC}"
if [ -f "$PROJECT_ROOT/.gnode/valkey.password" ]; then
    VALKEY_PASSWORD=$(cat "$PROJECT_ROOT/.gnode/valkey.password")

    # Helper function using REDISCLI_AUTH scoped per-command (password not visible in ps)
    valkey_func_cmd() {
        if docker ps | grep -q valkey; then
            docker exec -e REDISCLI_AUTH="$VALKEY_PASSWORD" valkey valkey-cli "$@" 2>/dev/null
        else
            REDISCLI_AUTH="$VALKEY_PASSWORD" valkey-cli "$@" 2>/dev/null
        fi
    }

    # Count loaded functions
    FUNC_COUNT=$(valkey_func_cmd FUNCTION LIST | grep -c "library_name" || echo "0")

    if [ "$FUNC_COUNT" -gt 0 ]; then
        echo -e "${GREEN}✓ $FUNC_COUNT ValKey function libraries loaded${NC}"

        # Test a basic function
        if valkey_func_cmd FCALL GNODE_TEST_HELLO 0 | grep -q "Hello"; then
            echo -e "${GREEN}✓ Test function GNODE_TEST_HELLO works${NC}"
        else
            echo -e "${YELLOW}⚠ Test function failed or not found${NC}"
        fi
    else
        echo -e "${RED}✗ No ValKey functions loaded${NC}"
        echo "  Load with: $PROJECT_ROOT/scripts/load-valkey-functions.sh"
    fi
else
    echo -e "${RED}✗ Cannot check functions (no password file)${NC}"
fi
echo

# 7. Resource usage
echo -e "${BLUE}[7] Resource Usage${NC}"
if pgrep -f "gnode-daemon" > /dev/null; then
    PID=$(pgrep -f "gnode-daemon" | head -1)
    echo "PID: $PID"
    ps -p $PID -o pid,ppid,cmd,%mem,%cpu,rss,vsz,etime
else
    echo -e "${YELLOW}⚠ Daemon not running${NC}"
fi
echo

# 8. Recent logs (if systemd service)
echo -e "${BLUE}[8] Recent Logs${NC}"
if systemctl is-active --quiet gnode-daemon.service 2>/dev/null; then
    echo "Last 10 log entries:"
    journalctl -u gnode-daemon.service -n 10 --no-pager
else
    echo -e "${YELLOW}⚠ Service not running via systemd${NC}"
fi
echo

# Summary
echo -e "${BLUE}=== Summary ===${NC}"
RUNNING=false

if systemctl is-active --quiet gnode-daemon.service 2>/dev/null; then
    echo -e "${GREEN}✓ gNode daemon is running as a systemd service${NC}"
    RUNNING=true
elif pgrep -f "gnode-daemon" > /dev/null; then
    echo -e "${YELLOW}⚠ gNode daemon is running manually (not as a service)${NC}"
    RUNNING=true
else
    echo -e "${RED}✗ gNode daemon is not running${NC}"
fi

if [ "$RUNNING" = true ]; then
    echo
    echo "Quick commands:"
    echo "  sudo systemctl status gnode-daemon    # Service status"
    echo "  sudo systemctl restart gnode-daemon   # Restart"
    echo "  sudo journalctl -u gnode-daemon -f    # Follow logs"
else
    echo
    echo "To start the daemon:"
    echo "  sudo systemctl start gnode-daemon     # If installed as service"
    echo "  $PROJECT_ROOT/scripts/start-gnode.sh  # Manual start"
fi
echo
