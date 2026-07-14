#!/bin/bash
# =============================================================================
# Relay Routing Validation Script
# =============================================================================
# Validates that gNode inter-service relay routing works end-to-end:
#   1. Source service posts a command to its OWN unified stream with _rt target
#   2. gNode daemon picks it up, checks policy, translates format if needed
#   3. gNode posts the command to the TARGET's unified stream
#   4. Target service reads the relayed command from its own stream
#
# Usage:
#   ./validate-relay-routing.sh <source_site_id> <target_site_id>
# =============================================================================

set -euo pipefail

# --- Configuration ---
VALKEY_PORT="${VALKEY_PORT:-47445}"
CREDS_DIR="/etc/geodineum/credentials"
VALKEY_CLI="$(which valkey-cli 2>/dev/null || echo "/usr/local/bin/valkey-cli")"

SOURCE_SITE="${1:?usage: $0 <source_site_id> <target_site_id>}"
TARGET_SITE="${2:?usage: $0 <source_site_id> <target_site_id>}"
TIMEOUT_SEC=10

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

pass() { echo -e "  ${GREEN}PASS${NC}  $1"; }
fail() { echo -e "  ${RED}FAIL${NC}  $1"; FAILURES=$((FAILURES + 1)); }
info() { echo -e "  ${CYAN}INFO${NC}  $1"; }
warn() { echo -e "  ${YELLOW}WARN${NC}  $1"; }

FAILURES=0
TESTS=0

# --- Helper: run valkey-cli as a specific user ---
vcli() {
    local user="$1"; shift
    local password_file

    if [ "$user" = "gnode_daemon" ]; then
        password_file="$CREDS_DIR/valkey_daemon.password"
    else
        password_file="$CREDS_DIR/valkey_${user#gnode_}.password"
    fi

    if [ ! -r "$password_file" ]; then
        echo "ERROR: Cannot read $password_file" >&2
        return 1
    fi

    REDISCLI_AUTH="$(cat "$password_file")" "$VALKEY_CLI" \
        -p "$VALKEY_PORT" --user "$user" "$@"
}

# =============================================================================
echo ""
echo "================================================================"
echo "  gNode Relay Routing Validation"
echo "  Source: $SOURCE_SITE  →  Target: $TARGET_SITE"
echo "================================================================"
echo ""

# --- Phase 1: Prerequisites ---
echo "Phase 1: Prerequisites"
echo "--------------------------------------------------------------"

TESTS=$((TESTS + 1))
if pgrep -f gnode-daemon > /dev/null 2>&1; then
    pass "gNode daemon is running"
else
    fail "gNode daemon is NOT running"
    echo "  Cannot validate relay without the daemon. Start it with:"
    echo "    systemctl start gnode-daemon"
    exit 1
fi

TESTS=$((TESTS + 1))
if "$VALKEY_CLI" -p "$VALKEY_PORT" PING > /dev/null 2>&1; then
    pass "ValKey is reachable on port $VALKEY_PORT"
else
    fail "ValKey is NOT reachable on port $VALKEY_PORT"
    exit 1
fi

# Check source credentials
TESTS=$((TESTS + 1))
SOURCE_CRED="$CREDS_DIR/valkey_client_${SOURCE_SITE}.password"
if [ -r "$SOURCE_CRED" ]; then
    pass "Source credentials readable: $SOURCE_CRED"
else
    fail "Source credentials not readable: $SOURCE_CRED"
    exit 1
fi

# Check target credentials
TESTS=$((TESTS + 1))
TARGET_CRED="$CREDS_DIR/valkey_client_${TARGET_SITE}.password"
if [ -r "$TARGET_CRED" ]; then
    pass "Target credentials readable: $TARGET_CRED"
else
    fail "Target credentials not readable: $TARGET_CRED"
    exit 1
fi

# Authenticate source
TESTS=$((TESTS + 1))
if vcli "gnode_client_${SOURCE_SITE}" PING 2>/dev/null | grep -q "PONG"; then
    pass "Source auth OK: gnode_client_${SOURCE_SITE}"
else
    fail "Source auth FAILED: gnode_client_${SOURCE_SITE}"
    exit 1
fi

# Authenticate target
TESTS=$((TESTS + 1))
if vcli "gnode_client_${TARGET_SITE}" PING 2>/dev/null | grep -q "PONG"; then
    pass "Target auth OK: gnode_client_${TARGET_SITE}"
else
    fail "Target auth FAILED: gnode_client_${TARGET_SITE}"
    exit 1
fi

echo ""

# --- Phase 2: Discover environments ---
echo "Phase 2: Environment Discovery"
echo "--------------------------------------------------------------"

# Detect source environment from config.yaml
SOURCE_CONFIG=""
for config_path in \
    "/etc/geodineum/sites/${SOURCE_SITE}/config.yaml" \
    "/etc/geodineum/services/${SOURCE_SITE}/config.yaml" \
    "/var/www/${SOURCE_SITE//_/.}/.geodineum/config.yaml" \
    "/var/www/${SOURCE_SITE//_//}/.geodineum/config.yaml"; do
    if [ -f "$config_path" ]; then
        SOURCE_CONFIG="$config_path"
        break
    fi
done

# Try common WordPress paths
if [ -z "$SOURCE_CONFIG" ]; then
    for www_dir in /var/www/*/; do
        if [ -f "${www_dir}.geodineum/config.yaml" ]; then
            site_in_config=$(grep -m1 'site_id:' "${www_dir}.geodineum/config.yaml" 2>/dev/null | awk '{print $2}' | tr -d '"')
            if [ "$site_in_config" = "$SOURCE_SITE" ]; then
                SOURCE_CONFIG="${www_dir}.geodineum/config.yaml"
                break
            fi
        fi
    done
fi

# For services, check home directories
if [ -z "$SOURCE_CONFIG" ]; then
    for svc_dir in /home/*/gh/*/; do
        if [ -f "${svc_dir}.geodineum/config.yaml" ]; then
            site_in_config=$(grep -m1 'site_id:' "${svc_dir}.geodineum/config.yaml" 2>/dev/null | awk '{print $2}' | tr -d '"')
            if [ "$site_in_config" = "$SOURCE_SITE" ]; then
                SOURCE_CONFIG="${svc_dir}.geodineum/config.yaml"
                break
            fi
        fi
    done
fi

if [ -n "$SOURCE_CONFIG" ]; then
    SOURCE_ENV=$(grep -A1 'environment:' "$SOURCE_CONFIG" | grep 'active:' | awk '{print $2}' | tr -d '"')
    info "Source config: $SOURCE_CONFIG"
else
    SOURCE_ENV="production"
    warn "Source config not found, assuming environment: production"
fi

# Detect target environment
TARGET_CONFIG=""
for config_path in \
    "/etc/geodineum/sites/${TARGET_SITE}/config.yaml" \
    "/etc/geodineum/services/${TARGET_SITE}/config.yaml"; do
    if [ -f "$config_path" ]; then
        TARGET_CONFIG="$config_path"
        break
    fi
done

if [ -z "$TARGET_CONFIG" ]; then
    for search_dir in /var/www/*/ /home/*/gh/*/; do
        if [ -f "${search_dir}.geodineum/config.yaml" ]; then
            site_in_config=$(grep -m1 'site_id:' "${search_dir}.geodineum/config.yaml" 2>/dev/null | awk '{print $2}' | tr -d '"')
            if [ "$site_in_config" = "$TARGET_SITE" ]; then
                TARGET_CONFIG="${search_dir}.geodineum/config.yaml"
                break
            fi
        fi
    done
fi

if [ -n "$TARGET_CONFIG" ]; then
    TARGET_ENV=$(grep -A1 'environment:' "$TARGET_CONFIG" | grep 'active:' | awk '{print $2}' | tr -d '"')
    info "Target config: $TARGET_CONFIG"
else
    TARGET_ENV="production"
    warn "Target config not found, assuming environment: production"
fi

TESTS=$((TESTS + 1))
if [ "$SOURCE_ENV" = "$TARGET_ENV" ]; then
    pass "Environments match: $SOURCE_ENV (DTAP isolation preserved)"
else
    fail "Environment mismatch: source=$SOURCE_ENV, target=$TARGET_ENV"
    echo "  Relay enforces DTAP isolation — source and target must share the same environment."
    echo "  Use --source and --target to pick two services in the same environment."
    exit 1
fi

SOURCE_STREAM="{${SOURCE_SITE}}:gnode:unified:${SOURCE_ENV}"
TARGET_STREAM="{${TARGET_SITE}}:gnode:unified:${TARGET_ENV}"

# Also check for old-format (braceless) streams as a diagnostic
SOURCE_STREAM_OLD="${SOURCE_SITE}:gnode:unified:${SOURCE_ENV}"
TARGET_STREAM_OLD="${TARGET_SITE}:gnode:unified:${TARGET_ENV}"

info "Source stream: $SOURCE_STREAM"
info "Target stream: $TARGET_STREAM"
echo ""

# --- Phase 3: Check streams exist ---
echo "Phase 3: Stream Verification"
echo "--------------------------------------------------------------"

TESTS=$((TESTS + 1))
SOURCE_EXISTS=$(vcli "gnode_client_${SOURCE_SITE}" EXISTS "$SOURCE_STREAM" 2>/dev/null || echo "0")
if [ "$SOURCE_EXISTS" = "1" ]; then
    pass "Source stream exists: $SOURCE_STREAM"
else
    warn "Source stream does not exist yet (will be created on first XADD)"
fi

TESTS=$((TESTS + 1))
TARGET_EXISTS=$(vcli "gnode_client_${TARGET_SITE}" EXISTS "$TARGET_STREAM" 2>/dev/null || echo "0")
if [ "$TARGET_EXISTS" = "1" ]; then
    pass "Target stream exists: $TARGET_STREAM"
else
    warn "Target stream does not exist yet (will be created by relay)"
fi

echo ""

# --- Phase 4: Record baseline ---
echo "Phase 4: Baseline"
echo "--------------------------------------------------------------"

# Get the last message ID on the target stream (so we know where to read from)
LAST_TARGET_ID=$(vcli "gnode_client_${TARGET_SITE}" XREVRANGE "$TARGET_STREAM" "+" "-" COUNT 1 2>/dev/null | head -1 || echo "0-0")
if [ -z "$LAST_TARGET_ID" ] || [ "$LAST_TARGET_ID" = "" ]; then
    LAST_TARGET_ID="0-0"
fi
info "Target stream last ID: $LAST_TARGET_ID"

# Generate a unique test command ID for correlation
TEST_CMD_ID="relay-test-$(date +%s%N | head -c 16)"
info "Test command ID: $TEST_CMD_ID"
echo ""

# --- Phase 5: Send relay command ---
echo "Phase 5: Send Relay Command"
echo "--------------------------------------------------------------"
echo "  Posting to source stream with _rt=$TARGET_SITE..."

TESTS=$((TESTS + 1))
XADD_RESULT=$(vcli "gnode_client_${SOURCE_SITE}" XADD "$SOURCE_STREAM" "*" \
    "t" "c" \
    "i" "$TEST_CMD_ID" \
    "c" "ping" \
    "p" "{\"relay_test\":true,\"timestamp\":$(date +%s)}" \
    "ss" "$SOURCE_SITE" \
    "sn" "relay_validator" \
    "_rt" "$TARGET_SITE" \
    2>/dev/null)

if [ -n "$XADD_RESULT" ] && [ "$XADD_RESULT" != "" ]; then
    pass "XADD succeeded: message ID $XADD_RESULT"
else
    fail "XADD to source stream failed"
    exit 1
fi

echo ""

# --- Phase 6: Wait for relay ---
echo "Phase 6: Wait for Relay Delivery (${TIMEOUT_SEC}s timeout)"
echo "--------------------------------------------------------------"

TESTS=$((TESTS + 1))
RELAYED=false
RELAY_MSG_ID=""
RELAY_FIELDS=""
ELAPSED=0

while [ $ELAPSED -lt $TIMEOUT_SEC ]; do
    # Read new messages on target stream after our baseline
    NEW_MSGS=$(vcli "gnode_client_${TARGET_SITE}" XRANGE "$TARGET_STREAM" "($LAST_TARGET_ID" "+" 2>/dev/null || echo "")

    if echo "$NEW_MSGS" | grep -q "$TEST_CMD_ID"; then
        RELAYED=true
        # Extract the message ID (first line of matching entry)
        RELAY_MSG_ID=$(echo "$NEW_MSGS" | grep -B1 "$TEST_CMD_ID" | head -1)
        RELAY_FIELDS="$NEW_MSGS"
        break
    fi

    sleep 0.5
    ELAPSED=$((ELAPSED + 1))
done

if $RELAYED; then
    pass "Relay delivered! Message found on target stream"
    echo ""
    echo "  Relayed message contents:"
    echo "  -------------------------"

    # Parse and display the relayed message fields
    echo "$RELAY_FIELDS" | while IFS= read -r line; do
        # Skip empty lines
        [ -z "$line" ] && continue
        echo "    $line"
    done

    echo ""

    # Verify key relay properties
    TESTS=$((TESTS + 1))
    if echo "$RELAY_FIELDS" | grep -q "relay_test"; then
        pass "Parameters preserved in relay"
    else
        fail "Parameters missing from relayed message"
    fi

    TESTS=$((TESTS + 1))
    if echo "$RELAY_FIELDS" | grep -q "_rt"; then
        fail "_rt field NOT cleared (risk of infinite relay loop)"
    else
        pass "_rt field cleared (no re-relay risk)"
    fi

    TESTS=$((TESTS + 1))
    if echo "$RELAY_FIELDS" | grep -q "_rr"; then
        pass "_rr field set (response routing configured)"
        RR_VALUE=$(echo "$RELAY_FIELDS" | grep -A1 "_rr" | tail -1 | tr -d ' ')
        info "  Response will route to: $RR_VALUE"
    else
        warn "_rr field not found (response routing may not work)"
    fi
else
    fail "Relay NOT delivered within ${TIMEOUT_SEC}s"
    echo ""
    echo "  Troubleshooting:"
    echo "    1. Check daemon logs: journalctl -u gnode-daemon --since '2 min ago'"
    echo "    2. Verify daemon processes source stream:"
    echo "       XINFO GROUPS $SOURCE_STREAM"
    echo "    3. Check relay policy:"
    echo "       Send 'relay_policy_list' command via unified stream"
    echo "    4. Verify target site is discoverable:"
    echo "       Check discovery-paths.conf includes target service"
fi

echo ""

# --- Phase 7: Check relay telemetry ---
echo "Phase 7: Relay Telemetry"
echo "--------------------------------------------------------------"

# Read relay telemetry (requires daemon ACL — may fail with client creds)
# We try with the source client first, fall back gracefully
TELEM_KEY="{geodineum}:gnode:telemetry:relay"
PAIR_KEY="${SOURCE_SITE}:${TARGET_SITE}"

# Try reading with source client (may not have access to telemetry key)
TELEM_DATA=$(vcli "gnode_client_${SOURCE_SITE}" HGET "$TELEM_KEY" "$PAIR_KEY" 2>/dev/null || echo "")

if [ -n "$TELEM_DATA" ] && [ "$TELEM_DATA" != "" ]; then
    info "Telemetry for ${SOURCE_SITE} → ${TARGET_SITE}:"
    echo "    $TELEM_DATA"
else
    info "Telemetry not readable with client credentials (expected — requires daemon ACL)"
    info "Check manually: HGETALL $TELEM_KEY"
fi

echo ""

# --- Phase 8: Check relay ack on source stream ---
echo "Phase 8: Relay Acknowledgment"
echo "--------------------------------------------------------------"

TESTS=$((TESTS + 1))
# Read recent messages on source stream for the relay ack
ACK_MSGS=$(vcli "gnode_client_${SOURCE_SITE}" XRANGE "$SOURCE_STREAM" "($XADD_RESULT" "+" 2>/dev/null || echo "")

if echo "$ACK_MSGS" | grep -q "relayed"; then
    pass "Relay acknowledgment received on source stream"
    if echo "$ACK_MSGS" | grep -q "$TARGET_SITE"; then
        info "  Ack confirms target: $TARGET_SITE"
    fi
else
    warn "No relay ack found (may not have flushed yet or may require different read pattern)"
fi

echo ""

# --- Summary ---
echo "================================================================"
echo "  Results"
echo "================================================================"

PASSED=$((TESTS - FAILURES))
if [ $FAILURES -eq 0 ]; then
    echo -e "  ${GREEN}ALL $TESTS TESTS PASSED${NC}"
    echo ""
    echo "  Relay routing is operational:"
    echo "    $SOURCE_SITE → gNode (policy + format translation) → $TARGET_SITE"
else
    echo -e "  ${RED}$FAILURES of $TESTS tests FAILED${NC}"
fi

echo ""
echo "  Stream flow validated:"
echo "    Source: $SOURCE_STREAM"
echo "    Target: $TARGET_STREAM"
echo "    Command: ping (ID: $TEST_CMD_ID)"
echo "    Environment: $SOURCE_ENV"
echo ""

exit $FAILURES
