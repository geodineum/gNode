#!/bin/bash
#
# gNode Diagnostic Tool
#
# This script runsdiagnostics on the gNode daemon and client,
# helping to identify and resolve issues in production environments.
#

# Configuration
GNODE_DIR="${GNODE_DIR:-/opt/gNode}"
LOG_DIR="${GNODE_LOG_DIR:-${GNODE_DIR}/logs}"
LOG_FILE="${GNODE_LOG_FILE:-${LOG_DIR}/daemon.log}"
PID_FILE="${GNODE_PID_FILE:-${LOG_DIR}/daemon.pid}"
DAEMON_BIN="${GNODE_DAEMON_BIN:-${GNODE_DIR}/daemon/target/release/gnode-daemon}"
DIAGNOSTICS_LOG="${LOG_DIR}/diagnostics_$(date +%Y%m%d_%H%M%S).log"

# ValKey configuration
VALKEY_HOST="${VALKEY_HOST:-127.0.0.1}"
VALKEY_PORT="${VALKEY_PORT:-47445}"

# gNode configuration
SITE_ID="${SITE_ID:-default}"
NODE_ID="${NODE_ID:-default}"
STREAM_PREFIX="${STREAM_PREFIX:-gnode}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Make sure log directory exists
mkdir -p "$LOG_DIR"

echo "gNode Diagnostic Tool"
echo "==================="
echo
echo "Running diagnostics at $(date)"
echo "Results will be saved to: $DIAGNOSTICS_LOG"
echo

# Start logging to file
exec > >(tee -a "$DIAGNOSTICS_LOG") 2>&1

# Check if ValKey is running
echo -e "${BLUE}[1/7] Checking ValKey status${NC}"
if ${VALKEY_CLI:-valkey-cli} -h "$VALKEY_HOST" -p "$VALKEY_PORT" ping >/dev/null 2>&1; then
    echo -e "${GREEN}✓ ValKey is running on $VALKEY_HOST:$VALKEY_PORT${NC}"
else
    echo -e "${RED}✗ ValKey is not running or not accessible on $VALKEY_HOST:$VALKEY_PORT${NC}"
    echo "This is a critical error - gNode requires ValKey to function"
    echo "Please ensure ValKey is running and accessible"
    echo -e "${YELLOW}Suggestion: Check 'systemctl status valkey-gnode' or start with 'systemctl start valkey-gnode'${NC}"
fi

# Check daemon binary
echo -e "\n${BLUE}[2/7] Checking gNode daemon binary${NC}"
if [ -f "$DAEMON_BIN" ]; then
    echo -e "${GREEN}✓ Daemon binary found at $DAEMON_BIN${NC}"
    if [ -x "$DAEMON_BIN" ]; then
        echo -e "${GREEN}✓ Daemon binary is executable${NC}"
    else
        echo -e "${YELLOW}! Daemon binary is not executable${NC}"
        echo -e "${YELLOW}Suggestion: Run 'chmod +x $DAEMON_BIN'${NC}"
    fi
    
    echo -e "\nDaemon version info:"
    "$DAEMON_BIN" --version
else
    echo -e "${RED}✗ Daemon binary not found at $DAEMON_BIN${NC}"
    echo -e "${YELLOW}Suggestion: Build the daemon with 'cargo build --release' in the daemon directory${NC}"
fi

# Check if daemon is running
echo -e "\n${BLUE}[3/7] Checking daemon process${NC}"
if [ -f "$PID_FILE" ]; then
    pid=$(cat "$PID_FILE")
    if ps -p "$pid" > /dev/null; then
        echo -e "${GREEN}✓ Daemon is running with PID $pid${NC}"
        
        uptime=$(ps -o etime= -p "$pid")
        echo -e "${GREEN}✓ Daemon uptime: $uptime${NC}"
        
        # Check resources
        cpu=$(ps -p "$pid" -o %cpu | tail -n 1 | tr -d ' ')
        mem=$(ps -p "$pid" -o %mem | tail -n 1 | tr -d ' ')
        echo -e "${CYAN}• CPU usage: $cpu%${NC}"
        echo -e "${CYAN}• Memory usage: $mem%${NC}"
        
        # Check open files
        fd_count=$(ls -l /proc/$pid/fd | wc -l)
        echo -e "${CYAN}• Open file descriptors: $fd_count${NC}"
        
        # Check threads
        thread_count=$(ps -L -p "$pid" | wc -l)
        thread_count=$((thread_count - 1)) # Subtract header
        echo -e "${CYAN}• Thread count: $thread_count${NC}"
    else
        echo -e "${RED}✗ Daemon process not running, but PID file exists${NC}"
        echo -e "${YELLOW}Suggestion: PID file at $PID_FILE is stale and should be removed${NC}"
    fi
else
    echo -e "${YELLOW}! Daemon PID file not found at $PID_FILE${NC}"
    echo -e "${YELLOW}Suggestion: Start the daemon using run-gnode-daemon.sh${NC}"
    
    # Check if process is running without PID file
    daemon_pids=$(pgrep -f "gnode-daemon")
    if [ -n "$daemon_pids" ]; then
        echo -e "${CYAN}• Found running gNode daemon processes without PID files:${NC}"
        for pid in $daemon_pids; do
            cmd=$(ps -p "$pid" -o cmd=)
            echo -e "${CYAN}  - PID $pid: $cmd${NC}"
        done
    else
        echo -e "${RED}✗ No running gNode daemon processes found${NC}"
    fi
fi

# Check daemon log file
echo -e "\n${BLUE}[4/7] Checking daemon logs${NC}"
if [ -f "$LOG_FILE" ]; then
    log_size=$(du -h "$LOG_FILE" | cut -f1)
    echo -e "${GREEN}✓ Log file found at $LOG_FILE (size: $log_size)${NC}"
    
    # Check last modification time
    last_modified=$(stat -c %y "$LOG_FILE")
    echo -e "${CYAN}• Last modified: $last_modified${NC}"
    
    # Count errors and warnings
    error_count=$(grep -c "ERROR" "$LOG_FILE")
    warn_count=$(grep -c "WARN" "$LOG_FILE")
    echo -e "${CYAN}• Errors: $error_count${NC}"
    echo -e "${CYAN}• Warnings: $warn_count${NC}"
    
    # Show recent errors
    if [ "$error_count" -gt 0 ]; then
        echo -e "\n${YELLOW}Recent errors:${NC}"
        grep "ERROR" "$LOG_FILE" | tail -n 5
    fi
    
    # Show recent log entries
    echo -e "\n${CYAN}Last 10 log entries:${NC}"
    tail -n 10 "$LOG_FILE"
else
    echo -e "${YELLOW}! Log file not found at $LOG_FILE${NC}"
    echo -e "${YELLOW}Suggestion: Check if daemon is running and logging properly${NC}"
fi

# Check ValKey streams
echo -e "\n${BLUE}[5/7] Checking ValKey streams${NC}"
stream_pattern="{$SITE_ID}:$STREAM_PREFIX:stream:*"
streams=$(${VALKEY_CLI:-valkey-cli} -h "$VALKEY_HOST" -p "$VALKEY_PORT" keys "$stream_pattern" | sort)

if [ -z "$streams" ]; then
    echo -e "${YELLOW}! No gNode streams found with pattern: $stream_pattern${NC}"
    echo -e "${YELLOW}Suggestion: Ensure the daemon has been initialized with correct site_id and stream_prefix${NC}"
else
    echo -e "${GREEN}✓ Found $(echo "$streams" | wc -l) gNode streams${NC}"
    
    for stream in $streams; do
        # Get stream length
        length=$(${VALKEY_CLI:-valkey-cli} -h "$VALKEY_HOST" -p "$VALKEY_PORT" xlen "$stream")
        
        # Check if command or response stream
        if [[ "$stream" == *":commands" ]]; then
            stream_type="Command"
        elif [[ "$stream" == *":responses" ]]; then
            stream_type="Response"
        else
            stream_type="Unknown"
        fi
        
        echo -e "${CYAN}• $stream_type stream: $stream (messages: $length)${NC}"
        
        # Check consumer groups
        groups=$(${VALKEY_CLI:-valkey-cli} -h "$VALKEY_HOST" -p "$VALKEY_PORT" xinfo groups "$stream" 2>/dev/null)
        if [ -z "$groups" ]; then
            echo -e "${YELLOW}  ! No consumer groups found for this stream${NC}"
        else
            group_count=$(echo "$groups" | grep -c "name")
            echo -e "${CYAN}  • Consumer groups: $group_count${NC}"
            
            # For command streams, check if daemon consumer group exists
            if [[ "$stream" == *":commands" ]]; then
                if echo "$groups" | grep -q "gnode-daemon"; then
                    echo -e "${GREEN}  ✓ Daemon consumer group found${NC}"
                else
                    echo -e "${RED}  ✗ Daemon consumer group missing${NC}"
                    echo -e "${YELLOW}  Suggestion: Restart the daemon to create the consumer group${NC}"
                fi
            fi
            
            # For response streams, check if client consumer group exists
            if [[ "$stream" == *":responses" ]]; then
                if echo "$groups" | grep -q "gnode-client"; then
                    echo -e "${GREEN}  ✓ Client consumer group found${NC}"
                else
                    echo -e "${RED}  ✗ Client consumer group missing${NC}"
                    echo -e "${YELLOW}  Suggestion: Initialize a client to create the consumer group${NC}"
                fi
            fi
        fi
        
        # Sample messages if any
        if [ "$length" -gt 0 ]; then
            echo -e "${CYAN}  • Sample messages (up to 3):${NC}"
            ${VALKEY_CLI:-valkey-cli} -h "$VALKEY_HOST" -p "$VALKEY_PORT" xrange "$stream" - + count 3 | sed 's/^/    /'
        fi
    done
fi

# Check service registration
echo -e "\n${BLUE}[6/7] Checking service registry${NC}"
registry_pattern="{$SITE_ID}:service_registry:*"
registry_keys=$(${VALKEY_CLI:-valkey-cli} -h "$VALKEY_HOST" -p "$VALKEY_PORT" keys "$registry_pattern" | sort)

if [ -z "$registry_keys" ]; then
    echo -e "${YELLOW}! No service registry entries found with pattern: $registry_pattern${NC}"
    echo -e "${YELLOW}Suggestion: Register services through the gNode client${NC}"
else
    echo -e "${GREEN}✓ Found $(echo "$registry_keys" | wc -l) service registry entries${NC}"
    
    for key in $registry_keys; do
        # Get service data
        service_data=$(${VALKEY_CLI:-valkey-cli} -h "$VALKEY_HOST" -p "$VALKEY_PORT" get "$key")
        
        # Extract service ID from key
        service_id=$(echo "$key" | sed "s/{$SITE_ID}:service_registry://")
        
        echo -e "${CYAN}• Service: $service_id${NC}"
        
        # Pretty-print service data if possible
        if command -v jq >/dev/null 2>&1; then
            echo "$service_data" | jq . | sed 's/^/  /' || echo "  $service_data"
        else
            echo "  $service_data"
        fi
    done
fi

# Generate system info
echo -e "\n${BLUE}[7/7] System information${NC}"
echo -e "${CYAN}• Operating system:${NC}"
uname -a
echo

echo -e "${CYAN}• CPU:${NC}"
grep "model name" /proc/cpuinfo | head -1
echo -e "${CYAN}• CPU cores:${NC} $(grep -c processor /proc/cpuinfo)"
echo

echo -e "${CYAN}• Memory:${NC}"
free -h
echo

echo -e "${CYAN}• Disk space:${NC}"
df -h | grep -v "tmpfs"
echo

echo -e "${CYAN}• Network interfaces:${NC}"
ip -br addr
echo

echo -e "${CYAN}• Environment variables:${NC}"
env | grep -E 'GNODE_|VALKEY_|REDIS_|SITE_|NODE_' | sort
echo

# Final summary
echo -e "\n${PURPLE}Diagnostic Summary${NC}"
echo "====================="
echo "Completed at: $(date)"
echo "Log file: $DIAGNOSTICS_LOG"
echo

echo "For more help with the gNode system, please see the following resources:"
echo -e "${CYAN}• Documentation: ${GNODE_DIR}/docs/${NC}"
echo -e "${CYAN}• Reference: ${GNODE_DIR}/docs/reference/FCALL_COOKBOOK.md${NC}"
echo -e "${CYAN}• API: ${GNODE_DIR}/docs/api/${NC}"

echo -e "\n${GREEN}Diagnostics complete!${NC}"