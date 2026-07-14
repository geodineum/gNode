#!/bin/bash
#
# Geodineum Configuration Validation
#
# Treats configuration health as a 7-layer coordinate system where each
# dimension has a health value 0.0-1.0, inspired by gnode_topology.lua's
# dimensional matching model.
#
# Layers:
#   1. STRUCTURE     - directories exist, correct hierarchy
#   2. PERMISSIONS   - ownership, modes, group membership
#   3. CREDENTIALS   - password files exist, readable, correct ownership
#   4. CONFIGURATION - env files present, no secrets in world-readable
#   5. CONNECTIVITY  - ValKey reachable, ACL users functional
#   6. COMPONENTS    - binaries built, autoloaders present
#   7. CONSISTENCY   - no legacy refs, symlinks valid, no orphans
#
# Usage:
#   ./scripts/validate-geodineum-config.sh [OPTIONS]
#
# Options:
#   --json          JSON output (machine-readable)
#   --verbose       Show all checks (not just failures)
#   --fix           Attempt to fix discovered issues (requires root)
#   --layer LAYER   Check specific layer only (1-7 or name)
#   --site SITE_ID  Validate specific site
#   --quiet         Exit code only (0=healthy, 1=degraded, 2=critical)
#   -h, --help      Show help
#

set -euo pipefail

# =============================================================================
# Configuration
# =============================================================================

CONFIG_ROOT="/etc/geodineum"
INSTALL_ROOT="/opt/geodineum"
GNODE_USER="gnode"
GNODE_GROUP="gnode"
WEB_GROUP="www-data"
VALKEY_PORT="47445"
VALKEY_HOST="127.0.0.1"

# Canonical ecosystem config loader (installed by Geodineum installer).
# Note: validate-geodineum-config.sh intentionally tolerates loader absence —
# it's the validation tool operators use to diagnose a broken install.
# Tier-4 absorbs this into `geodineum doctor` (Commit 4.5).
GEODINEUM_LIB="${GEODINEUM_LIB:-/usr/local/lib/geodineum}"
if [[ -r "$GEODINEUM_LIB/bootstrap-loader.sh" ]]; then
    # shellcheck source=/usr/local/lib/geodineum/bootstrap-loader.sh
    source "$GEODINEUM_LIB/bootstrap-loader.sh"
    load_ecosystem_config 2>/dev/null || true
fi
VALKEY_HOST="${VALKEY_HOST:-127.0.0.1}"
VALKEY_PORT="${VALKEY_PORT:-47445}"

# =============================================================================
# Colors & Output
# =============================================================================

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
DIM='\033[2m'
NC='\033[0m'

# =============================================================================
# CLI Arguments
# =============================================================================

OUTPUT_JSON=false
VERBOSE=false
FIX_MODE=false
CHECK_LAYER=""
CHECK_SITE=""
QUIET=false

show_help() {
    cat << 'EOF'
Geodineum Configuration Validation

Usage: validate-geodineum-config.sh [OPTIONS]

Options:
  --json          JSON output (machine-readable dimensional report)
  --verbose       Show all checks (not just failures)
  --fix           Attempt to fix discovered issues (requires root)
  --layer LAYER   Check specific layer only (1-7 or name)
  --site SITE_ID  Validate specific site
  --quiet         Exit code only (0=healthy, 1=degraded, 2=critical)
  -h, --help      Show help

Layers:
  1 | structure      Directories exist, correct hierarchy
  2 | permissions    Ownership, modes, group membership
  3 | credentials    Password files exist, readable by correct users
  4 | configuration  Env files present, no secrets in world-readable
  5 | connectivity   ValKey reachable, ACL users functional
  6 | components     Binaries built, PHP autoloaders present
  7 | consistency    No legacy refs, symlinks valid, no orphans

Exit codes:
  0  Healthy   (all layers > 0.8)
  1  Degraded  (any layer 0.4-0.8)
  2  Critical  (any layer < 0.4)
EOF
}

while [[ $# -gt 0 ]]; do
    case $1 in
        --json) OUTPUT_JSON=true; shift ;;
        --verbose) VERBOSE=true; shift ;;
        --fix) FIX_MODE=true; shift ;;
        --layer) CHECK_LAYER="$2"; shift 2 ;;
        --site) CHECK_SITE="$2"; shift 2 ;;
        --quiet) QUIET=true; shift ;;
        -h|--help) show_help; exit 0 ;;
        *) echo "Unknown option: $1" >&2; show_help; exit 1 ;;
    esac
done

# Normalize layer name to number
normalize_layer() {
    case "$1" in
        1|structure)     echo 1 ;;
        2|permissions)   echo 2 ;;
        3|credentials)   echo 3 ;;
        4|configuration) echo 4 ;;
        5|connectivity)  echo 5 ;;
        6|components)    echo 6 ;;
        7|consistency)   echo 7 ;;
        *) echo "Invalid layer: $1" >&2; exit 1 ;;
    esac
}

if [[ -n "$CHECK_LAYER" ]]; then
    CHECK_LAYER=$(normalize_layer "$CHECK_LAYER")
fi

# =============================================================================
# Check Tracking
# =============================================================================

declare -a LAYER_NAMES=("" "STRUCTURE" "PERMISSIONS" "CREDENTIALS" "CONFIGURATION" "CONNECTIVITY" "COMPONENTS" "CONSISTENCY")
declare -a LAYER_PASSES=()
declare -a LAYER_TOTALS=()
declare -a CHECK_RESULTS=()

for i in $(seq 1 7); do
    LAYER_PASSES[$i]=0
    LAYER_TOTALS[$i]=0
done

# Record a check result
# Usage: record_check LAYER STATUS MESSAGE
#   LAYER: 1-7
#   STATUS: PASS|WARN|FAIL
record_check() {
    local layer=$1
    local status=$2
    local message=$3

    LAYER_TOTALS[$layer]=$(( ${LAYER_TOTALS[$layer]} + 1 ))

    case "$status" in
        PASS) LAYER_PASSES[$layer]=$(( ${LAYER_PASSES[$layer]} + 1 )) ;;
        WARN) LAYER_PASSES[$layer]=$(( ${LAYER_PASSES[$layer]} )) ;; # 0.5 applied in score calc
        FAIL) ;; # 0.0
    esac

    CHECK_RESULTS+=("$layer|$status|$message")

    # Output is deferred to print_text_report / print_json_report
}

# Calculate layer score (0.0 - 1.0)
# PASS=1.0, WARN=0.5, FAIL=0.0
layer_score() {
    local layer=$1
    local total=${LAYER_TOTALS[$layer]}

    if [[ $total -eq 0 ]]; then
        echo "1.00"
        return
    fi

    local pass_count=${LAYER_PASSES[$layer]}
    local warn_count=0
    local fail_count=0

    for result in "${CHECK_RESULTS[@]}"; do
        local rl rs
        rl=$(echo "$result" | cut -d'|' -f1)
        rs=$(echo "$result" | cut -d'|' -f2)
        if [[ "$rl" == "$layer" ]]; then
            case "$rs" in
                WARN) warn_count=$((warn_count + 1)) ;;
                FAIL) fail_count=$((fail_count + 1)) ;;
            esac
        fi
    done

    # Score = (passes * 1.0 + warns * 0.5) / total
    local numerator=$(( pass_count * 100 + warn_count * 50 ))
    local score=$(( numerator / total ))

    # Format as X.XX
    printf "%d.%02d" $((score / 100)) $((score % 100))
}

# Print layer header (no-op: output is deferred to report)
layer_header() {
    :
}

# =============================================================================
# Helper Functions
# =============================================================================

# Check if path exists with expected type
check_path() {
    local path=$1
    local type=$2  # d=directory, f=file, L=symlink
    local desc=$3
    local layer=$4

    case "$type" in
        d) [[ -d "$path" ]] && record_check "$layer" PASS "$desc" || record_check "$layer" FAIL "$desc (missing)" ;;
        f) [[ -f "$path" ]] && record_check "$layer" PASS "$desc" || record_check "$layer" FAIL "$desc (missing)" ;;
        L) [[ -L "$path" ]] && record_check "$layer" PASS "$desc" || record_check "$layer" FAIL "$desc (not symlink)" ;;
    esac
}

# Check ownership matches expected owner:group
check_ownership() {
    local path=$1
    local expected_owner=$2
    local expected_group=$3
    local desc=$4
    local layer=$5

    if [[ ! -e "$path" ]]; then
        record_check "$layer" FAIL "$desc (path missing)"
        return
    fi

    local actual_owner actual_group
    actual_owner=$(stat -c '%U' "$path" 2>/dev/null)
    actual_group=$(stat -c '%G' "$path" 2>/dev/null)

    if [[ "$actual_owner" == "$expected_owner" && "$actual_group" == "$expected_group" ]]; then
        record_check "$layer" PASS "$desc: $actual_owner:$actual_group"
    else
        record_check "$layer" FAIL "$desc: expected $expected_owner:$expected_group, got $actual_owner:$actual_group"
    fi
}

# Check permission mode (octal)
check_mode() {
    local path=$1
    local expected_mode=$2
    local desc=$3
    local layer=$4

    if [[ ! -e "$path" ]]; then
        record_check "$layer" FAIL "$desc (path missing)"
        return
    fi

    local actual_mode
    actual_mode=$(stat -c '%a' "$path" 2>/dev/null)

    if [[ "$actual_mode" == "$expected_mode" ]]; then
        record_check "$layer" PASS "$desc: mode $actual_mode"
    else
        record_check "$layer" WARN "$desc: expected $expected_mode, got $actual_mode"
    fi
}

# Check permission mode is at most max_mode
check_mode_max() {
    local path=$1
    local max_mode=$2
    local desc=$3
    local layer=$4

    if [[ ! -e "$path" ]]; then
        record_check "$layer" FAIL "$desc (path missing)"
        return
    fi

    local actual_mode
    actual_mode=$(stat -c '%a' "$path" 2>/dev/null)

    if [[ "$actual_mode" -le "$max_mode" ]]; then
        record_check "$layer" PASS "$desc: mode $actual_mode (<= $max_mode)"
    else
        record_check "$layer" FAIL "$desc: mode $actual_mode exceeds $max_mode"
        if $FIX_MODE && [[ $EUID -eq 0 ]]; then
            chmod "$max_mode" "$path"
            echo -e "  ${CYAN}[FIXED]${NC} Set $path to $max_mode"
        fi
    fi
}

# Guardrail: every .htaccess Apache may read MUST be www-data-readable, or
# Apache fails closed ("unable to read htaccess file") and silently 403s the
# whole subtree. This caught the recurring example.com breakage; the check
# turns any future regression (a stray root:geodineum/root:root chown) into a
# loud FAIL instead of a production outage. --fix re-chowns offenders.
check_htaccess_readable() {
    local root=$1 layer=$2
    [[ -d "$root" ]] || return
    local f sym grp ok bad=0
    while IFS= read -r f; do
        [[ -n "$f" ]] || continue
        sym=$(stat -c '%A' "$f" 2>/dev/null); grp=$(stat -c '%G' "$f" 2>/dev/null)
        ok=false
        if [[ "${sym:7:1}" == "r" ]]; then ok=true                       # other-readable
        elif [[ "${sym:4:1}" == "r" ]] && id -nG www-data 2>/dev/null | tr ' ' '\n' | grep -qx "$grp"; then ok=true  # group www-data is in
        fi
        if ! $ok; then
            bad=$((bad+1))
            record_check "$layer" FAIL ".htaccess not www-data-readable: ${f#$root/} ($(stat -c '%U:%G %a' "$f"))"
            if $FIX_MODE && [[ $EUID -eq 0 ]]; then
                chown root:www-data "$f" && chmod 640 "$f" && echo -e "  ${CYAN}[FIXED]${NC} $f → root:www-data 640"
            fi
        fi
    done < <(find "$root" -name .htaccess 2>/dev/null)
    [[ $bad -eq 0 ]] && record_check "$layer" PASS "all .htaccess under ${root} are www-data-readable"
}

# Guardrail: shared framework/library source (gCore, gNode-Client) MUST be
# readable by every identity that consumes it as an in-process library —
# group geodineum-code, with those identities as members. A drift here (the
# tree slipping back to geodineum-web/www-data, or a consumer missing from
# geodineum-code) is exactly what crash-looped geodine: "Interface ... not
# found" at autoload because include() hit Permission denied. This turns that
# class into a loud FAIL. CONSUMERS lists the service users that link gCore.
check_source_readable() {
    local root=$1 layer=$2
    [[ -d "$root" ]] || return
    local CONSUMERS="geodine"   # add gshield/gsignals here when they link gCore
    local tree grp sample u bad=0
    for tree in gCore gNode-Client; do
        [[ -d "$root/$tree" ]] || continue
        grp=$(stat -c '%G' "$root/$tree" 2>/dev/null)
        if [[ "$grp" != "geodineum-code" ]]; then
            bad=$((bad+1))
            record_check "$layer" FAIL "$tree source group is '$grp', expected geodineum-code (conflation/drift)"
            if $FIX_MODE && [[ $EUID -eq 0 ]]; then
                getent group geodineum-code >/dev/null 2>&1 || groupadd --system geodineum-code
                chgrp -R geodineum-code "$root/$tree" && echo -e "  ${CYAN}[FIXED]${NC} $tree → :geodineum-code"
            fi
        fi
        sample=$(find "$root/$tree" -type f -name '*.php' 2>/dev/null | head -1)
        for u in $CONSUMERS; do
            id "$u" >/dev/null 2>&1 || continue
            [[ -n "$sample" ]] || continue
            if ! runuser -u "$u" -- test -r "$sample" 2>/dev/null; then
                bad=$((bad+1))
                record_check "$layer" FAIL "$u CANNOT read $tree source (not in geodineum-code? — gCore consumer will crash at autoload)"
                if $FIX_MODE && [[ $EUID -eq 0 ]]; then
                    usermod -aG geodineum-code "$u" && echo -e "  ${CYAN}[FIXED]${NC} $u → +geodineum-code (restart $u's service)"
                fi
            fi
        done
    done
    [[ $bad -eq 0 ]] && record_check "$layer" PASS "shared source (gCore, gNode-Client) is geodineum-code + readable by consumers"
}

# Check file does not contain secrets patterns
check_no_secrets() {
    local path=$1
    local desc=$2
    local layer=$3

    if [[ ! -f "$path" ]]; then
        record_check "$layer" FAIL "$desc (file missing)"
        return
    fi

    local secrets_found=false

    # Check for common secret patterns
    if grep -qiE '(password|secret|token)\s*=' "$path" 2>/dev/null; then
        # Exclude comments and known-safe patterns
        if grep -vE '^\s*#' "$path" | grep -qiE '(VALKEY_PASSWORD|SECRET_KEY|AUTH_TOKEN)\s*=' 2>/dev/null; then
            secrets_found=true
        fi
    fi

    if $secrets_found; then
        record_check "$layer" FAIL "$desc: contains credential assignments"
    else
        record_check "$layer" PASS "$desc: no secrets detected"
    fi
}

# Check ValKey connectivity with given user
check_valkey_auth() {
    local user=$1
    local password_file=$2
    local desc=$3
    local layer=$4

    if [[ ! -f "$password_file" ]]; then
        record_check "$layer" FAIL "$desc (password file missing: $password_file)"
        return
    fi

    if ! command -v valkey-cli &>/dev/null; then
        record_check "$layer" WARN "$desc (valkey-cli not in PATH)"
        return
    fi

    local password
    password=$(cat "$password_file" 2>/dev/null || true)

    if [[ -z "$password" ]]; then
        record_check "$layer" FAIL "$desc (password file empty or unreadable)"
        return
    fi

    local result
    result=$(REDISCLI_AUTH="$password" valkey-cli -h "$VALKEY_HOST" -p "$VALKEY_PORT" --user "$user" PING 2>&1) || true

    if [[ "$result" == "PONG" ]]; then
        record_check "$layer" PASS "$desc: PONG"
    else
        record_check "$layer" FAIL "$desc: $result"
    fi
}

# Check symlink target is valid
check_symlink_valid() {
    local path=$1
    local desc=$2
    local layer=$3

    if [[ ! -L "$path" ]]; then
        return  # Not a symlink, skip
    fi

    local target
    target=$(readlink -f "$path" 2>/dev/null)

    if [[ -e "$target" ]]; then
        record_check "$layer" PASS "$desc: -> $target"
    else
        record_check "$layer" FAIL "$desc: broken symlink -> $(readlink "$path")"
    fi
}

# =============================================================================
# Layer 1: STRUCTURE
# =============================================================================

check_layer_1() {
    layer_header 1

    check_path "$CONFIG_ROOT" d "/etc/geodineum/ exists" 1
    check_path "$CONFIG_ROOT/credentials" d "/etc/geodineum/credentials/ exists" 1
    check_path "$CONFIG_ROOT/components" d "/etc/geodineum/components/ exists" 1
    check_path "$CONFIG_ROOT/components/gnode-daemon" d "/etc/geodineum/components/gnode-daemon/ exists" 1
    check_path "$CONFIG_ROOT/components/gCore" d "/etc/geodineum/components/gCore/ exists" 1
    check_path "$CONFIG_ROOT/sites" d "/etc/geodineum/sites/ exists" 1

    # Check install root
    check_path "$INSTALL_ROOT" d "/opt/geodineum/ exists" 1

    # Check log directory
    local log_root="${GEODINEUM_LOG_DIR:-/var/log/geodineum}"
    if [[ -d "$log_root" ]]; then
        record_check 1 PASS "$log_root/ exists"
    else
        record_check 1 WARN "$log_root/ missing (logging not centralized)"
    fi
}

# =============================================================================
# Layer 2: PERMISSIONS
# =============================================================================

check_layer_2() {
    layer_header 2

    # ownership model aligned with least-privilege rework.
    #   /etc/geodineum/             root:geodineum         0751
    #   /etc/geodineum/credentials/ root:geodineum-creds   0751
    #   /etc/geodineum/dashboard/   root:geodineum-dash    0750
    #   /etc/geodineum/components/  gnode:gnode            0755
    #   /etc/geodineum/sites/       root:geodineum-dash    0750
    #     <site_id>/                gnode:www-data         0750
    #
    # Each ownership choice reflects who must read what:
    #   credentials/  → gnode + deploy_user only (admin + daemon creds)
    #   dashboard/    → gnode + www-data + deploy_user (gDash ACL token)
    #   sites/        → www-data traverses (per-site valkey_client creds)
    check_ownership "$CONFIG_ROOT" root geodineum "/etc/geodineum/" 2
    check_mode "$CONFIG_ROOT" 751 "/etc/geodineum/" 2

    check_ownership "$CONFIG_ROOT/credentials" root geodineum-creds "credentials/" 2
    check_mode "$CONFIG_ROOT/credentials" 751 "credentials/" 2

    # The .htaccess-readability guardrail — the recurring-403 class, made self-detecting.
    check_htaccess_readable "${GEODINEUM_ROOT:-/opt/geodineum}" 2

    # The shared-source-readability guardrail — the recurring geodine crash class.
    check_source_readable "${GEODINEUM_ROOT:-/opt/geodineum}" 2

    if [[ -d "$CONFIG_ROOT/dashboard" ]]; then
        check_ownership "$CONFIG_ROOT/dashboard" root geodineum-dash "dashboard/" 2
        check_mode "$CONFIG_ROOT/dashboard" 750 "dashboard/" 2
    fi

    check_ownership "$CONFIG_ROOT/components" "$GNODE_USER" "$GNODE_GROUP" "components/" 2
    # sites/ → gnode:www-data 0750. gnode owns (daemon can create
    # per-site subdirs at runtime when WP-CLI provisions a new site);
    # www-data group traverses (gCore PHP). Per-site subdirs are
    # gnode:www-data 0750 + per-site credentials gnode:www-data 0640.
    if [[ -d "$CONFIG_ROOT/sites" ]]; then
        check_ownership "$CONFIG_ROOT/sites" "$GNODE_USER" "$WEB_GROUP" "sites/" 2
        check_mode "$CONFIG_ROOT/sites" 750 "sites/" 2
    fi

    # Check gnode user exists
    if id "$GNODE_USER" &>/dev/null; then
        record_check 2 PASS "User '$GNODE_USER' exists"
    else
        record_check 2 FAIL "User '$GNODE_USER' does not exist"
    fi

    # Check gnode group exists
    if getent group "$GNODE_GROUP" &>/dev/null; then
        record_check 2 PASS "Group '$GNODE_GROUP' exists"
    else
        record_check 2 FAIL "Group '$GNODE_GROUP' does not exist"
    fi

    # Credential groups
    for grp in geodineum-creds geodineum-dash; do
        if getent group "$grp" &>/dev/null; then
            record_check 2 PASS "Group '$grp' exists"
        else
            record_check 2 FAIL "Group '$grp' does not exist (run Phase 7 of install.sh)"
        fi
    done

    # Check credential file permissions (none should be world-readable)
    if [[ -d "$CONFIG_ROOT/credentials" ]]; then
        for pwfile in "$CONFIG_ROOT/credentials"/*.password; do
            [[ -f "$pwfile" ]] || continue
            local filename
            filename=$(basename "$pwfile")

            # Symlinks get a warning
            if [[ -L "$pwfile" ]]; then
                record_check 2 WARN "$filename: symlink (should be regular file)"
                continue
            fi

            check_mode_max "$pwfile" 640 "$filename" 2
        done
    fi
}

# =============================================================================
# Layer 3: CREDENTIALS
# =============================================================================

check_layer_3() {
    layer_header 3

    local creds_dir="$CONFIG_ROOT/credentials"

    # Check daemon password
    if [[ -f "$creds_dir/valkey_daemon.password" ]]; then
        record_check 3 PASS "valkey_daemon.password exists"
        check_ownership "$creds_dir/valkey_daemon.password" "$GNODE_USER" "$GNODE_GROUP" "valkey_daemon.password ownership" 3

        # Check not empty
        if [[ -s "$creds_dir/valkey_daemon.password" ]]; then
            record_check 3 PASS "valkey_daemon.password is not empty"
        else
            record_check 3 FAIL "valkey_daemon.password is empty"
        fi
    else
        record_check 3 FAIL "valkey_daemon.password missing"
    fi

    # Check admin password
    if [[ -f "$creds_dir/valkey.password" ]]; then
        record_check 3 PASS "valkey.password exists"
    else
        record_check 3 WARN "valkey.password missing (admin access unavailable)"
    fi

    # Check client passwords. per-site credentials moved from
    # the flat /etc/geodineum/credentials/valkey_client_<id>.password
    # layout to per-site /etc/geodineum/sites/<id>/valkey_client.password
    # Glob both locations so older and newer installs both verify.
    local client_count=0
    local client_ok=0
    # New layout : /etc/geodineum/sites/<id>/valkey_client.password
    for pwfile in /etc/geodineum/sites/*/valkey_client.password; do
        [[ -f "$pwfile" ]] || continue
        client_count=$((client_count + 1))
        local filename
        filename=$(basename "$(dirname "$pwfile")")
        check_ownership "$pwfile" "$GNODE_USER" "$WEB_GROUP" "sites/${filename}/valkey_client.password ownership" 3
        client_ok=$((client_ok + 1))
    done
    # Legacy layout: /etc/geodineum/credentials/valkey_client_*.password
    for pwfile in "$creds_dir"/valkey_client_*.password; do
        [[ -f "$pwfile" ]] || continue
        client_count=$((client_count + 1))
        local filename
        filename=$(basename "$pwfile")

        if [[ -L "$pwfile" ]]; then
            record_check 3 WARN "$filename is a symlink (should be copied)"
            if $FIX_MODE && [[ $EUID -eq 0 ]]; then
                local target
                target=$(readlink -f "$pwfile")
                if [[ -f "$target" ]]; then
                    rm "$pwfile"
                    cp "$target" "$pwfile"
                    chown "$GNODE_USER:$WEB_GROUP" "$pwfile"
                    chmod 640 "$pwfile"
                    echo -e "  ${CYAN}[FIXED]${NC} Replaced symlink with copy: $filename"
                fi
            fi
        else
            check_ownership "$pwfile" "$GNODE_USER" "$WEB_GROUP" "$filename ownership" 3
            client_ok=$((client_ok + 1))
        fi
    done

    # zero client passwords is the canonical state for a fresh
    # install with no sites registered yet — not a degradation. Sites
    # are registered post-install via `geodineum new site <domain>`
    # which triggers provision-gnode-site.sh. Demote to PASS with
    # contextual message, same shape as the optional-file fix.
    if [[ $client_count -gt 0 ]]; then
        record_check 3 PASS "$client_count client password(s) found"
    else
        record_check 3 PASS "No site credentials yet — fresh install, no sites registered (run \`geodineum new site <domain>\` to register)"
    fi

    # Check for password duplication across stores
    local legacy_creds="$INSTALL_ROOT/gNode/.gnode"
    if [[ -d "$legacy_creds" ]] && [[ ! -L "$legacy_creds" ]]; then
        record_check 3 WARN "Legacy credential store exists: $legacy_creds (should be symlink to centralized)"
    elif [[ -L "$legacy_creds" ]]; then
        record_check 3 PASS "Legacy .gnode/ is symlink to centralized"
    fi
}

# =============================================================================
# Layer 4: CONFIGURATION
# =============================================================================

check_layer_4() {
    layer_header 4

    # bootstrap.env — disk-minimal 3-key surface per Commit 0.1
    # (single-canonical-route model). Pre-fix the validator expected a
    # 4-key shape that included GNODE_TOPOLOGY_NAMESPACE and GNODE_DIR;
    # those keys moved to the ValKey-resident config tier
    # (geodineum:bootstrap:*) per 91bc731's whitelist tightening. The
    # disk tier carries the minimum 3 keys (host/port/creds-path)
    # needed to bootstrap the bootstrap-loader; everything else lives
    # in ValKey.
    if [[ -f "$CONFIG_ROOT/bootstrap.env" ]]; then
        record_check 4 PASS "bootstrap.env exists"
        check_no_secrets "$CONFIG_ROOT/bootstrap.env" "bootstrap.env" 4

        # Three canonical disk-tier keys (must be present)
        for var in VALKEY_HOST VALKEY_PORT VALKEY_CREDS_PATH; do
            if grep -q "^${var}=" "$CONFIG_ROOT/bootstrap.env" 2>/dev/null; then
                record_check 4 PASS "bootstrap.env defines $var"
            else
                record_check 4 FAIL "bootstrap.env missing $var (canonical disk-tier key)"
            fi
        done
    else
        record_check 4 FAIL "bootstrap.env missing"
    fi

    # daemon.env — OPTIONAL override. Systemd unit declares it as
    # `EnvironmentFile=-...` (the `-` prefix = "optional, no error if
    # missing"). The daemon's clap definitions in main.rs supply
    # defaults for every var via #[clap(env = "...", default_value = "...")]
    # attributes. daemon.env exists only to override those defaults
    # in operator-customized installs. Absence is NOT a problem.
    local daemon_env="$CONFIG_ROOT/components/gnode-daemon/daemon.env"
    if [[ -f "$daemon_env" ]]; then
        record_check 4 PASS "daemon.env exists (operator overrides active)"
    else
        record_check 4 PASS "daemon.env absent — using clap defaults (canonical)"
    fi

    # gcore.env — same shape (optional override).
    local gcore_env="$CONFIG_ROOT/components/gCore/gcore.env"
    if [[ -f "$gcore_env" ]]; then
        record_check 4 PASS "gcore.env exists"
    else
        record_check 4 PASS "gcore.env absent — using defaults"
    fi

    # Check node config directory. Empty is fine for master-only
    # single-node installs (Ch.1 default); multi-node clusters populate
    # this directory via `geodineum constellation join-replica`.
    local nodes_dir="$CONFIG_ROOT/components/gnode-daemon/nodes"
    if [[ -d "$nodes_dir" ]]; then
        local node_count
        node_count=$(find "$nodes_dir" -name '*.yaml' 2>/dev/null | wc -l)
        if [[ $node_count -gt 0 ]]; then
            record_check 4 PASS "Node configs: $node_count YAML files"
        else
            record_check 4 PASS "Node config directory empty (master-only — expected for Ch.1 single-node)"
        fi
    fi
}

# =============================================================================
# Layer 5: CONNECTIVITY
# =============================================================================

check_layer_5() {
    layer_header 5

    # Check ValKey port reachable
    if timeout 2 bash -c "cat < /dev/null > /dev/tcp/$VALKEY_HOST/$VALKEY_PORT" 2>/dev/null; then
        record_check 5 PASS "ValKey reachable at $VALKEY_HOST:$VALKEY_PORT"
    else
        record_check 5 FAIL "ValKey unreachable at $VALKEY_HOST:$VALKEY_PORT"
        return  # Skip auth checks if not reachable
    fi

    # Check daemon auth
    local creds_dir="$CONFIG_ROOT/credentials"
    check_valkey_auth "gnode_daemon" "$creds_dir/valkey_daemon.password" "Daemon auth (gnode_daemon)" 5

    # Check client auth for registered sites
    for pwfile in "$creds_dir"/valkey_client_*.password; do
        [[ -f "$pwfile" ]] || continue
        local filename client_user
        filename=$(basename "$pwfile")
        # Extract user from filename: valkey_client_foo.password -> gnode_client_foo
        client_user="gnode_${filename%.password}"
        # valkey_client -> gnode_client
        client_user="${client_user/valkey_/}"

        check_valkey_auth "$client_user" "$pwfile" "Client auth ($client_user)" 5
    done

    # Check if gnode-daemon systemd service is running.
    #
    # short retry loop. systemctl is-active can race a recent
    # `systemctl start` because units transition activating → active
    # asynchronously, and the ExecStartPre password check adds extra
    # latency on first start. install.sh's Phase 8 starts the daemon
    # then immediately runs this validator, so a fresh install routinely
    # caught the unit mid-transition and reported WARN — confusing
    # operators into thinking something's broken when the daemon is
    # actually fine. Retry up to ~3s before declaring failure.
    local _gnode_active="false"
    local _retry
    for _retry in 1 2 3 4 5 6; do
        if systemctl is-active --quiet gnode-daemon 2>/dev/null; then
            _gnode_active="true"
            break
        fi
        sleep 0.5
    done
    if [[ "$_gnode_active" == "true" ]]; then
        record_check 5 PASS "gnode-daemon service is running"
    else
        # Still failing after retry — check the unit's actual state so
        # the operator gets a useful diagnostic instead of just "not
        # running".
        local _state
        _state=$(systemctl is-active gnode-daemon 2>/dev/null || echo "unknown")
        if [[ "$_state" == "activating" ]]; then
            record_check 5 WARN "gnode-daemon service is still activating after 3s — check 'journalctl -u gnode-daemon'"
        elif [[ "$_state" == "failed" ]]; then
            record_check 5 FAIL "gnode-daemon service failed — see 'journalctl -u gnode-daemon -n 50'"
        else
            record_check 5 WARN "gnode-daemon service is not running (state: ${_state})"
        fi
    fi

    # Check if the canonical Valkey unit is running. accept
    # either valkey-server.service (modern installer-side name from
    # fddf810's source-build path AND the apt valkey-server package)
    # OR valkey-gnode.service (legacy gNode-side name from the
    # retired setup-valkey-smart.sh path). Pre-fix only checked the
    # legacy name and reported WARN on every modern install — feeding
    # noise into the install summary's `[WARN] Configuration validation
    # found issues`.
    if systemctl is-active --quiet valkey-server 2>/dev/null; then
        record_check 5 PASS "valkey-server service is running"
    elif systemctl is-active --quiet valkey-gnode 2>/dev/null; then
        record_check 5 PASS "valkey-gnode service is running (legacy unit name)"
    else
        record_check 5 WARN "no valkey systemd unit is running (looked for valkey-server, valkey-gnode)"
    fi
}

# =============================================================================
# Layer 6: COMPONENTS
# =============================================================================

check_layer_6() {
    layer_header 6

    local gnode_dir="${GNODE_DIR:-$INSTALL_ROOT/gNode}"
    local gnode_client_dir="${GNODE_CLIENT_DIR:-$INSTALL_ROOT/gNode-Client}"
    local gcore_dir="${GCORE_DIR:-$INSTALL_ROOT/gCore}"

    # gNode daemon binary
    local daemon_bin="$gnode_dir/daemon/target/release/gnode-daemon"
    if [[ -x "$daemon_bin" ]]; then
        record_check 6 PASS "gNode daemon binary exists and is executable"
    elif [[ -f "$daemon_bin" ]]; then
        record_check 6 WARN "gNode daemon binary exists but is not executable"
    else
        record_check 6 FAIL "gNode daemon binary not found (cargo build needed)"
    fi

    # Lua functions
    if [[ -d "$gnode_dir/daemon/functions" ]]; then
        local lua_count
        lua_count=$(find "$gnode_dir/daemon/functions" -name '*.lua' 2>/dev/null | wc -l)
        if [[ $lua_count -gt 0 ]]; then
            record_check 6 PASS "Lua functions: $lua_count files"
        else
            record_check 6 FAIL "No Lua function files found"
        fi
    else
        record_check 6 FAIL "Lua functions directory missing"
    fi

    # gNode-Client
    if [[ -d "$gnode_client_dir" ]]; then
        if [[ -f "$gnode_client_dir/vendor/autoload.php" ]]; then
            record_check 6 PASS "gNode-Client: vendor/autoload.php present"
        elif [[ -f "$gnode_client_dir/composer.json" ]]; then
            record_check 6 WARN "gNode-Client: composer install needed"
        else
            record_check 6 WARN "gNode-Client: not a composer project?"
        fi
    else
        record_check 6 WARN "gNode-Client not installed at $gnode_client_dir"
    fi

    # gCore
    if [[ -d "$gcore_dir" ]]; then
        if [[ -f "$gcore_dir/vendor/autoload.php" ]]; then
            record_check 6 PASS "gCore: vendor/autoload.php present"
        elif [[ -f "$gcore_dir/composer.json" ]]; then
            record_check 6 WARN "gCore: composer install needed"
        else
            record_check 6 WARN "gCore: not a composer project?"
        fi
    else
        record_check 6 WARN "gCore not installed at $gcore_dir"
    fi

    # Check Cargo.toml for version info
    if [[ -f "$gnode_dir/daemon/Cargo.toml" ]]; then
        local version
        version=$(grep '^version' "$gnode_dir/daemon/Cargo.toml" 2>/dev/null | head -1 | sed 's/.*"\(.*\)".*/\1/')
        if [[ -n "$version" ]]; then
            record_check 6 PASS "gNode daemon version: $version"
        fi
    fi
}

# =============================================================================
# Layer 7: CONSISTENCY
# =============================================================================

check_layer_7() {
    layer_header 7

    local creds_dir="$CONFIG_ROOT/credentials"

    # Check for reverse symlinks in credentials (should point INTO centralized, not OUT)
    for pwfile in "$creds_dir"/*.password; do
        [[ -L "$pwfile" ]] || continue
        local target filename
        filename=$(basename "$pwfile")
        target=$(readlink "$pwfile")

        # Symlinks pointing outside /etc/geodineum are anomalous
        if [[ "$target" != /etc/geodineum/* ]]; then
            record_check 7 WARN "$filename: reverse symlink to $target"
            if $FIX_MODE && [[ $EUID -eq 0 ]]; then
                local resolved
                resolved=$(readlink -f "$pwfile")
                if [[ -f "$resolved" ]]; then
                    rm "$pwfile"
                    cp "$resolved" "$pwfile"
                    # Determine correct ownership
                    if [[ "$filename" == "valkey_daemon.password" ]] || [[ "$filename" == "valkey.password" ]]; then
                        chown "$GNODE_USER:$GNODE_GROUP" "$pwfile"
                        chmod 600 "$pwfile"
                    else
                        chown "$GNODE_USER:$WEB_GROUP" "$pwfile"
                        chmod 640 "$pwfile"
                    fi
                    echo -e "  ${CYAN}[FIXED]${NC} Replaced reverse symlink: $filename"
                fi
            fi
        fi
    done

    # Check dev .gnode/ is a symlink to centralized
    local dev_gnode="/opt/gNode/.gnode"
    if [[ -L "$dev_gnode" ]]; then
        local dev_target
        dev_target=$(readlink "$dev_gnode")
        if [[ "$dev_target" == "$creds_dir" || "$dev_target" == "$creds_dir/" ]]; then
            record_check 7 PASS "Dev .gnode/ symlinks to centralized"
        else
            record_check 7 WARN "Dev .gnode/ symlinks to unexpected: $dev_target"
        fi
    elif [[ -d "$dev_gnode" ]]; then
        record_check 7 WARN "Dev .gnode/ is a directory (should be symlink)"
    fi

    # Check production .gnode/ is a symlink to centralized
    local prod_gnode="$INSTALL_ROOT/gNode/.gnode"
    if [[ -L "$prod_gnode" ]]; then
        record_check 7 PASS "Production .gnode/ symlinks to centralized"
    elif [[ -d "$prod_gnode" ]]; then
        record_check 7 WARN "Production .gnode/ is a directory (should be symlink to centralized)"
    fi

    # Check systemd service file references centralized paths
    local service_file="/etc/systemd/system/gnode-daemon.service"
    if [[ -f "$service_file" ]]; then
        if grep -q "/etc/geodineum/" "$service_file" 2>/dev/null; then
            record_check 7 PASS "Systemd service references /etc/geodineum/"
        else
            record_check 7 WARN "Systemd service does not reference /etc/geodineum/"
        fi
    fi

    # Check for broken symlinks in credentials
    for pwfile in "$creds_dir"/*.password; do
        [[ -L "$pwfile" ]] || continue
        check_symlink_valid "$pwfile" "$(basename "$pwfile")" 7
    done

    # Check bootstrap.env consistency between template and deployed
    local gnode_dir="${GNODE_DIR:-$INSTALL_ROOT/gNode}"
    local template="$gnode_dir/config/bootstrap.env"
    local deployed="$CONFIG_ROOT/bootstrap.env"
    if [[ -f "$template" && -f "$deployed" ]]; then
        # Check that deployed has all vars from template
        local missing_vars=0
        while IFS= read -r line; do
            [[ "$line" =~ ^[A-Z] ]] || continue
            local var_name
            var_name=$(echo "$line" | cut -d= -f1)
            if ! grep -q "^${var_name}=" "$deployed" 2>/dev/null; then
                missing_vars=$((missing_vars + 1))
            fi
        done < "$template"

        if [[ $missing_vars -eq 0 ]]; then
            record_check 7 PASS "Deployed bootstrap.env has all template variables"
        else
            record_check 7 WARN "Deployed bootstrap.env missing $missing_vars variable(s) from template"
        fi
    fi
}

# =============================================================================
# Site-specific validation
# =============================================================================

check_site() {
    local site_id=$1

    if ! $QUIET && ! $OUTPUT_JSON; then
        echo ""
        echo -e "${BOLD}Site: $site_id${NC}"
    fi

    local creds_dir="$CONFIG_ROOT/credentials"

    # Check client password exists
    local pw_file="$creds_dir/valkey_client_${site_id}.password"
    if [[ -f "$pw_file" ]]; then
        record_check 3 PASS "Site $site_id: client password exists"
    else
        record_check 3 FAIL "Site $site_id: client password missing"
    fi

    # Check site config directory
    local site_dir="$CONFIG_ROOT/sites/$site_id"
    if [[ -d "$site_dir" ]]; then
        record_check 1 PASS "Site $site_id: config directory exists"
    else
        record_check 1 WARN "Site $site_id: no per-site config directory"
    fi

    # Check ValKey auth if connected
    if timeout 2 bash -c "cat < /dev/null > /dev/tcp/$VALKEY_HOST/$VALKEY_PORT" 2>/dev/null; then
        check_valkey_auth "gnode_client_${site_id}" "$pw_file" "Site $site_id: ValKey auth" 5
    fi
}

# =============================================================================
# Output Functions
# =============================================================================

print_text_report() {
    echo ""
    echo -e "${BOLD}Geodineum Configuration Health Report${NC}"
    echo "======================================="

    local overall_min="1.00"

    for layer in $(seq 1 7); do
        local score
        score=$(layer_score "$layer")
        local name=${LAYER_NAMES[$layer]}
        local total=${LAYER_TOTALS[$layer]}

        # Determine health label
        local label color
        local score_int=${score%.*}${score#*.}
        score_int=$((10#$score_int))  # Remove leading zeros

        if [[ $score_int -gt 80 ]]; then
            label="HEALTHY"
            color="$GREEN"
        elif [[ $score_int -ge 40 ]]; then
            label="DEGRADED"
            color="$YELLOW"
        else
            label="CRITICAL"
            color="$RED"
        fi

        # Pad dots between name and score
        local name_len=${#name}
        local dots_count=$((40 - name_len - ${#score} - ${#label} - 5))
        [[ $dots_count -lt 3 ]] && dots_count=3
        local dots
        dots=$(printf '%*s' "$dots_count" '' | tr ' ' '.')

        echo ""
        echo -e "${BOLD}Layer $layer: $name${NC} ${DIM}$dots${NC} ${color}$score [$label]${NC}"

        # Print individual check results for this layer
        for result in "${CHECK_RESULTS[@]}"; do
            local rl rs rm
            rl=$(echo "$result" | cut -d'|' -f1)
            rs=$(echo "$result" | cut -d'|' -f2)
            rm=$(echo "$result" | cut -d'|' -f3-)

            if [[ "$rl" != "$layer" ]]; then
                continue
            fi

            case "$rs" in
                PASS)
                    if $VERBOSE; then
                        echo -e "  ${GREEN}[PASS]${NC} $rm"
                    fi
                    ;;
                WARN) echo -e "  ${YELLOW}[WARN]${NC} $rm" ;;
                FAIL) echo -e "  ${RED}[FAIL]${NC} $rm" ;;
            esac
        done

        # Track minimum score
        if [[ $score_int -lt $((10#${overall_min%.*}${overall_min#*.})) ]]; then
            overall_min="$score"
        fi
    done

    # Overall verdict
    local overall_int=${overall_min%.*}${overall_min#*.}
    overall_int=$((10#$overall_int))
    local overall_label overall_color
    if [[ $overall_int -gt 80 ]]; then
        overall_label="HEALTHY"
        overall_color="$GREEN"
    elif [[ $overall_int -ge 40 ]]; then
        overall_label="DEGRADED"
        overall_color="$YELLOW"
    else
        overall_label="CRITICAL"
        overall_color="$RED"
    fi

    echo ""
    echo "======================================="
    echo -e "${BOLD}OVERALL: ${overall_color}$overall_min $overall_label${NC}"

    # Show which layers need attention
    for layer in $(seq 1 7); do
        local score
        score=$(layer_score "$layer")
        local score_int=${score%.*}${score#*.}
        score_int=$((10#$score_int))
        if [[ $score_int -le 80 ]]; then
            echo -e "  ${YELLOW}Layer $layer (${LAYER_NAMES[$layer]}) needs attention${NC}"
        fi
    done

    echo ""
}

print_json_report() {
    python3 -c "
import json, sys

layers = {}
for i in range(1, 8):
    layers[str(i)] = {'name': '', 'checks': [], 'score': 0.0}

names = ['', 'STRUCTURE', 'PERMISSIONS', 'CREDENTIALS', 'CONFIGURATION', 'CONNECTIVITY', 'COMPONENTS', 'CONSISTENCY']
for i in range(1, 8):
    layers[str(i)]['name'] = names[i]

results = []
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    parts = line.split('|', 2)
    if len(parts) != 3:
        continue
    layer, status, message = parts
    layers[layer]['checks'].append({'status': status, 'message': message})

# Calculate scores
overall_min = 1.0
for i in range(1, 8):
    checks = layers[str(i)]['checks']
    if not checks:
        layers[str(i)]['score'] = 1.0
        continue
    total = 0.0
    for c in checks:
        if c['status'] == 'PASS':
            total += 1.0
        elif c['status'] == 'WARN':
            total += 0.5
    score = total / len(checks)
    layers[str(i)]['score'] = round(score, 2)
    overall_min = min(overall_min, score)

# Determine health
def health(score):
    if score > 0.8: return 'HEALTHY'
    if score >= 0.4: return 'DEGRADED'
    return 'CRITICAL'

report = {
    'layers': {},
    'overall_score': round(overall_min, 2),
    'overall_health': health(overall_min)
}

for i in range(1, 8):
    l = layers[str(i)]
    report['layers'][l['name'].lower()] = {
        'score': l['score'],
        'health': health(l['score']),
        'checks': l['checks']
    }

print(json.dumps(report, indent=2))
" <<< "$(printf '%s\n' "${CHECK_RESULTS[@]}")"
}

# =============================================================================
# Main
# =============================================================================

main() {
    # Run layer checks
    if [[ -n "$CHECK_LAYER" ]]; then
        eval "check_layer_$CHECK_LAYER"
    else
        check_layer_1
        check_layer_2
        check_layer_3
        check_layer_4
        check_layer_5
        check_layer_6
        check_layer_7
    fi

    # Run site-specific checks
    if [[ -n "$CHECK_SITE" ]]; then
        check_site "$CHECK_SITE"
    fi

    # Calculate exit code
    local exit_code=0
    for layer in $(seq 1 7); do
        if [[ -n "$CHECK_LAYER" && "$layer" != "$CHECK_LAYER" ]]; then
            continue
        fi

        local score
        score=$(layer_score "$layer")
        local score_int=${score%.*}${score#*.}
        score_int=$((10#$score_int))

        if [[ $score_int -lt 40 && $exit_code -lt 2 ]]; then
            exit_code=2
        elif [[ $score_int -le 80 && $exit_code -lt 1 ]]; then
            exit_code=1
        fi
    done

    # Output
    if $QUIET; then
        exit $exit_code
    elif $OUTPUT_JSON; then
        print_json_report
    else
        print_text_report
    fi

    exit $exit_code
}

main
