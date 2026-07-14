#!/bin/bash
#
# gNode Site Registration - Canonical Script
#
# This is the SINGLE canonical script for registering new gNode sites.
# It consolidates create-site-acl-user.sh and register-site-gnode.sh.
#
# Creates:
#   1. ACL user (gnode_client_{site_id}) with site-isolated keyspace
#   2. Password file (.gnode/valkey_client_{site_id}.password)
#   3. gNode streams via FCALL GNODE_PROVISION_SERVICE
#   4. Site registry entry
#
# Usage:
#   ./scripts/register-site.sh <site_id> [OPTIONS]
#
# Examples:
#   ./scripts/register-site.sh my_app
#   ./scripts/register-site.sh my_app --environments '["production","staging"]'
#   ./scripts/register-site.sh dev_mysite --acl-only
#   ./scripts/register-site.sh mysite --streams-only
#   ./scripts/register-site.sh mysite --dry-run
#

set -euo pipefail

# ─── PERMISSION / ACL MODEL — 3 COORDINATED LOCATIONS (keep in sync) ─────────
# ValKey credential ownership + client ACL grants are defined in THREE places
# that MUST change together (fixing one alone is what caused the geodine
# NOAUTH crash-loop). Canonical model + rationale: ~/gh/PERMISSION_MODEL.md.
#   [1] gNode/scripts/register-site.sh   — web-site creds: ACL grant +
#                                          root:geodineum-web 0640 + .owner sidecar  (THIS FILE)
#   [2] gNode/scripts/onboard-service.sh — service/component creds: ACL grant +
#                                          root:geodineum 0640 + .owner sidecar
#   [3] Geodineum[-pro]/lib/geodeploy.sh :: geodeploy_fix_all_credentials()
#                                          — deploy-time enforcement from the
#                                          .owner sidecars (NO www-data default)
# Change the cred owner/group/mode or the client ACL command set in any one →
# mirror it in the other two (and any live-migration script).
# ────────────────────────────────────────────────────────────────────────────

# =============================================================================
# Configuration
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
PASSWORD_DIR="${GNODE_PASSWORD_DIR:-$PROJECT_ROOT/.gnode}"
VALKEY_CLI="$SCRIPT_DIR/valkey-cli-secure.sh"

# For ACL operations, we need the default user (gnode_daemon has -@dangerous which blocks ACL)
VALKEY_ADMIN_PASSWORD_FILE="$PASSWORD_DIR/valkey.password"
valkey_admin_cli() {
    if [[ -f "$VALKEY_ADMIN_PASSWORD_FILE" ]]; then
        REDISCLI_AUTH="$(cat "$VALKEY_ADMIN_PASSWORD_FILE")" valkey-cli -p "${VALKEY_PORT:-47445}" "$@"
    else
        log_error "Admin password file not found: $VALKEY_ADMIN_PASSWORD_FILE"
        log_error "ACL operations require the default user password"
        return 1
    fi
}

# v1 LAYER 5: optionally source the manifest-policy + manifest-registry libs
# from the deployed Geodineum repo. When available AND the site_id has a
# registered manifest with a `data:` section, the ACL grants are composed
# from that manifest (validated against the static namespace policy) instead
# of the hardcoded pattern list below. Graceful: missing libs ⇒ legacy path.
_GEO_LIB_DIR="${GEODINEUM_ROOT:-/opt/geodineum}/Geodineum/lib"
if [[ -r "$_GEO_LIB_DIR/manifest-policy.sh" ]]; then
    # shellcheck source=/opt/geodineum/Geodineum/lib/manifest-policy.sh
    source "$_GEO_LIB_DIR/manifest-policy.sh"
fi
if [[ -r "$_GEO_LIB_DIR/manifest-registry.sh" ]]; then
    # shellcheck source=/opt/geodineum/Geodineum/lib/manifest-registry.sh
    source "$_GEO_LIB_DIR/manifest-registry.sh"
fi

# Canonical ecosystem config loader (installed by Geodineum installer).
GEODINEUM_LIB="${GEODINEUM_LIB:-/usr/local/lib/geodineum}"
if [[ ! -r "$GEODINEUM_LIB/bootstrap-loader.sh" ]]; then
    echo "FATAL: $GEODINEUM_LIB/bootstrap-loader.sh not found. Run installer first." >&2
    exit 1
fi
# shellcheck source=/usr/local/lib/geodineum/bootstrap-loader.sh
source "$GEODINEUM_LIB/bootstrap-loader.sh"
load_ecosystem_config

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

# Logging
log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[OK]${NC} $1"; }
log_warning() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1" >&2; }
log_step() { echo ""; echo -e "${CYAN}==>${NC} ${BOLD}$1${NC}"; }
log_dry() { echo -e "${YELLOW}[DRY-RUN]${NC} Would: $1"; }

# =============================================================================
# Argument Parsing
# =============================================================================

SITE_ID=""
ENVIRONMENTS='["testing"]'
ACL_ONLY=false
STREAMS_ONLY=false
DRY_RUN=false
FORCE=false
SKIP_VALIDATION=false
TOPOLOGY_NS="geodineum"

print_usage() {
    cat << EOF
Usage: $0 <site_id> [OPTIONS]

Registers a new gNode site with ACL user and streams.

Arguments:
  site_id             Site identifier (lowercase, numbers, underscores only)

Options:
  --environment ENV   Single environment (shorthand for --environments '["ENV"]')
                      Values: testing, staging, acceptance, production
  --environments JSON JSON array of environments (default: '["testing"]')
                      Example: '["production","staging"]'
  --acl-only          Only create ACL user, skip stream creation
  --streams-only      Only create streams (assumes ACL already exists)
  --dry-run           Show what would be done without making changes
  --force             Overwrite existing ACL user (regenerate password)
  --skip-validation   Skip pre-flight validation checks
  --topology-ns NS    Topology namespace for shared streams (default: geodineum)
  -h, --help          Show this help message

Examples:
  # Register a new production site
  $0 my_app

  # Register with multiple environments
  $0 my_app --environments '["production","staging"]'

  # Only create ACL (useful for testing credentials)
  $0 test_site --acl-only

  # Only create streams (if ACL was created separately)
  $0 existing_site --streams-only

  # Preview changes without applying
  $0 new_site --dry-run

EOF
}

while [[ $# -gt 0 ]]; do
    case $1 in
        --environment|-e)
            ENVIRONMENTS="[\"$2\"]"
            shift 2
            ;;
        --environments)
            ENVIRONMENTS="$2"
            shift 2
            ;;
        --acl-only)
            ACL_ONLY=true
            shift
            ;;
        --streams-only)
            STREAMS_ONLY=true
            shift
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --force)
            FORCE=true
            shift
            ;;
        --skip-validation)
            SKIP_VALIDATION=true
            shift
            ;;
        --topology-ns)
            TOPOLOGY_NS="$2"
            shift 2
            ;;
        -h|--help)
            print_usage
            exit 0
            ;;
        -*)
            log_error "Unknown option: $1"
            print_usage
            exit 1
            ;;
        *)
            if [[ -z "$SITE_ID" ]]; then
                SITE_ID="$1"
            else
                log_error "Unexpected argument: $1"
                print_usage
                exit 1
            fi
            shift
            ;;
    esac
done

# =============================================================================
# Validation
# =============================================================================

if [[ -z "$SITE_ID" ]]; then
    log_error "site_id is required"
    print_usage
    exit 1
fi

# Validate site_id format
if [[ ! "$SITE_ID" =~ ^[a-z0-9_]+$ ]]; then
    log_error "site_id must contain only lowercase letters, numbers, and underscores"
    log_info "Got: $SITE_ID"
    log_info "Example: my_app, staging_mysite"
    exit 1
fi

# Validate environments JSON
if ! echo "$ENVIRONMENTS" | python3 -c "import sys, json; json.load(sys.stdin)" 2>/dev/null; then
    log_error "Invalid environments JSON: $ENVIRONMENTS"
    log_info "Must be a JSON array, e.g.: '[\"production\",\"staging\"]'"
    exit 1
fi

# Validate each environment value
VALID_ENVS=("testing" "staging" "acceptance" "production")
for env in $(echo "$ENVIRONMENTS" | python3 -c "import sys, json; print(' '.join(json.load(sys.stdin)))"); do
    valid=false
    for valid_env in "${VALID_ENVS[@]}"; do
        if [[ "$env" == "$valid_env" ]]; then
            valid=true
            break
        fi
    done
    if [[ "$valid" != "true" ]]; then
        log_error "Invalid environment: $env"
        log_info "Valid environments: testing, staging, acceptance, production"
        exit 1
    fi
done

# Conflicting options
if [[ "$ACL_ONLY" == "true" && "$STREAMS_ONLY" == "true" ]]; then
    log_error "Cannot use --acl-only and --streams-only together"
    exit 1
fi

# =============================================================================
# Pre-flight Checks
# =============================================================================

if [[ "$SKIP_VALIDATION" != "true" ]]; then
    log_step "Pre-flight checks"

    # Check ValKey CLI
    if [[ ! -x "$VALKEY_CLI" ]]; then
        log_error "ValKey CLI not found: $VALKEY_CLI"
        exit 1
    fi

    # Check ValKey connection
    if ! VALKEY_USER=gnode_daemon "$VALKEY_CLI" PING &>/dev/null; then
        log_error "Cannot connect to ValKey"
        log_info "Ensure ValKey is running: systemctl status valkey-gnode"
        log_info "Check daemon password exists: ls -la $PASSWORD_DIR/valkey_daemon.password"
        exit 1
    fi
    log_success "ValKey connection OK"

    # Check password directory
    if [[ ! -d "$PASSWORD_DIR" ]]; then
        log_info "Creating password directory: $PASSWORD_DIR"
        if [[ "$DRY_RUN" != "true" ]]; then
            mkdir -p "$PASSWORD_DIR"
            chmod 750 "$PASSWORD_DIR"
        fi
    fi
    log_success "Password directory OK: $PASSWORD_DIR"
fi

# =============================================================================
# Variables
# =============================================================================

ACL_USER="gnode_client_${SITE_ID}"
PASSWORD_FILE="$PASSWORD_DIR/valkey_client_${SITE_ID}.password"

# =============================================================================
# Header
# =============================================================================

echo ""
echo "=============================================="
echo "  gNode Site Registration"
echo "=============================================="
echo ""
log_info "Site ID:      $SITE_ID"
log_info "ACL User:     $ACL_USER"
log_info "Environments: $ENVIRONMENTS"
log_info "Topology NS:  $TOPOLOGY_NS"
if [[ "$ACL_ONLY" == "true" ]]; then
    log_info "Mode:         ACL only (no streams)"
elif [[ "$STREAMS_ONLY" == "true" ]]; then
    log_info "Mode:         Streams only (no ACL)"
else
    log_info "Mode:         Full (ACL + streams)"
fi
if [[ "$DRY_RUN" == "true" ]]; then
    log_warning "DRY RUN - No changes will be made"
fi
echo ""

# =============================================================================
# ACL User Creation
# =============================================================================

if [[ "$STREAMS_ONLY" != "true" ]]; then
    log_step "ACL User Setup"

    # Check if password file exists (our source of truth)
    if [[ -f "$PASSWORD_FILE" && "$FORCE" != "true" ]]; then
        log_info "Password file exists: $PASSWORD_FILE"
        log_info "Using existing credentials (use --force to regenerate)"
        PASSWORD=$(cat "$PASSWORD_FILE")
    else
        if [[ -f "$PASSWORD_FILE" && "$FORCE" == "true" ]]; then
            log_warning "Regenerating credentials (--force specified)"
        fi
        # Generate 64-character hex password
        PASSWORD=$(openssl rand -hex 32)

        if [[ "$DRY_RUN" == "true" ]]; then
            log_dry "Generate password and store at $PASSWORD_FILE"
            log_dry "Set permissions: 640, owner: gnode:www-data"
        else
            echo -n "$PASSWORD" > "$PASSWORD_FILE"
            chmod 640 "$PASSWORD_FILE"
            # Web-site cred: read by the web tier (PHP-FPM as www-data). Owner
            # root (integrity); group geodineum-web (sole member www-data)
            # grants the read — NOT the broad www-data group, and never
            # www-data ownership. The .owner sidecar lets the deploy
            # credentials pass preserve this deterministically.
            chown root:geodineum-web "$PASSWORD_FILE" 2>/dev/null \
                || log_warning "Could not set ownership root:geodineum-web on $PASSWORD_FILE"
            printf 'root:geodineum-web:640\n' > "${PASSWORD_FILE}.owner" 2>/dev/null || true
            log_success "Password generated: $PASSWORD_FILE"
        fi
    fi

    # Create/update ACL user
    if [[ "$DRY_RUN" == "true" ]]; then
        log_dry "ACL SETUSER $ACL_USER on resetpass >***"
        log_dry "Set keyspace patterns for site isolation"
        log_dry "Set channel permissions"
        log_dry "Set command permissions"
        log_dry "ACL SAVE"
    else
        # Create user with password via stdin to avoid exposure in `ps aux`
        if [[ -f "$VALKEY_ADMIN_PASSWORD_FILE" ]]; then
            REDISCLI_AUTH="$(cat "$VALKEY_ADMIN_PASSWORD_FILE")" \
                valkey-cli -p "${VALKEY_PORT:-47445}" <<EOF >/dev/null
ACL SETUSER $ACL_USER on resetpass >$PASSWORD
EOF
        fi
        log_success "User credentials set"

        # v1 LAYER 5: prefer manifest-driven patterns when available.
        # A site_id with a registered manifest containing a `data:` section
        # produces ACL grants composed from manifest.data.consumes/produces,
        # interpolated against {site_id}/{ecosystem} placeholders and
        # validated against the static namespace policy. Refused patterns
        # abort registration before any ACL change is committed (security
        # boundary is strict, not best-effort).
        _manifest_path=""; _use_manifest=false
        _key_grants=(); _channel_grants=()
        if declare -F manifest_get_field >/dev/null 2>&1; then
            _manifest_path=$(manifest_get_field "$SITE_ID" "manifest_path" 2>/dev/null || true)
        fi
        if [[ -n "$_manifest_path" && -f "$_manifest_path" ]] && \
           declare -F policy_manifest_has_data >/dev/null 2>&1 && \
           policy_manifest_has_data "$_manifest_path"; then
            log_info "Manifest data: section detected at $_manifest_path"
            if mapfile -t _key_grants < <(policy_compose_key_grants "$_manifest_path" "$SITE_ID" "$SITE_ID" "geodineum") \
               && mapfile -t _channel_grants < <(policy_compose_channel_grants "$_manifest_path" "$SITE_ID" "$SITE_ID" "geodineum") \
               && [[ ${#_key_grants[@]} -gt 0 ]]; then
                _use_manifest=true
                log_success "Composed ${#_key_grants[@]} key + ${#_channel_grants[@]} channel grants from manifest"
            else
                log_error "Manifest data validation failed — refusing to provision (security policy)"
                exit 1
            fi
        fi

        # Set keyspace patterns
        if [[ "$_use_manifest" == "true" ]]; then
            valkey_admin_cli ACL SETUSER "$ACL_USER" resetkeys "${_key_grants[@]}" >/dev/null
            log_success "Keyspace patterns set (manifest-driven, ${#_key_grants[@]} grants)"
        else
            # Legacy hardcoded pattern set. Used when no manifest is registered
            # (existing WordPress sites + first-time registrations during early
            # migration). Covers per-site isolation, global defaults, ecosystem
            # shared, and DTAP environments.
            valkey_admin_cli ACL SETUSER "$ACL_USER" resetkeys \
                "~error:${SITE_ID}:*" \
                "~cache:${SITE_ID}:*" \
                "~session:${SITE_ID}:*" \
                "~${SITE_ID}:error:*" \
                "~${SITE_ID}:cache:*" \
                "~${SITE_ID}:session:*" \
                "~${SITE_ID}:gnode:*" \
                "~${SITE_ID}:*" \
                "~{${SITE_ID}}:gnode:*" \
                "~{${SITE_ID}}:bundle:*" \
                "~{${SITE_ID}}:cache:*" \
                "~{${SITE_ID}}:metrics:*" \
                "~{${SITE_ID}}:*" \
                "~{testing}:gnode:*" \
                "~{staging}:gnode:*" \
                "~{acceptance}:gnode:*" \
                "~{production}:gnode:*" \
                "~{default}:gnode:*" \
                "~{default}:gcore:*" \
                "~{geodineum}:gnode:*" \
                "~gnode:*" \
                "~gnode:routing:*" \
                "~topology:*" \
                "~template:*" \
                "~membership:*" >/dev/null
            log_success "Keyspace patterns set (legacy hardcoded set)"
        fi

        # Set channel permissions
        if [[ "$_use_manifest" == "true" ]] && [[ ${#_channel_grants[@]} -gt 0 ]]; then
            valkey_admin_cli ACL SETUSER "$ACL_USER" resetchannels "${_channel_grants[@]}" >/dev/null
            log_success "Channel permissions set (manifest-driven, ${#_channel_grants[@]} grants)"
        else
            valkey_admin_cli ACL SETUSER "$ACL_USER" resetchannels "&*" >/dev/null
            log_success "Channel permissions set (legacy wildcard)"
        fi

        # Set command permissions
        valkey_admin_cli ACL SETUSER "$ACL_USER" nocommands \
            +xread +xreadgroup +xadd +xack +xclaim +xpending +xinfo +xlen +xtrim +xrange +xrevrange +xgroup +xdel \
            +fcall +fcall_ro \
            +get +set +setex +setnx +del +exists +ttl +expire +mget +mset +incr +decr +incrby +decrby \
            +hget +hset +hgetall +hdel +hexists +hkeys +hvals +hincrby +hmget +hmset \
            +sadd +smembers +sismember +srem +scard \
            +lpush +rpush +lpop +rpop +lrange +llen +lindex +ltrim \
            +zadd +zrange +zrevrange +zrem +zscore +zcard \
            +scan +ping +publish +auth +select +info +client|id +client|getname +client|setname +client|setinfo +multi +exec +discard +time +type +object >/dev/null
        log_success "Command permissions set"

        # Save ACL
        valkey_admin_cli ACL SAVE >/dev/null
        log_success "ACL configuration saved"
    fi
fi

# =============================================================================
# Stream Creation (via canonical Lua function)
# =============================================================================

if [[ "$ACL_ONLY" != "true" ]]; then
    log_step "Stream Creation"

    if [[ "$DRY_RUN" == "true" ]]; then
        log_dry "FCALL GNODE_PROVISION_SERVICE 0 $SITE_ID '$ENVIRONMENTS' $TOPOLOGY_NS"
        log_info "Would create:"
        for env in $(echo "$ENVIRONMENTS" | python3 -c "import sys, json; print(' '.join(json.load(sys.stdin)))"); do
            log_info "  - {${SITE_ID}}:gnode:unified:${env} (groups: gnode-daemon, gnode-client)"
        done
        log_info "  - {${SITE_ID}}:gnode:health (groups: gnode-daemon)"
        log_info "  - {${TOPOLOGY_NS}}:gnode:broadcast:global (shared)"
        log_info "  - {${TOPOLOGY_NS}}:gnode:unified (shared)"
        log_info "  - geodineum:unified:stream (global)"
    else
        # Call canonical Lua function for stream creation
        RESULT=$(VALKEY_USER=gnode_daemon "$VALKEY_CLI" FCALL GNODE_PROVISION_SERVICE 0 "$SITE_ID" "$ENVIRONMENTS" "$TOPOLOGY_NS" 2>&1)

        # Parse result
        if echo "$RESULT" | python3 -c "import sys, json; d=json.load(sys.stdin); exit(0 if d.get('success', False) else 1)" 2>/dev/null; then
            log_success "Streams created successfully"

            # Parse stream/group counts (disable errexit for pipeline compatibility)
            CREATED=0
            GROUPS=0
            set +e
            PARSED=$(echo "$RESULT" | python3 -c "
import sys, json
d = json.load(sys.stdin)
s = d.get('created_streams', [])
g = d.get('created_groups', [])
print(len(s) if isinstance(s, list) else 0)
print(len(g) if isinstance(g, list) else 0)
" 2>/dev/null)
            if [[ $? -eq 0 && -n "$PARSED" ]]; then
                CREATED=$(echo "$PARSED" | head -1)
                GROUPS=$(echo "$PARSED" | tail -1)
            fi
            set -e
            if [[ "$CREATED" != "0" || "$GROUPS" != "0" ]]; then
                log_info "Created $CREATED new streams, $GROUPS new consumer groups"
            fi

            # Show stream details if any were created
            echo "$RESULT" | python3 -c "
import sys, json
d = json.load(sys.stdin)
streams = d.get('created_streams', [])
if isinstance(streams, list):
    for stream in streams:
        print(f'  + {stream}')
" 2>/dev/null || true
        else
            log_warning "Stream creation had issues"
            # Show errors if any
            echo "$RESULT" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    for err in d.get('errors', []):
        print(f'  - {err}')
except:
    print(sys.stdin.read())
" 2>/dev/null || echo "$RESULT"
        fi
    fi
fi

# =============================================================================
# Summary
# =============================================================================

echo ""
echo "=============================================="
echo "  Registration Complete"
echo "=============================================="
echo ""

if [[ "$DRY_RUN" == "true" ]]; then
    log_warning "DRY RUN - No changes were made"
    log_info "Run without --dry-run to apply changes"
else
    if [[ "$STREAMS_ONLY" != "true" ]]; then
        log_success "ACL User: $ACL_USER"
        log_success "Password: $PASSWORD_FILE"
    fi

    if [[ "$ACL_ONLY" != "true" ]]; then
        log_success "Streams created for environments: $ENVIRONMENTS"
    fi

    echo ""
    log_step "Verification Commands"
    echo ""
    echo "  # Test authentication"
    echo "  VALKEY_USER=$ACL_USER $VALKEY_CLI PING"
    echo ""
    echo "  # Check streams"
    for env in $(echo "$ENVIRONMENTS" | python3 -c "import sys, json; print(' '.join(json.load(sys.stdin)))" 2>/dev/null || echo "testing"); do
        echo "  VALKEY_USER=gnode_daemon $VALKEY_CLI XINFO STREAM \"{${SITE_ID}}:gnode:unified:${env}\""
        break  # Only show first environment
    done
    echo ""
    echo "  # Check site registry"
    echo "  VALKEY_USER=gnode_daemon $VALKEY_CLI SISMEMBER gnode:sites:registry $SITE_ID"
    echo ""

    log_step "WordPress Configuration"
    echo ""
    echo "  Add to wp-config.php (before 'That's all, stop editing!'):"
    echo ""
    echo "    // gNode Configuration"
    echo "    define('GNODE_SITE_ID', '${SITE_ID}');"
    echo "    define('GNODE_ENVIRONMENT', '$(echo "$ENVIRONMENTS" | python3 -c "import sys,json; print(json.load(sys.stdin)[0])" 2>/dev/null || echo "testing")');"
    echo "    define('VALKEY_HOST', '127.0.0.1');"
    echo "    define('VALKEY_PORT', 47445);"
    echo "    define('VALKEY_USER', '${ACL_USER}');"
    echo "    define('VALKEY_PASSWORD', file_get_contents('${PASSWORD_FILE}'));"
    echo ""
    echo "  Or use environment variables (.env):"
    echo ""
    echo "    GNODE_SITE_ID=${SITE_ID}"
    echo "    GNODE_ENVIRONMENT=$(echo "$ENVIRONMENTS" | python3 -c "import sys,json; print(json.load(sys.stdin)[0])" 2>/dev/null || echo "testing")"
    echo "    VALKEY_HOST=127.0.0.1"
    echo "    VALKEY_PORT=47445"
    echo "    VALKEY_USER=${ACL_USER}"
    echo "    VALKEY_PASSWORD_FILE=${PASSWORD_FILE}"
    echo ""
    echo "  Quick copy password to clipboard (if xclip installed):"
    echo "    cat $PASSWORD_FILE | xclip -selection clipboard"
    echo ""
fi
