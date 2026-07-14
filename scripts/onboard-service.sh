#!/bin/bash
#
# gNode Service Onboarding — One-command setup for new services
#
# Creates everything a service needs to participate in the gNode mesh:
#   1. ACL user (idempotent — skips if already exists)
#   2. ValKey streams + registry (via GNODE_PROVISION_SERVICE)
#   3. Discovery path registration (adds to discovery-paths.conf)
#   4. Tenant/owner group (optional, for cross-site discovery)
#
# Default environment is "testing" — sites are gated behind a ViewKey
# until explicitly promoted to production. This prevents new sites from
# being publicly visible before they are ready.
#
# Usage:
#   ./scripts/onboard-service.sh <site_id> [OPTIONS]
#
# Examples:
#   # Full onboarding with YAML discovery (default: testing environment)
#   ./scripts/onboard-service.sh my_inference --yaml /opt/my-inference-service
#
#   # Production-ready service
#   ./scripts/onboard-service.sh my_app --yaml /opt/app --environment production
#
#   # With tenant grouping for cross-site discovery
#   ./scripts/onboard-service.sh my_app --owner acme --yaml /opt/acme/app/config
#
#   # Dry-run to preview
#   ./scripts/onboard-service.sh my_service --yaml /opt/my-service --dry-run
#
# For service removal, see: deregister-service.sh
#

set -euo pipefail

# ─── PERMISSION / ACL MODEL — 3 COORDINATED LOCATIONS (keep in sync) ─────────
# ValKey credential ownership + client ACL grants are defined in THREE places
# that MUST change together (fixing one alone is what caused the geodine
# NOAUTH crash-loop). Canonical model + rationale: ~/gh/PERMISSION_MODEL.md.
#   [1] gNode/scripts/register-site.sh   — web-site creds: ACL grant +
#                                          root:geodineum-web 0640 + .owner sidecar
#   [2] gNode/scripts/onboard-service.sh — service/component creds: ACL grant +
#                                          root:<component> 0640 (per-component group) + sidecar (THIS FILE)
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

# Credential and config directories (centralized FHS layout)
CREDENTIAL_DIR="${GNODE_CREDENTIAL_DIR:-/etc/geodineum/credentials}"
DISCOVERY_PATHS_FILE="${GNODE_DISCOVERY_PATHS_FILE:-/etc/geodineum/components/gnode-daemon/discovery-paths.conf}"
VALKEY_CLI="$SCRIPT_DIR/valkey-cli-secure.sh"

# Admin password for ACL operations (gnode_daemon has -@dangerous, so we need default user)
VALKEY_ADMIN_PASSWORD_FILE="${CREDENTIAL_DIR}/valkey.password"
valkey_admin_cli() {
    if [[ -f "$VALKEY_ADMIN_PASSWORD_FILE" ]]; then
        REDISCLI_AUTH="$(cat "$VALKEY_ADMIN_PASSWORD_FILE")" valkey-cli -p "${VALKEY_PORT:-47445}" "$@"
    else
        log_error "Admin password file not found: $VALKEY_ADMIN_PASSWORD_FILE"
        log_error "ACL operations require the default user password"
        return 1
    fi
}

# Daemon password for non-ACL operations
valkey_daemon_cli() {
    local daemon_pass_file="${CREDENTIAL_DIR}/valkey_daemon.password"
    if [[ -f "$daemon_pass_file" ]]; then
        REDISCLI_AUTH="$(cat "$daemon_pass_file")" valkey-cli -p "${VALKEY_PORT:-47445}" --user gnode_daemon "$@"
    else
        log_error "Daemon password file not found: $daemon_pass_file"
        return 1
    fi
}

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
# Usage
# =============================================================================

usage() {
    cat << 'EOF'
Usage: onboard-service.sh <site_id> [OPTIONS]

Arguments:
  site_id              Unique identifier (lowercase, numbers, underscores only)

Options:
  --yaml PATH          Path to service YAML (file or directory)
                       Directories are probed for gnode_services.yaml / geometric_topology.yaml
  --owner OWNER_ID     Tenant/owner group for cross-site discovery
  --environment ENV    Single DTAP environment (default: testing)
  --environments JSON  JSON array of environments (e.g. '["testing","production"]')
  --notify-email EMAIL Notification recipient (configures COMMS email channel)
  --dry-run            Preview all actions without making changes
  --force              Regenerate ACL password even if it exists
  -h, --help           Show this help message

Service YAML format (gnode_services.yaml):
  services:
    - id: "MyService"
      metadata:
        description: "What this service does"
        type: "service"
        tier: "SERVICE"
      capabilities:
        - name: "protocol"
          value: "http_rest"
        - name: "domain_primary"
          value: "inference"

See: docs/operations/ONBOARDING.md for full documentation.
EOF
}

# =============================================================================
# Argument Parsing
# =============================================================================

SITE_ID=""
YAML_PATH=""
# Capability profile for the service's own (C) entity (web|headless|service|
# system|component). The site registers ONE entity from this profile's 30-dim
# defaults. Defaults to `web` (the common case); override with --profile.
PROFILE="${GNODE_SERVICE_PROFILE:-web}"
OWNER=""
ENVIRONMENT=""
ENVIRONMENTS=""
NOTIFY_EMAIL=""
DRY_RUN="false"
FORCE="false"
# Per-component cred isolation: the single-member group that may read THIS
# component's cred (only its own runtime identity). Defaults to the shared
# geodineum group for back-compat; `register component` passes the component's
# private group so no component can read another's cred.
CRED_GROUP="${GNODE_CRED_GROUP:-geodineum}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --yaml)         YAML_PATH="$2"; shift 2 ;;
        --profile)      PROFILE="$2"; shift 2 ;;
        --cred-group)   CRED_GROUP="$2"; shift 2 ;;
        --owner)        OWNER="$2"; shift 2 ;;
        --environment)  ENVIRONMENT="$2"; shift 2 ;;
        --environments) ENVIRONMENTS="$2"; shift 2 ;;
        --notify-email) NOTIFY_EMAIL="$2"; shift 2 ;;
        --dry-run)      DRY_RUN="true"; shift ;;
        --force)        FORCE="true"; shift ;;
        -h|--help)      usage; exit 0 ;;
        -*)             log_error "Unknown option: $1"; usage; exit 1 ;;
        *)
            if [[ -z "$SITE_ID" ]]; then
                SITE_ID="$1"
            else
                log_error "Unexpected argument: $1"
                usage; exit 1
            fi
            shift ;;
    esac
done

# =============================================================================
# Validation
# =============================================================================

if [[ -z "$SITE_ID" ]]; then
    log_error "Site ID is required"
    usage
    exit 1
fi

# Validate site_id format
if ! [[ "$SITE_ID" =~ ^[a-z0-9_]+$ ]]; then
    log_error "Invalid site_id: '$SITE_ID' (must be lowercase letters, numbers, and underscores only)"
    exit 1
fi

# Resolve YAML path
RESOLVED_YAML=""
if [[ -n "$YAML_PATH" ]]; then
    if [[ -f "$YAML_PATH" ]]; then
        RESOLVED_YAML="$YAML_PATH"
    elif [[ -d "$YAML_PATH" ]]; then
        # Probe for recognized filenames (same order as daemon)
        if [[ -f "$YAML_PATH/gnode_services.yaml" ]]; then
            RESOLVED_YAML="$YAML_PATH/gnode_services.yaml"
        elif [[ -f "$YAML_PATH/geometric_topology.yaml" ]]; then
            RESOLVED_YAML="$YAML_PATH/geometric_topology.yaml"
        else
            log_error "No gnode_services.yaml or geometric_topology.yaml found in: $YAML_PATH"
            exit 1
        fi
    else
        log_error "YAML path does not exist: $YAML_PATH"
        exit 1
    fi
    log_info "Service config: $RESOLVED_YAML"
fi

# Resolve environments
TOPOLOGY_NS="${GNODE_TOPOLOGY_NAMESPACE:-geodineum}"
if [[ -n "$ENVIRONMENT" && -n "$ENVIRONMENTS" ]]; then
    log_error "Cannot specify both --environment and --environments"
    exit 1
fi
if [[ -n "$ENVIRONMENT" ]]; then
    ENVIRONMENTS="[\"$ENVIRONMENT\"]"
fi
if [[ -z "$ENVIRONMENTS" ]]; then
    ENVIRONMENTS='["testing"]'
fi
# Primary DTAP env for dim-20 embedding — the first (and, for sites, only) of
# ENVIRONMENTS, which now subsumes both --environment and the default.
PRIMARY_ENV=$(echo "$ENVIRONMENTS" | python3 -c "import sys,json; print(json.loads(sys.stdin.read())[0])" 2>/dev/null || echo "")

ACL_USER="gnode_client_${SITE_ID}"
PASSWORD_FILE="${CREDENTIAL_DIR}/valkey_client_${SITE_ID}.password"

# =============================================================================
# COMMS Settings Builder
# =============================================================================

# Build a SiteSettings JSON blob matching the Rust SiteSettings struct
# (Geodineum-COMMS deserializes this with serde — must match exactly)
build_comms_settings_json() {
    local site_id="$1"
    local email="$2"
    local domain="${site_id//_/.}"

    cat <<ENDJSON
{
  "site_id": "${site_id}",
  "enabled": true,
  "channels": {
    "email": {
      "enabled": true,
      "config": {
        "smtp_host": "localhost",
        "smtp_port": 25,
        "smtp_tls": false,
        "from_address": "noreply@${domain}",
        "from_name": "Geodineum"
      },
      "recipients": [
        {"email": "${email}", "types": ["all"], "min_priority": 5}
      ]
    }
  },
  "routing_rules": [
    {"type": "all", "channels": ["email"]}
  ],
  "rate_limits": {},
  "filters": {"spam_enabled": false},
  "retry": {"max_attempts": 5, "base_delay_secs": 30, "max_delay_secs": 3600}
}
ENDJSON
}

# =============================================================================
# Summary
# =============================================================================

echo ""
echo -e "${BOLD}gNode Service Onboarding${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
log_info "Site ID:      $SITE_ID"
log_info "ACL User:     $ACL_USER"
log_info "Environments: $ENVIRONMENTS"
if [[ -n "$OWNER" ]]; then
    log_info "Owner/Tenant: $OWNER"
fi
if [[ -n "$NOTIFY_EMAIL" ]]; then
    log_info "Notify Email: $NOTIFY_EMAIL"
fi
if [[ -n "$RESOLVED_YAML" ]]; then
    log_info "YAML Config:  $RESOLVED_YAML"
fi
if [[ "$DRY_RUN" == "true" ]]; then
    log_warning "DRY RUN — No changes will be made"
fi
echo ""

# =============================================================================
# Step 1: ACL User Creation (idempotent)
# =============================================================================

log_step "Step 1: ACL User Setup"

# Check current state
ACL_EXISTS="false"
PASSWORD_EXISTS="false"
PASSWORD=""

if [[ -f "$PASSWORD_FILE" ]]; then
    PASSWORD_EXISTS="true"
    PASSWORD=$(cat "$PASSWORD_FILE")
fi

# Check if ACL user exists in ValKey
if valkey_admin_cli ACL GETUSER "$ACL_USER" >/dev/null 2>&1; then
    ACL_EXISTS="true"
fi

if [[ "$ACL_EXISTS" == "true" && "$PASSWORD_EXISTS" == "true" && "$FORCE" != "true" ]]; then
    log_success "ACL user already provisioned (skipping)"
elif [[ "$PASSWORD_EXISTS" == "true" && "$ACL_EXISTS" == "false" ]]; then
    log_warning "Password file exists but ACL user missing — recreating user"
    if [[ "$DRY_RUN" == "true" ]]; then
        log_dry "ACL SETUSER $ACL_USER on resetpass >*** (using existing password)"
    else
        REDISCLI_AUTH="$(cat "$VALKEY_ADMIN_PASSWORD_FILE")" \
            valkey-cli -p "${VALKEY_PORT:-47445}" <<EOF >/dev/null
ACL SETUSER $ACL_USER on resetpass >$PASSWORD
EOF
        # Set keyspace patterns
        valkey_admin_cli ACL SETUSER "$ACL_USER" resetkeys \
            "~${SITE_ID}:*" \
            "~{${SITE_ID}}:*" \
            "~{testing}:gnode:*" \
            "~{staging}:gnode:*" \
            "~{acceptance}:gnode:*" \
            "~{production}:gnode:*" \
            "~{${TOPOLOGY_NS}}:gnode:*" \
            "~gnode:*" \
            "~topology:*" \
            "~template:*" \
            "~membership:*" >/dev/null
        valkey_admin_cli ACL SETUSER "$ACL_USER" resetchannels "&*" >/dev/null
        valkey_admin_cli ACL SETUSER "$ACL_USER" nocommands \
            +xread +xreadgroup +xadd +xack +xclaim +xpending +xinfo +xlen +xtrim +xrange +xrevrange +xgroup +xdel \
            +fcall +fcall_ro \
            +get +set +setex +setnx +del +exists +ttl +expire +mget +mset +incr +decr +incrby +decrby \
            +hget +hset +hgetall +hdel +hexists +hkeys +hvals +hincrby +hmget +hmset \
            +sadd +smembers +sismember +srem +scard \
            +lpush +rpush +lpop +rpop +lrange +llen +lindex +ltrim \
            +zadd +zrange +zrevrange +zrem +zscore +zcard \
            +scan +ping +publish +auth +select +info '+client|id' '+client|getname' '+client|setname' '+client|setinfo' +multi +exec +discard +time +type +object >/dev/null
        valkey_admin_cli ACL SAVE >/dev/null
        log_success "ACL user recreated with existing password"
    fi
else
    # Generate new password and create ACL user
    if [[ "$FORCE" == "true" && "$PASSWORD_EXISTS" == "true" ]]; then
        log_warning "Regenerating credentials (--force)"
    fi
    PASSWORD=$(openssl rand -hex 32)

    if [[ "$DRY_RUN" == "true" ]]; then
        log_dry "Generate 64-char hex password"
        log_dry "Store at: $PASSWORD_FILE (640, gnode:www-data)"
        log_dry "ACL SETUSER $ACL_USER on resetpass >***"
        log_dry "Set keyspace + command permissions"
        log_dry "ACL SAVE"
    else
        # Store password file first (fail-safe for re-runs)
        echo -n "$PASSWORD" > "$PASSWORD_FILE"
        chmod 640 "$PASSWORD_FILE"
        # Service cred — PER-COMPONENT isolation: owner root (integrity), group
        # = this component's single-member group (CRED_GROUP), so ONLY its own
        # runtime user reads it and no component can read another's cred.
        # Defaults to geodineum (shared) for back-compat; `register component`
        # passes the private group. www-data is in NEITHER → a web-tier
        # compromise cannot read service creds. The .owner sidecar lets the
        # deploy credentials pass preserve this.
        if [[ "$CRED_GROUP" != "geodineum" ]] && ! getent group "$CRED_GROUP" >/dev/null 2>&1; then
            groupadd --system "$CRED_GROUP" 2>/dev/null || CRED_GROUP="geodineum"
        fi
        chown "root:${CRED_GROUP}" "$PASSWORD_FILE" 2>/dev/null || true
        printf 'root:%s:640\n' "$CRED_GROUP" > "${PASSWORD_FILE}.owner" 2>/dev/null || true
        log_success "Password stored: $PASSWORD_FILE (group ${CRED_GROUP})"

        # Create ACL user
        REDISCLI_AUTH="$(cat "$VALKEY_ADMIN_PASSWORD_FILE")" \
            valkey-cli -p "${VALKEY_PORT:-47445}" <<EOF >/dev/null
ACL SETUSER $ACL_USER on resetpass >$PASSWORD
EOF
        # Set keyspace patterns
        valkey_admin_cli ACL SETUSER "$ACL_USER" resetkeys \
            "~${SITE_ID}:*" \
            "~{${SITE_ID}}:*" \
            "~{testing}:gnode:*" \
            "~{staging}:gnode:*" \
            "~{acceptance}:gnode:*" \
            "~{production}:gnode:*" \
            "~{${TOPOLOGY_NS}}:gnode:*" \
            "~gnode:*" \
            "~topology:*" \
            "~template:*" \
            "~membership:*" >/dev/null
        valkey_admin_cli ACL SETUSER "$ACL_USER" resetchannels "&*" >/dev/null
        valkey_admin_cli ACL SETUSER "$ACL_USER" nocommands \
            +xread +xreadgroup +xadd +xack +xclaim +xpending +xinfo +xlen +xtrim +xrange +xrevrange +xgroup +xdel \
            +fcall +fcall_ro \
            +get +set +setex +setnx +del +exists +ttl +expire +mget +mset +incr +decr +incrby +decrby \
            +hget +hset +hgetall +hdel +hexists +hkeys +hvals +hincrby +hmget +hmset \
            +sadd +smembers +sismember +srem +scard \
            +lpush +rpush +lpop +rpop +lrange +llen +lindex +ltrim \
            +zadd +zrange +zrevrange +zrem +zscore +zcard \
            +scan +ping +publish +auth +select +info '+client|id' '+client|getname' '+client|setname' '+client|setinfo' +multi +exec +discard +time +type +object >/dev/null
        valkey_admin_cli ACL SAVE >/dev/null
        log_success "ACL user created: $ACL_USER"
    fi
fi

# =============================================================================
# Step 2: Stream Provisioning (always — single registration path)
# =============================================================================

log_step "Step 2: Stream Provisioning"

PROVISION_ARGS="$SITE_ID $ENVIRONMENTS $TOPOLOGY_NS"
if [[ -n "$OWNER" ]]; then
    PROVISION_ARGS="$PROVISION_ARGS $OWNER"
fi

if [[ "$DRY_RUN" == "true" ]]; then
    log_dry "FCALL GNODE_PROVISION_SERVICE 0 $PROVISION_ARGS"
else
    RESULT=$(valkey_daemon_cli FCALL GNODE_PROVISION_SERVICE 0 "$SITE_ID" "$ENVIRONMENTS" "$TOPOLOGY_NS" "${OWNER:-}" 2>&1) || {
        log_error "Stream provisioning failed: $RESULT"
        log_error "Cannot continue without streams — registration incomplete"
        exit 1
    }
    log_success "Streams provisioned for $SITE_ID"
fi

# =============================================================================
# Step 3: Register Discovery Path
# =============================================================================

if [[ -n "$YAML_PATH" ]]; then
    log_step "Step 3: Discovery Path Registration"

    # Use the original --yaml path (directory or file), not the resolved YAML
    DISCOVERY_ENTRY="$YAML_PATH"

    if [[ "$DRY_RUN" == "true" ]]; then
        log_dry "Add to $DISCOVERY_PATHS_FILE: $DISCOVERY_ENTRY"
    else
        # Create discovery paths file if it doesn't exist
        if [[ ! -f "$DISCOVERY_PATHS_FILE" ]]; then
            cat > "$DISCOVERY_PATHS_FILE" << 'HEADER'
# gNode Service Discovery Paths
# One path per line. Directories probed for gnode_services.yaml / geometric_topology.yaml
HEADER
            chmod 640 "$DISCOVERY_PATHS_FILE"
            chown gnode:gnode "$DISCOVERY_PATHS_FILE" 2>/dev/null || true
        fi

        # Idempotent append
        if grep -qxF "$DISCOVERY_ENTRY" "$DISCOVERY_PATHS_FILE" 2>/dev/null; then
            log_info "Path already in discovery manifest (skipping)"
        else
            echo "$DISCOVERY_ENTRY" >> "$DISCOVERY_PATHS_FILE"
            log_success "Added to discovery manifest: $DISCOVERY_ENTRY"
        fi
    fi
else
    log_step "Step 3: Discovery Path Registration (skipped — no --yaml)"
    log_info "Pass --yaml to register a service YAML for daemon discovery"
fi

# =============================================================================
# Step 3.5: Immediate topology registration (S6c)
# Register the service's capability vector into the canonical (C) topology NOW,
# rather than waiting ~120s for the daemon's discovery scan. Uses the daemon's
# register-tools subcommand, which performs the Q64.64/gMath vector computation
# that Lua cannot — so this is the architecture-correct "provision yields a (C)
# entity" path (a pure-Lua composed verb can't compute the vector). Best-effort:
# the periodic scan (or a daemon restart) is the fallback if it doesn't run.
# =============================================================================

if [[ "$DRY_RUN" != "true" ]]; then
    log_step "Step 3.5: Immediate Topology Registration"
    DAEMON_BIN="${GNODE_DAEMON_BIN:-${PROJECT_ROOT}/daemon/target/release/gnode-daemon}"
    [[ -x "$DAEMON_BIN" ]] || DAEMON_BIN="$(command -v gnode-daemon 2>/dev/null || true)"
    DAEMON_CRED="${CREDENTIAL_DIR}/valkey_daemon.password"
    # Register the SITE'S OWN single (C) entity from its capability profile
    # (--profile, default web) — gMath computes the 30-dim vector. NOT a loop of
    # framework components (those are global tool-tier; see register-tools --tier tool).
    if [[ -z "$DAEMON_BIN" ]] || [[ ! -r "$DAEMON_CRED" ]]; then
        log_info "Daemon binary/credential unavailable — service registers on the next daemon scan (~120s) or restart"
    elif VALKEY_USER=gnode_daemon GNODE_REDIS_AUTH_FILE="$DAEMON_CRED" \
            "$DAEMON_BIN" register-tools --tier service --site "$SITE_ID" --profile "$PROFILE" \
            ${PRIMARY_ENV:+--environment "$PRIMARY_ENV"} >/dev/null 2>&1; then
        log_success "Registered '$SITE_ID' now — '$PROFILE'-profile capability vector (C) + snapshot (B)"
    else
        log_info "Immediate registration skipped — service registers on the next daemon scan (~120s) or restart"
    fi
fi

# =============================================================================
# Step 4: Tenant/Owner Group
# =============================================================================

if [[ -n "$OWNER" ]]; then
    log_step "Step 4: Tenant Group Setup"

    if [[ "$DRY_RUN" == "true" ]]; then
        log_dry "FCALL GNODE_UPDATE_SERVICE 0 $SITE_ID '{\"owner\":\"$OWNER\"}'"
    else
        # Set owner in metadata (also updates tenant index via Lua)
        valkey_daemon_cli FCALL GNODE_UPDATE_SERVICE 0 "$SITE_ID" "{\"owner\":\"$OWNER\"}" >/dev/null 2>&1 || {
            log_warning "Could not set owner (site may not be provisioned yet)"
            log_info "Owner will be set on next GNODE_PROVISION_SERVICE call"
        }
        log_success "Tenant group: $OWNER (cross-site discovery enabled)"
    fi
else
    log_step "Step 4: Tenant Group (skipped — no --owner)"
fi

# =============================================================================
# Step 5: COMMS Channel Setup
# =============================================================================

if [[ -n "$NOTIFY_EMAIL" ]]; then
    log_step "Step 5: COMMS Channel Setup"

    # Brace-literal hash-tag — the canonical key format the COMMS daemon
    # reads (Geodineum-COMMS settings/store.rs, fc062a5). Unbraced keys
    # are invisible to dispatch ("Site not found").
    COMMS_KEY="{${SITE_ID}}:comms:config"
    COMMS_JSON=$(build_comms_settings_json "$SITE_ID" "$NOTIFY_EMAIL")

    if [[ "$DRY_RUN" == "true" ]]; then
        log_dry "SET ${COMMS_KEY} <SiteSettings JSON>"
        log_dry "Email channel: ${NOTIFY_EMAIL} via localhost:25"
    else
        if valkey_daemon_cli SET "$COMMS_KEY" "$COMMS_JSON" >/dev/null 2>&1; then
            log_success "COMMS channel configured: email → ${NOTIFY_EMAIL}"
        else
            log_warning "Could not configure COMMS channel"
            log_info "Configure later via wp-admin → Geodineum → Notification Settings"
        fi
    fi
else
    log_step "Step 5: COMMS Channel Setup (skipped — no --notify-email)"
    log_info "Pass --notify-email to configure email notifications"
    log_info "Or configure later via wp-admin → Geodineum → Notification Settings"
fi

# =============================================================================
# Step 6: Schema Discovery + Publication
# =============================================================================

log_step "Step 6: Schema Discovery"

# Derive component root from --yaml path or component directory
SCHEMAS_DIR=""
if [[ -n "$YAML_PATH" ]]; then
    if [[ -d "$YAML_PATH" ]]; then
        SCHEMAS_DIR="$YAML_PATH/config/schemas"
    elif [[ -f "$YAML_PATH" ]]; then
        SCHEMAS_DIR="$(dirname "$YAML_PATH")/config/schemas"
        # If YAML is at root/gnode_services.yaml, schemas is at root/config/schemas
        if [[ ! -d "$SCHEMAS_DIR" ]]; then
            SCHEMAS_DIR="$(dirname "$(dirname "$YAML_PATH")")/config/schemas"
        fi
    fi
fi

if [[ -d "$SCHEMAS_DIR" ]]; then
    SCHEMA_COUNT=0
    SCHEMA_ERRORS=0

    for schema_file in "$SCHEMAS_DIR"/*.yaml "$SCHEMAS_DIR"/*.yml; do
        [[ -f "$schema_file" ]] || continue

        # Skip meta-files (starting with _)
        fname=$(basename "$schema_file")
        [[ "$fname" == _* ]] && continue

        # Extract contract fields from YAML
        contract_name=$(grep -m1 '^\s*name:' "$schema_file" | sed 's/.*name:\s*//' | tr -d '"' | tr -d "'")
        contract_component=$(grep -m1 '^\s*component:' "$schema_file" | sed 's/.*component:\s*//' | tr -d '"' | tr -d "'")

        if [[ -z "$contract_name" || -z "$contract_component" ]]; then
            log_warning "Skipping $fname — missing name or component field"
            SCHEMA_ERRORS=$((SCHEMA_ERRORS + 1))
            continue
        fi

        # Resolve first environment from ENVIRONMENTS JSON array
        FIRST_ENV=$(echo "$ENVIRONMENTS" | python3 -c "import sys,json; print(json.loads(sys.stdin.read())[0])" 2>/dev/null || echo "production")

        # Convert YAML contract to JSON with placeholder resolution
        schema_json=$(python3 -c "
import sys, json, yaml
with open(sys.argv[1]) as f:
    data = yaml.safe_load(f)
contract = data.get('contract', data)
s = json.dumps(contract)
s = s.replace('{site_id}', sys.argv[2])
s = s.replace('{env}', sys.argv[3])
print(s)
" "$schema_file" "$SITE_ID" "$FIRST_ENV" 2>/dev/null)

        if [[ -z "$schema_json" ]]; then
            log_warning "Failed to parse $fname as YAML"
            SCHEMA_ERRORS=$((SCHEMA_ERRORS + 1))
            continue
        fi

        schema_key="${SITE_ID}:gnode:schema:${contract_component}:${contract_name}"

        if [[ "$DRY_RUN" == "true" ]]; then
            log_dry "SET $schema_key <contract JSON>"
            log_dry "SADD ${SITE_ID}:gnode:schema:_index ${contract_component}:${contract_name}"
        else
            if valkey_daemon_cli SET "$schema_key" "$schema_json" >/dev/null 2>&1; then
                valkey_daemon_cli SADD "${SITE_ID}:gnode:schema:_index" "${contract_component}:${contract_name}" >/dev/null 2>&1
                log_success "Published schema: ${contract_component}:${contract_name}"
                SCHEMA_COUNT=$((SCHEMA_COUNT + 1))
            else
                log_warning "Failed to publish schema: $schema_key"
                SCHEMA_ERRORS=$((SCHEMA_ERRORS + 1))
            fi
        fi
    done

    if [[ $SCHEMA_COUNT -gt 0 ]]; then
        log_success "Published $SCHEMA_COUNT schema contract(s) to ValKey"
    fi
    if [[ $SCHEMA_ERRORS -gt 0 ]]; then
        log_warning "$SCHEMA_ERRORS schema(s) had errors"
    fi
    if [[ $SCHEMA_COUNT -eq 0 && $SCHEMA_ERRORS -eq 0 ]]; then
        log_info "No schema files found in $SCHEMAS_DIR (files starting with _ are skipped)"
    fi
else
    log_info "No config/schemas/ directory found (skipping schema publication)"
    log_info "Components can add config/schemas/*.yaml for auto-discovery"
fi

# =============================================================================
# Summary
# =============================================================================

echo ""
echo -e "${BOLD}Onboarding Complete${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [[ "$DRY_RUN" != "true" ]]; then
    echo ""
    echo -e "${BOLD}Verification commands:${NC}"
    echo "  # Test ACL authentication"
    echo "  REDISCLI_AUTH=\"\$(cat $PASSWORD_FILE)\" valkey-cli -p ${VALKEY_PORT:-47445} --user $ACL_USER PING"
    echo ""
    echo "  # Check site registry"
    echo "  $VALKEY_CLI SISMEMBER gnode:sites:registry $SITE_ID"
    echo ""
    if [[ -n "$OWNER" ]]; then
        echo "  # List tenant group sites"
        echo "  $VALKEY_CLI FCALL GNODE_TENANT_LIST_SITES 0 $OWNER"
        echo ""
    fi
    if [[ -n "$NOTIFY_EMAIL" ]]; then
        echo "  # Check COMMS configuration"
        echo "  $VALKEY_CLI GET '{${SITE_ID}}:comms:config'"
        echo ""
    fi
    echo "  # Services will appear in topology within 120s (daemon scan interval)"
    echo "  # Or restart the daemon for immediate discovery:"
    echo "  #   sudo systemctl restart gnode-daemon"
fi
