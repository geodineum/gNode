# gNode Service Management Guide

Complete guide for managing services in the Geodineum Service Daemon (gNode).

## Overview

gNode manages **services** - stateless applications that use ValKey for state persistence. Services can be:
- WordPress sites
- API endpoints
- Inference nodes
- Any application using the gNode client

Each service gets:
- Isolated keyspace (`{service_id}:*`)
- DTAP environment streams (unified + health)
- ACL user with restricted permissions
- Metadata and configuration storage

## Quick Reference

### Shell Scripts

```bash
# Register a new service
./scripts/register-site.sh my_service

# Preview what deregistration would delete
./scripts/deregister-service.sh my_service --dry-run

# Completely remove a service
./scripts/deregister-service.sh my_service --remove-acl --force
```

### Lua Functions (FCALL)

```bash
# Create
FCALL GNODE_PROVISION_SERVICE 0 my_service

# Read
FCALL GNODE_SERVICE_GET 0 my_service '{"include_stream_info":true}'
FCALL GNODE_SERVICE_LIST 0 '{"status":"active"}'

# Update
FCALL GNODE_UPDATE_SERVICE 0 my_service '{"status":"maintenance"}'

# Delete
FCALL GNODE_DEPROVISION_SERVICE 0 my_service '{"dry_run":true}'
```

---

## Service Lifecycle

### 1. Provisioning (Create)

Register a new service with streams, ACL user, and registry entry.

#### Using Shell Script (Recommended)

```bash
# Full provisioning with ACL user
./scripts/register-site.sh my_service

# Specify environments
./scripts/register-site.sh my_service --environments '["production","staging"]'

# ACL only (no streams)
./scripts/register-site.sh my_service --acl-only

# Streams only (ACL exists)
./scripts/register-site.sh my_service --streams-only

# Preview without changes
./scripts/register-site.sh my_service --dry-run
```

#### Using Lua Function

```bash
# Basic provisioning (all 4 DTAP environments)
FCALL GNODE_PROVISION_SERVICE 0 my_service

# Custom environments
FCALL GNODE_PROVISION_SERVICE 0 my_service '["production","staging"]'

# With custom topology namespace
FCALL GNODE_PROVISION_SERVICE 0 my_service '["production"]' my_namespace
```

**What Gets Created:**

| Resource | Key Pattern | Count |
|----------|-------------|-------|
| Unified streams | `{service_id}:gnode:unified:{env}` | 1 per environment |
| Health stream | `{service_id}:gnode:health` | 1 |
| Metadata | `gnode:site:{service_id}:meta` | 1 |
| Registry entry | `gnode:sites:registry` (SET) | 1 member |
| ACL user | `gnode_client_{service_id}` | 1 |
| Password file | `/etc/geodineum/credentials/valkey_client_{service_id}.password` | 1 |

---

### 2. Reading Service Information

#### Get Single Service

```bash
# Basic info
FCALL GNODE_SERVICE_GET 0 my_service

# With stream statistics
FCALL GNODE_SERVICE_GET 0 my_service '{"include_stream_info":true}'
```

**Response:**
```json
{
  "service_id": "my_service",
  "status": "active",
  "active_environment": "production",
  "environments": ["testing", "staging", "acceptance", "production"],
  "topology_namespace": "geodineum",
  "created_at": 1769469354,
  "streams": {
    "production": {"key": "{my_service}:gnode:unified:production", "exists": true, "length": 1523},
    "health": {"key": "{my_service}:gnode:health", "exists": true, "length": 45}
  }
}
```

#### List All Services

```bash
# All services
FCALL GNODE_SERVICE_LIST 0

# Filter by status
FCALL GNODE_SERVICE_LIST 0 '{"status":"active"}'
FCALL GNODE_SERVICE_LIST 0 '{"status":"maintenance"}'

# Filter by active environment
FCALL GNODE_SERVICE_LIST 0 '{"environment":"production"}'

# Minimal output (no metadata)
FCALL GNODE_SERVICE_LIST 0 '{"include_meta":false}'
```

---

### 3. Updating Services

#### Update Status

```bash
# Set to maintenance mode
FCALL GNODE_UPDATE_SERVICE 0 my_service '{"status":"maintenance"}'

# Set to inactive
FCALL GNODE_UPDATE_SERVICE 0 my_service '{"status":"inactive"}'

# Restore to active
FCALL GNODE_UPDATE_SERVICE 0 my_service '{"status":"active"}'
```

**Valid Status Values:**
- `active` - Normal operation, daemon processes messages
- `inactive` - Service disabled, daemon ignores messages
- `maintenance` - Maintenance mode, broadcasts notification

#### Change Active Environment

```bash
# Switch to staging
FCALL GNODE_UPDATE_SERVICE 0 my_service '{"active_environment":"staging"}'

# Switch to production
FCALL GNODE_UPDATE_SERVICE 0 my_service '{"active_environment":"production"}'
```

The daemon will automatically switch to listening on the new environment's unified stream.

#### Update Metadata

```bash
# Set description
FCALL GNODE_UPDATE_SERVICE 0 my_service '{"description":"Main production site"}'

# Set owner
FCALL GNODE_UPDATE_SERVICE 0 my_service '{"owner":"team-backend"}'

# Set display name
FCALL GNODE_UPDATE_SERVICE 0 my_service '{"display_name":"My Service"}'

# Set tags (array)
FCALL GNODE_UPDATE_SERVICE 0 my_service '{"tags":["wordpress","production","critical"]}'

# Multiple fields at once
FCALL GNODE_UPDATE_SERVICE 0 my_service '{
  "status": "active",
  "description": "Main site",
  "owner": "team-backend",
  "tags": ["wordpress", "production"]
}'

# Custom fields (stored with custom_ prefix)
FCALL GNODE_UPDATE_SERVICE 0 my_service '{"priority":"high","region":"us-east-1"}'
# Stored as: custom_priority, custom_region
```

---

### 4. Environment Management

#### Add New Environment

Add a DTAP environment to an existing service (creates the unified stream).

```bash
# Add staging environment
FCALL GNODE_SERVICE_ADD_ENVIRONMENT 0 my_service staging

# Add and set as active
FCALL GNODE_SERVICE_ADD_ENVIRONMENT 0 my_service staging true
```

**Response:**
```json
{
  "success": true,
  "service_id": "my_service",
  "environment": "staging",
  "stream_key": "{my_service}:gnode:unified:staging",
  "created_streams": ["{my_service}:gnode:unified:staging"],
  "created_groups": ["{my_service}:gnode:unified:staging:gnode-daemon"],
  "set_active": true,
  "all_environments": ["production", "staging"]
}
```

#### Remove Environment

Remove a DTAP environment (deletes only that environment's stream).

```bash
# Remove testing environment
FCALL GNODE_SERVICE_REMOVE_ENVIRONMENT 0 my_service testing

# Force remove active environment (will switch to another)
FCALL GNODE_SERVICE_REMOVE_ENVIRONMENT 0 my_service production '{"force":true}'
```

**Note:** Cannot remove the active environment without `force:true`. When forced, automatically switches to another available environment.

---

### 5. Deprovisioning (Delete)

Completely remove a service and all its data.

#### Preview First (Recommended)

```bash
# Using script
./scripts/deregister-service.sh my_service --dry-run

# Using Lua function
FCALL GNODE_DEPROVISION_SERVICE 0 my_service '{"dry_run":true}'
```

#### Full Removal

```bash
# Remove service data only (keep ACL user)
./scripts/deregister-service.sh my_service

# Remove everything including ACL user
./scripts/deregister-service.sh my_service --remove-acl

# Force without confirmation
./scripts/deregister-service.sh my_service --remove-acl --force

# Keep cache keys (only remove streams and registry)
./scripts/deregister-service.sh my_service --keep-cache
```

**What Gets Deleted:**

| Resource | Key Pattern |
|----------|-------------|
| Registry entry | `gnode:sites:registry` (SREM) |
| Metadata | `gnode:site:{service_id}:meta` |
| Unified streams | `{service_id}:gnode:unified:*` |
| Health stream | `{service_id}:gnode:health` |
| Broadcast stream | `{service_id}:gnode:broadcast` |
| Cache keys | `{service_id}:cache:*` |
| Rate limit keys | `{service_id}:ratelimit:*` |
| Circuit breaker | `{service_id}:circuit:*` |
| Metrics | `{service_id}:metrics` |
| All namespaced keys | `{service_id}:*` |
| ACL user | `gnode_client_{service_id}` (if --remove-acl) |
| Password file | `/etc/geodineum/credentials/valkey_client_{service_id}.password` |

**Daemon Behavior:** The daemon automatically detects removed streams and stops listening (no restart required).

---

## Best Practices

### Service Naming

- Use lowercase with underscores: `my_service`, `api_gateway`
- Include environment in name if needed: `staging_mysite`
- Avoid special characters (only `a-z`, `0-9`, `_`)

### Environment Strategy

```
Development:  testing environment
QA/Staging:   staging environment
Pre-prod:     acceptance environment
Production:   production environment
```

### Maintenance Workflow

```bash
# 1. Set maintenance mode (notifies clients)
FCALL GNODE_UPDATE_SERVICE 0 my_service '{"status":"maintenance"}'

# 2. Perform maintenance...

# 3. Restore to active
FCALL GNODE_UPDATE_SERVICE 0 my_service '{"status":"active"}'
```

### Safe Deregistration

```bash
# 1. Set inactive first (stops processing)
FCALL GNODE_UPDATE_SERVICE 0 my_service '{"status":"inactive"}'

# 2. Preview deletion
./scripts/deregister-service.sh my_service --dry-run

# 3. Confirm and delete
./scripts/deregister-service.sh my_service --remove-acl
```

---

## API Reference

### GNODE_PROVISION_SERVICE

Creates a new service with streams and registry entry.

```
FCALL GNODE_PROVISION_SERVICE 0 <service_id> [environments_json] [topology_namespace]
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| service_id | string | required | Unique service identifier |
| environments_json | JSON array | `["testing","staging","acceptance","production"]` | DTAP environments |
| topology_namespace | string | `"geodineum"` | Namespace for shared streams |

### GNODE_SERVICE_GET

Gets complete service information.

```
FCALL GNODE_SERVICE_GET 0 <service_id> [options_json]
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| include_stream_info | boolean | false | Include stream lengths and existence |

### GNODE_SERVICE_LIST

Lists services with optional filtering.

```
FCALL GNODE_SERVICE_LIST 0 [options_json]
```

| Option | Type | Description |
|--------|------|-------------|
| status | string | Filter by status (active/inactive/maintenance) |
| environment | string | Filter by active environment |
| include_meta | boolean | Include metadata (default: true) |

### GNODE_UPDATE_SERVICE

Updates service metadata and status.

```
FCALL GNODE_UPDATE_SERVICE 0 <service_id> <updates_json>
```

| Field | Type | Valid Values |
|-------|------|--------------|
| status | string | active, inactive, maintenance |
| active_environment | string | testing, staging, acceptance, production |
| topology_namespace | string | any string |
| description | string | any string |
| display_name | string | any string |
| owner | string | any string |
| tags | array | array of strings |
| (custom) | any | stored with `custom_` prefix |

### GNODE_SERVICE_ADD_ENVIRONMENT

Adds a DTAP environment to an existing service.

```
FCALL GNODE_SERVICE_ADD_ENVIRONMENT 0 <service_id> <environment> [set_active]
```

| Parameter | Type | Description |
|-----------|------|-------------|
| service_id | string | Service identifier |
| environment | string | testing/staging/acceptance/production |
| set_active | string | "true" to also set as active environment |

### GNODE_SERVICE_REMOVE_ENVIRONMENT

Removes a DTAP environment from a service.

```
FCALL GNODE_SERVICE_REMOVE_ENVIRONMENT 0 <service_id> <environment> [options_json]
```

| Option | Type | Description |
|--------|------|-------------|
| force | boolean | Allow removing active environment |

### GNODE_DEPROVISION_SERVICE

Completely removes a service.

```
FCALL GNODE_DEPROVISION_SERVICE 0 <service_id> [options_json]
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| dry_run | boolean | false | Preview only, no changes |
| include_cache | boolean | true | Delete cache/rate-limit keys |
| include_all_namespaced | boolean | true | Delete all `{service_id}:*` keys |

---

## Troubleshooting

### Service Not Found

```bash
# Check if service is in registry
FCALL GNODE_SERVICE_LIST 0

# Check for orphaned keys
./scripts/valkey-cli-secure.sh KEYS "{my_service}:*"
```

### Daemon Not Listening to Service

```bash
# Check daemon logs
journalctl -u gnode-daemon -f

# Verify streams exist
FCALL GNODE_SERVICE_GET 0 my_service '{"include_stream_info":true}'

# Check active environment matches daemon config
FCALL GNODE_SERVICE_GET_ENVIRONMENT 0 my_service
```

### Stale Streams After Deletion

The daemon automatically detects NOGROUP errors and removes stale streams. If issues persist:

```bash
# Restart daemon to force stream refresh
sudo systemctl restart gnode-daemon
```

### ACL Permission Denied

```bash
# Check ACL user exists
./scripts/valkey-cli-secure.sh ACL LIST | grep gnode_client_my_service

# Repair ACL
./scripts/repair-site-acl.sh my_service
```

---

## Related Documentation

- [CONFIGURATION.md](CONFIGURATION.md) - Configuration reference
- [SYSTEMD_SERVICES.md](SYSTEMD_SERVICES.md) - Service management
- [../valkey/VALKEY_ACL_NUANCES.md](../valkey/VALKEY_ACL_NUANCES.md) - ACL configuration and nuances
