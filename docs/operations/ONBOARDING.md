# Third-Party Service Onboarding Guide

How to register your service with gNode so it's discoverable in the geometric topology mesh.

## Quick Start

```bash
# 1. Create your service definition
cat > /opt/my-service/gnode_services.yaml << 'EOF'
services:
  - id: "MyInferenceService"
    metadata:
      description: "GPU inference endpoint for image classification"
      type: "service"
      tier: "SERVICE"
    capabilities:
      - name: "protocol"
        value: "http_rest"
      - name: "domain_primary"
        value: "inference"
      - name: "reliability_tier"
        value: "high"
EOF

# 2. Onboard (creates ACL user, registers discovery path)
./scripts/onboard-service.sh my_inference --yaml /opt/my-service

# 3. Services appear in topology within 120s (or restart daemon)
```

## What Gets Created

| Component | Description | Key/Path |
|-----------|-------------|----------|
| **ACL user** | ValKey credentials for stream communication | `gnode_client_{service_id}` |
| **Password file** | 64-char hex, readable by PHP via group | `/etc/geodineum/credentials/valkey_client_{service_id}.password` |
| **Streams** | Unified (per DTAP env) + health (full mode) | `{service_id}:gnode:unified:{env}` |
| **Registry** | Membership in site registry | `gnode:sites:registry` SET |
| **Discovery path** | Added to daemon's scan manifest | `discovery-paths.conf` |
| **Tenant group** | Cross-site discovery index (if `--owner`) | `gnode:tenant:{owner}:sites` SET |

## YAML Service Definition Format

### Minimal (3 capabilities)

```yaml
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
      - name: "reliability_tier"
        value: "high"
```

### Full (all dimensions)

```yaml
services:
  - id: "MyService"
    metadata:
      class: "com.example.MyService"
      description: "Full-featured service with all dimensions"
      type: "service"
      tier: "SERVICE"
    capabilities:
      # Layer 1: Interface Identity
      - name: "protocol"
        value: "http_rest"           # http_rest|graphql|grpc|websocket|gnode_stream|resp3_direct
      - name: "native_format"
        value: "json"                # json|xml|protobuf|msgpack|yaml|plaintext
      - name: "api_version"
        value: 0.5                   # 0.0-1.0 (normalized semver)
      - name: "contract_stability"
        value: "stable"              # experimental|alpha|beta|stable|frozen
      # Layer 2: Access Control
      - name: "clearance_required"
        value: "authenticated"       # public|anonymous|authenticated|privileged|admin|system
      - name: "auth_method"
        value: "token"               # none|api_key|basic|token|oauth2|mtls|certificate|saml
      - name: "data_sensitivity"
        value: "internal"            # public|internal|confidential|restricted|secret
      # Layer 3: Scope
      - name: "service_scope"
        value: "internal_api"        # public_web|public_api|partner_api|internal_api|daemon|system
      # Layer 4: Domain
      - name: "domain_primary"
        value: "inference"           # platform|content|security|commerce|inference|analytics|...
      - name: "domain_secondary"
        value: "compute"             # Same values as primary
      - name: "specialization"
        value: "specialist"          # generalist|multi_domain|specialist|niche
      # Layer 5: Performance
      - name: "throughput_tier"
        value: "enterprise"          # hobby|starter|professional|enterprise|unlimited
      - name: "latency_class"
        value: "interactive"         # realtime|interactive|standard|batch|offline
      - name: "reliability_tier"
        value: "high"                # best_effort|standard|high|critical|mission_critical
      # Layer 6: Workflow
      - name: "pipeline_stage"
        value: "processor"           # ingress|validator|transformer|processor|enricher|egress
      - name: "execution_priority"
        value: "normal"              # background|low|normal|high|critical|emergency
      # Layer 8: Classification
      - name: "service_tier"
        value: "standard"            # free|basic|standard|advanced|enterprise|unlimited
      - name: "environment"
        value: "production"          # development|testing|staging|production|disaster_recovery
```

Dimension values are mapped to 0.0-1.0 coordinates by the active tier schema (`service_schema.yaml` for the default service tier; `tool_schema.yaml` / `constellation_schema.yaml` / `galaxy_schema.yaml` for those tiers). Unspecified dimensions default to 0.0. Custom topologies declare their own dim metadata at creation time via `topo_create` or the `gNode-TOPO` extension.

## Two Integration Paths

### Path A: gCore Framework

If your application uses the Geodineum gCore framework:

1. Install gNode-Client via Composer: `composer require geodineum/gnode-client`
2. Configure in your app's `.env` or `wp-config.php`:
   ```php
   define('GNODE_CLIENT_SITE_ID', 'my_inference');
   define('GNODE_CLIENT_PASSWORD_FILE', '/etc/geodineum/credentials/valkey_client_my_inference.password');
   ```
3. Use gCore managers (CacheManager, StateManager, etc.) — they communicate via gNode streams automatically.
4. Your services are registered by the daemon from `geometric_topology.yaml`.

### Path B: Direct gNode Integration

No framework required. Communicate directly via ValKey streams or Lua FCALL:

1. **Stream-based**: XADD commands to `{service_id}:gnode:unified:{env}`, XREADGROUP for responses
2. **Lua FCALL**: 270+ functions for cache, topology, resilience, tracing, etc.
3. **RESP3**: Native ValKey protocol for lowest latency

Example (stream command):
```bash
# Send a geometric discover command
REDISCLI_AUTH="$(cat /etc/geodineum/credentials/valkey_client_my_inference.password)" \
  valkey-cli -p 47445 --user gnode_client_my_inference \
  XADD my_inference:gnode:unified:production '*' \
    id "cmd-1" cmd "geometric_discover" \
    params '{"capabilities":{"protocol":"http_rest","domain_primary":"inference"},"limit":5}'
```

See: `docs/reference/FCALL_COOKBOOK.md` for the polyglot integration reference.

## Onboarding Modes

### Minimal (`--mode minimal`)

Creates only what's needed for service discovery:
- ACL user with stream + FCALL permissions
- Site registry entry
- Discovery path (if `--yaml` provided)

**No streams created.** Use this when:
- Your service only needs to be discoverable (not communicate via streams)
- You'll create streams later
- You're adding an existing site to the topology

### Full (`--mode full`, default)

Everything in minimal, plus:
- Unified streams for all DTAP environments (`{service_id}:gnode:unified:{testing|staging|acceptance|production}`)
- Health stream (`{service_id}:gnode:health`)
- Consumer groups (`gnode-daemon`, `gnode-client`)
- Shared broadcast/registration streams (idempotent)

## Multi-Tenant Grouping

Services under the same owner can discover each other across site boundaries:

```bash
# Onboard two sites under the same owner
./scripts/onboard-service.sh staging_my_app --owner acme --yaml /opt/acme/staging
./scripts/onboard-service.sh my_app --owner acme --yaml /opt/acme/production
```

### Query tenant group

```bash
# List all sites in tenant group
./scripts/valkey-cli-secure.sh FCALL GNODE_TENANT_LIST_SITES 0 acme
# → {"owner":"acme","sites":["staging_my_app","my_app"],"count":2}

# Discover services across all tenant sites
./scripts/valkey-cli-secure.sh FCALL GNODE_TENANT_DISCOVER 0 acme '{"protocol":"http_rest"}' 10
# → {"owner":"acme","results":[...services from both sites...],"total":N,"sites_queried":2}
```

**Security**: Cross-site queries run at daemon level. PHP clients are still ACL-isolated to their own `{service_id}:*` keyspace. They send commands to their stream, the daemon executes the cross-site query, and returns results.

## Decommissioning

```bash
# Remove a service completely (streams + keys + registry)
./scripts/deregister-service.sh my_inference --remove-acl

# Preview what would be removed
./scripts/deregister-service.sh my_inference --dry-run
```

Tenant group cleanup happens automatically during deprovisioning.

## Troubleshooting

### ACL verification
```bash
REDISCLI_AUTH="$(cat /etc/geodineum/credentials/valkey_client_my_service.password)" \
  valkey-cli -p 47445 --user gnode_client_my_service PING
# Expected: PONG
```

### Check site is registered
```bash
./scripts/valkey-cli-secure.sh SISMEMBER gnode:sites:registry my_service
# Expected: (integer) 1
```

### Discovery not picking up YAML
1. Verify path is in discovery manifest: `cat /etc/geodineum/components/gnode-daemon/discovery-paths.conf`
2. Verify YAML has `services:` key with at least one entry
3. Wait up to 120s for next scan cycle, or `sudo systemctl restart gnode-daemon`
4. Check logs: `journalctl -u gnode-daemon -f | grep service-discovery`

### Common errors
- **"Admin password file not found"**: The `default` user password must exist at `/etc/geodineum/credentials/valkey.password`
- **"NOPERM"**: ACL user lacks permissions. Run `./scripts/repair-site-acl.sh {service_id}`
- **"BUSYGROUP"**: Consumer group already exists — this is fine (idempotent)
