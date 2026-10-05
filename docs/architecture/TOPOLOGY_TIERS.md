# Geodineum 4-Tier Topology Architecture

> **Status**: service tier at schema v4.0 (23D). Header counts corrected
> 2026-10-05 — the zone detail below was already v4.0 while the summary still
> read 30D.

## Overview

Geodineum uses geometric service discovery across 4 topology tiers. Each tier has its own dimension schema tailored to its query patterns, but all share the same spatial hash infrastructure (Lua + Rust + ValKey).

```
Galaxy (20D)        — federation of Geodineum Constellations
  └── Constellation (20D) — nodes within a WireGuard VPN cluster
        └── Service (23D)     — services within a site (per-site)
              └── Tool (16D)      — base ecosystem components (global)
```

## Tier Summary

| Tier | Dims | Discovery | Storage | Keyspace | Schema |
|------|------|-----------|---------|----------|--------|
| Service | 23 | 19 (16 hashed) | 4 | `{service_id}:gnode:services:*` | `service_schema.yaml` |
| Tool | 16 | 12 | 4 | `{ecosystem}:gnode:services:*` | `tool_schema.yaml` |
| Constellation | 20 | 16 | 4 | `constellation:{cid}:topology:*` | `constellation_schema.yaml` |
| Galaxy | 20 | 16 | 4 | `galaxy:topology:*` | `galaxy_schema.yaml` |

## ValKey Storage Format

Each topology stores data in distributed keys (NOT a single JSON blob):

```
{service_id}:gnode:services:entities    — HASH: entity_id → entity JSON
{service_id}:gnode:services:z_order     — ZSET: entity_id scored by dim 16
{service_id}:gnode:services:meta        — HASH: topology metadata
{service_id}:gnode:services:voxel:{bk}  — SET: entity IDs per spatial bucket
```

## Service Tier — 23D

The production topology for per-site service discovery. 16 declared dimensions are hashed into the spatial bucket key; 16-18 are derived by the daemon and ranked but never hashed, so a measurement that moves never rewrites voxel membership; 19-22 are storage-only and refused in a query.

**Zones** (an axis's index IS its zone; both cuts are prefix truncations —
`hashed_dimensions` 16, `discovery_dimensions` 19, `total_dimensions` 23):

*Declared by the provider, hashed into the bucket key*
- 0-2: Interface Identity (protocol, api_version, contract_stability)
- 3-5: Access Control (clearance_required, auth_method, data_sensitivity)
- 6-9: Scope and Domain (service_scope, domain_primary, domain_secondary, specialization)
- 10-12: Declared service levels (throughput_tier, latency_class, reliability_tier)
- 13-15: Workflow and placement (pipeline_stage, execution_priority, environment)

*Derived by the daemon, ranked in a query, never hashed*
- 16-18: current_load, health_status (`writer: sampler`), lifecycle_state
  (`writer: daemon`). Code 0.00 is `unknown`, never a real state.

*Storage: stored and returned, refused in a query*
- 19-22: native_format, implementation_language, data_persistence, service_tier

Dropped in v4.0 (30D → 23D): `network_zone` and `data_residency` live at the
constellation tier, where the fact belongs and a service inherits its node's;
`registration_order` duplicated `m.ro` and held values its own enum could not
name; `user_x/y/z` duplicated the projection the topology meta already declares
from the real axes; `update_channel` and `deployment_model` were never written.

**Registration**: `gnode-daemon register-tools --tier service --site {service_id}`

## Tool Tier — 16D

Static registry of the ~10 base ecosystem components. Visual coordinates (12-14) encode a dependency pyramid computed from the `depends_on` graph.

**Pyramid hierarchy** (z=1.0 at top):
```
Level 0: gMath, ValKey                    (z=1.00)
Level 1: gNode, gNode-Client              (z=0.75)
Level 2: gNode-CMS, gCore, COMMS, BAK     (z=0.50)
Level 3: gTemplate                         (z=0.25)
Level 4: gCube                             (z=0.00)
```

**Layers:**
- 0-3: Interface (protocol, native_format, api_version, contract_stability)
- 4: Domain (domain_primary)
- 5-8: Classification (implementation_language, build_type, component_tier, license_tier)
- 9-10: Dependency (requires_valkey, systemd_managed)
- 11: Security (data_sensitivity)
- 12-14: Visual — storage-only (pyramid_x, pyramid_y, pyramid_z)
- 15: Metadata — storage-only (registration_order)

**Registration**: `gnode-daemon register-tools --tier tool`
**Data source**: `Geodineum/config/ecosystem_tools.yaml`

## Constellation Tier — 20D (planned)

Node-level management within a WireGuard VPN constellation. Not yet wired to registration pipeline.

**Key dimensions**: node_role, valkey_mode, cpu_tier, memory_tier, aggregate_load, node_health, data_residency, specialization.

## Galaxy Tier — 20D (Future)

Federation-level discovery of Geodineum Constellations. Schema exists as design artifact.

**Key dimensions**: offering_primary, trust_level, pricing_tier, constellation_scale, geography_region, availability_sla.

## Evolution Labels

Services receive evolutionary labels based on relative Stream Contribution Score (SCS):

| Percentile | Label | Meaning |
|-----------|-------|---------|
| 0-15% | Claim | Newly registered, minimal activity |
| 15-40% | Outpost | Low but consistent activity |
| 40-70% | Settlement | Steady contributor |
| 70-90% | City | High contributor |
| 90-99% | Metropole | Major hub |
| Top 1 | Capital | Highest ecosystem contributor |

Labels are **schema-driven** — defined in `daemon/config/evolution_schema.yaml`. The Lua computation function has been removed; nothing currently computes these labels at runtime, the schema is retained as the label definition. Purely display — no operational impact.

## Cross-Tier Shared Values

`shared_values.yaml` maintains identical value→coordinate mappings for concepts that appear across tiers (version, contract_stability, data_residency, aggregate_load, registration_order).

## DTAP Environment Detection

`dtap_schema.yaml` maps domain prefixes to environments:
- `staging.example.com` → staging (ViewKey-gated)
- `dev.example.com` → development (ViewKey-gated)
- `example.com` → production (public)

Schema-driven with i18n aliases (Dutch: `acceptatie` → acceptance).

## Registration CLI

```bash
# Register all base components (tool pyramid)
gnode-daemon --redis-user gnode_daemon register-tools --tier tool

# Register gCore managers for a site (service topology)
gnode-daemon --redis-user gnode_daemon register-tools --tier service --site example_site

# Dry run (preview without writing)
gnode-daemon --redis-user gnode_daemon register-tools --tier tool --dry-run
```

## Files

| File | Purpose |
|------|---------|
| `daemon/config/service_schema.yaml` | Service tier 23D dimension definitions |
| `daemon/config/tool_schema.yaml` | Tool tier 16D dimension definitions |
| `daemon/config/constellation_schema.yaml` | Constellation tier 20D (planned) |
| `daemon/config/galaxy_schema.yaml` | Galaxy tier 20D (future) |
| `daemon/config/shared_values.yaml` | Cross-tier value mappings |
| `daemon/config/evolution_schema.yaml` | Lifecycle label configuration |
| `daemon/config/dtap_schema.yaml` | DTAP prefix→environment mapping |
| ~~`daemon/config/capability_schema.yaml`~~ | **REMOVED** — was the legacy 23D schema, replaced by `service_schema.yaml` pre-launch |
| `daemon/functions/gnode_topology.lua` | Dimension tables + translation |
| `daemon/functions/gnode_topo.lua` | Topology CRUD + evolution labels |
| `daemon/src/tool_registration.rs` | Registration pipeline + pyramid layout |
