# gNode ValKey Functions

This directory contains 22 Lua function libraries (237+ functions) loaded into ValKey
via `./scripts/load-valkey-functions.sh`. Functions are first-class ValKey artifacts:
persisted, replicated, and callable via `FCALL` from any language.

## Loading

```bash
# Load every library discovered in base + registered extension dirs
./scripts/load-valkey-functions.sh
```

The script scans this directory plus any extension `functions/`
directories registered via `GNODE_EXT_<NAME>_PATH` or `GNODE_EXT_DIR`
(see the top-level README's Extension System section).

## Base Libraries (22)

### Core & Utilities
| Library | Functions | Purpose |
|---------|-----------|---------|
| `gnode_core` | 9 | Core utilities |
| `gnode_utils` | 8 | Key building, metrics, validation |
| `gnode_hash` | 7 | Hash field operations |
| `gnode_lock` | 4 | Distributed locking |
| `gnode_transaction` | 5 | Atomic multi-key operations |

### Discovery & Topology
| Library | Functions | Purpose |
|---------|-----------|---------|
| `gnode_geometric` | 5 | O(1) spatial-hash discovery |
| `gnode_topology` | 28 | 23D semantic topology |
| `gnode_topo` | 18 | Topology CRUD + traversal |
| `gnode_node` | 12 | Node registration + metrics |
| `gnode_site` | 9 | Site management + discovery |

### Streaming & Messaging
| Library | Functions | Purpose |
|---------|-----------|---------|
| `gnode_stream` | 19 | Stream ops + consumer groups |
| `gnode_protocol` | 6 | JSON/RESP3 encoding |
| `gnode_pubsub` | 5 | Channel-based pub/sub |
| `gnode_broadcast` | 4 | Pub-sub without consumer groups |
| `gnode_group` | 9 | Application-level consumer groups |
| `gnode_direct` | 5 | Direct channel provisioning |

### Data & Caching
| Library | Functions | Purpose |
|---------|-----------|---------|
| `gnode_cache` | 9 | Cache operations with TTL |
| `gnode_batch` | 11 | Batch multi-key operations |
| `gnode_batch_resp3` | 3 | RESP3 batch responses |

### Operations
| Library | Functions | Purpose |
|---------|-----------|---------|
| `gnode_config` | 11 | Configuration management |
| `gnode_monitoring` | 7 | System monitoring |
| `gnode_resilience` | 13 | Circuit breaker, idempotency, leader election |

## Optional Extension Libraries

Additional libraries ship via signed extension bundles. They are
discovered from `$GNODE_EXT_DIR` and loaded by the same script after
the base libraries. See the top-level README's Extension System section
for the discovery and signing protocol.

Companion (unsigned) extension: [geodineum/gNode-CMS](https://github.com/geodineum/gNode-CMS)
 — `gnode_asset` (10 functions).

## Calling Functions

```bash
# CLI
./scripts/valkey-cli-secure.sh FCALL GNODE_CACHE_GET 1 "mysite:cache" "user:123"

# Read-only variant (safe for replicas)
./scripts/valkey-cli-secure.sh FCALL_RO GNODE_TOPO_GET_ENTITY 1 "{mysite}:gnode:services" "MyService"
```

## Conventions

- All functions use `server.*` API (ValKey 7.2+), never `redis.*`
- All `cjson.encode`/`cjson.decode` calls wrapped in `pcall`
- Keys follow `{site_id}:namespace:key` pattern for ACL isolation
- See `CLAUDE.md` section 8 for full reference
