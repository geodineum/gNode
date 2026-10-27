# gNode Command & Function Reference

Definitive reference for all daemon commands and ValKey Lua functions.

**Base daemon**: 39 commands, 23 Lua libraries (210 functions)
**With CMS extension** (default companion; opt out with `GEODINEUM_SKIP_CMS=true`): 58 commands, 24 Lua libraries (220 functions)

> Counting basis — *commands* = one canonical command per row in Part 1
> (the uppercase / camelCase / short forms are aliases, not separate
> commands); *functions* = one registered `FCALL` entry per row in Part 2
> (Σ `function_name =` across `daemon/functions/gnode_*.lua`); *libraries*
> = `gnode_*.lua` files. Additional signed extensions are commercial and
> documented separately — they are not part of this distribution.

---

## Message Format

The gNode wire protocol uses RESP3 stream entries with abbreviated field
names. **Every parser in the daemon** (RESP3 stream parser, script-format
parser, key-based compute reader) resolves fields through the same
canonical alias list in `daemon/src/utils.rs::field_names` — one wire
format, one contract, every path parses identically.

An entry whose type field (`t`/`type`) resolves to nothing is not a
command and not a response: it is ACKed and dropped, and the daemon
logs a rate-limited warning (one line per 30s with a suppressed count)
naming the stream, entry id, and the field keys present. Malformed
producers are visible in the journal, never silent.

### Canonical fields

| Field | Used in     | Alias chain (preferred → fallback)    | Meaning |
|-------|-------------|---------------------------------------|---------|
| `t`   | all         | `t`, `type`                          | Message type — `c` (command), `r` (response), `bc`/`br`/`b` (batch), `i` (init) |
| `id`  | all         | `id`, `request_id`                   | Unique request id; daemon writes the response to `{ss}:res:{id}` |
| `c`   | `t=c`       | `c`, `cmd`, `command`, `command_name`| Command name |
| `p`   | `t=c`       | `p`, `params`, `parameters`          | JSON-encoded parameters |
| `ss`  | `t=c`/`t=r` | `ss`, `source_site`, `service_id`, `site_id`, `st`*| Source service (message writer's `service_id`; `site_id` is a legacy alias) |
| `sn`  | `t=c`/`t=r` | `sn`, `source_node`, `node_id`, `n`* | Source node (message writer's node_id) |
| `ds`  | `t=c`/`t=r` | `ds`, `dest_site`                    | Destination site (daemon echoes this on responses) |
| `dn`  | `t=c`/`t=r` | `dn`, `dest_node`                    | Destination node |
| `st`  | `t=r`       | `st`, `s`, `status`                  | Response status — `ok` or `error` |
| `r`   | `t=r`       | `r`, `result`                        | Response result (JSON) |
| `e`   | `t=r`       | `e`, `error`                         | Error message (when `st=error`) |
| `ri`  | `t=r`       | `ri`                                 | Request id this response replies to |
| `ts`  | all         | `ts`, `timestamp`                    | Timestamp in milliseconds (**not** `t` — collides with type) |
| `bi`  | `t=b*`      | `bi`                                 | Batch id |
| `tc`  | `t=b*`      | `tc`                                 | Total count of messages in batch |
| `pz`  | any entry   | `pz`, `persist`, `persistence`       | Persistence mark — `1` STORE keeps the entry, `0` never kept (beats a detector), absent = detectors decide |

\* `st` and `n` as aliases for source-site / source-node are accepted
for backwards compatibility only. New writers must use `ss` / `sn`. The
field name `st` also means STATUS in response messages — disambiguation
is by message type (`t=c` reads `st` as service_id, `t=r` reads it as status).

### Command (write)

```
XADD {service_id}:gnode:unified:{environment} *
  t   "c"
  id  "<unique-command-id>"
  c   "<command-name>"
  p   '<json-parameters>'
  ss  "<source-site-id>"
  sn  "<source-node-id>"
  ts  "<milliseconds>"
```

### Response (daemon writes)

The daemon writes both a stream response **and** a polling key the
client can `GET` directly (the polling key is the primary path for PHP
clients):

```
SET {ss}:res:{request_id} '<json>' EX 10
```

Where `<json>` = `{"id":..., "status":"ok|error", "result":..., "error":..., "timestamp":...}`.

The `request_id` is taken from `_request_id` inside the parameters
object (the key `pollForResponse` writes and polls on); if that is
absent it falls back to the top-level `id` field. The Concurrent and Ordered
lanes resolve it identically (one shared implementation:
`integration/response_key.rs`), so either form returns a polling response.
An empty-string `_request_id` is treated as absent — it falls through to the
top-level `id` rather than producing the shared key `{site}:res:`
— a message carrying only a top-level `id` (no `_request_id`) is written
to `{ss}:res:{id}` on both lanes.

### Batch wrapper

`c=batch`, `p='{"commands":[{"id":"...","c":"...","p":{}}]}'`

---

## Lane Semantics

Every command is dispatched through one of two execution lanes. The lane
is declared per command in source (see `CommandDescriptor.lane` in
`daemon/src/integration/handlers/types.rs`) and surfaced to clients via
the `describe` command's response — operators can introspect the daemon
to see exactly which lane each command takes.

### Concurrent lane (default)

- **Execution**: async-spawned onto a shared tokio runtime. The consumer
  thread reads a command, hands it to `tokio::spawn`, and immediately
  reads the next message — no blocking on handler completion.
- **Ordering**: no guarantee across separate requests. Within a single
  client request the client awaits its response before sending the next
  command (synchronous client pattern), so ordering still holds for that
  client's commands; what's not guaranteed is the relative completion
  order across independent clients hitting the same daemon.
- **Use for**: idempotent FCALL wrappers, read-only operations, anything
  whose effect doesn't need to be observable to the next command in the
  *same* batch.
- **Response**: handler writes to `{ss}:res:{id}` polling key
  asynchronously. Client polls that key as today.

### Ordered lane

- **Execution**: synchronous inline. The consumer thread blocks on the
  handler before reading the next message in the same batch.
- **Ordering**: preserved across the batch. A subsequent command in the
  same batch is guaranteed to see the effects of an earlier Ordered
  command.
- **Use for**: setup ops whose result subsequent commands depend on,
  destructive ops where stale reads would be dangerous, or daemon-state
  mutations (policy/config) that change subsequent dispatch behaviour.
- **Response**: handler writes to `{ss}:res:{id}` synchronously from the
  consumer thread.

### Current Ordered commands

These 7 commands are the only ones in the base catalog that opt into
`Lane::Ordered`. Each declares a justification comment at its
descriptor's call site.

| Command | Why Ordered |
|---|---|
| `channel_open` | Creates a stream resource subsequent inter-service sends depend on. |
| `channel_close` | Destructive cleanup — pending sends must observe closure. |
| `config_set` | Daemon-behaviour change — pending `config_get` reads must observe new value. |
| `topology_edge_add` | An edge decision is a potential comparison over the stored indexes; a later edge in the batch must see it. |
| `topology_edge_remove` | Destructive — pending reads must observe post-remove state. |
| `topology_edge_list` | Reads edges a preceding `topology_edge_add` in the batch wrote. |
| `topology_edge_validate` | A dry run of `topology_edge_add`; ordered with it so the answer is the one the add would give. |

All other commands default to **Lane::Concurrent**.

### Fallbacks

- If the Concurrent lane runtime fails to initialize at daemon startup, every
  command falls back to synchronous dispatch (today's previously
  behaviour). The daemon still serves correctly, just without the
  async throughput improvement.
- If a Lane::Concurrent command has no async handler registered (rare —
  defensive case for signed extensions), it also falls back to
  synchronous dispatch.

---

## Part 1: Daemon Commands

Commands are sent via XADD to the unified stream. Most accept both lowercase and UPPERCASE variants.

---

### System (10 commands)

Source: `daemon/src/integration/handlers/system.rs`

| Command | Aliases | Parameters | Returns | Description |
|---------|---------|------------|---------|-------------|
| `ping` | `PING` | `{message?: string}` | `"pong"` or echoed message | Health check |
| `status` | `STATUS`, `info`, `INFO` | `{detail?: "basic"\|"full"\|"schema"}` | `{version, uptime, registered_services, timestamp, ...}` | Daemon status and metrics |
| `health` | `HEALTH` | `{}` | `{status, valkey_connected, uptime, ...}` | Health check with ValKey connectivity |
| `version` | `VERSION` | `{}` | `{version, rust_version, build_profile}` | Daemon version info |
| `echo` | `ECHO` | `{message: string}` | Echoed message | Echo back parameters |
| `get_node_info` | `GET_NODE_INFO`, `node_info` | `{}` | `{node_id, node_type, environment, namespace, ...}` | Current node configuration |
| `get_site_info` | `GET_SITE_INFO`, `site_info` | `{site_id: string}` | `{site_id, streams, environment, ...}` | Site registration info |
| `describe` | — | `{}` | Array of `{name, category, description, params_schema, returns_schema, example}` | List all available commands with schemas |
| `extension_list` | `EXTENSION_LIST` | `{}` | Array of `{name, version, commands, lua_libraries}` | List loaded extensions |
| `extension_info` | `EXTENSION_INFO` | `{name: string}` | `{name, version, path, commands, functions}` | Extension details |

---

### Geometric (2 commands)

Source: `daemon/src/integration/handlers/geometric.rs`. One query on the one topology (`GNODE_TOPO_QUERY`): the entities of a layer whose colours hold every named facet and whose ordered axes fall in every named range; axes the query does not name do not count. Records carry their colour.

| Command | Aliases | Parameters | Returns | Description |
|---------|---------|------------|---------|-------------|
| `geometric_discover` | `discover`, `DISCOVER`, `geo_disc` | `{layer?: string = "3", facets?: {axis: word \| [words]}, axes?: {axis: [lo, hi]}, limit?: int = 10}` — `layer` is a shape word (site, app, daemon, component, node) or a layer key; words are the shape's vocabulary, ranges words of an ordered axis or fractions in [0, 1] | `{total_matches, results: [{service_id, shape, environment, colour, metadata}], layer, topology_key, lookup: "index"}` | Sorted by the daemon; unknown axes and words are errors |
| `geometric_discover_range` | `GEOMETRIC_DISCOVER_RANGE` | the same | the same | The same query, kept as a name |

---

### One topology — edges (4 commands)

Source: `daemon/src/integration/handlers/one_topology.rs`. The dependency edges of the constellation (SPEC-ONE-TOPOLOGY §8): an edge exists only if φ(from) > φ(to), φ = (top-level layer digit, fundamentality), so a cycle is impossible by one integer comparison. The entity's constellation is found from `from` (`for_entity`).

| Command | Aliases | Parameters | Returns | Description |
|---|---|---|---|---|
| `topology_edge_add` | — | `{from: string, to: string, metadata?: object}` | `{ok, from, to}` | Add a dependency edge; refused with the two potentials when φ(from) ≤ φ(to) or the ends are unknown |
| `topology_edge_remove` | — | `{from: string, to: string}` | `{ok, from, to}` | Remove an edge; refused when absent |
| `topology_edge_list` | — | `{id: string, direction?: "outgoing"\|"incoming" = "outgoing"}` | `{id, direction, edges: [id]}` | What an entity depends on, or what depends on it; sorted |
| `topology_edge_validate` | — | `{from: string, to: string}` | `{valid, from, to, phi_from: [layer, fundamentality], phi_to, reason}` | Dry run of `topology_edge_add` |


### Service (3 commands)

Source: `daemon/src/integration/handlers/service.rs`

| Command | Aliases | Parameters | Returns | Description |
|---------|---------|------------|---------|-------------|
| `register_service` | `registerService`, `REGISTER_SERVICE` | `{id: string, shape?: string = "app", environment?: string = "production", axes?: {axis: word \| [words] \| fraction}, metadata?: object, on?: string, capacity_weight?: int = 1, heartbeat?: string, holds?: [string], requires?: [string], peers?: [string], constellation?: string}` | `{status, service_id, shape, topology_key, placement?, updated}` | Declare an entity by name on the one topology (SPEC-ONE-TOPOLOGY §4): the daemon codes the words and refuses one it does not know; a number for an axis word is refused; `on` makes a 4.2 placement with `capacity_weight` (0 = standby); `holds`/`requires`/`peers` are the REACH labels |
| `deregister_service` | `deregisterService`, `DEREGISTER_SERVICE` | `{service_id: string}` (alias `id`) | `{status: "deregistered"\|"not_found", service_id}` | Remove an entity with its indexes, edges and placements |
| `discover_with_endpoints` | `DISCOVER_WITH_ENDPOINTS`, `service_endpoints` | `{layer?, facets?, axes?, endpoint_registry?: string, limit?: int = 10}` | `{count, services: [{service_id, endpoints}], endpoints_error?}` | The query of `geometric_discover`, each id enriched with its endpoints from `GNODE_ENDPOINT_LIST` (gNode-BROKER); `endpoints_error` gives the reason when endpoints could not be listed |

---

### Introspection (2 commands)

Source: `daemon/src/integration/handlers/introspection.rs`

| Command | Aliases | Parameters | Returns | Description |
|---------|---------|------------|---------|-------------|
| `service_describe` | `SERVICE_DESCRIBE` | `{service_id: string}` | `{service_id, capabilities, metadata, health, tier}` | Detailed service introspection with health/tier info |
| `constellation_status` | `CONSTELLATION_STATUS` | `{}` | `{constellation, ts, nodes, services, tools, components_live}` | Constellation at a glance: nodes with liveness + resources (from the daemon heartbeat), which components run where, registered service intents, tool count. Agent-facing twin of `geodineum status` (same JSON shape) |

---

### Stream (4 commands)

Source: `daemon/src/integration/handlers/stream.rs`

| Command | Aliases | Parameters | Returns | Description |
|---------|---------|------------|---------|-------------|
| `stream_info` | `STREAM_INFO` | `{stream?: string}` | `{length, groups, first_id, last_id}` | Stream metadata |
| `stream_group_info` | `STREAM_GROUP_INFO` | `{stream?: string}` | Array of consumer group info | Consumer group details |
| `stream_consumer_info` | `STREAM_CONSUMER_INFO` | `{stream?: string, group: string}` | Array of consumer info | Consumer details within a group |
| `stream_pending` | `STREAM_PENDING` | `{stream?: string, group: string, count?: int}` | Pending message list | Pending message info |

---

### Config (3 commands)

Source: `daemon/src/integration/handlers/config.rs`

| Command | Aliases | Parameters | Returns | Description |
|---------|---------|------------|---------|-------------|
| `config_get` | `CONFIG_GET` | `{category: string, key: string}` | Config value | Get a configuration value |
| `config_set` | `CONFIG_SET` | `{category: string, key: string, value: string}` | `{ok: true}` | Set a configuration value |
| `config_list` | `CONFIG_LIST` | `{category?: string}` | Object of config key-value pairs | List configuration values |

---

### Direct Channel (4 commands)

Source: `daemon/src/integration/handlers/direct_channel.rs`

| Command | Aliases | Parameters | Returns | Description |
|---------|---------|------------|---------|-------------|
| `channel_open` | `direct_provision` | `{target_site: string, mode?: "temporary"\|"persistent", ttl_seconds?: int, metadata?: object}` | `{channel_id, stream_key, mode, expires_at}` | Open a direct inter-service channel |
| `channel_close` | `direct_close` | `{channel_id: string}` | `{ok, deleted_keys}` | Close and clean up a channel |
| `channel_info` | `direct_info` | `{channel_id: string}` | `{channel_id, source, target, mode, stream_info}` | Get channel metadata |
| `channel_list` | `direct_list` | `{site_filter?: string, env_filter?: string}` | Array of channel metadata | List direct channels |

---

### Diagnostic (7 commands)

Source: `daemon/src/integration/handlers/diagnostic.rs`

| Command | Aliases | Parameters | Returns | Description |
|---------|---------|------------|---------|-------------|
| `debug_info` | `DEBUG_INFO` | `{}` | `{topology, connections, workers, ...}` | Detailed debug information |
| `memory_stats` | `MEMORY_STATS` | `{}` | `{rss_kb, topology_size, pool_size, ...}` | Memory usage statistics |
| `thread_status` | `THREAD_STATUS` | `{}` | `{thread_count, workers, ...}` | Thread pool status |
| `connection_status` | `CONNECTION_STATUS` | `{}` | `{pool_size, active, idle, ...}` | Connection pool status |
| `performance_metrics` | `PERFORMANCE_METRICS` | `{}` | `{commands_processed, avg_latency_ms, ...}` | Performance counters |
| `security_status` | `SECURITY_STATUS` | `{}` | `{acl_user, keyspace_pattern, ...}` | Security posture info |
| `topology_status` | `TOPOLOGY_STATUS` | `{}` | `{services, buckets, dimensions, ...}` | Current topology state |

---

### CMS Extension Commands (19 commands)

Delivered as a signed extension: discovered in `GNODE_EXT_DIR` at build time, verified against the author pubkey, and staged into the build (default companion; opt out with `GEODINEUM_SKIP_CMS=true`). No Cargo feature flag is involved. Source: `gNode-CMS` repo.

#### Template (3)

| Command | Parameters | Returns | Description |
|---------|------------|---------|-------------|
| `render_template` | `{template_id: string, variables: object}` | Rendered HTML/text | Render a Tera template with variables |
| `render_string` | `{template: string, variables?: object}` | `{html}` | Render an ad-hoc Tera template string (no pre-registration needed) |
| `serve_fragment` | `{fragment_id: string}` | Pre-rendered fragment | Serve a cached template fragment |

#### Content (4)

| Command | Parameters | Returns | Description |
|---------|------------|---------|-------------|
| `content_store` | `{id: string, content: string, type?: string, minify?: bool}` | `{id, stored: true}` | Store content with optional minification |
| `content_retrieve` | `{id: string}` | `{id, content, type, metadata}` | Retrieve stored content |
| `template_fragment` | `{template: string, fragment: string, variables?: object}` | Rendered fragment | Render a template fragment |
| `asset_bundle` | `{manifest_id: string}` | Bundled asset output | Bundle assets from a manifest |

#### Asset (8)

| Command | Parameters | Returns | Description |
|---------|------------|---------|-------------|
| `asset_store` | `{asset_id: string, content: string, content_type: string, version?: string}` | `{ok, asset_id, version}` | Store an asset |
| `asset_get` | `{asset_id: string}` | `{asset_id, content, content_type, version}` | Retrieve an asset |
| `asset_delete` | `{asset_id: string}` | `{ok, deleted}` | Delete an asset |
| `asset_list` | `{content_type?: string}` | Array of asset IDs | List assets |
| `manifest_set` | `{manifest_id: string, manifest: {layout, ...}}` | `{ok, manifest_id, updated, layout}` | Create or update a bundle manifest definition |
| `manifest_get` | `{manifest_id: string}` | `{ok, manifest}` | Retrieve a bundle manifest definition |
| `manifest_delete` | `{manifest_id: string}` | `{ok, deleted}` | Delete a bundle manifest and its built bundle |
| `manifest_list` | `{}` | `{ok, manifests, count}` | List all bundle manifests for the site |

#### Format (4)

| Command | Parameters | Returns | Description |
|---------|------------|---------|-------------|
| `register_format` | `{name: string, schema: JSONSchema, patterns: array}` | `{status, format_name}` | Register a custom message format |
| `list_formats` | `{}` | Array of format definitions | List registered formats |
| `detect_format` | `{message: string}` | `{format_name, version, confidence}` | Auto-detect message format |
| `convert_format` | `{source_format: string, target_format: string, message: string, source_version?, target_version?}` | Converted message | Convert between formats |

All four format commands are thin wrappers over the base daemon's native `FormatProcessor` (the canonical wire-format engine); custom format definitions persist to ValKey via the processor, and the relay path uses the same engine directly for inline translation.

---

## Part 2: ValKey Lua Functions

All functions use `FCALL <function_name> <numkeys> [keys...] [args...]`. The `server.*` API (ValKey 7.2+) is used throughout. All cjson operations are pcall-wrapped.

---

### gnode_topo — 28 functions

The one topology of a constellation (`{c}:gnode:constellation`, SPEC-ONE-TOPOLOGY §1–§5): one record shape for every layer, indexes and facet sets maintained by the library, placements with rings, in-flight sets, REACH labels and the deny list, derived writes under the writer lease. `GNODE_TOPO_CREATE/LIST/DELETE/STATS/EXISTS` and the entity/edge getters also serve the Pro named topologies `{c}:gnode:topo:<name>` (D16).

| Function | Keys | Args | Returns | Description |
|----------|------|------|---------|-------------|
| `GNODE_TOPO_CREATE` | `{c}:gnode:topo:registry` | topology_key, definition_json | JSON topology metadata | Create a named topology |
| `GNODE_TOPO_SET_DERIVED` | tk [, lease_key] | entity_id, updates_json `{"<slot>": {"pd": float, "pr": "<raw>"}}`, "", "", ts?, "", node_id | `{ok}` | Zone-B (derived) write under the writer lease; indexes and the ring follow (the arguments kept for position: z_score, snapshot_key, hashed_width? | `{ok, eid, n, ds, zs}` | Write measured coordinates into a stored entity without moving it: `bk` and voxel membership are untouched, `m.ds` records the measurement time, and the `z_order` set is updated when `z_score` is given. `updates_json` is `{"<index>": {"pd": <float>, "pr": "<raw>"}}` — both scales come from the daemon, Lua has no Q64.64. An index below `hashed_width` is refused: those coordinates are the bucket key |
| `GNODE_TOPO_ADD_EDGE` | topology_key | from_id, to_id, edge_json | `{ok, f, t}` | Add directed edge |
| `GNODE_TOPO_REMOVE_EDGE` | topology_key | from_id, to_id | `{ok}` | Remove directed edge |
| `GNODE_TOPO_GET_ENTITIES` | topology_key | entity_ids_json or `"*"`, include_edges? | `{req, fnd, mis?, ents}` | Entities by ID array, or every entity of the topology with `"*"` |
| `GNODE_TOPO_GET_ENTITY` | topology_key | entity_id | `{id, x, y, z, bk, zs, ra, m}` | Get single entity |
| `GNODE_TOPO_GET_EDGE` | topology_key | from_id, to_id | `{f, t, zd, m}` | Get specific edge |
| `GNODE_TOPO_GET_EDGES` | topology_key | entity_id, direction | `{out, in}` | Get all edges for entity |
| `GNODE_TOPO_STATS` | topology_key | — | `{entity_count, edge_count, ...}` | Topology statistics |
| `GNODE_TOPO_LIST` | `{c}:gnode:topo:registry` | filter_type? | JSON array of topology metadata | List the constellation's named topologies |
| `GNODE_TOPO_DELETE` | `{c}:gnode:topo:registry` | topology_key, "CONFIRM" | `{ok, deleted_keys}` | Delete topology (safety flag required) |
| `GNODE_TOPO_EXISTS` | topology_key | — | `{exists: bool}` | Check topology existence |

---
| `GNODE_ENSURE_CONSTELLATION` | tk | — | `{ok, tk, created}` | The one key of a constellation (`{c}:gnode:constellation`), created once with meta `sv=5` |
| `GNODE_TOPO_REGISTER` | tk | id, shape_key, record_json, ordered_json `{axis: score}`, colours_json `{facet: [values]}`, preserve_csv | `{ok, eid, sh, ro, upd}` | Register an entity of a shape against the published per-shape map: layer interval, vocabularies, one-valued facets; `ro` allocated once; zone-B slots in preserve_csv kept on update; the shape's `fundamentality_default` applied when none is declared |
| `GNODE_TOPO_PLACE` | tk | id, shape_key (4.1\|4.2), of, on, env, heartbeat_component, weight, record_json, ordered_json, colours_json [, heartbeat_env] | `{ok, eid, sh, ro, upd}` | A placement of a component (4.1) or service (4.2) on a node; a 4.2 joins the ring; weight 0 = standby |
| `GNODE_TOPO_DEREGISTER` | tk | id | `{ok, eid, placements_removed}` | Remove an entity, its indexes, edges and placements |
| `GNODE_TOPO_QUERY` | tk | layer_lo, layer_hi, filters_json `{axes: {name: [lo, hi]}, facets: {name: [any-of]}}` [, opts_json `{limit}`] | array of record JSON | The layer interval drives, every other constraint is one index lookup per candidate, survivors decode. `no-writes` |
| `GNODE_TOPO_ORDER` | tk | axis [, limit, offset, desc] | ids with scores | A ranking by one indexed axis. `no-writes` |
| `GNODE_TOPO_ROUTE` | tk | service | `[placement, node, "ring"\|"standby"]` | Exact weighted round-robin over the service's healthy active placements from one shared counter; weight 0 only when the ring is empty |
| `GNODE_TOPO_DEPART` | tk | placement, cid, now_ms | `OK` | A relayed command left for this placement (in-flight set scored by departure) |
| `GNODE_TOPO_ARRIVE` | tk | placement, cid | 0\|1 | Its reply arrived |
| `GNODE_TOPO_INFLIGHT` | tk | placement, now_ms, ttl_ms | integer | In-flight count after reaping departures older than ttl |
| `GNODE_TOPO_LABEL` | tk | id, holds_json, requires_json, peers_json | `{ok, eid, holds, requires, peers}` | REACH labels and declared same-layer peers |
| `GNODE_TOPO_DENY` | tk | caller, target, now_ms, ttl_ms | `OK` | A TTL-bounded deny (ttl must be > 0) |
| `GNODE_TOPO_UNDENY` | tk | caller, target | 0\|1 | Remove a deny |
| `GNODE_TOPO_CAN_REACH` | tk | caller, target, now_ms | `[0\|1, reason]` | φ(target) < φ(caller) or declared peer; env ∈ {env(caller), global}; requires ⊆ holds; no live deny. Undeclared ⇒ 0. `no-writes` |
| `GNODE_TOPO_HEALTH_DERIVE` | tk [, lease_key] | now_s, fresh_s [, node_id] | `[fresh, placements, entities]` | Placement health from the heartbeat key it names (dead when missing or stale, unknown when none can be named); entity health = max over placements; ring membership follows |
| `GNODE_TOPO_VERIFY` | tk | — | `OK` or the first violation | Invariants: counts, index ↔ record agreement, placement references, φ on every edge, ring membership. `no-writes` |

### gnode_topology — 1 function

The per-shape schema the daemon published at start (`{c}:gnode:constellation:schema:<shape>`), read back by clients.

| Function | Keys | Args | Returns | Description |
|----------|------|------|---------|-------------|
| `GNODE_SCHEMA_GET` | — | topology_ns, shape key (`1`, `2`, `3.1`, `3.2`, `3.3`, `4.1`, `4.2`; default `3.3`) | the shape's published hash: `schema_version, shape, name, width, layer_code, layer_hi, slots, dimension_index, dimension_values, fundamentality_default` | Read-only; an absent shape is an error that names the key it looked for |

---

### gnode_node — 12 functions

Node registration, configuration, and metrics.

| Function | Keys | Args | Returns | Description |
|----------|------|------|---------|-------------|
| `GNODE_REGISTER_NODE` | — | node_id, node_type, config_json, site_id, hostname, ip | `"OK"` | Register a node with configuration |
| `GNODE_NODE_DEREGISTER` | — | node_id | `"OK"` | Mark node inactive |
| `GNODE_NODE_HEARTBEAT` | — | node_id, load, cpu, mem, requests, latency | heartbeat count | Update node health (high-frequency) |
| `GNODE_NODE_RECORD_METRICS` | — | node_id, processed, failed, latency, bytes_in, bytes_out | `"OK"` | Record command processing metrics |
| `GNODE_NODE_GET_INFO` | — | node_id | `{node_id, config, health, metrics}` | Getnode info |
| `GNODE_NODE_GET_TOPOLOGY` | — | include_metrics, filter_type | `{node_count, nodes, by_type}` | Get topology of all nodes |
| `GNODE_NODE_CLEANUP_STALE` | — | stale_threshold_s, dry_run | `{stale_count, cleaned_count}` | Remove stale nodes |
| `GNODE_NODE_LIST_TYPES` | — | — | `{total_nodes, types}` | List node types and counts |
| `GNODE_NODE_STORE_CONFIG` | — | node_type, config_json | `"OK"` | Store node type config (master only) |
| `GNODE_NODE_FETCH_CONFIG` | — | node_type | JSON config | Fetch node type config (workers) |
| `GNODE_NODE_LIST_CONFIGS` | — | — | `{count, configs}` | List available configs |
| `GNODE_NODE_AGGREGATE_METRICS` | — | filter_type? | JSON aggregate stats | Aggregate metrics across nodes |

---

### gnode_stream — 26 functions

Stream operations, consumer groups, service provisioning/deprovisioning, DTAP environment management.

| Function | Keys | Args | Returns | Description |
|----------|------|------|---------|-------------|
| `GNODE_STREAM_GROUP_READ` | stream_key | group, consumer, count, block_ms, id, site_id | JSON messages | XREADGROUP consumer group read |
| `GNODE_STREAM_READ` | stream_key | min_id, max_id, count | JSON messages | XRANGE stream read |
| `GNODE_STREAM_ADD` | stream_key | id, command, params, site_id, node_id, ts | entry ID | XADD command message |
| `GNODE_STREAM_ACK` | stream_key | group, msg_ids_json, thread_id | count ACKed | Atomic acknowledge with distributed lock |
| `GNODE_STREAM_GROUP` | stream_key | group, action, id | `"OK"` | Manage consumer groups |
| `GNODE_STREAM_DEL` | stream_key | msg_ids_json | count deleted | Delete stream messages |
| `GNODE_STREAM_PENDING` | stream_key | group, start, end, count, consumer | JSON pending list | Get pending messages |
| `GNODE_STREAM_RESPOND` | stream_key | message_id, response_json | entry ID | Add response to stream |
| `GNODE_STREAM_CLAIM` | stream_key | group, consumer, min_idle_ms, msg_ids_json | JSON claimed | Claim pending messages |
| `GNODE_STREAM_TRIM` | stream_key | max_len, approximate | count trimmed | XTRIM MAXLEN |
| `GNODE_STREAM_BATCH_READ` | stream_key | streams_json, count | JSON map | Multi-stream XREAD |
| `GNODE_STREAM_BATCH_GROUP_READ` | — | group, consumer, streams_json, count, block_ms | JSON map | Multi-stream XREADGROUP |
| `GNODE_STREAM_INFO` | stream_key | — | `{length, groups, first_id, last_id}` | XINFO STREAM |
| `GNODE_STREAM_GROUPS_INFO` | stream_key | — | JSON array of group info | XINFO GROUPS |
| `GNODE_STREAM_CONSUMERS_INFO` | stream_key | group_name | JSON array of consumer info | XINFO CONSUMERS |
| `GNODE_STREAM_ADD_RESP3` | stream_key | entry_data, site_id | RESP3 entry ID | XADD with backpressure |
| `GNODE_PROVISION_SERVICE` | — | service_id, environments_json, namespace, owner | JSON created streams | Create DTAP streams + consumer groups |
| `GNODE_DEPROVISION_SERVICE` | — | service_id, options_json | JSON cleanup results | Complete service removal. The topology entity is removed separately with `GNODE_TOPO_DEREGISTER` (`geodineum topology deregister <id>`) |
| `GNODE_UPDATE_SERVICE` | — | service_id, updates_json | JSON update results | Update service metadata/status |
| `GNODE_SERVICE_ADD_ENVIRONMENT` | — | service_id, environment, set_active | JSON creation results | Add DTAP environment to service |
| `GNODE_SERVICE_REMOVE_ENVIRONMENT` | — | service_id, environment, options_json | JSON removal results | Remove DTAP environment |
| `GNODE_SERVICE_GET` | — | service_id, options_json | JSON service details | Get complete service info |
| `GNODE_SERVICE_LIST` | — | options_json | JSON service list | List services with filtering |
| `GNODE_STREAM_ENSURE_CONSUMER_GROUPS` | stream_key | groups_json, start_id | `{results}` | Idempotently ensure consumer groups |
| `GNODE_STREAM_GET_SITE_STREAMS` | — | site_id, filter_env, filter_type | `{streams}` | Get stream keys for a site |
| `GNODE_RESPONSE_GET` | — | site_id, correlation_id [, correlation_id ...] | Flat array `[id, value, id, value, ...]` | Batch-read keyed-rendezvous replies `{site}:res:{id}` for a set of ids. `no-writes`, so callable through `FCALL_RO` by clients holding only `+fcall_ro`. Absent ids are omitted, so "no reply yet" and "no such request" are the same answer |

---

### gnode_site — 13 functions

Site registration, rate limiting, circuit breaking, environment management, tenant grouping.

| Function | Keys | Args | Returns | Description |
|----------|------|------|---------|-------------|
| `GNODE_SITE_REGISTER` | — | site_id, config_json | JSON registration result | Register a site |
| `GNODE_SERVICE_RATE_LIMIT` | — | site_id, operation, limit, window_ms | `{allowed, count, remaining}` | Sliding-window rate limit check |
| `GNODE_SERVICE_CIRCUIT_BREAKER` | — | site_id, service, threshold | `{state, allowed}` | Check circuit breaker state |
| `GNODE_SERVICE_CIRCUIT_RECORD_FAILURE` | — | service, site_id | `{failures, state}` | Record circuit breaker failure |
| `GNODE_SERVICE_CIRCUIT_RESET` | — | service, site_id | `{ok}` | Reset circuit breaker |
| `GNODE_SERVICE_GET_INFO` | — | site_id | JSON site metadata | Get site info |
| `GNODE_SERVICE_GET_NODE_INFO` | — | site_id, node_id | JSON node info | Get node info for a site |
| `GNODE_SERVICE_LIST_ALL` | — | include_meta | JSON array of sites | List all registered sites |
| `GNODE_SERVICE_GET_DAEMON_STREAMS` | — | environment, filter_type, namespace | `{streams}` | Get daemon subscription streams |
| `GNODE_SERVICE_SET_ENVIRONMENT` | — | site_id, environment | `{ok}` | Set active DTAP environment |
| `GNODE_SERVICE_GET_ENVIRONMENT` | — | site_id | `{environment}` | Get active DTAP environment |
| `GNODE_SERVICE_GET_ALL_STREAMS` | — | namespace | `{unified, health, broadcast}` | Get all streams by type |
| `GNODE_TENANT_LIST_SITES` | — | owner_id | `{owner, sites, count}` | List sites for a tenant |

---

### gnode_resilience — 13 functions

Circuit breaker, idempotency, cache stampede prevention, leader election, retry budget.

| Function | Keys | Args | Returns | Description |
|----------|------|------|---------|-------------|
| `GNODE_RESILIENCE_CIRCUIT_CHECK` | circuit_name | site_id, config_json | `{state, allowed, stats}` | Check circuit breaker state |
| `GNODE_RESILIENCE_CIRCUIT_SUCCESS` | circuit_name | site_id | `{state, transitions}` | Record successful call |
| `GNODE_RESILIENCE_CIRCUIT_FAILURE` | circuit_name | site_id, error_msg, error_type | `{state, failures, transitions}` | Record failed call |
| `GNODE_RESILIENCE_IDEMPOTENT_CHECK` | idempotency_key | site_id, ttl_ms | `{status, result}` | Check/acquire idempotency lock |
| `GNODE_RESILIENCE_IDEMPOTENT_COMPLETE` | idempotency_key | site_id, result_json | `{ok}` | Store idempotency result |
| `GNODE_RESILIENCE_IDEMPOTENT_RELEASE` | idempotency_key | site_id | `{ok}` | Release lock on failure |
| `GNODE_RESILIENCE_CACHE_GET_SAFE` | cache_key | site_id, compute_time_ms, beta | `{value, hit, recompute_hint}` | XFetch probabilistic early expiration |
| `GNODE_RESILIENCE_CACHE_SET_SAFE` | cache_key | site_id, data, ttl_ms | `{ok}` | Set with stampede prevention metadata |
| `GNODE_RESILIENCE_LEADER_ACQUIRE` | election_name | site_id, node_id, ttl_s | `{acquired, leader}` | Try to become leader (SET NX EX) |
| `GNODE_RESILIENCE_LEADER_RELEASE` | election_name | site_id, node_id | `{released}` | Release leadership |
| `GNODE_RESILIENCE_LEADER_WHO` | election_name | site_id | `{leader, ttl_remaining}` | Check current leader |
| `GNODE_RESILIENCE_RETRY_CHECK` | service_name | site_id, max_retries, window_ms | `{allowed, used, remaining}` | Check and consume retry budget |
| `GNODE_RESILIENCE_RETRY_STATUS` | service_name | site_id | `{used, remaining, window_ms}` | Get retry budget without consuming |

---

### gnode_cache — 9 functions

Site-namespaced key-value cache.

| Function | Keys | Args | Returns | Description |
|----------|------|------|---------|-------------|
| `GNODE_CACHE_GET` | — | key, site_id | value or nil | Get cached value |
| `GNODE_CACHE_SET` | — | key, value, ttl_s, site_id, nx?, xx? | `"OK"` or nil | Set with optional TTL and NX/XX |
| `GNODE_CACHE_DEL` | — | key, site_id | count deleted | Delete cached value |
| `GNODE_CACHE_EXISTS` | — | key, site_id | 1 or 0 | Check key existence |
| `GNODE_CACHE_INCR` | — | key, amount, site_id | new value | Increment counter |
| `GNODE_CACHE_DECR` | — | key, amount, site_id | new value | Decrement counter |
| `GNODE_CACHE_TTL` | — | key, site_id | seconds remaining | Get key TTL |
| `GNODE_CACHE_PERSIST` | — | key, site_id | 1 or 0 | Remove expiration |
| `GNODE_CACHE_STATS` | — | site_id | `{hit_count, miss_count, key_count}` | Cache statistics |

---

### gnode_config — 13 functions

Configuration with site-scoped and global defaults.

| Function | Keys | Args | Returns | Description |
|----------|------|------|---------|-------------|
| `GNODE_CONFIG_GET` | — | site_id, category, key, default | value or default | Get config (site → global → default) |
| `GNODE_CONFIG_GET_INT` | — | site_id, category, key, default | integer value | Get config as integer |
| `GNODE_CONFIG_SET` | — | site_id, category, key, value | 1 | Set config value |
| `GNODE_CONFIG_MSET` | — | site_id, category, fields_json | count set | Set multiple fields |
| `GNODE_CONFIG_HGETALL` | — | site_id, category | array [k, v, ...] | Get all fields (merged with defaults) |
| `GNODE_CONFIG_DELETE` | — | site_id, category, key | 1 or 0 | Delete config value |
| `GNODE_CONFIG_RESET` | — | site_id, category | 1 | Reset category to defaults |
| `GNODE_CONFIG_SEED` | — | site_id, json_config | categories seeded | Seed from JSON |
| `GNODE_CONFIG_EXPORT` | — | site_id | JSON all categories | Export all config |
| `GNODE_CONFIG_LIST_CATEGORIES` | — | site_id | array of names | List config categories |
| `GNODE_CONFIG_GET_DEFAULTS` | — | category | array [k, v, ...] | Get built-in defaults |
| `GNODE_CONSTELLATION_GENERATION_INCR` | — | site_id, reason? | new generation (int) | Bump the constellation config generation and broadcast `config_updated`; every config reader compares its APCu entry against it. Callers: `geodineum env set`, `geodineum config set`, geodeploy `opcache-clear` (all registered sites), gCore `compile-config.php` |
| `GNODE_CONSTELLATION_GENERATION_GET` | — | site_id | current generation (int, 0 if unset) | Read the current constellation config generation |

---

### gnode_batch — 11 functions

Multi-key batch operations.

| Function | Keys | Args | Returns | Description |
|----------|------|------|---------|-------------|
| `GNODE_BATCH_EXEC` | — | site_id, operations_json | JSON per-op results | Heterogeneous batch execute |
| `GNODE_BATCH_MGET` | — | site_id, keys... | JSON array of values | Multi-GET |
| `GNODE_BATCH_MSET` | — | site_id, key/value pairs... | `{ok, count}` | Multi-SET |
| `GNODE_BATCH_MDEL` | — | site_id, keys... | count deleted | Multi-DELETE |
| `GNODE_BATCH_MEXISTS` | — | site_id, keys... | `{key: bool}` | Multi-EXISTS |
| `GNODE_BATCH_MEXPIRE` | — | site_id, ttl_s, keys... | `{key: result}` | Multi-EXPIRE |
| `GNODE_BATCH_MPERSIST` | — | site_id, keys... | `{key: result}` | Multi-PERSIST |
| `GNODE_BATCH_MINCR` | — | site_id, keys... | `{key: new_value}` | Multi-INCR |
| `GNODE_BATCH_MTTL` | — | site_id, keys... | `{key: ttl}` | Multi-TTL |
| `GNODE_BATCH_MHGET` | — | site_id, key:field pairs... | `{key:field: value}` | Multi-HGET |
| `GNODE_BATCH_MHSET` | — | site_id, key:field:value triples... | `{ok, count}` | Multi-HSET |

---

### gnode_batch_resp3 — 3 functions

RESP3-native batch variants.

| Function | Keys | Args | Returns | Description |
|----------|------|------|---------|-------------|
| `GNODE_BATCH_MGET_RESP3` | — | site_id, keys... | RESP3 map | RESP3-encoded multi-GET |
| `GNODE_BATCH_MSET_RESP3` | — | site_id, key/value pairs... | RESP3 status | RESP3-encoded multi-SET |
| `GNODE_BATCH_MDEL_RESP3` | — | site_id, keys... | RESP3 integer | RESP3-encoded multi-DELETE |

---

### gnode_broadcast — 4 functions

Pub-sub via XREAD (no consumer groups).

| Function | Keys | Args | Returns | Description |
|----------|------|------|---------|-------------|
| `GNODE_BROADCAST_READ` | stream_key | last_id, count, block_ms | JSON array of messages | Read broadcast messages |
| `GNODE_BROADCAST_WRITE` | stream_key | message_type, fields_json | entry ID | Write broadcast message |
| `GNODE_BROADCAST_TRIM` | stream_key | retention_seconds | count trimmed | Trim by retention time |
| `GNODE_BROADCAST_INFO` | stream_key | — | `{length, first_id, last_id}` | Broadcast stream metadata |

---

### gnode_core — 9 functions

Core key-value operations with site namespacing.

| Function | Keys | Args | Returns | Description |
|----------|------|------|---------|-------------|
| `GNODE_CORE_GET` | — | key, site_id | value or nil | Get cached value |
| `GNODE_CORE_SET_WITH_TTL` | — | key, value, ttl_s, site_id | `"OK"` | Set with TTL |
| `GNODE_CORE_DELETE` | — | key, site_id | count deleted | Delete value |
| `GNODE_CORE_EXISTS` | — | key, site_id | 1 or 0 | Check existence |
| `GNODE_CORE_TTL` | — | key, site_id | seconds | Get TTL |
| `GNODE_CORE_INCREMENT` | — | key, amount, ttl_s, site_id | new value | Increment counter |
| `GNODE_CORE_DECREMENT` | — | key, amount, ttl_s, site_id | new value | Decrement counter |
| `GNODE_CORE_GET_SET` | — | key, value, ttl_s, site_id | old value or nil | Atomic get-and-set |
| `GNODE_CORE_SET_IF_NOT_EXISTS` | — | key, value, ttl_s, site_id | 1 or 0 | Set only if key absent (NX) |

---

### gnode_direct — 5 functions

Direct inter-service channels (gNode-provisioned, peer-to-peer).

| Function | Keys | Args | Returns | Description |
|----------|------|------|---------|-------------|
| `GNODE_DIRECT_PROVISION` | base_key | channel_id, source, target, mode, ttl, metadata, env | `{channel_id, stream_key, mode, expires_at}` | Provision direct channel |
| `GNODE_DIRECT_CLOSE` | base_key | channel_id | `{ok, deleted_keys}` | Close and clean up channel |
| `GNODE_DIRECT_INFO` | base_key | channel_id | `{channel_id, source, target, mode, stream_info}` | Get channel metadata |
| `GNODE_DIRECT_LIST` | base_key | site_filter, env_filter | JSON array of channels | List channels |
| `GNODE_DIRECT_CHECK_EXPIRY` | base_key | — | `{checked, closed, channels}` | Auto-close expired channels |

---

### gnode_group — 9 functions

Application-level consumer group management.

| Function | Keys | Args | Returns | Description |
|----------|------|------|---------|-------------|
| `GNODE_GROUP_LIST` | — | site_id, filter | JSON array of names | List groups |
| `GNODE_GROUP_CREATE` | — | group_name, settings_json, site_id | `{ok, group_name}` | Create group |
| `GNODE_GROUP_DELETE` | — | group_name, site_id | `{ok}` | Delete group |
| `GNODE_GROUP_ADD_MEMBER` | — | group_name, member_id, site_id | `{ok, count}` | Add member |
| `GNODE_GROUP_REMOVE_MEMBER` | — | group_name, member_id, site_id | `{ok, count}` | Remove member |
| `GNODE_GROUP_GET_MEMBERS` | — | group_name, site_id | JSON array of members | Get members |
| `GNODE_GROUP_IS_MEMBER` | — | group_name, member_id, site_id | 1 or 0 | Check membership |
| `GNODE_GROUP_SET_PROPERTY` | — | group_name, property, value, site_id | `{ok}` | Set group property |
| `GNODE_GROUP_GET_PROPERTY` | — | group_name, property, site_id | value or nil | Get group property |

---

### gnode_hash — 7 functions

Hash operations with metric tracking.

| Function | Keys | Args | Returns | Description |
|----------|------|------|---------|-------------|
| `GNODE_HASH_HINCRBY` | key | field, amount, site_id | new value | Increment hash field |
| `GNODE_HASH_HGETALL` | key | site_id | array [field, value, ...] | Get all hash fields |
| `GNODE_HASH_HGET` | key | field, site_id | value or nil | Get single hash field |
| `GNODE_HASH_HEXISTS` | key | field, site_id | 1 or 0 | Check field existence |
| `GNODE_HASH_HSET` | key | field, value, site_id | 1 (new) or 0 (updated) | Set hash field |
| `GNODE_HASH_LPUSH` | key | value, site_id | list length | Push to list |
| `GNODE_HASH_HMSET` | key | field/value pairs..., site_id | `"OK"` | Set multiple hash fields |

---

### gnode_lock — 4 functions

Distributed locking (single atomic attempt).

| Function | Keys | Args | Returns | Description |
|----------|------|------|---------|-------------|
| `GNODE_LOCK_ACQUIRE` | resource | site_id, token, ttl_s | 1 or 0 | Acquire lock (SET NX EX) |
| `GNODE_LOCK_RELEASE` | resource | site_id, token | 1 or 0 | Release lock (token must match) |
| `GNODE_LOCK_IS_LOCKED` | resource | site_id | 1 or 0 | Check if locked |
| `GNODE_LOCK_INFO` | resource | site_id | JSON `{locked, exists, info: {acquired_at, timeout, site, ttl, attempts}}` (never the token) | Lock details |

---

### gnode_monitoring — 7 functions

Metrics, health checks, error tracking.

| Function | Keys | Args | Returns | Description |
|----------|------|------|---------|-------------|
| `GNODE_MONITORING_TRACK_METRIC` | — | site_id, metric_type, value, `extra_json?` | `{ok}` | Track a metric. `extra_json` is OPTIONAL — omit the argument entirely, or pass `''`; both mean absent. Only a non-empty, non-JSON value is an error. |
| `GNODE_MONITORING_METRICS_AGGREGATE` | — | site_id, metric_type, window_s | `{min, max, avg, count, sum}` | Aggregate over time window |
| `GNODE_MONITORING_CLEANUP` | — | site_id, max_age_s | `{deleted_count}` | Clean stale metrics |
| `GNODE_MONITORING_HEALTH_CHECK` | — | site_id | `{status, checks}` | Site health check |
| `GNODE_MONITORING_TRACK_ERROR` | — | site_id, error_type, message, context_json | `{ok, error_count}` | Track error occurrence |
| `GNODE_MONITORING_SCAN_CLUSTER` | — | site_id, pattern | JSON array of keys | Cluster-safe pattern scan (SCAN) |
| `GNODE_MONITORING_GET_METRIC_HISTORY` | — | site_id, metric_name, limit | JSON array of `{ts, value}` | Get metric history |

---

### gnode_protocol — 6 functions

RESP3/JSON encoding-agnostic unified stream read/write.

| Function | Keys | Args | Returns | Description |
|----------|------|------|---------|-------------|
| `GNODE_PROTOCOL_ENCODE` | stream_key | json_str | message ID | Encode and XADD to stream |
| `GNODE_PROTOCOL_DECODE` | stream_key | message_id | JSON string | Decode message by ID |
| `GNODE_PROTOCOL_READ_GROUP` | stream_key | group, consumer, count, block_ms | JSON messages | XREADGROUP read |
| `GNODE_PROTOCOL_ACK` | stream_key | group, message_param | count ACKed | Acknowledge messages |
| `GNODE_PROTOCOL_CLAIM` | stream_key | group, consumer, min_idle_ms, count | JSON claimed | Claim idle messages |
| `GNODE_PROTOCOL_INFO` | — | — | `{stream_count, total_messages}` | Protocol usage stats |

---

### gnode_pubsub — 5 functions

Channel-based pub/sub with history.

| Function | Keys | Args | Returns | Description |
|----------|------|------|---------|-------------|
| `GNODE_PUBSUB_PUBLISH` | channel | site_id, message, options_json | `{message_id, subscriber_count}` | Publish to channel |
| `GNODE_PUBSUB_SUBSCRIBE` | channel | consumer_id, site_id | `{ok, subscriber_count}` | Subscribe to channel |
| `GNODE_PUBSUB_UNSUBSCRIBE` | channel | consumer_id, site_id | `{ok, subscriber_count}` | Unsubscribe from channel |
| `GNODE_PUBSUB_LIST_SUBSCRIBERS` | channel | site_id | JSON array of subscriber IDs | List subscribers |
| `GNODE_PUBSUB_GET_HISTORY` | channel | site_id, count, since_id | JSON messages | Get message history |

---

### gnode_transaction — 5 functions

Client-side multi-operation transaction emulation.

| Function | Keys | Args | Returns | Description |
|----------|------|------|---------|-------------|
| `GNODE_TRANSACTION_BEGIN` | — | tx_id, site_id | `{ok, tx_id}` | Begin transaction |
| `GNODE_TRANSACTION_ADD_OP` | — | tx_id, op_type, key, value, site_id | `{ok, op_count}` | Queue an operation |
| `GNODE_TRANSACTION_COMMIT` | — | tx_id, site_id | JSON per-op results | Execute all queued ops |
| `GNODE_TRANSACTION_ROLLBACK` | — | tx_id, site_id | `{ok, ops_discarded}` | Discard queued ops |
| `GNODE_TRANSACTION_STATUS` | — | tx_id, site_id | `{status, op_count, ops}` | Get pending transaction status |

---

### gnode_utils — 8 functions

Utilities: key building, metrics, validation, server info.

| Function | Keys | Args | Returns | Description |
|----------|------|------|---------|-------------|
| `GNODE_UTILS_BUILD_KEY` | — | site_id, key, group, prefix | namespaced key | Build site-isolated key |
| `GNODE_UTILS_TRACK_METRIC` | — | site_id, metric_type, value, extra_json | `{ok}` | Track metric |
| `GNODE_UTILS_VALIDATE_KEYS` | — | site_id, keys_json | `{valid, invalid, reasons}` | Validate key batch |
| `GNODE_UTILS_SERVER_INFO` | — | — | JSON server info | ValKey server info |
| `GNODE_UTILS_DETECT_CYCLES` | — | site_id, node, edges_json | `{has_cycle, cycle_path}` | Graph cycle detection |
| `GNODE_UTILS_VALIDATE_PERMISSIONS` | — | permissions_json | `{allowed, reason}` | Validate against ACL rules |
| `GNODE_KEYS_PATTERN` | — | pattern, site_id | JSON array of keys | SCAN-based key search |
| `GNODE_UTILS_GET_TIME` | — | — | `{unix_s, unix_ms, iso8601}` | Server time |

---

### gnode_gcore_config — 9 functions

gCore manager-config homogenization. Scopes config (and secrets) per gCore
manager (CacheManager, ResourceManager, …) with a site-override → global-default
fallback chain. Mirrors the `gnode_config` precedent. Args are positional (no `KEYS`).

| Function | Keys | Args | Returns | Description |
|----------|------|------|---------|-------------|
| `GCORE_MGR_CONFIG_GET` | — | site_id, manager, key, default? | value, default, or nil | Single lookup (site → global → caller default) |
| `GCORE_MGR_CONFIG_HGETALL` | — | site_id, manager | flat array [k, v, ...] | Merged effective config (defaults overlaid by site overrides) |
| `GCORE_MGR_CONFIG_SET` | — | site_id, manager, key, value | 1 new / 0 updated | Write a per-site override |
| `GCORE_MGR_CONFIG_SEED` | — | site_id, manager, json, mode=NX\|OVERWRITE | count written | Bulk seed; NX is idempotent (installer default) |
| `GCORE_MGR_CONFIG_DELETE` | — | site_id, manager, key | 1 removed / 0 | Remove a per-site override (falls back to default) |
| `GCORE_MGR_CONFIG_VERSION` | — | site_id, manager | version (int, 0 if unset) | Config version counter for change detection |
| `GCORE_MGR_SECRETS_GET` | — | site_id, manager, key, default? | value, default, or nil | Secrets-keyspace lookup (no metric tracking) |
| `GCORE_MGR_SECRETS_SET` | — | site_id, manager, key, value | 1 new / 0 updated | Write a per-site secret |
| `GCORE_MGR_SECRETS_SEED` | — | site_id, manager, json, mode=NX\|OVERWRITE | count written | Bulk seed secrets |

---

### gnode_analytics — 3 functions

Server-side visitor analytics that run at the data. A front-end beacon posts to a gCore/gTemplate REST endpoint, which resolves site + visitor hash + bot flag server-side and calls `GNODE_ANALYTICS_HIT`; the operator dashboard reads everything back through `GNODE_ANALYTICS_SUMMARY`. All keys are hash-tagged `{site_id}:` so per-site ACLs (`~{site}:*`) can serve them, and every key carries a 90-day EXPIRE.

| Function | Keys | Args | Returns | Description |
|----------|------|------|---------|-------------|
| `GNODE_ANALYTICS_HIT` | — | site_id, visitor_hash, page, referrer_host, is_bot, ts, ymd | `"OK"` | Record one hit atomically across the per-day bot/page/visit/visitor/referrer/transition keys |
| `GNODE_ANALYTICS_CLICK` | — | site_id, visitor_hash, label, page, is_bot, ts, ymd | `"OK"` | Record one labelled click on a page, keyed like a hit |
| `GNODE_ANALYTICS_SUMMARY` | — | site_id, ymd1 [, ymd2 ...] | `{pages_served, unique_visitors, avg_pages_per_visitor, human_hits, bot_hits, top_pages, top_referrers, top_paths, daily}` | Aggregate a set of days for the operator dashboard. `no-writes` |

---

## Extension Libraries

### gnode_asset — 10 functions (gNode-CMS)

Asset storage and bundle manifest management.

| Function | Keys | Args | Returns | Description |
|----------|------|------|---------|-------------|
| `GNODE_ASSET_STORE` | — | asset_id, content, type, ttl, site_id, version, compressed | `{ok, asset_id, version}` | Store asset |
| `GNODE_ASSET_GET` | — | asset_id, site_id | `{asset_id, content, content_type, version}` | Retrieve asset |
| `GNODE_ASSET_DELETE` | — | asset_id, site_id | `{ok, deleted}` | Delete asset |
| `GNODE_ASSET_EXISTS` | — | asset_id, site_id | `{exists}` | Check asset existence |
| `GNODE_ASSET_LIST` | — | site_id, type_filter, cursor, count | `{assets, cursor, count}` | List assets with pagination |
| `GNODE_ASSET_MANIFEST_SET` | — | manifest_id, manifest_json, site_id | `{ok, manifest_id}` | Store bundle manifest |
| `GNODE_ASSET_MANIFEST_GET` | — | manifest_id, site_id | JSON manifest | Retrieve manifest |
| `GNODE_ASSET_MANIFEST_DELETE` | — | manifest_id, site_id | `{ok}` | Delete manifest |
| `GNODE_ASSET_MANIFEST_LIST` | — | site_id | JSON array of IDs | List manifests |
| `GNODE_ASSET_BUILD_STATUS` | — | manifest_id, site_id | `{manifest_id, status, built_at}` | Manifest build status |

---

## Summary

| Tier | Libraries | Functions | Commands |
|------|-----------|-----------|----------|
| Base | 23 | 210 | 39 |
| CMS (default companion) | 1 | 10 | 19 |
| **Total (core + CMS)** | **24** | **220** | **58** |

Additional signed extensions are commercial and load from `$GNODE_EXT_DIR`;
they contribute their own libraries, functions, and commands and are
documented separately, outside this distribution.

Lua libraries are loaded via `./scripts/load-valkey-functions.sh`, which
scans the base `daemon/functions/` directory plus any extension
`functions/` directories registered by the discovery mechanism.
