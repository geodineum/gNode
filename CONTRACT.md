# gNode — Integration Contract

**Role:** Stateless Rust daemon (tokio async) that owns the RESP3 command-stream wire protocol, unified multi-tenant routing, the geometric topology system, and the signed-extension pipeline between PHP services and ValKey. All state lives in ValKey; the daemon rediscovers everything on restart.

> This file is the human-readable **integration contract** — the wire format, stream keys, how to send a command, and the signed-extension expectation. For the exhaustive catalog of every command and Lua function, see **[COMMAND_SCHEMA.md](COMMAND_SCHEMA.md)** (do not duplicate it here).

---

## 1. PROVIDES (interfaces other components may rely on)

| Interface | Kind | Signature / Key | Evidence |
|---|---|---|---|
| Daemon commands | command | 60 base commands across 10 categories (system, geometric, topology, service, config, stream, relay, diagnostic, direct_channel, custom_topology) + 23 CMS extension commands, delivered via RESP3 `XADD` to the unified stream | COMMAND_SCHEMA.md, daemon/src/integration/handlers/ |
| ValKey Lua functions | fcall | `FCALL <function_name> <numkeys> [keys...] [args...]` — 23 base libraries (203 functions) + 1 CMS library (10 functions) = **213** total, registered via `FUNCTION LOAD` (ValKey 7.2+) | COMMAND_SCHEMA.md, daemon/functions/*.lua |
| Unified stream | stream | `{site_id}:gnode:unified:{environment}` — RESP3 command stream, field aliases resolved by `utils::field_names` | config.rs, COMMAND_SCHEMA.md |
| Response polling key | stream (kv) | `SET {ss}:res:{request_id} '<json>' EX 10` — written by daemon after execution | COMMAND_SCHEMA.md, integration/fast_lane.rs |
| Receipt stream | stream | `XADD {site_id}:gnode:receipts:{environment}` — signed durable receipt per keyed response (ed25519 per-node key; verifiers resolve `signer` via `{topology_ns}:gnode:receipt_pubkeys` HASH), MINID age-trim 30 d | integration/receipt.rs, installer CONTRACTS/receipt-stream.md |
| Health stream | stream | `{site_id}:gnode:health:{environment}` — optional health-check consumer groups | config.rs, integration/processor/health_processor.rs |
| Broadcast stream | stream | `{site_id}:gnode:broadcast` — one-to-many, environment-independent | config.rs, integration/processor/broadcast_reader.rs |
| Field-alias resolver | method | `utils::get_field(map, keys) -> String` — resolves canonical alias lists | utils.rs (alias lists) |
| Inter-service routing | command | Commands carrying `_rt` (relay target) are routed to the target site's unified stream; `_rr` overrides the reply-to stream. Resolution: entity lookup → `site_id` direct → JSON capability query | README.md, integration/relay/router.rs |
| Lane execution | method | `Lane::Fast` (async-spawned, no ordering) vs `Lane::Ordered` (synchronous inline). 7 Ordered commands: `topo_create`, `topo_delete`, `channel_open`, `channel_close`, `relay_policy_set`, `relay_policy_remove`, `config_set` | COMMAND_SCHEMA.md, integration/handlers/types.rs |
| Batch wrapper | command | `c=batch`, `p='{"commands":[{"id":...,"c":...,"p":{}}]}'` — groups commands for atomic-ish processing | COMMAND_SCHEMA.md |
| Topology schema tiers | data | 4 canonical tiers — service:30D, tool:16D, constellation:20D, galaxy:20D — plus user-defined custom topologies with arbitrary dimensions | README.md, daemon/config/service_schema.yaml, daemon/src/custom_topology.rs |
| Signed-extension loader | method | Loads ed25519-signed `.so` extensions from `$GNODE_EXT_DIR`; validates against the public key constant in `ext_author.rs` | daemon/src/extensions/mod.rs, ext_author.rs, ext_verify.rs |

---

## 2. CONSUMES / REQUIRES (what gNode needs, and from whom)

| Need | From component | Expected format | Evidence |
|---|---|---|---|
| Stream input | ValKey 7.2+ (unified streams + consumer groups) | `XREAD`/`XREADGROUP` on `{site_id}:gnode:unified:{env}` returns `[[stream_key, [[entry_id, [field,value,...]],...]],...]` with canonical field aliases (`t,id,c,p,ss,sn,ts,…`) | config.rs, utils.rs, command_processor.rs |
| Function execution | ValKey Lua functions (daemon/functions/*.lua) | `FCALL <fn> <numkeys> [keys...] [args...]`; responses are JSON strings (pcall-wrapped `cjson.encode`) | COMMAND_SCHEMA.md, daemon/functions/gnode_stream.lua |
| Site discovery | ValKey service topology (populated by `register_service`) | `StreamDiscoveryManager` reads registered sites from `{topology_namespace}:gnode:topology` and enumerates `(site, env)` pairs to auto-subscribe | stream_discovery.rs, compute_handler.rs |
| Relay policy | `relay_policy_set` result stored in ValKey | `RelayPolicy` matches `source:target` patterns: exact → source-wildcard → target-wildcard → default=**allow**; invoked on `_rt` | README.md, integration/relay/router.rs |
| Geometric input | Caller (PHP gNode-Client or direct XADD) | Q64.64 fixed-point bucket keys, Z-scores, entity coords via `register_service`/`topo_register`; Lua stores under `{topology_key}:entities:*`, `{topology_key}:edges:*` | README.md, COMMAND_SCHEMA.md, daemon/functions/gnode_topo.lua |
| DTAP environment | Caller (embedded in stream key) | Environment suffix on the stream key (`testing|staging|acceptance|production`) must match the daemon's `--environment` filter | config.rs (`DTAP_ENVIRONMENTS`), daemon/config/dtap_schema.yaml |

---

## 3. Wire formats

All field names are **canonical aliases** resolved EXACTLY by `daemon/src/utils.rs::field_names`. Producers should emit the canonical-first (compact) name; legacy aliases resolve identically but are discouraged.

### 3.1 Field-name alias table

| Canonical | Aliases | Meaning |
|---|---|---|
| `t` | `type` | message type |
| `id` | `request_id` | request id |
| `c` | `cmd`, `command`, `command_name` | command name |
| `p` | `params`, `parameters` | params (JSON string) |
| `ss` | `source_site`, `service_id`, `site_id`, `st` | source site |
| `sn` | `source_node`, `node_id`, `n` | source node |
| `ds` | `dest_site` | dest site |
| `dn` | `dest_node` | dest node |
| `st` | `s`, `status` | status (**response only**) |
| `r` | `result` | result |
| `e` | `error` | error message |
| `ri` | — | request_id (in response) |
| `ts` | `timestamp` | timestamp |
| `bi` | — | batch_id |
| `tc` | — | total_count |
| `_rt` | — | relay_target |
| `_rr` | — | relay_reply_to |
| `_gh` | — | group_hint |

> **`st` is overloaded.** In a command (`t=c`) `st` = source_site; in a response (`t=r`) `st` = status. The daemon disambiguates by the `t` field. `st`-as-source-site and `n`-as-source-node are **LEGACY** aliases (COMMAND_SCHEMA.md). New writers MUST use `ss`/`sn`.

### 3.2 Message shapes

**Command (`t=c`):**
```json
{"id":"", "t":"c", "c":"command_name", "p":"{...json...}", "ss":"source_site", "sn":"source_node", "ts":"<milliseconds>"}
```

**Response (`t=r`):**
```json
{"id":"", "t":"r", "st":"ok|error", "r":"{...json...}", "e":"error message", "ri":"<request_id>", "ts":"<milliseconds>"}
```

**Batch command (`t=bc`):**
```json
{"id":"", "t":"bc", "bi":"batch_id", "tc":"<count>", "p":"{\"commands\":[{\"id\":\"...\",\"c\":\"...\",\"p\":{}}]}"}
```

**Response polling key JSON** (plain JSON, *not* field-pair format):
```json
{"id":"", "status":"ok|error", "result":{...}, "error":null, "timestamp":<float>}
```

**Stream response (XREAD):** `[[stream_key, [[entry_id, [field1, value1, field2, value2, ...]],...]],...]`

**Topology entity JSON:**
```json
{"id":"", "x":<float>, "y":<float>, "z":<float>, "bk":"<bucket_key>", "zs":<z_score>, "ra":[outgoing_ids], "m":{metadata}}
```

**Capability vector:** `{"dimension_name": <float_value>, ...}` mapped to the tier's dimension count (30D for service tier).

**FCALL args (Lua):** positional, auto-JSON-encoded for non-scalar; keys array first, then args array per ValKey function spec.

### 3.3 Field requirement / type by message type

| Field | Command (`t=c`) | Response (`t=r`) | Type |
|---|---|---|---|
| `t` | required | required | scalar |
| `id` | required | — | scalar |
| `c` | required | — | scalar |
| `p` | required | — | **JSON-string** |
| `ss` | required | — | scalar |
| `sn` | required | — | scalar |
| `ts` | required (ms) | required (ms) | scalar |
| `st` | — | required (status) | scalar |
| `r` | — | optional | **JSON-string** |
| `e` | — | optional | scalar |
| `ri` | — | required | scalar |

---

## 4. Public types

```text
daemon::Command            { id, command, parameters: serde_json::Value, site_id, node_id, timestamp }
daemon::Response           { id, status: ok|error, result: Option<Value>, error: Option<String>, timestamp, batch_id, sequence }
handlers::types::Lane      enum { Fast (default, async-spawned), Ordered (synchronous inline) }
handlers::types::CommandDescriptor
                           { name, category, description, params_schema: JSONSchema, returns_schema: JSONSchema, example, async_capable, lane }
integration::RelayDecision enum { Forward { target_site_id, target_stream_key, target_entity_id }, Local, NotFound(String), Error(String) }
integration::OptimizedCommand
                           RESP3-native command struct with optional routing fields (relay_target, relay_reply_to, group_hint)
compute_handler::ComputeRequest
                           { command, parameters, requested_at, timeout_ms, site_id }
compute_handler::ComputeResponse
                           { status, result, error, error_code, computed_at, compute_time_ms, request_id }
custom_topology::CustomTopology
                           { name, dimensions (count), schema: dim_name→index map, entities, edges, metadata }
```

---

## 5. Copy-paste: send a command and poll the response

```text
# 1. Write a command to the site's unified stream (production environment)
XADD {mysite}:gnode:unified:production *
     id   req-001
     t    c
     c    register_service
     p    {"id":"svc-1","capabilities":{"compute":0.8,"latency_class":2}}
     ss   mysite
     sn   node-1
     ts   1718000000000

# 2. Poll the response polling key (10-second TTL — poll within a few seconds)
GET {mysite}:res:req-001
# -> {"id":"req-001","status":"ok","result":{...},"error":null,"timestamp":1718000000.123}

# 3. Call a Lua function directly (name MUST match ^(GNODE|GCUBE|COMMS|GC)_[A-Z0-9_]+$)
FCALL GNODE_TOPOLOGY_GET_SCHEMA 0
# (semantic discovery is a daemon command — send `discover` via the unified stream, not FCALL)
```

Inter-service relay: add `_rt service-b` to the command fields; the daemon resolves the target site, enforces relay policy, and forwards to `{site_b}:gnode:unified:{env}`. Add `_rr <stream>` to override the reply destination.

---

## 6. Cross-deps (who else is in the loop)

- **ValKey 7.2+** — `FUNCTION LOAD`, RESP3, consumer groups, streams. gNode reads commands from unified streams, runs Lua, writes responses to `{ss}:res:{id}`.
- **PHP gNode-Client** (`gCore/gNode/gNodeClient`) — `XADD`s commands, polls `{site_id}:res:{id}`, calls `FCALL`.
- **Geodineum-COMMS daemon** — reads `{site_id}:gnode:comms:{env}` (provisioned by gNode via `provision_service`), archives to SQLite, ACKs **only after persistence**. gNode does not read this stream.
- **gCore** (WordPress plugin ecosystem) — consumes topology discovery, service registration, custom-topology APIs via gNode-Client.
- **gTemplate** (Tera CMS extension) — relies on `GNODE_ASSET_*` and `render_template`.
- **Geodineum installer** — provisioning/bootstrap; writes `/etc/geodineum/credentials/*`, `config/dtap_schema.yaml`, `daemon/config/service_schema.yaml`.

---

## 7. Signed-extension expectation

Extensions are `.so` binaries loaded from `$GNODE_EXT_DIR`. Each ships:
- `extension.sig`, `extension.yaml`, handler files (e.g. `template/content/asset/format.rs`), and `lua_libraries` (e.g. `gnode_asset.lua`).
- `ext_verify.rs` computes a canonical hash over `extension.yaml` + handler files + lua libraries and `verify_strict` against `AUTHOR_PUBKEY` (fingerprint `2ff9966fcad06b6d`).

Verification is fail-soft: a failed/unsigned extension is skipped and the daemon runs with a reduced feature set. Rotating the key requires recompiling the daemon (single hardcoded key, no versioning).

---

## 8. Adherence (known cross-component facts & risks)

Verified-aligned (from ecosystem cross-check):
- **Comms message wire format** — gNode-Client (`queueCommsMessage`/`queueContactForm`), child themes and gTemplate direct-XADD all emit exactly the fields Geodineum-COMMS `parse_message` reads on brace-literal `{site_id}:gnode:comms:{env}`: scalar `id/type/timestamp/site_id/environment/priority` + JSON-string `sender/content/metadata/dispatch`. The non-prod-gating field is the **top-level scalar `environment`** (not nested `metadata.environment`). ✓
- **Unified command field names** — gNode-Client emits `t/c/p/id/ss/sn/ts`, matching `utils::field_names` (utils.rs) with no legacy `st/n` aliases. ✓
- **FCALL allowlist** — client-side `^(GNODE|GCUBE|COMMS|GC)_[A-Z0-9_]+$` (gNodeClient.php) is a correct superset of all 213 registered `GNODE_*` functions. `COMMS_/GCUBE_/GC_` are reserved, unused by core. ✓
- **Signed extension** — gNode-CMS verifies against `AUTHOR_PUBKEY` `2ff9966fcad06b6d`; every `GNODE_ASSET_*` the handlers call is registered in `gnode_asset.lua` (Lua registers a superset). ✓

Operator-relevant risks (design, not divergence):
- ⚠️ **Relay policy default = ALLOW (fail-open).** To lock down, operators must explicitly set `*:*` DENY (relay/policy.rs).
- ⚠️ **`st` overloaded** between source_site (`t=c`) and status (`t=r`); disambiguated by `t`. New writers use `ss`. (COMMAND_SCHEMA.md)
- ⚠️ **Response polling key TTL = 10s** (fast_lane.rs). Slow/late pollers silently lose the response; no retry. Poll within a few seconds, handle 404 gracefully.
- ⚠️ **`Lane::Ordered` commands block the consumer thread** with no per-command timeout; a slow handler stalls subsequent messages, including other tenants' (intentional per COMMAND_SCHEMA.md).

Latent inconsistency (not a live mismatch):
- The `face_mapping` cache key is written **braced** by gCube (`content-sync.php`) and **unbraced** by other child themes; `GNODE_CACHE_SET` `build_key` (gnode_cache.lua) normalizes both to identical `{site_id}:gnode:face_mapping`. Harmless today; would split the keyspace if either ever wrote via raw `XADD`/`SET`.

Unconfirmed:
- **Health-metrics compressed field names** (`t='lu',si,l,cpu,mem,rq,lat,err,ts`) on `{site_id}:gnode:health:{env}` — producer (gNode-Client `HealthMetrics`) verified; the daemon's `health_processor.rs` reader was not byte-for-byte confirmed in the audit pass.
