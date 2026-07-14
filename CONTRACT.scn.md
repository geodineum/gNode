# gNode :: CONTRACT primer (SCN)
> one-line: SCN primer — TRUTH = code on disk, this file is a point-in-time compression. Companion: CONTRACT.md (authoritative); full catalog: COMMAND_SCHEMA.md.

## ::ROLE
gNode = the **Sun** of each Constellation. Stateless (tokio async) Rust daemon `gnode-daemon v0.1.0`, edition 2021. Owns: RESP3 command-stream wire protocol + unified multi-tenant routing + geometric topology + signed-extension pipeline, sitting between PHP services and ValKey. State-aware-not-stateful: ∀ state ∈ ValKey; nothing in-process survives restart.

## ::ANCHOR
- Stream keys: `{site_id}:gnode:unified:{env}` (cmds, config.rs) · `{ss}:res:{id}` `EX 10` (response poll, fast_lane.rs) · `{site_id}:gnode:health:{env}` (config.rs) · `{site_id}:gnode:broadcast` (env-indep, config.rs) · `{site_id}:gnode:comms:{env}` (COMMS-owned, gNode provisions) · `{topology_ns}:gnode:topology` (registry).
- Wire resolver: `utils::field_names` canonical alias lists utils.rs; `utils::get_field(map,keys)` utils.rs.
- Aliases: t/type · id/request_id · c/cmd/command/command_name · p/params/parameters · ss/source_site/service_id/site_id/st · sn/source_node/node_id/n · ds/dest_site · dn/dest_node · st/s/status(resp) · r/result · e/error · ri · ts/timestamp · bi · tc · _rt(relay_target) · _rr(relay_reply_to) · _gh(group_hint).
- Types: `Command`{id,command,parameters:Value,site_id,node_id,ts} · `Response`{id,status,result,error,ts,batch_id,sequence} · `Lane`{Fast|Ordered} (handlers/types.rs) · `CommandDescriptor` · `RelayDecision`{Forward{target_site_id,target_stream_key,target_entity_id}|Local|NotFound|Error} · `OptimizedCommand` · `ComputeRequest/Response` · `CustomTopology`{name,dimensions,schema,entities,edges,metadata}.
- Counts: 60 base cmds / 11 categories + 23 CMS cmds. 23 Lua libs / 203 fns + 1 CMS lib (gnode_asset.lua) / 10 = **213** FCALL fns (COMMAND_SCHEMA.md).
- Tiers: service:30D · tool:16D · constellation:20D · galaxy:20D + custom (arbitrary D) → custom_topology.rs.
- Ext: ext_author.rs (AUTHOR_PUBKEY `2ff9966fcad06b6d`) + ext_verify.rs (verify_strict) + extensions/mod.rs; load from `$GNODE_EXT_DIR`.

## ::ARCHITECTURE
Rust + tokio. Single-threaded consumer-group dispatch loop: `XREADGROUP` unified streams → parse field-aliased RESP3 via `utils::field_names` (command_processor.rs) → dispatch to 60 cmds in 11 handler categories (system, geometric, topology, service, config, stream, +direct_channel, relay_ops, introspection, diagnostic, custom_topology). **Lane::Fast** = spawn async task (fast_lane.rs), write resp async to `{ss}:res:{id}`. **Lane::Ordered** (7 cmds: topo_create, topo_delete, channel_open, channel_close, relay_policy_set, relay_policy_remove, config_set) = synchronous inline, BLOCKS consumer thread. Topology state stateless in ValKey under `{topology_ns}:gnode:*` + `{site_id}:gnode:*`. Geometric precision = g_math 0.4.0 Q64.64 fixed-point → deterministic cross-node bucket keys + Z-scores. Relay (relay_ops.rs) resolves `_rt` → entity_id→site | site_id→direct | JSON capability→geometric discovery. Stream discovery (stream_discovery.rs) auto-subscribes ∀ registered (site,env). 
Philosophy: stateless (all state ValKey) · ONE canonical wire resolver (utils::field_names) · one-shot discovery, no cross-restart cache · fail-soft ext load (reduced feature set) · alias backward-compat (st/n/site_id/node_id ≡ ss/sn) · async-first + sync fallback · RESP3-native FCALL (pcall-wrapped JSON) · pre-XADD schema validation · per-site rate-limit + circuit-breaker · explicit DTAP gating (non-prod never auto-provisions) · relay default=ALLOW (explicit *:* DENY to fail-closed) · runtime-introspectable schemas (`describe`).

## ::IO
IN ← ValKey `XREAD/XREADGROUP` `{site_id}:gnode:unified:{env}` = `[[stream_key,[[entry_id,[field,value,…]],…]],…]` aliased fields. ← FCALL results = JSON strings (cjson.encode, pcall, gnode_stream.lua). ← site registry `{topology_ns}:gnode:topology`. ← relay policy (RelayPolicy, router.rs). ← geometric input Q64.64 buckets/Z-scores via register_service/topo_register. ← DTAP env = stream-key suffix (config.rs, dtap_schema.yaml).
OUT → `SET {ss}:res:{id} '<json>' EX 10` (plain JSON: {id,status,result,error,timestamp:float}). → FCALL exec on ValKey. → relayed cmd to `{target}:gnode:unified:{env}`. → provisions COMMS stream via provision_service.

## ::CONTRACT
PROVIDES → unified-stream cmd protocol (t=c {id,t,c,p,ss,sn,ts}; p=JSON-string) | response (t=r {id,t,st,r,e,ri,ts}) | batch (t=bc {bi,tc,p:{commands:[…]}}) | 213 FCALL fns `^(GNODE|GCUBE|COMMS|GC)_…` | topology entity JSON {id,x,y,z,bk,zs,ra,m} | capability vector {dim:float} → tier D | signed-ext loader | inter-svc relay via `_rt`/`_rr`.
CONSUMES ← ValKey 7.2+ (FUNCTION LOAD, RESP3, consumer groups, streams) | Lua fns daemon/functions/*.lua | site registry | relay policies | DTAP env from key suffix.

## ::USECASES
- Service self-register (register_service → Q64.64 bucket store) → others `discover` daemon cmd (native geometric ranking; semantic discovery is NOT a Lua fn).
- Capability discovery: geometricDiscover({dims},limit) → bucket key → GNODE_TOPO_QUERY_VOXEL → rank top-N.
- Notification queue → `{site}:gnode:comms:{env}` (gNode ignores, COMMS consumes/dispatches/ACK-after-SQLite).
- Inter-service relay via `_rt` → resolve entity→site → policy → forward.
- Custom multi-tenant topology (topo_create N custom dims) → `custom_topology_discover` daemon cmd.
- Config distribution: config_set ↔ GNODE_NODE_FETCH_CONFIG via `{node_type}:gnode:config:*`.
- Direct hot-pair channels: channel_open → dedicated stream, daemon steps out of data path.
- Health: GNODE_NODE_HEARTBEAT → GNODE_NODE_AGGREGATE_METRICS → health/topology_heatmap.

## ::LIMITATIONS
- No cross-request ordering on Fast lane (only within one synchronous client batch).
- Response poll TTL fixed 10s (fast_lane.rs) → silent expiry on slow poll, no retry.
- 7 Ordered cmds block whole consumer thread, no per-cmd timeout → 1 slow handler stalls all tenants.
- Single hardcoded ed25519 pubkey (ext_author.rs); rotation = recompile, no key versioning.
- Custom topo dims user-supplied + unversioned → semantic change invalidates coords, no schema evolution.
- g_math Q64.64 precision ±2^-64/dim; 1000+ dims or very-high-precision may overflow, unvalidated in field.
- MAXLEN bounds msg count not size; per-msg cap now 64KB (parse_params_safely, command_processor.rs), wasn't always enforced.
- Consumer-group orphan window: crash between read and XACK → orphaned until idle-reclaim (default timeout unspecified).
- Relay precedence exact→src-wild→tgt-wild→ALLOW; no fail-closed default, must add `*:*` DENY.
- DTAP from key suffix only; wrong-env XADD NOT caught (gNode has no non-prod gate, unlike COMMS).
- Lua returns JSON strings; huge results (1000+ entities) = one RESP string, no pagination/streaming.
- Cross-site federated queries require extension binaries; core does not federate.
- No global backpressure; rate-limit per-site only → one tenant can monopolize async runtime via slow FCALLs.

## ::GRAPH
DEPENDS_ON: ValKey 7.2+ · g_math 0.4.0 (Q64.64) · daemon/functions/*.lua · Geodineum installer (bootstrap/creds/schemas).
PROVIDES_TO: PHP gNode-Client (gCore/gNode/gNodeClient) · gCore · gTemplate · child themes (gCube et al.) · Geodineum-COMMS (provisions its stream).
ADHERES_TO: RESP3 unified-stream field contract ↔ gNode-Client (utils.rs, ✓ no legacy aliases emitted) · FCALL allowlist `^(GNODE|GCUBE|COMMS|GC)_` ↔ gNode-Client (✓ superset gate) · Ed25519 signed-ext scheme ↔ gNode-CMS (✓ pubkey 2ff9966fcad06b6d) · comms wire format ↔ COMMS parse_message (✓ producers stamp top-level scalar environment).
ISOLATED_FROM: `{site_id}:comms:config` (COMMS-owned settings, gNode no knowledge) · COMMS SQLite archive · COMMS non-prod gating logic.
RISK: relay default=ALLOW fail-open ⚠ · `st` overloaded source_site(t=c)|status(t=r), disambig by `t` ⚠ · 10s poll TTL ⚠ · Ordered-lane thread block ⚠ · face_mapping braced(gCube)/unbraced(other child themes) latent split, normalized only by build_key gnode_cache.lua ⚠ · health compressed-field consumer side UNCONFIRMED.

## ::LATENT
- "ONE canonical wire resolver — utils::field_names, ss/sn over legacy st/n, st overloaded by message type t"
- "stateless Sun: all topology/registry/telemetry rediscovered from ValKey on restart, nothing in-process"
- "Lane::Fast async-to-{ss}:res:{id}-EX10 vs Lane::Ordered 7-cmds-block-consumer-thread"
- "Q64.64 g_math fixed-point → deterministic cross-node bucket keys + Z-scores → geometric capability discovery"
- "relay `_rt` resolve entity→site→policy(default ALLOW)→forward to {target}:gnode:unified:{env}, `_rr` overrides reply"
- "213 FCALL fns GNODE_* pcall-wrapped JSON strings, allowlist ^(GNODE|GCUBE|COMMS|GC)_"
- "ed25519 signed .so from $GNODE_EXT_DIR, fail-soft, single hardcoded pubkey 2ff9966fcad06b6d"
- "gNode provisions {site}:gnode:comms:{env} but never reads it — COMMS owns that stream"
