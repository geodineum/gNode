# gNode :: CONTRACT primer (SCN)
> one-line: SCN primer — TRUTH = code on disk, this file is a point-in-time compression. Companion: CONTRACT.md (authoritative); full catalog: COMMAND_SCHEMA.md.

## ::ROLE
gNode = the **Sun** of each Constellation. Stateless (tokio async) Rust daemon `gnode-daemon v0.1.0`, edition 2021. Owns: RESP3 command-stream wire protocol + unified multi-tenant routing + one topology + signed-extension pipeline, sitting between PHP services and ValKey. State-aware-not-stateful: ∀ state ∈ ValKey; nothing in-process survives restart.

## ::ANCHOR
- Stream keys: `{site_id}:gnode:unified:{env}` (cmds, config.rs) · `{ss}:res:{id}` `EX 10` (response poll, concurrent_lane.rs) · `{site_id}:gnode:receipts:{env}` (signed receipts, MINID 30d, receipt.rs) · `{topology_ns}:gnode:receipt_pubkeys` (HASH signer_id→`alg:pubkey_hex`) · `{site_id}:gnode:health` (NO env suffix, config.rs::build_health_stream_key; groups gnode-workers|gnode-daemon, acked by the group that read it) · `{site_id}:gnode:broadcast` (env-indep, config.rs) · `{site_id}:gnode:comms:{env}` (COMMS-owned, gNode provisions) · `{topology_ns}:gnode:topology` (registry).
- Wire resolver: `utils::field_names` canonical alias lists utils.rs; `utils::get_field(map,keys)` utils.rs.
- Persistence mark: `pz` (aliases `persist`, `persistence`) on any entry; `1` = STORE keeps it, `0` = never kept (beats a detector), absent = detectors decide; utils.rs field_names::PERSIST.
- Aliases: t/type · id/request_id · c/cmd/command/command_name · p/params/parameters · ss/source_site/service_id/site_id/st · sn/source_node/node_id/n · ds/dest_site · dn/dest_node · st/s/status(resp) · r/result · e/error · ri · ts/timestamp · bi · tc · _rt(relay_target) · _rr(relay_reply_to) · _gh(group_hint).
- Types: `Command`{id,command,parameters:Value,site_id,node_id,ts} · `Response`{id,status,result,error,ts,batch_id,sequence} · `Lane`{Concurrent|Ordered} (handlers/types.rs) · `CommandDescriptor` · `RelayDecision`{Forward{target_site_id,target_stream_key,target_entity_id}|Local|NotFound|Error} · `OptimizedCommand` · `ComputeRequest/Response` · `CustomTopology`{name,dimensions,schema,entities,edges,metadata}.
- Counts: 39 base cmds / 9 categories + 19 CMS cmds. 23 Lua libs / 210 fns + 1 CMS lib (gnode_asset.lua) / 10 = **220** FCALL fns (COMMAND_SCHEMA.md; `php scripts/check-command-schema.php --bin=<base build>` keeps header, rows and Summary honest).
- ONE topology per constellation, 7 shapes (1 node · 2 component · 3.1 site · 3.2 app · 3.3 daemon · 4.1 component@node · 4.2 service@node), width 24; named topologies `{c}:gnode:topo:<name>` run on the one library (D16, Pro creation). **Slot maps NOT quoted** — `FCALL_RO GNODE_SCHEMA_GET <ns> <shape key>`; prose copies drift silently. Registrable by name: every shape (nodes by the daemon or `--tier node`, components from the catalogue, sites/apps/daemons from manifests or profiles; placements by heartbeat).
- Component affordances: `entity.m.affordances` = [`scheme:target`] — `describe:<component>` (machine-readable cmd schemas) | `cli:<repo>/geodeploy.yaml` (the manifest declaring its commands; `geodineum --help` renders them) | `doc:<repo>/<path>` (floor). Returned verbatim by `service_describe`.m. Sibling of `schema_keys`, same pointer-to-artifact role. Declared in the installer's config/ecosystem_tools.yaml (Geodineum repo, not gNode; /etc/geodineum/ecosystem_tools.yaml read first).
- Ext: ext_author.rs (AUTHOR_PUBKEY `2ff9966fcad06b6d`) + ext_verify.rs (verify_strict) + extensions/mod.rs; load from `$GNODE_EXT_DIR`.

## ::ARCHITECTURE
Rust + tokio. Single-threaded consumer-group dispatch loop: `XREADGROUP` unified streams → parse field-aliased RESP3 via `utils::field_names` (command_processor.rs) → dispatch to 39 cmds in 9 handler categories (system, geometric, one_topology, service, config, stream, direct_channel, introspection, diagnostic). **Lane::Concurrent** = spawn async task (concurrent_lane.rs), write resp async to `{ss}:res:{id}`. **Lane::Ordered** (7 cmds: channel_open, channel_close, config_set, topology_edge_add/remove/list/validate) = synchronous inline, BLOCKS consumer thread. Topology = ONE key family per constellation `{c}:gnode:constellation:*` (SPEC-ONE-TOPOLOGY): 24-slot Q64.64 points formed by g_math from declarations by NAME through the compiled topology_schema.yaml; indexes + facet sets in Lua; routing imposed (GNODE_TOPO_ROUTE weighted round-robin over a service's ring of 4.2 placements, standby weight 0). Relay (relay/router.rs) resolves `_rt` ONCE per command: entity id → ring → placement stream `{service}:gnode:unified:<env>:<node>`, or a JSON query `{layer,facets,axes}` → service by colour → ring; sites refused as targets; permission = GNODE_TOPO_CAN_REACH (REACH labels + deny list, fail closed). Relay SOURCE = hash tag of the stream read from (ACL-authenticated), never the caller's `ss`; each unified stream (site or placement) is read beside its OWN site's health stream (consumer_groups.rs health_twins); Stream discovery (stream_discovery.rs) auto-subscribes ∀ registered (site,env). 
Philosophy: stateless (all state ValKey) · ONE canonical wire resolver (utils::field_names) · one-shot discovery, no cross-restart cache · fail-soft ext load (reduced feature set) · alias backward-compat (st/n/site_id/node_id ≡ ss/sn) · async-first + sync fallback · RESP3-native FCALL (pcall-wrapped JSON) · pre-XADD schema validation · per-site rate-limit + circuit-breaker · explicit DTAP gating (non-prod never auto-provisions) · relay permission fail-closed · runtime-introspectable schemas (`describe`).

## ::IO
IN ← ValKey `XREAD/XREADGROUP` `{site_id}:gnode:unified:{env}` = `[[stream_key,[[entry_id,[field,value,…]],…]],…]` aliased fields. ← FCALL results = JSON strings (cjson.encode, pcall, gnode_stream.lua). ← the constellation key `{c}:gnode:constellation` (entities by layer, placements on this node). ← REACH labels + deny list (gnode_topo.lua). ← declarations by name via register_service / the manifest gnode: block. ← DTAP env = stream-key suffix (config.rs, dtap_schema.yaml).
OUT → `SET {ss}:res:{id} '<json>' EX 10` (plain JSON: {id,status,result,error,timestamp:float}). → `XADD {ss}:gnode:receipts:{env}` signed receipt beside every keyed reply (ed25519 NodeSigner, fail-closed: unsigned never emitted; body_ref=res-key, bh=sha256; receipt.rs). → FCALL exec on ValKey. → relayed cmd to `{target}:gnode:unified:{env}`. → provisions COMMS stream via provision_service.

## ::CONTRACT
PROVIDES → unified-stream cmd protocol (t=c {id,t,c,p,ss,sn,ts}; p=JSON-string) | response (t=r {id,t,st,r,e,ri,ts}) | batch (t=bc {bi,tc,p:{commands:[…]}}) | 210 FCALL fns `^(GNODE|GCUBE|COMMS|GC)_…` | topology record {id,sh,env,ly,pd[24],pr[24],q,c,m,ra,ro (+of,on,hb,w for placements)} | declaration by name {shape, axes: words} → one 24-slot point | signed-ext loader | inter-svc relay via `_rt`/`_rr`.
CONSUMES ← ValKey 7.2+ (FUNCTION LOAD, RESP3, consumer groups, streams) | Lua fns daemon/functions/*.lua | topology key | reach labels and denies | DTAP env from key suffix.

## ::USECASES
- Service self-register (register_service by NAME: shape + axes as words; the daemon codes) → others `geometric_discover` {layer, facets, axes} (one GNODE_TOPO_QUERY; records carry their colour) or relay by `_rt` (the ring picks the placement).
- Discovery: geometric_discover {layer, facets, axes} → GNODE_TOPO_QUERY: layer interval, facet sets, axis-range index lookups; unknown axes and words are errors.
- Notification queue → `{site}:gnode:comms:{env}` (gNode ignores, COMMS consumes/dispatches/ACK-after-SQLite).
- Inter-service relay via `_rt` → resolve service→ring→CAN_REACH → forward.
- Named topologies: `{c}:gnode:topo:<name>` on gnode_topo.lua; creation is a Pro (gNode-TOPO) command (D16); `GNODE_TOPO_CREATE/LIST/DELETE` take keys[1]=`{c}:gnode:topo:registry` and refuse a topology key of another hash tag or not `{c}:gnode:topo:<name>`. The JSON-document engine and topo_create/topo_register 3-D model retired 2026-10-11.
- Config distribution: config_set ↔ GNODE_NODE_FETCH_CONFIG via `{node_type}:gnode:config:*`.
- Direct hot-pair channels: channel_open → dedicated stream, daemon steps out of data path.
- Health: daemon heartbeat `{c}:gnode:heartbeat:<env>:gnode-daemon:<node>` per SERVED constellation → node + placement health on the one topology (no `gnode:node:*` record). Reconcile: master, 120 s, per served c, intents vs entities, drift → WARN, never writes.

## ::LIMITATIONS
- Unclassifiable entries (no resolvable `t`/`type`) are ACKed+dropped with a rate-limited WARN (30s window, suppressed-count) — was fully silent until 2026-08-07.
- No cross-request ordering on Concurrent lane (only within one synchronous client batch).
- Response poll TTL fixed 10s (concurrent_lane.rs) → silent expiry on slow poll, no retry.
- 9 Ordered cmds block whole consumer thread, no per-cmd timeout → 1 slow handler stalls all tenants.
- Single hardcoded ed25519 pubkey (ext_author.rs); rotation = recompile, no key versioning.
- Custom topo dims user-supplied + unversioned → semantic change invalidates coords, no schema evolution.
- g_math Q64.64 precision ±2^-64/dim; 1000+ dims or very-high-precision may overflow, unvalidated in field.
- MAXLEN bounds msg count not size; per-msg cap now 64KB (parse_params_safely, command_processor.rs), wasn't always enforced.
- Consumer-group orphan window: crash between read and XACK → orphaned until idle-reclaim (default timeout unspecified).
- Relay permission = GNODE_TOPO_CAN_REACH, fail closed (no default allow, no allow on store error).
- DTAP from key suffix only; wrong-env XADD NOT caught (gNode has no non-prod gate, unlike COMMS).
- Lua returns JSON strings; huge results (1000+ entities) = one RESP string, no pagination/streaming.
- Cross-site federated queries require extension binaries; core does not federate.
- No global backpressure; rate-limit per-site only → one tenant can monopolize async runtime via slow FCALLs.

## ::GRAPH
DEPENDS_ON: ValKey 7.2+ · g_math 0.4.0 (Q64.64) · daemon/functions/*.lua · Geodineum installer (bootstrap/creds/schemas).
PROVIDES_TO: PHP gNode-Client (gCore/gNode/gNodeClient) · gCore · gTemplate · child themes (gCube et al.) · Geodineum-COMMS (provisions its stream).
ADHERES_TO: RESP3 unified-stream field contract ↔ gNode-Client (utils.rs, ✓ no legacy aliases emitted) · FCALL allowlist `^(GNODE|GCUBE|COMMS|GC)_` ↔ gNode-Client (✓ superset gate) · Ed25519 signed-ext scheme ↔ gNode-CMS (✓ pubkey 2ff9966fcad06b6d) · comms wire format ↔ COMMS parse_message (✓ producers stamp top-level scalar environment).
ISOLATED_FROM: `{site_id}:comms:config` (COMMS-owned settings, gNode no knowledge) · COMMS SQLite archive · COMMS non-prod gating logic.
SAMPLER: two observation sources — the daemon's OWN command handling (`{ns}:gnode:health`, si=node_id → node load, the traffic this estate has) and relayed requests (`{target}:gnode:health`, si=target entity → service `load`). WHO IS MEASURED = WHO DID THE WORK: the daemon for commands it runs, the TARGET for commands it relays, NEVER the requester. Relayed `lat` = ROUND TRIP between two stream entry ids on the target's stream (forward accepted → reply accepted; the store's clock, no daemon read lag); departure in `{ns}:gnode:relay:sent:<correlation_id>` 24h, claimed by GETDEL at the reply = exactly-once across groups and nodes; wrong replier stream credits nobody; unanswered = no observation = `unknown`; ONLY the owning service's `_rr` reply closes a trip and it is the ONLY reply: the daemon never answers a cmd carrying `_rt`/`_rr`, handler or not (one `gnode-daemon` group across nodes → first reader would answer and win the origin's res key the service's ACL cannot overwrite; reply.rs `addressed_to_another_party`); a service that never reads its stream is never measured and a relayed `ping` to it times out = truth. PRODUCER `by=gnode` on every arbitrator record; sampler reads ONLY `by=gnode` (`record_from_fields`); a provider's self-report (`by=self`, Geodine, carries `pipeline`; missing `by` = self) is telemetry — clock starts after the read, never sees the queue, cannot inflate, would halve the signal. Both `t=rq by=gnode si cmd lat ok ts` (MAXLEN ~2000) · lease holder aggregates via OWN group `gnode-sampler` · baseline `{site}:gnode:baseline` H `<entity>|<command>`→p50ms (EWMA .05, learns only below 1.5× inflation) · ρ = 1−1/I, pd = .20+.80ρ (0.00 = unknown, never measured) · write on band change (up 1 tick, down 2) or stamp refresh at horizon/2 · `GNODE_SAMPLER=off|shadow|write` (shadow default) · `geodineum topology load [site]` reads it back.
ZONES: per-shape slot map from the published schema; load/health/lifecycle are DERIVED (written by the lease holder via GNODE_TOPO_SET_DERIVED, 0.00=unknown); all other axes are declared by name.
RISK: `st` overloaded source_site(t=c)|status(t=r), disambig by `t` ⚠ · 10s poll TTL ⚠ · Ordered-lane thread block ⚠ · face_mapping braced(gCube)/unbraced(other child themes) latent split, normalized only by build_key gnode_cache.lua ⚠ · health consumer CONFIRMED 2026-09-29 (t='lu' only, others dismissed+acked).

## ::LATENT
- "ONE canonical wire resolver — utils::field_names, ss/sn over legacy st/n, st overloaded by message type t"
- "stateless Sun: all topology/registry/telemetry rediscovered from ValKey on restart, nothing in-process"
- "Lane::Concurrent async-to-{ss}:res:{id}-EX10 vs Lane::Ordered 7-cmds-block-consumer-thread"
- "Declarations by name → g_math Q64.64 points through one compiled schema → one indexed topology per constellation; routing imposed by the ring, never ranked by distance"
- "relay `_rt` resolve service→ring→CAN_REACH (fail closed)→forward to {target}:gnode:unified:{env} stamping `_rr`=stream read + `ss`=its site over the caller's (stamp_origin: targets scope by them); Deny/NotFound/Error verdict keyed on origin `{site}:res:<id>` (status error, 10 s) via response_key::plan beside the stream reply — a refusal is never a timeout; the `relayed:true` ack is stream-only"
- "213 FCALL fns GNODE_* pcall-wrapped JSON strings, allowlist ^(GNODE|GCUBE|COMMS|GC)_"
- "ed25519 signed .so from $GNODE_EXT_DIR, fail-soft, single hardcoded pubkey 2ff9966fcad06b6d"
- "gNode provisions {site}:gnode:comms:{env} but never reads it — COMMS owns that stream"
