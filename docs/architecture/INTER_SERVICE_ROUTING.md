# Inter-Service Routing & Format Translation

gNode provides curated inter-service communication where all cross-service messages flow through the daemon. Services post to their own unified stream — gNode decides whether the message should reach the target, translates the format if needed, and delivers it. No stream-key pollution, no direct cross-keyspace access.

---

## How It Works

```
Service A                      gNode Daemon                     Service B
    │                               │                                │
    ├── XADD to own stream ────────►│                                │
    │   {cmd, params, _rt:"svc_b"}  │                                │
    │                               ├── resolve target               │
    │                               ├── check relay policy           │
    │                               ├── detect source format         │
    │                               ├── look up target format pref   │
    │                               ├── translate params if needed   │
    │                               ├── XADD to B's unified stream ─►│
    │◄── relay ack ────────────────┤                                │
    │   {relayed:true, target:...}  │                     processes ─┤
    │                               │                                │
    │◄──────────────── response via _rr (B writes to A's stream) ───┤
```

1. Service A sends a command to its own unified stream with `_rt` (relay target)
2. gNode resolves the target — by entity ID, site ID, or explicit stream key
3. Policy check — is A allowed to talk to B? (first-match rule, default: allow)
4. Format translation — if A speaks `standard_json` and B expects `compact_json`, params are converted automatically
5. gNode posts the translated command to B's unified stream with `_rr` pointing back to A
6. A receives an immediate relay-accepted ack (so it doesn't time out)
7. B processes the command normally and responds to the `_rr` stream
8. A reads the response from its own unified stream

Services never need to know each other's stream keys. They never need cross-keyspace ACL permissions. gNode handles all routing, policy, and translation.

---

## Sending a Relay Command

Add the `_rt` field to any command on your unified stream. Everything else stays the same.

### By site ID or entity ID

```json
{
  "i": "cmd-001",
  "t": "c",
  "c": "get_status",
  "p": "{\"verbose\": true}",
  "ss": "my_service",
  "sn": "worker_1",
  "_rt": "target_service"
}
```

gNode looks up `target_service` in the site registry. If not found as a site ID, it searches across all topologies for an entity with that ID.

### By explicit stream key

```json
{
  "_rt": "target_service:gnode:unified:production"
}
```

Bypasses discovery — use when you know the exact stream. gNode still enforces policy.

### Environment isolation

The source environment is preserved. A command from `my_service:gnode:unified:staging` is delivered to `target_service:gnode:unified:staging`. Commands never cross DTAP boundaries.

---

## Relay Policy

Policy rules control which services can talk to which. Stored in ValKey:

```
Key:   {topology_ns}:gnode:relay:policy  (HASH)
Field: source_site:target_site
Value: {"action":"allow|deny", "reason":"...", "commands":["*"]}
```

### Evaluation order (first match wins)

1. Exact pair: `service_a:service_b`
2. Source wildcard: `*:service_b` (anyone → service_b)
3. Target wildcard: `service_a:*` (service_a → anyone)
4. **Default: Allow**

### Managing policies

Set a policy:
```json
{"c": "relay_policy_set", "p": "{\"source\": \"untrusted_svc\", \"target\": \"core_db\", \"policy\": {\"action\": \"deny\", \"reason\": \"No direct DB access\", \"commands\": [\"*\"]}}"}
```

Allow specific commands only:
```json
{"c": "relay_policy_set", "p": "{\"source\": \"monitoring\", \"target\": \"core_db\", \"policy\": {\"action\": \"deny\", \"reason\": \"Read-only access\", \"commands\": [\"write\", \"delete\"]}}"}
```

This denies `write` and `delete` but allows all other commands from `monitoring` → `core_db`.

List all policies:
```json
{"c": "relay_policy_list"}
```

Remove a policy:
```json
{"c": "relay_policy_remove", "p": "{\"source\": \"untrusted_svc\", \"target\": \"core_db\"}"}
```

### Switching to fail-closed

The default policy is allow-all (fail-open). For a curated ecosystem where gNode explicitly controls who talks to whom, add a global deny rule and then whitelist specific pairs:

```bash
# Deny everything by default
relay_policy_set  *:*  deny  "Fail-closed: explicit allow required"  ["*"]

# Whitelist specific pairs
relay_policy_set  analytics:geodineum_comms      allow  "Analytics escalates alerts via COMMS"  ["*"]
relay_policy_set  geodineum_bak:geodineum_comms  allow  "BAK notifies on backup events"  ["*"]
relay_policy_set  *:analytics                    allow  "Any service can request analysis"  ["*"]
```

Exact-pair rules are checked before wildcards, so whitelisted pairs pass even with `*:*` deny.

---

## Format Translation

When services use different wire formats, gNode translates automatically during relay.

### Supported formats

| Format | Description | Detection | Field mapping |
|--------|-------------|-----------|---------------|
| `standard_json` | Full field names: `id`, `command`, `parameters` | Prefix `{` | Identity (canonical) |
| `resp3` | Compact: `i`, `c`, `p`, `ss`, `sn`, `ts` | Prefix `*`, `+`, `:`, `$` | `i`→`id`, `c`→`command`, `ts` with ms→s transform |
| `compact_json` | Bandwidth-optimized: `i`, `c`, `p`, `s`, `n`, `t` | Regex `^\s*\{\s*"i":` | `i`→`id`, `c`→`command`, `t` with s precision |

Custom formats can be added via JSON/YAML definition files.

### How translation works

1. **Detect** — gNode detects the source format natively via the base daemon's `FormatProcessor` (`relay/translator.rs::detect_format`). Returns format name + confidence score (0.0–1.0). Minimum confidence: 0.5.
2. **Look up** — gNode reads the target entity's `native_format` from its topology metadata.
3. **Skip if same** — If source and target formats match (common case), no work done.
4. **Convert** — gNode converts natively via the same `FormatProcessor` (`relay/translator.rs::convert_format`) with source/target format names and the params payload. Field mappings and transforms are applied bidirectionally.
5. **Apply** — Converted params replace the originals in the forwarded command.
6. **Fallback** — If translation fails, the original params are forwarded unchanged. A warning is logged. The relay is never blocked by a translation failure.

### Declaring format preference

Services declare their preferred format in topology metadata during registration:

```yaml
# In .geodineum/gnode_services.yaml
metadata:
  native_format: "standard_json"
  accepts_formats: "standard_json,compact_json,resp3"
  output_format: "standard_json"
```

If no `native_format` is declared, translation is skipped (NoOp).

### Field mapping and transforms

Each format defines a field mapping with optional transforms:

| Transform | Effect | Example |
|-----------|--------|---------|
| `string` | Cast to string | `123` → `"123"` |
| `number` | Cast to number | `"123"` → `123` |
| `boolean` | Cast to boolean | `"true"` → `true` |
| `timestamp_ms` | Milliseconds ↔ seconds | `1711900800000` → `1711900800` |
| `timestamp_s` | Seconds precision | `1711900800.123` → `1711900800` |
| `iso_date` | ISO 8601 conversion | `1711900800` → `"2024-03-31T..."` |
| `json` | Parse/stringify JSON | `"{\"a\":1}"` → `{"a":1}` |

Transforms are applied in the forward direction (source → target) and automatically reversed when converting back.

---

## Direct Channels (High-Throughput Alternative)

For services that need sustained high-throughput communication, direct channels bypass per-message relay processing. gNode provisions the channel, then steps out of the data path.

```json
{"c": "channel_open", "p": "{\"target_site\": \"service_b\", \"mode\": \"persistent\"}"}
```

Response:
```json
{
  "channel_id": "dc_abc123",
  "stream_key": "{topology_ns}:gnode:direct:dc_abc123",
  "mode": "persistent"
}
```

Both services get their own consumer group on the shared stream. Communication is direct — gNode does not process individual messages.

| Command | Parameters | Description |
|---------|-----------|-------------|
| `channel_open` | `target_site`, `mode` (persistent\|temporary), `ttl_seconds`, `metadata` | Provision a channel |
| `channel_close` | `channel_id` | Clean up channel (delete stream + metadata) |
| `channel_info` | `channel_id` | Get channel metadata and stream stats |
| `channel_list` | `site_filter`, `env_filter` | List active channels |

### When to use which

| Pattern | Use case | gNode in data path? | Format translation? | Policy enforced? |
|---------|----------|---------------------|---------------------|-----------------|
| **Relay** (`_rt`) | Infrequent cross-service commands, heterogeneous formats, policy-controlled access | Yes | Yes | Yes |
| **Direct channel** | High-throughput streaming between two services, homogeneous format | No (provision only) | No | At provision time only |
| **Broadcast** | One-to-many announcements, topology updates, service events | No | No | No (shared bus) |

---

## Telemetry & Diagnostics

Every relay operation is tracked. Metrics are aggregated per source:target pair and flushed to ValKey every 30 seconds.

### Viewing relay stats

```json
{"c": "topology_heatmap"}
```

or the alias:

```json
{"c": "relay_stats"}
```

Response:
```json
{
  "pairs": [
    {
      "source": "analytics",
      "target": "geodineum_comms",
      "count": 142,
      "ok": 140,
      "err": 2,
      "translated": 12,
      "avg_latency_ms": 3,
      "commands": {"send_notification": 130, "get_status": 12}
    }
  ],
  "total_relays": 142,
  "total_ok": 140,
  "total_err": 2,
  "total_translated": 12,
  "pair_count": 1
}
```

Fields:
- `count` — total relay operations for this pair
- `ok` / `err` — successful / failed relays
- `translated` — relays where format translation was applied
- `avg_latency_ms` — average relay processing time (resolve + policy + translate + XADD)
- `commands` — per-command breakdown

Storage key: `{topology_ns}:gnode:telemetry:relay` (HASH). Counters are additive across daemon restarts and worker threads.

---

## Response Routing

Responses are routed back to the source automatically via the `_rr` (relay_reply_to) field.

When gNode forwards a command, it sets `_rr` to the source's unified stream key. The target service processes the command normally. When the response is generated, gNode checks for `_rr` and sends the response to that stream instead of the local stream.

The source service reads the response from its own unified stream — no special handling required. The response includes the original `ri` (request ID) for correlation.

If the target never responds, the source will not receive a response. The RelayTracker evicts stale tracking entries after 30 seconds. Client-side timeouts should be implemented by the calling service.

---

## Security Model

| Layer | Mechanism |
|-------|-----------|
| **Stream isolation** | Each service can only XADD/XREADGROUP on its own `{service_id}:*` keyspace |
| **Relay gating** | gNode daemon (`~*` ACL) is the only entity that can write to another service's stream |
| **Policy enforcement** | Per-pair allow/deny rules checked on every relay, before forwarding |
| **DTAP isolation** | Environment preserved — staging commands never reach production streams |
| **No credential sharing** | Services never receive another service's ValKey credentials |
| **Audit trail** | Every relay logged (info level) + telemetry counters in ValKey |
| **Graceful degradation** | Translation failures forward original params; XADD failures return error to source |

---

## Quick Reference

### Command fields for relay

| Field | Description | Required |
|-------|-------------|----------|
| `_rt` | Relay target: entity ID, site ID, or explicit stream key | Yes (triggers relay) |
| `_rr` | Reply-to stream override (set automatically by gNode) | No (auto-set) |

### Relay policy commands

| Command | Parameters |
|---------|-----------|
| `relay_policy_set` | `{source, target, policy: {action, reason, commands}}` |
| `relay_policy_list` | `{}` |
| `relay_policy_remove` | `{source, target}` |

### Channel commands

| Command | Parameters |
|---------|-----------|
| `channel_open` | `{target_site, mode, ttl_seconds, metadata}` |
| `channel_close` | `{channel_id}` |
| `channel_info` | `{channel_id}` |
| `channel_list` | `{site_filter, env_filter}` |

### Diagnostics

| Command | Description |
|---------|-------------|
| `topology_heatmap` / `relay_stats` | Relay interaction matrix with format translation stats |

### Format commands

Format detection/conversion is a base capability: the relay path uses the native `FormatProcessor` directly, and the same engine backs the daemon commands `register_format`, `list_formats`, `detect_format`, `convert_format` (CMS extension handlers; see COMMAND_SCHEMA.md).

| Function | Description |
|----------|-------------|
| `GNODE_ENDPOINT_TRANSLATE` (Pro, gNode-BROKER Lua) | Translate between named endpoints with field mapping |
