// Service Introspection Command Handlers
//
// Handles: service_describe
// Provides detailed information about a registered service entity including
// colour words, metadata, shape, derived slots, edges.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use std::pin::Pin;
use std::future::Future;
use redis::Connection;
use redis::aio::MultiplexedConnection as AsyncConnection;
use log::debug;
use serde_json::{Value, json};
use crate::daemon::Command;
use crate::GeometricTopology;

use super::types::{
    CommandResult, CommandHandlerFn, AsyncCommandHandlerFn, CommandDescriptor, Lane,
};

/// Register all introspection command handlers
pub fn register(
    handlers: &mut HashMap<String, CommandHandlerFn>,
    async_handlers: &mut HashMap<String, AsyncCommandHandlerFn>,
    descriptors: &mut Vec<CommandDescriptor>,
) {
    // Sync handlers
    handlers.insert("service_describe".to_string(), handle_service_describe as CommandHandlerFn);
    handlers.insert("SERVICE_DESCRIBE".to_string(), handle_service_describe as CommandHandlerFn);
    handlers.insert("constellation_status".to_string(), handle_constellation_status as CommandHandlerFn);
    handlers.insert("CONSTELLATION_STATUS".to_string(), handle_constellation_status as CommandHandlerFn);

    // Async handlers
    async_handlers.insert("service_describe".to_string(), handle_service_describe_async as AsyncCommandHandlerFn);
    async_handlers.insert("SERVICE_DESCRIBE".to_string(), handle_service_describe_async as AsyncCommandHandlerFn);

    descriptors.push(CommandDescriptor {
        name: "constellation_status",
        category: "system",
        description: "Constellation at a glance: nodes with liveness + resources (from the daemon heartbeat), which components run where, registered service intents, tool count. The agent-facing twin of `geodineum status` (same JSON shape).",
        params_schema: json!({"type": "object", "properties": {}}),
        returns_schema: json!({"type": "object", "properties": {
            "constellation": {"type": "string"},
            "ts": {"type": "number"},
            "nodes": {"type": "array"},
            "services": {"type": "array"},
            "tools": {"type": "number"},
            "components_live": {"type": "number"}
        }}),
        example: r#"{"cmd":"constellation_status","params":{}}"#,
        async_capable: false,
        lane: Lane::Concurrent,
    });

    // Command descriptor
    descriptors.push(CommandDescriptor {
        name: "service_describe",
        category: "service",
        description: "The stored record of an entity on the one topology: shape, environment, colour words, metadata, derived load / health / lifecycle, point and edges",
        params_schema: json!({
            "type": "object",
            "properties": {
                "entity_id": {"type": "string", "description": "Service entity identifier"},
                "topology_key": {"type": "string", "description": "Optional topology key (defaults to site's services topology)"}
            },
            "required": ["entity_id"]
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "id": {"type": "string"},
                "shape": {"type": "string", "description": "1 | 2 | 3.1 | 3.2 | 3.3 | 4.1 | 4.2"},
                "shape_name": {"type": "string"},
                "environment": {"type": "string"},
                "derived": {"type": "object", "properties": {"load": {"type": "number"}, "health": {"type": "string"}, "lifecycle": {"type": "string"}}},
                "registered_at": {"type": "number"},
                "capabilities": {"type": "object"},
                "metadata": {"type": "object"},
                "coordinates": {"type": "object"},
                "edges": {"type": "object"},
                "health": {"type": "object"}
            }
        }),
        example: r#"{"cmd":"service_describe","params":{"entity_id":"SecurityManager"}}"#,
        async_capable: true,
        lane: Lane::Concurrent,
    });
}

// =========================================================================
// The record → a description
// =========================================================================

const HEALTH_WORDS: [(f64, &str); 4] = [(0.0, "unknown"), (0.25, "dead"), (0.5, "degraded"), (1.0, "healthy")];
const LIFECYCLE_WORDS: [(f64, &str); 5] = [(0.0, "registering"), (0.25, "active"), (0.5, "draining"), (0.75, "stopped"), (1.0, "failed")];
const SHAPE_NAMES: [(&str, &str); 7] = [("1", "node"), ("2", "component"), ("3.1", "site"), ("3.2", "app"), ("3.3", "daemon"), ("4.1", "component@node"), ("4.2", "service@node")];

fn word_of(table: &[(f64, &str)], v: Option<f64>) -> Value {
    match v {
        Some(x) => table.iter().find(|(c, _)| (c - x).abs() < 1e-9).map(|(_, w)| json!(w)).unwrap_or(json!(x)),
        None => Value::Null,
    }
}

/// The stored record of the one topology (`sh`, `env`, `c`, `m`, `pd`, `pr`, `q`, placement
/// facts) → the `service_describe` answer: shape and its name, environment, colour, metadata,
/// derived load / health / lifecycle, the point, the edges.
fn build_describe_response(entity_id: &str, entity: &Value, edges_out: Value, edges_in: Value) -> Value {
    let sh = entity.get("sh").and_then(|v| v.as_str()).unwrap_or("");
    let pd = entity.get("pd").and_then(|v| v.as_array());
    let slot = |i: usize| pd.and_then(|a| a.get(i)).and_then(|v| v.as_f64());
    let shape_name = SHAPE_NAMES.iter().find(|(k, _)| *k == sh).map(|(_, n)| *n).unwrap_or(sh);
    let mut out = json!({
        "id": entity_id,
        "shape": sh,
        "shape_name": shape_name,
        "environment": entity.get("env").cloned().unwrap_or(Value::Null),
        "registered_at": entity.get("ra").cloned().unwrap_or(Value::Null),
        "registered_order": entity.get("ro").cloned().unwrap_or(Value::Null),
        "capabilities": entity.get("c").cloned().unwrap_or(json!({})),
        "ordered": entity.get("q").cloned().unwrap_or(json!({})),
        "metadata": entity.get("m").cloned().unwrap_or(json!({})),
        "coordinates": {
            "display": entity.get("pd").cloned().unwrap_or(json!([])),
            "raw": entity.get("pr").cloned().unwrap_or(json!([]))
        },
        "derived": {
            "load": slot(16),
            "health": word_of(&HEALTH_WORDS, slot(17)),
            "lifecycle": word_of(&LIFECYCLE_WORDS, slot(18))
        },
        "edges": { "outgoing": edges_out, "incoming": edges_in }
    });
    for (k, name) in [("of", "of"), ("on", "on"), ("w", "capacity_weight"), ("hb", "heartbeat"), ("hbe", "heartbeat_env")] {
        if let Some(v) = entity.get(k) { out[name] = v.clone(); }
    }
    out
}

fn describe_from(entity_id: &str, raw: Option<String>, out: Vec<String>, inc: Vec<String>, tk: &str) -> CommandResult {
    match raw {
        None => CommandResult::error(format!("Entity '{}' not found in {}", entity_id, tk)),
        Some(json_str) => match serde_json::from_str::<Value>(&json_str) {
            Ok(entity) => CommandResult::success(build_describe_response(entity_id, &entity, json!(out), json!(inc))),
            Err(e) => CommandResult::error(format!("Failed to parse entity data: {}", e)),
        },
    }
}

// =========================================================================
// Sync handler
// =========================================================================

pub fn handle_service_describe(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    _site_id: &str,
    debug_mode: bool,
) -> CommandResult {
    if debug_mode {
        debug!("Handling service_describe command: {}", command.id);
    }
    let entity_id = match command.parameters.get("entity_id")
        .or_else(|| command.parameters.get("id"))
        .or_else(|| command.parameters.get("service_id"))
        .and_then(|v| v.as_str())
    {
        Some(id) if !id.is_empty() => id.to_string(),
        _ => return CommandResult::error("Missing or empty entity_id parameter"),
    };
    let c = crate::constellation::for_entity(conn, &entity_id);
    let tk = c.key();
    let raw: Option<String> = redis::cmd("HGET").arg(format!("{}:entities", tk)).arg(&entity_id).query(conn).unwrap_or(None);
    let out: Vec<String> = redis::cmd("SMEMBERS").arg(format!("{}:out:{}", tk, entity_id)).query(conn).unwrap_or_default();
    let inc: Vec<String> = redis::cmd("SMEMBERS").arg(format!("{}:in:{}", tk, entity_id)).query(conn).unwrap_or_default();
    describe_from(&entity_id, raw, out, inc, &tk)
}

// =========================================================================
// Async handler
// =========================================================================

pub fn handle_service_describe_async<'a>(
    command: &'a Command,
    conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    _site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling async service_describe command: {}", command.id);
        }
        let entity_id = match command.parameters.get("entity_id")
            .or_else(|| command.parameters.get("id"))
            .or_else(|| command.parameters.get("service_id"))
            .and_then(|v| v.as_str())
        {
            Some(id) if !id.is_empty() => id.to_string(),
            _ => return CommandResult::error("Missing or empty entity_id parameter"),
        };
        let c = crate::constellation::for_entity_async(conn, &entity_id).await;
        let tk = c.key();
        let raw: Option<String> = redis::cmd("HGET").arg(format!("{}:entities", tk)).arg(&entity_id).query_async(conn).await.unwrap_or(None);
        let out: Vec<String> = redis::cmd("SMEMBERS").arg(format!("{}:out:{}", tk, entity_id)).query_async(conn).await.unwrap_or_default();
        let inc: Vec<String> = redis::cmd("SMEMBERS").arg(format!("{}:in:{}", tk, entity_id)).query_async(conn).await.unwrap_or_default();
        describe_from(&entity_id, raw, out, inc, &tk)
    })
}


/// Constellation at a glance: the agent-facing twin of `geodineum status`.
///
/// An agent cannot sudo and must not hold credentials for every key it would
/// need; it CAN send one command to its own unified stream. The daemon reads
/// under its own authority and answers with the same JSON shape the CLI
/// renders, so an operator's table and an agent's function-call result never
/// disagree about the estate.
pub fn handle_constellation_status(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    _site_id: &str,
    debug_mode: bool,
) -> CommandResult {
    if debug_mode {
        debug!("Handling constellation_status command: {}", command.id);
    }
    let ns = std::env::var("GNODE_TOPOLOGY_NAMESPACE").unwrap_or_else(|_| "geodineum".to_string());
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);

    let ent_ids: Vec<String> = redis::cmd("HKEYS")
        .arg(format!("{{{}}}:gnode:constellation:entities", ns))
        .query(conn).unwrap_or_default();

    // Heartbeat family via SCAN (cursor-iterated; never KEYS).
    let mut hb_keys: Vec<String> = Vec::new();
    let mut cursor: u64 = 0;
    loop {
        let (next, batch): (u64, Vec<String>) = match redis::cmd("SCAN")
            .arg(cursor).arg("MATCH").arg(format!("{{{}}}:gnode:heartbeat:*", ns))
            .arg("COUNT").arg(200).query(conn)
        {
            Ok(r) => r,
            Err(e) => return CommandResult::error(format!("heartbeat scan failed: {}", e)),
        };
        hb_keys.extend(batch);
        if next == 0 { break; }
        cursor = next;
    }

    #[derive(Default)]
    struct Node {
        hb_age_s: Option<u64>, la1: Option<f64>, cores: Option<u64>,
        mem_used_mb: Option<u64>, mem_total_mb: Option<u64>, runs: Vec<String>,
    }
    let mut nodes: std::collections::BTreeMap<String, Node> = ent_ids.into_iter()
        .map(|n| (n, Node::default())).collect();
    let mut beats: Vec<(String, String, Option<u64>)> = Vec::new();
    let mut live = 0u64;

    for k in &hb_keys {
        let seg: Vec<&str> = k.split(':').collect();
        if seg.len() < 6 { continue; } // legacy flat key
        let (comp, node) = (seg[4].to_string(), seg[5..].join(":"));
        let val: Option<String> = redis::cmd("GET").arg(k).query(conn).ok();
        let d: Value = val.and_then(|v| serde_json::from_str(&v).ok()).unwrap_or(json!({}));
        let age = d.get("ts").and_then(Value::as_u64).map(|ts| now.saturating_sub(ts));
        if age.map_or(false, |a| a <= 120) { live += 1; }
        let n = nodes.entry(node.clone()).or_default();
        if !n.runs.contains(&comp) { n.runs.push(comp.clone()); }
        if let Some(a) = age {
            if n.hb_age_s.map_or(true, |cur| a < cur) { n.hb_age_s = Some(a); }
        }
        if comp == "gnode-daemon" {
            n.la1 = d.get("la1").and_then(Value::as_f64);
            n.cores = d.get("cores").and_then(Value::as_u64);
            n.mem_used_mb = d.get("mem_used_mb").and_then(Value::as_u64);
            n.mem_total_mb = d.get("mem_total_mb").and_then(Value::as_u64);
        }
        beats.push((comp, node, age));
    }

    let intents: std::collections::HashMap<String, String> = redis::cmd("HGETALL")
        .arg(format!("{{{}}}:gnode:registrations", ns))
        .query(conn).unwrap_or_default();
    let mut services: Vec<Value> = Vec::new();
    let mut sites: Vec<&String> = intents.keys().collect();
    sites.sort();
    for site in sites {
        let it: Value = serde_json::from_str(&intents[site]).unwrap_or(json!({}));
        let entity: bool = redis::cmd("HEXISTS")
            .arg(format!("{{{}}}:gnode:services:entities", site)).arg(site)
            .query(conn).unwrap_or(false);
        let best = beats.iter()
            .filter(|(c, _, a)| c == site && a.is_some())
            .min_by_key(|(_, _, a)| a.unwrap());
        services.push(json!({
            "service": site,
            "profile": it.get("profile"),
            "env": it.get("environment"),
            "entity": entity,
            "hb_age_s": best.and_then(|(_, _, a)| *a),
            "hb_node": best.map(|(_, n, _)| n.clone()),
        }));
    }

    let tools: u64 = redis::cmd("HLEN")
        .arg("{ecosystem}:gnode:services:entities").query(conn).unwrap_or(0);

    CommandResult::success(json!({
        "constellation": ns,
        "ts": now,
        "nodes": nodes.into_iter().map(|(name, n)| json!({
            "node": name, "hb_age_s": n.hb_age_s, "la1": n.la1, "cores": n.cores,
            "mem_used_mb": n.mem_used_mb, "mem_total_mb": n.mem_total_mb, "runs": n.runs,
        })).collect::<Vec<_>>(),
        "services": services,
        "tools": tools,
        "components_live": live,
    }))
}
