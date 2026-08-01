// Service Introspection Command Handlers
//
// Handles: service_describe
// Provides detailed information about a registered service entity including
// capabilities, metadata, tier classification, edges, and health status.

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
    CommandResult, CommandHandlerFn, AsyncCommandHandlerFn, CommandDescriptor,
    TOTAL_DIMENSIONS, Lane,
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
        description: "Get detailed description of a registered service entity including capabilities, tier, edges, and health",
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
                "tier": {"type": "string", "enum": ["TOOL", "SERVICE", "PIPELINE", "INFRASTRUCTURE", "ORCHESTRATOR", "UNKNOWN"]},
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
// Tier classification
// =========================================================================

/// Map service_tier dimension value to tier name.
/// Tier values: TOOL=0.10, SERVICE=0.30, PIPELINE=0.50, INFRASTRUCTURE=0.70, ORCHESTRATOR=0.90
fn classify_tier(service_tier_value: f64) -> &'static str {
    if service_tier_value <= 0.15 {
        "TOOL"
    } else if service_tier_value <= 0.40 {
        "SERVICE"
    } else if service_tier_value <= 0.60 {
        "PIPELINE"
    } else if service_tier_value <= 0.80 {
        "INFRASTRUCTURE"
    } else if service_tier_value <= 1.0 {
        "ORCHESTRATOR"
    } else {
        "UNKNOWN"
    }
}

/// Build a reverse mapping from dimension index to canonical capability name.
/// Skips aliases (multiple names mapping to same index) — keeps the first seen.
fn build_index_to_name() -> HashMap<usize, String> {
    // Canonical names (non-alias) for each dimension
    let canonical: [(usize, &str); 23] = [
        (0, "protocol"),
        (1, "native_format"),
        (2, "api_version"),
        (3, "contract_stability"),
        (4, "clearance_required"),
        (5, "auth_method"),
        (6, "data_sensitivity"),
        (7, "service_scope"),
        (8, "domain_primary"),
        (9, "domain_secondary"),
        (10, "specialization"),
        (11, "throughput_tier"),
        (12, "latency_class"),
        (13, "reliability_tier"),
        (14, "pipeline_stage"),
        (15, "execution_priority"),
        (16, "current_load"),
        (17, "service_tier"),
        (18, "environment"),
        (19, "user_x"),
        (20, "user_y"),
        (21, "user_z"),
        (22, "registration_order"),
    ];
    canonical.iter().map(|&(i, n)| (i, n.to_string())).collect()
}

/// Enrich entity JSON from GNODE_TOPO_GET_ENTITY into a service_describe response.
fn build_describe_response(entity_id: &str, entity: &Value, _site_id: &str) -> Value {
    let index_to_name = build_index_to_name();

    // Extract point_display (pd) — array of f64 display values
    let pd = entity.get("pd").and_then(|v| v.as_array());

    // Build human-readable capabilities from pd
    let mut capabilities = json!({});
    if let Some(pd_arr) = pd {
        if let Some(cap_obj) = capabilities.as_object_mut() {
            for (idx, val) in pd_arr.iter().enumerate() {
                if idx >= TOTAL_DIMENSIONS { break; }
                if let Some(f) = val.as_f64() {
                    if f != 0.0 {
                        if let Some(name) = index_to_name.get(&idx) {
                            cap_obj.insert(name.clone(), json!(f));
                        }
                    }
                }
            }
        }
    }

    // Determine tier from dimension 17 (service_tier)
    let service_tier_val = pd.and_then(|arr| arr.get(17))
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    let tier = classify_tier(service_tier_val);

    // Extract registered_at (ra)
    let registered_at = entity.get("ra").and_then(|v| v.as_i64()).unwrap_or(0);

    // Extract metadata (m)
    let metadata = entity.get("m").cloned().unwrap_or(json!({}));

    // Extract original capabilities if present (c)
    let original_caps = entity.get("c");
    // If original caps exist, prefer them for the response
    let final_capabilities = if let Some(c) = original_caps {
        c.clone()
    } else {
        capabilities
    };

    // Coordinates
    let bucket_key = entity.get("bk").and_then(|v| v.as_str()).unwrap_or("");
    let z_score = entity.get("zs");
    let point_display = entity.get("pd").cloned().unwrap_or(json!([]));
    let point_raw = entity.get("pr").cloned().unwrap_or(json!([]));

    // Edges
    let outgoing = entity.get("out").cloned().unwrap_or(json!([]));
    let incoming = entity.get("in").cloned().unwrap_or(json!([]));

    // Health: check LoadMetricsManager
    let health = if tier != "TOOL" {
        if let Some(load_manager) = crate::daemon::GNodeDaemon::get_load_metrics_manager_ref() {
            if let Some(metrics) = load_manager.get(entity_id) {
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64;
                let status = if metrics.is_stale(now_ms) { "stale" } else { "active" };
                json!({
                    "status": status,
                    "load": metrics.load_factor,
                    "last_update": metrics.last_update,
                    "cpu": metrics.cpu_usage,
                    "memory": metrics.memory_usage,
                    "error_rate": metrics.error_rate
                })
            } else {
                json!({"status": "unknown"})
            }
        } else {
            json!({"status": "unknown"})
        }
    } else {
        // TOOLs don't have health metrics — they run at deploy-time only
        json!({"status": "n/a", "reason": "tool-tier entities have no runtime health"})
    };

    json!({
        "id": entity_id,
        "tier": tier,
        "registered_at": registered_at,
        "capabilities": final_capabilities,
        "metadata": metadata,
        "coordinates": {
            "display": point_display,
            "raw": point_raw,
            "bucket_key": bucket_key,
            "z_score": z_score
        },
        "edges": {
            "outgoing": outgoing,
            "incoming": incoming
        },
        "health": health
    })
}

// =========================================================================
// Sync handler
// =========================================================================

pub fn handle_service_describe(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    site_id: &str,
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

    let topology_key = match command.parameters.get("topology_key").and_then(|v| v.as_str()) {
        Some(k) if !k.is_empty() => k.to_string(),
        _ => GeometricTopology::get_services_topology_key(site_id),
    };

    if debug_mode {
        debug!("service_describe: entity={}, topology={}", entity_id, topology_key);
    }

    // FCALL GNODE_TOPO_GET_ENTITY to get entity with edges
    let result: redis::RedisResult<String> = redis::cmd("FCALL")
        .arg("GNODE_TOPO_GET_ENTITY")
        .arg(1)
        .arg(&topology_key)
        .arg(&entity_id)
        .query(conn);

    match result {
        Ok(json_str) => {
            match serde_json::from_str::<Value>(&json_str) {
                Ok(entity) => {
                    let response = build_describe_response(&entity_id, &entity, site_id);
                    CommandResult::success(response)
                },
                Err(e) => CommandResult::error(format!("Failed to parse entity data: {}", e)),
            }
        },
        Err(e) => {
            let err_str = format!("{}", e);
            if err_str.contains("Entity not found") {
                CommandResult::error(format!("Entity '{}' not found in topology '{}'", entity_id, topology_key))
            } else {
                CommandResult::error(format!("service_describe failed: {}", e))
            }
        }
    }
}

// =========================================================================
// Async handler
// =========================================================================

pub fn handle_service_describe_async<'a>(
    command: &'a Command,
    conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("[STATELESS] Handling async service_describe command: {}", command.id);
        }

        let entity_id = match command.parameters.get("entity_id")
            .or_else(|| command.parameters.get("id"))
            .or_else(|| command.parameters.get("service_id"))
            .and_then(|v| v.as_str())
        {
            Some(id) if !id.is_empty() => id.to_string(),
            _ => return CommandResult::error("Missing or empty entity_id parameter"),
        };

        let topology_key = match command.parameters.get("topology_key").and_then(|v| v.as_str()) {
            Some(k) if !k.is_empty() => k.to_string(),
            _ => GeometricTopology::get_services_topology_key(site_id),
        };

        if debug_mode {
            debug!("service_describe: entity={}, topology={}", entity_id, topology_key);
        }

        // FCALL GNODE_TOPO_GET_ENTITY to get entity with edges
        let result: redis::RedisResult<String> = redis::cmd("FCALL")
            .arg("GNODE_TOPO_GET_ENTITY")
            .arg(1)
            .arg(&topology_key)
            .arg(&entity_id)
            .query_async(conn)
            .await;

        match result {
            Ok(json_str) => {
                match serde_json::from_str::<Value>(&json_str) {
                    Ok(entity) => {
                        let response = build_describe_response(&entity_id, &entity, site_id);
                        CommandResult::success(response)
                    },
                    Err(e) => CommandResult::error(format!("Failed to parse entity data: {}", e)),
                }
            },
            Err(e) => {
                let err_str = format!("{}", e);
                if err_str.contains("Entity not found") {
                    CommandResult::error(format!("Entity '{}' not found in topology '{}'", entity_id, topology_key))
                } else {
                    CommandResult::error(format!("service_describe failed: {}", e))
                }
            }
        }
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
