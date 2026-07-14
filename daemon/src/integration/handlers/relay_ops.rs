// Relay Operations Command Handlers
//
// Handles: topology_heatmap, relay_stats, relay_policy_set, relay_policy_list, relay_policy_remove
// These commands provide relay observability and policy management.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use std::pin::Pin;
use std::future::Future;
use redis::Connection;
use redis::aio::MultiplexedConnection as AsyncConnection;
use log::debug;
use serde_json::json;
use crate::daemon::{Command, GNodeDaemon};
use crate::GeometricTopology;

use super::types::{
    CommandResult, CommandHandlerFn, AsyncCommandHandlerFn, CommandDescriptor, Lane,
};

/// Register all relay operations command handlers
pub fn register(
    handlers: &mut HashMap<String, CommandHandlerFn>,
    async_handlers: &mut HashMap<String, AsyncCommandHandlerFn>,
    descriptors: &mut Vec<CommandDescriptor>,
) {
    // Sync handlers
    handlers.insert("topology_heatmap".to_string(), handle_topology_heatmap as CommandHandlerFn);
    handlers.insert("TOPOLOGY_HEATMAP".to_string(), handle_topology_heatmap as CommandHandlerFn);
    handlers.insert("relay_stats".to_string(), handle_topology_heatmap as CommandHandlerFn);
    handlers.insert("relay_policy_set".to_string(), handle_relay_policy_set as CommandHandlerFn);
    handlers.insert("relay_policy_list".to_string(), handle_relay_policy_list as CommandHandlerFn);
    handlers.insert("relay_policy_remove".to_string(), handle_relay_policy_remove as CommandHandlerFn);

    // Async handlers
    async_handlers.insert("topology_heatmap".to_string(), handle_topology_heatmap_async as AsyncCommandHandlerFn);
    async_handlers.insert("TOPOLOGY_HEATMAP".to_string(), handle_topology_heatmap_async as AsyncCommandHandlerFn);
    async_handlers.insert("relay_stats".to_string(), handle_topology_heatmap_async as AsyncCommandHandlerFn);
    async_handlers.insert("relay_policy_set".to_string(), handle_relay_policy_set_async as AsyncCommandHandlerFn);
    async_handlers.insert("relay_policy_list".to_string(), handle_relay_policy_list_async as AsyncCommandHandlerFn);
    async_handlers.insert("relay_policy_remove".to_string(), handle_relay_policy_remove_async as AsyncCommandHandlerFn);

    // Descriptors
    descriptors.push(CommandDescriptor {
        name: "topology_heatmap",
        category: "relay",
        description: "Get service interaction matrix from relay telemetry data",
        params_schema: json!({
            "type": "object",
            "properties": {},
            "additionalProperties": false
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "pairs": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "source": {"type": "string"},
                            "target": {"type": "string"},
                            "count": {"type": "integer"},
                            "ok": {"type": "integer"},
                            "err": {"type": "integer"},
                            "avg_latency_ms": {"type": "integer"},
                            "commands": {"type": "object"}
                        }
                    }
                },
                "total_relays": {"type": "integer"},
                "total_ok": {"type": "integer"},
                "total_err": {"type": "integer"},
                "pair_count": {"type": "integer"}
            }
        }),
        example: r#"{"cmd":"topology_heatmap","params":{}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });

    descriptors.push(CommandDescriptor {
        name: "relay_policy_set",
        category: "relay",
        description: "Set a relay ACL policy rule (deny/allow between service pairs)",
        params_schema: json!({
            "type": "object",
            "properties": {
                "pattern": {"type": "string", "description": "Policy pattern: source:target, *:target, or source:*"},
                "action": {"type": "string", "enum": ["allow", "deny"]},
                "reason": {"type": "string", "description": "Human-readable reason"},
                "commands": {"type": "array", "items": {"type": "string"}, "description": "Commands this applies to, [\"*\"] for all"}
            },
            "required": ["pattern", "action"]
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "set": {"type": "boolean"}
            }
        }),
        example: r#"{"cmd":"relay_policy_set","params":{"pattern":"site_a:site_b","action":"deny","reason":"Maintenance window"}}"#,
        async_capable: true,
        // Ordered: changes routing policy. Subsequent relay decisions must
        // observe the new policy — Fast lane could let a relay attempt
        // through while the policy write is still pending.
        lane: Lane::Ordered,
    });

    descriptors.push(CommandDescriptor {
        name: "relay_policy_list",
        category: "relay",
        description: "List all relay ACL policy rules",
        params_schema: json!({"type": "object", "properties": {}}),
        returns_schema: json!({"type": "object"}),
        example: r#"{"cmd":"relay_policy_list","params":{}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });

    descriptors.push(CommandDescriptor {
        name: "relay_policy_remove",
        category: "relay",
        description: "Remove a relay ACL policy rule",
        params_schema: json!({
            "type": "object",
            "properties": {
                "pattern": {"type": "string", "description": "Policy pattern to remove"}
            },
            "required": ["pattern"]
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "removed": {"type": "boolean"}
            }
        }),
        example: r#"{"cmd":"relay_policy_remove","params":{"pattern":"site_a:site_b"}}"#,
        async_capable: true,
        // Ordered: same rationale as relay_policy_set — pending relay
        // decisions need to observe the removal.
        lane: Lane::Ordered,
    });
}

// =========================================================================
// topology_heatmap / relay_stats
// =========================================================================

pub fn handle_topology_heatmap(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    _site_id: &str,
    debug_mode: bool,
) -> CommandResult {
    if debug_mode {
        debug!("Handling topology_heatmap command: {}", command.id);
    }

    let ns = GNodeDaemon::get_topology_namespace();

    match crate::integration::relay::get_relay_stats(conn, ns, debug_mode) {
        Ok(stats) => CommandResult::success(stats),
        Err(e) => CommandResult::error(format!("topology_heatmap failed: {}", e)),
    }
}

pub fn handle_topology_heatmap_async<'a>(
    command: &'a Command,
    _conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    _site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling async topology_heatmap command: {}", command.id);
        }

        // HGETALL requires sync connection — get one from the pool
        match crate::integration::connection_manager::get_connection() {
            Ok(mut conn) => {
                let ns = GNodeDaemon::get_topology_namespace();
                match crate::integration::relay::get_relay_stats(&mut conn, ns, debug_mode) {
                    Ok(stats) => CommandResult::success(stats),
                    Err(e) => CommandResult::error(format!("topology_heatmap failed: {}", e)),
                }
            }
            Err(e) => CommandResult::error(format!("Failed to get connection: {}", e)),
        }
    })
}

// =========================================================================
// relay_policy_set
// =========================================================================

pub fn handle_relay_policy_set(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    _site_id: &str,
    debug_mode: bool,
) -> CommandResult {
    if debug_mode {
        debug!("Handling relay_policy_set command: {}", command.id);
    }

    let pattern = match command.parameters.get("pattern").and_then(|v| v.as_str()) {
        Some(p) if !p.is_empty() => p,
        _ => return CommandResult::error("Missing or empty 'pattern' parameter"),
    };

    let action = match command.parameters.get("action").and_then(|v| v.as_str()) {
        Some(a) if a == "allow" || a == "deny" => a,
        _ => return CommandResult::error("'action' must be 'allow' or 'deny'"),
    };

    let reason = command.parameters.get("reason")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    let commands: Vec<&str> = command.parameters.get("commands")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect())
        .unwrap_or_else(|| vec!["*"]);

    let ns = GNodeDaemon::get_topology_namespace();

    match crate::integration::relay::policy::set_relay_policy(conn, ns, pattern, action, reason, &commands) {
        Ok(()) => CommandResult::success(json!({"set": true, "pattern": pattern, "action": action})),
        Err(e) => CommandResult::error(format!("relay_policy_set failed: {}", e)),
    }
}

pub fn handle_relay_policy_set_async<'a>(
    command: &'a Command,
    _conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    _site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling async relay_policy_set: {}", command.id);
        }

        match crate::integration::connection_manager::get_connection() {
            Ok(mut conn) => {
                let ns = GNodeDaemon::get_topology_namespace();
                let pattern = match command.parameters.get("pattern").and_then(|v| v.as_str()) {
                    Some(p) if !p.is_empty() => p,
                    _ => return CommandResult::error("Missing or empty 'pattern' parameter"),
                };
                let action = match command.parameters.get("action").and_then(|v| v.as_str()) {
                    Some(a) if a == "allow" || a == "deny" => a,
                    _ => return CommandResult::error("'action' must be 'allow' or 'deny'"),
                };
                let reason = command.parameters.get("reason")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let commands: Vec<&str> = command.parameters.get("commands")
                    .and_then(|v| v.as_array())
                    .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect())
                    .unwrap_or_else(|| vec!["*"]);

                match crate::integration::relay::policy::set_relay_policy(&mut conn, ns, pattern, action, reason, &commands) {
                    Ok(()) => CommandResult::success(json!({"set": true, "pattern": pattern, "action": action})),
                    Err(e) => CommandResult::error(format!("relay_policy_set failed: {}", e)),
                }
            }
            Err(e) => CommandResult::error(format!("Failed to get connection: {}", e)),
        }
    })
}

// =========================================================================
// relay_policy_list
// =========================================================================

pub fn handle_relay_policy_list(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    _site_id: &str,
    debug_mode: bool,
) -> CommandResult {
    if debug_mode {
        debug!("Handling relay_policy_list: {}", command.id);
    }

    let ns = GNodeDaemon::get_topology_namespace();

    match crate::integration::relay::policy::list_relay_policies(conn, ns) {
        Ok(policies) => CommandResult::success(json!({
            "policies": policies,
        })),
        Err(e) => CommandResult::error(format!("relay_policy_list failed: {}", e)),
    }
}

pub fn handle_relay_policy_list_async<'a>(
    command: &'a Command,
    _conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    _site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling async relay_policy_list: {}", command.id);
        }

        match crate::integration::connection_manager::get_connection() {
            Ok(mut conn) => {
                let ns = GNodeDaemon::get_topology_namespace();
                match crate::integration::relay::policy::list_relay_policies(&mut conn, ns) {
                    Ok(policies) => CommandResult::success(json!({"policies": policies})),
                    Err(e) => CommandResult::error(format!("relay_policy_list failed: {}", e)),
                }
            }
            Err(e) => CommandResult::error(format!("Failed to get connection: {}", e)),
        }
    })
}

// =========================================================================
// relay_policy_remove
// =========================================================================

pub fn handle_relay_policy_remove(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    _site_id: &str,
    debug_mode: bool,
) -> CommandResult {
    if debug_mode {
        debug!("Handling relay_policy_remove: {}", command.id);
    }

    let pattern = match command.parameters.get("pattern").and_then(|v| v.as_str()) {
        Some(p) if !p.is_empty() => p,
        _ => return CommandResult::error("Missing or empty 'pattern' parameter"),
    };

    let ns = GNodeDaemon::get_topology_namespace();

    match crate::integration::relay::policy::remove_relay_policy(conn, ns, pattern) {
        Ok(removed) => CommandResult::success(json!({"removed": removed, "pattern": pattern})),
        Err(e) => CommandResult::error(format!("relay_policy_remove failed: {}", e)),
    }
}

pub fn handle_relay_policy_remove_async<'a>(
    command: &'a Command,
    _conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    _site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling async relay_policy_remove: {}", command.id);
        }

        match crate::integration::connection_manager::get_connection() {
            Ok(mut conn) => {
                let ns = GNodeDaemon::get_topology_namespace();
                let pattern = match command.parameters.get("pattern").and_then(|v| v.as_str()) {
                    Some(p) if !p.is_empty() => p,
                    _ => return CommandResult::error("Missing or empty 'pattern' parameter"),
                };
                match crate::integration::relay::policy::remove_relay_policy(&mut conn, ns, pattern) {
                    Ok(removed) => CommandResult::success(json!({"removed": removed, "pattern": pattern})),
                    Err(e) => CommandResult::error(format!("relay_policy_remove failed: {}", e)),
                }
            }
            Err(e) => CommandResult::error(format!("Failed to get connection: {}", e)),
        }
    })
}
