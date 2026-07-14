// Direct Channel Command Handlers
//
// Handles: channel_open, channel_close, channel_info, channel_list
// These commands provision and manage direct inter-service communication channels.
// gNode creates the stream + consumer groups, then clients talk directly.
//
// Two modes:
//   temporary  — TTL-based, auto-expires
//   persistent — no TTL, explicit close only

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

use super::types::{CommandResult, CommandDescriptor, CommandHandlerFn, AsyncCommandHandlerFn, Lane};

/// Register all direct channel command handlers
pub fn register(
    handlers: &mut HashMap<String, CommandHandlerFn>,
    async_handlers: &mut HashMap<String, AsyncCommandHandlerFn>,
    descriptors: &mut Vec<CommandDescriptor>,
) {
    // Sync handlers
    handlers.insert("channel_open".to_string(), handle_channel_open as CommandHandlerFn);
    handlers.insert("direct_provision".to_string(), handle_channel_open as CommandHandlerFn);
    handlers.insert("channel_close".to_string(), handle_channel_close as CommandHandlerFn);
    handlers.insert("direct_close".to_string(), handle_channel_close as CommandHandlerFn);
    handlers.insert("channel_info".to_string(), handle_channel_info as CommandHandlerFn);
    handlers.insert("direct_info".to_string(), handle_channel_info as CommandHandlerFn);
    handlers.insert("channel_list".to_string(), handle_channel_list as CommandHandlerFn);
    handlers.insert("direct_list".to_string(), handle_channel_list as CommandHandlerFn);

    // Async handlers
    async_handlers.insert("channel_open".to_string(), handle_channel_open_async as AsyncCommandHandlerFn);
    async_handlers.insert("direct_provision".to_string(), handle_channel_open_async as AsyncCommandHandlerFn);
    async_handlers.insert("channel_close".to_string(), handle_channel_close_async as AsyncCommandHandlerFn);
    async_handlers.insert("direct_close".to_string(), handle_channel_close_async as AsyncCommandHandlerFn);
    async_handlers.insert("channel_info".to_string(), handle_channel_info_async as AsyncCommandHandlerFn);
    async_handlers.insert("direct_info".to_string(), handle_channel_info_async as AsyncCommandHandlerFn);
    async_handlers.insert("channel_list".to_string(), handle_channel_list_async as AsyncCommandHandlerFn);
    async_handlers.insert("direct_list".to_string(), handle_channel_list_async as AsyncCommandHandlerFn);

    // Descriptors
    descriptors.push(CommandDescriptor {
        name: "channel_open",
        category: "direct_channel",
        description: "Provision a direct inter-service channel (temporary or persistent)",
        params_schema: json!({
            "type": "object",
            "properties": {
                "target_site": {"type": "string", "description": "Target site ID"},
                "mode": {"type": "string", "enum": ["temporary", "persistent"], "default": "temporary"},
                "ttl_seconds": {"type": "integer", "description": "TTL for temporary channels (default 300)"},
                "max_idle_seconds": {"type": "integer", "description": "Inactivity timeout (default 3600)"},
                "environment": {"type": "string", "enum": ["testing", "staging", "acceptance", "production"], "default": "production", "description": "DTAP environment scope"},
                "metadata": {"type": "object", "description": "Arbitrary channel metadata"}
            },
            "required": ["target_site"]
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "channel_id": {"type": "string"},
                "stream_key": {"type": "string"},
                "mode": {"type": "string"},
                "environment": {"type": "string"},
                "source_site": {"type": "string"},
                "target_site": {"type": "string"},
                "consumer_groups": {"type": "array"},
                "created_at": {"type": "integer"},
                "expires_at": {"type": ["integer", "null"]}
            }
        }),
        example: r#"{"cmd":"channel_open","params":{"target_site":"site_b","mode":"persistent"}}"#,
        async_capable: true,
        // Ordered: creates a channel resource (stream + consumer groups +
        // metadata key) that subsequent inter-service ops on this client
        // depend on. A Fast-lane channel_open could let a follow-up send
        // start before the stream exists.
        lane: Lane::Ordered,
    });

    descriptors.push(CommandDescriptor {
        name: "channel_close",
        category: "direct_channel",
        description: "Close and clean up a direct channel",
        params_schema: json!({
            "type": "object",
            "properties": {
                "channel_id": {"type": "string", "description": "Channel ID to close"}
            },
            "required": ["channel_id"]
        }),
        returns_schema: json!({"type": "object", "properties": {"ok": {"type": "boolean"}, "channel_id": {"type": "string"}}}),
        example: r#"{"cmd":"channel_close","params":{"channel_id":"ch_a1b2c3d4"}}"#,
        async_capable: true,
        // Ordered: destructive cleanup. Pending sends on this channel
        // should observe the closed state; Fast lane could let a send
        // arrive against a half-deleted channel.
        lane: Lane::Ordered,
    });

    descriptors.push(CommandDescriptor {
        name: "channel_info",
        category: "direct_channel",
        description: "Get direct channel metadata and stream stats",
        params_schema: json!({
            "type": "object",
            "properties": {
                "channel_id": {"type": "string", "description": "Channel ID to inspect"}
            },
            "required": ["channel_id"]
        }),
        returns_schema: json!({"type": "object"}),
        example: r#"{"cmd":"channel_info","params":{"channel_id":"ch_a1b2c3d4"}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });

    descriptors.push(CommandDescriptor {
        name: "channel_list",
        category: "direct_channel",
        description: "List direct channels, optionally filtered by site and/or environment",
        params_schema: json!({
            "type": "object",
            "properties": {
                "site_id": {"type": "string", "description": "Filter by participant site (optional)"},
                "environment": {"type": "string", "enum": ["testing", "staging", "acceptance", "production"], "description": "Filter by DTAP environment (optional)"}
            }
        }),
        returns_schema: json!({"type": "object", "properties": {"channels": {"type": "array"}, "count": {"type": "integer"}}}),
        example: r#"{"cmd":"channel_list","params":{"site_id":"my_app"}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });
}


// ============================================================================
// Sync handlers
// ============================================================================

pub fn handle_channel_open(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    site_id: &str,
    debug_mode: bool,
) -> CommandResult {
    if debug_mode {
        debug!("Handling channel_open command: {}", command.id);
    }

    let ns = GNodeDaemon::get_topology_namespace();

    // Extract parameters
    let target_site = command.parameters.get("target_site")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    if target_site.is_empty() {
        return CommandResult::error("Missing required parameter: target_site");
    }

    // Source site is the site that sent the command
    let source_site = if !command.site_id.is_empty() {
        command.site_id.as_str()
    } else {
        site_id
    };

    // Validate not opening channel to self
    if source_site == target_site {
        return CommandResult::error("Cannot open a direct channel to the same site");
    }

    let mode = command.parameters.get("mode")
        .and_then(|v| v.as_str())
        .unwrap_or("temporary");

    // Validate mode
    if mode != "temporary" && mode != "persistent" {
        return CommandResult::error("Invalid mode: must be 'temporary' or 'persistent'");
    }

    let ttl_seconds = command.parameters.get("ttl_seconds")
        .and_then(|v| v.as_u64())
        .unwrap_or(300);

    // Cap TTL at 24 hours
    let ttl_seconds = ttl_seconds.min(86400);

    let metadata = command.parameters.get("metadata")
        .cloned()
        .unwrap_or_else(|| json!({}));

    // DTAP environment — caller specifies or defaults to "production"
    let environment = command.parameters.get("environment")
        .and_then(|v| v.as_str())
        .unwrap_or("production");

    // Validate environment
    match environment {
        "testing" | "staging" | "acceptance" | "production" => {},
        _ => return CommandResult::error(
            "Invalid environment: must be 'testing', 'staging', 'acceptance', or 'production'"
        ),
    }

    match crate::integration::direct::provision_channel(
        conn, ns, source_site, target_site, mode, ttl_seconds, &metadata, environment, debug_mode,
    ) {
        Ok(result) => CommandResult::success(result),
        Err(e) => CommandResult::error(format!("Failed to provision channel: {}", e)),
    }
}

pub fn handle_channel_close(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    _site_id: &str,
    debug_mode: bool,
) -> CommandResult {
    if debug_mode {
        debug!("Handling channel_close command: {}", command.id);
    }

    let ns = GNodeDaemon::get_topology_namespace();

    let channel_id = command.parameters.get("channel_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    if channel_id.is_empty() {
        return CommandResult::error("Missing required parameter: channel_id");
    }

    // Verify caller is a participant before closing
    let source_site = if !command.site_id.is_empty() {
        command.site_id.as_str()
    } else {
        ""
    };

    // Get channel info to verify participant
    if !source_site.is_empty() {
        match crate::integration::direct::get_channel_info(conn, ns, channel_id, false) {
            Ok(info) => {
                let ch_source = info.get("source_site").and_then(|v| v.as_str()).unwrap_or("");
                let ch_target = info.get("target_site").and_then(|v| v.as_str()).unwrap_or("");
                if source_site != ch_source && source_site != ch_target {
                    return CommandResult::error("Only channel participants can close the channel");
                }
            },
            Err(_) => {
                // Channel might not exist — close will handle gracefully
            }
        }
    }

    match crate::integration::direct::close_channel(conn, ns, channel_id, debug_mode) {
        Ok(result) => CommandResult::success(result),
        Err(e) => CommandResult::error(format!("Failed to close channel: {}", e)),
    }
}

pub fn handle_channel_info(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    _site_id: &str,
    debug_mode: bool,
) -> CommandResult {
    if debug_mode {
        debug!("Handling channel_info command: {}", command.id);
    }

    let ns = GNodeDaemon::get_topology_namespace();

    let channel_id = command.parameters.get("channel_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    if channel_id.is_empty() {
        return CommandResult::error("Missing required parameter: channel_id");
    }

    match crate::integration::direct::get_channel_info(conn, ns, channel_id, debug_mode) {
        Ok(result) => CommandResult::success(result),
        Err(e) => CommandResult::error(format!("Failed to get channel info: {}", e)),
    }
}

pub fn handle_channel_list(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    _site_id: &str,
    debug_mode: bool,
) -> CommandResult {
    if debug_mode {
        debug!("Handling channel_list command: {}", command.id);
    }

    let ns = GNodeDaemon::get_topology_namespace();

    let site_filter = command.parameters.get("site_id")
        .and_then(|v| v.as_str());

    let env_filter = command.parameters.get("environment")
        .and_then(|v| v.as_str());

    match crate::integration::direct::list_channels(conn, ns, site_filter, env_filter, debug_mode) {
        Ok(result) => CommandResult::success(result),
        Err(e) => CommandResult::error(format!("Failed to list channels: {}", e)),
    }
}


// ============================================================================
// Async handlers (delegate to sync — FCALL is synchronous in ValKey)
// ============================================================================

pub fn handle_channel_open_async<'a>(
    command: &'a Command,
    _conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        // Direct channel provisioning uses FCALL which requires sync connection.
        // Get a sync connection from the pool for the FCALL call.
        match crate::integration::connection_manager::get_connection() {
            Ok(mut sync_conn) => {
                handle_channel_open(command, &mut sync_conn, &GNodeDaemon::get_topology_ref(), site_id, debug_mode)
            },
            Err(e) => CommandResult::error(format!("Failed to get connection: {}", e)),
        }
    })
}

pub fn handle_channel_close_async<'a>(
    command: &'a Command,
    _conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        match crate::integration::connection_manager::get_connection() {
            Ok(mut sync_conn) => {
                handle_channel_close(command, &mut sync_conn, &GNodeDaemon::get_topology_ref(), site_id, debug_mode)
            },
            Err(e) => CommandResult::error(format!("Failed to get connection: {}", e)),
        }
    })
}

pub fn handle_channel_info_async<'a>(
    command: &'a Command,
    _conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        match crate::integration::connection_manager::get_connection() {
            Ok(mut sync_conn) => {
                handle_channel_info(command, &mut sync_conn, &GNodeDaemon::get_topology_ref(), site_id, debug_mode)
            },
            Err(e) => CommandResult::error(format!("Failed to get connection: {}", e)),
        }
    })
}

pub fn handle_channel_list_async<'a>(
    command: &'a Command,
    _conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        match crate::integration::connection_manager::get_connection() {
            Ok(mut sync_conn) => {
                handle_channel_list(command, &mut sync_conn, &GNodeDaemon::get_topology_ref(), site_id, debug_mode)
            },
            Err(e) => CommandResult::error(format!("Failed to get connection: {}", e)),
        }
    })
}
