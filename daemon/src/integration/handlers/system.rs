// System Command Handlers
//
// Handles: ping, health, version, echo, status, load_update, node_info, site_info
// These are the core operational commands for daemon health and introspection.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use std::pin::Pin;
use std::future::Future;
use redis::Connection;
use redis::aio::MultiplexedConnection as AsyncConnection;
use serde::Deserialize;
use log::debug;
use serde_json::{Value, json};
use crate::daemon::Command;
use crate::GeometricTopology;
use crate::integration::valkey_functions::execute_function;
use crate::integration::processor::stream_utils::current_timestamp;

use super::types::{CommandResult, CommandDescriptor, CommandHandlerFn, AsyncCommandHandlerFn, parse_parameters, Lane};

/// Register all system command handlers
pub fn register(
    handlers: &mut HashMap<String, CommandHandlerFn>,
    async_handlers: &mut HashMap<String, AsyncCommandHandlerFn>,
    descriptors: &mut Vec<CommandDescriptor>,
) {
    // Sync handlers
    handlers.insert("ping".to_string(), handle_ping as CommandHandlerFn);
    handlers.insert("PING".to_string(), handle_ping as CommandHandlerFn);
    handlers.insert("status".to_string(), handle_status as CommandHandlerFn);
    handlers.insert("STATUS".to_string(), handle_status as CommandHandlerFn);
    handlers.insert("health".to_string(), handle_health as CommandHandlerFn);
    handlers.insert("HEALTH".to_string(), handle_health as CommandHandlerFn);
    handlers.insert("version".to_string(), handle_version as CommandHandlerFn);
    handlers.insert("VERSION".to_string(), handle_version as CommandHandlerFn);
    handlers.insert("echo".to_string(), handle_echo as CommandHandlerFn);
    handlers.insert("ECHO".to_string(), handle_echo as CommandHandlerFn);
    handlers.insert("info".to_string(), handle_status as CommandHandlerFn);
    handlers.insert("INFO".to_string(), handle_status as CommandHandlerFn);
    handlers.insert("get_node_info".to_string(), handle_node_info as CommandHandlerFn);
    handlers.insert("GET_NODE_INFO".to_string(), handle_node_info as CommandHandlerFn);
    handlers.insert("get_site_info".to_string(), handle_site_info as CommandHandlerFn);
    handlers.insert("GET_SITE_INFO".to_string(), handle_site_info as CommandHandlerFn);
    handlers.insert("load_update".to_string(), handle_load_update as CommandHandlerFn);
    handlers.insert("describe".to_string(), handle_describe as CommandHandlerFn);
    handlers.insert("extension_list".to_string(), handle_extension_list as CommandHandlerFn);
    handlers.insert("extension_info".to_string(), handle_extension_info as CommandHandlerFn);

    // Async handlers
    async_handlers.insert("ping".to_string(), handle_ping_async as AsyncCommandHandlerFn);
    async_handlers.insert("PING".to_string(), handle_ping_async as AsyncCommandHandlerFn);
    async_handlers.insert("health".to_string(), handle_health_async as AsyncCommandHandlerFn);
    async_handlers.insert("HEALTH".to_string(), handle_health_async as AsyncCommandHandlerFn);
    async_handlers.insert("version".to_string(), handle_version_async as AsyncCommandHandlerFn);
    async_handlers.insert("VERSION".to_string(), handle_version_async as AsyncCommandHandlerFn);
    async_handlers.insert("echo".to_string(), handle_echo_async as AsyncCommandHandlerFn);
    async_handlers.insert("ECHO".to_string(), handle_echo_async as AsyncCommandHandlerFn);
    async_handlers.insert("status".to_string(), handle_status_async as AsyncCommandHandlerFn);
    async_handlers.insert("STATUS".to_string(), handle_status_async as AsyncCommandHandlerFn);
    async_handlers.insert("info".to_string(), handle_status_async as AsyncCommandHandlerFn);
    async_handlers.insert("INFO".to_string(), handle_status_async as AsyncCommandHandlerFn);
    async_handlers.insert("load_update".to_string(), handle_load_update_async as AsyncCommandHandlerFn);
    async_handlers.insert("LOAD_UPDATE".to_string(), handle_load_update_async as AsyncCommandHandlerFn);
    async_handlers.insert("get_node_info".to_string(), handle_node_info_async as AsyncCommandHandlerFn);
    async_handlers.insert("GET_NODE_INFO".to_string(), handle_node_info_async as AsyncCommandHandlerFn);
    async_handlers.insert("node_info".to_string(), handle_node_info_async as AsyncCommandHandlerFn);
    async_handlers.insert("get_site_info".to_string(), handle_site_info_async as AsyncCommandHandlerFn);
    async_handlers.insert("GET_SITE_INFO".to_string(), handle_site_info_async as AsyncCommandHandlerFn);
    async_handlers.insert("site_info".to_string(), handle_site_info_async as AsyncCommandHandlerFn);
    async_handlers.insert("describe".to_string(), handle_describe_async as AsyncCommandHandlerFn);
    async_handlers.insert("extension_list".to_string(), handle_extension_list_async as AsyncCommandHandlerFn);
    async_handlers.insert("EXTENSION_LIST".to_string(), handle_extension_list_async as AsyncCommandHandlerFn);
    async_handlers.insert("extension_info".to_string(), handle_extension_info_async as AsyncCommandHandlerFn);
    async_handlers.insert("EXTENSION_INFO".to_string(), handle_extension_info_async as AsyncCommandHandlerFn);

    // Descriptors
    descriptors.push(CommandDescriptor {
        name: "ping",
        category: "system",
        description: "Health check that returns pong, or echoes back a message",
        params_schema: json!({"type": "object", "properties": {"message": {"type": "string", "description": "Optional message to echo back"}}}),
        returns_schema: json!({"oneOf": [{"type": "boolean", "description": "true when no message provided"}, {"type": "string", "description": "Echoed message"}]}),
        example: r#"{"cmd":"ping","params":{}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });
    descriptors.push(CommandDescriptor {
        name: "status",
        category: "system",
        description: "Daemon status and metrics. Use detail=full for connection pool, ValKey info, and supported commands. Use detail=schema for command documentation summary",
        params_schema: json!({"type": "object", "properties": {"detail": {"type": "string", "enum": ["basic", "full", "schema"], "default": "basic"}}}),
        returns_schema: json!({"type": "object", "properties": {"version": {"type": "string"}, "uptime": {"type": "integer"}, "timestamp": {"type": "number"}}}),
        example: r#"{"cmd":"status","params":{"detail":"full"}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });
    descriptors.push(CommandDescriptor {
        name: "health",
        category: "system",
        description: "Health check with ValKey connectivity status",
        params_schema: json!({"type": "object", "properties": {}}),
        returns_schema: json!({"type": "object", "properties": {"status": {"type": "string", "enum": ["healthy", "unhealthy"]}, "checks": {"type": "object"}}}),
        example: r#"{"cmd":"health","params":{}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });
    descriptors.push(CommandDescriptor {
        name: "version",
        category: "system",
        description: "Daemon version, build date, and Rust compiler version",
        params_schema: json!({"type": "object", "properties": {}}),
        returns_schema: json!({"type": "object", "properties": {"version": {"type": "string"}, "build_date": {"type": "string"}, "rust_version": {"type": "string"}}}),
        example: r#"{"cmd":"version","params":{}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });
    descriptors.push(CommandDescriptor {
        name: "echo",
        category: "system",
        description: "Echo back the provided message or all parameters",
        params_schema: json!({"type": "object", "properties": {"message": {"description": "Value to echo back. If omitted, all params are returned"}}}),
        returns_schema: json!({"description": "The message value or full parameters object"}),
        example: r#"{"cmd":"echo","params":{"message":"hello"}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });
    descriptors.push(CommandDescriptor {
        name: "get_node_info",
        category: "system",
        description: "Get configuration and status for a specific daemon node",
        params_schema: json!({"type": "object", "required": ["node_id"], "properties": {"node_id": {"type": "string", "description": "Node identifier to query"}}}),
        returns_schema: json!({"type": "object", "description": "Node configuration and status from ValKey"}),
        example: r#"{"cmd":"get_node_info","params":{"node_id":"default"}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });
    descriptors.push(CommandDescriptor {
        name: "get_site_info",
        category: "system",
        description: "Get registration info for the current site",
        params_schema: json!({"type": "object", "properties": {}}),
        returns_schema: json!({"type": "object", "description": "Site registration metadata from ValKey"}),
        example: r#"{"cmd":"get_site_info","params":{}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });
    descriptors.push(CommandDescriptor {
        name: "load_update",
        category: "system",
        description: "Update load metrics for a service (used by health monitoring)",
        params_schema: json!({"type": "object", "required": ["service_id", "load_factor"], "properties": {
            "service_id": {"type": "string"},
            "load_factor": {"type": "number", "minimum": 0.0, "maximum": 1.0},
            "cpu_usage": {"type": "number", "minimum": 0.0, "maximum": 1.0},
            "memory_usage": {"type": "number", "minimum": 0.0, "maximum": 1.0},
            "active_requests": {"type": "integer", "minimum": 0},
            "avg_latency_ms": {"type": "integer", "minimum": 0},
            "error_rate": {"type": "number", "minimum": 0.0, "maximum": 1.0}
        }}),
        returns_schema: json!({"type": "object", "properties": {"service_id": {"type": "string"}, "load_factor": {"type": "number"}, "score": {"type": "number"}, "updated": {"type": "boolean"}}}),
        example: r#"{"cmd":"load_update","params":{"service_id":"web1","load_factor":0.4}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });
    descriptors.push(CommandDescriptor {
        name: "describe",
        category: "system",
        description: "Query command schemas for runtime API discovery. Returns parameter and return schemas for one or all commands",
        params_schema: json!({"type": "object", "properties": {
            "command": {"type": "string", "description": "Specific command name to describe. If omitted, returns all commands"},
            "format": {"type": "string", "enum": ["full", "list"], "default": "full", "description": "full=complete schemas, list=names and descriptions only"},
            "category": {"type": "string", "description": "Filter by category (e.g. system, geometric, topology)"}
        }}),
        returns_schema: json!({"oneOf": [
            {"type": "object", "description": "Single command descriptor (when command param is set)"},
            {"type": "object", "description": "Commands grouped by category"}
        ]}),
        example: r#"{"cmd":"describe","params":{"command":"ping"}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });
    descriptors.push(CommandDescriptor {
        name: "extension_list",
        category: "system",
        description: "List all gNode extensions with their operational status. Shows which pro extensions are compiled, have Lua libraries available, and are enabled",
        params_schema: json!({"type": "object", "properties": {
            "tier": {"type": "string", "enum": ["base", "pro", "all"], "default": "all", "description": "Filter by tier"}
        }}),
        returns_schema: json!({"type": "object", "properties": {
            "extensions": {"type": "array", "items": {"type": "object"}},
            "summary": {"type": "object", "properties": {
                "total": {"type": "integer"},
                "operational": {"type": "integer"},
                "pro_compiled": {"type": "boolean"}
            }}
        }}),
        example: r#"{"cmd":"extension_list","params":{}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });
    descriptors.push(CommandDescriptor {
        name: "extension_info",
        category: "system",
        description: "Get detailed information about a specific gNode extension including its commands, Lua libraries, and operational status",
        params_schema: json!({"type": "object", "required": ["name"], "properties": {
            "name": {"type": "string", "description": "Extension name (one of the values returned by extension_list)"}
        }}),
        returns_schema: json!({"type": "object", "description": "Full extension status including commands, Lua deps, and operational details"}),
        example: r#"{"cmd":"extension_info","params":{"name":"cms"}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });
}

// =========================================================================
// Parameter structs
// =========================================================================

#[derive(Debug, Deserialize)]
struct StatusParams {
    #[serde(default = "default_detail")]
    detail: String,
}

fn default_detail() -> String {
    "basic".to_string()
}

// =========================================================================
// Sync handlers
// =========================================================================

pub fn handle_ping(
    command: &Command,
    _conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    _site_id: &str,
    debug_mode: bool
) -> CommandResult {
    if debug_mode {
        debug!("Handling ping command: {}", command.id);
    }

    if let Some(message) = command.parameters.get("message") {
        if let Some(message_str) = message.as_str() {
            return CommandResult::success(message_str);
        }
    }

    CommandResult::success(true)
}

pub fn handle_status(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    site_id: &str,
    debug_mode: bool
) -> CommandResult {
    if debug_mode {
        debug!("Handling status command: {}", command.id);
    }

    let params = match parse_parameters::<StatusParams>(command) {
        Ok(p) => p,
        Err(e) => return CommandResult::error(e),
    };

    let mut status = json!({
        "version": env!("CARGO_PKG_VERSION"),
        "uptime": std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
        "timestamp": current_timestamp(),
    });

    if params.detail == "schema" {
        let command_registry = super::super::command_handler::get_command_registry();
        let by_category = command_registry.get_descriptors_by_category();

        let mut category_counts = serde_json::Map::new();
        let mut total = 0usize;
        for (cat, descs) in &by_category {
            category_counts.insert(cat.clone(), json!(descs.len()));
            total += descs.len();
        }

        let ext_manager = crate::extensions::get_extension_manager();

        if let Value::Object(ref mut map) = status {
            map.insert("command_schema".to_string(), json!({
                "total_documented_commands": total,
                "categories": category_counts,
            }));
            map.insert("extensions".to_string(), json!({
                "total": ext_manager.total_count(),
                "operational": ext_manager.operational_count(),
                "extension_paths": ext_manager.all_extension_paths().iter()
                    .map(|(k, v)| (k.clone(), v.display().to_string()))
                    .collect::<std::collections::HashMap<_, _>>(),
            }));
        }
    } else if params.detail == "full" {
        let redis_info: redis::RedisResult<String> = redis::cmd("INFO")
            .query(conn);

        let pool_status = match crate::integration::connection_manager::get_pool_status() {
            Ok((total, idle)) => json!({
                "total_connections": total,
                "idle_connections": idle
            }),
            Err(_) => json!({}),
        };

        let command_registry = super::super::command_handler::get_command_registry();
        let supported_commands = command_registry.get_command_names();

        let valkey_functions = match execute_function(
            conn,
            "GNODE_UTILS_SERVER_INFO",
            &[],
            &[site_id],
            site_id,
            debug_mode
        ) {
            Ok(info) => {
                match serde_json::from_str(&info) {
                    Ok(json) => json,
                    Err(_) => json!({ "status": "error", "message": "Failed to parse ValKey function info" }),
                }
            },
            Err(_) => {
                json!({ "status": "unavailable" })
            }
        };

        if let serde_json::Value::Object(ref mut map) = status {
            map.insert("connection_pool".to_string(), pool_status);
            map.insert("supported_commands".to_string(), json!(supported_commands));
            map.insert("valkey_functions".to_string(), valkey_functions);

            if let Ok(info) = redis_info {
                let mut redis_data = serde_json::Map::new();
                for line in info.lines() {
                    if line.starts_with('#') || line.is_empty() {
                        continue;
                    }
                    if let Some(idx) = line.find(':') {
                        let key = line[..idx].trim();
                        let value = line[idx+1..].trim();
                        redis_data.insert(key.to_string(), Value::String(value.to_string()));
                    }
                }
                map.insert("redis_info".to_string(), Value::Object(redis_data));
            }
        }
    }

    CommandResult::success(status)
}

pub fn handle_health(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    _site_id: &str,
    debug_mode: bool
) -> CommandResult {
    if debug_mode {
        debug!("Handling health command: {}", command.id);
    }

    let redis_ok = match redis::cmd("PING").query::<String>(conn) {
        Ok(response) => response == "PONG",
        Err(_) => false,
    };

    let health = json!({
        "status": if redis_ok { "healthy" } else { "unhealthy" },
        "checks": {
            "redis": {
                "status": if redis_ok { "pass" } else { "fail" },
                "message": if redis_ok { "Redis connection is healthy" } else { "Redis connection failed" }
            }
        },
        "timestamp": current_timestamp()
    });

    CommandResult::success(health)
}

pub fn handle_version(
    command: &Command,
    _conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    _site_id: &str,
    debug_mode: bool
) -> CommandResult {
    if debug_mode {
        debug!("Handling version command: {}", command.id);
    }

    let version = json!({
        "version": env!("CARGO_PKG_VERSION"),
        "build_date": env!("CARGO_PKG_AUTHORS"),
        "rust_version": format!("{}.{}.{}",
            rustc_version_runtime::version_meta().semver.major,
            rustc_version_runtime::version_meta().semver.minor,
            rustc_version_runtime::version_meta().semver.patch)
    });

    CommandResult::success(version)
}

pub fn handle_echo(
    command: &Command,
    _conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    _site_id: &str,
    debug_mode: bool
) -> CommandResult {
    if debug_mode {
        debug!("Handling echo command: {}", command.id);
    }

    if let Some(message) = command.parameters.get("message") {
        return CommandResult::success(message.clone());
    }

    CommandResult::success(command.parameters.clone())
}

pub fn handle_node_info(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    site_id: &str,
    debug_mode: bool
) -> CommandResult {
    if debug_mode {
        debug!("Handling node_info command: {}", command.id);
    }

    let node_id = match command.parameters.get("node_id") {
        Some(Value::String(id)) => id.clone(),
        _ => return CommandResult::error("Missing or invalid node_id parameter"),
    };

    let result = execute_function(
        conn,
        "GNODE_SERVICE_GET_NODE_INFO",
        &[],
        &[site_id, &node_id],
        site_id,
        debug_mode
    );

    match result {
        Ok(json_str) => CommandResult::success_json(json_str),
        Err(e) => CommandResult::error(format!("Error retrieving node info: {}", e)),
    }
}

pub fn handle_site_info(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    site_id: &str,
    debug_mode: bool
) -> CommandResult {
    if debug_mode {
        debug!("Handling site_info command: {}", command.id);
    }

    let result = execute_function(
        conn,
        "GNODE_SERVICE_GET_INFO",
        &[],
        &[site_id],
        site_id,
        debug_mode
    );

    match result {
        Ok(json_str) => CommandResult::success_json(json_str),
        Err(e) => CommandResult::error(format!("Error retrieving site info: {}", e)),
    }
}

pub fn handle_load_update(
    command: &Command,
    _conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    _site_id: &str,
    debug_mode: bool
) -> CommandResult {
    if debug_mode {
        debug!("Handling load_update command: {}", command.id);
    }

    let service_id = match command.parameters.get("service_id").and_then(|v| v.as_str()) {
        Some(id) if !id.is_empty() => id.to_string(),
        _ => {
            return CommandResult::error("Missing or empty service_id parameter");
        }
    };

    let load_factor = match command.parameters.get("load_factor").and_then(|v| v.as_f64()) {
        Some(l) if (0.0..=1.0).contains(&l) => l,
        _ => {
            return CommandResult::error("Missing or invalid load_factor parameter (must be 0.0-1.0)");
        }
    };

    let cpu_usage = command.parameters.get("cpu_usage").and_then(|v| v.as_f64());
    let memory_usage = command.parameters.get("memory_usage").and_then(|v| v.as_f64());
    let active_requests = command.parameters.get("active_requests").and_then(|v| v.as_u64()).map(|v| v as u32);
    let avg_latency_ms = command.parameters.get("avg_latency_ms").and_then(|v| v.as_u64());
    let error_rate = command.parameters.get("error_rate").and_then(|v| v.as_f64());

    let metrics = crate::integration::load_metrics::LoadMetrics {
        service_id: service_id.clone(),
        load_factor,
        cpu_usage,
        memory_usage,
        active_requests,
        avg_latency_ms,
        error_rate,
        last_update: crate::utils::current_timestamp_ms(),
        ttl_seconds: 30,
    };

    if let Some(load_manager) = crate::daemon::GNodeDaemon::get_load_metrics_manager_ref() {
        load_manager.update(metrics.clone());

        if debug_mode {
            debug!("Updated load metrics for service {}: load={:.2}, score={:.2}",
                service_id, load_factor, metrics.score());
        }

        CommandResult::success(json!({
            "service_id": service_id,
            "load_factor": load_factor,
            "score": metrics.score(),
            "updated": true
        }))
    } else {
        CommandResult::error("Load metrics manager not initialized")
    }
}

pub fn handle_describe(
    command: &Command,
    _conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    _site_id: &str,
    debug_mode: bool
) -> CommandResult {
    if debug_mode {
        debug!("Handling describe command: {}", command.id);
    }

    let registry = super::super::command_handler::get_command_registry();

    // Single command mode
    if let Some(Value::String(cmd_name)) = command.parameters.get("command") {
        return match registry.get_descriptor(cmd_name) {
            Some(desc) => CommandResult::success(desc.to_json()),
            None => CommandResult::error(format!("Unknown command: {}", cmd_name)),
        };
    }

    // Multi-command mode
    let format = command.parameters.get("format")
        .and_then(|v| v.as_str())
        .unwrap_or("full");
    let category_filter = command.parameters.get("category")
        .and_then(|v| v.as_str());

    let by_category = registry.get_descriptors_by_category();
    let mut result = serde_json::Map::new();

    for (cat, descs) in &by_category {
        if let Some(filter) = category_filter {
            if cat != filter { continue; }
        }
        let entries: Vec<Value> = descs.iter().map(|d| {
            if format == "list" {
                json!({"name": d.name, "description": d.description, "async_capable": d.async_capable})
            } else {
                d.to_json()
            }
        }).collect();
        result.insert(cat.clone(), json!(entries));
    }

    CommandResult::success(Value::Object(result))
}

pub fn handle_extension_list(
    command: &Command,
    _conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    _site_id: &str,
    debug_mode: bool
) -> CommandResult {
    if debug_mode {
        debug!("Handling extension_list command: {}", command.id);
    }

    let ext_manager = crate::extensions::get_extension_manager();
    let statuses = ext_manager.list();

    let extensions_json: Vec<Value> = statuses.iter().map(|s| {
        json!({
            "name": s.name,
            "display_name": s.display_name,
            "version": s.version,
            "operational": s.operational,
            "rust_compiled": s.rust_compiled,
            "lua_available": s.lua_available,
            "config_enabled": s.config_enabled,
            "commands_count": s.commands.len(),
        })
    }).collect();

    CommandResult::success(json!({
        "extensions": extensions_json,
        "summary": {
            "total": ext_manager.total_count(),
            "operational": ext_manager.operational_count(),
            "extension_paths": ext_manager.all_extension_paths().iter()
                .map(|(k, v)| (k.clone(), v.display().to_string()))
                .collect::<std::collections::HashMap<_, _>>(),
        }
    }))
}

pub fn handle_extension_info(
    command: &Command,
    _conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    _site_id: &str,
    debug_mode: bool
) -> CommandResult {
    if debug_mode {
        debug!("Handling extension_info command: {}", command.id);
    }

    let name = match command.parameters.get("name").and_then(|v| v.as_str()) {
        Some(n) if !n.is_empty() => n,
        _ => return CommandResult::error("Missing or empty 'name' parameter"),
    };

    let ext_manager = crate::extensions::get_extension_manager();
    match ext_manager.get(name) {
        Some(status) => CommandResult::success(json!({
            "name": status.name,
            "display_name": status.display_name,
            "version": status.version,
            "description": status.description,
            "operational": status.operational,
            "rust_compiled": status.rust_compiled,
            "lua_available": status.lua_available,
            "config_enabled": status.config_enabled,
            "lua_libraries": status.lua_libraries,
            "commands": status.commands,
        })),
        None => CommandResult::error(format!("Extension '{}' not found", name)),
    }
}

// =========================================================================
// Async handlers
// =========================================================================

pub fn handle_ping_async<'a>(
    command: &'a Command,
    _conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    _site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling async ping command: {}", command.id);
        }

        if let Some(message) = command.parameters.get("message") {
            if let Some(message_str) = message.as_str() {
                return CommandResult::success(message_str);
            }
        }

        CommandResult::success(true)
    })
}

pub fn handle_health_async<'a>(
    command: &'a Command,
    conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    _site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling async health command: {}", command.id);
        }

        let redis_ok = match redis::cmd("PING")
            .query_async::<String>(conn)
            .await
        {
            Ok(response) => response == "PONG",
            Err(_) => false,
        };

        let health = json!({
            "status": if redis_ok { "healthy" } else { "unhealthy" },
            "checks": {
                "redis": {
                    "status": if redis_ok { "pass" } else { "fail" },
                    "message": if redis_ok { "Redis connection is healthy" } else { "Redis connection failed" }
                }
            },
            "timestamp": current_timestamp()
        });

        CommandResult::success(health)
    })
}

pub fn handle_version_async<'a>(
    command: &'a Command,
    _conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    _site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling async version command: {}", command.id);
        }

        let version = json!({
            "version": env!("CARGO_PKG_VERSION"),
            "build_date": env!("CARGO_PKG_AUTHORS"),
            "rust_version": format!("{}.{}.{}",
                rustc_version_runtime::version_meta().semver.major,
                rustc_version_runtime::version_meta().semver.minor,
                rustc_version_runtime::version_meta().semver.patch)
        });

        CommandResult::success(version)
    })
}

pub fn handle_echo_async<'a>(
    command: &'a Command,
    _conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    _site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling async echo command: {}", command.id);
        }

        if let Some(message) = command.parameters.get("message") {
            return CommandResult::success(message.clone());
        }

        CommandResult::success(command.parameters.clone())
    })
}

pub fn handle_status_async<'a>(
    command: &'a Command,
    _conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    _site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling async status command: {}", command.id);
        }

        let status = json!({
            "version": env!("CARGO_PKG_VERSION"),
            "uptime": std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            "timestamp": current_timestamp(),
            "async": true,
        });

        CommandResult::success(status)
    })
}

pub fn handle_load_update_async<'a>(
    command: &'a Command,
    _conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    _site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling async load_update command: {}", command.id);
        }

        let service_id = match command.parameters.get("service_id").and_then(|v| v.as_str()) {
            Some(id) if !id.is_empty() => id.to_string(),
            _ => {
                return CommandResult::error("Missing or empty service_id parameter");
            }
        };

        let load_factor = match command.parameters.get("load_factor").and_then(|v| v.as_f64()) {
            Some(l) if (0.0..=1.0).contains(&l) => l,
            _ => {
                return CommandResult::error("Missing or invalid load_factor parameter (must be 0.0-1.0)");
            }
        };

        let cpu_usage = command.parameters.get("cpu_usage").and_then(|v| v.as_f64());
        let memory_usage = command.parameters.get("memory_usage").and_then(|v| v.as_f64());
        let active_requests = command.parameters.get("active_requests").and_then(|v| v.as_u64()).map(|v| v as u32);
        let avg_latency_ms = command.parameters.get("avg_latency_ms").and_then(|v| v.as_u64());
        let error_rate = command.parameters.get("error_rate").and_then(|v| v.as_f64());

        let metrics = crate::integration::load_metrics::LoadMetrics {
            service_id: service_id.clone(),
            load_factor,
            cpu_usage,
            memory_usage,
            active_requests,
            avg_latency_ms,
            error_rate,
            last_update: crate::utils::current_timestamp_ms(),
            ttl_seconds: 30,
        };

        if let Some(load_manager) = crate::daemon::GNodeDaemon::get_load_metrics_manager_ref() {
            load_manager.update(metrics.clone());

            if debug_mode {
                debug!("Updated load metrics for service {}: load={:.2}, score={:.2}",
                    service_id, load_factor, metrics.score());
            }

            CommandResult::success(json!({
                "service_id": service_id,
                "load_factor": load_factor,
                "score": metrics.score(),
                "updated": true,
                "async": true
            }))
        } else {
            CommandResult::error("Load metrics manager not initialized")
        }
    })
}

pub fn handle_node_info_async<'a>(
    command: &'a Command,
    _conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling async node_info command: {}", command.id);
        }

        CommandResult::success(json!({
            "node_id": "default",
            "site_id": site_id,
            "role": "primary",
            "status": "active",
            "version": env!("CARGO_PKG_VERSION"),
            "uptime_seconds": std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            "async": true
        }))
    })
}

pub fn handle_site_info_async<'a>(
    command: &'a Command,
    _conn: &'a mut AsyncConnection,
    topology: &'a Arc<RwLock<GeometricTopology>>,
    site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling async site_info command: {}", command.id);
        }

        let services_count = match topology.read() {
            Ok(t) => t.services.len(),
            Err(_) => 0,
        };

        CommandResult::success(json!({
            "site_id": site_id,
            "environment": "production",
            "services_registered": services_count,
            "streams_active": 4,
            "status": "operational",
            "async": true
        }))
    })
}

pub fn handle_describe_async<'a>(
    command: &'a Command,
    _conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    _site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling async describe command: {}", command.id);
        }

        let registry = super::super::command_handler::get_command_registry();

        if let Some(Value::String(cmd_name)) = command.parameters.get("command") {
            return match registry.get_descriptor(cmd_name) {
                Some(desc) => CommandResult::success(desc.to_json()),
                None => CommandResult::error(format!("Unknown command: {}", cmd_name)),
            };
        }

        let format = command.parameters.get("format")
            .and_then(|v| v.as_str())
            .unwrap_or("full");
        let category_filter = command.parameters.get("category")
            .and_then(|v| v.as_str());

        let by_category = registry.get_descriptors_by_category();
        let mut result = serde_json::Map::new();

        for (cat, descs) in &by_category {
            if let Some(filter) = category_filter {
                if cat != filter { continue; }
            }
            let entries: Vec<Value> = descs.iter().map(|d| {
                if format == "list" {
                    json!({"name": d.name, "description": d.description, "async_capable": d.async_capable})
                } else {
                    d.to_json()
                }
            }).collect();
            result.insert(cat.clone(), json!(entries));
        }

        CommandResult::success(Value::Object(result))
    })
}

pub fn handle_extension_list_async<'a>(
    command: &'a Command,
    _conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    _site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling async extension_list command: {}", command.id);
        }

        let ext_manager = crate::extensions::get_extension_manager();
        let statuses = ext_manager.list();

        let extensions_json: Vec<Value> = statuses.iter().map(|s| {
            json!({
                "name": s.name,
                "display_name": s.display_name,
                "version": s.version,
                "operational": s.operational,
                "rust_compiled": s.rust_compiled,
                "lua_available": s.lua_available,
                "config_enabled": s.config_enabled,
                "commands_count": s.commands.len(),
                "async": true,
            })
        }).collect();

        CommandResult::success(json!({
            "extensions": extensions_json,
            "summary": {
                "total": ext_manager.total_count(),
                "operational": ext_manager.operational_count(),
                "extension_paths": ext_manager.all_extension_paths().iter()
                    .map(|(k, v)| (k.clone(), v.display().to_string()))
                    .collect::<std::collections::HashMap<_, _>>(),
            },
            "async": true,
        }))
    })
}

pub fn handle_extension_info_async<'a>(
    command: &'a Command,
    _conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    _site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling async extension_info command: {}", command.id);
        }

        let name = match command.parameters.get("name").and_then(|v| v.as_str()) {
            Some(n) if !n.is_empty() => n,
            _ => return CommandResult::error("Missing or empty 'name' parameter"),
        };

        let ext_manager = crate::extensions::get_extension_manager();
        match ext_manager.get(name) {
            Some(status) => CommandResult::success(json!({
                "name": status.name,
                "display_name": status.display_name,
                "version": status.version,
                    "description": status.description,
                "operational": status.operational,
                "rust_compiled": status.rust_compiled,
                "lua_available": status.lua_available,
                "config_enabled": status.config_enabled,
                "lua_libraries": status.lua_libraries,
                "commands": status.commands,
                "async": true,
            })),
            None => CommandResult::error(format!("Extension '{}' not found", name)),
        }
    })
}
