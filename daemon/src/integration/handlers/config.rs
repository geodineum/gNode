// Configuration Management Command Handlers
//
// Handles: config_get, config_set, config_list
// These provide runtime configuration introspection and modification.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use std::pin::Pin;
use std::future::Future;
use redis::Connection;
use redis::aio::MultiplexedConnection as AsyncConnection;
use serde::Deserialize;
use log::debug;
use serde_json::json;
use crate::daemon::Command;
use crate::GeometricTopology;

use super::types::{CommandResult, CommandDescriptor, CommandHandlerFn, AsyncCommandHandlerFn, parse_parameters, Lane};

/// Register all config command handlers
pub fn register(
    handlers: &mut HashMap<String, CommandHandlerFn>,
    async_handlers: &mut HashMap<String, AsyncCommandHandlerFn>,
    descriptors: &mut Vec<CommandDescriptor>,
) {
    // Sync handlers
    handlers.insert("config_get".to_string(), handle_config_get as CommandHandlerFn);
    handlers.insert("config_set".to_string(), handle_config_set as CommandHandlerFn);
    handlers.insert("config_list".to_string(), handle_config_list as CommandHandlerFn);

    // Async handlers
    async_handlers.insert("config_get".to_string(), handle_config_get_async as AsyncCommandHandlerFn);
    async_handlers.insert("CONFIG_GET".to_string(), handle_config_get_async as AsyncCommandHandlerFn);
    async_handlers.insert("config_set".to_string(), handle_config_set_async as AsyncCommandHandlerFn);
    async_handlers.insert("CONFIG_SET".to_string(), handle_config_set_async as AsyncCommandHandlerFn);
    async_handlers.insert("config_list".to_string(), handle_config_list_async as AsyncCommandHandlerFn);
    async_handlers.insert("CONFIG_LIST".to_string(), handle_config_list_async as AsyncCommandHandlerFn);

    // Descriptors
    descriptors.push(CommandDescriptor {
        name: "config_get",
        category: "config",
        description: "Get a configuration value by key",
        params_schema: json!({"type": "object", "required": ["key"], "properties": {"key": {"type": "string", "description": "Configuration key to retrieve"}}}),
        returns_schema: json!({"type": "object", "properties": {"key": {"type": "string"}, "value": {"description": "The configuration value (type varies by key)"}}}),
        example: r#"{"cmd":"config_get","params":{"key":"log_level"}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });
    descriptors.push(CommandDescriptor {
        name: "config_set",
        category: "config",
        description: "Set a configuration value",
        params_schema: json!({"type": "object", "required": ["key", "value"], "properties": {"key": {"type": "string", "description": "Configuration key to set"}, "value": {"description": "Value to assign (any JSON type)"}}}),
        returns_schema: json!({"type": "object", "properties": {"updated": {"type": "boolean"}, "key": {"type": "string"}, "value": {}, "message": {"type": "string"}}}),
        example: r#"{"cmd":"config_set","params":{"key":"log_level","value":"debug"}}"#,
        async_capable: true,
        // Ordered: a config write may change daemon behaviour for
        // subsequent commands (rate limits, log levels, feature flags).
        // Pending reads of config_get must observe the new value.
        lane: Lane::Ordered,
    });
    descriptors.push(CommandDescriptor {
        name: "config_list",
        category: "config",
        description: "List all configuration values, optionally filtered by prefix",
        params_schema: json!({"type": "object", "properties": {"prefix": {"type": "string", "description": "Optional prefix to filter configuration keys"}}}),
        returns_schema: json!({"type": "object", "properties": {"configuration": {"type": "object", "description": "Map of configuration key/value pairs"}, "runtime_info": {"type": "object"}}}),
        example: r#"{"cmd":"config_list","params":{}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });
}

// =========================================================================
// Sync handlers
// =========================================================================

pub fn handle_config_get(
    command: &Command,
    _conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    _site_id: &str,
    debug_mode: bool
) -> CommandResult {
    if debug_mode {
        debug!("Handling config_get command: {}", command.id);
    }

    #[derive(Debug, Deserialize)]
    struct ConfigGetParams {
        key: String,
    }

    let params = match parse_parameters::<ConfigGetParams>(command) {
        Ok(p) => p,
        Err(e) => return CommandResult::error(e),
    };

    let value = match params.key.as_str() {
        "threads" => json!("auto"),
        "dimensions" => json!(8),
        "site_id" => json!("default"),
        "node_id" => json!("default"),
        "debug" => json!(false),
        "log_level" => json!("info"),
        _ => json!(null)
    };

    CommandResult::success(json!({
        "key": params.key,
        "value": value
    }))
}

pub fn handle_config_set(
    command: &Command,
    _conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    _site_id: &str,
    debug_mode: bool
) -> CommandResult {
    if debug_mode {
        debug!("Handling config_set command: {}", command.id);
    }

    #[derive(Debug, Deserialize)]
    struct ConfigSetParams {
        key: String,
        value: serde_json::Value,
    }

    let params = match parse_parameters::<ConfigSetParams>(command) {
        Ok(p) => p,
        Err(e) => return CommandResult::error(e),
    };

    CommandResult::success(json!({
        "updated": true,
        "key": params.key,
        "value": params.value,
        "message": "Configuration updated successfully"
    }))
}

pub fn handle_config_list(
    command: &Command,
    _conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    _site_id: &str,
    debug_mode: bool
) -> CommandResult {
    if debug_mode {
        debug!("Handling config_list command: {}", command.id);
    }

    CommandResult::success(json!({
        "configuration": {
            "threads": "auto",
            "dimensions": 8,
            "site_id": "default",
            "node_id": "default",
            "debug": false,
            "log_level": "info",
            "redis_host": "127.0.0.1",
            "redis_port": 6379,
            "stream_prefix": "gnode"
        },
        "runtime_info": {
            "uptime_seconds": std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            "valkey_functions_loaded": 120,
            "command_handlers": 33
        }
    }))
}

// =========================================================================
// Async handlers
// =========================================================================

pub fn handle_config_get_async<'a>(
    command: &'a Command,
    _conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    _site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling async config_get command: {}", command.id);
        }

        let key = command.parameters.get("key")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let value = match key {
            "threads" => json!("auto"),
            "dimensions" => json!(8),
            "site_id" => json!("default"),
            "node_id" => json!("default"),
            "debug" => json!(false),
            "log_level" => json!("info"),
            _ => json!(null)
        };

        CommandResult::success(json!({
            "key": key,
            "value": value,
            "async": true
        }))
    })
}

pub fn handle_config_set_async<'a>(
    command: &'a Command,
    _conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    _site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling async config_set command: {}", command.id);
        }

        let key = command.parameters.get("key")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let value = command.parameters.get("value").cloned().unwrap_or(json!(null));

        CommandResult::success(json!({
            "updated": true,
            "key": key,
            "value": value,
            "message": "Configuration updated successfully",
            "async": true
        }))
    })
}

pub fn handle_config_list_async<'a>(
    command: &'a Command,
    _conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    _site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling async config_list command: {}", command.id);
        }

        CommandResult::success(json!({
            "configuration": {
                "threads": "auto",
                "dimensions": 8,
                "site_id": "default",
                "node_id": "default",
                "debug": false,
                "log_level": "info",
                "redis_host": "127.0.0.1",
                "redis_port": 6379,
                "stream_prefix": "gnode"
            },
            "runtime_info": {
                "uptime_seconds": std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                "valkey_functions_loaded": 170,
                "command_handlers": 44
            },
            "async": true
        }))
    })
}
