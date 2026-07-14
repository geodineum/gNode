// Stream Command Handlers
//
// Handles: stream_info, stream_group_info, stream_consumer_info, stream_pending
// These provide ValKey stream introspection capabilities.

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
use crate::{json_redis_cmd, json_redis_cmd_async};

use super::types::{CommandResult, CommandDescriptor, CommandHandlerFn, AsyncCommandHandlerFn, parse_parameters, default_group, Lane};

/// Register all stream command handlers
pub fn register(
    handlers: &mut HashMap<String, CommandHandlerFn>,
    async_handlers: &mut HashMap<String, AsyncCommandHandlerFn>,
    descriptors: &mut Vec<CommandDescriptor>,
) {
    // Sync handlers
    handlers.insert("stream_info".to_string(), handle_stream_info as CommandHandlerFn);
    handlers.insert("stream_group_info".to_string(), handle_stream_group_info as CommandHandlerFn);
    handlers.insert("stream_consumer_info".to_string(), handle_stream_consumer_info as CommandHandlerFn);
    handlers.insert("stream_pending".to_string(), handle_stream_pending as CommandHandlerFn);

    // Async handlers
    async_handlers.insert("stream_info".to_string(), handle_stream_info_async as AsyncCommandHandlerFn);
    async_handlers.insert("STREAM_INFO".to_string(), handle_stream_info_async as AsyncCommandHandlerFn);
    async_handlers.insert("stream_group_info".to_string(), handle_stream_group_info_async as AsyncCommandHandlerFn);
    async_handlers.insert("STREAM_GROUP_INFO".to_string(), handle_stream_group_info_async as AsyncCommandHandlerFn);
    async_handlers.insert("stream_consumer_info".to_string(), handle_stream_consumer_info_async as AsyncCommandHandlerFn);
    async_handlers.insert("STREAM_CONSUMER_INFO".to_string(), handle_stream_consumer_info_async as AsyncCommandHandlerFn);
    async_handlers.insert("stream_pending".to_string(), handle_stream_pending_async as AsyncCommandHandlerFn);
    async_handlers.insert("STREAM_PENDING".to_string(), handle_stream_pending_async as AsyncCommandHandlerFn);

    // Descriptors
    descriptors.push(CommandDescriptor {
        name: "stream_info",
        category: "stream",
        description: "Get metadata about a ValKey stream (length, first/last entry, etc.)",
        params_schema: json!({"type": "object", "properties": {"stream": {"type": "string", "description": "Stream key to inspect", "default": "gnode:stream:{site_id}"}}}),
        returns_schema: json!({"type": "object", "description": "XINFO STREAM output"}),
        example: r#"{"cmd":"stream_info","params":{"stream":"mysite:gnode:unified:production"}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });
    descriptors.push(CommandDescriptor {
        name: "stream_group_info",
        category: "stream",
        description: "Get consumer group information for a stream",
        params_schema: json!({"type": "object", "properties": {"stream": {"type": "string", "default": "gnode:stream:{site_id}"}}}),
        returns_schema: json!({"type": "array", "description": "Array of consumer group info objects"}),
        example: r#"{"cmd":"stream_group_info","params":{}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });
    descriptors.push(CommandDescriptor {
        name: "stream_consumer_info",
        category: "stream",
        description: "Get consumer details within a consumer group",
        params_schema: json!({"type": "object", "properties": {"stream": {"type": "string", "default": "gnode:stream:{site_id}"}, "group": {"type": "string", "default": "default"}}}),
        returns_schema: json!({"type": "array", "description": "Array of consumer info objects"}),
        example: r#"{"cmd":"stream_consumer_info","params":{"group":"gnode-daemon"}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });
    descriptors.push(CommandDescriptor {
        name: "stream_pending",
        category: "stream",
        description: "Get pending (unacknowledged) messages in a consumer group",
        params_schema: json!({"type": "object", "properties": {"stream": {"type": "string", "default": "gnode:stream:{site_id}"}, "group": {"type": "string", "default": "default"}, "count": {"type": "integer", "default": 0, "description": "Number of detailed entries to return (0=summary only)"}}}),
        returns_schema: json!({"type": "object", "properties": {"summary": {"description": "Pending summary"}, "details": {"type": "array", "description": "Detailed pending entries (if count > 0)"}}}),
        example: r#"{"cmd":"stream_pending","params":{"count":10}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });
}

// =========================================================================
// Parameter structs
// =========================================================================

fn default_stream() -> String {
    "gnode:stream:default".to_string()
}

#[derive(Debug, Deserialize)]
struct StreamInfoParams {
    #[serde(default = "default_stream")]
    stream: String,
}

#[derive(Debug, Deserialize)]
struct StreamGroupInfoParams {
    #[serde(default = "default_stream")]
    stream: String,
}

#[derive(Debug, Deserialize)]
struct StreamConsumerInfoParams {
    #[serde(default = "default_stream")]
    stream: String,
    #[serde(default = "default_group")]
    group: String,
}

#[derive(Debug, Deserialize)]
struct StreamPendingParams {
    #[serde(default = "default_stream")]
    stream: String,
    #[serde(default = "default_group")]
    group: String,
    #[serde(default)]
    count: usize,
}

// =========================================================================
// Sync handlers
// =========================================================================

pub fn handle_stream_info(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    site_id: &str,
    debug_mode: bool
) -> CommandResult {
    if debug_mode {
        debug!("Handling stream_info command: {}", command.id);
    }

    let params = match parse_parameters::<StreamInfoParams>(command) {
        Ok(p) => p,
        Err(e) => return CommandResult::error(e),
    };

    let stream_key = if params.stream.is_empty() {
        format!("gnode:stream:{}", site_id)
    } else {
        params.stream
    };

    match json_redis_cmd("XINFO", vec!["STREAM", &stream_key], conn) {
        Ok(info) => CommandResult::success(info),
        Err(e) => CommandResult::error(format!("Error getting stream info: {}", e)),
    }
}

pub fn handle_stream_group_info(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    site_id: &str,
    debug_mode: bool
) -> CommandResult {
    if debug_mode {
        debug!("Handling stream_group_info command: {}", command.id);
    }

    let params = match parse_parameters::<StreamGroupInfoParams>(command) {
        Ok(p) => p,
        Err(e) => return CommandResult::error(e),
    };

    let stream_key = if params.stream.is_empty() {
        format!("gnode:stream:{}", site_id)
    } else {
        params.stream
    };

    match json_redis_cmd("XINFO", vec!["GROUPS", &stream_key], conn) {
        Ok(info) => CommandResult::success(info),
        Err(e) => CommandResult::error(format!("Error getting stream group info: {}", e)),
    }
}

pub fn handle_stream_consumer_info(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    site_id: &str,
    debug_mode: bool
) -> CommandResult {
    if debug_mode {
        debug!("Handling stream_consumer_info command: {}", command.id);
    }

    let params = match parse_parameters::<StreamConsumerInfoParams>(command) {
        Ok(p) => p,
        Err(e) => return CommandResult::error(e),
    };

    let stream_key = if params.stream.is_empty() {
        format!("gnode:stream:{}", site_id)
    } else {
        params.stream
    };

    match json_redis_cmd("XINFO", vec!["CONSUMERS", &stream_key, &params.group], conn) {
        Ok(info) => CommandResult::success(info),
        Err(e) => CommandResult::error(format!("Error getting stream consumer info: {}", e)),
    }
}

pub fn handle_stream_pending(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    site_id: &str,
    debug_mode: bool
) -> CommandResult {
    if debug_mode {
        debug!("Handling stream_pending command: {}", command.id);
    }

    let params = match parse_parameters::<StreamPendingParams>(command) {
        Ok(p) => p,
        Err(e) => return CommandResult::error(e),
    };

    let stream_key = if params.stream.is_empty() {
        format!("gnode:stream:{}", site_id)
    } else {
        params.stream
    };

    let summary = json_redis_cmd("XPENDING", vec![&stream_key, &params.group], conn);

    let details = if params.count > 0 {
        json_redis_cmd("XPENDING", vec![&stream_key, &params.group, "-", "+", &params.count.to_string()], conn).ok()
    } else {
        None
    };

    match summary {
        Ok(summary_value) => {
            let mut result = json!({
                "summary": summary_value
            });

            if let Some(details_value) = details {
                if let Value::Object(ref mut map) = result {
                    map.insert("details".to_string(), details_value);
                }
            }

            CommandResult::success(result)
        },
        Err(e) => CommandResult::error(format!("Error getting pending messages: {}", e)),
    }
}

// =========================================================================
// Async handlers
// =========================================================================

pub fn handle_stream_info_async<'a>(
    command: &'a Command,
    conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling async stream_info command: {}", command.id);
        }

        let stream = command.parameters.get("stream")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("gnode:stream:{}", site_id));

        match json_redis_cmd_async("XINFO", vec!["STREAM", &stream], conn).await {
            Ok(info) => CommandResult::success(json!({
                "stream": stream,
                "info": info,
                "async": true
            })),
            Err(e) => CommandResult::error(format!("Error getting stream info: {}", e)),
        }
    })
}

pub fn handle_stream_group_info_async<'a>(
    command: &'a Command,
    conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling async stream_group_info command: {}", command.id);
        }

        let stream = command.parameters.get("stream")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("gnode:stream:{}", site_id));

        match json_redis_cmd_async("XINFO", vec!["GROUPS", &stream], conn).await {
            Ok(info) => CommandResult::success(json!({
                "stream": stream,
                "groups": info,
                "async": true
            })),
            Err(e) => CommandResult::error(format!("Error getting stream group info: {}", e)),
        }
    })
}

pub fn handle_stream_consumer_info_async<'a>(
    command: &'a Command,
    conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling async stream_consumer_info command: {}", command.id);
        }

        let stream = command.parameters.get("stream")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("gnode:stream:{}", site_id));

        let group = command.parameters.get("group")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .unwrap_or_else(|| "gnode-daemon".to_string());

        match json_redis_cmd_async("XINFO", vec!["CONSUMERS", &stream, &group], conn).await {
            Ok(info) => CommandResult::success(json!({
                "stream": stream,
                "group": group,
                "consumers": info,
                "async": true
            })),
            Err(e) => CommandResult::error(format!("Error getting stream consumer info: {}", e)),
        }
    })
}

pub fn handle_stream_pending_async<'a>(
    command: &'a Command,
    conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling async stream_pending command: {}", command.id);
        }

        let stream = command.parameters.get("stream")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("gnode:stream:{}", site_id));

        let group = command.parameters.get("group")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .unwrap_or_else(|| "gnode-daemon".to_string());

        let count = command.parameters.get("count")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as usize;

        let summary = json_redis_cmd_async("XPENDING", vec![&stream, &group], conn).await;

        let details = if count > 0 {
            (json_redis_cmd_async("XPENDING", vec![&stream, &group, "-", "+", &count.to_string()], conn).await).ok()
        } else {
            None
        };

        match summary {
            Ok(summary_value) => {
                let mut result = json!({
                    "stream": stream,
                    "group": group,
                    "summary": summary_value,
                    "async": true
                });

                if let Some(details_value) = details {
                    if let serde_json::Value::Object(ref mut map) = result {
                        map.insert("details".to_string(), details_value);
                    }
                }

                CommandResult::success(result)
            },
            Err(e) => CommandResult::error(format!("Error getting pending messages: {}", e)),
        }
    })
}
