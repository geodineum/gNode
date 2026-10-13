//! `registerService`, `deregisterService`, `discover_with_endpoints`: the service commands,
//! answered on the one topology (`one_topology.rs`). Registration and deregistration are
//! declarations by name; endpoint discovery is one QUERY whose ids are enriched with their
//! endpoints from gNode-BROKER's registry.

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

use super::types::{CommandResult, CommandHandlerFn, AsyncCommandHandlerFn, CommandDescriptor, Lane};

pub fn register(
    handlers: &mut HashMap<String, CommandHandlerFn>,
    async_handlers: &mut HashMap<String, AsyncCommandHandlerFn>,
    descriptors: &mut Vec<CommandDescriptor>,
) {
    handlers.insert("registerService".to_string(), handle_register_service as CommandHandlerFn);
    handlers.insert("deregisterService".to_string(), handle_deregister_service as CommandHandlerFn);
    handlers.insert("discover_with_endpoints".to_string(), handle_discover_with_endpoints as CommandHandlerFn);
    handlers.insert("DISCOVER_WITH_ENDPOINTS".to_string(), handle_discover_with_endpoints as CommandHandlerFn);
    handlers.insert("service_endpoints".to_string(), handle_discover_with_endpoints as CommandHandlerFn);

    async_handlers.insert("register_service".to_string(), handle_register_service_async as AsyncCommandHandlerFn);
    async_handlers.insert("registerService".to_string(), handle_register_service_async as AsyncCommandHandlerFn);
    async_handlers.insert("REGISTER_SERVICE".to_string(), handle_register_service_async as AsyncCommandHandlerFn);
    async_handlers.insert("deregister_service".to_string(), handle_deregister_service_async as AsyncCommandHandlerFn);
    async_handlers.insert("deregisterService".to_string(), handle_deregister_service_async as AsyncCommandHandlerFn);
    async_handlers.insert("DEREGISTER_SERVICE".to_string(), handle_deregister_service_async as AsyncCommandHandlerFn);
    async_handlers.insert("discover_with_endpoints".to_string(), handle_discover_with_endpoints_async as AsyncCommandHandlerFn);
    async_handlers.insert("DISCOVER_WITH_ENDPOINTS".to_string(), handle_discover_with_endpoints_async as AsyncCommandHandlerFn);
    async_handlers.insert("service_endpoints".to_string(), handle_discover_with_endpoints_async as AsyncCommandHandlerFn);

    descriptors.push(CommandDescriptor {
        name: "registerService",
        category: "service",
        description: "Declare a service on the one topology by name: its shape, its axes as words of the shape's vocabulary, its labels and, with `on`, its placement. Numbers for axis words are refused; unknown words are refused by name",
        params_schema: json!({
            "type": "object",
            "properties": {
                "id": {"type": "string", "description": "The entity id (a site's id is its site id)"},
                "shape": {"type": "string", "description": "site | app | daemon | component | node, or a shape key such as 3.3", "default": "app"},
                "environment": {"type": "string", "enum": ["global", "testing", "staging", "acceptance", "production"], "default": "production"},
                "axes": {"type": "object", "description": "axis name → word, list of words (many-valued colours), or fraction in [0, 1] (freeform axes)", "additionalProperties": true},
                "metadata": {"type": "object"},
                "on": {"type": "string", "description": "The node this instance runs on: makes a 4.2 placement"},
                "capacity_weight": {"type": "integer", "minimum": 0, "default": 1, "description": "Ring weight of the placement; 0 = standby"},
                "heartbeat": {"type": "string", "description": "The component name in the heartbeat key when it differs from id"},
                "holds": {"type": "array", "items": {"type": "string"}, "description": "REACH labels the entity carries"},
                "requires": {"type": "array", "items": {"type": "string"}, "description": "Labels a caller must hold to reach it"},
                "peers": {"type": "array", "items": {"type": "string"}},
                "constellation": {"type": "string", "description": "The constellation to register in; must be served by this daemon"}
            },
            "required": ["id"]
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "status": {"type": "string"},
                "service_id": {"type": "string"},
                "shape": {"type": "string"},
                "topology_key": {"type": "string"},
                "placement": {"type": "string"},
                "updated": {"type": "boolean"}
            }
        }),
        example: r#"{"cmd":"registerService","params":{"id":"gflow","shape":"daemon","axes":{"protocol":["gnode_stream"],"domain":["workflow"]},"on":"aesir-solutions","holds":["internal"]}}"#,
        async_capable: true,
        lane: Lane::Concurrent,
    });

    descriptors.push(CommandDescriptor {
        name: "deregisterService",
        category: "service",
        description: "Remove an entity from the one topology with its indexes, edges and placements",
        params_schema: json!({
            "type": "object",
            "properties": {"service_id": {"type": "string", "description": "The entity id (alias: id)"}},
            "required": ["service_id"]
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {"status": {"type": "string", "enum": ["deregistered", "not_found"]}, "service_id": {"type": "string"}}
        }),
        example: r#"{"cmd":"deregisterService","params":{"service_id":"gflow"}}"#,
        async_capable: true,
        lane: Lane::Concurrent,
    });

    descriptors.push(CommandDescriptor {
        name: "discover_with_endpoints",
        category: "service",
        description: "One query on the one topology (layer, facets, axis ranges), each matching service enriched with its endpoints from GNODE_ENDPOINT_LIST (gNode-BROKER)",
        params_schema: json!({
            "type": "object",
            "properties": {
                "layer": {"type": "string", "description": "Shape word or layer key; '3' = every service", "default": "3"},
                "facets": {"type": "object", "description": "axis → word or words (any of)"},
                "axes": {"type": "object", "description": "axis → [lo, hi] as words or fractions"},
                "endpoint_registry": {"type": "string", "description": "Override the endpoint registry key (default {site}:gnode:endpoints)"},
                "limit": {"type": "integer", "default": 10, "description": "0 = unlimited"}
            }
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "count": {"type": "integer"},
                "services": {"type": "array", "items": {"type": "object", "properties": {"service_id": {"type": "string"}, "endpoints": {"type": "array"}}}},
                "endpoints_error": {"type": "string"}
            }
        }),
        example: r#"{"cmd":"discover_with_endpoints","params":{"facets":{"domain":"workflow"},"limit":5}}"#,
        async_capable: true,
        lane: Lane::Concurrent,
    });
}

#[derive(Debug, Deserialize)]
struct DiscoverWithEndpointsParams {
    #[serde(default)]
    endpoint_registry: Option<String>,
}

fn endpoints_from_reply(reply: redis::RedisResult<String>) -> Result<Value, String> {
    match reply {
        Ok(json) => serde_json::from_str::<Value>(&json)
            .map(|v| v.get("endpoints").cloned().unwrap_or(v))
            .map_err(|e| format!("endpoint registry reply is not JSON: {}", e)),
        Err(e) => Err(format!("{}", e)),
    }
}

fn endpoints_reply(services: Vec<(String, Result<Value, String>)>) -> CommandResult {
    let mut error: Option<String> = None;
    let list: Vec<Value> = services.into_iter().map(|(id, eps)| match eps {
        Ok(v) => json!({"service_id": id, "endpoints": v}),
        Err(e) => { error.get_or_insert(e); json!({"service_id": id, "endpoints": []}) }
    }).collect();
    let mut out = json!({"count": list.len(), "services": list});
    if let Some(e) = error { out["endpoints_error"] = json!(e); }
    CommandResult::success(out)
}

fn endpoint_registry(params: &Value, site_id: &str) -> String {
    serde_json::from_value::<DiscoverWithEndpointsParams>(params.clone()).ok()
        .and_then(|p| p.endpoint_registry)
        .unwrap_or_else(|| format!("{{{}}}:gnode:endpoints", site_id))
}

pub fn handle_register_service(command: &Command, conn: &mut Connection, _topology: &Arc<RwLock<GeometricTopology>>, site_id: &str, debug_mode: bool) -> CommandResult {
    if debug_mode { debug!("Handling registerService command: {}", command.id); }
    super::one_topology::register(conn, site_id, command.parameters.clone())
}

pub fn handle_deregister_service(command: &Command, conn: &mut Connection, _topology: &Arc<RwLock<GeometricTopology>>, site_id: &str, debug_mode: bool) -> CommandResult {
    if debug_mode { debug!("Handling deregisterService command: {}", command.id); }
    super::one_topology::deregister(conn, site_id, command.parameters.clone())
}

pub fn handle_discover_with_endpoints(command: &Command, conn: &mut Connection, _topology: &Arc<RwLock<GeometricTopology>>, site_id: &str, debug_mode: bool) -> CommandResult {
    if debug_mode { debug!("Handling discover_with_endpoints command: {}", command.id); }
    let ids = match super::one_topology::discover_ids(conn, site_id, &command.parameters) {
        Ok(ids) => ids,
        Err(e) => return CommandResult::error(e),
    };
    let registry = endpoint_registry(&command.parameters, site_id);
    let mut services = Vec::with_capacity(ids.len());
    for id in ids {
        let reply = redis::cmd("FCALL").arg("GNODE_ENDPOINT_LIST").arg(1).arg(&registry).arg(&id).query::<String>(conn);
        services.push((id, endpoints_from_reply(reply)));
    }
    endpoints_reply(services)
}

pub fn handle_register_service_async<'a>(command: &'a Command, conn: &'a mut AsyncConnection, _topology: &'a Arc<RwLock<GeometricTopology>>, site_id: &'a str, debug_mode: bool) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode { debug!("Handling async registerService command: {}", command.id); }
        super::one_topology::register_async(conn, site_id, command.parameters.clone()).await
    })
}

pub fn handle_deregister_service_async<'a>(command: &'a Command, conn: &'a mut AsyncConnection, _topology: &'a Arc<RwLock<GeometricTopology>>, site_id: &'a str, debug_mode: bool) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode { debug!("Handling async deregisterService command: {}", command.id); }
        super::one_topology::deregister_async(conn, site_id, command.parameters.clone()).await
    })
}

pub fn handle_discover_with_endpoints_async<'a>(command: &'a Command, conn: &'a mut AsyncConnection, _topology: &'a Arc<RwLock<GeometricTopology>>, site_id: &'a str, debug_mode: bool) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode { debug!("Handling async discover_with_endpoints command: {}", command.id); }
        let ids = match super::one_topology::discover_ids_async(conn, site_id, &command.parameters).await {
            Ok(ids) => ids,
            Err(e) => return CommandResult::error(e),
        };
        let registry = endpoint_registry(&command.parameters, site_id);
        let mut services = Vec::with_capacity(ids.len());
        for id in ids {
            let reply = redis::cmd("FCALL").arg("GNODE_ENDPOINT_LIST").arg(1).arg(&registry).arg(&id).query_async::<String>(conn).await;
            services.push((id, endpoints_from_reply(reply)));
        }
        endpoints_reply(services)
    })
}
