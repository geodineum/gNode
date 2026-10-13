//! `geometric_discover` and `geometric_discover_range`: one QUERY on the one topology
//! (`one_topology::discover`), answered with records that carry their colour. The two
//! names are kept for the wire; both take `{layer?, facets?, axes?, limit?}`.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use std::pin::Pin;
use std::future::Future;
use redis::Connection;
use redis::aio::MultiplexedConnection as AsyncConnection;
use log::debug;
use serde_json::json;
use crate::daemon::Command;
use crate::GeometricTopology;

use super::types::{CommandResult, CommandHandlerFn, AsyncCommandHandlerFn, CommandDescriptor, Lane};

pub fn register(
    handlers: &mut HashMap<String, CommandHandlerFn>,
    async_handlers: &mut HashMap<String, AsyncCommandHandlerFn>,
    descriptors: &mut Vec<CommandDescriptor>,
) {
    handlers.insert("geometric_discover".to_string(), handle_geometric_discover as CommandHandlerFn);
    handlers.insert("geometric_discover_range".to_string(), handle_geometric_discover as CommandHandlerFn);
    for name in ["discover", "DISCOVER", "geometric_discover", "geo_disc", "geometric_discover_range", "GEOMETRIC_DISCOVER_RANGE"] {
        async_handlers.insert(name.to_string(), handle_geometric_discover_async as AsyncCommandHandlerFn);
    }

    let params = json!({
        "type": "object",
        "properties": {
            "layer": {"type": "string", "description": "Shape word (site, app, daemon, component, node) or layer key; '3' = every service", "default": "3"},
            "facets": {"type": "object", "description": "axis → word, or list of words (any of); the shape's colour vocabulary", "additionalProperties": true},
            "axes": {"type": "object", "description": "axis → [lo, hi] as words of an ordered axis or fractions in [0, 1]", "additionalProperties": true},
            "limit": {"type": "integer", "default": 10, "description": "0 = unlimited"}
        }
    });
    let returns = json!({
        "type": "object",
        "properties": {
            "total_matches": {"type": "integer"},
            "results": {"type": "array", "items": {"type": "object", "properties": {
                "service_id": {"type": "string"}, "shape": {"type": "string"}, "environment": {"type": "string"},
                "colour": {"type": "object"}, "metadata": {"type": "object"}
            }}},
            "layer": {"type": "string"},
            "topology_key": {"type": "string"},
            "lookup": {"type": "string", "enum": ["index"]}
        }
    });
    descriptors.push(CommandDescriptor {
        name: "geometric_discover",
        category: "geometric",
        description: "One query on the one topology: entities of the layer whose colours hold every named facet and whose ordered axes fall in every named range; axes not named do not count. Records carry their colour",
        params_schema: params.clone(),
        returns_schema: returns.clone(),
        example: r#"{"cmd":"geometric_discover","params":{"layer":"daemon","facets":{"domain":"workflow"},"axes":{"contract_version":["v2","v5"]},"limit":5}}"#,
        async_capable: true,
        lane: Lane::Concurrent,
    });
    descriptors.push(CommandDescriptor {
        name: "geometric_discover_range",
        category: "geometric",
        description: "The same query as geometric_discover (kept as a name)",
        params_schema: params,
        returns_schema: returns,
        example: r#"{"cmd":"geometric_discover_range","params":{"axes":{"latency_class":["realtime","responsive"]}}}"#,
        async_capable: true,
        lane: Lane::Concurrent,
    });
}

pub fn handle_geometric_discover(command: &Command, conn: &mut Connection, _topology: &Arc<RwLock<GeometricTopology>>, site_id: &str, debug_mode: bool) -> CommandResult {
    if debug_mode { debug!("Handling {} command: {}", command.command, command.id); }
    super::one_topology::discover(conn, site_id, command.parameters.clone())
}

pub fn handle_geometric_discover_async<'a>(command: &'a Command, conn: &'a mut AsyncConnection, _topology: &'a Arc<RwLock<GeometricTopology>>, site_id: &'a str, debug_mode: bool) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode { debug!("Handling async {} command: {}", command.command, command.id); }
        super::one_topology::discover_async(conn, site_id, command.parameters.clone()).await
    })
}
