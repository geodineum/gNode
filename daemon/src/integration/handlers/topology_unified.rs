// Unified Topology Command Handlers (Stateless Q64.64 Architecture)
//
// Handles: topo_create, topo_register, topo_deregister, topo_add_edge,
//          topo_discover, topo_z_order, topo_z_range, topo_chain,
//          topo_stats, topo_list, topo_delete, topo_get_entity, topo_validate_edge
// Each command builds its Lua calls once (`plan_*`); the sync lane (Ordered commands,
// batches, pending) and the async lane run the same calls. A 3-D topology never
// writes the shared service snapshot.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use std::pin::Pin;
use std::future::Future;
use redis::Connection;
use redis::aio::MultiplexedConnection as AsyncConnection;
use serde::Deserialize;
use log::debug;
use serde_json::{json, Value};
use crate::daemon::Command;
use crate::GeometricTopology;

use super::types::{CommandResult, CommandDescriptor, CommandHandlerFn, AsyncCommandHandlerFn, Lane, POINT_FRAC_BITS};

/// Register all unified topology command handlers
pub fn register(
    handlers: &mut HashMap<String, CommandHandlerFn>,
    async_handlers: &mut HashMap<String, AsyncCommandHandlerFn>,
    descriptors: &mut Vec<CommandDescriptor>,
) {
    // Sync handlers (lower + UPPER case variants)
    handlers.insert("topo_create".to_string(), handle_topo_create as CommandHandlerFn);
    handlers.insert("TOPO_CREATE".to_string(), handle_topo_create as CommandHandlerFn);
    handlers.insert("topo_register".to_string(), handle_topo_register as CommandHandlerFn);
    handlers.insert("TOPO_REGISTER".to_string(), handle_topo_register as CommandHandlerFn);
    handlers.insert("topo_deregister".to_string(), handle_topo_deregister as CommandHandlerFn);
    handlers.insert("TOPO_DEREGISTER".to_string(), handle_topo_deregister as CommandHandlerFn);
    handlers.insert("topo_add_edge".to_string(), handle_topo_add_edge as CommandHandlerFn);
    handlers.insert("TOPO_ADD_EDGE".to_string(), handle_topo_add_edge as CommandHandlerFn);
    handlers.insert("topo_discover".to_string(), handle_topo_discover as CommandHandlerFn);
    handlers.insert("TOPO_DISCOVER".to_string(), handle_topo_discover as CommandHandlerFn);
    handlers.insert("topo_z_order".to_string(), handle_topo_z_order as CommandHandlerFn);
    handlers.insert("TOPO_Z_ORDER".to_string(), handle_topo_z_order as CommandHandlerFn);
    handlers.insert("topo_z_range".to_string(), handle_topo_z_range as CommandHandlerFn);
    handlers.insert("TOPO_Z_RANGE".to_string(), handle_topo_z_range as CommandHandlerFn);
    handlers.insert("topo_chain".to_string(), handle_topo_chain as CommandHandlerFn);
    handlers.insert("TOPO_CHAIN".to_string(), handle_topo_chain as CommandHandlerFn);
    handlers.insert("topo_stats".to_string(), handle_topo_stats as CommandHandlerFn);
    handlers.insert("TOPO_STATS".to_string(), handle_topo_stats as CommandHandlerFn);
    handlers.insert("topo_list".to_string(), handle_topo_list as CommandHandlerFn);
    handlers.insert("TOPO_LIST".to_string(), handle_topo_list as CommandHandlerFn);
    handlers.insert("topo_delete".to_string(), handle_topo_delete as CommandHandlerFn);
    handlers.insert("TOPO_DELETE".to_string(), handle_topo_delete as CommandHandlerFn);
    handlers.insert("topo_get_entity".to_string(), handle_topo_get_entity as CommandHandlerFn);
    handlers.insert("TOPO_GET_ENTITY".to_string(), handle_topo_get_entity as CommandHandlerFn);
    handlers.insert("topo_validate_edge".to_string(), handle_topo_validate_edge as CommandHandlerFn);
    handlers.insert("TOPO_VALIDATE_EDGE".to_string(), handle_topo_validate_edge as CommandHandlerFn);

    // Async handlers (lower + UPPER case variants)
    async_handlers.insert("topo_create".to_string(), handle_topo_create_async as AsyncCommandHandlerFn);
    async_handlers.insert("TOPO_CREATE".to_string(), handle_topo_create_async as AsyncCommandHandlerFn);
    async_handlers.insert("topo_register".to_string(), handle_topo_register_async as AsyncCommandHandlerFn);
    async_handlers.insert("TOPO_REGISTER".to_string(), handle_topo_register_async as AsyncCommandHandlerFn);
    async_handlers.insert("topo_deregister".to_string(), handle_topo_deregister_async as AsyncCommandHandlerFn);
    async_handlers.insert("TOPO_DEREGISTER".to_string(), handle_topo_deregister_async as AsyncCommandHandlerFn);
    async_handlers.insert("topo_add_edge".to_string(), handle_topo_add_edge_async as AsyncCommandHandlerFn);
    async_handlers.insert("TOPO_ADD_EDGE".to_string(), handle_topo_add_edge_async as AsyncCommandHandlerFn);
    async_handlers.insert("topo_discover".to_string(), handle_topo_discover_async as AsyncCommandHandlerFn);
    async_handlers.insert("TOPO_DISCOVER".to_string(), handle_topo_discover_async as AsyncCommandHandlerFn);
    async_handlers.insert("topo_z_order".to_string(), handle_topo_z_order_async as AsyncCommandHandlerFn);
    async_handlers.insert("TOPO_Z_ORDER".to_string(), handle_topo_z_order_async as AsyncCommandHandlerFn);
    async_handlers.insert("topo_z_range".to_string(), handle_topo_z_range_async as AsyncCommandHandlerFn);
    async_handlers.insert("TOPO_Z_RANGE".to_string(), handle_topo_z_range_async as AsyncCommandHandlerFn);
    async_handlers.insert("topo_chain".to_string(), handle_topo_chain_async as AsyncCommandHandlerFn);
    async_handlers.insert("TOPO_CHAIN".to_string(), handle_topo_chain_async as AsyncCommandHandlerFn);
    async_handlers.insert("topo_stats".to_string(), handle_topo_stats_async as AsyncCommandHandlerFn);
    async_handlers.insert("TOPO_STATS".to_string(), handle_topo_stats_async as AsyncCommandHandlerFn);
    async_handlers.insert("topo_list".to_string(), handle_topo_list_async as AsyncCommandHandlerFn);
    async_handlers.insert("TOPO_LIST".to_string(), handle_topo_list_async as AsyncCommandHandlerFn);
    async_handlers.insert("topo_delete".to_string(), handle_topo_delete_async as AsyncCommandHandlerFn);
    async_handlers.insert("TOPO_DELETE".to_string(), handle_topo_delete_async as AsyncCommandHandlerFn);
    async_handlers.insert("topo_get_entity".to_string(), handle_topo_get_entity_async as AsyncCommandHandlerFn);
    async_handlers.insert("TOPO_GET_ENTITY".to_string(), handle_topo_get_entity_async as AsyncCommandHandlerFn);
    async_handlers.insert("topo_validate_edge".to_string(), handle_topo_validate_edge_async as AsyncCommandHandlerFn);
    async_handlers.insert("TOPO_VALIDATE_EDGE".to_string(), handle_topo_validate_edge_async as AsyncCommandHandlerFn);

    // Command descriptors (canonical lowercase only). Replies are the named Lua
    // function's JSON, passed through; its fields are listed in COMMAND_SCHEMA.md.
    descriptors.push(CommandDescriptor {
        name: "topo_create",
        category: "topology",
        description: "Create a named 3-D (x, y, z) topology with an edge constraint",
        params_schema: json!({
            "type": "object",
            "properties": {
                "name": {"type": "string", "description": "Topology name"},
                "topology_key": {"type": "string", "description": "Explicit key; default {site_id}:<name>"},
                "constraint_type": {"type": "string", "enum": ["none", "z_monotonic", "bidirectional", "custom"], "default": "none", "description": "Edge constraint type"},
                "topology_type": {"type": "string", "default": "custom", "description": "Type label topo_list can filter on"},
                "description": {"type": "string", "description": "Optional description"},
                "axis_semantics": {"type": "object", "description": "Optional labels for x, y, z"}
            },
            "required": ["name"]
        }),
        returns_schema: lua_reply("GNODE_TOPO_CREATE"),
        example: r#"{"cmd":"topo_create","params":{"name":"pipeline","constraint_type":"z_monotonic"}}"#,
        async_capable: true,
        // Ordered: subsequent topo_register and topo_add_edge calls reference
        // this topology by name. If the create hasn't finished, those fail
        // with "topology not found".
        lane: Lane::Ordered,
    });

    descriptors.push(CommandDescriptor {
        name: "topo_register",
        category: "topology",
        description: "Register or move an entity at (x, y, z); optionally link it to existing entities that satisfy Z-monotonicity",
        params_schema: json!({
            "type": "object",
            "properties": {
                "topology_key": {"type": "string", "description": "Topology key"},
                "entity_id": {"type": "string", "description": "Unique entity identifier"},
                "x": {"type": "number", "default": 0.5, "description": "X coordinate (0.0-1.0)"},
                "y": {"type": "number", "default": 0.5, "description": "Y coordinate (0.0-1.0)"},
                "z": {"type": "number", "default": 0.5, "description": "Z coordinate (0.0-1.0, hierarchy/depth)"},
                "metadata": {"type": "object", "description": "Arbitrary entity metadata"},
                "edges_to": {"type": "array", "items": {"type": "string"}, "description": "Entities to link to; the reply's `edges` lists which were added and why others were skipped"},
                "edge_metadata": {"type": "object", "description": "Metadata for the edges_to edges"}
            },
            "required": ["topology_key", "entity_id"]
        }),
        returns_schema: lua_reply("GNODE_REGISTER_CAPABILITY_VECTOR"),
        example: r#"{"cmd":"topo_register","params":{"topology_key":"{mysite}:pipeline","entity_id":"auth-svc","x":0.2,"y":0.5,"z":0.1,"metadata":{"version":"2.0"}}}"#,
        async_capable: true,
        lane: Lane::Concurrent,
    });

    descriptors.push(CommandDescriptor {
        name: "topo_deregister",
        category: "topology",
        description: "Remove an entity and its edges from a topology",
        params_schema: json!({
            "type": "object",
            "properties": {
                "topology_key": {"type": "string", "description": "Topology key"},
                "entity_id": {"type": "string", "description": "Entity identifier to remove"}
            },
            "required": ["topology_key", "entity_id"]
        }),
        returns_schema: lua_reply("GNODE_DEREGISTER_CAPABILITY_VECTOR"),
        example: r#"{"cmd":"topo_deregister","params":{"topology_key":"{mysite}:pipeline","entity_id":"auth-svc"}}"#,
        async_capable: true,
        lane: Lane::Concurrent,
    });

    descriptors.push(CommandDescriptor {
        name: "topo_add_edge",
        category: "topology",
        description: "Add a directed edge between two existing entities, stamped with their Z delta",
        params_schema: json!({
            "type": "object",
            "properties": {
                "topology_key": {"type": "string", "description": "Topology key"},
                "from_id": {"type": "string", "description": "Source entity ID"},
                "to_id": {"type": "string", "description": "Target entity ID"},
                "edge_metadata": {"type": "object", "description": "Edge metadata"}
            },
            "required": ["topology_key", "from_id", "to_id"]
        }),
        returns_schema: lua_reply("GNODE_TOPO_ADD_EDGE"),
        example: r#"{"cmd":"topo_add_edge","params":{"topology_key":"{mysite}:pipeline","from_id":"api-gw","to_id":"auth-svc","edge_metadata":{"weight":1.0}}}"#,
        async_capable: true,
        lane: Lane::Concurrent,
    });

    descriptors.push(CommandDescriptor {
        name: "topo_discover",
        category: "topology",
        description: "Entities in the voxel cell at (x, y, z), or at an explicit bucket_key",
        params_schema: json!({
            "type": "object",
            "properties": {
                "topology_key": {"type": "string", "description": "Topology key"},
                "x": {"type": "number", "default": 0.5, "description": "X coordinate of the cell"},
                "y": {"type": "number", "default": 0.5, "description": "Y coordinate of the cell"},
                "z": {"type": "number", "default": 0.5, "description": "Z coordinate of the cell"},
                "bucket_key": {"type": "string", "description": "Cell key; overrides x, y, z"},
                "include_data": {"type": "boolean", "default": false, "description": "Return entity data, not only ids"}
            },
            "required": ["topology_key"]
        }),
        returns_schema: lua_reply("GNODE_TOPO_QUERY_VOXEL"),
        example: r#"{"cmd":"topo_discover","params":{"topology_key":"{mysite}:pipeline","x":0.2,"y":0.5,"z":0.1,"include_data":true}}"#,
        async_capable: true,
        lane: Lane::Concurrent,
    });

    descriptors.push(CommandDescriptor {
        name: "topo_z_order",
        category: "topology",
        description: "Entity ids ordered by Z (DAG load order)",
        params_schema: json!({
            "type": "object",
            "properties": {
                "topology_key": {"type": "string", "description": "Topology key"},
                "limit": {"type": "integer", "description": "Maximum ids; all when omitted"},
                "offset": {"type": "integer", "default": 0, "description": "Ids to skip"},
                "descending": {"type": "boolean", "default": false, "description": "High Z first"}
            },
            "required": ["topology_key"]
        }),
        returns_schema: lua_reply("GNODE_TOPO_Z_ORDER"),
        example: r#"{"cmd":"topo_z_order","params":{"topology_key":"{mysite}:pipeline","limit":20}}"#,
        async_capable: true,
        lane: Lane::Concurrent,
    });

    descriptors.push(CommandDescriptor {
        name: "topo_z_range",
        category: "topology",
        description: "Entities whose Z lies within a range",
        params_schema: json!({
            "type": "object",
            "properties": {
                "topology_key": {"type": "string", "description": "Topology key"},
                "z_min": {"type": "number", "description": "Lower Z bound; unbounded when omitted"},
                "z_max": {"type": "number", "description": "Upper Z bound; unbounded when omitted"},
                "include_data": {"type": "boolean", "default": false, "description": "Return entity data, not only ids"},
                "limit": {"type": "integer", "description": "Maximum entities"}
            },
            "required": ["topology_key"]
        }),
        returns_schema: lua_reply("GNODE_TOPO_QUERY_Z_RANGE"),
        example: r#"{"cmd":"topo_z_range","params":{"topology_key":"{mysite}:pipeline","z_min":0.0,"z_max":0.5}}"#,
        async_capable: true,
        lane: Lane::Concurrent,
    });

    descriptors.push(CommandDescriptor {
        name: "topo_chain",
        category: "topology",
        description: "Traverse the edge graph from a starting entity (BFS)",
        params_schema: json!({
            "type": "object",
            "properties": {
                "topology_key": {"type": "string", "description": "Topology key"},
                "entity_id": {"type": "string", "description": "Starting entity ID"},
                "direction": {"type": "string", "enum": ["outgoing", "incoming"], "default": "outgoing", "description": "Traversal direction"},
                "max_depth": {"type": "integer", "default": 100, "description": "Maximum traversal depth"}
            },
            "required": ["topology_key", "entity_id"]
        }),
        returns_schema: lua_reply("GNODE_TOPO_CHAIN"),
        example: r#"{"cmd":"topo_chain","params":{"topology_key":"{mysite}:pipeline","entity_id":"api-gw","direction":"outgoing","max_depth":5}}"#,
        async_capable: true,
        lane: Lane::Concurrent,
    });

    descriptors.push(CommandDescriptor {
        name: "topo_stats",
        category: "topology",
        description: "Statistics for a topology",
        params_schema: json!({
            "type": "object",
            "properties": {
                "topology_key": {"type": "string", "description": "Topology key"}
            },
            "required": ["topology_key"]
        }),
        returns_schema: lua_reply("GNODE_TOPO_STATS"),
        example: r#"{"cmd":"topo_stats","params":{"topology_key":"{mysite}:pipeline"}}"#,
        async_capable: true,
        lane: Lane::Concurrent,
    });

    descriptors.push(CommandDescriptor {
        name: "topo_list",
        category: "topology",
        description: "List the site's topologies",
        params_schema: json!({
            "type": "object",
            "properties": {
                "topology_type": {"type": "string", "description": "Only topologies with this type label"}
            }
        }),
        returns_schema: lua_reply("GNODE_TOPO_LIST"),
        example: r#"{"cmd":"topo_list","params":{}}"#,
        async_capable: true,
        lane: Lane::Concurrent,
    });

    descriptors.push(CommandDescriptor {
        name: "topo_delete",
        category: "topology",
        description: "Delete an entire topology and all its entities and edges",
        params_schema: json!({
            "type": "object",
            "properties": {
                "topology_key": {"type": "string", "description": "Topology key"},
                "confirm": {"type": "string", "enum": ["CONFIRM"], "description": "Must be the literal string CONFIRM"}
            },
            "required": ["topology_key", "confirm"]
        }),
        returns_schema: lua_reply("GNODE_TOPO_DELETE"),
        example: r#"{"cmd":"topo_delete","params":{"topology_key":"{mysite}:pipeline","confirm":"CONFIRM"}}"#,
        async_capable: true,
        // Ordered: destructive. Pending reads must observe post-delete state
        // (entities, edges, voxel buckets, z_order all gone) — a Concurrent-lane
        // read that started during the delete could observe inconsistent
        // intermediate state.
        lane: Lane::Ordered,
    });

    descriptors.push(CommandDescriptor {
        name: "topo_get_entity",
        category: "topology",
        description: "A single entity with its stored data and edges",
        params_schema: json!({
            "type": "object",
            "properties": {
                "topology_key": {"type": "string", "description": "Topology key"},
                "entity_id": {"type": "string", "description": "Entity identifier to retrieve"}
            },
            "required": ["topology_key", "entity_id"]
        }),
        returns_schema: lua_reply("GNODE_TOPO_GET_ENTITY"),
        example: r#"{"cmd":"topo_get_entity","params":{"topology_key":"{mysite}:pipeline","entity_id":"auth-svc"}}"#,
        async_capable: true,
        lane: Lane::Concurrent,
    });

    descriptors.push(CommandDescriptor {
        name: "topo_validate_edge",
        category: "topology",
        description: "Check if an edge would satisfy Z-monotonicity without creating it (Q64.64)",
        params_schema: json!({
            "type": "object",
            "properties": {
                "topology_key": {"type": "string", "description": "Topology key"},
                "from_id": {"type": "string", "description": "Source entity ID"},
                "to_id": {"type": "string", "description": "Target entity ID"}
            },
            "required": ["topology_key", "from_id", "to_id"]
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "valid": {"type": "boolean", "description": "Whether the edge satisfies constraints"},
                "reason": {"type": "string", "description": "Reason if invalid, null if valid"},
                "from_z": {"type": "number"},
                "to_z": {"type": "number"},
                "z_delta": {"type": "number", "description": "Z coordinate difference"}
            }
        }),
        example: r#"{"cmd":"topo_validate_edge","params":{"topology_key":"{mysite}:pipeline","from_id":"api-gw","to_id":"auth-svc"}}"#,
        async_capable: true,
        lane: Lane::Concurrent,
    });
}

fn lua_reply(function: &str) -> Value {
    json!({
        "type": "object",
        "description": format!("{} reply, passed through; fields in COMMAND_SCHEMA.md", function)
    })
}

#[derive(Debug, Deserialize, Default)]
#[cfg_attr(test, derive(serde::Serialize))]
pub struct TopoParams {
    #[serde(default)]
    pub topology_key: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub constraint_type: Option<String>,
    #[serde(default)]
    pub topology_type: Option<String>,
    #[serde(default)]
    pub entity_id: Option<String>,
    #[serde(default)]
    pub from_id: Option<String>,
    #[serde(default)]
    pub to_id: Option<String>,
    #[serde(default)]
    pub x: Option<f64>,
    #[serde(default)]
    pub y: Option<f64>,
    #[serde(default)]
    pub z: Option<f64>,
    #[serde(default)]
    pub bucket_key: Option<String>,
    #[serde(default)]
    pub metadata: Option<Value>,
    #[serde(default)]
    pub edge_metadata: Option<Value>,
    #[serde(default)]
    pub direction: Option<String>,
    #[serde(default)]
    pub max_depth: Option<i32>,
    #[serde(default)]
    pub limit: Option<i32>,
    #[serde(default)]
    pub offset: Option<i32>,
    #[serde(default)]
    pub include_data: Option<bool>,
    #[serde(default)]
    pub z_min: Option<f64>,
    #[serde(default)]
    pub z_max: Option<f64>,
    #[serde(default)]
    pub descending: Option<bool>,
    #[serde(default)]
    pub confirm: Option<String>,
    #[serde(default)]
    pub entity_ids: Option<Vec<String>>,
    #[serde(default)]
    pub edges_to: Option<Vec<String>>,
    #[serde(default)]
    pub axis_semantics: Option<Value>,
    #[serde(default)]
    pub description: Option<String>,
}

// =========================================================================
// Plans: the Lua calls each command makes, shared by both lanes
// =========================================================================

/// One Lua function call.
struct Fcall {
    function: &'static str,
    keys: Vec<String>,
    args: Vec<String>,
}

impl Fcall {
    fn new(function: &'static str, key: &str) -> Self {
        Self { function, keys: vec![key.to_string()], args: Vec::new() }
    }

    fn arg(mut self, value: impl ToString) -> Self {
        self.args.push(value.to_string());
        self
    }

    fn cmd(&self) -> redis::Cmd {
        let mut cmd = redis::cmd("FCALL");
        cmd.arg(self.function).arg(self.keys.len());
        for key in &self.keys {
            cmd.arg(key);
        }
        for arg in &self.args {
            cmd.arg(arg);
        }
        cmd
    }
}

fn parse_params(command: &Command) -> Result<TopoParams, CommandResult> {
    let raw = if command.parameters.is_null() { json!({}) } else { command.parameters.clone() };
    serde_json::from_value(raw).map_err(|e| CommandResult::error(format!("Invalid parameters: {}", e)))
}

fn required<'p>(value: &'p Option<String>, name: &str) -> Result<&'p str, CommandResult> {
    match value.as_deref() {
        Some(v) if !v.is_empty() => Ok(v),
        _ => Err(CommandResult::error(format!("Missing '{}' parameter", name))),
    }
}

fn flag(value: Option<bool>) -> &'static str {
    if value.unwrap_or(false) { "true" } else { "false" }
}

fn optional_count(value: Option<i32>) -> String {
    value.map(|v| v.to_string()).unwrap_or_default()
}

fn passthrough(command: &str, reply: redis::RedisResult<String>) -> CommandResult {
    match reply {
        Ok(json_str) => CommandResult::success_json(json_str),
        Err(e) => CommandResult::error(format!("{} failed: {}", command, e)),
    }
}

fn plan_create(p: &TopoParams, site_id: &str) -> Result<Fcall, CommandResult> {
    let name = required(&p.name, "name")?;
    let topology_key = p.topology_key.clone().unwrap_or_else(|| format!("{{{}}}:{}", site_id, name));
    let definition = json!({
        "name": name,
        "constraint_type": p.constraint_type.as_deref().unwrap_or("none"),
        "topology_type": p.topology_type.as_deref().unwrap_or("custom"),
        "description": p.description.as_deref().unwrap_or(""),
        "axis_semantics": p.axis_semantics
    });
    Ok(Fcall::new("GNODE_TOPO_CREATE", site_id).arg(topology_key).arg(definition))
}

/// The registration call and the entity's Z, which its edges are validated against.
fn plan_register(p: &TopoParams, _site_id: &str) -> Result<(Fcall, f64), CommandResult> {
    let topology_key = required(&p.topology_key, "topology_key")?;
    let entity_id = required(&p.entity_id, "entity_id")?;
    let (x, y, z) = (p.x.unwrap_or(0.5), p.y.unwrap_or(0.5), p.z.unwrap_or(0.5));
    let entity = json!({
        "position": { "x": x, "y": y, "z": z },
        "metadata": p.metadata.clone().unwrap_or(json!({}))
    });
    let call = Fcall::new("GNODE_REGISTER_CAPABILITY_VECTOR", topology_key)
        .arg(entity_id)
        .arg(entity)
        .arg(GeometricTopology::compute_3d_bucket_key(x, y, z, 10))
        .arg(GeometricTopology::compute_z_score(z))
        .arg("")  // args[5]: no snapshot — a 3-D topology is not the shared service topology
        .arg(-1)  // args[6]: no registration_order axis
        .arg(POINT_FRAC_BITS);  // args[7]
    Ok((call, z))
}

fn plan_deregister(p: &TopoParams, _site_id: &str) -> Result<Fcall, CommandResult> {
    let topology_key = required(&p.topology_key, "topology_key")?;
    let entity_id = required(&p.entity_id, "entity_id")?;
    // args[2] empty: removing a 3-D entity must not touch a same-named service in the shared snapshot
    Ok(Fcall::new("GNODE_DEREGISTER_CAPABILITY_VECTOR", topology_key).arg(entity_id).arg(""))
}

fn plan_discover(p: &TopoParams, _site_id: &str) -> Result<Fcall, CommandResult> {
    let topology_key = required(&p.topology_key, "topology_key")?;
    let bucket_key = match &p.bucket_key {
        Some(bk) => bk.clone(),
        None => GeometricTopology::compute_3d_bucket_key(p.x.unwrap_or(0.5), p.y.unwrap_or(0.5), p.z.unwrap_or(0.5), 10),
    };
    Ok(Fcall::new("GNODE_TOPO_QUERY_VOXEL", topology_key).arg(bucket_key).arg(flag(p.include_data)))
}

fn plan_z_order(p: &TopoParams, _site_id: &str) -> Result<Fcall, CommandResult> {
    let topology_key = required(&p.topology_key, "topology_key")?;
    Ok(Fcall::new("GNODE_TOPO_Z_ORDER", topology_key)
        .arg(optional_count(p.limit))
        .arg(p.offset.unwrap_or(0))
        .arg(flag(p.descending)))
}

fn plan_z_range(p: &TopoParams, _site_id: &str) -> Result<Fcall, CommandResult> {
    let topology_key = required(&p.topology_key, "topology_key")?;
    let bound = |z: Option<f64>, open: &str| {
        z.map(|z| GeometricTopology::compute_z_score(z).to_string()).unwrap_or_else(|| open.to_string())
    };
    Ok(Fcall::new("GNODE_TOPO_QUERY_Z_RANGE", topology_key)
        .arg(bound(p.z_min, "-inf"))
        .arg(bound(p.z_max, "+inf"))
        .arg(flag(p.include_data))
        .arg(optional_count(p.limit)))
}

fn plan_chain(p: &TopoParams, _site_id: &str) -> Result<Fcall, CommandResult> {
    let topology_key = required(&p.topology_key, "topology_key")?;
    let entity_id = required(&p.entity_id, "entity_id")?;
    let direction = p.direction.as_deref().unwrap_or("outgoing");
    if direction != "outgoing" && direction != "incoming" {
        return Err(CommandResult::error(format!("direction must be 'outgoing' or 'incoming', got '{}'", direction)));
    }
    Ok(Fcall::new("GNODE_TOPO_CHAIN", topology_key)
        .arg(entity_id)
        .arg(direction)
        .arg(p.max_depth.unwrap_or(100)))
}

fn plan_stats(p: &TopoParams, _site_id: &str) -> Result<Fcall, CommandResult> {
    Ok(Fcall::new("GNODE_TOPO_STATS", required(&p.topology_key, "topology_key")?))
}

fn plan_list(p: &TopoParams, site_id: &str) -> Result<Fcall, CommandResult> {
    Ok(Fcall::new("GNODE_TOPO_LIST", site_id).arg(p.topology_type.as_deref().unwrap_or("")))
}

fn plan_delete(p: &TopoParams, site_id: &str) -> Result<Fcall, CommandResult> {
    let topology_key = required(&p.topology_key, "topology_key")?;
    if p.confirm.as_deref() != Some("CONFIRM") {
        return Err(CommandResult::error("Must provide confirm: 'CONFIRM' to delete topology"));
    }
    Ok(Fcall::new("GNODE_TOPO_DELETE", site_id).arg(topology_key).arg("CONFIRM"))
}

fn plan_get_entity(p: &TopoParams, _site_id: &str) -> Result<Fcall, CommandResult> {
    let topology_key = required(&p.topology_key, "topology_key")?;
    let entity_id = required(&p.entity_id, "entity_id")?;
    Ok(Fcall::new("GNODE_TOPO_GET_ENTITY", topology_key).arg(entity_id))
}

/// Endpoints of an edge command: (topology_key, from_id, to_id).
fn edge_endpoints(p: &TopoParams) -> Result<(&str, &str, &str), CommandResult> {
    Ok((
        required(&p.topology_key, "topology_key")?,
        required(&p.from_id, "from_id")?,
        required(&p.to_id, "to_id")?,
    ))
}

fn plan_entity_lookup(topology_key: &str, ids: &[&str]) -> Fcall {
    Fcall::new("GNODE_TOPO_GET_ENTITIES", topology_key)
        .arg(serde_json::to_string(ids).unwrap_or_default())
        .arg("false")
}

/// Z of an entity in a GNODE_TOPO_GET_ENTITIES reply (`{ents: {id: {position: {z}}}}`).
fn entity_z(reply: &str, id: &str) -> Option<f64> {
    serde_json::from_str::<Value>(reply).ok()?
        .get("ents")?.get(id)?.get("position")?.get("z")?.as_f64()
}

fn plan_edge(topology_key: &str, from_id: &str, to_id: &str, from_z: f64, to_z: f64, metadata: &Option<Value>) -> Fcall {
    let edge = json!({
        "z_delta": GeometricTopology::compute_z_delta(from_z, to_z),
        "from_z": from_z,
        "to_z": to_z,
        "metadata": metadata
    });
    Fcall::new("GNODE_TOPO_ADD_EDGE", topology_key).arg(from_id).arg(to_id).arg(edge)
}

fn both_z(reply: &str, from_id: &str, to_id: &str) -> Result<(f64, f64), CommandResult> {
    match (entity_z(reply, from_id), entity_z(reply, to_id)) {
        (Some(from_z), Some(to_z)) => Ok((from_z, to_z)),
        (None, _) => Err(CommandResult::error(format!("Entity not found: {}", from_id))),
        (_, None) => Err(CommandResult::error(format!("Entity not found: {}", to_id))),
    }
}

fn validate_edge_reply(reply: &str, from_id: &str, to_id: &str) -> CommandResult {
    match both_z(reply, from_id, to_id) {
        Ok((from_z, to_z)) => {
            let (valid, reason) = GeometricTopology::validate_z_monotonic(from_z, to_z);
            CommandResult::success(json!({
                "valid": valid,
                "reason": reason,
                "from_z": from_z,
                "to_z": to_z,
                "z_delta": GeometricTopology::compute_z_delta(from_z, to_z)
            }))
        }
        Err(e) => e,
    }
}

/// The edge to `target` from an entity at `z`, or why there is none.
fn plan_edge_to(topology_key: &str, entity_id: &str, z: f64, target: &str, lookup: &str, metadata: &Option<Value>) -> Result<Fcall, String> {
    let target_z = entity_z(lookup, target).ok_or_else(|| "target not found".to_string())?;
    let (valid, reason) = GeometricTopology::validate_z_monotonic(z, target_z);
    if !valid {
        return Err(reason.unwrap_or_else(|| "violates Z-monotonicity".to_string()));
    }
    Ok(plan_edge(topology_key, entity_id, target, z, target_z, metadata))
}

/// The registration reply, with `edges` added when the caller asked for links.
fn register_reply(reply: String, added: Vec<String>, skipped: Vec<Value>) -> CommandResult {
    let mut v: Value = serde_json::from_str(&reply).unwrap_or_else(|_| json!({ "reply": reply }));
    v["edges"] = json!({ "added": added, "skipped": skipped });
    CommandResult::success(v)
}

// =========================================================================
// Handlers
// =========================================================================

macro_rules! single_call_handlers {
    ($sync_fn:ident, $async_fn:ident, $name:literal, $plan:ident) => {
        pub fn $sync_fn(
            command: &Command,
            conn: &mut Connection,
            _topology: &Arc<RwLock<GeometricTopology>>,
            site_id: &str,
            debug_mode: bool,
        ) -> CommandResult {
            if debug_mode { debug!("Handling {} command: {}", $name, command.id); }
            match parse_params(command).and_then(|p| $plan(&p, site_id)) {
                Ok(call) => passthrough($name, call.cmd().query(conn)),
                Err(e) => e,
            }
        }

        pub fn $async_fn<'a>(
            command: &'a Command,
            conn: &'a mut AsyncConnection,
            _topology: &'a Arc<RwLock<GeometricTopology>>,
            site_id: &'a str,
            debug_mode: bool,
        ) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
            Box::pin(async move {
                if debug_mode { debug!("Handling {} command: {}", $name, command.id); }
                let call = match parse_params(command).and_then(|p| $plan(&p, site_id)) {
                    Ok(call) => call,
                    Err(e) => return e,
                };
                passthrough($name, call.cmd().query_async(conn).await)
            })
        }
    };
}

single_call_handlers!(handle_topo_create, handle_topo_create_async, "topo_create", plan_create);
single_call_handlers!(handle_topo_deregister, handle_topo_deregister_async, "topo_deregister", plan_deregister);
single_call_handlers!(handle_topo_discover, handle_topo_discover_async, "topo_discover", plan_discover);
single_call_handlers!(handle_topo_z_order, handle_topo_z_order_async, "topo_z_order", plan_z_order);
single_call_handlers!(handle_topo_z_range, handle_topo_z_range_async, "topo_z_range", plan_z_range);
single_call_handlers!(handle_topo_chain, handle_topo_chain_async, "topo_chain", plan_chain);
single_call_handlers!(handle_topo_stats, handle_topo_stats_async, "topo_stats", plan_stats);
single_call_handlers!(handle_topo_list, handle_topo_list_async, "topo_list", plan_list);
single_call_handlers!(handle_topo_delete, handle_topo_delete_async, "topo_delete", plan_delete);
single_call_handlers!(handle_topo_get_entity, handle_topo_get_entity_async, "topo_get_entity", plan_get_entity);

/// Sync topo_register: register, then link `edges_to` targets that satisfy Z-monotonicity.
pub fn handle_topo_register(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    site_id: &str,
    debug_mode: bool,
) -> CommandResult {
    if debug_mode { debug!("Handling topo_register command: {}", command.id); }
    let p = match parse_params(command) { Ok(p) => p, Err(e) => return e };
    let (call, z) = match plan_register(&p, site_id) { Ok(plan) => plan, Err(e) => return e };
    let reply: String = match call.cmd().query(conn) {
        Ok(reply) => reply,
        Err(e) => return CommandResult::error(format!("topo_register failed: {}", e)),
    };
    let targets = p.edges_to.clone().unwrap_or_default();
    if targets.is_empty() {
        return CommandResult::success_json(reply);
    }

    let (topology_key, entity_id) = (p.topology_key.as_deref().unwrap_or_default(), p.entity_id.as_deref().unwrap_or_default());
    let ids: Vec<&str> = targets.iter().map(String::as_str).collect();
    let lookup: String = match plan_entity_lookup(topology_key, &ids).cmd().query(conn) {
        Ok(lookup) => lookup,
        Err(e) => {
            let skipped = targets.iter().map(|t| json!({ "to": t, "reason": e.to_string() })).collect();
            return register_reply(reply, Vec::new(), skipped);
        }
    };
    let (mut added, mut skipped) = (Vec::new(), Vec::new());
    for target in &targets {
        match plan_edge_to(topology_key, entity_id, z, target, &lookup, &p.edge_metadata) {
            Ok(edge) => match edge.cmd().query::<String>(conn) {
                Ok(_) => added.push(target.clone()),
                Err(e) => skipped.push(json!({ "to": target, "reason": e.to_string() })),
            },
            Err(reason) => skipped.push(json!({ "to": target, "reason": reason })),
        }
    }
    register_reply(reply, added, skipped)
}

/// Async topo_register: register, then link `edges_to` targets that satisfy Z-monotonicity.
pub fn handle_topo_register_async<'a>(
    command: &'a Command,
    conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode { debug!("Handling topo_register command: {}", command.id); }
        let p = match parse_params(command) { Ok(p) => p, Err(e) => return e };
        let (call, z) = match plan_register(&p, site_id) { Ok(plan) => plan, Err(e) => return e };
        let reply: String = match call.cmd().query_async(conn).await {
            Ok(reply) => reply,
            Err(e) => return CommandResult::error(format!("topo_register failed: {}", e)),
        };
        let targets = p.edges_to.clone().unwrap_or_default();
        if targets.is_empty() {
            return CommandResult::success_json(reply);
        }

        let (topology_key, entity_id) = (p.topology_key.as_deref().unwrap_or_default(), p.entity_id.as_deref().unwrap_or_default());
        let ids: Vec<&str> = targets.iter().map(String::as_str).collect();
        let lookup: String = match plan_entity_lookup(topology_key, &ids).cmd().query_async(conn).await {
            Ok(lookup) => lookup,
            Err(e) => {
                let skipped = targets.iter().map(|t| json!({ "to": t, "reason": e.to_string() })).collect();
                return register_reply(reply, Vec::new(), skipped);
            }
        };
        let (mut added, mut skipped) = (Vec::new(), Vec::new());
        for target in &targets {
            match plan_edge_to(topology_key, entity_id, z, target, &lookup, &p.edge_metadata) {
                Ok(edge) => match edge.cmd().query_async::<String>(conn).await {
                    Ok(_) => added.push(target.clone()),
                    Err(e) => skipped.push(json!({ "to": target, "reason": e.to_string() })),
                },
                Err(reason) => skipped.push(json!({ "to": target, "reason": reason })),
            }
        }
        register_reply(reply, added, skipped)
    })
}

/// Sync topo_add_edge: both entities must exist; the edge carries their Z delta.
pub fn handle_topo_add_edge(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    _site_id: &str,
    debug_mode: bool,
) -> CommandResult {
    if debug_mode { debug!("Handling topo_add_edge command: {}", command.id); }
    let p = match parse_params(command) { Ok(p) => p, Err(e) => return e };
    let (topology_key, from_id, to_id) = match edge_endpoints(&p) { Ok(e) => e, Err(e) => return e };
    let lookup: String = match plan_entity_lookup(topology_key, &[from_id, to_id]).cmd().query(conn) {
        Ok(lookup) => lookup,
        Err(e) => return CommandResult::error(format!("Failed to get entities: {}", e)),
    };
    let (from_z, to_z) = match both_z(&lookup, from_id, to_id) { Ok(z) => z, Err(e) => return e };
    passthrough("topo_add_edge", plan_edge(topology_key, from_id, to_id, from_z, to_z, &p.edge_metadata).cmd().query(conn))
}

/// Async topo_add_edge: both entities must exist; the edge carries their Z delta.
pub fn handle_topo_add_edge_async<'a>(
    command: &'a Command,
    conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    _site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode { debug!("Handling topo_add_edge command: {}", command.id); }
        let p = match parse_params(command) { Ok(p) => p, Err(e) => return e };
        let (topology_key, from_id, to_id) = match edge_endpoints(&p) { Ok(e) => e, Err(e) => return e };
        let lookup: String = match plan_entity_lookup(topology_key, &[from_id, to_id]).cmd().query_async(conn).await {
            Ok(lookup) => lookup,
            Err(e) => return CommandResult::error(format!("Failed to get entities: {}", e)),
        };
        let (from_z, to_z) = match both_z(&lookup, from_id, to_id) { Ok(z) => z, Err(e) => return e };
        let edge = plan_edge(topology_key, from_id, to_id, from_z, to_z, &p.edge_metadata);
        passthrough("topo_add_edge", edge.cmd().query_async(conn).await)
    })
}

/// Sync topo_validate_edge
pub fn handle_topo_validate_edge(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    _site_id: &str,
    debug_mode: bool,
) -> CommandResult {
    if debug_mode { debug!("Handling topo_validate_edge command: {}", command.id); }
    let p = match parse_params(command) { Ok(p) => p, Err(e) => return e };
    let (topology_key, from_id, to_id) = match edge_endpoints(&p) { Ok(e) => e, Err(e) => return e };
    match plan_entity_lookup(topology_key, &[from_id, to_id]).cmd().query::<String>(conn) {
        Ok(lookup) => validate_edge_reply(&lookup, from_id, to_id),
        Err(e) => CommandResult::error(format!("Failed to get entities: {}", e)),
    }
}

/// Async topo_validate_edge
pub fn handle_topo_validate_edge_async<'a>(
    command: &'a Command,
    conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    _site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode { debug!("Handling topo_validate_edge command: {}", command.id); }
        let p = match parse_params(command) { Ok(p) => p, Err(e) => return e };
        let (topology_key, from_id, to_id) = match edge_endpoints(&p) { Ok(e) => e, Err(e) => return e };
        match plan_entity_lookup(topology_key, &[from_id, to_id]).cmd().query_async::<String>(conn).await {
            Ok(lookup) => validate_edge_reply(&lookup, from_id, to_id),
            Err(e) => CommandResult::error(format!("Failed to get entities: {}", e)),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::types::assert_descriptor_fields;

    fn params(v: Value) -> TopoParams {
        serde_json::from_value(v).unwrap()
    }

    #[test]
    fn descriptors_name_only_fields_the_shared_parser_reads() {
        let (mut handlers, mut async_handlers, mut descriptors) = (HashMap::new(), HashMap::new(), Vec::new());
        register(&mut handlers, &mut async_handlers, &mut descriptors);
        for d in &descriptors {
            assert_descriptor_fields::<TopoParams>(&descriptors, d.name, false);
        }
    }

    #[test]
    fn delete_requires_confirm_on_every_lane() {
        assert!(plan_delete(&params(json!({"topology_key": "{s}:t"})), "s").is_err());
        let call = plan_delete(&params(json!({"topology_key": "{s}:t", "confirm": "CONFIRM"})), "s").ok().unwrap();
        assert_eq!((call.function, call.keys.clone(), call.args.clone()), ("GNODE_TOPO_DELETE", vec!["s".to_string()], vec!["{s}:t".to_string(), "CONFIRM".to_string()]));
    }

    #[test]
    fn z_order_and_z_range_send_the_arguments_the_lua_reads() {
        let order = plan_z_order(&params(json!({"topology_key": "k", "limit": 5, "descending": true})), "s").ok().unwrap();
        assert_eq!(order.args, vec!["5", "0", "true"]);
        let range = plan_z_range(&params(json!({"topology_key": "k"})), "s").ok().unwrap();
        assert_eq!(range.function, "GNODE_TOPO_QUERY_Z_RANGE");
        assert_eq!(range.args, vec!["-inf", "+inf", "false", ""]);
    }

    #[test]
    fn a_3d_topology_never_writes_the_shared_service_snapshot() {
        let (register, _) = plan_register(&params(json!({"topology_key": "{s}:t", "entity_id": "auth"})), "s").ok().unwrap();
        assert_eq!(register.args[4], "");
        let deregister = plan_deregister(&params(json!({"topology_key": "{s}:t", "entity_id": "auth"})), "s").ok().unwrap();
        assert_eq!(deregister.args, vec!["auth", ""]);
    }

    #[test]
    fn edge_commands_read_z_from_the_ents_reply() {
        let reply = json!({"ents": {"a": {"position": {"z": 0.8}}, "b": {"position": {"z": 0.2}}}}).to_string();
        assert_eq!(entity_z(&reply, "a"), Some(0.8));
        let v = validate_edge_reply(&reply, "a", "b").result.unwrap();
        assert_eq!(v["from_z"], 0.8);
        assert!(validate_edge_reply(&reply, "a", "missing").error.unwrap().contains("missing"));
    }
}
