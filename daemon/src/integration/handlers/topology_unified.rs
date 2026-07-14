// Unified Topology Command Handlers (Stateless Q64.64 Architecture)
//
// Handles: topo_create, topo_register, topo_deregister, topo_add_edge,
//          topo_discover, topo_z_order, topo_z_range, topo_chain,
//          topo_stats, topo_list, topo_delete, topo_get_entity, topo_validate_edge
// These provide unified topology CRUD and query operations via ValKey functions.

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

use super::types::{CommandResult, CommandDescriptor, CommandHandlerFn, AsyncCommandHandlerFn, Lane};

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

    // Command descriptors (canonical lowercase only)
    descriptors.push(CommandDescriptor {
        name: "topo_create",
        category: "topology",
        description: "Create a new topology with specified constraint type",
        params_schema: json!({
            "type": "object",
            "properties": {
                "name": {"type": "string", "description": "Topology name"},
                "constraint_type": {"type": "string", "enum": ["none", "z_monotonic", "bidirectional"], "default": "none", "description": "Edge constraint type"},
                "dimensions": {"type": "integer", "minimum": 1, "description": "Number of dimensions in this custom topology. No default — declare your topology's dim count explicitly. Service tier uses 30; tool tier 16; constellation/galaxy 20. Custom topologies can use any count >= 1."},
                "axis_semantics": {"type": "object", "description": "Optional axis labels for x, y, z"}
            },
            "required": ["name"]
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "ok": {"type": "boolean"},
                "topology_key": {"type": "string", "description": "Created topology key"}
            }
        }),
        example: r#"{"cmd":"topo_create","params":{"name":"services","constraint_type":"z_monotonic","dimensions":30}}"#,
        async_capable: true,
        // Ordered: subsequent topo_register and topo_add_edge calls reference
        // this topology by name. If the create hasn't finished, those fail
        // with "topology not found".
        lane: Lane::Ordered,
    });

    descriptors.push(CommandDescriptor {
        name: "topo_register",
        category: "topology",
        description: "Register an entity in a topology with Q64.64 bucket key computation",
        params_schema: json!({
            "type": "object",
            "properties": {
                "topology": {"type": "string", "description": "Topology key or name"},
                "entity_id": {"type": "string", "description": "Unique entity identifier"},
                "x": {"type": "number", "description": "X coordinate (0.0-1.0)"},
                "y": {"type": "number", "description": "Y coordinate (0.0-1.0)"},
                "z": {"type": "number", "description": "Z coordinate (0.0-1.0, hierarchy/depth)"},
                "metadata": {"type": "object", "description": "Arbitrary entity metadata"}
            },
            "required": ["topology", "entity_id"]
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "ok": {"type": "boolean"},
                "entity_id": {"type": "string"},
                "bucket_key": {"type": "string", "description": "Computed Q64.64 voxel bucket key"}
            }
        }),
        example: r#"{"cmd":"topo_register","params":{"topology":"services","entity_id":"auth-svc","x":0.2,"y":0.5,"z":0.1,"metadata":{"version":"2.0"}}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });

    descriptors.push(CommandDescriptor {
        name: "topo_deregister",
        category: "topology",
        description: "Remove an entity from a topology",
        params_schema: json!({
            "type": "object",
            "properties": {
                "topology": {"type": "string", "description": "Topology key or name"},
                "entity_id": {"type": "string", "description": "Entity identifier to remove"}
            },
            "required": ["topology", "entity_id"]
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "ok": {"type": "boolean"},
                "entity_id": {"type": "string", "description": "Deregistered entity ID"}
            }
        }),
        example: r#"{"cmd":"topo_deregister","params":{"topology":"services","entity_id":"auth-svc"}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });

    descriptors.push(CommandDescriptor {
        name: "topo_add_edge",
        category: "topology",
        description: "Add a directed edge between two entities in a topology",
        params_schema: json!({
            "type": "object",
            "properties": {
                "topology": {"type": "string", "description": "Topology key or name"},
                "from": {"type": "string", "description": "Source entity ID"},
                "to": {"type": "string", "description": "Target entity ID"},
                "metadata": {"type": "object", "description": "Edge metadata"}
            },
            "required": ["topology", "from", "to"]
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "ok": {"type": "boolean"},
                "from": {"type": "string"},
                "to": {"type": "string"},
                "z_delta": {"type": "number", "description": "Z coordinate difference between entities"}
            }
        }),
        example: r#"{"cmd":"topo_add_edge","params":{"topology":"services","from":"api-gw","to":"auth-svc","metadata":{"weight":1.0}}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });

    descriptors.push(CommandDescriptor {
        name: "topo_discover",
        category: "topology",
        description: "Discover entities near a point in a topology via spatial hash",
        params_schema: json!({
            "type": "object",
            "properties": {
                "topology": {"type": "string", "description": "Topology key or name"},
                "x": {"type": "number", "description": "X coordinate to search near"},
                "y": {"type": "number", "description": "Y coordinate to search near"},
                "z": {"type": "number", "description": "Z coordinate to search near"},
                "limit": {"type": "integer", "default": 10, "description": "Maximum number of results"}
            },
            "required": ["topology"]
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "ok": {"type": "boolean"},
                "entities": {"type": "array", "items": {"type": "object"}, "description": "Nearby entities with distances"}
            }
        }),
        example: r#"{"cmd":"topo_discover","params":{"topology":"services","x":0.2,"y":0.5,"z":0.1,"limit":5}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });

    descriptors.push(CommandDescriptor {
        name: "topo_z_order",
        category: "topology",
        description: "Get entities ordered by Z coordinate (DAG load order)",
        params_schema: json!({
            "type": "object",
            "properties": {
                "topology": {"type": "string", "description": "Topology key or name"},
                "direction": {"type": "string", "enum": ["asc", "desc"], "default": "asc", "description": "Sort direction"}
            },
            "required": ["topology"]
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "ok": {"type": "boolean"},
                "entities": {"type": "array", "items": {"type": "object"}, "description": "Entities sorted by Z coordinate"}
            }
        }),
        example: r#"{"cmd":"topo_z_order","params":{"topology":"services","direction":"asc"}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });

    descriptors.push(CommandDescriptor {
        name: "topo_z_range",
        category: "topology",
        description: "Get entities within a Z coordinate range",
        params_schema: json!({
            "type": "object",
            "properties": {
                "topology": {"type": "string", "description": "Topology key or name"},
                "min_z": {"type": "number", "description": "Minimum Z coordinate"},
                "max_z": {"type": "number", "description": "Maximum Z coordinate"}
            },
            "required": ["topology", "min_z", "max_z"]
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "ok": {"type": "boolean"},
                "entities": {"type": "array", "items": {"type": "object"}, "description": "Entities within the Z range"}
            }
        }),
        example: r#"{"cmd":"topo_z_range","params":{"topology":"services","min_z":0.0,"max_z":0.5}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });

    descriptors.push(CommandDescriptor {
        name: "topo_chain",
        category: "topology",
        description: "Traverse the edge graph from a starting entity (BFS)",
        params_schema: json!({
            "type": "object",
            "properties": {
                "topology": {"type": "string", "description": "Topology key or name"},
                "entity_id": {"type": "string", "description": "Starting entity ID"},
                "direction": {"type": "string", "enum": ["outgoing", "incoming"], "default": "outgoing", "description": "Traversal direction"},
                "max_depth": {"type": "integer", "default": 10, "description": "Maximum traversal depth"}
            },
            "required": ["topology", "entity_id"]
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "ok": {"type": "boolean"},
                "chain": {"type": "array", "description": "Traversal chain of entity IDs"},
                "by_depth": {"type": "object", "description": "Entities grouped by depth level"},
                "max_depth": {"type": "integer", "description": "Maximum depth reached"}
            }
        }),
        example: r#"{"cmd":"topo_chain","params":{"topology":"services","entity_id":"api-gw","direction":"outgoing","max_depth":5}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });

    descriptors.push(CommandDescriptor {
        name: "topo_stats",
        category: "topology",
        description: "Get statistics for a topology",
        params_schema: json!({
            "type": "object",
            "properties": {
                "topology": {"type": "string", "description": "Topology key or name"}
            },
            "required": ["topology"]
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "ok": {"type": "boolean"},
                "entity_count": {"type": "integer", "description": "Number of entities"},
                "edge_count": {"type": "integer", "description": "Number of edges"},
                "dimensions": {"type": "integer", "description": "Number of dimensions"}
            }
        }),
        example: r#"{"cmd":"topo_stats","params":{"topology":"services"}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });

    descriptors.push(CommandDescriptor {
        name: "topo_list",
        category: "topology",
        description: "List all topologies for the current site",
        params_schema: json!({
            "type": "object",
            "properties": {}
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "ok": {"type": "boolean"},
                "topologies": {"type": "array", "items": {"type": "object"}, "description": "Array of topology names with metadata"}
            }
        }),
        example: r#"{"cmd":"topo_list","params":{}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });

    descriptors.push(CommandDescriptor {
        name: "topo_delete",
        category: "topology",
        description: "Delete an entire topology and all its entities and edges",
        params_schema: json!({
            "type": "object",
            "properties": {
                "topology": {"type": "string", "description": "Topology key or name"}
            },
            "required": ["topology"]
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "ok": {"type": "boolean"},
                "deleted": {"type": "boolean", "description": "Whether the topology was deleted"}
            }
        }),
        example: r#"{"cmd":"topo_delete","params":{"topology":"services"}}"#,
        async_capable: true,
        // Ordered: destructive. Pending reads must observe post-delete state
        // (entities, edges, voxel buckets, z_order all gone) — a Fast-lane
        // read that started during the delete could observe inconsistent
        // intermediate state.
        lane: Lane::Ordered,
    });

    descriptors.push(CommandDescriptor {
        name: "topo_get_entity",
        category: "topology",
        description: "Get a single entity's data including coordinates, metadata, and connections",
        params_schema: json!({
            "type": "object",
            "properties": {
                "topology": {"type": "string", "description": "Topology key or name"},
                "entity_id": {"type": "string", "description": "Entity identifier to retrieve"}
            },
            "required": ["topology", "entity_id"]
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "ok": {"type": "boolean"},
                "entity_id": {"type": "string"},
                "position": {"type": "object", "properties": {"x": {"type": "number"}, "y": {"type": "number"}, "z": {"type": "number"}}},
                "metadata": {"type": "object"},
                "outgoing": {"type": "array", "description": "Outgoing edge targets"},
                "incoming": {"type": "array", "description": "Incoming edge sources"}
            }
        }),
        example: r#"{"cmd":"topo_get_entity","params":{"topology":"services","entity_id":"auth-svc"}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });

    descriptors.push(CommandDescriptor {
        name: "topo_validate_edge",
        category: "topology",
        description: "Check if an edge would be valid without creating it (Q64.64 constraint validation)",
        params_schema: json!({
            "type": "object",
            "properties": {
                "topology": {"type": "string", "description": "Topology key or name"},
                "from": {"type": "string", "description": "Source entity ID"},
                "to": {"type": "string", "description": "Target entity ID"}
            },
            "required": ["topology", "from", "to"]
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
        example: r#"{"cmd":"topo_validate_edge","params":{"topology":"services","from":"api-gw","to":"auth-svc"}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });
}

#[derive(Debug, Deserialize)]
pub struct TopoParams {
    /// Topology key (e.g., "{site_id}:my_topology")
    #[serde(default)]
    pub topology_key: Option<String>,
    /// Topology name (for creation)
    #[serde(default)]
    pub name: Option<String>,
    /// Constraint type: none, z_monotonic, bidirectional, custom
    #[serde(default)]
    pub constraint_type: Option<String>,
    /// Topology type for categorization
    #[serde(default)]
    pub topology_type: Option<String>,
    /// Entity ID
    #[serde(default)]
    pub entity_id: Option<String>,
    /// Source entity ID (for edges)
    #[serde(default)]
    pub from_id: Option<String>,
    /// Target entity ID (for edges)
    #[serde(default)]
    pub to_id: Option<String>,
    /// X coordinate (0.0 to 1.0)
    #[serde(default)]
    pub x: Option<f64>,
    /// Y coordinate (0.0 to 1.0)
    #[serde(default)]
    pub y: Option<f64>,
    /// Z coordinate (0.0 to 1.0, hierarchy/depth)
    #[serde(default)]
    pub z: Option<f64>,
    /// Pre-computed bucket key (optional, for direct voxel query)
    #[serde(default)]
    pub bucket_key: Option<String>,
    /// Entity metadata
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
    /// Edge metadata
    #[serde(default)]
    pub edge_metadata: Option<serde_json::Value>,
    /// Traversal direction: outgoing, incoming
    #[serde(default)]
    pub direction: Option<String>,
    /// Maximum traversal depth
    #[serde(default)]
    pub max_depth: Option<i32>,
    /// Result limit
    #[serde(default)]
    pub limit: Option<i32>,
    /// Result offset
    #[serde(default)]
    pub offset: Option<i32>,
    /// Include full entity data in results
    #[serde(default)]
    pub include_data: Option<bool>,
    /// Z-range minimum score
    #[serde(default)]
    pub z_min: Option<f64>,
    /// Z-range maximum score
    #[serde(default)]
    pub z_max: Option<f64>,
    /// Descending order
    #[serde(default)]
    pub descending: Option<bool>,
    /// Confirmation flag for destructive operations
    #[serde(default)]
    pub confirm: Option<String>,
    /// Entity IDs for batch operations
    #[serde(default)]
    pub entity_ids: Option<Vec<String>>,
    /// Edges to create with entity
    #[serde(default)]
    pub edges_to: Option<Vec<String>>,
    /// Axis semantics definition
    #[serde(default)]
    pub axis_semantics: Option<serde_json::Value>,
    /// Description
    #[serde(default)]
    pub description: Option<String>,
}

pub fn handle_topo_create_async<'a>(
    command: &'a Command,
    conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling topo_create command: {}", command.id);
        }

        let params: TopoParams = match serde_json::from_value(command.parameters.clone()) {
            Ok(p) => p,
            Err(e) => return CommandResult::error(format!("Invalid parameters: {}", e)),
        };

        // Validate required fields
        let name = match &params.name {
            Some(n) if !n.is_empty() => n.clone(),
            _ => return CommandResult::error("Missing 'name' parameter"),
        };

        // Build topology key
        let topology_key = params.topology_key
            .unwrap_or_else(|| format!("{{{}}}:{}", site_id, name));

        // Build definition JSON
        let definition = json!({
            "name": name,
            "constraint_type": params.constraint_type.as_deref().unwrap_or("none"),
            "topology_type": params.topology_type.as_deref().unwrap_or("custom"),
            "description": params.description.as_deref().unwrap_or(""),
            "axis_semantics": params.axis_semantics.clone()
        });
        let def_json = serde_json::to_string(&definition).unwrap_or_default();

        // Call Lua function
        let result: redis::RedisResult<String> = redis::cmd("FCALL")
            .arg("GNODE_TOPO_CREATE")
            .arg(1)  // numkeys
            .arg(site_id)
            .arg(&topology_key)
            .arg(&def_json)
            .query_async(conn)
            .await;

        match result {
            Ok(json_str) => CommandResult::success_json(json_str),
            Err(e) => CommandResult::error(format!("topo_create failed: {}", e)),
        }
    })
}

/// Handle topo_register command - Register entity with Q64.64 bucket key computation
pub fn handle_topo_register_async<'a>(
    command: &'a Command,
    conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    _site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling topo_register command: {}", command.id);
        }

        let params: TopoParams = match serde_json::from_value(command.parameters.clone()) {
            Ok(p) => p,
            Err(e) => return CommandResult::error(format!("Invalid parameters: {}", e)),
        };

        // Validate required fields
        let topology_key = match &params.topology_key {
            Some(k) if !k.is_empty() => k.clone(),
            _ => return CommandResult::error("Missing 'topology_key' parameter"),
        };

        let entity_id = match &params.entity_id {
            Some(id) if !id.is_empty() => id.clone(),
            _ => return CommandResult::error("Missing 'entity_id' parameter"),
        };

        let x = params.x.unwrap_or(0.5);
        let y = params.y.unwrap_or(0.5);
        let z = params.z.unwrap_or(0.5);

        // COMPUTE: Q64.64 bucket key (deterministic across all nodes)
        let bucket_key = GeometricTopology::compute_3d_bucket_key(x, y, z, 10);

        // COMPUTE: Z-score for sorted set ordering
        let z_score = GeometricTopology::compute_z_score(z);

        // Build entity JSON
        let entity_data = json!({
            "position": { "x": x, "y": y, "z": z },
            "metadata": params.metadata.clone().unwrap_or(json!({}))
        });
        let entity_json = serde_json::to_string(&entity_data).unwrap_or_default();

        // Call Lua function with pre-computed values
        let result: redis::RedisResult<String> = redis::cmd("FCALL")
            .arg("GNODE_REGISTER_CAPABILITY_VECTOR")
            .arg(1)  // numkeys
            .arg(&topology_key)
            .arg(&entity_id)
            .arg(&entity_json)
            .arg(&bucket_key)
            .arg(z_score)
            .arg(crate::daemon::GNodeDaemon::topology_snapshot_key())  // args[5]: (B) snapshot
            .query_async(conn)
            .await;

        // Handle edges_to if provided
        if let Ok(ref _register_result) = result {
            if let Some(edges_to) = &params.edges_to {
                for target_id in edges_to {
                    // Get target entity to validate constraint
                    let target_result: redis::RedisResult<String> = redis::cmd("FCALL")
                        .arg("GNODE_TOPO_GET_ENTITY")
                        .arg(1)
                        .arg(&topology_key)
                        .arg(target_id)
                        .query_async(conn)
                        .await;

                    if let Ok(target_json) = target_result {
                        if let Ok(target) = serde_json::from_str::<serde_json::Value>(&target_json) {
                            if let Some(target_z) = target.get("position").and_then(|p| p.get("z")).and_then(|z| z.as_f64()) {
                                // COMPUTE: Validate Z-monotonicity using Q64.64
                                let (valid, err_msg) = GeometricTopology::validate_z_monotonic(z, target_z);

                                if valid {
                                    let z_delta = GeometricTopology::compute_z_delta(z, target_z);
                                    let edge_data = json!({
                                        "z_delta": z_delta,
                                        "constraint_valid": true,
                                        "metadata": params.edge_metadata.clone()
                                    });
                                    let edge_json = serde_json::to_string(&edge_data).unwrap_or_default();

                                    let _: redis::RedisResult<String> = redis::cmd("FCALL")
                                        .arg("GNODE_TOPO_ADD_EDGE")
                                        .arg(1)
                                        .arg(&topology_key)
                                        .arg(&entity_id)
                                        .arg(target_id)
                                        .arg(&edge_json)
                                        .query_async(conn)
                                        .await;
                                } else if debug_mode {
                                    debug!("Skipping edge to {}: {}", target_id, err_msg.unwrap_or_default());
                                }
                            }
                        }
                    }
                }
            }
        }

        match result {
            Ok(json_str) => CommandResult::success_json(json_str),
            Err(e) => CommandResult::error(format!("topo_register failed: {}", e)),
        }
    })
}

/// Handle topo_deregister command - Remove entity from topology
pub fn handle_topo_deregister_async<'a>(
    command: &'a Command,
    conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    _site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling topo_deregister command: {}", command.id);
        }

        let params: TopoParams = match serde_json::from_value(command.parameters.clone()) {
            Ok(p) => p,
            Err(e) => return CommandResult::error(format!("Invalid parameters: {}", e)),
        };

        let topology_key = match &params.topology_key {
            Some(k) if !k.is_empty() => k.clone(),
            _ => return CommandResult::error("Missing 'topology_key' parameter"),
        };

        let entity_id = match &params.entity_id {
            Some(id) if !id.is_empty() => id.clone(),
            _ => return CommandResult::error("Missing 'entity_id' parameter"),
        };

        let result: redis::RedisResult<String> = redis::cmd("FCALL")
            .arg("GNODE_DEREGISTER_CAPABILITY_VECTOR")
            .arg(1)
            .arg(&topology_key)
            .arg(&entity_id)
            .arg(crate::daemon::GNodeDaemon::topology_snapshot_key())  // args[2]: (B) snapshot
            .query_async(conn)
            .await;

        match result {
            Ok(json_str) => CommandResult::success_json(json_str),
            Err(e) => CommandResult::error(format!("topo_deregister failed: {}", e)),
        }
    })
}

/// Handle topo_add_edge command - Add edge with Q64.64 constraint validation
pub fn handle_topo_add_edge_async<'a>(
    command: &'a Command,
    conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    _site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling topo_add_edge command: {}", command.id);
        }

        let params: TopoParams = match serde_json::from_value(command.parameters.clone()) {
            Ok(p) => p,
            Err(e) => return CommandResult::error(format!("Invalid parameters: {}", e)),
        };

        let topology_key = match &params.topology_key {
            Some(k) if !k.is_empty() => k.clone(),
            _ => return CommandResult::error("Missing 'topology_key' parameter"),
        };

        let from_id = match &params.from_id {
            Some(id) if !id.is_empty() => id.clone(),
            _ => return CommandResult::error("Missing 'from_id' parameter"),
        };

        let to_id = match &params.to_id {
            Some(id) if !id.is_empty() => id.clone(),
            _ => return CommandResult::error("Missing 'to_id' parameter"),
        };

        // Get both entities to compute z_delta and validate constraint
        let entities_json = serde_json::to_string(&vec![&from_id, &to_id]).unwrap_or_default();
        let entities_result: redis::RedisResult<String> = redis::cmd("FCALL")
            .arg("GNODE_TOPO_GET_ENTITIES")
            .arg(1)
            .arg(&topology_key)
            .arg(&entities_json)
            .arg("false")
            .query_async(conn)
            .await;

        let (from_z, to_z) = match entities_result {
            Ok(json_str) => {
                match serde_json::from_str::<serde_json::Value>(&json_str) {
                    Ok(data) => {
                        let from_z = data.get("entities")
                            .and_then(|e| e.get(&from_id))
                            .and_then(|e| e.get("position"))
                            .and_then(|p| p.get("z"))
                            .and_then(|z| z.as_f64())
                            .unwrap_or(0.5);
                        let to_z = data.get("entities")
                            .and_then(|e| e.get(&to_id))
                            .and_then(|e| e.get("position"))
                            .and_then(|p| p.get("z"))
                            .and_then(|z| z.as_f64())
                            .unwrap_or(0.5);
                        (from_z, to_z)
                    }
                    Err(_) => (0.5, 0.5),
                }
            }
            Err(e) => return CommandResult::error(format!("Failed to get entities: {}", e)),
        };

        // COMPUTE: Z-delta using Q64.64
        let z_delta = GeometricTopology::compute_z_delta(from_z, to_z);

        // Build edge data
        let edge_data = json!({
            "z_delta": z_delta,
            "from_z": from_z,
            "to_z": to_z,
            "metadata": params.edge_metadata.clone()
        });
        let edge_json = serde_json::to_string(&edge_data).unwrap_or_default();

        let result: redis::RedisResult<String> = redis::cmd("FCALL")
            .arg("GNODE_TOPO_ADD_EDGE")
            .arg(1)
            .arg(&topology_key)
            .arg(&from_id)
            .arg(&to_id)
            .arg(&edge_json)
            .query_async(conn)
            .await;

        match result {
            Ok(json_str) => CommandResult::success_json(json_str),
            Err(e) => CommandResult::error(format!("topo_add_edge failed: {}", e)),
        }
    })
}

/// Handle topo_discover command - Query entities in voxel by position or bucket_key
pub fn handle_topo_discover_async<'a>(
    command: &'a Command,
    conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    _site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling topo_discover command: {}", command.id);
        }

        let params: TopoParams = match serde_json::from_value(command.parameters.clone()) {
            Ok(p) => p,
            Err(e) => return CommandResult::error(format!("Invalid parameters: {}", e)),
        };

        let topology_key = match &params.topology_key {
            Some(k) if !k.is_empty() => k.clone(),
            _ => return CommandResult::error("Missing 'topology_key' parameter"),
        };

        // COMPUTE: bucket_key from position OR use provided bucket_key
        let bucket_key = if let Some(bk) = &params.bucket_key {
            bk.clone()
        } else {
            let x = params.x.unwrap_or(0.5);
            let y = params.y.unwrap_or(0.5);
            let z = params.z.unwrap_or(0.5);
            GeometricTopology::compute_3d_bucket_key(x, y, z, 10)
        };

        let include_data = if params.include_data.unwrap_or(false) { "true" } else { "false" };

        let result: redis::RedisResult<String> = redis::cmd("FCALL")
            .arg("GNODE_TOPO_QUERY_VOXEL")
            .arg(1)
            .arg(&topology_key)
            .arg(&bucket_key)
            .arg(include_data)
            .query_async(conn)
            .await;

        match result {
            Ok(json_str) => CommandResult::success_json(json_str),
            Err(e) => CommandResult::error(format!("topo_discover failed: {}", e)),
        }
    })
}

/// Handle topo_z_order command - Get entities in Z-ascending order (DAG load order)
pub fn handle_topo_z_order_async<'a>(
    command: &'a Command,
    conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    _site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling topo_z_order command: {}", command.id);
        }

        let params: TopoParams = match serde_json::from_value(command.parameters.clone()) {
            Ok(p) => p,
            Err(e) => return CommandResult::error(format!("Invalid parameters: {}", e)),
        };

        let topology_key = match &params.topology_key {
            Some(k) if !k.is_empty() => k.clone(),
            _ => return CommandResult::error("Missing 'topology_key' parameter"),
        };

        let limit = params.limit.map(|l| l.to_string()).unwrap_or_default();
        let offset = params.offset.map(|o| o.to_string()).unwrap_or_else(|| "0".to_string());
        let descending = if params.descending.unwrap_or(false) { "true" } else { "false" };

        let result: redis::RedisResult<String> = redis::cmd("FCALL")
            .arg("GNODE_TOPO_Z_ORDER")
            .arg(1)
            .arg(&topology_key)
            .arg(&limit)
            .arg(&offset)
            .arg(descending)
            .query_async(conn)
            .await;

        match result {
            Ok(json_str) => CommandResult::success_json(json_str),
            Err(e) => CommandResult::error(format!("topo_z_order failed: {}", e)),
        }
    })
}

/// Handle topo_z_range command - Query entities within Z range
pub fn handle_topo_z_range_async<'a>(
    command: &'a Command,
    conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    _site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling topo_z_range command: {}", command.id);
        }

        let params: TopoParams = match serde_json::from_value(command.parameters.clone()) {
            Ok(p) => p,
            Err(e) => return CommandResult::error(format!("Invalid parameters: {}", e)),
        };

        let topology_key = match &params.topology_key {
            Some(k) if !k.is_empty() => k.clone(),
            _ => return CommandResult::error("Missing 'topology_key' parameter"),
        };

        // COMPUTE: Z-scores for range boundaries
        let min_score = params.z_min
            .map(|z| GeometricTopology::compute_z_score(z).to_string())
            .unwrap_or_else(|| "-inf".to_string());
        let max_score = params.z_max
            .map(|z| GeometricTopology::compute_z_score(z).to_string())
            .unwrap_or_else(|| "+inf".to_string());

        let include_data = if params.include_data.unwrap_or(false) { "true" } else { "false" };
        let limit = params.limit.map(|l| l.to_string()).unwrap_or_default();

        let result: redis::RedisResult<String> = redis::cmd("FCALL")
            .arg("GNODE_TOPO_QUERY_Z_RANGE")
            .arg(1)
            .arg(&topology_key)
            .arg(&min_score)
            .arg(&max_score)
            .arg(include_data)
            .arg(&limit)
            .query_async(conn)
            .await;

        match result {
            Ok(json_str) => CommandResult::success_json(json_str),
            Err(e) => CommandResult::error(format!("topo_z_range failed: {}", e)),
        }
    })
}

/// Handle topo_chain command - BFS traversal for dependency chains
pub fn handle_topo_chain_async<'a>(
    command: &'a Command,
    conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    _site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling topo_chain command: {}", command.id);
        }

        let params: TopoParams = match serde_json::from_value(command.parameters.clone()) {
            Ok(p) => p,
            Err(e) => return CommandResult::error(format!("Invalid parameters: {}", e)),
        };

        let topology_key = match &params.topology_key {
            Some(k) if !k.is_empty() => k.clone(),
            _ => return CommandResult::error("Missing 'topology_key' parameter"),
        };

        let entity_id = match &params.entity_id {
            Some(id) if !id.is_empty() => id.clone(),
            _ => return CommandResult::error("Missing 'entity_id' parameter"),
        };

        let direction = params.direction.as_deref().unwrap_or("outgoing");
        let max_depth = params.max_depth.map(|d| d.to_string()).unwrap_or_else(|| "100".to_string());

        let result: redis::RedisResult<String> = redis::cmd("FCALL")
            .arg("GNODE_TOPO_CHAIN")
            .arg(1)
            .arg(&topology_key)
            .arg(&entity_id)
            .arg(direction)
            .arg(&max_depth)
            .query_async(conn)
            .await;

        match result {
            Ok(json_str) => CommandResult::success_json(json_str),
            Err(e) => CommandResult::error(format!("topo_chain failed: {}", e)),
        }
    })
}

/// Handle topo_stats command - Get topology statistics
pub fn handle_topo_stats_async<'a>(
    command: &'a Command,
    conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    _site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling topo_stats command: {}", command.id);
        }

        let params: TopoParams = match serde_json::from_value(command.parameters.clone()) {
            Ok(p) => p,
            Err(e) => return CommandResult::error(format!("Invalid parameters: {}", e)),
        };

        let topology_key = match &params.topology_key {
            Some(k) if !k.is_empty() => k.clone(),
            _ => return CommandResult::error("Missing 'topology_key' parameter"),
        };

        let result: redis::RedisResult<String> = redis::cmd("FCALL")
            .arg("GNODE_TOPO_STATS")
            .arg(1)
            .arg(&topology_key)
            .query_async(conn)
            .await;

        match result {
            Ok(json_str) => CommandResult::success_json(json_str),
            Err(e) => CommandResult::error(format!("topo_stats failed: {}", e)),
        }
    })
}

/// Handle topo_list command - List all topologies for site
pub fn handle_topo_list_async<'a>(
    command: &'a Command,
    conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling topo_list command: {}", command.id);
        }

        let params: TopoParams = match serde_json::from_value(command.parameters.clone()) {
            Ok(p) => p,
            Err(_) => TopoParams {
                topology_key: None, name: None, constraint_type: None, topology_type: None,
                entity_id: None, from_id: None, to_id: None, x: None, y: None, z: None,
                bucket_key: None, metadata: None, edge_metadata: None, direction: None,
                max_depth: None, limit: None, offset: None, include_data: None,
                z_min: None, z_max: None, descending: None, confirm: None,
                entity_ids: None, edges_to: None, axis_semantics: None, description: None,
            },
        };

        let filter_type = params.topology_type.as_deref().unwrap_or("");

        let result: redis::RedisResult<String> = redis::cmd("FCALL")
            .arg("GNODE_TOPO_LIST")
            .arg(1)
            .arg(site_id)
            .arg(filter_type)
            .query_async(conn)
            .await;

        match result {
            Ok(json_str) => CommandResult::success_json(json_str),
            Err(e) => CommandResult::error(format!("topo_list failed: {}", e)),
        }
    })
}

/// Handle topo_delete command - Delete topology (requires CONFIRM)
pub fn handle_topo_delete_async<'a>(
    command: &'a Command,
    conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling topo_delete command: {}", command.id);
        }

        let params: TopoParams = match serde_json::from_value(command.parameters.clone()) {
            Ok(p) => p,
            Err(e) => return CommandResult::error(format!("Invalid parameters: {}", e)),
        };

        let topology_key = match &params.topology_key {
            Some(k) if !k.is_empty() => k.clone(),
            _ => return CommandResult::error("Missing 'topology_key' parameter"),
        };

        let confirm = params.confirm.as_deref().unwrap_or("");
        if confirm != "CONFIRM" {
            return CommandResult::error("Must provide confirm: 'CONFIRM' to delete topology");
        }

        let result: redis::RedisResult<String> = redis::cmd("FCALL")
            .arg("GNODE_TOPO_DELETE")
            .arg(1)
            .arg(site_id)
            .arg(&topology_key)
            .arg("CONFIRM")
            .query_async(conn)
            .await;

        match result {
            Ok(json_str) => CommandResult::success_json(json_str),
            Err(e) => CommandResult::error(format!("topo_delete failed: {}", e)),
        }
    })
}

/// Handle topo_get_entity command - Get single entity with edges
pub fn handle_topo_get_entity_async<'a>(
    command: &'a Command,
    conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    _site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling topo_get_entity command: {}", command.id);
        }

        let params: TopoParams = match serde_json::from_value(command.parameters.clone()) {
            Ok(p) => p,
            Err(e) => return CommandResult::error(format!("Invalid parameters: {}", e)),
        };

        let topology_key = match &params.topology_key {
            Some(k) if !k.is_empty() => k.clone(),
            _ => return CommandResult::error("Missing 'topology_key' parameter"),
        };

        let entity_id = match &params.entity_id {
            Some(id) if !id.is_empty() => id.clone(),
            _ => return CommandResult::error("Missing 'entity_id' parameter"),
        };

        let result: redis::RedisResult<String> = redis::cmd("FCALL")
            .arg("GNODE_TOPO_GET_ENTITY")
            .arg(1)
            .arg(&topology_key)
            .arg(&entity_id)
            .query_async(conn)
            .await;

        match result {
            Ok(json_str) => CommandResult::success_json(json_str),
            Err(e) => CommandResult::error(format!("topo_get_entity failed: {}", e)),
        }
    })
}

/// Handle topo_validate_edge command - Validate edge constraint without creating
pub fn handle_topo_validate_edge_async<'a>(
    command: &'a Command,
    conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    _site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling topo_validate_edge command: {}", command.id);
        }

        let params: TopoParams = match serde_json::from_value(command.parameters.clone()) {
            Ok(p) => p,
            Err(e) => return CommandResult::error(format!("Invalid parameters: {}", e)),
        };

        let topology_key = match &params.topology_key {
            Some(k) if !k.is_empty() => k.clone(),
            _ => return CommandResult::error("Missing 'topology_key' parameter"),
        };

        let from_id = match &params.from_id {
            Some(id) if !id.is_empty() => id.clone(),
            _ => return CommandResult::error("Missing 'from_id' parameter"),
        };

        let to_id = match &params.to_id {
            Some(id) if !id.is_empty() => id.clone(),
            _ => return CommandResult::error("Missing 'to_id' parameter"),
        };

        // Get both entities
        let entities_json = serde_json::to_string(&vec![&from_id, &to_id]).unwrap_or_default();
        let entities_result: redis::RedisResult<String> = redis::cmd("FCALL")
            .arg("GNODE_TOPO_GET_ENTITIES")
            .arg(1)
            .arg(&topology_key)
            .arg(&entities_json)
            .arg("false")
            .query_async(conn)
            .await;

        match entities_result {
            Ok(json_str) => {
                match serde_json::from_str::<serde_json::Value>(&json_str) {
                    Ok(data) => {
                        let from_z = data.get("entities")
                            .and_then(|e| e.get(&from_id))
                            .and_then(|e| e.get("position"))
                            .and_then(|p| p.get("z"))
                            .and_then(|z| z.as_f64());

                        let to_z = data.get("entities")
                            .and_then(|e| e.get(&to_id))
                            .and_then(|e| e.get("position"))
                            .and_then(|p| p.get("z"))
                            .and_then(|z| z.as_f64());

                        match (from_z, to_z) {
                            (Some(fz), Some(tz)) => {
                                // COMPUTE: Validate using Q64.64
                                let (valid, reason) = GeometricTopology::validate_z_monotonic(fz, tz);
                                let z_delta = GeometricTopology::compute_z_delta(fz, tz);

                                CommandResult::success(json!({
                                    "valid": valid,
                                    "reason": reason,
                                    "from_z": fz,
                                    "to_z": tz,
                                    "z_delta": z_delta
                                }))
                            }
                            _ => CommandResult::error("Could not get Z coordinates for both entities"),
                        }
                    }
                    Err(e) => CommandResult::error(format!("Failed to parse entities: {}", e)),
                }
            }
            Err(e) => CommandResult::error(format!("Failed to get entities: {}", e)),
        }
    })
}

pub fn handle_topo_create(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    site_id: &str,
    debug_mode: bool,
) -> CommandResult {
    if debug_mode { debug!("Handling topo_create command"); }
    let params: TopoParams = match serde_json::from_value(command.parameters.clone()) {
        Ok(p) => p,
        Err(e) => return CommandResult::error(format!("Invalid parameters: {}", e)),
    };
    let name = match &params.name {
        Some(n) if !n.is_empty() => n.clone(),
        _ => return CommandResult::error("Missing 'name' parameter"),
    };
    let topology_key = params.topology_key.unwrap_or_else(|| format!("{{{}}}:{}", site_id, name));
    let definition = json!({
        "name": name,
        "constraint_type": params.constraint_type.as_deref().unwrap_or("none"),
        "topology_type": params.topology_type.as_deref().unwrap_or("custom"),
        "description": params.description.as_deref().unwrap_or(""),
        "axis_semantics": params.axis_semantics.clone()
    });
    let def_json = serde_json::to_string(&definition).unwrap_or_default();
    let result: redis::RedisResult<String> = redis::cmd("FCALL")
        .arg("GNODE_TOPO_CREATE").arg(1).arg(site_id).arg(&topology_key).arg(&def_json)
        .query(conn);
    match result {
        Ok(json_str) => CommandResult::success_json(json_str),
        Err(e) => CommandResult::error(format!("topo_create failed: {}", e)),
    }
}

/// Sync version of handle_topo_register
pub fn handle_topo_register(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    _site_id: &str,
    debug_mode: bool,
) -> CommandResult {
    if debug_mode { debug!("Handling topo_register command"); }
    let params: TopoParams = match serde_json::from_value(command.parameters.clone()) {
        Ok(p) => p,
        Err(e) => return CommandResult::error(format!("Invalid parameters: {}", e)),
    };
    let topology_key = match &params.topology_key {
        Some(k) if !k.is_empty() => k.clone(),
        _ => return CommandResult::error("Missing 'topology_key' parameter"),
    };
    let entity_id = match &params.entity_id {
        Some(id) if !id.is_empty() => id.clone(),
        _ => return CommandResult::error("Missing 'entity_id' parameter"),
    };
    let x = params.x.unwrap_or(0.5);
    let y = params.y.unwrap_or(0.5);
    let z = params.z.unwrap_or(0.5);
    let bucket_key = GeometricTopology::compute_3d_bucket_key(x, y, z, 10);
    let z_score = GeometricTopology::compute_z_score(z);
    let entity_data = json!({
        "position": { "x": x, "y": y, "z": z },
        "metadata": params.metadata.clone().unwrap_or(json!({}))
    });
    let entity_json = serde_json::to_string(&entity_data).unwrap_or_default();
    let result: redis::RedisResult<String> = redis::cmd("FCALL")
        .arg("GNODE_REGISTER_CAPABILITY_VECTOR").arg(1).arg(&topology_key)
        .arg(&entity_id).arg(&entity_json).arg(&bucket_key).arg(z_score)
        .arg(crate::daemon::GNodeDaemon::topology_snapshot_key())  // args[5]: (B) snapshot
        .query(conn);
    match result {
        Ok(json_str) => CommandResult::success_json(json_str),
        Err(e) => CommandResult::error(format!("topo_register failed: {}", e)),
    }
}

/// Sync version of handle_topo_deregister
pub fn handle_topo_deregister(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    _site_id: &str,
    debug_mode: bool,
) -> CommandResult {
    if debug_mode { debug!("Handling topo_deregister command"); }
    let params: TopoParams = match serde_json::from_value(command.parameters.clone()) {
        Ok(p) => p,
        Err(e) => return CommandResult::error(format!("Invalid parameters: {}", e)),
    };
    let topology_key = match &params.topology_key {
        Some(k) if !k.is_empty() => k.clone(),
        _ => return CommandResult::error("Missing 'topology_key' parameter"),
    };
    let entity_id = match &params.entity_id {
        Some(id) if !id.is_empty() => id.clone(),
        _ => return CommandResult::error("Missing 'entity_id' parameter"),
    };
    let result: redis::RedisResult<String> = redis::cmd("FCALL")
        .arg("GNODE_DEREGISTER_CAPABILITY_VECTOR").arg(1).arg(&topology_key).arg(&entity_id)
        .arg(crate::daemon::GNodeDaemon::topology_snapshot_key())  // args[2]: (B) snapshot
        .query(conn);
    match result {
        Ok(json_str) => CommandResult::success_json(json_str),
        Err(e) => CommandResult::error(format!("topo_deregister failed: {}", e)),
    }
}

/// Sync version of handle_topo_add_edge
pub fn handle_topo_add_edge(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    _site_id: &str,
    debug_mode: bool,
) -> CommandResult {
    if debug_mode { debug!("Handling topo_add_edge command"); }
    let params: TopoParams = match serde_json::from_value(command.parameters.clone()) {
        Ok(p) => p,
        Err(e) => return CommandResult::error(format!("Invalid parameters: {}", e)),
    };
    let topology_key = match &params.topology_key {
        Some(k) if !k.is_empty() => k.clone(),
        _ => return CommandResult::error("Missing 'topology_key' parameter"),
    };
    let from_id = match &params.from_id {
        Some(id) if !id.is_empty() => id.clone(),
        _ => return CommandResult::error("Missing 'from_id' parameter"),
    };
    let to_id = match &params.to_id {
        Some(id) if !id.is_empty() => id.clone(),
        _ => return CommandResult::error("Missing 'to_id' parameter"),
    };
    let edge_data = json!({ "metadata": params.edge_metadata.clone() });
    let edge_json = serde_json::to_string(&edge_data).unwrap_or_default();
    let result: redis::RedisResult<String> = redis::cmd("FCALL")
        .arg("GNODE_TOPO_ADD_EDGE").arg(1).arg(&topology_key)
        .arg(&from_id).arg(&to_id).arg(&edge_json)
        .query(conn);
    match result {
        Ok(json_str) => CommandResult::success_json(json_str),
        Err(e) => CommandResult::error(format!("topo_add_edge failed: {}", e)),
    }
}

/// Sync version of handle_topo_discover
pub fn handle_topo_discover(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    _site_id: &str,
    debug_mode: bool,
) -> CommandResult {
    if debug_mode { debug!("Handling topo_discover command"); }
    let params: TopoParams = match serde_json::from_value(command.parameters.clone()) {
        Ok(p) => p,
        Err(e) => return CommandResult::error(format!("Invalid parameters: {}", e)),
    };
    let topology_key = match &params.topology_key {
        Some(k) if !k.is_empty() => k.clone(),
        _ => return CommandResult::error("Missing 'topology_key' parameter"),
    };
    let bucket_key = if let Some(bk) = &params.bucket_key {
        bk.clone()
    } else {
        let x = params.x.unwrap_or(0.5);
        let y = params.y.unwrap_or(0.5);
        let z = params.z.unwrap_or(0.5);
        GeometricTopology::compute_3d_bucket_key(x, y, z, 10)
    };
    let include_data = if params.include_data.unwrap_or(false) { "true" } else { "false" };
    let result: redis::RedisResult<String> = redis::cmd("FCALL")
        .arg("GNODE_TOPO_QUERY_VOXEL").arg(1).arg(&topology_key).arg(&bucket_key).arg(include_data)
        .query(conn);
    match result {
        Ok(json_str) => CommandResult::success_json(json_str),
        Err(e) => CommandResult::error(format!("topo_discover failed: {}", e)),
    }
}

/// Sync version of handle_topo_z_order
pub fn handle_topo_z_order(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    _site_id: &str,
    debug_mode: bool,
) -> CommandResult {
    if debug_mode { debug!("Handling topo_z_order command"); }
    let params: TopoParams = match serde_json::from_value(command.parameters.clone()) {
        Ok(p) => p,
        Err(e) => return CommandResult::error(format!("Invalid parameters: {}", e)),
    };
    let topology_key = match &params.topology_key {
        Some(k) if !k.is_empty() => k.clone(),
        _ => return CommandResult::error("Missing 'topology_key' parameter"),
    };
    let direction = params.direction.as_deref().unwrap_or("asc");
    let limit = params.limit.unwrap_or(100);
    let result: redis::RedisResult<String> = redis::cmd("FCALL")
        .arg("GNODE_TOPO_Z_ORDER").arg(1).arg(&topology_key).arg(direction).arg(limit)
        .query(conn);
    match result {
        Ok(json_str) => CommandResult::success_json(json_str),
        Err(e) => CommandResult::error(format!("topo_z_order failed: {}", e)),
    }
}

/// Sync version of handle_topo_z_range
pub fn handle_topo_z_range(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    _site_id: &str,
    debug_mode: bool,
) -> CommandResult {
    if debug_mode { debug!("Handling topo_z_range command"); }
    let params: TopoParams = match serde_json::from_value(command.parameters.clone()) {
        Ok(p) => p,
        Err(e) => return CommandResult::error(format!("Invalid parameters: {}", e)),
    };
    let topology_key = match &params.topology_key {
        Some(k) if !k.is_empty() => k.clone(),
        _ => return CommandResult::error("Missing 'topology_key' parameter"),
    };
    let min_z = params.z_min.unwrap_or(0.0);
    let max_z = params.z_max.unwrap_or(1.0);
    let include_data = if params.include_data.unwrap_or(false) { "true" } else { "false" };
    let result: redis::RedisResult<String> = redis::cmd("FCALL")
        .arg("GNODE_TOPO_Z_RANGE").arg(1).arg(&topology_key).arg(min_z).arg(max_z).arg(include_data)
        .query(conn);
    match result {
        Ok(json_str) => CommandResult::success_json(json_str),
        Err(e) => CommandResult::error(format!("topo_z_range failed: {}", e)),
    }
}

/// Sync version of handle_topo_chain
pub fn handle_topo_chain(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    _site_id: &str,
    debug_mode: bool,
) -> CommandResult {
    if debug_mode { debug!("Handling topo_chain command"); }
    let params: TopoParams = match serde_json::from_value(command.parameters.clone()) {
        Ok(p) => p,
        Err(e) => return CommandResult::error(format!("Invalid parameters: {}", e)),
    };
    let topology_key = match &params.topology_key {
        Some(k) if !k.is_empty() => k.clone(),
        _ => return CommandResult::error("Missing 'topology_key' parameter"),
    };
    let entity_id = match &params.entity_id {
        Some(id) if !id.is_empty() => id.clone(),
        _ => return CommandResult::error("Missing 'entity_id' parameter"),
    };
    let direction = params.direction.as_deref().unwrap_or("outgoing");
    let max_depth = params.max_depth.unwrap_or(10);
    let result: redis::RedisResult<String> = redis::cmd("FCALL")
        .arg("GNODE_TOPO_CHAIN").arg(1).arg(&topology_key).arg(&entity_id).arg(direction).arg(max_depth)
        .query(conn);
    match result {
        Ok(json_str) => CommandResult::success_json(json_str),
        Err(e) => CommandResult::error(format!("topo_chain failed: {}", e)),
    }
}

/// Sync version of handle_topo_stats
pub fn handle_topo_stats(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    _site_id: &str,
    debug_mode: bool,
) -> CommandResult {
    if debug_mode { debug!("Handling topo_stats command"); }
    let params: TopoParams = match serde_json::from_value(command.parameters.clone()) {
        Ok(p) => p,
        Err(e) => return CommandResult::error(format!("Invalid parameters: {}", e)),
    };
    let topology_key = match &params.topology_key {
        Some(k) if !k.is_empty() => k.clone(),
        _ => return CommandResult::error("Missing 'topology_key' parameter"),
    };
    let result: redis::RedisResult<String> = redis::cmd("FCALL")
        .arg("GNODE_TOPO_STATS").arg(1).arg(&topology_key)
        .query(conn);
    match result {
        Ok(json_str) => CommandResult::success_json(json_str),
        Err(e) => CommandResult::error(format!("topo_stats failed: {}", e)),
    }
}

/// Sync version of handle_topo_list
pub fn handle_topo_list(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    site_id: &str,
    debug_mode: bool,
) -> CommandResult {
    if debug_mode { debug!("Handling topo_list command"); }
    // Extract prefix from parameters if provided, otherwise empty
    let prefix = command.parameters.get("prefix")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let result: redis::RedisResult<String> = redis::cmd("FCALL")
        .arg("GNODE_TOPO_LIST").arg(1).arg(site_id).arg(prefix)
        .query(conn);
    match result {
        Ok(json_str) => CommandResult::success_json(json_str),
        Err(e) => CommandResult::error(format!("topo_list failed: {}", e)),
    }
}

/// Sync version of handle_topo_delete
pub fn handle_topo_delete(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    _site_id: &str,
    debug_mode: bool,
) -> CommandResult {
    if debug_mode { debug!("Handling topo_delete command"); }
    let params: TopoParams = match serde_json::from_value(command.parameters.clone()) {
        Ok(p) => p,
        Err(e) => return CommandResult::error(format!("Invalid parameters: {}", e)),
    };
    let topology_key = match &params.topology_key {
        Some(k) if !k.is_empty() => k.clone(),
        _ => return CommandResult::error("Missing 'topology_key' parameter"),
    };
    let result: redis::RedisResult<String> = redis::cmd("FCALL")
        .arg("GNODE_TOPO_DELETE").arg(1).arg(&topology_key)
        .query(conn);
    match result {
        Ok(json_str) => CommandResult::success_json(json_str),
        Err(e) => CommandResult::error(format!("topo_delete failed: {}", e)),
    }
}

/// Sync version of handle_topo_get_entity
pub fn handle_topo_get_entity(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    _site_id: &str,
    debug_mode: bool,
) -> CommandResult {
    if debug_mode { debug!("Handling topo_get_entity command"); }
    let params: TopoParams = match serde_json::from_value(command.parameters.clone()) {
        Ok(p) => p,
        Err(e) => return CommandResult::error(format!("Invalid parameters: {}", e)),
    };
    let topology_key = match &params.topology_key {
        Some(k) if !k.is_empty() => k.clone(),
        _ => return CommandResult::error("Missing 'topology_key' parameter"),
    };
    let entity_id = match &params.entity_id {
        Some(id) if !id.is_empty() => id.clone(),
        _ => return CommandResult::error("Missing 'entity_id' parameter"),
    };
    let result: redis::RedisResult<String> = redis::cmd("FCALL")
        .arg("GNODE_TOPO_GET_ENTITY").arg(1).arg(&topology_key).arg(&entity_id)
        .query(conn);
    match result {
        Ok(json_str) => CommandResult::success_json(json_str),
        Err(e) => CommandResult::error(format!("topo_get_entity failed: {}", e)),
    }
}

/// Sync version of handle_topo_validate_edge
pub fn handle_topo_validate_edge(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    _site_id: &str,
    debug_mode: bool,
) -> CommandResult {
    if debug_mode { debug!("Handling topo_validate_edge command"); }
    let params: TopoParams = match serde_json::from_value(command.parameters.clone()) {
        Ok(p) => p,
        Err(e) => return CommandResult::error(format!("Invalid parameters: {}", e)),
    };
    let topology_key = match &params.topology_key {
        Some(k) if !k.is_empty() => k.clone(),
        _ => return CommandResult::error("Missing 'topology_key' parameter"),
    };
    let from_id = match &params.from_id {
        Some(id) if !id.is_empty() => id.clone(),
        _ => return CommandResult::error("Missing 'from_id' parameter"),
    };
    let to_id = match &params.to_id {
        Some(id) if !id.is_empty() => id.clone(),
        _ => return CommandResult::error("Missing 'to_id' parameter"),
    };
    // Get entities to validate Z constraint
    let entities_json = serde_json::to_string(&vec![&from_id, &to_id]).unwrap_or_default();
    let entities_result: redis::RedisResult<String> = redis::cmd("FCALL")
        .arg("GNODE_TOPO_GET_ENTITIES").arg(1).arg(&topology_key).arg(&entities_json).arg("false")
        .query(conn);
    match entities_result {
        Ok(json_str) => {
            match serde_json::from_str::<serde_json::Value>(&json_str) {
                Ok(data) => {
                    let from_z = data.get("entities").and_then(|e| e.get(&from_id))
                        .and_then(|e| e.get("position")).and_then(|p| p.get("z")).and_then(|z| z.as_f64());
                    let to_z = data.get("entities").and_then(|e| e.get(&to_id))
                        .and_then(|e| e.get("position")).and_then(|p| p.get("z")).and_then(|z| z.as_f64());
                    match (from_z, to_z) {
                        (Some(fz), Some(tz)) => {
                            let (valid, reason) = GeometricTopology::validate_z_monotonic(fz, tz);
                            let z_delta = GeometricTopology::compute_z_delta(fz, tz);
                            CommandResult::success(json!({
                                "valid": valid, "reason": reason, "from_z": fz, "to_z": tz, "z_delta": z_delta
                            }))
                        }
                        _ => CommandResult::error("Could not get Z coordinates for both entities"),
                    }
                }
                Err(e) => CommandResult::error(format!("Failed to parse entities: {}", e)),
            }
        }
        Err(e) => CommandResult::error(format!("Failed to get entities: {}", e)),
    }
}
