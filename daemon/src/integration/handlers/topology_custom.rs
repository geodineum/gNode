// Custom Topology Command Handlers (Rust Q64.64 Precision)
//
// Handles: custom_topology_discover, custom_topology_distance, custom_topology_knn,
//          custom_topology_similarity
// These use Rust's fixed-point arithmetic for cluster-safe calculations.

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

/// Register all custom topology command handlers
pub fn register(
    handlers: &mut HashMap<String, CommandHandlerFn>,
    async_handlers: &mut HashMap<String, AsyncCommandHandlerFn>,
    descriptors: &mut Vec<CommandDescriptor>,
) {
    // Sync handlers
    handlers.insert("custom_topology_discover".to_string(), handle_custom_topology_discover as CommandHandlerFn);
    handlers.insert("CUSTOM_TOPOLOGY_DISCOVER".to_string(), handle_custom_topology_discover as CommandHandlerFn);
    handlers.insert("custom_topology_distance".to_string(), handle_custom_topology_distance as CommandHandlerFn);
    handlers.insert("CUSTOM_TOPOLOGY_DISTANCE".to_string(), handle_custom_topology_distance as CommandHandlerFn);
    handlers.insert("custom_topology_knn".to_string(), handle_custom_topology_knn as CommandHandlerFn);
    handlers.insert("CUSTOM_TOPOLOGY_KNN".to_string(), handle_custom_topology_knn as CommandHandlerFn);
    handlers.insert("custom_topology_similarity".to_string(), handle_custom_topology_similarity as CommandHandlerFn);
    handlers.insert("CUSTOM_TOPOLOGY_SIMILARITY".to_string(), handle_custom_topology_similarity as CommandHandlerFn);

    // Async handlers
    async_handlers.insert("custom_topology_discover".to_string(), handle_custom_topology_discover_async as AsyncCommandHandlerFn);
    async_handlers.insert("CUSTOM_TOPOLOGY_DISCOVER".to_string(), handle_custom_topology_discover_async as AsyncCommandHandlerFn);
    async_handlers.insert("custom_topology_distance".to_string(), handle_custom_topology_distance_async as AsyncCommandHandlerFn);
    async_handlers.insert("CUSTOM_TOPOLOGY_DISTANCE".to_string(), handle_custom_topology_distance_async as AsyncCommandHandlerFn);
    async_handlers.insert("custom_topology_knn".to_string(), handle_custom_topology_knn_async as AsyncCommandHandlerFn);
    async_handlers.insert("CUSTOM_TOPOLOGY_KNN".to_string(), handle_custom_topology_knn_async as AsyncCommandHandlerFn);
    async_handlers.insert("custom_topology_similarity".to_string(), handle_custom_topology_similarity_async as AsyncCommandHandlerFn);
    async_handlers.insert("CUSTOM_TOPOLOGY_SIMILARITY".to_string(), handle_custom_topology_similarity_async as AsyncCommandHandlerFn);

    // Command descriptors
    descriptors.push(CommandDescriptor {
        name: "custom_topology_discover",
        category: "topology_custom",
        description: "Discover entities in a custom topology by capability matching",
        params_schema: json!({
            "type": "object",
            "properties": {
                "topology": {"type": "string", "description": "Topology name"},
                "capabilities": {"type": "object", "description": "Capability requirements for matching"},
                "limit": {"type": "integer", "description": "Maximum results to return"}
            },
            "required": ["topology", "capabilities"]
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "results": {"type": "array", "description": "Matching entities"},
                "count": {"type": "integer", "description": "Number of matches"}
            }
        }),
        example: r#"{"cmd":"custom_topology_discover","params":{"topology":"services","capabilities":{"compute":0.8},"limit":10}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });

    descriptors.push(CommandDescriptor {
        name: "custom_topology_distance",
        category: "topology_custom",
        description: "Calculate distance between two entities in a custom topology",
        params_schema: json!({
            "type": "object",
            "properties": {
                "topology": {"type": "string", "description": "Topology name"},
                "entity_a": {"type": "string", "description": "First entity identifier"},
                "entity_b": {"type": "string", "description": "Second entity identifier"}
            },
            "required": ["topology", "entity_a", "entity_b"]
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "distance": {"type": "number", "description": "Euclidean distance between entities"}
            }
        }),
        example: r#"{"cmd":"custom_topology_distance","params":{"topology":"services","entity_a":"svc-auth","entity_b":"svc-gateway"}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });

    descriptors.push(CommandDescriptor {
        name: "custom_topology_knn",
        category: "topology_custom",
        description: "K-nearest-neighbors search in a custom topology",
        params_schema: json!({
            "type": "object",
            "properties": {
                "topology": {"type": "string", "description": "Topology name"},
                "entity_id": {"type": "string", "description": "Reference entity identifier"},
                "k": {"type": "integer", "description": "Number of nearest neighbors to return"}
            },
            "required": ["topology", "entity_id", "k"]
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "results": {"type": "array", "description": "Array of k nearest entities with distances"},
                "k": {"type": "integer", "description": "Requested neighbor count"}
            }
        }),
        example: r#"{"cmd":"custom_topology_knn","params":{"topology":"services","entity_id":"svc-auth","k":5}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });

    descriptors.push(CommandDescriptor {
        name: "custom_topology_similarity",
        category: "topology_custom",
        description: "Similarity search in a custom topology",
        params_schema: json!({
            "type": "object",
            "properties": {
                "topology": {"type": "string", "description": "Topology name"},
                "entity_id": {"type": "string", "description": "Reference entity identifier"},
                "threshold": {"type": "number", "description": "Similarity threshold (0.0-1.0)"}
            },
            "required": ["topology", "entity_id"]
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "results": {"type": "array", "description": "Entities within similarity threshold"},
                "count": {"type": "integer", "description": "Number of matches"}
            }
        }),
        example: r#"{"cmd":"custom_topology_similarity","params":{"topology":"services","entity_id":"svc-auth","threshold":0.8}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });
}

#[derive(Debug, Deserialize)]
struct CustomTopologyDiscoverParams {
    topology_key: String,
    requirements: serde_json::Value,
    #[serde(default = "default_max_results")]
    max_results: usize,
    #[serde(default = "default_true")]
    include_metadata: bool,
}

fn default_max_results() -> usize { 10 }
fn default_true() -> bool { true }

/// Parameters for custom topology distance calculation
#[derive(Debug, Deserialize)]
struct CustomTopologyDistanceParams {
    topology_key: String,
    point1: Vec<f64>,
    point2: Vec<f64>,
}

/// Parameters for custom topology KNN search
#[derive(Debug, Deserialize)]
struct CustomTopologyKnnParams {
    topology_key: String,
    query_point: Vec<f64>,
    #[serde(default = "default_k")]
    k: usize,
}

fn default_k() -> usize { 5 }

/// Parameters for custom topology similarity calculation
#[derive(Debug, Deserialize)]
struct CustomTopologySimilarityParams {
    topology_key: String,
    entity_id_1: String,
    entity_id_2: String,
}

/// Async handler for custom topology discovery using Rust Q64.64 precision
///
/// Loads topology from ValKey, performs discovery with fixed-point math,
/// returns cluster-safe deterministic results.
pub fn handle_custom_topology_discover_async<'a>(
    command: &'a Command,
    conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    _site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling custom_topology_discover with Q64.64 precision");
        }

        let params: CustomTopologyDiscoverParams = match serde_json::from_value(command.parameters.clone()) {
            Ok(p) => p,
            Err(e) => return CommandResult::error(format!("Invalid parameters: {}", e)),
        };

        // Load topology from ValKey
        let topology_json: redis::RedisResult<String> = redis::cmd("GET")
            .arg(&params.topology_key)
            .query_async(conn)
            .await;

        let topology_data = match topology_json {
            Ok(json) => json,
            Err(e) => return CommandResult::error(format!("Failed to load topology: {}", e)),
        };

        // Parse into CustomTopology
        let custom_topology = match crate::custom_topology::CustomTopology::from_json(&topology_data) {
            Ok(t) => t,
            Err(e) => return CommandResult::error(format!("Invalid topology data: {}", e)),
        };

        // Perform discovery with Q64.64 precision
        let results = custom_topology.discover_precise(
            &params.requirements,
            params.max_results,
            params.include_metadata,
        );

        CommandResult::success(json!({
            "total_matches": results.len(),
            "results": results,
            "precision": "Q64.64",
            "cluster_safe": true
        }))
    })
}

/// Async handler for custom topology distance using Rust Q64.64 precision
pub fn handle_custom_topology_distance_async<'a>(
    command: &'a Command,
    conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    _site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling custom_topology_distance with Q64.64 precision");
        }

        let params: CustomTopologyDistanceParams = match serde_json::from_value(command.parameters.clone()) {
            Ok(p) => p,
            Err(e) => return CommandResult::error(format!("Invalid parameters: {}", e)),
        };

        if params.point1.len() != params.point2.len() {
            return CommandResult::error(format!(
                "Points must have same dimensions: {} vs {}",
                params.point1.len(), params.point2.len()
            ));
        }

        // Load topology from ValKey for context (optional, mainly for dimension validation)
        let topology_json: redis::RedisResult<String> = redis::cmd("GET")
            .arg(&params.topology_key)
            .query_async(conn)
            .await;

        let custom_topology = match topology_json {
            Ok(json) => crate::custom_topology::CustomTopology::from_json(&json).ok(),
            Err(_) => None,
        };

        // Calculate distance using Q64.64 fixed-point
        let distance = if let Some(topology) = custom_topology {
            topology.distance_precise(&params.point1, &params.point2)
        } else {
            // Fallback: create minimal topology for calculation
            let temp = crate::custom_topology::CustomTopology {
                dimensions: params.point1.len(),
                capability_dimensions: std::collections::HashMap::new(),
                query_types: std::collections::HashMap::new(),
                values: std::collections::HashMap::new(),
                services: std::collections::HashMap::new(),
                metadata: serde_json::Value::Null,
                schema_version: None,
            };
            temp.distance_precise(&params.point1, &params.point2)
        };

        CommandResult::success(json!({
            "distance": distance,
            "dimensions": params.point1.len(),
            "precision": "Q64.64",
            "cluster_safe": true
        }))
    })
}

/// Async handler for custom topology KNN search using Rust Q64.64 precision
pub fn handle_custom_topology_knn_async<'a>(
    command: &'a Command,
    conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    _site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling custom_topology_knn with Q64.64 precision");
        }

        let params: CustomTopologyKnnParams = match serde_json::from_value(command.parameters.clone()) {
            Ok(p) => p,
            Err(e) => return CommandResult::error(format!("Invalid parameters: {}", e)),
        };

        // Load topology from ValKey
        let topology_json: redis::RedisResult<String> = redis::cmd("GET")
            .arg(&params.topology_key)
            .query_async(conn)
            .await;

        let topology_data = match topology_json {
            Ok(json) => json,
            Err(e) => return CommandResult::error(format!("Failed to load topology: {}", e)),
        };

        // Parse into CustomTopology
        let custom_topology = match crate::custom_topology::CustomTopology::from_json(&topology_data) {
            Ok(t) => t,
            Err(e) => return CommandResult::error(format!("Invalid topology data: {}", e)),
        };

        // Perform KNN search with Q64.64 precision
        let results = custom_topology.knn_precise(&params.query_point, params.k);

        CommandResult::success(json!({
            "k": params.k,
            "results": results,
            "precision": "Q64.64",
            "cluster_safe": true
        }))
    })
}

/// Async handler for custom topology similarity using Rust Q64.64 precision
pub fn handle_custom_topology_similarity_async<'a>(
    command: &'a Command,
    conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    _site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling custom_topology_similarity with Q64.64 precision");
        }

        let params: CustomTopologySimilarityParams = match serde_json::from_value(command.parameters.clone()) {
            Ok(p) => p,
            Err(e) => return CommandResult::error(format!("Invalid parameters: {}", e)),
        };

        // Load topology from ValKey
        let topology_json: redis::RedisResult<String> = redis::cmd("GET")
            .arg(&params.topology_key)
            .query_async(conn)
            .await;

        let topology_data = match topology_json {
            Ok(json) => json,
            Err(e) => return CommandResult::error(format!("Failed to load topology: {}", e)),
        };

        // Parse into CustomTopology
        let custom_topology = match crate::custom_topology::CustomTopology::from_json(&topology_data) {
            Ok(t) => t,
            Err(e) => return CommandResult::error(format!("Invalid topology data: {}", e)),
        };

        // Get entity points
        let entity1 = custom_topology.services.get(&params.entity_id_1);
        let entity2 = custom_topology.services.get(&params.entity_id_2);

        match (entity1, entity2) {
            (Some(e1), Some(e2)) => {
                let distance = custom_topology.distance_precise(&e1.point, &e2.point);
                let similarity = custom_topology.similarity_precise(&e1.point, &e2.point);

                CommandResult::success(json!({
                    "entity_1": params.entity_id_1,
                    "entity_2": params.entity_id_2,
                    "distance": distance,
                    "similarity": similarity,
                    "precision": "Q64.64",
                    "cluster_safe": true
                }))
            }
            (None, _) => CommandResult::error(format!("Entity not found: {}", params.entity_id_1)),
            (_, None) => CommandResult::error(format!("Entity not found: {}", params.entity_id_2)),
        }
    })
}

pub fn handle_custom_topology_discover(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    _site_id: &str,
    debug_mode: bool,
) -> CommandResult {
    if debug_mode { debug!("Handling custom_topology_discover command"); }
    let params: CustomTopologyDiscoverParams = match serde_json::from_value(command.parameters.clone()) {
        Ok(p) => p,
        Err(e) => return CommandResult::error(format!("Invalid parameters: {}", e)),
    };
    // Load topology from ValKey
    let topology_json: redis::RedisResult<String> = redis::cmd("GET")
        .arg(&params.topology_key)
        .query(conn);
    let topology_data = match topology_json {
        Ok(json) => json,
        Err(e) => return CommandResult::error(format!("Failed to load topology: {}", e)),
    };
    // Parse into CustomTopology
    let custom_topology = match crate::custom_topology::CustomTopology::from_json(&topology_data) {
        Ok(t) => t,
        Err(e) => return CommandResult::error(format!("Invalid topology data: {}", e)),
    };
    // Perform discovery with Q64.64 precision
    let results = custom_topology.discover_precise(&params.requirements, params.max_results, params.include_metadata);
    CommandResult::success(json!({
        "results": results,
        "count": results.len(),
        "precision": "Q64.64",
        "cluster_safe": true
    }))
}

/// Sync version of handle_custom_topology_distance
pub fn handle_custom_topology_distance(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    _site_id: &str,
    debug_mode: bool,
) -> CommandResult {
    if debug_mode { debug!("Handling custom_topology_distance command"); }
    let params: CustomTopologyDistanceParams = match serde_json::from_value(command.parameters.clone()) {
        Ok(p) => p,
        Err(e) => return CommandResult::error(format!("Invalid parameters: {}", e)),
    };
    if params.point1.len() != params.point2.len() {
        return CommandResult::error(format!(
            "Points must have same dimensions: {} vs {}",
            params.point1.len(), params.point2.len()
        ));
    }
    // Load topology from ValKey for context (optional)
    let topology_json: redis::RedisResult<String> = redis::cmd("GET")
        .arg(&params.topology_key)
        .query(conn);
    let custom_topology = match topology_json {
        Ok(json) => crate::custom_topology::CustomTopology::from_json(&json).ok(),
        Err(_) => None,
    };
    // Calculate distance using Q64.64 fixed-point
    let distance = if let Some(topology) = custom_topology {
        topology.distance_precise(&params.point1, &params.point2)
    } else {
        // Fallback: create minimal topology for calculation
        let temp = crate::custom_topology::CustomTopology {
            dimensions: params.point1.len(),
            capability_dimensions: std::collections::HashMap::new(),
            query_types: std::collections::HashMap::new(),
            values: std::collections::HashMap::new(),
            services: std::collections::HashMap::new(),
            metadata: serde_json::Value::Null,
            schema_version: None,
        };
        temp.distance_precise(&params.point1, &params.point2)
    };
    CommandResult::success(json!({
        "distance": distance,
        "dimensions": params.point1.len(),
        "precision": "Q64.64",
        "cluster_safe": true
    }))
}

/// Sync version of handle_custom_topology_knn
pub fn handle_custom_topology_knn(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    _site_id: &str,
    debug_mode: bool,
) -> CommandResult {
    if debug_mode { debug!("Handling custom_topology_knn command"); }
    let params: CustomTopologyKnnParams = match serde_json::from_value(command.parameters.clone()) {
        Ok(p) => p,
        Err(e) => return CommandResult::error(format!("Invalid parameters: {}", e)),
    };
    // Load topology from ValKey
    let topology_json: redis::RedisResult<String> = redis::cmd("GET")
        .arg(&params.topology_key)
        .query(conn);
    let topology_data = match topology_json {
        Ok(json) => json,
        Err(e) => return CommandResult::error(format!("Failed to load topology: {}", e)),
    };
    // Parse into CustomTopology
    let custom_topology = match crate::custom_topology::CustomTopology::from_json(&topology_data) {
        Ok(t) => t,
        Err(e) => return CommandResult::error(format!("Invalid topology data: {}", e)),
    };
    // Perform KNN search with Q64.64 precision
    let results = custom_topology.knn_precise(&params.query_point, params.k);
    CommandResult::success(json!({
        "k": params.k,
        "results": results,
        "precision": "Q64.64",
        "cluster_safe": true
    }))
}

/// Sync version of handle_custom_topology_similarity
pub fn handle_custom_topology_similarity(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    _site_id: &str,
    debug_mode: bool,
) -> CommandResult {
    if debug_mode { debug!("Handling custom_topology_similarity command"); }
    let params: CustomTopologySimilarityParams = match serde_json::from_value(command.parameters.clone()) {
        Ok(p) => p,
        Err(e) => return CommandResult::error(format!("Invalid parameters: {}", e)),
    };
    // Load topology from ValKey
    let topology_json: redis::RedisResult<String> = redis::cmd("GET")
        .arg(&params.topology_key)
        .query(conn);
    let topology_data = match topology_json {
        Ok(json) => json,
        Err(e) => return CommandResult::error(format!("Failed to load topology: {}", e)),
    };
    // Parse into CustomTopology
    let custom_topology = match crate::custom_topology::CustomTopology::from_json(&topology_data) {
        Ok(t) => t,
        Err(e) => return CommandResult::error(format!("Invalid topology data: {}", e)),
    };
    // Look up entities by ID to get their coordinates
    let entity1 = match custom_topology.services.get(&params.entity_id_1) {
        Some(e) => e,
        None => return CommandResult::error(format!("Entity not found: {}", params.entity_id_1)),
    };
    let entity2 = match custom_topology.services.get(&params.entity_id_2) {
        Some(e) => e,
        None => return CommandResult::error(format!("Entity not found: {}", params.entity_id_2)),
    };
    // Calculate similarity with Q64.64 precision using entity point coordinates
    let similarity = custom_topology.similarity_precise(&entity1.point, &entity2.point);
    CommandResult::success(json!({
        "entity_id_1": params.entity_id_1,
        "entity_id_2": params.entity_id_2,
        "similarity": similarity,
        "precision": "Q64.64",
        "cluster_safe": true
    }))
}
