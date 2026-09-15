// Custom Topology Command Handlers (Rust Q64.64 Precision)
//
// Handles: custom_topology_discover, custom_topology_distance, custom_topology_knn,
//          custom_topology_similarity
// A custom topology is one JSON document its owner stores with SET at `topology_key`
// (format: COMMAND_SCHEMA.md). Both lanes share one compute per command.

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
use crate::custom_topology::{fixed_distance, CustomTopology};
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
        description: "Filter and rank the entities of a custom topology document by per-dimension requirements",
        params_schema: json!({
            "type": "object",
            "properties": {
                "topology_key": {"type": "string", "description": "Key of the topology document"},
                "requirements": {"type": "object", "description": "Dimension name → exact value, {min}, {max} or {min, max}; values may be names from the document's `values` map"},
                "max_results": {"type": "integer", "default": 10, "description": "Maximum results to return"},
                "include_metadata": {"type": "boolean", "default": true, "description": "Include each entity's metadata"}
            },
            "required": ["topology_key", "requirements"]
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "total_matches": {"type": "integer", "description": "Results returned"},
                "results": {"type": "array", "description": "{id, score, distance, point, metadata?}: highest score first, then nearest the origin, then id"},
                "precision": {"type": "string"},
                "cluster_safe": {"type": "boolean"}
            }
        }),
        example: r#"{"cmd":"custom_topology_discover","params":{"topology_key":"{mysite}:signals","requirements":{"regime":"range_bound","confidence":{"min":0.6}},"max_results":5}}"#,
        async_capable: true,
        lane: Lane::Concurrent,
    });

    descriptors.push(CommandDescriptor {
        name: "custom_topology_distance",
        category: "topology_custom",
        description: "Q64.64 Euclidean distance between two points; reads no stored data",
        params_schema: json!({
            "type": "object",
            "properties": {
                "topology_key": {"type": "string", "description": "Accepted for symmetry with the other custom-topology commands; not read"},
                "point1": {"type": "array", "items": {"type": "number"}, "description": "First point"},
                "point2": {"type": "array", "items": {"type": "number"}, "description": "Second point, same width"}
            },
            "required": ["point1", "point2"]
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "distance": {"type": "number"},
                "dimensions": {"type": "integer"},
                "precision": {"type": "string"},
                "cluster_safe": {"type": "boolean"}
            }
        }),
        example: r#"{"cmd":"custom_topology_distance","params":{"topology_key":"{mysite}:signals","point1":[0.5,0.7],"point2":[0.3,0.9]}}"#,
        async_capable: true,
        lane: Lane::Concurrent,
    });

    descriptors.push(CommandDescriptor {
        name: "custom_topology_knn",
        category: "topology_custom",
        description: "The k entities nearest a query point in a custom topology document",
        params_schema: json!({
            "type": "object",
            "properties": {
                "topology_key": {"type": "string", "description": "Key of the topology document"},
                "query_point": {"type": "array", "items": {"type": "number"}, "description": "Point with the document's dimension count"},
                "k": {"type": "integer", "default": 5, "description": "Number of neighbours"}
            },
            "required": ["topology_key", "query_point"]
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "k": {"type": "integer"},
                "results": {"type": "array", "description": "{id, score, distance, point, metadata}: nearest first, equal distances by id"},
                "precision": {"type": "string"},
                "cluster_safe": {"type": "boolean"}
            }
        }),
        example: r#"{"cmd":"custom_topology_knn","params":{"topology_key":"{mysite}:signals","query_point":[0.5,0.6,0.67,0.5],"k":3}}"#,
        async_capable: true,
        lane: Lane::Concurrent,
    });

    descriptors.push(CommandDescriptor {
        name: "custom_topology_similarity",
        category: "topology_custom",
        description: "Distance and similarity (1 / (1 + distance)) between two entities of a custom topology document",
        params_schema: json!({
            "type": "object",
            "properties": {
                "topology_key": {"type": "string", "description": "Key of the topology document"},
                "entity_id_1": {"type": "string", "description": "First entity"},
                "entity_id_2": {"type": "string", "description": "Second entity"}
            },
            "required": ["topology_key", "entity_id_1", "entity_id_2"]
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "entity_id_1": {"type": "string"},
                "entity_id_2": {"type": "string"},
                "distance": {"type": "number"},
                "similarity": {"type": "number"},
                "precision": {"type": "string"},
                "cluster_safe": {"type": "boolean"}
            }
        }),
        example: r#"{"cmd":"custom_topology_similarity","params":{"topology_key":"{mysite}:signals","entity_id_1":"s01","entity_id_2":"s02"}}"#,
        async_capable: true,
        lane: Lane::Concurrent,
    });
}

#[derive(Debug, Deserialize)]
#[cfg_attr(test, derive(serde::Serialize, Default))]
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

#[derive(Debug, Deserialize)]
#[cfg_attr(test, derive(serde::Serialize, Default))]
struct CustomTopologyDistanceParams {
    #[serde(default)]
    #[allow(dead_code)]
    topology_key: String,
    point1: Vec<f64>,
    point2: Vec<f64>,
}

#[derive(Debug, Deserialize)]
#[cfg_attr(test, derive(serde::Serialize, Default))]
struct CustomTopologyKnnParams {
    topology_key: String,
    query_point: Vec<f64>,
    #[serde(default = "default_k")]
    k: usize,
}

fn default_k() -> usize { 5 }

#[derive(Debug, Deserialize)]
#[cfg_attr(test, derive(serde::Serialize, Default))]
struct CustomTopologySimilarityParams {
    topology_key: String,
    entity_id_1: String,
    entity_id_2: String,
}

// =========================================================================
// Compute shared by both lanes
// =========================================================================

fn parse<T: for<'de> Deserialize<'de>>(command: &Command) -> Result<T, CommandResult> {
    serde_json::from_value(command.parameters.clone())
        .map_err(|e| CommandResult::error(format!("Invalid parameters: {}", e)))
}

fn load(topology_key: &str, stored: Option<String>) -> Result<CustomTopology, CommandResult> {
    let json = stored.ok_or_else(|| CommandResult::error(format!("No custom topology stored at {}", topology_key)))?;
    CustomTopology::from_json(&json)
        .map_err(|e| CommandResult::error(format!("Invalid topology document at {}: {}", topology_key, e)))
}

fn discover(p: &CustomTopologyDiscoverParams, stored: Option<String>) -> CommandResult {
    let topology = match load(&p.topology_key, stored) { Ok(t) => t, Err(e) => return e };
    match topology.discover_precise(&p.requirements, p.max_results, p.include_metadata) {
        Ok(results) => CommandResult::success(json!({
            "total_matches": results.len(),
            "results": results,
            "precision": "Q64.64",
            "cluster_safe": true
        })),
        Err(e) => CommandResult::error(e),
    }
}

fn distance(p: &CustomTopologyDistanceParams) -> CommandResult {
    if p.point1.len() != p.point2.len() {
        return CommandResult::error(format!(
            "Points must have same dimensions: {} vs {}",
            p.point1.len(), p.point2.len()
        ));
    }
    CommandResult::success(json!({
        "distance": fixed_distance(&p.point1, &p.point2),
        "dimensions": p.point1.len(),
        "precision": "Q64.64",
        "cluster_safe": true
    }))
}

fn knn(p: &CustomTopologyKnnParams, stored: Option<String>) -> CommandResult {
    let topology = match load(&p.topology_key, stored) { Ok(t) => t, Err(e) => return e };
    match topology.knn_precise(&p.query_point, p.k) {
        Ok(results) => CommandResult::success(json!({
            "k": p.k,
            "results": results,
            "precision": "Q64.64",
            "cluster_safe": true
        })),
        Err(e) => CommandResult::error(e),
    }
}

fn similarity(p: &CustomTopologySimilarityParams, stored: Option<String>) -> CommandResult {
    let topology = match load(&p.topology_key, stored) { Ok(t) => t, Err(e) => return e };
    let point = |id: &str| {
        topology.services.get(id).map(|e| e.point.clone()).ok_or_else(|| format!("Entity not found: {}", id))
    };
    let (a, b) = match (point(&p.entity_id_1), point(&p.entity_id_2)) {
        (Ok(a), Ok(b)) => (a, b),
        (Err(e), _) | (_, Err(e)) => return CommandResult::error(e),
    };
    CommandResult::success(json!({
        "entity_id_1": p.entity_id_1,
        "entity_id_2": p.entity_id_2,
        "distance": fixed_distance(&a, &b),
        "similarity": topology.similarity_precise(&a, &b),
        "precision": "Q64.64",
        "cluster_safe": true
    }))
}

async fn stored_async(conn: &mut AsyncConnection, key: &str) -> Result<Option<String>, CommandResult> {
    redis::cmd("GET").arg(key).query_async(conn).await
        .map_err(|e| CommandResult::error(format!("Failed to load topology {}: {}", key, e)))
}

fn stored_sync(conn: &mut Connection, key: &str) -> Result<Option<String>, CommandResult> {
    redis::cmd("GET").arg(key).query(conn)
        .map_err(|e| CommandResult::error(format!("Failed to load topology {}: {}", key, e)))
}

// =========================================================================
// Async handlers
// =========================================================================

pub fn handle_custom_topology_discover_async<'a>(
    command: &'a Command,
    conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    _site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode { debug!("Handling custom_topology_discover"); }
        let p: CustomTopologyDiscoverParams = match parse(command) { Ok(p) => p, Err(e) => return e };
        match stored_async(conn, &p.topology_key).await { Ok(s) => discover(&p, s), Err(e) => e }
    })
}

pub fn handle_custom_topology_distance_async<'a>(
    command: &'a Command,
    _conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    _site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode { debug!("Handling custom_topology_distance"); }
        match parse::<CustomTopologyDistanceParams>(command) { Ok(p) => distance(&p), Err(e) => e }
    })
}

pub fn handle_custom_topology_knn_async<'a>(
    command: &'a Command,
    conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    _site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode { debug!("Handling custom_topology_knn"); }
        let p: CustomTopologyKnnParams = match parse(command) { Ok(p) => p, Err(e) => return e };
        match stored_async(conn, &p.topology_key).await { Ok(s) => knn(&p, s), Err(e) => e }
    })
}

pub fn handle_custom_topology_similarity_async<'a>(
    command: &'a Command,
    conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    _site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode { debug!("Handling custom_topology_similarity"); }
        let p: CustomTopologySimilarityParams = match parse(command) { Ok(p) => p, Err(e) => return e };
        match stored_async(conn, &p.topology_key).await { Ok(s) => similarity(&p, s), Err(e) => e }
    })
}

// =========================================================================
// Sync handlers
// =========================================================================

pub fn handle_custom_topology_discover(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    _site_id: &str,
    debug_mode: bool,
) -> CommandResult {
    if debug_mode { debug!("Handling custom_topology_discover command"); }
    let p: CustomTopologyDiscoverParams = match parse(command) { Ok(p) => p, Err(e) => return e };
    match stored_sync(conn, &p.topology_key) { Ok(s) => discover(&p, s), Err(e) => e }
}

pub fn handle_custom_topology_distance(
    command: &Command,
    _conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    _site_id: &str,
    debug_mode: bool,
) -> CommandResult {
    if debug_mode { debug!("Handling custom_topology_distance command"); }
    match parse::<CustomTopologyDistanceParams>(command) { Ok(p) => distance(&p), Err(e) => e }
}

pub fn handle_custom_topology_knn(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    _site_id: &str,
    debug_mode: bool,
) -> CommandResult {
    if debug_mode { debug!("Handling custom_topology_knn command"); }
    let p: CustomTopologyKnnParams = match parse(command) { Ok(p) => p, Err(e) => return e };
    match stored_sync(conn, &p.topology_key) { Ok(s) => knn(&p, s), Err(e) => e }
}

pub fn handle_custom_topology_similarity(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    _site_id: &str,
    debug_mode: bool,
) -> CommandResult {
    if debug_mode { debug!("Handling custom_topology_similarity command"); }
    let p: CustomTopologySimilarityParams = match parse(command) { Ok(p) => p, Err(e) => return e };
    match stored_sync(conn, &p.topology_key) { Ok(s) => similarity(&p, s), Err(e) => e }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::types::assert_descriptor_fields;

    fn descriptors() -> Vec<CommandDescriptor> {
        let (mut handlers, mut async_handlers, mut descriptors) = (HashMap::new(), HashMap::new(), Vec::new());
        register(&mut handlers, &mut async_handlers, &mut descriptors);
        descriptors
    }

    #[test]
    fn descriptors_name_exactly_the_fields_the_parsers_read() {
        let d = descriptors();
        assert_descriptor_fields::<CustomTopologyDiscoverParams>(&d, "custom_topology_discover", true);
        assert_descriptor_fields::<CustomTopologyDistanceParams>(&d, "custom_topology_distance", true);
        assert_descriptor_fields::<CustomTopologyKnnParams>(&d, "custom_topology_knn", true);
        assert_descriptor_fields::<CustomTopologySimilarityParams>(&d, "custom_topology_similarity", true);
    }

    #[test]
    fn a_missing_document_is_an_error_that_names_its_key() {
        let p = CustomTopologyDiscoverParams { topology_key: "{s}:t".into(), requirements: json!({}), max_results: 10, include_metadata: false };
        assert!(discover(&p, None).error.unwrap().contains("{s}:t"));
    }

    #[test]
    fn similarity_reports_the_distance_it_scored() {
        let doc = json!({
            "dimensions": 2,
            "services": {
                "a": {"id": "a", "point": [0.0, 0.0]},
                "b": {"id": "b", "point": [0.0, 1.0]}
            }
        }).to_string();
        let p = CustomTopologySimilarityParams { topology_key: "k".into(), entity_id_1: "a".into(), entity_id_2: "b".into() };
        let r = similarity(&p, Some(doc)).result.unwrap();
        assert_eq!(r["entity_id_1"], "a");
        assert!((r["distance"].as_f64().unwrap() - 1.0).abs() < 1e-9);
        assert!((r["similarity"].as_f64().unwrap() - 0.5).abs() < 1e-9);
    }
}
