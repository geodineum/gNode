// Geometric Command Handlers
//
// Handles: geometric_discover, geometric_discover_range, geometric_store_topology,
//          geometric_load_sequence, geometric_distance, geometric_dimensions
// These implement O(1) spatial-hash discovery with Q64.64 determinism.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use std::pin::Pin;
use std::future::Future;
use redis::Connection;
use redis::aio::MultiplexedConnection as AsyncConnection;
use serde::Deserialize;
use log::{debug, warn};
use serde_json::{Value, json};
use crate::daemon::Command;
use crate::GeometricTopology;
use crate::integration::valkey_functions::execute_function;

use super::types::{
    CommandResult, CommandHandlerFn, AsyncCommandHandlerFn, CommandDescriptor,
    parse_parameters, get_service_dimensions, discovery_point_from_full,
    TOTAL_DIMENSIONS, default_group, Lane,
};

/// Register all geometric command handlers
pub fn register(
    handlers: &mut HashMap<String, CommandHandlerFn>,
    async_handlers: &mut HashMap<String, AsyncCommandHandlerFn>,
    descriptors: &mut Vec<CommandDescriptor>,
) {
    // Sync handlers
    handlers.insert("geometric_discover".to_string(), handle_geometric_discover as CommandHandlerFn);
    handlers.insert("geometric_discover_range".to_string(), handle_geometric_discover_range as CommandHandlerFn);
    handlers.insert("geometric_store_topology".to_string(), handle_geometric_store_topology as CommandHandlerFn);
    handlers.insert("geometric_load_sequence".to_string(), handle_geometric_load_sequence as CommandHandlerFn);
    handlers.insert("geometric_distance".to_string(), handle_geometric_distance as CommandHandlerFn);
    handlers.insert("geometric_dimensions".to_string(), handle_geometric_dimensions as CommandHandlerFn);

    // Async handlers - Phase 2 hot-path
    async_handlers.insert("discover".to_string(), handle_geometric_discover_async as AsyncCommandHandlerFn);
    async_handlers.insert("DISCOVER".to_string(), handle_geometric_discover_async as AsyncCommandHandlerFn);
    async_handlers.insert("geometric_discover".to_string(), handle_geometric_discover_async as AsyncCommandHandlerFn);
    async_handlers.insert("geo_disc".to_string(), handle_geometric_discover_async as AsyncCommandHandlerFn);

    // Async handlers - Phase 5 complete coverage
    async_handlers.insert("geometric_store_topology".to_string(), handle_geometric_store_topology_async as AsyncCommandHandlerFn);
    async_handlers.insert("GEOMETRIC_STORE_TOPOLOGY".to_string(), handle_geometric_store_topology_async as AsyncCommandHandlerFn);
    async_handlers.insert("geometric_load_sequence".to_string(), handle_geometric_load_sequence_async as AsyncCommandHandlerFn);
    async_handlers.insert("GEOMETRIC_LOAD_SEQUENCE".to_string(), handle_geometric_load_sequence_async as AsyncCommandHandlerFn);
    async_handlers.insert("geometric_distance".to_string(), handle_geometric_distance_async as AsyncCommandHandlerFn);
    async_handlers.insert("GEOMETRIC_DISTANCE".to_string(), handle_geometric_distance_async as AsyncCommandHandlerFn);
    async_handlers.insert("geometric_dimensions".to_string(), handle_geometric_dimensions_async as AsyncCommandHandlerFn);
    async_handlers.insert("GEOMETRIC_DIMENSIONS".to_string(), handle_geometric_dimensions_async as AsyncCommandHandlerFn);
    async_handlers.insert("geometric_discover_range".to_string(), handle_geometric_discover_range_async as AsyncCommandHandlerFn);
    async_handlers.insert("GEOMETRIC_DISCOVER_RANGE".to_string(), handle_geometric_discover_range_async as AsyncCommandHandlerFn);

    // Command descriptors
    descriptors.push(CommandDescriptor {
        name: "geometric_discover",
        category: "geometric",
        description: "O(1) spatial-hash service discovery by capability vector matching",
        params_schema: json!({
            "type": "object",
            "properties": {
                "capabilities": {
                    "type": "object",
                    "description": "Capability dimension names mapped to values (0.0-1.0)",
                    "additionalProperties": {"type": "number", "minimum": 0.0, "maximum": 1.0}
                },
                "limit": {"type": "integer", "default": 10, "description": "Maximum number of results"},
                "threshold": {"type": "number", "description": "Optional distance threshold for filtering"}
            },
            "required": ["capabilities"]
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "total_matches": {"type": "integer"},
                "results": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "service_id": {"type": "string"},
                            "distance": {"type": "number"}
                        }
                    }
                }
            }
        }),
        example: r#"{"cmd":"geometric_discover","params":{"capabilities":{"compute":0.8,"memory":0.5},"limit":10}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });

    descriptors.push(CommandDescriptor {
        name: "geometric_discover_range",
        category: "geometric",
        description: "Range-based discovery with min/max bounds per dimension",
        params_schema: json!({
            "type": "object",
            "properties": {
                "capabilities": {
                    "type": "object",
                    "description": "Capability requirements with range operators",
                    "additionalProperties": {"type": "number"}
                },
                "range": {"type": "number", "description": "Search radius"},
                "limit": {"type": "integer", "description": "Maximum number of results"}
            },
            "required": ["capabilities"]
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "services": {"type": "array"},
                "count": {"type": "integer"}
            }
        }),
        example: r#"{"cmd":"geometric_discover_range","params":{"capabilities":{"compute":0.5},"range":0.3,"limit":5}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });

    descriptors.push(CommandDescriptor {
        name: "geometric_store_topology",
        category: "geometric",
        description: "Store or update the service topology data",
        params_schema: json!({
            "type": "object",
            "properties": {
                "topology": {"type": "object", "description": "The topology data to store"}
            },
            "required": ["topology"]
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "stored": {"type": "boolean"}
            }
        }),
        example: r#"{"cmd":"geometric_store_topology","params":{"topology":{"services":{}}}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });

    descriptors.push(CommandDescriptor {
        name: "geometric_load_sequence",
        category: "geometric",
        description: "Load a sequence of points into the topology",
        params_schema: json!({
            "type": "object",
            "properties": {
                "points": {
                    "type": "array",
                    "items": {"type": "object"},
                    "description": "Array of point objects to load"
                }
            },
            "required": ["points"]
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "loaded": {"type": "integer", "description": "Number of points loaded"}
            }
        }),
        example: r#"{"cmd":"geometric_load_sequence","params":{"points":[{"id":"svc1","coords":[0.5,0.3]}]}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });

    descriptors.push(CommandDescriptor {
        name: "geometric_distance",
        category: "geometric",
        description: "Calculate Q64.64 fixed-point distance between two capability vectors",
        params_schema: json!({
            "type": "object",
            "properties": {
                "point_a": {
                    "type": "object",
                    "description": "First capability vector",
                    "additionalProperties": {"type": "number"}
                },
                "point_b": {
                    "type": "object",
                    "description": "Second capability vector",
                    "additionalProperties": {"type": "number"}
                }
            },
            "required": ["point_a", "point_b"]
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "distance": {"type": "number"}
            }
        }),
        example: r#"{"cmd":"geometric_distance","params":{"point_a":{"compute":0.8},"point_b":{"compute":0.3}}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });

    descriptors.push(CommandDescriptor {
        name: "geometric_dimensions",
        category: "geometric",
        description: "Get configured dimension names and count",
        params_schema: json!({"type": "object", "properties": {}}),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "dimensions": {"type": "object", "description": "Map of dimension names to indices"}
            }
        }),
        example: r#"{"cmd":"geometric_dimensions","params":{}}"#,
        async_capable: true,
        lane: Lane::Fast,
    });
}

// =========================================================================
// Parameter structs
// =========================================================================

/// Parameters for the geometric_discover command
/// Supports two formats for capabilities:
///   1. capabilities: ["compute", "memory"] - names only (sets to 1.0)
///   2. capability_values: {"compute": 0.8, "memory": 0.5} - precise values
#[derive(Debug, Deserialize)]
struct GeometricDiscoverParams {
    /// List of capability names (set to 1.0 when querying)
    #[serde(default)]
    capabilities: Vec<String>,
    /// Actual capability values for precise matching
    #[serde(default)]
    capability_values: HashMap<String, f64>,
    /// Limit on number of results (default: 10)
    #[serde(default)]
    limit: usize,
    /// Maximum distance threshold for filtering
    #[serde(default)]
    distance: f64,
    /// Alias for limit
    #[serde(default)]
    dimensions: usize,
}

/// Parameters for the geometric_discover_range command (Phase 1 - Dynamic Dimensions)
#[derive(Debug, Deserialize)]
struct GeometricDiscoverRangeParams {
    /// Map of dimension index to requirement operators
    /// Format: { "8": {"eq": 0.10}, "9": {"gt": 100} }
    requirements: HashMap<String, HashMap<String, f64>>,
}

/// Parameters for the geometric_store_topology command (retired — kept for the
/// deprecation-no-op parse so legacy callers still validate against the old shape).
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct GeometricStoreTopologyParams {
    data: Value,
}

/// Parameters for the geometric_load_sequence command
#[derive(Debug, Deserialize)]
struct GeometricLoadSequenceParams {
    #[serde(default = "default_group")]
    #[allow(dead_code)]
    group: String,
}

/// Parameters for the geometric_distance command
#[derive(Debug, Deserialize)]
struct GeometricDistanceParams {
    point1: Vec<f64>,
    point2: Vec<f64>,
}

// =========================================================================
// Sync handlers
// =========================================================================

/// Handle 'geometric_discover' command
pub fn handle_geometric_discover(
    command: &Command,
    _conn: &mut Connection,
    topology: &Arc<RwLock<GeometricTopology>>,
    _site_id: &str,
    debug_mode: bool
) -> CommandResult {
    if debug_mode {
        debug!("Handling geometric_discover command: {}", command.id);
    }

    // Parse parameters
    let params = match parse_parameters::<GeometricDiscoverParams>(command) {
        Ok(p) => p,
        Err(e) => return CommandResult::error(e),
    };

    // Query in-memory topology for matching services
    match crate::integration::thread_safety::with_arc_read(topology, |topology| {
        topology.discover_service(&params.capabilities, params.dimensions, params.distance)
    }) {
        Ok(result) => match result {
            Ok(services) => CommandResult::success(services),
            Err(e) => CommandResult::error(format!("Failed to discover services: {:?}", e)),
        },
        Err(e) => CommandResult::error(format!("Failed to access topology: {}", e)),
    }
}

/// Filter a GNODE_TOPO_GET_ENTITIES response (`{ents:{id:{pd,...}}}`) by per-dimension
/// range requirements against each entity's `pd` (point_display). Returns matching ids.
fn filter_entities_by_range(entities_json: &str, reqs: &HashMap<String, HashMap<String, f64>>) -> Vec<String> {
    let mut out = Vec::new();
    let v: Value = match serde_json::from_str(entities_json) { Ok(v) => v, Err(_) => return out };
    let ents = match v.get("ents").and_then(|e| e.as_object()) { Some(e) => e, None => return out };
    for (id, data) in ents {
        let pd = data.get("pd").and_then(|p| p.as_array());
        let mut ok = true;
        for (dim_str, ops) in reqs {
            let dim: usize = match dim_str.parse() { Ok(d) => d, Err(_) => { ok = false; break } };
            match pd.and_then(|a| a.get(dim)).and_then(|x| x.as_f64()) {
                Some(val) if range_op_match(ops, val) => {},
                _ => { ok = false; break }
            }
        }
        if ok { out.push(id.clone()); }
    }
    out
}

/// Evaluate one dimension's operator set against a value (eq/neq/gt/gte/lt/lte/range).
fn range_op_match(ops: &HashMap<String, f64>, val: f64) -> bool {
    if let Some(eq) = ops.get("eq") { return (val - *eq).abs() < 1e-6; }
    if let Some(neq) = ops.get("neq") { return (val - *neq).abs() >= 1e-6; }
    if let (Some(gte), Some(lte)) = (ops.get("gte"), ops.get("lte")) { return val >= *gte && val <= *lte; }
    if let Some(gt) = ops.get("gt") { return val > *gt; }
    if let Some(gte) = ops.get("gte") { return val >= *gte; }
    if let Some(lt) = ops.get("lt") { return val < *lt; }
    if let Some(lte) = ops.get("lte") { return val <= *lte; }
    false
}

/// Handle 'geometric_discover_range' — STATELESS: fetch (C) entities + filter by `pd` range.
pub fn handle_geometric_discover_range(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    site_id: &str,
    debug_mode: bool
) -> CommandResult {
    if debug_mode {
        debug!("Handling geometric_discover_range command: {}", command.id);
    }
    let params = match parse_parameters::<GeometricDiscoverRangeParams>(command) {
        Ok(p) => p,
        Err(e) => return CommandResult::error(e),
    };
    let topology_key = GeometricTopology::get_services_topology_key(site_id);
    let result: redis::RedisResult<String> = redis::cmd("FCALL")
        .arg("GNODE_TOPO_GET_ENTITIES").arg(1).arg(&topology_key).arg("*")
        .query(conn);
    match result {
        Ok(json) => CommandResult::success(filter_entities_by_range(&json, &params.requirements)),
        Err(e) => CommandResult::error(format!("Range discovery FCALL failed: {:?}", e)),
    }
}

/// Handle 'geometric_store_topology' command
pub fn handle_geometric_store_topology(
    command: &Command,
    _conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    _site_id: &str,
    debug_mode: bool
) -> CommandResult {
    if debug_mode {
        debug!("Handling geometric_store_topology command: {}", command.id);
    }

    // RETIRED (S6d): bulk in-memory topology store is incompatible with the
    // stateless (C) model — services register individually via
    // GNODE_REGISTER_CAPABILITY_VECTOR. Kept as a deprecation no-op so legacy
    // callers get a clear signal instead of silently mutating dead in-mem state.
    let _ = parse_parameters::<GeometricStoreTopologyParams>(command);
    CommandResult::success(json!({
        "stored": false,
        "deprecated": true,
        "note": "geometric_store_topology is retired; register services individually (stateless (C) topology)"
    }))
}

/// Handle 'geometric_load_sequence' command
pub fn handle_geometric_load_sequence(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    site_id: &str,
    debug_mode: bool
) -> CommandResult {
    if debug_mode {
        debug!("Handling geometric_load_sequence command: {}", command.id);
    }

    let _params = match parse_parameters::<GeometricLoadSequenceParams>(command) {
        Ok(p) => p,
        Err(e) => return CommandResult::error(e),
    };

    // STATELESS: load sequence = z-ordered (C) entities via FCALL (was in-memory).
    let topology_key = GeometricTopology::get_services_topology_key(site_id);
    let result: redis::RedisResult<String> = redis::cmd("FCALL")
        .arg("GNODE_TOPO_Z_ORDER").arg(1).arg(&topology_key)
        .query(conn);
    match result {
        Ok(json) => CommandResult::success(z_order_eids(&json)),
        Err(e) => CommandResult::error(format!("Load sequence FCALL failed: {:?}", e)),
    }
}

/// Extract the ordered entity-id list from a GNODE_TOPO_Z_ORDER response (`{eids:[...]}`).
fn z_order_eids(json: &str) -> Vec<String> {
    serde_json::from_str::<Value>(json).ok()
        .and_then(|v| v.get("eids").and_then(|e| e.as_array()).map(|a|
            a.iter().filter_map(|x| x.as_str().map(String::from)).collect()))
        .unwrap_or_default()
}

/// Handle 'geometric_distance' command
pub fn handle_geometric_distance(
    command: &Command,
    _conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    _site_id: &str,
    debug_mode: bool
) -> CommandResult {
    if debug_mode {
        debug!("Handling geometric_distance command: {}", command.id);
    }

    // Parse parameters
    let params = match parse_parameters::<GeometricDistanceParams>(command) {
        Ok(p) => p,
        Err(e) => return CommandResult::error(e),
    };

    // Check that points have the same dimensions
    if params.point1.len() != params.point2.len() {
        return CommandResult::error(
            format!("Points must have the same dimensions: {} vs {}",
                params.point1.len(), params.point2.len())
        );
    }

    // Calculate Q64.64 Euclidean distance
    let mut sum_sq = 0.0;
    for i in 0..params.point1.len() {
        let diff = params.point1[i] - params.point2[i];
        sum_sq += diff * diff;
    }
    let distance = sum_sq.sqrt();

    CommandResult::success(json!({
        "distance": distance,
        "dimensions": params.point1.len()
    }))
}

/// Handle 'geometric_dimensions' command
pub fn handle_geometric_dimensions(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    site_id: &str,
    debug_mode: bool
) -> CommandResult {
    if debug_mode {
        debug!("Handling geometric_dimensions command: {}", command.id);
    }

    // Execute ValKey function
    match execute_function(
        conn,
        "GNODE_GEOMETRIC_GET_DIMENSIONS",
        &[],
        &[site_id],
        site_id,
        debug_mode
    ) {
        Ok(result) => CommandResult::success_json(result),
        Err(e) => {
            warn!("ValKey function failed for geometric_dimensions: {}", e);

            // Return a default dimensions map
            CommandResult::success(json!({
                "capabilities": 10,
                "services": 5,
                "default": 3
            }))
        }
    }
}

// =========================================================================
// Async handlers
// =========================================================================

/// Async version of handle_geometric_discover (STATELESS Architecture)
///
/// Uses FCALL to GNODE_TOPO_QUERY_VOXEL + Rust Q64.64 distance ranking.
/// NO in-memory state - all topology data lives in ValKey.
///
/// Flow:
///   1. Build query point from capability requirements (service-tier 30D)
///   2. Compute bucket_key using Q64.64 arithmetic
///   3. FCALL GNODE_TOPO_QUERY_VOXEL for O(1) candidate lookup
///   4. Fetch candidate entity data (includes pr: Q64.64 values)
///   5. Compute distances in Rust using Q64.64 for determinism
///   6. Rank and return sorted results
pub fn handle_geometric_discover_async<'a>(
    command: &'a Command,
    conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,  // UNUSED - stateless architecture
    site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        use crate::geometric_precision::{FixedVector, FixedPoint};

        if debug_mode {
            debug!("[STATELESS] Handling async geometric_discover command: {}", command.id);
        }

        // Parse parameters
        let params: GeometricDiscoverParams = match serde_json::from_value(command.parameters.clone()) {
            Ok(p) => p,
            Err(e) => return CommandResult::error(format!("Invalid parameters: {}", e)),
        };

        // ====================================================================
        // STATELESS: Build service-tier query point (30D = TOTAL_DIMENSIONS)
        // from capability requirements.
        // ====================================================================
        let dims = get_service_dimensions();
        let mut query_point = FixedVector::new(TOTAL_DIMENSIONS);

        // Prefer capability_values (HashMap with actual values) over capabilities (Vec<String>)
        if !params.capability_values.is_empty() {
            // Use actual capability values (preferred - precise matching)
            for (cap_name, &cap_value) in &params.capability_values {
                if let Some(&dim_idx) = dims.get(cap_name) {
                    if dim_idx < TOTAL_DIMENSIONS {
                        // Reject NaN/Infinity — clamp alone doesn't handle these
                        if !cap_value.is_finite() {
                            continue;
                        }
                        let clamped = cap_value.clamp(0.0, 1.0);
                        query_point[dim_idx] = FixedPoint::from_f64(clamped);
                    }
                }
            }
        } else {
            // Name-only format: set requested capabilities to 1.0
            for cap_name in &params.capabilities {
                if let Some(&dim_idx) = dims.get(cap_name) {
                    if dim_idx < TOTAL_DIMENSIONS {
                        query_point[dim_idx] = FixedPoint::from_f64(1.0);
                    }
                }
            }
        }

        // Build service-tier discovery point for bucket key (25D; storage dims 25-29 excluded)
        let disc_point = discovery_point_from_full(&query_point);
        let query_bucket_key = GeometricTopology::point_to_bucket_key(&disc_point, 10);
        let topology_key = GeometricTopology::get_services_topology_key(site_id);

        if debug_mode {
            debug!("[STATELESS] Query bucket_key: {} (76 chars, discovery dims (25D for service tier))", query_bucket_key);
        }

        // ====================================================================
        // STATELESS: Query voxel for candidates via FCALL
        // ====================================================================
        let voxel_result: redis::RedisResult<String> = redis::cmd("FCALL")
            .arg("GNODE_TOPO_QUERY_VOXEL")
            .arg(1)
            .arg(&topology_key)
            .arg(&query_bucket_key)
            .arg("true")  // include_data = true
            .query_async(conn)
            .await;

        // Parse candidates from voxel query
        let mut candidates: Vec<(String, f64)> = Vec::new();

        match voxel_result {
            Ok(json_str) => {
                if let Ok(result) = serde_json::from_str::<Value>(&json_str) {
                    // Extract entities from response (ents field)
                    if let Some(entities) = result.get("ents").and_then(|e| e.as_object()) {
                        for (entity_id, entity_data) in entities {
                            // Extract point_raw (pr) for Q64.64 distance calculation
                            if let Some(point_raw) = entity_data.get("pr").and_then(|p| p.as_array()) {
                                // Reconstruct entity point from Q64.64 raw values (full service-tier 30D)
                                let mut entity_point = FixedVector::new(TOTAL_DIMENSIONS);
                                for (i, val) in point_raw.iter().enumerate().take(TOTAL_DIMENSIONS) {
                                    if let Some(raw_str) = val.as_str() {
                                        // Q64.64 format: i128 as decimal string
                                        if let Ok(raw) = raw_str.parse::<i128>() {
                                            entity_point[i] = FixedPoint::from_raw(raw);
                                        }
                                    } else if let Some(raw_i64) = val.as_i64() {
                                        // Legacy Q32.32 format: promote to Q64.64
                                        entity_point[i] = FixedPoint::from_raw((raw_i64 as i128) << 32);
                                    }
                                }

                                // Compute distance using discovery dims only (25D for service tier)
                                let entity_disc = discovery_point_from_full(&entity_point);
                                let distance = disc_point.distance_to(&entity_disc).to_f64();
                                candidates.push((entity_id.clone(), distance));
                            }
                        }
                    }
                }
            },
            Err(e) => {
                if debug_mode {
                    debug!("[STATELESS] Voxel query failed: {:?}, trying fallback", e);
                }
                // Voxel might not exist yet - return empty results
            }
        }

        // ====================================================================
        // STATELESS: If no candidates in exact voxel, try adjacent voxels
        // ====================================================================
        if candidates.is_empty() {
            // Get all entities and filter (fallback for sparse topologies)
            let all_result: redis::RedisResult<String> = redis::cmd("FCALL")
                .arg("GNODE_TOPO_GET_ENTITIES")
                .arg(1)
                .arg(&topology_key)
                .arg("*")  // all entities
                .query_async(conn)
                .await;

            if let Ok(json_str) = all_result {
                if let Ok(result) = serde_json::from_str::<Value>(&json_str) {
                    if let Some(entities) = result.get("ents").and_then(|e| e.as_object()) {
                        for (entity_id, entity_data) in entities {
                            if let Some(point_raw) = entity_data.get("pr").and_then(|p| p.as_array()) {
                                let mut entity_point = FixedVector::new(TOTAL_DIMENSIONS);
                                for (i, val) in point_raw.iter().enumerate().take(TOTAL_DIMENSIONS) {
                                    if let Some(raw_str) = val.as_str() {
                                        // Q64.64 format: i128 as decimal string
                                        if let Ok(raw) = raw_str.parse::<i128>() {
                                            entity_point[i] = FixedPoint::from_raw(raw);
                                        }
                                    } else if let Some(raw_i64) = val.as_i64() {
                                        // Legacy Q32.32 format: promote to Q64.64
                                        entity_point[i] = FixedPoint::from_raw((raw_i64 as i128) << 32);
                                    }
                                }
                                // Compute distance using discovery dims only (25D for service tier)
                                let entity_disc = discovery_point_from_full(&entity_point);
                                let distance = disc_point.distance_to(&entity_disc).to_f64();

                                // Filter by distance threshold if specified
                                if params.distance <= 0.0 || distance <= params.distance {
                                    candidates.push((entity_id.clone(), distance));
                                }
                            }
                        }
                    }
                }
            }
        }

        // ====================================================================
        // Sort by distance (Q64.64 computed) and apply limit
        // ====================================================================
        candidates.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        // Apply limit (prefer `limit`, fallback to `dimensions` alias, default 10)
        let result_limit = if params.limit > 0 {
            params.limit
        } else if params.dimensions > 0 {
            params.dimensions
        } else {
            10  // Default
        };
        candidates.truncate(result_limit);

        // Build response
        let results: Vec<Value> = candidates.iter().map(|(id, dist)| {
            json!({
                "service_id": id,
                "distance": (dist * 1000.0).round() / 1000.0  // 3 decimal display
            })
        }).collect();

        CommandResult::success(json!({
            "total_matches": results.len(),
            "services": results,
            "results": results,
            "topology_key": topology_key,
            "query_bucket_key": query_bucket_key,
            "stateless": true,
            "precision": "Q64.64"
        }))
    })
}

pub fn handle_geometric_store_topology_async<'a>(
    command: &'a Command,
    _conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    _site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling async geometric_store_topology command: {} (retired)", command.id);
        }
        // RETIRED (S6d): bulk in-memory store is incompatible with the stateless
        // (C) model — register services individually via REGISTER_CAPABILITY_VECTOR.
        CommandResult::success(json!({
            "stored": false,
            "deprecated": true,
            "async": true,
            "note": "geometric_store_topology is retired; register services individually (stateless (C) topology)"
        }))
    })
}

/// Async version of handle_geometric_load_sequence
pub fn handle_geometric_load_sequence_async<'a>(
    command: &'a Command,
    conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling async geometric_load_sequence command: {}", command.id);
        }
        // STATELESS: load sequence = z-ordered (C) entities via FCALL (was in-memory).
        let topology_key = GeometricTopology::get_services_topology_key(site_id);
        let result: redis::RedisResult<String> = redis::cmd("FCALL")
            .arg("GNODE_TOPO_Z_ORDER").arg(1).arg(&topology_key)
            .query_async(conn)
            .await;
        match result {
            Ok(json) => CommandResult::success(z_order_eids(&json)),
            Err(e) => CommandResult::error(format!("Load sequence FCALL failed: {:?}", e)),
        }
    })
}

/// Async version of handle_geometric_distance
pub fn handle_geometric_distance_async<'a>(
    command: &'a Command,
    _conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    _site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling async geometric_distance command: {}", command.id);
        }

        let params: GeometricDistanceParams = match serde_json::from_value(command.parameters.clone()) {
            Ok(p) => p,
            Err(e) => return CommandResult::error(format!("Invalid parameters: {}", e)),
        };

        if params.point1.len() != params.point2.len() {
            return CommandResult::error(format!(
                "Points must have the same dimensions: {} vs {}",
                params.point1.len(), params.point2.len()
            ));
        }

        // Calculate Q64.64 Euclidean distance
        let mut sum_sq = 0.0;
        for i in 0..params.point1.len() {
            let diff = params.point1[i] - params.point2[i];
            sum_sq += diff * diff;
        }
        let distance = sum_sq.sqrt();
        CommandResult::success(json!({
            "distance": distance,
            "dimensions": params.point1.len(),
            "async": true
        }))
    })
}

/// Async version of handle_geometric_dimensions
pub fn handle_geometric_dimensions_async<'a>(
    command: &'a Command,
    conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling async geometric_dimensions command: {}", command.id);
        }

        let topology_key = format!("{{{}}}:gnode:topology", site_id);

        let result: redis::RedisResult<String> = redis::cmd("FCALL")
            .arg("GNODE_GEOMETRIC_GET_DIMENSIONS")
            .arg(1)
            .arg(&topology_key)
            .arg(site_id)
            .query_async(conn)
            .await;

        match result {
            Ok(json_result) => CommandResult::success_json(json_result),
            Err(e) => {
                if debug_mode {
                    debug!("ValKey function failed: {}", e);
                }
                CommandResult::success(json!({
                    "dimensions": 8,
                    "default": true,
                    "async": true
                }))
            }
        }
    })
}

/// Async version of handle_geometric_discover_range
pub fn handle_geometric_discover_range_async<'a>(
    command: &'a Command,
    conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling async geometric_discover_range command: {}", command.id);
        }

        let params: GeometricDiscoverRangeParams = match serde_json::from_value(command.parameters.clone()) {
            Ok(p) => p,
            Err(e) => return CommandResult::error(format!("Invalid parameters: {}", e)),
        };
        let topology_key = GeometricTopology::get_services_topology_key(site_id);
        let result: redis::RedisResult<String> = redis::cmd("FCALL")
            .arg("GNODE_TOPO_GET_ENTITIES").arg(1).arg(&topology_key).arg("*")
            .query_async(conn)
            .await;
        match result {
            Ok(json) => {
                let services = filter_entities_by_range(&json, &params.requirements);
                let count = services.len();
                CommandResult::success(json!({ "services": services, "count": count, "async": true }))
            },
            Err(e) => CommandResult::error(format!("Range discovery FCALL failed: {:?}", e)),
        }
    })
}
