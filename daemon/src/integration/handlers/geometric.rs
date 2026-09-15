// Geometric Command Handlers
//
// Handles: geometric_discover, geometric_discover_range, geometric_store_topology,
//          geometric_load_sequence, geometric_distance, geometric_dimensions
// Discovery ranks the canonical (C) entities by Q64.64 distance over the axes a query
// names; both lanes share one compute and differ only in connection type.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::RwLock;
use std::pin::Pin;
use std::future::Future;
use redis::Connection;
use redis::aio::MultiplexedConnection as AsyncConnection;
use serde::Deserialize;
use log::debug;
use serde_json::{Value, json};
use crate::custom_topology::fixed_distance;
use crate::daemon::Command;
use crate::geometric_precision::{FixedPoint, FixedVector};
use crate::GeometricTopology;

use super::types::{
    CommandResult, CommandHandlerFn, AsyncCommandHandlerFn, CommandDescriptor,
    parse_parameters, get_service_dimensions, DISCOVERY_DIMENSIONS,
    SERVICE_DIMENSION_ALIASES, TOTAL_DIMENSIONS, default_group, Lane,
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
        description: "Rank registered services by Q64.64 distance over the capability axes the query names; unnamed axes do not count",
        params_schema: json!({
            "type": "object",
            "properties": {
                "capabilities": {
                    "type": "object",
                    "description": "Service-tier discovery axis name → value in [0, 1] (names and codes: GNODE_SCHEMA_GET)",
                    "additionalProperties": {"type": "number", "minimum": 0.0, "maximum": 1.0}
                },
                "limit": {"type": "integer", "default": 10, "description": "Maximum number of results"},
                "threshold": {"type": "number", "default": 0, "description": "Drop services farther than this distance; 0 keeps all"}
            },
            "required": ["capabilities"]
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "total_matches": {"type": "integer"},
                "results": {
                    "type": "array",
                    "description": "Nearest first; equal distances by service_id",
                    "items": {
                        "type": "object",
                        "properties": {
                            "service_id": {"type": "string"},
                            "distance": {"type": "number"}
                        }
                    }
                },
                "topology_key": {"type": "string"},
                "lookup": {"type": "string", "enum": ["voxel", "scan"], "description": "voxel only when every discovery axis is named and the query's own cell is occupied; the cell's members are returned, not a global nearest set"}
            }
        }),
        example: r#"{"cmd":"geometric_discover","params":{"capabilities":{"protocol":0.1,"domain_primary":0.9},"limit":5}}"#,
        async_capable: true,
        lane: Lane::Concurrent,
    });

    descriptors.push(CommandDescriptor {
        name: "geometric_discover_range",
        category: "geometric",
        description: "Services whose stored point satisfies every operator on every named axis",
        params_schema: json!({
            "type": "object",
            "properties": {
                "requirements": {
                    "type": "object",
                    "description": "Axis index as a string → operators eq, neq, gt, gte, lt, lte; all must hold",
                    "additionalProperties": {"type": "object", "additionalProperties": {"type": "number"}}
                }
            },
            "required": ["requirements"]
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "services": {"type": "array", "items": {"type": "string"}, "description": "Matching service ids, sorted"},
                "count": {"type": "integer"}
            }
        }),
        example: r#"{"cmd":"geometric_discover_range","params":{"requirements":{"8":{"eq":0.9},"12":{"gte":0.0,"lte":0.25}}}}"#,
        async_capable: true,
        lane: Lane::Concurrent,
    });

    descriptors.push(CommandDescriptor {
        name: "geometric_store_topology",
        category: "geometric",
        description: "Retired: accepted and ignored; services register individually with register_service",
        params_schema: json!({
            "type": "object",
            "properties": {
                "data": {"description": "Ignored"}
            }
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "stored": {"type": "boolean", "description": "Always false"},
                "deprecated": {"type": "boolean"},
                "note": {"type": "string"}
            }
        }),
        example: r#"{"cmd":"geometric_store_topology","params":{"data":{}}}"#,
        async_capable: true,
        lane: Lane::Concurrent,
    });

    descriptors.push(CommandDescriptor {
        name: "geometric_load_sequence",
        category: "geometric",
        description: "Service ids of the site's topology in z-order (the load sequence)",
        params_schema: json!({
            "type": "object",
            "properties": {
                "group": {"type": "string", "default": "default", "description": "Accepted; the sequence covers the whole site topology"}
            }
        }),
        returns_schema: json!({
            "type": "array",
            "items": {"type": "string"}
        }),
        example: r#"{"cmd":"geometric_load_sequence","params":{}}"#,
        async_capable: true,
        lane: Lane::Concurrent,
    });

    descriptors.push(CommandDescriptor {
        name: "geometric_distance",
        category: "geometric",
        description: "Q64.64 Euclidean distance between two coordinate vectors",
        params_schema: json!({
            "type": "object",
            "properties": {
                "point1": {"type": "array", "items": {"type": "number"}, "description": "First point"},
                "point2": {"type": "array", "items": {"type": "number"}, "description": "Second point, same width"}
            },
            "required": ["point1", "point2"]
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "distance": {"type": "number"},
                "dimensions": {"type": "integer"}
            }
        }),
        example: r#"{"cmd":"geometric_distance","params":{"point1":[0.8,0.2],"point2":[0.3,0.2]}}"#,
        async_capable: true,
        lane: Lane::Concurrent,
    });

    descriptors.push(CommandDescriptor {
        name: "geometric_dimensions",
        category: "geometric",
        description: "The service-tier axes discovery matches against, by name and index",
        params_schema: json!({"type": "object", "properties": {}}),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "tier": {"type": "string"},
                "total_dimensions": {"type": "integer"},
                "discovery_dimensions": {"type": "integer"},
                "dimension_index": {"type": "object", "description": "Axis name → index"},
                "aliases": {"type": "object", "description": "Alternative name → axis name"}
            }
        }),
        example: r#"{"cmd":"geometric_dimensions","params":{}}"#,
        async_capable: true,
        lane: Lane::Concurrent,
    });
}

// =========================================================================
// Parameter structs
// =========================================================================

/// Parameters for the geometric_discover command
#[derive(Debug, Deserialize)]
#[cfg_attr(test, derive(serde::Serialize, Default))]
struct GeometricDiscoverParams {
    /// Discovery axis name → value in [0, 1]; only these axes take part in the match
    capabilities: HashMap<String, f64>,
    /// Maximum results (0 → 10)
    #[serde(default)]
    limit: usize,
    /// Maximum distance; 0 keeps all
    #[serde(default)]
    threshold: f64,
}

/// Parameters for the geometric_discover_range command
#[derive(Debug, Deserialize)]
#[cfg_attr(test, derive(serde::Serialize, Default))]
struct GeometricDiscoverRangeParams {
    /// Axis index → operators, e.g. { "8": {"eq": 0.90}, "12": {"lte": 0.25} }
    requirements: HashMap<String, HashMap<String, f64>>,
}

/// Parameters for the geometric_store_topology command (retired — kept for the
/// deprecation-no-op parse so legacy callers still validate against the old shape).
#[derive(Debug, Deserialize)]
#[cfg_attr(test, derive(serde::Serialize, Default))]
#[allow(dead_code)]
struct GeometricStoreTopologyParams {
    data: Value,
}

/// Parameters for the geometric_load_sequence command
#[derive(Debug, Deserialize)]
#[cfg_attr(test, derive(serde::Serialize, Default))]
struct GeometricLoadSequenceParams {
    #[serde(default = "default_group")]
    #[allow(dead_code)]
    group: String,
}

/// Parameters for the geometric_distance command
#[derive(Debug, Deserialize)]
#[cfg_attr(test, derive(serde::Serialize, Default))]
struct GeometricDistanceParams {
    point1: Vec<f64>,
    point2: Vec<f64>,
}

// =========================================================================
// Discovery compute (shared by both lanes)
// =========================================================================

/// A validated discovery query: the named discovery axes, by index, in Q64.64.
struct DiscoveryQuery {
    axes: Vec<(usize, FixedPoint)>,
    limit: usize,
    threshold: f64,
}

impl DiscoveryQuery {
    fn plan(params: &GeometricDiscoverParams) -> Result<Self, String> {
        if params.capabilities.is_empty() {
            return Err("capabilities must name at least one axis".to_string());
        }
        let dims = get_service_dimensions();
        let mut axes = Vec::with_capacity(params.capabilities.len());
        for (name, &value) in &params.capabilities {
            let index = *dims.get(name).ok_or_else(|| format!("unknown capability axis '{}'", name))?;
            if index >= DISCOVERY_DIMENSIONS {
                return Err(format!("'{}' is storage-only and cannot be searched", name));
            }
            if !value.is_finite() || !(0.0..=1.0).contains(&value) {
                return Err(format!("capability '{}' must be a number in [0, 1], got {}", name, value));
            }
            axes.push((index, FixedPoint::from_f64(value)));
        }
        axes.sort_by_key(|(index, _)| *index);
        if let Some(pair) = axes.windows(2).find(|w| w[0].0 == w[1].0) {
            return Err(format!("capabilities name axis {} more than once (an alias and its axis?)", pair[0].0));
        }
        Ok(Self {
            axes,
            limit: if params.limit > 0 { params.limit } else { 10 },
            threshold: params.threshold,
        })
    }

    /// The query's voxel cell, only when every discovery axis is named: a cell built
    /// from defaulted axes would describe a request nobody made.
    fn bucket_key(&self) -> Option<String> {
        if self.axes.len() != DISCOVERY_DIMENSIONS {
            return None;
        }
        let mut point = FixedVector::new(DISCOVERY_DIMENSIONS);
        for (index, value) in &self.axes {
            point[*index] = *value;
        }
        Some(GeometricTopology::point_to_bucket_key(&point, 10))
    }

    fn distance_to(&self, point_raw: &[Value]) -> Option<f64> {
        let mut query = FixedVector::new(self.axes.len());
        let mut entity = FixedVector::new(self.axes.len());
        for (k, (index, value)) in self.axes.iter().enumerate() {
            query[k] = *value;
            entity[k] = raw_coordinate(point_raw.get(*index)?)?;
        }
        Some(query.distance_to(&entity).to_f64())
    }

    /// Rank a GNODE_TOPO_QUERY_VOXEL / GNODE_TOPO_GET_ENTITIES reply (`{ents:{id:{pr}}}`).
    fn rank(&self, reply: &str) -> Result<Vec<(String, f64)>, String> {
        let v: Value = serde_json::from_str(reply).map_err(|e| format!("unreadable topology reply: {}", e))?;
        let mut ranked = Vec::new();
        if let Some(ents) = v.get("ents").and_then(|e| e.as_object()) {
            for (id, data) in ents {
                let Some(distance) = data.get("pr").and_then(|p| p.as_array()).and_then(|pr| self.distance_to(pr)) else {
                    continue;
                };
                if self.threshold <= 0.0 || distance <= self.threshold {
                    ranked.push((id.clone(), distance));
                }
            }
        }
        ranked.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal).then_with(|| a.0.cmp(&b.0)));
        ranked.truncate(self.limit);
        Ok(ranked)
    }
}

/// A `pr` coordinate: Q64.64 as a decimal string, or a legacy Q32.32 integer.
fn raw_coordinate(v: &Value) -> Option<FixedPoint> {
    match v {
        Value::String(s) => s.parse::<i128>().ok().map(FixedPoint::from_raw),
        other => other.as_i64().map(|raw| FixedPoint::from_raw((raw as i128) << 32)),
    }
}

fn discovery_reply(ranked: Vec<(String, f64)>, topology_key: &str, lookup: &str) -> CommandResult {
    let results: Vec<Value> = ranked
        .into_iter()
        .map(|(service_id, distance)| json!({"service_id": service_id, "distance": distance}))
        .collect();
    CommandResult::success(json!({
        "total_matches": results.len(),
        "results": results,
        "topology_key": topology_key,
        "lookup": lookup,
    }))
}

const RANGE_OPERATORS: [&str; 6] = ["eq", "neq", "gt", "gte", "lt", "lte"];

fn validate_range(reqs: &HashMap<String, HashMap<String, f64>>) -> Result<(), String> {
    for (axis, ops) in reqs {
        let index: usize = axis
            .parse()
            .map_err(|_| format!("requirement key '{}' must be an axis index", axis))?;
        if index >= TOTAL_DIMENSIONS {
            return Err(format!("axis {} is outside the {}-axis service tier", index, TOTAL_DIMENSIONS));
        }
        if ops.is_empty() {
            return Err(format!("axis {} names no operator", index));
        }
        if let Some(op) = ops.keys().find(|op| !RANGE_OPERATORS.contains(&op.as_str())) {
            return Err(format!("unknown operator '{}' on axis {} (use {})", op, index, RANGE_OPERATORS.join(", ")));
        }
    }
    Ok(())
}

/// Ids of entities in a GNODE_TOPO_GET_ENTITIES reply whose `pd` satisfies every requirement.
fn filter_entities_by_range(entities_json: &str, reqs: &HashMap<String, HashMap<String, f64>>) -> Result<Vec<String>, String> {
    let v: Value = serde_json::from_str(entities_json).map_err(|e| format!("unreadable topology reply: {}", e))?;
    let mut out = Vec::new();
    let Some(ents) = v.get("ents").and_then(|e| e.as_object()) else {
        return Ok(out);
    };
    for (id, data) in ents {
        let pd = data.get("pd").and_then(|p| p.as_array());
        let matches = reqs.iter().all(|(axis, ops)| {
            let index: usize = axis.parse().unwrap_or(usize::MAX);
            pd.and_then(|a| a.get(index))
                .and_then(|x| x.as_f64())
                .map(|val| range_op_match(ops, val))
                .unwrap_or(false)
        });
        if matches {
            out.push(id.clone());
        }
    }
    out.sort();
    Ok(out)
}

/// Every operator on the axis must hold.
fn range_op_match(ops: &HashMap<String, f64>, val: f64) -> bool {
    ops.iter().all(|(op, bound)| match op.as_str() {
        "eq" => (val - bound).abs() < 1e-6,
        "neq" => (val - bound).abs() >= 1e-6,
        "gt" => val > *bound,
        "gte" => val >= *bound,
        "lt" => val < *bound,
        "lte" => val <= *bound,
        _ => false,
    })
}

fn range_reply(reply: &str, reqs: &HashMap<String, HashMap<String, f64>>) -> CommandResult {
    match filter_entities_by_range(reply, reqs) {
        Ok(services) => {
            let count = services.len();
            CommandResult::success(json!({ "services": services, "count": count }))
        }
        Err(e) => CommandResult::error(e),
    }
}

fn distance_reply(params: &GeometricDistanceParams) -> CommandResult {
    if params.point1.len() != params.point2.len() {
        return CommandResult::error(format!(
            "Points must have the same dimensions: {} vs {}",
            params.point1.len(), params.point2.len()
        ));
    }
    if params.point1.iter().chain(&params.point2).any(|v| !v.is_finite()) {
        return CommandResult::error("Coordinates must be finite numbers");
    }
    CommandResult::success(json!({
        "distance": fixed_distance(&params.point1, &params.point2),
        "dimensions": params.point1.len()
    }))
}

fn dimensions_reply() -> CommandResult {
    let aliases: BTreeMap<&str, &str> = SERVICE_DIMENSION_ALIASES.iter().copied().collect();
    let index: BTreeMap<&str, usize> = get_service_dimensions()
        .iter()
        .filter(|(name, _)| !aliases.contains_key(name.as_str()))
        .map(|(n, i)| (n.as_str(), *i))
        .collect();
    CommandResult::success(json!({
        "tier": "service",
        "total_dimensions": TOTAL_DIMENSIONS,
        "discovery_dimensions": DISCOVERY_DIMENSIONS,
        "dimension_index": index,
        "aliases": aliases,
    }))
}

/// Extract the ordered entity-id list from a GNODE_TOPO_Z_ORDER response (`{eids:[...]}`).
fn z_order_eids(json: &str) -> Vec<String> {
    serde_json::from_str::<Value>(json).ok()
        .and_then(|v| v.get("eids").and_then(|e| e.as_array()).map(|a|
            a.iter().filter_map(|x| x.as_str().map(String::from)).collect()))
        .unwrap_or_default()
}

fn retired_store_reply() -> CommandResult {
    // RETIRED (S6d): bulk in-memory topology store is incompatible with the
    // stateless (C) model — services register individually via
    // GNODE_REGISTER_CAPABILITY_VECTOR. Kept as a deprecation no-op so legacy
    // callers get a clear signal instead of silently mutating dead in-mem state.
    CommandResult::success(json!({
        "stored": false,
        "deprecated": true,
        "note": "geometric_store_topology is retired; register services individually (stateless (C) topology)"
    }))
}

// =========================================================================
// Sync handlers
// =========================================================================

/// Handle 'geometric_discover' command
pub fn handle_geometric_discover(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    site_id: &str,
    debug_mode: bool
) -> CommandResult {
    if debug_mode {
        debug!("Handling geometric_discover command: {}", command.id);
    }
    let params = match parse_parameters::<GeometricDiscoverParams>(command) {
        Ok(p) => p,
        Err(e) => return CommandResult::error(e),
    };
    let query = match DiscoveryQuery::plan(&params) {
        Ok(q) => q,
        Err(e) => return CommandResult::error(e),
    };
    let topology_key = GeometricTopology::get_services_topology_key(site_id);

    if let Some(bucket_key) = query.bucket_key() {
        let cell: redis::RedisResult<String> = redis::cmd("FCALL")
            .arg("GNODE_TOPO_QUERY_VOXEL").arg(1).arg(&topology_key).arg(&bucket_key).arg("true")
            .query(conn);
        match cell.map_err(|e| format!("Voxel query FCALL failed: {:?}", e)).and_then(|reply| query.rank(&reply)) {
            Ok(ranked) if !ranked.is_empty() => return discovery_reply(ranked, &topology_key, "voxel"),
            Ok(_) => {}
            Err(e) => return CommandResult::error(e),
        }
    }

    let all: redis::RedisResult<String> = redis::cmd("FCALL")
        .arg("GNODE_TOPO_GET_ENTITIES").arg(1).arg(&topology_key).arg("*")
        .query(conn);
    match all.map_err(|e| format!("Discovery FCALL failed: {:?}", e)).and_then(|reply| query.rank(&reply)) {
        Ok(ranked) => discovery_reply(ranked, &topology_key, "scan"),
        Err(e) => CommandResult::error(e),
    }
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
    if let Err(e) = validate_range(&params.requirements) {
        return CommandResult::error(e);
    }
    let topology_key = GeometricTopology::get_services_topology_key(site_id);
    let result: redis::RedisResult<String> = redis::cmd("FCALL")
        .arg("GNODE_TOPO_GET_ENTITIES").arg(1).arg(&topology_key).arg("*")
        .query(conn);
    match result {
        Ok(json) => range_reply(&json, &params.requirements),
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
    let _ = parse_parameters::<GeometricStoreTopologyParams>(command);
    retired_store_reply()
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
    match parse_parameters::<GeometricDistanceParams>(command) {
        Ok(params) => distance_reply(&params),
        Err(e) => CommandResult::error(e),
    }
}

/// Handle 'geometric_dimensions' command
pub fn handle_geometric_dimensions(
    command: &Command,
    _conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    _site_id: &str,
    debug_mode: bool
) -> CommandResult {
    if debug_mode {
        debug!("Handling geometric_dimensions command: {}", command.id);
    }
    dimensions_reply()
}

// =========================================================================
// Async handlers
// =========================================================================

/// Async version of handle_geometric_discover
pub fn handle_geometric_discover_async<'a>(
    command: &'a Command,
    conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling async geometric_discover command: {}", command.id);
        }
        let params: GeometricDiscoverParams = match serde_json::from_value(command.parameters.clone()) {
            Ok(p) => p,
            Err(e) => return CommandResult::error(format!("Invalid parameters: {}", e)),
        };
        let query = match DiscoveryQuery::plan(&params) {
            Ok(q) => q,
            Err(e) => return CommandResult::error(e),
        };
        let topology_key = GeometricTopology::get_services_topology_key(site_id);

        if let Some(bucket_key) = query.bucket_key() {
            let cell: redis::RedisResult<String> = redis::cmd("FCALL")
                .arg("GNODE_TOPO_QUERY_VOXEL").arg(1).arg(&topology_key).arg(&bucket_key).arg("true")
                .query_async(conn)
                .await;
            match cell.map_err(|e| format!("Voxel query FCALL failed: {:?}", e)).and_then(|reply| query.rank(&reply)) {
                Ok(ranked) if !ranked.is_empty() => return discovery_reply(ranked, &topology_key, "voxel"),
                Ok(_) => {}
                Err(e) => return CommandResult::error(e),
            }
        }

        let all: redis::RedisResult<String> = redis::cmd("FCALL")
            .arg("GNODE_TOPO_GET_ENTITIES").arg(1).arg(&topology_key).arg("*")
            .query_async(conn)
            .await;
        match all.map_err(|e| format!("Discovery FCALL failed: {:?}", e)).and_then(|reply| query.rank(&reply)) {
            Ok(ranked) => discovery_reply(ranked, &topology_key, "scan"),
            Err(e) => CommandResult::error(e),
        }
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
        retired_store_reply()
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
        match serde_json::from_value::<GeometricDistanceParams>(command.parameters.clone()) {
            Ok(params) => distance_reply(&params),
            Err(e) => CommandResult::error(format!("Invalid parameters: {}", e)),
        }
    })
}

/// Async version of handle_geometric_dimensions
pub fn handle_geometric_dimensions_async<'a>(
    command: &'a Command,
    _conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    _site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling async geometric_dimensions command: {}", command.id);
        }
        dimensions_reply()
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
        if let Err(e) = validate_range(&params.requirements) {
            return CommandResult::error(e);
        }
        let topology_key = GeometricTopology::get_services_topology_key(site_id);
        let result: redis::RedisResult<String> = redis::cmd("FCALL")
            .arg("GNODE_TOPO_GET_ENTITIES").arg(1).arg(&topology_key).arg("*")
            .query_async(conn)
            .await;
        match result {
            Ok(json) => range_reply(&json, &params.requirements),
            Err(e) => CommandResult::error(format!("Range discovery FCALL failed: {:?}", e)),
        }
    })
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

    fn axis(name: &str) -> usize {
        get_service_dimensions()[name]
    }

    /// A reply entity whose full `pr` is zero except the given axes.
    fn entity(values: &[(usize, f64)]) -> Value {
        let mut pr = vec![FixedPoint::from_f64(0.0).raw().to_string(); TOTAL_DIMENSIONS];
        for (index, value) in values {
            pr[*index] = FixedPoint::from_f64(*value).raw().to_string();
        }
        json!({ "pr": pr })
    }

    fn query(caps: Value) -> Result<DiscoveryQuery, String> {
        let params: GeometricDiscoverParams = serde_json::from_value(json!({ "capabilities": caps })).unwrap();
        DiscoveryQuery::plan(&params)
    }

    #[test]
    fn descriptors_name_exactly_the_fields_the_parsers_read() {
        let d = descriptors();
        assert_descriptor_fields::<GeometricDiscoverParams>(&d, "geometric_discover", true);
        assert_descriptor_fields::<GeometricDiscoverRangeParams>(&d, "geometric_discover_range", true);
        assert_descriptor_fields::<GeometricStoreTopologyParams>(&d, "geometric_store_topology", true);
        assert_descriptor_fields::<GeometricLoadSequenceParams>(&d, "geometric_load_sequence", true);
        assert_descriptor_fields::<GeometricDistanceParams>(&d, "geometric_distance", true);
    }

    #[test]
    fn axes_the_query_does_not_name_do_not_count_against_a_provider() {
        let q = query(json!({"protocol": 0.1, "domain_primary": 0.9})).unwrap();
        let reply = json!({"ents": {
            "mailer": entity(&[(axis("protocol"), 0.1), (axis("domain_primary"), 0.9), (axis("environment"), 1.0), (axis("network_zone"), 1.0)]),
            "cache": entity(&[(axis("protocol"), 0.1), (axis("domain_primary"), 0.25)]),
        }}).to_string();
        let ranked = q.rank(&reply).unwrap();
        assert_eq!(ranked[0], ("mailer".to_string(), 0.0));
        assert_eq!(ranked[1].0, "cache");
    }

    #[test]
    fn only_a_query_naming_every_discovery_axis_has_a_voxel_cell() {
        assert!(query(json!({"protocol": 0.1})).unwrap().bucket_key().is_none());
        let aliases: Vec<&str> = SERVICE_DIMENSION_ALIASES.iter().map(|(alias, _)| *alias).collect();
        let every_axis: serde_json::Map<String, Value> = get_service_dimensions()
            .iter()
            .filter(|(name, i)| **i < DISCOVERY_DIMENSIONS && !aliases.contains(&name.as_str()))
            .map(|(name, _)| (name.clone(), json!(0.5)))
            .collect();
        assert!(query(Value::Object(every_axis)).unwrap().bucket_key().is_some());
    }

    #[test]
    fn unknown_storage_only_out_of_range_and_empty_queries_are_refused() {
        for caps in [json!({"compute": 0.8}), json!({"user_x": 0.5}), json!({"protocol": 2.0}), json!({}), json!({"load": 0.5, "current_load": 0.5})] {
            assert!(query(caps.clone()).is_err(), "accepted {caps}");
        }
    }

    #[test]
    fn equal_distances_order_by_id_and_the_threshold_drops_far_services() {
        let q = DiscoveryQuery { threshold: 0.3, ..query(json!({"protocol": 0.5})).unwrap() };
        let reply = json!({"ents": {
            "b": entity(&[(0, 0.25)]),
            "a": entity(&[(0, 0.75)]),
            "far": entity(&[(0, 1.0)]),
        }}).to_string();
        let ids: Vec<String> = q.rank(&reply).unwrap().into_iter().map(|(id, _)| id).collect();
        assert_eq!(ids, vec!["a", "b"]);
    }

    #[test]
    fn every_range_operator_on_an_axis_must_hold() {
        let ops = HashMap::from([("gt".to_string(), 0.1), ("lt".to_string(), 0.5)]);
        assert!(range_op_match(&ops, 0.3));
        assert!(!range_op_match(&ops, 0.7));
    }

    #[test]
    fn range_requirements_are_checked_before_the_scan() {
        let rejected = |r: Value| validate_range(&serde_json::from_value(r).unwrap()).is_err();
        assert!(rejected(json!({"protocol": {"eq": 0.1}})));
        assert!(rejected(json!({"99": {"eq": 0.1}})));
        assert!(rejected(json!({"0": {"between": 0.1}})));
        assert!(!rejected(json!({"0": {"gte": 0.1, "lte": 0.2}})));
    }

    #[test]
    fn dimensions_come_from_the_map_discovery_matches_against() {
        let r = dimensions_reply().result.unwrap();
        assert_eq!(r["dimension_index"]["network_zone"], axis("network_zone"));
        assert_eq!(r["discovery_dimensions"], DISCOVERY_DIMENSIONS);
        assert!(r["dimension_index"].get("load").is_none());
        assert_eq!(r["aliases"]["load"], "current_load");
    }
}
