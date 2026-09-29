// Shared types, constants, and utilities for command handlers
//
// This module contains all types shared across handler modules:
// - Schema-driven capability dimension mapping (default: 30D service tier)
// - Generic capability vector builders for multi-tier topology
// - CommandResult struct
// - Handler function type aliases
// - Utility functions for capability vectors and parameter parsing

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use std::pin::Pin;
use std::future::Future;
use redis::Connection;
use redis::aio::MultiplexedConnection as AsyncConnection;
use serde::{Deserialize, Serialize};
use log::warn;
use serde_json::Value;
use crate::daemon::{Command, Response};
use crate::GeometricTopology;
use crate::geometric_precision::FixedPoint;
use crate::integration::processor::stream_utils::current_timestamp;

use once_cell::sync::Lazy;

// ============================================================================
// SERVICE TIER CAPABILITY DIMENSION MAPPING (Schema: service_schema.yaml)
// ============================================================================
//
// Static mapping of capability names to dimension indices for the 23-dimensional
// service topology. Schema version 4.0, three zones cut by index:
//
//   0..HASHED_DIMENSIONS      declared by the provider AND hashed into the
//                             bucket key
//   HASHED..DISCOVERY         derived: written by the sampler or the daemon,
//                             ranked in a query, never hashed (a coordinate
//                             that moves must not decide voxel membership)
//   DISCOVERY..TOTAL          storage: stored and returned, refused in a query
//
// Used by stateless command handlers for:
//   - Building capability vectors from service registration
//   - Computing bucket keys for voxel storage (hashed dims only)
//   - Distance calculations for service discovery
//
// These defaults are for the SERVICE tier. Other tiers (tool=16D,
// constellation=24D) use the generic build_capability_vector() with
// schema-derived counts.
// See: gNode/daemon/config/{service,tool,constellation}_schema.yaml

/// Service tier: dimensions a query may name (declared + derived)
pub const DISCOVERY_DIMENSIONS: usize = 19;
/// Service tier: dimensions that feed the spatial-hash bucket key
pub const HASHED_DIMENSIONS: usize = 16;
/// Service tier: total dimensions (declared + derived + storage)
pub const TOTAL_DIMENSIONS: usize = 23;

/// Fractional bits of the Q-format `pr` is encoded with. g_math's default
/// table format is Q64.64; the registration primitive needs this to place a
/// storage-only axis value in the same scale as the rest of the point.
pub const POINT_FRAC_BITS: u32 = 64;

/// 0-based index of a tier's `registration_order` axis, or -1 when the tier
/// has none. Passed to GNODE_REGISTER_CAPABILITY_VECTOR so the Lua allocator
/// writes the counter where the schema says, never at a fixed slot.
pub fn registration_order_index(dim_map: &HashMap<String, usize>) -> i64 {
    dim_map
        .get("registration_order")
        .map(|&i| i as i64)
        .unwrap_or(-1)
}

/// Static 30D capability dimension mapping for service topology (schema v3.0).
/// Maps capability names to their dimension indices (0-29).
///
/// Layer architecture:
///   Layer 1 (0-2):   Interface Identity - protocol, version, stability
///   Layer 2 (3-5):   Access Control - clearance, auth, sensitivity
///   Layer 3 (6-9):   Scope and Domain - scope, primary, secondary, specialization
///   Layer 4 (10-12): Declared service levels - throughput, latency, reliability
///   Layer 5 (13-15): Workflow and placement - pipeline_stage, priority, environment
///   Derived (16-18):  current_load, health_status, lifecycle_state — measured
///   Storage (19-22):  native_format, implementation_language, data_persistence,
///                     service_tier
/// Accepted alternative names for service-tier axes: (alias, canonical axis).
pub const SERVICE_DIMENSION_ALIASES: [(&str, &str); 6] = [
    ("load", "current_load"),
    ("health", "health_status"),
    ("lifecycle", "lifecycle_state"),
    ("tier", "service_tier"),
    ("env", "environment"),
    ("language", "implementation_language"),
];

/// The service tier's axes in index order — the ONE list. Both views (name to
/// index, index to name) are built from it, and a test pins it to
/// service_schema.yaml, so an index can no longer drift in a second copy.
pub const SERVICE_AXES: [&str; TOTAL_DIMENSIONS] = [
    // Declared, hashed (0-15)
    "protocol", "api_version", "contract_stability",
    "clearance_required", "auth_method", "data_sensitivity",
    "service_scope", "domain_primary", "domain_secondary", "specialization",
    "throughput_tier", "latency_class", "reliability_tier",
    "pipeline_stage", "execution_priority", "environment",
    // Derived, ranked, never hashed (16-18)
    "current_load", "health_status", "lifecycle_state",
    // Storage (19-22)
    "native_format", "implementation_language", "data_persistence", "service_tier",
];

/// Indices whose value belongs to the sampler, not to the registering provider.
/// Passed to GNODE_REGISTER_CAPABILITY_VECTOR so a re-registration preserves the
/// last measurement instead of resetting it to the schema's unknown code, the
/// same way `m.ro` is preserved. `lifecycle_state` is absent on purpose: the
/// registration itself is that axis's writer.
pub const SERVICE_SAMPLER_AXES: [usize; 2] = [16, 17];

/// The sampler-owned indices as the CSV the registration primitive expects.
pub fn sampler_axes_csv() -> String {
    SERVICE_SAMPLER_AXES
        .iter()
        .map(|i| i.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

pub static SERVICE_DIMENSIONS: Lazy<HashMap<String, usize>> = Lazy::new(|| {
    let mut dims: HashMap<String, usize> = SERVICE_AXES
        .iter()
        .enumerate()
        .map(|(index, name)| (name.to_string(), index))
        .collect();

    for (alias, canonical) in SERVICE_DIMENSION_ALIASES {
        let index = dims[canonical];
        dims.insert(alias.to_string(), index);
    }

    dims
});

/// Get the service tier dimension mapping (30D = TOTAL_DIMENSIONS).
/// Canonical accessor; route every call through this instead of cloning the
/// static map. For other tiers (tool/constellation/galaxy) load the tier's
/// schema YAML via tool_registration::find_schema_path + load_schema and
/// use the schema's dim_map directly.
#[inline]
pub fn get_service_dimensions() -> &'static HashMap<String, usize> {
    &SERVICE_DIMENSIONS
}

// ============================================================================
// Schema-driven capability vector builders (multi-tier)
// ============================================================================

/// Build a capability vector of arbitrary dimension count from a name→value map.
/// Used by all topology tiers. The dim_map and total_dims come from the tier's schema.
///
/// # Arguments
/// * `capabilities` - HashMap of capability names to values (0.0-1.0)
/// * `total_dims` - Total dimension count for this tier's schema
/// * `dim_map` - Mapping of capability names to dimension indices
pub fn build_capability_vector(
    capabilities: &HashMap<String, f64>,
    total_dims: usize,
    dim_map: &HashMap<String, usize>,
) -> crate::geometric_precision::FixedVector {
    use crate::geometric_precision::{FixedVector, FixedPoint};

    let mut point = FixedVector::new(total_dims);

    for (cap_name, &cap_value) in capabilities {
        if let Some(&dim_idx) = dim_map.get(cap_name) {
            if dim_idx < total_dims {
                if !cap_value.is_finite() {
                    continue;
                }
                let clamped = cap_value.clamp(0.0, 1.0);
                point[dim_idx] = FixedPoint::from_f64(clamped);
            }
        }
    }

    point
}

/// Extract a prefix of a full point. Both zone cuts are prefix truncations, so
/// this serves the discovery slice (what a query may name) and the hashed slice
/// (what the bucket key is built from); the caller says which by the count it
/// passes, and that count comes from the tier's schema.
pub fn discovery_point(
    full_point: &crate::geometric_precision::FixedVector,
    discovery_dims: usize,
) -> crate::geometric_precision::FixedVector {
    use crate::geometric_precision::FixedVector;

    let mut disc_point = FixedVector::new(discovery_dims);
    for i in 0..discovery_dims {
        if i < full_point.len() {
            disc_point[i] = full_point[i];
        }
    }
    disc_point
}

// ============================================================================
// Service tier convenience wrappers
// ============================================================================
//
// Service tier = the local-per-site service topology. 23 total dimensions, 19
// of which a query may name, 16 of which feed the bucket key. Other tiers
// (tool, constellation) have their own dim counts loaded from their tier schema
// YAML — see daemon/config/{service,tool,constellation}_schema.yaml.
//
// Custom topologies created via topo_create / gNode-TOPO have user-defined
// dim counts and live in handlers/topology_custom.rs + custom_topology.rs.

/// Build a service-tier FixedVector from a capability name→value HashMap.
/// Reads dim count + dim map from the service tier (TOTAL_DIMENSIONS = 23).
pub fn build_service_capability_vector(capabilities: &HashMap<String, f64>) -> crate::geometric_precision::FixedVector {
    build_capability_vector(capabilities, TOTAL_DIMENSIONS, get_service_dimensions())
}

/// Build a discovery-only point from a full service-tier point.
/// Service tier: 19 queryable dims sliced from the 23D full vector.
pub fn discovery_point_from_full(full_point: &crate::geometric_precision::FixedVector) -> crate::geometric_precision::FixedVector {
    discovery_point(full_point, DISCOVERY_DIMENSIONS)
}

/// Build the hashed point from a full service-tier point — what the bucket key
/// is computed over. Stops below the derived axes: load and health move, and an
/// entity must not change voxel because it got busy.
pub fn hashed_point_from_full(full_point: &crate::geometric_precision::FixedVector) -> crate::geometric_precision::FixedVector {
    discovery_point(full_point, HASHED_DIMENSIONS)
}

// ============================================================================
// Command Result Type
// ============================================================================

/// Result type returned by command handlers
#[derive(Debug, Clone)]
pub struct CommandResult {
    pub status: String,
    pub result: Option<Value>,
    pub error: Option<String>,
}

impl CommandResult {
    /// Create a success result with a value
    pub fn success(result: impl Into<Value>) -> Self {
        Self {
            status: "ok".to_string(),
            result: Some(result.into()),
            error: None,
        }
    }

    /// Create a success result with a JSON value
    pub fn success_json(json_str: String) -> Self {
        match serde_json::from_str(&json_str) {
            Ok(value) => Self {
                status: "ok".to_string(),
                result: Some(value),
                error: None,
            },
            Err(e) => {
                warn!("Failed to parse JSON result: {}", e);
                Self {
                    status: "ok".to_string(),
                    result: Some(Value::String(json_str)),
                    error: None,
                }
            }
        }
    }

    /// Create an error result with an error message
    pub fn error(error: impl Into<String>) -> Self {
        Self {
            status: "error".to_string(),
            result: None,
            error: Some(error.into()),
        }
    }

    /// Convert to a Response object
    pub fn to_response(&self, command_id: &str) -> Response {
        Response {
            id: command_id.to_string(),
            status: self.status.clone(),
            result: self.result.clone(),
            error: self.error.clone(),
            timestamp: current_timestamp(),
            batch_id: None,
            sequence: None,
        }
    }
}

// ============================================================================
// Handler Type Aliases
// ============================================================================

/// Type alias for synchronous command handler functions
pub type CommandHandlerFn = fn(&Command, &mut Connection, &Arc<RwLock<GeometricTopology>>, &str, bool) -> CommandResult;

/// Type alias for asynchronous command handler functions
/// Uses Pin<Box<dyn Future>> to allow async fn with references
pub type AsyncCommandHandlerFn = for<'a> fn(
    &'a Command,
    &'a mut AsyncConnection,
    &'a Arc<RwLock<GeometricTopology>>,
    &'a str,  // site_id
    bool,     // debug
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>>;

// ============================================================================
// Parameter Parsing
// ============================================================================

/// Parse parameters for a specific type
pub fn parse_parameters<T: for<'de> Deserialize<'de>>(command: &Command) -> Result<T, String> {
    match serde_json::from_value::<T>(command.parameters.clone()) {
        Ok(params) => Ok(params),
        Err(e) => Err(format!("Invalid parameters: {}", e)),
    }
}

// ============================================================================
// Utility Functions
// ============================================================================

/// Default group name for serde defaults
pub fn default_group() -> String {
    "default".to_string()
}

/// Asserts a descriptor's params name only fields its parser reads (all of them when
/// `exact`), its `required` list is among them, and its example parses. `P` derives
/// `Serialize + Default` under `cfg(test)` so its field names can be read.
#[cfg(test)]
pub fn assert_descriptor_fields<P>(descriptors: &[CommandDescriptor], command: &str, exact: bool)
where
    P: Serialize + Default + for<'de> Deserialize<'de>,
{
    let d = descriptors.iter().find(|d| d.name == command)
        .unwrap_or_else(|| panic!("no descriptor for {command}"));
    let keys = |v: &Value| -> Vec<String> {
        let mut k: Vec<String> = v.as_object().map(|o| o.keys().cloned().collect()).unwrap_or_default();
        k.sort();
        k
    };
    let declared = keys(&d.params_schema["properties"]);
    let parsed = keys(&serde_json::to_value(P::default()).unwrap());
    if exact {
        assert_eq!(declared, parsed, "{command}: descriptor properties vs parser fields");
    } else {
        let unknown: Vec<&String> = declared.iter().filter(|f| !parsed.contains(f)).collect();
        assert!(unknown.is_empty(), "{command}: descriptor names fields the parser never reads: {unknown:?}");
    }
    for required in d.params_schema["required"].as_array().into_iter().flatten() {
        let name = required.as_str().unwrap_or_default();
        assert!(declared.iter().any(|f| f == name), "{command}: required '{name}' is not a declared property");
    }
    let example: Value = serde_json::from_str(d.example)
        .unwrap_or_else(|e| panic!("{command}: example is not JSON: {e}"));
    assert_eq!(example["cmd"], command, "{command}: example names another command");
    for field in keys(&example["params"]) {
        assert!(declared.contains(&field), "{command}: example uses undeclared '{field}'");
    }
    serde_json::from_value::<P>(example["params"].clone())
        .unwrap_or_else(|e| panic!("{command}: example params do not parse: {e}"));
}

/// Get current memory usage in KB (rough estimate)
pub fn get_memory_usage_kb() -> u64 {
    // Simple estimation based on typical gNode usage
    4600 // ~4.6MB as observed in production
}

/// Calculate Euclidean distance between two 8D capability vectors
///
/// Computes the Euclidean distance in 8-dimensional capability space for template similarity.
/// Uses the standard 8 dimensions: html, complexity, interactivity, data_density, reusability,
/// cacheability, semantic_layout, and render_cost.
pub fn calculate_euclidean_distance(
    caps_a: &HashMap<String, f64>,
    caps_b: &HashMap<String, f64>
) -> f64 {
    // Standard 8 dimensions for template capability space
    let dimensions = [
        "html",
        "complexity",
        "interactivity",
        "data_density",
        "reusability",
        "cacheability",
        "semantic_layout",
        "render_cost"
    ];

    // Use Q64.64 fixed-point arithmetic for deterministic results across all nodes
    let mut sum_squared = FixedPoint::from_int(0);
    for dim in dimensions.iter() {
        let a = FixedPoint::from_f64(caps_a.get(*dim).copied().unwrap_or(0.0));
        let b = FixedPoint::from_f64(caps_b.get(*dim).copied().unwrap_or(0.0));
        let diff = a - b;
        sum_squared = sum_squared + diff * diff;
    }

    // Q64.64 sqrt for determinism, then convert to f64 for JSON output
    sum_squared.sqrt().to_f64()
}

// ============================================================================
// Command Descriptor (Autodocumentation)
// ============================================================================

/// Execution lane for a command.
///
/// The daemon executes commands through one of two pipelines, declared
/// per-command rather than per-code-path:
///
/// - `Concurrent`     — async-spawned execution. The consumer-group reader
///                hands the command to a tokio task and immediately
///                reads the next batch. Many in-flight per consumer
///                thread, no ordering guarantee between requests.
///                Default for idempotent FCALL wrappers and read-only
///                operations (registerService, geometric_discover,
///                template_fragment, ping, etc.).
///
/// - `Ordered`  — synchronous in-thread execution. The handler runs
///                inline before the consumer reads the next message;
///                ordering preserved across the batch. For commands
///                with cross-key transactional semantics or commands
///                whose effects subsequent reads must observe (site
///                provisioning, deprovisioning, relay policy writes,
///                topo_delete, batch when caller flags ordered).
///
/// See `COMMAND_SCHEMA.md` § Lane Semantics for the full rationale +
/// per-command assignment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Lane {
    /// Async-spawned, unordered, high-throughput. Safe default.
    Concurrent,
    /// Synchronous inline, ordering preserved. Use only when caller
    /// semantics demand it.
    Ordered,
}

impl Default for Lane {
    fn default() -> Self {
        // Concurrent is the safe default — most commands are idempotent
        // FCALL wrappers or read-only and don't need ordering. Opt
        // into Ordered explicitly in the handler registration.
        Lane::Concurrent
    }
}

/// Schema descriptor for a command, enabling runtime API discovery.
///
/// Each handler module registers descriptors alongside its command handlers.
/// Clients can query these via the `describe` command to learn parameter
/// formats without external documentation.
#[derive(Debug, Clone, Serialize)]
pub struct CommandDescriptor {
    /// Canonical command name (lowercase, no aliases)
    pub name: &'static str,
    /// Handler category (e.g. "system", "geometric", "topology")
    pub category: &'static str,
    /// Human-readable description of what the command does
    pub description: &'static str,
    /// JSON Schema for command parameters
    pub params_schema: Value,
    /// JSON Schema for the result payload (inside status:"ok")
    pub returns_schema: Value,
    /// Example invocation as a JSON string
    pub example: &'static str,
    /// Whether the command has an async handler
    pub async_capable: bool,
    /// Execution lane (Concurrent = async-spawned, Ordered = synchronous inline).
    /// Defaults to Concurrent — opt into Ordered for commands with
    /// cross-request ordering semantics. See `Lane` doc above.
    #[serde(default)]
    pub lane: Lane,
}

impl CommandDescriptor {
    /// Convert to a JSON value for API responses
    pub fn to_json(&self) -> Value {
        serde_json::to_value(self).unwrap_or_default()
    }
}

// ============================================================================
// Utility Functions
// ============================================================================

/// Convert a FixedVector point to a HashMap of capabilities
///
/// Converts the geometric representation (FixedVector with fixed-point arithmetic)
/// back to a human-readable HashMap<String, f64> using the capability dimensions mapping.
pub fn fixed_vector_to_capabilities(
    point: &crate::FixedVector,
    capability_dimensions: &HashMap<String, usize>
) -> HashMap<String, f64> {
    let mut capabilities = HashMap::new();

    for (name, &dimension) in capability_dimensions {
        if dimension < point.len() {
            let value = point[dimension].to_f64();
            capabilities.insert(name.clone(), value);
        }
    }

    capabilities
}
