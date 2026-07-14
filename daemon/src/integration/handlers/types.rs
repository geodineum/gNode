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
// Static mapping of capability names to dimension indices for the 30-dimensional
// service topology (25 discovery + 5 storage-only). Schema version 3.0.
//
// Used by stateless command handlers for:
//   - Building capability vectors from service registration
//   - Computing bucket keys for voxel storage (discovery dims 0-24)
//   - Distance calculations for service discovery
//
// These defaults are for the SERVICE tier. Other tiers (tool=16D, constellation=20D,
// galaxy=20D) use the generic build_capability_vector() with schema-derived counts.
// See: gNode/daemon/config/{service,tool,constellation,galaxy}_schema.yaml

/// Service tier: dimensions used for discovery (bucket key hashing)
pub const DISCOVERY_DIMENSIONS: usize = 25;
/// Service tier: total dimensions (discovery + storage-only)
pub const TOTAL_DIMENSIONS: usize = 30;

/// Static 30D capability dimension mapping for service topology (schema v3.0).
/// Maps capability names to their dimension indices (0-29).
///
/// Layer architecture:
///   Layer 1 (0-3):   Interface Identity - protocol, format, version, stability
///   Layer 2 (4-6):   Access Control - clearance, auth, sensitivity
///   Layer 3 (7):     Service Scope
///   Layer 4 (8-10):  Functional Domain - primary, secondary, specialization
///   Layer 5 (11-13): Performance Profile - throughput, latency, reliability
///   Layer 6 (14-15): Workflow Context - pipeline_stage, priority
///   Layer 7 (16-18): Runtime State - current_load, health_status, lifecycle_state
///   Layer 8 (19-21): Classification - service_tier, environment, implementation_language
///   Layer 9 (22-24): Network Context - network_zone, data_persistence, update_channel
///   Layer 10 (25-27): Visual Topology - user_x, user_y, user_z (storage-only)
///   Layer 11 (28-29): Metadata - deployment_model, registration_order (storage-only)
pub static SERVICE_DIMENSIONS: Lazy<HashMap<String, usize>> = Lazy::new(|| {
    let mut dims = HashMap::new();

    // Layer 1: Interface Identity (0-3)
    dims.insert("protocol".to_string(), 0);
    dims.insert("native_format".to_string(), 1);
    dims.insert("api_version".to_string(), 2);
    dims.insert("contract_stability".to_string(), 3);

    // Layer 2: Access Control (4-6)
    dims.insert("clearance_required".to_string(), 4);
    dims.insert("auth_method".to_string(), 5);
    dims.insert("data_sensitivity".to_string(), 6);

    // Layer 3: Service Scope (7)
    dims.insert("service_scope".to_string(), 7);

    // Layer 4: Functional Domain (8-10)
    dims.insert("domain_primary".to_string(), 8);
    dims.insert("domain_secondary".to_string(), 9);
    dims.insert("specialization".to_string(), 10);

    // Layer 5: Performance Profile (11-13)
    dims.insert("throughput_tier".to_string(), 11);
    dims.insert("latency_class".to_string(), 12);
    dims.insert("reliability_tier".to_string(), 13);

    // Layer 6: Workflow Context (14-15)
    dims.insert("pipeline_stage".to_string(), 14);
    dims.insert("execution_priority".to_string(), 15);

    // Layer 7: Runtime State (16-18) — dynamic
    dims.insert("current_load".to_string(), 16);
    dims.insert("health_status".to_string(), 17);
    dims.insert("lifecycle_state".to_string(), 18);

    // Layer 8: Classification (19-21)
    dims.insert("service_tier".to_string(), 19);
    dims.insert("environment".to_string(), 20);
    dims.insert("implementation_language".to_string(), 21);

    // Layer 9: Network Context (22-24)
    dims.insert("network_zone".to_string(), 22);
    dims.insert("data_persistence".to_string(), 23);
    dims.insert("update_channel".to_string(), 24);

    // Layer 10: Visual Topology (25-27) — storage-only
    dims.insert("user_x".to_string(), 25);
    dims.insert("user_y".to_string(), 26);
    dims.insert("user_z".to_string(), 27);

    // Layer 11: Metadata (28-29) — storage-only
    dims.insert("deployment_model".to_string(), 28);
    dims.insert("registration_order".to_string(), 29);

    // Common aliases
    dims.insert("load".to_string(), 16);
    dims.insert("health".to_string(), 17);
    dims.insert("lifecycle".to_string(), 18);
    dims.insert("tier".to_string(), 19);
    dims.insert("env".to_string(), 20);
    dims.insert("language".to_string(), 21);

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

/// Extract discovery-only dimensions from a full point (for bucket key computation).
/// The discovery_dims count comes from the tier's schema.
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
// Service tier = the local-per-site service topology. 30 total dimensions,
// 25 of which feed the discovery bucket key. Other tiers (tool, constellation,
// galaxy) have their own dim counts loaded from their tier schema YAML —
// see daemon/config/{service,tool,constellation,galaxy}_schema.yaml.
//
// Custom topologies created via topo_create / gNode-TOPO have user-defined
// dim counts and live in handlers/topology_custom.rs + custom_topology.rs.

/// Build a service-tier FixedVector from a capability name→value HashMap.
/// Reads dim count + dim map from the service tier (TOTAL_DIMENSIONS = 30).
pub fn build_service_capability_vector(capabilities: &HashMap<String, f64>) -> crate::geometric_precision::FixedVector {
    build_capability_vector(capabilities, TOTAL_DIMENSIONS, get_service_dimensions())
}

/// Build a discovery-only point from a full service-tier point.
/// Service tier: 25 discovery dims sliced from the 30D full vector.
pub fn discovery_point_from_full(full_point: &crate::geometric_precision::FixedVector) -> crate::geometric_precision::FixedVector {
    discovery_point(full_point, DISCOVERY_DIMENSIONS)
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
/// - `Fast`     — async-spawned execution. The consumer-group reader
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
    Fast,
    /// Synchronous inline, ordering preserved. Use only when caller
    /// semantics demand it.
    Ordered,
}

impl Default for Lane {
    fn default() -> Self {
        // Fast is the safe default — most commands are idempotent
        // FCALL wrappers or read-only and don't need ordering. Opt
        // into Ordered explicitly in the handler registration.
        Lane::Fast
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
    /// Execution lane (Fast = async-spawned, Ordered = synchronous inline).
    /// Defaults to Fast — opt into Ordered for commands with
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
