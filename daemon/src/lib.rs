use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use serde_json::Value;
use redis::{FromRedisValue, RedisResult, Value as RedisValue};
use thiserror::Error;

// Import FixedPoint/FixedVector for Q64.64 operations (via g_math)
use crate::geometric_precision::{FixedPoint, FixedVector};

// Re-export daemon module
pub mod daemon;
pub use daemon::{GNodeDaemon, Command, Response, ThreadConfig};

// KeyBased architecture modules
pub mod compute_handler;

// Asset management (manifest-driven bundle builder — CMS extension)
pub mod asset_builder;

// Geometric precision module (thin re-export from g_math crate)
pub mod geometric_precision;


// Utility functions
pub mod utils;

// Integration module (replacing daemon_script_integration)
pub mod integration;

// ValKey function manager
pub mod valkey_function_manager;

// Template module for extensible message formats (CMS extension)
pub mod template;

// Configuration module
pub mod config;

// Routing configuration module for dynamic message routing
pub mod routing_config;

// Node configuration module for custom node types
pub mod node_config;

// Worker abstraction for multi-threaded and single-threaded modes
pub mod worker;

// Custom topology module for user-defined topologies with Q64.64 precision

// Relational topology module for 3D geometric relationship encoding
// Transforms graph problems into spatial problems with O(1) operations

// Unified configuration module - consolidates GNodeSettings, NodeConfig, RoutingConfig
// Supports hot reload via SIGHUP for applicable settings
pub mod unified_config;

// Authorized signer (Ed25519 public key baked into the daemon)
pub mod ext_author;
// Runtime signed-extension verifier (mirrors build.rs; used by the Lua loader)
pub mod ext_verify;

// Extension manager for optional feature discovery and introspection
pub mod extensions;

// Tool registration module for deploy-time registration of tool-tier services
pub mod tool_registration;

/// One schema for every shape of the constellation topology (SPEC-ONE-TOPOLOGY §3).
pub mod topology_schema;

/// The daemon's client of the one topology: encoder and typed FCALL wrappers (SPEC §4).
pub mod constellation;

/// Placements from heartbeats and the health tick, under one lease per constellation (SPEC §5).
pub mod constellation_worker;


/// Registration intent: a site declares what it is once, and its capability
/// vector is DERIVED from that intent times the current schema — so a schema
/// change re-derives every entity instead of leaving them at whatever layout
/// was current the day they were onboarded.
pub mod registration_intent;

// Ecosystem bootstrap loader (disk-minimal + ValKey-resident config).
// Sole entry point for ecosystem config; replaces dotenv calls.
pub mod ecosystem_config;

// The daemon_script_integration module has been completely replaced by the integration module
// The format_handler module has been moved into integration/command_processor.rs

// Implementation of FromRedisValue for serde_json::Value to fix trait bound issues
// We need to use a type wrapper since we can't implement a foreign trait for a foreign type directly
#[derive(Debug, Clone)]
pub struct JsonValue(Value);

impl From<JsonValue> for Value {
    fn from(json_value: JsonValue) -> Self {
        json_value.0
    }
}

// Helper function to convert Redis commands returning JsonValue to Value
pub fn json_redis_cmd<T: redis::ToRedisArgs>(cmd: &str, args: T, conn: &mut redis::Connection) -> RedisResult<Value> {
    let json_value: JsonValue = redis::cmd(cmd).arg(args).query(conn)?;
    Ok(json_value.0)
}

// Async helper function to convert Redis commands returning JsonValue to Value
pub async fn json_redis_cmd_async<T: redis::ToRedisArgs>(cmd: &str, args: T, conn: &mut redis::aio::MultiplexedConnection) -> RedisResult<Value> {
    let json_value: JsonValue = redis::cmd(cmd).arg(args).query_async(conn).await?;
    Ok(json_value.0)
}

impl FromRedisValue for JsonValue {
    fn from_redis_value(v: &RedisValue) -> RedisResult<Self> {
        match v {
            RedisValue::BulkString(data) => {
                match serde_json::from_slice(data) {
                    Ok(value) => Ok(JsonValue(value)),
                    Err(_e) => {
                        // Create a RedisError using the proper constructor
                        let err = redis::RedisError::from((redis::ErrorKind::TypeError, "Invalid JSON", "Failed to parse JSON data".to_string()));
                        Err(err)
                    }
                }
            },
            RedisValue::Int(i) => Ok(JsonValue(Value::Number((*i).into()))),
            RedisValue::Nil => Ok(JsonValue(Value::Null)),
            RedisValue::SimpleString(s) => Ok(JsonValue(Value::String(s.to_string()))),
            RedisValue::Array(items) => {
                // Collect all values that can be converted
                let mut values = Vec::new();
                for item in items {
                    match JsonValue::from_redis_value(item) {
                        Ok(value) => values.push(value.0),
                        Err(_) => continue,
                    }
                }
                
                // Create a JSON array from the collected values
                Ok(JsonValue(Value::Array(values)))
            },
            // Remove the RedisValue::Map variant as it doesn't exist in the redis crate
            // Handle other cases as needed
            _ => {
                // Create a RedisError using the proper constructor
                let err = redis::RedisError::from((redis::ErrorKind::TypeError, "Unsupported Redis value type", "Cannot convert this Redis value type to JSON".to_string()));
                Err(err)
            }
        }
    }
}

// Error handling
#[derive(Error, Debug)]
pub enum GeometricError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    
    #[error("YAML error: {0}")]
    Yaml(String),
    
    #[error("Bincode error: {0}")]
    Bincode(#[from] bincode::Error),
    
    #[error("Dimension mismatch: expected {expected} but got {actual}")]
    DimensionMismatch { expected: usize, actual: usize },
    
    #[error("Service not found: {0}")]
    ServiceNotFound(String),
    
    #[error("Invalid state: {0}")]
    InvalidState(String),
    
    #[error("Redis error: {0}")]
    Redis(#[from] redis::RedisError),
    
    #[error("Service registration failed: {0}")]
    RegistrationFailed(String),
    
    #[error("Other error: {0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, GeometricError>;

// Basic types

/// What remains of the in-memory topology engine (WP3e K12): a handle the handler
/// signatures and the daemon still thread, plus the Q64.64 helpers the legacy registration
/// paths call until Phase B removes them. The engine's state, discovery and storage left
/// with the custom-topology and 3-D handlers; the one topology lives in ValKey and is read
/// through `constellation.rs`.
#[derive(Debug, Clone, Default)]
pub struct GeometricTopology {
    pub dimensions: usize,
    pub services: HashMap<String, Value>,
    pub capability_dimensions: HashMap<String, usize>,
}

impl GeometricTopology {
    pub fn new(dimensions: usize) -> Self {
        Self { dimensions, ..Self::default() }
    }

    /// A point's grid cell as a fixed-width string, Q64.64 multiply then integer truncation:
    /// identical on every node. Legacy registration keeps writing it until Phase B.
    pub fn point_to_bucket_key(point: &FixedVector, grid_size: usize) -> String {
        let grid_fp = FixedPoint::from_int(grid_size as i32);
        let mut key = String::with_capacity(point.len() * 4);
        for i in 0..point.len() {
            key.push_str(&format!("{:04}", (point[i] * grid_fp).to_int()));
        }
        key
    }

    /// The same cell as integers, for a consumer that composes its own key.
    pub fn point_to_bucket_key_raw(point: &FixedVector, grid_size: usize) -> Vec<i64> {
        let grid_fp = FixedPoint::from_int(grid_size as i32);
        (0..point.len()).map(|i| (point[i] * grid_fp).to_int() as i64).collect()
    }

    /// The legacy z-order score: axis 16 (load) scaled to [0, 1_000_000]; axis 0 for a short point.
    pub fn compute_service_z_score(point: &FixedVector) -> i64 {
        let scale = FixedPoint::from_int(1_000_000);
        if point.len() > 16 {
            (point[16] * scale).to_int() as i64
        } else if !point.is_empty() {
            (point[0] * scale).to_int() as i64
        } else {
            0
        }
    }

    /// The legacy per-site topology key, `{site}:gnode:services`; retires with Phase B.
    pub fn get_services_topology_key(site_id: &str) -> String {
        format!("{{{}}}:gnode:services", site_id)
    }
}

/// The shared handle the daemon constructs and the handlers receive.
#[derive(Clone)]
pub struct SharedTopology {
    inner: Arc<RwLock<GeometricTopology>>,
}

impl SharedTopology {
    pub fn new(dimensions: usize) -> Self {
        Self { inner: Arc::new(RwLock::new(GeometricTopology::new(dimensions))) }
    }

    /// Kept for the daemon's constructor; the storage arguments no longer mean anything.
    pub fn with_storage(dimensions: usize, _redis_url: &str, _site_id: &str, _prefix: &str) -> Result<Self> {
        Ok(Self::new(dimensions))
    }

    pub fn get_topology_ref(&self) -> Arc<RwLock<GeometricTopology>> {
        Arc::clone(&self.inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_keys_are_deterministic_and_the_raw_form_agrees() {
        let p = FixedVector::from_slice(&[FixedPoint::from_f64(0.35), FixedPoint::from_f64(0.9), FixedPoint::from_f64(0.0)]);
        assert_eq!(GeometricTopology::point_to_bucket_key(&p, 10), "000300090000");
        assert_eq!(GeometricTopology::point_to_bucket_key(&p, 10), GeometricTopology::point_to_bucket_key(&p, 10));
        assert_eq!(GeometricTopology::point_to_bucket_key_raw(&p, 10), vec![3, 9, 0]);
    }

    #[test]
    fn the_legacy_z_score_reads_axis_sixteen() {
        let mut v = vec![FixedPoint::from_f64(0.0); 23];
        v[16] = FixedPoint::from_f64(0.75);
        assert_eq!(GeometricTopology::compute_service_z_score(&FixedVector::from_slice(&v)), 750_000, "a binary-exact value: the legacy score truncates");
        assert_eq!(GeometricTopology::compute_service_z_score(&FixedVector::from_slice(&[FixedPoint::from_f64(0.5)])), 500_000);
        assert_eq!(GeometricTopology::compute_service_z_score(&FixedVector::new(0)), 0);
    }

    #[test]
    fn the_legacy_key_is_per_site() {
        assert_eq!(GeometricTopology::get_services_topology_key("nierto_com"), "{nierto_com}:gnode:services");
    }
}

/// Serialises tests that mutate process-global environment variables.
///
/// `std::env::set_var` affects the whole process, not the calling thread, and
/// cargo runs tests in parallel threads of one process. Two tests pointing
/// `GNODE_FUNCTIONS_DIR` at different temp directories therefore read each
/// other's value — or a removed one — and fail intermittently. Any test that
/// sets or removes an environment variable must hold this lock.
#[cfg(test)]
pub(crate) static TEST_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Acquire `TEST_ENV_LOCK`, ignoring poisoning: a panicking test leaves the
/// mutex poisoned, and failing every later test for it would hide the original
/// failure behind a cascade.
#[cfg(test)]
pub(crate) fn test_env_guard() -> std::sync::MutexGuard<'static, ()> {
    TEST_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}
