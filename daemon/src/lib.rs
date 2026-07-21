use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use serde::{Serialize, Deserialize};
use serde_json::Value;
use redis::{FromRedisValue, RedisResult, Value as RedisValue};
use thiserror::Error;
use log::info;

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
pub mod custom_topology;

// Relational topology module for 3D geometric relationship encoding
// Transforms graph problems into spatial problems with O(1) operations
pub mod relational_topology;

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
pub type ServiceId = String;
pub type CapabilityPoint = FixedVector;
pub type RequirementPoint = FixedVector;

// Service configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Capability {
    pub name: String,
    pub value: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Requirement {
    pub name: String,
    pub min_value: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RequirementSet {
    pub requirements: Vec<Requirement>,
}

/// Range query operators for dynamic dimensions (Phase 1)
/// Enables flexible service discovery with comparison operators
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DimensionRequirement {
    /// Equal to (with 0.005 tolerance for float comparison)
    Eq(f64),
    /// Not equal to
    Neq(f64),
    /// Greater than
    Gt(f64),
    /// Greater than or equal
    Gte(f64),
    /// Less than
    Lt(f64),
    /// Less than or equal
    Lte(f64),
    /// Between min and max (inclusive)
    Range(f64, f64),
}

impl DimensionRequirement {
    /// Check if a Q64.64 fixed-point value matches this requirement
    /// This is the multi-node deterministic version - use this for all discovery operations
    pub fn matches_fixed(&self, value: FixedPoint) -> bool {
        // Tolerance for equality: 0.005 in Q64.64 = 0.005 * 2^64 ≈ 92233720368547758
        const EQ_TOLERANCE: i128 = 92233720368547758;

        match self {
            Self::Eq(target) => {
                let target_fp = FixedPoint::from_f64(*target);
                let diff = if value >= target_fp {
                    value - target_fp
                } else {
                    target_fp - value
                };
                diff.raw() < EQ_TOLERANCE
            }
            Self::Neq(target) => {
                let target_fp = FixedPoint::from_f64(*target);
                let diff = if value >= target_fp {
                    value - target_fp
                } else {
                    target_fp - value
                };
                diff.raw() >= EQ_TOLERANCE
            }
            Self::Gt(min) => value > FixedPoint::from_f64(*min),
            Self::Gte(min) => value >= FixedPoint::from_f64(*min),
            Self::Lt(max) => value < FixedPoint::from_f64(*max),
            Self::Lte(max) => value <= FixedPoint::from_f64(*max),
            Self::Range(min, max) => {
                value >= FixedPoint::from_f64(*min) && value <= FixedPoint::from_f64(*max)
            }
        }
    }
}

/// Requirements for range-based discovery
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RangeRequirements {
    /// Map of dimension index to requirement
    pub requirements: HashMap<usize, DimensionRequirement>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServiceConfig {
    pub id: ServiceId,
    pub capabilities: HashMap<String, f64>,
    pub metadata: HashMap<String, String>,
}

// Geometric Topology
#[derive(Clone, Debug, Serialize)]
pub struct GeometricTopology {
    pub dimensions: usize,
    pub services: HashMap<ServiceId, ServicePointData>,
    pub capability_dimensions: HashMap<String, usize>,
    pub dependencies: HashMap<ServiceId, Vec<ServiceId>>,

    /// Spatial hash for O(1) service discovery
    /// Maps grid bucket keys to lists of service IDs in that bucket
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spatial_hash: Option<SpatialHash>,
}

/// Spatial hash structure for O(1) geometric lookup
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SpatialHash {
    /// Grid size for quantization (default: 10)
    pub grid_size: usize,

    /// Buckets mapping grid keys to service IDs
    pub buckets: HashMap<String, Vec<ServiceId>>,
}

// Custom Deserialize to handle capability_dimensions as both Array and Object
impl<'de> serde::Deserialize<'de> for GeometricTopology {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, MapAccess, Visitor};
        use std::fmt;

        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "snake_case")]
        enum Field {
            Dimensions,
            Services,
            CapabilityDimensions,
            Dependencies,
            SpatialHash,
            #[serde(rename = "hash")]
            Hash, // Alternate field name from ValKey Lua functions
        }

        struct GeometricTopologyVisitor;

        impl<'de> Visitor<'de> for GeometricTopologyVisitor {
            type Value = GeometricTopology;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct GeometricTopology")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<GeometricTopology, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut dimensions = None;
                let mut services = None;
                let mut capability_dimensions: Option<HashMap<String, usize>> = None;
                let mut dependencies = None;
                let mut spatial_hash = None;

                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Dimensions => {
                            if dimensions.is_some() {
                                return Err(de::Error::duplicate_field("dimensions"));
                            }
                            dimensions = Some(map.next_value()?);
                        }
                        Field::Services => {
                            if services.is_some() {
                                return Err(de::Error::duplicate_field("services"));
                            }

                            // Manually deserialize services HashMap to reconstruct id field
                            // ValKey stores: {"service-id": {point, metadata, dependencies}}
                            // We need: {"service-id": {id: "service-id", point, metadata}}
                            let services_value: serde_json::Value = map.next_value()?;

                            if let Some(obj) = services_value.as_object() {
                                let mut services_map = HashMap::new();

                                for (service_id, service_data) in obj {
                                    // Extract point array
                                    let point_vec: Vec<f64> = service_data
                                        .get("point")
                                        .and_then(|v| v.as_array())
                                        .ok_or_else(|| de::Error::missing_field("point"))?
                                        .iter()
                                        .filter_map(|v| v.as_f64())
                                        .collect();

                                    // Convert Vec<f64> → FixedVector
                                    let fixed_point = FixedVector::from_f32_slice(
                                        &point_vec.iter().map(|&x| x as f32).collect::<Vec<_>>()
                                    );

                                    // Extract metadata (optional, default to empty)
                                    let metadata = service_data
                                        .get("metadata")
                                        .and_then(|v| v.as_object())
                                        .map(|obj| {
                                            obj.iter()
                                                .filter_map(|(k, v)| {
                                                    v.as_str().map(|s| (k.clone(), s.to_string()))
                                                })
                                                .collect::<HashMap<String, String>>()
                                        })
                                        .unwrap_or_else(HashMap::new);

                                    // Reconstruct ServicePointData with id from HashMap key
                                    services_map.insert(
                                        service_id.clone(),
                                        ServicePointData {
                                            id: service_id.clone(),
                                            point: fixed_point,
                                            metadata,
                                        }
                                    );
                                }

                                services = Some(services_map);
                            } else {
                                return Err(de::Error::custom("services must be an object"));
                            }
                        }
                        Field::CapabilityDimensions => {
                            if capability_dimensions.is_some() {
                                return Err(de::Error::duplicate_field("capability_dimensions"));
                            }

                            // Try to deserialize as HashMap first (preferred format)
                            let value: serde_json::Value = map.next_value()?;

                            if let Some(obj) = value.as_object() {
                                // Object format: {"storage": 0, "compute": 1, ...}
                                let mut cap_dims = HashMap::new();
                                for (k, v) in obj {
                                    if let Some(idx) = v.as_u64() {
                                        cap_dims.insert(k.clone(), idx as usize);
                                    }
                                }
                                capability_dimensions = Some(cap_dims);
                            } else if let Some(arr) = value.as_array() {
                                // Array format: ["storage", "compute", ...]
                                // Convert to HashMap with index as value
                                let mut cap_dims = HashMap::new();
                                for (idx, item) in arr.iter().enumerate() {
                                    if let Some(name) = item.as_str() {
                                        cap_dims.insert(name.to_string(), idx);
                                    }
                                }
                                capability_dimensions = Some(cap_dims);
                            }
                        }
                        Field::Dependencies => {
                            if dependencies.is_some() {
                                return Err(de::Error::duplicate_field("dependencies"));
                            }
                            dependencies = Some(map.next_value()?);
                        }
                        Field::SpatialHash | Field::Hash => {
                            if spatial_hash.is_some() {
                                return Err(de::Error::duplicate_field("spatial_hash"));
                            }
                            spatial_hash = Some(map.next_value()?);
                        }
                    }
                }

                let dimensions = dimensions.ok_or_else(|| de::Error::missing_field("dimensions"))?;
                let services = services.ok_or_else(|| de::Error::missing_field("services"))?;
                let capability_dimensions = capability_dimensions.unwrap_or_default();
                let dependencies = dependencies.unwrap_or_else(HashMap::new);

                Ok(GeometricTopology {
                    dimensions,
                    services,
                    capability_dimensions,
                    dependencies,
                    spatial_hash,
                })
            }
        }

        const FIELDS: &[&str] = &["dimensions", "services", "capability_dimensions", "dependencies", "spatial_hash", "hash"];
        deserializer.deserialize_struct("GeometricTopology", FIELDS, GeometricTopologyVisitor)
    }
}

#[derive(Clone, Debug)]
pub struct ServicePointData {
    pub id: ServiceId,
    pub point: CapabilityPoint,
    pub metadata: HashMap<String, String>,
}

// Custom Serialize implementation to convert FixedVector → JSON
// Includes Q64.64 fixed-point values for deterministic distributed operations
//
// Field abbreviations (to save space in ValKey storage):
//   pr  = point_raw      : Q64.64 i128 values as strings (authoritative for calculations)
//   pd  = point_display  : Floats capped at 3 decimals (human readable)
//   bk  = bucket_key     : Pre-computed voxel bucket key string
//   bkr = bucket_key_raw : Bucket key as Vec<i64> for Lua comparison
//   zs  = z_score        : Scaled i64 for ValKey ZADD (dimension 16 or first dim)
//   m   = metadata       : User-provided metadata
//
// CRITICAL: All calculations MUST use 'pr' (point_raw) values, NEVER 'pd' floats
impl Serialize for ServicePointData {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;

        // pr: Q64.64 raw i128 values as decimal strings - USE THIS FOR ALL CALCULATIONS
        // Serialized as strings because JSON numbers cannot represent i128
        let point_raw: Vec<String> = (0..self.point.len())
            .map(|i| self.point[i].raw().to_string())
            .collect();

        // pd: Floats rounded to 3 decimals - DISPLAY ONLY, never use for math
        let point_display: Vec<f64> = (0..self.point.len())
            .map(|i| (self.point[i].to_f64() * 1000.0).round() / 1000.0)
            .collect();

        // bk/bkr: Pre-compute bucket key using Q64.64 arithmetic (deterministic)
        // This allows Lua to do O(1) hash lookups without any float math
        let grid_size = 10; // Standard grid size
        let bucket_key_raw = GeometricTopology::point_to_bucket_key_raw(&self.point, grid_size);
        let bucket_key: String = bucket_key_raw.iter()
            .map(|&v| format!("{:04}", v))
            .collect();

        // zs: Z-score for sorted set ordering (scaled to i64)
        // Uses dimension 16 (current_load) × 1,000,000 for ZADD ordering
        let z_score: i64 = GeometricTopology::compute_service_z_score(&self.point);

        // Serialize with abbreviated field names for space efficiency
        let mut state = serializer.serialize_struct("ServicePointData", 8)?;
        state.serialize_field("id", &self.id)?;
        state.serialize_field("pr", &point_raw)?;           // Q64.64 raw (authoritative)
        state.serialize_field("pd", &point_display)?;       // Display floats (3 decimals)
        state.serialize_field("bk", &bucket_key)?;          // Bucket key string
        state.serialize_field("bkr", &bucket_key_raw)?;     // Bucket key raw
        state.serialize_field("zs", &z_score)?;             // Z-score for ZADD
        state.serialize_field("m", &self.metadata)?;        // Metadata
        state.serialize_field("point", &point_display)?;    // Full-name alias for pd
        state.end()
    }
}

// Custom Deserialize implementation to convert JSON → ServicePointData
// Prioritizes 'pr' (point_raw) Q64.64 values for deterministic precision
// Falls back to 'point'/'pd' floats when Q64.64 raw values are absent
//
// Field priority for point reconstruction:
//   1. pr (point_raw) - Q64.64 i128 values as strings (PREFERRED - deterministic)
//      Also accepts legacy Q32.32 i64 numbers (promoted via << 32)
//   2. point/pd - Float values (FALLBACK - loses precision)
impl<'de> serde::Deserialize<'de> for ServicePointData {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, MapAccess, Visitor, IgnoredAny};
        use std::fmt;

        #[derive(Deserialize)]
        #[serde(field_identifier)]
        enum Field {
            #[serde(rename = "id")]
            Id,
            #[serde(rename = "pr")]
            PointRaw,           // Q64.64 i128 strings or legacy Q32.32 i64 numbers
            #[serde(rename = "pd")]
            PointDisplay,       // Display floats (ignored, use pr)
            #[serde(rename = "point")]
            Point,              // Full-name float values (fallback)
            #[serde(rename = "m")]
            MetadataAbbrev,     // Abbreviated metadata
            #[serde(rename = "metadata")]
            Metadata,           // Full-name metadata
            #[serde(rename = "bk")]
            BucketKey,          // Computed, ignored
            #[serde(rename = "bkr")]
            BucketKeyRaw,       // Computed, ignored
            #[serde(rename = "bucket_key")]
            BucketKeyLegacy,    // Full-name alias, ignored
            #[serde(rename = "bucket_key_raw")]
            BucketKeyRawLegacy, // Full-name alias, ignored
            #[serde(rename = "zs")]
            ZScore,             // Computed, ignored
            #[serde(rename = "c")]
            Capabilities,       // Original capabilities, ignored
            #[serde(other)]
            Unknown,
        }

        struct ServicePointDataVisitor;

        impl<'de> Visitor<'de> for ServicePointDataVisitor {
            type Value = ServicePointData;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct ServicePointData")
            }

            fn visit_map<V>(self, mut map: V) -> std::result::Result<ServicePointData, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut id = None;
                // pr is now Vec<serde_json::Value> to accept both strings (Q64.64) and numbers (legacy Q32.32)
                let mut point_raw: Option<Vec<serde_json::Value>> = None;
                let mut point_float: Option<Vec<f64>> = None; // Fallback
                let mut metadata: Option<HashMap<String, String>> = None;

                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Id => {
                            if id.is_some() {
                                return Err(de::Error::duplicate_field("id"));
                            }
                            id = Some(map.next_value()?);
                        }
                        Field::PointRaw => {
                            // pr: Q64.64 raw values (strings) or legacy Q32.32 (numbers) - PREFERRED
                            if point_raw.is_some() {
                                return Err(de::Error::duplicate_field("pr"));
                            }
                            point_raw = Some(map.next_value()?);
                        }
                        Field::Point => {
                            // Full-name point field - fallback when pr absent
                            if point_float.is_none() {
                                point_float = Some(map.next_value()?);
                            } else {
                                let _: IgnoredAny = map.next_value()?;
                            }
                        }
                        Field::MetadataAbbrev => {
                            // m: abbreviated metadata
                            if metadata.is_some() {
                                return Err(de::Error::duplicate_field("m"));
                            }
                            metadata = Some(map.next_value()?);
                        }
                        Field::Metadata => {
                            // Full-name metadata field
                            if metadata.is_none() {
                                metadata = Some(map.next_value()?);
                            } else {
                                let _: IgnoredAny = map.next_value()?;
                            }
                        }
                        // Ignore computed/display fields
                        Field::PointDisplay | Field::BucketKey | Field::BucketKeyRaw |
                        Field::BucketKeyLegacy | Field::BucketKeyRawLegacy |
                        Field::ZScore | Field::Capabilities | Field::Unknown => {
                            let _: IgnoredAny = map.next_value()?;
                        }
                    }
                }

                let id = id.ok_or_else(|| de::Error::missing_field("id"))?;
                let metadata = metadata.unwrap_or_default();

                // Reconstruct FixedVector - prefer raw values for precision
                let fixed_point = if let Some(raw_values) = point_raw {
                    let mut vec = FixedVector::new(raw_values.len());
                    for (i, val) in raw_values.iter().enumerate() {
                        if let Some(raw_str) = val.as_str() {
                            // Q64.64 format: i128 as decimal string
                            if let Ok(raw) = raw_str.parse::<i128>() {
                                vec[i] = FixedPoint::from_raw(raw);
                            }
                        } else if let Some(raw_i64) = val.as_i64() {
                            // Legacy Q32.32 format: promote i64 to Q64.64 via << 32
                            vec[i] = FixedPoint::from_raw((raw_i64 as i128) << 32);
                        }
                    }
                    vec
                } else if let Some(float_values) = point_float {
                    // Fallback: convert from floats (loses precision)
                    FixedVector::from_f32_slice(
                        &float_values.iter().map(|&x| x as f32).collect::<Vec<_>>()
                    )
                } else {
                    return Err(de::Error::missing_field("pr or point"));
                };

                Ok(ServicePointData {
                    id,
                    point: fixed_point,
                    metadata,
                })
            }
        }

        const FIELDS: &[&str] = &[
            "id", "pr", "pd", "point", "m", "metadata",
            "bk", "bkr", "bucket_key", "bucket_key_raw", "zs", "c"
        ];
        deserializer.deserialize_struct("ServicePointData", FIELDS, ServicePointDataVisitor)
    }
}

impl GeometricTopology {
    pub fn new(dimensions: usize) -> Self {
        Self {
            dimensions,
            services: HashMap::new(),
            capability_dimensions: HashMap::new(),
            dependencies: HashMap::new(),
            spatial_hash: Some(SpatialHash {
                grid_size: 10,
                buckets: HashMap::new(),
            }),
        }
    }

    /// Convert a point to a grid bucket key for spatial hashing
    /// Quantizes each dimension to grid cells for O(1) lookup
    ///
    /// Uses Q64.64 fixed-point arithmetic for deterministic results across all nodes.
    /// This ensures that the same service always lands in the same bucket regardless
    /// of which node processes the registration.
    fn point_to_bucket_key(point: &FixedVector, grid_size: usize) -> String {
        let mut key = String::with_capacity(point.len() * 4);
        // Use fixed-point multiplication to ensure determinism
        let grid_fp = FixedPoint::from_int(grid_size as i32);
        for i in 0..point.len() {
            // Q64.64 multiplication followed by integer truncation
            // This is deterministic because:
            // 1. FixedPoint multiplication uses 256-bit intermediate with exact scaling
            // 2. to_int() performs integer division by SCALE (2^64), which is exact
            let grid_pos = (point[i] * grid_fp).to_int();
            key.push_str(&format!("{:04}", grid_pos));
        }
        key
    }

    /// Generate bucket key as vector of i64 values for external storage
    /// Used when serializing topology for Lua to consume pre-computed bucket keys
    pub fn point_to_bucket_key_raw(point: &FixedVector, grid_size: usize) -> Vec<i64> {
        let grid_fp = FixedPoint::from_int(grid_size as i32);
        (0..point.len())
            .map(|i| (point[i] * grid_fp).to_int() as i64)
            .collect()
    }

    // ===== UNIFIED TOPOLOGY HELPERS (Stateless Q64.64 Computation) =====
    // These functions compute values for the stateless unified topology system.
    // Daemon computes these using Q64.64 fixed-point, Lua stores in ValKey.
    // This ensures multi-node determinism - identical results on all nodes.

    /// Compute 3D bucket key from (x, y, z) coordinates using Q64.64 arithmetic.
    ///
    /// Returns a string bucket key suitable for O(1) voxel lookup in ValKey.
    /// The bucket key is computed deterministically using fixed-point math,
    /// ensuring identical results across all cluster nodes.
    ///
    /// # Arguments
    /// * `x` - X coordinate (0.0 to 1.0 typically)
    /// * `y` - Y coordinate (0.0 to 1.0 typically)
    /// * `z` - Z coordinate (0.0 to 1.0 typically, hierarchy/depth)
    /// * `grid_size` - Grid divisions per axis (default 10 = 10x10x10 = 1000 voxels)
    ///
    /// # Returns
    /// String bucket key like "347" for grid_size=10
    pub fn compute_3d_bucket_key(x: f64, y: f64, z: f64, grid_size: usize) -> String {
        let grid_fp = FixedPoint::from_int(grid_size as i32);
        let max_bucket = (grid_size - 1) as i32;

        // Q64.64 multiplication followed by integer truncation, clamped to grid
        let bx = (FixedPoint::from_f64(x) * grid_fp).to_int().clamp(0, max_bucket);
        let by = (FixedPoint::from_f64(y) * grid_fp).to_int().clamp(0, max_bucket);
        let bz = (FixedPoint::from_f64(z) * grid_fp).to_int().clamp(0, max_bucket);

        format!("{}{}{}", bx, by, bz)
    }

    /// Compute Z-score for sorted set ordering using Q64.64 arithmetic.
    ///
    /// Returns an i64 score suitable for ZADD operations in ValKey.
    /// The score preserves Z-ordering for DAG load order queries.
    ///
    /// # Arguments
    /// * `z` - Z coordinate (hierarchy/depth value)
    ///
    /// # Returns
    /// i64 score = z * 1,000,000 (microsecond-like precision)
    pub fn compute_z_score(z: f64) -> i64 {
        let z_fp = FixedPoint::from_f64(z);
        let scale = FixedPoint::from_int(1_000_000);
        (z_fp * scale).to_int() as i64
    }

    /// Compute the service-tier Z-score using Q64.64 arithmetic.
    ///
    /// Reads dimension 16 (`current_load` in Layer 7: Runtime State) and scales it
    /// to an i64 suitable for ZADD ordering. This enables load-aware service
    /// discovery — services with lower current_load sort to the front of the
    /// range query.
    ///
    /// Service-tier dimension mapping (30 dims = 25 discovery + 5 storage;
    /// canonical source: daemon/integration/handlers/types.rs::SERVICE_DIMENSIONS
    /// and daemon/config/service_schema.yaml):
    ///   0-3:   Layer 1  - Interface Identity (protocol, native_format, api_version, contract_stability)
    ///   4-6:   Layer 2  - Access Control (clearance_required, auth_method, data_sensitivity)
    ///   7:     Layer 3  - Service Scope
    ///   8-10:  Layer 4  - Functional Domain (primary, secondary, specialization)
    ///   11-13: Layer 5  - Performance Profile (throughput, latency, reliability)
    ///   14-15: Layer 6  - Workflow Context (pipeline_stage, execution_priority)
    ///   16-18: Layer 7  - Runtime State (current_load ← Z-SCORE SOURCE, health_status, lifecycle_state)
    ///   19-21: Layer 8  - Classification (service_tier, environment, implementation_language)
    ///   22-24: Layer 9  - Network Context (network_zone, data_persistence, update_channel)
    ///   25-27: Layer 10 - Visual Topology (user_x, user_y, user_z) — STORAGE-ONLY
    ///   28-29: Layer 11 - Metadata (deployment_model, registration_order) — STORAGE-ONLY
    ///
    /// # Arguments
    /// * `point` - Service-tier FixedVector (up to 30 dims; truncated points OK)
    ///
    /// # Returns
    /// Scaled i64 in [0, 1_000_000] for coordinates in [0.0, 1.0]. Ordering is
    /// preserved (higher coordinate → higher z_score). Falls back to dim 0 if the
    /// vector has fewer than 17 dimensions; returns 0 for an empty vector.
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

    /// Get the default services topology key for a site.
    /// Used by stateless command handlers to locate the service discovery topology.
    ///
    /// # Arguments
    /// * `site_id` - Site identifier (e.g., "my_app")
    ///
    /// # Returns
    /// Topology key in format "{site_id}:gnode:services"
    pub fn get_services_topology_key(site_id: &str) -> String {
        format!("{{{}}}:gnode:services", site_id)
    }

    /// Validate Z-monotonicity constraint using Q64.64 comparison.
    ///
    /// For DAG topologies, edges must flow from higher Z to lower Z.
    /// This ensures acyclic dependency ordering.
    ///
    /// # Arguments
    /// * `from_z` - Source entity Z coordinate
    /// * `to_z` - Target entity Z coordinate
    ///
    /// # Returns
    /// (valid, error_message) - true if from_z > to_z (valid DAG edge)
    pub fn validate_z_monotonic(from_z: f64, to_z: f64) -> (bool, Option<String>) {
        let from_fp = FixedPoint::from_f64(from_z);
        let to_fp = FixedPoint::from_f64(to_z);

        if from_fp <= to_fp {
            (false, Some(format!(
                "Z-monotonicity violation: from.z ({:.6}) must be > to.z ({:.6})",
                from_z, to_z
            )))
        } else {
            (true, None)
        }
    }

    /// Compute Z-delta between two entities using Q64.64 arithmetic.
    ///
    /// # Returns
    /// f64 delta = from_z - to_z (positive for valid DAG edges)
    pub fn compute_z_delta(from_z: f64, to_z: f64) -> f64 {
        let from_fp = FixedPoint::from_f64(from_z);
        let to_fp = FixedPoint::from_f64(to_z);
        (from_fp - to_fp).to_f64()
    }

    /// Rebuild the spatial hash index from current services
    pub fn rebuild_spatial_hash(&mut self) {
        let grid_size = self.spatial_hash.as_ref().map(|h| h.grid_size).unwrap_or(10);

        let mut buckets: HashMap<String, Vec<ServiceId>> = HashMap::new();

        for (service_id, service_data) in &self.services {
            let bucket_key = Self::point_to_bucket_key(&service_data.point, grid_size);
            buckets.entry(bucket_key).or_default().push(service_id.clone());
        }

        self.spatial_hash = Some(SpatialHash {
            grid_size,
            buckets,
        });
    }
    
    pub fn register_service(&mut self, service: &ServiceConfig) -> Result<()> {
        // Convert capabilities to point using FixedVector
        let mut point = FixedVector::new(self.dimensions);

        for (name, value) in &service.capabilities {
            if let Some(dim) = self.capability_dimensions.get(name) {
                if *dim < self.dimensions {
                    // Convert f64 capability value to FixedPoint
                    point[*dim] = FixedPoint::from_f64(*value);
                }
            }
        }

        // Create service data
        let service_data = ServicePointData {
            id: service.id.clone(),
            point: point.clone(),  // Clone for spatial hash update
            metadata: service.metadata.clone(),
        };

        // Update spatial hash if it exists
        if let Some(spatial_hash) = &mut self.spatial_hash {
            let bucket_key = Self::point_to_bucket_key(&point, spatial_hash.grid_size);
            let bucket = spatial_hash
                .buckets
                .entry(bucket_key)
                .or_insert_with(Vec::new);
            // Prevent duplicates in spatial hash bucket
            if !bucket.contains(&service.id) {
                bucket.push(service.id.clone());
            }
        }

        // Register service
        self.services.insert(service.id.clone(), service_data);

        // Extract dependencies from metadata if available
        if let Some(deps_str) = service.metadata.get("dependencies") {
            if let Ok(deps) = serde_json::from_str::<Vec<ServiceId>>(deps_str) {
                self.dependencies.insert(service.id.clone(), deps);
            }
        }

        Ok(())
    }

    /// Deregister a service from the topology
    ///
    /// This removes the service from:
    /// - The services HashMap
    /// - The spatial hash buckets
    /// - The dependencies map
    ///
    /// Returns Ok(true) if service was found and removed, Ok(false) if not found
    pub fn deregister_service(&mut self, service_id: &str) -> Result<bool> {
        // Check if service exists
        if !self.services.contains_key(service_id) {
            return Ok(false);
        }

        // Get service point before removal (needed for spatial hash cleanup)
        let service_point = self.services.get(service_id).map(|s| s.point.clone());

        // Remove from services HashMap
        self.services.remove(service_id);

        // Remove from spatial hash if it exists
        if let (Some(spatial_hash), Some(point)) = (&mut self.spatial_hash, service_point) {
            let bucket_key = Self::point_to_bucket_key(&point, spatial_hash.grid_size);
            if let Some(bucket) = spatial_hash.buckets.get_mut(&bucket_key) {
                bucket.retain(|id| id != service_id);
                // Remove empty bucket
                if bucket.is_empty() {
                    spatial_hash.buckets.remove(&bucket_key);
                }
            }
        }

        // Remove from dependencies map
        self.dependencies.remove(service_id);

        // Also remove this service from other services' dependency lists
        for deps in self.dependencies.values_mut() {
            deps.retain(|id| id != service_id);
        }

        Ok(true)
    }

    pub fn find_services(&self, requirements: &RequirementSet) -> Result<Vec<ServiceId>> {
        // Try O(1) spatial hash lookup first
        if let Some(spatial_hash) = &self.spatial_hash {
            // Build requirement point vector
            let mut req_point = FixedVector::new(self.dimensions);
            for req in &requirements.requirements {
                if let Some(dim) = self.capability_dimensions.get(&req.name) {
                    if *dim < self.dimensions {
                        req_point[*dim] = FixedPoint::from_f64(req.min_value);
                    }
                }
            }

            // Get bucket key for O(1) lookup
            let bucket_key = Self::point_to_bucket_key(&req_point, spatial_hash.grid_size);

            // Get candidates from spatial hash bucket
            if let Some(candidates) = spatial_hash.buckets.get(&bucket_key) {
                // Filter candidates by exact requirements
                let matches: Vec<ServiceId> = candidates
                    .iter()
                    .filter(|service_id| {
                        if let Some(service) = self.services.get(*service_id) {
                            // Check if service meets all requirements
                            for req in &requirements.requirements {
                                if let Some(dim) = self.capability_dimensions.get(&req.name) {
                                    if *dim < self.dimensions && *dim < service.point.len() {
                                        let min_value_fixed = FixedPoint::from_f64(req.min_value);
                                        if service.point[*dim] < min_value_fixed {
                                            return false;
                                        }
                                    }
                                }
                            }
                            true
                        } else {
                            false
                        }
                    })
                    .cloned()
                    .collect();

                if !matches.is_empty() {
                    return Ok(matches);
                }
            }
        }

        // Fallback to linear scan if:
        // - No spatial hash available
        // - No bucket found for requirements
        // - No matches in bucket (edge case: might be in adjacent buckets)
        let matches = self.services.iter()
            .filter(|(_, service)| {
                // Check if service meets requirements
                for req in &requirements.requirements {
                    if let Some(dim) = self.capability_dimensions.get(&req.name) {
                        if *dim < self.dimensions && *dim < service.point.len() {
                            let min_value_fixed = FixedPoint::from_f64(req.min_value);
                            if service.point[*dim] < min_value_fixed {
                                return false;
                            }
                        }
                    }
                }
                true
            })
            .map(|(id, _)| id.clone())
            .collect();

        Ok(matches)
    }

    /// Discover services using range query operators (Phase 1)
    ///
    /// This method enables flexible service discovery with comparison operators:
    /// - Eq: Equal to (within 0.005 tolerance)
    /// - Neq: Not equal to
    /// - Gt/Gte: Greater than (or equal)
    /// - Lt/Lte: Less than (or equal)
    /// - Range: Between min and max (inclusive)
    ///
    /// # Arguments
    ///
    /// * `requirements` - Map of dimension index to requirement operator
    ///
    /// # Returns
    ///
    /// Vector of service IDs matching all requirements
    ///
    /// # Example
    ///
    /// ```
    /// use std::collections::HashMap;
    /// use gnode_daemon::{GeometricTopology, RangeRequirements, DimensionRequirement};
    ///
    /// let topology = GeometricTopology::new(9);
    /// let mut requirements = HashMap::new();
    /// requirements.insert(8, DimensionRequirement::Eq(0.10)); // topology_tier == 0.10
    /// let range_reqs = RangeRequirements { requirements };
    /// let services = topology.discover_range(&range_reqs).unwrap();
    /// ```
    /// Discover services using range-based queries with Q64.64 fixed-point comparison.
    /// This method is multi-node deterministic - all comparisons use fixed-point arithmetic.
    pub fn discover_range(&self, requirements: &RangeRequirements) -> Result<Vec<ServiceId>> {
        // Zero value in Q64.64 for out-of-bounds dimensions
        let zero_fp = FixedPoint::from_int(0);

        let matches: Vec<ServiceId> = self.services
            .iter()
            .filter(|(_, service)| {
                // Check if service meets all requirements using Q64.64 comparison
                for (dim_idx, req) in &requirements.requirements {
                    if *dim_idx < self.dimensions && *dim_idx < service.point.len() {
                        // Direct Q64.64 comparison - no float conversion!
                        if !req.matches_fixed(service.point[*dim_idx]) {
                            return false;
                        }
                    } else {
                        // Dimension out of bounds - treat as 0 (in Q64.64)
                        if !req.matches_fixed(zero_fp) {
                            return false;
                        }
                    }
                }
                true
            })
            .map(|(id, _)| id.clone())
            .collect();

        Ok(matches)
    }

    /// Discover services by capabilities
    ///
    /// This method finds services that have the requested capabilities.
    /// It's a more flexible version of find_services that takes a list of capability names.
    ///
    /// # Arguments
    ///
    /// * `capabilities` - List of capability names to match
    /// * `dimensions` - Optional number of dimensions to consider (defaults to all)
    /// * `distance` - Optional maximum Euclidean distance for matching (defaults to infinity)
    ///
    /// # Returns
    ///
    /// * `Result<Vec<ServiceId>>` - List of matching service IDs or error
    pub fn discover_service(&self, capabilities: &[String], dimensions: usize, distance: f64) -> Result<Vec<ServiceId>> {
        // Create a requirements set from capabilities
        let mut requirements = RequirementSet {
            requirements: Vec::new(),
        };
        
        // Convert capabilities to requirements
        for cap in capabilities {
            requirements.requirements.push(Requirement {
                name: cap.clone(),
                min_value: 0.1, // Minimal presence of capability
            });
        }
        
        // Find matching services
        let mut matches = self.find_services(&requirements)?;
        
        // Filter by distance if specified
        if distance > 0.0 {
            // Create query point as FixedVector
            let mut query_point = FixedVector::new(self.dimensions);
            for cap in capabilities {
                if let Some(dim) = self.capability_dimensions.get(cap) {
                    if *dim < self.dimensions {
                        query_point[*dim] = FixedPoint::from_f64(1.0); // Full capability
                    }
                }
            }

            // Convert distance threshold to FixedPoint
            let distance_threshold = FixedPoint::from_f64(distance);

            // Determine dimension limit
            let dim_limit = if dimensions > 0 && dimensions < self.dimensions {
                dimensions
            } else {
                self.dimensions
            };

            // Filter services by distance using native FixedVector distance calculation
            matches.retain(|id| {
                    if let Some(service) = self.services.get(id) {
                        // Calculate Euclidean distance
                        let calc_distance = if dim_limit == self.dimensions {
                            // Full dimension case - use direct euclidean_distance
                            service.point.distance_to(&query_point)
                        } else {
                            // Partial dimension case - create subvectors
                            let mut service_sub = FixedVector::new(dim_limit);
                            let mut query_sub = FixedVector::new(dim_limit);
                            for i in 0..dim_limit {
                                service_sub[i] = service.point[i];
                                query_sub[i] = query_point[i];
                            }
                            service_sub.distance_to(&query_sub)
                        };

                        calc_distance <= distance_threshold
                    } else {
                        false
                    }
                });
        }
        
        Ok(matches)
    }

    /// Discover services with load-aware selection
    ///
    /// This method combines two-phase service discovery:
    /// 1. **Phase 1**: Capability-based discovery using geometric topology
    /// 2. **Phase 2**: Load-based selection using runtime metrics
    ///
    /// This approach separates static capabilities (architectural properties)
    /// from dynamic operational state (runtime load), enabling optimal
    /// service selection while maintaining clean separation of concerns.
    ///
    /// # Arguments
    ///
    /// * `requirements` - Capability requirements for service matching
    /// * `load_manager` - Manager containing runtime load metrics
    ///
    /// # Returns
    ///
    /// * `Result<Option<ServiceId>>` - Selected optimal service or None
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use gnode_daemon::{GeometricTopology, RequirementSet, Requirement};
    /// use gnode_daemon::integration::LoadMetricsManager;
    ///
    /// let topology = GeometricTopology::new(8);
    /// let load_manager = LoadMetricsManager::new(30);
    ///
    /// let requirements = RequirementSet {
    ///     requirements: vec![
    ///         Requirement {
    ///             name: "cache".to_string(),
    ///             min_value: 0.7,
    ///         },
    ///     ],
    /// };
    ///
    /// // Discovers cache services with >=0.7 capability, then selects the least loaded one
    /// let service = topology.discover_with_load(&requirements, &load_manager)?;
    /// ```
    pub fn discover_with_load(
        &self,
        requirements: &RequirementSet,
        load_manager: &crate::integration::LoadMetricsManager,
    ) -> Result<Option<ServiceId>> {
        // Phase 1: Capability-based discovery (geometric topology)
        let candidates = self.find_services(requirements)?;

        if candidates.is_empty() {
            return Ok(None);
        }

        // Phase 2: Load-based selection (runtime metrics)
        // Uses composite scoring: load*0.6 + cpu*0.2 + mem*0.1 + latency*0.1
        Ok(load_manager.select_optimal(candidates))
    }

    /// Store topology data from JSON
    ///
    /// This method updates the topology with data from a JSON value.
    /// It's used for updating the topology configuration from external sources.
    ///
    /// # Arguments
    ///
    /// * `data` - JSON data containing topology information
    ///
    /// # Returns
    ///
    /// * `Result<()>` - Success or error
    pub fn store_topology_data(&mut self, data: &serde_json::Value) -> Result<()> {
        // Extract dimensions if present
        if let Some(dimensions) = data.get("dimensions").and_then(|v| v.as_u64()) {
            // Only update dimensions if the new value is greater
            if dimensions as usize > self.dimensions {
                self.dimensions = dimensions as usize;
            }
        }
        
        // Extract capability dimensions
        if let Some(capability_dimensions) = data.get("capability_dimensions") {
            if let Some(obj) = capability_dimensions.as_object() {
                for (name, dim) in obj {
                    if let Some(dim_value) = dim.as_u64() {
                        self.capability_dimensions.insert(name.clone(), dim_value as usize);
                    }
                }
            }
        }
        
        // Extract services
        if let Some(services) = data.get("services") {
            if let Some(arr) = services.as_array() {
                for service_data in arr {
                    // Parse service configuration
                    if let Ok(service_config) = serde_json::from_value::<ServiceConfig>(service_data.clone()) {
                        // Register the service
                        self.register_service(&service_config)?;
                    } else {
                        return Err(GeometricError::Other(
                            "Invalid service configuration format".to_string()
                        ));
                    }
                }
            } else if let Some(obj) = services.as_object() {
                // Handle object format where keys are service IDs
                for (id, service_data) in obj {
                    // Create ServiceConfig from object
                    let capabilities = if let Some(caps_arr) = service_data.get("capabilities").and_then(|v| v.as_array()) {
                        let mut capabilities = HashMap::new();
                        
                        // Extract capabilities from array format
                        for cap_data in caps_arr {
                            if let (Some(name), Some(value)) = (
                                cap_data.get("name").and_then(|v| v.as_str()),
                                cap_data.get("value").and_then(|v| v.as_f64())
                            ) {
                                capabilities.insert(name.to_string(), value);
                            }
                        }
                        capabilities
                    } else if let Some(caps_obj) = service_data.get("capabilities").and_then(|v| v.as_object()) {
                        let mut capabilities = HashMap::new();
                        
                        // Extract capabilities from object format
                        for (name, value) in caps_obj {
                            if let Some(val) = value.as_f64() {
                                capabilities.insert(name.clone(), val);
                            }
                        }
                        capabilities
                    } else {
                        HashMap::new()
                    };
                        
                    // Extract metadata
                    let mut metadata = HashMap::new();
                    if let Some(meta_obj) = service_data.get("metadata").and_then(|v| v.as_object()) {
                        for (key, value) in meta_obj {
                            if let Some(val_str) = value.as_str() {
                                metadata.insert(key.clone(), val_str.to_string());
                            } else {
                                // Convert non-string values to string
                                metadata.insert(key.clone(), value.to_string());
                            }
                        }
                    }
                    
                    // Create and register service
                    let service_config = ServiceConfig {
                        id: id.clone(),
                        capabilities,
                        metadata,
                    };
                    
                    self.register_service(&service_config)?;
                }
            }
        }
        
        // Extract dependencies
        if let Some(dependencies) = data.get("dependencies") {
            if let Some(obj) = dependencies.as_object() {
                for (service_id, deps) in obj {
                    if let Some(deps_arr) = deps.as_array() {
                        let mut deps_vec = Vec::new();
                        
                        for dep in deps_arr {
                            if let Some(dep_str) = dep.as_str() {
                                deps_vec.push(dep_str.to_string());
                            }
                        }
                        
                        if !deps_vec.is_empty() {
                            self.dependencies.insert(service_id.clone(), deps_vec);
                        }
                    }
                }
            }
        }
        
        Ok(())
    }
    
    pub fn get_load_sequence(&self) -> Result<Vec<ServiceId>> {
        // If there are no dependencies, just return all services
        if self.dependencies.is_empty() {
            return Ok(self.services.keys().cloned().collect());
        }
        
        // Use topological sort to determine load order
        let mut visited = HashMap::new();
        let mut temp = HashMap::new();
        let mut order = Vec::new();
        
        // Process each service
        for id in self.services.keys() {
            if !visited.contains_key(id) {
                self.visit_node(id, &mut visited, &mut temp, &mut order)?;
            }
        }
        
        // In a typical topological sort, dependencies come before dependents
        // Since our visit_node implementation adds nodes after their dependencies,
        // the order is already correct, so we no longer reverse it here.
        
        Ok(order)
    }
    
    fn visit_node(
        &self,
        node: &ServiceId,
        visited: &mut HashMap<ServiceId, bool>,
        temp: &mut HashMap<ServiceId, bool>,
        order: &mut Vec<ServiceId>
    ) -> Result<()> {
        // Check for circular dependencies
        if temp.contains_key(node) {
            return Err(GeometricError::InvalidState(
                format!("Circular dependency detected involving {}", node)
            ));
        }
        
        // Skip if already visited
        if visited.contains_key(node) {
            return Ok(());
        }
        
        // Mark as temporarily visited
        temp.insert(node.clone(), true);
        
        // Visit dependencies
        if let Some(deps) = self.dependencies.get(node) {
            for dep in deps {
                if self.services.contains_key(dep) {
                    self.visit_node(dep, visited, temp, order)?;
                }
            }
        }
        
        // Mark as visited and add to order
        temp.remove(node);
        visited.insert(node.clone(), true);
        order.push(node.clone());
        
        Ok(())
    }
    
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string(self)
            .map_err(GeometricError::Json)
    }
    
    pub fn from_json(json: &str) -> Result<Self> {
        serde_json::from_str(json)
            .map_err(GeometricError::Json)
    }
    
    pub fn to_bincode(&self) -> Result<Vec<u8>> {
        bincode::serialize(self)
            .map_err(GeometricError::Bincode)
    }
    
    pub fn from_bincode(data: &[u8]) -> Result<Self> {
        bincode::deserialize(data)
            .map_err(GeometricError::Bincode)
    }
    
    pub fn get_load_distribution(&self) -> HashMap<ServiceId, f64> {
        let mut distribution = HashMap::new();
        let total_services = self.services.len() as f64;

        if total_services > 0.0 {
            for id in self.services.keys() {
                distribution.insert(id.clone(), 1.0 / total_services);
            }
        }

        distribution
    }

    /// Get all registered WordPress site IDs
    /// Returns a vector of site_ids for all services with type="wordpress-site"
    pub fn get_registered_sites(&self) -> Vec<String> {
        self.services
            .values()
            .filter(|service| {
                // Filter for WordPress site services
                service.metadata.get("type")
                    .map(|t| t == "wordpress-site" || t == "wordpress_site")
                    .unwrap_or(false)
            })
            .filter_map(|service| {
                // Extract site_id from metadata
                service.metadata.get("site_id").cloned()
            })
            .collect()
    }
}

/// Stateless topology compute engine (2026-01-15 standardization)
///
/// STATELESS ARCHITECTURE:
/// - Topology state lives in ValKey (single source of truth)
/// - Rust daemon is pure Q64.64 compute engine
/// - All state mutations via FCALL to gnode_topo.lua functions
///
/// ValKey stores topology atomically via:
/// - {topo_key}:entities (Hash) - entity data by ID
/// - {topo_key}:voxel:{bucket_key} (Set) - spatial index
/// - {topo_key}:z_order (Sorted Set) - Z-ordered iteration
///
/// This struct provides:
/// - Q64.64 bucket key computation (point_to_bucket_key)
/// - Q64.64 distance calculations (euclidean_distance)
/// - service-tier z-score computation (compute_service_z_score)
pub struct SharedTopology {
    topology: Arc<RwLock<GeometricTopology>>,
}

impl SharedTopology {
    /// Create a new stateless topology compute engine
    pub fn new(dimensions: usize) -> Self {
        Self {
            topology: Arc::new(RwLock::new(GeometricTopology::new(dimensions))),
        }
    }

    /// Create a stateless topology compute engine (API-compatible constructor)
    ///
    /// Parameters are accepted for API compatibility but ignored - the daemon
    /// is stateless and does not load/save topology blobs.
    #[allow(unused_variables)]
    pub fn with_storage(dimensions: usize, redis_url: &str, site_id: &str, prefix: &str) -> Result<Self> {
        info!(
            "Stateless topology compute engine initialized ({} dimensions). \
             Services register via FCALL GNODE_REGISTER_CAPABILITY_VECTOR.",
            dimensions
        );

        Ok(Self::new(dimensions))
    }

    /// Get reference to the topology for Q64.64 compute operations
    pub fn get_topology_ref(&self) -> Arc<RwLock<GeometricTopology>> {
        Arc::clone(&self.topology)
    }

    /// Find services matching requirements (in-memory fallback, prefer FCALL)
    pub fn find_services(&self, requirements: &RequirementSet) -> Result<Vec<ServiceId>> {
        let topology = self.topology.read().map_err(|e| {
            GeometricError::InvalidState(format!("Failed to read-lock topology: {}", e))
        })?;

        topology.find_services(requirements)
    }

    /// Discover services using range query operators (in-memory fallback, prefer FCALL)
    pub fn discover_range(&self, requirements: &RangeRequirements) -> Result<Vec<ServiceId>> {
        let topology = self.topology.read().map_err(|e| {
            GeometricError::InvalidState(format!("Failed to read-lock topology: {}", e))
        })?;

        topology.discover_range(requirements)
    }

    /// Get load sequence (in-memory fallback, prefer ZRANGE on {topo_key}:z_order)
    pub fn get_load_sequence(&self) -> Result<Vec<ServiceId>> {
        let topology = self.topology.read().map_err(|e| {
            GeometricError::InvalidState(format!("Failed to read-lock topology: {}", e))
        })?;

        topology.get_load_sequence()
    }

    /// Get capability dimensions mapping
    /// Note: Prefer SERVICE_DIMENSIONS static map in integration/handlers/types.rs
    /// for the canonical service tier; for custom topologies the dim map comes
    /// from the CustomTopology struct loaded from ValKey.
    pub fn get_capability_dimensions(&self) -> Result<HashMap<String, usize>> {
        let topology = self.topology.read().map_err(|e| {
            GeometricError::InvalidState(format!("Failed to read-lock topology: {}", e))
        })?;

        Ok(topology.capability_dimensions.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topology_json_deserialization() {
        // Test that topology deserializes correctly with capability_dimensions field (snake_case)
        let json = r#"{"dimensions":9,"services":{"test-service":{"id":"test-service","point":[0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5],"metadata":{"type":"test","site_id":"test_site"}}},"capability_dimensions":{"security":0,"auth":1,"crypto":2},"dependencies":{},"spatial_hash":{"grid_size":10,"buckets":{"000500050005000500050005000500050005":["test-service"]}}}"#;

        let topology: GeometricTopology = serde_json::from_str(json)
            .expect("Failed to deserialize topology JSON with capability_dimensions field");

        assert_eq!(topology.dimensions, 9);
        assert_eq!(topology.services.len(), 1);
        assert!(topology.services.contains_key("test-service"));
        assert_eq!(topology.capability_dimensions.len(), 3);
        assert_eq!(topology.capability_dimensions.get("security"), Some(&0));
        assert_eq!(topology.capability_dimensions.get("auth"), Some(&1));
        assert_eq!(topology.capability_dimensions.get("crypto"), Some(&2));
    }

    #[test]
    fn test_register_service() {
        let mut topology = GeometricTopology::new(2);
        
        // Register capability dimensions
        topology.capability_dimensions.insert("storage".to_string(), 0);
        topology.capability_dimensions.insert("compute".to_string(), 1);
        
        // Create service
        let mut capabilities = HashMap::new();
        capabilities.insert("storage".to_string(), 0.7);
        capabilities.insert("compute".to_string(), 0.3);
        
        let service = ServiceConfig {
            id: "test-service".to_string(),
            capabilities,
            metadata: HashMap::new(),
        };
        
        // Register service
        topology.register_service(&service).unwrap();
        
        // Verify registration
        assert!(topology.services.contains_key(&"test-service".to_string()));

        let service_data = topology.services.get(&"test-service".to_string()).unwrap();
        // Compare using FixedPoint conversion with precision tolerance
        assert!((service_data.point[0].to_f64() - 0.7).abs() < 1e-4, "Storage value mismatch");
        assert!((service_data.point[1].to_f64() - 0.3).abs() < 1e-4, "Compute value mismatch");
    }
    
    #[test]
    fn test_find_services() {
        let mut topology = GeometricTopology::new(2);
        
        // Register capability dimensions
        topology.capability_dimensions.insert("storage".to_string(), 0);
        topology.capability_dimensions.insert("compute".to_string(), 1);
        
        // Create services
        let mut capabilities1 = HashMap::new();
        capabilities1.insert("storage".to_string(), 0.9);
        capabilities1.insert("compute".to_string(), 0.1);
        
        let service1 = ServiceConfig {
            id: "service1".to_string(),
            capabilities: capabilities1,
            metadata: HashMap::new(),
        };
        
        let mut capabilities2 = HashMap::new();
        capabilities2.insert("storage".to_string(), 0.5);
        capabilities2.insert("compute".to_string(), 0.5);
        
        let service2 = ServiceConfig {
            id: "service2".to_string(),
            capabilities: capabilities2,
            metadata: HashMap::new(),
        };
        
        // Register services
        topology.register_service(&service1).unwrap();
        topology.register_service(&service2).unwrap();
        
        // Find services with storage > 0.7
        let requirements = RequirementSet {
            requirements: vec![
                Requirement {
                    name: "storage".to_string(),
                    min_value: 0.7,
                },
            ],
        };
        
        let matches = topology.find_services(&requirements).unwrap();
        
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0], "service1");
    }
    
    #[test]
    fn test_load_sequence() {
        let mut topology = GeometricTopology::new(2);
        
        // Register services
        let service1 = ServiceConfig {
            id: "service1".to_string(),
            capabilities: HashMap::new(),
            metadata: HashMap::new(),
        };
        
        let service2 = ServiceConfig {
            id: "service2".to_string(),
            capabilities: HashMap::new(),
            metadata: {
                let mut map = HashMap::new();
                map.insert("dependencies".to_string(), r#"["service1"]"#.to_string());
                map
            },
        };
        
        let service3 = ServiceConfig {
            id: "service3".to_string(),
            capabilities: HashMap::new(),
            metadata: {
                let mut map = HashMap::new();
                map.insert("dependencies".to_string(), r#"["service2"]"#.to_string());
                map
            },
        };
        
        // Register services
        topology.register_service(&service1).unwrap();
        topology.register_service(&service2).unwrap();
        topology.register_service(&service3).unwrap();
        
        // Get load sequence
        let sequence = topology.get_load_sequence().unwrap();
        
        // Print the sequence and dependencies for debugging
        println!("Load sequence: {:?}", sequence);
        println!("Dependencies: {:?}", topology.dependencies);
        
        // In a topological sort, dependents come after their dependencies
        // service2 depends on service1, service3 depends on service2
        // So the expected order is: service1, service2, service3
        let pos1 = sequence.iter().position(|id| id == "service1").unwrap();
        let pos2 = sequence.iter().position(|id| id == "service2").unwrap();
        let pos3 = sequence.iter().position(|id| id == "service3").unwrap();
        
        println!("Positions: service1={}, service2={}, service3={}", pos1, pos2, pos3);
        
        assert!(pos1 < pos2, "service1 should come before service2 in load sequence");
        assert!(pos2 < pos3, "service2 should come before service3 in load sequence");
    }

    // ===== FIXEDVECTOR INTEGRATION TESTS =====

    #[test]
    fn test_capability_value_precision() {
        // Test common capability values maintain precision
        let test_values = vec![0.0, 0.1, 0.3, 0.5, 0.7, 1.0];

        for &val in &test_values {
            let fixed = FixedPoint::from_f64(val);
            let recovered = fixed.to_f64();

            // Precision requirement: 4 decimal places
            assert!((recovered - val).abs() < 1e-4,
                "Precision loss for {}: got {}", val, recovered);
        }
    }

    #[test]
    fn test_distance_calculation_determinism() {
        // Ensure same inputs always produce same outputs
        let v1 = FixedVector::from_f32_slice(&[0.7, 0.3, 0.5]);
        let v2 = FixedVector::from_f32_slice(&[0.2, 0.8, 0.1]);

        let dist1 = v1.distance_to(&v2);
        let dist2 = v1.distance_to(&v2);
        let dist3 = v1.distance_to(&v2);

        assert_eq!(dist1.raw(), dist2.raw(), "Distance should be deterministic");
        assert_eq!(dist2.raw(), dist3.raw(), "Distance should be deterministic");
    }

    #[test]
    fn test_distance_commutative_property() {
        let v1 = FixedVector::from_f32_slice(&[0.5, 0.5]);
        let v2 = FixedVector::from_f32_slice(&[0.2, 0.8]);

        let d12 = v1.distance_to(&v2);
        let d21 = v2.distance_to(&v1);

        assert_eq!(d12.raw(), d21.raw(), "Distance should be commutative");
    }

    #[test]
    fn test_triangle_inequality() {
        // d(a,c) <= d(a,b) + d(b,c)
        // NOTE: Due to fixed-point sqrt approximation, we need a small tolerance
        let a = FixedVector::from_f32_slice(&[0.0, 0.0]);
        let b = FixedVector::from_f32_slice(&[0.5, 0.5]);
        let c = FixedVector::from_f32_slice(&[1.0, 1.0]);

        let dac = a.distance_to(&c);
        let dab = a.distance_to(&b);
        let dbc = b.distance_to(&c);

        // Triangle inequality should hold or be very close due to rounding
        // Allow a small epsilon for fixed-point sqrt approximation errors
        // Due to Newton-Raphson iterations, sqrt may have ~1e-4 relative error
        let sum = dab + dbc;
        let epsilon = FixedPoint::from_f64(1e-3);  // 0.001 tolerance for sqrt rounding
        assert!(dac <= sum + epsilon,
            "Triangle inequality violated: d(a,c)={:.6} > d(a,b)+d(b,c)={:.6}",
            dac.to_f64(), sum.to_f64());
    }

    #[test]
    fn test_service_registration_precision() {
        let mut topology = GeometricTopology::new(8);

        // Register capability dimensions
        topology.capability_dimensions.insert("storage".to_string(), 0);
        topology.capability_dimensions.insert("compute".to_string(), 1);

        // Create service with precise values
        let service = ServiceConfig {
            id: "test-service-precision".to_string(),
            capabilities: [
                ("storage".to_string(), 0.7),
                ("compute".to_string(), 0.3),
            ].iter().cloned().collect(),
            metadata: HashMap::new(),
        };

        topology.register_service(&service).unwrap();

        // Verify precision is maintained
        let registered = topology.services.get("test-service-precision").unwrap();
        let storage_val = registered.point[0].to_f64();
        let compute_val = registered.point[1].to_f64();

        assert!((storage_val - 0.7).abs() < 1e-4, "Storage capability precision lost");
        assert!((compute_val - 0.3).abs() < 1e-4, "Compute capability precision lost");
    }

    #[test]
    fn test_distance_filtering_accuracy() {
        let mut topology = GeometricTopology::new(2);
        topology.capability_dimensions.insert("x".to_string(), 0);
        topology.capability_dimensions.insert("y".to_string(), 1);

        // Register services at known positions
        let services = vec![
            ("s1", 0.0, 0.0),  // Origin
            ("s2", 0.3, 0.4),  // Distance 0.5 from origin
            ("s3", 0.6, 0.8),  // Distance 1.0 from origin
            ("s4", 1.0, 1.0),  // Distance sqrt(2) ≈ 1.414 from origin
        ];

        for (id, x, y) in services {
            let service = ServiceConfig {
                id: id.to_string(),
                capabilities: [
                    ("x".to_string(), x),
                    ("y".to_string(), y),
                ].iter().cloned().collect(),
                metadata: HashMap::new(),
            };
            topology.register_service(&service).unwrap();
        }

        // Query with distance threshold 0.6
        // Query point is (1.0, 1.0) based on requested capabilities
        // Distances: s1->query=1.414, s2->query=0.922, s3->query=0.447, s4->query=0.0
        // Should find s3 and s4 (within 0.6 distance from query point)
        let results = topology.discover_service(
            &["x".to_string(), "y".to_string()],
            2,
            0.6
        ).unwrap();

        // Should find services within 0.6 distance from query point (1.0, 1.0)
        assert!(results.len() >= 1, "Should find at least one service");
        assert!(results.contains(&"s4".to_string()), "Should find s4 (matches query point exactly)");
    }

    #[test]
    fn test_json_serialization_compatibility() {
        // Ensure JSON format hasn't changed
        let mut topology = GeometricTopology::new(2);
        topology.capability_dimensions.insert("a".to_string(), 0);
        topology.capability_dimensions.insert("b".to_string(), 1);

        let service = ServiceConfig {
            id: "test-json".to_string(),
            capabilities: [
                ("a".to_string(), 0.5),
                ("b".to_string(), 0.8),
            ].iter().cloned().collect(),
            metadata: HashMap::new(),
        };

        topology.register_service(&service).unwrap();

        // Serialize to JSON
        let json = serde_json::to_string(&topology.services.get("test-json").unwrap()).unwrap();

        // Should contain f64 array, not FixedPoint internals
        assert!(json.contains("\"point\":["), "JSON should contain point array");

        // Deserialize and verify precision
        let deserialized: ServicePointData = serde_json::from_str(&json).unwrap();
        assert!((deserialized.point[0].to_f64() - 0.5).abs() < 1e-4, "Deserialization precision lost for dimension 0");
        assert!((deserialized.point[1].to_f64() - 0.8).abs() < 1e-4, "Deserialization precision lost for dimension 1");
    }

    // ===== Q64.64 BUCKET KEY DETERMINISM TESTS =====
    // These tests verify that bucket keys are computed deterministically across all nodes

    #[test]
    fn test_bucket_key_determinism() {
        // Test that the same point always produces the same bucket key
        let point = FixedVector::from_f32_slice(&[0.7, 0.3, 0.5]);

        // Run 1000 times to detect any non-determinism
        let first_key = GeometricTopology::point_to_bucket_key(&point, 10);
        for i in 0..1000 {
            let key = GeometricTopology::point_to_bucket_key(&point, 10);
            assert_eq!(key, first_key, "Bucket key must be deterministic (iteration {})", i);
        }
    }

    #[test]
    fn test_bucket_key_raw_determinism() {
        // Test that raw bucket key values are deterministic
        let point = FixedVector::from_f32_slice(&[0.7, 0.3, 0.5]);

        let first_raw = GeometricTopology::point_to_bucket_key_raw(&point, 10);
        for i in 0..1000 {
            let raw = GeometricTopology::point_to_bucket_key_raw(&point, 10);
            assert_eq!(raw, first_raw, "Raw bucket key must be deterministic (iteration {})", i);
        }
    }

    #[test]
    fn test_bucket_key_consistency_string_and_raw() {
        // Verify that string bucket key matches the raw values
        let point = FixedVector::from_f32_slice(&[0.7, 0.3, 0.5]);

        let string_key = GeometricTopology::point_to_bucket_key(&point, 10);
        let raw_key = GeometricTopology::point_to_bucket_key_raw(&point, 10);

        // Build expected string from raw
        let expected_string: String = raw_key.iter()
            .map(|&v| format!("{:04}", v))
            .collect();

        assert_eq!(string_key, expected_string,
            "String bucket key should match concatenated raw values");
    }

    #[test]
    fn test_bucket_key_known_values() {
        // Test specific known values to catch algorithm changes
        // Note: 0.7f32 is NOT exactly 0.7 in IEEE 754 - it's approximately 0.6999999880790710449
        // So when we do floor(0.6999999... * 10), we get 6, not 7
        // This is correct Q64.64 behavior - it faithfully represents the f32 input

        // Use values that are exactly representable in f32/f64
        let point = FixedVector::from_f32_slice(&[0.5, 0.25, 0.75]);

        let raw = GeometricTopology::point_to_bucket_key_raw(&point, 10);
        assert_eq!(raw[0], 5, "0.5 * 10 should produce bucket 5");
        assert_eq!(raw[1], 2, "0.25 * 10 should produce bucket 2");
        assert_eq!(raw[2], 7, "0.75 * 10 should produce bucket 7");

        // Test a non-exact value to ensure consistency
        let point2 = FixedVector::from_f32_slice(&[0.7, 0.3]);
        let raw2 = GeometricTopology::point_to_bucket_key_raw(&point2, 10);
        // 0.7f32 ≈ 0.6999999..., so bucket is 6
        assert_eq!(raw2[0], 6, "0.7f32 (~0.69999...) * 10 truncated should produce bucket 6");
        // 0.3f32 ≈ 0.3000000..., bucket is 3
        assert_eq!(raw2[1], 3, "0.3f32 * 10 truncated should produce bucket 3");
    }

    #[test]
    fn test_bucket_key_boundary_values() {
        // Test edge cases for bucket key computation
        let zero_point = FixedVector::from_f32_slice(&[0.0, 0.0]);
        let raw_zero = GeometricTopology::point_to_bucket_key_raw(&zero_point, 10);
        assert_eq!(raw_zero[0], 0, "0.0 should produce bucket 0");
        assert_eq!(raw_zero[1], 0, "0.0 should produce bucket 0");

        // Near bucket boundary
        let near_one = FixedVector::from_f32_slice(&[0.99, 0.999]);
        let raw_near = GeometricTopology::point_to_bucket_key_raw(&near_one, 10);
        assert_eq!(raw_near[0], 9, "0.99 should produce bucket 9");
        assert_eq!(raw_near[1], 9, "0.999 should produce bucket 9");

        // Exactly 1.0
        let one_point = FixedVector::from_f32_slice(&[1.0, 1.0]);
        let raw_one = GeometricTopology::point_to_bucket_key_raw(&one_point, 10);
        assert_eq!(raw_one[0], 10, "1.0 should produce bucket 10");
        assert_eq!(raw_one[1], 10, "1.0 should produce bucket 10");
    }

    #[test]
    fn test_service_point_data_includes_bucket_key() {
        // Verify that ServicePointData serialization includes pre-computed bucket keys
        // Field abbreviations (stateless architecture 2026-01-14):
        //   bk  = bucket_key      (string for ValKey hash tags)
        //   bkr = bucket_key_raw  (i64 array for Lua comparison)
        //   pr  = point_raw       (Q64.64 i128 values as strings - authoritative)
        //   pd  = point_display   (floats capped at 3 decimals)
        //   zs  = z_score         (i64 for ZADD)
        let point = FixedVector::from_f32_slice(&[0.5, 0.8]);
        let service_data = ServicePointData {
            id: "test-bucket".to_string(),
            point,
            metadata: HashMap::new(),
        };

        let json = serde_json::to_string(&service_data).unwrap();

        // Verify abbreviated field names are present
        assert!(json.contains("\"bk\""), "JSON should contain bk (bucket_key) field");
        assert!(json.contains("\"bkr\""), "JSON should contain bkr (bucket_key_raw) field");
        assert!(json.contains("\"pr\""), "JSON should contain pr (point_raw) field");
        assert!(json.contains("\"pd\""), "JSON should contain pd (point_display) field");
        assert!(json.contains("\"zs\""), "JSON should contain zs (z_score) field");

        // Parse and verify values
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        let bucket_key = parsed["bk"].as_str().unwrap();
        let bucket_key_raw = parsed["bkr"].as_array().unwrap();

        // Expected: 0.5 * 10 = 5, 0.8 * 10 = 8
        assert_eq!(bucket_key, "00050008", "Bucket key should be 00050008 for [0.5, 0.8]");
        assert_eq!(bucket_key_raw.len(), 2, "Bucket key raw should have 2 elements");
        assert_eq!(bucket_key_raw[0].as_i64().unwrap(), 5, "First bucket should be 5");
        assert_eq!(bucket_key_raw[1].as_i64().unwrap(), 8, "Second bucket should be 8");

        // Verify pr (point_raw) contains Q64.64 i128 values as strings
        let point_raw = parsed["pr"].as_array().unwrap();
        assert_eq!(point_raw.len(), 2, "point_raw should have 2 elements");
        // 0.5 in Q64.64 = 0.5 * 2^64 = 9223372036854775808
        let pr_val: i128 = point_raw[0].as_str().unwrap().parse().unwrap();
        assert_eq!(pr_val, 9223372036854775808i128, "First pr value should be 0.5 in Q64.64");

        // Verify pd (point_display) contains floats
        let point_display = parsed["pd"].as_array().unwrap();
        assert_eq!(point_display.len(), 2, "point_display should have 2 elements");
        assert!((point_display[0].as_f64().unwrap() - 0.5).abs() < 0.001, "First pd value should be ~0.5");
    }

    #[test]
    fn test_spatial_hash_uses_q32_bucket_keys() {
        // Verify that services registered to topology use Q64.64 bucket keys
        let mut topology = GeometricTopology::new(2);
        topology.capability_dimensions.insert("x".to_string(), 0);
        topology.capability_dimensions.insert("y".to_string(), 1);

        // Use values exactly representable in f64/f32 to avoid floating-point surprises
        let service = ServiceConfig {
            id: "q32-test".to_string(),
            capabilities: [
                ("x".to_string(), 0.5),  // Exactly representable: bucket 5
                ("y".to_string(), 0.25), // Exactly representable: bucket 2
            ].iter().cloned().collect(),
            metadata: HashMap::new(),
        };

        topology.register_service(&service).unwrap();

        // Expected bucket key for [0.5, 0.25] with grid_size 10: "00050002"
        let expected_key = "00050002";

        // Verify service is in the correct bucket
        let spatial_hash = topology.spatial_hash.as_ref().unwrap();
        assert!(spatial_hash.buckets.contains_key(expected_key),
            "Spatial hash should contain bucket {}. Available buckets: {:?}",
            expected_key, spatial_hash.buckets.keys().collect::<Vec<_>>());
        assert!(spatial_hash.buckets[expected_key].contains(&"q32-test".to_string()),
            "Bucket {} should contain the registered service", expected_key);
    }

    #[test]
    fn test_range_query_uses_q32_comparison() {
        // Verify that range queries use Q64.64 fixed-point comparison
        // This is critical for multi-node determinism
        use crate::geometric_precision::FixedPoint;

        // Test DimensionRequirement::matches_fixed
        let req_gt = DimensionRequirement::Gt(0.5);
        let req_gte = DimensionRequirement::Gte(0.5);
        let req_lt = DimensionRequirement::Lt(0.5);
        let req_lte = DimensionRequirement::Lte(0.5);
        let req_eq = DimensionRequirement::Eq(0.5);
        let req_range = DimensionRequirement::Range(0.3, 0.7);

        // Test with exactly 0.5 (exactly representable)
        let val_05 = FixedPoint::from_f64(0.5);
        assert!(!req_gt.matches_fixed(val_05), "0.5 should NOT be > 0.5");
        assert!(req_gte.matches_fixed(val_05), "0.5 should be >= 0.5");
        assert!(!req_lt.matches_fixed(val_05), "0.5 should NOT be < 0.5");
        assert!(req_lte.matches_fixed(val_05), "0.5 should be <= 0.5");
        assert!(req_eq.matches_fixed(val_05), "0.5 should be == 0.5");
        assert!(req_range.matches_fixed(val_05), "0.5 should be in range [0.3, 0.7]");

        // Test with 0.6 (slightly above)
        let val_06 = FixedPoint::from_f64(0.6);
        assert!(req_gt.matches_fixed(val_06), "0.6 should be > 0.5");
        assert!(req_gte.matches_fixed(val_06), "0.6 should be >= 0.5");
        assert!(!req_lt.matches_fixed(val_06), "0.6 should NOT be < 0.5");
        assert!(!req_lte.matches_fixed(val_06), "0.6 should NOT be <= 0.5");
        assert!(req_range.matches_fixed(val_06), "0.6 should be in range [0.3, 0.7]");

        // Test with 0.4 (slightly below)
        let val_04 = FixedPoint::from_f64(0.4);
        assert!(!req_gt.matches_fixed(val_04), "0.4 should NOT be > 0.5");
        assert!(!req_gte.matches_fixed(val_04), "0.4 should NOT be >= 0.5");
        assert!(req_lt.matches_fixed(val_04), "0.4 should be < 0.5");
        assert!(req_lte.matches_fixed(val_04), "0.4 should be <= 0.5");
        assert!(req_range.matches_fixed(val_04), "0.4 should be in range [0.3, 0.7]");

        // Test outside range
        let val_02 = FixedPoint::from_f64(0.2);
        let val_08 = FixedPoint::from_f64(0.8);
        assert!(!req_range.matches_fixed(val_02), "0.2 should NOT be in range [0.3, 0.7]");
        assert!(!req_range.matches_fixed(val_08), "0.8 should NOT be in range [0.3, 0.7]");
    }

    #[test]
    fn test_range_query_determinism() {
        // Verify that range query comparison is deterministic across many iterations
        use crate::geometric_precision::FixedPoint;

        let req = DimensionRequirement::Gt(0.7);
        let val = FixedPoint::from_f64(0.71);

        // Run 1000 times to detect any non-determinism
        let first_result = req.matches_fixed(val);
        for i in 0..1000 {
            let result = req.matches_fixed(val);
            assert_eq!(result, first_result,
                "Range query must be deterministic (iteration {})", i);
        }
    }

    #[test]
    fn test_discover_range_uses_q32() {
        // Integration test: verify discover_range uses Q64.64 comparison
        let mut topology = GeometricTopology::new(2);
        topology.capability_dimensions.insert("reliability".to_string(), 0);
        topology.capability_dimensions.insert("speed".to_string(), 1);

        // Register services with different reliability levels
        let low_service = ServiceConfig {
            id: "low-reliability".to_string(),
            capabilities: [
                ("reliability".to_string(), 0.3),
                ("speed".to_string(), 0.5),
            ].iter().cloned().collect(),
            metadata: HashMap::new(),
        };

        let high_service = ServiceConfig {
            id: "high-reliability".to_string(),
            capabilities: [
                ("reliability".to_string(), 0.8),
                ("speed".to_string(), 0.5),
            ].iter().cloned().collect(),
            metadata: HashMap::new(),
        };

        topology.register_service(&low_service).unwrap();
        topology.register_service(&high_service).unwrap();

        // Query for reliability > 0.5
        let mut requirements = HashMap::new();
        requirements.insert(0, DimensionRequirement::Gt(0.5));
        let range_reqs = RangeRequirements { requirements };

        let results = topology.discover_range(&range_reqs).unwrap();

        // Should only find high-reliability service
        assert_eq!(results.len(), 1, "Should find exactly 1 high-reliability service");
        assert!(results.contains(&"high-reliability".to_string()),
            "Should find high-reliability service");
        assert!(!results.contains(&"low-reliability".to_string()),
            "Should NOT find low-reliability service");
    }

    // ===== UNIFIED TOPOLOGY HELPER TESTS =====

    #[test]
    fn test_compute_3d_bucket_key_determinism() {
        // Test that 3D bucket key computation is deterministic
        let first_key = GeometricTopology::compute_3d_bucket_key(0.35, 0.42, 0.67, 10);
        for i in 0..1000 {
            let key = GeometricTopology::compute_3d_bucket_key(0.35, 0.42, 0.67, 10);
            assert_eq!(key, first_key, "3D bucket key must be deterministic (iteration {})", i);
        }
    }

    #[test]
    fn test_compute_3d_bucket_key_known_values() {
        // Test with exactly representable values
        assert_eq!(
            GeometricTopology::compute_3d_bucket_key(0.5, 0.25, 0.75, 10),
            "527",
            "0.5, 0.25, 0.75 should produce bucket 527"
        );

        // Test boundaries
        assert_eq!(
            GeometricTopology::compute_3d_bucket_key(0.0, 0.0, 0.0, 10),
            "000",
            "Origin should produce bucket 000"
        );

        // Test clamping at upper bound
        assert_eq!(
            GeometricTopology::compute_3d_bucket_key(1.5, 1.5, 1.5, 10),
            "999",
            "Values > 1.0 should clamp to max bucket"
        );

        // Test clamping at lower bound (negative values)
        assert_eq!(
            GeometricTopology::compute_3d_bucket_key(-0.5, -0.5, -0.5, 10),
            "000",
            "Negative values should clamp to 0"
        );
    }

    #[test]
    fn test_compute_z_score_determinism() {
        let first_score = GeometricTopology::compute_z_score(0.567);
        for i in 0..1000 {
            let score = GeometricTopology::compute_z_score(0.567);
            assert_eq!(score, first_score, "Z-score must be deterministic (iteration {})", i);
        }
    }

    #[test]
    fn test_compute_z_score_ordering() {
        // Test that Z-scores preserve ordering
        let z1 = GeometricTopology::compute_z_score(0.2);
        let z2 = GeometricTopology::compute_z_score(0.5);
        let z3 = GeometricTopology::compute_z_score(0.8);

        assert!(z1 < z2, "Z-score ordering: 0.2 < 0.5");
        assert!(z2 < z3, "Z-score ordering: 0.5 < 0.8");
        assert!(z1 < z3, "Z-score ordering: 0.2 < 0.8");
    }

    #[test]
    fn test_compute_z_score_precision() {
        // Test that Z-scores have sufficient precision for sorting
        let z1 = GeometricTopology::compute_z_score(0.500000);
        let z2 = GeometricTopology::compute_z_score(0.500001);

        assert!(z1 < z2, "Z-score should distinguish 0.500000 from 0.500001");
        assert_eq!(z2 - z1, 1, "Difference should be 1 (microsecond precision)");
    }

    #[test]
    fn test_validate_z_monotonic() {
        // Valid DAG edge: high Z to low Z
        let (valid, _) = GeometricTopology::validate_z_monotonic(0.8, 0.3);
        assert!(valid, "Edge from z=0.8 to z=0.3 should be valid (high to low)");

        // Invalid: low Z to high Z
        let (valid, err) = GeometricTopology::validate_z_monotonic(0.3, 0.8);
        assert!(!valid, "Edge from z=0.3 to z=0.8 should be invalid");
        assert!(err.is_some(), "Should return error message");
        assert!(err.unwrap().contains("Z-monotonicity violation"));

        // Invalid: same Z (no cycles allowed)
        let (valid, _) = GeometricTopology::validate_z_monotonic(0.5, 0.5);
        assert!(!valid, "Edge from z=0.5 to z=0.5 should be invalid (same level)");
    }

    #[test]
    fn test_compute_z_delta() {
        let delta = GeometricTopology::compute_z_delta(0.8, 0.3);
        assert!((delta - 0.5).abs() < 0.0001, "Delta should be approximately 0.5");

        let negative_delta = GeometricTopology::compute_z_delta(0.3, 0.8);
        assert!((negative_delta - (-0.5)).abs() < 0.0001, "Delta should be approximately -0.5");
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
