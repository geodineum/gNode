//! Node Configuration Module for gNode
//!
//! This module provides node-specific configuration management for multi-node
//! gNode deployments. It handles loading node type configurations from YAML files,
//! storing them in ValKey, and enabling remote nodes to bootstrap without local files.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                     Master Node                                  │
//! │  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────┐  │
//! │  │ config/nodes/   │───▶│ NodeConfig      │───▶│ ValKey      │  │
//! │  │ inference.yaml  │    │ general.yaml    │    │ gnode:node_*  │  │
//! │  └─────────────────┘    └─────────────────┘    └─────────────┘  │
//! └─────────────────────────────────────────────────────────────────┘
//!                                                        │
//!                                                        │ Remote fetch
//!                                                        ▼
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                   Remote Worker Node                             │
//! │  ┌─────────────────┐    ┌─────────────────┐                     │
//! │  │ GNODE_NODE_FETCH_ │───▶│ NodeConfig      │                     │
//! │  │ CONFIG (Lua)    │    │ in memory       │                     │
//! │  └─────────────────┘    └─────────────────┘                     │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Key Patterns
//!
//! - `gnode:node_config:{type}` - JSON config for a node type
//! - `gnode:node_config:_types` - Set of available node types
//! - `gnode:node:{node_id}:config` - Per-instance registration
//! - `gnode:node:{node_id}:metrics` - Runtime metrics
//! - `gnode:node:{node_id}:health` - Health status

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, RwLock, OnceLock};
use serde::{Serialize, Deserialize};
use log::{info, warn, debug};
use redis::Connection;

/// Resource allocation for a node
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ResourceConfig {
    /// Number of CPU cores to use (0 = auto-detect)
    #[serde(default)]
    pub cores: u32,
    /// Maximum memory in MB (0 = no limit)
    #[serde(default)]
    pub max_memory_mb: u32,
    /// Thread pool size (0 = auto = core count)
    #[serde(default)]
    pub thread_pool_size: u32,
}

/// Batch size configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchSizeConfig {
    #[serde(default = "default_batch_initial")]
    pub initial: u32,
    #[serde(default = "default_batch_min")]
    pub min: u32,
    #[serde(default = "default_batch_max")]
    pub max: u32,
}

fn default_batch_initial() -> u32 { 250 }
fn default_batch_min() -> u32 { 50 }
fn default_batch_max() -> u32 { 500 }

impl Default for BatchSizeConfig {
    fn default() -> Self {
        Self {
            initial: 250,
            min: 50,
            max: 500,
        }
    }
}

impl BatchSizeConfig {
    /// Validate batch size configuration for logical consistency
    /// Returns Ok(()) if valid, Err with description if invalid
    pub fn validate(&self) -> Result<(), String> {
        if self.min > self.max {
            return Err(format!(
                "batch_size.min ({}) cannot exceed batch_size.max ({})",
                self.min, self.max
            ));
        }
        if self.initial < self.min {
            return Err(format!(
                "batch_size.initial ({}) cannot be less than batch_size.min ({})",
                self.initial, self.min
            ));
        }
        if self.initial > self.max {
            return Err(format!(
                "batch_size.initial ({}) cannot exceed batch_size.max ({})",
                self.initial, self.max
            ));
        }
        Ok(())
    }
}

/// Timeout configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeoutConfig {
    #[serde(default = "default_idle_ms")]
    pub idle_ms: u32,
    #[serde(default = "default_block_ms")]
    pub block_ms: u32,
}

fn default_idle_ms() -> u32 { 30000 }
fn default_block_ms() -> u32 { 1000 }

impl Default for TimeoutConfig {
    fn default() -> Self {
        Self {
            idle_ms: 30000,
            block_ms: 1000,
        }
    }
}

/// Circuit breaker configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircuitBreakerConfig {
    #[serde(default = "default_threshold")]
    pub threshold: u32,
    #[serde(default = "default_cooldown")]
    pub cooldown_secs: u32,
}

fn default_threshold() -> u32 { 5 }
fn default_cooldown() -> u32 { 30 }

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            threshold: 5,
            cooldown_secs: 30,
        }
    }
}

/// Performance tuning configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PerformanceConfig {
    #[serde(default)]
    pub batch_size: BatchSizeConfig,
    #[serde(default)]
    pub timeouts: TimeoutConfig,
    #[serde(default)]
    pub circuit_breaker: CircuitBreakerConfig,
}

/// Routing configuration for a node type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeRoutingConfig {
    /// Routing mode: include, exclude, or all
    #[serde(default = "default_routing_mode")]
    pub mode: String,
    /// Group hints for message filtering
    #[serde(default)]
    pub group_hints: Vec<String>,
}

fn default_routing_mode() -> String { "all".to_string() }

impl Default for NodeRoutingConfig {
    fn default() -> Self {
        Self {
            mode: "all".to_string(),
            group_hints: vec![],
        }
    }
}

/// Capability dimensions for geometric topology
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CapabilitiesConfig {
    /// Capability name -> value (0.0-1.0)
    #[serde(default)]
    pub dimensions: HashMap<String, f64>,
}

/// Health reporting configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthConfig {
    #[serde(default = "default_report_interval")]
    pub report_interval_ms: u32,
    #[serde(default = "default_metrics_enabled")]
    pub metrics_enabled: bool,
    #[serde(default = "default_metrics_retention")]
    pub metrics_retention_secs: u32,
}

fn default_report_interval() -> u32 { 5000 }
fn default_metrics_enabled() -> bool { true }
fn default_metrics_retention() -> u32 { 3600 }

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            report_interval_ms: 5000,
            metrics_enabled: true,
            metrics_retention_secs: 3600,
        }
    }
}

/// Metadata for node configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeMetadata {
    #[serde(default = "default_version")]
    pub version: String,
    #[serde(default = "default_created_by")]
    pub created_by: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
}

fn default_version() -> String { "1.0.0".to_string() }
fn default_created_by() -> String { "system".to_string() }

impl Default for NodeMetadata {
    fn default() -> Self {
        Self {
            version: "1.0.0".to_string(),
            created_by: "system".to_string(),
            created_at: None,
            updated_at: None,
        }
    }
}

/// Full node configuration from YAML
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    /// Node type identifier
    pub node_type: String,
    /// Human-readable description
    pub description: String,
    /// Routing configuration
    #[serde(default)]
    pub routing: NodeRoutingConfig,
    /// Resource allocation
    #[serde(default)]
    pub resources: ResourceConfig,
    /// Performance tuning
    #[serde(default)]
    pub performance: PerformanceConfig,
    /// Capability dimensions
    #[serde(default)]
    pub capabilities: CapabilitiesConfig,
    /// Health reporting
    #[serde(default)]
    pub health: HealthConfig,
    /// Metadata
    #[serde(default)]
    pub metadata: NodeMetadata,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            node_type: "general".to_string(),
            description: "Default gNode node".to_string(),
            routing: NodeRoutingConfig::default(),
            resources: ResourceConfig::default(),
            performance: PerformanceConfig::default(),
            capabilities: CapabilitiesConfig::default(),
            health: HealthConfig::default(),
            metadata: NodeMetadata::default(),
        }
    }
}

impl NodeConfig {
    /// Validate the node configuration for logical consistency
    /// Returns Ok(()) if valid, Err with description if invalid
    pub fn validate(&self) -> Result<(), String> {
        // Validate batch size configuration
        self.performance.batch_size.validate()?;

        // Validate routing mode
        let valid_modes = ["include", "exclude", "all"];
        if !valid_modes.contains(&self.routing.mode.as_str()) {
            return Err(format!(
                "Invalid routing mode '{}'. Valid modes: {:?}",
                self.routing.mode, valid_modes
            ));
        }

        // Validate circuit breaker
        if self.performance.circuit_breaker.threshold == 0 {
            return Err("circuit_breaker.threshold cannot be 0".into());
        }
        if self.performance.circuit_breaker.cooldown_secs == 0 {
            return Err("circuit_breaker.cooldown_secs cannot be 0".into());
        }

        // Validate timeouts
        if self.performance.timeouts.block_ms == 0 {
            return Err("timeouts.block_ms cannot be 0".into());
        }

        Ok(())
    }

    /// Create a default general node config
    pub fn default_general() -> Self {
        let mut config = Self {
            node_type: "general".to_string(),
            description: "Default gNode node - processes standard requests, excludes specialized workloads".to_string(),
            routing: NodeRoutingConfig {
                mode: "exclude".to_string(),
                group_hints: vec!["inference".to_string(), "gpu_compute".to_string()],
            },
            ..Self::default()
        };
        config.capabilities.dimensions.insert("general".to_string(), 1.0);
        config.capabilities.dimensions.insert("caching".to_string(), 0.9);
        config
    }

    /// Create a default inference node config
    pub fn default_inference() -> Self {
        let mut config = Self {
            node_type: "inference".to_string(),
            description: "AI/ML inference specialized node - handles model predictions and embeddings".to_string(),
            routing: NodeRoutingConfig {
                mode: "include".to_string(),
                group_hints: vec!["inference".to_string(), "ml_predict".to_string(), "embedding".to_string()],
            },
            ..Self::default()
        };
        config.performance.batch_size = BatchSizeConfig {
            initial: 50,
            min: 10,
            max: 100,
        };
        config.health.report_interval_ms = 2000;
        config.capabilities.dimensions.insert("inference".to_string(), 1.0);
        config.capabilities.dimensions.insert("gpu_compute".to_string(), 0.8);
        config
    }

    /// Create a default GPU compute node config
    pub fn default_gpu_compute() -> Self {
        let mut config = Self {
            node_type: "gpu_compute".to_string(),
            description: "GPU compute specialized node - handles CUDA/OpenCL workloads".to_string(),
            routing: NodeRoutingConfig {
                mode: "include".to_string(),
                group_hints: vec!["gpu_compute".to_string(), "tensor_ops".to_string(), "matrix_mult".to_string()],
            },
            resources: ResourceConfig {
                cores: 2,
                max_memory_mb: 4096,
                thread_pool_size: 2,
            },
            ..Self::default()
        };
        config.performance.batch_size = BatchSizeConfig {
            initial: 20,
            min: 5,
            max: 50,
        };
        config.capabilities.dimensions.insert("gpu_compute".to_string(), 1.0);
        config.capabilities.dimensions.insert("tensor_ops".to_string(), 1.0);
        config
    }

    /// Create a universal "all" node config
    pub fn default_all() -> Self {
        Self {
            node_type: "all".to_string(),
            description: "Universal gNode node - processes all messages regardless of routing hint".to_string(),
            routing: NodeRoutingConfig {
                mode: "all".to_string(),
                group_hints: vec![],
            },
            ..Self::default()
        }
    }
}

/// Global node config cache
static NODE_CONFIG_CACHE: OnceLock<Arc<RwLock<HashMap<String, NodeConfig>>>> = OnceLock::new();

fn get_node_cache() -> &'static Arc<RwLock<HashMap<String, NodeConfig>>> {
    NODE_CONFIG_CACHE.get_or_init(|| {
        Arc::new(RwLock::new(HashMap::new()))
    })
}

/// Load a single node config from a YAML file
pub fn load_node_config_from_file<P: AsRef<Path>>(path: P) -> Result<NodeConfig, String> {
    let path = path.as_ref();
    let content = std::fs::read_to_string(path)
        .map_err(|e| format!("Failed to read file {:?}: {}", path, e))?;

    let config: NodeConfig = serde_yaml::from_str(&content)
        .map_err(|e| format!("Failed to parse YAML {:?}: {}", path, e))?;

    // Validate configuration for logical consistency
    config.validate()
        .map_err(|e| format!("Invalid configuration in {:?}: {}", path, e))?;

    Ok(config)
}

/// Load all node configs from a directory
pub fn load_node_configs_from_directory<P: AsRef<Path>>(dir: P) -> Result<Vec<NodeConfig>, String> {
    let dir = dir.as_ref();
    let mut configs = Vec::new();

    if !dir.exists() {
        warn!("Node config directory does not exist: {:?}", dir);
        return Ok(configs);
    }

    let entries = std::fs::read_dir(dir)
        .map_err(|e| format!("Failed to read directory {:?}: {}", dir, e))?;

    for entry in entries {
        let entry = entry.map_err(|e| format!("Failed to read directory entry: {}", e))?;
        let path = entry.path();

        // Skip schema files
        if path.file_name()
            .is_some_and(|n| n.to_string_lossy().contains("schema"))
        {
            continue;
        }

        if path.extension().is_some_and(|ext| ext == "yaml" || ext == "yml") {
            match load_node_config_from_file(&path) {
                Ok(config) => {
                    info!("Loaded node config for type '{}' from {:?}", config.node_type, path);
                    configs.push(config);
                },
                Err(e) => {
                    warn!("Failed to load node config from {:?}: {}", path, e);
                }
            }
        }
    }

    Ok(configs)
}

/// ValKey key for node config storage
fn get_node_config_key(node_type: &str) -> String {
    format!("gnode:node_config:{}", node_type)
}

/// Store a single node config to ValKey
pub fn store_node_config_to_valkey(conn: &mut Connection, config: &NodeConfig) -> Result<(), String> {
    let key = get_node_config_key(&config.node_type);
    let json = serde_json::to_string(config)
        .map_err(|e| format!("Failed to serialize node config: {}", e))?;

    redis::cmd("SET")
        .arg(&key)
        .arg(&json)
        .query::<String>(conn)
        .map_err(|e| format!("Failed to store node config in ValKey: {}", e))?;

    // Add to types set
    redis::cmd("SADD")
        .arg("gnode:node_config:_types")
        .arg(&config.node_type)
        .query::<i64>(conn)
        .map_err(|e| format!("Failed to update node types set: {}", e))?;

    info!("Stored node config for '{}' in ValKey at key '{}'", config.node_type, key);
    Ok(())
}

/// Store multiple node configs to ValKey
pub fn store_node_configs_to_valkey(conn: &mut Connection, configs: &[NodeConfig]) -> Result<(), String> {
    for config in configs {
        store_node_config_to_valkey(conn, config)?;
    }

    info!("Stored {} node configs to ValKey", configs.len());
    Ok(())
}

/// Fetch a node config from ValKey
pub fn fetch_node_config_from_valkey(conn: &mut Connection, node_type: &str) -> Result<NodeConfig, String> {
    let key = get_node_config_key(node_type);

    let json: Option<String> = redis::cmd("GET")
        .arg(&key)
        .query(conn)
        .map_err(|e| format!("Failed to fetch node config from ValKey: {}", e))?;

    match json {
        Some(data) => {
            let config: NodeConfig = serde_json::from_str(&data)
                .map_err(|e| format!("Failed to deserialize node config: {}", e))?;
            // Validate configuration for logical consistency
            config.validate()
                .map_err(|e| format!("Invalid config for '{}' in ValKey: {}", node_type, e))?;
            debug!("Fetched node config for '{}' from ValKey", node_type);
            Ok(config)
        },
        None => {
            Err(format!("No node config found for type '{}' in ValKey", node_type))
        }
    }
}

/// Fetch all available node configs from ValKey
pub fn fetch_all_node_configs_from_valkey(conn: &mut Connection) -> Result<Vec<NodeConfig>, String> {
    // Get all node types
    let node_types: Vec<String> = redis::cmd("SMEMBERS")
        .arg("gnode:node_config:_types")
        .query(conn)
        .map_err(|e| format!("Failed to fetch node types: {}", e))?;

    let mut configs = Vec::new();
    for node_type in node_types {
        match fetch_node_config_from_valkey(conn, &node_type) {
            Ok(config) => configs.push(config),
            Err(e) => {
                debug!("Could not fetch config for '{}': {}", node_type, e);
            }
        }
    }

    Ok(configs)
}

/// Get or fetch node config (with caching)
pub fn get_node_config(conn: &mut Connection, node_type: &str) -> Result<NodeConfig, String> {
    // Check cache first
    {
        let cache = get_node_cache();
        let cache_read = cache.read().map_err(|_| "Cache lock poisoned")?;
        if let Some(config) = cache_read.get(node_type) {
            return Ok(config.clone());
        }
    }

    // Fetch from ValKey
    let config = fetch_node_config_from_valkey(conn, node_type)?;

    // Update cache
    {
        let cache = get_node_cache();
        let mut cache_write = cache.write().map_err(|_| "Cache lock poisoned")?;
        cache_write.insert(node_type.to_string(), config.clone());
    }

    Ok(config)
}

/// Get node config from cache only, returns default if not cached
pub fn get_cached_node_config(node_type: &str) -> NodeConfig {
    let cache = get_node_cache();
    if let Ok(cache_read) = cache.read() {
        if let Some(config) = cache_read.get(node_type) {
            return config.clone();
        }
    }

    // Return defaults if not in cache
    match node_type {
        "inference" => NodeConfig::default_inference(),
        "gpu_compute" => NodeConfig::default_gpu_compute(),
        "all" => NodeConfig::default_all(),
        _ => NodeConfig::default_general(),
    }
}

/// Pre-load node config into cache
pub fn cache_node_config(config: NodeConfig) {
    let cache = get_node_cache();
    if let Ok(mut cache_write) = cache.write() {
        let node_type = config.node_type.clone();
        cache_write.insert(node_type, config);
    }
}

/// Clear the node config cache
pub fn clear_node_cache() {
    let cache = get_node_cache();
    if let Ok(mut cache_write) = cache.write() {
        cache_write.clear();
    }
}

/// Register this node instance in ValKey
///
/// This function should be called during daemon startup to register
/// the node with its configuration and make it visible in the topology.
pub fn register_node_instance(
    conn: &mut Connection,
    node_id: &str,
    node_type: &str,
    site_id: &str,
    hostname: &str,
    ip_address: &str,
    config: &NodeConfig,
) -> Result<(), String> {
    let config_json = serde_json::to_string(config)
        .map_err(|e| format!("Failed to serialize config: {}", e))?;

    // Call the Lua function
    let result: Result<String, redis::RedisError> = redis::cmd("FCALL")
        .arg("GNODE_REGISTER_NODE")
        .arg(0)  // no keys
        .arg(node_id)
        .arg(node_type)
        .arg(&config_json)
        .arg(site_id)
        .arg(hostname)
        .arg(ip_address)
        .query(conn);

    match result {
        Ok(_) => {
            info!("Registered node '{}' (type: {}) in ValKey", node_id, node_type);
            Ok(())
        },
        Err(e) => {
            // If Lua function doesn't exist yet, register manually
            if e.to_string().contains("NOSCRIPT") || e.to_string().contains("Unknown function") {
                warn!("GNODE_REGISTER_NODE function not loaded, registering manually");
                register_node_manually(conn, node_id, node_type, site_id, hostname, ip_address, config)
            } else {
                Err(format!("Failed to register node: {}", e))
            }
        }
    }
}

/// Manual node registration (fallback when Lua function not loaded)
fn register_node_manually(
    conn: &mut Connection,
    node_id: &str,
    node_type: &str,
    site_id: &str,
    hostname: &str,
    ip_address: &str,
    _config: &NodeConfig,
) -> Result<(), String> {
    let now: i64 = redis::cmd("TIME")
        .query::<Vec<i64>>(conn)
        .map_err(|e| format!("Failed to get time: {}", e))?
        .first()
        .copied()
        .unwrap_or(0);

    let config_key = format!("gnode:node:{}:config", node_id);
    let health_key = format!("gnode:node:{}:health", node_id);
    let metrics_key = format!("gnode:node:{}:metrics", node_id);

    // Store config
    redis::cmd("HSET")
        .arg(&config_key)
        .arg(&[
            ("node_id", node_id),
            ("node_type", node_type),
            ("site_id", site_id),
            ("hostname", hostname),
            ("ip_address", ip_address),
            ("status", "active"),
        ])
        .query::<i64>(conn)
        .map_err(|e| format!("Failed to store node config: {}", e))?;

    redis::cmd("HSET")
        .arg(&config_key)
        .arg("registered_at")
        .arg(now)
        .query::<i64>(conn)
        .ok();

    // Initialize health
    redis::cmd("HSET")
        .arg(&health_key)
        .arg(&[
            ("status", "healthy"),
            ("heartbeat_count", "0"),
        ])
        .query::<i64>(conn)
        .ok();

    redis::cmd("HSET")
        .arg(&health_key)
        .arg("last_heartbeat")
        .arg(now)
        .query::<i64>(conn)
        .ok();

    // Initialize metrics
    redis::cmd("HSET")
        .arg(&metrics_key)
        .arg(&[
            ("commands_processed", "0"),
            ("commands_failed", "0"),
            ("bytes_in", "0"),
            ("bytes_out", "0"),
            ("load_factor", "0"),
        ])
        .query::<i64>(conn)
        .ok();

    // Add to registries
    redis::cmd("SADD")
        .arg("gnode:nodes:registry")
        .arg(node_id)
        .query::<i64>(conn)
        .ok();

    redis::cmd("SADD")
        .arg(format!("gnode:nodes:by_type:{}", node_type))
        .arg(node_id)
        .query::<i64>(conn)
        .ok();

    info!("Manually registered node '{}' (type: {})", node_id, node_type);
    Ok(())
}

/// Send a heartbeat for this node
pub fn send_node_heartbeat(
    conn: &mut Connection,
    node_id: &str,
    load_factor: f64,
    cpu_usage: Option<f64>,
    memory_usage: Option<f64>,
    active_requests: Option<u32>,
    avg_latency_ms: Option<u64>,
) -> Result<(), String> {
    // Try Lua function first
    let mut cmd = redis::cmd("FCALL");
    cmd.arg("GNODE_NODE_HEARTBEAT")
        .arg(0)
        .arg(node_id)
        .arg(load_factor);

    if let Some(cpu) = cpu_usage {
        cmd.arg(cpu);
    } else {
        cmd.arg("");
    }
    if let Some(mem) = memory_usage {
        cmd.arg(mem);
    } else {
        cmd.arg("");
    }
    if let Some(req) = active_requests {
        cmd.arg(req);
    } else {
        cmd.arg("");
    }
    if let Some(lat) = avg_latency_ms {
        cmd.arg(lat);
    } else {
        cmd.arg("");
    }

    let result: Result<i64, redis::RedisError> = cmd.query(conn);

    match result {
        Ok(_) => Ok(()),
        Err(e) => {
            // Fallback to manual update
            if e.to_string().contains("NOSCRIPT") || e.to_string().contains("Unknown function") {
                let now: i64 = redis::cmd("TIME")
                    .query::<Vec<i64>>(conn)
                    .ok()
                    .and_then(|v| v.first().copied())
                    .unwrap_or(0);

                let health_key = format!("gnode:node:{}:health", node_id);
                let metrics_key = format!("gnode:node:{}:metrics", node_id);

                redis::cmd("HSET")
                    .arg(&health_key)
                    .arg("last_heartbeat")
                    .arg(now)
                    .query::<i64>(conn)
                    .ok();

                redis::cmd("HINCRBY")
                    .arg(&health_key)
                    .arg("heartbeat_count")
                    .arg(1)
                    .query::<i64>(conn)
                    .ok();

                redis::cmd("HSET")
                    .arg(&metrics_key)
                    .arg("load_factor")
                    .arg(load_factor)
                    .query::<i64>(conn)
                    .ok();

                if let Some(cpu) = cpu_usage {
                    redis::cmd("HSET")
                        .arg(&metrics_key)
                        .arg("cpu_usage")
                        .arg(cpu)
                        .query::<i64>(conn)
                        .ok();
                }

                Ok(())
            } else {
                Err(format!("Failed to send heartbeat: {}", e))
            }
        }
    }
}

/// Record metrics for this node
pub fn record_node_metrics(
    conn: &mut Connection,
    node_id: &str,
    commands_processed: u32,
    commands_failed: u32,
    latency_ms: Option<u64>,
    bytes_in: Option<u64>,
    bytes_out: Option<u64>,
) -> Result<(), String> {
    let mut cmd = redis::cmd("FCALL");
    cmd.arg("GNODE_NODE_RECORD_METRICS")
        .arg(0)
        .arg(node_id)
        .arg(commands_processed)
        .arg(commands_failed);

    if let Some(lat) = latency_ms {
        cmd.arg(lat);
    } else {
        cmd.arg("");
    }
    if let Some(bin) = bytes_in {
        cmd.arg(bin);
    } else {
        cmd.arg("");
    }
    if let Some(bout) = bytes_out {
        cmd.arg(bout);
    } else {
        cmd.arg("");
    }

    let result: Result<String, redis::RedisError> = cmd.query(conn);

    match result {
        Ok(_) => Ok(()),
        Err(e) => {
            // Fallback to manual
            if e.to_string().contains("NOSCRIPT") || e.to_string().contains("Unknown function") {
                let metrics_key = format!("gnode:node:{}:metrics", node_id);

                redis::cmd("HINCRBY")
                    .arg(&metrics_key)
                    .arg("commands_processed")
                    .arg(commands_processed)
                    .query::<i64>(conn)
                    .ok();

                if commands_failed > 0 {
                    redis::cmd("HINCRBY")
                        .arg(&metrics_key)
                        .arg("commands_failed")
                        .arg(commands_failed)
                        .query::<i64>(conn)
                        .ok();
                }

                Ok(())
            } else {
                Err(format!("Failed to record metrics: {}", e))
            }
        }
    }
}

/// Initialize node configs: Load from files (master) or fetch from ValKey (worker)
pub fn initialize_node_configs(
    conn: &mut Connection,
    config_dir: Option<&str>,
    is_master: bool,
    node_type: &str,
) -> Result<NodeConfig, String> {
    if is_master {
        // Master: Load from files and store to ValKey
        if let Some(dir) = config_dir {
            let configs = load_node_configs_from_directory(dir)?;
            if !configs.is_empty() {
                store_node_configs_to_valkey(conn, &configs)?;

                // Cache all
                for config in &configs {
                    cache_node_config(config.clone());
                }

                return configs.into_iter()
                    .find(|c| c.node_type == node_type)
                    .ok_or_else(|| format!("No config found for node_type '{}'", node_type));
            }
        }

        // Fallback: Create defaults
        info!("No node config files found, creating defaults");
        let defaults = vec![
            NodeConfig::default_general(),
            NodeConfig::default_inference(),
            NodeConfig::default_gpu_compute(),
            NodeConfig::default_all(),
        ];
        store_node_configs_to_valkey(conn, &defaults)?;

        for config in &defaults {
            cache_node_config(config.clone());
        }

        defaults.into_iter()
            .find(|c| c.node_type == node_type)
            .ok_or_else(|| format!("No default config for node_type '{}'", node_type))
    } else {
        // Worker: Fetch from ValKey
        match get_node_config(conn, node_type) {
            Ok(config) => Ok(config),
            Err(e) => {
                warn!("Failed to fetch node config from ValKey: {}. Using default.", e);
                let config = match node_type {
                    "inference" => NodeConfig::default_inference(),
                    "gpu_compute" => NodeConfig::default_gpu_compute(),
                    "all" => NodeConfig::default_all(),
                    _ => NodeConfig::default_general(),
                };
                cache_node_config(config.clone());
                Ok(config)
            }
        }
    }
}

/// Check if a node is already registered in ValKey
///
/// Returns Ok(true) if node exists, Ok(false) if not, Err on failure
pub fn check_node_exists(
    conn: &mut Connection,
    node_id: &str,
) -> Result<bool, String> {
    // Call GNODE_NODE_GET_INFO - returns error if node not found
    let result: Result<String, redis::RedisError> = redis::cmd("FCALL")
        .arg("GNODE_NODE_GET_INFO")
        .arg(0)  // no keys
        .arg(node_id)
        .query(conn);

    match result {
        Ok(_) => {
            // Node exists
            Ok(true)
        },
        Err(e) => {
            let err_str = e.to_string();
            // "Node not found" is expected when node doesn't exist
            if err_str.contains("Node not found") {
                Ok(false)
            } else if err_str.contains("NOSCRIPT") || err_str.contains("Unknown function") {
                // Lua function not loaded - check manually via EXISTS
                let config_key = format!("gnode:node:{}:config", node_id);
                let exists: Result<i64, redis::RedisError> = redis::cmd("EXISTS")
                    .arg(&config_key)
                    .query(conn);
                match exists {
                    Ok(n) => Ok(n > 0),
                    Err(e2) => Err(format!("Failed to check node existence: {}", e2)),
                }
            } else {
                Err(format!("Failed to check node: {}", e))
            }
        }
    }
}

/// Register node with idempotency check
///
/// This function checks if the node already exists:
/// - If exists: sends a heartbeat to update status
/// - If not exists: performs full registration
///
/// Returns Ok(true) if newly registered, Ok(false) if already existed (heartbeat sent)
pub fn register_node_with_idempotency(
    conn: &mut Connection,
    node_id: &str,
    node_type: &str,
    site_id: &str,
    hostname: &str,
    ip_address: &str,
    config: &NodeConfig,
) -> Result<bool, String> {
    // Check if node already exists
    let exists = check_node_exists(conn, node_id)?;

    if exists {
        // Node already registered - send heartbeat instead
        info!("Node '{}' already registered, sending heartbeat", node_id);
        send_node_heartbeat(conn, node_id, 0.0, None, None, None, None)?;
        Ok(false)
    } else {
        // Node not registered - perform full registration
        info!("Node '{}' not found, performing registration", node_id);
        register_node_instance(conn, node_id, node_type, site_id, hostname, ip_address, config)?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_configs() {
        let general = NodeConfig::default_general();
        assert_eq!(general.node_type, "general");
        assert_eq!(general.routing.mode, "exclude");

        let inference = NodeConfig::default_inference();
        assert_eq!(inference.node_type, "inference");
        assert_eq!(inference.routing.mode, "include");
        assert!(inference.routing.group_hints.contains(&"inference".to_string()));

        let gpu = NodeConfig::default_gpu_compute();
        assert_eq!(gpu.node_type, "gpu_compute");
        assert_eq!(gpu.resources.cores, 2);

        let all = NodeConfig::default_all();
        assert_eq!(all.node_type, "all");
        assert_eq!(all.routing.mode, "all");
    }

    #[test]
    fn test_serialization() {
        let config = NodeConfig::default_inference();
        let json = serde_json::to_string(&config).expect("Serialization failed");
        let parsed: NodeConfig = serde_json::from_str(&json).expect("Deserialization failed");
        assert_eq!(parsed.node_type, config.node_type);
    }

    #[test]
    fn test_batch_size_validation_valid() {
        let batch = BatchSizeConfig {
            initial: 100,
            min: 50,
            max: 200,
        };
        assert!(batch.validate().is_ok());
    }

    #[test]
    fn test_batch_size_validation_min_exceeds_max() {
        let batch = BatchSizeConfig {
            initial: 100,
            min: 200,  // Invalid: min > max
            max: 100,
        };
        let result = batch.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("cannot exceed"));
    }

    #[test]
    fn test_batch_size_validation_initial_less_than_min() {
        let batch = BatchSizeConfig {
            initial: 25,  // Invalid: initial < min
            min: 50,
            max: 200,
        };
        let result = batch.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("cannot be less than"));
    }

    #[test]
    fn test_batch_size_validation_initial_exceeds_max() {
        let batch = BatchSizeConfig {
            initial: 300,  // Invalid: initial > max
            min: 50,
            max: 200,
        };
        let result = batch.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("cannot exceed"));
    }

    #[test]
    fn test_node_config_validation_valid() {
        let config = NodeConfig::default_general();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_node_config_validation_invalid_routing_mode() {
        let mut config = NodeConfig::default_general();
        config.routing.mode = "invalid_mode".to_string();
        let result = config.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid routing mode"));
    }
}
