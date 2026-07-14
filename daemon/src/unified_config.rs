//! Unified Configuration Module for gNode
//!
//! This module consolidates configuration from multiple sources into a single,
//! coherent structure. It replaces the overlapping fields in:
//! - `GNodeSettings` (config.rs) - stream processing settings
//! - `NodeConfig` (node_config.rs) - node-specific settings
//! - `RoutingConfig` (routing_config.rs) - message routing settings
//!
//! ## Configuration Precedence
//!
//! 1. CLI arguments (highest)
//! 2. Environment variables (GNODE_*)
//! 3. YAML configuration files
//! 4. Compiled defaults (lowest)
//!
//! ## Hot Reload Support
//!
//! Some configuration sections support hot reload via SIGHUP:
//! - stream: YES - tuning parameters
//! - consumer: YES (except group_name) - tuning parameters
//! - performance: YES (except threads.count) - thread pool fixed at start
//! - routing: YES - has cache invalidation
//! - health: YES - reporting intervals
//! - features: YES - feature flags
//!
//! NOT hot-reloadable (requires restart):
//! - connection: ValKey pool initialized at start
//! - identity: Consumer group names, node ID baked in
//!
//! ## Type Normalization
//!
//! All integer types are normalized to u64 to eliminate usize/u32 mismatches
//! between GNodeSettings, NodeConfig, and RoutingConfig.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};
use log::{debug, info, warn};

// =============================================================================
// Global Configuration Storage
// =============================================================================

/// Global unified config storage
static UNIFIED_CONFIG: OnceLock<Arc<RwLock<UnifiedConfig>>> = OnceLock::new();

/// Get the global unified configuration
/// Initializes with defaults if not yet set
pub fn get_config() -> Arc<RwLock<UnifiedConfig>> {
    UNIFIED_CONFIG
        .get_or_init(|| Arc::new(RwLock::new(UnifiedConfig::default())))
        .clone()
}

/// Update the global configuration (for hot reload)
/// Only updates hot-reloadable sections
pub fn update_config(new_config: UnifiedConfig) -> Result<(), String> {
    new_config.validate()?;

    let config_arc = get_config();
    let mut config = config_arc
        .write()
        .map_err(|e| format!("Config lock poisoned: {}", e))?;

    // Check for restart-required changes
    let restart_changes = config.requires_restart_from(&new_config);
    if !restart_changes.is_empty() {
        warn!("Some changes require restart: {:?}", restart_changes);
    }

    // Apply hot-reloadable sections only
    config.stream = new_config.stream;
    config.consumer = new_config.consumer;
    config.performance = new_config.performance;
    config.routing = new_config.routing;
    config.health = new_config.health;
    config.features = new_config.features;

    info!("Configuration hot-reloaded successfully");
    Ok(())
}

// =============================================================================
// Unified Configuration Structure
// =============================================================================

/// Unified configuration for gNode daemon
/// Consolidates GNodeSettings, NodeConfig, and RoutingConfig
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnifiedConfig {
    /// Schema version for compatibility
    #[serde(default = "default_version")]
    pub version: String,

    /// Connection settings (NOT hot-reloadable)
    #[serde(default)]
    pub connection: ConnectionConfig,

    /// Identity settings (NOT hot-reloadable)
    #[serde(default)]
    pub identity: IdentityConfig,

    /// Stream processing settings (hot-reloadable)
    #[serde(default)]
    pub stream: StreamConfig,

    /// Consumer group settings (hot-reloadable, except group_name)
    #[serde(default)]
    pub consumer: ConsumerConfig,

    /// Performance tuning (hot-reloadable, except thread count)
    #[serde(default)]
    pub performance: PerformanceConfig,

    /// Message routing (hot-reloadable)
    #[serde(default)]
    pub routing: RoutingConfigSection,

    /// Health reporting (hot-reloadable)
    #[serde(default)]
    pub health: HealthConfigSection,

    /// Feature flags (hot-reloadable)
    #[serde(default)]
    pub features: FeatureFlags,
}

fn default_version() -> String {
    "1.0.0".to_string()
}

impl Default for UnifiedConfig {
    fn default() -> Self {
        Self {
            version: default_version(),
            connection: ConnectionConfig::default(),
            identity: IdentityConfig::default(),
            stream: StreamConfig::default(),
            consumer: ConsumerConfig::default(),
            performance: PerformanceConfig::default(),
            routing: RoutingConfigSection::default(),
            health: HealthConfigSection::default(),
            features: FeatureFlags::default(),
        }
    }
}

impl UnifiedConfig {
    /// Validate the configuration for logical consistency
    pub fn validate(&self) -> Result<(), String> {
        // Validate batch sizes
        self.performance.batch.validate()?;

        // Validate backoff
        self.performance.backoff.validate()?;

        // Validate circuit breaker
        self.performance.circuit_breaker.validate()?;

        // Validate routing mode
        self.routing.validate()?;

        Ok(())
    }

    /// Check what changes would require a restart
    pub fn requires_restart_from(&self, other: &UnifiedConfig) -> Vec<String> {
        let mut changes = Vec::new();

        // Connection changes always require restart
        if self.connection != other.connection {
            changes.push("connection".to_string());
        }

        // Identity changes require restart
        if self.identity != other.identity {
            changes.push("identity".to_string());
        }

        // Thread count change requires restart
        if self.performance.threads.count != other.performance.threads.count {
            changes.push("performance.threads.count".to_string());
        }

        // Consumer group name change requires restart
        if self.consumer.group_name != other.consumer.group_name {
            changes.push("consumer.group_name".to_string());
        }

        changes
    }

    /// Load configuration from environment variables
    pub fn apply_env_overrides(&mut self) {
        // Connection
        if let Ok(val) = std::env::var("GNODE_VALKEY_HOST") {
            debug!("ENV override: connection.host = {}", val);
            self.connection.host = val;
        }
        if let Ok(val) = std::env::var("GNODE_VALKEY_PORT") {
            if let Ok(v) = val.parse() {
                debug!("ENV override: connection.port = {}", v);
                self.connection.port = v;
            }
        }

        // Identity
        if let Ok(val) = std::env::var("GNODE_NODE_ID") {
            debug!("ENV override: identity.node_id = {}", val);
            self.identity.node_id = val;
        }
        if let Ok(val) = std::env::var("GNODE_NODE_TYPE") {
            debug!("ENV override: identity.node_type = {}", val);
            self.identity.node_type = val;
        }
        if let Ok(val) = std::env::var("GNODE_ENVIRONMENT") {
            debug!("ENV override: identity.environment = {}", val);
            self.identity.environment = val;
        }
        if let Ok(val) = std::env::var("GNODE_TOPOLOGY_NAMESPACE") {
            debug!("ENV override: identity.topology_namespace = {}", val);
            self.identity.topology_namespace = val;
        }
        if let Ok(val) = std::env::var("GNODE_STREAM_PREFIX") {
            debug!("ENV override: identity.stream_prefix = {}", val);
            self.identity.stream_prefix = val;
        }
        if let Ok(val) = std::env::var("GNODE_DIMENSIONS") {
            if let Ok(v) = val.parse() {
                debug!("ENV override: identity.dimensions = {}", v);
                self.identity.dimensions = v;
            }
        }
        if let Ok(val) = std::env::var("GNODE_MASTER") {
            let v = val.to_lowercase() == "true" || val == "1";
            debug!("ENV override: identity.master = {}", v);
            self.identity.master = v;
        }

        // Stream
        if let Ok(val) = std::env::var("GNODE_STREAM_MAX_LENGTH") {
            if let Ok(v) = val.parse() {
                debug!("ENV override: stream.max_length = {}", v);
                self.stream.max_length = v;
            }
        }
        if let Ok(val) = std::env::var("GNODE_TRIM_INTERVAL_SECS") {
            if let Ok(v) = val.parse() {
                debug!("ENV override: stream.trim_interval_secs = {}", v);
                self.stream.trim_interval_secs = v;
            }
        }
        if let Ok(val) = std::env::var("GNODE_APPROXIMATE_TRIM") {
            let v = val.to_lowercase() == "true" || val == "1";
            debug!("ENV override: stream.approximate_trim = {}", v);
            self.stream.approximate_trim = v;
        }
        if let Ok(val) = std::env::var("GNODE_STREAM_REFRESH_SECS") {
            if let Ok(v) = val.parse() {
                debug!("ENV override: stream.refresh_secs = {}", v);
                self.stream.refresh_secs = v;
            }
        }

        // Consumer
        if let Ok(val) = std::env::var("GNODE_GROUP_NAME") {
            debug!("ENV override: consumer.group_name = {}", val);
            self.consumer.group_name = val;
        }
        if let Ok(val) = std::env::var("GNODE_CONSUMER_PREFIX") {
            debug!("ENV override: consumer.prefix = {}", val);
            self.consumer.prefix = val;
        }
        if let Ok(val) = std::env::var("GNODE_BLOCK_TIMEOUT_MS") {
            if let Ok(v) = val.parse() {
                debug!("ENV override: consumer.block_timeout_ms = {}", v);
                self.consumer.block_timeout_ms = v;
            }
        }
        if let Ok(val) = std::env::var("GNODE_CLAIM_INTERVAL_MS") {
            if let Ok(v) = val.parse() {
                debug!("ENV override: consumer.claim_interval_ms = {}", v);
                self.consumer.claim_interval_ms = v;
            }
        }
        if let Ok(val) = std::env::var("GNODE_BATCH_ACKNOWLEDGE") {
            let v = val.to_lowercase() == "true" || val == "1";
            debug!("ENV override: consumer.batch_acknowledge = {}", v);
            self.consumer.batch_acknowledge = v;
        }
        if let Ok(val) = std::env::var("GNODE_MAX_PENDING_CLAIM") {
            if let Ok(v) = val.parse() {
                debug!("ENV override: consumer.max_pending_claim = {}", v);
                self.consumer.max_pending_claim = v;
            }
        }

        // Performance - Batch
        if let Ok(val) = std::env::var("GNODE_INITIAL_BATCH_SIZE") {
            if let Ok(v) = val.parse() {
                debug!("ENV override: performance.batch.initial = {}", v);
                self.performance.batch.initial = v;
            }
        }
        if let Ok(val) = std::env::var("GNODE_MIN_BATCH_SIZE") {
            if let Ok(v) = val.parse() {
                debug!("ENV override: performance.batch.min = {}", v);
                self.performance.batch.min = v;
            }
        }
        if let Ok(val) = std::env::var("GNODE_MAX_BATCH_SIZE") {
            if let Ok(v) = val.parse() {
                debug!("ENV override: performance.batch.max = {}", v);
                self.performance.batch.max = v;
            }
        }

        // Performance - Backoff
        if let Ok(val) = std::env::var("GNODE_BASE_BACKOFF_MS") {
            if let Ok(v) = val.parse() {
                debug!("ENV override: performance.backoff.base_ms = {}", v);
                self.performance.backoff.base_ms = v;
            }
        }
        if let Ok(val) = std::env::var("GNODE_MAX_BACKOFF_MS") {
            if let Ok(v) = val.parse() {
                debug!("ENV override: performance.backoff.max_ms = {}", v);
                self.performance.backoff.max_ms = v;
            }
        }
        if let Ok(val) = std::env::var("GNODE_IDLE_TIME_MS") {
            if let Ok(v) = val.parse() {
                debug!("ENV override: performance.backoff.idle_time_ms = {}", v);
                self.performance.backoff.idle_time_ms = v;
            }
        }
        if let Ok(val) = std::env::var("GNODE_PENDING_CHECK_MS") {
            if let Ok(v) = val.parse() {
                debug!("ENV override: performance.backoff.pending_check_ms = {}", v);
                self.performance.backoff.pending_check_ms = v;
            }
        }

        // Performance - Circuit Breaker
        if let Ok(val) = std::env::var("GNODE_CB_THRESHOLD")
            .or_else(|_| std::env::var("GNODE_CIRCUIT_BREAKER_THRESHOLD"))
        {
            if let Ok(v) = val.parse() {
                debug!("ENV override: performance.circuit_breaker.threshold = {}", v);
                self.performance.circuit_breaker.threshold = v;
            }
        }
        if let Ok(val) = std::env::var("GNODE_CB_COOLDOWN_SECS")
            .or_else(|_| std::env::var("GNODE_CIRCUIT_BREAKER_COOLDOWN_SECS"))
        {
            if let Ok(v) = val.parse() {
                debug!("ENV override: performance.circuit_breaker.cooldown_secs = {}", v);
                self.performance.circuit_breaker.cooldown_secs = v;
            }
        }

        // Performance - Threads
        if let Ok(val) = std::env::var("GNODE_THREADS") {
            debug!("ENV override: performance.threads.count = {}", val);
            self.performance.threads.count = val;
        }
        if let Ok(val) = std::env::var("GNODE_MAX_THREADS") {
            if let Ok(v) = val.parse() {
                debug!("ENV override: performance.threads.max = {}", v);
                self.performance.threads.max = v;
            }
        }

        // Routing
        if let Ok(val) = std::env::var("GNODE_ROUTING_MODE") {
            debug!("ENV override: routing.mode = {}", val);
            self.routing.mode = val;
        }
        if let Ok(val) = std::env::var("GNODE_ROUTING_HINTS") {
            let hints: Vec<String> = val.split(',').map(|s| s.trim().to_string()).collect();
            debug!("ENV override: routing.group_hints = {:?}", hints);
            self.routing.group_hints = hints;
        }

        // Feature flags
        if let Ok(val) = std::env::var("GNODE_DEBUG") {
            let v = val.to_lowercase() == "true" || val == "1";
            debug!("ENV override: features.debug = {}", v);
            self.features.debug = v;
        }
        if let Ok(val) = std::env::var("GNODE_LOG_LEVEL") {
            debug!("ENV override: features.log_level = {}", val);
            self.features.log_level = val;
        }
    }
}

// =============================================================================
// Configuration Sections
// =============================================================================

/// Connection configuration (NOT hot-reloadable)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConnectionConfig {
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u64,
    #[serde(default)]
    pub user: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
}

fn default_host() -> String { "127.0.0.1".to_string() }
fn default_port() -> u64 { 47445 }

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
            user: None,
            password: None,
        }
    }
}

/// Identity configuration (NOT hot-reloadable)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IdentityConfig {
    #[serde(default = "default_node_id")]
    pub node_id: String,
    #[serde(default = "default_node_type")]
    pub node_type: String,
    #[serde(default = "default_environment")]
    pub environment: String,
    #[serde(default = "default_topology_namespace")]
    pub topology_namespace: String,
    #[serde(default = "default_stream_prefix")]
    pub stream_prefix: String,
    #[serde(default = "default_dimensions")]
    pub dimensions: u64,
    #[serde(default)]
    pub master: bool,
}

fn default_node_id() -> String { "default".to_string() }
fn default_node_type() -> String { "general".to_string() }
fn default_environment() -> String { "all".to_string() }
fn default_topology_namespace() -> String { "geodineum".to_string() }
fn default_stream_prefix() -> String { "gnode".to_string() }
// Default to the service-tier total_dimensions (30 = 25 discovery + 5 storage,
// per daemon/config/service_schema.yaml). Other tiers and custom topologies
// override this at registration time.
fn default_dimensions() -> u64 { 30 }

impl Default for IdentityConfig {
    fn default() -> Self {
        Self {
            node_id: default_node_id(),
            node_type: default_node_type(),
            environment: default_environment(),
            topology_namespace: default_topology_namespace(),
            stream_prefix: default_stream_prefix(),
            dimensions: default_dimensions(),
            master: false,
        }
    }
}

/// Stream configuration (hot-reloadable)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamConfig {
    #[serde(default = "default_max_length")]
    pub max_length: u64,
    #[serde(default = "default_trim_interval_secs")]
    pub trim_interval_secs: u64,
    #[serde(default = "default_approximate_trim")]
    pub approximate_trim: bool,
    #[serde(default = "default_refresh_secs")]
    pub refresh_secs: u64,
}

fn default_max_length() -> u64 { 10000 }
fn default_trim_interval_secs() -> u64 { 60 }
fn default_approximate_trim() -> bool { true }
fn default_refresh_secs() -> u64 { 60 }

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            max_length: default_max_length(),
            trim_interval_secs: default_trim_interval_secs(),
            approximate_trim: default_approximate_trim(),
            refresh_secs: default_refresh_secs(),
        }
    }
}

/// Consumer configuration (hot-reloadable, except group_name)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerConfig {
    #[serde(default = "default_group_name")]
    pub group_name: String,
    #[serde(default = "default_prefix")]
    pub prefix: String,
    #[serde(default = "default_block_timeout_ms")]
    pub block_timeout_ms: u64,
    #[serde(default = "default_claim_interval_ms")]
    pub claim_interval_ms: u64,
    #[serde(default = "default_batch_acknowledge")]
    pub batch_acknowledge: bool,
    #[serde(default = "default_max_pending_claim")]
    pub max_pending_claim: u64,
}

fn default_group_name() -> String { "gnode-daemon".to_string() }
fn default_prefix() -> String { "consumer-".to_string() }
fn default_block_timeout_ms() -> u64 { 1000 }
fn default_claim_interval_ms() -> u64 { 5000 }
fn default_batch_acknowledge() -> bool { true }
fn default_max_pending_claim() -> u64 { 50 }

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            group_name: default_group_name(),
            prefix: default_prefix(),
            block_timeout_ms: default_block_timeout_ms(),
            claim_interval_ms: default_claim_interval_ms(),
            batch_acknowledge: default_batch_acknowledge(),
            max_pending_claim: default_max_pending_claim(),
        }
    }
}

/// Performance configuration (hot-reloadable, except thread count)
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PerformanceConfig {
    #[serde(default)]
    pub batch: BatchConfig,
    #[serde(default)]
    pub backoff: BackoffConfig,
    #[serde(default)]
    pub circuit_breaker: CircuitBreakerConfig,
    #[serde(default)]
    pub threads: ThreadConfig,
}

/// Batch size configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchConfig {
    #[serde(default = "default_batch_initial")]
    pub initial: u64,
    #[serde(default = "default_batch_min")]
    pub min: u64,
    #[serde(default = "default_batch_max")]
    pub max: u64,
}

fn default_batch_initial() -> u64 { 250 }
fn default_batch_min() -> u64 { 50 }
fn default_batch_max() -> u64 { 500 }

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            initial: default_batch_initial(),
            min: default_batch_min(),
            max: default_batch_max(),
        }
    }
}

impl BatchConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.min > self.max {
            return Err(format!(
                "batch.min ({}) cannot exceed batch.max ({})",
                self.min, self.max
            ));
        }
        if self.initial < self.min {
            return Err(format!(
                "batch.initial ({}) cannot be less than batch.min ({})",
                self.initial, self.min
            ));
        }
        if self.initial > self.max {
            return Err(format!(
                "batch.initial ({}) cannot exceed batch.max ({})",
                self.initial, self.max
            ));
        }
        Ok(())
    }
}

/// Backoff configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackoffConfig {
    #[serde(default = "default_base_ms")]
    pub base_ms: u64,
    #[serde(default = "default_max_ms")]
    pub max_ms: u64,
    #[serde(default = "default_idle_time_ms")]
    pub idle_time_ms: u64,
    #[serde(default = "default_pending_check_ms")]
    pub pending_check_ms: u64,
}

fn default_base_ms() -> u64 { 100 }
fn default_max_ms() -> u64 { 1000 }
fn default_idle_time_ms() -> u64 { 30000 }
fn default_pending_check_ms() -> u64 { 5000 }

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            base_ms: default_base_ms(),
            max_ms: default_max_ms(),
            idle_time_ms: default_idle_time_ms(),
            pending_check_ms: default_pending_check_ms(),
        }
    }
}

impl BackoffConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.base_ms == 0 {
            return Err("backoff.base_ms cannot be 0 (would cause busy-loop)".to_string());
        }
        if self.max_ms < self.base_ms {
            return Err(format!(
                "backoff.max_ms ({}) must be >= backoff.base_ms ({})",
                self.max_ms, self.base_ms
            ));
        }
        Ok(())
    }
}

/// Circuit breaker configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircuitBreakerConfig {
    #[serde(default = "default_cb_threshold")]
    pub threshold: u64,
    #[serde(default = "default_cb_cooldown_secs")]
    pub cooldown_secs: u64,
}

fn default_cb_threshold() -> u64 { 5 }
fn default_cb_cooldown_secs() -> u64 { 30 }

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            threshold: default_cb_threshold(),
            cooldown_secs: default_cb_cooldown_secs(),
        }
    }
}

impl CircuitBreakerConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.threshold == 0 {
            return Err("circuit_breaker.threshold cannot be 0".to_string());
        }
        if self.cooldown_secs == 0 {
            return Err("circuit_breaker.cooldown_secs cannot be 0".to_string());
        }
        Ok(())
    }
}

/// Thread configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadConfig {
    /// Thread count: "auto" or a specific number
    #[serde(default = "default_thread_count")]
    pub count: String,
    /// Maximum threads when using auto
    #[serde(default = "default_thread_max")]
    pub max: u64,
}

fn default_thread_count() -> String { "auto".to_string() }
fn default_thread_max() -> u64 { 16 }

impl Default for ThreadConfig {
    fn default() -> Self {
        Self {
            count: default_thread_count(),
            max: default_thread_max(),
        }
    }
}

/// Routing configuration (hot-reloadable)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingConfigSection {
    /// Routing mode: "include", "exclude", or "all"
    #[serde(default = "default_routing_mode")]
    pub mode: String,
    /// Group hints for message filtering
    #[serde(default = "default_routing_hints")]
    pub group_hints: Vec<String>,
}

fn default_routing_mode() -> String { "exclude".to_string() }
fn default_routing_hints() -> Vec<String> {
    vec!["inference".to_string(), "gpu_compute".to_string()]
}

impl Default for RoutingConfigSection {
    fn default() -> Self {
        Self {
            mode: default_routing_mode(),
            group_hints: default_routing_hints(),
        }
    }
}

impl RoutingConfigSection {
    pub fn validate(&self) -> Result<(), String> {
        let valid_modes = ["include", "exclude", "all"];
        if !valid_modes.contains(&self.mode.as_str()) {
            return Err(format!(
                "Invalid routing mode '{}'. Valid modes: {:?}",
                self.mode, valid_modes
            ));
        }
        Ok(())
    }
}

/// Health reporting configuration (hot-reloadable)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthConfigSection {
    #[serde(default = "default_report_interval_ms")]
    pub report_interval_ms: u64,
    #[serde(default = "default_metrics_enabled")]
    pub metrics_enabled: bool,
    #[serde(default = "default_metrics_retention_secs")]
    pub metrics_retention_secs: u64,
}

fn default_report_interval_ms() -> u64 { 5000 }
fn default_metrics_enabled() -> bool { true }
fn default_metrics_retention_secs() -> u64 { 3600 }

impl Default for HealthConfigSection {
    fn default() -> Self {
        Self {
            report_interval_ms: default_report_interval_ms(),
            metrics_enabled: default_metrics_enabled(),
            metrics_retention_secs: default_metrics_retention_secs(),
        }
    }
}

/// Feature flags (hot-reloadable)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureFlags {
    #[serde(default)]
    pub debug: bool,
    #[serde(default = "default_log_level")]
    pub log_level: String,
    #[serde(default)]
    pub enable_htt: bool,
    #[serde(default)]
    pub enable_geometric_threading: bool,
    #[serde(default)]
    pub enable_fixed_point: bool,
    /// Custom feature flags
    #[serde(default)]
    pub custom: HashMap<String, bool>,
}

fn default_log_level() -> String { "info".to_string() }

impl Default for FeatureFlags {
    fn default() -> Self {
        Self {
            debug: false,
            log_level: default_log_level(),
            enable_htt: false,
            enable_geometric_threading: false,
            enable_fixed_point: false,
            custom: HashMap::new(),
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = UnifiedConfig::default();
        assert_eq!(config.version, "1.0.0");
        // Service tier total_dimensions (canonical default).
        assert_eq!(config.identity.dimensions, 30);
        assert_eq!(config.performance.batch.initial, 250);
    }

    #[test]
    fn test_validation_valid() {
        let config = UnifiedConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validation_invalid_batch() {
        let mut config = UnifiedConfig::default();
        config.performance.batch.min = 500;
        config.performance.batch.max = 100;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validation_invalid_routing_mode() {
        let mut config = UnifiedConfig::default();
        config.routing.mode = "invalid".to_string();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_restart_required_detection() {
        let config1 = UnifiedConfig::default();
        let mut config2 = config1.clone();

        // No changes - should be empty
        assert!(config1.requires_restart_from(&config2).is_empty());

        // Change identity - should require restart
        config2.identity.node_id = "different".to_string();
        assert!(!config1.requires_restart_from(&config2).is_empty());
    }

}
