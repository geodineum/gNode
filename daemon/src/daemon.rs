use std::collections::HashMap;
use std::sync::{Arc, RwLock, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;
use redis::Client;
use serde::{Serialize, Deserialize};
use log::{info, error, debug, warn};

// ========================================
// Graceful Shutdown Infrastructure
// ========================================

/// Global shutdown flag for coordinating graceful shutdown
/// Checked by all worker threads to know when to exit
static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

/// Check if shutdown has been requested
/// Called by worker threads in their main loops
pub fn is_shutdown_requested() -> bool {
    SHUTDOWN_REQUESTED.load(Ordering::SeqCst)
}

/// Request shutdown (called by signal handler)
/// Sets the global flag that all workers check
pub fn request_shutdown() {
    info!("Shutdown requested - signaling all workers to stop");
    SHUTDOWN_REQUESTED.store(true, Ordering::SeqCst);
}

// Import StreamDiscoveryManager for dynamic site discovery
use crate::integration::stream_discovery::{StreamDiscoveryManager, StreamDiscoveryConfig};

use crate::{
    SharedTopology, Result, GeometricError
};

/// Command received from clients
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Command {
    pub id: String,
    pub command: String,
    pub parameters: serde_json::Value,
    pub site_id: String,
    pub node_id: String,
    pub timestamp: f64,
}

/// Response sent back to clients
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    pub id: String,
    pub status: String,
    pub result: Option<serde_json::Value>,
    pub error: Option<String>,
    pub timestamp: f64,
    pub batch_id: Option<String>,
    pub sequence: Option<u32>,
}

// Include the thread configuration enum from main.rs
#[derive(Debug, Clone, Copy)]
pub enum ThreadConfig {
    /// Automatic configuration based on CPU cores
    Auto(usize),
    /// Fixed number of threads
    Fixed(usize),
}

/// Node type for message routing within consumer groups
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeType {
    /// Processes all non-inference messages (default)
    General,
    /// Only processes messages with _gh:"inference" routing hint
    Inference,
    /// Processes all messages regardless of routing hint
    All,
}

impl From<&str> for NodeType {
    fn from(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "inference" => NodeType::Inference,
            "all" => NodeType::All,
            _ => NodeType::General,
        }
    }
}

impl std::fmt::Display for NodeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeType::General => write!(f, "general"),
            NodeType::Inference => write!(f, "inference"),
            NodeType::All => write!(f, "all"),
        }
    }
}

/// Main daemon struct
pub struct GNodeDaemon {
    pub client: Client,
    pub topology_manager: SharedTopology,

    /// Format processor for custom message format support (requires "cms" feature)
    /// Uses Arc<Mutex<Option<Arc<T>>>> for interior mutability since run() takes &self
    pub format_processor: Arc<std::sync::Mutex<Option<Arc<crate::integration::processor::FormatProcessor>>>>,

    /// Stream discovery manager for dynamic site discovery
    /// Discovers registered sites and their streams from topology
    /// Replaces hardcoded site_id - daemon is now site-agnostic
    pub stream_discovery: Arc<RwLock<StreamDiscoveryManager>>,

    /// Service discovery manager for periodic config-based service registration
    /// Scans geometric_topology.yaml and registers services for all discovered sites
    pub service_discovery: Arc<RwLock<crate::integration::service_discovery::ServiceDiscoveryManager>>,

    /// Worker thread handles for graceful shutdown
    /// Stores JoinHandles so we can join threads on shutdown instead of detaching
    pub worker_handles: Mutex<Vec<JoinHandle<()>>>,

    /// Topology namespace - the shared namespace for service registration and discovery
    /// All services across all sites register to {topology_namespace}:gnode:topology
    /// Default: "geodineum" → creates key {geodineum}:gnode:topology
    pub topology_namespace: String,
    /// DTAP environment for stream isolation (testing, staging, acceptance, production)
    /// All nodes in the same environment share streams via consumer groups
    /// NOTE: site_id has been removed - daemon discovers sites dynamically via StreamDiscoveryManager
    pub environment: String,
    /// Unique node identifier within the environment (used as consumer name)
    pub node_id: String,
    /// Node type for message routing (general, inference, all)
    pub node_type: NodeType,
    /// Whether this node operates as master (loads configs from YAML, stores to ValKey)
    /// Master nodes load configurations from daemon/config/nodes/*.yaml and store them
    /// to ValKey for worker nodes to fetch. Can be set via --master flag or --node-id=master.
    pub is_master: bool,
    pub stream_prefix: String,
    pub debug: bool,
    pub debug_level: LogLevel,
    pub dimensions: usize,
    pub thread_config: Option<ThreadConfig>,
    pub stream_config: crate::config::GNodeSettings,
    /// Single-threaded mode: cooperative tick-based scheduling instead of spawning threads
    /// When enabled, all workers run in the main thread using WorkerRegistry
    pub single_threaded: bool,
    /// Directory containing node type configuration files (*.yaml)
    /// If None, uses resolve_node_config_dir() fallback chain
    pub node_config_dir: Option<String>,
}

/// Resolve the node configuration directory with fallback chain:
/// 1. Explicit path (if provided)
/// 2. GNODE_NODE_CONFIG_DIR environment variable
/// 3. /etc/geodineum/components/gnode-daemon/nodes/ (centralized ecosystem location)
/// 4. daemon/config/nodes (local fallback)
pub fn resolve_node_config_dir(explicit_path: Option<&str>) -> Option<String> {
    use std::path::Path;

    // 1. Explicit path takes highest priority
    if let Some(path) = explicit_path {
        if Path::new(path).exists() {
            return Some(path.to_string());
        }
    }

    // 2. Environment variable
    if let Ok(env_path) = std::env::var("GNODE_NODE_CONFIG_DIR") {
        if Path::new(&env_path).exists() {
            info!("Using node config dir from GNODE_NODE_CONFIG_DIR: {}", env_path);
            return Some(env_path);
        }
    }

    // 3. Centralized ecosystem location (/etc/geodineum)
    let ecosystem_path = "/etc/geodineum/components/gnode-daemon/nodes";
    if Path::new(ecosystem_path).exists() {
        info!("Using centralized node config dir: {}", ecosystem_path);
        return Some(ecosystem_path.to_string());
    }

    // 4. Local fallback (relative to working directory)
    let legacy_path = "daemon/config/nodes";
    if Path::new(legacy_path).exists() {
        return Some(legacy_path.to_string());
    }

    // No valid path found
    None
}

// Add this enum
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogLevel {
    Error,
    Warning,
    Info,
    Debug,
    Trace
}

impl From<&str> for LogLevel {
    fn from(level: &str) -> Self {
        match level.to_lowercase().as_str() {
            "error" => LogLevel::Error,
            "warning" | "warn" => LogLevel::Warning,
            "info" => LogLevel::Info,
            "debug" => LogLevel::Debug,
            "trace" => LogLevel::Trace,
            _ => LogLevel::Info, // Default to Info
        }
    }
}

// Module-level static for format processor storage (requires "format" feature)
static FORMAT_PROCESSOR_STORAGE: std::sync::OnceLock<Arc<crate::integration::processor::FormatProcessor>> = std::sync::OnceLock::new();

// Module-level static for load metrics manager storage
static LOAD_METRICS_MANAGER_STORAGE: std::sync::OnceLock<Arc<crate::integration::load_metrics::LoadMetricsManager>> = std::sync::OnceLock::new();

// Module-level static for shared topology storage
// This allows the topology loaded from ValKey to be accessed statically by command handlers
static TOPOLOGY_STORAGE: std::sync::OnceLock<Arc<RwLock<crate::GeometricTopology>>> = std::sync::OnceLock::new();

// Module-level static for topology namespace (set once at daemon init)
static TOPOLOGY_NAMESPACE: std::sync::OnceLock<String> = std::sync::OnceLock::new();

impl GNodeDaemon {
    /// Create a new daemon instance with custom stream config
    ///
    /// # Arguments
    /// * `topology_namespace` - Shared namespace for topology (all services register to {topology_namespace}:gnode:topology)
    /// * `environment` - DTAP environment (testing/staging/acceptance/production)
    /// * `node_id` - Unique node identifier
    /// * `is_master` - Whether this node operates as master (loads configs from YAML, stores to ValKey).
    ///   Can be set via `--master` flag or `--node-id=master`.
    ///
    /// NOTE: site_id has been removed. The daemon is infrastructure, not a site.
    /// Sites are discovered dynamically via StreamDiscoveryManager.
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_config(redis_url: &str, dimensions: usize, topology_namespace: String, environment: String, node_id: String, node_type: String, stream_prefix: String, debug: bool, debug_level: &str, is_master: bool, stream_config: crate::config::GNodeSettings) -> Result<Self> {
        let node_type_enum = NodeType::from(node_type.as_str());
        info!("Initializing gNode daemon with Redis URL: {}, dimensions: {}, topology_namespace: {} → {{{}}}:gnode:topology, environment: {}, node_id: {}, node_type: {}, is_master: {}, stream_prefix: {}, debug_level: {}",
            redis_url, dimensions, topology_namespace, topology_namespace, environment, node_id, node_type_enum, is_master, stream_prefix, debug_level);
        info!("  Stream discovery: DYNAMIC (sites discovered from topology)");

        let client = Client::open(redis_url).map_err(GeometricError::Redis)?;

        // Create topology manager with Redis storage using topology_namespace for the topology key
        // This creates the shared topology at {topology_namespace}:gnode:topology (e.g., {geodineum}:gnode:topology)
        let topology_manager = SharedTopology::with_storage(
            dimensions,
            redis_url,
            &topology_namespace,  // Use topology_namespace instead of site_id for topology key
            &stream_prefix
        )?;

        // STATELESS ARCHITECTURE:
        // Capability dimensions are statically defined in integration/handlers/types.rs
        // via SERVICE_DIMENSIONS lazy_static (30 dims, TOTAL_DIMENSIONS constant).
        // No runtime registration needed for the canonical service tier; custom
        // topologies created via topo_create / gNode-TOPO have their own dim counts
        // declared at creation time and stored in ValKey.
        // Schema source-of-truth: daemon/config/service_schema.yaml (30D = 25 discovery + 5 storage).

        // Set the static topology reference so command handlers can access the same topology
        // This is critical for proper registration/deregistration of services
        Self::set_topology_ref(topology_manager.get_topology_ref());
        Self::set_topology_namespace(topology_namespace.clone());
        info!("Static topology reference set for command handlers");

        // Initialize StreamDiscoveryManager for dynamic site discovery
        // Uses per-site environment configuration via GNODE_SERVICE_GET_ALL_STREAMS
        let stream_discovery_config = StreamDiscoveryConfig {
            refresh_interval_secs: stream_config.stream_refresh_secs,  // From CLI --stream-refresh-secs
            environment: environment.clone(),
            include_broadcast: true,
            include_health: true,
            include_unified: true,
        };
        let stream_discovery = Arc::new(RwLock::new(StreamDiscoveryManager::new(stream_discovery_config)));
        info!("StreamDiscoveryManager initialized for environment: {}", environment);

        // Initialize ServiceDiscoveryManager with default config
        // CLI flags will override via set_service_discovery_config() after construction
        let service_discovery_config = crate::integration::service_discovery::ServiceDiscoveryConfig::default();
        let service_discovery = Arc::new(RwLock::new(
            crate::integration::service_discovery::ServiceDiscoveryManager::new(service_discovery_config)
        ));
        info!("ServiceDiscoveryManager initialized (default config, will be updated from CLI)");

        Ok(Self {
            client,
            topology_manager,

            format_processor: Arc::new(std::sync::Mutex::new(None)), // Will be set during run() when format system initializes

            stream_discovery,
            service_discovery,

            // Initialize empty worker handles vec for graceful shutdown
            worker_handles: Mutex::new(Vec::new()),

            topology_namespace,
            environment,
            node_id,
            node_type: node_type_enum,
            is_master,
            stream_prefix,
            debug,
            debug_level: LogLevel::from(debug_level),
            dimensions,
            thread_config: None, // Default is None, will be set later
            stream_config,
            single_threaded: false, // Default is multi-threaded, can be set via set_single_threaded()
            node_config_dir: None, // Will use resolve_node_config_dir() fallback chain
        })
    }

    /// Create a new daemon instance with default stream config
    /// Note: is_master defaults to false; use new_with_config for master nodes
    /// Uses "geodineum" as default topology namespace
    /// NOTE: site_id has been removed - daemon discovers sites dynamically
    #[allow(clippy::too_many_arguments)]
    pub fn new(redis_url: &str, dimensions: usize, environment: String, node_id: String, node_type: String, stream_prefix: String, debug: bool, debug_level: &str) -> Result<Self> {
        let is_master = node_id == "master";
        // Default topology namespace
        Self::new_with_config(redis_url, dimensions, "geodineum".to_string(), environment, node_id, node_type, stream_prefix, debug, debug_level, is_master, crate::config::GNodeSettings::default())
    }
    
    /// Set the thread configuration for worker threads
    pub fn set_thread_config(&mut self, config: ThreadConfig) {
        self.thread_config = Some(config);
        info!("Thread configuration set to: {:?}", config);
    }

    /// Enable or disable single-threaded mode
    ///
    /// In single-threaded mode, all workers run cooperatively in the main thread
    /// using tick-based scheduling (WorkerRegistry). This is useful for:
    /// - Debugging and profiling (deterministic execution)
    /// - Resource-constrained environments
    /// - Simpler deployment without thread management
    pub fn set_single_threaded(&mut self, enabled: bool) {
        self.single_threaded = enabled;
        if enabled {
            info!("Single-threaded mode ENABLED: workers will run cooperatively in main thread");
        } else {
            info!("Multi-threaded mode (default): workers will spawn in separate threads");
        }
    }

    /// Set the directory containing node type configuration files
    ///
    /// If not set, the daemon uses resolve_node_config_dir() fallback chain:
    /// 1. GNODE_NODE_CONFIG_DIR environment variable
    /// 2. /etc/geodineum/components/gnode-daemon/nodes/ (centralized)
    /// 3. daemon/config/nodes (local fallback)
    pub fn set_node_config_dir(&mut self, dir: Option<String>) {
        if let Some(ref path) = dir {
            info!("Node config directory set to: {}", path);
        }
        self.node_config_dir = dir;
    }

    /// Configure service discovery from CLI flags
    pub fn set_service_discovery_config(
        &mut self,
        enabled: bool,
        interval_secs: u64,
        extra_paths: Option<String>,
        paths_file: Option<String>,
    ) {
        if let Ok(mut sd) = self.service_discovery.write() {
            sd.config_mut().enabled = enabled;
            sd.config_mut().scan_interval_secs = interval_secs;
            if let Some(paths) = extra_paths {
                sd.config_mut().extra_config_paths = paths
                    .split(',')
                    .map(|s| std::path::PathBuf::from(s.trim()))
                    .collect();
            }
            if let Some(pf) = paths_file {
                sd.config_mut().discovery_paths_file = Some(std::path::PathBuf::from(pf));
            }
            info!("ServiceDiscovery configured: enabled={}, interval={}s", enabled, interval_secs);
        }
    }

    /// Get the thread count based on configuration
    pub fn get_thread_count(&self) -> usize {
        match self.thread_config {
            Some(ThreadConfig::Auto(count)) => count,
            Some(ThreadConfig::Fixed(count)) => count,
            None => {
                // Default to minimum of CPU count or 4 if not configured
                let num_cores = num_cpus::get();
                std::cmp::min(num_cores, 4)
            }
        }
    }
    
    /// Get topology reference for static access
    /// This is used by the integration modules
    ///
    /// IMPORTANT: This returns the topology loaded from ValKey storage if set_topology_ref()
    /// was called during daemon initialization. Otherwise falls back to a default topology.
    pub fn get_topology_ref() -> Arc<RwLock<crate::GeometricTopology>> {
        // Try to get the topology from storage first (set during daemon init)
        if let Some(topology) = TOPOLOGY_STORAGE.get() {
            return Arc::clone(topology);
        }

        // Fallback: create a default topology (this should rarely happen)
        // Using OnceLock for thread-safe lazy initialization
        use crate::integration::thread_safety::ThreadSafeSingleton;
        static FALLBACK_TOPOLOGY: ThreadSafeSingleton<crate::GeometricTopology> = ThreadSafeSingleton::new();

        warn!("Using fallback topology - set_topology_ref() was not called during daemon initialization");
        FALLBACK_TOPOLOGY.get_or_init(|| crate::GeometricTopology::new(64))
    }

    /// Set the topology reference for static access
    /// This should be called during daemon initialization with the topology loaded from storage
    /// Can only be called once - subsequent calls will be ignored
    pub fn set_topology_ref(topology: Arc<RwLock<crate::GeometricTopology>>) {
        if TOPOLOGY_STORAGE.set(topology).is_err() {
            warn!("Topology reference already set, ignoring duplicate initialization");
        }
    }

    /// Set the topology namespace for static access (called once at daemon init)
    pub fn set_topology_namespace(ns: String) {
        if TOPOLOGY_NAMESPACE.set(ns).is_err() {
            warn!("Topology namespace already set, ignoring duplicate initialization");
        }
    }

    /// Get the topology namespace (defaults to "geodineum" if not set)
    pub fn get_topology_namespace() -> &'static str {
        TOPOLOGY_NAMESPACE.get().map(|s| s.as_str()).unwrap_or("geodineum")
    }

    /// Global derived-snapshot hash key: `{topology_namespace}:gnode:topology:services`.
    /// Passed to GNODE_REGISTER/DEREGISTER_CAPABILITY_VECTOR so every registration
    /// transport maintains the PHP-facing snapshot (field=entity_id, value={point,metadata}).
    pub fn topology_snapshot_key() -> String {
        format!("{{{}}}:gnode:topology:services", Self::get_topology_namespace())
    }

    /// Get format processor reference for static access (requires "format" feature)
    /// This is used by the integration modules for thread-safe access to the format processor
    /// Returns None if format system is not initialized or feature is disabled
    pub fn get_format_processor_ref() -> Option<Arc<crate::integration::processor::FormatProcessor>> {
        FORMAT_PROCESSOR_STORAGE.get().cloned()
    }

    /// Set format processor reference for static access (requires "format" feature)
    /// This is called during daemon initialization to make the processor available globally
    /// Can only be called once - subsequent calls will be ignored
    pub fn set_format_processor_ref(processor: Arc<crate::integration::processor::FormatProcessor>) {
        if FORMAT_PROCESSOR_STORAGE.set(processor).is_err() {
            warn!("Format processor reference already set, ignoring duplicate initialization");
        }
    }

    /// Get format processor if available (requires "format" feature)
    /// Returns None if format system is not initialized or feature is disabled
    pub fn get_format_processor(&self) -> Option<Arc<crate::integration::processor::FormatProcessor>> {
        match self.format_processor.lock() {
            Ok(guard) => guard.clone(),
            Err(e) => {
                error!("Failed to lock format_processor for reading: {}", e);
                None
            }
        }
    }

    /// Get load metrics manager reference for static access
    /// This is used by the integration modules for thread-safe access to load metrics
    /// Returns None if load manager is not initialized
    pub fn get_load_metrics_manager_ref() -> Option<Arc<crate::integration::load_metrics::LoadMetricsManager>> {
        LOAD_METRICS_MANAGER_STORAGE.get().cloned()
    }

    /// Set load metrics manager reference for static access
    /// This is called during daemon initialization to make the manager available globally
    /// Can only be called once - subsequent calls will be ignored
    pub fn set_load_metrics_manager_ref(manager: Arc<crate::integration::load_metrics::LoadMetricsManager>) {
        if LOAD_METRICS_MANAGER_STORAGE.set(manager).is_err() {
            warn!("Load metrics manager reference already set, ignoring duplicate initialization");
        }
    }
}

/// Register gNode daemon as a discoverable geo-node service
/// Uses semantic capability dimensions from NEW_PRACTICAL_TOPOLOGY.md
/// NOTE: topology_namespace is used instead of site_id because daemon is infrastructure, not a site
fn register_gnode_as_service(
    _topology_manager: &SharedTopology,  // Kept for API compatibility
    topology_namespace: &str,
    node_id: &str,
    debug_mode: bool
) -> Result<()> {

    // Service ID uses only node_id (daemon serves ALL sites via unified streams)
    // topology_namespace is kept in metadata for reference
    let service_id = format!("gnode-daemon-{}", node_id);

    // Define gNode capabilities using semantic dimensions (see NEW_PRACTICAL_TOPOLOGY.md)
    let mut capabilities: HashMap<String, f64> = HashMap::new();

    // LAYER 1: Interface Identity
    capabilities.insert("protocol".to_string(), 0.50);           // gnode_stream (native ValKey streams)
    capabilities.insert("native_format".to_string(), 0.20);      // json (primary format)
    capabilities.insert("api_version".to_string(), 0.10);        // v1.x
    capabilities.insert("contract_stability".to_string(), 0.75); // stable

    // LAYER 2: Access Control
    capabilities.insert("clearance_required".to_string(), 0.20); // authenticated
    capabilities.insert("auth_method".to_string(), 0.40);        // bearer_token (ACL-based)
    capabilities.insert("data_sensitivity".to_string(), 0.25);   // internal

    // LAYER 3: Service Scope
    capabilities.insert("service_scope".to_string(), 0.00);      // infrastructure (platform-level)

    // LAYER 4: Functional Domain
    capabilities.insert("domain_primary".to_string(), 0.05);     // platform (infrastructure services)
    capabilities.insert("domain_secondary".to_string(), 0.40);   // messaging (also acts as message broker)
    capabilities.insert("specialization".to_string(), 0.25);     // generalist (handles many functions)

    // LAYER 5: Performance Profile
    capabilities.insert("throughput_tier".to_string(), 0.75);    // high-throughput capability tier
    capabilities.insert("latency_class".to_string(), 0.25);      // interactive (<100ms p99)
    capabilities.insert("reliability_tier".to_string(), 0.50);   // high (99.9% target)

    // LAYER 6: Workflow Context
    capabilities.insert("pipeline_stage".to_string(), 0.40);     // process (central router)
    capabilities.insert("execution_priority".to_string(), 0.75); // high (critical infrastructure)

    // LAYER 7: Runtime State (initialized, will be updated by health stream)
    capabilities.insert("current_load".to_string(), 0.00);       // idle at startup

    // Define service metadata for gCore integration
    let mut metadata: HashMap<String, String> = HashMap::new();
    metadata.insert("type".to_string(), "gnode-daemon".to_string());
    metadata.insert("tier".to_string(), "ORCHESTRATOR".to_string());
    metadata.insert("tier_description".to_string(), "Central orchestrator daemon".to_string());
    metadata.insert("version".to_string(), env!("CARGO_PKG_VERSION").to_string());
    metadata.insert("startup_time".to_string(), crate::integration::processor::stream_utils::current_timestamp().to_string());
    metadata.insert("topology_namespace".to_string(), topology_namespace.to_string());  // Use topology_namespace, not site_id
    metadata.insert("node_id".to_string(), node_id.to_string());
    metadata.insert("functions_loaded".to_string(), "170".to_string());
    metadata.insert("api_version".to_string(), "1.0".to_string());
    metadata.insert("stream_discovery".to_string(), "dynamic".to_string());  // Daemon discovers sites dynamically

    // Format system integration metadata
    metadata.insert("native_format".to_string(), "json_v1".to_string());
    metadata.insert("accepts_formats".to_string(), "json_v1,msgpack_v1,resp3".to_string());
    metadata.insert("output_format".to_string(), "json_v1".to_string());

    // ========================================================================
    // STATELESS ARCHITECTURE: Register daemon via FCALL to ValKey
    // ========================================================================
    use crate::geometric_precision::{FixedVector, FixedPoint};
    use crate::GeometricTopology;

    // Get a synchronous connection for startup registration
    let mut conn = match crate::integration::connection_manager::get_connection() {
        Ok(c) => c,
        Err(e) => {
            warn!("Failed to get connection for daemon registration: {:?}", e);
            return Ok(()); // Non-fatal, continue startup
        }
    };

    // Build the service-tier Q64.64 capability vector.
    // Dim count comes from the canonical service-tier schema (TOTAL_DIMENSIONS = 30
    // = 25 discovery + 5 storage; see daemon/config/service_schema.yaml).
    use crate::integration::handlers::TOTAL_DIMENSIONS;
    let mut point = FixedVector::new(TOTAL_DIMENSIONS);
    let dims = crate::integration::command_handler::get_service_dimensions();

    for (cap_name, cap_value) in &capabilities {
        if let Some(&dim_idx) = dims.get(cap_name) {
            if dim_idx < TOTAL_DIMENSIONS {
                let clamped = (*cap_value).clamp(0.0, 1.0);
                point[dim_idx] = FixedPoint::from_f64(clamped);
            }
        }
    }

    // Compute bucket key from the discovery slice (DISCOVERY_DIMENSIONS = 25 for
    // service tier) using Q64.64 arithmetic. Storage-only dims (25-29) are excluded
    // from the bucket key.
    let disc_point = crate::integration::command_handler::discovery_point_from_full(&point);
    let bucket_key = GeometricTopology::point_to_bucket_key(&disc_point, 10);

    // Compute z_score (dimension 16: current_load) for ZADD ordering
    let z_score = GeometricTopology::compute_service_z_score(&point);

    // Build point_raw (Q64.64 i128 values) for storage
    let point_raw: Vec<String> = (0..point.len())
        .map(|i| point[i].raw().to_string())
        .collect();

    // Build point_display (3 decimal floats) for human readability
    let point_display: Vec<f64> = (0..point.len())
        .map(|i| (point[i].to_f64() * 1000.0).round() / 1000.0)
        .collect();

    // Build entity JSON with abbreviated fields
    let entity_json = serde_json::json!({
        "pr": point_raw,        // point_raw: Q64.64 i128 (authoritative)
        "pd": point_display,    // point_display: 3 decimal floats
        "c": capabilities,      // original capabilities for reference
        "m": metadata           // service metadata
    });

    // Use topology_namespace as "site_id" for services topology
    // (daemon is infrastructure, registers in namespace topology)
    let topology_key = GeometricTopology::get_services_topology_key(topology_namespace);

    if debug_mode {
        debug!("Daemon registration: service_id={}, topology_key={}", service_id, topology_key);
        debug!("Daemon bucket_key: {} (68 chars)", bucket_key);
        debug!("Daemon z_score: {}", z_score);
    }

    // Ensure services topology exists for this namespace
    let ensure_result: redis::RedisResult<String> = redis::cmd("FCALL")
        .arg("GNODE_ENSURE_TOPOLOGY")
        .arg(1)
        .arg(topology_namespace)
        .query(&mut conn);

    if let Err(e) = ensure_result {
        warn!("Failed to ensure services topology for daemon: {:?}", e);
        return Ok(()); // Non-fatal
    }

    // Register daemon as entity via FCALL
    let entity_json_str = entity_json.to_string();
    let register_result: redis::RedisResult<String> = redis::cmd("FCALL")
        .arg("GNODE_REGISTER_CAPABILITY_VECTOR")
        .arg(1)
        .arg(&topology_key)
        .arg(&service_id)
        .arg(&entity_json_str)
        .arg(&bucket_key)
        .arg(z_score)
        .arg(GNodeDaemon::topology_snapshot_key())  // args[5]: maintain (B) snapshot
        .query(&mut conn);

    match register_result {
        Ok(result_json) => {
            // Parse response to check if update or new registration
            if let Ok(result) = serde_json::from_str::<serde_json::Value>(&result_json) {
                let is_update = result.get("upd").and_then(|v| v.as_bool()).unwrap_or(false);
                if is_update {
                    info!("Daemon {} re-registered in services topology (updated)", service_id);
                } else {
                    info!("Daemon {} registered in services topology (new)", service_id);
                }
            } else {
                info!("Daemon {} registered in services topology", service_id);
            }
        },
        Err(e) => {
            warn!("Failed to register daemon in services topology: {:?}", e);
            // Non-fatal, continue startup
        }
    }

    Ok(())
}

impl GNodeDaemon {
    /// Run the daemon
    pub fn run(&self) -> Result<()> {
        info!("Starting gNode daemon");

        // ValKey function libraries are a namespace shared by every node on
        // this ValKey, so exactly one node owns writing them: the master. A
        // constellation worker verifies read-only — its own tree may differ in
        // version from the master's, and silently replacing the libraries all
        // live sites call is never the right outcome of a worker restart.
        let valkey_initialized = if self.is_master {
            info!("Initializing ValKey functions (master owns the library namespace)");

            // Probe function support. This writes, which is why it is inside
            // the master branch.
            let mut valkey_support_verified = false;
            match self.client.get_connection() {
                Ok(mut conn) => {
                    let test_function = "#!lua name=test_gnode\nserver.register_function('TEST_PING', function() return 'PONG' end)";
                    let test_result: redis::RedisResult<String> = redis::cmd("FUNCTION")
                        .arg("LOAD")
                        .arg(test_function)
                        .query(&mut conn);

                    match test_result {
                        Ok(_) => {
                            info!("ValKey functions support verified");
                            valkey_support_verified = true;

                            let _: redis::RedisResult<()> = redis::cmd("FUNCTION")
                                .arg("DELETE")
                                .arg("test_gnode")
                                .query(&mut conn);
                        },
                        Err(e) => {
                            error!("ValKey does not support functions: {}. Check your ValKey version.", e);
                        }
                    }
                },
                Err(e) => {
                    error!("Failed to connect to ValKey to verify function support: {}", e);
                }
            }

            if valkey_support_verified {
                // Use topology_namespace for function loading (daemon-level, not site-specific)
                match crate::integration::valkey_functions::sync_functions(
                    &self.client,
                    &self.topology_namespace,  // Daemon uses topology namespace, not site_id
                    self.debug
                ) {
                    Ok(count) => {
                        if count > 0 {
                            info!("ValKey functions initialized: {} functions loaded", count);
                            true
                        } else {
                            warn!("No ValKey functions were loaded. If this is unexpected, check the function files and run scripts/load-valkey-functions.sh manually to diagnose.");
                            false
                        }
                    },
                    Err(e) => {
                        error!("ValKey functions failed to initialize: {}. Will use fallbacks.", e);
                        error!("For a one-time fix, run: scripts/load-valkey-functions.sh");
                        false
                    }
                }
            } else {
                warn!("Skipping ValKey function loading due to lack of function support");
                false
            }
        } else {
            info!("Verifying ValKey functions (worker — the master owns the library namespace)");
            match crate::integration::valkey_functions::verify_functions(&self.client, self.debug) {
                Ok(missing) if missing.is_empty() => {
                    info!("ValKey function libraries verified");
                    true
                },
                Ok(missing) => {
                    error!(
                        "ValKey is missing {} function librar{} this node needs: {}",
                        missing.len(),
                        if missing.len() == 1 { "y" } else { "ies" },
                        missing.join(", ")
                    );
                    error!("Load them ON THE MASTER: scripts/load-valkey-functions.sh");
                    false
                },
                Err(e) => {
                    error!("Could not verify ValKey functions: {}. Will use fallbacks.", e);
                    false
                }
            }
        };

        info!("Function initialization complete - ValKey: {}",
            if valkey_initialized { "OK" } else { "FAILED" });

        // Custom format definitions are restored from ValKey by the native
        // FormatProcessor after it is initialized (see "Initialize format
        // system" below). Built-in formats self-register on registry init, so
        // nothing format-related needs to happen here anymore.
        if valkey_initialized {
            if let Ok(mut conn) = self.client.get_connection() {
                // Ensure this node's receipt signing key exists (generated on
                // first start, private key stays on this node) and publish its
                // PUBLIC key so verifiers can resolve receipt signers. Fully
                // non-fatal: receipts are not emitted yet, so a key/permission
                // hiccup must never affect startup — log and carry on.
                let signer_path = crate::integration::receipt::default_signer_path();
                match crate::integration::receipt::load_or_generate_signer(&signer_path) {
                    Ok(signer) => {
                        match crate::integration::receipt::publish_pubkey(
                            &mut conn, &signer, &self.topology_namespace,
                        ) {
                            Ok(()) => info!(
                                "Receipt signer ready: {} ({}), pubkey published to {{{}}}:gnode:receipt_pubkeys",
                                signer.signer_id(), signer.alg_id(), self.topology_namespace
                            ),
                            // Publication is idempotent-per-boot; a transient
                            // failure leaves an earlier boot's pubkey resolvable,
                            // so emission stays enabled either way.
                            Err(e) => warn!("Receipt pubkey publish failed (non-fatal): {}", e),
                        }
                        crate::integration::receipt::init_receipt_context(
                            signer,
                            self.node_id.clone(),
                            self.environment.clone(),
                        );
                    }
                    Err(e) => warn!(
                        "Receipt signer unavailable at {} (non-fatal; receipts will be unsigned when wired): {}",
                        signer_path.display(), e
                    ),
                }

                // Publish gNode's own stream contracts + config_schema to ValKey
                // for runtime discovery by agents and other components.
                // Uses the topology_namespace as the site_id since gNode is
                // infrastructure, not a per-site service.
                //
                // Commit 0.5.c (GN-D3.09): replaces the former silent-debug! noop
                // under systemd CWD with env-var resolution (GNODE_SCHEMAS_DIR
                // set by installer phase_config) + compile-time CARGO_MANIFEST_DIR
                // fallback for dev-box `cargo run`. Hard-errors if neither
                // resolves to a real directory.
                let env_for_schemas = std::env::var("GNODE_ENVIRONMENT")
                    .unwrap_or_else(|_| "production".to_string());

                // Stream contracts live at <repo-root>/config/schemas/, one
                // level above the daemon crate. CARGO_MANIFEST_DIR points at
                // the daemon crate, so the dev fallback reaches up one level.
                let schemas_dir = std::env::var("GNODE_SCHEMAS_DIR").unwrap_or_else(
                    |_| concat!(env!("CARGO_MANIFEST_DIR"), "/../config/schemas").to_string(),
                );
                if !std::path::Path::new(&schemas_dir).is_dir() {
                    error!(
                        "GNODE_SCHEMAS_DIR does not resolve to a directory: {} \
                         (set GNODE_SCHEMAS_DIR in bootstrap or install a tree \
                         containing config/schemas/)",
                        schemas_dir
                    );
                    return Err(crate::GeometricError::Other(format!(
                        "schemas dir not found: {}",
                        schemas_dir
                    )));
                }
                let count = geodineum_schema::publish_sync(
                    &schemas_dir,
                    &mut conn,
                    &self.topology_namespace,
                    &env_for_schemas,
                );
                info!("gNode stream contracts published: {} schemas", count);

                // Publish config_schema (Commit 0.5) — enumerates every
                // operator-visible GNODE_* key the daemon reads.
                // Keyed at geodineum:config_schema:gnode (HSET) and indexed
                // via SADD geodineum:config_schema:_index gnode.
                let config_schema_path = std::env::var("GNODE_CONFIG_SCHEMA_FILE").unwrap_or_else(
                    |_| concat!(env!("CARGO_MANIFEST_DIR"), "/config/config_schema.yaml").to_string(),
                );
                if !std::path::Path::new(&config_schema_path).is_file() {
                    error!(
                        "GNODE_CONFIG_SCHEMA_FILE does not resolve to a file: {}",
                        config_schema_path
                    );
                    return Err(crate::GeometricError::Other(format!(
                        "config_schema file not found: {}",
                        config_schema_path
                    )));
                }
                match geodineum_schema::publish_config_schema_sync(
                    std::path::Path::new(&config_schema_path),
                    &mut conn,
                ) {
                    Ok(n) => info!("gNode config_schema published: {} entries", n),
                    Err(e) => {
                        error!("failed to publish gNode config_schema: {}", e);
                        return Err(crate::GeometricError::Other(e.to_string()));
                    }
                }

                // Commit 2.9 (Decision-C follow-through): publish each
                // extension's config_schema.yaml under its own component
                // namespace so the wp-admin schema consumer's
                // SMEMBERS geodineum:config_schema:_index iteration
                // surfaces every operator-visible key from CMS / BROKER /
                // OBSERVE / SIGNALS / TOPO alongside gNode's own.
                //
                // Discovery is OPT-IN via GEODINEUM_EXT_SCHEMAS_DIR so a
                // Ch.1 deploy without Pro extensions doesn't log
                // misleading "extension dir missing" errors. The installer
                // sets it during phase_config when pro/gNode/ exists.
                //
                // Per-extension publish failures are logged but do NOT
                // abort daemon startup — gNode's own schema is the load-
                // bearing one and is already verified above. An extension
                // that fails to publish degrades gracefully (its keys
                // simply don't appear in wp-admin's enumeration).
                if let Ok(ext_schemas_dir) = std::env::var("GEODINEUM_EXT_SCHEMAS_DIR") {
                    let ext_root = std::path::Path::new(&ext_schemas_dir);
                    if !ext_root.is_dir() {
                        warn!(
                            "GEODINEUM_EXT_SCHEMAS_DIR is set but not a directory: {}",
                            ext_schemas_dir
                        );
                    } else {
                        match std::fs::read_dir(ext_root) {
                            Ok(entries) => {
                                let mut published = 0u32;
                                let mut skipped = 0u32;
                                for entry in entries.flatten() {
                                    let ext_dir = entry.path();
                                    if !ext_dir.is_dir() {
                                        continue;
                                    }
                                    let schema_path = ext_dir.join("config_schema.yaml");
                                    if !schema_path.is_file() {
                                        skipped += 1;
                                        continue;
                                    }
                                    match geodineum_schema::publish_config_schema_sync(
                                        &schema_path,
                                        &mut conn,
                                    ) {
                                        Ok(n) => {
                                            info!(
                                                "extension config_schema published: {} ({} entries)",
                                                ext_dir
                                                    .file_name()
                                                    .map(|s| s.to_string_lossy().into_owned())
                                                    .unwrap_or_default(),
                                                n
                                            );
                                            published += 1;
                                        }
                                        Err(e) => {
                                            warn!(
                                                "failed to publish extension config_schema at {}: {}",
                                                schema_path.display(),
                                                e
                                            );
                                        }
                                    }
                                }
                                info!(
                                    "extension config_schema discovery: {} published, {} dirs without config_schema.yaml",
                                    published, skipped
                                );
                            }
                            Err(e) => warn!(
                                "cannot enumerate GEODINEUM_EXT_SCHEMAS_DIR {}: {}",
                                ext_schemas_dir, e
                            ),
                        }
                    }
                }
            }
        }

        // Initialize the connection manager with proper connection pooling
        let connection_config = crate::integration::connection_manager::ConnectionConfig {
            max_retries: 3,
            base_backoff_ms: 100,
            max_backoff_ms: 2000,
            connection_timeout_ms: 5000,
            max_pool_size: (self.get_thread_count() * 2) as u32, // Set pool size based on thread count
            min_idle_connections: 2,
            connection_idle_timeout_secs: 300, // 5 minutes
        };
        
        crate::integration::connection_manager::initialize_connection_manager(
            self.client.clone(), 
            Some(connection_config)
        ).map_err(|e| GeometricError::Other(format!("Failed to initialize connection manager: {}", e)))?;
        
        // Log initial pool status
        if let Err(e) = crate::integration::connection_manager::log_pool_status() {
            warn!("Failed to log connection pool status: {}", e);
        }
        
        // Initialize command handler registry
        info!("Initializing command handler registry");
        crate::integration::command_handler::initialize_command_registry();
        let registry = crate::integration::command_handler::get_command_registry();
        let command_count = registry.get_command_names().len();
        info!("Command handler registry initialized with {} commands", command_count);

        // ========================================
        // Dynamic Stream Discovery (Phase 2)
        // Discover registered sites and their streams
        // ========================================
        info!("🔍 Initializing dynamic stream discovery...");
        {
            // Get a connection for discovery
            match crate::integration::connection_manager::get_connection() {
                Ok(mut conn) => {
                    // Acquire write lock on stream discovery
                    match self.stream_discovery.write() {
                        Ok(discovery) => {
                            // Discover registered sites
                            match discovery.discover_sites(&mut conn) {
                                Ok(sites) => {
                                    debug!("  Discovered {} registered sites", sites.len());
                                    for site in &sites {
                                        debug!("    - {} (status: {}, streams: {})", site.id, site.status, site.stream_count);
                                    }
                                },
                                Err(e) => {
                                    warn!("  Failed to discover sites: {:?}", e);
                                    warn!("  Will use topology_namespace as fallback for streams");
                                }
                            }

                            // Discover streams for this environment
                            match discovery.discover_streams(&mut conn) {
                                Ok(streams) => {
                                    debug!("  Discovered {} streams for environment '{}'", streams.len(), self.environment);
                                    let unified_count = streams.iter().filter(|s| s.stream_type == "unified").count();
                                    let health_count = streams.iter().filter(|s| s.stream_type == "health").count();
                                    let broadcast_count = streams.iter().filter(|s| s.stream_type == "broadcast").count();
                                    debug!("    - Unified: {}, Health: {}, Broadcast: {}", unified_count, health_count, broadcast_count);
                                },
                                Err(e) => {
                                    warn!("  Failed to discover streams: {:?}", e);
                                    warn!("  Will use topology_namespace as fallback for stream key construction");
                                }
                            }
                        },
                        Err(e) => {
                            warn!("  Failed to acquire stream discovery lock: {}", e);
                        }
                    }
                },
                Err(e) => {
                    warn!("  Failed to get connection for stream discovery: {:?}", e);
                    warn!("  Will retry periodically.");
                }
            }
        }
        info!("✅ Stream discovery initialized");

        // Initialize routing configuration for message filtering
        // Master nodes (--master flag or --node-id=master) load from YAML files and store to ValKey
        // Remote nodes fetch their routing config from ValKey
        // NOTE: Routing config is extracted from node config files (daemon/config/nodes/*.yaml)
        // to avoid duplication - nodes/*.yaml is the single source of truth
        info!("Initializing routing configuration for node_type '{}' (master: {})", self.node_type, self.is_master);
        let routing_config_dir = if self.is_master {
            // Master node: use nodes config directory (routing is subset of node config)
            // Uses fallback chain: explicit -> env var -> /etc/geodineum -> daemon/config/nodes
            resolve_node_config_dir(self.node_config_dir.as_deref())
        } else {
            None
        };

        // Get a connection and initialize routing configs
        let routing_result = crate::integration::connection_manager::get_connection()
            .map_err(|e| e.to_string())
            .and_then(|mut conn| {
                crate::routing_config::initialize_routing_configs(
                    &mut conn,
                    routing_config_dir.as_deref(),
                    self.is_master,
                    &self.node_type.to_string()
                )
            });

        match routing_result {
            Ok(routing_config) => {
                info!("Routing config loaded for node_type '{}': mode={:?}, hints={:?}",
                    self.node_type, routing_config.routing.mode, routing_config.routing.group_hints);
                if self.is_master {
                    info!("Master node stored routing configs to ValKey for remote nodes");
                } else {
                    info!("Remote node fetched routing config from ValKey");
                }
            },
            Err(e) => {
                warn!("Failed to initialize routing config: {}. Using defaults.", e);
                // Cache default config so stream workers can still function
                let default_config = match self.node_type.to_string().as_str() {
                    "inference" => crate::routing_config::RoutingConfig::default_inference(),
                    "all" => crate::routing_config::RoutingConfig::default_all(),
                    _ => crate::routing_config::RoutingConfig::default_general(),
                };
                crate::routing_config::cache_routing_config(default_config);
            }
        }

        // Initialize node configuration and register this node
        info!("Initializing node configuration for type '{}'", self.node_type);
        let node_config_dir = if self.is_master {
            // Uses fallback chain: explicit -> env var -> /etc/geodineum -> daemon/config/nodes
            resolve_node_config_dir(self.node_config_dir.as_deref())
        } else {
            None
        };

        let node_config_result = crate::integration::connection_manager::get_connection()
            .map_err(|e| e.to_string())
            .and_then(|mut conn| {
                crate::node_config::initialize_node_configs(
                    &mut conn,
                    node_config_dir.as_deref(),
                    self.is_master,
                    &self.node_type.to_string()
                )
            });

        match &node_config_result {
            Ok(node_config) => {
                info!("Node config loaded for type '{}': batch_size={}, report_interval={}ms",
                    self.node_type,
                    node_config.performance.batch_size.initial,
                    node_config.health.report_interval_ms);
                if self.is_master {
                    info!("Master node stored node configs to ValKey for remote nodes");
                }
            },
            Err(e) => {
                warn!("Failed to initialize node config: {}. Using defaults.", e);
            }
        }

        // Register this node instance in ValKey topology (with idempotency check)
        info!("Checking/registering node '{}' (type: {}) in topology", self.node_id, self.node_type);
        let hostname = std::env::var("HOSTNAME")
            .or_else(|_| hostname::get().map(|h| h.to_string_lossy().to_string()))
            .unwrap_or_else(|_| self.node_id.clone());

        // Try to get the IP address
        let ip_address = std::env::var("GNODE_IP_ADDRESS")
            .unwrap_or_else(|_| "127.0.0.1".to_string());

        let register_result = crate::integration::connection_manager::get_connection()
            .map_err(|e| e.to_string())
            .and_then(|mut conn| {
                let node_config = node_config_result.clone()
                    .unwrap_or_else(|_| crate::node_config::get_cached_node_config(&self.node_type.to_string()));

                // Use idempotent registration: checks if exists first, then registers or heartbeats
                // Use topology_namespace for node registration (daemon identity, not site-specific)
                crate::node_config::register_node_with_idempotency(
                    &mut conn,
                    &self.node_id,
                    &self.node_type.to_string(),
                    &self.topology_namespace,  // Daemon uses topology namespace, not site_id
                    &hostname,
                    &ip_address,
                    &node_config,
                )
            });

        match register_result {
            Ok(true) => {
                info!("✅ Node '{}' newly registered in topology", self.node_id);
            },
            Ok(false) => {
                info!("✅ Node '{}' already registered, heartbeat sent", self.node_id);
            },
            Err(e) => {
                warn!("Failed to register node in topology: {}. Node may not be visible to other nodes.", e);
            }
        }

        // Initialize template + format system (CMS extension)
        let template_dir = "daemon/src/template".to_string();
        {
            info!("Initializing template system");
            match crate::template::initialize_templates(&template_dir) {
                Ok((_template_manager, _template_engine)) => {
                    info!("Template system initialized successfully");
                },
                Err(e) => {
                    warn!("Failed to initialize template system: {}", e);
                }
            }
        }

        // Initialize format system (CMS extension)
        {
            info!("Initializing format system");
            match crate::template::initialize_formats(&template_dir) {
                Ok(format_registry) => {
                    info!("Format system initialized successfully");

                    // Create format processor and store in daemon struct
                    let format_processor = Arc::new(crate::integration::processor::FormatProcessor::new(format_registry));

                    // Store in the daemon's format_processor field using Mutex for interior mutability
                    match self.format_processor.lock() {
                        Ok(mut guard) => {
                            *guard = Some(format_processor.clone());
                            info!("Format processor created and stored successfully");
                        },
                        Err(e) => {
                            error!("Failed to lock format_processor for storage: {}", e);
                        }
                    }

                    // Also store in static singleton for global access
                    GNodeDaemon::set_format_processor_ref(format_processor.clone());
                    info!("Format processor registered for global access");

                    // Restore custom formats from ValKey (replaces the former
                    // GNODE_LOAD_FORMATS/GNODE_PERSIST_FORMATS Lua FCALLs).
                    // Built-ins already self-registered above; only customs
                    // need rehydrating. Stateless invariant: durable format
                    // state lives in ValKey, never on the daemon's disk.
                    match self.client.get_connection() {
                        Ok(mut conn) => match format_processor.hydrate_formats(&mut conn, &self.topology_namespace) {
                            Ok(n) => info!("Format hydration from ValKey: {} custom format(s) restored", n),
                            Err(e) => warn!("Format hydration from ValKey failed: {}", e),
                        },
                        Err(e) => warn!("Format hydration skipped, no ValKey connection: {}", e),
                    }
                },
                Err(e) => {
                    warn!("Failed to initialize format system: {}", e);
                }
            }
        }
        // Removed cfg(not(feature="cms")) "skipped" log — the cms
        // feature no longer exists; template/format always initialize.

        // Create LoadMetricsManager for health stream
        info!("Initializing LoadMetricsManager for health stream");
        let load_manager = Arc::new(crate::integration::load_metrics::LoadMetricsManager::new(30)); // 30s TTL
        info!("LoadMetricsManager initialized with 30s TTL");

        // Store in static singleton for global access
        GNodeDaemon::set_load_metrics_manager_ref(Arc::clone(&load_manager));
        info!("LoadMetricsManager registered for global access");

        // Spawn cleanup task for stale metrics
        // Store JoinHandle and check shutdown flag
        // Single-threaded mode: Worker registered later in WorkerRegistry
        if !self.single_threaded {
            let load_manager_cleanup = Arc::clone(&load_manager);
            let health_cleanup_handle = std::thread::spawn(move || {
                info!("Health metrics cleanup task started (10s interval)");
                while !is_shutdown_requested() {
                    // Sleep in 1-second intervals to check shutdown flag more frequently
                    for _ in 0..10 {
                        if is_shutdown_requested() { break; }
                        std::thread::sleep(Duration::from_secs(1));
                    }
                    if is_shutdown_requested() { break; }

                    let now = crate::utils::current_timestamp_ms();
                    let removed = load_manager_cleanup.cleanup_stale(now);
                    if removed > 0 {
                        debug!("Cleaned up {} stale health metrics", removed);
                    }
                }
                info!("Health metrics cleanup task shutting down");
            });

            // Store handle for graceful shutdown
            if let Ok(mut handles) = self.worker_handles.lock() {
                handles.push(health_cleanup_handle);
            }
        } else {
            debug!("Health cleanup worker will be registered in WorkerRegistry (single-threaded mode)");
        }

        // Register gNode daemon as geo-node service after all systems are initialized
        // Use topology_namespace for registration (daemon is infrastructure, not a site)
        info!("Registering gNode daemon as geo-node service");
        if let Err(e) = register_gnode_as_service(&self.topology_manager, &self.topology_namespace, &self.node_id, self.debug) {
            error!("Failed to register gNode as service: {:?}", e);
            // Continue startup but log the failure - non-critical for basic operation
        }

        // Initialize broadcast stream (global, not per-node)
        // Use topology_namespace for broadcast (daemon-level, shared across all sites)
        info!("Initializing global broadcast stream");
        let broadcast_init_result = crate::integration::connection_manager::with_connection(|conn| {
            crate::integration::processor::initialize_broadcast_stream(
                conn,
                &self.topology_namespace,  // Use topology_namespace for global broadcast
                &self.stream_prefix,
                self.debug
            )
        });

        match broadcast_init_result {
            Ok(broadcast_stream_key) => {
                info!("Broadcast stream initialized: {}", broadcast_stream_key);

                // Spawn broadcast reader worker thread (multi-threaded mode only)
                // Use topology_namespace for broadcast reader (daemon-level, shared across all sites)
                // Note: In single-threaded mode, broadcast reading is limited (requires async-capable worker)
                if self.single_threaded {
                    info!("Single-threaded mode: broadcast reader will run with reduced frequency in main loop");
                }

                if !self.single_threaded {
                    let topology_namespace_bc = self.topology_namespace.clone();
                    let stream_prefix_bc = self.stream_prefix.clone();
                    let debug_bc = self.debug;
                    let stream_discovery_bc = self.stream_discovery.clone();  // For environment_changed handling

                    // Store JoinHandle and check shutdown flag
                    let broadcast_handle = std::thread::spawn(move || {
                    use crate::integration::processor::BroadcastReader;

                    info!("Broadcast reader worker thread started");

                    // Create broadcast reader (start from latest messages)
                    // Use topology_namespace for global broadcast stream
                    let broadcast_key = crate::integration::processor::get_broadcast_stream(&topology_namespace_bc, &stream_prefix_bc);
                    let mut reader = BroadcastReader::new(broadcast_key, topology_namespace_bc.clone(), false);

                    while !is_shutdown_requested() {
                        match crate::integration::connection_manager::get_connection() {
                            Ok(mut conn) => {
                                // Read broadcast messages (block for 5 seconds)
                                match reader.read_messages(&mut conn, 100, 5000, debug_bc) {
                                    Ok(messages) => {
                                        if !messages.is_empty() {
                                            info!("Received {} broadcast messages", messages.len());

                                            // Process each broadcast message
                                            for msg in messages {
                                                if debug_bc {
                                                    debug!("Broadcast message type: {}, ID: {}",
                                                           msg.message_type, msg.id);
                                                }

                                                // Route by message type
                                                match msg.message_type.as_str() {
                                                    "topology_update" => {
                                                        info!("Received topology update broadcast - refreshing topology from ValKey");
                                                        // Refresh topology from ValKey using the shared topology namespace
                                                        let topology_key = format!("{{{}}}:{}:topology", topology_namespace_bc, stream_prefix_bc);
                                                        if let Ok(Some(json_str)) = redis::cmd("GET").arg(&topology_key).query::<Option<String>>(&mut conn) {
                                                            if let Ok(mut new_topology) = crate::GeometricTopology::from_json(&json_str) {
                                                                // Rebuild spatial hash if needed
                                                                if new_topology.spatial_hash.is_none() ||
                                                                   new_topology.spatial_hash.as_ref().map(|h| h.buckets.is_empty()).unwrap_or(true) {
                                                                    new_topology.rebuild_spatial_hash();
                                                                }
                                                                let service_count = new_topology.services.len();
                                                                // Update in-memory topology
                                                                let topology_ref = GNodeDaemon::get_topology_ref();
                                                                let update_result = topology_ref.write().map(|mut t| {
                                                                    *t = new_topology;
                                                                });
                                                                drop(update_result);
                                                                info!("Topology refreshed successfully ({} services)", service_count);
                                                            } else {
                                                                warn!("Failed to parse topology JSON from ValKey");
                                                            }
                                                        }
                                                    },
                                                    "service_registered" => {
                                                        if let Some(service_id) = msg.fields.get("service_id") {
                                                            info!("Service registered broadcast: {}", service_id);
                                                        }
                                                    },
                                                    "service_deregistered" => {
                                                        if let Some(service_id) = msg.fields.get("service_id") {
                                                            info!("Service deregistered broadcast: {}", service_id);
                                                        }
                                                    },
                                                    "format_registered" => {
                                                        if let Some(format_name) = msg.fields.get("format_name") {
                                                            info!("Format registered broadcast: {} - loading from ValKey", format_name);
                                                            let format_key = format!("format:{}:definition", format_name);
                                                            match redis::cmd("GET").arg(&format_key).query::<Option<String>>(&mut conn) {
                                                                Ok(Some(format_json)) => {
                                                                    if let Some(format_processor) = GNodeDaemon::get_format_processor_ref() {
                                                                        match serde_json::from_str::<serde_json::Value>(&format_json) {
                                                                            Ok(format_value) => {
                                                                                if let Err(e) = format_processor.get_registry().register_format(&format_value) {
                                                                                    warn!("Failed to register format {}: {:?}", format_name, e);
                                                                                } else {
                                                                                    info!("Format {} registered successfully", format_name);
                                                                                    // Mirror into the native ValKey scheme
                                                                                    // ({ns}:gnode:format:*) so startup hydration
                                                                                    // restores it without the retired Lua path.
                                                                                    if let Err(e) = format_processor.persist_format(&mut conn, &topology_namespace_bc, &format_value) {
                                                                                        warn!("Failed to mirror format {} to ValKey: {}", format_name, e);
                                                                                    }
                                                                                }
                                                                            },
                                                                            Err(e) => warn!("Failed to parse format JSON for {}: {}", format_name, e),
                                                                        }
                                                                    }
                                                                },
                                                                Ok(None) => debug!("No format definition found for {}", format_name),
                                                                Err(e) => warn!("Failed to load format {} from ValKey: {}", format_name, e),
                                                            }
                                                        }
                                                        // The cms Cargo feature is retired.
                                                    },
                                                    "global_announcement" => {
                                                        if let Some(message) = msg.fields.get("msg") {
                                                            info!("Global announcement: {}", message);
                                                        }
                                                    },
                                                    "environment_changed" => {
                                                        // Site environment changed - trigger immediate stream refresh
                                                        let data = msg.fields.get("data")
                                                            .and_then(|d| serde_json::from_str::<serde_json::Value>(d).ok());

                                                        if let Some(data) = data {
                                                            let site_id = data.get("site_id").and_then(|v| v.as_str()).unwrap_or("unknown");
                                                            let old_env = data.get("old_environment").and_then(|v| v.as_str()).unwrap_or("?");
                                                            let new_env = data.get("new_environment").and_then(|v| v.as_str()).unwrap_or("?");

                                                            info!("Site {} environment changed: {} → {} - triggering immediate stream refresh",
                                                                  site_id, old_env, new_env);

                                                            // Refresh stream discovery immediately and signal workers
                                                            match stream_discovery_bc.read() {
                                                                Ok(discovery) => {
                                                                    if let Err(e) = discovery.refresh(&mut conn) {
                                                                        warn!("Failed to refresh streams after environment change: {:?}", e);
                                                                    } else {
                                                                        // Signal workers to sync immediately
                                                                        discovery.signal_immediate_sync();
                                                                        info!("Stream discovery refreshed and workers notified");
                                                                    }
                                                                },
                                                                Err(e) => {
                                                                    warn!("Failed to acquire stream_discovery lock: {}", e);
                                                                }
                                                            }
                                                        } else {
                                                            warn!("environment_changed broadcast missing data field");
                                                        }
                                                    },
                                                    _ => {
                                                        if debug_bc {
                                                            debug!("Unknown broadcast message type: {}", msg.message_type);
                                                        }
                                                    }
                                                }
                                            }
                                        }

                                        // Periodically trim old broadcast messages (every 100 reads)
                                        static TRIM_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
                                        if TRIM_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed).is_multiple_of(100) {
                                            let stream_key = crate::integration::processor::get_broadcast_stream(&topology_namespace_bc, &stream_prefix_bc);
                                            if let Err(e) = crate::integration::processor::trim_broadcast_stream(
                                                &mut conn,
                                                &stream_key,
                                                300, // 5 minute retention
                                                debug_bc
                                            ) {
                                                warn!("Failed to trim broadcast stream: {:?}", e);
                                            }
                                        }
                                    },
                                    Err(e) => {
                                        warn!("Error reading broadcast messages: {:?}", e);
                                        std::thread::sleep(Duration::from_secs(5));
                                    }
                                }
                            },
                            Err(e) => {
                                error!("Failed to get connection for broadcast reader: {:?}", e);
                                std::thread::sleep(Duration::from_secs(5));
                            }
                        }
                    }
                    info!("Broadcast reader worker thread shutting down");
                });

                    // Store handle for graceful shutdown
                    if let Ok(mut handles) = self.worker_handles.lock() {
                        handles.push(broadcast_handle);
                    }

                    info!("Broadcast reader worker thread spawned successfully");
                } // end if !self.single_threaded
            },
            Err(e) => {
                warn!("Failed to initialize broadcast stream: {:?}", e);
                warn!("Broadcast functionality will not be available");
            }
        }

        // ========================================
        // Initialization Summary
        // Clearly distinguish REQUIRED vs OPTIONAL components
        // ========================================
        info!("═══════════════════════════════════════════════════════════════");
        info!("                    INITIALIZATION SUMMARY                      ");
        info!("═══════════════════════════════════════════════════════════════");
        info!("REQUIRED components (daemon cannot start without these):");
        info!("  ✓ Connection manager: initialized (pool size: {})", (self.get_thread_count() * 2));
        info!("  ✓ Command registry: {} commands registered", command_count);
        info!("");
        info!("OPTIONAL components (fallbacks available if these fail):");
        info!("  {} ValKey functions: {}",
            if valkey_initialized { "✓" } else { "⚠" },
            if valkey_initialized { "loaded" } else { "using direct Redis commands" });
        info!("  ✓ Stream discovery: initialized (refresh interval: 60s)");
        info!("  ✓ Routing config: loaded for node_type '{}'", self.node_type);
        info!("  ✓ Node registration: node '{}' registered in topology", self.node_id);
        // Extensions loaded via signed-extension pipeline at build
        // time; runtime detection comes from CommandHandlerRegistry's
        // registered commands. The "compiled-in CMS" status line was
        // misleading for this model (always-true at build time +
        // verify_and_stage decides what's actually staged).
        info!("  ✓ Load metrics manager: initialized (TTL: 30s)");
        info!("  ✓ Broadcast stream: initialized");
        info!("═══════════════════════════════════════════════════════════════");

        // ========================================
        // Fast lane initialization (lane-aware dispatch)
        // ========================================
        // Initialize ONCE before any consumer worker spawns. The Fast
        // lane owns a shared tokio runtime + redis::Client used to
        // dispatch Lane::Fast commands asynchronously, so consumer
        // workers don't block waiting for each handler to finish.
        // See integration/fast_lane.rs for the design rationale.
        //
        // Worker count: 2 is sufficient for typical loads. Each worker
        // can hold many concurrent async tasks (Fast lane handlers are
        // mostly I/O-bound on FCALL round-trips, not CPU-bound).
        if let Err(e) = crate::integration::fast_lane::init(
            std::sync::Arc::new(self.client.clone()),
            2,
        ) {
            // Non-fatal: if Fast-lane init fails, every command falls
            // back to synchronous dispatch (today's behaviour). The
            // daemon still serves correctly, just without the async
            // throughput improvement.
            warn!("Fast-lane init failed: {} — all commands will run synchronously", e);
        }

        // ========================================
        // KeyBased Architecture Integration
        // ========================================
        info!("🔑 Initializing KeyBased architecture (11× performance boost)");

        // Spawn compute handler (stream-based request processing)
        // IMPORTANT: Uses SHARED StreamDiscoveryManager - single source of truth for all streams
        // Commit 1.5.b (GN-D2.04): JoinHandle captured into worker_handles so
        // shutdown_workers() reaps it on graceful shutdown (previously dropped).
        {
            let compute_client = self.client.clone();
            let compute_node_id = self.node_id.clone();
            let compute_topology = GNodeDaemon::get_topology_ref();
            // SHARED discovery - compute handler uses daemon's discovery, not its own
            let compute_discovery = self.stream_discovery.clone();
            let compute_debug = self.debug;

            let compute_handle = std::thread::spawn(move || {
                // Create tokio runtime for async operations
                // Bounded runtime: this is a single listener loop doing async
                // ValKey I/O — it does NOT need one worker thread per CPU core
                // (what the default Runtime::new() allocates). 2 workers is
                // plenty and keeps the daemon's thread count flat regardless of
                // how many cores the host has.
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()
                    .expect("Failed to create tokio runtime for compute handler");

                rt.block_on(async {
                    info!("🔑 KeyBased: Starting compute handler with SHARED stream discovery...");

                    let handler = crate::compute_handler::ComputeHandler::new(
                        compute_client,
                        compute_node_id.clone(),
                        compute_topology,
                        compute_discovery,  // SHARED discovery from daemon
                        compute_debug,
                    );

                    match handler.start_listener().await {
                        Ok(task) => {
                            info!("✅ KeyBased: Compute handler started successfully (multi-tenant mode)");

                            // Wait for task to complete (it runs forever)
                            if let Err(e) = task.await {
                                error!("❌ KeyBased: Compute handler task failed: {}", e);
                            }
                        }
                        Err(e) => {
                            error!("❌ KeyBased: Failed to start compute handler: {}", e);
                        }
                    }
                });
            });
            if let Ok(mut handles) = self.worker_handles.lock() {
                handles.push(compute_handle);
            } else {
                warn!("Failed to register compute handler JoinHandle for graceful shutdown");
            }
        }

        // Spawn asset builder (manifest-driven bundle creation + compression — CMS extension)
        // Reads manifests from {site_id}:asset:manifests, builds bundles for each.
        // Falls back to face_mapping path for sites without manifests.
        // Commit 1.5.b (GN-D2.04): JoinHandle captured into worker_handles.
        {
            let asset_client = self.client.clone();
            let asset_topology = GNodeDaemon::get_topology_ref();
            let asset_debug = self.debug;
            let rebuild_interval = 300; // 5 minutes

            let asset_handle = std::thread::spawn(move || {
                // Bounded runtime: the asset builder wakes every 5 min to
                // rebuild bundles — it does not need one worker per CPU core.
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()
                    .expect("Failed to create tokio runtime for asset builder");

                rt.block_on(async {
                    info!("Starting asset builder (manifest-driven, {}s interval)", rebuild_interval);

                    let builder = crate::asset_builder::AssetBuilder::new(
                        asset_client,
                        asset_topology,
                        rebuild_interval,
                        asset_debug,
                    );

                    match builder.start_builder().await {
                        Ok(task) => {
                            info!("Asset builder started successfully");
                            if let Err(e) = task.await {
                                error!("Asset builder task failed: {}", e);
                            }
                        }
                        Err(e) => {
                            error!("Failed to start asset builder: {}", e);
                        }
                    }
                });
            });
            if let Ok(mut handles) = self.worker_handles.lock() {
                handles.push(asset_handle);
            } else {
                warn!("Failed to register asset builder JoinHandle for graceful shutdown");
            }
        }

        info!("✅ KeyBased architecture initialized (compute + bundle tasks spawned)");
        info!("   → Compute requests: {{site}}:events:compute_request");
        info!("   → Bundle rebuilds: {{site}}:events:invalidate");
        info!("   → Performance: 114ms → 10ms (11× faster)");

        // ========================================
        // Start unified stream processor using environment-based shared streams
        // Multi-node architecture: All nodes in the same environment share streams
        // via consumer groups for automatic load distribution
        // Dynamic Discovery: StreamDiscoveryManager discovers and subscribes to ALL site streams
        // ========================================
        info!("Starting environment-based stream processor");
        info!("  Environment: {} (shared stream for all {} nodes)", self.environment, self.environment);
        info!("  Node ID: {} (unique consumer name: gnode-{})", self.node_id, self.node_id);
        info!("  Topology namespace: {} (fallback if no sites discovered)", self.topology_namespace);
        info!("  Consumer group: gnode-workers (shared), Consumer: gnode-{}", self.node_id);
        info!("  Dynamic discovery: ENABLED (syncs every 60s, immediate on environment_changed)");

        // Start the environment-based stream processor
        self.start_stream_processor();
        
        // ========================================
        // Periodic Stream Discovery Refresh Loop
        // Check for new site registrations and update stream subscriptions
        // ========================================
        info!("Starting periodic stream discovery refresh loop (60s interval)");

        // Clone references for the refresh thread
        // Single-threaded mode: Worker registered in WorkerRegistry below
        if !self.single_threaded {
            let stream_discovery_ref = Arc::clone(&self.stream_discovery);
            let refresh_environment = self.environment.clone();
            let refresh_debug = self.debug;

            // Spawn a thread for periodic stream discovery refresh with shutdown check
            let discovery_refresh_handle = std::thread::spawn(move || {
                while !is_shutdown_requested() {
                    // Wait for refresh interval (check shutdown every 1 second)
                    for _ in 0..60 {
                        if is_shutdown_requested() { break; }
                        thread::sleep(Duration::from_secs(1));
                    }
                    if is_shutdown_requested() { break; }

                    // Check if refresh is needed and perform discovery
                    match crate::integration::connection_manager::get_connection() {
                        Ok(mut conn) => {
                            match stream_discovery_ref.write() {
                                Ok(discovery) => {
                                    if discovery.needs_refresh() {
                                        if refresh_debug {
                                            debug!("Refreshing stream discovery for environment: {}", refresh_environment);
                                        }

                                        // Refresh sites and streams
                                        match discovery.refresh(&mut conn) {
                                            Ok(()) => {
                                                let site_count = discovery.site_count();
                                                let stream_count = discovery.stream_count();

                                                // Check for newly added streams
                                                if discovery.has_new_streams() {
                                                    let new_streams = discovery.take_newly_added();
                                                    info!("🔍 Stream discovery: {} new streams detected!", new_streams.len());
                                                    for key in &new_streams {
                                                        info!("  + New stream: {}", key);
                                                    }
                                                    // Dynamic subscription is handled automatically:
                                                    // The worker thread syncs with shared_discovery every 60s
                                                    // and subscribes to newly discovered streams.
                                                }

                                                if refresh_debug {
                                                    debug!("Stream discovery refresh complete: {} sites, {} streams",
                                                           site_count, stream_count);
                                                }
                                            },
                                            Err(e) => {
                                                warn!("Stream discovery refresh failed: {:?}", e);
                                            }
                                        }
                                    }
                                },
                                Err(e) => {
                                    warn!("Failed to acquire stream discovery lock: {}", e);
                                }
                            }
                        },
                        Err(e) => {
                            warn!("Periodic stream discovery: failed to get connection: {:?}", e);
                        }
                    }
                }
                info!("Stream discovery refresh task shutting down");
            });

            // Store handle for graceful shutdown
            if let Ok(mut handles) = self.worker_handles.lock() {
                handles.push(discovery_refresh_handle);
            }
        } else {
            debug!("Discovery refresh worker will be registered in WorkerRegistry (single-threaded mode)");
        }

        // ========================================
        // PID Registration
        // Register PID in ValKey for stop/status commands
        // ========================================
        info!("Registering daemon PID in ValKey...");
        let pid = std::process::id();
        let pid_key = format!("{}:daemon:pid:{}:{}", self.stream_prefix, self.environment, self.node_id);

        match crate::integration::connection_manager::get_connection() {
            Ok(mut conn) => {
                // SETEX with 2-minute TTL (heartbeat loop will refresh)
                let result: redis::RedisResult<()> = redis::cmd("SETEX")
                    .arg(&pid_key)
                    .arg(120) // 2 minute TTL
                    .arg(pid.to_string())
                    .query(&mut conn);

                match result {
                    Ok(_) => info!("  ✓ PID {} registered at key: {}", pid, pid_key),
                    Err(e) => warn!("  ⚠ Failed to register PID: {}", e),
                }
            },
            Err(e) => warn!("  ⚠ Failed to get connection for PID registration: {}", e),
        }

        // Unified component-liveness heartbeat — the same key family the
        // operator dashboard reads for every component ({..}:gnode:heartbeat:
        // {env}:{component}). Distinct from the pid key above: a stable
        // component name and a fresh ts, refreshed by the heartbeat loop below.
        if let Ok(mut conn) = crate::integration::connection_manager::get_connection() {
            let hb_key = format!("{{{}}}:{}:heartbeat:{}:gnode-daemon", self.topology_namespace, self.stream_prefix, self.environment);
            let hb_ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            let hb_val = format!("{{\"ts\":{},\"pid\":{},\"comp\":\"gnode-daemon\"}}", hb_ts, pid);
            let _: redis::RedisResult<()> = redis::cmd("SETEX").arg(&hb_key).arg(120).arg(&hb_val).query(&mut conn);
        }

        // ========================================
        // Initial Service Discovery Scan
        // Register tool-tier services for all known sites
        // ========================================
        {
            let sd_enabled = self.service_discovery.read()
                .map(|sd| sd.is_enabled())
                .unwrap_or(false);

            if sd_enabled {
                info!("Running initial service discovery scan...");
                match crate::integration::connection_manager::get_connection() {
                    Ok(mut conn) => {
                        match self.service_discovery.write() {
                            Ok(mut sd) => {
                                match sd.discover_and_register(&mut conn) {
                                    Ok(result) => {
                                        if result.skipped {
                                            info!("  ✓ Service discovery: config unchanged, skipped");
                                        } else {
                                            info!("  ✓ Service discovery: registered {} services for {} sites ({} errors)",
                                                  result.registered, result.sites, result.errors);
                                        }
                                    }
                                    Err(e) => warn!("  ⚠ Initial service discovery failed: {:?}", e),
                                }
                            }
                            Err(e) => warn!("  ⚠ Failed to acquire service discovery lock: {}", e),
                        }
                    }
                    Err(e) => warn!("  ⚠ Failed to get connection for service discovery: {:?}", e),
                }
            } else {
                info!("Service discovery disabled, skipping initial scan");
            }
        }

        // Main loop: multi-threaded vs single-threaded
        if self.single_threaded {
            // ========================================
            // Single-Threaded Mode: Cooperative Scheduling
            // ========================================
            info!("═══════════════════════════════════════════════════════════════");
            info!("              SINGLE-THREADED MODE ACTIVE                       ");
            info!("═══════════════════════════════════════════════════════════════");

            // Create WorkerRegistry and register workers
            let mut registry = crate::worker::WorkerRegistry::new();

            // Register health cleanup worker
            let health_worker = crate::worker::HealthCleanupWorker::new(Arc::clone(&load_manager));
            registry.register(health_worker);
            info!("  ✓ Registered health cleanup worker");

            // Register discovery refresh worker
            let discovery_worker = crate::worker::DiscoveryRefreshWorker::new(
                Arc::clone(&self.stream_discovery),
                self.environment.clone(),
                self.debug,
            );
            registry.register(discovery_worker);
            info!("  ✓ Registered discovery refresh worker");

            // Register service discovery worker (if enabled)
            {
                let sd_enabled = self.service_discovery.read()
                    .map(|sd| sd.is_enabled())
                    .unwrap_or(false);
                let sd_interval = self.service_discovery.read()
                    .map(|sd| sd.scan_interval_secs())
                    .unwrap_or(120);

                if sd_enabled {
                    let sd_worker = crate::worker::ServiceDiscoveryWorker::new(
                        Arc::clone(&self.service_discovery),
                        sd_interval,
                    );
                    registry.register(sd_worker);
                    info!("  ✓ Registered service discovery worker (interval: {}s)", sd_interval);
                }
            }

            info!("Running single-threaded main loop with {} workers", registry.worker_count());
            info!("═══════════════════════════════════════════════════════════════");

            // Run the cooperative scheduler (blocks until shutdown)
            registry.run_single_threaded();
        } else {
            // ========================================
            // Multi-Threaded Mode: Standard Main Loop
            // ========================================
            // Keep the main thread alive with periodic saves and heartbeat
            // Check shutdown flag and exit gracefully
            info!("gNode daemon main loop started");
            while !is_shutdown_requested() {
                // Sleep in 1-second intervals to check shutdown flag (vs 1 hour)
                for _ in 0..60 {
                    if is_shutdown_requested() { break; }
                    thread::sleep(Duration::from_secs(1));
                }
                if is_shutdown_requested() { break; }

                info!("gNode daemon heartbeat");

                // STATELESS: No topology save needed (state lives in ValKey via FCALL)

                // Refresh PID TTL in ValKey
                if let Ok(mut conn) = crate::integration::connection_manager::get_connection() {
                    let pid_key = format!("{}:daemon:pid:{}:{}", self.stream_prefix, self.environment, self.node_id);
                    let _: redis::RedisResult<()> = redis::cmd("EXPIRE")
                        .arg(&pid_key)
                        .arg(120) // Refresh TTL to 2 minutes
                        .query(&mut conn);
                }

                // Refresh the unified component heartbeat with a fresh ts so the
                // dashboard's last-seen stays accurate.
                if let Ok(mut conn) = crate::integration::connection_manager::get_connection() {
                    let hb_key = format!("{{{}}}:{}:heartbeat:{}:gnode-daemon", self.topology_namespace, self.stream_prefix, self.environment);
                    let hb_ts = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or(0);
                    let hb_val = format!("{{\"ts\":{},\"pid\":{},\"comp\":\"gnode-daemon\"}}", hb_ts, std::process::id());
                    let _: redis::RedisResult<()> = redis::cmd("SETEX").arg(&hb_key).arg(120).arg(&hb_val).query(&mut conn);
                }

                // Log current stream discovery status
                if let Ok(discovery) = self.stream_discovery.read() {
                    info!("Stream discovery status: {} sites, {} streams",
                          discovery.site_count(), discovery.stream_count());
                }

                // Periodic service discovery scan (if enabled and due)
                {
                    let needs_scan = self.service_discovery.read()
                        .map(|sd| sd.is_enabled() && sd.needs_scan())
                        .unwrap_or(false);

                    if needs_scan {
                        if let Ok(mut conn) = crate::integration::connection_manager::get_connection() {
                            match self.service_discovery.write() {
                                Ok(mut sd) => {
                                    match sd.discover_and_register(&mut conn) {
                                        Ok(result) => {
                                            if !result.skipped && result.registered > 0 {
                                                info!("Service discovery: registered {} services for {} sites",
                                                      result.registered, result.sites);
                                            }
                                        }
                                        Err(e) => warn!("Service discovery scan failed: {:?}", e),
                                    }
                                }
                                Err(e) => warn!("Service discovery lock poisoned: {}", e),
                            }
                        }
                    }
                }
            }
        }

        // ========================================
        // Graceful Shutdown Sequence
        // ========================================
        info!("═══════════════════════════════════════════════════════════════");
        info!("                    GRACEFUL SHUTDOWN                           ");
        info!("═══════════════════════════════════════════════════════════════");

        // 1. Deregister PID from ValKey
        info!("Deregistering daemon PID from ValKey...");
        if let Ok(mut conn) = crate::integration::connection_manager::get_connection() {
            let pid_key = format!("{}:daemon:pid:{}:{}", self.stream_prefix, self.environment, self.node_id);
            let _: redis::RedisResult<i32> = redis::cmd("DEL")
                .arg(&pid_key)
                .query(&mut conn);
            info!("  ✓ PID key deleted: {}", pid_key);
        }

        // 2. Deregister node from topology
        info!("Deregistering node from topology...");
        if let Ok(mut conn) = crate::integration::connection_manager::get_connection() {
            let _: redis::RedisResult<i32> = redis::cmd("FCALL")
                .arg("GNODE_NODE_DEREGISTER")
                .arg(0)
                .arg(&self.node_id)
                .query(&mut conn);
            info!("  ✓ Node deregistered: {}", self.node_id);
        }

        // 3. STATELESS: No topology save needed (state lives in ValKey via FCALL)
        info!("  ✓ Topology state lives in ValKey (stateless daemon)");

        // 4. Wait for worker threads to finish
        info!("Waiting for worker threads to finish...");
        self.shutdown_workers();

        info!("═══════════════════════════════════════════════════════════════");
        info!("                 SHUTDOWN COMPLETE                              ");
        info!("═══════════════════════════════════════════════════════════════");

        Ok(())
    }

    /// Shutdown all worker threads gracefully
    /// Called when shutdown is requested to join all spawned worker threads
    fn shutdown_workers(&self) {
        let handles = match self.worker_handles.lock() {
            Ok(mut guard) => std::mem::take(&mut *guard),
            Err(e) => {
                error!("Failed to lock worker_handles for shutdown: {}", e);
                return;
            }
        };

        let total = handles.len();
        info!("Joining {} worker threads...", total);

        for (i, handle) in handles.into_iter().enumerate() {
            match handle.join() {
                Ok(_) => debug!("  Worker thread {}/{} joined", i + 1, total),
                Err(e) => warn!("  Worker thread {}/{} panicked: {:?}", i + 1, total, e),
            }
        }

        info!("  ✓ All {} worker threads joined", total);
    }
    
    /// Start a stream processor for the configured environment
    /// Multi-node architecture: All nodes in the same environment share streams
    /// via consumer groups for automatic load distribution
    ///
    /// Dynamic Stream Discovery: Uses StreamDiscoveryManager to discover and subscribe
    /// to ALL registered site streams. Falls back to topology_namespace if no sites found.
    ///
    /// Stream pattern: {site_id}:{stream_prefix}:{environment}:unified
    /// Consumer group: gnode-workers (shared across all nodes)
    /// Consumer name: gnode-{node_id} (unique per node)
    fn start_stream_processor(&self) {
        info!("Creating environment-based stream processor");
        info!("  Namespace: {}, Environment: {}, Node: {}, Type: {}", self.topology_namespace, self.environment, self.node_id, self.node_type);

        // Use the connection manager to get a connection and initialize streams
        // Note: topology_namespace is used as fallback if no sites discovered yet
        let result = crate::integration::connection_manager::with_retry_connection(|conn| {
            debug!("Initializing environment-based streams: namespace={}, environment={}, node={}, type={}, prefix={}",
                self.topology_namespace, self.environment, self.node_id, self.node_type, self.stream_prefix);

            // Initialize fallback streams (dynamic discovery happens in worker thread)
            crate::integration::processor::unified_stream_processor::initialize_environment_streams(
                conn,
                &self.topology_namespace,  // Fallback site - worker uses discovered sites
                &self.environment,
                &self.node_id,
                &self.stream_prefix,
                self.debug
            ).map(|_| ())
        });

        match result {
            Ok(_) => {
                info!("Successfully initialized environment-based streams");
                debug!("Creating processing thread for environment: {}, node: {}, type: {}", self.environment, self.node_id, self.node_type);

                // Clone values for the worker thread
                let namespace_owned = self.topology_namespace.clone();  // Fallback site for static mode
                let environment_owned = self.environment.clone();
                let node_id_owned = self.node_id.clone();
                let node_type_owned = self.node_type.to_string();
                let stream_prefix_owned = self.stream_prefix.clone();
                let debug_mode = self.debug;
                let config = self.stream_config.clone();

                // Clone shared discovery for dynamic stream subscription (Phase 2 implementation)
                let shared_discovery = self.stream_discovery.clone();

                // Create and spawn the process thread using the environment-based worker
                // with dynamic stream discovery enabled
                match crate::integration::consumer_groups::create_environment_stream_worker_dynamic(
                    &namespace_owned,  // Fallback site for static mode
                    &environment_owned,
                    &node_id_owned,
                    &node_type_owned,
                    &stream_prefix_owned,
                    &config,
                    debug_mode,
                    Some(shared_discovery)  // Enable dynamic stream discovery
                ) {
                    Ok(_) => {
                        info!("✅ Environment stream processor started with DYNAMIC discovery");
                        info!("  Fallback stream: {{{}}}:{}:{}:unified", namespace_owned, stream_prefix_owned, environment_owned);
                        info!("  Consumer group: gnode-workers, Consumer: gnode-{}, Type: {}", node_id_owned, node_type_owned);
                        info!("  Dynamic mode: subscribes to ALL discovered site streams (refreshes every 60s)");
                    },
                    Err(e) => {
                        error!("Failed to create environment stream processor: {:?}", e);
                        warn!("Using processor module recovery mechanisms");

                        // Let the processor module handle recovery
                        if let Err(recovery_err) = crate::integration::processor::recovery_processor::recover_environment_with_client(
                            self.client.clone(),
                            &self.topology_namespace,  // Placeholder
                            &self.environment,
                            &self.node_id,
                            &self.stream_prefix,
                            self.debug
                        ) {
                            error!("Recovery also failed: {:?}", recovery_err);
                        }
                    }
                }
            },
            Err(e) => {
                error!("Failed to initialize environment streams: {:?}", e);

                // Let the processor module handle recovery
                if let Err(recovery_err) = crate::integration::processor::recovery_processor::recover_environment_with_client(
                    self.client.clone(),
                    &self.topology_namespace,  // Placeholder
                    &self.environment,
                    &self.node_id,
                    &self.stream_prefix,
                    self.debug
                ) {
                    error!("Recovery also failed: {:?}", recovery_err);
                }
            }
        }
    }
}