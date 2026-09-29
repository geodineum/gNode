use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::fs;
use log::{info, warn, debug};

// =============================================================================
// Stream Pattern Constants
// =============================================================================
// Pattern: {site_id}:gnode:{stream_type}:{environment}
// =============================================================================

/// Build a unified stream key for a site/environment
/// Canonical pattern: {site_id}:gnode:unified:{environment}
/// Hash-tag braces ensure cluster slot consistency.
pub fn build_unified_stream_key(site_id: &str, environment: &str) -> String {
    format!("{{{}}}:gnode:unified:{}", site_id, environment)
}

/// Build a health stream key for a site.
///
/// Canonical pattern: `{site_id}:gnode:health` — no environment suffix, because
/// this is the key `gnode_site.lua` creates and REPORTS through stream
/// discovery, which is what every publisher subscribes to in practice. Three
/// other shapes existed in the tree (`:health:{environment}` here,
/// `:health:{node_id}` in the consumer's fallback, and the PHP writer's copy);
/// all three named streams nothing wrote, while 115 real records sat in the
/// Lua-named one.
///
/// Unsuffixed is also the right shape: a health record is about one site's
/// service, and that service's entity carries its own environment on the
/// `environment` axis. A site whose lanes differ by DTAP is separate sites.
pub fn build_health_stream_key(site_id: &str) -> String {
    format!("{{{}}}:gnode:health", site_id)
}

/// A connection URL with its password replaced by `***`, for logging.
///
/// Parses the credential out rather than searching for a known secret: the
/// call site that searched (`redis_url.replace(&redis_auth, "***")`) redacted
/// nothing whenever its copy of the password was empty or differently escaped,
/// and the call site that did not redact at all wrote the daemon's ValKey
/// password into the journal at INFO on every start.
pub fn redact_url(url: &str) -> String {
    let Some((scheme, rest)) = url.split_once("://") else { return url.to_string() };
    let Some((authority, tail)) = rest.split_once('@') else { return url.to_string() };
    // Only an authority of the form user:password carries a secret.
    match authority.split_once(':') {
        Some((user, _)) => format!("{}://{}:***@{}", scheme, user, tail),
        None => url.to_string(),
    }
}

/// The site a `{site}:...` stream key belongs to, for locating that site's
/// service topology from a stream the discovery loop handed us.
pub fn site_of_stream_key(stream_key: &str) -> Option<&str> {
    stream_key.strip_prefix('{')?.split('}').next()
}

/// Build a broadcast stream key for a site (environment-independent)
/// Canonical pattern: {site_id}:gnode:broadcast
pub fn build_broadcast_stream_key(site_id: &str) -> String {
    format!("{{{}}}:gnode:broadcast", site_id)
}

/// Build a request key for the key-based pattern
/// Pattern: {site_id}:req:{request_id}
pub fn build_request_key(site_id: &str, request_id: &str) -> String {
    format!("{{{}}}:req:{}", site_id, request_id)
}

/// Build a response key for the key-based pattern
/// Pattern: {site_id}:res:{request_id}
pub fn build_response_key(site_id: &str, request_id: &str) -> String {
    format!("{{{}}}:res:{}", site_id, request_id)
}

/// DTAP environments supported by gNode
pub const DTAP_ENVIRONMENTS: [&str; 4] = ["testing", "staging", "acceptance", "production"];

/// Default consumer group for daemon
pub const DEFAULT_DAEMON_GROUP: &str = "gnode-daemon";

/// Default consumer group for clients
pub const DEFAULT_CLIENT_GROUP: &str = "gnode-client";

/// Unified gNode Configuration
/// 
/// This structure combines the essential configuration needed for MVP
/// while providing hooks for future advanced features
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GNodeSettings {
    // Core stream processing settings
    #[serde(default = "default_base_backoff_ms")]
    pub base_backoff_ms: u64,
    
    #[serde(default = "default_max_backoff_ms")]
    pub max_backoff_ms: u64,
    
    #[serde(default = "default_initial_batch_size")]
    pub initial_batch_size: usize,
    
    #[serde(default = "default_max_batch_size")]
    pub max_batch_size: usize,
    
    #[serde(default = "default_min_batch_size")]
    pub min_batch_size: usize,
    
    #[serde(default = "default_idle_time_ms")]
    pub idle_time_ms: u64,
    
    #[serde(default = "default_trim_interval_secs")]
    pub trim_interval_secs: u64,
    
    #[serde(default = "default_max_stream_length")]
    pub max_stream_length: usize,
    
    #[serde(default = "default_approximate_trim")]
    pub approximate_trim: bool,
    
    #[serde(default = "default_pending_check_interval_ms")]
    pub pending_check_interval_ms: u64,
    
    #[serde(default = "default_circuit_breaker_threshold")]
    pub circuit_breaker_threshold: usize,
    
    #[serde(default = "default_circuit_breaker_cooldown_secs")]
    pub circuit_breaker_cooldown_secs: u64,
    
    // Consumer group settings (merged from old ConsumerGroupConfig)
    #[serde(default = "default_group_name")]
    pub group_name: String,
    
    #[serde(default = "default_consumer_prefix")]
    pub consumer_prefix: String,
    
    #[serde(default = "default_block_timeout_ms")]
    pub block_timeout_ms: u64,
    
    #[serde(default = "default_claim_interval_ms")]
    pub claim_interval_ms: u64,
    
    #[serde(default = "default_batch_acknowledge")]
    pub batch_acknowledge: bool,
    
    #[serde(default = "default_max_pending_claim")]
    pub max_pending_claim: usize,
    
    // Advanced features (disabled by default for MVP)
    #[serde(default)]
    pub enable_htt: bool,

    #[serde(default)]
    pub enable_geometric_threading: bool,

    #[serde(default)]
    pub enable_fixed_point: bool,

    // DTAP environment configuration
    #[serde(default = "default_environments")]
    pub environments: Vec<String>,

    #[serde(default)]
    pub stream_pattern: Option<String>,

    // Stream configuration from stream-config.yaml
    #[serde(default)]
    pub stream_config: Option<StreamConfig>,

    // Stream discovery settings
    /// How often to refresh site/stream discovery from ValKey (in seconds)
    /// Lower values = faster detection of new sites, higher values = less overhead
    #[serde(default = "default_stream_refresh_secs")]
    pub stream_refresh_secs: u64,
}

fn default_stream_refresh_secs() -> u64 {
    60  // Default: refresh every 60 seconds
}

/// Stream configuration for DTAP environments
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamConfig {
    pub consumer_group: String,
    pub block_time: u64,
    pub batch_size: usize,
    pub trim_enabled: bool,
    pub max_stream_length: usize,
    pub trim_strategy: String,
    pub approximate_trim: bool,
}

// Default value functions
fn default_base_backoff_ms() -> u64 { 100 }
fn default_max_backoff_ms() -> u64 { 1000 }
fn default_initial_batch_size() -> usize { 250 }
fn default_max_batch_size() -> usize { 500 }
fn default_min_batch_size() -> usize { 50 }
fn default_idle_time_ms() -> u64 { 30000 }
fn default_trim_interval_secs() -> u64 { 60 }
fn default_max_stream_length() -> usize { 10000 }
fn default_approximate_trim() -> bool { true }
fn default_pending_check_interval_ms() -> u64 { 5000 }
fn default_circuit_breaker_threshold() -> usize { 5 }
fn default_circuit_breaker_cooldown_secs() -> u64 { 30 }
fn default_group_name() -> String { "gnode-daemon".to_string() }
fn default_consumer_prefix() -> String { "consumer-".to_string() }
fn default_block_timeout_ms() -> u64 { 1000 }
fn default_claim_interval_ms() -> u64 { 5000 }
fn default_batch_acknowledge() -> bool { true }
fn default_max_pending_claim() -> usize { 50 }
fn default_environments() -> Vec<String> { vec!["default".to_string()] }

impl Default for GNodeSettings {
    fn default() -> Self {
        GNodeSettings {
            base_backoff_ms: default_base_backoff_ms(),
            max_backoff_ms: default_max_backoff_ms(),
            initial_batch_size: default_initial_batch_size(),
            max_batch_size: default_max_batch_size(),
            min_batch_size: default_min_batch_size(),
            idle_time_ms: default_idle_time_ms(),
            trim_interval_secs: default_trim_interval_secs(),
            max_stream_length: default_max_stream_length(),
            approximate_trim: default_approximate_trim(),
            pending_check_interval_ms: default_pending_check_interval_ms(),
            circuit_breaker_threshold: default_circuit_breaker_threshold(),
            circuit_breaker_cooldown_secs: default_circuit_breaker_cooldown_secs(),
            group_name: default_group_name(),
            consumer_prefix: default_consumer_prefix(),
            block_timeout_ms: default_block_timeout_ms(),
            claim_interval_ms: default_claim_interval_ms(),
            batch_acknowledge: default_batch_acknowledge(),
            max_pending_claim: default_max_pending_claim(),
            enable_htt: false,
            enable_geometric_threading: false,
            enable_fixed_point: false,
            environments: default_environments(),
            stream_pattern: None,
            stream_config: None,
            stream_refresh_secs: default_stream_refresh_secs(),
        }
    }
}

/// Load configuration from a YAML file
pub fn load_config_from_file(path: &PathBuf) -> Result<GNodeSettings, Box<dyn std::error::Error>> {
    let contents = fs::read_to_string(path)?;
    let config: GNodeSettings = serde_yaml::from_str(&contents)?;
    Ok(config)
}

/// Apply environment variable overrides to an existing config
/// Called internally by load_config() to apply env vars after file loading
///
/// Env var naming convention: GNODE_SECTION_FIELD (e.g., GNODE_CB_THRESHOLD for circuit breaker)
/// Precedence: CLI args > env vars > YAML file > defaults
fn apply_env_overrides(config: &mut GNodeSettings) {
    // ============================================================
    // Backoff configuration
    // ============================================================
    if let Ok(val) = std::env::var("GNODE_BASE_BACKOFF_MS") {
        if let Ok(parsed) = val.parse() {
            debug!("Applying env override: GNODE_BASE_BACKOFF_MS={}", parsed);
            config.base_backoff_ms = parsed;
        }
    }

    if let Ok(val) = std::env::var("GNODE_MAX_BACKOFF_MS") {
        if let Ok(parsed) = val.parse() {
            debug!("Applying env override: GNODE_MAX_BACKOFF_MS={}", parsed);
            config.max_backoff_ms = parsed;
        }
    }

    if let Ok(val) = std::env::var("GNODE_IDLE_TIME_MS") {
        if let Ok(parsed) = val.parse() {
            debug!("Applying env override: GNODE_IDLE_TIME_MS={}", parsed);
            config.idle_time_ms = parsed;
        }
    }

    if let Ok(val) = std::env::var("GNODE_PENDING_CHECK_MS") {
        if let Ok(parsed) = val.parse() {
            debug!("Applying env override: GNODE_PENDING_CHECK_MS={}", parsed);
            config.pending_check_interval_ms = parsed;
        }
    }

    // ============================================================
    // Batch configuration
    // ============================================================
    if let Ok(val) = std::env::var("GNODE_INITIAL_BATCH_SIZE") {
        if let Ok(parsed) = val.parse() {
            debug!("Applying env override: GNODE_INITIAL_BATCH_SIZE={}", parsed);
            config.initial_batch_size = parsed;
        }
    }

    if let Ok(val) = std::env::var("GNODE_MAX_BATCH_SIZE") {
        if let Ok(parsed) = val.parse() {
            debug!("Applying env override: GNODE_MAX_BATCH_SIZE={}", parsed);
            config.max_batch_size = parsed;
        }
    }

    if let Ok(val) = std::env::var("GNODE_MIN_BATCH_SIZE") {
        if let Ok(parsed) = val.parse() {
            debug!("Applying env override: GNODE_MIN_BATCH_SIZE={}", parsed);
            config.min_batch_size = parsed;
        }
    }

    // ============================================================
    // Stream configuration
    // ============================================================
    if let Ok(val) = std::env::var("GNODE_STREAM_MAX_LENGTH") {
        if let Ok(parsed) = val.parse() {
            debug!("Applying env override: GNODE_STREAM_MAX_LENGTH={}", parsed);
            config.max_stream_length = parsed;
        }
    }

    if let Ok(val) = std::env::var("GNODE_TRIM_INTERVAL_SECS") {
        if let Ok(parsed) = val.parse() {
            debug!("Applying env override: GNODE_TRIM_INTERVAL_SECS={}", parsed);
            config.trim_interval_secs = parsed;
        }
    }

    if let Ok(val) = std::env::var("GNODE_APPROXIMATE_TRIM") {
        let parsed = val.to_lowercase() == "true" || val == "1";
        debug!("Applying env override: GNODE_APPROXIMATE_TRIM={}", parsed);
        config.approximate_trim = parsed;
    }

    if let Ok(val) = std::env::var("GNODE_STREAM_REFRESH_SECS") {
        if let Ok(parsed) = val.parse() {
            debug!("Applying env override: GNODE_STREAM_REFRESH_SECS={}", parsed);
            config.stream_refresh_secs = parsed;
        }
    }

    // ============================================================
    // Consumer group configuration
    // ============================================================
    if let Ok(val) = std::env::var("GNODE_GROUP_NAME") {
        debug!("Applying env override: GNODE_GROUP_NAME={}", val);
        config.group_name = val;
    }

    if let Ok(val) = std::env::var("GNODE_CONSUMER_PREFIX") {
        debug!("Applying env override: GNODE_CONSUMER_PREFIX={}", val);
        config.consumer_prefix = val;
    }

    if let Ok(val) = std::env::var("GNODE_BLOCK_TIMEOUT_MS") {
        if let Ok(parsed) = val.parse() {
            debug!("Applying env override: GNODE_BLOCK_TIMEOUT_MS={}", parsed);
            config.block_timeout_ms = parsed;
        }
    }

    if let Ok(val) = std::env::var("GNODE_CLAIM_INTERVAL_MS") {
        if let Ok(parsed) = val.parse() {
            debug!("Applying env override: GNODE_CLAIM_INTERVAL_MS={}", parsed);
            config.claim_interval_ms = parsed;
        }
    }

    if let Ok(val) = std::env::var("GNODE_BATCH_ACKNOWLEDGE") {
        let parsed = val.to_lowercase() == "true" || val == "1";
        debug!("Applying env override: GNODE_BATCH_ACKNOWLEDGE={}", parsed);
        config.batch_acknowledge = parsed;
    }

    if let Ok(val) = std::env::var("GNODE_MAX_PENDING_CLAIM") {
        if let Ok(parsed) = val.parse() {
            debug!("Applying env override: GNODE_MAX_PENDING_CLAIM={}", parsed);
            config.max_pending_claim = parsed;
        }
    }

    // ============================================================
    // Circuit breaker configuration
    // ============================================================
    // Support both long form (GNODE_CIRCUIT_BREAKER_*) and short form (GNODE_CB_*)
    if let Ok(val) = std::env::var("GNODE_CIRCUIT_BREAKER_THRESHOLD")
        .or_else(|_| std::env::var("GNODE_CB_THRESHOLD"))
    {
        if let Ok(parsed) = val.parse() {
            debug!("Applying env override: circuit_breaker_threshold={}", parsed);
            config.circuit_breaker_threshold = parsed;
        }
    }

    if let Ok(val) = std::env::var("GNODE_CIRCUIT_BREAKER_COOLDOWN_SECS")
        .or_else(|_| std::env::var("GNODE_CB_COOLDOWN_SECS"))
    {
        if let Ok(parsed) = val.parse() {
            debug!("Applying env override: circuit_breaker_cooldown_secs={}", parsed);
            config.circuit_breaker_cooldown_secs = parsed;
        }
    }

    // ============================================================
    // Feature flags
    // ============================================================
    if let Ok(val) = std::env::var("GNODE_ENABLE_HTT") {
        let parsed = val.to_lowercase() == "true" || val == "1";
        debug!("Applying env override: GNODE_ENABLE_HTT={}", parsed);
        config.enable_htt = parsed;
    }

    if let Ok(val) = std::env::var("GNODE_ENABLE_GEOMETRIC_THREADING") {
        let parsed = val.to_lowercase() == "true" || val == "1";
        debug!("Applying env override: GNODE_ENABLE_GEOMETRIC_THREADING={}", parsed);
        config.enable_geometric_threading = parsed;
    }

    if let Ok(val) = std::env::var("GNODE_ENABLE_FIXED_POINT") {
        let parsed = val.to_lowercase() == "true" || val == "1";
        debug!("Applying env override: GNODE_ENABLE_FIXED_POINT={}", parsed);
        config.enable_fixed_point = parsed;
    }
}

/// Validate configuration values for logical consistency and safe bounds
/// Returns Ok(()) if valid, Err with description if invalid
pub fn validate_config(config: &GNodeSettings) -> Result<(), String> {
    // Batch size validation
    if config.min_batch_size > config.max_batch_size {
        return Err(format!(
            "min_batch_size ({}) cannot exceed max_batch_size ({})",
            config.min_batch_size, config.max_batch_size
        ));
    }

    if config.initial_batch_size < config.min_batch_size {
        return Err(format!(
            "initial_batch_size ({}) cannot be less than min_batch_size ({})",
            config.initial_batch_size, config.min_batch_size
        ));
    }

    if config.initial_batch_size > config.max_batch_size {
        return Err(format!(
            "initial_batch_size ({}) cannot exceed max_batch_size ({})",
            config.initial_batch_size, config.max_batch_size
        ));
    }

    // Backoff validation
    if config.base_backoff_ms == 0 {
        return Err("base_backoff_ms cannot be 0 (would cause busy-loop)".into());
    }

    if config.max_backoff_ms < config.base_backoff_ms {
        return Err(format!(
            "max_backoff_ms ({}) must be >= base_backoff_ms ({})",
            config.max_backoff_ms, config.base_backoff_ms
        ));
    }

    // Timeout validation
    if config.block_timeout_ms == 0 {
        return Err("block_timeout_ms cannot be 0".into());
    }

    // Circuit breaker validation
    if config.circuit_breaker_threshold == 0 {
        return Err("circuit_breaker_threshold cannot be 0".into());
    }

    if config.circuit_breaker_cooldown_secs == 0 {
        return Err("circuit_breaker_cooldown_secs cannot be 0".into());
    }

    Ok(())
}

/// Command line arguments structure for overriding configuration
#[derive(Debug, Clone)]
pub struct GNodeArgs {
    pub base_backoff_ms: Option<u64>,
    pub max_backoff_ms: Option<u64>,
    pub initial_batch_size: Option<usize>,
    pub max_batch_size: Option<usize>,
    pub min_batch_size: Option<usize>,
    pub idle_time_ms: Option<u64>,
    pub trim_interval_secs: Option<u64>,
    pub max_stream_length: Option<usize>,
    pub approximate_trim: Option<bool>,
    pub pending_check_interval_ms: Option<u64>,
    pub circuit_breaker_threshold: Option<usize>,
    pub circuit_breaker_cooldown_secs: Option<u64>,
    pub stream_refresh_secs: Option<u64>,
}

/// Load configuration from command line arguments
pub fn load_config_from_args(args: &GNodeArgs) -> GNodeSettings {
    let mut config = GNodeSettings::default();
    
    // Override defaults with command line arguments if provided
    if let Some(v) = args.base_backoff_ms {
        config.base_backoff_ms = v;
    }
    if let Some(v) = args.max_backoff_ms {
        config.max_backoff_ms = v;
    }
    if let Some(v) = args.initial_batch_size {
        config.initial_batch_size = v;
    }
    if let Some(v) = args.max_batch_size {
        config.max_batch_size = v;
    }
    if let Some(v) = args.min_batch_size {
        config.min_batch_size = v;
    }
    if let Some(v) = args.idle_time_ms {
        config.idle_time_ms = v;
    }
    if let Some(v) = args.trim_interval_secs {
        config.trim_interval_secs = v;
    }
    if let Some(v) = args.max_stream_length {
        config.max_stream_length = v;
    }
    if let Some(v) = args.approximate_trim {
        config.approximate_trim = v;
    }
    if let Some(v) = args.pending_check_interval_ms {
        config.pending_check_interval_ms = v;
    }
    if let Some(v) = args.circuit_breaker_threshold {
        config.circuit_breaker_threshold = v;
    }
    if let Some(v) = args.circuit_breaker_cooldown_secs {
        config.circuit_breaker_cooldown_secs = v;
    }
    if let Some(v) = args.stream_refresh_secs {
        config.stream_refresh_secs = v;
    }

    config
}

/// Load configuration from both file and command line arguments
/// Command line arguments take precedence over file configuration
pub fn load_config(config_path: Option<&PathBuf>, args: &GNodeArgs) -> Result<GNodeSettings, Box<dyn std::error::Error>> {
    let mut config = if let Some(path) = config_path {
        match load_config_from_file(path) {
            Ok(cfg) => {
                info!("Loaded unified stream configuration from {}", path.display());
                cfg
            },
            Err(e) => {
                warn!("Failed to load configuration from {}: {}. Using defaults.", path.display(), e);
                GNodeSettings::default()
            }
        }
    } else {
        GNodeSettings::default()
    };

    // Apply environment variable overrides (precedence: CLI > env > YAML > defaults)
    apply_env_overrides(&mut config);

    // Override with command line arguments (highest precedence)
    let args_config = load_config_from_args(args);
    
    // Merge configurations (args take precedence)
    if args.base_backoff_ms.is_some() { config.base_backoff_ms = args_config.base_backoff_ms; }
    if args.max_backoff_ms.is_some() { config.max_backoff_ms = args_config.max_backoff_ms; }
    if args.initial_batch_size.is_some() { config.initial_batch_size = args_config.initial_batch_size; }
    if args.max_batch_size.is_some() { config.max_batch_size = args_config.max_batch_size; }
    if args.min_batch_size.is_some() { config.min_batch_size = args_config.min_batch_size; }
    if args.idle_time_ms.is_some() { config.idle_time_ms = args_config.idle_time_ms; }
    if args.trim_interval_secs.is_some() { config.trim_interval_secs = args_config.trim_interval_secs; }
    if args.max_stream_length.is_some() { config.max_stream_length = args_config.max_stream_length; }
    if args.approximate_trim.is_some() { config.approximate_trim = args_config.approximate_trim; }
    if args.pending_check_interval_ms.is_some() { config.pending_check_interval_ms = args_config.pending_check_interval_ms; }
    if args.circuit_breaker_threshold.is_some() { config.circuit_breaker_threshold = args_config.circuit_breaker_threshold; }
    if args.circuit_breaker_cooldown_secs.is_some() { config.circuit_breaker_cooldown_secs = args_config.circuit_breaker_cooldown_secs; }

    // Validate final configuration
    validate_config(&config).map_err(|e| -> Box<dyn std::error::Error> {
        Box::new(std::io::Error::new(std::io::ErrorKind::InvalidInput, e))
    })?;

    debug!("Final unified stream configuration: {:?}", config);
    Ok(config)
}
#[cfg(test)]
mod stream_key_tests {
    use super::*;

    #[test]
    fn a_logged_url_never_carries_its_password() {
        assert_eq!(redact_url("redis://gnode_daemon:s3cr3t@10.66.0.1:47445"),
                   "redis://gnode_daemon:***@10.66.0.1:47445");
        // A password with an @ or a : in it still redacts: the split is on the
        // LAST authority separator, and everything between user: and @ goes.
        assert_eq!(redact_url("redis://u:p:a@ss@host:1"), "redis://u:***@ss@host:1");
        // Nothing to redact, nothing changed.
        assert_eq!(redact_url("redis://127.0.0.1:47445"), "redis://127.0.0.1:47445");
        assert_eq!(redact_url("redis://user@host:1"), "redis://user@host:1");
        assert_eq!(redact_url("not a url"), "not a url");
        assert_eq!(redact_url(""), "");
        // The property that matters: the secret is never in the output.
        for url in ["redis://gnode_daemon:s3cr3t@h:1", "rediss://u:s3cr3t@h:1/0"] {
            assert!(!redact_url(url).contains("s3cr3t"), "leaked: {}", redact_url(url));
        }
    }

    #[test]
    fn the_health_key_is_the_one_the_lua_creates_and_reports() {
        // gnode_site.lua provisions and reports `{site}:gnode:health`, which is
        // where every real publisher writes. Three other shapes named streams
        // nothing wrote to; this is the check that keeps them from coming back.
        assert_eq!(build_health_stream_key("nierto_com"), "{nierto_com}:gnode:health");
        assert!(!build_health_stream_key("nierto_com").contains("production"));
    }

    #[test]
    fn a_health_stream_names_the_site_whose_topology_to_write() {
        // The discovery loop hands the processor a stream key and nothing else;
        // the derived load has to land in that site's own topology.
        assert_eq!(site_of_stream_key("{nierto_com}:gnode:health"), Some("nierto_com"));
        assert_eq!(site_of_stream_key("{geodine}:gnode:unified:production"), Some("geodine"));
        // A key written without the hash tag (they exist live) names no site
        // rather than the wrong one.
        assert_eq!(site_of_stream_key("geodine:gnode:health"), None);
        assert_eq!(site_of_stream_key(""), None);
    }
}
