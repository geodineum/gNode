//! Routing Configuration Module for gNode
//!
//! This module provides dynamic routing configuration for message distribution
//! across different node types in a multi-node gNode deployment.
//!
//! Configuration is stored in YAML files and loaded into ValKey for distributed access.
//! This allows remote nodes to fetch their routing configuration without needing
//! local file access.
//!
//! ## Architecture
//!
//! ```
//! ┌─────────────────────────────────────────────────────────────┐
//! │                     Master Node                              │
//! │  ┌─────────────┐    ┌─────────────┐    ┌─────────────────┐  │
//! │  │ general.yaml│    │inference.yaml│   │ ValKey Storage  │  │
//! │  │ (local)     │───▶│ (local)     │──▶ │ gnode:routing:*   │  │
//! │  └─────────────┘    └─────────────┘    └─────────────────┘  │
//! └─────────────────────────────────────────────────────────────┘
//!                                                │
//!                                                │ Remote nodes fetch
//!                                                ▼
//! ┌─────────────────────────────────────────────────────────────┐
//! │                   Remote Node (inference)                    │
//! │  ┌─────────────────┐    ┌─────────────────┐                 │
//! │  │ Fetch from      │───▶│ RoutingConfig   │                 │
//! │  │ gnode:routing:*   │    │ in memory       │                 │
//! │  └─────────────────┘    └─────────────────┘                 │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Usage
//!
//! ```rust,ignore
//! // Master node: Load configs from YAML and store in ValKey
//! let configs = load_routing_configs_from_directory("daemon/config/routing")?;
//! store_routing_configs_to_valkey(&mut conn, &configs)?;
//!
//! // Any node: Fetch config from ValKey
//! let config = fetch_routing_config_from_valkey(&mut conn, "inference")?;
//!
//! // Check if message should be processed
//! if config.should_process_message(group_hint) {
//!     // Process message
//! }
//! ```

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, RwLock, OnceLock};
use serde::{Serialize, Deserialize};
use log::{info, warn, debug};
use redis::Connection;

/// Routing mode for message filtering
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum RoutingMode {
    /// Process ONLY messages with these group_hints
    Include,
    /// Process all messages EXCEPT those with these group_hints
    #[default]
    Exclude,
    /// Process all messages (ignore group_hints)
    All,
}


/// Routing rules for a node type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingRules {
    /// Routing mode: include, exclude, or all
    pub mode: RoutingMode,
    /// List of group_hint values for filtering
    pub group_hints: Vec<String>,
}

impl Default for RoutingRules {
    fn default() -> Self {
        Self {
            mode: RoutingMode::Exclude,
            group_hints: vec!["inference".to_string()],
        }
    }
}

/// Metadata for routing configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingMetadata {
    pub version: String,
    pub created_by: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
}

impl Default for RoutingMetadata {
    fn default() -> Self {
        Self {
            version: "1.0.0".to_string(),
            created_by: "system".to_string(),
            updated_at: None,
        }
    }
}

/// Full routing configuration for a node type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingConfig {
    /// Node type identifier (e.g., "general", "inference", "gpu_compute")
    pub node_type: String,
    /// Human-readable description
    pub description: String,
    /// Routing rules
    pub routing: RoutingRules,
    /// Metadata
    #[serde(default)]
    pub metadata: RoutingMetadata,
    /// Cached HashSet for O(1) lookups (not serialized)
    #[serde(skip)]
    hints_set: Option<HashSet<String>>,
}

impl RoutingConfig {
    /// Create a new routing config
    pub fn new(node_type: &str, description: &str, mode: RoutingMode, hints: Vec<String>) -> Self {
        let hints_set = hints.iter().cloned().collect();
        Self {
            node_type: node_type.to_string(),
            description: description.to_string(),
            routing: RoutingRules {
                mode,
                group_hints: hints,
            },
            metadata: RoutingMetadata::default(),
            hints_set: Some(hints_set),
        }
    }

    /// Create default config for "general" node type
    pub fn default_general() -> Self {
        Self::new(
            "general",
            "Default gNode node - processes standard requests, excludes specialized workloads",
            RoutingMode::Exclude,
            vec!["inference".to_string()],
        )
    }

    /// Create default config for "inference" node type
    pub fn default_inference() -> Self {
        Self::new(
            "inference",
            "AI inference specialized node - handles ML/AI inference requests only",
            RoutingMode::Include,
            vec!["inference".to_string()],
        )
    }

    /// Create default config for "all" node type
    pub fn default_all() -> Self {
        Self::new(
            "all",
            "Universal gNode node - processes all messages regardless of routing hint",
            RoutingMode::All,
            vec![],
        )
    }

    /// Initialize the hints_set cache after deserialization
    pub fn init_cache(&mut self) {
        self.hints_set = Some(self.routing.group_hints.iter().cloned().collect());
    }

    /// Check if a message with the given group_hint should be processed
    pub fn should_process_message(&self, group_hint: Option<&str>) -> bool {
        let gh = group_hint.unwrap_or("");

        match self.routing.mode {
            RoutingMode::All => true,
            RoutingMode::Include => {
                // Only process if group_hint is in our list
                if gh.is_empty() {
                    false
                } else if let Some(ref set) = self.hints_set {
                    set.contains(gh)
                } else {
                    self.routing.group_hints.contains(&gh.to_string())
                }
            },
            RoutingMode::Exclude => {
                // Process unless group_hint is in our exclusion list
                if gh.is_empty() {
                    true
                } else if let Some(ref set) = self.hints_set {
                    !set.contains(gh)
                } else {
                    !self.routing.group_hints.contains(&gh.to_string())
                }
            }
        }
    }
}

/// Global routing config cache
static ROUTING_CONFIG_CACHE: OnceLock<Arc<RwLock<HashMap<String, RoutingConfig>>>> = OnceLock::new();

/// Get or initialize the global routing config cache
fn get_routing_cache() -> &'static Arc<RwLock<HashMap<String, RoutingConfig>>> {
    ROUTING_CONFIG_CACHE.get_or_init(|| {
        Arc::new(RwLock::new(HashMap::new()))
    })
}

/// Load a single routing config from a YAML file
pub fn load_routing_config_from_file<P: AsRef<Path>>(path: P) -> Result<RoutingConfig, String> {
    let path = path.as_ref();
    let content = std::fs::read_to_string(path)
        .map_err(|e| format!("Failed to read file {:?}: {}", path, e))?;

    let mut config: RoutingConfig = serde_yaml::from_str(&content)
        .map_err(|e| format!("Failed to parse YAML {:?}: {}", path, e))?;

    config.init_cache();
    Ok(config)
}

/// Load all routing configs from a directory
pub fn load_routing_configs_from_directory<P: AsRef<Path>>(dir: P) -> Result<Vec<RoutingConfig>, String> {
    let dir = dir.as_ref();
    let mut configs = Vec::new();

    if !dir.exists() {
        return Err(format!("Directory does not exist: {:?}", dir));
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
            match load_routing_config_from_file(&path) {
                Ok(config) => {
                    info!("Loaded routing config for node_type '{}' from {:?}", config.node_type, path);
                    configs.push(config);
                },
                Err(e) => {
                    warn!("Failed to load routing config from {:?}: {}", path, e);
                }
            }
        }
    }

    Ok(configs)
}

/// ValKey key for routing config storage
fn get_routing_key(node_type: &str) -> String {
    format!("gnode:routing:{}", node_type)
}

/// Store a single routing config to ValKey
pub fn store_routing_config_to_valkey(conn: &mut Connection, config: &RoutingConfig) -> Result<(), String> {
    let key = get_routing_key(&config.node_type);
    let json = serde_json::to_string(config)
        .map_err(|e| format!("Failed to serialize config: {}", e))?;

    redis::cmd("SET")
        .arg(&key)
        .arg(&json)
        .query::<String>(conn)
        .map_err(|e| format!("Failed to store config in ValKey: {}", e))?;

    info!("Stored routing config for '{}' in ValKey at key '{}'", config.node_type, key);
    Ok(())
}

/// Store multiple routing configs to ValKey
pub fn store_routing_configs_to_valkey(conn: &mut Connection, configs: &[RoutingConfig]) -> Result<(), String> {
    for config in configs {
        store_routing_config_to_valkey(conn, config)?;
    }

    // Store a list of available node types
    let node_types: Vec<String> = configs.iter().map(|c| c.node_type.clone()).collect();
    let types_json = serde_json::to_string(&node_types)
        .map_err(|e| format!("Failed to serialize node types list: {}", e))?;

    redis::cmd("SET")
        .arg("gnode:routing:_node_types")
        .arg(&types_json)
        .query::<String>(conn)
        .map_err(|e| format!("Failed to store node types list: {}", e))?;

    info!("Stored {} routing configs and node types list", configs.len());
    Ok(())
}

/// Fetch a routing config from ValKey
pub fn fetch_routing_config_from_valkey(conn: &mut Connection, node_type: &str) -> Result<RoutingConfig, String> {
    let key = get_routing_key(node_type);

    let json: Option<String> = redis::cmd("GET")
        .arg(&key)
        .query(conn)
        .map_err(|e| format!("Failed to fetch config from ValKey: {}", e))?;

    match json {
        Some(data) => {
            let mut config: RoutingConfig = serde_json::from_str(&data)
                .map_err(|e| format!("Failed to deserialize config: {}", e))?;
            config.init_cache();
            debug!("Fetched routing config for '{}' from ValKey", node_type);
            Ok(config)
        },
        None => {
            Err(format!("No routing config found for node_type '{}' in ValKey", node_type))
        }
    }
}

/// Fetch all available routing configs from ValKey
pub fn fetch_all_routing_configs_from_valkey(conn: &mut Connection) -> Result<Vec<RoutingConfig>, String> {
    // First try to get the list of node types
    let types_json: Option<String> = redis::cmd("GET")
        .arg("gnode:routing:_node_types")
        .query(conn)
        .map_err(|e| format!("Failed to fetch node types list: {}", e))?;

    let node_types: Vec<String> = match types_json {
        Some(data) => serde_json::from_str(&data)
            .map_err(|e| format!("Failed to deserialize node types list: {}", e))?,
        None => {
            // Fallback: try known node types
            vec!["general".to_string(), "inference".to_string(), "all".to_string()]
        }
    };

    let mut configs = Vec::new();
    for node_type in node_types {
        match fetch_routing_config_from_valkey(conn, &node_type) {
            Ok(config) => configs.push(config),
            Err(e) => {
                debug!("Could not fetch config for '{}': {}", node_type, e);
            }
        }
    }

    Ok(configs)
}

/// Get or fetch routing config for a node type (with caching)
pub fn get_routing_config(conn: &mut Connection, node_type: &str) -> Result<RoutingConfig, String> {
    // Check cache first
    {
        let cache = get_routing_cache();
        let cache_read = cache.read().map_err(|_| "Cache lock poisoned")?;
        if let Some(config) = cache_read.get(node_type) {
            return Ok(config.clone());
        }
    }

    // Not in cache, fetch from ValKey
    let config = fetch_routing_config_from_valkey(conn, node_type)?;

    // Update cache
    {
        let cache = get_routing_cache();
        let mut cache_write = cache.write().map_err(|_| "Cache lock poisoned")?;
        cache_write.insert(node_type.to_string(), config.clone());
    }

    Ok(config)
}

/// Get routing config from cache only (no ValKey fetch)
/// Returns default config if not cached
pub fn get_cached_routing_config(node_type: &str) -> RoutingConfig {
    let cache = get_routing_cache();
    if let Ok(cache_read) = cache.read() {
        if let Some(config) = cache_read.get(node_type) {
            return config.clone();
        }
    }

    // Return defaults if not in cache
    match node_type {
        "inference" => RoutingConfig::default_inference(),
        "all" => RoutingConfig::default_all(),
        _ => RoutingConfig::default_general(),
    }
}

/// A node's EXPOSURE: the set of routing configs whose work it is willing to do.
///
/// `node_type` was a single string, so a node could serve exactly one class of
/// work. A real participant is often willing to do several — a laptop that
/// offers both `inference` and `sync`, say. Exposure is that set, parsed from a
/// comma-separated `node_type` (`"inference,sync"`), and an entry is processed
/// if ANY member would process it. The union is the correct rule: adding an
/// exposure only ever widens what a node accepts, never narrows it.
///
/// A single, unqualified type (`"general"`) parses to a one-member set and
/// behaves exactly as before — this is a superset of the old semantics.
pub struct ExposureSet {
    members: Vec<RoutingConfig>,
    /// The spec as given, for logging.
    spec: String,
}

impl ExposureSet {
    /// Parse a comma-separated exposure spec into its member routing configs.
    /// Whitespace and empty segments are ignored; an empty spec falls back to
    /// `general`, never to "expose to nothing".
    pub fn parse(spec: &str) -> Self {
        let mut members: Vec<RoutingConfig> = spec
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(get_cached_routing_config)
            .collect();
        if members.is_empty() {
            members.push(get_cached_routing_config("general"));
        }
        ExposureSet { members, spec: spec.to_string() }
    }

    /// Process an entry if ANY exposure would. Union, never intersection:
    /// exposure is opt-in, so more exposures means more accepted, not less.
    pub fn should_process_message(&self, group_hint: Option<&str>) -> bool {
        self.members.iter().any(|c| c.should_process_message(group_hint))
    }

    /// True when this is a single-member set naming exactly `node_type` — used
    /// to keep single-type log lines unchanged.
    pub fn is_single(&self, node_type: &str) -> bool {
        self.members.len() == 1 && self.spec.trim() == node_type
    }

    pub fn spec(&self) -> &str { &self.spec }
    pub fn len(&self) -> usize { self.members.len() }
    pub fn is_empty(&self) -> bool { self.members.is_empty() }
}

/// Pre-load routing config into cache (call during daemon startup)
pub fn cache_routing_config(config: RoutingConfig) {
    let cache = get_routing_cache();
    if let Ok(mut cache_write) = cache.write() {
        let node_type = config.node_type.clone();
        cache_write.insert(node_type, config);
    }
}

/// Clear the routing config cache
pub fn clear_routing_cache() {
    let cache = get_routing_cache();
    if let Ok(mut cache_write) = cache.write() {
        cache_write.clear();
    }
}

/// Initialize routing configs: Load from files (master) or fetch from ValKey (worker)
///
/// # Arguments
/// * `conn` - ValKey connection
/// * `config_dir` - Directory containing routing YAML files (e.g., "daemon/config/routing")
/// * `is_master` - If true, loads from files and stores to ValKey. If false, fetches from ValKey.
/// * `node_type` - The node type this daemon will use for filtering
pub fn initialize_routing_configs(
    conn: &mut Connection,
    config_dir: Option<&str>,
    is_master: bool,
    node_type: &str
) -> Result<RoutingConfig, String> {
    if is_master {
        // Master node: Load from files and store to ValKey
        if let Some(dir) = config_dir {
            let configs = load_routing_configs_from_directory(dir)?;
            if !configs.is_empty() {
                store_routing_configs_to_valkey(conn, &configs)?;

                // Cache all configs
                for config in &configs {
                    cache_routing_config(config.clone());
                }

                // node_type may be a set spec ("inference,sync"); every member is
                // now cached above. Return the first member's config for the
                // caller's log line — the cache, not this value, drives routing.
                let first = node_type.split(',').map(|s| s.trim()).find(|s| !s.is_empty()).unwrap_or("general");
                return configs.into_iter()
                    .find(|c| c.node_type == first)
                    .ok_or_else(|| format!("No config found for node_type '{}'", first));
            }
        }

        // Fallback: Create and store default configs
        info!("No routing config files found, creating defaults");
        let defaults = vec![
            RoutingConfig::default_general(),
            RoutingConfig::default_inference(),
            RoutingConfig::default_all(),
        ];
        store_routing_configs_to_valkey(conn, &defaults)?;

        for config in &defaults {
            cache_routing_config(config.clone());
        }

        let first = node_type.split(',').map(|s| s.trim()).find(|s| !s.is_empty()).unwrap_or("general");
        defaults.into_iter()
            .find(|c| c.node_type == first)
            .ok_or_else(|| format!("No default config for node_type '{}'", first))
    } else {
        // Worker node: fetch every member of the exposure spec from ValKey and
        // cache each. A worker exposed to "inference,sync" needs both configs
        // present, not a single lookup of the literal spec string.
        let members: Vec<&str> = {
            let m: Vec<&str> = node_type.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()).collect();
            if m.is_empty() { vec!["general"] } else { m }
        };

        let mut first_config: Option<RoutingConfig> = None;
        for member in &members {
            let config = match get_routing_config(conn, member) {
                Ok(config) => config,
                Err(e) => {
                    warn!("Failed to fetch routing config for '{}' from ValKey: {}. Using default.", member, e);
                    match *member {
                        "inference" => RoutingConfig::default_inference(),
                        "all" => RoutingConfig::default_all(),
                        _ => RoutingConfig::default_general(),
                    }
                }
            };
            cache_routing_config(config.clone());
            if first_config.is_none() {
                first_config = Some(config);
            }
        }
        // First member's config is returned for the caller's log line only;
        // routing reads the cache, where every member now lives.
        Ok(first_config.unwrap_or_else(RoutingConfig::default_general))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ExposureSet caches configs by name via get_cached_routing_config, which
    // falls back to hardcoded defaults when the cache is empty — so these tests
    // exercise the union logic against the built-in general/inference/all
    // defaults without needing a populated cache.
    #[test]
    fn test_exposure_single_general_matches_legacy() {
        let e = ExposureSet::parse("general");
        assert!(e.should_process_message(None));            // untagged -> yes
        assert!(e.should_process_message(Some("")));        // untagged -> yes
        assert!(e.should_process_message(Some("report")));  // non-special -> yes
        assert!(!e.should_process_message(Some("inference"))); // excluded
    }

    #[test]
    fn test_exposure_union_general_plus_inference_is_effectively_all() {
        // general excludes inference; add inference back and the node accepts both.
        let e = ExposureSet::parse("general,inference");
        assert!(e.should_process_message(Some("inference"))); // via inference member
        assert!(e.should_process_message(Some("report")));    // via general member
        assert!(e.should_process_message(None));              // via general member
        assert_eq!(e.len(), 2);
    }

    #[test]
    fn test_exposure_two_inclusive_types_or_together() {
        // A node exposed only to inference processes inference and nothing untagged.
        let only_inf = ExposureSet::parse("inference");
        assert!(only_inf.should_process_message(Some("inference")));
        assert!(!only_inf.should_process_message(None));
        assert!(!only_inf.should_process_message(Some("report")));
    }

    #[test]
    fn test_exposure_whitespace_and_empty_segments() {
        let e = ExposureSet::parse("  inference , , general ,");
        assert_eq!(e.len(), 2);
        assert!(e.should_process_message(Some("inference")));
        assert!(e.should_process_message(Some("report")));
    }

    #[test]
    fn test_exposure_empty_spec_falls_back_to_general_not_nothing() {
        let e = ExposureSet::parse("");
        assert_eq!(e.len(), 1);
        // Must accept ordinary work, never silently expose to nothing.
        assert!(e.should_process_message(None));
        assert!(e.should_process_message(Some("report")));
    }

    #[test]
    fn test_exposure_is_single() {
        assert!(ExposureSet::parse("general").is_single("general"));
        assert!(!ExposureSet::parse("general,inference").is_single("general"));
    }

    #[test]
    fn test_routing_config_include_mode() {
        let config = RoutingConfig::new(
            "inference",
            "Test inference",
            RoutingMode::Include,
            vec!["inference".to_string()],
        );

        assert!(config.should_process_message(Some("inference")));
        assert!(!config.should_process_message(Some("general")));
        assert!(!config.should_process_message(None));
        assert!(!config.should_process_message(Some("")));
    }

    #[test]
    fn test_routing_config_exclude_mode() {
        let config = RoutingConfig::new(
            "general",
            "Test general",
            RoutingMode::Exclude,
            vec!["inference".to_string()],
        );

        assert!(!config.should_process_message(Some("inference")));
        assert!(config.should_process_message(Some("general")));
        assert!(config.should_process_message(None));
        assert!(config.should_process_message(Some("")));
        assert!(config.should_process_message(Some("other")));
    }

    #[test]
    fn test_routing_config_all_mode() {
        let config = RoutingConfig::new(
            "all",
            "Test all",
            RoutingMode::All,
            vec![],
        );

        assert!(config.should_process_message(Some("inference")));
        assert!(config.should_process_message(Some("general")));
        assert!(config.should_process_message(None));
        assert!(config.should_process_message(Some("")));
        assert!(config.should_process_message(Some("anything")));
    }

    #[test]
    fn test_default_configs() {
        let general = RoutingConfig::default_general();
        assert_eq!(general.node_type, "general");
        assert!(!general.should_process_message(Some("inference")));
        assert!(general.should_process_message(Some("anything_else")));

        let inference = RoutingConfig::default_inference();
        assert_eq!(inference.node_type, "inference");
        assert!(inference.should_process_message(Some("inference")));
        assert!(!inference.should_process_message(Some("anything_else")));

        let all = RoutingConfig::default_all();
        assert_eq!(all.node_type, "all");
        assert!(all.should_process_message(Some("inference")));
        assert!(all.should_process_message(Some("anything")));
    }
}
