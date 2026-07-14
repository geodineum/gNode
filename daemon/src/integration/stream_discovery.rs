//! Stream Discovery Module for gNode
//!
//! This module provides topology-driven stream discovery for the gNode daemon.
//! Instead of requiring site_id as a command-line argument, the daemon discovers
//! registered sites and their streams dynamically from ValKey.
//!
//! Key features:
//! - Discover registered sites via GNODE_SERVICE_LIST_ALL
//! - Get streams for environment via GNODE_SERVICE_GET_DAEMON_STREAMS
//! - Dynamic subscription management
//! - Periodic refresh for new sites
//! - Thread-safe stream registry

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use log::{info, warn, error, debug};
use redis::{cmd, Connection, Value};
use serde::{Deserialize, Serialize};

use crate::integration::error_handlings::{
    IntegrationResult, valkey_function_error,
};

/// Represents a discovered stream that the daemon should subscribe to
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct DiscoveredStream {
    /// The ValKey stream key
    pub key: String,
    /// The site this stream belongs to (None for shared streams like registration/global)
    pub site_id: Option<String>,
    /// The environment (testing/staging/acceptance/production or "global" for broadcast)
    pub environment: Option<String>,
    /// The topology namespace (for shared streams)
    pub topology_namespace: Option<String>,
    /// The stream type (unified/health/broadcast/registration/global)
    pub stream_type: String,
}

/// Represents a registered site
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisteredSite {
    /// Site identifier
    pub id: String,
    /// Site status
    pub status: String,
    /// Environments configured for this site
    #[serde(default)]
    pub environments: Vec<String>,
    /// Number of streams created for this site
    #[serde(default)]
    pub stream_count: u32,
    /// When the site was created (Unix timestamp)
    pub created_at: Option<u64>,
}

/// Response from GNODE_SERVICE_LIST_ALL
#[derive(Debug, Clone, Deserialize)]
struct SiteListResponse {
    sites: Vec<RegisteredSite>,
    count: u32,
}

/// Response from GNODE_SERVICE_GET_DAEMON_STREAMS
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)] // Fields read by serde deserialization
struct DaemonStreamsResponse {
    environment: String,
    streams: DaemonStreamCategories,
    total_streams: u32,
    site_count: u32,
}

/// Response from GNODE_SERVICE_GET_ALL_STREAMS (per-site environment configuration)
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)] // Fields read by serde deserialization
struct AllStreamsResponse {
    streams: DaemonStreamCategories,
    total_streams: u32,
    site_count: u32,
    #[serde(default)]
    expected_per_site: u32,
    #[serde(default)]
    expected_shared: u32,
    #[serde(default)]
    expected_total: u32,
}

#[derive(Debug, Clone, Deserialize)]
struct DaemonStreamCategories {
    unified: Vec<StreamEntry>,
    health: Vec<StreamEntry>,
    broadcast: Vec<StreamEntry>,
    #[serde(default)]
    registration: Vec<StreamEntry>,
    #[serde(default)]
    global: Vec<StreamEntry>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)] // Fields read by serde deserialization
struct StreamEntry {
    key: String,
    #[serde(default)]
    site_id: Option<String>,
    #[serde(default)]
    environment: Option<String>,
    #[serde(default)]
    topology_namespace: Option<String>,
    #[serde(rename = "type")]
    stream_type: String,
}

/// Configuration for stream discovery
#[derive(Debug, Clone)]
pub struct StreamDiscoveryConfig {
    /// How often to refresh the site list (in seconds)
    pub refresh_interval_secs: u64,
    /// The DTAP environment to discover streams for
    pub environment: String,
    /// Whether to include broadcast streams
    pub include_broadcast: bool,
    /// Whether to include health streams
    pub include_health: bool,
    /// Whether to include unified streams
    pub include_unified: bool,
}

impl Default for StreamDiscoveryConfig {
    fn default() -> Self {
        Self {
            refresh_interval_secs: 60,
            environment: "all".to_string(),  // Listen to ALL DTAP environments by default
            include_broadcast: true,
            include_health: true,
            include_unified: true,
        }
    }
}

/// Thread-safe stream discovery manager
pub struct StreamDiscoveryManager {
    /// Configuration
    config: StreamDiscoveryConfig,
    /// Discovered streams (by stream key)
    streams: Arc<RwLock<HashMap<String, DiscoveredStream>>>,
    /// Registered sites
    sites: Arc<RwLock<HashMap<String, RegisteredSite>>>,
    /// Last refresh time
    last_refresh: Arc<RwLock<Option<Instant>>>,
    /// Stream keys that have been added since last check
    newly_added: Arc<RwLock<HashSet<String>>>,
    /// Flag indicating workers should sync immediately (set by broadcast handler)
    needs_immediate_sync: Arc<AtomicBool>,
}

impl StreamDiscoveryManager {
    /// Create a new stream discovery manager
    pub fn new(config: StreamDiscoveryConfig) -> Self {
        Self {
            config,
            streams: Arc::new(RwLock::new(HashMap::new())),
            sites: Arc::new(RwLock::new(HashMap::new())),
            last_refresh: Arc::new(RwLock::new(None)),
            newly_added: Arc::new(RwLock::new(HashSet::new())),
            needs_immediate_sync: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Create with default production configuration
    pub fn for_environment(environment: &str) -> Self {
        Self::new(StreamDiscoveryConfig {
            environment: environment.to_string(),
            ..Default::default()
        })
    }

    /// Get the configured environment
    pub fn environment(&self) -> &str {
        &self.config.environment
    }

    /// Check if a refresh is needed based on configured interval
    pub fn needs_refresh(&self) -> bool {
        let last = match self.last_refresh.read() {
            Ok(guard) => guard,
            Err(poisoned) => {
                error!("StreamDiscoveryManager last_refresh lock poisoned — prior thread panicked. Forcing refresh for data integrity.");
                // Recover guard but force refresh since timestamp may be stale
                drop(poisoned.into_inner());
                return true;
            }
        };
        match *last {
            None => true,
            Some(instant) => instant.elapsed() > Duration::from_secs(self.config.refresh_interval_secs),
        }
    }

    /// Discover all registered sites from ValKey
    pub fn discover_sites(&self, conn: &mut Connection) -> IntegrationResult<Vec<RegisteredSite>> {
        debug!("Discovering registered sites via GNODE_SERVICE_LIST_ALL");

        // Call the Lua function with metadata
        let result: Value = cmd("FCALL")
            .arg("GNODE_SERVICE_LIST_ALL")
            .arg(0)  // No keys
            .arg("true")  // include_meta = true
            .query(conn)
            .map_err(|e| valkey_function_error(format!("GNODE_SERVICE_LIST_ALL failed: {}", e)))?;

        // Parse the JSON response
        let json_str = match &result {
            Value::BulkString(bytes) => String::from_utf8_lossy(bytes).to_string(),
            Value::SimpleString(s) => s.clone(),
            Value::Array(arr) if !arr.is_empty() => {
                // Handle case where result is wrapped in array
                match &arr[0] {
                    Value::BulkString(bytes) => String::from_utf8_lossy(bytes).to_string(),
                    _ => return Err(valkey_function_error("Unexpected nested response type".to_string())),
                }
            }
            _ => {
                return Err(valkey_function_error(format!(
                    "Unexpected response type from GNODE_SERVICE_LIST_ALL: {:?}",
                    result
                )));
            }
        };

        let response: SiteListResponse = serde_json::from_str(&json_str)
            .map_err(|e| valkey_function_error(format!("Failed to parse site list: {}", e)))?;

        // Update internal state
        {
            let mut sites = match self.sites.write() {
                Ok(guard) => guard,
                Err(poisoned) => {
                    error!("StreamDiscoveryManager sites lock poisoned — prior thread panicked. Data will be cleared and repopulated.");
                    poisoned.into_inner()
                }
            };
            sites.clear();
            for site in &response.sites {
                sites.insert(site.id.clone(), site.clone());
            }
        }

        debug!("Discovered {} registered sites", response.count);
        Ok(response.sites)
    }

    /// Discover streams for the configured environment (or ALL environments if "all")
    pub fn discover_streams(&self, conn: &mut Connection) -> IntegrationResult<Vec<DiscoveredStream>> {
        // If environment is "all", use per-site environment configuration
        // This calls GNODE_SERVICE_GET_ALL_STREAMS which reads each site's active_environment
        // and returns exactly 3 streams per site (unified + health + broadcast)
        if self.config.environment == "all" {
            return self.discover_streams_per_site_config(conn);
        }

        debug!("Discovering streams for environment: {}", self.config.environment);

        // Call the Lua function
        let result: Value = cmd("FCALL")
            .arg("GNODE_SERVICE_GET_DAEMON_STREAMS")
            .arg(0)  // No keys
            .arg(&self.config.environment)
            .query(conn)
            .map_err(|e| valkey_function_error(format!("GNODE_SERVICE_GET_DAEMON_STREAMS failed: {}", e)))?;

        // Parse the JSON response
        let json_str = match &result {
            Value::BulkString(bytes) => String::from_utf8_lossy(bytes).to_string(),
            Value::SimpleString(s) => s.clone(),
            Value::Array(arr) if !arr.is_empty() => {
                match &arr[0] {
                    Value::BulkString(bytes) => String::from_utf8_lossy(bytes).to_string(),
                    _ => return Err(valkey_function_error("Unexpected nested response type".to_string())),
                }
            }
            _ => {
                return Err(valkey_function_error(format!(
                    "Unexpected response type from GNODE_SERVICE_GET_DAEMON_STREAMS: {:?}",
                    result
                )));
            }
        };

        let response: DaemonStreamsResponse = serde_json::from_str(&json_str)
            .map_err(|e| valkey_function_error(format!("Failed to parse daemon streams: {}", e)))?;

        // Convert to DiscoveredStream instances
        let mut discovered = Vec::new();
        let mut newly_added_keys = HashSet::new();

        // Get current stream keys for comparison
        let current_keys: HashSet<String> = {
            let streams = match self.streams.read() {
                Ok(guard) => guard,
                Err(poisoned) => {
                    error!("StreamDiscoveryManager streams lock poisoned — prior thread panicked. Recovering guard; data may be stale until next refresh.");
                    poisoned.into_inner()
                }
            };
            streams.keys().cloned().collect()
        };

        // Process unified streams
        if self.config.include_unified {
            for entry in response.streams.unified {
                let stream = DiscoveredStream {
                    key: entry.key.clone(),
                    site_id: entry.site_id,
                    environment: entry.environment,
                    topology_namespace: entry.topology_namespace,
                    stream_type: "unified".to_string(),
                };
                if !current_keys.contains(&entry.key) {
                    newly_added_keys.insert(entry.key.clone());
                }
                discovered.push(stream);
            }
        }

        // Process health streams
        if self.config.include_health {
            for entry in response.streams.health {
                let stream = DiscoveredStream {
                    key: entry.key.clone(),
                    site_id: entry.site_id,
                    environment: entry.environment,
                    topology_namespace: entry.topology_namespace,
                    stream_type: "health".to_string(),
                };
                if !current_keys.contains(&entry.key) {
                    newly_added_keys.insert(entry.key.clone());
                }
                discovered.push(stream);
            }
        }

        // Process broadcast streams (shared across all sites)
        if self.config.include_broadcast {
            for entry in response.streams.broadcast {
                let stream = DiscoveredStream {
                    key: entry.key.clone(),
                    site_id: entry.site_id,
                    environment: entry.environment,
                    topology_namespace: entry.topology_namespace,
                    stream_type: "broadcast".to_string(),
                };
                if !current_keys.contains(&entry.key) {
                    newly_added_keys.insert(entry.key.clone());
                }
                discovered.push(stream);
            }
        }

        // Process registration streams (shared - for service registrations from gNode-Client)
        for entry in response.streams.registration {
            let stream = DiscoveredStream {
                key: entry.key.clone(),
                site_id: entry.site_id,
                environment: entry.environment,
                topology_namespace: entry.topology_namespace,
                stream_type: "registration".to_string(),
            };
            if !current_keys.contains(&entry.key) {
                newly_added_keys.insert(entry.key.clone());
            }
            discovered.push(stream);
        }

        // Process global streams (shared - for future multi-topology coordination)
        for entry in response.streams.global {
            let stream = DiscoveredStream {
                key: entry.key.clone(),
                site_id: entry.site_id,
                environment: entry.environment,
                topology_namespace: entry.topology_namespace,
                stream_type: "global".to_string(),
            };
            if !current_keys.contains(&entry.key) {
                newly_added_keys.insert(entry.key.clone());
            }
            discovered.push(stream);
        }

        // Update internal state
        {
            let mut streams = match self.streams.write() {
                Ok(guard) => guard,
                Err(poisoned) => {
                    error!("StreamDiscoveryManager streams lock poisoned — prior thread panicked. Recovering guard; data may be stale until next refresh.");
                    poisoned.into_inner()
                }
            };
            streams.clear();
            for stream in &discovered {
                streams.insert(stream.key.clone(), stream.clone());
            }
        }

        // Track newly added streams
        {
            let mut added = match self.newly_added.write() {
                Ok(guard) => guard,
                Err(poisoned) => {
                    error!("StreamDiscoveryManager newly_added lock poisoned — prior thread panicked. Recovering guard.");
                    poisoned.into_inner()
                }
            };
            *added = newly_added_keys;
        }

        // Update last refresh time
        {
            let mut last = match self.last_refresh.write() {
                Ok(guard) => guard,
                Err(poisoned) => {
                    error!("StreamDiscoveryManager last_refresh lock poisoned — prior thread panicked. Recovering with possibly stale timestamp; forcing refresh.");
                    poisoned.into_inner()
                }
            };
            *last = Some(Instant::now());
        }

        debug!(
            "Discovered {} streams for environment '{}' (unified: {}, health: {}, broadcast: {})",
            discovered.len(),
            self.config.environment,
            discovered.iter().filter(|s| s.stream_type == "unified").count(),
            discovered.iter().filter(|s| s.stream_type == "health").count(),
            discovered.iter().filter(|s| s.stream_type == "broadcast").count(),
        );

        Ok(discovered)
    }

    /// Discover streams using per-site environment configuration
    ///
    /// This is the PRIMARY discovery method. It calls GNODE_SERVICE_GET_ALL_STREAMS which:
    /// - Reads each site's active_environment from metadata
    /// - Returns exactly 3 streams per site: unified + health + broadcast
    /// - No duplication of broadcast streams (fixed bug where broadcast was returned 4x)
    ///
    /// For 4 sites: expects 12 streams total (4 unified + 4 health + 4 broadcast)
    fn discover_streams_per_site_config(&self, conn: &mut Connection) -> IntegrationResult<Vec<DiscoveredStream>> {
        debug!("Discovering streams using per-site environment configuration");

        // Call the new Lua function that handles per-site environments
        let result: Value = cmd("FCALL")
            .arg("GNODE_SERVICE_GET_ALL_STREAMS")
            .arg(0)  // No keys
            .query(conn)
            .map_err(|e| valkey_function_error(format!("GNODE_SERVICE_GET_ALL_STREAMS failed: {}", e)))?;

        // Parse the JSON response
        let json_str = match &result {
            Value::BulkString(bytes) => String::from_utf8_lossy(bytes).to_string(),
            Value::SimpleString(s) => s.clone(),
            Value::Array(arr) if !arr.is_empty() => {
                match &arr[0] {
                    Value::BulkString(bytes) => String::from_utf8_lossy(bytes).to_string(),
                    _ => return Err(valkey_function_error("Unexpected nested response type".to_string())),
                }
            }
            _ => {
                return Err(valkey_function_error(format!(
                    "Unexpected response type from GNODE_SERVICE_GET_ALL_STREAMS: {:?}",
                    result
                )));
            }
        };

        let response: AllStreamsResponse = serde_json::from_str(&json_str)
            .map_err(|e| valkey_function_error(format!("Failed to parse all streams response: {}", e)))?;

        let mut all_discovered = Vec::new();
        let mut total_unified = 0;
        let mut total_health = 0;
        let mut total_broadcast = 0;

        // Process unified streams
        if self.config.include_unified {
            for entry in response.streams.unified {
                let stream = DiscoveredStream {
                    key: entry.key.clone(),
                    site_id: entry.site_id,
                    environment: entry.environment,
                    topology_namespace: entry.topology_namespace,
                    stream_type: "unified".to_string(),
                };
                all_discovered.push(stream);
                total_unified += 1;
            }
        }

        // Process health streams
        if self.config.include_health {
            for entry in response.streams.health {
                let stream = DiscoveredStream {
                    key: entry.key.clone(),
                    site_id: entry.site_id,
                    environment: entry.environment,
                    topology_namespace: entry.topology_namespace,
                    stream_type: "health".to_string(),
                };
                all_discovered.push(stream);
                total_health += 1;
            }
        }

        // Process broadcast streams (shared across all sites)
        if self.config.include_broadcast {
            for entry in response.streams.broadcast {
                let stream = DiscoveredStream {
                    key: entry.key.clone(),
                    site_id: entry.site_id,
                    environment: entry.environment,
                    topology_namespace: entry.topology_namespace,
                    stream_type: "broadcast".to_string(),
                };
                all_discovered.push(stream);
                total_broadcast += 1;
            }
        }

        // Get counts before consuming the vectors
        let total_registration = response.streams.registration.len();
        let total_global = response.streams.global.len();

        // Process registration streams (shared - for service registrations from gNode-Client)
        for entry in response.streams.registration {
            let stream = DiscoveredStream {
                key: entry.key.clone(),
                site_id: entry.site_id,
                environment: entry.environment,
                topology_namespace: entry.topology_namespace,
                stream_type: "registration".to_string(),
            };
            all_discovered.push(stream);
        }

        // Process global streams (shared - for future multi-topology coordination)
        for entry in response.streams.global {
            let stream = DiscoveredStream {
                key: entry.key.clone(),
                site_id: entry.site_id,
                environment: entry.environment,
                topology_namespace: entry.topology_namespace,
                stream_type: "global".to_string(),
            };
            all_discovered.push(stream);
        }

        // Update internal registry
        {
            let mut streams = match self.streams.write() {
                Ok(guard) => guard,
                Err(poisoned) => {
                    error!("StreamDiscoveryManager streams lock poisoned — prior thread panicked. Recovering guard; data may be stale until next refresh.");
                    poisoned.into_inner()
                }
            };
            streams.clear();
            for stream in &all_discovered {
                streams.insert(stream.key.clone(), stream.clone());
            }
        }

        debug!(
            "Discovered {} streams for {} sites (unified: {}, health: {}, broadcast: {}, registration: {}, global: {})",
            all_discovered.len(),
            response.site_count,
            total_unified,
            total_health,
            total_broadcast,
            total_registration,
            total_global
        );

        // Verify expected counts. The message spells out the full
        // formula — the old "({}×{})" printed sites×per-site as if that
        // were the whole expectation, hiding the shared-stream term and
        // making the mismatch look like broken arithmetic.
        if response.expected_total > 0 && all_discovered.len() != response.expected_total as usize {
            warn!(
                "Stream count mismatch: discovered {} but expected {} ({} sites × {} per-site + {} shared)",
                all_discovered.len(),
                response.expected_total,
                response.site_count,
                response.expected_per_site,
                response.expected_shared
            );
        }

        Ok(all_discovered)
    }

    /// Refresh both sites and streams
    pub fn refresh(&self, conn: &mut Connection) -> IntegrationResult<()> {
        self.discover_sites(conn)?;
        self.discover_streams(conn)?;
        Ok(())
    }

    /// Refresh if needed (based on configured interval)
    pub fn refresh_if_needed(&self, conn: &mut Connection) -> IntegrationResult<bool> {
        if self.needs_refresh() {
            self.refresh(conn)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Signal that workers should sync streams immediately
    /// Called by broadcast handler when environment_changed is received
    pub fn signal_immediate_sync(&self) {
        self.needs_immediate_sync.store(true, Ordering::SeqCst);
        info!("🔔 Stream sync signaled - workers will update on next iteration");
    }

    /// Check if immediate sync was signaled and clear the flag
    /// Returns true if sync was requested (flag was set)
    pub fn check_and_clear_sync_signal(&self) -> bool {
        self.needs_immediate_sync.swap(false, Ordering::SeqCst)
    }

    /// Check if immediate sync is needed (without clearing)
    pub fn is_sync_signaled(&self) -> bool {
        self.needs_immediate_sync.load(Ordering::SeqCst)
    }

    /// Get all discovered streams
    pub fn get_streams(&self) -> Vec<DiscoveredStream> {
        let streams = match self.streams.read() {
            Ok(guard) => guard,
            Err(poisoned) => {
                warn!("StreamDiscoveryManager streams lock poisoned, recovering");
                poisoned.into_inner()
            }
        };
        streams.values().cloned().collect()
    }

    /// Get unified streams only
    pub fn get_unified_streams(&self) -> Vec<DiscoveredStream> {
        let streams = match self.streams.read() {
            Ok(guard) => guard,
            Err(poisoned) => {
                warn!("StreamDiscoveryManager streams lock poisoned, recovering");
                poisoned.into_inner()
            }
        };
        streams
            .values()
            .filter(|s| s.stream_type == "unified")
            .cloned()
            .collect()
    }

    /// Get health streams only
    pub fn get_health_streams(&self) -> Vec<DiscoveredStream> {
        let streams = match self.streams.read() {
            Ok(guard) => guard,
            Err(poisoned) => {
                warn!("StreamDiscoveryManager streams lock poisoned, recovering");
                poisoned.into_inner()
            }
        };
        streams
            .values()
            .filter(|s| s.stream_type == "health")
            .cloned()
            .collect()
    }

    /// Get broadcast streams only
    pub fn get_broadcast_streams(&self) -> Vec<DiscoveredStream> {
        let streams = match self.streams.read() {
            Ok(guard) => guard,
            Err(poisoned) => {
                warn!("StreamDiscoveryManager streams lock poisoned, recovering");
                poisoned.into_inner()
            }
        };
        streams
            .values()
            .filter(|s| s.stream_type == "broadcast")
            .cloned()
            .collect()
    }

    /// Get stream keys as a list (useful for XREADGROUP across multiple streams)
    pub fn get_stream_keys(&self) -> Vec<String> {
        let streams = match self.streams.read() {
            Ok(guard) => guard,
            Err(poisoned) => {
                warn!("StreamDiscoveryManager streams lock poisoned, recovering");
                poisoned.into_inner()
            }
        };
        streams.keys().cloned().collect()
    }

    /// Get unified stream keys only
    pub fn get_unified_stream_keys(&self) -> Vec<String> {
        self.get_unified_streams()
            .iter()
            .map(|s| s.key.clone())
            .collect()
    }

    /// Get streams by site
    pub fn get_streams_for_site(&self, site_id: &str) -> Vec<DiscoveredStream> {
        let streams = match self.streams.read() {
            Ok(guard) => guard,
            Err(poisoned) => {
                warn!("StreamDiscoveryManager streams lock poisoned, recovering");
                poisoned.into_inner()
            }
        };
        streams
            .values()
            .filter(|s| s.site_id.as_deref() == Some(site_id))
            .cloned()
            .collect()
    }

    /// Get streams that were newly added since last check
    pub fn take_newly_added(&self) -> HashSet<String> {
        let mut added = match self.newly_added.write() {
            Ok(guard) => guard,
            Err(poisoned) => {
                warn!("StreamDiscoveryManager newly_added lock poisoned, recovering");
                poisoned.into_inner()
            }
        };
        std::mem::take(&mut *added)
    }

    /// Check if any streams have been newly added
    pub fn has_new_streams(&self) -> bool {
        let added = match self.newly_added.read() {
            Ok(guard) => guard,
            Err(poisoned) => {
                warn!("StreamDiscoveryManager newly_added lock poisoned, recovering");
                poisoned.into_inner()
            }
        };
        !added.is_empty()
    }

    /// Get all registered sites
    pub fn get_sites(&self) -> Vec<RegisteredSite> {
        let sites = match self.sites.read() {
            Ok(guard) => guard,
            Err(poisoned) => {
                warn!("StreamDiscoveryManager sites lock poisoned, recovering");
                poisoned.into_inner()
            }
        };
        sites.values().cloned().collect()
    }

    /// Get site IDs only
    pub fn get_site_ids(&self) -> Vec<String> {
        let sites = match self.sites.read() {
            Ok(guard) => guard,
            Err(poisoned) => {
                warn!("StreamDiscoveryManager sites lock poisoned, recovering");
                poisoned.into_inner()
            }
        };
        sites.keys().cloned().collect()
    }

    /// Get count of discovered streams
    pub fn stream_count(&self) -> usize {
        let streams = match self.streams.read() {
            Ok(guard) => guard,
            Err(poisoned) => {
                warn!("StreamDiscoveryManager streams lock poisoned, recovering");
                poisoned.into_inner()
            }
        };
        streams.len()
    }

    /// Get count of registered sites
    pub fn site_count(&self) -> usize {
        let sites = match self.sites.read() {
            Ok(guard) => guard,
            Err(poisoned) => {
                warn!("StreamDiscoveryManager sites lock poisoned, recovering");
                poisoned.into_inner()
            }
        };
        sites.len()
    }
}

/// Create streams for a new site using GNODE_PROVISION_SERVICE
pub fn create_site_streams(
    conn: &mut Connection,
    site_id: &str,
    environments: Option<&[&str]>,
) -> IntegrationResult<serde_json::Value> {
    info!("Creating streams for site: {}", site_id);

    let envs_json = environments
        .map(|e| serde_json::to_string(e).unwrap_or_default())
        .unwrap_or_default();

    let result: Value = cmd("FCALL")
        .arg("GNODE_PROVISION_SERVICE")
        .arg(0)  // No keys
        .arg(site_id)
        .arg(&envs_json)
        .query(conn)
        .map_err(|e| valkey_function_error(format!("GNODE_PROVISION_SERVICE failed: {}", e)))?;

    // Parse the JSON response
    let json_str = parse_fcall_response(&result)?;
    serde_json::from_str(&json_str)
        .map_err(|e| valkey_function_error(format!("Failed to parse response: {}", e)))
}

/// Ensure consumer groups exist on a stream
pub fn ensure_consumer_groups(
    conn: &mut Connection,
    stream_key: &str,
    groups: &[&str],
    start_id: Option<&str>,
) -> IntegrationResult<serde_json::Value> {
    debug!("Ensuring consumer groups on stream: {}", stream_key);

    let groups_json = serde_json::to_string(groups)
        .map_err(|e| valkey_function_error(format!("Failed to serialize groups: {}", e)))?;

    let start = start_id.unwrap_or("$");

    let result: Value = cmd("FCALL")
        .arg("GNODE_STREAM_ENSURE_CONSUMER_GROUPS")
        .arg(1)  // One key
        .arg(stream_key)
        .arg(&groups_json)
        .arg(start)
        .query(conn)
        .map_err(|e| valkey_function_error(format!("GNODE_STREAM_ENSURE_CONSUMER_GROUPS failed: {}", e)))?;

    let json_str = parse_fcall_response(&result)?;
    serde_json::from_str(&json_str)
        .map_err(|e| valkey_function_error(format!("Failed to parse response: {}", e)))
}

/// Get stream information for a site
pub fn get_site_streams(
    conn: &mut Connection,
    site_id: &str,
    environment: Option<&str>,
    stream_type: Option<&str>,
) -> IntegrationResult<serde_json::Value> {
    let result: Value = cmd("FCALL")
        .arg("GNODE_STREAM_GET_SITE_STREAMS")
        .arg(0)  // No keys
        .arg(site_id)
        .arg(environment.unwrap_or(""))
        .arg(stream_type.unwrap_or(""))
        .query(conn)
        .map_err(|e| valkey_function_error(format!("GNODE_STREAM_GET_SITE_STREAMS failed: {}", e)))?;

    let json_str = parse_fcall_response(&result)?;
    serde_json::from_str(&json_str)
        .map_err(|e| valkey_function_error(format!("Failed to parse response: {}", e)))
}

/// Helper function to parse FCALL response into a string
fn parse_fcall_response(result: &Value) -> IntegrationResult<String> {
    match result {
        Value::BulkString(bytes) => Ok(String::from_utf8_lossy(bytes).to_string()),
        Value::SimpleString(s) => Ok(s.clone()),
        Value::Array(arr) if !arr.is_empty() => {
            match &arr[0] {
                Value::BulkString(bytes) => Ok(String::from_utf8_lossy(bytes).to_string()),
                _ => Err(valkey_function_error("Unexpected nested response type".to_string())),
            }
        }
        _ => Err(valkey_function_error(format!("Unexpected response type: {:?}", result))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stream_discovery_config_default() {
        let config = StreamDiscoveryConfig::default();
        assert_eq!(config.refresh_interval_secs, 60);
        assert_eq!(config.environment, "all");  // Default is "all" to listen to ALL DTAP environments
        assert!(config.include_broadcast);
        assert!(config.include_health);
        assert!(config.include_unified);
    }

    #[test]
    fn test_stream_discovery_manager_creation() {
        let manager = StreamDiscoveryManager::for_environment("staging");
        assert_eq!(manager.environment(), "staging");
        assert!(manager.needs_refresh());
        assert_eq!(manager.stream_count(), 0);
        assert_eq!(manager.site_count(), 0);
    }

    #[test]
    fn test_discovered_stream_equality() {
        let stream1 = DiscoveredStream {
            key: "{site1}:gnode:unified:production".to_string(),
            site_id: Some("site1".to_string()),
            environment: Some("production".to_string()),
            topology_namespace: None,
            stream_type: "unified".to_string(),
        };

        let stream2 = DiscoveredStream {
            key: "{site1}:gnode:unified:production".to_string(),
            site_id: Some("site1".to_string()),
            environment: Some("production".to_string()),
            topology_namespace: None,
            stream_type: "unified".to_string(),
        };

        assert_eq!(stream1, stream2);
    }

    #[test]
    fn test_shared_stream_no_site_id() {
        // Registration and global streams don't have site_id
        let registration = DiscoveredStream {
            key: "{geodineum}:gnode:unified".to_string(),
            site_id: None,
            environment: None,
            topology_namespace: Some("geodineum".to_string()),
            stream_type: "registration".to_string(),
        };

        let global = DiscoveredStream {
            key: "geodineum:unified:stream".to_string(),
            site_id: None,
            environment: None,
            topology_namespace: Some("geodineum".to_string()),
            stream_type: "global".to_string(),
        };

        assert!(registration.site_id.is_none());
        assert!(global.site_id.is_none());
        assert_eq!(registration.stream_type, "registration");
        assert_eq!(global.stream_type, "global");
    }
}
