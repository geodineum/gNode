//! Service Discovery Module — Daemon-driven periodic registration from config files
//!
//! The daemon periodically scans whitelisted YAML config files and automatically
//! registers discovered services for all known sites. This replaces the old pattern
//! where PHP clients self-registered on every page load.
//!
//! Architecture:
//! - LOCAL services: Discovered from YAML config by this module (zero PHP involvement)
//! - REMOTE services: Continue using stream-based `registerService` command
//!
//! Discovery paths are scanned in order, and services from ALL paths are aggregated.
//! This allows third-party deployments to contribute their own services by adding
//! paths to the whitelist (--discovery-config-paths).
//!
//! Config file conventions (when a directory path is whitelisted):
//! - `gnode_services.yaml`      — Generic convention for any service-tier deployment
//! - `geometric_topology.yaml`  — gCore convention (framework-level tool-tier services)
//!
//! The first match per directory wins. Direct file paths bypass this probe.
//!
//! Minimal service config (gnode_services.yaml):
//! ```yaml
//! services:
//!   - id: "MyService"
//!     metadata:
//!       description: "What this service does"
//!       type: "service"
//!       tier: "SERVICE"
//!     capabilities:
//!       - name: "protocol"
//!         value: "http_rest"
//!       - name: "domain_primary"
//!         value: "inference"
//! ```
//!
//! Discovery paths come from three sources (merged):
//! 1. CLI `--discovery-config-paths` (comma-separated, always included)
//! 2. Paths manifest file (default: /etc/geodineum/components/gnode-daemon/discovery-paths.conf)
//!    - Re-read on each scan cycle when mtime changes — no daemon restart needed
//!    - One path per line, `#` comments, empty lines ignored
//! 3. Default gCore location (auto-detected, always included as fallback)
//!
//! The daemon admin controls discovery via CLI flags:
//! - `--service-discovery` (bool): enable/disable
//! - `--discovery-interval-secs` (u64): scan interval
//! - `--discovery-config-paths` (comma-separated): additional whitelisted paths
//! - `--discovery-paths-file` (path): paths manifest file
//!
//! Future direction: auto-scan OpenAPI specs from whitelisted /api directories
//! for endpoint+format registration alongside capability-based services.

use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime};
use log::{info, warn, debug};

use crate::tool_registration::{
    self, CapabilitySchema, TranslatedService,
};
use crate::{Result, GeometricError};

// ============================================================================
// Configuration
// ============================================================================

/// Default path for the discovery paths manifest file.
const DEFAULT_PATHS_FILE: &str = "/etc/geodineum/components/gnode-daemon/discovery-paths.conf";

/// Configuration for daemon-side service discovery.
pub struct ServiceDiscoveryConfig {
    /// Whether service discovery is enabled (--service-discovery)
    pub enabled: bool,
    /// How often to scan for config changes in seconds (--discovery-interval-secs)
    pub scan_interval_secs: u64,
    /// Additional whitelisted config paths (--discovery-config-paths)
    /// Each path is scanned for gnode_services.yaml / geometric_topology.yaml.
    /// Can be a directory (probes for recognized filenames inside)
    /// or a direct file path. Services from ALL paths are aggregated.
    pub extra_config_paths: Vec<PathBuf>,
    /// Override schema path (reuses --schema if provided via register-tools)
    pub schema_path: Option<PathBuf>,
    /// Path to discovery-paths.conf manifest (re-read each scan cycle on mtime change).
    /// One path per line, `#` comments and empty lines ignored.
    pub discovery_paths_file: Option<PathBuf>,
}

impl Default for ServiceDiscoveryConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            scan_interval_secs: 120,
            extra_config_paths: Vec::new(),
            schema_path: None,
            discovery_paths_file: Some(PathBuf::from(DEFAULT_PATHS_FILE)),
        }
    }
}

// ============================================================================
// Result type
// ============================================================================

/// Result of a discovery + registration cycle.
pub struct RegistrationResult {
    /// Number of services successfully registered
    pub registered: usize,
    /// Number of registration errors
    pub errors: usize,
    /// True if config was unchanged and registration was skipped
    pub skipped: bool,
    /// Number of sites services were registered for
    pub sites: usize,
}

// ============================================================================
// Tracked config source (per-path mtime tracking)
// ============================================================================

/// A discovered config file with its mtime for change tracking.
#[derive(Debug)]
struct TrackedConfigSource {
    path: PathBuf,
    mtime: Option<SystemTime>,
}

impl TrackedConfigSource {
    fn new(path: PathBuf) -> Self {
        Self { path, mtime: None }
    }

    /// Check if the file has changed since last recorded mtime.
    fn has_changed(&self) -> bool {
        match get_file_mtime(&self.path) {
            Ok(current) => self.mtime.as_ref() != Some(&current),
            Err(_) => false, // File disappeared — will be caught on reload
        }
    }

    /// Update the recorded mtime.
    fn update_mtime(&mut self) {
        self.mtime = get_file_mtime(&self.path).ok();
    }
}

// ============================================================================
// Manager
// ============================================================================

/// Manages periodic discovery of services from local YAML config files.
///
/// Follows the same pattern as `StreamDiscoveryManager`:
/// - Periodic scan on configurable interval
/// - File mtime tracking to avoid re-reading unchanged configs
/// - Caches translated services between scans
///
/// Supports multiple config sources (aggregated):
/// - Default: gCore's geometric_topology.yaml (auto-resolved)
/// - Extra: any number of --discovery-config-paths (whitelisted)
/// - Each source contributes services; all are registered per-site
pub struct ServiceDiscoveryManager {
    config: ServiceDiscoveryConfig,
    /// Cached capability schema
    schema: Option<CapabilitySchema>,
    /// Pre-translated services ready for registration (aggregated from all sources)
    translated: Vec<TranslatedService>,
    /// All discovered config sources with mtime tracking
    config_sources: Vec<TrackedConfigSource>,
    /// Resolved schema file path (default: service_schema.yaml)
    resolved_schema_path: Option<PathBuf>,
    /// Schema file mtime
    schema_mtime: Option<SystemTime>,
    /// When the last scan completed
    last_scan: Option<Instant>,
    /// Whether paths have been resolved at least once
    paths_resolved: bool,
    /// Tracked mtime of the discovery-paths.conf manifest file
    paths_file_mtime: Option<SystemTime>,
}

impl ServiceDiscoveryManager {
    pub fn new(config: ServiceDiscoveryConfig) -> Self {
        Self {
            config,
            schema: None,
            translated: Vec::new(),
            config_sources: Vec::new(),
            resolved_schema_path: None,
            schema_mtime: None,
            last_scan: None,
            paths_resolved: false,
            paths_file_mtime: None,
        }
    }

    /// Whether service discovery is enabled.
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Get the configured scan interval in seconds.
    pub fn scan_interval_secs(&self) -> u64 {
        self.config.scan_interval_secs
    }

    /// Mutable access to config (for CLI flag overrides after construction).
    pub fn config_mut(&mut self) -> &mut ServiceDiscoveryConfig {
        // Reset paths_resolved since config changed
        self.paths_resolved = false;
        &mut self.config
    }

    /// Whether it's time for a new scan.
    pub fn needs_scan(&self) -> bool {
        if !self.config.enabled {
            return false;
        }
        match self.last_scan {
            None => true,
            Some(last) => last.elapsed().as_secs() >= self.config.scan_interval_secs,
        }
    }

    /// Check if the paths manifest file has changed and needs re-resolution.
    fn paths_file_changed(&self) -> bool {
        if let Some(ref paths_file) = self.config.discovery_paths_file {
            match get_file_mtime(paths_file) {
                Ok(current) => self.paths_file_mtime.as_ref() != Some(&current),
                Err(_) => false, // File doesn't exist — use CLI paths only
            }
        } else {
            false
        }
    }

    /// Read discovery paths from the manifest file (one path per line).
    fn read_paths_file(&self) -> Vec<PathBuf> {
        let paths_file = match self.config.discovery_paths_file.as_ref() {
            Some(p) => p,
            None => return Vec::new(),
        };
        match std::fs::read_to_string(paths_file) {
            Ok(content) => {
                content.lines()
                    .map(|line| line.trim())
                    .filter(|line| !line.is_empty() && !line.starts_with('#'))
                    .map(PathBuf::from)
                    .collect()
            }
            Err(_) => Vec::new(),
        }
    }

    /// Resolve a single path entry to a config source.
    /// Directories are probed for recognized filenames; files are used directly.
    fn resolve_path_entry(path: &Path) -> Option<PathBuf> {
        // Recognized config filenames (checked in order when path is a directory)
        const CONFIG_FILENAMES: &[&str] = &[
            "gnode_services.yaml",          // Generic convention for any service
            "geometric_topology.yaml",      // gCore convention
        ];

        if path.is_file() {
            return Some(path.to_path_buf());
        }
        // Directory — probe for recognized config filenames
        for filename in CONFIG_FILENAMES {
            let candidate = path.join(filename);
            if candidate.exists() {
                return Some(candidate);
            }
        }
        None
    }

    /// Resolve all config file paths from all sources.
    ///
    /// Called on first scan, and re-called when the paths manifest file changes.
    /// Merges paths from three sources:
    /// 1. CLI `--discovery-config-paths` (always included)
    /// 2. Paths manifest file (discovery-paths.conf, re-read on mtime change)
    /// 3. Default gCore location (auto-detected fallback)
    fn resolve_paths(&mut self) {
        // On first call, always resolve. After that, only when paths file changes.
        if self.paths_resolved && !self.paths_file_changed() {
            return;
        }

        let is_refresh = self.paths_resolved;

        // Resolve schema path (only on first call)
        if !self.paths_resolved {
            self.resolved_schema_path = tool_registration::find_schema_path(
                self.config.schema_path.as_ref()
            );
        }

        // Collect ALL config sources from all path providers
        self.config_sources.clear();
        let mut seen_paths: Vec<PathBuf> = Vec::new();

        // Helper: add a resolved path if not already tracked
        let add_source = |resolved: PathBuf, sources: &mut Vec<TrackedConfigSource>, seen: &mut Vec<PathBuf>| {
            if !seen.contains(&resolved) {
                seen.push(resolved.clone());
                sources.push(TrackedConfigSource::new(resolved));
            }
        };

        // 1. CLI paths (--discovery-config-paths, highest priority)
        let cli_paths: Vec<PathBuf> = self.config.extra_config_paths.clone();
        for path in &cli_paths {
            if let Some(resolved) = Self::resolve_path_entry(path) {
                add_source(resolved, &mut self.config_sources, &mut seen_paths);
            } else {
                debug!("[service-discovery] CLI path not found: {:?}", path);
            }
        }

        // 2. Paths manifest file (dynamic, no restart needed)
        let file_paths = self.read_paths_file();
        for path in &file_paths {
            if let Some(resolved) = Self::resolve_path_entry(path) {
                add_source(resolved, &mut self.config_sources, &mut seen_paths);
            } else {
                debug!("[service-discovery] Manifest path not found: {:?}", path);
            }
        }

        // 3. Default gCore location (always checked as fallback)
        if let Some(default_path) = tool_registration::find_config_path(None) {
            add_source(default_path, &mut self.config_sources, &mut seen_paths);
        }

        // Update paths file mtime
        if let Some(ref paths_file) = self.config.discovery_paths_file {
            self.paths_file_mtime = get_file_mtime(paths_file).ok();
        }

        self.paths_resolved = true;

        if !is_refresh {
            if let Some(ref p) = self.resolved_schema_path {
                debug!("[service-discovery] Schema path: {:?}", p);
            } else {
                warn!("[service-discovery] Could not find service_schema.yaml");
            }
        }

        if is_refresh {
            info!("[service-discovery] Paths re-resolved: {} config source(s)", self.config_sources.len());
        } else {
            info!("[service-discovery] {} config source(s) resolved", self.config_sources.len());
        }
    }

    /// Check if any config files have changed since last scan.
    fn files_changed(&self) -> bool {
        // Check paths manifest file (triggers re-resolution + reload)
        if self.paths_file_changed() {
            return true;
        }
        // Check schema
        if let Some(ref path) = self.resolved_schema_path {
            if let Ok(mtime) = get_file_mtime(path) {
                if self.schema_mtime.as_ref() != Some(&mtime) {
                    return true;
                }
            }
        }
        // Check all config sources
        for source in &self.config_sources {
            if source.has_changed() {
                return true;
            }
        }
        false
    }

    /// Reload all config files and re-translate services (aggregated from all sources).
    fn reload_and_translate(&mut self) -> Result<()> {
        let schema_path = self.resolved_schema_path.as_ref()
            .ok_or_else(|| GeometricError::Other(
                "Service discovery: service_schema.yaml not found".to_string()
            ))?;

        if self.config_sources.is_empty() {
            return Err(GeometricError::Other(
                "Service discovery: no geometric_topology.yaml sources found".to_string()
            ));
        }

        // Load schema
        let schema = tool_registration::load_schema(schema_path)?;

        // Aggregate services from ALL config sources
        let mut all_translated: Vec<TranslatedService> = Vec::new();

        for source in &mut self.config_sources {
            match tool_registration::load_service_definitions(&source.path) {
                Ok(services) => {
                    if services.is_empty() {
                        warn!("[service-discovery] No services found in {:?}", source.path);
                    } else {
                        let translated = tool_registration::translate_all_services(&services, &schema);
                        info!("[service-discovery] Translated {} services from {:?}",
                             translated.len(), source.path);
                        all_translated.extend(translated);
                    }
                    source.update_mtime();
                }
                Err(e) => {
                    warn!("[service-discovery] Failed to load {:?}: {:?}", source.path, e);
                }
            }
        }

        self.translated = all_translated;

        // Update schema mtime
        if let Ok(mtime) = get_file_mtime(schema_path) {
            self.schema_mtime = Some(mtime);
        }

        self.schema = Some(schema);
        Ok(())
    }

    /// Top-level method: discover services from config and register for all sites.
    ///
    /// Called periodically by the daemon's heartbeat loop or ServiceDiscoveryWorker.
    /// Returns registration results or skips if config is unchanged.
    pub fn discover_and_register(
        &mut self,
        conn: &mut redis::Connection,
    ) -> Result<RegistrationResult> {
        // Resolve paths on first call
        self.resolve_paths();

        // Check if we have the required files
        if self.config_sources.is_empty() || self.resolved_schema_path.is_none() {
            self.last_scan = Some(Instant::now());
            return Ok(RegistrationResult {
                registered: 0,
                errors: 0,
                skipped: true,
                sites: 0,
            });
        }

        // Check if files changed
        let needs_reload = self.schema.is_none() || self.files_changed();

        if !needs_reload && !self.translated.is_empty() {
            // Config unchanged, skip registration
            self.last_scan = Some(Instant::now());
            debug!("[service-discovery] Config unchanged, skipping registration");
            return Ok(RegistrationResult {
                registered: 0,
                errors: 0,
                skipped: true,
                sites: 0,
            });
        }

        // Reload and translate
        self.reload_and_translate()?;

        if self.translated.is_empty() {
            self.last_scan = Some(Instant::now());
            return Ok(RegistrationResult {
                registered: 0,
                errors: 0,
                skipped: false,
                sites: 0,
            });
        }

        // Discover registered sites
        let sites = tool_registration::discover_registered_sites(conn)?;

        if sites.is_empty() {
            warn!("[service-discovery] No registered sites found, skipping registration");
            self.last_scan = Some(Instant::now());
            return Ok(RegistrationResult {
                registered: 0,
                errors: 0,
                skipped: false,
                sites: 0,
            });
        }

        // Register services for each site
        let mut total_registered = 0;
        let mut total_errors = 0;

        for site_id in &sites {
            match tool_registration::register_services_for_site(
                conn, site_id, &self.translated, ""
            ) {
                Ok((registered, errors)) => {
                    total_registered += registered;
                    total_errors += errors;
                }
                Err(e) => {
                    warn!("[service-discovery] Failed to register for site {}: {:?}",
                         site_id, e);
                    total_errors += self.translated.len();
                }
            }
        }

        info!("[service-discovery] Registered {} services for {} sites ({} errors)",
             total_registered, sites.len(), total_errors);

        self.last_scan = Some(Instant::now());

        Ok(RegistrationResult {
            registered: total_registered,
            errors: total_errors,
            skipped: false,
            sites: sites.len(),
        })
    }
}

// ============================================================================
// Helpers
// ============================================================================

/// Get file modification time, returning an error if the file doesn't exist.
fn get_file_mtime(path: &Path) -> std::result::Result<SystemTime, std::io::Error> {
    std::fs::metadata(path)?.modified()
}
