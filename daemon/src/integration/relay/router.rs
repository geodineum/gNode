//! Relay target resolution logic
//!
//! Resolves the `_rt` field value to a concrete target site + stream.
//! Supports three resolution modes:
//!   1. Entity ID lookup — find which site owns the entity on its services topology
//!   2. Site ID — direct stream construction
//!   3. JSON capability query — geometric discovery on namespace topology

use log::{debug, info};
use redis::Connection;
use std::sync::{Arc, RwLock};

use crate::integration::stream_discovery::StreamDiscoveryManager;

/// Result of resolving a relay target
#[derive(Debug)]
pub enum RelayDecision {
    /// Target found — relay to this stream
    Forward {
        target_site_id: String,
        target_stream_key: String,
        target_entity_id: String,
    },
    /// Target is on the SAME site — process locally (not a relay)
    Local,
    /// Target not found
    NotFound(String),
    /// Error during resolution
    Error(String),
}

/// Resolve a relay target (`_rt` field) to a concrete stream destination.
///
/// Resolution order:
///   1. If `_rt` starts with `{` → JSON capability query (geometric discovery)
///   2. If `_rt` contains `:gnode:` → treated as an explicit stream key
///   3. Otherwise → entity ID lookup via GNODE_TOPO_FIND_ENTITY_SITE
///
/// # Arguments
/// * `conn` — ValKey connection (gnode_daemon ACL)
/// * `relay_target` — raw value of the `_rt` field
/// * `current_site_id` — the site_id of the stream we read this command from
/// * `current_stream` — the stream key we read this command from (used for environment extraction)
/// * `shared_discovery` — optional StreamDiscoveryManager for site lookups
/// * `debug_mode` — verbose logging
pub fn resolve_relay_target(
    conn: &mut Connection,
    relay_target: &str,
    current_site_id: &str,
    current_stream: &str,
    shared_discovery: Option<&Arc<RwLock<StreamDiscoveryManager>>>,
    debug_mode: bool,
) -> RelayDecision {
    if relay_target.is_empty() {
        return RelayDecision::NotFound("Empty relay target".to_string());
    }

    if debug_mode {
        info!("Resolving relay target: '{}' (source site: {})", relay_target, current_site_id);
    }

    // --- Mode 1: JSON capability query ---
    if relay_target.starts_with('{') {
        return resolve_capability_query(
            conn, relay_target, current_site_id, current_stream,
            shared_discovery, debug_mode,
        );
    }

    // --- Mode 2: Explicit stream key (contains ":gnode:") ---
    if relay_target.contains(":gnode:") {
        // Treat as a direct stream key — extract site_id from the key
        if let Some(site_id) = relay_target.split(":gnode:").next() {
            if site_id == current_site_id {
                return RelayDecision::Local;
            }
            return RelayDecision::Forward {
                target_site_id: site_id.to_string(),
                target_stream_key: relay_target.to_string(),
                target_entity_id: String::new(),
            };
        }
        return RelayDecision::Error("Could not extract site_id from stream key".to_string());
    }

    // --- Mode 3: Entity ID or site_id lookup ---
    // First, gather all known site_ids from discovery
    let site_ids = collect_site_ids(shared_discovery, current_site_id);

    if debug_mode {
        debug!("Searching for entity '{}' across {} sites", relay_target, site_ids.len());
    }

    // Check if _rt matches a known site_id directly
    if site_ids.contains(&relay_target.to_string()) {
        if relay_target == current_site_id {
            return RelayDecision::Local;
        }
        // Resolve to that site's unified stream
        return match resolve_stream_for_site(relay_target, current_stream, shared_discovery) {
            Some(stream_key) => RelayDecision::Forward {
                target_site_id: relay_target.to_string(),
                target_stream_key: stream_key,
                target_entity_id: String::new(),
            },
            None => RelayDecision::NotFound(format!(
                "Site '{}' found but no unified stream available",
                relay_target
            )),
        };
    }

    // Otherwise, treat as entity_id — search across site topologies
    match find_entity_site(conn, relay_target, &site_ids, debug_mode) {
        Ok(Some(target_site_id)) => {
            if target_site_id == current_site_id {
                return RelayDecision::Local;
            }
            match resolve_stream_for_site(&target_site_id, current_stream, shared_discovery) {
                Some(stream_key) => RelayDecision::Forward {
                    target_site_id,
                    target_stream_key: stream_key,
                    target_entity_id: relay_target.to_string(),
                },
                None => RelayDecision::NotFound(format!(
                    "Entity '{}' found on site '{}' but no unified stream available",
                    relay_target, target_site_id
                )),
            }
        }
        Ok(None) => RelayDecision::NotFound(format!(
            "Entity '{}' not found in any site topology",
            relay_target
        )),
        Err(e) => RelayDecision::Error(format!(
            "Error searching for entity '{}': {}",
            relay_target, e
        )),
    }
}

/// Collect all known site_ids from discovery + current site
fn collect_site_ids(
    shared_discovery: Option<&Arc<RwLock<StreamDiscoveryManager>>>,
    current_site_id: &str,
) -> Vec<String> {
    let mut site_ids = Vec::new();

    if let Some(discovery) = shared_discovery {
        if let Ok(disc) = discovery.read() {
            site_ids = disc.get_site_ids();
        }
    }

    // Ensure current site is included
    if !site_ids.contains(&current_site_id.to_string()) {
        site_ids.push(current_site_id.to_string());
    }

    site_ids
}

/// Use Lua function GNODE_TOPO_FIND_ENTITY_SITE to search across site topologies
fn find_entity_site(
    conn: &mut Connection,
    entity_id: &str,
    site_ids: &[String],
    debug_mode: bool,
) -> Result<Option<String>, String> {
    let site_ids_json =
        serde_json::to_string(site_ids).map_err(|e| format!("JSON encode error: {}", e))?;

    let result: redis::RedisResult<String> = redis::cmd("FCALL")
        .arg("GNODE_TOPO_FIND_ENTITY_SITE")
        .arg(0) // 0 keys — reads from multiple key patterns
        .arg(entity_id)
        .arg(&site_ids_json)
        .query(conn);

    match result {
        Ok(json_str) => {
            match serde_json::from_str::<serde_json::Value>(&json_str) {
                Ok(val) => {
                    let ok = val.get("ok").and_then(|v| v.as_bool()).unwrap_or(false);
                    if ok {
                        let site_id = val
                            .get("site_id")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string());
                        if debug_mode {
                            info!(
                                "Entity '{}' found on site '{}'",
                                entity_id,
                                site_id.as_deref().unwrap_or("unknown")
                            );
                        }
                        Ok(site_id)
                    } else {
                        if debug_mode {
                            debug!(
                                "Entity '{}' not found: {}",
                                entity_id,
                                val.get("error")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("unknown")
                            );
                        }
                        Ok(None)
                    }
                }
                Err(e) => Err(format!("Failed to parse FCALL response: {}", e)),
            }
        }
        Err(e) => {
            // If the Lua function doesn't exist yet, fall back to manual scan
            let err_str = e.to_string();
            if err_str.contains("Function not found") || err_str.contains("NOSCRIPT") {
                if debug_mode {
                    debug!(
                        "GNODE_TOPO_FIND_ENTITY_SITE not loaded, falling back to manual scan"
                    );
                }
                return find_entity_site_manual(conn, entity_id, site_ids, debug_mode);
            }
            Err(format!("FCALL GNODE_TOPO_FIND_ENTITY_SITE failed: {}", e))
        }
    }
}

/// Manual fallback: scan each site's services topology for the entity
fn find_entity_site_manual(
    conn: &mut Connection,
    entity_id: &str,
    site_ids: &[String],
    debug_mode: bool,
) -> Result<Option<String>, String> {
    for site_id in site_ids {
        let topology_key = crate::GeometricTopology::get_services_topology_key(site_id);
        let entities_key = format!("{}:entities", topology_key);

        let exists: redis::RedisResult<bool> =
            redis::cmd("HEXISTS").arg(&entities_key).arg(entity_id).query(conn);

        match exists {
            Ok(true) => {
                if debug_mode {
                    info!(
                        "Manual scan: entity '{}' found on site '{}' (topology: {})",
                        entity_id, site_id, topology_key
                    );
                }
                return Ok(Some(site_id.clone()));
            }
            Ok(false) => continue,
            Err(e) => {
                if debug_mode {
                    debug!(
                        "Manual scan: error checking {} for entity '{}': {}",
                        entities_key, entity_id, e
                    );
                }
                continue;
            }
        }
    }
    Ok(None)
}

/// Resolve a site_id to its unified stream key, matching the source environment.
///
/// Tries discovery manager first (filtering by environment), then falls back
/// to constructing from the current stream's environment pattern.
fn resolve_stream_for_site(
    target_site_id: &str,
    current_stream: &str,
    shared_discovery: Option<&Arc<RwLock<StreamDiscoveryManager>>>,
) -> Option<String> {
    // Extract environment from current_stream pattern: {site_id}:gnode:unified:{environment}
    let source_env = current_stream.rsplit(':').next().unwrap_or("production");

    // Try discovery manager — filter by matching environment
    if let Some(discovery) = shared_discovery {
        if let Ok(disc) = discovery.read() {
            let streams = disc.get_streams_for_site(target_site_id);
            // Prefer unified stream matching the source environment
            if let Some(unified) = streams.iter().find(|s| {
                s.stream_type == "unified"
                    && s.environment.as_deref() == Some(source_env)
            }) {
                return Some(unified.key.clone());
            }
            // No environment match found — do NOT fall through to a different
            // environment's stream. That would break DTAP isolation.
        }
    }

    // Fallback: construct from the extracted environment
    // Uses canonical format with hash-tag braces for cluster slot consistency
    Some(format!("{{{}}}:gnode:unified:{}", target_site_id, source_env))
}

/// Resolve a JSON capability query to a relay target.
///
/// Parses `_rt` as JSON like `{"capabilities":{"domain_primary":0.5},"limit":1}`.
///
/// NOTE: Capability-based relay routing is not yet implemented against the live
/// topology query path (GNODE_TOPO_QUERY_VOXEL). A proper implementation requires
/// Q64.64 point construction + per-site voxel querying.
///
/// For now, use entity ID or site ID relay targets instead of capability queries.
fn resolve_capability_query(
    _conn: &mut Connection,
    query_json: &str,
    _current_site_id: &str,
    _current_stream: &str,
    _shared_discovery: Option<&Arc<RwLock<StreamDiscoveryManager>>>,
    debug_mode: bool,
) -> RelayDecision {
    // Validate the JSON is at least well-formed
    let query: serde_json::Value = match serde_json::from_str(query_json) {
        Ok(v) => v,
        Err(e) => {
            return RelayDecision::Error(format!(
                "Invalid JSON capability query: {}", e
            ));
        }
    };

    if query.get("capabilities").is_none() {
        return RelayDecision::Error(
            "Capability query must contain a 'capabilities' object".to_string()
        );
    }

    if debug_mode {
        info!(
            "Capability-based relay routing not yet implemented (query: {}). \
             Use entity ID or site ID relay targets instead.",
            query_json
        );
    }

    RelayDecision::Error(
        "Capability-based relay routing requires the topology voxel query rewrite. \
         Use entity ID or site_id in _rt instead.".to_string()
    )
}
