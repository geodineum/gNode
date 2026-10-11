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
        /// The placement the share was imposed on (one topology); empty on the legacy path.
        target_placement: String,
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
        let tagged = relay_target.split(":gnode:").next();
        if let Some(site_id) = tagged.map(|t| crate::config::site_of_stream_key(t).unwrap_or(t)) {
            if site_id == current_site_id {
                return RelayDecision::Local;
            }
            return RelayDecision::Forward {
                target_site_id: site_id.to_string(),
                target_stream_key: relay_target.to_string(),
                target_entity_id: String::new(),
                target_placement: String::new(),
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
                target_placement: String::new(),
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
                    target_placement: String::new(),
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

// ============================================================================
// One topology (SPEC-ONE-TOPOLOGY §6): the target is an entity of the constellation,
// the share is imposed by the ring, the stream is the chosen placement's.
// ============================================================================

/// A capability query in `_rt`: `{"layer":"3","facets":{"domain":["workflow"]},"axes":{"contract_version":[0.2,0.5]}}`.
/// `layer` defaults to every service. The legacy numeric `capabilities` form has no meaning
/// under colours and is refused by name.
pub fn filters_from_query(query: &serde_json::Value) -> Result<(String, crate::constellation::Filters), String> {
    if query.get("capabilities").is_some() {
        return Err("capability queries name facets and axes, not numeric capabilities".to_string());
    }
    let layer = query.get("layer").and_then(|l| l.as_str()).unwrap_or("3").to_string();
    let mut f = crate::constellation::Filters::default();
    if let Some(facets) = query.get("facets").and_then(|v| v.as_object()) {
        for (name, vals) in facets {
            let words: Vec<&str> = match vals {
                serde_json::Value::String(s) => vec![s.as_str()],
                serde_json::Value::Array(a) => a.iter().filter_map(|v| v.as_str()).collect(),
                _ => return Err(format!("facet {} must be a word or a list of words", name)),
            };
            f = f.facet(name, &words);
        }
    }
    if let Some(axes) = query.get("axes").and_then(|v| v.as_object()) {
        for (name, range) in axes {
            let (Some(lo), Some(hi)) = (range.get(0).and_then(|v| v.as_f64()), range.get(1).and_then(|v| v.as_f64())) else {
                return Err(format!("axis {} must be [lo, hi] fractions", name));
            };
            f = f.range(name, lo, hi);
        }
    }
    Ok((layer, f))
}

/// Candidates ranked for a capability relay: health high, execution priority high, oldest first.
pub fn rank_candidates(records: &mut Vec<serde_json::Value>) {
    let score = |r: &serde_json::Value, axis: &str| r["q"][axis].as_str().and_then(|s| s.parse::<u64>().ok()).unwrap_or(0);
    records.sort_by(|a, b| {
        score(b, "health").cmp(&score(a, "health"))
            .then(score(b, "execution_priority").cmp(&score(a, "execution_priority")))
            .then(a["ro"].as_u64().unwrap_or(u64::MAX).cmp(&b["ro"].as_u64().unwrap_or(u64::MAX)))
    });
}

/// Resolve `_rt` against the one topology. Exactly one call per relayed command: the
/// ring counter advances here, so a second call would be a second request (insight
/// `an-imposed-share-needs-exactly-one-resolution-per-request`).
pub fn resolve_one_topology(
    conn: &mut Connection,
    c: &crate::constellation::Constellation,
    relay_target: &str,
    current_site_id: &str,
    current_stream: &str,
) -> RelayDecision {
    if relay_target.is_empty() {
        return RelayDecision::NotFound("Empty relay target".to_string());
    }
    let env = crate::config::env_of_stream_key(current_stream).unwrap_or("production");

    // An explicit stream key is honoured as written.
    if relay_target.contains(":gnode:") {
        let site = relay_target.split(":gnode:").next().and_then(crate::config::site_of_stream_key);
        return match site {
            Some(s) if s == current_site_id => RelayDecision::Local,
            Some(s) => RelayDecision::Forward {
                target_site_id: s.to_string(), target_stream_key: relay_target.to_string(),
                target_entity_id: String::new(), target_placement: String::new(),
            },
            None => RelayDecision::Error("Could not extract site_id from stream key".to_string()),
        };
    }

    // A capability query picks the service; the ring picks the placement.
    let service_id = if relay_target.starts_with('{') {
        let query: serde_json::Value = match serde_json::from_str(relay_target) {
            Ok(v) => v,
            Err(e) => return RelayDecision::Error(format!("Invalid JSON capability query: {}", e)),
        };
        let (layer, filters) = match filters_from_query(&query) {
            Ok(x) => x,
            Err(e) => return RelayDecision::Error(e),
        };
        let mut found = match c.query(conn, &layer, &filters, 0) {
            Ok(v) => v,
            Err(e) => return RelayDecision::Error(format!("capability query failed: {}", e)),
        };
        found.retain(|r| r["sh"].as_str().map(|s| s != "3.1").unwrap_or(false));
        rank_candidates(&mut found);
        match found.first().and_then(|r| r["id"].as_str()) {
            Some(id) => id.to_string(),
            None => return RelayDecision::NotFound(format!("no service matches {}", relay_target)),
        }
    } else {
        relay_target.to_string()
    };

    if service_id == current_site_id {
        return RelayDecision::Local;
    }
    let record = match c.query(conn, "3", &crate::constellation::Filters::default(), 0) {
        Ok(v) => v.into_iter().find(|r| r["id"].as_str() == Some(service_id.as_str())),
        Err(e) => return RelayDecision::Error(format!("entity lookup failed: {}", e)),
    };
    let Some(record) = record else {
        return RelayDecision::NotFound(format!("'{}' is not a service of constellation {}", service_id, c.name));
    };
    if record["sh"].as_str() == Some("3.1") {
        // A site has no reader (M28); fabricating its stream would park the command forever.
        return RelayDecision::NotFound(format!("'{}' is a site: sites take no relayed commands", service_id));
    }
    match c.route(conn, &service_id) {
        Ok(routed) => {
            debug!("Routed '{}' to placement {} on {}{}", service_id, routed.placement, routed.node, if routed.standby { " (standby)" } else { "" });
            RelayDecision::Forward {
                target_site_id: service_id.clone(),
                target_stream_key: crate::config::build_placement_stream_key(&service_id, env, &routed.node),
                target_entity_id: service_id,
                target_placement: routed.placement,
            }
        }
        Err(e) => RelayDecision::NotFound(format!("'{}' has no healthy active placement: {}", service_id, e)),
    }
}

#[cfg(test)]
mod one_topology_tests {
    use super::*;

    #[test]
    fn capability_queries_name_facets_and_axes() {
        let (layer, f) = filters_from_query(&serde_json::json!({"facets":{"domain":["workflow"],"implementation_language":"rust"},"axes":{"contract_version":[0.2,0.5]}})).unwrap();
        assert_eq!(layer, "3");
        assert_eq!(f.facets["domain"], vec!["workflow"]);
        assert_eq!(f.facets["implementation_language"], vec!["rust"]);
        assert!(f.axes.contains_key("contract_version"));
        assert!(filters_from_query(&serde_json::json!({"capabilities":{"domain_primary":0.5}})).is_err(), "the numeric form is refused by name");
        let (layer, _) = filters_from_query(&serde_json::json!({"layer":"3.3"})).unwrap();
        assert_eq!(layer, "3.3");
    }

    #[test]
    fn candidates_rank_by_health_then_priority_then_age() {
        let mut v = vec![
            serde_json::json!({"id":"old-sick","q":{"health":"0","execution_priority":"100"},"ro":1}),
            serde_json::json!({"id":"young-well","q":{"health":"281474976710656","execution_priority":"50"},"ro":9}),
            serde_json::json!({"id":"old-well","q":{"health":"281474976710656","execution_priority":"50"},"ro":2}),
        ];
        rank_candidates(&mut v);
        assert_eq!(v.iter().map(|r| r["id"].as_str().unwrap()).collect::<Vec<_>>(), vec!["old-well", "young-well", "old-sick"]);
    }
}
