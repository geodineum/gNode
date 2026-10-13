//! Relay target resolution logic
//!
//! Resolves the `_rt` field value to a concrete target site + stream.
//! Supports three resolution modes:
//!   1. Entity ID lookup — find which site owns the entity on its services topology
//!   2. Site ID — direct stream construction
//!   3. JSON capability query — geometric discovery on namespace topology

use log::debug;
use redis::Connection;

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
