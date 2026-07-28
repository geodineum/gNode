//! Constellation-tier registration — a node announces itself geometrically.
//!
//! A node already registered itself before this module existed, but only as
//! flat hashes: `gnode:node:{id}:config|health|metrics` plus two sets. That is
//! a plain service registry sitting beside a geometric one, and it meant the
//! constellation tier had a complete 24-dimension schema and zero rows —
//! `node_role`, `valkey_mode`, `cpu_tier`, `specialization` and the rest all
//! specified, none of them ever written.
//!
//! The consequence was not cosmetic. Work could only be steered by WHICH
//! STREAMS a node consumes, so adding a GPU box and expecting inference to
//! land on it did not work: `--node-type` was parsed, logged, and never read
//! again. With the node's capabilities in the topology, "find an inference
//! node with a local ValKey replica" becomes a discovery query.
//!
//! The flat hashes are left exactly as they were. They keep their single
//! consumer (`gnode_node.lua`) and become a derived projection of this, which
//! is the same shape the (B)-snapshot took when service registration moved.

use std::collections::HashMap;
use log::{debug, info, warn};

use crate::node_config::NodeConfig;
use crate::tool_registration::{build_entity_data, find_tier_schema_path, load_schema};
use crate::Result;

/// Everything about a node that maps onto a constellation dimension.
pub struct NodeFacts<'a> {
    pub node_id: &'a str,
    pub node_type: &'a str,
    pub is_master: bool,
    pub config: &'a NodeConfig,
}

/// Translate node facts into constellation capability NAMES.
///
/// Names, not indices — the daemon resolves names against the loaded schema, so
/// this cannot drift out of step with the dimension layout the way a private
/// index table would.
///
/// Only what can be honestly derived is set. Everything else is left for the
/// operator to declare in the node YAML's `capabilities.dimensions`, because a
/// guessed capability is worse than an absent one: absent matches nothing,
/// guessed matches the wrong work confidently.
fn derive_capabilities(facts: &NodeFacts) -> HashMap<String, String> {
    let mut caps: HashMap<String, String> = HashMap::new();

    // Role follows the master election that already happened. A worker is a
    // full_node rather than a replica: "replica" is a ValKey-level statement
    // and belongs to valkey_mode, not to what the node does.
    caps.insert(
        "node_role".to_string(),
        if facts.is_master { "master" } else { "full_node" }.to_string(),
    );

    // The whole point of the exercise: --node-type stops being a log line and
    // becomes a queryable axis. Vocabularies were designed to match; anything
    // outside it is left unset rather than coerced to "general", so an
    // operator's typo does not silently register as a general-purpose node.
    match facts.node_type {
        t @ ("general" | "inference" | "storage" | "compute" | "content") => {
            caps.insert("specialization".to_string(), t.to_string());
        }
        other => {
            warn!(
                "node_type {:?} is not a constellation specialization \
                 (general|inference|storage|compute|content) — leaving the dimension unset. \
                 Work will not be routed to this node by specialization until it is declared.",
                other
            );
        }
    }

    // ValKey topology. The master runs the primary; a worker holds a client
    // connection unless it has been told otherwise.
    caps.insert(
        "valkey_mode".to_string(),
        if facts.is_master { "primary" } else { "client_only" }.to_string(),
    );

    // Capacity CLASS from declared resources. Deliberately coarse: these are
    // hardware classes, not utilisation, and dim 16 carries live load.
    let cores = facts.config.resources.cores;
    if cores > 0 {
        caps.insert(
            "cpu_tier".to_string(),
            match cores {
                1..=2 => "minimal",
                3..=8 => "standard",
                _ => "compute",
            }
            .to_string(),
        );
    }
    // "gpu" is never inferred. A GPU is not implied by core count and getting
    // it wrong sends inference somewhere that cannot serve it — declare it.

    let mem_mb = facts.config.resources.max_memory_mb;
    if mem_mb > 0 {
        caps.insert(
            "memory_tier".to_string(),
            match mem_mb {
                0..=4096 => "minimal",
                4097..=32768 => "standard",
                _ => "high_memory",
            }
            .to_string(),
        );
    }

    caps
}

/// Resolve capability names to floats against the constellation schema.
fn resolve(
    named: &HashMap<String, String>,
    schema: &crate::tool_registration::CapabilitySchema,
) -> HashMap<String, f64> {
    let mut out = HashMap::new();
    for (dim, value) in named {
        match schema.dimensions.get(dim) {
            Some(def) => match def.values.get(value) {
                Some(&v) => {
                    out.insert(dim.clone(), v);
                }
                None => warn!(
                    "constellation dimension {:?} has no value {:?} — skipping. \
                     Valid: {:?}",
                    dim,
                    value,
                    def.values.keys().collect::<Vec<_>>()
                ),
            },
            None => warn!("constellation schema has no dimension {:?} — skipping", dim),
        }
    }
    out
}

/// The topology every node in this constellation registers into.
pub fn constellation_topology_key(topology_ns: &str) -> String {
    format!("{{{}}}:gnode:constellation", topology_ns)
}

/// Register this node as a constellation-tier entity.
///
/// Best-effort by design: a node that cannot describe itself geometrically
/// must still boot and serve. Failure is logged with what it would have
/// registered, because a silent absence here looks identical to a node that
/// simply has not started.
pub fn register_node_geometrically(
    conn: &mut redis::Connection,
    topology_ns: &str,
    facts: &NodeFacts,
) -> Result<()> {
    let schema_path = match find_tier_schema_path("constellation", None) {
        Some(p) => p,
        None => {
            warn!("constellation_schema.yaml not found — node not registered geometrically");
            return Ok(());
        }
    };
    let schema = load_schema(&schema_path)?;

    let mut named = derive_capabilities(facts);

    // Operator declarations WIN over anything derived. They are already a
    // HashMap<String, f64> in the node YAML, so they bypass name resolution
    // and are applied after — this is how a GPU node says it has a GPU, and
    // how anything this module refuses to guess gets declared.
    let mut capabilities = resolve(&named, &schema);
    for (dim, &value) in &facts.config.capabilities.dimensions {
        if schema.dimensions.contains_key(dim) {
            if capabilities.insert(dim.clone(), value).is_some() {
                debug!("node config overrides derived constellation dimension {:?}", dim);
            }
        } else {
            warn!(
                "node config declares {:?}, which is not a constellation dimension — ignored",
                dim
            );
        }
    }
    named.clear();

    let total = schema.total_dimensions;
    let discovery = schema.discovery_dimensions.unwrap_or(total);
    let dim_map: HashMap<String, usize> =
        schema.dimensions.iter().map(|(n, d)| (n.clone(), d.index)).collect();

    let (entity_json, bucket_key, z_score) = build_entity_data(
        facts.node_id,
        &capabilities,
        &None,
        total,
        discovery,
        &dim_map,
    );

    let topology_key = constellation_topology_key(topology_ns);
    let result: redis::RedisResult<String> = redis::cmd("FCALL")
        .arg("GNODE_REGISTER_CAPABILITY_VECTOR")
        .arg(1)
        .arg(&topology_key)
        .arg(facts.node_id)
        .arg(&entity_json)
        .arg(&bucket_key)
        .arg(z_score.to_string())
        .arg(crate::daemon::GNodeDaemon::topology_snapshot_key())
        .query(conn);

    match result {
        Ok(_) => {
            info!(
                "Registered node '{}' in the constellation topology: {} of {} dimensions declared \
                 ({} discovery), schema v{}",
                facts.node_id,
                capabilities.len(),
                total,
                discovery,
                schema.schema_version
            );
            Ok(())
        }
        Err(e) => {
            warn!(
                "Constellation registration failed for '{}' — the node is running but will not be \
                 discoverable by capability. Would have registered: {:?}. Error: {}",
                facts.node_id, capabilities, e
            );
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node_config::NodeConfig;

    fn facts_for<'a>(node_type: &'a str, is_master: bool, cfg: &'a NodeConfig) -> NodeFacts<'a> {
        NodeFacts { node_id: "n1", node_type, is_master, config: cfg }
    }

    #[test]
    fn master_registers_as_master_and_primary() {
        let cfg = NodeConfig::default();
        let c = derive_capabilities(&facts_for("general", true, &cfg));
        assert_eq!(c.get("node_role").map(String::as_str), Some("master"));
        assert_eq!(c.get("valkey_mode").map(String::as_str), Some("primary"));
    }

    #[test]
    fn worker_is_a_full_node_not_a_replica() {
        // "replica" is a ValKey statement; it belongs to valkey_mode.
        let cfg = NodeConfig::default();
        let c = derive_capabilities(&facts_for("general", false, &cfg));
        assert_eq!(c.get("node_role").map(String::as_str), Some("full_node"));
        assert_eq!(c.get("valkey_mode").map(String::as_str), Some("client_only"));
    }

    #[test]
    fn node_type_becomes_specialization() {
        let cfg = NodeConfig::default();
        let c = derive_capabilities(&facts_for("inference", false, &cfg));
        assert_eq!(c.get("specialization").map(String::as_str), Some("inference"));
    }

    #[test]
    fn an_unknown_node_type_is_left_unset_not_coerced() {
        // Coercing a typo to "general" would register a node as general-purpose
        // and route ordinary work to it — worse than not registering the axis.
        let cfg = NodeConfig::default();
        let c = derive_capabilities(&facts_for("inferance", false, &cfg));
        assert!(!c.contains_key("specialization"));
    }

    #[test]
    fn gpu_is_never_inferred_from_core_count() {
        let mut cfg = NodeConfig::default();
        cfg.resources.cores = 128;
        let c = derive_capabilities(&facts_for("inference", false, &cfg));
        assert_eq!(c.get("cpu_tier").map(String::as_str), Some("compute"));
        assert_ne!(c.get("cpu_tier").map(String::as_str), Some("gpu"));
    }

    #[test]
    fn every_derived_name_resolves_against_the_shipped_schema() {
        // The guard against this module and the schema drifting apart: every
        // name it can emit must exist, with that value, in the real YAML.
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let schema = load_schema(&root.join("config/constellation_schema.yaml")).unwrap();
        let mut cfg = NodeConfig::default();
        cfg.resources.cores = 8;
        cfg.resources.max_memory_mb = 65536;

        for &(ty, master) in &[
            ("general", true), ("inference", false), ("storage", false),
            ("compute", false), ("content", false),
        ] {
            let named = derive_capabilities(&facts_for(ty, master, &cfg));
            let resolved = resolve(&named, &schema);
            assert_eq!(
                resolved.len(), named.len(),
                "{:?}/{} — some derived name did not resolve: derived {:?}, resolved {:?}",
                ty, master, named, resolved
            );
        }
    }

    #[test]
    fn load_sits_at_the_z_score_dimension() {
        // compute_service_z_score reads index 16. If aggregate_load moves off
        // it, "route to the least busy node" silently orders by whatever dim 16
        // became.
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let schema = load_schema(&root.join("config/constellation_schema.yaml")).unwrap();
        assert_eq!(schema.dimensions.get("aggregate_load").unwrap().index, 16);
    }

    #[test]
    fn measured_dimensions_sit_above_the_discovery_cut() {
        // discovery_point() is a prefix truncation, so a measured value below
        // the cut bakes a config-time default into the bucket key.
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let schema = load_schema(&root.join("config/constellation_schema.yaml")).unwrap();
        let cut = schema.discovery_dimensions.unwrap();
        for dim in ["aggregate_load", "storage_available", "node_health"] {
            let idx = schema.dimensions.get(dim).unwrap().index;
            assert!(idx >= cut, "{} is measured but sits at {}, inside the {}-dim cut", dim, idx, cut);
        }
    }
}
