//! The constellation tick (SPEC-ONE-TOPOLOGY §5): every node declares itself; the lease
//! holder of each constellation derives placements from heartbeats and health from
//! placements, once per tick. Nothing here is measured from a service; everything is
//! read from what gNode already knows.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use log::{info, warn};

use crate::constellation::{declare_gnode_component, declare_gnode_placement, declare_node, encode, Constellation, Declared, Filters, NodeHardware, Placement};
use crate::integration::lease::hold_lease;
use crate::topology_schema::TopologySchema;
use crate::worker::{DaemonWorker, TickResult, WorkerConfig};

/// A heartbeat key `{c}:gnode:heartbeat:<env>:<component>:<node>`, parsed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Heartbeat {
    pub env: String,
    pub component: String,
    pub node: String,
}

/// `{geodineum}:gnode:heartbeat:production:gflow:aesir-solutions` → Heartbeat. A key
/// without a node segment (the pre-contract COMMS writer) parses to `None`.
pub fn parse_heartbeat_key(key: &str) -> Option<Heartbeat> {
    let rest = key.split_once("}:gnode:heartbeat:")?.1;
    let mut parts = rest.splitn(3, ':');
    let env = parts.next()?.to_string();
    let component = parts.next()?.to_string();
    let node = parts.next()?.to_string();
    if env.is_empty() || component.is_empty() || node.is_empty() {
        return None;
    }
    Some(Heartbeat { env, component, node })
}

/// Which entity a heartbeat component name belongs to: the record's `m.heartbeat`
/// first, else the id compared without case. Returns (id, shape).
pub fn match_heartbeat<'a>(name: &str, entities: &'a [serde_json::Value]) -> Vec<(&'a str, &'a str)> {
    let lower = name.to_ascii_lowercase();
    let mut out = Vec::new();
    for e in entities {
        let id = e["id"].as_str().unwrap_or("");
        let sh = e["sh"].as_str().unwrap_or("");
        let declared = e["m"]["heartbeat"].as_str();
        if declared == Some(name) || (declared.is_none() && id.to_ascii_lowercase() == lower) {
            out.push((id, sh));
        }
    }
    out
}

#[derive(Debug, Default, Clone)]
pub struct PassReport {
    pub leased: bool,
    pub heartbeats: usize,
    pub unparsed: Vec<String>,
    pub unmatched: Vec<String>,
    pub placed: Vec<String>,
    pub fresh: u64,
    pub placements: u64,
    pub entities_with_health: u64,
}

/// What this node says about itself.
#[derive(Debug, Clone)]
pub struct SelfFacts {
    pub node_id: String,
    pub hardware: NodeHardware,
    pub version: String,
}

pub struct ConstellationWorker {
    constellations: Vec<Constellation>,
    schema: TopologySchema,
    facts: SelfFacts,
    tick_interval: Duration,
    last_tick: Option<Instant>,
    fresh_secs: u64,
    declared: bool,
}

impl ConstellationWorker {
    pub fn new(constellations: Vec<Constellation>, schema: TopologySchema, facts: SelfFacts) -> Self {
        Self { constellations, schema, facts, tick_interval: Duration::from_secs(15), last_tick: None, fresh_secs: 75, declared: false }
    }

    /// The constellations a daemon serves: `GNODE_CONSTELLATIONS` (comma separated), else the namespace.
    pub fn constellations_from_env(default_ns: &str) -> Vec<Constellation> {
        let raw = std::env::var("GNODE_CONSTELLATIONS").unwrap_or_default();
        let mut names: Vec<String> = raw.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect();
        if names.is_empty() {
            names.push(default_ns.to_string());
        }
        names.into_iter().map(|n| Constellation::new(&n)).collect()
    }

    /// Every node describes ITSELF, lease or not: the node entity, the gNode component
    /// and its placement on this node. Idempotent.
    pub fn declare_self(&self, conn: &mut redis::Connection, c: &Constellation) -> crate::Result<()> {
        c.ensure(conn)?;
        c.register(conn, &encode(&self.schema, &declare_node(&self.facts.node_id, &self.facts.hardware))?)?;
        c.register(conn, &encode(&self.schema, &declare_gnode_component())?)?;
        let (p, d) = declare_gnode_placement(&self.facts.node_id, &self.facts.version);
        c.place(conn, &p, &encode(&self.schema, &d)?)?;
        Ok(())
    }

    fn heartbeat_keys(&self, conn: &mut redis::Connection, c: &Constellation) -> Vec<String> {
        let pattern = format!("{{{}}}:gnode:heartbeat:*", c.name);
        let mut cursor: u64 = 0;
        let mut keys = Vec::new();
        loop {
            let reply: redis::RedisResult<(u64, Vec<String>)> = redis::cmd("SCAN").arg(cursor).arg("MATCH").arg(&pattern).arg("COUNT").arg(200).query(conn);
            let Ok((next, batch)) = reply else { break };
            keys.extend(batch);
            cursor = next;
            if cursor == 0 {
                break;
            }
        }
        keys.sort();
        keys.dedup();
        keys
    }

    /// Placements from heartbeats: a heartbeat `<component>:<node>` proves a component
    /// (4.1) or a daemon-service (4.2) runs on that node. Only the lease holder writes.
    pub fn pass(&mut self, conn: &mut redis::Connection, c: &Constellation) -> crate::Result<PassReport> {
        let mut report = PassReport::default();
        report.leased = hold_lease(conn, &c.lease_key(), &self.facts.node_id);
        if !report.leased {
            return Ok(report);
        }
        let components = c.query(conn, "2", &Filters::default(), 0)?;
        let services = c.query(conn, "3", &Filters::default(), 0)?;
        let nodes: Vec<String> = c.query(conn, "1", &Filters::default(), 0)?
            .iter().filter_map(|n| n["id"].as_str().map(str::to_string)).collect();
        let existing: BTreeMap<String, serde_json::Value> = c.query(conn, "4", &Filters::default(), 0)?
            .into_iter().filter_map(|p| { let id = p["id"].as_str()?.to_string(); Some((id, p)) }).collect();

        for key in self.heartbeat_keys(conn, c) {
            report.heartbeats += 1;
            let Some(hb) = parse_heartbeat_key(&key) else { report.unparsed.push(key); continue };
            if !nodes.contains(&hb.node) {
                report.unmatched.push(format!("{} (node {} is not registered)", key, hb.node));
                continue;
            }
            let mut matched = match_heartbeat(&hb.component, &components);
            matched.extend(match_heartbeat(&hb.component, &services));
            if matched.is_empty() {
                report.unmatched.push(key.clone());
                continue;
            }
            for (id, sh) in matched {
                let placement_shape = if sh == "2" { "4.1" } else { "4.2" };
                // The declared capacity_weight rides in the entity's metadata (D13: 0 = standby).
                let weight = services.iter().chain(components.iter())
                    .find(|e| e["id"].as_str() == Some(id))
                    .and_then(|e| e["m"]["capacity_weight"].as_u64())
                    .unwrap_or(1) as u32;
                let p = Placement {
                    of: id.to_string(),
                    on: hb.node.clone(),
                    env: if sh == "2" { "global".to_string() } else { env_word(&hb.env) },
                    heartbeat: hb.component.clone(),
                    heartbeat_env: Some(hb.env.clone()),
                    weight,
                };
                let pid = p.id();
                if let Some(prev) = existing.get(&pid) {
                    // A placement that registration already made keeps its declared weight and version.
                    if prev["hb"].as_str() == Some(hb.component.as_str()) {
                        continue;
                    }
                }
                let mut d = Declared::new(&pid, placement_shape, &p.env);
                if placement_shape == "4.2" {
                    d = d.fraction("capacity_weight", (weight as f64 / 100.0).min(1.0));
                }
                match encode(&self.schema, &d).and_then(|e| c.place(conn, &p, &e)) {
                    Ok(_) => report.placed.push(pid),
                    Err(e) => warn!("placement {} from heartbeat {} refused: {}", pid, key, e),
                }
            }
        }

        let now = crate::utils::current_timestamp_ms().max(0) as u64 / 1000;
        let h = c.derive_health(conn, now, self.fresh_secs, &self.facts.node_id)?;
        report.fresh = h.fresh;
        report.placements = h.placements;
        report.entities_with_health = h.entities;
        Ok(report)
    }
}

/// The environment vocabulary word for a heartbeat env segment (`all` is the daemon's).
fn env_word(env: &str) -> String {
    match env {
        "all" | "global" => "global".to_string(),
        other => other.to_string(),
    }
}

impl DaemonWorker for ConstellationWorker {
    fn name(&self) -> &str {
        "constellation"
    }

    fn tick(&mut self) -> TickResult {
        if let Some(t) = self.last_tick {
            if t.elapsed() < self.tick_interval {
                return TickResult::Idle;
            }
        }
        self.last_tick = Some(Instant::now());
        let Ok(mut conn) = crate::integration::connection_manager::get_connection() else { return TickResult::Error };
        let constellations = self.constellations.clone();
        for c in &constellations {
            if !self.declared {
                match self.declare_self(&mut conn, c) {
                    Ok(()) => info!("{}: declared node {}, gNode and gNode@{}", c.name, self.facts.node_id, self.facts.node_id),
                    Err(e) => { warn!("{}: self-declaration failed: {}", c.name, e); return TickResult::Error; }
                }
            }
            match self.pass(&mut conn, c) {
                Ok(r) if r.leased => {
                    if !r.placed.is_empty() || !r.unmatched.is_empty() || !r.unparsed.is_empty() {
                        info!("{}: {} heartbeats, placed {:?}, unmatched {:?}, unparsed {:?}; health {}/{} fresh, {} entities",
                            c.name, r.heartbeats, r.placed, r.unmatched, r.unparsed, r.fresh, r.placements, r.entities_with_health);
                    }
                }
                Ok(_) => {}
                Err(e) => warn!("{}: constellation pass failed: {}", c.name, e),
            }
        }
        self.declared = true;
        TickResult::Busy
    }

    fn config(&self) -> WorkerConfig {
        WorkerConfig { idle_interval: Duration::from_secs(1), ..WorkerConfig::default() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heartbeat_keys_parse_and_the_node_segment_is_required() {
        let hb = parse_heartbeat_key("{geodineum}:gnode:heartbeat:production:gflow:aesir-solutions").unwrap();
        assert_eq!(hb, Heartbeat { env: "production".into(), component: "gflow".into(), node: "aesir-solutions".into() });
        let hb = parse_heartbeat_key("{geodineum}:gnode:heartbeat:all:gnode-daemon:squad").unwrap();
        assert_eq!((hb.env.as_str(), hb.component.as_str(), hb.node.as_str()), ("all", "gnode-daemon", "squad"));
        assert!(parse_heartbeat_key("{geodineum}:gnode:heartbeat:production:comms").is_none(), "COMMS writes no node segment today");
        assert!(parse_heartbeat_key("{geodineum}:gnode:constellation:entities").is_none());
        assert_eq!(env_word("all"), "global");
    }

    #[test]
    fn a_heartbeat_name_matches_the_declared_name_first_and_the_id_without_case_second() {
        let entities = vec![
            serde_json::json!({ "id": "gFlow", "sh": "2", "m": {} }),
            serde_json::json!({ "id": "Geodineum-COMMS", "sh": "2", "m": { "heartbeat": "comms" } }),
            serde_json::json!({ "id": "gflow", "sh": "3.3", "m": {} }),
        ];
        let m = match_heartbeat("gflow", &entities);
        assert_eq!(m, vec![("gFlow", "2"), ("gflow", "3.3")], "the component and the daemon-service both run where the heartbeat is");
        assert_eq!(match_heartbeat("comms", &entities), vec![("Geodineum-COMMS", "2")]);
        assert!(match_heartbeat("gshield", &entities).is_empty());
    }

    #[test]
    fn constellations_default_to_the_namespace() {
        std::env::remove_var("GNODE_CONSTELLATIONS");
        let c = ConstellationWorker::constellations_from_env("geodineum");
        assert_eq!(c, vec![Constellation::new("geodineum")]);
    }
}
