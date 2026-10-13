//! The daemon's side of the one topology per constellation (SPEC-ONE-TOPOLOGY §1–§4):
//! the key family, the encoder from declared axis values to a 24-wide Q64.64 point
//! through the compiled schema, and typed wrappers over the `GNODE_TOPO_*` functions.
//! Compare in Lua, multiply in Rust: every number the library stores is produced here.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::geometric_precision::FixedPoint;
use crate::topology_schema::{index_score, layer_code, layer_coordinate, layer_interval, Kind, Shape, TopologySchema, Writer, WIDTH};
use crate::{GeometricError, Result};

pub const LIFECYCLE_ACTIVE: f64 = 0.25;

/// The constellations this daemon serves (SPEC §10a): `GNODE_CONSTELLATIONS`, comma separated,
/// else the topology namespace alone. The first is the default for anything not yet placed.
pub fn served() -> Vec<Constellation> {
    let default_ns = crate::daemon::GNodeDaemon::get_topology_namespace();
    let raw = std::env::var("GNODE_CONSTELLATIONS").unwrap_or_default();
    let mut names: Vec<String> = raw.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect();
    if names.is_empty() {
        names.push(default_ns.to_string());
    }
    names.into_iter().map(|n| Constellation::new(&n)).collect()
}

/// The constellation an entity (a site, a service) belongs to: the served constellation whose
/// key holds it; a site named like a constellation is its own; otherwise the default. One owner
/// per constellation (D2) makes the first hit the only hit.
pub fn for_entity(conn: &mut redis::Connection, entity_id: &str) -> Constellation {
    let served = served();
    if let Some(c) = served.iter().find(|c| c.name == entity_id) {
        return c.clone();
    }
    for c in &served {
        let hit: redis::RedisResult<i64> = redis::cmd("HEXISTS").arg(format!("{}:entities", c.key())).arg(entity_id).query(conn);
        if matches!(hit, Ok(1)) {
            return c.clone();
        }
    }
    served.into_iter().next().unwrap_or_else(|| Constellation::new("geodineum"))
}

pub async fn for_entity_async(conn: &mut redis::aio::MultiplexedConnection, entity_id: &str) -> Constellation {
    let served = served();
    if let Some(c) = served.iter().find(|c| c.name == entity_id) {
        return c.clone();
    }
    for c in &served {
        let hit: redis::RedisResult<i64> = redis::cmd("HEXISTS").arg(format!("{}:entities", c.key())).arg(entity_id).query_async(conn).await;
        if matches!(hit, Ok(1)) {
            return c.clone();
        }
    }
    served.into_iter().next().unwrap_or_else(|| Constellation::new("geodineum"))
}

/// A registration may name its constellation; it must be one this daemon serves.
pub fn named_or_for_site(conn: &mut redis::Connection, named: Option<&str>, site_id: &str) -> std::result::Result<Constellation, String> {
    match named {
        Some(n) => served().into_iter().find(|c| c.name == n).ok_or_else(|| format!("constellation {} is not served by this daemon", n)),
        None => Ok(for_entity(conn, site_id)),
    }
}
const HEALTH_SLOT: usize = 17;
const LIFECYCLE_SLOT: usize = 18;

/// One constellation: its name is the hash tag of every key it owns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Constellation {
    pub name: String,
}

impl Constellation {
    pub fn new(name: &str) -> Self {
        Self { name: name.to_string() }
    }

    pub fn key(&self) -> String {
        format!("{{{}}}:gnode:constellation", self.name)
    }

    pub fn lease_key(&self) -> String {
        format!("{{{}}}:gnode:cluster:writer", self.name)
    }


    pub fn heartbeat_key(&self, env: &str, component: &str, node: &str) -> String {
        format!("{{{}}}:gnode:heartbeat:{}:{}:{}", self.name, env, component, node)
    }

    pub fn ensure(&self, conn: &mut redis::Connection) -> Result<bool> {
        let reply: String = self.fcall(conn, "GNODE_ENSURE_CONSTELLATION", &[])?;
        Ok(reply.contains("\"created\":true"))
    }

    pub fn publish_schema(&self, conn: &mut redis::Connection, schema: &TopologySchema) -> Result<()> {
        schema.publish(conn, &self.key()).map_err(GeometricError::Redis)
    }

    fn fcall<T: redis::FromRedisValue>(&self, conn: &mut redis::Connection, f: &str, args: &[&str]) -> Result<T> {
        let mut c = redis::cmd("FCALL");
        c.arg(f).arg(1).arg(self.key());
        for a in args {
            c.arg(*a);
        }
        c.query(conn).map_err(GeometricError::Redis)
    }

    fn fcall_leased<T: redis::FromRedisValue>(&self, conn: &mut redis::Connection, f: &str, args: &[&str]) -> Result<T> {
        let mut c = redis::cmd("FCALL");
        c.arg(f).arg(2).arg(self.key()).arg(self.lease_key());
        for a in args {
            c.arg(*a);
        }
        c.query(conn).map_err(GeometricError::Redis)
    }

    pub fn register(&self, conn: &mut redis::Connection, e: &Encoded) -> Result<RegisterReply> {
        let reply: String = self.fcall(conn, "GNODE_TOPO_REGISTER", &[&e.id, &e.shape, &e.record, &e.ordered, &e.colours, &e.preserve])?;
        serde_json::from_str(&reply).map_err(GeometricError::Json)
    }

    pub fn place(&self, conn: &mut redis::Connection, p: &Placement, e: &Encoded) -> Result<RegisterReply> {
        let weight = p.weight.to_string();
        let hbe = p.heartbeat_env.clone().unwrap_or_default();
        let reply: String = self.fcall(conn, "GNODE_TOPO_PLACE",
            &[&e.id, &e.shape, &p.of, &p.on, &p.env, &p.heartbeat, &weight, &e.record, &e.ordered, &e.colours, &hbe])?;
        serde_json::from_str(&reply).map_err(GeometricError::Json)
    }

    pub fn deregister(&self, conn: &mut redis::Connection, id: &str) -> Result<()> {
        let _: String = self.fcall(conn, "GNODE_TOPO_DEREGISTER", &[id])?;
        Ok(())
    }

    /// Records of every entity in the layer interval of `shape_or_layer` that passes the filters.
    pub fn query(&self, conn: &mut redis::Connection, shape_or_layer: &str, filters: &Filters, limit: usize) -> Result<Vec<serde_json::Value>> {
        let (lo, hi) = layer_interval(shape_or_layer).map_err(GeometricError::InvalidState)?;
        let f = serde_json::to_string(filters).map_err(GeometricError::Json)?;
        let opts = serde_json::json!({ "limit": limit }).to_string();
        let raw: Vec<String> = self.fcall(conn, "GNODE_TOPO_QUERY", &[&lo.to_string(), &hi.to_string(), &f, &opts])?;
        raw.iter().map(|r| serde_json::from_str(r).map_err(GeometricError::Json)).collect()
    }

    pub fn route(&self, conn: &mut redis::Connection, service: &str) -> Result<Routed> {
        let r: Vec<String> = self.fcall(conn, "GNODE_TOPO_ROUTE", &[service])?;
        if r.len() < 3 {
            return Err(GeometricError::InvalidState(format!("route reply too short: {r:?}")));
        }
        Ok(Routed { placement: r[0].clone(), node: r[1].clone(), standby: r[2] == "standby" })
    }

    pub fn can_reach(&self, conn: &mut redis::Connection, caller: &str, target: &str, now_ms: u64) -> Result<Reach> {
        let r: Vec<String> = self.fcall(conn, "GNODE_TOPO_CAN_REACH", &[caller, target, &now_ms.to_string()])?;
        Ok(Reach { allowed: r.first().map(|v| v == "1").unwrap_or(false), reason: r.get(1).cloned().unwrap_or_default() })
    }

    pub fn label(&self, conn: &mut redis::Connection, id: &str, holds: &[String], requires: &[String], peers: &[String]) -> Result<()> {
        let h = serde_json::to_string(holds).unwrap_or_else(|_| "[]".into());
        let r = serde_json::to_string(requires).unwrap_or_else(|_| "[]".into());
        let p = serde_json::to_string(peers).unwrap_or_else(|_| "[]".into());
        let _: String = self.fcall(conn, "GNODE_TOPO_LABEL", &[id, &h, &r, &p])?;
        Ok(())
    }

    pub fn deny(&self, conn: &mut redis::Connection, caller: &str, target: &str, now_ms: u64, ttl_ms: u64) -> Result<()> {
        let _: String = self.fcall(conn, "GNODE_TOPO_DENY", &[caller, target, &now_ms.to_string(), &ttl_ms.to_string()])?;
        Ok(())
    }

    pub fn depart(&self, conn: &mut redis::Connection, placement: &str, correlation_id: &str, now_ms: u64) -> Result<()> {
        let _: String = self.fcall(conn, "GNODE_TOPO_DEPART", &[placement, correlation_id, &now_ms.to_string()])?;
        Ok(())
    }

    pub fn arrive(&self, conn: &mut redis::Connection, placement: &str, correlation_id: &str) -> Result<bool> {
        let n: i64 = self.fcall(conn, "GNODE_TOPO_ARRIVE", &[placement, correlation_id])?;
        Ok(n == 1)
    }

    pub fn in_flight(&self, conn: &mut redis::Connection, placement: &str, now_ms: u64, ttl_ms: u64) -> Result<u64> {
        let n: i64 = self.fcall(conn, "GNODE_TOPO_INFLIGHT", &[placement, &now_ms.to_string(), &ttl_ms.to_string()])?;
        Ok(n.max(0) as u64)
    }

    /// Lease holder only: placement health from heartbeats, entity health as the max over placements.
    pub fn derive_health(&self, conn: &mut redis::Connection, now_s: u64, fresh_s: u64, node_id: &str) -> Result<HealthPass> {
        let r: Vec<i64> = self.fcall_leased(conn, "GNODE_TOPO_HEALTH_DERIVE", &[&now_s.to_string(), &fresh_s.to_string(), node_id])?;
        Ok(HealthPass { fresh: r.first().copied().unwrap_or(0) as u64, placements: r.get(1).copied().unwrap_or(0) as u64, entities: r.get(2).copied().unwrap_or(0) as u64 })
    }

    /// Lease holder only: derived slots (zone B) of one entity, with the index score the Lua side stores.
    pub fn set_derived(&self, conn: &mut redis::Connection, id: &str, slots: &[(usize, f64)], ts: u64, node_id: &str) -> Result<()> {
        let mut upd = serde_json::Map::new();
        for (slot, value) in slots {
            let fp = FixedPoint::from_f64(*value);
            upd.insert(slot.to_string(), serde_json::json!({ "pd": value, "pr": fp.raw().to_string(), "s": index_score(&fp).to_string() }));
        }
        let updates = serde_json::Value::Object(upd).to_string();
        let _: String = self.fcall_leased(conn, "GNODE_TOPO_SET_DERIVED", &[id, &updates, "", "", &ts.to_string(), "", node_id])?;
        Ok(())
    }

    /// Lease holder only: a derived fact beside the point (`m.scale_out`, `m.share`), merged.
    pub fn set_meta(&self, conn: &mut redis::Connection, id: &str, meta: serde_json::Value, ts: u64, node_id: &str) -> Result<()> {
        let updates = serde_json::json!({ "m": meta }).to_string();
        let _: String = self.fcall_leased(conn, "GNODE_TOPO_SET_DERIVED", &[id, &updates, "", "", &ts.to_string(), "", node_id])?;
        Ok(())
    }

    pub fn verify(&self, conn: &mut redis::Connection) -> Result<String> {
        self.fcall(conn, "GNODE_TOPO_VERIFY", &[])
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct RegisterReply {
    pub ok: bool,
    pub eid: String,
    pub sh: String,
    pub ro: u64,
    pub upd: bool,
}

#[derive(Debug, Clone)]
pub struct Routed {
    pub placement: String,
    pub node: String,
    pub standby: bool,
}

#[derive(Debug, Clone)]
pub struct Reach {
    pub allowed: bool,
    pub reason: String,
}

#[derive(Debug, Clone, Copy)]
pub struct HealthPass {
    pub fresh: u64,
    pub placements: u64,
    pub entities: u64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct Filters {
    /// ordered axis → [lo, hi] as index scores (strings, exact)
    pub axes: BTreeMap<String, [String; 2]>,
    /// colour facet → any of these values
    pub facets: BTreeMap<String, Vec<String>>,
}

impl Filters {
    pub fn facet(mut self, name: &str, values: &[&str]) -> Self {
        self.facets.insert(name.to_string(), values.iter().map(|v| v.to_string()).collect());
        self
    }

    /// An ordered axis between two vocabulary values or two fractions of the unit interval.
    pub fn range(mut self, name: &str, lo: f64, hi: f64) -> Self {
        self.axes.insert(name.to_string(), [score_of(lo), score_of(hi)]);
        self
    }
}

/// A declared axis value: a colour (one or many vocabulary words) or an ordered value
/// (a vocabulary word, or a fraction of the unit interval where the axis is freeform).
#[derive(Debug, Clone, PartialEq)]
pub enum AxisValue {
    Word(String),
    Words(Vec<String>),
    Fraction(f64),
}

/// What a manifest, the installer or the daemon itself declares about one entity.
#[derive(Debug, Clone)]
pub struct Declared {
    pub id: String,
    pub shape: String,
    pub env: String,
    pub axes: BTreeMap<String, AxisValue>,
    pub meta: serde_json::Value,
}

impl Declared {
    pub fn new(id: &str, shape: &str, env: &str) -> Self {
        Self { id: id.to_string(), shape: shape.to_string(), env: env.to_string(), axes: BTreeMap::new(), meta: serde_json::json!({}) }
    }

    pub fn word(mut self, axis: &str, value: &str) -> Self {
        self.axes.insert(axis.to_string(), AxisValue::Word(value.to_string()));
        self
    }

    pub fn words(mut self, axis: &str, values: &[&str]) -> Self {
        self.axes.insert(axis.to_string(), AxisValue::Words(values.iter().map(|v| v.to_string()).collect()));
        self
    }

    pub fn fraction(mut self, axis: &str, value: f64) -> Self {
        self.axes.insert(axis.to_string(), AxisValue::Fraction(value));
        self
    }

    pub fn meta(mut self, meta: serde_json::Value) -> Self {
        self.meta = meta;
        self
    }
}

/// A placement's references (shape 4.1 component@node, 4.2 service@node).
#[derive(Debug, Clone)]
pub struct Placement {
    pub of: String,
    pub on: String,
    pub env: String,
    pub heartbeat: String,
    /// The env segment of the heartbeat key when it differs from `env` (the daemon writes `all`).
    pub heartbeat_env: Option<String>,
    pub weight: u32,
}

impl Placement {
    pub fn id(&self) -> String {
        format!("{}@{}", self.of, self.on)
    }
}

/// The library's arguments for one entity, every number already fixed point.
#[derive(Debug, Clone)]
pub struct Encoded {
    pub id: String,
    pub shape: String,
    pub record: String,
    pub ordered: String,
    pub colours: String,
    pub preserve: String,
    pub pd: Vec<f64>,
    pub pr: Vec<String>,
}

fn score_of(fraction: f64) -> String {
    index_score(&FixedPoint::from_f64(fraction.clamp(0.0, 1.0))).to_string()
}

fn code_of(shape: &Shape, slot_name: &str, index: usize, value: &AxisValue) -> std::result::Result<(f64, Vec<String>), String> {
    let def = shape.slot(index).ok_or_else(|| format!("slot {index} unnamed"))?;
    match (def.kind, value) {
        (Kind::Colour, AxisValue::Word(w)) => {
            let code = *def.values.get(w).ok_or_else(|| format!("{slot_name} = {w} is not in the vocabulary"))?;
            Ok((code, vec![w.clone()]))
        }
        (Kind::Colour, AxisValue::Words(ws)) => {
            if ws.is_empty() {
                return Err(format!("{slot_name} has no value"));
            }
            if !def.many && ws.len() > 1 {
                return Err(format!("{slot_name} takes one value"));
            }
            let code = *def.values.get(&ws[0]).ok_or_else(|| format!("{slot_name} = {} is not in the vocabulary", ws[0]))?;
            for w in ws {
                if !def.values.contains_key(w) {
                    return Err(format!("{slot_name} = {w} is not in the vocabulary"));
                }
            }
            Ok((code, ws.clone()))
        }
        (Kind::Colour, AxisValue::Fraction(_)) => Err(format!("{slot_name} is a colour, not a number")),
        (Kind::Ordered, AxisValue::Word(w)) => {
            let code = *def.values.get(w).ok_or_else(|| format!("{slot_name} = {w} is not a rung of the axis"))?;
            Ok((code, vec![]))
        }
        (Kind::Ordered, AxisValue::Words(_)) => Err(format!("{slot_name} is ordered and takes one value")),
        (Kind::Ordered, AxisValue::Fraction(f)) => {
            if !def.freeform {
                return Err(format!("{slot_name} takes a vocabulary word, not a number"));
            }
            if !f.is_finite() || !(0.0..=1.0).contains(f) {
                return Err(format!("{slot_name} = {f} is outside [0, 1]"));
            }
            Ok((*f, vec![]))
        }
    }
}

/// Declared values → the library's arguments. Fails on the first value the schema refuses,
/// so a wrong manifest is refused here with the axis named, before anything reaches the store.
pub fn encode(schema: &TopologySchema, d: &Declared) -> Result<Encoded> {
    let shape = schema.shape(&d.shape).ok_or_else(|| GeometricError::InvalidState(format!("unknown shape {}", d.shape)))?;
    let code = layer_code(&d.shape).map_err(GeometricError::InvalidState)?;
    let mut pd = vec![0.0f64; WIDTH];
    let mut pr: Vec<String> = vec![FixedPoint::from_f64(0.0).raw().to_string(); WIDTH];
    let layer = layer_coordinate(code);
    pd[0] = code as f64 / (1u64 << 48) as f64;
    pr[0] = layer.raw().to_string();

    let mut ordered: BTreeMap<String, String> = BTreeMap::new();
    let mut colours: BTreeMap<String, Vec<String>> = BTreeMap::new();

    let env_def = shape.slot(1).ok_or_else(|| GeometricError::InvalidState("shape without environment".into()))?;
    let env_code = *env_def.values.get(&d.env)
        .ok_or_else(|| GeometricError::InvalidState(format!("environment {} is not in the vocabulary", d.env)))?;
    pd[1] = env_code;
    pr[1] = FixedPoint::from_f64(env_code).raw().to_string();
    colours.insert("environment".into(), vec![d.env.clone()]);

    for (name, value) in &d.axes {
        let index = shape.resolve(name)
            .ok_or_else(|| GeometricError::InvalidState(format!("{name} is not an axis of shape {}", d.shape)))?;
        if index < 2 {
            return Err(GeometricError::InvalidState(format!("{name} is a header slot and is not declared")));
        }
        let def = shape.slot(index).unwrap();
        if def.writer != Writer::Provider {
            return Err(GeometricError::InvalidState(format!("{name} is written by the {:?}, not declared", def.writer)));
        }
        let (code_f, words) = code_of(shape, name, index, value).map_err(GeometricError::InvalidState)?;
        let fp = FixedPoint::from_f64(code_f);
        pd[index] = code_f;
        pr[index] = fp.raw().to_string();
        match def.kind {
            Kind::Colour => { colours.insert(name.clone(), words); }
            Kind::Ordered => { ordered.insert(name.clone(), index_score(&fp).to_string()); }
        }
    }

    // Zone B at registration: lifecycle active (daemon-written), health and load unknown until derived.
    if shape.slot(LIFECYCLE_SLOT).is_some() {
        pd[LIFECYCLE_SLOT] = LIFECYCLE_ACTIVE;
        pr[LIFECYCLE_SLOT] = FixedPoint::from_f64(LIFECYCLE_ACTIVE).raw().to_string();
        ordered.insert("lifecycle".into(), score_of(LIFECYCLE_ACTIVE));
    }
    if shape.slot(HEALTH_SLOT).is_some() {
        ordered.insert("health".into(), "0".into());
    }

    let preserve: Vec<String> = shape.named().into_iter()
        .filter(|i| *i >= 16 && shape.slot(*i).map(|s| s.writer != Writer::Provider).unwrap_or(false) && *i != LIFECYCLE_SLOT)
        .map(|i| i.to_string())
        .collect();

    let record = serde_json::json!({ "ly": code, "env": d.env, "pd": pd, "pr": pr, "m": d.meta }).to_string();
    Ok(Encoded {
        id: d.id.clone(),
        shape: d.shape.clone(),
        record,
        ordered: serde_json::to_string(&ordered).map_err(GeometricError::Json)?,
        colours: serde_json::to_string(&colours).map_err(GeometricError::Json)?,
        preserve: preserve.join(","),
        pd,
        pr,
    })
}

/// What a node knows about itself without being told (K1): the legacy derivation, kept.
#[derive(Debug, Clone, Default)]
pub struct NodeHardware {
    pub is_master: bool,
    /// `general | inference | storage | compute | content`; anything else stays undeclared with a warning.
    pub node_type: String,
    pub cores: u32,
    pub max_memory_mb: u32,
    /// Operator words for the axes nothing derives (bandwidth, storage, accelerator, residency, zone).
    pub axes: BTreeMap<String, String>,
}

/// A node that runs no daemon (the founder's laptop observer, an inference box behind a
/// tunnel): declared by the operator with its role word; ValKey mode client_only; the
/// specialization and the tiers only when given (D13 deployment shapes).
pub fn declare_operator_node(node_id: &str, role: &str, specialization: Option<&str>, cores: u32, max_memory_mb: u32) -> Declared {
    let mut d = Declared::new(node_id, "1", "global")
        .word("node_role", role)
        .word("valkey_mode", "client_only");
    if let Some(sp) = specialization.filter(|s| !s.is_empty()) {
        d = d.word("specialization", sp);
    }
    if cores > 0 {
        d = d.word("cpu_tier", if cores <= 2 { "minimal" } else if cores <= 8 { "standard" } else { "compute" });
    }
    if max_memory_mb > 0 {
        d = d.word("memory_tier", if max_memory_mb < 4_096 { "minimal" } else if max_memory_mb < 32_768 { "standard" } else { "high_memory" });
    }
    d
}

/// The node entity the daemon declares for the host it runs on. Role and ValKey mode follow the
/// master election; cpu and memory tiers follow the resources the node config reports (0 = unknown,
/// left undeclared); the specialization is the declared node type when it is a vocabulary word.
pub fn declare_node(node_id: &str, hw: &NodeHardware) -> Declared {
    let mut d = Declared::new(node_id, "1", "global")
        .word("node_role", if hw.is_master { "master" } else { "full_node" })
        .word("valkey_mode", if hw.is_master { "primary" } else { "client_only" });
    match hw.node_type.as_str() {
        t @ ("general" | "inference" | "storage" | "compute" | "content") => { d = d.word("specialization", t); }
        "" => {}
        other => log::warn!("node_type {:?} is not a specialization (general|inference|storage|compute|content); left undeclared", other),
    }
    if hw.cores > 0 {
        d = d.word("cpu_tier", if hw.cores <= 2 { "minimal" } else if hw.cores <= 8 { "standard" } else { "compute" });
    }
    if hw.max_memory_mb > 0 {
        d = d.word("memory_tier", if hw.max_memory_mb < 4_096 { "minimal" } else if hw.max_memory_mb < 32_768 { "standard" } else { "high_memory" });
    }
    for (axis, word) in &hw.axes {
        d = d.word(axis, word);
    }
    d
}

/// The gNode component and its placement on this node; the heartbeat the placement reads is the daemon's own.
pub fn declare_gnode_component() -> Declared {
    Declared::new("gNode", "2", "global")
        .words("protocol", &["resp3_direct", "gnode_stream"])
        .words("domain", &["infrastructure", "orchestration"])
        .word("implementation_language", "rust")
        .word("build_type", "cargo_release")
        .word("component_tier", "infrastructure")
        .word("license_tier", "open_source")
        .word("requires_valkey", "yes")
        .word("systemd_managed", "yes")
        .word("native_format", "resp3")
        .word("contract_stability", "stable")
        .meta(serde_json::json!({ "heartbeat": "gnode-daemon" }))
}

pub fn declare_gnode_placement(node_id: &str, version: &str) -> (Placement, Declared) {
    let p = Placement { of: "gNode".into(), on: node_id.into(), env: "global".into(), heartbeat: "gnode-daemon".into(), heartbeat_env: Some("all".into()), weight: 1 };
    let d = Declared::new(&p.id(), "4.1", "global")
        .word("build", "source")
        .fraction("installed_version", version_fraction(version));
    (p, d)
}

/// `0.1.4` → 0.000104…: three fields, each in [0, 100), ordered as the version orders.
pub fn version_fraction(version: &str) -> f64 {
    let mut parts = version.trim_start_matches('v').split('.').map(|p| p.parse::<u32>().unwrap_or(0).min(99));
    let (a, b, c) = (parts.next().unwrap_or(0), parts.next().unwrap_or(0), parts.next().unwrap_or(0));
    ((a as f64) * 10_000.0 + (b as f64) * 100.0 + c as f64) / 1_000_000.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn schema() -> TopologySchema {
        TopologySchema::load(&Path::new(env!("CARGO_MANIFEST_DIR")).join("config/topology_schema.yaml")).unwrap()
    }

    #[test]
    fn the_layer_coordinate_is_exact_and_the_header_is_filled() {
        let e = encode(&schema(), &Declared::new("gflow", "3.3", "production")).unwrap();
        let code = layer_code("3.3").unwrap();
        assert_eq!(e.pr[0], ((code as i128) << 16).to_string());
        assert_eq!(e.pd.len(), 24);
        assert_eq!(e.pd[1], 1.0, "production");
        assert_eq!(e.pd[18], LIFECYCLE_ACTIVE);
        let ordered: BTreeMap<String, String> = serde_json::from_str(&e.ordered).unwrap();
        assert_eq!(ordered["lifecycle"], score_of(LIFECYCLE_ACTIVE));
        assert_eq!(ordered["health"], "0");
        assert!(!ordered.contains_key("fundamentality"), "the default is applied by the library, not declared here");
        assert_eq!(e.preserve, "16,17,19", "sampler and daemon slots are preserved on re-registration");
    }

    #[test]
    fn colours_and_ordered_axes_land_in_their_slots() {
        let d = Declared::new("gflow", "3.3", "production")
            .words("protocol", &["gnode_stream", "resp3_direct"])
            .word("implementation_language", "rust")
            .word("contract_version", "v2")
            .word("latency_class", "interactive")
            .fraction("fundamentality", 0.3);
        let e = encode(&schema(), &d).unwrap();
        let colours: BTreeMap<String, Vec<String>> = serde_json::from_str(&e.colours).unwrap();
        assert_eq!(colours["protocol"], vec!["gnode_stream", "resp3_direct"]);
        assert_eq!(colours["implementation_language"], vec!["rust"]);
        let ordered: BTreeMap<String, String> = serde_json::from_str(&e.ordered).unwrap();
        assert_eq!(ordered["contract_version"], score_of(0.2));
        assert_eq!(ordered["latency_class"], score_of(0.25));
        assert_eq!(ordered["fundamentality"], score_of(0.3));
        assert_eq!(e.pd[2], 0.5, "protocol's first word codes the slot");
        assert_eq!(e.pd[14], 0.3);
        assert_eq!(e.pr[14], FixedPoint::from_f64(0.3).raw().to_string());
    }

    #[test]
    fn an_operator_node_declares_its_role_and_nothing_it_does_not_know() {
        let s = schema();
        let d = declare_operator_node("laptop", "observer", Some("inference"), 0, 0);
        let e = encode(&s, &d).unwrap();
        assert_eq!(e.shape, "1");
        assert!(e.colours.contains("observer") && e.colours.contains("client_only") && e.colours.contains("inference"));
        assert!(!e.colours.contains("cpu_tier"), "undeclared tiers stay undeclared");
        assert!(encode(&s, &declare_operator_node("x", "pilot", None, 0, 0)).is_err(), "a role outside the vocabulary is refused");
    }

    #[test]
    fn the_encoder_refuses_what_the_schema_refuses() {
        let s = schema();
        let bad = [
            Declared::new("x", "9", "production"),
            Declared::new("x", "3.3", "mars"),
            Declared::new("x", "3.3", "production").word("cpu_tier", "gpu"),
            Declared::new("x", "3.3", "production").word("protocol", "carrier_pigeon"),
            Declared::new("x", "3.3", "production").words("auth_method", &["none", "mtls"]),
            Declared::new("x", "3.3", "production").fraction("latency_class", 0.3),
            Declared::new("x", "3.3", "production").fraction("fundamentality", 1.5),
            Declared::new("x", "3.3", "production").word("load", "idle"),
            Declared::new("x", "3.3", "production").word("environment", "staging"),
        ];
        for d in bad {
            assert!(encode(&s, &d).is_err(), "{:?} was accepted", d.axes);
        }
    }

    #[test]
    fn the_node_derives_what_the_legacy_path_derived_and_never_coerces() {
        let s = schema();
        let hw = NodeHardware { is_master: true, node_type: "inference".into(), cores: 32, max_memory_mb: 128_713, axes: [("accelerator_tier".to_string(), "gpu".to_string())].into() };
        let e = encode(&s, &declare_node("squad", &hw)).unwrap();
        let c: BTreeMap<String, Vec<String>> = serde_json::from_str(&e.colours).unwrap();
        assert_eq!(c["node_role"], vec!["master"]);
        assert_eq!(c["valkey_mode"], vec!["primary"]);
        assert_eq!(c["specialization"], vec!["inference"]);
        let o: BTreeMap<String, String> = serde_json::from_str(&e.ordered).unwrap();
        assert_eq!(o["cpu_tier"], score_of(0.67), "32 cores is compute");
        assert_eq!(o["memory_tier"], score_of(1.0), "128 GB is high_memory");
        assert_eq!(o["accelerator_tier"], score_of(0.67), "the operator's word");
        let plain = encode(&s, &declare_node("laptop", &NodeHardware { node_type: "laptop-thing".into(), ..Default::default() })).unwrap();
        let c: BTreeMap<String, Vec<String>> = serde_json::from_str(&plain.colours).unwrap();
        assert!(!c.contains_key("specialization"), "an unknown node type is left undeclared, never coerced");
        assert_eq!(c["valkey_mode"], vec!["client_only"]);
        let o: BTreeMap<String, String> = serde_json::from_str(&plain.ordered).unwrap();
        assert!(!o.contains_key("cpu_tier") && !o.contains_key("memory_tier"), "0 resources stay undeclared");
        assert!(encode(&s, &declare_node("x", &NodeHardware { axes: [("accelerator_tier".to_string(), "warp-drive".to_string())].into(), ..Default::default() })).is_err(), "a word outside the vocabulary is refused by name");
    }

    #[test]
    fn node_component_and_placement_declarations_encode() {
        let s = schema();
        let n = encode(&s, &declare_node("aesir-solutions", &NodeHardware { is_master: true, node_type: "general".into(), cores: 8, max_memory_mb: 64_083, ..Default::default() })).unwrap();
        assert_eq!(n.shape, "1");
        let c = encode(&s, &declare_gnode_component()).unwrap();
        assert_eq!(c.shape, "2");
        let (p, d) = declare_gnode_placement("aesir-solutions", "0.1.4");
        assert_eq!(p.id(), "gNode@aesir-solutions");
        let e = encode(&s, &d).unwrap();
        assert_eq!(e.shape, "4.1");
        assert!(version_fraction("0.1.4") < version_fraction("0.2.0"));
        assert!(version_fraction("1.0.0") > version_fraction("0.99.99"));
        assert_eq!(e.pd[2], version_fraction("0.1.4"));
    }
}
