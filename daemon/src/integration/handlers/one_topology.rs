//! `register_service`, `deregister_service`, `geometric_discover` and
//! `geometric_discover_range` on the one topology (SPEC-ONE-TOPOLOGY §4, §13.3).
//!
//! A registration declares axes BY NAME (words for colours and rungs, a fraction where
//! the axis is freeform); the encoder refuses a wrong declaration with the axis named
//! before anything reaches the store. Discovery is one `GNODE_TOPO_QUERY`: a layer
//! interval drives, facets and axis ranges verify, records come back with their colour.
//! Both lanes (sync batch, async concurrent) run the same plan; only the executor differs.

use std::collections::BTreeMap;
use std::sync::OnceLock;

use log::warn;
use redis::aio::MultiplexedConnection as AsyncConnection;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::constellation::{encode, AxisValue, Constellation, Declared, Encoded, Filters, Placement};
use crate::integration::handlers::types::CommandResult;
use crate::topology_schema::{layer_interval, TopologySchema};

static SCHEMA: OnceLock<Option<TopologySchema>> = OnceLock::new();

/// The compiled schema, loaded once from `topology_schema.yaml` beside the tier files.
pub fn schema() -> Option<&'static TopologySchema> {
    SCHEMA.get_or_init(|| {
        TopologySchema::locate()
            .or_else(|| crate::tool_registration::find_schema_path(None).and_then(|sp| TopologySchema::default_path(&sp)))
            .and_then(|tp| match TopologySchema::load(&tp) {
                Ok(s) => Some(s),
                Err(e) => { warn!("topology_schema.yaml does not compile: {}", e); None }
            })
    }).as_ref()
}

/// `register_service` under the one topology.
#[derive(Debug, Deserialize)]
pub struct RegisterParams {
    pub id: String,
    /// `site` | `app` | `daemon` | `component` | `node`, or a shape key such as `3.3`.
    #[serde(default = "default_shape")]
    pub shape: String,
    #[serde(default = "default_env")]
    pub environment: String,
    /// axis name → word, list of words, or fraction in [0, 1]
    #[serde(default)]
    pub axes: BTreeMap<String, Value>,
    #[serde(default)]
    pub metadata: Value,
    /// The node this instance runs on: makes a 4.2 placement with `capacity_weight`.
    #[serde(default)]
    pub on: Option<String>,
    #[serde(default = "default_weight")]
    pub capacity_weight: u32,
    /// The component name in the heartbeat key, when it differs from `id`.
    #[serde(default)]
    pub heartbeat: Option<String>,
    #[serde(default)]
    pub holds: Vec<String>,
    #[serde(default)]
    pub requires: Vec<String>,
    #[serde(default)]
    pub peers: Vec<String>,
    /// The legacy numeric form; refused by name when present.
    #[serde(default)]
    pub capabilities: Option<Value>,
    /// The constellation to register in; must be served by this daemon. Default: the caller's.
    #[serde(default)]
    pub constellation: Option<String>,
}

fn default_shape() -> String { "app".to_string() }
fn default_env() -> String { "production".to_string() }
fn default_weight() -> u32 { 1 }

/// A shape word or key → the shape key the schema knows.
pub fn shape_key(word: &str) -> Result<String, String> {
    Ok(match word {
        "node" | "1" => "1",
        "component" | "2" => "2",
        "site" | "web" | "3.1" => "3.1",
        "app" | "headless" | "3.2" => "3.2",
        "daemon" | "service" | "3.3" => "3.3",
        other => return Err(format!("shape {:?} is not one of site, app, daemon, component, node", other)),
    }.to_string())
}

/// What one registration writes: the entity, optionally its placement, its labels.
#[derive(Debug)]
pub struct RegisterPlan {
    pub entity: Encoded,
    pub placement: Option<(Placement, Encoded)>,
    pub labels: Option<(Vec<String>, Vec<String>, Vec<String>)>,
}

pub fn plan_register(schema: &TopologySchema, p: &RegisterParams) -> Result<RegisterPlan, String> {
    if p.id.is_empty() {
        return Err("Service ID cannot be empty".to_string());
    }
    if p.capabilities.is_some() {
        return Err("capabilities by number are not axes: declare `shape` and `axes` by name".to_string());
    }
    let sh = shape_key(&p.shape)?;
    let mut d = Declared::new(&p.id, &sh, &p.environment);
    let mut meta = if p.metadata.is_object() { p.metadata.clone() } else { json!({}) };
    if let Some(hb) = &p.heartbeat {
        meta["heartbeat"] = json!(hb);
    }
    d = d.meta(meta);
    for (name, v) in &p.axes {
        let value = match v {
            Value::String(w) => AxisValue::Word(w.clone()),
            Value::Array(a) => AxisValue::Words(a.iter().filter_map(|x| x.as_str().map(str::to_string)).collect()),
            Value::Number(n) => AxisValue::Fraction(n.as_f64().unwrap_or(f64::NAN)),
            _ => return Err(format!("axis {} must be a word, a list of words or a fraction", name)),
        };
        d.axes.insert(name.clone(), value);
    }
    let entity = encode(schema, &d).map_err(|e| e.to_string())?;
    let placement = match &p.on {
        Some(node) if sh.starts_with('3') => {
            let pl = Placement {
                of: p.id.clone(), on: node.clone(), env: p.environment.clone(),
                heartbeat: p.heartbeat.clone().unwrap_or_else(|| p.id.clone()), heartbeat_env: None, weight: p.capacity_weight,
            };
            let pd = Declared::new(&pl.id(), "4.2", &p.environment).fraction("capacity_weight", (p.capacity_weight as f64 / 100.0).min(1.0));
            Some((pl, encode(schema, &pd).map_err(|e| e.to_string())?))
        }
        Some(_) => return Err("`on` places a service; nodes and components are placed by heartbeat".to_string()),
        None => None,
    };
    let labels = if p.holds.is_empty() && p.requires.is_empty() && p.peers.is_empty() { None } else { Some((p.holds.clone(), p.requires.clone(), p.peers.clone())) };
    Ok(RegisterPlan { entity, placement, labels })
}

fn fcall(tk: &str, f: &str, args: &[&str]) -> redis::Cmd {
    let mut c = redis::cmd("FCALL");
    c.arg(f).arg(1).arg(tk);
    for a in args { c.arg(*a); }
    c
}

/// The FCALLs a plan issues, in order (pure, so a test can read them).
pub fn register_cmds(c: &Constellation, plan: &RegisterPlan) -> Vec<redis::Cmd> {
    let tk = c.key();
    let e = &plan.entity;
    let mut cmds = vec![
        fcall(&tk, "GNODE_ENSURE_CONSTELLATION", &[]),
        fcall(&tk, "GNODE_TOPO_REGISTER", &[&e.id, &e.shape, &e.record, &e.ordered, &e.colours, &e.preserve]),
    ];
    if let Some((p, pe)) = &plan.placement {
        let w = p.weight.to_string();
        cmds.push(fcall(&tk, "GNODE_TOPO_PLACE", &[&pe.id, &pe.shape, &p.of, &p.on, &p.env, &p.heartbeat, &w, &pe.record, &pe.ordered, &pe.colours, ""]));
    }
    if let Some((h, r, pe)) = &plan.labels {
        let (h, r, pe) = (serde_json::to_string(h).unwrap_or_default(), serde_json::to_string(r).unwrap_or_default(), serde_json::to_string(pe).unwrap_or_default());
        cmds.push(fcall(&tk, "GNODE_TOPO_LABEL", &[&e.id, &h, &r, &pe]));
    }
    cmds
}

fn register_reply(plan: &RegisterPlan, c: &Constellation) -> CommandResult {
    CommandResult::success(json!({
        "status": "ok",
        "service_id": plan.entity.id,
        "registered": true,
        "shape": plan.entity.shape,
        "constellation": c.name,
        "topology_key": c.key(),
        "placement": plan.placement.as_ref().map(|(p, _)| p.id()),
    }))
}

pub fn register(conn: &mut redis::Connection, site_id: &str, params: Value) -> CommandResult {
    let Some(schema) = schema() else { return CommandResult::error("topology schema not loaded") };
    let p: RegisterParams = match serde_json::from_value(params) { Ok(p) => p, Err(e) => return CommandResult::error(format!("Invalid parameters: {}", e)) };
    let c = match crate::constellation::named_or_for_site(conn, p.constellation.as_deref(), site_id) { Ok(c) => c, Err(e) => return CommandResult::error(e) };
    let c = &c;
    let plan = match plan_register(schema, &p) { Ok(p) => p, Err(e) => return CommandResult::error(e) };
    for cmd in register_cmds(c, &plan) {
        if let Err(e) = cmd.query::<String>(conn) {
            return CommandResult::error(format!("Registration refused: {}", e));
        }
    }
    register_reply(&plan, c)
}

pub async fn register_async(conn: &mut AsyncConnection, site_id: &str, params: Value) -> CommandResult {
    let Some(schema) = schema() else { return CommandResult::error("topology schema not loaded") };
    let p: RegisterParams = match serde_json::from_value(params) { Ok(p) => p, Err(e) => return CommandResult::error(format!("Invalid parameters: {}", e)) };
    let c = match p.constellation.as_deref() {
        Some(n) => match crate::constellation::served().into_iter().find(|c| c.name == n) { Some(c) => c, None => return CommandResult::error(format!("constellation {} is not served by this daemon", n)) },
        None => crate::constellation::for_entity_async(conn, site_id).await,
    };
    let c = &c;
    let plan = match plan_register(schema, &p) { Ok(p) => p, Err(e) => return CommandResult::error(e) };
    for cmd in register_cmds(c, &plan) {
        if let Err(e) = cmd.query_async::<String>(conn).await {
            return CommandResult::error(format!("Registration refused: {}", e));
        }
    }
    register_reply(&plan, c)
}

#[derive(Debug, Deserialize)]
pub struct DeregisterParams {
    #[serde(alias = "id")]
    pub service_id: String,
}

fn deregister_reply(id: &str, r: redis::RedisResult<String>) -> CommandResult {
    match r {
        Ok(_) => CommandResult::success(json!({ "status": "deregistered", "service_id": id })),
        Err(e) if e.to_string().contains("unknown") => CommandResult::success(json!({ "status": "not_found", "service_id": id })),
        Err(e) => CommandResult::error(format!("Deregistration refused: {}", e)),
    }
}

pub fn deregister(conn: &mut redis::Connection, _site_id: &str, params: Value) -> CommandResult {
    let p: DeregisterParams = match serde_json::from_value(params) { Ok(p) => p, Err(e) => return CommandResult::error(format!("Invalid parameters: {}", e)) };
    if p.service_id.is_empty() { return CommandResult::error("Service ID cannot be empty"); }
    if p.service_id == "gNode" || p.service_id.starts_with("gNode@") { return CommandResult::error("Cannot deregister the gNode component itself"); }
    let c = crate::constellation::for_entity(conn, &p.service_id);
    deregister_reply(&p.service_id, fcall(&c.key(), "GNODE_TOPO_DEREGISTER", &[&p.service_id]).query(conn))
}

pub async fn deregister_async(conn: &mut AsyncConnection, _site_id: &str, params: Value) -> CommandResult {
    let p: DeregisterParams = match serde_json::from_value(params) { Ok(p) => p, Err(e) => return CommandResult::error(format!("Invalid parameters: {}", e)) };
    if p.service_id.is_empty() { return CommandResult::error("Service ID cannot be empty"); }
    if p.service_id == "gNode" || p.service_id.starts_with("gNode@") { return CommandResult::error("Cannot deregister the gNode component itself"); }
    let c = crate::constellation::for_entity_async(conn, &p.service_id).await;
    deregister_reply(&p.service_id, fcall(&c.key(), "GNODE_TOPO_DEREGISTER", &[&p.service_id]).query_async(conn).await)
}

/// `geometric_discover` and `geometric_discover_range` under the one topology.
#[derive(Debug, Deserialize)]
pub struct DiscoverParams {
    /// A shape word or layer key; default every service.
    #[serde(default = "default_layer")]
    pub layer: String,
    #[serde(default)]
    pub facets: BTreeMap<String, Value>,
    /// ordered axis → [lo, hi] as fractions of the unit interval, or vocabulary words
    #[serde(default)]
    pub axes: BTreeMap<String, Value>,
    #[serde(default)]
    pub limit: usize,
    /// Legacy forms, refused by name.
    #[serde(default)]
    pub capabilities: Option<Value>,
    #[serde(default)]
    pub requirements: Option<Value>,
}

fn default_layer() -> String { "3".to_string() }

pub fn plan_discover(schema: &TopologySchema, p: &DiscoverParams) -> Result<(String, Filters), String> {
    if p.capabilities.is_some() {
        return Err("capabilities by number are not axes: name `facets` (words) and `axes` ([lo, hi])".to_string());
    }
    if p.requirements.is_some() {
        return Err("requirements by axis index are not axes: name `axes` ({name: [lo, hi]})".to_string());
    }
    let layer = match p.layer.as_str() {
        "3" | "4" | "1" | "2" => p.layer.clone(),
        w => shape_key(w)?,
    };
    layer_interval(&layer)?;
    let mut f = Filters::default();
    for (name, v) in &p.facets {
        let words: Vec<&str> = match v {
            Value::String(s) => vec![s.as_str()],
            Value::Array(a) => a.iter().filter_map(|x| x.as_str()).collect(),
            _ => return Err(format!("facet {} must be a word or a list of words", name)),
        };
        f = f.facet(name, &words);
    }
    for (name, v) in &p.axes {
        let bounds = v.as_array().ok_or_else(|| format!("axis {} must be [lo, hi]", name))?;
        if bounds.len() != 2 { return Err(format!("axis {} must be [lo, hi]", name)); }
        let code = |b: &Value| -> Result<f64, String> {
            if let Some(n) = b.as_f64() { return Ok(n); }
            let word = b.as_str().ok_or_else(|| format!("axis {} bound must be a fraction or a word", name))?;
            // A word means what it means INSIDE the queried layer: `contract_version` has one
            // ladder on components and another on services.
            let in_layer = |k: &&str| *k == layer || k.starts_with(&format!("{}.", layer));
            let code = schema.shape_keys().into_iter().filter(in_layer)
                .find_map(|k| schema.shape(k).and_then(|s| s.resolve(name).and_then(|i| s.slot(i)).and_then(|d| d.values.get(word).copied())));
            code.ok_or_else(|| format!("{} = {} is not a rung of the axis in layer {}", name, word, layer))
        };
        f = f.range(name, code(&bounds[0])?, code(&bounds[1])?);
    }
    Ok((layer, f))
}

fn discover_reply(records: Vec<Value>, layer: &str, c: &Constellation) -> CommandResult {
    let results: Vec<Value> = records.iter().map(|r| json!({
        "service_id": r["id"], "shape": r["sh"], "environment": r["env"], "colour": r["c"], "metadata": r["m"],
        "health": r["pd"][17], "load": r["pd"][16],
    })).collect();
    CommandResult::success(json!({ "total_matches": results.len(), "results": results, "layer": layer, "topology_key": c.key(), "lookup": "index" }))
}

pub fn discover(conn: &mut redis::Connection, site_id: &str, params: Value) -> CommandResult {
    let Some(schema) = schema() else { return CommandResult::error("topology schema not loaded") };
    let c = &crate::constellation::for_entity(conn, site_id);
    let p: DiscoverParams = match serde_json::from_value(params) { Ok(p) => p, Err(e) => return CommandResult::error(format!("Invalid parameters: {}", e)) };
    let (layer, f) = match plan_discover(schema, &p) { Ok(x) => x, Err(e) => return CommandResult::error(e) };
    match c.query(conn, &layer, &f, p.limit) {
        Ok(records) => discover_reply(records, &layer, c),
        Err(e) => CommandResult::error(format!("Discovery refused: {}", e)),
    }
}

pub async fn discover_async(conn: &mut AsyncConnection, site_id: &str, params: Value) -> CommandResult {
    let Some(schema) = schema() else { return CommandResult::error("topology schema not loaded") };
    let c = &crate::constellation::for_entity_async(conn, site_id).await;
    let p: DiscoverParams = match serde_json::from_value(params) { Ok(p) => p, Err(e) => return CommandResult::error(format!("Invalid parameters: {}", e)) };
    let (layer, f) = match plan_discover(schema, &p) { Ok(x) => x, Err(e) => return CommandResult::error(e) };
    let (lo, hi) = layer_interval(&layer).unwrap();
    let filters = serde_json::to_string(&f).unwrap_or_default();
    let opts = json!({ "limit": p.limit }).to_string();
    let raw: redis::RedisResult<Vec<String>> = fcall(&c.key(), "GNODE_TOPO_QUERY", &[&lo.to_string(), &hi.to_string(), &filters, &opts]).query_async(conn).await;
    match raw {
        Ok(rs) => discover_reply(rs.iter().filter_map(|r| serde_json::from_str(r).ok()).collect(), &layer, c),
        Err(e) => CommandResult::error(format!("Discovery refused: {}", e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn schema() -> TopologySchema {
        TopologySchema::load(&Path::new(env!("CARGO_MANIFEST_DIR")).join("config/topology_schema.yaml")).unwrap()
    }

    #[test]
    fn a_registration_declares_axes_by_name_and_may_place_itself() {
        let s = schema();
        let p: RegisterParams = serde_json::from_value(json!({
            "id": "gflow", "shape": "daemon", "environment": "production",
            "axes": { "protocol": ["gnode_stream"], "domain": ["workflow"], "implementation_language": "rust", "contract_version": "v2", "fundamentality": 0.3 },
            "metadata": { "manifest": "gFlow/geodeploy.yaml" }, "on": "aesir-solutions", "capacity_weight": 3, "heartbeat": "gflow",
            "holds": ["infra"], "requires": []
        })).unwrap();
        let plan = plan_register(&s, &p).unwrap();
        assert_eq!(plan.entity.shape, "3.3");
        let (pl, pe) = plan.placement.as_ref().unwrap();
        assert_eq!(pl.id(), "gflow@aesir-solutions");
        assert_eq!(pl.weight, 3);
        assert_eq!(pe.shape, "4.2");
        assert!(plan.labels.is_some());
        let cmds = register_cmds(&Constellation::new("geodineum"), &plan);
        assert_eq!(cmds.len(), 4, "ensure, register, place, label");
        let record: Value = serde_json::from_str(&plan.entity.record).unwrap();
        assert_eq!(record["m"]["heartbeat"], "gflow");
    }

    #[test]
    fn the_legacy_forms_are_refused_by_name() {
        let s = schema();
        let p: RegisterParams = serde_json::from_value(json!({ "id": "x", "capabilities": { "protocol": 0.5 } })).unwrap();
        assert!(plan_register(&s, &p).unwrap_err().contains("by name"));
        let p: RegisterParams = serde_json::from_value(json!({ "id": "x", "shape": "pipeline" })).unwrap();
        assert!(plan_register(&s, &p).unwrap_err().contains("shape"));
        let p: RegisterParams = serde_json::from_value(json!({ "id": "x", "shape": "node", "on": "aesir-solutions" })).unwrap();
        assert!(plan_register(&s, &p).unwrap_err().contains("heartbeat"));
        let p: RegisterParams = serde_json::from_value(json!({ "id": "x", "shape": "daemon", "axes": { "protocol": ["pigeon"] } })).unwrap();
        assert!(plan_register(&s, &p).unwrap_err().contains("vocabulary"));
        let d: DiscoverParams = serde_json::from_value(json!({ "capabilities": { "domain_primary": 0.5 } })).unwrap();
        assert!(plan_discover(&s, &d).unwrap_err().contains("by number"));
        let d: DiscoverParams = serde_json::from_value(json!({ "requirements": { "8": { "eq": 0.9 } } })).unwrap();
        assert!(plan_discover(&s, &d).unwrap_err().contains("index"));
    }

    #[test]
    fn a_discovery_names_facets_and_axis_ranges_by_word_or_fraction() {
        let s = schema();
        let d: DiscoverParams = serde_json::from_value(json!({ "layer": "daemon", "facets": { "implementation_language": "rust", "protocol": ["gnode_stream", "resp3_direct"] }, "axes": { "contract_version": ["v2", "v5"], "latency_class": [0.0, 0.5] }, "limit": 5 })).unwrap();
        let (layer, f) = plan_discover(&s, &d).unwrap();
        assert_eq!(layer, "3.3");
        assert_eq!(f.facets["protocol"].len(), 2);
        assert!(f.axes.contains_key("contract_version") && f.axes.contains_key("latency_class"));
        let d: DiscoverParams = serde_json::from_value(json!({})).unwrap();
        assert_eq!(plan_discover(&s, &d).unwrap().0, "3", "default: every service");
    }
}

// ============================================================================
// Edges on the one topology (K7): the daemon's only edge surface once the 3-D handlers go.
// An edge exists only if φ(from) > φ(to); the library refuses the rest (P-c).
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct EdgeParams {
    pub from: String,
    pub to: String,
    #[serde(default)]
    pub metadata: Value,
}

#[derive(Debug, Deserialize)]
pub struct EdgeListParams {
    pub id: String,
    #[serde(default = "default_dir")]
    pub direction: String,
}

fn default_dir() -> String { "outgoing".to_string() }

fn edge_reply(from: &str, to: &str, r: redis::RedisResult<String>) -> CommandResult {
    match r {
        Ok(_) => CommandResult::success(json!({ "ok": true, "from": from, "to": to })),
        Err(e) => CommandResult::error(format!("edge {} -> {} refused: {}", from, to, e)),
    }
}

/// The potential check without the write: what `GNODE_TOPO_ADD_EDGE` would decide.
pub fn validate_edge(conn: &mut redis::Connection, c: &Constellation, from: &str, to: &str) -> CommandResult {
    let tk = c.key();
    let score = |conn: &mut redis::Connection, axis: &str, id: &str| -> Option<f64> {
        redis::cmd("ZSCORE").arg(format!("{}:ix:{}", tk, axis)).arg(id).query::<Option<f64>>(conn).ok().flatten()
    };
    let (Some(lf), Some(lt)) = (score(conn, "layer", from), score(conn, "layer", to)) else {
        return CommandResult::success(json!({ "valid": false, "reason": "both ends must carry a layer" }));
    };
    let pf = score(conn, "fundamentality", from).unwrap_or(0.0);
    let pt = score(conn, "fundamentality", to).unwrap_or(0.0);
    let (tf, tt) = ((lf / 2f64.powi(40)).floor(), (lt / 2f64.powi(40)).floor());
    let valid = from != to && (tf > tt || (tf == tt && pf > pt));
    CommandResult::success(json!({
        "valid": valid, "from": from, "to": to,
        "phi_from": [tf, pf], "phi_to": [tt, pt],
        "reason": if valid { Value::Null } else { json!("phi(from) must exceed phi(to): the top-level layer digit, then fundamentality") },
    }))
}

pub fn edge_add(conn: &mut redis::Connection, site_id: &str, params: Value) -> CommandResult {
    let p: EdgeParams = match serde_json::from_value(params) { Ok(p) => p, Err(e) => return CommandResult::error(format!("Invalid parameters: {}", e)) };
    let c = crate::constellation::for_entity(conn, &p.from);
    let meta = if p.metadata.is_object() { p.metadata.to_string() } else { "{}".to_string() };
    edge_reply(&p.from, &p.to, fcall(&c.key(), "GNODE_TOPO_ADD_EDGE", &[&p.from, &p.to, &meta]).query(conn))
}

pub fn edge_remove(conn: &mut redis::Connection, _site_id: &str, params: Value) -> CommandResult {
    let p: EdgeParams = match serde_json::from_value(params) { Ok(p) => p, Err(e) => return CommandResult::error(format!("Invalid parameters: {}", e)) };
    let c = crate::constellation::for_entity(conn, &p.from);
    edge_reply(&p.from, &p.to, fcall(&c.key(), "GNODE_TOPO_REMOVE_EDGE", &[&p.from, &p.to]).query(conn))
}

pub fn edge_list(conn: &mut redis::Connection, _site_id: &str, params: Value) -> CommandResult {
    let p: EdgeListParams = match serde_json::from_value(params) { Ok(p) => p, Err(e) => return CommandResult::error(format!("Invalid parameters: {}", e)) };
    let c = crate::constellation::for_entity(conn, &p.id);
    let set = match p.direction.as_str() { "incoming" => "in", "outgoing" => "out", other => return CommandResult::error(format!("direction {} is not outgoing or incoming", other)) };
    let ids: redis::RedisResult<Vec<String>> = redis::cmd("SMEMBERS").arg(format!("{}:{}:{}", c.key(), set, p.id)).query(conn);
    match ids {
        Ok(mut v) => { v.sort(); CommandResult::success(json!({ "id": p.id, "direction": p.direction, "edges": v })) }
        Err(e) => CommandResult::error(format!("edge list failed: {}", e)),
    }
}

pub fn edge_validate(conn: &mut redis::Connection, _site_id: &str, params: Value) -> CommandResult {
    let p: EdgeParams = match serde_json::from_value(params) { Ok(p) => p, Err(e) => return CommandResult::error(format!("Invalid parameters: {}", e)) };
    let c = crate::constellation::for_entity(conn, &p.from);
    validate_edge(conn, &c, &p.from, &p.to)
}

use crate::daemon::Command;
use crate::integration::handlers::types::{AsyncCommandHandlerFn, CommandDescriptor, CommandHandlerFn};
use crate::GeometricTopology;
use std::sync::{Arc, RwLock};

macro_rules! sync_edge_handler {
    ($name:ident, $f:ident) => {
        fn $name(command: &Command, conn: &mut redis::Connection, _t: &Arc<RwLock<GeometricTopology>>, site_id: &str, _debug: bool) -> CommandResult {
            $f(conn, site_id, command.parameters.clone())
        }
    };
}
sync_edge_handler!(h_edge_add, edge_add);
sync_edge_handler!(h_edge_remove, edge_remove);
sync_edge_handler!(h_edge_list, edge_list);
sync_edge_handler!(h_edge_validate, edge_validate);

/// Registers the edge commands. They write through the Lua potential check, so they run on
/// the sync lane only: an edge decision is ordered by nature.
pub fn register_commands(
    handlers: &mut std::collections::HashMap<String, CommandHandlerFn>,
    _async_handlers: &mut std::collections::HashMap<String, AsyncCommandHandlerFn>,
    descriptors: &mut Vec<CommandDescriptor>,
) {
    handlers.insert("topology_edge_add".to_string(), h_edge_add as CommandHandlerFn);
    handlers.insert("topology_edge_remove".to_string(), h_edge_remove as CommandHandlerFn);
    handlers.insert("topology_edge_list".to_string(), h_edge_list as CommandHandlerFn);
    handlers.insert("topology_edge_validate".to_string(), h_edge_validate as CommandHandlerFn);
    let edge_params = json!({ "type": "object", "properties": { "from": {"type": "string"}, "to": {"type": "string"}, "metadata": {"type": "object"} }, "required": ["from", "to"] });
    descriptors.push(CommandDescriptor {
        name: "topology_edge_add", category: "topology",
        description: "Add a dependency edge from -> to in the constellation; stored only if phi(from) > phi(to) (top-level layer, then fundamentality), so a cycle is impossible",
        params_schema: edge_params.clone(),
        returns_schema: json!({ "type": "object", "properties": { "ok": {"type": "boolean"}, "from": {"type": "string"}, "to": {"type": "string"} } }),
        example: r#"{"cmd":"topology_edge_add","params":{"from":"nierto_com","to":"gCore"}}"#, async_capable: false, lane: crate::integration::handlers::types::Lane::Ordered,
    });
    descriptors.push(CommandDescriptor {
        name: "topology_edge_remove", category: "topology",
        description: "Remove the dependency edge from -> to",
        params_schema: edge_params.clone(),
        returns_schema: json!({ "type": "object", "properties": { "ok": {"type": "boolean"} } }),
        example: r#"{"cmd":"topology_edge_remove","params":{"from":"nierto_com","to":"gCore"}}"#, async_capable: false, lane: crate::integration::handlers::types::Lane::Ordered,
    });
    descriptors.push(CommandDescriptor {
        name: "topology_edge_list", category: "topology",
        description: "The ids an entity depends on (outgoing) or that depend on it (incoming), sorted",
        params_schema: json!({ "type": "object", "properties": { "id": {"type": "string"}, "direction": {"type": "string", "enum": ["outgoing", "incoming"]} }, "required": ["id"] }),
        returns_schema: json!({ "type": "object", "properties": { "id": {"type": "string"}, "direction": {"type": "string"}, "edges": {"type": "array", "items": {"type": "string"}} } }),
        example: r#"{"cmd":"topology_edge_list","params":{"id":"nierto_com","direction":"outgoing"}}"#, async_capable: false, lane: crate::integration::handlers::types::Lane::Ordered,
    });
    descriptors.push(CommandDescriptor {
        name: "topology_edge_validate", category: "topology",
        description: "Dry run of topology_edge_add: the potential of both ends and whether the edge would be stored",
        params_schema: edge_params,
        returns_schema: json!({ "type": "object", "properties": { "valid": {"type": "boolean"}, "phi_from": {"type": "array"}, "phi_to": {"type": "array"}, "reason": {"type": ["string", "null"]} } }),
        example: r#"{"cmd":"topology_edge_validate","params":{"from":"gCore","to":"nierto_com"}}"#, async_capable: false, lane: crate::integration::handlers::types::Lane::Ordered,
    });
}

// ============================================================================
// K5: `discover_with_endpoints` picks its services with the same query; the handler enriches.
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct DiscoverIdsParams {
    #[serde(default = "default_layer")]
    pub layer: String,
    #[serde(default)]
    pub facets: BTreeMap<String, Value>,
    #[serde(default)]
    pub axes: BTreeMap<String, Value>,
    #[serde(default)]
    pub limit: usize,
    /// The legacy list of capability NAMES; refused by name (a colour is a word, not a presence).
    #[serde(default)]
    pub capabilities: Option<Value>,
}

fn plan_ids(p: &DiscoverIdsParams) -> Result<(String, Filters), String> {
    let Some(schema) = schema() else { return Err("topology schema not loaded".to_string()) };
    if p.capabilities.is_some() {
        return Err("capabilities by name are not axes: name `facets` (words) and `axes` ([lo, hi])".to_string());
    }
    plan_discover(schema, &DiscoverParams { layer: p.layer.clone(), facets: p.facets.clone(), axes: p.axes.clone(), limit: p.limit, capabilities: None, requirements: None })
}

pub fn discover_ids(conn: &mut redis::Connection, site_id: &str, params: &Value) -> Result<Vec<String>, String> {
    let p: DiscoverIdsParams = serde_json::from_value(params.clone()).map_err(|e| format!("Invalid parameters: {}", e))?;
    let (layer, f) = plan_ids(&p)?;
    let c = crate::constellation::for_entity(conn, site_id);
    let mut ids: Vec<String> = c.query(conn, &layer, &f, p.limit).map_err(|e| e.to_string())?
        .iter().filter_map(|r| r["id"].as_str().map(str::to_string)).collect();
    ids.sort();
    Ok(ids)
}

pub async fn discover_ids_async(conn: &mut AsyncConnection, site_id: &str, params: &Value) -> Result<Vec<String>, String> {
    let p: DiscoverIdsParams = serde_json::from_value(params.clone()).map_err(|e| format!("Invalid parameters: {}", e))?;
    let (layer, f) = plan_ids(&p)?;
    let c = crate::constellation::for_entity_async(conn, site_id).await;
    let (lo, hi) = layer_interval(&layer)?;
    let filters = serde_json::to_string(&f).unwrap_or_default();
    let opts = json!({ "limit": p.limit }).to_string();
    let raw: Vec<String> = fcall(&c.key(), "GNODE_TOPO_QUERY", &[&lo.to_string(), &hi.to_string(), &filters, &opts]).query_async(conn).await.map_err(|e| e.to_string())?;
    let mut ids: Vec<String> = raw.iter().filter_map(|r| serde_json::from_str::<Value>(r).ok()).filter_map(|r| r["id"].as_str().map(str::to_string)).collect();
    ids.sort();
    Ok(ids)
}

// ============================================================================
// K2: the components of the ecosystem catalogue register by name as shape 2.
// ============================================================================

/// `ecosystem_tools.yaml` component → `register_service` parameters by name: the tool-tier
/// capability names mapped to the component body (`domain_primary` → `domain`, `api_version` →
/// `contract_version`), `data_sensitivity` → a `requires` label, the pyramid and order axes dropped
/// (positions the index replaces), the heartbeat name from the catalogue or the known writers.
/// Legacy catalogue and profile words → the one topology's axis names (the log §3
/// renames): `domain_primary`/`domain_secondary` fold into `domain`, `api_version` is
/// `contract_version`, clearance and sensitivity become `requires` labels, and the axes
/// that left the vector are dropped. Words the schema does not know are refused later,
/// by name, when the plan is encoded.
fn words_by_name(caps: &[crate::tool_registration::CapabilityEntry]) -> (serde_json::Map<String, Value>, Vec<String>) {
    let mut axes = serde_json::Map::new();
    let mut requires: Vec<String> = Vec::new();
    let mut push_domain = |axes: &mut serde_json::Map<String, Value>, w: Value| {
        let e = axes.entry("domain").or_insert_with(|| json!([]));
        if let Some(a) = e.as_array_mut() {
            match w { Value::Array(ws) => a.extend(ws), v => a.push(v) }
        }
    };
    for cap in caps {
        let word: Value = match &cap.value {
            serde_yaml::Value::String(s) => json!(s),
            serde_yaml::Value::Bool(b) => json!(if *b { "yes" } else { "no" }),
            serde_yaml::Value::Number(n) => json!(n.to_string()),
            serde_yaml::Value::Sequence(seq) => json!(seq.iter().filter_map(|v| v.as_str().map(str::to_string)).collect::<Vec<_>>()),
            _ => continue,
        };
        match cap.name.as_str() {
            "domain" | "domain_primary" | "domain_secondary" => push_domain(&mut axes, word),
            "protocol" => { axes.insert("protocol".into(), if word.is_array() { word } else { json!([word]) }); }
            "api_version" => { axes.insert("contract_version".into(), word); }
            "clearance_required" | "data_sensitivity" => {
                if let Some(w) = word.as_str() {
                    if !matches!(w, "public" | "public_data" | "none" | "undefined") && !requires.iter().any(|r| r == w) { requires.push(w.to_string()); }
                }
            }
            "specialization" | "pipeline_stage" | "service_tier" | "environment" | "registration_order"
            | "pyramid_x" | "pyramid_y" | "pyramid_z" | "user_x" | "user_y" | "user_z" => {}
            other => { axes.insert(other.into(), word); }
        }
    }
    (axes, requires)
}

/// `ecosystem_tools.yaml` component → `register_service` parameters by name: shape 2,
/// environment global, the catalogue's words, its `requires` labels, `depends_on` kept in
/// the metadata as the edge list, and the heartbeat name where the writer's differs.
pub fn component_params(comp: &crate::tool_registration::ToolServiceDef) -> Value {
    let (axes, mut requires) = words_by_name(&comp.capabilities);
    for r in &comp.requires { if !requires.contains(r) { requires.push(r.clone()); } }
    let heartbeat = comp.metadata.as_ref().and_then(|m| m.heartbeat.clone()).or_else(|| match comp.id.as_str() {
        "gNode" => Some("gnode-daemon".into()), "Geodineum-COMMS" => Some("comms".into()),
        "gFlow" => Some("gflow".into()), "gSchedule" => Some("gschedule".into()), _ => None,
    });
    let mut metadata = json!({});
    if let Some(m) = &comp.metadata {
        if let Some(d) = &m.description { metadata["description"] = json!(d); }
        if let Some(a) = &m.affordances { metadata["affordances"] = json!(a); }
        if let Some(k) = &m.schema_keys { metadata["schema_keys"] = json!(k); }
    }
    if !comp.depends_on.is_empty() { metadata["depends_on"] = json!(comp.depends_on); }
    let mut p = json!({ "id": comp.id, "shape": "component", "environment": "global", "axes": axes, "metadata": metadata, "requires": requires });
    if let Some(h) = heartbeat { p["heartbeat"] = json!(h); }
    p
}

/// A registration profile (`service_schema.yaml` `profiles:`) → the site's declaration by
/// name: web is a site (3.1), headless an app (3.2), service / system / component a daemon
/// (3.3). The environment is a header slot, never an axis word.
pub fn profile_params(site: &str, profile: &str, caps: &[crate::tool_registration::CapabilityEntry], environment: Option<&str>) -> Value {
    let shape = match profile { "web" => "site", "headless" => "app", _ => "daemon" };
    let (axes, requires) = words_by_name(caps);
    json!({
        "id": site, "shape": shape, "environment": environment.unwrap_or("production"), "axes": axes,
        "metadata": { "type": profile, "description": format!("{} ({} profile)", site, profile) },
        "requires": requires,
    })
}

#[cfg(test)]
mod component_tests {
    use super::*;
    use crate::tool_registration::{CapabilityEntry, ToolServiceDef};

    fn cap(n: &str, v: &str) -> CapabilityEntry { CapabilityEntry { name: n.into(), value: serde_yaml::Value::String(v.into()) } }

    #[test]
    fn a_catalogue_component_registers_by_name_as_shape_two() {
        let comp = ToolServiceDef {
            id: "Geodineum-COMMS".into(), metadata: None, site: None, depends_on: vec!["ValKey".into(), "gNode".into()], requires: vec!["credentials".into()],
            capabilities: vec![cap("protocol", "gnode_stream"), cap("native_format", "json"), cap("api_version", "v2"), cap("contract_stability", "stable"),
                cap("domain_primary", "messaging"), cap("implementation_language", "rust"), cap("build_type", "cargo_release"), cap("component_tier", "infrastructure"),
                cap("license_tier", "open_source"), cap("requires_valkey", "yes"), cap("systemd_managed", "yes"),
                cap("pyramid_x", "0.5"), cap("registration_order", "early")],
        };
        let p = component_params(&comp);
        assert_eq!(p["shape"], "component");
        assert_eq!(p["environment"], "global");
        assert_eq!(p["axes"]["domain"], json!(["messaging"]));
        assert_eq!(p["axes"]["contract_version"], "v2");
        assert_eq!(p["axes"]["protocol"], json!(["gnode_stream"]));
        assert!(p["axes"].get("pyramid_x").is_none() && p["axes"].get("registration_order").is_none() && p["axes"].get("data_sensitivity").is_none());
        assert_eq!(p["requires"], json!(["credentials"]));
        assert_eq!(p["heartbeat"], "comms", "the known writer's name");
        assert_eq!(p["metadata"]["depends_on"], json!(["ValKey", "gNode"]));
        let plan = plan_register(&TopologySchema::load(&std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("config/topology_schema.yaml")).unwrap(),
            &serde_json::from_value::<RegisterParams>(p).unwrap()).unwrap();
        assert_eq!(plan.entity.shape, "2");
        assert!(plan.labels.is_some());
    }
}

#[cfg(test)]
mod profile_tests {
    use super::*;
    use crate::tool_registration::CapabilityEntry;

    fn cap(n: &str, v: &str) -> CapabilityEntry { CapabilityEntry { name: n.into(), value: serde_yaml::Value::String(v.into()) } }

    #[test]
    fn a_web_profile_is_a_site_declared_by_name() {
        let caps = vec![cap("protocol", "http_rest"), cap("api_version", "v1"), cap("clearance_required", "public"), cap("data_sensitivity", "internal"),
            cap("domain_primary", "presentation"), cap("domain_secondary", "content"), cap("specialization", "generalist"), cap("pipeline_stage", "deliver"), cap("latency_class", "interactive")];
        let p = profile_params("nierto_com", "web", &caps, Some("staging"));
        assert_eq!(p["shape"], "site");
        assert_eq!(p["environment"], "staging");
        assert_eq!(p["axes"]["protocol"], json!(["http_rest"]));
        assert_eq!(p["axes"]["contract_version"], "v1");
        assert_eq!(p["axes"]["domain"], json!(["presentation", "content"]));
        assert!(p["axes"].get("specialization").is_none() && p["axes"].get("pipeline_stage").is_none());
        assert_eq!(p["requires"], json!(["internal"]), "public is no label");
        assert_eq!(profile_params("x", "headless", &[], None)["shape"], "app");
        assert_eq!(profile_params("x", "system", &[], None)["shape"], "daemon");
        assert_eq!(profile_params("x", "component", &[], None)["environment"], "production");
    }

    #[test]
    fn a_catalogue_domain_list_and_requires_pass_through() {
        let comp = crate::tool_registration::ToolServiceDef {
            id: "gCore".into(), metadata: None, site: None, depends_on: vec![], requires: vec!["internal".into()],
            capabilities: vec![CapabilityEntry { name: "domain".into(), value: serde_yaml::Value::Sequence(vec!["framework".into(), "presentation".into()]) },
                cap("contract_version", "v2"), cap("protocol", "library")],
        };
        let p = component_params(&comp);
        assert_eq!(p["axes"]["domain"], json!(["framework", "presentation"]));
        assert_eq!(p["axes"]["contract_version"], "v2");
        assert_eq!(p["requires"], json!(["internal"]));
    }
}
