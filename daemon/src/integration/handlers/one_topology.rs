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
        std::env::var("GNODE_TOPOLOGY_SCHEMA").ok().map(std::path::PathBuf::from).filter(|p| p.exists())
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
