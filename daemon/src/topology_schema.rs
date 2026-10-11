//! One topology schema for every shape (SPEC-ONE-TOPOLOGY §3).
//!
//! A SHAPE is the layer prefix taken down to the depth where slot maps differ
//! (`1` node, `2` component, `3.1` site, `3.2` app, `3.3` daemon, `4.1`
//! component-on-node, `4.2` service-on-node). Every shape has the same width;
//! a slot the shape does not name resolves to `None` and is never a coordinate
//! that compares. Header slots are positions, not axes every shape has.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::geometric_precision::FixedPoint;
use crate::{GeometricError, Result};

pub const WIDTH: usize = 24;
pub const LAYER_SLOT: usize = 0;
pub const LAYER_LEVELS: u32 = 6;
pub const LAYER_BITS: u32 = 8 * LAYER_LEVELS;
/// Q64.64 raw of `code · 2^-48` is `code << 16`; the sorted-set score is the raw shifted back.
pub const SCORE_SHIFT: u32 = 64 - LAYER_BITS;
const BODY_SLOTS: std::ops::RangeInclusive<usize> = 2..=15;
const DERIVED_SLOTS: std::ops::RangeInclusive<usize> = 16..=19;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    Colour,
    Ordered,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Writer {
    Provider,
    Sampler,
    Daemon,
}

impl Default for Writer {
    fn default() -> Self {
        Writer::Provider
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct SlotDef {
    /// Header slots take their name from the header key.
    #[serde(default)]
    pub name: String,
    pub kind: Kind,
    #[serde(default)]
    pub values: BTreeMap<String, f64>,
    #[serde(default)]
    pub many: bool,
    #[serde(default)]
    pub writer: Writer,
    #[serde(default, rename = "static")]
    pub is_static: bool,
    /// Any value in [0, 1] is accepted, not only the vocabulary (fundamentality).
    #[serde(default)]
    pub freeform: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct HeaderSlotDef {
    slot: usize,
    #[serde(flatten)]
    def: SlotDef,
    /// Shapes that carry this header slot; absent = every shape.
    #[serde(default)]
    shapes: Option<Vec<String>>,
}

#[derive(Debug, Clone, Deserialize)]
struct ShapeDef {
    name: String,
    body: String,
    #[serde(default)]
    fundamentality: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
struct SchemaFile {
    schema_version: String,
    width: usize,
    header: BTreeMap<String, HeaderSlotDef>,
    bodies: BTreeMap<String, BTreeMap<usize, SlotDef>>,
    shapes: BTreeMap<String, ShapeDef>,
    #[serde(default)]
    component_ladder: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct Shape {
    pub key: String,
    pub name: String,
    pub slots: Vec<Option<SlotDef>>,
    pub fundamentality_default: Option<f64>,
}

impl Shape {
    pub fn slot(&self, index: usize) -> Option<&SlotDef> {
        self.slots.get(index).and_then(|s| s.as_ref())
    }

    pub fn resolve(&self, name: &str) -> Option<usize> {
        self.slots.iter().position(|s| s.as_ref().map(|d| d.name == name).unwrap_or(false))
    }

    pub fn name_of(&self, index: usize) -> Option<&str> {
        self.slot(index).map(|d| d.name.as_str())
    }

    /// Indices this shape names, ascending.
    pub fn named(&self) -> Vec<usize> {
        (0..self.slots.len()).filter(|i| self.slots[*i].is_some()).collect()
    }

    /// Zone A: static, hashed and indexed.
    pub fn zone_a(&self) -> Vec<usize> {
        self.named().into_iter().filter(|i| *i < *DERIVED_SLOTS.start()).collect()
    }

    pub fn sampler_slots(&self) -> Vec<usize> {
        self.named().into_iter().filter(|i| self.slot(*i).map(|d| d.writer == Writer::Sampler).unwrap_or(false)).collect()
    }
}

#[derive(Debug, Clone)]
pub struct TopologySchema {
    pub version: String,
    pub width: usize,
    shapes: BTreeMap<String, Shape>,
    component_ladder: Vec<String>,
}

impl TopologySchema {
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| GeometricError::Io(e))?;
        let file: SchemaFile = serde_yaml::from_str(&text)
            .map_err(|e| GeometricError::Yaml(format!("{}: {}", path.display(), e)))?;
        Self::compile(file).map_err(GeometricError::Yaml)
    }

    /// `topology_schema.yaml` beside the tier schemas.
    pub fn default_path(service_schema: &Path) -> Option<PathBuf> {
        let p = service_schema.parent()?.join("topology_schema.yaml");
        p.exists().then_some(p)
    }

    /// Where the one schema lives, independent of the tier files (K10): `GNODE_TOPOLOGY_SCHEMA`,
    /// then the in-tree and installed locations.
    pub fn locate() -> Option<PathBuf> {
        if let Some(p) = std::env::var("GNODE_TOPOLOGY_SCHEMA").ok().map(PathBuf::from).filter(|p| p.exists()) {
            return Some(p);
        }
        [
            "daemon/config/topology_schema.yaml",
            "config/topology_schema.yaml",
            "/opt/geodineum/gNode/daemon/config/topology_schema.yaml",
            "/opt/gNode/daemon/config/topology_schema.yaml",
        ].iter().map(PathBuf::from).find(|p| p.exists())
    }

    fn compile(file: SchemaFile) -> std::result::Result<Self, String> {
        if file.width != WIDTH {
            return Err(format!("width must be {}, schema says {}", WIDTH, file.width));
        }
        let mut shapes = BTreeMap::new();
        for (key, def) in &file.shapes {
            layer_code(key)?;
            let body = file.bodies.get(&def.body)
                .ok_or_else(|| format!("shape {} names body {} which does not exist", key, def.body))?;
            let mut slots: Vec<Option<SlotDef>> = vec![None; WIDTH];
            for (name, h) in &file.header {
                if h.slot >= WIDTH {
                    return Err(format!("header {} at slot {} is outside the width", name, h.slot));
                }
                let carried = h.shapes.as_ref().map(|s| s.iter().any(|k| k == key)).unwrap_or(true);
                if carried {
                    let mut d = h.def.clone();
                    d.name = name.clone();
                    slots[h.slot] = Some(d);
                }
            }
            for (index, d) in body {
                if !(BODY_SLOTS.contains(index) || *index == *DERIVED_SLOTS.end()) {
                    return Err(format!("body {} names slot {}: bodies may name 2-15 and 19 only", def.body, index));
                }
                if slots[*index].is_some() {
                    return Err(format!("body {} names slot {} which the header owns", def.body, index));
                }
                if slots.iter().flatten().any(|s| s.name == d.name) {
                    return Err(format!("shape {} names {} twice", key, d.name));
                }
                slots[*index] = Some(d.clone());
            }
            for (i, s) in slots.iter().enumerate().filter_map(|(i, s)| s.as_ref().map(|s| (i, s))) {
                check_values(key, i, s)?;
            }
            shapes.insert(key.clone(), Shape {
                key: key.clone(),
                name: def.name.clone(),
                slots,
                fundamentality_default: def.fundamentality,
            });
        }
        if shapes.is_empty() {
            return Err("no shapes".to_string());
        }
        Ok(Self { version: file.schema_version, width: file.width, shapes, component_ladder: file.component_ladder })
    }

    pub fn shape(&self, key: &str) -> Option<&Shape> {
        self.shapes.get(key)
    }

    pub fn shape_keys(&self) -> Vec<&str> {
        self.shapes.keys().map(String::as_str).collect()
    }

    pub fn resolve(&self, shape: &str, name: &str) -> Option<usize> {
        self.shape(shape)?.resolve(name)
    }

    pub fn name_of(&self, shape: &str, index: usize) -> Option<&str> {
        self.shape(shape)?.name_of(index)
    }

    /// The slots two points may be compared on: the same shape key, the slots it
    /// names in zone A. Different shapes are not a distance.
    pub fn compare_mask(&self, a: &str, b: &str) -> std::result::Result<Vec<usize>, String> {
        if a != b {
            return Err(format!("cross-shape compare refused: {} vs {}", a, b));
        }
        let s = self.shape(a).ok_or_else(|| format!("unknown shape {}", a))?;
        Ok(s.zone_a())
    }

    /// The intra-layer potential when the manifest declares none (D11).
    pub fn fundamentality_default(&self, shape: &str, entity: &str) -> Option<f64> {
        let s = self.shape(shape)?;
        if shape == "2" {
            let n = self.component_ladder.len();
            let pos = self.component_ladder.iter().position(|c| c == entity)?;
            return Some(if n <= 1 { 0.0 } else { pos as f64 / (n - 1) as f64 });
        }
        s.fundamentality_default
    }

    /// Publish the compiled map so Lua and clients read the same slots the daemon encodes.
    pub fn publish(&self, conn: &mut redis::Connection, tk: &str) -> redis::RedisResult<()> {
        for (key, shape) in &self.shapes {
            let slots: BTreeMap<usize, serde_json::Value> = shape.named().into_iter().map(|i| {
                let d = shape.slot(i).unwrap();
                (i, serde_json::json!({
                    "name": d.name,
                    "kind": match d.kind { Kind::Colour => "colour", Kind::Ordered => "ordered" },
                    "many": d.many,
                    "writer": match d.writer { Writer::Provider => "provider", Writer::Sampler => "sampler", Writer::Daemon => "daemon" },
                    "static": d.is_static,
                    "freeform": d.freeform,
                }))
            }).collect();
            let values: BTreeMap<&str, &BTreeMap<String, f64>> =
                shape.slots.iter().flatten().map(|d| (d.name.as_str(), &d.values)).collect();
            let index: BTreeMap<&str, usize> = shape.named().into_iter().map(|i| (shape.name_of(i).unwrap(), i)).collect();
            let (lo, hi) = layer_interval(key).map_err(|e| redis::RedisError::from((redis::ErrorKind::ClientError, "layer", e)))?;
            redis::cmd("HSET")
                .arg(format!("{}:schema:{}", tk, key))
                .arg("schema_version").arg(&self.version)
                .arg("shape").arg(key)
                .arg("name").arg(&shape.name)
                .arg("width").arg(self.width)
                .arg("layer_code").arg(lo)
                .arg("layer_hi").arg(hi)
                .arg("slots").arg(serde_json::to_string(&slots).unwrap_or_default())
                .arg("dimension_index").arg(serde_json::to_string(&index).unwrap_or_default())
                .arg("dimension_values").arg(serde_json::to_string(&values).unwrap_or_default())
                .arg("fundamentality_default").arg(shape.fundamentality_default.map(|v| v.to_string()).unwrap_or_default())
                .query::<()>(conn)?;
        }
        redis::cmd("HSET")
            .arg(format!("{}:schema:shapes", tk))
            .arg("schema_version").arg(&self.version)
            .arg("keys").arg(serde_json::to_string(&self.shape_keys()).unwrap_or_default())
            .arg("component_ladder").arg(serde_json::to_string(&self.component_ladder).unwrap_or_default())
            .query(conn)
    }
}

fn check_values(shape: &str, index: usize, s: &SlotDef) -> std::result::Result<(), String> {
    let mut seen = Vec::new();
    for (name, v) in &s.values {
        if !(0.0..=1.0).contains(v) {
            return Err(format!("shape {} slot {} {}: {} = {} is outside [0, 1]", shape, index, s.name, name, v));
        }
        if seen.contains(v) {
            return Err(format!("shape {} slot {} {}: code {} is used twice", shape, index, s.name, v));
        }
        seen.push(*v);
    }
    if s.values.is_empty() && !s.freeform && index != LAYER_SLOT {
        return Err(format!("shape {} slot {} {}: no vocabulary and not freeform", shape, index, s.name));
    }
    Ok(())
}

/// `"3.1"` → `0x03_01_00_00_00_00`: six eight-bit digits, most significant first.
pub fn layer_code(shape_key: &str) -> std::result::Result<u64, String> {
    let digits: Vec<u64> = shape_key.split('.')
        .map(|d| d.parse::<u64>().map_err(|_| format!("layer digit {:?} in {}", d, shape_key)))
        .collect::<std::result::Result<_, _>>()?;
    if digits.is_empty() || digits.len() > LAYER_LEVELS as usize || digits.iter().any(|d| *d == 0 || *d > 255) {
        return Err(format!("layer key {} must be 1-{} digits in 1..=255", shape_key, LAYER_LEVELS));
    }
    let mut code = 0u64;
    for level in 0..LAYER_LEVELS as usize {
        code = (code << 8) | digits.get(level).copied().unwrap_or(0);
    }
    Ok(code)
}

/// A layer and all its sub-layers are one interval of codes.
pub fn layer_interval(shape_key: &str) -> std::result::Result<(u64, u64), String> {
    let lo = layer_code(shape_key)?;
    let depth = shape_key.split('.').count() as u32;
    Ok((lo, lo + (1u64 << (8 * (LAYER_LEVELS - depth))) - 1))
}

/// The axis-0 coordinate `code · 2^-48` as Q64.64, exact, never through f64.
pub fn layer_coordinate(code: u64) -> FixedPoint {
    FixedPoint::from_raw((code as i128) << SCORE_SHIFT)
}

/// The sorted-set score of an ordered coordinate: Q0.48, exact in a double.
pub fn index_score(fp: &FixedPoint) -> i128 {
    fp.raw() >> SCORE_SHIFT
}

/// The top-level layer digit the dependency potential compares (P-c).
pub fn top_layer(code: u64) -> u64 {
    code >> (8 * (LAYER_LEVELS - 1))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema() -> TopologySchema {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        TopologySchema::load(&root.join("config/topology_schema.yaml")).unwrap()
    }

    const LIVE_SHAPES: [&str; 7] = ["1", "2", "3.1", "3.2", "3.3", "4.1", "4.2"];

    #[test]
    fn every_live_shape_compiles_at_width_24() {
        let s = schema();
        for k in LIVE_SHAPES {
            let sh = s.shape(k).unwrap_or_else(|| panic!("shape {k} missing"));
            assert_eq!(sh.slots.len(), WIDTH);
        }
    }

    #[test]
    fn layer_code_is_exact_on_every_path() {
        for k in LIVE_SHAPES {
            let code = layer_code(k).unwrap();
            let fp = layer_coordinate(code);
            assert_eq!(fp.raw(), (code as i128) << 16, "{k}: raw is code << 16");
            assert_eq!(index_score(&fp), code as i128, "{k}: score is the code");
            let via_f64 = FixedPoint::from_f64(code as f64 / (1u64 << 48) as f64);
            assert_eq!(via_f64.raw(), fp.raw(), "{k}: from_f64 agrees");
            let pr = fp.raw().to_string();
            assert_eq!(pr.parse::<i128>().map(FixedPoint::from_raw).unwrap().raw(), fp.raw(), "{k}: pr string round-trips");
        }
        let max = layer_code("255.255.255.255.255.255").unwrap();
        assert_eq!(max, 0xFFFF_FFFF_FFFF);
        assert!(index_score(&layer_coordinate(max)) < (1i128 << 53));
    }

    #[test]
    fn layer_intervals_nest() {
        let (slo, shi) = layer_interval("3").unwrap();
        for k in ["3.1", "3.2", "3.3"] {
            let (lo, hi) = layer_interval(k).unwrap();
            assert!(slo <= lo && hi <= shi, "{k} inside 3");
        }
        let (a_lo, a_hi) = layer_interval("3.1").unwrap();
        let (b_lo, _) = layer_interval("3.2").unwrap();
        assert!(a_hi < b_lo && a_lo < a_hi, "sub-layers are disjoint");
        assert_eq!(top_layer(layer_code("3.3").unwrap()), 3);
        assert_eq!(top_layer(layer_code("4.2").unwrap()), 4);
    }

    #[test]
    fn names_resolve_per_shape_and_empty_slots_are_none() {
        let s = schema();
        assert_eq!(s.name_of("1", 2), Some("node_role"));
        assert_eq!(s.name_of("3.3", 2), Some("protocol"));
        assert_eq!(s.resolve("1", "cpu_tier"), Some(7));
        for k in ["2", "3.1", "4.1", "4.2"] {
            assert_eq!(s.resolve(k, "cpu_tier"), None, "{k} has no cpu_tier");
        }
        assert_eq!(s.resolve("4.1", "build"), Some(3));
        assert_eq!(s.resolve("4.2", "capacity_weight"), Some(3));
        assert_eq!(s.name_of("3.1", 15), None);
        assert_eq!(s.name_of("1", 13), None);
        for k in LIVE_SHAPES {
            for i in 20..24 {
                assert_eq!(s.name_of(k, i), None, "{k} slot {i} is storage, unnamed");
            }
        }
    }

    #[test]
    fn header_positions_are_fixed_and_load_is_not_everywhere() {
        let s = schema();
        for k in LIVE_SHAPES {
            assert_eq!(s.name_of(k, 0), Some("layer"), "{k}");
            assert_eq!(s.name_of(k, 1), Some("environment"), "{k}");
            assert_eq!(s.name_of(k, 17), Some("health"), "{k}");
            assert_eq!(s.name_of(k, 18), Some("lifecycle"), "{k}");
            assert!(s.name_of(k, 19).is_some(), "{k} names slot 19");
        }
        for k in ["1", "3.1", "3.2", "3.3", "4.2"] {
            assert_eq!(s.name_of(k, 16), Some("load"), "{k}");
        }
        for k in ["2", "4.1"] {
            assert_eq!(s.name_of(k, 16), None, "{k} has no load: a header slot is a position");
        }
        assert_eq!(s.name_of("1", 19), Some("storage_available"));
        assert_eq!(s.name_of("2", 19), Some("installations"));
        assert_eq!(s.name_of("3.2", 19), Some("instances"));
        assert_eq!(s.name_of("4.1", 19), Some("heartbeat_age"));
        assert_eq!(s.name_of("4.2", 19), Some("in_flight"));
    }

    #[test]
    fn compare_is_refused_across_shapes_and_masked_within() {
        let s = schema();
        assert!(s.compare_mask("1", "3.1").is_err());
        assert!(s.compare_mask("4.1", "4.2").is_err(), "placements differ at slots 3, 16, 19");
        let m = s.compare_mask("3.3", "3.3").unwrap();
        assert!(m.contains(&0) && m.contains(&14) && !m.contains(&15) && !m.contains(&16));
        let n = s.compare_mask("1", "1").unwrap();
        assert!(!n.iter().any(|i| (13..=15).contains(i)), "node never touches 13-15");
    }

    #[test]
    fn fundamentality_defaults_follow_d11() {
        let s = schema();
        assert_eq!(s.fundamentality_default("3.3", "gflow"), Some(0.3));
        assert_eq!(s.fundamentality_default("3.2", "x"), Some(0.5));
        assert_eq!(s.fundamentality_default("3.1", "x"), Some(0.7));
        let valkey = s.fundamentality_default("2", "ValKey").unwrap();
        let gcore = s.fundamentality_default("2", "gCore").unwrap();
        assert!(valkey < gcore, "ValKey is more fundamental than gCore");
        assert_eq!(s.fundamentality_default("2", "not-a-component"), None);
        assert_eq!(s.resolve("3.3", "fundamentality"), Some(14));
        assert_eq!(s.resolve("2", "fundamentality"), Some(13));
    }

    #[test]
    fn the_lua_library_agrees_on_the_width() {
        let lua = std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("functions/gnode_topo.lua")).unwrap();
        let line = lua.lines().find(|l| l.trim_start().starts_with("local W = ")).expect("gnode_topo.lua declares local W");
        let w: usize = line.trim().trim_start_matches("local W = ").trim().parse().unwrap();
        assert_eq!(w, WIDTH, "gnode_topo.lua's width must be the compiler's (K15)");
    }

    #[test]
    fn sampler_slots_are_zone_b_only() {
        let s = schema();
        for k in LIVE_SHAPES {
            for i in s.shape(k).unwrap().sampler_slots() {
                assert!((16..=19).contains(&i), "{k}: sampler writes {i}, outside zone B");
            }
        }
    }
}
