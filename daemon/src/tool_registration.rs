//! Tool Registration Module — Deploy-time and discovery-based registration of services
//!
//! This module provides the core capability translation and registration pipeline:
//!
//! 1. Reads the active tier schema (service_schema.yaml | tool_schema.yaml |
//!    constellation_schema.yaml | galaxy_schema.yaml) — human-readable
//!    capability name → f64 coordinate mapping. Service tier is the default.
//! 2. Reads geometric_topology.yaml (service definitions with human-readable capabilities)
//! 3. Translates capabilities to N-D coordinate vectors using Q64.64 fixed-point,
//!    where N is the tier's total_dimensions (30 service / 16 tool / 20 constellation / 20 galaxy).
//! 4. Registers each service to site topologies via FCALL.
//!
//! Custom topologies (created via topo_create / gNode-TOPO extension) have
//! user-specified dim counts and value mappings stored in ValKey at creation
//! time; they bypass this schema-loading path.
//!
//! Used by:
//! - `register-tools` CLI subcommand (one-time deploy-time registration)
//! - `ServiceDiscoveryManager` (periodic daemon-side discovery from config files)
//!
//! All Q64.64 math reuses existing GeometricTopology/FixedPoint functions — zero duplication.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use log::{info, warn, error};
use serde::Deserialize;

use crate::integration::handlers::{
    build_capability_vector, discovery_point,
};
use crate::GeometricTopology;
use crate::{Result, GeometricError};

// ============================================================================
// Schema types — deserialization of the active tier schema (service_schema.yaml
// by default; tool/constellation/galaxy_schema.yaml for those tiers).
// ============================================================================

#[derive(Debug, Deserialize, Clone)]
pub struct CapabilitySchema {
    pub schema_version: String,
    pub tier: Option<String>,
    pub total_dimensions: usize,
    pub discovery_dimensions: Option<usize>,
    pub dimensions: HashMap<String, DimensionDef>,
    /// Named capability profiles (web|headless|service|system|component) that
    /// supply 30-dim defaults for registering one service entity per site.
    #[serde(default)]
    pub profiles: HashMap<String, Vec<CapabilityEntry>>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DimensionDef {
    pub index: usize,
    pub values: HashMap<String, f64>,
}

// ============================================================================
// Config types — deserialization of geometric_topology.yaml service entries
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct GeometricTopologyConfig {
    #[allow(dead_code)]
    pub dimensions: Option<usize>,
    pub services: Option<Vec<ToolServiceDef>>,
    #[serde(flatten)]
    pub _extra: HashMap<String, serde_yaml::Value>,
}

#[derive(Debug, Deserialize)]
pub struct ToolServiceDef {
    pub id: String,
    pub metadata: Option<ServiceMetadata>,
    pub capabilities: Vec<CapabilityEntry>,
    #[serde(default)]
    pub depends_on: Vec<String>,
}

/// Ecosystem tools config — deserialization of ecosystem_tools.yaml
#[derive(Debug, Deserialize)]
pub struct EcosystemToolsConfig {
    pub schema: Option<String>,
    pub tier: Option<String>,
    pub components: Option<Vec<ToolServiceDef>>,
    #[serde(flatten)]
    pub _extra: HashMap<String, serde_yaml::Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServiceMetadata {
    pub class: Option<String>,
    pub description: Option<String>,
    #[serde(rename = "type")]
    pub service_type: Option<String>,
    pub tier: Option<String>,
    /// Schema keys for schema↔topology cross-reference.
    /// Each entry is "{component}:{contract_name}" matching ValKey schema keys.
    pub schema_keys: Option<Vec<String>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CapabilityEntry {
    pub name: String,
    pub value: serde_yaml::Value,
}

// ============================================================================
// Translated service — pre-computed, ready for ValKey registration
// ============================================================================

/// A service definition with pre-computed Q64.64 coordinates, bucket key, and z_score.
/// Ready to be registered via FCALL GNODE_REGISTER_CAPABILITY_VECTOR.
pub struct TranslatedService {
    pub id: String,
    pub entity_json: String,
    pub bucket_key: String,
    pub z_score: i64,
}

// ============================================================================
// Registration results
// ============================================================================

/// Result of a registration batch operation.
pub struct RegistrationResult {
    pub registered: usize,
    pub errors: usize,
    pub skipped: bool,
    pub sites: usize,
}

// ============================================================================
// Registration arguments (from CLI)
// ============================================================================

pub struct RegisterToolsArgs {
    pub site: Option<String>,
    pub config_path: Option<PathBuf>,
    pub schema_path: Option<PathBuf>,
    pub dry_run: bool,
    pub redis_url: String,
    pub topology_namespace: String,
    pub tier: String,  // "service" (default) or "tool"
    /// When set (service tier), register ONE entity for `site` from this named
    /// profile (web|headless|…) instead of looping geometric_topology.yaml.
    pub profile: Option<String>,
    /// DTAP environment override (testing|staging|acceptance|production) injected
    /// into dim-20 of the profile entity, so a non-prod site's geometric placement
    /// matches its active_environment. None → profile/schema default (production).
    pub environment: Option<String>,
}

// ============================================================================
// Reusable core functions — used by both CLI and ServiceDiscoveryManager
// ============================================================================

/// Load and parse a tier schema YAML (service_schema.yaml / tool_schema.yaml / etc).
pub fn load_schema(path: &Path) -> Result<CapabilitySchema> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| GeometricError::Other(format!("Failed to read schema {:?}: {}", path, e)))?;
    let schema: CapabilitySchema = serde_yaml::from_str(&content)
        .map_err(|e| GeometricError::Other(format!("Failed to parse schema {:?}: {}", path, e)))?;
    Ok(schema)
}

/// Publish the active tier schema so clients stop hardcoding its shape.
///
/// The dimension count is not one number and never was — it is per TIER
/// (service 30, tool 16, constellation/galaxy 20, each with its own discovery
/// subset). That is a sound design; what was unsound is that every consumer
/// carried its own copy of the answer. Counts of 8, 9, 12, 16, 19, 23 and 25
/// were all findable in the tree at once, some stale by months, and nothing
/// could tell you which was true without reading this daemon's YAML.
///
/// So the schema is published where any client can read it, keyed by tier, and
/// stamped with the version and the file it came from. A client that fetches
/// this cannot disagree with the daemon; a client that hardcodes 23 can only be
/// wrong quietly.
///
/// Master-only, like the function-library namespace: exactly one node owns
/// writing shared topology state, and a worker publishing its own copy is how
/// two nodes come to disagree about the schema they are both matching against.
pub fn publish_schema(
    conn: &mut redis::Connection,
    topology_ns: &str,
    schema: &CapabilitySchema,
    source_path: &Path,
) -> Result<()> {
    let tier = schema.tier.clone().unwrap_or_else(|| "service".to_string());
    let key = format!("{{{}}}:gnode:schema:{}", topology_ns, tier);

    // Dimension NAME → INDEX as well as the counts. A consumer that only knows
    // "30" still cannot build a coordinate vector; it needs the index each
    // named capability occupies, which is the part that actually drifts.
    let mut index_map: Vec<(String, usize)> =
        schema.dimensions.iter().map(|(n, d)| (n.clone(), d.index)).collect();
    index_map.sort_by_key(|(_, i)| *i);
    let dims_json = serde_json::to_string(
        &index_map.iter().map(|(n, i)| (n.as_str(), *i)).collect::<std::collections::BTreeMap<_, _>>()
    ).unwrap_or_else(|_| "{}".to_string());

    // The value vocabulary too, not only the index map. Publishing indices alone
    // leaves each consumer holding a private copy of what `latency_class:
    // interactive` means numerically — the same drift one level down. The copy
    // in gNode-Client is missing seven of these dimensions outright and drops
    // any capability it does not recognise without saying so.
    let values_json = serde_json::to_string(
        &schema.dimensions.iter()
            .map(|(n, d)| (n.as_str(), d.values.iter()
                .map(|(k, v)| (k.as_str(), *v))
                .collect::<std::collections::BTreeMap<_, _>>()))
            .collect::<std::collections::BTreeMap<_, _>>()
    ).unwrap_or_else(|_| "{}".to_string());

    let _: redis::RedisResult<()> = redis::cmd("HSET")
        .arg(&key)
        .arg("schema_version").arg(&schema.schema_version)
        .arg("tier").arg(&tier)
        .arg("total_dimensions").arg(schema.total_dimensions)
        .arg("discovery_dimensions").arg(schema.discovery_dimensions.unwrap_or(schema.total_dimensions))
        .arg("dimension_index").arg(&dims_json)
        .arg("dimension_values").arg(&values_json)
        .arg("source").arg(source_path.to_string_lossy().as_ref())
        .query(conn);

    info!(
        "Published {} schema: {} dimensions ({} discovery), version {} — clients can read {}",
        tier,
        schema.total_dimensions,
        schema.discovery_dimensions.unwrap_or(schema.total_dimensions),
        schema.schema_version,
        key
    );

    // The declared total and the actual entry count must agree, or every
    // consumer that trusts the header builds vectors of the wrong width. Cheap
    // to check here, invisible everywhere else.
    if schema.dimensions.len() != schema.total_dimensions {
        warn!(
            "SCHEMA MISMATCH in {:?}: total_dimensions says {} but {} dimensions are defined. \
             Consumers sizing a vector from the header will disagree with consumers counting entries.",
            source_path, schema.total_dimensions, schema.dimensions.len()
        );
    }

    Ok(())
}

/// Fail startup if a Lua library's dimension constant disagrees with the schema.
///
/// Lua function libraries do not share scope, so gnode_topology and gnode_topo
/// each declare their own `TOTAL_DIMENSIONS`. Two copies of a number that must
/// match is exactly the arrangement that produced the drift this whole change is
/// about, so the copies are checked rather than trusted.
///
/// Loud on purpose. A wrong constant here does not crash anything — it writes a
/// wrong width into every site's topology metadata and produces coordinate
/// vectors that match slightly worse than they should, which is invisible until
/// someone asks why discovery is imprecise.
pub fn assert_lua_dimension_constants(schema: &CapabilitySchema, functions_dir: &Path) -> Result<()> {
    let entries = match std::fs::read_dir(functions_dir) {
        Ok(e) => e,
        // Not every deployment ships the sources beside the binary; absent is
        // not a disagreement.
        Err(_) => return Ok(()),
    };

    let mut checked = 0usize;
    for entry in entries.filter_map(|e| e.ok()) {
        let path = entry.path();
        if path.extension().and_then(|x| x.to_str()) != Some("lua") {
            continue;
        }
        let Ok(src) = std::fs::read_to_string(&path) else { continue };
        for line in src.lines() {
            let t = line.trim();
            let Some(rest) = t.strip_prefix("local TOTAL_DIMENSIONS") else { continue };
            let Some(v) = rest.trim_start().strip_prefix('=') else { continue };
            let n: usize = match v.split("--").next().unwrap_or("").trim().parse() {
                Ok(n) => n,
                Err(_) => continue,
            };
            checked += 1;
            if n != schema.total_dimensions {
                return Err(GeometricError::Other(format!(
                    "{:?} declares TOTAL_DIMENSIONS = {} but {:?} says {}. \
                     Both describe the same capability space; fix the schema or the library, \
                     not one of them.",
                    path, n, schema.tier.as_deref().unwrap_or("service"), schema.total_dimensions
                )));
            }
        }
    }

    if checked > 0 {
        info!("Lua dimension constants agree with the schema ({} checked, all {})",
              checked, schema.total_dimensions);
    }

    Ok(())
}

/// Load and parse a geometric_topology.yaml file, returning the service definitions.
pub fn load_service_definitions(path: &Path) -> Result<Vec<ToolServiceDef>> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| GeometricError::Other(format!("Failed to read config {:?}: {}", path, e)))?;
    let config: GeometricTopologyConfig = serde_yaml::from_str(&content)
        .map_err(|e| GeometricError::Other(format!("Failed to parse config {:?}: {}", path, e)))?;
    Ok(config.services.unwrap_or_default())
}

/// Translate all service definitions to pre-computed TranslatedService structs.
pub fn translate_all_services(
    services: &[ToolServiceDef],
    schema: &CapabilitySchema,
) -> Vec<TranslatedService> {
    let mut translated = Vec::with_capacity(services.len());

    // Build dimension name→index map from schema
    let dim_map: HashMap<String, usize> = schema.dimensions.iter()
        .map(|(name, def)| (name.clone(), def.index))
        .collect();
    let total_dims = schema.total_dimensions;
    let discovery_dims = schema.discovery_dimensions.unwrap_or(total_dims);

    for svc in services {
        let mut capabilities = translate_capabilities(&svc.capabilities, schema);
        inject_classification_dims(&mut capabilities, &svc.metadata, schema);

        let (entity_json, bucket_key, z_score) =
            build_entity_data(&svc.id, &capabilities, &svc.metadata,
                              total_dims, discovery_dims, &dim_map);

        translated.push(TranslatedService {
            id: svc.id.clone(),
            entity_json,
            bucket_key,
            z_score,
        });
    }

    translated
}

/// Register pre-translated services for a single site.
/// Ensures the site topology exists, then registers each service entity.
/// Returns (registered_count, error_count).
pub fn register_services_for_site(
    conn: &mut redis::Connection,
    site_id: &str,
    services: &[TranslatedService],
    args_namespace: &str,
) -> Result<(usize, usize)> {
    // Ensure topology exists
    let ensure_result: redis::RedisResult<String> = redis::cmd("FCALL")
        .arg("GNODE_ENSURE_TOPOLOGY")
        .arg(1)  // numkeys
        .arg(site_id)
        .query(conn);

    match ensure_result {
        Ok(ref json) => {
            info!("  Topology ensured for {}: {}", site_id, json);
        }
        Err(e) => {
            error!("  Failed to ensure topology for site {}: {}", site_id, e);
            return Ok((0, services.len()));
        }
    }

    let topology_key = if args_namespace.is_empty() || args_namespace == "default" {
        format!("{{{}}}:gnode:services", site_id)
    } else {
        // Custom namespace: e.g., "ecosystem:tools" → "ecosystem:tools:topology"
        format!("{}:topology", args_namespace)
    };
    let mut registered = 0;
    let mut errors = 0;

    for svc in services {
        let result: redis::RedisResult<String> = redis::cmd("FCALL")
            .arg("GNODE_REGISTER_CAPABILITY_VECTOR")
            .arg(1)  // numkeys
            .arg(&topology_key)
            .arg(&svc.id)
            .arg(&svc.entity_json)
            .arg(&svc.bucket_key)
            .arg(svc.z_score.to_string())
            .arg(crate::daemon::GNodeDaemon::topology_snapshot_key())  // args[5]: (B) snapshot
            .query(conn);

        match result {
            Ok(ref json) => {
                info!("  Registered: {} → {}", svc.id, json);
                registered += 1;
            }
            Err(e) => {
                error!("  Failed to register {}: {}", svc.id, e);
                errors += 1;
            }
        }
    }

    Ok((registered, errors))
}

/// Discover all registered sites from ValKey (SMEMBERS gnode:sites:registry).
pub fn discover_registered_sites(conn: &mut redis::Connection) -> Result<Vec<String>> {
    let sites: Vec<String> = redis::cmd("SMEMBERS")
        .arg("gnode:sites:registry")
        .query(conn)
        .map_err(GeometricError::Redis)?;
    Ok(sites)
}

// ============================================================================
// File discovery helpers
// ============================================================================

/// Find the service-tier schema YAML from known paths.
/// Canonical: `daemon/config/service_schema.yaml` (30D — 25 discovery + 5 storage).
/// Caller can supply --schema to override.
///
/// For non-service tiers (tool/constellation/galaxy) pass the tier-specific
/// schema as override_path. The legacy `capability_schema.yaml` (23D, deprecated) was
/// removed per pre-launch "no backward compatibility" discipline.
pub fn find_schema_path(override_path: Option<&PathBuf>) -> Option<PathBuf> {
    if let Some(p) = override_path {
        if p.exists() {
            return Some(p.clone());
        }
    }

    let candidates = [
        PathBuf::from("daemon/config/service_schema.yaml"),
        PathBuf::from("/opt/gNode/daemon/config/service_schema.yaml"),
        PathBuf::from("/opt/geodineum/gNode/daemon/config/service_schema.yaml"),
    ];

    for p in &candidates {
        if p.exists() {
            return Some(p.clone());
        }
    }

    None
}

/// Locate every tier schema that ships beside the service one.
///
/// The daemon loads the service schema at startup because that is the tier it
/// matches against. But a client asking "how wide is the tool tier" has nowhere
/// to ask if only service is published, and falls back to its built-in copy —
/// which is the failure this whole mechanism exists to remove, just moved to a
/// tier nobody was looking at.
///
/// Derived from the service schema's own directory rather than a second
/// candidate list: the tiers live together, and two path lists that must agree
/// is the same shape of bug one directory up.
pub fn find_all_tier_schemas(service_schema: &Path) -> Vec<PathBuf> {
    let Some(dir) = service_schema.parent() else { return vec![] };

    ["service", "tool", "constellation", "galaxy"]
        .iter()
        .map(|t| dir.join(format!("{}_schema.yaml", t)))
        .filter(|p| p.exists())
        .collect()
}

/// Find geometric_topology.yaml from known paths, including extra discovery paths.
pub fn find_config_path(override_path: Option<&PathBuf>) -> Option<PathBuf> {
    if let Some(p) = override_path {
        if p.exists() {
            return Some(p.clone());
        }
    }

    // Check GCORE_DIR env var first, then standard install path
    let mut candidates: Vec<PathBuf> = Vec::new();
    if let Ok(gcore_dir) = std::env::var("GCORE_DIR") {
        candidates.push(PathBuf::from(gcore_dir).join("config/geometric_topology.yaml"));
    }
    candidates.push(PathBuf::from("/opt/geodineum/gCore/config/geometric_topology.yaml"));

    for p in &candidates {
        if p.exists() {
            return Some(p.clone());
        }
    }

    None
}

// ============================================================================
// Core translation logic (internal)
// ============================================================================

/// Translate human-readable capability values to f64 coordinates using the schema.
fn translate_capabilities(
    entries: &[CapabilityEntry],
    schema: &CapabilitySchema,
) -> HashMap<String, f64> {
    let mut capabilities: HashMap<String, f64> = HashMap::new();

    for entry in entries {
        let dim_name = &entry.name;

        // Look up the dimension in the schema
        if let Some(dim_def) = schema.dimensions.get(dim_name) {
            let value = match &entry.value {
                serde_yaml::Value::Number(n) => {
                    // Already a numeric value, use directly
                    n.as_f64().unwrap_or(0.0)
                }
                serde_yaml::Value::String(s) => {
                    // Look up the human-readable name in the dimension's value map
                    if let Some(&coord) = dim_def.values.get(s.as_str()) {
                        coord
                    } else {
                        warn!(
                            "Unknown value '{}' for dimension '{}', skipping",
                            s, dim_name
                        );
                        continue;
                    }
                }
                _ => {
                    warn!("Unsupported value type for dimension '{}'", dim_name);
                    continue;
                }
            };

            capabilities.insert(dim_name.clone(), value);
        } else {
            warn!("Unknown dimension '{}' in schema, skipping", dim_name);
        }
    }

    capabilities
}

/// Inject classification dimensions (tier, environment) and visual defaults.
fn inject_classification_dims(
    capabilities: &mut HashMap<String, f64>,
    metadata: &Option<ServiceMetadata>,
    schema: &CapabilitySchema,
) {
    // Inject tier coordinate (dimension 17) — default TOOL=0.10
    if !capabilities.contains_key("service_tier") {
        let tier_value = metadata
            .as_ref()
            .and_then(|m| m.tier.as_ref())
            .and_then(|tier| {
                schema
                    .dimensions
                    .get("service_tier")
                    .and_then(|d| d.values.get(tier.as_str()).copied())
            })
            .unwrap_or(0.10); // Default: TOOL
        capabilities.insert("service_tier".to_string(), tier_value);
    }

    // Inject environment (dimension 18) — default production=1.0
    if !capabilities.contains_key("environment") {
        capabilities.insert("environment".to_string(), 1.0);
    }

    // Inject visual defaults (dims 19-21) — 0.5 center
    if !capabilities.contains_key("user_x") {
        capabilities.insert("user_x".to_string(), 0.5);
    }
    if !capabilities.contains_key("user_y") {
        capabilities.insert("user_y".to_string(), 0.5);
    }
    if !capabilities.contains_key("user_z") {
        capabilities.insert("user_z".to_string(), 0.5);
    }

    // Inject current_load default (dim 16) — idle=0.0
    if !capabilities.contains_key("current_load") {
        capabilities.insert("current_load".to_string(), 0.0);
    }
}

/// Build the entity JSON for GNODE_REGISTER_CAPABILITY_VECTOR.
/// Schema-driven: uses total_dims and discovery_dims from the loaded schema.
/// Returns (entity_json, bucket_key, z_score).
pub(crate) fn build_entity_data(
    _service_id: &str,
    capabilities: &HashMap<String, f64>,
    metadata: &Option<ServiceMetadata>,
    total_dims: usize,
    discovery_dims: usize,
    dim_map: &HashMap<String, usize>,
) -> (String, String, i64) {
    // Build capability vector using schema-derived dimension count and mapping
    let full_point = build_capability_vector(capabilities, total_dims, dim_map);

    // Extract discovery-only point for bucket key (schema-driven count)
    let discovery_point = discovery_point(&full_point, discovery_dims);

    // Compute bucket key from discovery point (reconstruct from public raw function)
    let grid_size = 10;
    let bucket_key_raw = GeometricTopology::point_to_bucket_key_raw(&discovery_point, grid_size);
    let bucket_key: String = bucket_key_raw
        .iter()
        .map(|&v| format!("{:04}", v))
        .collect();

    // Compute z_score from full point (dim 16 = current_load)
    let z_score = GeometricTopology::compute_service_z_score(&full_point);

    // Build point_raw (pr): Q64.64 i128 values for all tier dimensions.
    // Dim count comes from the loaded tier schema (30 for service tier,
    // 16 for tool tier, 20 for constellation/galaxy; user-specified for
    // custom topologies created via topo_create / gNode-TOPO).
    let pr: Vec<String> = (0..full_point.len())
        .map(|i| full_point[i].raw().to_string())
        .collect();

    // Build point_display (pd): float values for all tier dimensions.
    let pd: Vec<f64> = (0..full_point.len())
        .map(|i| full_point[i].to_f64())
        .collect();

    // Build capability map (c): dimension_name → float
    let c: HashMap<String, f64> = capabilities.clone();

    // Build metadata map (m)
    let mut m: serde_json::Map<String, serde_json::Value> = serde_json::Map::new();
    if let Some(meta) = metadata {
        if let Some(ref class) = meta.class {
            m.insert("class".to_string(), serde_json::Value::String(class.clone()));
        }
        if let Some(ref desc) = meta.description {
            m.insert("description".to_string(), serde_json::Value::String(desc.clone()));
        }
        if let Some(ref stype) = meta.service_type {
            m.insert("type".to_string(), serde_json::Value::String(stype.clone()));
        }
        if let Some(ref tier) = meta.tier {
            m.insert("tier".to_string(), serde_json::Value::String(tier.clone()));
        }
        // Schema keys for schema↔topology cross-reference
        if let Some(ref skeys) = meta.schema_keys {
            m.insert(
                "schema_keys".to_string(),
                serde_json::Value::Array(
                    skeys.iter().map(|s| serde_json::Value::String(s.clone())).collect(),
                ),
            );
        }
    }

    // Construct entity JSON with abbreviated field names (matching ServicePointData serialization)
    let entity = serde_json::json!({
        "pr": pr,
        "pd": pd,
        "c": c,
        "m": m,
    });

    let entity_json = serde_json::to_string(&entity).unwrap_or_default();

    (entity_json, bucket_key, z_score)
}

// ============================================================================
// Pyramid Layout Algorithm (Tool Tier)
// ============================================================================
// Computes x/y/z coordinates from the dependency graph using a layered layout:
//   z = 1.0 - (depth / max_depth)     → top of pyramid = highest z
//   x = centroid of parent x positions → centered under dependencies
//   y = 0.5 (flat pyramid, single depth plane)

/// Compute pyramid layout coordinates for tool-tier components.
/// Returns a map of component_id → (pyramid_x, pyramid_y, pyramid_z).
pub fn compute_pyramid_layout(components: &[ToolServiceDef]) -> HashMap<String, (f64, f64, f64)> {
    let mut layout: HashMap<String, (f64, f64, f64)> = HashMap::new();

    // Build adjacency: id → depth (topological depth from roots)
    let id_set: std::collections::HashSet<&str> = components.iter().map(|c| c.id.as_str()).collect();
    let mut depths: HashMap<String, usize> = HashMap::new();

    // Compute depth for each node (max depth of any dependency + 1)
    fn compute_depth(
        id: &str,
        components: &[ToolServiceDef],
        depths: &mut HashMap<String, usize>,
        id_set: &std::collections::HashSet<&str>,
    ) -> usize {
        if let Some(&d) = depths.get(id) {
            return d;
        }
        let comp = components.iter().find(|c| c.id == id);
        let depth = match comp {
            Some(c) if !c.depends_on.is_empty() => {
                let max_parent = c.depends_on.iter()
                    .filter(|dep| id_set.contains(dep.as_str()))
                    .map(|dep| compute_depth(dep, components, depths, id_set))
                    .max()
                    .unwrap_or(0);
                max_parent + 1
            }
            _ => 0,
        };
        depths.insert(id.to_string(), depth);
        depth
    }

    for comp in components {
        compute_depth(&comp.id, components, &mut depths, &id_set);
    }

    let max_depth = depths.values().copied().max().unwrap_or(0).max(1);

    // Group components by depth level
    let mut levels: HashMap<usize, Vec<String>> = HashMap::new();
    for (id, &depth) in &depths {
        levels.entry(depth).or_default().push(id.clone());
    }
    // Sort within each level for deterministic positioning
    for level in levels.values_mut() {
        level.sort();
    }

    // Assign z (height) and x (horizontal spread within level)
    for (&depth, ids) in &levels {
        let z = 1.0 - (depth as f64 / max_depth as f64);
        let count = ids.len();

        for (i, id) in ids.iter().enumerate() {
            let x = if count == 1 {
                0.5
            } else {
                // Spread evenly in [0.15, 0.85] range to leave margins
                0.15 + (i as f64 / (count - 1).max(1) as f64) * 0.70
            };

            // Try to center under parent centroid
            let comp = components.iter().find(|c| c.id == *id);
            let parent_centroid = comp.and_then(|c| {
                let parent_xs: Vec<f64> = c.depends_on.iter()
                    .filter_map(|dep| layout.get(dep).map(|(px, _, _)| *px))
                    .collect();
                if parent_xs.is_empty() {
                    None
                } else {
                    Some(parent_xs.iter().sum::<f64>() / parent_xs.len() as f64)
                }
            });

            let final_x = parent_centroid.unwrap_or(x);
            layout.insert(id.clone(), (final_x, 0.5, z));
        }
    }

    layout
}

/// Load tool-tier components from ecosystem_tools.yaml
pub fn load_ecosystem_tools(path: &Path) -> Result<Vec<ToolServiceDef>> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| GeometricError::Other(format!("Failed to read {:?}: {}", path, e)))?;
    let config: EcosystemToolsConfig = serde_yaml::from_str(&content)
        .map_err(|e| GeometricError::Other(format!("Failed to parse {:?}: {}", path, e)))?;
    Ok(config.components.unwrap_or_default())
}

/// Find ecosystem_tools.yaml in standard locations
fn find_ecosystem_tools_path(explicit: Option<&PathBuf>) -> Option<PathBuf> {
    if let Some(p) = explicit {
        if p.exists() { return Some(p.clone()); }
    }
    let candidates = [
        // Operator-local FIRST, shipped default second — the /etc-over-/usr
        // precedence every other config in this estate follows.
        //
        // This is not a preference. A host may run the PUBLIC installer while
        // participating in a constellation that includes private components,
        // and a private component must not be added to a public repository to
        // become registerable. /etc is outside every git repo, so it is the
        // only correct home for that entry. With the old order the shipped
        // copy always won and the override could never take effect.
        PathBuf::from("/etc/geodineum/ecosystem_tools.yaml"),
        PathBuf::from("/opt/geodineum/Geodineum/config/ecosystem_tools.yaml"),
        // Dev fallback
        PathBuf::from("../Geodineum/config/ecosystem_tools.yaml"),
        PathBuf::from("../../Geodineum/config/ecosystem_tools.yaml"),
    ];
    candidates.into_iter().find(|p| p.exists())
}

/// Find tool_schema.yaml in standard locations
fn find_tool_schema_path(explicit: Option<&PathBuf>) -> Option<PathBuf> {
    find_tier_schema_path("tool", explicit)
}

/// Locate a named tier's schema.
///
/// Generalised from the tool-only version because `find_schema_path` always
/// resolves the SERVICE schema regardless of the tier asked for — which is how
/// `--tier constellation` came to register 30-dimension service entities while
/// reporting success.
pub fn find_tier_schema_path(tier: &str, explicit: Option<&PathBuf>) -> Option<PathBuf> {
    if let Some(p) = explicit {
        if p.exists() { return Some(p.clone()); }
    }
    let file = format!("{}_schema.yaml", tier);
    [
        PathBuf::from(format!("config/{}", file)),
        PathBuf::from(format!("/opt/geodineum/gNode/daemon/config/{}", file)),
        PathBuf::from(format!("daemon/config/{}", file)),
    ]
    .into_iter()
    .find(|p| p.exists())
}

// ============================================================================
// CLI entry point — register-tools subcommand
// ============================================================================

/// Register services for one or more sites, or ecosystem tools globally.
pub fn run(args: RegisterToolsArgs) -> Result<()> {
    // Fail BEFORE doing the work, not after. A URL with no credential loads the
    // schema, reads the catalogue, computes the whole pyramid layout and logs
    // every component's coordinates — then fails each write with NOAUTH and
    // reports "0/N registered" beneath a page of healthy-looking output. The
    // dry run passes either way because it never connects, so the preview gives
    // no warning.
    //
    // The check is on the URL shape rather than a connection attempt: no
    // credential means no '@' before the host, and that is knowable instantly
    // without a round trip.
    if !args.dry_run && !args.redis_url.contains('@') {
        return Err(GeometricError::Other(format!(
            "ValKey URL carries no credential ({}). Registration writes would all \
             fail NOAUTH after the layout is computed. Pass --redis-auth-file and \
             --redis-user (env: GNODE_REDIS_AUTH_FILE / VALKEY_USER), e.g.\n  \
             GNODE_REDIS_AUTH_FILE=/etc/geodineum/credentials/valkey_daemon.password \
             VALKEY_USER=gnode_daemon gnode-daemon register-tools --tier {}",
            args.redis_url, args.tier
        )));
    }

    // Match the tier EXHAUSTIVELY. This used to be `if tier == "tool" { … }`
    // followed by an unconditional fall-through to the service path, so
    // `--tier constellation`, `--tier galaxy` and `--tier typo` all registered
    // 30-dimension SERVICE entities against service_schema.yaml and reported
    // success. An operator had no way to see it: the command exited 0 and the
    // entities were real, just the wrong tier in the wrong shape.
    match args.tier.as_str() {
        "tool" => run_tool_tier(&args),
        "service" => {
            if args.profile.is_some() {
                run_service_profile(&args)
            } else {
                run_service_tier(args)
            }
        }
        "constellation" => Err(GeometricError::Other(
            "constellation-tier entities are not registered by this command — a node \
             registers itself into its constellation at daemon startup. Nothing to do here."
                .to_string(),
        )),
        "galaxy" => Err(GeometricError::Other(
            "galaxy-tier registration is not implemented. A constellation joins a galaxy \
             by publishing into a signed epoch artifact, not through this command."
                .to_string(),
        )),
        other => Err(GeometricError::Other(format!(
            "unknown tier {:?}. Valid tiers: service, tool (constellation and galaxy are \
             registered elsewhere).",
            other
        ))),
    }
}

/// Register a SINGLE service entity for one site from a named profile
/// (web|headless|service|system|component). The profile lives in the tier
/// schema's `profiles:` section and supplies the 30-dim capability defaults, so
/// a site gets ONE discoverable (C) entity (itself) — not a loop of framework
/// components. Used by onboard Step 3.5. Honors --schema/--config overrides.
/// Derive the ONE service entity a site is, from (profile, environment, schema).
///
/// Extracted so intent-driven re-assertion and the CLI share a single
/// derivation. Two implementations of "what vector does this site have" would
/// drift, and the drift would be invisible — the entity looks equally valid
/// whichever produced it.
///
/// This is the pure half: no connection, no writes. Given the same inputs it
/// yields the same TranslatedService, which is what makes the stored entity a
/// derived projection rather than a record.
pub fn derive_profile_entity(
    site: &str,
    profile: &str,
    environment: Option<&str>,
    schema: &CapabilitySchema,
) -> Result<TranslatedService> {
    let mut caps = schema.profiles.get(profile).cloned().ok_or_else(|| {
        let avail: Vec<&String> = schema.profiles.keys().collect();
        GeometricError::Other(format!(
            "Profile '{}' not found in schema profiles (available: {:?})",
            profile, avail
        ))
    })?;

    if let Some(env) = environment {
        let env_dim = schema.dimensions.get("environment");
        let valid = env_dim.map(|d| d.values.contains_key(env)).unwrap_or(false);
        if !valid {
            let allowed: Vec<&String> =
                env_dim.map(|d| d.values.keys().collect()).unwrap_or_default();
            return Err(GeometricError::Other(format!(
                "Invalid environment '{}' (allowed: {:?})",
                env, allowed
            )));
        }
        caps.retain(|e| e.name != "environment");
        caps.push(CapabilityEntry {
            name: "environment".to_string(),
            value: serde_yaml::Value::String(env.to_string()),
        });
    }

    let def = ToolServiceDef {
        id: site.to_string(),
        metadata: Some(ServiceMetadata {
            class: None,
            description: Some(format!("{} ({} profile)", site, profile)),
            service_type: Some(profile.to_string()),
            tier: Some("SERVICE".to_string()),
            schema_keys: None,
        }),
        capabilities: caps,
        depends_on: Vec::new(),
    };

    translate_all_services(&[def], schema)
        .into_iter()
        .next()
        .ok_or_else(|| GeometricError::Other(format!("translation produced nothing for '{}'", site)))
}

fn run_service_profile(args: &RegisterToolsArgs) -> Result<()> {
    let profile = args.profile.as_deref().unwrap_or("web");
    let site = args.site.as_deref().ok_or_else(|| {
        GeometricError::Other("--profile requires --site <id>".to_string())
    })?;

    let schema_path = find_schema_path(args.schema_path.as_ref()).ok_or_else(|| {
        GeometricError::Other(
            "Cannot find service_schema.yaml. Use --schema to specify path.".to_string(),
        )
    })?;
    info!("Loading tier schema from: {:?}", schema_path);
    let schema = load_schema(&schema_path)?;

    let mut caps = schema.profiles.get(profile).cloned().ok_or_else(|| {
        let avail: Vec<&String> = schema.profiles.keys().collect();
        GeometricError::Other(format!(
            "Profile '{}' not found in schema profiles (available: {:?})",
            profile, avail
        ))
    })?;

    // Embed the DTAP environment into dim-20. The profiles set no `environment`,
    // so without this it defaults to production — leaving a non-prod site's
    // geometric placement (dim-20) diverged from its active_environment. When
    // --environment is given, override it (validated against the schema's
    // environment values). This is the single re-embed path used by initial
    // registration AND `geodineum env set` (promotion), keeping the two env
    // stores reconciled.
    if let Some(ref env) = args.environment {
        let env_dim = schema.dimensions.get("environment");
        let valid = env_dim.map(|d| d.values.contains_key(env)).unwrap_or(false);
        if !valid {
            let allowed: Vec<&String> =
                env_dim.map(|d| d.values.keys().collect()).unwrap_or_default();
            return Err(GeometricError::Other(format!(
                "Invalid --environment '{}' (allowed: {:?})",
                env, allowed
            )));
        }
        caps.retain(|e| e.name != "environment");
        caps.push(CapabilityEntry {
            name: "environment".to_string(),
            value: serde_yaml::Value::String(env.clone()),
        });
        info!("  Embedding environment='{}' into dim-20 for site '{}'", env, site);
    }

    let def = ToolServiceDef {
        id: site.to_string(),
        metadata: Some(ServiceMetadata {
            class: None,
            description: Some(format!("{} ({} profile)", site, profile)),
            service_type: Some(profile.to_string()),
            tier: Some("SERVICE".to_string()),
            schema_keys: None,
        }),
        capabilities: caps,
        depends_on: Vec::new(),
    };

    let translated = translate_all_services(&[def], &schema);
    info!("Registering '{}' as a '{}'-profile service entity", site, profile);

    if args.dry_run {
        info!("Dry run — would register '{}' into {{{}}}:gnode:services", site, site);
        return Ok(());
    }

    let client = redis::Client::open(args.redis_url.as_str()).map_err(GeometricError::Redis)?;
    let mut conn = client.get_connection().map_err(GeometricError::Redis)?;

    let (registered, errors) = register_services_for_site(&mut conn, site, &translated, "")?;
    info!("  Profile registration: {}/{} for site '{}'", registered, registered + errors, site);
    if errors > 0 {
        return Err(GeometricError::Other(format!(
            "{} error(s) registering site '{}'",
            errors, site
        )));
    }
    Ok(())
}

/// Register ecosystem tools (tool tier) into ecosystem:tools:topology:*
fn run_tool_tier(args: &RegisterToolsArgs) -> Result<()> {
    // 1. Load tool schema
    let schema_path = find_tool_schema_path(args.schema_path.as_ref())
        .ok_or_else(|| GeometricError::Other(
            "Cannot find tool_schema.yaml. Use --schema to specify path.".to_string()
        ))?;
    info!("Loading tool schema from: {:?}", schema_path);
    let schema = load_schema(&schema_path)?;

    // 2. Load ecosystem tools
    let config_path = find_ecosystem_tools_path(args.config_path.as_ref())
        .ok_or_else(|| GeometricError::Other(
            "Cannot find ecosystem_tools.yaml. Use --config to specify path.".to_string()
        ))?;
    info!("Loading ecosystem tools from: {:?}", config_path);
    let mut components = load_ecosystem_tools(&config_path)?;

    if components.is_empty() {
        warn!("No components found in ecosystem_tools.yaml");
        return Ok(());
    }

    info!("Found {} ecosystem tool definitions", components.len());

    // 3. Compute pyramid layout
    let layout = compute_pyramid_layout(&components);
    info!("Pyramid layout computed for {} components", layout.len());

    // 4. Inject pyramid coordinates into capabilities
    for comp in &mut components {
        if let Some(&(px, py, pz)) = layout.get(&comp.id) {
            comp.capabilities.push(CapabilityEntry {
                name: "pyramid_x".to_string(),
                value: serde_yaml::Value::Number(serde_yaml::Number::from(px)),
            });
            comp.capabilities.push(CapabilityEntry {
                name: "pyramid_y".to_string(),
                value: serde_yaml::Value::Number(serde_yaml::Number::from(py)),
            });
            comp.capabilities.push(CapabilityEntry {
                name: "pyramid_z".to_string(),
                value: serde_yaml::Value::Number(serde_yaml::Number::from(pz)),
            });
            info!("  {} → x={:.2} y={:.2} z={:.2}", comp.id, px, py, pz);
        }
    }

    // 5. Translate capabilities to coordinates
    let translated = translate_all_services(&components, &schema);

    if args.dry_run {
        for svc in &translated {
            info!("  [DRY-RUN] {} → bucket_key={}", svc.id,
                &svc.bucket_key[..std::cmp::min(20, svc.bucket_key.len())]);
        }
        info!("Dry run complete. {} tools would be registered.", translated.len());
        return Ok(());
    }

    // 6. Connect to ValKey
    let client = redis::Client::open(args.redis_url.as_str())
        .map_err(GeometricError::Redis)?;
    let mut conn = client.get_connection()
        .map_err(GeometricError::Redis)?;

    // 7. Register into ecosystem tools topology
    // Uses standard key format {ecosystem}:gnode:services — compatible with Lua ENSURE/REGISTER
    let (registered, errors) = register_services_for_site(
        &mut conn, "ecosystem", &translated, ""
    )?;

    info!("═══════════════════════════════════════════════════════════");
    info!("  Tool registration complete: {}/{} registered", registered, translated.len());
    if errors > 0 {
        warn!("  {} errors during registration", errors);
    }
    info!("═══════════════════════════════════════════════════════════");

    Ok(())
}

/// Register service-tier tools for one or more sites (original behavior).
fn run_service_tier(args: RegisterToolsArgs) -> Result<()> {
    // 1. Find and load service-tier schema (default: service_schema.yaml).
    //    Override with --schema for a non-default location or alternate tier.
    let schema_path = find_schema_path(args.schema_path.as_ref())
        .ok_or_else(|| GeometricError::Other(
            "Cannot find service_schema.yaml. Use --schema to specify path.".to_string()
        ))?;

    info!("Loading tier schema from: {:?}", schema_path);
    let schema = load_schema(&schema_path)?;

    // 2. Find and load geometric topology config
    let config_path = find_config_path(args.config_path.as_ref())
        .ok_or_else(|| GeometricError::Other(
            "Cannot find geometric_topology.yaml. Use --config to specify path.".to_string()
        ))?;

    info!("Loading tool definitions from: {:?}", config_path);
    let services = load_service_definitions(&config_path)?;

    if services.is_empty() {
        warn!("No services found in geometric_topology.yaml");
        return Ok(());
    }

    info!("Found {} tool service definitions", services.len());

    // 3. Pre-translate all services (before connecting to ValKey)
    let translated = translate_all_services(&services, &schema);

    if args.dry_run {
        for svc in &translated {
            info!(
                "  [DRY-RUN] {} → bucket_key={} z_score={}",
                svc.id,
                &svc.bucket_key[..std::cmp::min(20, svc.bucket_key.len())],
                svc.z_score,
            );
        }
        info!("Dry run complete. {} tools would be registered.", translated.len());
        return Ok(());
    }

    // 4. Connect to ValKey
    let client = redis::Client::open(args.redis_url.as_str())
        .map_err(GeometricError::Redis)?;
    let mut conn = client.get_connection()
        .map_err(GeometricError::Redis)?;

    // 5. Determine target sites
    let sites = if let Some(ref site) = args.site {
        vec![site.clone()]
    } else {
        let discovered = discover_registered_sites(&mut conn)?;
        if discovered.is_empty() {
            error!("No registered sites found. Use --site to specify one, or register sites first.");
            return Err(GeometricError::Other("No sites found".to_string()));
        }
        discovered
    };

    info!("Registering tools for {} site(s): {:?}", sites.len(), sites);

    // 6. Register tools for each site
    let mut total_registered = 0;
    let mut total_errors = 0;

    for site_id in &sites {
        info!("--- Registering tools for site: {} ---", site_id);
        // Always use standard key format {site_id}:gnode:services — matches Lua ENSURE/REGISTER
        let (registered, errors) = register_services_for_site(&mut conn, site_id, &translated, "")?;
        total_registered += registered;
        total_errors += errors;
    }

    info!("═══════════════════════════════════════════════════════════");
    info!("  Registration complete: {} tools × {} sites", translated.len(), sites.len());
    info!("  Registered: {}, Errors: {}", total_registered, total_errors);
    info!("═══════════════════════════════════════════════════════════");

    if total_errors > 0 {
        Err(GeometricError::Other(format!("{} registration errors occurred", total_errors)))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod profile_tests {
    use super::*;
    use std::path::Path;

    // Validates the `profiles:` section of service_schema.yaml parses and that
    // every profile's capability values resolve via the schema (no typo'd value
    // names). Runs without ValKey. cwd is the crate root (daemon/) under cargo test.
    #[test]
    fn profiles_load_and_translate() {
        let schema = load_schema(Path::new("config/service_schema.yaml"))
            .expect("service_schema.yaml loads");
        for p in ["web", "headless", "service", "system", "component"] {
            let caps = schema.profiles.get(p).unwrap_or_else(|| panic!("profile '{}' present", p));
            assert!(!caps.is_empty(), "profile '{}' non-empty", p);
            let translated = translate_capabilities(caps, &schema);
            // Each named capability must resolve to a dimension (else a typo'd
            // value/name silently drops it) — every entry should land.
            assert_eq!(
                translated.len(),
                caps.len(),
                "profile '{}': all {} capabilities resolved (got {})",
                p, caps.len(), translated.len()
            );
        }
        // Spot-check the web profile's key dimensions.
        let web = translate_capabilities(&schema.profiles["web"], &schema);
        assert!(web.contains_key("service_scope"));
        assert!(web.contains_key("domain_primary"));
    }
}

#[cfg(test)]
mod dimension_constant_tests {
    use super::*;
    use std::io::Write;

    fn schema(total: usize) -> CapabilitySchema {
        CapabilitySchema {
            schema_version: "3.0".into(),
            tier: Some("service".into()),
            total_dimensions: total,
            discovery_dimensions: Some(total),
            dimensions: HashMap::new(),
            profiles: Default::default(),
        }
    }

    fn dir_with(lines: &[&str]) -> tempfile::TempDir {
        let d = tempfile::tempdir().unwrap();
        for (i, l) in lines.iter().enumerate() {
            let mut f = std::fs::File::create(d.path().join(format!("lib{}.lua", i))).unwrap();
            writeln!(f, "#!lua name=lib{}\n{}", i, l).unwrap();
        }
        d
    }

    #[test]
    fn agreeing_constants_pass() {
        let d = dir_with(&["local TOTAL_DIMENSIONS = 30", "local TOTAL_DIMENSIONS = 30"]);
        assert!(assert_lua_dimension_constants(&schema(30), d.path()).is_ok());
    }

    #[test]
    fn a_disagreeing_constant_is_an_error() {
        // The case this exists for: one library left behind at the old width.
        let d = dir_with(&["local TOTAL_DIMENSIONS = 30", "local TOTAL_DIMENSIONS = 23"]);
        let err = assert_lua_dimension_constants(&schema(30), d.path()).unwrap_err();
        assert!(format!("{}", err).contains("23"), "error must name the offending value: {}", err);
    }

    #[test]
    fn trailing_comments_do_not_break_parsing() {
        // gnode_topo.lua carries one, so a parser that chokes on it would report
        // agreement it never checked.
        let d = dir_with(&["local TOTAL_DIMENSIONS = 23  -- service tier"]);
        assert!(assert_lua_dimension_constants(&schema(30), d.path()).is_err());
    }

    #[test]
    fn a_missing_directory_is_not_a_disagreement() {
        assert!(assert_lua_dimension_constants(&schema(30), Path::new("/nonexistent-xyz")).is_ok());
    }

    #[test]
    fn the_shipped_libraries_agree_with_the_shipped_schema() {
        // The real files, not a fixture. This is the test that would have caught
        // the drift in the first place.
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let s = load_schema(&root.join("config/service_schema.yaml")).unwrap();
        assert_eq!(s.total_dimensions, s.dimensions.len(), "service_schema.yaml header vs entries");
        assert_lua_dimension_constants(&s, &root.join("functions")).unwrap();
    }
}

#[cfg(test)]
mod tier_discovery_tests {
    use super::*;

    #[test]
    fn all_four_shipped_tiers_are_found() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let found = find_all_tier_schemas(&root.join("config/service_schema.yaml"));
        assert_eq!(found.len(), 4, "expected all four tier schemas, got {:?}", found);
    }

    #[test]
    fn every_shipped_tier_is_self_consistent() {
        // The header and the body of each schema must agree. A tier whose
        // declared total differs from its entry count publishes a width that
        // sizes vectors nothing else will match.
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        for p in find_all_tier_schemas(&root.join("config/service_schema.yaml")) {
            let s = load_schema(&p).unwrap_or_else(|e| panic!("{:?}: {}", p, e));
            assert_eq!(s.total_dimensions, s.dimensions.len(), "{:?} header vs entries", p);
            let disc = s.discovery_dimensions.unwrap_or(s.total_dimensions);
            assert!(disc <= s.total_dimensions, "{:?}: discovery {} exceeds total {}", p, disc, s.total_dimensions);
            // Indices must be a contiguous 0..n, or a vector built from the map
            // has holes the daemon reads as zeroed capabilities.
            let mut idx: Vec<usize> = s.dimensions.values().map(|d| d.index).collect();
            idx.sort_unstable();
            assert_eq!(idx, (0..s.total_dimensions).collect::<Vec<_>>(), "{:?}: indices not contiguous", p);
        }
    }

    #[test]
    fn a_missing_tier_is_skipped_not_fatal() {
        let d = tempfile::tempdir().unwrap();
        let svc = d.path().join("service_schema.yaml");
        std::fs::write(&svc, "schema_version: '3.0'\ntier: service\ntotal_dimensions: 0\ndimensions: {}\n").unwrap();
        assert_eq!(find_all_tier_schemas(&svc).len(), 1);
    }
}

#[cfg(test)]
mod preflight_tests {
    use super::*;

    fn args(url: &str, dry: bool) -> RegisterToolsArgs {
        RegisterToolsArgs {
            site: None, config_path: None, schema_path: None,
            dry_run: dry, redis_url: url.to_string(),
            topology_namespace: "geodineum".into(), tier: "tool".into(),
            profile: None, environment: None,
        }
    }

    #[test]
    fn an_unauthenticated_url_fails_before_doing_the_work() {
        // The case this exists for: every write NOAUTHs after the full layout
        // is computed and logged, reporting "0/N registered" under healthy output.
        let e = run(args("redis://127.0.0.1:47445", false)).unwrap_err();
        let msg = format!("{}", e);
        assert!(msg.contains("no credential"), "must name the cause: {}", msg);
        assert!(msg.contains("GNODE_REDIS_AUTH_FILE"), "must name the fix: {}", msg);
    }

    #[test]
    fn a_dry_run_is_exempt_because_it_never_connects() {
        // Refusing here would break the preview, which is the one thing that
        // works without a credential.
        let r = run(args("redis://127.0.0.1:47445", true));
        assert!(!format!("{:?}", r).contains("no credential"));
    }

    #[test]
    fn an_authenticated_url_passes_the_preflight() {
        let e = run(args("redis://user:pw@127.0.0.1:47445", false));
        assert!(!format!("{:?}", e).contains("no credential"));
    }
}
