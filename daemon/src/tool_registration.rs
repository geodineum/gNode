//! `gnode-daemon register-tools`: registration from the operator's side, by name, on the
//! one topology (SPEC-ONE-TOPOLOGY §4, §10).
//!
//!   --tier tool                       the ecosystem components (shape 2) from ecosystem_tools.yaml
//!   --tier service --site <id>        one site or service, from its manifest's gnode: block
//!                 [--manifest <path>] (or the named --profile when the block is absent)
//!                 [--profile <word>] [--environment <dtap>]
//!   --tier node --site <id>           a node that runs no daemon (an observer laptop, an app
//!                 --node-role <word>  box behind a tunnel), in --topology-namespace
//!
//! The daemon codes every word through the compiled schema and refuses one it does not
//! know; nothing here computes a coordinate.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use log::{info, warn};
use serde::Deserialize;
use crate::{Result, GeometricError};

// ============================================================================
// The component catalogue (ecosystem_tools.yaml)
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct ToolServiceDef {
    pub id: String,
    pub metadata: Option<ServiceMetadata>,
    pub capabilities: Vec<CapabilityEntry>,
    #[serde(default)]
    pub depends_on: Vec<String>,
    /// REACH labels the component requires of a caller (one topology, D8).
    #[serde(default)]
    pub requires: Vec<String>,
    /// The registered site this service belongs to, where the catalogue names one.
    #[serde(default)]
    pub site: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct EcosystemToolsConfig {
    pub schema: Option<String>,
    pub tier: Option<String>,
    pub shape: Option<String>,
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
    /// Schema keys for schema↔topology cross-reference: "{component}:{contract_name}".
    pub schema_keys: Option<Vec<String>>,
    /// The component name in its heartbeat key when it differs from the id.
    #[serde(default)]
    pub heartbeat: Option<String>,
    /// How to ask this component what it can do: `describe:<site>`, `cli:<repo>/geodeploy.yaml`,
    /// `doc:<path>` — pointers to resolvable artifacts, several allowed.
    pub affordances: Option<Vec<String>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CapabilityEntry {
    pub name: String,
    pub value: serde_yaml::Value,
}

pub fn load_ecosystem_tools(path: &Path) -> Result<Vec<ToolServiceDef>> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| GeometricError::Other(format!("Failed to read {:?}: {}", path, e)))?;
    let config: EcosystemToolsConfig = serde_yaml::from_str(&content)
        .map_err(|e| GeometricError::Other(format!("Failed to parse {:?}: {}", path, e)))?;
    Ok(config.components.unwrap_or_default())
}

/// ecosystem_tools.yaml: operator-local first (/etc, outside every repo, where a private
/// component is added to a public installer's host), the shipped copy second.
fn find_ecosystem_tools_path(explicit: Option<&PathBuf>) -> Option<PathBuf> {
    if let Some(p) = explicit {
        if p.exists() { return Some(p.clone()); }
    }
    [
        PathBuf::from("/etc/geodineum/ecosystem_tools.yaml"),
        PathBuf::from("/opt/geodineum/Geodineum/config/ecosystem_tools.yaml"),
        PathBuf::from("../Geodineum/config/ecosystem_tools.yaml"),
        PathBuf::from("../../Geodineum/config/ecosystem_tools.yaml"),
    ].into_iter().find(|p| p.exists())
}

// ============================================================================
// Arguments (from the CLI)
// ============================================================================

pub struct RegisterToolsArgs {
    pub site: Option<String>,
    pub config_path: Option<PathBuf>,
    pub schema_path: Option<PathBuf>,
    pub dry_run: bool,
    pub redis_url: String,
    pub topology_namespace: String,
    /// "service" (default) | "tool" | "node"
    pub tier: String,
    /// The registration profile (web | headless | service | system | component) when the
    /// manifest has no gnode: block.
    pub profile: Option<String>,
    /// DTAP environment override (testing | staging | acceptance | production).
    pub environment: Option<String>,
    /// The onboarding manifest whose `gnode:` block declares the service.
    pub manifest: Option<PathBuf>,
    /// `--tier node`: the node's role word (observer | headless | edge | replica | full_node).
    pub node_role: Option<String>,
    /// `--tier node`: the node's specialization word, when any.
    pub specialization: Option<String>,
    /// `--tier service`: the hosting node; registration places the entity there.
    pub on: Option<String>,
}

// ============================================================================
// Entry
// ============================================================================

pub fn run(args: RegisterToolsArgs) -> Result<()> {
    // Fail BEFORE doing the work: a URL with no credential would read the catalogue and
    // then fail every write with NOAUTH. The dry run never connects and is exempt.
    if !args.dry_run && !args.redis_url.contains('@') {
        return Err(GeometricError::Other(format!(
            "ValKey URL carries no credential ({}). Registration writes would all \
             fail NOAUTH. Pass --redis-auth-file and \
             --redis-user (env: GNODE_REDIS_AUTH_FILE / VALKEY_USER), e.g.\n  \
             GNODE_REDIS_AUTH_FILE=/etc/geodineum/credentials/valkey_daemon.password \
             VALKEY_USER=gnode_daemon gnode-daemon register-tools --tier {}",
            args.redis_url, args.tier
        )));
    }

    match args.tier.as_str() {
        "tool" | "component" | "components" => run_tool_tier(&args),
        "node" => run_node_tier(&args),
        "service" => run_service(&args),
        "constellation" => Err(GeometricError::Other(
            "a node registers itself into its constellation at daemon start; an operator declares a daemon-less node with --tier node".to_string(),
        )),
        "galaxy" => Err(GeometricError::Other(
            "galaxy-tier registration is not implemented. A constellation joins a galaxy by publishing into a signed epoch artifact, not through this command.".to_string(),
        )),
        other => Err(GeometricError::Other(format!(
            "unknown tier {:?}. Valid tiers: service, tool, node.", other
        ))),
    }
}

fn schema() -> Result<&'static crate::topology_schema::TopologySchema> {
    crate::integration::handlers::one_topology::schema()
        .ok_or_else(|| GeometricError::Other("topology_schema.yaml not found (GNODE_TOPOLOGY_SCHEMA)".to_string()))
}

fn connect(args: &RegisterToolsArgs) -> Result<redis::Connection> {
    let client = redis::Client::open(args.redis_url.as_str()).map_err(GeometricError::Redis)?;
    client.get_connection().map_err(GeometricError::Redis)
}

/// The manifest's own `profile:` word, used when it carries no `gnode:` block.
fn manifest_profile(path: &std::path::Path) -> Option<String> {
    let doc: serde_yaml::Value = serde_yaml::from_str(&std::fs::read_to_string(path).ok()?).ok()?;
    doc.get("profile").and_then(|v| v.as_str()).map(str::to_string)
}

/// One site or service, by name: the manifest's `gnode:` block, else the named profile's
/// words from the one schema (web → site, headless → app, the rest → daemon).
fn run_service(args: &RegisterToolsArgs) -> Result<()> {
    let site = args.site.as_deref().filter(|s| !s.is_empty()).ok_or_else(|| {
        GeometricError::Other("--tier service needs --site <id>".to_string())
    })?;
    let from_manifest = match &args.manifest {
        Some(path) => crate::integration::handlers::one_topology::manifest_params(site, path, args.environment.as_deref())?,
        None => None,
    };
    let mut p = match from_manifest {
        Some(p) => { info!("Declaration from the manifest's gnode: block"); p }
        None => {
            let declared = args.manifest.as_ref().and_then(|m| manifest_profile(m));
            let profile = args.profile.as_deref().or(declared.as_deref()).unwrap_or("web");
            let s = schema()?;
            let axes = s.profile(profile).ok_or_else(|| GeometricError::Other(format!(
                "profile '{}' is not in topology_schema.yaml (available: {:?})", profile, s.profile_names()
            )))?;
            crate::integration::handlers::one_topology::profile_params(site, profile, &axes, args.environment.as_deref())
        }
    };
    if let Some(node) = args.on.as_deref().filter(|n| !n.is_empty()) {
        p["on"] = serde_json::json!(node);
    }
    info!("Registering '{}' by name as shape {} in {}", site, p["shape"], p["constellation"].as_str().unwrap_or(&args.topology_namespace));
    if args.dry_run {
        info!("Dry run — would register {}", p);
        return Ok(());
    }
    let mut conn = connect(args)?;
    let r = crate::integration::handlers::one_topology::register(&mut conn, site, p);
    if r.status != "ok" {
        return Err(GeometricError::Other(format!("site '{}' refused: {:?}", site, r.error)));
    }
    info!("  Registered '{}' on the one topology: {}", site, r.result.unwrap_or_default());
    Ok(())
}

/// A node that runs no daemon, declared by the operator in --topology-namespace, so the
/// heartbeats its services write can become placements.
fn run_node_tier(args: &RegisterToolsArgs) -> Result<()> {
    let node = args.site.as_deref().filter(|s| !s.is_empty()).ok_or_else(|| GeometricError::Other("--tier node needs --site <node id>".to_string()))?;
    let role = args.node_role.as_deref().unwrap_or("observer");
    let d = crate::constellation::declare_operator_node(node, role, args.specialization.as_deref(), 0, 0);
    let encoded = crate::constellation::encode(schema()?, &d)?;
    let c = crate::constellation::Constellation::new(&args.topology_namespace);
    info!("Declaring node '{}' (role {}) in constellation {}", node, role, c.name);
    if args.dry_run {
        info!("Dry run — would register {:?}", d);
        return Ok(());
    }
    let mut conn = connect(args)?;
    c.ensure(&mut conn)?;
    c.register(&mut conn, &encoded)?;
    info!("  Node '{}' registered on {}", node, c.key());
    Ok(())
}

/// The ecosystem components (shape 2, environment global) from ecosystem_tools.yaml.
fn run_tool_tier(args: &RegisterToolsArgs) -> Result<()> {
    let config_path = find_ecosystem_tools_path(args.config_path.as_ref())
        .ok_or_else(|| GeometricError::Other("Cannot find ecosystem_tools.yaml. Use --config to specify path.".to_string()))?;
    info!("Loading ecosystem components from: {:?}", config_path);
    let components = load_ecosystem_tools(&config_path)?;
    if components.is_empty() {
        warn!("No components found in ecosystem_tools.yaml");
        return Ok(());
    }
    let params: Vec<serde_json::Value> = components.iter().map(crate::integration::handlers::one_topology::component_params).collect();
    if args.dry_run {
        for p in &params { info!("  [DRY-RUN] {} → shape 2 {}", p["id"], p["axes"]); }
        info!("Dry run complete. {} components would register on the one topology.", params.len());
        return Ok(());
    }
    let mut conn = connect(args)?;
    let (mut ok, mut err) = (0usize, 0usize);
    for p in params {
        let id = p["id"].as_str().unwrap_or("").to_string();
        let r = crate::integration::handlers::one_topology::register(&mut conn, &id, p);
        if r.status == "ok" { ok += 1 } else { err += 1; warn!("component {} refused: {:?}", id, r.error) }
    }
    info!("Component registration: {} registered, {} refused", ok, err);
    if err > 0 {
        return Err(GeometricError::Other(format!("{} component(s) refused", err)));
    }
    Ok(())
}

#[cfg(test)]
mod preflight_tests {
    use super::*;

    #[test]
    fn a_manifest_without_a_gnode_block_keeps_its_own_profile() {
        let dir = std::env::temp_dir().join(format!("gnode-manifest-profile-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let m = dir.join("gnode_services.yaml");
        std::fs::write(&m, "profile: service\nservices: []\n").unwrap();
        assert_eq!(manifest_profile(&m).as_deref(), Some("service"));
        std::fs::write(&m, "services: []\n").unwrap();
        assert_eq!(manifest_profile(&m), None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn args(url: &str, dry: bool) -> RegisterToolsArgs {
        RegisterToolsArgs {
            site: None, config_path: None, schema_path: None,
            dry_run: dry, redis_url: url.to_string(),
            topology_namespace: "geodineum".into(), tier: "tool".into(),
            profile: None, environment: None,
            manifest: None,
            node_role: None,
            specialization: None,
            on: None,
        }
    }

    #[test]
    fn an_unauthenticated_url_fails_before_doing_the_work() {
        let e = run(args("redis://127.0.0.1:47445", false)).unwrap_err();
        let msg = format!("{}", e);
        assert!(msg.contains("no credential"), "must name the cause: {}", msg);
        assert!(msg.contains("GNODE_REDIS_AUTH_FILE"), "must name the fix: {}", msg);
    }

    #[test]
    fn a_dry_run_is_exempt_because_it_never_connects() {
        let r = run(args("redis://127.0.0.1:47445", true));
        assert!(!format!("{:?}", r).contains("no credential"));
    }

    #[test]
    fn an_authenticated_url_passes_the_preflight() {
        let e = run(args("redis://user:pw@127.0.0.1:47445", false));
        assert!(!format!("{:?}", e).contains("no credential"));
    }

    #[test]
    fn the_shipped_profiles_name_shapes_the_schema_knows() {
        let s = crate::topology_schema::TopologySchema::load(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("config/topology_schema.yaml").as_path()).unwrap();
        for (profile, shape) in [("web", "site"), ("headless", "app"), ("service", "daemon"), ("system", "daemon"), ("component", "daemon")] {
            let axes = s.profile(profile).unwrap_or_else(|| panic!("profile {} present", profile));
            let p = crate::integration::handlers::one_topology::profile_params("x", profile, &axes, None);
            assert_eq!(p["shape"], shape);
            let params: crate::integration::handlers::one_topology::RegisterParams = serde_json::from_value(p).unwrap();
            crate::integration::handlers::one_topology::plan_register(&s, &params).unwrap_or_else(|e| panic!("profile {}: {}", profile, e));
        }
    }
}
