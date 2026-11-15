//! The tool tier records an intent per component, and the reconciler holds the topology to it.
//!
//!   valkey-server --port 6397 --save "" --appendonly no --requirepass pw --daemonize yes
//!   GNODE_TEST_VALKEY_URL=redis://default:pw@127.0.0.1:6397 cargo test --test tool_intent_live -- --ignored --test-threads=1

use std::path::Path;

use gnode::constellation::Constellation;
use gnode::registration_intent::{component_intent_key, intent_key, read_components, reconcile};
use gnode::tool_registration::{run, RegisterToolsArgs};
use gnode::topology_schema::TopologySchema;

const CATALOGUE: &str = r#"
components:
  - id: gMath
    capabilities:
      - { name: protocol, value: library }
      - { name: native_format, value: fixed_point }
      - { name: contract_version, value: v1 }
      - { name: domain, value: [math] }
      - { name: implementation_language, value: rust }
  - id: ValKey
    requires: [credentials]
    capabilities:
      - { name: protocol, value: resp3_direct }
      - { name: native_format, value: resp3 }
      - { name: contract_version, value: v1 }
      - { name: domain, value: [storage] }
      - { name: implementation_language, value: c }
"#;

fn setup() -> Option<(String, redis::Connection, Constellation)> {
    let url = std::env::var("GNODE_TEST_VALKEY_URL").ok()?;
    let mut conn = redis::Client::open(url.as_str()).unwrap().get_connection().unwrap();
    let _: String = redis::cmd("FLUSHALL").query(&mut conn).unwrap();
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    for lib in ["functions/gnode_topo.lua", "functions/gnode_topology.lua"] {
        let src = std::fs::read_to_string(root.join(lib)).unwrap();
        let _: String = redis::cmd("FUNCTION").arg("LOAD").arg("REPLACE").arg(src).query(&mut conn).unwrap();
    }
    let schema = TopologySchema::load(&root.join("config/topology_schema.yaml")).unwrap();
    let c = Constellation::new("geodineum");
    c.publish_schema(&mut conn, &schema).unwrap();
    Some((url, conn, c))
}

fn args(url: &str, config: &Path) -> RegisterToolsArgs {
    RegisterToolsArgs {
        site: None, config_path: Some(config.to_path_buf()), schema_path: None, dry_run: false,
        redis_url: url.to_string(), topology_namespace: "geodineum".into(), tier: "tool".into(),
        profile: None, environment: None, manifest: None, node_role: None, specialization: None, on: None,
    }
}

#[test]
#[ignore]
fn each_registered_component_leaves_an_intent_the_reconciler_holds() {
    let Some((url, mut conn, c)) = setup() else { return };
    let dir = std::env::temp_dir().join(format!("gnode-tool-intent-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let cfg = dir.join("ecosystem_tools.yaml");
    std::fs::write(&cfg, CATALOGUE).unwrap();

    run(args(&url, &cfg)).unwrap();

    let ids: Vec<String> = read_components(&mut conn, "geodineum").unwrap().into_iter().map(|i| i.site).collect();
    assert_eq!(ids, vec!["ValKey".to_string(), "gMath".to_string()]);
    let services: i64 = redis::cmd("HLEN").arg(intent_key("geodineum")).query(&mut conn).unwrap();
    assert_eq!(services, 0, "components stay out of the site and service intents");
    let raw: String = redis::cmd("HGET").arg(component_intent_key("geodineum")).arg("ValKey").query(&mut conn).unwrap();
    let v: serde_json::Value = serde_json::from_str(&raw).unwrap();
    assert_eq!((v["profile"].as_str(), v["shape"].as_str(), v["environment"].as_str()), (Some("component"), Some("component"), Some("global")));

    let r = reconcile(&mut conn, &c).unwrap();
    assert_eq!((r.examined, r.drifted.len()), (2, 0), "{:?}", r.drifted);

    let _: String = redis::cmd("FCALL").arg("GNODE_TOPO_DEREGISTER").arg(1).arg(c.key()).arg("ValKey").query(&mut conn).unwrap();
    let r = reconcile(&mut conn, &c).unwrap();
    assert_eq!(r.drifted, vec!["ValKey".to_string()], "a component gone from the topology is drift");

    run(args(&url, &cfg)).unwrap();
    let r = reconcile(&mut conn, &c).unwrap();
    assert!(r.drifted.is_empty(), "re-running the tier restores it: {:?}", r.drifted);
    let _ = std::fs::remove_dir_all(&dir);
}
