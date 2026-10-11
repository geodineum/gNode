//! The sampler's one-topology writes on a real ValKey (SPEC-ONE-TOPOLOGY §5, D13, D18):
//! a placement's load, the service's max over its placements, `idle` for a service the
//! ring routed nothing to in a window, and the scale-out suggestion beside the point.
//!
//!   valkey-server --port 6393 --save "" --daemonize yes
//!   GNODE_TEST_VALKEY_URL=redis://127.0.0.1:6393 cargo test --test sampler_one_topology_live -- --ignored --test-threads=1

use std::collections::HashMap;
use std::path::Path;

use gnode::constellation::{encode, Constellation, Declared, Filters, Placement};
use gnode::integration::lease::hold_lease;
use gnode::integration::sampler::{DemandTracker, DerivedWrite, Suggestion, Tier};
use gnode::topology_schema::TopologySchema;
use gnode::worker::LoadSamplerWorker;

fn connect() -> Option<(redis::Connection, Constellation)> {
    let url = std::env::var("GNODE_TEST_VALKEY_URL").ok()?;
    let mut conn = redis::Client::open(url).unwrap().get_connection().unwrap();
    let _: String = redis::cmd("FLUSHALL").query(&mut conn).unwrap();
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    for lib in ["functions/gnode_topo.lua", "functions/gnode_topology.lua"] {
        let src = std::fs::read_to_string(root.join(lib)).unwrap();
        let _: String = redis::cmd("FUNCTION").arg("LOAD").arg("REPLACE").arg(src).query(&mut conn).unwrap();
    }
    let s = TopologySchema::load(&root.join("config/topology_schema.yaml")).unwrap();
    let c = Constellation::new("geodineum");
    c.publish_schema(&mut conn, &s).unwrap();
    c.ensure(&mut conn).unwrap();
    for n in ["aesir-solutions", "squad"] {
        c.register(&mut conn, &encode(&s, &Declared::new(n, "1", "global").word("node_role", "full_node")).unwrap()).unwrap();
    }
    for svc in ["gflow", "gschedule"] {
        c.register(&mut conn, &encode(&s, &Declared::new(svc, "3.3", "production").words("protocol", &["gnode_stream"])).unwrap()).unwrap();
    }
    for node in ["aesir-solutions", "squad"] {
        let p = Placement { of: "gflow".into(), on: node.into(), env: "production".into(), heartbeat: "gflow".into(), heartbeat_env: None, weight: 1 };
        c.place(&mut conn, &p, &encode(&s, &Declared::new(&p.id(), "4.2", "production")).unwrap()).unwrap();
    }
    assert!(hold_lease(&mut conn, &c.lease_key(), "aesir-solutions"));
    Some((conn, c))
}

fn load_of(conn: &mut redis::Connection, c: &Constellation, shape: &str, id: &str) -> serde_json::Value {
    c.query(conn, shape, &Filters::default(), 0).unwrap().into_iter().find(|r| r["id"] == id).unwrap()
}

#[test]
#[ignore]
fn placements_services_idle_and_scale_out_are_written_to_the_constellation() {
    let Some((mut conn, c)) = connect() else { return };
    let services = vec!["gflow".to_string(), "gschedule".to_string()];
    let mut demand = DemandTracker::new(4);
    let writes = vec![
        DerivedWrite { tier: Tier::Service, site: "gflow".into(), entity: "gflow@aesir-solutions".into(), index: 16, pd: 0.7714, zs: 771400, reason: "test".into() },
        DerivedWrite { tier: Tier::Service, site: "gflow".into(), entity: "gflow@squad".into(), index: 16, pd: 0.40, zs: 400000, reason: "test".into() },
    ];
    let mut counters: HashMap<String, u64> = HashMap::new();
    counters.insert("gflow".into(), 12);
    counters.insert("gschedule".into(), 3);
    let done = LoadSamplerWorker::apply_one_topology(&mut conn, &c, "aesir-solutions", 1_700_000_000_000, &writes, &[], &mut demand, &services, &counters);
    assert!(done.iter().any(|d| d.starts_with("gflow=max 0.7714")), "{done:?}");
    assert_eq!(load_of(&mut conn, &c, "4.2", "gflow@aesir-solutions")["pd"][16], 0.7714);
    assert_eq!(load_of(&mut conn, &c, "4.2", "gflow@squad")["pd"][16], 0.40);
    assert_eq!(load_of(&mut conn, &c, "3.3", "gflow")["pd"][16], 0.7714, "the service reads the max over its placements");
    assert_eq!(load_of(&mut conn, &c, "3.3", "gschedule")["pd"][16], 0.0, "unknown: measured nothing, not yet a window quiet");

    // Four quiet ticks on gschedule's ring counter: idle, written once.
    let mut idle_written = 0;
    for _ in 0..4 {
        let done = LoadSamplerWorker::apply_one_topology(&mut conn, &c, "aesir-solutions", 1_700_000_015_000, &[], &[], &mut demand, &services, &counters);
        idle_written += done.iter().filter(|d| d.contains("gschedule=idle")).count();
    }
    assert_eq!(idle_written, 1);
    assert_eq!(load_of(&mut conn, &c, "3.3", "gschedule")["pd"][16], 0.2, "idle: gNode routed nothing to it for a window (D18)");
    assert_eq!(load_of(&mut conn, &c, "3.3", "gflow")["pd"][16], 0.7714, "a measured service is not overwritten by the idle rule");

    let s = Suggestion { tier: Tier::Service, site: "gflow".into(), entity: "gflow".into(), inflation: 3.5, ticks: 3 };
    let done = LoadSamplerWorker::apply_one_topology(&mut conn, &c, "aesir-solutions", 1_700_000_030_000, &[], &[s], &mut demand, &services, &counters);
    assert!(done.iter().any(|d| d.contains("scale-out")));
    let gflow = load_of(&mut conn, &c, "3.3", "gflow");
    assert_eq!(gflow["m"]["scale_out"]["inflation"], 3.5);
    assert_eq!(gflow["m"]["scale_out"]["since_ticks"], 3);
    assert_eq!(c.verify(&mut conn).unwrap(), "OK");

    // Without the lease nothing is written.
    let _: i64 = redis::cmd("DEL").arg(c.lease_key()).query(&mut conn).unwrap();
    assert!(hold_lease(&mut conn, &c.lease_key(), "squad"));
    let done = LoadSamplerWorker::apply_one_topology(&mut conn, &c, "aesir-solutions", 1_700_000_045_000, &writes, &[], &mut demand, &services, &counters);
    assert!(done.is_empty(), "a node without the lease writes nothing: {done:?}");
}
