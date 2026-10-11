//! Placements from heartbeats and health from placements, on a real ValKey, under the
//! per-constellation lease (SPEC-ONE-TOPOLOGY §5; P-d).
//!
//!   valkey-server --port 6395 --save "" --daemonize yes
//!   GNODE_TEST_VALKEY_URL=redis://127.0.0.1:6395 cargo test --test registrar_live -- --ignored --test-threads=1

use std::path::Path;

use gnode::constellation::{encode, Constellation, Declared, Filters};
use gnode::constellation_worker::{ConstellationWorker, SelfFacts};
use gnode::integration::lease::{hold_lease, release_lease};
use gnode::topology_schema::TopologySchema;

fn connect() -> Option<(redis::Connection, Constellation, TopologySchema)> {
    let url = std::env::var("GNODE_TEST_VALKEY_URL").ok()?;
    let mut conn = redis::Client::open(url).unwrap().get_connection().unwrap();
    let _: String = redis::cmd("FLUSHALL").query(&mut conn).unwrap();
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    for lib in ["functions/gnode_topo.lua", "functions/gnode_topology.lua"] {
        let src = std::fs::read_to_string(root.join(lib)).unwrap();
        let _: String = redis::cmd("FUNCTION").arg("LOAD").arg("REPLACE").arg(src).query(&mut conn).unwrap();
    }
    let schema = TopologySchema::load(&root.join("config/topology_schema.yaml")).unwrap();
    let c = Constellation::new("geodineum");
    c.publish_schema(&mut conn, &schema).unwrap();
    Some((conn, c, schema))
}

fn facts(node: &str) -> SelfFacts {
    SelfFacts { node_id: node.into(), role: "master".into(), specialization: "general".into(), version: "0.1.1".into() }
}

fn heartbeat(conn: &mut redis::Connection, c: &Constellation, env: &str, comp: &str, node: &str, ts: u64) {
    let _: () = redis::cmd("SET").arg(c.heartbeat_key(env, comp, node))
        .arg(format!(r#"{{"ts":{ts},"pid":1,"comp":"{comp}","node":"{node}"}}"#)).query(conn).unwrap();
}

#[test]
#[ignore]
fn the_live_census_shape_yields_placements_and_health() {
    let Some((mut conn, c, s)) = connect() else { return };
    let mut w = ConstellationWorker::new(vec![c.clone()], s.clone(), facts("aesir-solutions"));
    w.declare_self(&mut conn, &c).unwrap();
    let squad = ConstellationWorker::new(vec![c.clone()], s.clone(), facts("squad"));
    squad.declare_self(&mut conn, &c).unwrap();

    // The live census: components and daemon-services as they are named today.
    c.register(&mut conn, &encode(&s, &Declared::new("gFlow", "2", "global").words("protocol", &["gnode_stream"])).unwrap()).unwrap();
    c.register(&mut conn, &encode(&s, &Declared::new("gSchedule", "2", "global").words("protocol", &["gnode_stream"])).unwrap()).unwrap();
    c.register(&mut conn, &encode(&s, &Declared::new("Geodineum-COMMS", "2", "global").words("protocol", &["gnode_stream"]).meta(serde_json::json!({"heartbeat": "comms"}))).unwrap()).unwrap();
    for svc in ["gflow", "gschedule", "gshield"] {
        c.register(&mut conn, &encode(&s, &Declared::new(svc, "3.3", "production").words("protocol", &["gnode_stream"])).unwrap()).unwrap();
    }
    c.register(&mut conn, &encode(&s, &Declared::new("nierto_com", "3.1", "production").words("protocol", &["http_rest"])).unwrap()).unwrap();

    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
    heartbeat(&mut conn, &c, "all", "gnode-daemon", "aesir-solutions", now - 10);
    heartbeat(&mut conn, &c, "all", "gnode-daemon", "squad", now - 20);
    heartbeat(&mut conn, &c, "production", "gflow", "aesir-solutions", now - 21);
    heartbeat(&mut conn, &c, "production", "gschedule", "aesir-solutions", now - 23);
    heartbeat(&mut conn, &c, "production", "gshield", "aesir-solutions", now - 48);
    heartbeat(&mut conn, &c, "production", "geodine", "squad", now - 5);
    let _: () = redis::cmd("SET").arg(c.heartbeat_key("production", "comms", "")).arg(r#"{"ts":1,"pid":1,"comp":"comms"}"#).query(&mut conn).unwrap();
    // the key above ends with a trailing colon from an empty node; the pre-contract writer has no colon at all:
    let _: () = redis::cmd("DEL").arg(c.heartbeat_key("production", "comms", "")).query(&mut conn).unwrap();
    let _: () = redis::cmd("SET").arg("{geodineum}:gnode:heartbeat:production:comms").arg(r#"{"ts":1,"pid":1,"comp":"comms"}"#).query(&mut conn).unwrap();

    let r = w.pass(&mut conn, &c).unwrap();
    assert!(r.leased);
    assert_eq!(r.heartbeats, 7);
    assert_eq!(r.unparsed, vec!["{geodineum}:gnode:heartbeat:production:comms"], "COMMS without a node segment cannot be placed");
    assert_eq!(r.unmatched, vec!["{geodineum}:gnode:heartbeat:production:geodine:squad"], "no entity named geodine");
    let mut placed = r.placed.clone();
    placed.sort();
    assert_eq!(placed, vec!["gFlow@aesir-solutions", "gSchedule@aesir-solutions", "gflow@aesir-solutions", "gschedule@aesir-solutions", "gshield@aesir-solutions"],
        "a heartbeat proves the component (4.1) and the daemon-service (4.2); gnode-daemon placements were declared by the nodes themselves");
    assert_eq!((r.fresh, r.placements), (7, 7), "2 gNode@node + 5 from heartbeats, all fresh");
    let daemons = c.query(&mut conn, "3.3", &Filters::default(), 0).unwrap();
    assert_eq!(daemons.iter().filter(|d| d["pd"][17] == 1.0).count(), 3, "gflow, gschedule, gshield healthy");
    let site = c.query(&mut conn, "3.1", &Filters::default(), 0).unwrap();
    assert_eq!(site[0]["pd"][17], 0.0, "a site with no placement stays unknown until registration places it");

    // Idempotent: a second pass places nothing new and keeps every placement fresh.
    let r2 = w.pass(&mut conn, &c).unwrap();
    assert!(r2.placed.is_empty());
    assert_eq!(r2.fresh, 7);

    // Another node cannot pass while the lease is held; after release it can.
    let mut other = ConstellationWorker::new(vec![c.clone()], s.clone(), facts("squad"));
    assert!(!other.pass(&mut conn, &c).unwrap().leased);
    release_lease(&mut conn, &c.lease_key(), "aesir-solutions");
    assert!(other.pass(&mut conn, &c).unwrap().leased);
    assert!(hold_lease(&mut conn, &c.lease_key(), "squad"));

    // gshield's heartbeat goes stale: its placement reads dead and gshield leaves the healthy set.
    heartbeat(&mut conn, &c, "production", "gshield", "aesir-solutions", now - 300);
    let r3 = other.pass(&mut conn, &c).unwrap();
    assert_eq!(r3.fresh, 6);
    let gshield = c.query(&mut conn, "3.3", &Filters::default().facet("protocol", &["gnode_stream"]), 0).unwrap()
        .into_iter().find(|d| d["id"] == "gshield").unwrap();
    assert_eq!(gshield["pd"][17], 0.25, "dead");
    assert_eq!(c.verify(&mut conn).unwrap(), "OK");
}
