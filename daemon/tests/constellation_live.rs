//! The daemon's constellation client end to end on a real ValKey: encode through the
//! compiled schema, register, place, query with colour, route, reach, derive health from a
//! heartbeat key, verify.
//!
//!   valkey-server --port 6396 --save "" --daemonize yes
//!   GNODE_TEST_VALKEY_URL=redis://127.0.0.1:6396 cargo test --test constellation_live -- --ignored --test-threads=1

use std::path::Path;

use gnode::constellation::{declare_gnode_component, declare_gnode_placement, declare_node, encode, Constellation, Declared, Filters, Placement};
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
    assert!(c.ensure(&mut conn).unwrap(), "created once");
    assert!(!c.ensure(&mut conn).unwrap(), "idempotent");
    Some((conn, c, schema))
}

#[test]
#[ignore]
fn the_daemon_declares_its_node_component_and_placement_and_health_follows_the_heartbeat() {
    let Some((mut conn, c, s)) = connect() else { return };
    c.register(&mut conn, &encode(&s, &declare_node("aesir-solutions", "master", "general")).unwrap()).unwrap();
    c.register(&mut conn, &encode(&s, &declare_gnode_component()).unwrap()).unwrap();
    let (p, d) = declare_gnode_placement("aesir-solutions", "0.1.4");
    let r = c.place(&mut conn, &p, &encode(&s, &d).unwrap()).unwrap();
    assert!(r.ok && !r.upd && r.sh == "4.1");

    let pass = c.derive_health(&mut conn, 1_000, 75, "aesir-solutions").unwrap();
    assert_eq!((pass.fresh, pass.placements), (0, 1), "no heartbeat yet: dead");
    let gnode = c.query(&mut conn, "2", &Filters::default(), 0).unwrap();
    assert_eq!(gnode[0]["pd"][17], 0.25, "a named heartbeat that is missing reads dead, not unknown");
    let _: () = redis::cmd("SET").arg(c.heartbeat_key("all", "gnode-daemon", "aesir-solutions"))
        .arg(r#"{"ts":990,"pid":1,"comp":"gnode-daemon","node":"aesir-solutions"}"#).query(&mut conn).unwrap();
    let pass = c.derive_health(&mut conn, 1_000, 75, "aesir-solutions").unwrap();
    assert_eq!((pass.fresh, pass.placements, pass.entities), (1, 1, 2), "placement fresh; gNode and the node carry health");
    let nodes = c.query(&mut conn, "1", &Filters::default(), 0).unwrap();
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0]["pd"][17], 1.0, "node health derived");
    assert_eq!(nodes[0]["c"]["node_role"][0], "master");
    let pass = c.derive_health(&mut conn, 1_100, 75, "aesir-solutions").unwrap();
    assert_eq!(pass.fresh, 0, "75 s later the heartbeat is stale");
    assert_eq!(c.verify(&mut conn).unwrap(), "OK");
}

#[test]
#[ignore]
fn a_service_is_registered_placed_queried_routed_and_reached() {
    let Some((mut conn, c, s)) = connect() else { return };
    for n in ["aesir-solutions", "squad"] {
        c.register(&mut conn, &encode(&s, &declare_node(n, "full_node", "general")).unwrap()).unwrap();
    }
    let gflow = Declared::new("gflow", "3.3", "production")
        .words("protocol", &["gnode_stream"]).words("domain", &["workflow"]).word("implementation_language", "rust")
        .word("contract_version", "v2").word("latency_class", "interactive").fraction("fundamentality", 0.3)
        .meta(serde_json::json!({ "manifest": "gFlow/geodeploy.yaml" }));
    let site = Declared::new("nierto_com", "3.1", "production").words("protocol", &["http_rest"]).words("domain", &["presentation"]);
    c.register(&mut conn, &encode(&s, &gflow).unwrap()).unwrap();
    c.register(&mut conn, &encode(&s, &site).unwrap()).unwrap();
    for (node, w) in [("aesir-solutions", 2u32), ("squad", 1)] {
        let p = Placement { of: "gflow".into(), on: node.into(), env: "production".into(), heartbeat: "gflow".into(), heartbeat_env: None, weight: w };
        let d = Declared::new(&p.id(), "4.2", "production").fraction("installed_version", 0.000104).fraction("capacity_weight", w as f64 / 10.0);
        c.place(&mut conn, &p, &encode(&s, &d).unwrap()).unwrap();
    }

    let rust_daemons = c.query(&mut conn, "3.3", &Filters::default().facet("implementation_language", &["rust"]).range("contract_version", 0.2, 0.5), 0).unwrap();
    assert_eq!(rust_daemons.len(), 1);
    assert_eq!(rust_daemons[0]["id"], "gflow");
    assert_eq!(rust_daemons[0]["c"]["domain"][0], "workflow", "colour rides in the record");
    assert_eq!(rust_daemons[0]["m"]["manifest"], "gFlow/geodeploy.yaml");
    let services = c.query(&mut conn, "3", &Filters::default(), 0).unwrap();
    assert_eq!(services.len(), 2, "one interval covers every sub-layer");

    let mut aesir = 0;
    for _ in 0..30 {
        let r = c.route(&mut conn, "gflow").unwrap();
        assert!(!r.standby);
        if r.node == "aesir-solutions" { aesir += 1 }
    }
    assert_eq!(aesir, 20, "2:1 by declared weight");

    assert!(c.can_reach(&mut conn, "nierto_com", "gflow", 1_000).unwrap().allowed, "a site reaches a daemon by potential");
    assert_eq!(c.can_reach(&mut conn, "gflow", "nierto_com", 1_000).unwrap().reason, "layer");
    c.label(&mut conn, "gflow", &[], &["workflow".into()], &[]).unwrap();
    assert_eq!(c.can_reach(&mut conn, "nierto_com", "gflow", 1_000).unwrap().reason, "label:workflow");
    c.label(&mut conn, "nierto_com", &["workflow".into()], &[], &[]).unwrap();
    assert!(c.can_reach(&mut conn, "nierto_com", "gflow", 1_000).unwrap().allowed);
    c.deny(&mut conn, "nierto_com", "gflow", 1_000, 50).unwrap();
    assert_eq!(c.can_reach(&mut conn, "nierto_com", "gflow", 1_010).unwrap().reason, "deny");
    assert!(c.can_reach(&mut conn, "nierto_com", "gflow", 1_051).unwrap().allowed);

    c.depart(&mut conn, "gflow@squad", "c1", 5_000).unwrap();
    assert_eq!(c.in_flight(&mut conn, "gflow@squad", 5_001, 30_000).unwrap(), 1);
    assert!(c.arrive(&mut conn, "gflow@squad", "c1").unwrap());
    assert_eq!(c.in_flight(&mut conn, "gflow@squad", 5_002, 30_000).unwrap(), 0);

    c.set_derived(&mut conn, "gflow@squad", &[(17, 0.25)], 6_000, "aesir-solutions").unwrap();
    for _ in 0..10 {
        assert_eq!(c.route(&mut conn, "gflow").unwrap().node, "aesir-solutions", "a dead placement leaves the ring");
    }
    assert_eq!(c.verify(&mut conn).unwrap(), "OK");
    c.deregister(&mut conn, "gflow").unwrap();
    assert!(c.route(&mut conn, "gflow").is_err());
    assert_eq!(c.verify(&mut conn).unwrap(), "OK");
}
