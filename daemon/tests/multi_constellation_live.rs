//! One daemon, two constellations, one store (D2, T10 on the real code): each registration
//! lands in its owner's key, discovery and relay resolution see only their own constellation.
//!
//!   valkey-server --port 6390 --save "" --daemonize yes
//!   GNODE_TEST_VALKEY_URL=redis://127.0.0.1:6390 cargo test --test multi_constellation_live -- --ignored --test-threads=1

use std::path::Path;

use gnode::constellation::{encode, for_entity, served, Constellation, Declared, Filters};
use gnode::integration::handlers::one_topology as h;
use gnode::integration::relay::router::resolve_one_topology;
use gnode::integration::relay::RelayDecision;
use gnode::topology_schema::TopologySchema;
use serde_json::json;

#[test]
#[ignore]
fn two_constellations_on_one_daemon_stay_apart() {
    let Ok(url) = std::env::var("GNODE_TEST_VALKEY_URL") else { return };
    std::env::set_var("GNODE_CONSTELLATIONS", "geodineum, coursematch");
    let client = redis::Client::open(url).unwrap();
    let mut conn = client.get_connection().unwrap();
    let _: String = redis::cmd("FLUSHALL").query(&mut conn).unwrap();
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    for lib in ["functions/gnode_topo.lua", "functions/gnode_topology.lua"] {
        let src = std::fs::read_to_string(root.join(lib)).unwrap();
        let _: String = redis::cmd("FUNCTION").arg("LOAD").arg("REPLACE").arg(src).query(&mut conn).unwrap();
    }
    let s = TopologySchema::load(&root.join("config/topology_schema.yaml")).unwrap();
    let both = served();
    assert_eq!(both.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(), vec!["geodineum", "coursematch"]);
    for c in &both {
        c.publish_schema(&mut conn, &s).unwrap();
        c.ensure(&mut conn).unwrap();
        c.register(&mut conn, &encode(&s, &Declared::new("aesir-solutions", "1", "global").word("node_role", "master")).unwrap()).unwrap();
    }
    let geo = Constellation::new("geodineum");
    let cm = Constellation::new("coursematch");

    // gflow registers from a geodineum site; coursematch's service names its constellation.
    assert_eq!(h::register(&mut conn, "nierto_com", json!({ "id": "gflow", "shape": "daemon", "axes": { "protocol": ["gnode_stream"], "domain": ["workflow"] }, "on": "aesir-solutions" })).status, "ok");
    let r = h::register(&mut conn, "nierto_com", json!({ "id": "coursematch", "shape": "app", "constellation": "coursematch", "axes": { "protocol": ["http_rest"], "domain": ["content"] }, "on": "aesir-solutions" }));
    assert_eq!(r.status, "ok", "{:?}", r.error);
    assert_eq!(r.result.unwrap()["constellation"], "coursematch");
    assert_eq!(h::register(&mut conn, "nierto_com", json!({ "id": "x", "shape": "app", "constellation": "someone_else" })).status, "error", "an unserved constellation is refused");

    assert_eq!(for_entity(&mut conn, "coursematch").name, "coursematch");
    assert_eq!(for_entity(&mut conn, "gflow").name, "geodineum");
    assert_eq!(for_entity(&mut conn, "nobody").name, "geodineum", "the default is the first served");
    assert_eq!(geo.query(&mut conn, "3", &Filters::default(), 0).unwrap().len(), 1);
    assert_eq!(cm.query(&mut conn, "3", &Filters::default(), 0).unwrap().len(), 1);

    // Discovery from coursematch's own stream sees only coursematch.
    let r = h::discover(&mut conn, "coursematch", json!({ "facets": { "protocol": "http_rest" } }));
    let v = r.result.unwrap();
    assert_eq!(v["total_matches"], 1);
    assert_eq!(v["results"][0]["service_id"], "coursematch");
    assert_eq!(v["topology_key"], "{coursematch}:gnode:constellation");
    let r = h::discover(&mut conn, "coursematch", json!({ "facets": { "domain": "workflow" } }));
    assert_eq!(r.result.unwrap()["total_matches"], 0, "gflow is not visible from coursematch");

    // Relay from coursematch cannot resolve geodineum's service.
    assert!(matches!(resolve_one_topology(&mut conn, &cm, "gflow", "coursematch", "{coursematch}:gnode:unified:production"), RelayDecision::NotFound(_)));
    assert!(matches!(resolve_one_topology(&mut conn, &geo, "gflow", "nierto_com", "{nierto_com}:gnode:unified:production"), RelayDecision::Forward { .. }));

    // Deregistration finds the owner by the entity, whatever stream it came from.
    assert_eq!(h::deregister(&mut conn, "nierto_com", json!({ "service_id": "coursematch" })).result.unwrap()["status"], "deregistered");
    assert_eq!(cm.query(&mut conn, "3", &Filters::default(), 0).unwrap().len(), 0);
    assert_eq!(geo.verify(&mut conn).unwrap(), "OK");
    assert_eq!(cm.verify(&mut conn).unwrap(), "OK");
    std::env::remove_var("GNODE_CONSTELLATIONS");
}
