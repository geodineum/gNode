//! `register_service`, `deregister_service` and `geometric_discover` on the one topology,
//! both lanes, on a real ValKey (SPEC-ONE-TOPOLOGY §4, §13.3).
//!
//!   valkey-server --port 6391 --save "" --daemonize yes
//!   GNODE_TEST_VALKEY_URL=redis://127.0.0.1:6391 cargo test --test handlers_one_topology_live -- --ignored --test-threads=1

use std::path::Path;

use gnode::constellation::{encode, Constellation, Declared, Filters};
use gnode::integration::handlers::one_topology as h;
use gnode::topology_schema::TopologySchema;
use serde_json::json;

fn setup() -> Option<(redis::Client, Constellation)> {
    let url = std::env::var("GNODE_TEST_VALKEY_URL").ok()?;
    let client = redis::Client::open(url).unwrap();
    let mut conn = client.get_connection().unwrap();
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
    c.register(&mut conn, &encode(&s, &Declared::new("aesir-solutions", "1", "global").word("node_role", "master")).unwrap()).unwrap();
    Some((client, c))
}

#[test]
#[ignore]
fn the_sync_lane_registers_discovers_and_deregisters_by_name() {
    let Some((client, c)) = setup() else { return };
    let mut conn = client.get_connection().unwrap();
    let r = h::register(&mut conn, "nierto_com", json!({
        "id": "gflow", "shape": "daemon", "environment": "production",
        "axes": { "protocol": ["gnode_stream"], "domain": ["workflow"], "implementation_language": "rust", "contract_version": "v2" },
        "metadata": { "manifest": "gFlow/geodeploy.yaml" }, "on": "aesir-solutions", "capacity_weight": 2, "holds": ["infra"]
    }));
    assert_eq!(r.status, "ok", "{:?}", r.error);
    let v = r.result.unwrap();
    assert_eq!(v["shape"], "3.3");
    assert_eq!(v["placement"], "gflow@aesir-solutions");
    assert_eq!(c.route(&mut conn, "gflow").unwrap().node, "aesir-solutions", "the placement joined the ring");

    let r = h::register(&mut conn, "nierto_com", json!({ "id": "nierto_com", "shape": "site", "axes": { "protocol": ["http_rest"], "domain": ["presentation"] } }));
    assert_eq!(r.status, "ok", "{:?}", r.error);

    let r = h::discover(&mut conn, "nierto_com", json!({ "layer": "daemon", "facets": { "implementation_language": "rust" }, "axes": { "contract_version": ["v2", "v5"] } }));
    assert_eq!(r.status, "ok", "{:?}", r.error);
    let v = r.result.unwrap();
    assert_eq!(v["total_matches"], 1);
    assert_eq!(v["results"][0]["service_id"], "gflow");
    assert_eq!(v["results"][0]["colour"]["domain"][0], "workflow", "colour rides in the answer");
    assert_eq!(v["results"][0]["metadata"]["manifest"], "gFlow/geodeploy.yaml");
    assert_eq!(v["lookup"], "index");
    let r = h::discover(&mut conn, "nierto_com", json!({}));
    assert_eq!(r.result.unwrap()["total_matches"], 2, "default layer 3 covers site and daemon");

    for bad in [
        json!({ "id": "x", "capabilities": { "protocol": 0.5 } }),
        json!({ "id": "x", "shape": "daemon", "axes": { "protocol": ["pigeon"] } }),
        json!({ "id": "x", "shape": "daemon", "axes": { "cpu_tier": "gpu" } }),
        json!({ "id": "", "shape": "daemon" }),
    ] {
        let r = h::register(&mut conn, "nierto_com", bad.clone());
        assert_eq!(r.status, "error", "{bad} was accepted");
    }
    assert_eq!(h::discover(&mut conn, "nierto_com", json!({ "capabilities": { "x": 0.1 } })).status, "error");

    let r = h::deregister(&mut conn, "nierto_com", json!({ "service_id": "gflow" }));
    assert_eq!(r.status, "ok", "{:?}", r.error);
    assert_eq!(r.result.unwrap()["status"], "deregistered");
    assert!(c.route(&mut conn, "gflow").is_err(), "the placement left with the service");
    let r = h::deregister(&mut conn, "nierto_com", json!({ "id": "gflow" }));
    assert_eq!(r.result.unwrap()["status"], "not_found");
    assert_eq!(h::deregister(&mut conn, "nierto_com", json!({ "id": "gNode" })).status, "error");
    assert_eq!(c.verify(&mut conn).unwrap(), "OK");
    assert_eq!(c.query(&mut conn, "3", &Filters::default(), 0).unwrap().len(), 1);
}

#[tokio::test]
#[ignore]
async fn the_async_lane_runs_the_same_plan() {
    let Some((client, c)) = setup() else { return };
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let r = h::register_async(&mut conn, "nierto_com", json!({ "id": "gschedule", "shape": "daemon", "axes": { "protocol": ["gnode_stream"], "domain": ["workflow"] }, "on": "aesir-solutions" })).await;
    assert_eq!(r.status, "ok", "{:?}", r.error);
    let r = h::discover_async(&mut conn, "nierto_com", json!({ "facets": { "domain": "workflow" } })).await;
    assert_eq!(r.result.unwrap()["results"][0]["service_id"], "gschedule");
    let r = h::deregister_async(&mut conn, "nierto_com", json!({ "service_id": "gschedule" })).await;
    assert_eq!(r.result.unwrap()["status"], "deregistered");
    let mut sync = client.get_connection().unwrap();
    assert_eq!(c.verify(&mut sync).unwrap(), "OK");
}
