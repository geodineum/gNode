//! The geometric query path end to end, both lanes, through the shipped handlers and Lua.
//!
//! Needs an empty throwaway ValKey (the test loads the Lua libraries itself):
//!   valkey-server --port 6396 --save "" --daemonize yes
//!   GNODE_TEST_VALKEY_URL=redis://127.0.0.1:6396 cargo test --test query_path_live -- --ignored

use std::sync::{Arc, RwLock};

use gnode::daemon::Command;
use gnode::integration::handlers::types::{CommandResult, TOTAL_DIMENSIONS};
use gnode::integration::handlers::{geometric, service, topology_custom, topology_unified};
use gnode::GeometricTopology;
use serde_json::{json, Value};

const SITE: &str = "qpl";

fn command(name: &str, params: Value) -> Command {
    Command {
        id: format!("qpl-{name}"),
        command: name.into(),
        parameters: params,
        site_id: SITE.into(),
        node_id: "qpl".into(),
        timestamp: 0.0,
    }
}

fn ok(r: CommandResult) -> Value {
    assert!(r.error.is_none(), "unexpected error: {:?}", r.error);
    match r.result.unwrap_or(Value::Null) {
        Value::String(s) => serde_json::from_str(&s).unwrap_or(Value::String(s)),
        v => v,
    }
}

#[tokio::test]
#[ignore]
async fn query_path_end_to_end() {
    let Ok(url) = std::env::var("GNODE_TEST_VALKEY_URL") else { return };
    let client = redis::Client::open(url).unwrap();
    let mut sync_conn = client.get_connection().unwrap();
    let keys: i64 = redis::cmd("DBSIZE").query(&mut sync_conn).unwrap();
    assert_eq!(keys, 0, "GNODE_TEST_VALKEY_URL must point at an empty throwaway instance");
    for lib in ["gnode_topo", "gnode_geometric"] {
        let code = std::fs::read_to_string(format!("{}/functions/{lib}.lua", env!("CARGO_MANIFEST_DIR"))).unwrap();
        let _: String = redis::cmd("FUNCTION").arg("LOAD").arg("REPLACE").arg(code).query(&mut sync_conn).unwrap();
    }
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let topo = Arc::new(RwLock::new(GeometricTopology::new(TOTAL_DIMENSIONS)));

    for (id, caps) in [
        ("mailer", json!({"protocol": 0.1, "domain_primary": 0.9, "environment": 1.0, "network_zone": 1.0})),
        ("cache", json!({"protocol": 0.1, "domain_primary": 0.25})),
    ] {
        let register = command("registerService", json!({"id": id, "capabilities": caps}));
        ok(service::handle_register_service_async(&register, &mut conn, &topo, SITE, false).await);
    }

    // geometric_discover: axes the query does not name do not count; both lanes agree.
    let discover = command("geometric_discover", json!({"capabilities": {"protocol": 0.1, "domain_primary": 0.9}}));
    let from_async = ok(geometric::handle_geometric_discover_async(&discover, &mut conn, &topo, SITE, false).await);
    let from_sync = ok(geometric::handle_geometric_discover(&discover, &mut sync_conn, &topo, SITE, false));
    assert_eq!(from_async["results"], from_sync["results"]);
    assert_eq!(from_async["lookup"], "scan");
    assert_eq!(from_async["total_matches"], 2);
    assert_eq!(from_async["results"][0]["service_id"], "mailer");
    assert_eq!(from_async["results"][0]["distance"], 0.0);

    // geometric_discover_range: every operator on an axis must hold.
    let range = command("geometric_discover_range", json!({"requirements": {"8": {"gt": 0.1, "lt": 0.5}}}));
    let range_async = ok(geometric::handle_geometric_discover_range_async(&range, &mut conn, &topo, SITE, false).await);
    let range_sync = ok(geometric::handle_geometric_discover_range(&range, &mut sync_conn, &topo, SITE, false));
    assert_eq!(range_async["services"], json!(["cache"]));
    assert_eq!(range_async, range_sync);

    // discover_with_endpoints: without gNode-BROKER the reply says why endpoints are missing.
    let with_endpoints = command("discover_with_endpoints", json!({"capabilities": ["protocol"]}));
    let endpoints = ok(service::handle_discover_with_endpoints_async(&with_endpoints, &mut conn, &topo, SITE, false).await);
    assert_eq!(endpoints["count"], 2);
    assert_eq!(endpoints["services"][0]["service_id"], "cache");
    assert!(endpoints["endpoints_error"].is_string());

    // geometric_dimensions: canonical axes, aliases apart.
    let dims = ok(geometric::handle_geometric_dimensions(&command("geometric_dimensions", json!({})), &mut sync_conn, &topo, SITE, false));
    assert_eq!(dims["dimension_index"]["network_zone"], 22);
    assert_eq!(dims["aliases"]["env"], "environment");

    // topo_*: create on the sync lane (Ordered dispatch), link on register, query, guarded delete.
    let tk = format!("{{{SITE}}}:pipeline");
    ok(topology_unified::handle_topo_create(&command("topo_create", json!({"name": "pipeline", "constraint_type": "z_monotonic"})), &mut sync_conn, &topo, SITE, false));
    ok(topology_unified::handle_topo_register_async(&command("topo_register", json!({"topology_key": tk, "entity_id": "db", "z": 0.2})), &mut conn, &topo, SITE, false).await);
    let linked = ok(topology_unified::handle_topo_register(
        &command("topo_register", json!({"topology_key": tk, "entity_id": "api", "z": 0.8, "edges_to": ["db", "ghost"]})),
        &mut sync_conn, &topo, SITE, false,
    ));
    assert_eq!(linked["edges"]["added"], json!(["db"]));
    assert_eq!(linked["edges"]["skipped"][0]["to"], "ghost");

    let valid = ok(topology_unified::handle_topo_validate_edge_async(
        &command("topo_validate_edge", json!({"topology_key": tk, "from_id": "api", "to_id": "db"})),
        &mut conn, &topo, SITE, false,
    ).await);
    assert_eq!(valid["valid"], true);
    ok(topology_unified::handle_topo_add_edge(
        &command("topo_add_edge", json!({"topology_key": tk, "from_id": "api", "to_id": "db"})),
        &mut sync_conn, &topo, SITE, false,
    ));

    let top = ok(topology_unified::handle_topo_z_order(&command("topo_z_order", json!({"topology_key": tk, "limit": 1, "descending": true})), &mut sync_conn, &topo, SITE, false));
    assert_eq!(top["eids"], json!(["api"]));
    let low = ok(topology_unified::handle_topo_z_range(&command("topo_z_range", json!({"topology_key": tk, "z_max": 0.5})), &mut sync_conn, &topo, SITE, false));
    assert_eq!(low["eids"], json!(["db"]));

    let snapshot: Vec<String> = redis::cmd("HKEYS").arg("{geodineum}:gnode:topology:services").query(&mut sync_conn).unwrap();
    assert!(snapshot.contains(&"mailer".to_string()), "services belong in the shared snapshot: {snapshot:?}");
    assert!(!snapshot.contains(&"api".to_string()) && !snapshot.contains(&"db".to_string()), "3-D entities leaked into the shared snapshot: {snapshot:?}");

    let unconfirmed = topology_unified::handle_topo_delete(&command("topo_delete", json!({"topology_key": tk})), &mut sync_conn, &topo, SITE, false);
    assert!(unconfirmed.error.is_some(), "a delete without CONFIRM must be refused");
    ok(topology_unified::handle_topo_delete(&command("topo_delete", json!({"topology_key": tk, "confirm": "CONFIRM"})), &mut sync_conn, &topo, SITE, false));

    // custom_topology_discover: {min, max} is a closed range.
    let document = json!({
        "dimensions": 1,
        "capability_dimensions": {"span": 0},
        "query_types": {"span": "range"},
        "services": {
            "below": {"id": "below", "point": [0.1]},
            "inside": {"id": "inside", "point": [0.5]},
            "above": {"id": "above", "point": [0.9]}
        }
    });
    let _: () = redis::cmd("SET").arg("{qpl}:custom").arg(document.to_string()).query(&mut sync_conn).unwrap();
    let custom = ok(topology_custom::handle_custom_topology_discover_async(
        &command("custom_topology_discover", json!({"topology_key": "{qpl}:custom", "requirements": {"span": {"min": 0.25, "max": 0.75}}})),
        &mut conn, &topo, SITE, false,
    ).await);
    assert_eq!(custom["total_matches"], 1);
    assert_eq!(custom["results"][0]["id"], "inside");
}
