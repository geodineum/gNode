//! The one-topology schema published by the daemon is what `GNODE_SCHEMA_GET`
//! hands a client for a shape key, on a real ValKey with the real library.
//!
//!   valkey-server --port 6398 --save "" --daemonize yes
//!   GNODE_TEST_VALKEY_URL=redis://127.0.0.1:6398 cargo test --test schema_live -- --ignored --test-threads=1

use std::collections::HashMap;
use std::path::Path;

use gnode::topology_schema::{layer_code, TopologySchema};

fn connect() -> Option<redis::Connection> {
    let url = std::env::var("GNODE_TEST_VALKEY_URL").ok()?;
    let mut conn = redis::Client::open(url).unwrap().get_connection().unwrap();
    let keys: i64 = redis::cmd("DBSIZE").query(&mut conn).unwrap();
    assert_eq!(keys, 0, "GNODE_TEST_VALKEY_URL must point at an empty throwaway instance");
    Some(conn)
}

#[test]
#[ignore]
fn published_shape_map_is_what_the_lua_getter_serves() {
    let Some(mut conn) = connect() else { return };
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let schema = TopologySchema::load(&root.join("config/topology_schema.yaml")).unwrap();
    let lua = std::fs::read_to_string(root.join("functions/gnode_topology.lua")).unwrap();
    let _: String = redis::cmd("FUNCTION").arg("LOAD").arg("REPLACE").arg(lua).query(&mut conn).unwrap();

    schema.publish(&mut conn, "{geodineum}:gnode:constellation").unwrap();

    for (shape, name, slot) in [("4.2", "capacity_weight", 3usize), ("4.1", "build", 3), ("1", "node_role", 2), ("3.3", "fundamentality", 14), ("2", "fundamentality", 13)] {
        let flat: HashMap<String, String> = redis::cmd("FCALL_RO").arg("GNODE_SCHEMA_GET").arg(0).arg("geodineum").arg(shape).query(&mut conn).unwrap();
        let index: HashMap<String, usize> = serde_json::from_str(&flat["dimension_index"]).unwrap();
        assert_eq!(index.get(name).copied(), Some(slot), "{shape}: {name}");
        assert_eq!(flat["layer_code"].parse::<u64>().unwrap(), layer_code(shape).unwrap(), "{shape}: layer code");
        assert_eq!(flat["width"], "24");
        let slots: HashMap<String, serde_json::Value> = serde_json::from_str(&flat["slots"]).unwrap();
        assert_eq!(slots.contains_key("16"), matches!(shape, "1" | "3.3" | "4.2"), "{shape}: load is a position, not an axis everywhere");
    }
    let legacy: redis::RedisResult<HashMap<String, String>> = redis::cmd("FCALL_RO").arg("GNODE_SCHEMA_GET").arg(0).arg("geodineum").arg("service").query(&mut conn);
    assert!(legacy.is_err(), "the tier name still reads the legacy key, unpublished here");
}
