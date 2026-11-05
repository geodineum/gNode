//! A client sees the constellation's generation move on every change, and never otherwise.
//!
//!   valkey-server --port 6398 --save "" --bind 127.0.0.1 --daemonize yes
//!   GNODE_TEST_VALKEY_URL=redis://127.0.0.1:6398 cargo test --test generation_live -- --ignored --test-threads=1

use std::path::Path;
use gnode::constellation::{encode, Constellation, Declared};
use gnode::integration::handlers::one_topology as h;
use gnode::topology_schema::TopologySchema;
use serde_json::json;

fn gen(conn: &mut redis::Connection, c: &Constellation) -> u64 {
    let v: String = redis::cmd("FCALL").arg("GNODE_TOPO_GEN").arg(1).arg(c.key()).query(conn).unwrap();
    v.parse().unwrap()
}

#[test]
#[ignore]
fn the_generation_moves_on_every_change_and_never_otherwise() {
    let Ok(url) = std::env::var("GNODE_TEST_VALKEY_URL") else { return };
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
    assert_eq!(gen(&mut conn, &c), 0, "a new constellation starts at nothing");
    c.register(&mut conn, &encode(&s, &Declared::new("n1", "1", "global").word("node_role", "master")).unwrap()).unwrap();
    let mut was = gen(&mut conn, &c);
    assert_eq!(was, 1, "a node registers");
    let mut moved = |conn: &mut redis::Connection, what: &str, expect: bool| {
        let now = gen(conn, &c);
        assert_eq!(now != was, expect, "{what}: {was} -> {now}");
        if expect { assert!(now > was, "{what} went back"); }
        was = now;
    };
    let tk = c.key();
    assert_eq!(h::register(&mut conn, "s", json!({"id": "svc", "shape": "daemon", "on": "n1"})).status, "ok");
    moved(&mut conn, "a service with a placement", true);
    assert_eq!(h::register(&mut conn, "s", json!({"id": "svc2", "shape": "site", "fundamentality": null})).status, "ok");
    moved(&mut conn, "another", true);
    let _: String = redis::cmd("FCALL").arg("GNODE_TOPO_LABEL").arg(1).arg(&tk).arg("svc").arg(r#"["x"]"#).arg("[]").arg("[]").query(&mut conn).unwrap();
    moved(&mut conn, "labels", true);
    let _: String = redis::cmd("FCALL").arg("GNODE_TOPO_DENY").arg(1).arg(&tk).arg("svc").arg("svc2").arg(1000).arg(60000).query(&mut conn).unwrap();
    moved(&mut conn, "a deny", true);
    let _: i64 = redis::cmd("FCALL").arg("GNODE_TOPO_UNDENY").arg(1).arg(&tk).arg("svc").arg("svc2").query(&mut conn).unwrap();
    moved(&mut conn, "an undeny", true);
    let _: i64 = redis::cmd("FCALL").arg("GNODE_TOPO_UNDENY").arg(1).arg(&tk).arg("svc").arg("svc2").query(&mut conn).unwrap();
    moved(&mut conn, "an undeny of nothing", false);
    // reads and routing counters are not changes
    let _: String = redis::cmd("FCALL").arg("GNODE_TOPO_STATS").arg(1).arg(&tk).query(&mut conn).unwrap();
    let _: String = redis::cmd("FCALL").arg("GNODE_TOPO_VERIFY").arg(1).arg(&tk).query(&mut conn).unwrap();
    let _: redis::Value = redis::cmd("FCALL").arg("GNODE_TOPO_ROUTE").arg(1).arg(&tk).arg("svc").query(&mut conn).unwrap_or(redis::Value::Nil);
    moved(&mut conn, "reads and a route", false);
    // health: moves when a value moves, not at each tick
    let hb = format!("{{geodineum}}:gnode:heartbeat:global:svc:n1");
    let _ = hb;
    let _: redis::Value = redis::cmd("FCALL").arg("GNODE_TOPO_HEALTH_DERIVE").arg(1).arg(&tk).arg(1000).arg(5000).query(&mut conn).unwrap();
    let first = gen(&mut conn, &c);
    let _: redis::Value = redis::cmd("FCALL").arg("GNODE_TOPO_HEALTH_DERIVE").arg(1).arg(&tk).arg(2000).arg(5000).query(&mut conn).unwrap();
    assert_eq!(gen(&mut conn, &c), first, "a health tick that changes nothing moves nothing");
    let mut after = first;
    assert_eq!(h::deregister(&mut conn, "s", json!({"service_id": "svc2"})).status, "ok");
    assert!(gen(&mut conn, &c) > after, "a deregistration");
    after = gen(&mut conn, &c);
    let r: redis::RedisResult<String> = redis::cmd("FCALL").arg("GNODE_TOPO_REGISTER").arg(1).arg(&tk).arg("bad").arg("3.3").arg("{}").arg("{}").arg("{}").query(&mut conn);
    assert!(r.is_err());
    assert_eq!(gen(&mut conn, &c), after, "a refused write moves nothing");
    assert_eq!(c.verify(&mut conn).unwrap(), "OK", "the invariants hold");
}
