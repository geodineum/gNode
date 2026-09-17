//! Registration invariants the live topology failed on 2026-09-17, through the shipped Lua.
//!
//! Needs an empty throwaway ValKey (the test loads the Lua libraries itself):
//!   valkey-server --port 6397 --save "" --daemonize yes
//!   GNODE_TEST_VALKEY_URL=redis://127.0.0.1:6397 cargo test --test registration_live -- --ignored

use gnode::tool_registration::{register_services_for_site, TranslatedService};
use serde_json::{json, Value};

const SNAPSHOT: &str = "{geodineum}:gnode:topology:services";

fn connect() -> Option<redis::Connection> {
    let url = std::env::var("GNODE_TEST_VALKEY_URL").ok()?;
    let mut conn = redis::Client::open(url).unwrap().get_connection().unwrap();
    let keys: i64 = redis::cmd("DBSIZE").query(&mut conn).unwrap();
    assert_eq!(keys, 0, "GNODE_TEST_VALKEY_URL must point at an empty throwaway instance");
    for lib in ["gnode_topo", "gnode_stream"] {
        let code = std::fs::read_to_string(format!("{}/functions/{lib}.lua", env!("CARGO_MANIFEST_DIR"))).unwrap();
        let _: String = redis::cmd("FUNCTION").arg("LOAD").arg("REPLACE").arg(code).query(&mut conn).unwrap();
    }
    Some(conn)
}

fn point(width: usize) -> Value {
    json!({
        "pd": vec![0.5; width],
        "pr": vec!["9223372036854775808"; width],
        "m": {"type": "service"}
    })
}

fn register(conn: &mut redis::Connection, site: &str, id: &str, entity: &Value, width: usize) -> redis::RedisResult<String> {
    redis::cmd("FCALL")
        .arg("GNODE_REGISTER_CAPABILITY_VECTOR").arg(1)
        .arg(format!("{{{site}}}:gnode:services"))
        .arg(id).arg(entity.to_string()).arg("0005").arg(0)
        .arg(SNAPSHOT).arg(29).arg(64).arg(width)
        .query(conn)
}

fn entity(conn: &mut redis::Connection, site: &str, id: &str) -> Value {
    let raw: Option<String> = redis::cmd("HGET").arg(format!("{{{site}}}:gnode:services:entities")).arg(id).query(conn).unwrap();
    raw.map(|r| serde_json::from_str(&r).unwrap()).unwrap_or(Value::Null)
}

fn meta_width(conn: &mut redis::Connection, site: &str) -> Value {
    let raw: String = redis::cmd("HGET").arg(format!("{{{site}}}:gnode:services:meta")).arg("data").query(conn).unwrap();
    serde_json::from_str::<Value>(&raw).unwrap()["dm"].clone()
}

#[test]
#[ignore]
fn registration_invariants() {
    let Some(mut conn) = connect() else { return };

    // Ensure stamps the caller's width, and corrects a topology that recorded another.
    let _: String = redis::cmd("FCALL").arg("GNODE_ENSURE_TOPOLOGY").arg(1).arg("site_a").query(&mut conn).unwrap();
    assert_eq!(meta_width(&mut conn, "site_a"), 30);
    let fixed: String = redis::cmd("FCALL").arg("GNODE_ENSURE_TOPOLOGY").arg(1).arg("tools").arg(16).query(&mut conn).unwrap();
    assert_eq!(serde_json::from_str::<Value>(&fixed).unwrap()["cr"], true);
    assert_eq!(meta_width(&mut conn, "tools"), 16);
    let again: String = redis::cmd("FCALL").arg("GNODE_ENSURE_TOPOLOGY").arg(1).arg("site_a").arg(16).query(&mut conn).unwrap();
    assert_eq!(serde_json::from_str::<Value>(&again).unwrap()["dm_was"], 30);
    assert_eq!(meta_width(&mut conn, "site_a"), 16);
    let _: String = redis::cmd("FCALL").arg("GNODE_ENSURE_TOPOLOGY").arg(1).arg("site_a").arg(30).query(&mut conn).unwrap();

    // A point of the wrong width is refused and nothing is written.
    let refused = register(&mut conn, "site_a", "narrow", &point(23), 30);
    assert!(refused.unwrap_err().to_string().contains("width mismatch"));
    assert_eq!(entity(&mut conn, "site_a", "narrow"), Value::Null);
    let snap: Option<String> = redis::cmd("HGET").arg(SNAPSHOT).arg("narrow").query(&mut conn).unwrap();
    assert!(snap.is_none());

    // A new entity gets an order; an update keeps it.
    register(&mut conn, "site_a", "site_a", &point(30), 30).unwrap();
    let first = entity(&mut conn, "site_a", "site_a")["m"]["ro"].clone();
    assert!(first.is_number());
    register(&mut conn, "site_a", "site_a", &point(30), 30).unwrap();
    assert_eq!(entity(&mut conn, "site_a", "site_a")["m"]["ro"], first);

    // An entity stored before orders existed gains one on its next update.
    let mut legacy = point(30);
    legacy["id"] = json!("legacy");
    legacy["bk"] = json!("0001");
    let _: i64 = redis::cmd("HSET").arg("{site_a}:gnode:services:entities").arg("legacy").arg(legacy.to_string()).query(&mut conn).unwrap();
    register(&mut conn, "site_a", "legacy", &point(30), 30).unwrap();
    let backfilled = entity(&mut conn, "site_a", "legacy");
    assert!(backfilled["m"]["ro"].is_number(), "update must assign a missing order: {backfilled}");
    assert_ne!(backfilled["m"]["ro"], first);
    assert!(backfilled["pd"][29].as_f64().unwrap() < 0.5, "the order axis is rewritten with the new order");

    // register_services_for_site stamps the tier width on the topology it ensures.
    let tool = TranslatedService {
        id: "gNode".into(),
        entity_json: point(16).to_string(),
        bucket_key: "0003".into(),
        z_score: 0,
        ro_index: 15,
        width: 16,
        site: None,
    };
    assert_eq!(register_services_for_site(&mut conn, "ecosystem", &[tool], "").unwrap(), (1, 0));
    assert_eq!(meta_width(&mut conn, "ecosystem"), 16);

    // Deprovisioning a site removes its entities from the composed snapshot, except an
    // entity whose own topology still holds it.
    let _: String = redis::cmd("FCALL").arg("GNODE_ENSURE_TOPOLOGY").arg(1).arg("tenant").arg(30).query(&mut conn).unwrap();
    register(&mut conn, "tenant", "tenant", &point(30), 30).unwrap();
    register(&mut conn, "site_a", "tenant", &point(30), 30).unwrap();
    let _: i64 = redis::cmd("SADD").arg("gnode:sites:registry").arg("site_a").query(&mut conn).unwrap();
    let dry: String = redis::cmd("FCALL").arg("GNODE_DEPROVISION_SERVICE").arg(0).arg("site_a").arg(r#"{"dry_run":true}"#).query(&mut conn).unwrap();
    assert!(dry.contains("(HDEL site_a)"));
    let still: Option<String> = redis::cmd("HGET").arg(SNAPSHOT).arg("site_a").query(&mut conn).unwrap();
    assert!(still.is_some(), "a dry run writes nothing");
    let _: String = redis::cmd("FCALL").arg("GNODE_DEPROVISION_SERVICE").arg(0).arg("site_a").arg("{}").query(&mut conn).unwrap();
    for (id, kept) in [("site_a", false), ("legacy", false), ("tenant", true)] {
        let mirror: Option<String> = redis::cmd("HGET").arg(SNAPSHOT).arg(id).query(&mut conn).unwrap();
        assert_eq!(mirror.is_some(), kept, "snapshot entry for {id}");
    }
}
