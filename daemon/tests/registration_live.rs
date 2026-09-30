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
    // args[6] = -1: schema 4.0 has no registration_order axis, the order lives
    // in m.ro. args[9] names the sampler-owned axes, preserved across an update.
    redis::cmd("FCALL")
        .arg("GNODE_REGISTER_CAPABILITY_VECTOR").arg(1)
        .arg(format!("{{{site}}}:gnode:services"))
        .arg(id).arg(entity.to_string()).arg("0005").arg(0)
        .arg(SNAPSHOT).arg(-1).arg(64).arg(width).arg("16,17")
        .query(conn)
}

fn set_derived(conn: &mut redis::Connection, site: &str, id: &str, updates: &Value, z: i64) -> redis::RedisResult<String> {
    redis::cmd("FCALL")
        .arg("GNODE_TOPO_SET_DERIVED").arg(1)
        .arg(format!("{{{site}}}:gnode:services"))
        .arg(id).arg(updates.to_string()).arg(z).arg(SNAPSHOT).arg(1790000000).arg(16)
        .query(conn)
}

fn set_derived_as(conn: &mut redis::Connection, site: &str, id: &str, updates: &Value, z: i64, node: &str)
    -> redis::RedisResult<String> {
    redis::cmd("FCALL")
        .arg("GNODE_TOPO_SET_DERIVED").arg(1)
        .arg(format!("{{{site}}}:gnode:services"))
        .arg(id).arg(updates.to_string()).arg(z).arg(SNAPSHOT).arg(1790000000).arg(16).arg(node)
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
    assert_eq!(meta_width(&mut conn, "site_a"), 23);
    let fixed: String = redis::cmd("FCALL").arg("GNODE_ENSURE_TOPOLOGY").arg(1).arg("tools").arg(16).query(&mut conn).unwrap();
    assert_eq!(serde_json::from_str::<Value>(&fixed).unwrap()["cr"], true);
    assert_eq!(meta_width(&mut conn, "tools"), 16);
    let again: String = redis::cmd("FCALL").arg("GNODE_ENSURE_TOPOLOGY").arg(1).arg("site_a").arg(16).query(&mut conn).unwrap();
    assert_eq!(serde_json::from_str::<Value>(&again).unwrap()["dm_was"], 23);
    assert_eq!(meta_width(&mut conn, "site_a"), 16);
    let _: String = redis::cmd("FCALL").arg("GNODE_ENSURE_TOPOLOGY").arg(1).arg("site_a").arg(23).query(&mut conn).unwrap();

    // A point of the wrong width is refused and nothing is written.
    let refused = register(&mut conn, "site_a", "narrow", &point(30), 23);
    assert!(refused.unwrap_err().to_string().contains("width mismatch"));
    assert_eq!(entity(&mut conn, "site_a", "narrow"), Value::Null);
    let snap: Option<String> = redis::cmd("HGET").arg(SNAPSHOT).arg("narrow").query(&mut conn).unwrap();
    assert!(snap.is_none());

    // A new entity gets an order; an update keeps it.
    register(&mut conn, "site_a", "site_a", &point(23), 23).unwrap();
    let first = entity(&mut conn, "site_a", "site_a")["m"]["ro"].clone();
    assert!(first.is_number());
    register(&mut conn, "site_a", "site_a", &point(23), 23).unwrap();
    assert_eq!(entity(&mut conn, "site_a", "site_a")["m"]["ro"], first);

    // An entity stored before orders existed gains one on its next update.
    let mut legacy = point(23);
    legacy["id"] = json!("legacy");
    legacy["bk"] = json!("0001");
    let _: i64 = redis::cmd("HSET").arg("{site_a}:gnode:services:entities").arg("legacy").arg(legacy.to_string()).query(&mut conn).unwrap();
    register(&mut conn, "site_a", "legacy", &point(23), 23).unwrap();
    let backfilled = entity(&mut conn, "site_a", "legacy");
    assert!(backfilled["m"]["ro"].is_number(), "update must assign a missing order: {backfilled}");
    assert_ne!(backfilled["m"]["ro"], first);

    // A measurement is written without moving the entity, and a re-registration
    // keeps it: the discovery scanner re-registers on every manifest change, and
    // the caller's vector carries the schema's unknown code (0.00) for a derived
    // axis, not the last measurement.
    let before = entity(&mut conn, "site_a", "site_a");
    set_derived(&mut conn, "site_a", "site_a", &json!({"16": {"pd": 0.6, "pr": "11068046444225730969"}}), 600_000).unwrap();
    let measured = entity(&mut conn, "site_a", "site_a");
    assert_eq!(measured["pd"][16], json!(0.6));
    assert_eq!(measured["bk"], before["bk"], "a derived write must not rehash the entity");
    assert_eq!(measured["zs"], json!(600_000));
    assert_eq!(measured["m"]["ds"], json!(1790000000i64), "the measurement carries its stamp");
    let voxel: Vec<String> = redis::cmd("SMEMBERS")
        .arg(format!("{{site_a}}:gnode:services:voxel:{}", before["bk"].as_str().unwrap()))
        .query(&mut conn).unwrap();
    assert!(voxel.contains(&"site_a".to_string()), "voxel membership must survive a measurement");

    let kept: Value = serde_json::from_str(&register(&mut conn, "site_a", "site_a", &point(23), 23).unwrap()).unwrap();
    assert_eq!(kept["kept"], json!(2), "both sampler axes preserved");
    let after = entity(&mut conn, "site_a", "site_a");
    assert_eq!(after["pd"][16], json!(0.6), "a re-registration must not reset the measurement");
    assert_eq!(after["zs"], json!(600_000), "nor the ordering computed from it");
    assert_eq!(after["m"]["ds"], json!(1790000000i64));
    assert_eq!(after["pd"][15], json!(0.5), "a declared axis still comes from the caller");

    // Derived axes have one writer. A lease decides which node that is, enforced
    // inside the primitive rather than by convention: `is_master` is derived from the
    // daemon's NAME and is false on every live node, so a guard on it writes nowhere.
    let _: redis::RedisResult<()> = redis::cmd("SET").arg("gnode:cluster:writer").arg("node-a").query(&mut conn);
    let refused = set_derived_as(&mut conn, "site_a", "site_a", &json!({"16": {"pd": 0.4}}), 400_000, "node-b");
    assert!(refused.unwrap_err().to_string().contains("does not hold it"));
    assert_eq!(entity(&mut conn, "site_a", "site_a")["pd"][16], json!(0.6), "a refused write changes nothing");
    set_derived_as(&mut conn, "site_a", "site_a", &json!({"16": {"pd": 0.4}}), 400_000, "node-a").unwrap();
    assert_eq!(entity(&mut conn, "site_a", "site_a")["pd"][16], json!(0.4), "the holder writes");
    let _: redis::RedisResult<()> = redis::cmd("DEL").arg("gnode:cluster:writer").query(&mut conn);
    // No lease means nobody claims ownership, so a single-node estate still works.
    set_derived_as(&mut conn, "site_a", "site_a", &json!({"16": {"pd": 0.6}}), 600_000, "node-b").unwrap();
    assert_eq!(entity(&mut conn, "site_a", "site_a")["pd"][16], json!(0.6));

    // A hashed axis cannot be written as derived: those coordinates ARE the key.
    let hashed = set_derived(&mut conn, "site_a", "site_a", &json!({"5": {"pd": 0.9}}), 0);
    assert!(hashed.unwrap_err().to_string().contains("hashed into the bucket key"));
    assert_eq!(entity(&mut conn, "site_a", "site_a")["pd"][5], json!(0.5));
    assert!(set_derived(&mut conn, "site_a", "absent", &json!({"16": {"pd": 0.2}}), 0)
        .unwrap_err().to_string().contains("not found"));

    // register_services_for_site stamps the tier width on the topology it ensures.
    let tool = TranslatedService {
        id: "gNode".into(),
        entity_json: point(16).to_string(),
        bucket_key: "0003".into(),
        z_score: 0,
        ro_index: 15,
        width: 16,
        sampler_axes: String::new(),
        site: None,
    };
    assert_eq!(register_services_for_site(&mut conn, "ecosystem", &[tool], "").unwrap(), (1, 0));
    assert_eq!(meta_width(&mut conn, "ecosystem"), 16);

    // Deprovisioning a site removes its entities from the composed snapshot, except an
    // entity whose own topology still holds it.
    let _: String = redis::cmd("FCALL").arg("GNODE_ENSURE_TOPOLOGY").arg(1).arg("tenant").arg(23).query(&mut conn).unwrap();
    register(&mut conn, "tenant", "tenant", &point(23), 23).unwrap();
    register(&mut conn, "site_a", "tenant", &point(23), 23).unwrap();
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
