//! The load sampler's wire, end to end, on a real ValKey.
//!
//! Covers what unit tests cannot: the observation's shape on the stream, the
//! sampler's OWN consumer group seeing every record regardless of other readers,
//! and the derived write landing in the entity without moving it.
//!
//!   valkey-server --port 6397 --save "" --daemonize yes
//!   GNODE_TEST_VALKEY_URL=redis://127.0.0.1:6397 cargo test --test sampler_live -- --ignored

use gnode::integration::sampler::{landmark, Observation, Sampler, SamplerConfig};
use redis::streams::StreamReadReply;
use serde_json::{json, Value};

const SNAPSHOT: &str = "{geodineum}:gnode:topology:services";
const SITE: &str = "st";
const GROUP: &str = "gnode-sampler";

/// The emptiness check runs ONCE per binary: it is what proves the target is a
/// throwaway, and it must not be weakened. Having proved it, this binary may reuse
/// the instance for its own later tests by clearing what it wrote — otherwise the
/// second test would see the first's keys and refuse.
static PROVEN_THROWAWAY: std::sync::Once = std::sync::Once::new();

fn connect() -> Option<redis::Connection> {
    let url = std::env::var("GNODE_TEST_VALKEY_URL").ok()?;
    let mut conn = redis::Client::open(url).unwrap().get_connection().unwrap();
    let mut first = false;
    PROVEN_THROWAWAY.call_once(|| {
        let keys: i64 = redis::cmd("DBSIZE").query(&mut conn).unwrap();
        assert_eq!(keys, 0, "GNODE_TEST_VALKEY_URL must point at an empty throwaway instance");
        first = true;
    });
    if !first {
        let _: redis::RedisResult<String> = redis::cmd("FLUSHALL").query(&mut conn);
    }
    for lib in ["gnode_utils", "gnode_topo"] {
        let code = std::fs::read_to_string(format!("{}/functions/{lib}.lua", env!("CARGO_MANIFEST_DIR"))).unwrap();
        let _: String = redis::cmd("FUNCTION").arg("LOAD").arg("REPLACE").arg(code).query(&mut conn).unwrap();
    }
    Some(conn)
}

fn health_key() -> String { format!("{{{}}}:gnode:health", SITE) }
fn topo_key() -> String { format!("{{{}}}:gnode:services", SITE) }

/// What relay telemetry publishes on flush.
fn publish(conn: &mut redis::Connection, entity: &str, command: &str, lat_ms: u64, ts: i64) {
    let _: String = redis::cmd("XADD").arg(health_key()).arg("MAXLEN").arg("~").arg(1000).arg("*")
        .arg("t").arg("rq").arg("si").arg(entity).arg("cmd").arg(command)
        .arg("lat").arg(lat_ms).arg("ok").arg(1).arg("ts").arg(ts)
        .query(conn).unwrap();
}

/// What the worker does: its own group, every record, acked.
fn drain(conn: &mut redis::Connection, sampler: &mut Sampler) -> usize {
    let _: redis::RedisResult<String> = redis::cmd("XGROUP").arg("CREATE").arg(health_key())
        .arg(GROUP).arg("0").arg("MKSTREAM").query(conn);
    let reply: StreamReadReply = redis::cmd("XREADGROUP").arg("GROUP").arg(GROUP).arg("tester")
        .arg("COUNT").arg(500).arg("STREAMS").arg(health_key()).arg(">")
        .query(conn).unwrap();
    let mut taken = 0;
    for key in reply.keys {
        for entry in key.ids {
            let _: redis::RedisResult<i64> = redis::cmd("XACK")
                .arg(health_key()).arg(GROUP).arg(&entry.id).query(conn);
            if entry.get::<String>("t").as_deref() != Some("rq") { continue }
            sampler.observe(Observation {
                site: SITE.into(),
                entity: entry.get::<String>("si").unwrap(),
                command: entry.get::<String>("cmd").unwrap_or_else(|| "unknown".into()),
                elapsed_ms: entry.get::<String>("lat").unwrap().parse().unwrap(),
                ok: true,
                ts_ms: entry.get::<String>("ts").unwrap().parse().unwrap(),
            });
            taken += 1;
        }
    }
    taken
}

fn entity(conn: &mut redis::Connection, id: &str) -> Value {
    let raw: Option<String> = redis::cmd("HGET").arg(format!("{}:entities", topo_key())).arg(id).query(conn).unwrap();
    raw.map(|r| serde_json::from_str(&r).unwrap()).unwrap_or(Value::Null)
}

#[test]
#[ignore]
fn observations_become_a_coordinate() {
    let Some(mut conn) = connect() else { return };

    // A registered provider, at the schema's unknown code for load.
    let mut pd = vec![0.5; 23];
    pd[16] = 0.0;
    let ent = json!({"pd": pd, "pr": vec!["9223372036854775808"; 23], "m": {"type": "service"}});
    let bk: String = (0..16).map(|_| "0005".to_string()).collect();
    let _: String = redis::cmd("FCALL").arg("GNODE_ENSURE_TOPOLOGY").arg(1).arg(SITE).arg(23).query(&mut conn).unwrap();
    let _: String = redis::cmd("FCALL").arg("GNODE_REGISTER_CAPABILITY_VECTOR").arg(1).arg(topo_key())
        .arg("svc").arg(ent.to_string()).arg(&bk).arg(0).arg(SNAPSHOT).arg(-1).arg(64).arg(23).arg("16,17")
        .query(&mut conn).unwrap();
    // Compared as a number, not as JSON: cjson writes 0.0 as `0`.
    assert_eq!(entity(&mut conn, "svc")["pd"][16].as_f64(), Some(0.0), "unknown until measured");

    let mut sampler = Sampler::new(SamplerConfig::default());
    let t0: i64 = 1_800_000_000_000;

    // A first window teaches the baseline and says nothing about load.
    for _ in 0..10 { publish(&mut conn, "svc", "get", 100, t0); }
    assert_eq!(drain(&mut conn, &mut sampler), 10, "the sampler's own group sees every record");
    assert!(sampler.tick(t0 as u64).is_empty(), "bootstrapping is not a load reading");

    // Four times its own baseline, and the provider is heavy — measured past the
    // 60s window, so the median describes the present rather than mixing in the
    // baseline's own samples. (Mid-ramp it reads as an intermediate band, which is
    // what a median over a window should do.)
    let t1 = t0 + 70_000;
    for _ in 0..10 { publish(&mut conn, "svc", "get", 400, t1); }
    drain(&mut conn, &mut sampler);
    let writes = sampler.tick(t1 as u64);
    assert_eq!(writes.len(), 1);
    let w = &writes[0];
    assert_eq!(landmark(w.pd).1, "heavy", "pd was {}", w.pd);

    // The write lands, stamps itself, and does not move the entity.
    let before = entity(&mut conn, "svc");
    let updates = format!(r#"{{"16":{{"pd":{:.4},"pr":"{}"}}}}"#, w.pd, (w.pd * (2f64).powi(64)) as u128);
    let _: String = redis::cmd("FCALL").arg("GNODE_TOPO_SET_DERIVED").arg(1).arg(topo_key())
        .arg("svc").arg(&updates).arg(w.zs).arg(SNAPSHOT).arg(t1 / 1000).arg(16).arg("node-a")
        .query(&mut conn).unwrap();
    let after = entity(&mut conn, "svc");
    assert!((after["pd"][16].as_f64().unwrap() - w.pd).abs() < 1e-6);
    assert_eq!(after["bk"], before["bk"], "a measurement must not rehash the provider");
    assert_eq!(after["m"]["ds"].as_i64(), Some(t1 / 1000), "the measurement carries its stamp");
    let voxel: Vec<String> = redis::cmd("SMEMBERS")
        .arg(format!("{}:voxel:{}", topo_key(), bk)).query(&mut conn).unwrap();
    assert!(voxel.contains(&"svc".to_string()), "voxel membership survives");

    // And a re-registration keeps the measurement rather than resetting it to unknown.
    let _: String = redis::cmd("FCALL").arg("GNODE_REGISTER_CAPABILITY_VECTOR").arg(1).arg(topo_key())
        .arg("svc").arg(ent.to_string()).arg(&bk).arg(0).arg(SNAPSHOT).arg(-1).arg(64).arg(23).arg("16,17")
        .query(&mut conn).unwrap();
    assert!((entity(&mut conn, "svc")["pd"][16].as_f64().unwrap() - w.pd).abs() < 1e-6,
            "the sampler owns this axis; registration must not overwrite it");
}

#[test]
#[ignore]
fn the_workers_group_does_not_starve_the_sampler() {
    // The reason the sampler reads through its own group: the workers' group splits
    // messages between nodes, so a lease holder reading through it would aggregate
    // a fraction of the traffic and write it as the truth.
    let Some(mut conn) = connect() else { return };
    let t0: i64 = 1_800_000_000_000;
    for i in 0..6 { publish(&mut conn, "svc", "get", 100 + i, t0); }

    let _: redis::RedisResult<String> = redis::cmd("XGROUP").arg("CREATE").arg(health_key())
        .arg("gnode-workers").arg("0").arg("MKSTREAM").query(&mut conn);
    let workers: StreamReadReply = redis::cmd("XREADGROUP").arg("GROUP").arg("gnode-workers").arg("w1")
        .arg("COUNT").arg(100).arg("STREAMS").arg(health_key()).arg(">").query(&mut conn).unwrap();
    let consumed: usize = workers.keys.iter().map(|k| k.ids.len()).sum();
    assert_eq!(consumed, 6, "the workers' group took everything");

    let mut sampler = Sampler::new(SamplerConfig::default());
    assert_eq!(drain(&mut conn, &mut sampler), 6,
               "and the sampler's group still sees all six");
}
