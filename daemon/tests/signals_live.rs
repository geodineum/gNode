//! Scale-out and sickness through gNode-SIGNALS on a real ValKey: absent library, a start
//! and a clear per episode, nothing per tick, continuity across a restart, a library that
//! disappears, and a joined node's login.
//!
//!   valkey-server --port 6394 --save "" --appendonly no --daemonize yes
//!   GNODE_TEST_VALKEY_URL=redis://127.0.0.1:6394 GNODE_EXT_SIGNALS_PATH=~/gh/pro/gNode/gNode-SIGNALS \
//!     cargo test --test signals_live -- --ignored --test-threads=1

use std::collections::BTreeMap;
use std::path::Path;

use gnode::constellation::{encode, Constellation, Declared, Filters, Placement};
use gnode::constellation_worker::sick_placements;
use gnode::integration::lease::hold_lease;
use gnode::integration::sampler::{Observation, Sampler, SamplerConfig, Tier};
use gnode::integration::signals::{open_key, stream_key, Kind, SignalBus};
use gnode::topology_schema::TopologySchema;
use gnode::worker::LoadSamplerWorker;

fn url() -> Option<String> {
    std::env::var("GNODE_TEST_VALKEY_URL").ok()
}

fn signals_lua() -> String {
    let dir = std::env::var("GNODE_EXT_SIGNALS_PATH").expect("GNODE_EXT_SIGNALS_PATH names the gNode-SIGNALS checkout");
    std::fs::read_to_string(Path::new(&dir).join("functions/gnode_signal.lua")).unwrap()
}

fn setup(url: &str) -> (redis::Connection, Constellation, TopologySchema) {
    let mut conn = redis::Client::open(url).unwrap().get_connection().unwrap();
    let _: String = redis::cmd("FLUSHALL").query(&mut conn).unwrap();
    let _: String = redis::cmd("FUNCTION").arg("FLUSH").query(&mut conn).unwrap();
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    for lib in ["functions/gnode_topo.lua", "functions/gnode_topology.lua"] {
        let src = std::fs::read_to_string(root.join(lib)).unwrap();
        let _: String = redis::cmd("FUNCTION").arg("LOAD").arg("REPLACE").arg(src).query(&mut conn).unwrap();
    }
    let s = TopologySchema::load(&root.join("config/topology_schema.yaml")).unwrap();
    let c = Constellation::new("geodineum");
    c.publish_schema(&mut conn, &s).unwrap();
    c.ensure(&mut conn).unwrap();
    for n in ["aesir-solutions", "squad"] {
        c.register(&mut conn, &encode(&s, &Declared::new(n, "1", "global").word("node_role", "full_node")).unwrap()).unwrap();
    }
    c.register(&mut conn, &encode(&s, &Declared::new("gflow", "3.3", "production").words("protocol", &["gnode_stream"])).unwrap()).unwrap();
    assert!(hold_lease(&mut conn, &c.lease_key(), "aesir-solutions"));
    (conn, c, s)
}

fn load_signals(conn: &mut redis::Connection) {
    let _: String = redis::cmd("FUNCTION").arg("LOAD").arg("REPLACE").arg(signals_lua()).query(conn).unwrap();
}

fn entries(conn: &mut redis::Connection) -> Vec<serde_json::Value> {
    let r: redis::streams::StreamRangeReply = redis::cmd("XRANGE").arg(stream_key("geodineum")).arg("-").arg("+").query(conn).unwrap();
    r.ids.iter().map(|e| serde_json::from_str(&e.get::<String>("payload").unwrap()).unwrap()).collect()
}

fn open(conn: &mut redis::Connection) -> BTreeMap<String, String> {
    redis::cmd("HGETALL").arg(open_key("geodineum")).query(conn).unwrap()
}

/// A sampler under the live rule (θ 3, 3 ticks), fed the node `squad` at 1 ms then at `ms`.
struct Pressure {
    sampler: Sampler,
    t: u64,
}

impl Pressure {
    fn new() -> Self {
        let mut sampler = Sampler::new(SamplerConfig { min_samples: 2, ..SamplerConfig::default() });
        for i in 0..8 {
            sampler.observe(obs(1, 1_000 + i));
        }
        let _ = sampler.tick(2_000);
        Self { sampler, t: 0 }
    }

    fn tick(&mut self, ms: u64) {
        self.t += 1;
        let now = 100_000 * self.t;
        for i in 0..8 {
            self.sampler.observe(obs(ms, now - 10 + i));
        }
        let _ = self.sampler.tick(now);
    }
}

fn obs(ms: u64, ts: u64) -> Observation {
    Observation { tier: Tier::Node, site: "geodineum".into(), entity: "squad".into(), command: "ping".into(), elapsed_ms: ms, ok: true, ts_ms: ts }
}

#[test]
#[ignore]
fn scale_out_is_one_start_and_one_clear_per_episode_and_nothing_without_the_library() {
    let Some(url) = url() else { return };
    let (mut conn, c, _) = setup(&url);
    let mut p = Pressure::new();
    let mut bus = SignalBus::new(Kind::ScaleOut);

    // Without gNode-SIGNALS: the suggestion is there, nothing is published or recorded.
    for _ in 0..4 {
        p.tick(4);
    }
    let pressed = LoadSamplerWorker::scale_out_conditions(&mut conn, &c, p.sampler.suggestions());
    assert_eq!(pressed.keys().collect::<Vec<_>>(), vec!["squad"]);
    assert!(bus.evaluate(&mut conn, "geodineum", &pressed, 1_000, "aesir-solutions").is_empty());
    let exists: i64 = redis::cmd("EXISTS").arg(stream_key("geodineum")).arg(open_key("geodineum")).query(&mut conn).unwrap();
    assert_eq!(exists, 0, "no stream and no open record without the library");

    // Loaded: a new process learns of it at once; the held suggestion starts exactly once.
    load_signals(&mut conn);
    let mut bus = SignalBus::new(Kind::ScaleOut);
    let mut published = 0;
    for k in 0..6u64 {
        p.tick(4);
        let pressed = LoadSamplerWorker::scale_out_conditions(&mut conn, &c, p.sampler.suggestions());
        published += bus.evaluate(&mut conn, "geodineum", &pressed, 2_000 + 15 * k, "aesir-solutions").len();
    }
    assert_eq!(published, 1, "six ticks under pressure, one signal");
    let e = entries(&mut conn);
    assert_eq!(e.len(), 1);
    assert_eq!((e[0]["kind"].as_str(), e[0]["state"].as_str(), e[0]["subject"].as_str()), (Some("scale_out"), Some("start"), Some("squad")));
    assert_eq!(e[0]["since"], 2_000);
    assert_eq!(e[0]["detail"]["tier"], "node");
    assert!(e[0]["detail"]["inflation"].as_f64().unwrap() >= 3.0);
    assert!(open(&mut conn).contains_key("scale_out|squad"));

    // A restart (a new bus, the same open record) does not announce it again.
    let mut bus = SignalBus::new(Kind::ScaleOut);
    p.tick(4);
    let pressed = LoadSamplerWorker::scale_out_conditions(&mut conn, &c, p.sampler.suggestions());
    assert!(bus.evaluate(&mut conn, "geodineum", &pressed, 2_200, "squad").is_empty());

    // Relief: the suggestion goes at the first quiet tick, the clear after three.
    let mut cleared_at = None;
    for k in 0..5u64 {
        p.tick(1);
        let pressed = LoadSamplerWorker::scale_out_conditions(&mut conn, &c, p.sampler.suggestions());
        if !bus.evaluate(&mut conn, "geodineum", &pressed, 3_000 + 15 * k, "aesir-solutions").is_empty() {
            assert!(cleared_at.is_none(), "cleared twice");
            cleared_at = Some(k);
        }
    }
    assert_eq!(cleared_at, Some(2), "the third quiet evaluation clears");
    let e = entries(&mut conn);
    assert_eq!(e.len(), 2);
    assert_eq!((e[1]["state"].as_str(), e[1]["since"].as_u64(), e[1]["at"].as_u64()), (Some("clear"), Some(2_000), Some(3_030)));
    assert!(!open(&mut conn).contains_key("scale_out|squad"));

    // A second episode is a new start, not a dedup of the first.
    for k in 0..3u64 {
        p.tick(4);
        let pressed = LoadSamplerWorker::scale_out_conditions(&mut conn, &c, p.sampler.suggestions());
        bus.evaluate(&mut conn, "geodineum", &pressed, 4_000 + 15 * k, "aesir-solutions");
    }
    assert_eq!(entries(&mut conn).len(), 3);
    let published: i64 = redis::cmd("HGET").arg("{geodineum}:signals:global:metrics:published").arg("total").query(&mut conn).unwrap();
    assert_eq!(published, 3);

    // The library goes away under a bus that knew it: the change is held, not lost, and no error.
    let _: String = redis::cmd("FUNCTION").arg("DELETE").arg("gnode_signal").query(&mut conn).unwrap();
    let mut gone = 0;
    for k in 0..4u64 {
        p.tick(1);
        let pressed = LoadSamplerWorker::scale_out_conditions(&mut conn, &c, p.sampler.suggestions());
        gone += bus.evaluate(&mut conn, "geodineum", &pressed, 5_000 + 15 * k, "aesir-solutions").len();
    }
    assert_eq!(gone, 0);
    assert!(open(&mut conn).contains_key("scale_out|squad"), "unpublished, so still open");
}

#[test]
#[ignore]
fn a_placement_whose_heartbeat_stops_is_sick_once_and_recovers_once() {
    let Some(url) = url() else { return };
    let (mut conn, c, s) = setup(&url);
    load_signals(&mut conn);
    let p = Placement { of: "gflow".into(), on: "squad".into(), env: "production".into(), heartbeat: "gflow".into(), heartbeat_env: None, weight: 1 };
    c.place(&mut conn, &p, &encode(&s, &Declared::new(&p.id(), "4.2", "production")).unwrap()).unwrap();
    let hb = c.heartbeat_key("production", "gflow", "squad");
    let mut bus = SignalBus::new(Kind::Sick);
    let pass = |conn: &mut redis::Connection, bus: &mut SignalBus, now: u64| -> Vec<String> {
        c.derive_health(conn, now, 75, "aesir-solutions").unwrap();
        let placements = c.query(conn, "4", &Filters::default(), 0).unwrap();
        bus.evaluate(conn, "geodineum", &sick_placements(&placements), now, "aesir-solutions")
    };

    let _: () = redis::cmd("SET").arg(&hb).arg(r#"{"ts":1000}"#).query(&mut conn).unwrap();
    assert!(pass(&mut conn, &mut bus, 1_010).is_empty(), "fresh: healthy");
    assert!(pass(&mut conn, &mut bus, 1_100).is_empty(), "first dead pass: not yet");
    assert_eq!(pass(&mut conn, &mut bus, 1_115).len(), 1, "second dead pass: sick");
    for k in 0..5 {
        assert!(pass(&mut conn, &mut bus, 1_130 + 15 * k).is_empty(), "held, silent");
    }
    let _: () = redis::cmd("SET").arg(&hb).arg(r#"{"ts":1200}"#).query(&mut conn).unwrap();
    assert!(pass(&mut conn, &mut bus, 1_205).is_empty(), "one healthy pass is not a recovery");
    assert_eq!(pass(&mut conn, &mut bus, 1_210).len(), 1, "two: recovered");
    let e = entries(&mut conn);
    assert_eq!(e.len(), 2);
    assert_eq!((e[0]["kind"].as_str(), e[0]["state"].as_str(), e[0]["subject"].as_str()), (Some("sick"), Some("start"), Some("gflow@squad")));
    assert_eq!((e[0]["detail"]["of"].as_str(), e[0]["detail"]["on"].as_str()), (Some("gflow"), Some("squad")));
    assert_eq!((e[1]["state"].as_str(), e[1]["since"].as_u64(), e[1]["at"].as_u64()), (Some("clear"), Some(1_115), Some(1_210)));
    assert!(open(&mut conn).is_empty());
    assert_eq!(c.verify(&mut conn).unwrap(), "OK");
}

#[test]
#[ignore]
fn a_joined_nodes_login_lists_and_publishes_but_may_not_load() {
    let Some(url) = url() else { return };
    let (mut admin, _c, _) = setup(&url);
    load_signals(&mut admin);
    let _: String = redis::cmd("ACL").arg("SETUSER").arg("signals_node_test").arg("reset").arg("on").arg(">node-test-pw")
        .arg("~*").arg("&*").arg("+@all").arg("-@dangerous")
        .arg("-function|load").arg("-function|delete").arg("-function|flush").arg("-function|restore")
        .query(&mut admin).unwrap();
    let node_url = url.replacen("redis://", "redis://signals_node_test:node-test-pw@", 1);
    let mut node = redis::Client::open(node_url).unwrap().get_connection().unwrap();
    let denied: redis::RedisResult<String> = redis::cmd("FUNCTION").arg("LOAD").arg("REPLACE").arg(signals_lua()).query(&mut node);
    assert!(denied.is_err(), "the node tier may not load libraries");
    let mut bus = SignalBus::new(Kind::ScaleOut);
    let mut now = BTreeMap::new();
    now.insert("squad".to_string(), serde_json::json!({ "inflation": 3.4 }));
    assert_eq!(bus.evaluate(&mut node, "geodineum", &now, 9_000, "squad").len(), 1);
    assert_eq!(entries(&mut admin).len(), 1);
    let _: i64 = redis::cmd("ACL").arg("DELUSER").arg("signals_node_test").query(&mut admin).unwrap();

    // A login that may not write the signal keys: the publish fails, nothing is recorded as
    // published, and the transition stays pending instead of being lost.
    let _: String = redis::cmd("ACL").arg("SETUSER").arg("signals_narrow_test").arg("reset").arg("on").arg(">narrow-test-pw")
        .arg("~{geodineum}:gnode:*").arg("+@all").arg("-@dangerous").query(&mut admin).unwrap();
    let narrow_url = url.replacen("redis://", "redis://signals_narrow_test:narrow-test-pw@", 1);
    let mut narrow = redis::Client::open(narrow_url).unwrap().get_connection().unwrap();
    let mut bus = SignalBus::new(Kind::Sick);
    let mut now = BTreeMap::new();
    now.insert("gflow@squad".to_string(), serde_json::json!({}));
    for k in 0..4u64 {
        assert!(bus.evaluate(&mut narrow, "geodineum", &now, 9_100 + 15 * k, "squad").is_empty());
    }
    assert_eq!(entries(&mut admin).len(), 1);
    assert!(!open(&mut admin).contains_key("sick|gflow@squad"));
    let _: i64 = redis::cmd("ACL").arg("DELUSER").arg("signals_narrow_test").query(&mut admin).unwrap();
}
