//! The one-topology function library on a real ValKey: every refusal in
//! SPEC-ONE-TOPOLOGY §4 fires, and the measured prototypes (T6, T13, T16, P-c,
//! P-e, P-k) hold on the real library.
//!
//!   valkey-server --port 6399 --save "" --daemonize yes
//!   GNODE_TEST_VALKEY_URL=redis://127.0.0.1:6399 cargo test --test topology_live -- --ignored --test-threads=1

use std::collections::HashMap;
use std::path::Path;
use std::time::Instant;

use gnode::topology_schema::{layer_code, layer_interval, TopologySchema};
use redis::Value;

const TK: &str = "{geodineum}:gnode:constellation";
const Q48: f64 = 281_474_976_710_656.0;

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
        let _: String = redis::cmd("FLUSHALL").query(&mut conn).unwrap();
    }
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    for lib in ["functions/gnode_topo.lua", "functions/gnode_topology.lua"] {
        let src = std::fs::read_to_string(root.join(lib)).unwrap();
        let _: String = redis::cmd("FUNCTION").arg("LOAD").arg("REPLACE").arg(src).query(&mut conn).unwrap();
    }
    TopologySchema::load(&root.join("config/topology_schema.yaml")).unwrap().publish(&mut conn, TK).unwrap();
    let _: String = fcall(&mut conn, "GNODE_ENSURE_CONSTELLATION", &[]).unwrap();
    Some(conn)
}

fn fcall<T: redis::FromRedisValue>(conn: &mut redis::Connection, f: &str, args: &[&str]) -> redis::RedisResult<T> {
    let mut c = redis::cmd("FCALL");
    c.arg(f).arg(1).arg(TK);
    for a in args { c.arg(*a); }
    c.query(conn)
}

fn refused(conn: &mut redis::Connection, f: &str, args: &[&str], needle: &str) {
    let r: redis::RedisResult<Value> = fcall(conn, f, args);
    match r {
        Err(e) => assert!(e.to_string().contains(needle), "{f} refused for another reason: {e}"),
        Ok(v) => panic!("{f} {args:?} was accepted: {v:?}"),
    }
}

fn score(v: f64) -> String { format!("{}", (v * Q48).round() as u64) }

/// A 24-wide point with the layer at axis 0 and the given health/lifecycle doubles.
fn record(shape: &str, env: &str, health: f64, lifecycle: f64) -> String {
    let code = layer_code(shape).unwrap();
    let mut pd = vec![0.0f64; 24];
    let mut pr = vec!["0".to_string(); 24];
    pd[0] = code as f64 / Q48;
    pr[0] = ((code as i128) << 16).to_string();
    pd[17] = health; pr[17] = ((health * 2f64.powi(64)) as i128).to_string();
    pd[18] = lifecycle; pr[18] = ((lifecycle * 2f64.powi(64)) as i128).to_string();
    serde_json::json!({ "ly": code, "env": env, "pd": pd, "pr": pr, "m": {} }).to_string()
}

fn register(conn: &mut redis::Connection, id: &str, shape: &str, env: &str, ordered: &str, colours: &str) {
    let r: String = fcall(conn, "GNODE_TOPO_REGISTER", &[id, shape, &record(shape, env, 1.0, 0.25), ordered, colours]).unwrap();
    assert!(r.contains("\"ok\":true"), "{id}: {r}");
}

fn place(conn: &mut redis::Connection, id: &str, shape: &str, of: &str, on: &str, hb: &str, weight: &str) {
    let r: String = fcall(conn, "GNODE_TOPO_PLACE", &[id, shape, of, on, "production", hb, weight, &record(shape, "production", 1.0, 0.25), "{}", "{}"]).unwrap();
    assert!(r.contains("\"ok\":true"), "{id}: {r}");
}

fn seed(conn: &mut redis::Connection) {
    for n in ["aesir-solutions", "squad", "laptop"] {
        register(conn, n, "1", "global", "{}", r#"{"node_role":"full_node","specialization":"general"}"#);
    }
    for c in ["ValKey", "gCore", "gNode"] {
        register(conn, c, "2", "global", &format!(r#"{{"fundamentality":"{}"}}"#, score(if c == "ValKey" { 0.0 } else if c == "gNode" { 0.2 } else { 0.5 })), r#"{"protocol":["library"],"implementation_language":"rust"}"#);
    }
    register(conn, "nierto_com", "3.1", "production", &format!(r#"{{"fundamentality":"{}"}}"#, score(0.7)), r#"{"protocol":["http_rest"],"domain":["presentation"]}"#);
    register(conn, "gflow", "3.3", "production", &format!(r#"{{"fundamentality":"{}","contract_version":"{}"}}"#, score(0.3), score(0.2)), r#"{"protocol":["gnode_stream"],"domain":["workflow"],"implementation_language":"rust"}"#);
    register(conn, "geodine", "3.3", "production", &format!(r#"{{"fundamentality":"{}"}}"#, score(0.5)), r#"{"protocol":["gnode_stream"],"domain":["ml_inference"],"implementation_language":"php"}"#);
}

#[test]
#[ignore]
fn registration_refuses_what_the_spec_says() {
    let Some(mut conn) = connect() else { return };
    refused(&mut conn, "GNODE_TOPO_REGISTER", &["x", "9", &record("3.1", "production", 1.0, 0.25), "{}", "{}"], "not published");
    refused(&mut conn, "GNODE_TOPO_REGISTER", &["x", "3.1", r#"{"ly":1,"pd":[0.0],"pr":["0"]}"#, "{}", "{}"], "width");
    refused(&mut conn, "GNODE_TOPO_REGISTER", &["x", "3.1", &record("1", "production", 1.0, 0.25), "{}", "{}"], "outside shape");
    refused(&mut conn, "GNODE_TOPO_REGISTER", &["x", "3.1", &record("3.1", "production", 1.0, 0.25), r#"{"cpu_tier":"5"}"#, "{}"], "not an axis");
    refused(&mut conn, "GNODE_TOPO_REGISTER", &["x", "3.1", &record("3.1", "production", 1.0, 0.25), "{}", r#"{"protocol":["carrier_pigeon"]}"#], "vocabulary");
    refused(&mut conn, "GNODE_TOPO_REGISTER", &["x", "3.1", &record("3.1", "production", 1.0, 0.25), "{}", r#"{"auth_method":["none","mtls"]}"#], "one value");
    refused(&mut conn, "GNODE_TOPO_REGISTER", &["x", "4.2", &record("4.2", "production", 1.0, 0.25), "{}", "{}"], "GNODE_TOPO_PLACE");
    refused(&mut conn, "GNODE_TOPO_GET_ENTITIES", &["*"], "GNODE_TOPO_QUERY");
    seed(&mut conn);
    refused(&mut conn, "GNODE_TOPO_PLACE", &["p", "4.2", "gflow", "gCore", "production", "gflow", "1", &record("4.2", "production", 1.0, 0.25), "{}", "{}"], "not a node");
    refused(&mut conn, "GNODE_TOPO_PLACE", &["p", "4.2", "gCore", "aesir-solutions", "production", "x", "1", &record("4.2", "production", 1.0, 0.25), "{}", "{}"], "places a service");
    let v: String = fcall(&mut conn, "GNODE_TOPO_VERIFY", &[]).unwrap();
    assert_eq!(v, "OK");
}

#[test]
#[ignore]
fn edges_follow_the_potential() {
    let Some(mut conn) = connect() else { return };
    seed(&mut conn);
    let cases = [
        ("nierto_com", "gCore", true), ("gCore", "aesir-solutions", true), ("gCore", "ValKey", true),
        ("gCore", "nierto_com", false), ("geodine", "gflow", true), ("gflow", "geodine", false),
        ("gflow", "gflow", false), ("ValKey", "gCore", false), ("nierto_com", "geodine", true), ("aesir-solutions", "gCore", false),
    ];
    let mut right = 0;
    for (f, t, want) in cases {
        let r: redis::RedisResult<String> = fcall(&mut conn, "GNODE_TOPO_ADD_EDGE", &[f, t, "{}"]);
        if r.is_ok() == want { right += 1 } else { eprintln!("{f} -> {t}: {r:?}, expected ok={want}") }
    }
    assert_eq!(right, cases.len(), "P-c on the real library");
    let v: String = fcall(&mut conn, "GNODE_TOPO_VERIFY", &[]).unwrap();
    assert_eq!(v, "OK");
}

#[test]
#[ignore]
fn weighted_round_robin_with_standby_and_ring_exit() {
    let Some(mut conn) = connect() else { return };
    seed(&mut conn);
    place(&mut conn, "gflow@aesir-solutions", "4.2", "gflow", "aesir-solutions", "gflow", "3");
    place(&mut conn, "gflow@squad", "4.2", "gflow", "squad", "gflow", "3");
    place(&mut conn, "gflow@laptop", "4.2", "gflow", "laptop", "gflow", "0");
    register(&mut conn, "node-c", "1", "global", "{}", r#"{"node_role":"edge"}"#);
    place(&mut conn, "gflow@node-c", "4.2", "gflow", "node-c", "gflow", "1");
    let mut counts: HashMap<String, u32> = HashMap::new();
    for _ in 0..7000 {
        let r: Vec<String> = fcall(&mut conn, "GNODE_TOPO_ROUTE", &["gflow"]).unwrap();
        *counts.entry(r[0].clone()).or_default() += 1;
    }
    assert_eq!(counts["gflow@aesir-solutions"], 3000);
    assert_eq!(counts["gflow@squad"], 3000);
    assert_eq!(counts["gflow@node-c"], 1000);
    assert!(!counts.contains_key("gflow@laptop"), "standby takes nothing while the ring is served");
    let first: Vec<Vec<String>> = (0..7).map(|_| fcall(&mut conn, "GNODE_TOPO_ROUTE", &["gflow"]).unwrap()).collect();
    assert_eq!(first.iter().filter(|r| r[0] == "gflow@aesir-solutions").count(), 3, "one smooth cycle: {first:?}");
    assert_ne!(first[0][0], first[1][0], "smooth, not a,a,a,b,b,b,c");

    // Health 0 on two placements and lifecycle draining on the third: the ring empties, the standby carries.
    for p in ["gflow@aesir-solutions", "gflow@squad"] {
        let upd = format!(r#"{{"17":{{"pd":0.25,"pr":"{}","s":"{}"}}}}"#, 1i128 << 62, score(0.25));
        let _: String = fcall(&mut conn, "GNODE_TOPO_SET_DERIVED", &[p, &upd, "", "", "1", "", ""]).unwrap();
    }
    let upd = format!(r#"{{"18":{{"pd":0.5,"pr":"{}","s":"{}"}}}}"#, (0.5f64 * 2f64.powi(64)) as i128, score(0.5));
    let _: String = fcall(&mut conn, "GNODE_TOPO_SET_DERIVED", &["gflow@node-c", &upd, "", "", "1", "", ""]).unwrap();
    for _ in 0..20 {
        let r: Vec<String> = fcall(&mut conn, "GNODE_TOPO_ROUTE", &["gflow"]).unwrap();
        assert_eq!(r[0], "gflow@laptop"); assert_eq!(r[2], "standby");
    }
    let upd = format!(r#"{{"17":{{"pd":1.0,"pr":"{}","s":"{}"}}}}"#, 1i128 << 64, score(1.0));
    let _: String = fcall(&mut conn, "GNODE_TOPO_SET_DERIVED", &["gflow@squad", &upd, "", "", "1", "", ""]).unwrap();
    let r: Vec<String> = fcall(&mut conn, "GNODE_TOPO_ROUTE", &["gflow"]).unwrap();
    assert_eq!(r[0], "gflow@squad", "standby released when a weighted placement is healthy again");
    refused(&mut conn, "GNODE_TOPO_SET_DERIVED", &["gflow@squad", r#"{"3":{"pd":0.1,"pr":"1"}}"#, "", "", "1", "", ""], "not a derived axis");
    let v: String = fcall(&mut conn, "GNODE_TOPO_VERIFY", &[]).unwrap();
    assert_eq!(v, "OK");
    let r: String = fcall(&mut conn, "GNODE_TOPO_DEREGISTER", &["gflow"]).unwrap();
    assert!(r.contains("\"placements_removed\":4"), "{r}");
    refused(&mut conn, "GNODE_TOPO_ROUTE", &["gflow"], "NOPLACEMENT");
    let v: String = fcall(&mut conn, "GNODE_TOPO_VERIFY", &[]).unwrap();
    assert_eq!(v, "OK");
}

#[test]
#[ignore]
fn in_flight_is_exact_and_reaps() {
    let Some(mut conn) = connect() else { return };
    seed(&mut conn);
    place(&mut conn, "gflow@aesir-solutions", "4.2", "gflow", "aesir-solutions", "gflow", "1");
    for i in 0..2000 {
        let _: String = fcall(&mut conn, "GNODE_TOPO_DEPART", &["gflow@aesir-solutions", &format!("c{i}"), &i.to_string()]).unwrap();
        if i % 100 != 99 {
            let n: i64 = fcall(&mut conn, "GNODE_TOPO_ARRIVE", &["gflow@aesir-solutions", &format!("c{i}")]).unwrap();
            assert_eq!(n, 1);
        }
    }
    let n: i64 = fcall(&mut conn, "GNODE_TOPO_INFLIGHT", &["gflow@aesir-solutions", "2000", "30000"]).unwrap();
    assert_eq!(n, 20, "20 lost replies in flight");
    let n: i64 = fcall(&mut conn, "GNODE_TOPO_INFLIGHT", &["gflow@aesir-solutions", "40000", "30000"]).unwrap();
    assert_eq!(n, 0, "reaped after the TTL");
}

#[test]
#[ignore]
fn reach_labels_and_deny_fail_closed() {
    let Some(mut conn) = connect() else { return };
    seed(&mut conn);
    register(&mut conn, "staging_site", "3.1", "staging", "{}", "{}");
    let _: String = fcall(&mut conn, "GNODE_TOPO_LABEL", &["nierto_com", r#"["content"]"#, "[]", "[]"]).unwrap();
    let _: String = fcall(&mut conn, "GNODE_TOPO_LABEL", &["gCore", "[]", r#"["content"]"#, "[]"]).unwrap();
    let _: String = fcall(&mut conn, "GNODE_TOPO_LABEL", &["ValKey", "[]", r#"["credentials"]"#, "[]"]).unwrap();
    let _: String = fcall(&mut conn, "GNODE_TOPO_LABEL", &["gflow", "[]", "[]", r#"["geodine"]"#]).unwrap();
    let cases: [(&str, &str, &str); 9] = [
        ("nierto_com", "gCore", "reach"), ("nierto_com", "ValKey", "label:credentials"), ("gCore", "nierto_com", "layer"),
        ("ghost", "gCore", "undeclared"), ("gflow", "geodine", "reach"), ("geodine", "gflow", "reach"), ("gflow", "nierto_com", "layer"),
        ("staging_site", "gflow", "env"), ("staging_site", "gCore", "label:content"),
    ];
    for (c, t, want) in cases {
        let r: Vec<String> = fcall(&mut conn, "GNODE_TOPO_CAN_REACH", &[c, t, "1000"]).unwrap();
        assert_eq!(r[1], want, "{c} -> {t}");
        assert_eq!(r[0] == "1", want == "reach");
    }
    refused(&mut conn, "GNODE_TOPO_DENY", &["nierto_com", "gCore", "1000", "0"], "bounded");
    let _: String = fcall(&mut conn, "GNODE_TOPO_DENY", &["nierto_com", "gCore", "1000", "40"]).unwrap();
    let r: Vec<String> = fcall(&mut conn, "GNODE_TOPO_CAN_REACH", &["nierto_com", "gCore", "1000"]).unwrap();
    assert_eq!(r[1], "deny");
    let r: Vec<String> = fcall(&mut conn, "GNODE_TOPO_CAN_REACH", &["nierto_com", "gCore", "1041"]).unwrap();
    assert_eq!(r[1], "reach", "a deny expires on its TTL");
}

#[test]
#[ignore]
fn query_drives_by_layer_and_verifies_by_index_at_ten_thousand() {
    let Some(mut conn) = connect() else { return };
    seed(&mut conn);
    let protocols = ["http_rest", "grpc", "gnode_stream", "websocket"];
    let langs = ["rust", "php", "lua", "python"];
    let shapes = ["3.1", "3.2", "3.3"];
    for i in 0..10_000u32 {
        let shape = shapes[(i % 3) as usize];
        let ordered = format!(r#"{{"contract_version":"{}"}}"#, score([0.1, 0.2, 0.3, 0.4, 0.5][(i % 5) as usize]));
        let colours = format!(r#"{{"protocol":["{}"],"implementation_language":"{}"}}"#, protocols[(i % 4) as usize], langs[(i % 7 % 4) as usize]);
        register(&mut conn, &format!("svc{i}"), shape, "production", &ordered, &colours);
    }
    let (lo, hi) = layer_interval("3").unwrap();
    let filters = format!(r#"{{"axes":{{"contract_version":["{}","{}"]}},"facets":{{"protocol":["grpc"],"implementation_language":["rust"]}}}}"#, score(0.2), score(0.3));
    let t0 = Instant::now();
    let hits: Vec<String> = fcall(&mut conn, "GNODE_TOPO_QUERY", &[&lo.to_string(), &hi.to_string(), &filters, "{}"]).unwrap();
    let el = t0.elapsed();
    let expected = (0..10_000u32).filter(|i| i % 4 == 1 && i % 7 % 4 == 0 && (1..=2).contains(&(i % 5))).count();
    assert_eq!(hits.len(), expected, "every survivor and nothing else");
    for h in &hits {
        let v: serde_json::Value = serde_json::from_str(h).unwrap();
        assert_eq!(v["c"]["protocol"][0], "grpc");
        assert!(v["sh"].as_str().unwrap().starts_with("3."), "position, colour and record in one call");
    }
    eprintln!("T6 on the real library: {} hits of 10,003 in {:.1} ms", hits.len(), el.as_secs_f64() * 1e3);
    assert!(el.as_millis() < 60, "query took {:?}; the prototype measured 3-27 ms at 10^4", el);
    let (lo, hi) = layer_interval("3.3").unwrap();
    let daemons: Vec<String> = fcall(&mut conn, "GNODE_TOPO_QUERY", &[&lo.to_string(), &hi.to_string(), "{}", r#"{"limit":5}"#]).unwrap();
    assert_eq!(daemons.len(), 5, "limit honoured");
    let order: Vec<String> = fcall(&mut conn, "GNODE_TOPO_ORDER", &["contract_version", "3", "0", "desc"]).unwrap();
    assert_eq!(order.len(), 6, "three ids with scores");
}
