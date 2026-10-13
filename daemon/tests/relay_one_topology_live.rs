//! The relay under the one topology, on a real ValKey (SPEC-ONE-TOPOLOGY §6, §7): an
//! entity target routes to a placement-addressed stream by the ring; a site target is
//! refused; a capability query picks the service by colour; the departure record closes
//! the placement's in-flight entry on arrival.
//!
//!   valkey-server --port 6394 --save "" --daemonize yes
//!   GNODE_TEST_VALKEY_URL=redis://127.0.0.1:6394 cargo test --test relay_one_topology_live -- --ignored --test-threads=1

use std::collections::HashMap;
use std::path::Path;

use gnode::constellation::{encode, Constellation, Declared, Placement};
use gnode::integration::relay::router::resolve_one_topology;
use gnode::integration::relay::{roundtrip, RelayDecision};
use gnode::topology_schema::TopologySchema;

const NS: &str = "geodineum";

fn connect() -> Option<(redis::Connection, Constellation)> {
    let url = std::env::var("GNODE_TEST_VALKEY_URL").ok()?;
    let mut conn = redis::Client::open(url).unwrap().get_connection().unwrap();
    let _: String = redis::cmd("FLUSHALL").query(&mut conn).unwrap();
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    for lib in ["functions/gnode_topo.lua", "functions/gnode_topology.lua"] {
        let src = std::fs::read_to_string(root.join(lib)).unwrap();
        let _: String = redis::cmd("FUNCTION").arg("LOAD").arg("REPLACE").arg(src).query(&mut conn).unwrap();
    }
    let s = TopologySchema::load(&root.join("config/topology_schema.yaml")).unwrap();
    let c = Constellation::new(NS);
    c.publish_schema(&mut conn, &s).unwrap();
    c.ensure(&mut conn).unwrap();
    for n in ["aesir-solutions", "squad"] {
        c.register(&mut conn, &encode(&s, &Declared::new(n, "1", "global").word("node_role", "full_node")).unwrap()).unwrap();
    }
    c.register(&mut conn, &encode(&s, &Declared::new("gflow", "3.3", "production").words("protocol", &["gnode_stream"]).words("domain", &["workflow"]).word("implementation_language", "rust")).unwrap()).unwrap();
    c.register(&mut conn, &encode(&s, &Declared::new("geodine", "3.3", "production").words("protocol", &["gnode_stream"]).words("domain", &["ml_inference"]).word("implementation_language", "php").fraction("fundamentality", 0.5)).unwrap()).unwrap();
    c.register(&mut conn, &encode(&s, &Declared::new("nierto_com", "3.1", "production").words("protocol", &["http_rest"])).unwrap()).unwrap();
    for (node, w) in [("aesir-solutions", 2u32), ("squad", 1)] {
        let p = Placement { of: "gflow".into(), on: node.into(), env: "production".into(), heartbeat: "gflow".into(), heartbeat_env: None, weight: w };
        c.place(&mut conn, &p, &encode(&s, &Declared::new(&p.id(), "4.2", "production")).unwrap()).unwrap();
    }
    Some((conn, c))
}

const SITE_STREAM: &str = "{nierto_com}:gnode:unified:production";

#[test]
#[ignore]
fn an_entity_target_routes_to_a_placement_stream_by_the_ring() {
    let Some((mut conn, c)) = connect() else { return };
    let mut streams: HashMap<String, u32> = HashMap::new();
    for _ in 0..30 {
        match resolve_one_topology(&mut conn, &c, "gflow", "nierto_com", SITE_STREAM) {
            RelayDecision::Forward { target_site_id, target_stream_key, target_entity_id, target_placement } => {
                assert_eq!(target_site_id, "gflow");
                assert_eq!(target_entity_id, "gflow");
                assert!(target_placement.starts_with("gflow@"));
                assert!(target_stream_key.starts_with("{gflow}:gnode:unified:production:"), "{target_stream_key}");
                *streams.entry(target_stream_key).or_default() += 1;
            }
            other => panic!("{other:?}"),
        }
    }
    assert_eq!(streams["{gflow}:gnode:unified:production:aesir-solutions"], 20, "2:1 by declared weight, exactly");
    assert_eq!(streams["{gflow}:gnode:unified:production:squad"], 10);
    assert!(matches!(resolve_one_topology(&mut conn, &c, "nierto_com", "nierto_com", SITE_STREAM), RelayDecision::Local));
    assert!(matches!(resolve_one_topology(&mut conn, &c, "nierto_com", "gflow", "{gflow}:gnode:unified:production:squad"), RelayDecision::NotFound(ref r) if r.contains("site")),
        "a site has no reader: refused, never a fabricated stream");
    assert!(matches!(resolve_one_topology(&mut conn, &c, "nobody", "nierto_com", SITE_STREAM), RelayDecision::NotFound(_)));
    assert!(matches!(resolve_one_topology(&mut conn, &c, "geodine", "nierto_com", SITE_STREAM), RelayDecision::NotFound(ref r) if r.contains("placement")),
        "a service with no placement is not routable");
    match resolve_one_topology(&mut conn, &c, "{other}:gnode:unified:production", "nierto_com", SITE_STREAM) {
        RelayDecision::Forward { target_stream_key, target_placement, .. } => { assert_eq!(target_stream_key, "{other}:gnode:unified:production"); assert!(target_placement.is_empty()); }
        other => panic!("{other:?}"),
    }
}

#[test]
#[ignore]
fn a_capability_query_picks_the_service_by_colour() {
    let Some((mut conn, c)) = connect() else { return };
    match resolve_one_topology(&mut conn, &c, r#"{"facets":{"domain":["workflow"]}}"#, "nierto_com", SITE_STREAM) {
        RelayDecision::Forward { target_entity_id, .. } => assert_eq!(target_entity_id, "gflow"),
        other => panic!("{other:?}"),
    }
    assert!(matches!(resolve_one_topology(&mut conn, &c, r#"{"facets":{"domain":["search"]}}"#, "nierto_com", SITE_STREAM), RelayDecision::NotFound(_)));
    assert!(matches!(resolve_one_topology(&mut conn, &c, r#"{"capabilities":{"domain_primary":0.5}}"#, "nierto_com", SITE_STREAM), RelayDecision::Error(_)), "the numeric form is refused by name");
    assert!(matches!(resolve_one_topology(&mut conn, &c, r#"{"facets":{"protocol":["http_rest"]}}"#, "gflow", "{gflow}:gnode:unified:production:squad"), RelayDecision::NotFound(_)),
        "a query that only matches sites finds no relay target");
}

#[test]
#[ignore]
fn the_departure_closes_the_placements_in_flight_entry_on_arrival() {
    let Some((mut conn, c)) = connect() else { return };
    std::env::set_var("GNODE_SAMPLER", "write");
    let RelayDecision::Forward { target_site_id, target_entity_id, target_placement, .. } = resolve_one_topology(&mut conn, &c, "gflow", "nierto_com", SITE_STREAM) else { panic!() };
    let forward = "1700000000000-0";
    roundtrip::note_departure(&mut conn, NS, "cid-9", forward, &target_site_id, &target_entity_id, &target_placement, "run");
    c.depart(&mut conn, &target_placement, "cid-9", 1_700_000_000_000).unwrap();
    assert_eq!(c.in_flight(&mut conn, &target_placement, 1_700_000_000_001, 30_000).unwrap(), 1);
    let fields: HashMap<String, String> = [("ri".to_string(), "cid-9".to_string()), ("st".to_string(), "ok".to_string())].into_iter().collect();
    let observed = roundtrip::note_arrival(&mut conn, NS, "{gflow}:gnode:unified:production:aesir-solutions", "1700000000007-0", &fields, false);
    assert!(observed, "the trip settled and the provider was credited");
    assert_eq!(c.in_flight(&mut conn, &target_placement, 1_700_000_000_010, 30_000).unwrap(), 0, "arrival closed the in-flight entry");
}

fn xadd(conn: &mut redis::Connection, stream: &str, fields: &[(&str, &str)]) -> String {
    let mut cmd = redis::cmd("XADD");
    cmd.arg(stream).arg("*");
    for (k, v) in fields { cmd.arg(k).arg(v); }
    cmd.query(conn).unwrap()
}

fn health(conn: &mut redis::Connection, site: &str) -> Vec<HashMap<String, String>> {
    let reply: redis::streams::StreamRangeReply = redis::cmd("XRANGE").arg(format!("{{{site}}}:gnode:health")).arg("-").arg("+").query(conn).unwrap();
    reply.ids.iter().map(|e| e.map.iter().map(|(k, v)| (k.clone(), redis::from_redis_value::<String>(v).unwrap())).collect()).collect()
}

#[test]
#[ignore]
fn a_round_trip_on_a_placement_stream_is_credited_to_the_placement_exactly_once() {
    let Some((mut conn, c)) = connect() else { return };
    std::env::set_var("GNODE_SAMPLER", "write");
    let RelayDecision::Forward { target_site_id, target_stream_key, target_entity_id, target_placement } = resolve_one_topology(&mut conn, &c, "gflow", "nierto_com", SITE_STREAM) else { panic!() };
    let forward = xadd(&mut conn, &target_stream_key, &[("t", "c"), ("id", "cid-7"), ("c", "run")]);
    roundtrip::note_departure(&mut conn, NS, "cid-7", &forward, &target_site_id, &target_entity_id, &target_placement, "run");
    c.depart(&mut conn, &target_placement, "cid-7", roundtrip::entry_ms(&forward).unwrap()).unwrap();
    let reply = xadd(&mut conn, &target_stream_key, &[("t", "r"), ("ri", "cid-7"), ("st", "error")]);
    let f: HashMap<String, String> = [("t".to_string(), "r".to_string()), ("ri".to_string(), "cid-7".to_string()), ("st".to_string(), "error".to_string())].into_iter().collect();
    assert!(roundtrip::note_arrival(&mut conn, NS, &target_stream_key, &reply, &f, false));
    let obs = health(&mut conn, "gflow");
    assert_eq!(obs.len(), 1, "one record (M12)");
    assert_eq!(obs[0]["si"], target_placement, "credited to the placement, not the service");
    assert_eq!(obs[0]["ok"], "0", "a failed reply is still the provider's time, flagged");
    assert_eq!(obs[0]["by"], "gnode");
    assert!(!roundtrip::note_arrival(&mut conn, NS, &target_stream_key, &reply, &f, false), "a second reader measures nothing");
    assert_eq!(health(&mut conn, "gflow").len(), 1);
    assert_eq!(c.in_flight(&mut conn, &target_placement, roundtrip::entry_ms(&reply).unwrap() + 1, 30_000).unwrap(), 0);
    // A reply from another site burns the departure and credits nobody.
    let fwd2 = xadd(&mut conn, &target_stream_key, &[("t", "c"), ("id", "cid-8"), ("c", "run")]);
    roundtrip::note_departure(&mut conn, NS, "cid-8", &fwd2, &target_site_id, &target_entity_id, &target_placement, "run");
    let stray = xadd(&mut conn, "{geodine}:gnode:unified:production", &[("t", "r"), ("ri", "cid-8"), ("st", "ok")]);
    let f8: HashMap<String, String> = [("ri".to_string(), "cid-8".to_string())].into_iter().collect();
    assert!(!roundtrip::note_arrival(&mut conn, NS, "{geodine}:gnode:unified:production", &stray, &f8, false), "wrong replier");
    assert_eq!(health(&mut conn, "gflow").len(), 1);
    assert!(!roundtrip::note_arrival(&mut conn, NS, &target_stream_key, &reply, &[("ri".to_string(), "never-sent".to_string())].into_iter().collect(), false), "unknown correlation id");
}

#[test]
#[ignore]
fn a_refused_relay_is_visible_on_the_key_the_caller_polls() {
    let Some((mut conn, _c)) = connect() else { return };
    use gnode::integration::processor::resp3_protocol::OptimizedCommand;
    let fields: HashMap<String, String> = [("t", "c"), ("c", "run"), ("id", "req-refused-9"), ("ss", "nierto_com"), ("_rt", "gflow"), ("p", "{}")].into_iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
    let cmd = OptimizedCommand::from_resp3_fields("1-0".into(), fields).unwrap();
    let verdict = gnode::daemon::Response { id: "req-refused-9".into(), status: "error".into(), result: None, error: Some("Relay denied by policy: nierto_com may not reach gflow (label:workflow)".into()), timestamp: 0.0, batch_id: None, sequence: None };
    assert!(gnode::integration::relay::reply::key_relay_verdict(&mut conn, &cmd, &verdict, "nierto_com"));
    let body: Option<String> = redis::cmd("GET").arg("{nierto_com}:res:req-refused-9").query(&mut conn).unwrap();
    let body: serde_json::Value = serde_json::from_str(&body.expect("the verdict is on the origin's res key (M13)")).unwrap();
    assert_eq!(body["status"], "error");
    assert!(body["error"].as_str().unwrap().contains("denied"));
}
