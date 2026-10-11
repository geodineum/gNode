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
