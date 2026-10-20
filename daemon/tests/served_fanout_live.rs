//! One daemon serving two constellations beats and reconciles in both.
//!
//!   valkey-server --port 6391 --save "" --daemonize yes
//!   GNODE_TEST_VALKEY_URL=redis://127.0.0.1:6391 cargo test --test served_fanout_live -- --ignored --test-threads=1

use gnode::constellation::Constellation;
use gnode::integration::heartbeat::{write_daemon_heartbeats, HEARTBEAT_TTL_SECS};
use gnode::registration_intent::reconcile_served;

fn conn() -> Option<redis::Connection> {
    let url = std::env::var("GNODE_TEST_VALKEY_URL").ok()?;
    let mut c = redis::Client::open(url).unwrap().get_connection().unwrap();
    let _: String = redis::cmd("FLUSHALL").query(&mut c).unwrap();
    Some(c)
}

fn served() -> Vec<Constellation> {
    vec![Constellation::new("geodineum"), Constellation::new("coursematch")]
}

#[test]
#[ignore]
fn the_heartbeat_lands_in_every_served_constellation() {
    let Some(mut conn) = conn() else { return };
    write_daemon_heartbeats(&mut conn, &served(), "production", "aesir-solutions", 42);
    for c in ["geodineum", "coursematch"] {
        let key = format!("{{{}}}:gnode:heartbeat:production:gnode-daemon:aesir-solutions", c);
        let v: Option<String> = redis::cmd("GET").arg(&key).query(&mut conn).unwrap();
        let v: serde_json::Value = serde_json::from_str(&v.unwrap_or_else(|| panic!("{} missing", key))).unwrap();
        assert_eq!(v["comp"], "gnode-daemon");
        assert_eq!(v["node"], "aesir-solutions");
        let ttl: i64 = redis::cmd("TTL").arg(&key).query(&mut conn).unwrap();
        assert!(ttl > 0 && ttl <= HEARTBEAT_TTL_SECS as i64);
    }
}

#[test]
#[ignore]
fn the_reconciler_makes_one_pass_per_served_constellation() {
    let Some(mut conn) = conn() else { return };
    let intent = |site: &str| format!(r#"{{"site":"{}","profile":"web","environment":"production"}}"#, site);
    let _: i64 = redis::cmd("HSET").arg("{geodineum}:gnode:registrations").arg("nierto_com").arg(intent("nierto_com")).query(&mut conn).unwrap();
    let _: i64 = redis::cmd("HSET").arg("{geodineum}:gnode:constellation:entities").arg("nierto_com")
        .arg(r#"{"id":"nierto_com","sh":"3.1","env":"production"}"#).query(&mut conn).unwrap();
    let _: i64 = redis::cmd("HSET").arg("{coursematch}:gnode:registrations").arg("coursematch").arg(intent("coursematch")).query(&mut conn).unwrap();

    let passes = reconcile_served(&mut conn, &served());
    assert_eq!(passes.iter().map(|(c, _)| c.as_str()).collect::<Vec<_>>(), vec!["geodineum", "coursematch"]);
    let geo = passes[0].1.as_ref().unwrap();
    assert_eq!((geo.examined, geo.drifted.len()), (1, 0));
    let cm = passes[1].1.as_ref().unwrap();
    assert_eq!((cm.examined, cm.drifted.clone()), (1, vec!["coursematch".to_string()]), "absent in its own constellation is drift");
}
