//! The relay's permission check and round trip, on a real ValKey.
//!
//! Covers what unit tests cannot: a rule written the way `relay_policy_set`
//! stores it being matched by the source the router now derives, and the
//! departure record surviving the store to be claimed exactly once.
//!
//!   valkey-server --port 6397 --save "" --daemonize yes
//!   GNODE_TEST_VALKEY_URL=redis://127.0.0.1:6397 cargo test --test relay_live -- --ignored --test-threads=1

use std::collections::HashMap;

use gnode::config::build_health_stream_key;
use gnode::integration::relay::policy::{check_relay_policy, set_relay_policy, PolicyDecision};
use gnode::integration::relay::roundtrip;
use redis::streams::StreamRangeReply;

const NS: &str = "geodineum";

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
    Some(conn)
}

fn unified(site: &str) -> String { format!("{{{site}}}:gnode:unified:production") }

fn xadd(conn: &mut redis::Connection, stream: &str, fields: &[(&str, &str)]) -> String {
    let mut cmd = redis::cmd("XADD");
    cmd.arg(stream).arg("*");
    for (k, v) in fields { cmd.arg(k).arg(v); }
    cmd.query(conn).unwrap()
}

fn fields(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
}

fn health(conn: &mut redis::Connection, site: &str) -> Vec<HashMap<String, String>> {
    let reply: StreamRangeReply = redis::cmd("XRANGE").arg(build_health_stream_key(site)).arg("-").arg("+").query(conn).unwrap();
    reply.ids.iter().map(|e| e.map.iter()
        .map(|(k, v)| (k.clone(), redis::from_redis_value::<String>(v).unwrap()))
        .collect()).collect()
}

fn departure_exists(conn: &mut redis::Connection, cid: &str) -> bool {
    let n: i64 = redis::cmd("EXISTS").arg(format!("{{{NS}}}:gnode:relay:sent:{cid}")).query(conn).unwrap();
    n == 1
}

fn denied(d: &PolicyDecision) -> bool { !d.is_allowed() }

#[test]
#[ignore]
fn a_rule_is_matched_by_the_bare_site_id_the_router_now_derives() {
    let Some(mut conn) = connect() else { return };
    set_relay_policy(&mut conn, NS, "gflow:geodine", "deny", "test", &["*"]).unwrap();

    assert!(denied(&check_relay_policy(&mut conn, NS, "gflow", "geodine", "ping", false)));
    assert!(check_relay_policy(&mut conn, NS, "gshield", "geodine", "ping", false).is_allowed(), "no rule names gshield");
    assert!(check_relay_policy(&mut conn, NS, "{gflow}", "geodine", "ping", false).is_allowed(),
        "the source the router derived before the fix: braced, so no operator rule could ever match it");
}

#[test]
#[ignore]
fn a_deny_is_scoped_to_its_commands_and_an_exact_pair_overrides_a_wildcard() {
    let Some(mut conn) = connect() else { return };
    set_relay_policy(&mut conn, NS, "*:gschedule", "deny", "test", &["start_workflow"]).unwrap();

    assert!(check_relay_policy(&mut conn, NS, "gflow", "gschedule", "ping", false).is_allowed());
    assert!(denied(&check_relay_policy(&mut conn, NS, "gflow", "gschedule", "start_workflow", false)));

    set_relay_policy(&mut conn, NS, "gflow:gschedule", "allow", "test", &["*"]).unwrap();
    assert!(check_relay_policy(&mut conn, NS, "gflow", "gschedule", "start_workflow", false).is_allowed(),
        "an exact-pair allow is the escape from a wildcard deny: that is how a tenant is whitelisted");
    assert!(denied(&check_relay_policy(&mut conn, NS, "gshield", "gschedule", "start_workflow", false)));
}

#[test]
#[ignore]
fn a_round_trip_is_credited_to_the_target_exactly_once() {
    let Some(mut conn) = connect() else { return };
    let cid = "cid-1";
    let forward = xadd(&mut conn, &unified("gschedule"), &[("t", "c"), ("id", cid), ("c", "ping")]);
    roundtrip::note_departure(&mut conn, NS, cid, &forward, "gschedule", "", "ping");
    assert!(departure_exists(&mut conn, cid));

    let reply = xadd(&mut conn, &unified("gschedule"), &[("t", "r"), ("ri", cid), ("st", "ok")]);
    let f = fields(&[("t", "r"), ("ri", cid), ("st", "ok")]);
    assert!(roundtrip::note_arrival(&mut conn, NS, &unified("gschedule"), &reply, &f, false));

    let obs = health(&mut conn, "gschedule");
    assert_eq!(obs.len(), 1);
    assert_eq!(obs[0]["t"], "rq");
    assert_eq!(obs[0]["si"], "gschedule", "no entity named: the site is the provider");
    assert_eq!(obs[0]["cmd"], "ping");
    assert_eq!(obs[0]["ok"], "1");
    let lat: u64 = obs[0]["lat"].parse().unwrap();
    assert!(lat < 2_000, "two consecutive XADDs are not {lat} ms apart");
    assert_eq!(roundtrip::entry_ms(&reply).unwrap() - roundtrip::entry_ms(&forward).unwrap(), lat);

    assert!(!departure_exists(&mut conn, cid), "GETDEL consumed the departure");
    assert!(!roundtrip::note_arrival(&mut conn, NS, &unified("gschedule"), &reply, &f, false),
        "the other consumer group, or the other node, reading the same reply measures nothing");
    assert_eq!(health(&mut conn, "gschedule").len(), 1);
}

#[test]
#[ignore]
fn a_named_entity_is_the_provider_on_its_sites_stream() {
    let Some(mut conn) = connect() else { return };
    let forward = xadd(&mut conn, &unified("geodine"), &[("t", "c"), ("id", "cid-2"), ("c", "infer")]);
    roundtrip::note_departure(&mut conn, NS, "cid-2", &forward, "geodine", "pipeline-runner", "infer");
    let reply = xadd(&mut conn, &unified("geodine"), &[("t", "r"), ("ri", "cid-2"), ("st", "error")]);
    assert!(roundtrip::note_arrival(&mut conn, NS, &unified("geodine"), &reply, &fields(&[("ri", "cid-2"), ("st", "error")]), false));
    let obs = health(&mut conn, "geodine");
    assert_eq!(obs[0]["si"], "pipeline-runner");
    assert_eq!(obs[0]["cmd"], "infer");
    assert_eq!(obs[0]["ok"], "0", "a failed reply is still the provider's time, flagged");
}

#[test]
#[ignore]
fn a_reply_from_the_wrong_site_credits_nobody_and_burns_the_departure() {
    let Some(mut conn) = connect() else { return };
    let forward = xadd(&mut conn, &unified("gschedule"), &[("t", "c"), ("id", "cid-3"), ("c", "ping")]);
    roundtrip::note_departure(&mut conn, NS, "cid-3", &forward, "gschedule", "", "ping");
    let reply = xadd(&mut conn, &unified("gflow"), &[("t", "r"), ("ri", "cid-3"), ("st", "ok")]);
    assert!(!roundtrip::note_arrival(&mut conn, NS, &unified("gflow"), &reply, &fields(&[("ri", "cid-3")]), false));
    assert!(health(&mut conn, "gschedule").is_empty());
    assert!(health(&mut conn, "gflow").is_empty());
    assert!(!departure_exists(&mut conn, "cid-3"),
        "GETDEL runs before the replier is checked: the genuine reply, if it ever comes, now reads as unanswered");
}

#[test]
#[ignore]
fn a_reply_nobody_sent_for_measures_nothing() {
    let Some(mut conn) = connect() else { return };
    let reply = xadd(&mut conn, &unified("gschedule"), &[("t", "r"), ("ri", "never-sent"), ("st", "ok")]);
    assert!(!roundtrip::note_arrival(&mut conn, NS, &unified("gschedule"), &reply, &fields(&[("ri", "never-sent")]), false));
    assert!(health(&mut conn, "gschedule").is_empty());
    assert!(!roundtrip::note_arrival(&mut conn, NS, &unified("gschedule"), &reply, &fields(&[("t", "r")]), false), "no correlation id at all");
}
