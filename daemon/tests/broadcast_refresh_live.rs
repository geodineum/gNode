//! A registration announced on the broadcast stream is in site discovery within a second;
//! one whose announcement was lost is there after the next poll.
//!
//!   valkey-server --port 47991 --save "" --appendonly no --daemonize yes
//!   GNODE_TEST_VALKEY_URL=redis://127.0.0.1:47991 cargo test --test broadcast_refresh_live -- --ignored --test-threads=1

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use gnode::integration::processor::{get_broadcast_stream, triggers_discovery_refresh, BroadcastReader};
use gnode::integration::stream_discovery::{StreamDiscoveryConfig, StreamDiscoveryManager};

fn url() -> Option<String> { std::env::var("GNODE_TEST_VALKEY_URL").ok() }

fn connect(url: &str) -> redis::Connection {
    redis::Client::open(url).unwrap().get_connection().unwrap()
}

fn fresh(url: &str) -> redis::Connection {
    let mut conn = connect(url);
    let _: String = redis::cmd("FLUSHALL").query(&mut conn).unwrap();
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("functions");
    for lib in ["gnode_site.lua", "gnode_stream.lua"] {
        let src = std::fs::read_to_string(dir.join(lib)).unwrap();
        let _: String = redis::cmd("FUNCTION").arg("LOAD").arg("REPLACE").arg(src).query(&mut conn).unwrap();
    }
    conn
}

fn discovery(poll_secs: u64) -> Arc<StreamDiscoveryManager> {
    Arc::new(StreamDiscoveryManager::new(StreamDiscoveryConfig {
        refresh_interval_secs: poll_secs,
        environment: "all".into(),
        ..Default::default()
    }))
}

fn provision(conn: &mut redis::Connection, site: &str) {
    let _: String = redis::cmd("FCALL").arg("GNODE_PROVISION_SERVICE").arg(0)
        .arg(site).arg(r#"["production"]"#).arg("geodineum").arg("").query(conn).unwrap();
}

fn announce(conn: &mut redis::Connection, site: &str) {
    let data = format!(r#"{{"site_id":"{site}","environment":"production","constellation":"geodineum","ts":0}}"#);
    let _: String = redis::cmd("XADD").arg(get_broadcast_stream("geodineum", "gnode")).arg("MAXLEN").arg("~").arg(1000)
        .arg("*").arg("type").arg("site_registered").arg("data").arg(data).query(conn).unwrap();
}

fn knows(d: &StreamDiscoveryManager, site: &str) -> bool {
    let stream = format!("{{{site}}}:gnode:unified:production");
    d.get_site_ids().iter().any(|s| s == site) && d.get_unified_streams().iter().any(|s| s.key == stream)
}

/// The daemon's broadcast loop in miniature: read, and re-read discovery once per batch that asks for it.
fn listen(url: String, d: Arc<StreamDiscoveryManager>, stop: Arc<AtomicBool>, ready: Arc<AtomicBool>) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let mut conn = connect(&url);
        let mut reader = BroadcastReader::new(get_broadcast_stream("geodineum", "gnode"), "geodineum".into(), false);
        while !stop.load(Ordering::SeqCst) {
            ready.store(true, Ordering::SeqCst);
            let msgs = reader.read_messages(&mut conn, 100, 200, false).unwrap_or_default();
            if msgs.iter().any(|m| triggers_discovery_refresh(&m.message_type)) {
                d.refresh_now(&mut conn).unwrap();
            }
        }
    })
}

fn wait_for(d: &StreamDiscoveryManager, site: &str, limit: Duration) -> Option<Duration> {
    let t = Instant::now();
    while t.elapsed() < limit {
        if knows(d, site) { return Some(t.elapsed()); }
        std::thread::sleep(Duration::from_millis(10));
    }
    None
}

#[test]
#[ignore]
fn an_announced_registration_is_seen_within_a_second() {
    let Some(url) = url() else { return };
    let mut conn = fresh(&url);
    let d = discovery(60);
    d.refresh(&mut conn).unwrap();
    let _ = d.check_and_clear_sync_signal();

    let (stop, ready) = (Arc::new(AtomicBool::new(false)), Arc::new(AtomicBool::new(false)));
    let h = listen(url.clone(), Arc::clone(&d), Arc::clone(&stop), Arc::clone(&ready));
    while !ready.load(Ordering::SeqCst) { std::thread::sleep(Duration::from_millis(5)); }
    std::thread::sleep(Duration::from_millis(50));

    provision(&mut conn, "announced_com");
    announce(&mut conn, "announced_com");
    let seen = wait_for(&d, "announced_com", Duration::from_secs(1));
    stop.store(true, Ordering::SeqCst);
    h.join().unwrap();

    let seen = seen.expect("announced registration not in discovery within 1 s");
    eprintln!("announced registration seen after {} ms", seen.as_millis());
    assert!(d.check_and_clear_sync_signal(), "the worker was not woken to subscribe");
}

#[test]
#[ignore]
fn a_lost_announcement_is_caught_by_the_poll() {
    let Some(url) = url() else { return };
    let mut conn = fresh(&url);
    let d = discovery(1);
    d.refresh(&mut conn).unwrap();

    let (stop, ready) = (Arc::new(AtomicBool::new(false)), Arc::new(AtomicBool::new(false)));
    let h = listen(url.clone(), Arc::clone(&d), Arc::clone(&stop), Arc::clone(&ready));
    while !ready.load(Ordering::SeqCst) { std::thread::sleep(Duration::from_millis(5)); }

    provision(&mut conn, "unannounced_com");
    assert!(wait_for(&d, "unannounced_com", Duration::from_millis(700)).is_none(), "seen without an announcement or a poll");

    let t = Instant::now();
    while !knows(&d, "unannounced_com") && t.elapsed() < Duration::from_secs(3) {
        d.refresh_if_needed(&mut conn).unwrap();
        std::thread::sleep(Duration::from_millis(50));
    }
    stop.store(true, Ordering::SeqCst);
    h.join().unwrap();
    assert!(knows(&d, "unannounced_com"), "the poll did not catch the registration");
}
