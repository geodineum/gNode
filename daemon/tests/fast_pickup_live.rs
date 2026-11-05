//! A sweep without blocks, then one wait over every stream: a command is picked up at once, and one
//! that lands behind the sweep is not missed.
//!
//!   valkey-server --port 6393 --save "" --bind 127.0.0.1 --daemonize yes
//!   GNODE_TEST_VALKEY_URL=redis://127.0.0.1:6393 cargo test --test fast_pickup_live -- --ignored --test-threads=1

use gnode::integration::processor::{read_multi_stream, sweep_mark, wait_for_any};
use std::time::{Duration, Instant};

fn conn() -> Option<redis::Connection> {
    let url = std::env::var("GNODE_TEST_VALKEY_URL").ok()?;
    let mut c = redis::Client::open(url).unwrap().get_connection().unwrap();
    let _: String = redis::cmd("FLUSHALL").query(&mut c).unwrap();
    Some(c)
}

fn add(c: &mut redis::Connection, stream: &str) {
    let _: String = redis::cmd("XADD").arg(stream).arg("*").arg("t").arg("c").query(c).unwrap();
}

const ALL: [&str; 3] = ["{a}:u", "{b}:u", "{c}:u"];

#[test]
#[ignore]
fn nothing_new_waits_out_the_time() {
    let Some(mut c) = conn() else { return };
    for s in ALL { add(&mut c, s); }
    std::thread::sleep(Duration::from_millis(5));
    let mark = sweep_mark(&mut c).unwrap();
    std::thread::sleep(Duration::from_millis(2));
    let t = Instant::now();
    assert!(!wait_for_any(&mut c, &ALL, mark, 150).unwrap());
    assert!(t.elapsed() >= Duration::from_millis(140), "woke early: {:?}", t.elapsed());
}

#[test]
#[ignore]
fn an_entry_on_any_stream_ends_the_wait_at_once() {
    let Some(mut c) = conn() else { return };
    let url = std::env::var("GNODE_TEST_VALKEY_URL").unwrap();
    for target in ALL {
        let mark = sweep_mark(&mut c).unwrap();
        let url = url.clone();
        let adder = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(60));
            let mut w = redis::Client::open(url).unwrap().get_connection().unwrap();
            add(&mut w, target);
            Instant::now()
        });
        assert!(wait_for_any(&mut c, &ALL, mark, 5000).unwrap());
        let woke = Instant::now();
        let sent = adder.join().unwrap();
        let late = woke.saturating_duration_since(sent);
        assert!(late < Duration::from_millis(30), "{} picked up {:?} late", target, late);
    }
}

#[test]
#[ignore]
fn an_entry_landing_behind_the_sweep_is_not_missed() {
    let Some(mut c) = conn() else { return };
    let mark = sweep_mark(&mut c).unwrap();
    add(&mut c, "{b}:u"); // after the sweep passed this stream, before the wait began
    let t = Instant::now();
    assert!(wait_for_any(&mut c, &ALL, mark, 5000).unwrap());
    assert!(t.elapsed() < Duration::from_millis(30), "waited {:?}", t.elapsed());
}

#[test]
#[ignore]
fn the_wait_consumes_nothing_and_a_sweep_does_not_block() {
    let Some(mut c) = conn() else { return };
    for s in ["{a}:u", "{a}:h"] {
        let _: String = redis::cmd("XGROUP").arg("CREATE").arg(s).arg("g").arg("$").arg("MKSTREAM").query(&mut c).unwrap();
    }
    let t = Instant::now();
    let empty = read_multi_stream(&mut c, "{a}:u", "{a}:h", "g", "w", 10, 0, false).unwrap();
    assert!(empty.0.is_empty() && empty.2.is_empty());
    assert!(t.elapsed() < Duration::from_millis(30), "a sweep blocked {:?}", t.elapsed());

    let mark = sweep_mark(&mut c).unwrap();
    let _: String = redis::cmd("XADD").arg("{a}:u").arg("*").arg("t").arg("c").arg("c").arg("ping").arg("id").arg("x").arg("ss").arg("a").query(&mut c).unwrap();
    assert!(wait_for_any(&mut c, &["{a}:u", "{a}:h"], mark, 1000).unwrap());
    let got = read_multi_stream(&mut c, "{a}:u", "{a}:h", "g", "w", 10, 0, false).unwrap();
    assert_eq!(got.2.len(), 1, "the group read still gets the entry the wait saw");
}

#[test]
#[ignore]
fn a_user_without_time_or_xread_is_refused_so_the_loop_can_fall_back() {
    let Some(mut c) = conn() else { return };
    let _: String = redis::cmd("ACL").arg("SETUSER").arg("narrow").arg("on").arg("nopass").arg("~*").arg("+@all").arg("-time").arg("-xread").query(&mut c).unwrap();
    let _: String = redis::cmd("AUTH").arg("narrow").arg("x").query(&mut c).unwrap();
    assert!(sweep_mark(&mut c).is_err());
    assert!(wait_for_any(&mut c, &ALL, 1, 10).is_err());
}
