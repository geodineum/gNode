// Single-writer lease for shared topology state.
//
// The estate has no master. `is_master` is `cli.master || daemon_name == "master"`
// (main.rs) and `node_id == "master"` (daemon.rs), while both live daemons are named
// `default` — so every node computes FALSE, and everything gated on it silently did
// not run. That is how the published capability schema came to say 30 dimensions
// while the store held 23: its publisher sits behind that flag.
//
// A flag derived from a name cannot decide ownership anyway. A lease can: one node
// holds it, renews it while it lives, and loses it by dying. Whoever holds it owns
// the writes that exactly one node may perform — publishing the tier schemas, and
// writing the derived (sampler-owned) axes.
//
// Deliberately not a distributed lock. There is one ValKey, the writes are idempotent,
// and the failure this prevents is two nodes disagreeing about a measurement — not a
// split brain over money.

use log::{debug, info};
use redis::Connection;

/// The one lease. Its holder owns shared-topology writes.
pub const WRITER_LEASE_KEY: &str = "gnode:cluster:writer";

/// How often the holder renews. The daemon's main loop uses this for its sleep, so
/// the two cannot drift apart.
pub const RENEW_INTERVAL_SECS: usize = 60;

/// The lease must outlive several renew intervals. Set to 45s against a 60s renew,
/// it expired 15 seconds before every renewal: the journal read "Took the
/// shared-writer lease" once a minute — taking, not renewing — and for a quarter of
/// every minute the lease was free for the other node to grab. A TTL shorter than
/// the renew interval is not a lease, it is a lottery.
///
/// Three intervals of slack: a holder survives two missed ticks, and a dead holder
/// is replaced within three minutes. That is the right trade for "who publishes the
/// schema and writes the derived axes" — nobody is waiting on it interactively.
pub const LEASE_TTL_SECS: usize = RENEW_INTERVAL_SECS * 3;

/// Take the lease, or renew it if this node already holds it.
///
/// `true` means this node owns shared writes right now. A node that loses the lease
/// must stop writing immediately, so callers ask every tick rather than caching.
pub fn hold_writer_lease(conn: &mut Connection, node_id: &str) -> bool {
    // NX: nobody holds it. This is also the path taken after a holder dies and its
    // key expires.
    let taken: redis::RedisResult<Option<String>> = redis::cmd("SET")
        .arg(WRITER_LEASE_KEY).arg(node_id).arg("NX").arg("EX").arg(LEASE_TTL_SECS)
        .query(conn);
    if matches!(taken, Ok(Some(_))) {
        // Taking is news; renewing is not. If this line appears on every tick, the
        // TTL is shorter than the renew interval.
        info!("Took the shared-writer lease ({} for {}s)", WRITER_LEASE_KEY, LEASE_TTL_SECS);
        return true;
    }

    match lease_holder(conn) {
        Some(holder) if holder == node_id => {
            // XX so a lease that expired between the GET and here is not silently
            // re-taken: losing it must read as losing it.
            let renewed: redis::RedisResult<Option<String>> = redis::cmd("SET")
                .arg(WRITER_LEASE_KEY).arg(node_id).arg("XX").arg("EX").arg(LEASE_TTL_SECS)
                .query(conn);
            let ok = matches!(renewed, Ok(Some(_)));
            debug!("Renewed the shared-writer lease: {}", ok);
            ok
        }
        Some(holder) => {
            debug!("Shared-writer lease is held by {}; not writing shared state", holder);
            false
        }
        None => false,
    }
}

/// Who holds the lease, if anyone. Read-only: for a status line or an audit.
pub fn lease_holder(conn: &mut Connection) -> Option<String> {
    redis::cmd("GET").arg(WRITER_LEASE_KEY).query::<Option<String>>(conn).ok().flatten()
}

/// Give the lease up on a clean shutdown, so the other node takes over in seconds
/// rather than waiting out the TTL. Only ever releases our own.
pub fn release_writer_lease(conn: &mut Connection, node_id: &str) {
    if lease_holder(conn).as_deref() == Some(node_id) {
        let _: redis::RedisResult<i64> = redis::cmd("DEL").arg(WRITER_LEASE_KEY).query(conn);
        debug!("Released the shared-writer lease");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_lease_outlives_its_renew_interval() {
        // The bug this pins: a 45s TTL renewed every 60s is unheld for 15s of every
        // minute, so the holder re-TAKES rather than renews and the other node can
        // win the gap.
        assert!(LEASE_TTL_SECS >= RENEW_INTERVAL_SECS * 3,
            "TTL {}s must cover at least three {}s renew intervals",
            LEASE_TTL_SECS, RENEW_INTERVAL_SECS);
    }

    /// The semantics that matter are about a SECOND node, so they need a real
    /// server: `GNODE_TEST_VALKEY_URL=redis://127.0.0.1:6397 cargo test -- --ignored`.
    #[test]
    #[ignore]
    fn one_holder_at_a_time() {
        let Ok(url) = std::env::var("GNODE_TEST_VALKEY_URL") else { return };
        let client = redis::Client::open(url).unwrap();
        let mut a = client.get_connection().unwrap();
        let mut b = client.get_connection().unwrap();
        let _: redis::RedisResult<i64> = redis::cmd("DEL").arg(WRITER_LEASE_KEY).query(&mut a);

        assert!(hold_writer_lease(&mut a, "node-a"), "an unheld lease is takeable");
        assert!(!hold_writer_lease(&mut b, "node-b"), "a held lease is not takeable");
        assert!(hold_writer_lease(&mut a, "node-a"), "the holder renews");
        assert_eq!(lease_holder(&mut a).as_deref(), Some("node-a"));

        release_writer_lease(&mut b, "node-b");
        assert_eq!(lease_holder(&mut a).as_deref(), Some("node-a"), "releasing what you do not hold is a no-op");

        release_writer_lease(&mut a, "node-a");
        assert_eq!(lease_holder(&mut a), None);
        assert!(hold_writer_lease(&mut b, "node-b"), "a released lease goes to whoever asks next");
        release_writer_lease(&mut b, "node-b");
    }
}
