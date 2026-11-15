//! Scale-out and sickness as signals through gNode-SIGNALS (`GNODE_SIGNAL_PUBLISH`), one
//! entry when a condition starts and one when it clears, never one per tick.
//!
//! Stream `{c}:signals:global:stream` per constellation. The conditions with a start and no
//! clear yet live in `{c}:gnode:signals:open` (field `<kind>|<subject>`), so a restart or a new
//! lease holder continues where the last one stopped. Without the library loaded nothing is
//! published, nothing is recorded, and the daemon says so once.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use log::{info, warn};
use sha3::{Digest, Sha3_256};

pub const LIBRARY: &str = "gnode_signal";
pub const PRODUCER: &str = concat!("gnode-daemon@", env!("CARGO_PKG_VERSION"));
pub const SCHEMA_VERSION: u32 = 1;
const PROBE_EVERY: Duration = Duration::from_secs(300);

static ABSENT_SAID: AtomicBool = AtomicBool::new(false);

pub fn stream_key(constellation: &str) -> String {
    format!("{{{}}}:signals:global:stream", constellation)
}

pub fn open_key(constellation: &str) -> String {
    format!("{{{}}}:gnode:signals:open", constellation)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    ScaleOut,
    Sick,
}

impl Kind {
    pub fn word(self) -> &'static str {
        match self {
            Kind::ScaleOut => "scale_out",
            Kind::Sick => "sick",
        }
    }

    /// Consecutive evaluations a condition must hold before it starts, and be gone before it
    /// clears. Scale-out already needs three ticks in the sampler; a placement is dead only
    /// after its heartbeat is 75 s old, and one more pass keeps a late writer from flapping.
    pub fn hysteresis(self) -> (u32, u32) {
        match self {
            Kind::ScaleOut => (1, 3),
            Kind::Sick => (2, 2),
        }
    }
}

/// Start and clear decisions from consecutive evaluations. Pure.
#[derive(Debug, Default)]
pub struct Edges {
    start_after: u32,
    clear_after: u32,
    seen: HashMap<String, u32>,
    missed: HashMap<String, u32>,
}

impl Edges {
    pub fn new(start_after: u32, clear_after: u32) -> Self {
        Self { start_after: start_after.max(1), clear_after: clear_after.max(1), ..Default::default() }
    }

    /// `open`: subjects with a start and no clear. `now`: subjects whose condition holds this
    /// evaluation. Returns (to start, to clear).
    pub fn step(&mut self, open: &BTreeSet<String>, now: &BTreeSet<String>) -> (Vec<String>, Vec<String>) {
        let mut starts = Vec::new();
        let mut clears = Vec::new();
        self.seen.retain(|s, _| now.contains(s) && !open.contains(s));
        self.missed.retain(|s, _| open.contains(s) && !now.contains(s));
        for s in now {
            if open.contains(s) {
                continue;
            }
            let n = self.seen.entry(s.clone()).or_insert(0);
            *n += 1;
            if *n >= self.start_after {
                starts.push(s.clone());
            }
        }
        for s in open {
            if now.contains(s) {
                continue;
            }
            let n = self.missed.entry(s.clone()).or_insert(0);
            *n += 1;
            if *n >= self.clear_after {
                clears.push(s.clone());
            }
        }
        (starts, clears)
    }
}

/// The payload `GNODE_SIGNAL_PUBLISH` stores. The fingerprint covers what makes a transition
/// one transition (constellation, kind, state, subject, since), so a retry of an unconfirmed
/// publish dedups and a later episode of the same condition does not.
pub fn payload(constellation: &str, kind: Kind, state: &str, subject: &str, since: u64, at: u64, node: &str, detail: &serde_json::Value) -> serde_json::Value {
    let identity = format!("gnode-signal|{}|{}|{}|{}|{}|{}", SCHEMA_VERSION, constellation, kind.word(), state, subject, since);
    let fingerprint = hex::encode(Sha3_256::digest(identity.as_bytes()));
    serde_json::json!({
        "fingerprint": fingerprint,
        "producer": PRODUCER,
        "schema_version": SCHEMA_VERSION,
        "timestamp_fp": ((at as u128) << 64).to_string(),
        "constellation": constellation,
        "kind": kind.word(),
        "state": state,
        "subject": subject,
        "since": since,
        "at": at,
        "node": node,
        "detail": detail,
    })
}

/// One kind's publisher. The lease holder calls `evaluate` once per pass with the subjects
/// whose condition holds now.
pub struct SignalBus {
    kind: Kind,
    edges: HashMap<String, Edges>,
    present: Option<bool>,
    probed: Option<Instant>,
    failing: Option<String>,
}

enum Sent {
    Ok(String),
    Absent,
    Failed(String),
}

impl SignalBus {
    pub fn new(kind: Kind) -> Self {
        Self { kind, edges: HashMap::new(), present: None, probed: None, failing: None }
    }

    fn absent(&mut self) {
        self.present = Some(false);
        self.probed = Some(Instant::now());
        if !ABSENT_SAID.swap(true, Ordering::Relaxed) {
            info!("gNode-SIGNALS ({}) is not loaded in this ValKey: scale-out and sickness are not published as signals", LIBRARY);
        }
    }

    /// Loaded? Asked once, and again every few minutes while it is not.
    fn ready(&mut self, conn: &mut redis::Connection) -> bool {
        match self.present {
            Some(true) => return true,
            Some(false) if self.probed.map(|t| t.elapsed() < PROBE_EVERY).unwrap_or(false) => return false,
            _ => {}
        }
        self.probed = Some(Instant::now());
        let listed: redis::RedisResult<Vec<redis::Value>> = redis::cmd("FUNCTION").arg("LIST").arg("LIBRARYNAME").arg(LIBRARY).query(conn);
        match listed {
            Ok(libs) if libs.is_empty() => {
                self.absent();
                false
            }
            // A login that may not list libraries learns it from the publish itself.
            _ => {
                self.present = Some(true);
                true
            }
        }
    }

    fn send(conn: &mut redis::Connection, constellation: &str, p: &serde_json::Value) -> Sent {
        let reply: redis::RedisResult<String> = redis::cmd("FCALL").arg("GNODE_SIGNAL_PUBLISH").arg(1)
            .arg(stream_key(constellation)).arg(p.to_string()).query(conn);
        match reply {
            Ok(raw) => match serde_json::from_str::<serde_json::Value>(&raw) {
                Ok(v) if v["ok"] == true => Sent::Ok(v["msg_id"].as_str().unwrap_or("").to_string()),
                _ => Sent::Failed(format!("unexpected reply {}", raw)),
            },
            Err(e) if e.to_string().contains("Function not found") => Sent::Absent,
            Err(e) => Sent::Failed(e.to_string()),
        }
    }

    /// Publish what changed since the last evaluation. `now`: subject → detail. Returns the
    /// lines published, for the caller's log.
    pub fn evaluate(&mut self, conn: &mut redis::Connection, constellation: &str, now: &BTreeMap<String, serde_json::Value>, at: u64, node: &str) -> Vec<String> {
        let mut done = Vec::new();
        if !self.ready(conn) {
            return done;
        }
        let prefix = format!("{}|", self.kind.word());
        let stored: redis::RedisResult<HashMap<String, String>> = redis::cmd("HGETALL").arg(open_key(constellation)).query(conn);
        let Ok(stored) = stored else { return done };
        let open: BTreeMap<String, serde_json::Value> = stored.into_iter()
            .filter_map(|(f, v)| Some((f.strip_prefix(&prefix)?.to_string(), serde_json::from_str(&v).unwrap_or_default())))
            .collect();
        let open_set: BTreeSet<String> = open.keys().cloned().collect();
        let now_set: BTreeSet<String> = now.keys().cloned().collect();
        let (a, b) = self.kind.hysteresis();
        let (starts, clears) = self.edges.entry(constellation.to_string()).or_insert_with(|| Edges::new(a, b)).step(&open_set, &now_set);

        let mut work: Vec<(&str, String, u64, serde_json::Value)> = Vec::new();
        for s in starts {
            let detail = now.get(&s).cloned().unwrap_or_default();
            work.push(("start", s, at, detail));
        }
        for s in clears {
            let rec = open.get(&s).cloned().unwrap_or_default();
            let since = rec["since"].as_u64().unwrap_or(0);
            work.push(("clear", s, since, rec["detail"].clone()));
        }
        for (state, subject, since, detail) in work {
            let p = payload(constellation, self.kind, state, &subject, since, at, node, &detail);
            match Self::send(conn, constellation, &p) {
                Sent::Ok(id) => {
                    self.failing = None;
                    let field = format!("{}{}", prefix, subject);
                    let _: redis::RedisResult<i64> = if state == "start" {
                        let rec = serde_json::json!({ "since": since, "detail": detail });
                        redis::cmd("HSET").arg(open_key(constellation)).arg(&field).arg(rec.to_string()).query(conn)
                    } else {
                        redis::cmd("HDEL").arg(open_key(constellation)).arg(&field).query(conn)
                    };
                    done.push(format!("{} {} {} ({})", self.kind.word(), state, subject, id));
                }
                Sent::Absent => {
                    self.absent();
                    break;
                }
                Sent::Failed(e) => {
                    if self.failing.as_deref() != Some(e.as_str()) {
                        warn!("[signals] publish to {} failed, retried each pass: {}", stream_key(constellation), e);
                        self.failing = Some(e);
                    }
                    break;
                }
            }
        }
        done
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(xs: &[&str]) -> BTreeSet<String> {
        xs.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn a_condition_starts_once_and_a_held_one_says_nothing() {
        let mut e = Edges::new(1, 3);
        assert_eq!(e.step(&set(&[]), &set(&["squad"])), (vec!["squad".to_string()], vec![]));
        for _ in 0..10 {
            assert_eq!(e.step(&set(&["squad"]), &set(&["squad"])), (vec![], vec![]));
        }
    }

    #[test]
    fn a_clear_needs_the_condition_gone_for_the_whole_hysteresis_and_a_return_resets_it() {
        let mut e = Edges::new(1, 3);
        let open = set(&["squad"]);
        assert_eq!(e.step(&open, &set(&[])).1.len(), 0);
        assert_eq!(e.step(&open, &set(&[])).1.len(), 0);
        assert_eq!(e.step(&open, &set(&["squad"])), (vec![], vec![]), "back: the count resets");
        assert_eq!(e.step(&open, &set(&[])).1.len(), 0);
        assert_eq!(e.step(&open, &set(&[])).1.len(), 0);
        assert_eq!(e.step(&open, &set(&[])).1, vec!["squad".to_string()]);
    }

    #[test]
    fn a_start_needs_consecutive_evaluations() {
        let mut e = Edges::new(2, 2);
        assert_eq!(e.step(&set(&[]), &set(&["gflow@squad"])).0.len(), 0);
        assert_eq!(e.step(&set(&[]), &set(&[])).0.len(), 0, "one blip is not sickness");
        assert_eq!(e.step(&set(&[]), &set(&["gflow@squad"])).0.len(), 0);
        assert_eq!(e.step(&set(&[]), &set(&["gflow@squad"])).0, vec!["gflow@squad".to_string()]);
    }

    #[test]
    fn an_unconfirmed_publish_is_decided_again_next_pass() {
        let mut e = Edges::new(1, 1);
        assert_eq!(e.step(&set(&[]), &set(&["a"])).0, vec!["a".to_string()]);
        assert_eq!(e.step(&set(&[]), &set(&["a"])).0, vec!["a".to_string()], "the open set did not record it, so it is still to start");
        assert_eq!(e.step(&set(&["a"]), &set(&[])).1, vec!["a".to_string()]);
        assert_eq!(e.step(&set(&["a"]), &set(&[])).1, vec!["a".to_string()]);
    }

    #[test]
    fn the_fingerprint_names_the_transition_not_the_moment() {
        let d = serde_json::json!({ "inflation": 3.4 });
        let a = payload("geodineum", Kind::ScaleOut, "start", "squad", 100, 100, "aesir-solutions", &d);
        let retry = payload("geodineum", Kind::ScaleOut, "start", "squad", 100, 130, "squad", &serde_json::json!({}));
        let clear = payload("geodineum", Kind::ScaleOut, "clear", "squad", 100, 200, "aesir-solutions", &d);
        let again = payload("geodineum", Kind::ScaleOut, "start", "squad", 500, 500, "aesir-solutions", &d);
        assert_eq!(a["fingerprint"], retry["fingerprint"]);
        assert_ne!(a["fingerprint"], clear["fingerprint"]);
        assert_ne!(a["fingerprint"], again["fingerprint"]);
        assert_eq!(a["fingerprint"].as_str().unwrap().len(), 64);
    }

    #[test]
    fn the_payload_carries_the_library_contract_fields() {
        let p = payload("geodineum", Kind::Sick, "start", "gflow@squad", 7, 7, "aesir-solutions", &serde_json::json!({}));
        for f in ["fingerprint", "producer", "schema_version", "timestamp_fp"] {
            assert!(!p[f].is_null(), "{f}");
        }
        assert_eq!(p["timestamp_fp"], (7u128 << 64).to_string());
        assert!(p["producer"].as_str().unwrap().starts_with("gnode-daemon@"));
        assert_eq!(stream_key("geodineum"), "{geodineum}:signals:global:stream");
        assert_eq!(open_key("geodineum"), "{geodineum}:gnode:signals:open");
    }
}
