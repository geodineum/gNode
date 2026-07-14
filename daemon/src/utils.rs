/// Utility functions for gNode daemon
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// Get current timestamp in milliseconds as i64
pub fn current_timestamp_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// Get current timestamp in seconds as f64
pub fn current_timestamp() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

/// Canonical short-form field names for gNode stream messages.
///
/// Single source of truth — every parser in the daemon (script-format,
/// RESP3, key-based) MUST resolve fields via these lists so that one
/// command-on-the-wire is parsed identically by every code path.
///
/// Canonical compact form (what the PHP/Rust client SHOULD write):
///
///   Command  (t=c):  id, t, c, p, ss, sn, ts
///   Response (t=r):  id|ri, t, st, r, e, ts
///   Batch    (t=b…): id, t, bi, tc, msgs, ts
///
/// Routing fields use a directional pair: source / destination. The PHP
/// client writes `ss` (source_site) and `sn` (source_node); the daemon
/// echoes `ds`/`dn` (dest_site/dest_node) on the response side. The
/// preferred long form is `service_id` (a site IS a service); older code
/// wrote `st`/`n`/`site_id`/`node_id` — those remain accepted as fallbacks
/// but should NOT be used for new writes (in particular `st` collides with
/// response status — see STATUS).
pub mod field_names {
    // ─── Identity & dispatch ─────────────────────────────────────
    pub const ID:        &[&str] = &["id", "request_id"];
    pub const TYPE:      &[&str] = &["t", "type"];        // c | r | bc | br | b | i

    // ─── Command body (only when type=c) ─────────────────────────
    pub const CMD:       &[&str] = &["c", "cmd", "command", "command_name"];
    pub const PARAMS:    &[&str] = &["p", "params", "parameters"];

    // ─── Routing / addressing (canonical compact form first) ─────
    // SOURCE_SITE is the writer of the message (a service). PHP gNode-Client
    // writes "ss". "service_id" is the preferred long form; "site_id" and
    // "st" remain accepted as legacy aliases (a site IS a service) — note
    // that "st" also means STATUS in response messages, so it's only an
    // alias for SOURCE_SITE in command-type messages where status has no
    // meaning.
    pub const SOURCE_SITE: &[&str] = &["ss", "source_site", "service_id", "site_id", "st"];
    pub const SOURCE_NODE: &[&str] = &["sn", "source_node", "node_id", "n"];
    pub const DEST_SITE:   &[&str] = &["ds", "dest_site"];
    pub const DEST_NODE:   &[&str] = &["dn", "dest_node"];

    // Backwards-compatible aliases — older call sites use SITE/NODE
    // without realising they mean *source* site/node. Both resolve to
    // the same constant so the parse contract is identical.
    pub const SITE: &[&str] = SOURCE_SITE;
    pub const NODE: &[&str] = SOURCE_NODE;

    // ─── Response body (only when type=r) ────────────────────────
    // STATUS uses "st" per the RESP3 protocol spec (resp3_protocol.rs).
    // Reusing "st" here is safe because a single message is either
    // type=c (no status) or type=r (no site_id) — never both.
    pub const STATUS: &[&str] = &["st", "s", "status"];
    pub const RESULT: &[&str] = &["r", "result"];
    pub const ERROR:  &[&str] = &["e", "error"];

    // ─── Common ──────────────────────────────────────────────────
    // TIMESTAMP intentionally does NOT include "t" — that collides
    // with TYPE. Clients writing to the wire must use "ts".
    pub const TIMESTAMP: &[&str] = &["ts", "timestamp"];
}

/// Get a field from a HashMap trying multiple keys in order (short-form preferred)
/// Returns empty string if no key matches
pub fn get_field(map: &HashMap<String, String>, keys: &[&str]) -> String {
    for key in keys {
        if let Some(value) = map.get(*key) {
            return value.clone();
        }
    }
    String::new()
}

/// Get an optional field from a HashMap trying multiple keys in order
/// Returns None if no key matches or all values are empty
pub fn get_field_opt(map: &HashMap<String, String>, keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Some(value) = map.get(*key) {
            if !value.is_empty() {
                return Some(value.clone());
            }
        }
    }
    None
}