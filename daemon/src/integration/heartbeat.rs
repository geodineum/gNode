//! The component-liveness heartbeat contract, in one place.
//!
//! Every long-running component announces itself by writing
//!
//!     {<topology_ns>}:gnode:heartbeat:<env>:<component>:<node>
//!
//! with SETEX 120 and refreshing ~every 60s, so a dead process self-expires
//! and the dashboard reads absence as down. Four components across three
//! languages write this family (gnode-daemon here; COMMS, gSchedule, gFlow in
//! Rust; Geodine in PHP) and one reader joins it against the constellation
//! entities — a shared wire format, so the shape is pinned here and mirrored
//! in CONTRACTS/heartbeat.md rather than reconstructed per writer.
//!
//! Two rules that exist because their absence was a live defect:
//!
//! - The `<node>` segment. Before it, every node in a constellation wrote the
//!   same key and last-writer-won: a two-node estate could not say WHICH node
//!   a component ran on, and one dead daemon hid behind the other's fresh ts.
//!   The node id is the daemon's declared identity (GNODE_NODE_ID — the same
//!   value that names the constellation entity and the consumer-group
//!   consumer), which is the short hostname. It is a name, not a role:
//!   "master" belongs in node_role (constellation dim 0), never in an
//!   identity slot.
//!
//! - The literal `gnode`. The daemon used to build this key from its
//!   configurable stream_prefix while every other writer and the reader
//!   hardcoded `gnode` — one env override away from the daemon silently
//!   heartbeating into a family nobody reads.

/// TTL seconds. Twice the refresh cadence, so one missed refresh survives
/// and two reads as down.
pub const HEARTBEAT_TTL_SECS: u64 = 120;

/// The canonical key. `component` is the stable component name
/// ("gnode-daemon", "comms", "gschedule", "gflow", "geodine");
/// `node` is the node's declared id — see module docs.
pub fn heartbeat_key(topology_ns: &str, environment: &str, component: &str, node: &str) -> String {
    format!(
        "{{{}}}:gnode:heartbeat:{}:{}:{}",
        topology_ns, environment, component, node
    )
}

/// The canonical value: ts for staleness, pid for the curious, comp and node
/// so the value is self-describing when found detached from its key.
pub fn heartbeat_value(component: &str, node: &str, ts_secs: u64, pid: u32) -> String {
    format!(
        "{{\"ts\":{},\"pid\":{},\"comp\":\"{}\",\"node\":\"{}\"}}",
        ts_secs, pid, component, node
    )
}

/// Short hostname — first dot-label of the kernel hostname, verbatim case.
/// The fallback for components that carry no declared node id of their own.
/// The daemon itself must NOT use this: its identity is the configured
/// node_id, and heartbeat, constellation entity and consumer name must all
/// agree even when an operator declared something other than the hostname.
pub fn short_hostname() -> String {
    std::fs::read_to_string("/proc/sys/kernel/hostname")
        .ok()
        .and_then(|s| s.trim().split('.').next().map(str::to_string))
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown-node".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The exact wire form, as a literal. Three other repos construct this
    /// same string in their own languages; this test is the Rust half of the
    /// cross-repo agreement, the way utils::field_names pins the alias table.
    #[test]
    fn key_matches_the_contract_literally() {
        assert_eq!(
            heartbeat_key("geodineum", "production", "gnode-daemon", "aesir"),
            "{geodineum}:gnode:heartbeat:production:gnode-daemon:aesir"
        );
    }

    /// `gnode` is literal. A configurable prefix in a shared key family means
    /// one env override diverges the writer from every reader.
    #[test]
    fn the_gnode_segment_cannot_be_configured_away() {
        let k = heartbeat_key("ns", "e", "c", "n");
        assert!(k.contains(":gnode:heartbeat:"), "{}", k);
    }

    #[test]
    fn value_is_self_describing_json() {
        let v: serde_json::Value =
            serde_json::from_str(&heartbeat_value("comms", "aesir", 123, 42)).unwrap();
        assert_eq!(v["ts"], 123);
        assert_eq!(v["pid"], 42);
        assert_eq!(v["comp"], "comms");
        assert_eq!(v["node"], "aesir");
    }

    #[test]
    fn short_hostname_is_one_label() {
        let h = short_hostname();
        assert!(!h.is_empty());
        assert!(!h.contains('.'), "not a first label: {}", h);
    }
}
