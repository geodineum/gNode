//! The response-polling contract, in one place.
//!
//! A command's reply is written to `{site}:res:{request_id}` with a short TTL,
//! and the client polls that key. Two dispatch lanes produce that reply — the
//! Concurrent lane asynchronously (`concurrent_lane.rs`) and the Ordered lane inline
//! (`command_processor.rs`) — but the lane is a daemon-internal scheduling
//! decision. A caller sees one protocol and must not be able to tell which
//! lane served it.
//!
//! That property held only by two hand-copied blocks agreeing with each other.
//! They did not, once: the Ordered lane lacked the `command.id` fallback, so an
//! Ordered command sent with a top-level id and no `_request_id` wrote its
//! reply nowhere and hung the caller's poll to timeout. The bug was invisible
//! from either side — each block read correctly on its own.
//!
//! So the rule lives here, both lanes call it, and the tests below pin the
//! behaviour rather than the copies.

use crate::daemon::{Command, Response};

/// TTL on the response key. Short because a reply nobody collected is not
/// worth keeping; long enough for a client that polls promptly.
pub const RESPONSE_TTL_SECS: usize = 10;

/// Where a command's reply belongs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResponseTarget {
    /// Full ValKey key, brace-literal hash-tagged.
    pub key: String,
    /// The id the client polls on.
    pub request_id: String,
}

/// Resolve the polling target for a command, or `None` when the command
/// carries no id at all and no reply can be addressed.
///
/// `parameters._request_id` wins because that is what the PHP client keys its
/// poll on. `command.id` is the fallback for callers that put the id at the top
/// level of the wire message. `fallback_site` is used only when the command did
/// not carry its own — a command's own site_id is authoritative, since a relayed
/// command's reply belongs to its origin rather than to whichever node ran it.
pub fn resolve(command: &Command, fallback_site: &str) -> Option<ResponseTarget> {
    // `.filter(|s| !s.is_empty())` is load-bearing, and was missing from both
    // hand-rolled copies. An empty string is a perfectly good &str, so
    // `_request_id: ""` used to resolve to Some("") and the fallback never
    // ran — producing `{site}:res:`, one key SHARED by every such command.
    // Two callers would then read each other's replies.
    let request_id = command
        .parameters
        .get("_request_id")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(String::from)
        .or_else(|| {
            if command.id.is_empty() {
                None
            } else {
                Some(command.id.clone())
            }
        })?;

    let site = if command.site_id.is_empty() {
        fallback_site
    } else {
        command.site_id.as_str()
    };

    Some(ResponseTarget {
        key: format!("{{{}}}:res:{}", site, request_id),
        request_id,
    })
}

/// The reply body, identical whichever lane produced it. `batch_id` and
/// `sequence` are deliberately absent: they are internal bookkeeping and no
/// polling client reads them.
pub fn response_json(response: &Response) -> String {
    serde_json::json!({
        "id": response.id,
        "status": response.status,
        "result": response.result,
        "error": response.error,
        "timestamp": response.timestamp,
    })
    .to_string()
}

/// Everything needed to write a reply: where, what, and for how long.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResponseWrite {
    pub key: String,
    pub request_id: String,
    pub json: String,
    pub ttl_secs: usize,
}

/// The whole polling contract as one pure value. Both lanes call this and then
/// do nothing but `SET key json EX ttl` — there is no room left for them to
/// disagree about the key, the body, or the expiry, which is stronger than a
/// test asserting that two copies happen to match.
pub fn plan(command: &Command, fallback_site: &str, response: &Response) -> Option<ResponseWrite> {
    let target = resolve(command, fallback_site)?;
    Some(ResponseWrite {
        key: target.key,
        request_id: target.request_id,
        json: response_json(response),
        ttl_secs: RESPONSE_TTL_SECS,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn cmd(id: &str, site: &str, params: serde_json::Value) -> Command {
        Command {
            id: id.to_string(),
            command: "test_command".to_string(),
            parameters: params,
            site_id: site.to_string(),
            node_id: String::new(),
            timestamp: 0.0,
        }
    }

    #[test]
    fn request_id_parameter_wins() {
        let c = cmd("wire-id", "acme", json!({"_request_id": "poll-id"}));
        let t = resolve(&c, "fallback").unwrap();
        assert_eq!(t.request_id, "poll-id");
        assert_eq!(t.key, "{acme}:res:poll-id");
    }

    /// The regression that motivated this module. An Ordered-lane command with
    /// a top-level id and no _request_id must still be addressable; without
    /// the fallback the caller polls a key nothing ever writes.
    #[test]
    fn top_level_id_is_honoured_when_no_request_id() {
        let c = cmd("wire-id", "acme", json!({}));
        let t = resolve(&c, "fallback").unwrap();
        assert_eq!(t.request_id, "wire-id");
        assert_eq!(t.key, "{acme}:res:wire-id");
    }

    #[test]
    fn no_id_at_all_yields_no_target() {
        assert!(resolve(&cmd("", "acme", json!({})), "fallback").is_none());
    }

    /// An empty _request_id is not an id. Treating it as one would produce
    /// `{site}:res:` — a key every such command would share.
    #[test]
    fn empty_request_id_falls_through_to_the_wire_id() {
        let c = cmd("wire-id", "acme", json!({"_request_id": ""}));
        let t = resolve(&c, "fallback").unwrap();
        assert_eq!(t.request_id, "wire-id");
    }

    #[test]
    fn non_string_request_id_falls_through() {
        let c = cmd("wire-id", "acme", json!({"_request_id": 12345}));
        assert_eq!(resolve(&c, "fallback").unwrap().request_id, "wire-id");
    }

    #[test]
    fn command_site_beats_the_fallback() {
        let c = cmd("i", "origin_site", json!({}));
        assert_eq!(resolve(&c, "serving_node").unwrap().key, "{origin_site}:res:i");
    }

    #[test]
    fn fallback_site_used_only_when_command_has_none() {
        let c = cmd("i", "", json!({}));
        assert_eq!(resolve(&c, "serving_node").unwrap().key, "{serving_node}:res:i");
    }

    /// The hash-tag braces are not decoration: they pin every per-site key to
    /// one cluster slot. A key built without them routes elsewhere.
    #[test]
    fn key_is_hash_tagged() {
        let t = resolve(&cmd("i", "acme", json!({})), "x").unwrap();
        assert!(t.key.starts_with("{acme}:"), "lost the hash tag: {}", t.key);
    }

    #[test]
    fn response_body_carries_exactly_the_client_visible_fields() {
        let r = Response {
            id: "i".into(),
            status: "success".into(),
            result: Some(json!({"n": 1})),
            error: None,
            timestamp: 1.5,
            batch_id: Some("b".into()),
            sequence: Some(3),
        };
        let v: serde_json::Value = serde_json::from_str(&response_json(&r)).unwrap();
        let obj = v.as_object().unwrap();
        let mut keys: Vec<&str> = obj.keys().map(String::as_str).collect();
        keys.sort();
        assert_eq!(keys, vec!["error", "id", "result", "status", "timestamp"]);
    }

    fn resp() -> Response {
        Response {
            id: "i".into(),
            status: "success".into(),
            result: None,
            error: None,
            timestamp: 0.0,
            batch_id: None,
            sequence: None,
        }
    }

    /// The lane-divergence guard. Both lanes derive their entire write from
    /// `plan` and then do nothing but SET, so a lane cannot pick a different
    /// key, body or TTL without deleting its call to this function — which
    /// this test would not catch, but a reviewer would, because the lane would
    /// have to grow back the twenty lines that were deleted for being copies.
    ///
    /// Pinned here so the contract is stated once, in executable form.
    #[test]
    fn plan_is_the_whole_contract() {
        let c = cmd("wire", "acme", json!({"_request_id": "poll"}));
        let p = plan(&c, "node", &resp()).unwrap();
        assert_eq!(p.key, "{acme}:res:poll");
        assert_eq!(p.request_id, "poll");
        assert_eq!(p.ttl_secs, 10);
        let v: serde_json::Value = serde_json::from_str(&p.json).unwrap();
        assert_eq!(v["status"], "success");
    }

    /// Every id shape a caller can send must reach a plan or a clean None —
    /// never a malformed key. `{site}:res:` with an empty id would be shared
    /// by every such command, so one caller would read another's reply.
    #[test]
    fn no_id_shape_produces_a_malformed_key() {
        let cases = vec![
            cmd("wire", "acme", json!({"_request_id": "poll"})),
            cmd("wire", "acme", json!({})),
            cmd("", "acme", json!({"_request_id": "poll"})),
            cmd("wire", "", json!({})),
            cmd("", "", json!({})),
            cmd("", "acme", json!({"_request_id": ""})),
            cmd("", "acme", json!({"_request_id": null})),
        ];
        for c in cases {
            if let Some(p) = plan(&c, "node", &resp()) {
                assert!(!p.request_id.is_empty(), "empty request_id for {:?}", c.id);
                assert!(!p.key.ends_with(":res:"), "malformed key {}", p.key);
                assert!(p.key.starts_with('{'), "lost the hash tag: {}", p.key);
            }
        }
    }
}
