//! Delivery of a SERVICE's reply to a relayed command.
//!
//! The relay had a hole at exactly one point: the way home for a command the
//! daemon forwarded but did not execute.
//!
//! The daemon writes `{site}:res:{request_id}` for every command IT runs
//! (`command_processor`, via `response_key::plan`). A relayed command is run by
//! the target SERVICE — Geodine's pipeline runner, gFlow's stream consumer —
//! and that service cannot write the origin's res key: the origin lives in a
//! foreign keyspace its ACL refuses by design (SERVICE_ONBOARDING §0). So the
//! reply had no carrier. `RelayTracker::match_response` was written for this and
//! never wired to a caller; the origin polled `{origin}:res:{id}` until timeout.
//!
//! The carrier is this module. A service answers on its OWN stream with the
//! ordinary response shape plus the `_rr` it was given:
//!
//! ```text
//! XADD {gflow}:gnode:unified:production *
//!      t r  ri <request_id>  st ok  r <json result>
//!      ss gflow  sn <node>  ts <ms>
//!      _rr {gschedule}:gnode:unified:production
//! ```
//!
//! and the daemon — already reading that stream — turns it into the keyed reply
//! the origin is polling for. Own-namespace write by the service, cross-namespace
//! write by the daemon, which is the only actor entitled to make it.
//!
//! **Res key only, deliberately.** The daemon does not also XADD the response
//! onto the origin's stream. `SERVICE_ONBOARDING` §5 defines the reply contract
//! as "poll `{yoursite}:res:<uuid>`", every audited caller polls exactly that,
//! and a `SET` is idempotent — which matters because two consumer groups
//! (`gnode-daemon` and `gnode-workers`) read the same stream and may both
//! deliver. A duplicated XADD would be two entries; a duplicated SET is one key.

use log::{debug, info, warn};
use redis::Connection;
use std::collections::HashMap;

/// How long a carried-home reply survives.
///
/// Deliberately longer than `response_key::RESPONSE_TTL_SECS` (10s), which is
/// sized for a client polling in a tight loop right after it sent the command.
/// A relayed reply is awaited by a SERVICE on its own cadence — gFlow settles
/// inference on an executor tick measured in tens of seconds, and an inference
/// reply can arrive minutes after the request. At 10s those answers expire
/// between arriving and being read, which looks exactly like the daemon never
/// delivering them. Still ephemeral, still a rendezvous, just one that outlives
/// a slow poller. Correlation ids are UUIDs, so a long-lived key cannot be
/// picked up by a later request.
const RELAYED_REPLY_TTL_SECS: usize = 300;

/// The site a relayed reply belongs to, read off the reply-to stream key.
///
/// `_rr` is a stream key the daemon itself set when forwarding
/// (`{gschedule}:gnode:unified:production`), so the hash tag is the origin site.
/// Returns None for anything not shaped like a tagged stream key rather than
/// guessing — a wrong site here would write the reply to a key no one reads,
/// which is indistinguishable from the bug this module exists to fix.
pub fn origin_site_from_reply_stream(reply_stream: &str) -> Option<&str> {
    let rest = reply_stream.strip_prefix('{')?;
    let (site, tail) = rest.split_once('}')?;
    if site.is_empty() || !tail.starts_with(':') {
        return None;
    }
    Some(site)
}

/// Build the reply body. Identical in shape to `response_key::response_json`,
/// because a poller must not be able to tell which lane answered it.
///
/// `r` arrives as a JSON string on the wire. Parsed back where possible so the
/// result nests as a real value; a non-JSON payload passes through as a string
/// rather than being dropped.
fn reply_json(request_id: &str, status: &str, result: Option<&String>, error: Option<&String>) -> String {
    let result_value = match result {
        Some(raw) => serde_json::from_str::<serde_json::Value>(raw)
            .unwrap_or_else(|_| serde_json::Value::String(raw.clone())),
        None => serde_json::Value::Null,
    };
    serde_json::json!({
        "id": request_id,
        "status": status,
        "result": result_value,
        "error": error,
        "timestamp": crate::integration::processor::stream_utils::current_timestamp(),
    })
    .to_string()
}

/// True when these fields are a service reply that needs carrying home.
///
/// `_rr` marks it as relayed. The rest is discriminating a reply from the
/// relayed COMMAND that shares the stream and the field — forwarding that as a
/// reply would answer the origin with an echo of its own request.
///
/// There are TWO response spellings live in this estate and both have to pass:
///
///   - the daemon's own: `t=r`, status in `st`
///   - Geodine's runner: no `t` at all, status in `status`
///     (`writeResponse` XADDs the handler's array verbatim)
///
/// So "is a response" cannot be `t == "r"` alone — that predicate silently
/// ignores every Geodine reply, which is most of the relayed traffic. The test
/// used here is the one both gFlow's client and Geodine's runner already use to
/// tell the two apart on a shared stream: a non-empty command field means
/// command, a status field means response.
pub fn is_relayed_service_reply(fields: &HashMap<String, String>) -> bool {
    let relayed = fields.get("_rr").is_some_and(|s| !s.is_empty());
    if !relayed {
        return false;
    }

    let msg_type = fields.get("t").map(String::as_str).unwrap_or("");
    if msg_type == "c" || msg_type == "bc" {
        return false;
    }

    // Geodine's discriminator, and the reason its own runner does not read its
    // requests back as answers: a non-empty command field IS a command.
    let carries_command = ["c", "cmd", "command"]
        .iter()
        .any(|k| fields.get(*k).is_some_and(|v| !v.is_empty()));
    if carries_command {
        return false;
    }

    msg_type == "r"
        || msg_type == "br"
        || fields.contains_key("st")
        || fields.contains_key("status")
}

/// Write a service's reply to the origin's polling key.
///
/// Best-effort and never fatal: a reply that cannot be delivered leaves the
/// origin to time out, which is the behaviour before this module existed. It
/// must not take down the stream loop that found it.
///
/// Returns true when a key was written.
pub fn deliver_service_reply(
    conn: &mut Connection,
    fields: &HashMap<String, String>,
    debug_mode: bool,
) -> bool {
    let reply_stream = match fields.get("_rr") {
        Some(s) if !s.is_empty() => s,
        _ => return false,
    };

    let site = match origin_site_from_reply_stream(reply_stream) {
        Some(s) => s,
        None => {
            warn!("Relayed reply carries an unparseable _rr ({reply_stream}) — cannot address the origin");
            return false;
        }
    };

    // `ri` is where a response carries its correlation id (resp3_protocol).
    // `id` is the fallback for a service that put it at the top level; the
    // empty-string filter is the same one response_key::resolve needs, for the
    // same reason — `{site}:res:` would be one key shared by every reply.
    let request_id = fields
        .get("ri")
        .or_else(|| fields.get("id"))
        .map(String::as_str)
        .filter(|s| !s.is_empty());

    let request_id = match request_id {
        Some(id) => id,
        None => {
            warn!("Relayed reply for {site} carries no request id (ri/id) — nothing to key the reply on");
            return false;
        }
    };

    // Both spellings again, on every field: `st`/`r`/`e` from the daemon's
    // encoding, `status`/`result`/`error` from a service writing plain JSON.
    let status = fields
        .get("st")
        .or_else(|| fields.get("status"))
        .map(String::as_str)
        .unwrap_or("ok");
    let result = fields.get("r").or_else(|| fields.get("result"));
    let error = fields.get("e").or_else(|| fields.get("error"));

    let body = reply_json(request_id, status, result, error);
    let key = format!("{{{site}}}:res:{request_id}");

    let keyed = match redis::cmd("SET")
        .arg(&key)
        .arg(&body)
        .arg("EX")
        .arg(RELAYED_REPLY_TTL_SECS)
        .query::<()>(conn)
    {
        Ok(()) => {
            info!("Delivered relayed service reply to {key} (status={status})");
            true
        }
        Err(e) => {
            warn!("Failed to deliver relayed service reply to {key}: {e}");
            if debug_mode {
                debug!("Undelivered reply body: {body}");
            }
            false
        }
    };

    forward_to_origin_stream(conn, reply_stream, site, request_id, fields, debug_mode);

    keyed
}

/// Put the reply on the origin's unified stream as well — the durable half.
///
/// The keyed reply above is a rendezvous: it expires, and it is only read if
/// the origin is up and polling when it lands. That is fine for a command that
/// answers in milliseconds and wrong for one that answers in hours. gFlow
/// dispatches CPU inference that can run most of a day, gets restarted by its
/// own deploy trigger on any `engine/**` change, and would simply lose an
/// answer that arrived during the restart — hours of compute, silently gone.
///
/// The unified stream is never trimmed, so an entry there is recoverable on any
/// later tick. This is not a new pattern: it is exactly how gFlow recovers
/// today by scanning Geodine's stream (`docs/ASYNC-CORRELATION.md`, a 24h
/// window read forward from the oldest pending lease). Moving gFlow onto its
/// own identity takes that scan away — it will no longer hold the credential
/// that reads Geodine's keyspace — so the same durability has to arrive in its
/// OWN namespace instead. This is that.
///
/// Best-effort: a failed forward leaves the keyed reply, which is what existed
/// before. Never fatal to the stream loop.
fn forward_to_origin_stream(
    conn: &mut Connection,
    reply_stream: &str,
    site: &str,
    request_id: &str,
    fields: &HashMap<String, String>,
    debug_mode: bool,
) {
    // Exactly-once, cheaply. Both consumer groups (`gnode-daemon` and
    // `gnode-workers`) read every entry, so both reach this function for the
    // same reply. A duplicated SET is one key; a duplicated XADD is two stream
    // entries the origin has to reconcile. NX on a marker beside the reply
    // makes the first caller win and the second do nothing.
    let marker = format!("{{{site}}}:res:{request_id}:fwd");
    let claimed: Option<String> = redis::cmd("SET")
        .arg(&marker)
        .arg("1")
        .arg("NX")
        .arg("EX")
        .arg(RELAYED_REPLY_TTL_SECS)
        .query(conn)
        .unwrap_or(None);
    if claimed.is_none() {
        if debug_mode {
            debug!("Reply {request_id} already forwarded to {reply_stream} — skipping duplicate");
        }
        return;
    }

    // `_rr` MUST NOT survive the hop. The daemon reads the origin's stream too;
    // an entry there still carrying `_rr` looks like another relayed reply and
    // gets forwarded again, to the same stream, forever. `_rf` replaces it as
    // an inert breadcrumb saying where this came from.
    let mut pairs: Vec<(String, String)> = fields
        .iter()
        .filter(|(k, _)| k.as_str() != "_rr")
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    pairs.push(("_rf".to_string(), reply_stream.to_string()));

    match redis::cmd("XADD")
        .arg(reply_stream)
        .arg("*")
        .arg(&pairs)
        .query::<String>(conn)
    {
        Ok(msg_id) => info!("Forwarded relayed reply {request_id} to {reply_stream} ({msg_id})"),
        Err(e) => {
            // Release the claim: the reply was not actually forwarded, so the
            // other group should be allowed to try rather than inheriting a
            // marker that says the job is done.
            let _ = redis::cmd("DEL").arg(&marker).query::<i64>(conn);
            warn!("Failed to forward relayed reply {request_id} to {reply_stream}: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fields(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    #[test]
    fn origin_site_parses_from_tagged_stream_key() {
        assert_eq!(
            origin_site_from_reply_stream("{gschedule}:gnode:unified:production"),
            Some("gschedule")
        );
        assert_eq!(
            origin_site_from_reply_stream("{nierto_com}:gnode:unified:testing"),
            Some("nierto_com")
        );
    }

    /// An untagged key is a DIFFERENT key. Guessing a site from one would write
    /// the reply somewhere nobody polls — the silent failure this module fixes.
    #[test]
    fn origin_site_refuses_untagged_or_malformed() {
        assert_eq!(origin_site_from_reply_stream("gschedule:gnode:unified:production"), None);
        assert_eq!(origin_site_from_reply_stream("{}:gnode:unified:production"), None);
        assert_eq!(origin_site_from_reply_stream("{gschedule}"), None);
        assert_eq!(origin_site_from_reply_stream(""), None);
    }

    #[test]
    fn recognises_a_relayed_reply() {
        assert!(is_relayed_service_reply(&fields(&[
            ("t", "r"),
            ("_rr", "{gschedule}:gnode:unified:production"),
        ])));
    }

    /// The relayed command itself carries `_rr` too. Treating it as a reply
    /// would answer the origin with an echo of its own request.
    #[test]
    fn a_relayed_command_is_not_a_reply() {
        assert!(!is_relayed_service_reply(&fields(&[
            ("t", "c"),
            ("c", "start_workflow"),
            ("_rr", "{gschedule}:gnode:unified:production"),
        ])));
    }

    /// Geodine's runner writes no `t` at all — `writeResponse` XADDs the
    /// handler's array verbatim, so the reply is identified by `status`.
    /// Requiring `t=r` here would ignore most of the relayed traffic in the
    /// estate while every test still passed.
    #[test]
    fn recognises_geodines_untyped_reply() {
        assert!(is_relayed_service_reply(&fields(&[
            ("id", "cid-9"),
            ("status", "ok"),
            ("result", r#"{"text":"hello"}"#),
            ("_rr", "{gflow}:gnode:unified:production"),
        ])));
    }

    /// ...and its REQUEST, which shares that stream and carries no `t` either,
    /// must not be mistaken for one. `cmd` non-empty is the discriminator both
    /// gFlow's client and Geodine's runner already use.
    #[test]
    fn geodines_untyped_request_is_not_a_reply() {
        assert!(!is_relayed_service_reply(&fields(&[
            ("id", "cid-9"),
            ("cmd", "infer"),
            ("params", r#"{"prompt":"hi"}"#),
            ("_rr", "{gflow}:gnode:unified:production"),
        ])));
    }

    /// A service that answers in the plain spelling must have every field
    /// picked up, not just the status.
    #[test]
    fn reply_body_reads_the_plain_spelling() {
        let f = fields(&[
            ("id", "cid-10"),
            ("status", "error"),
            ("error", "pipeline unavailable"),
            ("_rr", "{gflow}:gnode:unified:production"),
        ]);
        let status = f.get("st").or_else(|| f.get("status")).map(String::as_str).unwrap_or("ok");
        let body = reply_json("cid-10", status, f.get("r").or_else(|| f.get("result")),
                              f.get("e").or_else(|| f.get("error")));
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["status"], "error");
        assert_eq!(v["error"], "pipeline unavailable");
    }

    /// The forward strips `_rr` and leaves `_rf`. If that ever regresses, the
    /// entry we just wrote to the origin's stream reads as another relayed
    /// reply on the next pass and is forwarded to the same stream again —
    /// an unbounded loop on a stream that is never trimmed.
    #[test]
    fn a_forwarded_reply_cannot_be_forwarded_again() {
        assert!(!is_relayed_service_reply(&fields(&[
            ("t", "r"),
            ("ri", "cid-1"),
            ("st", "ok"),
            ("_rf", "{gschedule}:gnode:unified:production"),
        ])));
    }

    #[test]
    fn a_local_reply_is_not_relayed() {
        assert!(!is_relayed_service_reply(&fields(&[("t", "r"), ("st", "ok")])));
        assert!(!is_relayed_service_reply(&fields(&[("t", "r"), ("_rr", "")])));
    }

    #[test]
    fn reply_body_nests_a_json_result() {
        let body = reply_json(
            "cid-1",
            "ok",
            Some(&r#"{"instance_id":"abc","status":"active"}"#.to_string()),
            None,
        );
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["status"], "ok");
        assert_eq!(v["id"], "cid-1");
        assert_eq!(v["result"]["instance_id"], "abc");
        assert!(v["error"].is_null());
    }

    /// A service that answers with a bare string must not have its reply
    /// swallowed by a parse failure.
    #[test]
    fn reply_body_passes_through_a_non_json_result() {
        let body = reply_json("cid-2", "ok", Some(&"plain text".to_string()), None);
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["result"], "plain text");
    }

    #[test]
    fn reply_body_carries_an_error() {
        let body = reply_json("cid-3", "error", None, Some(&"no published version".to_string()));
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["status"], "error");
        assert_eq!(v["error"], "no published version");
        assert!(v["result"].is_null());
    }
}
