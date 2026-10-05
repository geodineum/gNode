//! The provider's latency for relayed work, measured where the work ends.
//!
//! A relayed command is run by the target SERVICE, so its round trip is the one
//! honest reading of how that service is doing — the sampler's source S1. It
//! used to be stamped the moment the daemon finished FORWARDING, which times an
//! XADD: 34 relays to a provider whose inference takes a minute summed to 43 ms.
//!
//! The trip starts on one worker and ends wherever the reply is read, which may
//! be another consumer group or another node, so nothing about it can live in a
//! thread. The departure is a key beside the relay's other state; the arrival
//! takes it with GETDEL, which makes the observation exactly-once however many
//! readers see the reply.
//!
//! Both ends are read off STREAM ENTRY IDS, not off any daemon's clock. The
//! forward's id is when ValKey accepted the command into the target's stream,
//! the reply's id is when it accepted the answer into the same stream. One
//! clock, the store's, and no share of the daemon's own read lag in the figure.

use log::{debug, warn};
use redis::Connection;
use std::collections::HashMap;

/// A relayed job may legitimately take most of a day (CPU inference does).
/// Past this the departure is forgotten and the trip goes unmeasured, which is
/// the right failure: an absent reading is `unknown`, never a fast one.
const DEPARTURE_TTL_SECS: usize = 86_400;

/// What the daemon knew when it forwarded, kept for the reply to claim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Departure {
    pub forwarded_ms: u64,
    pub target_site: String,
    pub target_entity: String,
    pub command: String,
}

impl Departure {
    /// The command is last, so one containing the separator still round-trips.
    pub fn encode(&self) -> String {
        format!("{}|{}|{}|{}", self.forwarded_ms, self.target_site, self.target_entity, self.command)
    }

    pub fn decode(raw: &str) -> Option<Self> {
        let mut parts = raw.splitn(4, '|');
        let forwarded_ms = parts.next()?.parse::<u64>().ok()?;
        let target_site = parts.next()?.to_string();
        let target_entity = parts.next()?.to_string();
        let command = parts.next()?.to_string();
        if target_site.is_empty() {
            return None;
        }
        Some(Self { forwarded_ms, target_site, target_entity, command })
    }

    /// The entity the work is credited to. A relay addressed to a site rather
    /// than to one of its entities is answered by the site's own profile
    /// entity, which is registered under the site id.
    pub fn provider(&self) -> &str {
        if self.target_entity.is_empty() { &self.target_site } else { &self.target_entity }
    }
}

/// Milliseconds from a stream entry id (`<ms>-<seq>`).
pub fn entry_ms(entry_id: &str) -> Option<u64> {
    entry_id.split('-').next()?.parse::<u64>().ok()
}

/// The reading one settled trip produces, or why it produces none.
#[derive(Debug, PartialEq, Eq)]
pub enum Settled {
    Observed { site: String, entity: String, command: String, elapsed_ms: u64, at_ms: u64 },
    /// The answer came from a stream other than the one the command went to.
    /// Crediting it would let one service write another's load.
    WrongReplier { expected: String, got: String },
    /// A reply id older than its own forward: not a duration.
    NotADuration,
}

/// Pure half of the arrival: no I/O, so every refusal is a unit test.
pub fn settle(departure: &Departure, replier_site: &str, reply_entry_id: &str) -> Settled {
    if replier_site != departure.target_site {
        return Settled::WrongReplier {
            expected: departure.target_site.clone(),
            got: replier_site.to_string(),
        };
    }
    match entry_ms(reply_entry_id) {
        Some(at_ms) if at_ms >= departure.forwarded_ms => Settled::Observed {
            site: departure.target_site.clone(),
            entity: departure.provider().to_string(),
            command: departure.command.clone(),
            elapsed_ms: at_ms - departure.forwarded_ms,
            at_ms,
        },
        _ => Settled::NotADuration,
    }
}

fn departure_key(topology_namespace: &str, correlation_id: &str) -> String {
    format!("{{{}}}:gnode:relay:sent:{}", topology_namespace, correlation_id)
}

/// Whether a reply's status counts as the provider having done the work.
pub fn reply_ok(fields: &HashMap<String, String>) -> bool {
    let status = fields.get("st").or_else(|| fields.get("status")).map(String::as_str).unwrap_or("ok");
    !matches!(status, "error" | "failed" | "refused")
}

/// Record that a command left for its target. Best-effort: a relay that cannot
/// be measured is still a relay.
pub fn note_departure(
    conn: &mut Connection,
    topology_namespace: &str,
    correlation_id: &str,
    forward_entry_id: &str,
    target_site: &str,
    target_entity: &str,
    command: &str,
) {
    if !crate::integration::sampler::observations_enabled() || correlation_id.is_empty() {
        return;
    }
    let Some(forwarded_ms) = entry_ms(forward_entry_id) else { return };
    let departure = Departure {
        forwarded_ms,
        target_site: target_site.to_string(),
        target_entity: target_entity.to_string(),
        command: command.to_string(),
    };
    let _: redis::RedisResult<()> = redis::cmd("SET")
        .arg(departure_key(topology_namespace, correlation_id))
        .arg(departure.encode())
        .arg("EX").arg(DEPARTURE_TTL_SECS)
        .query(conn);
}

/// A service's reply was read off `reply_stream`: close the trip it ends and
/// publish the provider's observation. Returns true when one was published.
pub fn note_arrival(
    conn: &mut Connection,
    topology_namespace: &str,
    reply_stream: &str,
    reply_entry_id: &str,
    fields: &HashMap<String, String>,
    debug_mode: bool,
) -> bool {
    if !crate::integration::sampler::observations_enabled() {
        return false;
    }
    let Some(correlation_id) = fields.get("ri").or_else(|| fields.get("id"))
        .map(String::as_str).filter(|s| !s.is_empty()) else { return false };
    let Some(replier_site) = crate::config::site_of_stream_key(reply_stream) else { return false };

    let raw: Option<String> = redis::cmd("GETDEL")
        .arg(departure_key(topology_namespace, correlation_id))
        .query(conn)
        .unwrap_or(None);
    let Some(departure) = raw.as_deref().and_then(Departure::decode) else { return false };

    match settle(&departure, replier_site, reply_entry_id) {
        Settled::Observed { site, entity, command, elapsed_ms, at_ms } => {
            let cmd = crate::integration::sampler::provider_observation_cmd(
                &site, &entity, &command, elapsed_ms, reply_ok(fields), at_ms);
            let _: redis::RedisResult<String> = cmd.query(conn);
            if debug_mode {
                debug!("Relay round trip {} -> {} '{}': {} ms", correlation_id, entity, command, elapsed_ms);
            }
            true
        }
        Settled::WrongReplier { expected, got } => {
            warn!("Relayed reply {} was sent to {} and answered from {} — not credited", correlation_id, expected, got);
            false
        }
        Settled::NotADuration => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dep(entity: &str) -> Departure {
        Departure { forwarded_ms: 1_000, target_site: "geodine".into(),
                    target_entity: entity.into(), command: "infer".into() }
    }

    #[test]
    fn a_departure_survives_its_own_encoding() {
        let d = dep("pipeline-runner");
        assert_eq!(Departure::decode(&d.encode()), Some(d));
    }

    #[test]
    fn a_command_containing_the_separator_still_round_trips() {
        let d = Departure { command: "odd|name".into(), ..dep("") };
        assert_eq!(Departure::decode(&d.encode()), Some(d));
    }

    #[test]
    fn a_record_without_a_target_is_not_a_departure() {
        assert_eq!(Departure::decode("1000||e|infer"), None);
        assert_eq!(Departure::decode("soon|geodine|e|infer"), None);
        assert_eq!(Departure::decode("1000|geodine"), None);
    }

    #[test]
    fn the_provider_is_the_target_entity_and_the_site_when_none_was_named() {
        assert_eq!(dep("pipeline-runner").provider(), "pipeline-runner");
        assert_eq!(dep("").provider(), "geodine");
    }

    #[test]
    fn the_trip_is_the_distance_between_two_entry_ids() {
        assert_eq!(settle(&dep(""), "geodine", "58730-0"), Settled::Observed {
            site: "geodine".into(), entity: "geodine".into(), command: "infer".into(),
            elapsed_ms: 57_730, at_ms: 58_730,
        });
    }

    /// The reading this module exists to stop producing: a forward is not a trip.
    #[test]
    fn it_is_the_providers_time_and_not_the_forwards() {
        let Settled::Observed { elapsed_ms, .. } = settle(&dep(""), "geodine", "58730-0") else { panic!() };
        assert!(elapsed_ms > 1_000, "a minute-long inference must not read as a millisecond");
    }

    #[test]
    fn an_answer_from_another_stream_credits_nobody() {
        assert_eq!(settle(&dep(""), "gflow", "58730-0"), Settled::WrongReplier {
            expected: "geodine".into(), got: "gflow".into(),
        });
    }

    #[test]
    fn a_reply_older_than_its_forward_is_not_a_duration() {
        assert_eq!(settle(&dep(""), "geodine", "999-0"), Settled::NotADuration);
        assert_eq!(settle(&dep(""), "geodine", "not-an-id"), Settled::NotADuration);
    }

    #[test]
    fn both_reply_spellings_report_failure() {
        let f = |k: &str, v: &str| HashMap::from([(k.to_string(), v.to_string())]);
        assert!(reply_ok(&f("st", "ok")));
        assert!(reply_ok(&f("status", "success")));
        assert!(!reply_ok(&f("st", "error")));
        assert!(!reply_ok(&f("status", "failed")));
        assert!(reply_ok(&HashMap::new()));
    }
}
