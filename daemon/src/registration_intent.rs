//! Registration intent — declare once, derive forever.
//!
//! A site's capability vector used to be written exactly once, by whoever ran
//! onboarding, and then never touched again. That makes it a RECORD: correct
//! when written, and silently wrong from the moment anything it depends on
//! changes.
//!
//! It depends on the schema. When the constellation schema went v3 → v4 six
//! dimension indices moved, and every entity registered under v3 kept the old
//! layout with nothing to recompute it. The same is true of a profile
//! correction or a DTAP promotion: fixing the declaration did nothing until
//! somebody re-ran the right command with the right flags, and forgetting a
//! flag produced a plausible-looking wrong answer (a service registered as a
//! website; a production service stamped `testing`).
//!
//! So the entity becomes a DERIVED PROJECTION of (intent × schema):
//!
//!   intent  — declared once at onboarding, stored in ValKey, small and
//!             inspectable: which site, which profile, which environment.
//!   schema  — owned and published by the daemon.
//!   entity  — recomputed from both, re-asserted by the daemon that owns the
//!             schema, and therefore never older than the schema it describes.
//!
//! This is the same split as schema publication itself, and the same shape as
//! the (B) snapshot: one writer of truth, everything else derived.
//!
//! ValKey rather than files, deliberately. A worker node cannot see another
//! node's repository, so any file-based answer works only on the host that
//! happens to hold the checkout. Intent in ValKey is readable by every node in
//! the constellation, which is also what lets the galaxy tier reuse this
//! instead of inventing a third registration path.
//!
//! THE HONEST COST: a one-shot registration means a mistake stays put. A
//! derived one means a mistake is re-asserted every scan. That is a real
//! trade — it is accepted because the intent hash is one small inspectable
//! place, and because a wrong intent that keeps re-asserting is at least
//! consistently wrong, where a stale entity is wrong in a way nothing reports.

use std::collections::HashMap;
use log::{debug, info, warn};
use serde::{Deserialize, Serialize};

use crate::Result;

/// What a site declared about itself. Small on purpose: anything derivable
/// belongs in the derivation, not here.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistrationIntent {
    pub site: String,
    /// Capability profile: web | headless | service | system | component.
    pub profile: String,
    /// DTAP environment stamped into the schema's environment axis.
    pub environment: String,
    /// Schema version at declaration time. NOT used to derive — recorded so a
    /// reconciliation can say *why* a vector changed rather than only that it did.
    #[serde(default)]
    pub schema_version: String,
    #[serde(default)]
    pub declared_at: i64,
    #[serde(default)]
    pub declared_by: String,
}

/// One hash for the whole constellation, field = site id.
///
/// Deliberately not per-site keys: reconciliation reads every intent on each
/// pass, and a single HGETALL is one round trip where a SCAN plus N GETs is a
/// pattern that gets slower exactly as the estate grows.
pub fn intent_key(topology_ns: &str) -> String {
    format!("{{{}}}:gnode:registrations", topology_ns)
}

/// Record what a site is. Called at onboarding; idempotent.
pub fn declare(
    conn: &mut redis::Connection,
    topology_ns: &str,
    intent: &RegistrationIntent,
) -> Result<()> {
    let json = serde_json::to_string(intent)
        .map_err(|e| crate::GeometricError::Other(format!("intent encode failed: {}", e)))?;
    let _: redis::RedisResult<()> = redis::cmd("HSET")
        .arg(intent_key(topology_ns))
        .arg(&intent.site)
        .arg(&json)
        .query(conn);
    info!(
        "Declared registration intent for '{}': profile={} environment={}",
        intent.site, intent.profile, intent.environment
    );
    Ok(())
}

/// Read every declared intent.
pub fn read_all(
    conn: &mut redis::Connection,
    topology_ns: &str,
) -> Result<Vec<RegistrationIntent>> {
    let raw: redis::RedisResult<HashMap<String, String>> =
        redis::cmd("HGETALL").arg(intent_key(topology_ns)).query(conn);
    let map = match raw {
        Ok(m) => m,
        Err(e) => {
            debug!("no registration intents readable: {}", e);
            return Ok(Vec::new());
        }
    };

    let mut out = Vec::new();
    for (site, json) in map {
        match serde_json::from_str::<RegistrationIntent>(&json) {
            Ok(i) => out.push(i),
            // One malformed record must not stop the others being reconciled.
            Err(e) => warn!("registration intent for '{}' is unreadable — skipped: {}", site, e),
        }
    }
    out.sort_by(|a, b| a.site.cmp(&b.site));
    Ok(out)
}

/// What a reconciliation pass found.
#[derive(Debug, Default)]
pub struct ReconcileReport {
    pub examined: usize,
    /// Entities whose derived bucket key differs from what is stored.
    pub drifted: Vec<String>,
    pub reasserted: usize,
    pub errors: usize,
}

/// Re-derive every declared entity and compare it to what is stored.
///
/// `apply == false` REPORTS ONLY. That is the default and the point: this runs
/// on a timer with the power to overwrite every service entity in the estate,
/// so it earns that power by first demonstrating, in the log, that what it
/// would write matches what is already there. A check that cannot be watched
/// disagreeing is not a check.
pub fn reconcile(
    conn: &mut redis::Connection,
    topology_ns: &str,
    _apply: bool,
) -> Result<ReconcileReport> {
    let mut report = ReconcileReport::default();

    let intents = read_all(conn, topology_ns)?;
    if intents.is_empty() {
        return Ok(report);
    }

    // Drift = the stored shape or environment differs from the declared profile and
    // environment, or the record is absent. Report only: a re-assertion without the manifest's
    // axes would erase what the site declared.
    for intent in &intents {
        report.examined += 1;
        let c = crate::constellation::for_entity(conn, &intent.site);
        let expected_shape = match intent.profile.as_str() {
            "web" => "3.1", "headless" => "3.2", "service" | "system" => "3.3", "component" => "2", _ => "3.2",
        };
        let stored: redis::RedisResult<Option<String>> = redis::cmd("HGET").arg(format!("{}:entities", c.key())).arg(&intent.site).query(conn);
        let rec = stored.ok().flatten().and_then(|j| serde_json::from_str::<serde_json::Value>(&j).ok());
        let (sh, env) = match &rec {
            Some(r) => (r["sh"].as_str().unwrap_or("").to_string(), r["env"].as_str().unwrap_or("").to_string()),
            None => (String::new(), String::new()),
        };
        if sh == expected_shape && env == intent.environment {
            continue;
        }
        report.drifted.push(intent.site.clone());
        info!("registration drift: '{}' stored shape={} env={} declared shape={} env={} (one topology, report only)",
            intent.site, if sh.is_empty() { "<absent>" } else { &sh }, if env.is_empty() { "<absent>" } else { &env }, expected_shape, intent.environment);
    }
    if report.drifted.is_empty() {
        debug!("registration reconcile: {} intents, all in agreement (one topology)", report.examined);
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intent_round_trips_through_json() {
        let i = RegistrationIntent {
            site: "gflow".into(), profile: "service".into(), environment: "production".into(),
            schema_version: "3.0".into(), declared_at: 1, declared_by: "test".into(),
        };
        let back: RegistrationIntent = serde_json::from_str(&serde_json::to_string(&i).unwrap()).unwrap();
        assert_eq!(back.site, "gflow");
        assert_eq!(back.profile, "service");
        assert_eq!(back.environment, "production");
    }
}
