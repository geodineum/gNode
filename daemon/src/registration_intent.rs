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

use crate::constellation::Constellation;
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

/// The shape a declared profile is stored as.
fn expected_shape(profile: &str) -> &'static str {
    match profile {
        "web" => "3.1", "headless" => "3.2", "service" | "system" => "3.3", "component" => "2", _ => "3.2",
    }
}

/// Drift = the stored shape or environment differs from the declared profile and
/// environment, or the record is absent. Returns the stored (shape, env) when drifted.
fn drift(intent: &RegistrationIntent, stored: Option<&serde_json::Value>) -> Option<(String, String)> {
    let (sh, env) = match stored {
        Some(r) => (r["sh"].as_str().unwrap_or("").to_string(), r["env"].as_str().unwrap_or("").to_string()),
        None => (String::new(), String::new()),
    };
    if sh == expected_shape(&intent.profile) && env == intent.environment {
        return None;
    }
    Some((sh, env))
}

fn report_drift(c: &Constellation, intent: &RegistrationIntent, sh: &str, env: &str) {
    let or_absent = |v: &str| if v.is_empty() { "<absent>".to_string() } else { v.to_string() };
    warn!("registration drift in {{{}}}: '{}' stored shape={} env={} declared shape={} env={}",
        c.name, intent.site, or_absent(sh), or_absent(env), expected_shape(&intent.profile), intent.environment);
}

/// Compare every intent declared in `c` with the entity stored in `c`. Reports only:
/// a re-assertion without the manifest's axes would erase what the site declared.
pub fn reconcile(conn: &mut redis::Connection, c: &Constellation) -> Result<ReconcileReport> {
    let mut report = ReconcileReport::default();
    for intent in &read_all(conn, &c.name)? {
        report.examined += 1;
        let stored: redis::RedisResult<Option<String>> = redis::cmd("HGET").arg(format!("{}:entities", c.key())).arg(&intent.site).query(conn);
        let rec = stored.ok().flatten().and_then(|j| serde_json::from_str::<serde_json::Value>(&j).ok());
        if let Some((sh, env)) = drift(intent, rec.as_ref()) {
            report.drifted.push(intent.site.clone());
            report_drift(c, intent, &sh, &env);
        }
    }
    if report.examined > 0 && report.drifted.is_empty() {
        debug!("registration reconcile {{{}}}: {} intents, all in agreement", c.name, report.examined);
    }
    Ok(report)
}

/// One pass per served constellation; a failed one does not stop the rest.
pub fn reconcile_served(conn: &mut redis::Connection, served: &[Constellation]) -> Vec<(String, Result<ReconcileReport>)> {
    served.iter().map(|c| (c.name.clone(), reconcile(conn, c))).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static CAPTURED: Mutex<Vec<(log::Level, String)>> = Mutex::new(Vec::new());
    struct Capture;
    impl log::Log for Capture {
        fn enabled(&self, _: &log::Metadata) -> bool { true }
        fn log(&self, r: &log::Record) {
            if let Ok(mut v) = CAPTURED.lock() { v.push((r.level(), r.args().to_string())); }
        }
        fn flush(&self) {}
    }
    static LOGGER: Capture = Capture;

    fn intent(site: &str, profile: &str, env: &str) -> RegistrationIntent {
        RegistrationIntent { site: site.into(), profile: profile.into(), environment: env.into(),
            schema_version: String::new(), declared_at: 0, declared_by: String::new() }
    }

    #[test]
    fn drift_is_shape_or_environment_or_absence() {
        let i = intent("gflow", "service", "production");
        assert!(drift(&i, Some(&serde_json::json!({ "sh": "3.3", "env": "production" }))).is_none());
        assert_eq!(drift(&i, Some(&serde_json::json!({ "sh": "3.2", "env": "production" }))), Some(("3.2".into(), "production".into())));
        assert_eq!(drift(&i, Some(&serde_json::json!({ "sh": "3.3", "env": "testing" }))), Some(("3.3".into(), "testing".into())));
        assert_eq!(drift(&i, None), Some((String::new(), String::new())));
    }

    #[test]
    fn drift_logs_at_warn() {
        let _ = log::set_logger(&LOGGER);
        log::set_max_level(log::LevelFilter::Trace);
        report_drift(&Constellation::new("coursematch"), &intent("drift-probe-site", "web", "production"), "", "");
        let got: Vec<_> = CAPTURED.lock().unwrap().iter().filter(|(_, m)| m.contains("drift-probe-site")).cloned().collect();
        assert_eq!(got.len(), 1, "{:?}", got);
        assert_eq!(got[0].0, log::Level::Warn);
        assert!(got[0].1.contains("{coursematch}") && got[0].1.contains("<absent>"), "{}", got[0].1);
    }

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
