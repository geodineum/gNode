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
    /// The shape word registered (site | app | daemon | component); empty means the profile's.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub shape: String,
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

/// The ecosystem components a catalogue registered (`register-tools --tier tool`), kept apart
/// from the sites and services so every reader of `intent_key` still lists only those.
pub fn component_intent_key(topology_ns: &str) -> String {
    format!("{{{}}}:gnode:registrations:components", topology_ns)
}

fn store(conn: &mut redis::Connection, key: &str, intent: &RegistrationIntent) -> Result<()> {
    let json = serde_json::to_string(intent)
        .map_err(|e| crate::GeometricError::Other(format!("intent encode failed: {}", e)))?;
    redis::cmd("HSET").arg(key).arg(&intent.site).arg(&json)
        .query::<i64>(conn).map_err(crate::GeometricError::Redis)?;
    Ok(())
}

/// Record what a site is. Called at onboarding; idempotent.
pub fn declare(
    conn: &mut redis::Connection,
    topology_ns: &str,
    intent: &RegistrationIntent,
) -> Result<()> {
    store(conn, &intent_key(topology_ns), intent)?;
    info!(
        "Declared registration intent for '{}': profile={} environment={}",
        intent.site, intent.profile, intent.environment
    );
    Ok(())
}

/// What a catalogue component is: shape 2, environment global.
pub fn component_intent(id: &str, declared_by: &str) -> RegistrationIntent {
    RegistrationIntent {
        site: id.to_string(), profile: "component".into(), environment: "global".into(), shape: "component".into(),
        schema_version: crate::integration::handlers::one_topology::schema().map(|s| s.version.clone()).unwrap_or_default(),
        declared_at: crate::utils::current_timestamp_ms() / 1000, declared_by: declared_by.to_string(),
    }
}

/// Record a catalogue component. Called by the tool tier after it registered; idempotent.
pub fn declare_component(conn: &mut redis::Connection, topology_ns: &str, intent: &RegistrationIntent) -> Result<()> {
    store(conn, &component_intent_key(topology_ns), intent)?;
    debug!("Declared component intent for '{}' in {{{}}}", intent.site, topology_ns);
    Ok(())
}

/// Read every declared site and service intent.
pub fn read_all(
    conn: &mut redis::Connection,
    topology_ns: &str,
) -> Result<Vec<RegistrationIntent>> {
    read_key(conn, &intent_key(topology_ns))
}

/// Read every declared component intent.
pub fn read_components(conn: &mut redis::Connection, topology_ns: &str) -> Result<Vec<RegistrationIntent>> {
    read_key(conn, &component_intent_key(topology_ns))
}

fn read_key(conn: &mut redis::Connection, key: &str) -> Result<Vec<RegistrationIntent>> {
    let raw: redis::RedisResult<HashMap<String, String>> =
        redis::cmd("HGETALL").arg(key).query(conn);
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

/// The shape key an intent is stored as: its shape word when it names one, else its profile's
/// (web a site, headless an app, every other profile a daemon, as `profile_params` registers it).
fn expected_shape(intent: &RegistrationIntent) -> String {
    if let Ok(k) = crate::integration::handlers::one_topology::shape_key(&intent.shape) {
        return k;
    }
    match intent.profile.as_str() { "web" => "3.1", "headless" => "3.2", _ => "3.3" }.to_string()
}

/// Drift = the stored shape or environment differs from the declared profile and
/// environment, or the record is absent. Returns the stored (shape, env) when drifted.
fn drift(intent: &RegistrationIntent, stored: Option<&serde_json::Value>) -> Option<(String, String)> {
    let (sh, env) = match stored {
        Some(r) => (r["sh"].as_str().unwrap_or("").to_string(), r["env"].as_str().unwrap_or("").to_string()),
        None => (String::new(), String::new()),
    };
    if sh == expected_shape(intent) && env == intent.environment {
        return None;
    }
    Some((sh, env))
}

fn report_drift(c: &Constellation, intent: &RegistrationIntent, sh: &str, env: &str) {
    let or_absent = |v: &str| if v.is_empty() { "<absent>".to_string() } else { v.to_string() };
    warn!("registration drift in {{{}}}: '{}' stored shape={} env={} declared shape={} env={}",
        c.name, intent.site, or_absent(sh), or_absent(env), expected_shape(intent), intent.environment);
}

/// Compare every intent declared in `c`, sites, services and components, with the entity
/// stored in `c`. Reports only: a re-assertion without the manifest's axes would erase what
/// the site declared.
pub fn reconcile(conn: &mut redis::Connection, c: &Constellation) -> Result<ReconcileReport> {
    let mut report = ReconcileReport::default();
    let mut intents = read_all(conn, &c.name)?;
    intents.extend(read_components(conn, &c.name)?);
    for intent in &intents {
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
        RegistrationIntent { site: site.into(), profile: profile.into(), environment: env.into(), shape: String::new(),
            schema_version: String::new(), declared_at: 0, declared_by: String::new() }
    }

    #[test]
    fn the_expected_shape_is_the_registered_one() {
        for (profile, key) in [("web", "3.1"), ("headless", "3.2"), ("service", "3.3"), ("system", "3.3"), ("component", "3.3")] {
            assert_eq!(expected_shape(&intent("x", profile, "production")), key, "profile {}", profile);
        }
        let mut i = intent("x", "service", "production");
        for (shape, key) in [("site", "3.1"), ("app", "3.2"), ("daemon", "3.3"), ("component", "2"), ("3.1", "3.1")] {
            i.shape = shape.into();
            assert_eq!(expected_shape(&i), key, "shape {}", shape);
        }
        i.shape = "not-a-shape".into();
        assert_eq!(expected_shape(&i), "3.3");
    }

    #[test]
    fn a_component_intent_is_shape_two_and_global() {
        let i = component_intent("gFlow", "test");
        assert!(drift(&i, Some(&serde_json::json!({ "sh": "2", "env": "global" }))).is_none());
        assert!(drift(&i, Some(&serde_json::json!({ "sh": "3.3", "env": "global" }))).is_some());
        assert!(drift(&i, None).is_some());
        assert_eq!(component_intent_key("geodineum"), "{geodineum}:gnode:registrations:components");
    }

    #[test]
    fn an_onboarding_record_reads_with_its_shape() {
        let raw = r#"{"site":"s","profile":"service","shape":"site","environment":"production","on":"aesir","declared_at":1,"declared_by":"onboard-service.sh@aesir"}"#;
        let i: RegistrationIntent = serde_json::from_str(raw).unwrap();
        assert_eq!(expected_shape(&i), "3.1");
        let legacy: RegistrationIntent = serde_json::from_str(r#"{"site":"s","profile":"web","environment":"production"}"#).unwrap();
        assert!(legacy.shape.is_empty());
        assert!(!serde_json::to_string(&legacy).unwrap().contains("shape"));
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
            site: "gflow".into(), profile: "service".into(), environment: "production".into(), shape: String::new(),
            schema_version: "3.0".into(), declared_at: 1, declared_by: "test".into(),
        };
        let back: RegistrationIntent = serde_json::from_str(&serde_json::to_string(&i).unwrap()).unwrap();
        assert_eq!(back.site, "gflow");
        assert_eq!(back.profile, "service");
        assert_eq!(back.environment, "production");
    }
}
