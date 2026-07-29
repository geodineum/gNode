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

use crate::tool_registration::{
    derive_profile_entity, find_schema_path, load_schema, register_services_for_site,
};
use crate::Result;

/// What a site declared about itself. Small on purpose: anything derivable
/// belongs in the derivation, not here.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistrationIntent {
    pub site: String,
    /// Capability profile: web | headless | service | system | component.
    pub profile: String,
    /// DTAP environment stamped into dim-20.
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
    apply: bool,
) -> Result<ReconcileReport> {
    let mut report = ReconcileReport::default();

    let intents = read_all(conn, topology_ns)?;
    if intents.is_empty() {
        return Ok(report);
    }

    let schema_path = match find_schema_path(None) {
        Some(p) => p,
        None => {
            warn!("registration reconcile: no service schema found — skipped");
            return Ok(report);
        }
    };
    let schema = load_schema(&schema_path)?;

    for intent in &intents {
        report.examined += 1;

        let derived = match derive_profile_entity(
            &intent.site,
            &intent.profile,
            Some(&intent.environment),
            &schema,
        ) {
            Ok(d) => d,
            Err(e) => {
                warn!(
                    "registration reconcile: cannot derive '{}' (profile={} env={}): {}",
                    intent.site, intent.profile, intent.environment, e
                );
                report.errors += 1;
                continue;
            }
        };

        // The bucket key is the fingerprint: it is computed from the discovery
        // prefix of the vector, so any dimension that moved or any value that
        // changed shows up here. Comparing it is cheaper and stricter than
        // comparing entity JSON, which also carries timestamps.
        let stored: redis::RedisResult<String> = redis::cmd("HGET")
            .arg(format!("{{{}}}:gnode:services:entities", intent.site))
            .arg(&intent.site)
            .query(conn);

        let stored_bk = stored.ok().and_then(|j| {
            serde_json::from_str::<serde_json::Value>(&j)
                .ok()
                .and_then(|v| v.get("bk").and_then(|b| b.as_str().map(String::from)))
        });

        let matches = stored_bk.as_deref() == Some(derived.bucket_key.as_str());
        if matches {
            continue;
        }

        report.drifted.push(intent.site.clone());
        info!(
            "registration drift: '{}' stored bk={} derived bk={} (profile={} env={} schema={})",
            intent.site,
            stored_bk.as_deref().unwrap_or("<absent>"),
            derived.bucket_key,
            intent.profile,
            intent.environment,
            schema.schema_version,
        );

        if !apply {
            continue;
        }

        match register_services_for_site(conn, &intent.site, std::slice::from_ref(&derived), "") {
            Ok((ok, err)) => {
                report.reasserted += ok;
                report.errors += err;
            }
            Err(e) => {
                warn!("registration reconcile: re-assert failed for '{}': {}", intent.site, e);
                report.errors += 1;
            }
        }
    }

    if report.drifted.is_empty() {
        debug!("registration reconcile: {} intents, all in agreement", report.examined);
    } else if apply {
        info!(
            "registration reconcile: {} of {} re-asserted ({} errors)",
            report.reasserted, report.examined, report.errors
        );
    } else {
        info!(
            "registration reconcile: {} of {} DRIFTED — reporting only, nothing written. \
             Set GNODE_RECONCILE_REGISTRATIONS=apply to enable re-assertion. Drifted: {:?}",
            report.drifted.len(), report.examined, report.drifted
        );
    }

    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tool_registration::load_schema;

    fn schema() -> crate::tool_registration::CapabilitySchema {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        load_schema(&root.join("config/service_schema.yaml")).unwrap()
    }

    #[test]
    fn derivation_is_deterministic() {
        // The whole design rests on this: same inputs, same entity. If it were
        // not, reconciliation would report drift forever and rewrite on every
        // pass.
        let s = schema();
        let a = derive_profile_entity("gflow", "service", Some("production"), &s).unwrap();
        let b = derive_profile_entity("gflow", "service", Some("production"), &s).unwrap();
        assert_eq!(a.bucket_key, b.bucket_key);
        assert_eq!(a.z_score, b.z_score);
    }

    #[test]
    fn profile_changes_the_vector() {
        // If these collided, registering a daemon as a website would be
        // undetectable — which is the defect this whole mechanism exists to
        // stop repeating.
        let s = schema();
        let web = derive_profile_entity("x", "web", Some("production"), &s).unwrap();
        let svc = derive_profile_entity("x", "service", Some("production"), &s).unwrap();
        assert_ne!(web.bucket_key, svc.bucket_key,
            "web and service must not derive the same vector");
    }

    #[test]
    fn environment_changes_the_vector() {
        let s = schema();
        let prod = derive_profile_entity("x", "service", Some("production"), &s).unwrap();
        let test = derive_profile_entity("x", "service", Some("testing"), &s).unwrap();
        assert_ne!(prod.bucket_key, test.bucket_key, "DTAP must be visible in the vector");
    }

    #[test]
    fn an_invalid_environment_is_refused_not_coerced() {
        let s = schema();
        assert!(derive_profile_entity("x", "service", Some("prod"), &s).is_err());
    }

    #[test]
    fn an_unknown_profile_is_refused_and_names_the_valid_ones() {
        let s = schema();
        let e = match derive_profile_entity("x", "srvice", Some("production"), &s) {
            Err(e) => e,
            Ok(_) => panic!("an unknown profile must not derive an entity"),
        };
        assert!(format!("{}", e).contains("available"), "must list valid profiles: {}", e);
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

    #[test]
    fn a_partial_intent_still_parses() {
        // Records written by an older version must not break reconciliation
        // for every other site — read_all skips only the unreadable one.
        let j = r#"{"site":"x","profile":"web","environment":"production"}"#;
        let i: RegistrationIntent = serde_json::from_str(j).unwrap();
        assert_eq!(i.declared_at, 0);
        assert!(i.declared_by.is_empty());
    }
}
