//! Relay ACL policy engine
//!
//! Controls which services are allowed to communicate with which via relay.
//! Default policy is **Allow** — denial requires explicit policy rules.
//! This is practical for initial deployments: relay works out of the box,
//! and operators add deny rules as needed for security hardening.
//!
//! Policy rules are stored in a ValKey HASH:
//!   Key: `{topology_ns}:gnode:relay:policy`
//!   Field: `{source_site}:{target_site}` or `*:{target_site}` or `{source_site}:*`
//!   Value: JSON `{"action":"deny","reason":"...","commands":["*"]}`
//!
//! Evaluation order (first match wins):
//!   1. Exact pair: `source:target`
//!   2. Source wildcard: `*:target`
//!   3. Target wildcard: `source:*`
//!   4. Default: Allow

use log::{debug, info, warn};
use redis::Connection;
use serde_json::Value;

/// Result of a policy check
#[derive(Debug, Clone)]
pub enum PolicyDecision {
    /// Relay is allowed
    Allow,
    /// Relay is denied with a reason
    Deny(String),
}

impl PolicyDecision {
    pub fn is_allowed(&self) -> bool {
        matches!(self, PolicyDecision::Allow)
    }
}

/// Check whether a relay from source_site to target_site is permitted.
///
/// Evaluation order:
///   1. Exact match: `{source_site}:{target_site}`
///   2. Source wildcard: `*:{target_site}`
///   3. Target wildcard: `{source_site}:*`
///   4. Default: Allow
///
/// # Arguments
/// * `conn` — ValKey connection (gnode_daemon ACL)
/// * `topology_namespace` — Namespace for policy key construction
/// * `source_site` — Site originating the relay
/// * `target_site` — Site receiving the relay
/// * `command_name` — Command being relayed (for per-command policy)
/// * `debug_mode` — Verbose logging
pub fn check_relay_policy(
    conn: &mut Connection,
    topology_namespace: &str,
    source_site: &str,
    target_site: &str,
    command_name: &str,
    debug_mode: bool,
) -> PolicyDecision {
    let policy_key = format!("{{{}}}:gnode:relay:policy", topology_namespace);

    // Check the three patterns in priority order
    let patterns = [
        format!("{}:{}", source_site, target_site), // exact pair
        format!("*:{}", target_site),                // source wildcard
        format!("{}:*", source_site),                // target wildcard
    ];

    for pattern in &patterns {
        let result: redis::RedisResult<Option<String>> = redis::cmd("HGET")
            .arg(&policy_key)
            .arg(pattern)
            .query(conn);

        match result {
            Ok(Some(json_str)) => {
                match serde_json::from_str::<Value>(&json_str) {
                    Ok(rule) => {
                        let action = rule.get("action")
                            .and_then(|v| v.as_str())
                            .unwrap_or("allow");

                        if action == "deny" {
                            // Check if this deny applies to the specific command
                            if let Some(commands) = rule.get("commands").and_then(|v| v.as_array()) {
                                let applies = commands.iter().any(|c| {
                                    c.as_str().is_some_and(|s| s == "*" || s == command_name)
                                });
                                if !applies {
                                    if debug_mode {
                                        debug!(
                                            "Policy rule '{}' denies commands {:?} but not '{}', skipping",
                                            pattern,
                                            commands.iter().filter_map(|c| c.as_str()).collect::<Vec<_>>(),
                                            command_name
                                        );
                                    }
                                    continue;
                                }
                            }
                            // commands field absent or ["*"] → deny all

                            let reason = rule.get("reason")
                                .and_then(|v| v.as_str())
                                .unwrap_or("Denied by relay policy")
                                .to_string();

                            if debug_mode {
                                info!(
                                    "Relay DENIED by policy '{}': {} -> {} cmd='{}' reason='{}'",
                                    pattern, source_site, target_site, command_name, reason
                                );
                            }

                            return PolicyDecision::Deny(reason);
                        }
                        // action == "allow" → explicit allow, stop checking
                        if debug_mode {
                            debug!("Relay explicitly ALLOWED by policy '{}'", pattern);
                        }
                        return PolicyDecision::Allow;
                    }
                    Err(e) => {
                        warn!("Malformed relay policy rule for '{}': {}", pattern, e);
                        // Malformed rule → skip, continue checking
                        continue;
                    }
                }
            }
            Ok(None) => continue, // No rule for this pattern
            Err(e) => {
                // ValKey error → fail open (allow) with warning
                warn!("Policy check HGET failed for '{}': {} — defaulting to allow", pattern, e);
                return PolicyDecision::Allow;
            }
        }
    }

    // No matching rules found → default allow
    if debug_mode {
        debug!(
            "No relay policy rules for {} -> {}, defaulting to ALLOW",
            source_site, target_site
        );
    }
    PolicyDecision::Allow
}

/// Set a relay policy rule.
///
/// # Arguments
/// * `conn` — ValKey connection
/// * `topology_namespace` — Namespace for policy key
/// * `pattern` — Rule pattern (e.g., "site_a:site_b", "*:site_b", "site_a:*")
/// * `action` — "allow" or "deny"
/// * `reason` — Human-readable reason for the rule
/// * `commands` — List of commands this applies to, or ["*"] for all
pub fn set_relay_policy(
    conn: &mut Connection,
    topology_namespace: &str,
    pattern: &str,
    action: &str,
    reason: &str,
    commands: &[&str],
) -> Result<(), String> {
    let policy_key = format!("{{{}}}:gnode:relay:policy", topology_namespace);

    let rule = serde_json::json!({
        "action": action,
        "reason": reason,
        "commands": commands,
    });

    let rule_json = serde_json::to_string(&rule)
        .map_err(|e| format!("Failed to serialize policy rule: {}", e))?;

    redis::cmd("HSET")
        .arg(&policy_key)
        .arg(pattern)
        .arg(&rule_json)
        .query::<i64>(conn)
        .map_err(|e| format!("HSET relay policy failed: {}", e))?;

    info!("Relay policy set: {} = {} ({})", pattern, action, reason);
    Ok(())
}

/// Remove a relay policy rule.
pub fn remove_relay_policy(
    conn: &mut Connection,
    topology_namespace: &str,
    pattern: &str,
) -> Result<bool, String> {
    let policy_key = format!("{{{}}}:gnode:relay:policy", topology_namespace);

    let removed: i64 = redis::cmd("HDEL")
        .arg(&policy_key)
        .arg(pattern)
        .query(conn)
        .map_err(|e| format!("HDEL relay policy failed: {}", e))?;

    if removed > 0 {
        info!("Relay policy removed: {}", pattern);
    }
    Ok(removed > 0)
}

/// List all relay policy rules.
pub fn list_relay_policies(
    conn: &mut Connection,
    topology_namespace: &str,
) -> Result<Value, String> {
    let policy_key = format!("{{{}}}:gnode:relay:policy", topology_namespace);

    let result: redis::RedisResult<Vec<(String, String)>> = redis::cmd("HGETALL")
        .arg(&policy_key)
        .query(conn);

    match result {
        Ok(pairs) => {
            let mut rules = serde_json::Map::new();
            for (pattern, json_str) in pairs {
                match serde_json::from_str::<Value>(&json_str) {
                    Ok(rule) => { rules.insert(pattern, rule); }
                    Err(_) => { rules.insert(pattern, Value::String(json_str)); }
                }
            }
            Ok(Value::Object(rules))
        }
        Err(e) => Err(format!("HGETALL relay policy failed: {}", e)),
    }
}
