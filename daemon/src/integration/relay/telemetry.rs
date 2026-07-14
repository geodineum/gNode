//! Relay telemetry collection
//!
//! Collects per-relay metrics: source, target, command, latency, success/failure.
//! Metrics are aggregated in-memory per worker thread and flushed to ValKey
//! periodically (piggybacks on the 30s staleness check cycle).
//!
//! Storage key: `{topology_ns}:gnode:telemetry:relay` (HASH)
//!   Field: `{source_site}:{target_site}`
//!   Value: JSON `{"count":N,"ok":N,"err":N,"total_ms":N,"commands":{"cmd":N,...}}`
//!
//! The heatmap command reads this HASH to build the interaction matrix.

use log::{debug, info, warn};
use redis::Connection;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::time::Instant;

/// Metrics for a single relay operation (created at start, completed at end)
pub struct RelayMetrics {
    pub source_site: String,
    pub target_site: String,
    pub command: String,
    pub start_time: Instant,
    pub translated: bool,
    pub policy_checked: bool,
}

/// Per-pair aggregated counters (accumulated in-memory before flush)
#[derive(Debug, Clone, Default)]
struct PairCounters {
    count: u64,
    ok: u64,
    err: u64,
    total_ms: u64,
    translated: u64,
    commands: HashMap<String, u64>,
}

/// Thread-local relay telemetry collector.
/// Accumulates metrics in-memory and flushes to ValKey periodically.
pub struct RelayTelemetry {
    /// Aggregated counters per (source, target) pair
    pairs: HashMap<String, PairCounters>,
    /// Count of relay operations since last flush
    ops_since_flush: u64,
}

impl Default for RelayTelemetry {
    fn default() -> Self {
        Self::new()
    }
}

impl RelayTelemetry {
    pub fn new() -> Self {
        Self {
            pairs: HashMap::new(),
            ops_since_flush: 0,
        }
    }

    /// Record the start of a relay operation.
    /// Returns a `RelayMetrics` token to be completed later.
    pub fn record_start(
        source_site: &str,
        target_site: &str,
        command: &str,
    ) -> RelayMetrics {
        RelayMetrics {
            source_site: source_site.to_string(),
            target_site: target_site.to_string(),
            command: command.to_string(),
            start_time: Instant::now(),
            translated: false,
            policy_checked: false,
        }
    }

    /// Record the completion of a relay operation.
    pub fn record_complete(&mut self, metrics: RelayMetrics, success: bool) {
        let elapsed_ms = metrics.start_time.elapsed().as_millis() as u64;
        let pair_key = format!("{}:{}", metrics.source_site, metrics.target_site);

        let counters = self.pairs.entry(pair_key).or_default();
        counters.count += 1;
        if success {
            counters.ok += 1;
        } else {
            counters.err += 1;
        }
        if metrics.translated {
            counters.translated += 1;
        }
        counters.total_ms += elapsed_ms;
        *counters.commands.entry(metrics.command).or_insert(0) += 1;

        self.ops_since_flush += 1;
    }

    /// Flush accumulated metrics to ValKey.
    /// Called periodically from the staleness check loop (every 30s).
    /// Uses HGET + merge + HSET per pair to avoid losing data from other workers.
    pub fn flush(&mut self, conn: &mut Connection, topology_namespace: &str, debug_mode: bool) {
        if self.pairs.is_empty() {
            return;
        }

        let telemetry_key = format!("{{{}}}:gnode:telemetry:relay", topology_namespace);
        let flushing = self.ops_since_flush;

        for (pair_key, counters) in self.pairs.drain() {
            // Read existing counters for this pair (merge, don't overwrite)
            let existing: Option<String> = redis::cmd("HGET")
                .arg(&telemetry_key)
                .arg(&pair_key)
                .query(conn)
                .unwrap_or(None);

            let merged = match existing {
                Some(ref json_str) => merge_counters(json_str, &counters),
                None => counters_to_json(&counters),
            };

            let merged_str = serde_json::to_string(&merged).unwrap_or_else(|_| "{}".to_string());

            if let Err(e) = redis::cmd("HSET")
                .arg(&telemetry_key)
                .arg(&pair_key)
                .arg(&merged_str)
                .query::<i64>(conn)
            {
                warn!("Failed to flush relay telemetry for '{}': {}", pair_key, e);
            }
        }

        if debug_mode && flushing > 0 {
            debug!("Flushed {} relay telemetry operations to {}", flushing, telemetry_key);
        }

        self.ops_since_flush = 0;
    }

    /// Check if there are pending metrics to flush
    pub fn has_pending(&self) -> bool {
        !self.pairs.is_empty()
    }
}

/// Convert in-memory counters to a JSON Value
fn counters_to_json(c: &PairCounters) -> Value {
    json!({
        "count": c.count,
        "ok": c.ok,
        "err": c.err,
        "total_ms": c.total_ms,
        "translated": c.translated,
        "commands": c.commands,
    })
}

/// Merge in-memory counters with existing JSON from ValKey
fn merge_counters(existing_json: &str, new: &PairCounters) -> Value {
    match serde_json::from_str::<Value>(existing_json) {
        Ok(mut existing) => {
            // Add to existing numeric fields
            if let Some(obj) = existing.as_object_mut() {
                let old_count = obj.get("count").and_then(|v| v.as_u64()).unwrap_or(0);
                let old_ok = obj.get("ok").and_then(|v| v.as_u64()).unwrap_or(0);
                let old_err = obj.get("err").and_then(|v| v.as_u64()).unwrap_or(0);
                let old_total_ms = obj.get("total_ms").and_then(|v| v.as_u64()).unwrap_or(0);
                let old_translated = obj.get("translated").and_then(|v| v.as_u64()).unwrap_or(0);

                obj.insert("count".to_string(), json!(old_count + new.count));
                obj.insert("ok".to_string(), json!(old_ok + new.ok));
                obj.insert("err".to_string(), json!(old_err + new.err));
                obj.insert("total_ms".to_string(), json!(old_total_ms + new.total_ms));
                obj.insert("translated".to_string(), json!(old_translated + new.translated));

                // Merge command counters
                let mut commands: HashMap<String, u64> = obj.get("commands")
                    .and_then(|v| serde_json::from_value(v.clone()).ok())
                    .unwrap_or_default();
                for (cmd, count) in &new.commands {
                    *commands.entry(cmd.clone()).or_insert(0) += count;
                }
                obj.insert("commands".to_string(), json!(commands));
            }
            existing
        }
        Err(_) => counters_to_json(new), // Malformed existing → overwrite
    }
}

/// Read relay telemetry stats from ValKey.
/// Used by the `topology_heatmap` command and `relay_stats` diagnostics.
///
/// Returns a JSON object with interaction pairs and summary.
pub fn get_relay_stats(
    conn: &mut Connection,
    topology_namespace: &str,
    debug_mode: bool,
) -> Result<Value, String> {
    let telemetry_key = format!("{{{}}}:gnode:telemetry:relay", topology_namespace);

    let result: redis::RedisResult<Vec<(String, String)>> = redis::cmd("HGETALL")
        .arg(&telemetry_key)
        .query(conn);

    match result {
        Ok(pairs) => {
            let mut interaction_pairs = Vec::new();
            let mut total_relays: u64 = 0;
            let mut total_ok: u64 = 0;
            let mut total_err: u64 = 0;
            let mut total_translated: u64 = 0;

            for (pair_key, json_str) in &pairs {
                match serde_json::from_str::<Value>(json_str) {
                    Ok(val) => {
                        let count = val.get("count").and_then(|v| v.as_u64()).unwrap_or(0);
                        let ok = val.get("ok").and_then(|v| v.as_u64()).unwrap_or(0);
                        let err = val.get("err").and_then(|v| v.as_u64()).unwrap_or(0);
                        let total_ms = val.get("total_ms").and_then(|v| v.as_u64()).unwrap_or(0);
                        let translated = val.get("translated").and_then(|v| v.as_u64()).unwrap_or(0);
                        let avg_latency_ms = if count > 0 { total_ms / count } else { 0 };

                        // Split pair_key: "source:target"
                        let parts: Vec<&str> = pair_key.splitn(2, ':').collect();
                        let (source, target) = if parts.len() == 2 {
                            (parts[0], parts[1])
                        } else {
                            (pair_key.as_str(), "unknown")
                        };

                        interaction_pairs.push(json!({
                            "source": source,
                            "target": target,
                            "count": count,
                            "ok": ok,
                            "err": err,
                            "translated": translated,
                            "avg_latency_ms": avg_latency_ms,
                            "commands": val.get("commands").cloned().unwrap_or(json!({})),
                        }));

                        total_relays += count;
                        total_ok += ok;
                        total_err += err;
                        total_translated += translated;
                    }
                    Err(e) => {
                        if debug_mode {
                            info!("Skipping malformed telemetry entry '{}': {}", pair_key, e);
                        }
                    }
                }
            }

            Ok(json!({
                "pairs": interaction_pairs,
                "total_relays": total_relays,
                "total_ok": total_ok,
                "total_err": total_err,
                "total_translated": total_translated,
                "pair_count": interaction_pairs.len(),
            }))
        }
        Err(e) => Err(format!("Failed to read relay telemetry: {}", e)),
    }
}
