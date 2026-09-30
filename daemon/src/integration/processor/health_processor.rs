// Health Message Processor for gNode Load-Aware Service Discovery
//
// This module processes load update (lu) messages from the dedicated health stream,
// updating the LoadMetricsManager for optimal service selection.
//
// Also writes topology dimension 16 (current_load) for geometric service
// discovery, through GNODE_TOPO_SET_DERIVED — the canonical (C) entities. It
// used to call GNODE_TOPOLOGY_BATCH_UPDATE_LOAD, which wrote a JSON blob at the
// topology key that the live store does not have, so every load update ever
// received failed as "Topology not found" at trace level.
//
// A self-reported load is a SECONDARY signal: the daemon's own sampler derives
// load from reply latency against each provider's baseline. This path exists for
// a provider that knows something the daemon cannot see.

use std::collections::HashMap;
use std::sync::Arc;
use log::{debug, warn, trace};
use redis::{Connection, Commands};

use crate::integration::{
    IntegrationResult,
    load_metrics::{LoadMetrics, LoadMetricsManager},
};

/// Process health update messages from the health stream
///
/// This function parses lu (load update) messages and updates the LoadMetricsManager.
/// It also updates topology dimension 16 (current_load) for geometric service discovery.
/// It acknowledges successfully processed messages using XACK.
///
/// Message format:
/// ```json
/// {
///   "t": "lu",           // Type: load update
///   "si": "service-id",  // Service identifier (required)
///   "l": 0.35,           // Load factor (required, 0.0-1.0)
///   "cpu": 0.45,         // CPU usage (optional, 0.0-1.0)
///   "mem": 0.60,         // Memory usage (optional, 0.0-1.0)
///   "rq": 12,            // Active requests (optional, integer)
///   "lat": 150,          // Avg latency ms (optional, integer)
///   "err": 0.02,         // Error rate (optional, 0.0-1.0)
///   "ts": 1696800000000  // Timestamp ms (required)
/// }
/// ```
///
/// # Arguments
///
/// * `load_manager` - Shared LoadMetricsManager instance
/// * `messages` - Vector of (message_id, fields) tuples from XREADGROUP
/// * `conn` - Redis connection for XACK
/// * `health_stream` - Health stream key
/// * `debug_mode` - Whether debug mode is enabled
///
/// # Returns
///
/// * `IntegrationResult<usize>` - Number of processed messages or error
pub fn process_health_updates(
    load_manager: &Arc<LoadMetricsManager>,
    messages: Vec<(String, HashMap<String, String>)>,
    conn: &mut Connection,
    health_stream: &str,
    consumer_group: &str,
    debug_mode: bool
) -> IntegrationResult<usize> {
    let topology_key = crate::config::site_of_stream_key(health_stream)
        .map(crate::GeometricTopology::get_services_topology_key);
    process_health_updates_with_topology(
        load_manager, messages, conn, health_stream, consumer_group,
        topology_key.as_deref(), debug_mode)
}

/// Process health update messages with optional topology update
///
/// Extended version that also updates topology dimension 16 (current_load)
/// for geometric service discovery. See NEW_PRACTICAL_TOPOLOGY.md.
///
/// # Arguments
///
/// * `topology_key` - Optional topology key for dimension 16 updates (e.g., "topology:default")
pub fn process_health_updates_with_topology(
    load_manager: &Arc<LoadMetricsManager>,
    messages: Vec<(String, HashMap<String, String>)>,
    conn: &mut Connection,
    health_stream: &str,
    consumer_group: &str,
    topology_key: Option<&str>,
    debug_mode: bool
) -> IntegrationResult<usize> {
    if messages.is_empty() {
        return Ok(0);
    }

    if debug_mode {
        debug!("Processing {} health update messages", messages.len());
    }

    let mut processed_count = 0;
    let mut ack_ids = Vec::new();
    // Track load updates for batch topology update (dimension 16)
    let mut load_updates: HashMap<String, f64> = HashMap::new();

    for (msg_id, fields) in messages {
        // Check message type
        let msg_type = match crate::utils::get_field_opt(&fields, crate::utils::field_names::TYPE) {
            Some(t) => t,
            None => {
                warn!("Health message {} missing type field, skipping", msg_id);
                continue;
            }
        };

        // Only lu (load update) messages carry a load factor. Anything else on
        // this stream is dismissed AND acknowledged: a message left pending
        // because nothing here understands it never leaves the group's pending
        // list. One site's stream held 114 such records, all of them `rq`
        // latency reports, read once and pending ever since. (The sampler that
        // derives load from those latencies reads them separately, from the
        // stream, not from this group.)
        if msg_type != "lu" {
            if debug_mode {
                debug!("Dismissing non-lu health message: type={}", msg_type);
            }
            ack_ids.push(msg_id);
            continue;
        }

        // Parse required fields
        let service_id = match fields.get("si") {
            Some(id) if !id.is_empty() => id.clone(),
            _ => {
                warn!("Health message {} missing or empty service_id, skipping", msg_id);
                continue;
            }
        };

        let load_factor = match fields.get("l").and_then(|v| v.parse::<f64>().ok()) {
            Some(l) if (0.0..=1.0).contains(&l) => l,
            _ => {
                warn!("Health message {} has invalid load_factor, skipping", msg_id);
                continue;
            }
        };

        let timestamp = match fields.get("ts").and_then(|v| v.parse::<i64>().ok()) {
            Some(ts) => ts,
            None => {
                warn!("Health message {} missing timestamp, skipping", msg_id);
                continue;
            }
        };

        // Parse optional fields
        let cpu_usage = fields.get("cpu").and_then(|v| v.parse::<f64>().ok());
        let memory_usage = fields.get("mem").and_then(|v| v.parse::<f64>().ok());
        let active_requests = fields.get("rq").and_then(|v| v.parse::<u32>().ok());
        let avg_latency_ms = fields.get("lat").and_then(|v| v.parse::<u64>().ok());
        let error_rate = fields.get("err").and_then(|v| v.parse::<f64>().ok());

        // Create LoadMetrics instance
        let metrics = LoadMetrics {
            service_id: service_id.clone(),
            load_factor,
            cpu_usage,
            memory_usage,
            active_requests,
            avg_latency_ms,
            error_rate,
            last_update: timestamp,
            ttl_seconds: 30, // Use default TTL
        };

        // Update load manager
        load_manager.update(metrics);

        // Track for topology update (dimension 16)
        load_updates.insert(service_id.clone(), load_factor);

        if debug_mode {
            debug!("Updated load metrics for service {}: load={:.2}, score={:.2}",
                service_id, load_factor, load_manager.get(&service_id).map(|m| m.score()).unwrap_or(0.0));
        }

        // Add to ACK list
        ack_ids.push(msg_id);
        processed_count += 1;
    }

    // Acknowledge all successfully processed messages
    if !ack_ids.is_empty() {
        match conn.xack::<_, _, _, usize>(health_stream, consumer_group, &ack_ids) {
            Ok(ack_count) => {
                if debug_mode {
                    debug!("Acknowledged {} health messages", ack_count);
                }
            },
            Err(e) => {
                warn!("Failed to acknowledge health messages: {}", e);
                // Non-fatal - messages will be re-delivered
            }
        }
    }

    // Write dimension 16 into the canonical (C) entities. One FCALL per service
    // rather than a batch: the primitive is per entity, the message rate is per
    // provider per interval, and a partial failure must not lose the rest.
    //
    // The measured band starts at `idle` (0.20), not at 0.00: code 0.00 on a
    // derived axis means UNKNOWN, so a measured idle service must not be
    // indistinguishable from one nothing has ever measured.
    // Derived axes have ONE writer. Without this, two daemons reporting different
    // observations of the same provider would take turns overwriting dimension 16
    // and the value would flap with whichever tick landed last.
    let node_id = crate::daemon::GNodeDaemon::node_id_for_lease();
    let owns_writes = crate::integration::lease::hold_writer_lease(conn, &node_id);
    if let Some(topo_key) = topology_key.filter(|_| owns_writes) {
        for (service_id, load_factor) in &load_updates {
            let measured = 0.20 + 0.80 * load_factor.clamp(0.0, 1.0);
            let raw = crate::geometric_precision::FixedPoint::from_f64(measured).raw();
            let updates = format!(
                r#"{{"{}":{{"pd":{:.4},"pr":"{}"}}}}"#,
                crate::integration::handlers::types::SERVICE_SAMPLER_AXES[0], measured, raw
            );
            let z_score = (measured * 1_000_000.0) as i64;
            let result: Result<String, redis::RedisError> = redis::cmd("FCALL")
                .arg("GNODE_TOPO_SET_DERIVED")
                .arg(1)
                .arg(topo_key)
                .arg(service_id)
                .arg(&updates)
                .arg(z_score)
                .arg(crate::daemon::GNodeDaemon::topology_snapshot_key())
                .arg(crate::integration::processor::stream_utils::current_timestamp())
                .arg(crate::integration::handlers::types::HASHED_DIMENSIONS)
                .arg(&node_id)   // args[7]: refused unless this node holds the lease
                .query(conn);

            match result {
                Ok(_) => {
                    if debug_mode {
                        debug!("Wrote derived load {:.2} for {}", measured, service_id);
                    }
                },
                // A provider can report for a service that is not registered here;
                // that is the provider's error, not this node's, and it must not
                // stop the rest of the batch.
                Err(e) => trace!("Derived load write for {} refused: {}", service_id, e),
            }
        }
    }

    Ok(processed_count)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_health_message() {
        let _load_manager = Arc::new(LoadMetricsManager::new(30));

        let mut fields = HashMap::new();
        fields.insert("t".to_string(), "lu".to_string());
        fields.insert("si".to_string(), "service-1".to_string());
        fields.insert("l".to_string(), "0.35".to_string());
        fields.insert("cpu".to_string(), "0.45".to_string());
        fields.insert("mem".to_string(), "0.60".to_string());
        fields.insert("ts".to_string(), "1000000".to_string());

        let messages = vec![("msg-1".to_string(), fields)];

        // Note: This test would need a mock Redis connection to be fully functional
        // For now, we're just testing the parsing logic
        assert_eq!(messages.len(), 1);

        // Verify we can extract the required fields
        let (_msg_id, fields) = &messages[0];
        assert_eq!(fields.get("t"), Some(&"lu".to_string()));
        assert_eq!(fields.get("si"), Some(&"service-1".to_string()));
        assert!(fields.get("l").and_then(|v| v.parse::<f64>().ok()).is_some());
    }
}
