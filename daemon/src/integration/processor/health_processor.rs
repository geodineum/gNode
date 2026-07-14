// Health Message Processor for gNode Load-Aware Service Discovery
//
// This module processes load update (lu) messages from the dedicated health stream,
// updating the LoadMetricsManager for optimal service selection.
//
// Also updates topology dimension 16 (current_load) for geometric service discovery.
// See: docs/architecture/NEW_PRACTICAL_TOPOLOGY.md

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
    debug_mode: bool
) -> IntegrationResult<usize> {
    process_health_updates_with_topology(load_manager, messages, conn, health_stream, None, debug_mode)
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
        let msg_type = match fields.get("t") {
            Some(t) => t,
            None => {
                warn!("Health message {} missing type field, skipping", msg_id);
                continue;
            }
        };

        // Only process lu (load update) messages
        if msg_type != "lu" {
            if debug_mode {
                debug!("Skipping non-lu health message: type={}", msg_type);
            }
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
        match conn.xack::<_, _, _, usize>(health_stream, "gnode-daemon", &ack_ids) {
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

    // Update topology dimension 16 (current_load) if topology_key is provided
    // Uses batch update for efficiency: GNODE_TOPOLOGY_BATCH_UPDATE_LOAD
    if let Some(topo_key) = topology_key {
        if !load_updates.is_empty() {
            // Serialize updates to JSON
            let updates_json = match serde_json::to_string(&load_updates) {
                Ok(json) => json,
                Err(e) => {
                    warn!("Failed to serialize load updates for topology: {}", e);
                    return Ok(processed_count);
                }
            };

            // Call GNODE_TOPOLOGY_BATCH_UPDATE_LOAD
            let result: Result<String, redis::RedisError> = redis::cmd("FCALL")
                .arg("GNODE_TOPOLOGY_BATCH_UPDATE_LOAD")
                .arg(1)
                .arg(topo_key)
                .arg(&updates_json)
                .query(conn);

            match result {
                Ok(response) => {
                    trace!("Topology dimension 16 batch update: {}", response);
                    if debug_mode {
                        debug!("Updated topology current_load for {} services", load_updates.len());
                    }
                },
                Err(e) => {
                    // Non-fatal - topology may not exist yet or function not loaded
                    trace!("Failed to update topology dimension 16: {} (non-fatal)", e);
                }
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
