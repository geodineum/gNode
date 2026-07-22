// Broadcast Stream Reader Module for gNode
//
// This module provides XREAD-based broadcast message consumption for pub-sub
// semantics. Unlike unified/health streams (XREADGROUP), broadcast uses XREAD
// to allow ALL nodes/clients to see the same messages without consumer groups.
//
// Architecture:
// - No consumer groups (no PEL, no XACK)
// - Each reader tracks its own last-seen-ID
// - Messages are NOT removed on read (XTRIM based on retention time)
// - Perfect for topology updates, service registrations, global announcements

use log::{warn, debug, trace};
use redis::{Connection, RedisResult};
use std::collections::HashMap;
use crate::integration::{
    IntegrationResult,
    error_handlings::stream_processing_error,
};

/// Broadcast message structure
#[derive(Debug, Clone)]
pub struct BroadcastMessage {
    /// Message ID from stream
    pub id: String,

    /// Message type (topology_update, service_registered, etc.)
    pub message_type: String,

    /// Site ID
    pub site_id: String,

    /// Timestamp
    pub timestamp: i64,

    /// Full message fields
    pub fields: HashMap<String, String>,
}

/// Broadcast reader state (per-node)
pub struct BroadcastReader {
    /// Last message ID seen by this reader
    last_id: String,

    /// Broadcast stream key
    stream_key: String,

    /// Site ID
    site_id: String,
}

impl BroadcastReader {
    /// Create a new broadcast reader
    ///
    /// # Arguments
    ///
    /// * `stream_key` - Broadcast stream key
    /// * `site_id` - Site identifier
    /// * `start_from_beginning` - If true, starts from "0", else starts from "$" (only new)
    ///
    /// # Returns
    ///
    /// * `BroadcastReader` - New reader instance
    pub fn new(stream_key: String, site_id: String, start_from_beginning: bool) -> Self {
        let last_id = if start_from_beginning {
            "0".to_string()
        } else {
            "$".to_string()
        };

        BroadcastReader {
            last_id,
            stream_key,
            site_id,
        }
    }

    /// Read new broadcast messages using XREAD
    ///
    /// This method uses XREAD (not XREADGROUP) to read messages from the
    /// broadcast stream. Each reader maintains its own position independently.
    ///
    /// # Arguments
    ///
    /// * `conn` - Redis connection
    /// * `count` - Maximum number of messages to read
    /// * `block_ms` - Block timeout in milliseconds (0 = non-blocking)
    /// * `debug_mode` - Whether debug mode is enabled
    ///
    /// # Returns
    ///
    /// * `IntegrationResult<Vec<BroadcastMessage>>` - List of messages or error
    pub fn read_messages(
        &mut self,
        conn: &mut Connection,
        count: usize,
        block_ms: u64,
        debug_mode: bool
    ) -> IntegrationResult<Vec<BroadcastMessage>> {
        if debug_mode {
            trace!("Reading broadcast messages from {} (last_id: {})",
                   self.stream_key, self.last_id);
        }

        // Build XREAD command
        let mut cmd = redis::cmd("XREAD");

        if count > 0 {
            cmd.arg("COUNT").arg(count);
        }

        if block_ms > 0 {
            cmd.arg("BLOCK").arg(block_ms);
        }

        cmd.arg("STREAMS")
            .arg(&self.stream_key)
            .arg(&self.last_id);

        // Execute XREAD and parse manually from redis::Value
        let result: RedisResult<redis::Value> = cmd.query(conn);

        match result {
            Ok(value) => {
                let mut messages = Vec::new();

                // XREAD returns: [[stream_name, [[msg_id, [field, value, field, value, ...]]]]]
                if let redis::Value::Array(streams) = value {
                    for stream in streams {
                        if let redis::Value::Array(stream_data) = stream {
                            // stream_data[0] = stream name, stream_data[1] = messages
                            if stream_data.len() < 2 {
                                continue;
                            }

                            // Verify stream name
                            let stream_name = match &stream_data[0] {
                                redis::Value::BulkString(name_bytes) => String::from_utf8_lossy(name_bytes).to_string(),
                                _ => continue,
                            };

                            if stream_name != self.stream_key {
                                continue; // Safety check
                            }

                            // Parse messages
                            if let redis::Value::Array(stream_messages) = &stream_data[1] {
                                for msg in stream_messages {
                                    if let redis::Value::Array(msg_data) = msg {
                                        if msg_data.len() < 2 {
                                            continue;
                                        }

                                        // msg_data[0] = message ID, msg_data[1] = fields array
                                        let msg_id = match &msg_data[0] {
                                            redis::Value::BulkString(id_bytes) => String::from_utf8_lossy(id_bytes).to_string(),
                                            _ => continue,
                                        };

                                        // Parse fields (alternating field, value, field, value...)
                                        let mut field_map = HashMap::new();
                                        if let redis::Value::Array(fields) = &msg_data[1] {
                                            let mut i = 0;
                                            while i + 1 < fields.len() {
                                                let key = match &fields[i] {
                                                    redis::Value::BulkString(k_bytes) => String::from_utf8_lossy(k_bytes).to_string(),
                                                    _ => { i += 2; continue; }
                                                };
                                                let value = match &fields[i + 1] {
                                                    redis::Value::BulkString(v_bytes) => String::from_utf8_lossy(v_bytes).to_string(),
                                                    _ => { i += 2; continue; }
                                                };
                                                field_map.insert(key, value);
                                                i += 2;
                                            }
                                        }

                                        // Extract common fields
                                        let message_type = field_map.get("t")
                                            .or_else(|| field_map.get("type"))
                                            .cloned()
                                            .unwrap_or_else(|| "unknown".to_string());

                                        let site_id = field_map.get("ss")
                                            .or_else(|| field_map.get("site_id"))
                                            .cloned()
                                            .unwrap_or_else(|| self.site_id.clone());

                                        let timestamp = field_map.get("ts")
                                            .or_else(|| field_map.get("timestamp"))
                                            .and_then(|s| s.parse::<i64>().ok())
                                            .unwrap_or(0);

                                        let broadcast_msg = BroadcastMessage {
                                            id: msg_id.clone(),
                                            message_type,
                                            site_id,
                                            timestamp,
                                            fields: field_map,
                                        };

                                        messages.push(broadcast_msg);

                                        // Update last_id to this message ID
                                        self.last_id = msg_id;
                                    }
                                }
                            }
                        }
                    }
                }

                if debug_mode && !messages.is_empty() {
                    debug!("Read {} broadcast messages, new last_id: {}",
                           messages.len(), self.last_id);
                }

                Ok(messages)
            },
            Err(e) => {
                // Timeout is expected and not an error
                if e.to_string().contains("timeout") || e.to_string().contains("nil") {
                    return Ok(Vec::new());
                }

                Err(stream_processing_error(
                    format!("Failed to read broadcast messages: {}", e)
                ))
            }
        }
    }

    /// Get current last_id position
    pub fn get_last_id(&self) -> &str {
        &self.last_id
    }

    /// Reset reader to beginning
    pub fn reset_to_beginning(&mut self) {
        self.last_id = "0".to_string();
    }

    /// Set reader to only read new messages
    pub fn reset_to_latest(&mut self) {
        self.last_id = "$".to_string();
    }
}

/// Trim broadcast stream based on retention time
///
/// This function removes old messages from the broadcast stream based on
/// age (not ACK status, since there are no consumer groups).
///
/// # Arguments
///
/// * `conn` - Redis connection
/// * `stream_key` - Broadcast stream key
/// * `retention_seconds` - Keep messages newer than this (e.g., 300 = 5 minutes)
/// * `debug_mode` - Whether debug mode is enabled
///
/// # Returns
///
/// * `IntegrationResult<usize>` - Number of messages trimmed or error
pub fn trim_broadcast_stream(
    conn: &mut Connection,
    stream_key: &str,
    retention_seconds: u64,
    debug_mode: bool
) -> IntegrationResult<usize> {
    // Calculate cutoff timestamp (current time - retention period)
    let current_time_ms = crate::integration::current_timestamp_ms();
    let cutoff_ms = current_time_ms.saturating_sub(retention_seconds * 1000);

    // Get stream length before trim
    let length_before: usize = redis::cmd("XLEN")
        .arg(stream_key)
        .query(conn)
        .unwrap_or(0);

    if length_before == 0 {
        return Ok(0);
    }

    // Trim by AGE, which is what a retention period means.
    //
    // This previously converted seconds into a message count using a hardcoded
    // `estimated_rate = 10` msgs/sec. That made the advertised time semantics a
    // fiction: at 1 msg/s a 300s request retained 50 minutes, at 100 msg/s it
    // retained 30 seconds and silently discarded live data. The error was
    // unbounded in both directions and scaled with how wrong the guess was.
    //
    // Stream ids are millisecond timestamps, so MINID expresses the cutoff
    // directly and needs no rate at all.
    let cutoff_id = format!("{}-0", cutoff_ms);

    let trim_result: RedisResult<usize> = redis::cmd("XTRIM")
        .arg(stream_key)
        .arg("MINID")
        .arg("~") // Approximate: trims whole radix nodes, cheaper and adequate
        .arg(&cutoff_id)
        .query(conn);

    match trim_result {
        Ok(trimmed) => {
            if debug_mode && trimmed > 0 {
                debug!("Trimmed {} messages from broadcast stream {} (retention: {}s)",
                       trimmed, stream_key, retention_seconds);
            }
            Ok(trimmed)
        },
        Err(e) => {
            warn!("Failed to trim broadcast stream: {}", e);
            Ok(0) // Non-fatal
        }
    }
}

/// Parse broadcast message type from fields
pub fn parse_broadcast_type(fields: &HashMap<String, String>) -> String {
    fields.get("t")
        .or_else(|| fields.get("type"))
        .cloned()
        .unwrap_or_else(|| "unknown".to_string())
}

/// Check if message is a broadcast message (helper for routing)
pub fn is_broadcast_message(message_type: &str) -> bool {
    matches!(message_type,
        "topology_update" |
        "service_registered" |
        "service_deregistered" |
        "format_registered" |
        "global_announcement" |
        "bi" // broadcast initialization
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_broadcast_reader_initialization() {
        let reader = BroadcastReader::new(
            "{default}:gnode:broadcast:global".to_string(),
            "default".to_string(),
            false
        );

        assert_eq!(reader.get_last_id(), "$");
        assert_eq!(reader.stream_key, "{default}:gnode:broadcast:global");
    }

    #[test]
    fn test_broadcast_reader_reset() {
        let mut reader = BroadcastReader::new(
            "{default}:gnode:broadcast:global".to_string(),
            "default".to_string(),
            false
        );

        reader.last_id = "1234567890-0".to_string();
        reader.reset_to_beginning();
        assert_eq!(reader.get_last_id(), "0");

        reader.reset_to_latest();
        assert_eq!(reader.get_last_id(), "$");
    }

    #[test]
    fn test_is_broadcast_message() {
        assert!(is_broadcast_message("topology_update"));
        assert!(is_broadcast_message("service_registered"));
        assert!(is_broadcast_message("service_deregistered"));
        assert!(!is_broadcast_message("ping"));
        assert!(!is_broadcast_message("discover"));
    }
}
