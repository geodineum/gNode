// Stream reader module for gNode
//
// This module provides functionality for reading command and response
// messages from unified streams using Redis consumer groups.

use std::collections::HashMap;
use log::{debug, warn};
use redis::{Connection, RedisResult, Value};

use crate::integration::{
    IntegrationResult,
    error_handlings::{stream_processing_error, log_error},
};
use crate::integration::processor::resp3_protocol::OptimizedCommand;

/// Stream reader errors
#[derive(Debug)]
pub enum StreamReaderError {
    /// Redis connection error
    ConnectionError(String),
    /// Stream parsing error
    ParsingError(String),
    /// Validation error
    ValidationError(String),
}

/// Stream reader result type
pub type StreamReaderResult<T> = Result<T, StreamReaderError>;

/// One multi-stream read, unpacked.
///
/// `(commands, health messages, unified IDs to ACK, health IDs to ACK,
/// relayed service replies)`.
///
/// The fifth element is the one that is not a command. A service answering a
/// relayed command writes `t=r` carrying the `_rr` the daemon gave it
/// (`relay::reply`), and only the daemon can carry that home to the origin's
/// keyspace — so those entries have to survive the read instead of being
/// counted as noise and ACKed away, which is what happened before.
pub type MultiStreamBatch = (
    Vec<(String, OptimizedCommand)>,
    Vec<(String, HashMap<String, String>)>,
    Vec<String>,
    Vec<String>,
    Vec<(String, HashMap<String, String>)>,
);



/// Entries the classifier cannot type used to be ACKed and forgotten in
/// silence — a producer emitting malformed envelopes looked exactly like a
/// healthy stream (found via a gFlow relay envelope missing `t`). Warn,
/// rate-limited to one line per 30s with a suppressed-count, so noise cannot
/// flood the journal and a broken producer cannot hide.
fn warn_unclassifiable(where_: &str, msg_id: &str, fields: &HashMap<String, String>) {
    use std::sync::atomic::{AtomicU64, Ordering};
    static LAST: AtomicU64 = AtomicU64::new(0);
    static SUPPRESSED: AtomicU64 = AtomicU64::new(0);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let last = LAST.load(Ordering::Relaxed);
    if now.saturating_sub(last) >= 30
        && LAST.compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed).is_ok()
    {
        let n = SUPPRESSED.swap(0, Ordering::Relaxed);
        let mut keys: Vec<&str> = fields.keys().map(|s| s.as_str()).collect();
        keys.sort_unstable();
        warn!(
            "Unclassifiable entry in {} ({}): no resolvable type field (t/type); it will be ACKed and dropped. keys=[{}]{}",
            where_, msg_id, keys.join(","),
            if n > 0 { format!(" (+{} suppressed in last 30s)", n) } else { String::new() }
        );
    } else {
        SUPPRESSED.fetch_add(1, Ordering::Relaxed);
    }
}

// HELPER FUNCTIONS

/// Parse field array from XREADGROUP response
///
/// This function converts an array of alternating field names and values
/// into a HashMap for OptimizedCommand construction.
///
/// # Arguments
///
/// * `field_parts` - Array of field names and values
///
/// # Returns
///
/// * `HashMap<String, String>` - Field map
pub fn parse_field_array(field_parts: &[Value]) -> HashMap<String, String> {
    let mut fields = HashMap::new();
    
    // Process field parts in pairs (alternating key, value)
    for i in (0..field_parts.len()).step_by(2) {
        if i + 1 < field_parts.len() {
            let key = match &field_parts[i] {
                Value::BulkString(data) => String::from_utf8_lossy(data).to_string(),
                Value::SimpleString(s) => s.clone(),
                _ => continue,
            };
            
            let value = match &field_parts[i + 1] {
                Value::BulkString(data) => String::from_utf8_lossy(data).to_string(),
                Value::SimpleString(s) => s.clone(),
                Value::Int(n) => n.to_string(),
                Value::Nil => "null".to_string(),
                _ => {
                    // For complex types, use debug formatting instead of serde serialization
                    format!("{:?}", field_parts[i + 1])
                },
            };
            
            fields.insert(key, value);
        }
    }
    
    fields
}

/// The server's clock in ms, for `wait_for_any`: taken before a sweep, so an entry that lands behind
/// the sweep is still newer than the mark.
pub fn sweep_mark(conn: &mut Connection) -> RedisResult<u64> {
    let (secs, micros): (u64, u64) = redis::cmd("TIME").query(conn)?;
    Ok(secs * 1000 + micros / 1000)
}

/// Sleep until any of `streams` has an entry newer than `mark_ms`, or `block_ms` passes. Consumes
/// nothing: the group read that follows takes the entry. True if something woke it.
pub fn wait_for_any(conn: &mut Connection, streams: &[&str], mark_ms: u64, block_ms: u64) -> RedisResult<bool> {
    let after = format!("{}-{}", mark_ms.saturating_sub(1), u64::MAX);
    let mut wait = redis::cmd("XREAD");
    wait.arg("COUNT").arg(1).arg("BLOCK").arg(block_ms).arg("STREAMS");
    for s in streams {
        wait.arg(*s);
    }
    for _ in streams {
        wait.arg(&after);
    }
    let woke: Value = wait.query(conn)?;
    Ok(!matches!(woke, Value::Nil))
}

/// Read from multiple streams simultaneously (multi-stream XREADGROUP)
///
/// This function reads from both unified and health streams in a single XREADGROUP call,
/// routing messages appropriately based on which stream they came from.
///
/// # Arguments
///
/// * `conn` - Redis connection
/// * `unified_stream` - Unified stream key
/// * `health_stream` - Health stream key
/// * `group_name` - Consumer group name (should be "gnode-daemon")
/// * `consumer_name` - Consumer name
/// * `count` - Maximum number of messages to read per stream
/// * `block_ms` - Time to block for new messages in milliseconds
/// * `debug_mode` - Whether debug mode is enabled
///
/// # Returns
///
/// * `IntegrationResult<MultiStreamBatch>` - see the type alias: commands,
///   health messages, unified IDs, health IDs, and relayed service replies.
#[allow(clippy::too_many_arguments)]
pub fn read_multi_stream(
    conn: &mut Connection,
    unified_stream: &str,
    health_stream: &str,
    group_name: &str,
    consumer_name: &str,
    count: usize,
    block_ms: u64,
    debug_mode: bool
) -> IntegrationResult<MultiStreamBatch> {
    if debug_mode {
        debug!("Reading from multiple streams: unified={}, health={}", unified_stream, health_stream);
    }

    // Execute XREADGROUP for multiple streams; block_ms 0 reads what is there (BLOCK 0 would wait forever)
    let mut read = redis::cmd("XREADGROUP");
    read.arg("GROUP").arg(group_name).arg(consumer_name).arg("COUNT").arg(count);
    if block_ms > 0 {
        read.arg("BLOCK").arg(block_ms);
    }
    let result: RedisResult<Value> = read
        .arg("STREAMS")
        .arg(unified_stream)
        .arg(health_stream)
        .arg(">")  // For unified stream
        .arg(">")  // For health stream
        .query(conn);

    match result {
        Ok(value) => {
            process_multi_stream_response(value, unified_stream, health_stream, debug_mode)
        },
        Err(e) => {
            let error = stream_processing_error(format!("Failed to read from multiple streams: {}", e));
            log_error(&error, "reading from multiple streams");
            Err(error)
        }
    }
}

/// Process multi-stream XREADGROUP response
///
/// Routes messages to appropriate handlers based on source stream.
///
/// # Arguments
///
/// * `value` - Redis value from XREADGROUP response
/// * `unified_stream` - Unified stream key for routing
/// * `health_stream` - Health stream key for routing
/// * `debug_mode` - Whether debug mode is enabled
///
/// # Returns
///
/// * `IntegrationResult<MultiStreamBatch>` - see the type alias.
fn process_multi_stream_response(
    value: Value,
    unified_stream: &str,
    health_stream: &str,
    debug_mode: bool
) -> IntegrationResult<MultiStreamBatch> {
    // Handle empty response
    if let Value::Nil = value {
        return Ok(MultiStreamBatch::default());
    }

    let mut command_messages = Vec::new();
    let mut health_messages = Vec::new();
    let mut unified_message_ids = Vec::new();
    let mut health_message_ids = Vec::new();
    let mut relay_replies = Vec::new();

    match value {
        Value::Nil => {
            return Ok(MultiStreamBatch::default());
        },
        Value::Array(stream_entries) => {
            if stream_entries.is_empty() {
                return Ok(MultiStreamBatch::default());
            }

            // Process each stream in the response
            for stream_entry in stream_entries {
                if let Value::Array(stream_parts) = stream_entry {
                    if stream_parts.len() < 2 {
                        continue;
                    }

                    // Extract stream name
                    let stream_name = match &stream_parts[0] {
                        Value::BulkString(data) => String::from_utf8_lossy(data).to_string(),
                        Value::SimpleString(s) => s.clone(),
                        _ => continue,
                    };

                    if debug_mode {
                        debug!("Processing messages from stream: {}", stream_name);
                    }

                    // Extract messages
                    if let Value::Array(messages) = &stream_parts[1] {
                        for message in messages {
                            if let Value::Array(message_parts) = message {
                                if message_parts.len() < 2 {
                                    continue;
                                }

                                // Extract message ID
                                let msg_id = match &message_parts[0] {
                                    Value::BulkString(data) => String::from_utf8_lossy(data).to_string(),
                                    Value::SimpleString(s) => s.clone(),
                                    _ => continue,
                                };

                                // Collect message IDs by stream (for proper ACK routing)
                                if stream_name == unified_stream {
                                    unified_message_ids.push(msg_id.clone());
                                } else if stream_name == health_stream {
                                    health_message_ids.push(msg_id.clone());
                                }

                                // Extract fields
                                let fields = match &message_parts[1] {
                                    Value::Array(field_parts) => parse_field_array(field_parts),
                                    _ => {
                                        if debug_mode {
                                            warn!("Unexpected field format in message {}", msg_id);
                                        }
                                        continue;
                                    }
                                };

                                // Route based on source stream
                                if stream_name == unified_stream {
                                    // A service's answer to a relayed command,
                                    // on its way home. Collected before the
                                    // command branch because it is neither a
                                    // command nor noise: dropping it here is
                                    // what left the origin polling until it
                                    // timed out. See relay::reply.
                                    if crate::integration::relay::is_relayed_service_reply(&fields) {
                                        if debug_mode {
                                            debug!("Collected relayed service reply from {}: {}", stream_name, msg_id);
                                        }
                                        relay_replies.push((msg_id.clone(), fields));
                                        continue;
                                    }

                                    // Process as command message
                                    let resolved_type = crate::utils::get_field_opt(&fields, crate::utils::field_names::TYPE);
                                    if resolved_type.is_none() {
                                        warn_unclassifiable("unified stream (grouped)", &msg_id, &fields);
                                    }
                                    if let Some(msg_type) = resolved_type {
                                        if msg_type == "c" || msg_type == "bc" {
                                            match OptimizedCommand::from_resp3_fields(msg_id.clone(), fields) {
                                                Ok(cmd) => {
                                                    if debug_mode {
                                                        debug!("Parsed command from unified stream: {}", cmd.command);
                                                    }
                                                    command_messages.push((msg_id, cmd));
                                                },
                                                Err(e) => {
                                                    warn!("Failed to parse command from message {}: {}", msg_id, e);
                                                }
                                            }
                                        }
                                    }
                                } else if stream_name == health_stream {
                                    // Collect as health message (will be processed by health processor)
                                    if debug_mode {
                                        debug!("Collected health message: {}", msg_id);
                                    }
                                    health_messages.push((msg_id, fields));
                                }
                            }
                        }
                    }
                }
            }
        },
        _ => {
            return Err(stream_processing_error(
                format!("Unexpected multi-stream XREADGROUP response type: {:?}", value)
            ));
        }
    }

    if debug_mode {
        debug!("Multi-stream read complete: {} commands, {} health updates, {} unified IDs, {} health IDs for ACK, {} relayed replies",
            command_messages.len(), health_messages.len(), unified_message_ids.len(), health_message_ids.len(), relay_replies.len());
    }

    Ok((command_messages, health_messages, unified_message_ids, health_message_ids, relay_replies))
}