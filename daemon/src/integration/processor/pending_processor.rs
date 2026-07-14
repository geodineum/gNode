// Pending Processor Module for gNode
//
// This module provides handling for pending messages in consumer groups,
// ensuring that messages are not lost due to client failures.
use std::time::Duration;
use std::sync::{Arc, RwLock};
use log::{warn, debug, trace, error};
use redis::{Connection, RedisResult, Value};
use crate::integration::stream_processing_error;
use crate::integration::log_error;
use crate::daemon::Command;
use crate::integration::OptimizedCommand;
use crate::GeometricTopology;
use crate::integration::{
    IntegrationResult,
    command_handler::{CommandHandlerRegistry, unknown_command_error},
    send_response_with_routing,
};
use crate::integration::processor::stream_reader::parse_field_array;

use crate::integration::consumer_groups::{
    acknowledge_messages
};

/// Process pending messages by executing their handlers
///
/// This function claims and processes pending command messages by executing
/// their handlers and sending appropriate responses.
///
/// # Arguments
///
/// * `conn` - Redis connection
/// * `topology` - Shared geometric topology
/// * `stream_key` - Unified stream key
/// * `group_name` - Consumer group name
/// * `consumer_name` - Consumer name
/// * `min_idle_time` - Minimum idle time in milliseconds
/// * `registry` - Command handler registry
/// * `site_id` - Site identifier for namespacing
/// * `source_node` - Source node identifier
/// * `debug_mode` - Whether debug mode is enabled
///
/// # Returns
///
/// * `IntegrationResult<usize>` - Number of processed pending messages or error
#[allow(clippy::too_many_arguments)]
pub fn process_pending_commands(
    conn: &mut Connection,
    topology: &Arc<RwLock<GeometricTopology>>,
    stream_key: &str,
    group_name: &str,
    consumer_name: &str,
    min_idle_time: u64,
    registry: &CommandHandlerRegistry,
    site_id: &str,
    source_node: &str,
    debug_mode: bool
) -> IntegrationResult<usize> {
    // Claim pending messages
    let pending_commands = claim_pending_messages(
        conn,
        stream_key,
        group_name,
        consumer_name,
        min_idle_time,
        100, // Claim up to 100 pending messages
        site_id,
        debug_mode
    )?;
    
    if pending_commands.is_empty() {
        return Ok(0);
    }
    
    let mut processed_count = 0;
    let mut message_ids = Vec::new();
    
    // Process each claimed command
    for (msg_id, optimized) in pending_commands {
        // Convert to standard command
        let command = Command::from_optimized(&optimized);
        
        if debug_mode {
            debug!("Processing pending command: {} (ID: {})", command.command, command.id);
        }
        
        // Get handler for command
        let handler_opt = registry.get_handler(&command.command);
        
        // Execute handler
        let result = match handler_opt {
            Some(handler) => {
                // Call handler with proper arguments
                handler(&command, conn, topology, site_id, debug_mode)
            },
            None => {
                unknown_command_error(&command.command)
            }
        };
        
        // Convert result to response
        let response = result.to_response(&command.id);
        
        // Send response
        match send_response_with_routing(
            conn,
            &response,
            stream_key,
            site_id,
            source_node,
            &optimized.source_site,
            &optimized.source_node,
            site_id,
            debug_mode
        ) {
            Ok(_) => {
                processed_count += 1;
                message_ids.push(msg_id.clone());
            },
            Err(e) => {
                warn!("Failed to send response for pending command {}: {}", command.id, e);
                // Still acknowledge the message to avoid processing it again
                message_ids.push(msg_id.clone());
            }
        }
    }
    
    // Acknowledge processed messages
    if !message_ids.is_empty() {
        match acknowledge_messages(
            conn,
            stream_key,
            group_name,
            &message_ids,
            site_id,
            debug_mode
        ) {
            Ok(ack_count) => {
                if debug_mode {
                    debug!("Acknowledged {} pending messages", ack_count);
                }
            },
            Err(e) => {
                warn!("Failed to acknowledge pending messages: {}", e);
            }
        }
    }
    
    Ok(processed_count)
}

/// Claim pending messages from the unified stream
///
/// This function claims messages that have been pending for too long
/// in a consumer group, allowing them to be reprocessed.
///
/// # Arguments
///
/// * `conn` - Redis connection
/// * `stream_key` - Unified stream key
/// * `group_name` - Consumer group name
/// * `consumer_name` - Consumer name
/// * `min_idle_time` - Minimum idle time in milliseconds
/// * `count` - Maximum number of messages to claim
/// * `site_id` - Site identifier for namespacing
/// * `debug_mode` - Whether debug mode is enabled
///
/// # Returns
///
/// * `IntegrationResult<Vec<(String, OptimizedCommand)>>` - Message IDs and commands or error
#[allow(clippy::too_many_arguments)]
pub fn claim_pending_messages(
    conn: &mut Connection,
    stream_key: &str,
    group_name: &str,
    consumer_name: &str,
    min_idle_time: u64,
    count: usize,
    _site_id: &str,
    debug_mode: bool
) -> IntegrationResult<Vec<(String, OptimizedCommand)>> {
    if debug_mode {
        debug!("Claiming pending messages from unified stream {} with consumer {}", 
            stream_key, consumer_name);
    }
    
    // STEP 1: Check if the stream exists to avoid unnecessary operations
    let exists_result: RedisResult<bool> = redis::cmd("EXISTS")
        .arg(stream_key)
        .query(conn);
    
    match exists_result {
        Ok(false) => {
            // Stream doesn't exist, no need to proceed
            debug!("Stream {} doesn't exist, no pending messages to claim", stream_key);
            return Ok(Vec::new());
        },
        Err(e) => {
            // Log the error but continue - this is a non-critical check
            warn!("Failed to check if stream exists: {}", e);
            // Continue execution - we'll get a more specific error later if needed
        },
        _ => {} // Stream exists, continue
    }
    
    // STEP 2: Get pending messages count using more flexible approach
    // First, check if there are any pending messages by getting the summary count
    let summary_result: RedisResult<Value> = redis::cmd("XPENDING")
        .arg(stream_key)
        .arg(group_name)
        .query(conn);
    
    let pending_count = match summary_result {
        Ok(Value::Array(summary)) if !summary.is_empty() => {
            match &summary[0] {
                Value::Int(count) => *count,
                _ => {
                    // Try to handle RESP3 format which might return different structure
                    if summary.len() >= 4 {
                        if let Value::Int(count) = &summary[0] {
                            *count
                        } else {
                            0
                        }
                    } else {
                        warn!("Unexpected XPENDING summary format: {:?}", summary);
                        0
                    }
                }
            }
        },
        Ok(_) => 0,
        Err(e) => {
            // Check if this is a NOGROUP error, which is non-fatal
            let error_str = e.to_string();
            if error_str.contains("NOGROUP") {
                // No such consumer group - likely the stream is new
                debug!("No consumer group '{}' found for stream {}", group_name, stream_key);
                return Ok(Vec::new());
            } else {
                let error = stream_processing_error(format!("Failed to get pending message count: {}", e));
                log_error(&error, "getting pending message count from unified stream");
                return Err(error);
            }
        }
    };
    
    // If there are no pending messages, return an empty result
    if pending_count <= 0 {
        if debug_mode {
            debug!("No pending messages found in stream {}", stream_key);
        }
        return Ok(Vec::new());
    }
    
    if debug_mode {
        debug!("Found {} pending messages in stream {}", pending_count, stream_key);
    }
    
    // STEP 3: Get the detailed pending message information
    // Cap the count to a reasonable number to avoid large responses
    let effective_count = std::cmp::min(count, 100);
    
    let pending_result: RedisResult<Value> = redis::cmd("XPENDING")
        .arg(stream_key)
        .arg(group_name)
        .arg("-")          // Start with any ID
        .arg("+")          // End with any ID
        .arg(effective_count as isize)  // Limit result count
        .query(conn);
    
    let pending_value = match pending_result {
        Ok(val) => val,
        Err(e) => {
            let error_str = e.to_string();
            if error_str.contains("NOGROUP") {
                // No such consumer group - likely the stream is new or group was deleted
                debug!("No consumer group '{}' found for stream {} during detailed pending check", 
                    group_name, stream_key);
                return Ok(Vec::new());
            } else {
                let error = stream_processing_error(format!("Failed to get pending messages: {}", e));
                log_error(&error, "getting pending messages from unified stream");
                return Err(error);
            }
        }
    };
    
    // STEP 4: Extract message IDs from pending response with improved parsing
    let message_ids = extract_pending_message_ids(pending_value);
    
    if message_ids.is_empty() {
        debug!("No valid message IDs found in pending messages for stream {}", stream_key);
        return Ok(Vec::new());
    }
    
    if debug_mode {
        debug!("Claiming {} pending message(s) with IDs: {:?}", message_ids.len(), message_ids);
    }
    
    // STEP 5: Execute XCLAIM command with error handling
    // Try XCLAIM with retry on temporary errors (up to 3 attempts)
    let mut retries = 0;
    let max_retries = 3;
    let mut last_error = None;
    
    while retries < max_retries {
        let result: RedisResult<Value> = redis::cmd("XCLAIM")
            .arg(stream_key)
            .arg(group_name)
            .arg(consumer_name)
            .arg(min_idle_time)
            .arg(&message_ids)
            .query(conn);
        
        match result {
            Ok(value) => {
                // Process the Redis Value to extract claimed messages
                return process_xclaim_response(value, stream_key, group_name, conn, debug_mode);
            },
            Err(e) => {
                // Check if this is a retryable error
                let error_str = e.to_string();
                if error_str.contains("BUSYKEY") || error_str.contains("BUSY") || 
                   error_str.contains("OOM") || error_str.contains("TRYAGAIN") {
                    // These are temporary errors, retry after a delay
                    warn!("Temporary error claiming pending messages (attempt {}/{}): {}", 
                        retries + 1, max_retries, e);
                    last_error = Some(e.to_string());
                    retries += 1;
                    std::thread::sleep(Duration::from_millis(50 * (1 << retries))); // Exponential backoff
                } else if error_str.contains("NOGROUP") {
                    // No such consumer group - likely the stream is new or group was deleted
                    warn!("No consumer group '{}' found for stream {} during XCLAIM operation", 
                        group_name, stream_key);
                    return Ok(Vec::new());
                } else {
                    // Not a retryable error
                    let error = stream_processing_error(format!("Failed to claim pending messages: {}", e));
                    log_error(&error, "claiming pending messages from unified stream");
                    return Err(error);
                }
            }
        }
    }
    
    // All retries failed
    let error = stream_processing_error(format!(
        "Failed to claim pending messages after {} attempts: {}", 
        max_retries, last_error.unwrap_or_else(|| "Unknown error".to_string())
    ));
    log_error(&error, "claiming pending messages from unified stream");
    Err(error)
}

// helper functions below


/// Process XCLAIM response value to extract claimed messages
///
/// This function processes the raw Redis value returned by XCLAIM,
/// handling different format possibilities and extracting claimed command messages.
///
/// # Arguments
///
/// * `value` - Redis value from XCLAIM response
/// * `stream_key` - Stream key for malformed message eviction
/// * `group_name` - Consumer group name for malformed message eviction
/// * `conn` - Redis connection for malformed message eviction
/// * `debug_mode` - Whether debug mode is enabled
///
/// # Returns
///
/// * `IntegrationResult<Vec<(String, OptimizedCommand)>>` - Message IDs and commands or error
fn process_xclaim_response(
    value: Value,
    stream_key: &str,
    group_name: &str,
    conn: &mut Connection,
    debug_mode: bool
) -> IntegrationResult<Vec<(String, OptimizedCommand)>> {
    // Handle empty response
    if let Value::Nil = value {
        return Ok(Vec::new());
    }
    
    if debug_mode {
        trace!("XCLAIM response type: {:?}", value);
    }
    
    let mut commands = Vec::new();
    
    match value {
        Value::Nil => {
            // No messages available
            return Ok(Vec::new());
        },
        Value::BulkString(_) | Value::Int(_) | Value::SimpleString(_) | Value::Okay => {
            // Unexpected simple response type
            return Err(stream_processing_error(
                format!("Unexpected XCLAIM simple response type: {:?}", value)
            ));
        },
        // redis 0.26 RESP3 variants we don't expect on XCLAIM over RESP2.
        Value::Map(_) | Value::Set(_) | Value::Attribute { .. } | Value::Double(_)
        | Value::Boolean(_) | Value::VerbatimString { .. } | Value::BigNumber(_)
        | Value::Push { .. } | Value::ServerError(_) => {
            return Err(stream_processing_error(
                format!("Unexpected RESP3 XCLAIM response type: {:?}", value)
            ));
        },
        Value::Array(entries) => {
            if entries.is_empty() {
                return Ok(Vec::new());
            }
            
            // Process each claimed message
            for entry in entries {
                match entry {
                    Value::Array(message_parts) if message_parts.len() >= 2 => {
                        // First part is message ID
                        let msg_id = match &message_parts[0] {
                            Value::BulkString(data) => {
                                String::from_utf8_lossy(data).to_string()
                            },
                            Value::SimpleString(s) => s.clone(),
                            _ => continue,
                        };
                        
                        // Second part is fields
                        let fields = match &message_parts[1] {
                            Value::Array(field_parts) => {
                                parse_field_array(field_parts)
                            },
                            _ => {
                                if debug_mode {
                                    warn!("Unexpected field format in XCLAIM: {:?}", message_parts[1]);
                                }
                                continue;
                            }
                        };
                        
                        // Parse command from fields
                        match OptimizedCommand::from_resp3_fields(msg_id.clone(), fields) {
                            Ok(cmd) => {
                                commands.push((msg_id, cmd));
                            },
                            Err(e) => {
                                warn!("Failed to parse command from claimed message {}: {}", msg_id, e);
                                // Check if this message should be evicted due to repeated failures
                                check_and_evict_malformed_message(&msg_id, stream_key, group_name, conn, debug_mode);
                            }
                        }
                    },
                    _ => continue,
                }
            }
        }
    }

    Ok(commands)
}

/// Check if a malformed message should be evicted and evict it if needed
/// 
/// Messages are evicted if they have been delivered more than MAX_DELIVERY_ATTEMPTS times
/// 
/// # Arguments
/// 
/// * `msg_id` - Message ID to check and potentially evict
/// * `stream_key` - Stream key  
/// * `group_name` - Consumer group name
/// * `conn` - Redis connection
/// * `debug_mode` - Whether debug logging is enabled
fn check_and_evict_malformed_message(
    msg_id: &str,
    stream_key: &str, 
    group_name: &str,
    conn: &mut Connection,
    debug_mode: bool
) {
    const MAX_DELIVERY_ATTEMPTS: i64 = 5; // Evict after 5 failed attempts
    
    // Get detailed pending info for this specific message
    let result: RedisResult<Value> = redis::cmd("XPENDING")
        .arg(stream_key)
        .arg(group_name)
        .arg(msg_id)
        .arg(msg_id)
        .arg(1)
        .query(conn);
    
    let should_evict = match result {
        Ok(Value::Array(entries)) if !entries.is_empty() => {
            if let Some(Value::Array(entry_parts)) = entries.first() {
                if entry_parts.len() >= 4 {
                    if let Value::Int(delivery_count) = &entry_parts[3] {
                        if debug_mode {
                            debug!("Message {} has delivery count: {}", msg_id, delivery_count);
                        }
                        *delivery_count > MAX_DELIVERY_ATTEMPTS
                    } else {
                        false
                    }
                } else {
                    false
                }
            } else {
                false
            }
        },
        Ok(_) => {
            if debug_mode {
                debug!("No pending info found for message {}", msg_id);
            }
            false
        },
        Err(e) => {
            warn!("Failed to get pending info for message {}: {}", msg_id, e);
            false
        }
    };
    
    // If message should be evicted, acknowledge it to remove from pending
    if should_evict {
        let ack_result: RedisResult<i64> = redis::cmd("XACK")
            .arg(stream_key)
            .arg(group_name)
            .arg(msg_id)
            .query(conn);
        
        match ack_result {
            Ok(acked_count) if acked_count > 0 => {
                warn!("🗑️ EVICTED malformed message {} after repeated parse failures - removed from pending queue", msg_id);
                if debug_mode {
                    debug!("Successfully acknowledged and evicted message {}", msg_id);
                }
            },
            Ok(_) => {
                if debug_mode {
                    debug!("Message {} was already acknowledged or not in pending", msg_id);  
                }
            },
            Err(e) => {
                error!("Failed to acknowledge/evict malformed message {}: {}", msg_id, e);
            }
        }
    }
}

/// Extract message IDs from XPENDING response
///
/// This function extracts message IDs from the Redis value returned by XPENDING.
/// It handles different formats of the XPENDING response, which can vary depending
/// on Redis/ValKey version and parameters.
///
/// # Arguments
///
/// * `value` - Redis value from XPENDING response
///
/// # Returns
///
/// * `Vec<String>` - List of message IDs
fn extract_pending_message_ids(value: Value) -> Vec<String> {
    let mut message_ids = Vec::new();
    
    // Trace the value type for debugging
    trace!("XPENDING response value type: {:?}", value);
    
    match value {
        // Handle array of pending entries
        Value::Array(entries) => {
            for entry in entries {
                match entry {
                    // Each entry is an array where first element is message ID
                    Value::Array(parts) if !parts.is_empty() => {
                        // First element is the message ID
                        match &parts[0] {
                            Value::BulkString(data) => {
                                message_ids.push(String::from_utf8_lossy(data).to_string());
                            },
                            Value::SimpleString(s) => {
                                message_ids.push(s.clone());
                            },
                            Value::Int(n) => {
                                message_ids.push(n.to_string());
                            },
                            _ => continue,
                        }
                    },
                    // Handle case where the entry itself is a simple type (shouldn't happen but being defensive)
                    Value::BulkString(data) => {
                        message_ids.push(String::from_utf8_lossy(&data).to_string());
                    },
                    Value::SimpleString(s) => {
                        message_ids.push(s);
                    },
                    Value::Int(n) => {
                        message_ids.push(n.to_string());
                    },
                    _ => continue,
                }
            }
        },
        // Handle simple integer (count) response - not useful for message IDs
        Value::Int(_) => {
            // If we get here, it's likely just the count which isn't useful for getting IDs
            warn!("XPENDING returned only a count, not message IDs");
        },
        _ => {
            warn!("Unexpected XPENDING response format: {:?}", value);
        }
    }
    
    message_ids
}