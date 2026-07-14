// Stream Helpers Module for gNode
//
// This module provides utility functions for stream operations,
// including error handling, retry logic, and common stream tasks.

use std::time::Duration;
use log::{warn, debug};
use redis::{Connection, RedisResult};
use std::collections::HashMap;
use regex::Regex;
use once_cell::sync::Lazy;

/// Pre-compiled regex for RESP3 field extraction (P3CF003 fix: compile once, not per-call)
static MESSAGE_FIELD_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"string-data\('([^']*)'\)")
        .expect("MESSAGE_FIELD_REGEX pattern is invalid - this is a compile-time constant")
});
use crate::integration::{
    IntegrationResult,
    error_handlings::{stream_processing_error, log_error}
};

/// Execute a stream operation with retry logic
///
/// This function executes a stream operation with proper retry logic
/// and error handling, providing a consistent approach to stream interactions.
///
/// # Arguments
///
/// * `operation_name` - Name of the operation for logging
/// * `max_retries` - Maximum number of retry attempts
/// * `retry_delay_ms` - Delay between retries in milliseconds
/// * `operation` - Closure that performs the stream operation
///
/// # Returns
///
/// * `IntegrationResult<T>` - Operation result or error
pub fn with_retry<T, F>(
    operation_name: &str,
    max_retries: u32,
    retry_delay_ms: u64,
    mut operation: F
) -> IntegrationResult<T>
where
    F: FnMut() -> IntegrationResult<T>
{
    let mut retries = 0;
    
    loop {
        match operation() {
            Ok(result) => return Ok(result),
            Err(e) => {
                if retries >= max_retries {
                    // No more retries, return the error
                    return Err(e);
                }
                
                // Log the error and retry
                warn!("Error in {}, retry {} of {}: {}", 
                    operation_name, retries + 1, max_retries, e);
                
                // Increase retry delay based on retry count (exponential backoff)
                let delay = retry_delay_ms * (1 << retries);
                std::thread::sleep(Duration::from_millis(delay));
                
                retries += 1;
            }
        }
    }
}

/// Check if a stream exists
///
/// This function checks if a stream exists in the Redis database.
///
/// # Arguments
///
/// * `conn` - Redis connection
/// * `stream_key` - Stream key to check
///
/// # Returns
///
/// * `IntegrationResult<bool>` - True if stream exists, false otherwise
pub fn stream_exists(
    conn: &mut Connection,
    stream_key: &str
) -> IntegrationResult<bool> {
    let exists: RedisResult<bool> = redis::cmd("EXISTS")
        .arg(stream_key)
        .query(conn);
    
    match exists {
        Ok(exists) => Ok(exists),
        Err(e) => {
            let error = stream_processing_error(format!("Failed to check if stream exists: {}", e));
            log_error(&error, "checking stream existence");
            Err(error)
        }
    }
}

/// Create a stream with an initial message
///
/// This function creates a stream with an initial message to ensure
/// that the stream exists for consumer group operations.
///
/// # Arguments
///
/// * `conn` - Redis connection
/// * `stream_key` - Stream key to create
/// * `debug_mode` - Whether debug mode is enabled
///
/// # Returns
///
/// * `IntegrationResult<String>` - Message ID of the initial message
pub fn create_stream(
    conn: &mut Connection,
    stream_key: &str,
    debug_mode: bool
) -> IntegrationResult<String> {
    if debug_mode {
        debug!("Creating stream {}", stream_key);
    }
    
    let result: RedisResult<String> = redis::cmd("XADD")
        .arg(stream_key)
        .arg("*")
        .arg("init")
        .arg("true")
        .arg("timestamp")
        .arg(current_timestamp_ms().to_string())
        .query(conn);
    
    match result {
        Ok(message_id) => Ok(message_id),
        Err(e) => {
            let error = stream_processing_error(format!("Failed to create stream: {}", e));
            log_error(&error, "creating stream");
            Err(error)
        }
    }
}

/// Create a consumer group for a stream
///
/// This function creates a consumer group for a stream, with options
/// to create the stream if it doesn't exist and specify the starting position.
///
/// # Arguments
///
/// * `conn` - Redis connection
/// * `stream_key` - Stream key
/// * `group_name` - Consumer group name
/// * `start_position` - Starting position ($ for new messages only, 0 for all messages)
/// * `mk_stream` - Whether to create the stream if it doesn't exist
/// * `debug_mode` - Whether debug mode is enabled
///
/// # Returns
///
/// * `IntegrationResult<bool>` - True if group was created, false if it already existed
pub fn create_consumer_group(
    conn: &mut Connection,
    stream_key: &str,
    group_name: &str,
    start_position: &str,
    mk_stream: bool,
    debug_mode: bool
) -> IntegrationResult<bool> {
    if debug_mode {
        debug!("Creating consumer group {} for stream {}", group_name, stream_key);
    }
    
    let mut args = vec!["XGROUP", "CREATE", stream_key, group_name, start_position];
    
    if mk_stream {
        args.push("MKSTREAM");
    }
    
    let result: RedisResult<String> = redis::cmd(args[0])
        .arg(&args[1..])
        .query(conn);
    
    match result {
        Ok(_) => {
            // Group was created
            if debug_mode {
                debug!("Consumer group {} created for stream {}", group_name, stream_key);
            }
            Ok(true)
        },
        Err(e) => {
            let error_str = e.to_string();
            
            if error_str.contains("BUSYGROUP") {
                // Group already exists
                if debug_mode {
                    debug!("Consumer group {} already exists for stream {}", group_name, stream_key);
                }
                Ok(false)
            } else {
                // Other error
                let error = stream_processing_error(format!("Failed to create consumer group: {}", e));
                log_error(&error, "creating consumer group");
                Err(error)
            }
        }
    }
}

/// Get information about a stream
///
/// This function retrieves information about a stream using the XINFO STREAM command.
///
/// # Arguments
///
/// * `conn` - Redis connection
/// * `stream_key` - Stream key
/// * `debug_mode` - Whether debug mode is enabled
///
/// # Returns
///
/// * `IntegrationResult<serde_json::Value>` - Stream information as JSON
pub fn get_stream_info(
    conn: &mut Connection,
    stream_key: &str,
    debug_mode: bool
) -> IntegrationResult<serde_json::Value> {
    if debug_mode {
        debug!("Getting information for stream {}", stream_key);
    }
    
    let result: RedisResult<Vec<Vec<String>>> = redis::cmd("XINFO")
        .arg("STREAM")
        .arg(stream_key)
        .query(conn);
    
    match result {
        Ok(info) => {
            // Convert XINFO result to JSON
            let mut json_obj = serde_json::Map::new();
            
            for i in (0..info.len()).step_by(2) {
                if i + 1 < info.len() {
                    let key = info[i][0].clone();
                    let value = &info[i+1];
                    
                    // Handle different value types
                    let json_value = if value.len() == 1 {
                        // Simple string value
                        serde_json::Value::String(value[0].clone())
                    } else {
                        // Array value
                        serde_json::Value::Array(
                            value.iter()
                                .map(|s| serde_json::Value::String(s.clone()))
                                .collect()
                        )
                    };
                    
                    json_obj.insert(key, json_value);
                }
            }
            
            Ok(serde_json::Value::Object(json_obj))
        },
        Err(e) => {
            let error = stream_processing_error(format!("Failed to get stream info: {}", e));
            log_error(&error, "getting stream info");
            Err(error)
        }
    }
}

/// Get information about consumer groups for a stream
///
/// This function retrieves information about consumer groups using the XINFO GROUPS command.
///
/// # Arguments
///
/// * `conn` - Redis connection
/// * `stream_key` - Stream key
/// * `debug_mode` - Whether debug mode is enabled
///
/// # Returns
///
/// * `IntegrationResult<Vec<serde_json::Value>>` - Consumer group information as JSON array
pub fn get_consumer_groups(
    conn: &mut Connection,
    stream_key: &str,
    debug_mode: bool
) -> IntegrationResult<Vec<serde_json::Value>> {
    if debug_mode {
        debug!("Getting consumer groups for stream {}", stream_key);
    }
    
    let result: RedisResult<Vec<Vec<Vec<String>>>> = redis::cmd("XINFO")
        .arg("GROUPS")
        .arg(stream_key)
        .query(conn);
    
    match result {
        Ok(groups) => {
            // Convert result to JSON
            let mut json_groups = Vec::new();
            
            for group in groups {
                let mut json_group = serde_json::Map::new();
                
                for i in (0..group.len()).step_by(2) {
                    if i + 1 < group.len() {
                        let key = group[i][0].clone();
                        let value = &group[i+1];
                        
                        // Handle different value types
                        let json_value = if value.len() == 1 {
                            // Simple string value
                            serde_json::Value::String(value[0].clone())
                        } else {
                            // Array value
                            serde_json::Value::Array(
                                value.iter()
                                    .map(|s| serde_json::Value::String(s.clone()))
                                    .collect()
                            )
                        };
                        
                        json_group.insert(key, json_value);
                    }
                }
                
                json_groups.push(serde_json::Value::Object(json_group));
            }
            
            Ok(json_groups)
        },
        Err(e) => {
            let error = stream_processing_error(format!("Failed to get consumer groups: {}", e));
            log_error(&error, "getting consumer groups");
            Err(error)
        }
    }
}

/// Find existing nodes by scanning stream keys
///
/// This function scans for stream keys matching a pattern to discover active nodes
///
/// # Arguments
///
/// * `conn` - Redis connection
/// * `site_id` - Site identifier for namespacing
/// * `stream_prefix` - Stream prefix
/// * `debug_mode` - Whether debug mode is enabled
///
/// # Returns
///
/// * `IntegrationResult<Vec<(String, serde_json::Value)>>` - List of node IDs and their metadata
pub fn find_existing_nodes(
    conn: &mut Connection,
    site_id: &str,
    stream_prefix: &str,
    debug_mode: bool
) -> IntegrationResult<Vec<(String, serde_json::Value)>> {
    if debug_mode {
        debug!("Scanning for existing nodes with site_id={} and prefix={}", site_id, stream_prefix);
    }
    
    // Build scan pattern for unified streams
    let pattern = format!("{{{0}}}:{1}:unified:*", site_id, stream_prefix);

    // P3CF002 FIX: Use SCAN instead of KEYS to avoid O(N) blocking
    // SCAN is cursor-based and non-blocking, returning results incrementally
    let mut cursor = "0".to_string();
    let mut keys: Vec<String> = Vec::new();

    loop {
        // SCAN cursor MATCH pattern COUNT 100
        let scan_result: RedisResult<(String, Vec<String>)> = redis::cmd("SCAN")
            .arg(&cursor)
            .arg("MATCH")
            .arg(&pattern)
            .arg("COUNT")
            .arg(100)  // Hint for batch size per iteration
            .query(conn);

        match scan_result {
            Ok((new_cursor, batch_keys)) => {
                keys.extend(batch_keys);
                cursor = new_cursor;

                // Cursor "0" signals complete iteration
                if cursor == "0" {
                    break;
                }
            }
            Err(e) => {
                let error = stream_processing_error(format!("Failed to scan for node streams: {}", e));
                log_error(&error, "scanning for nodes");
                return Err(error);
            }
        }
    }
    
    if keys.is_empty() {
        if debug_mode {
            debug!("No existing nodes found");
        }
        return Ok(Vec::new());
    }
    
    // Process each key to extract node ID and metadata
    let mut nodes = Vec::new();
    
    for key in keys {
        // Extract node ID from key
        let parts: Vec<&str> = key.split(':').collect();
        if parts.len() < 4 {
            warn!("Invalid stream key format: {}", key);
            continue;
        }
        
        let node_id = parts[3].to_string();
        
        // Get stream info for metadata
        match get_stream_info(conn, &key, debug_mode) {
            Ok(info) => {
                nodes.push((node_id, info));
            },
            Err(e) => {
                warn!("Failed to get info for node stream {}: {}", key, e);
                // Continue with other nodes
            }
        }
    }
    
    if debug_mode {
        debug!("Found {} existing nodes", nodes.len());
    }
    
    Ok(nodes)
}


/// Get current timestamp
pub fn current_timestamp() -> f64 {
    let now = std::time::SystemTime::now();
    match now.duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => {
            duration.as_secs() as f64 + duration.subsec_nanos() as f64 / 1_000_000_000.0
        },
        Err(_) => 0.0,
    }
}

/// Get current timestamp in milliseconds
pub fn current_timestamp_ms() -> u64 {
    let now = std::time::SystemTime::now();
    match now.duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => {
            duration.as_millis() as u64
        },
        Err(_) => 0,
    }
}

/// Clean up unified stream
///
/// This function trims the unified stream to prevent unbounded growth.
///
/// # Arguments
///
/// * `conn` - Redis connection
/// * `stream_key` - Unified stream key
/// * `max_length` - Maximum stream length
/// * `approximate` - Whether to use approximate trimming
/// * `site_id` - Site identifier for namespacing
/// * `debug_mode` - Whether debug mode is enabled
///
/// # Returns
///
/// * `IntegrationResult<usize>` - Number of trimmed messages or error
pub fn trim_unified_stream(
    conn: &mut redis::Connection,
    stream_key: &str,
    max_length: usize,
    approximate: bool,
    _site_id: &str,
    debug_mode: bool
) -> crate::integration::IntegrationResult<usize> {
    if debug_mode {
        debug!("Trimming unified stream {} to max length {}", stream_key, max_length);
    }

    let trim_result: redis::RedisResult<i64> = if approximate {
        redis::cmd("XTRIM")
            .arg(stream_key)
            .arg("MAXLEN")
            .arg("~")
            .arg(max_length)
            .query(conn)
    } else {
        redis::cmd("XTRIM")
            .arg(stream_key)
            .arg("MAXLEN")
            .arg(max_length)
            .query(conn)
    };

    match trim_result {
        Ok(count) => {
            if debug_mode && count > 0 {
                debug!("Trimmed {} messages from unified stream", count);
            }
            Ok(count as usize)
        },
        Err(e) => {
            let error = crate::integration::error_handlings::stream_processing_error(format!("Failed to trim unified stream: {}", e));
            crate::integration::error_handlings::log_error(&error, "trimming unified stream");
            Err(error)
        }
    }
}

/// Helper function to extract field-value pairs from RESP3 formatted message data
/// 
/// Parses field-value pairs in RESP3 format where fields and values alternate in the format
/// string-data('field_name'), string-data('field_value'), string-data('field2_name'), string-data('field2_value')...
pub fn extract_message_fields(message_data: &str) -> HashMap<String, String> {
    let mut fields = HashMap::new();

    // P3CF003 FIX: Use pre-compiled static regex instead of compiling per-call
    let captures: Vec<_> = MESSAGE_FIELD_REGEX.captures_iter(message_data).collect();

    // Process in pairs (field, value)
    for i in (0..captures.len()).step_by(2) {
        if i + 1 < captures.len() {
            let field = captures[i][1].to_string();
            let value = captures[i+1][1].to_string();

            debug!("Extracted field: {} = {}", field, value);
            fields.insert(field, value);
        }
    }

    fields
}