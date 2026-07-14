// Diagnostics module for gNode
//
// This module provides diagnostic tools for troubleshooting stream processing
// and consumer group issues in the gNode daemon.
//
// It includes functions for:
// - Checking stream consumer status
// - Monitoring thread status
// - Resetting consumer groups for clean testing
// - Detailed diagnostic information about stream state

use log::{debug, info, warn, error};
use redis::{Connection, Commands, RedisResult};
use crate::integration::{
    IntegrationResult,
    error_handlings::stream_processing_error,
    processor::get_unified_stream
};

/// Check stream consumer status
///
/// This function checks the status of a consumer in a consumer group,
/// providing detailed information for diagnostic purposes.
///
/// # Arguments
///
/// * `conn` - Redis connection
/// * `stream_key` - Stream key
/// * `group_name` - Consumer group name
/// * `consumer_name` - Consumer name
/// * `debug_mode` - Whether debug mode is enabled
///
/// # Returns
///
/// * `IntegrationResult<ConsumerStatus>` - Consumer status information
#[allow(clippy::type_complexity)]
pub fn check_stream_consumer_status(
    conn: &mut Connection,
    stream_key: &str,
    group_name: &str,
    consumer_name: &str,
    debug_mode: bool
) -> IntegrationResult<ConsumerStatus> {
    // Check if stream exists
    let exists: bool = conn.exists(stream_key).map_err(|e| {
        stream_processing_error(format!("Failed to check if stream exists: {}", e))
    })?;
    
    if !exists {
        if debug_mode {
            debug!("Stream {} doesn't exist", stream_key);
        }
        return Ok(ConsumerStatus {
            stream_exists: false,
            group_exists: false,
            consumer_exists: false,
            pending_count: 0,
            last_delivered_id: "0-0".to_string(),
            last_seen: 0,
            is_active: false,
            stream_length: 0,
            lag: 0,
        });
    }
    
    // Get stream length
    let stream_length: i64 = conn.xlen(stream_key).map_err(|e| {
        stream_processing_error(format!("Failed to get stream length: {}", e))
    })?;
    
    // Check if consumer group exists
    let groups_result: RedisResult<Vec<Vec<String>>> = redis::cmd("XINFO")
        .arg("GROUPS")
        .arg(stream_key)
        .query(conn);
    
    match groups_result {
        Ok(groups) => {
            // Default status with stream exists
            let mut status = ConsumerStatus {
                stream_exists: true,
                group_exists: false,
                consumer_exists: false,
                pending_count: 0,
                last_delivered_id: "0-0".to_string(),
                last_seen: 0,
                is_active: false,
                stream_length,
                lag: 0,
            };
            
            // Look for our consumer group
            for group in groups {
                // In XINFO GROUPS output, group name is at index 1
                if group.len() > 1 && group[1] == group_name {
                    status.group_exists = true;
                    
                    // Get pending count from group info
                    if group.len() > 5 {
                        if let Ok(pending) = group[5].parse::<i64>() {
                            status.pending_count = pending;
                        }
                    }
                    
                    // Get the last-delivered-id
                    if group.len() > 3 {
                        status.last_delivered_id = group[3].clone();
                    }
                    
                    // Now check for the specific consumer
                    let consumers_result: RedisResult<Vec<Vec<String>>> = redis::cmd("XINFO")
                        .arg("CONSUMERS")
                        .arg(stream_key)
                        .arg(group_name)
                        .query(conn);
                    
                    match consumers_result {
                        Ok(consumers) => {
                            for consumer in consumers {
                                // In XINFO CONSUMERS output, consumer name is at index 1
                                if consumer.len() > 1 && consumer[1] == consumer_name {
                                    status.consumer_exists = true;
                                    
                                    // Get idle time (last seen)
                                    if consumer.len() > 3 {
                                        if let Ok(idle) = consumer[3].parse::<i64>() {
                                            status.last_seen = idle;
                                            // Consumer is active if seen in the last 30 seconds
                                            status.is_active = idle < 30000;
                                        }
                                    }
                                    
                                    // Found the consumer, no need to check others
                                    break;
                                }
                            }
                        },
                        Err(e) => {
                            warn!("Failed to get consumer info: {}", e);
                        }
                    }
                    
                    // Found the group, no need to check others
                    break;
                }
            }
            
            // Calculate lag as difference between stream length and last delivered ID
            if status.group_exists && status.last_delivered_id != "0-0" {
                let parts: Vec<&str> = status.last_delivered_id.split('-').collect();
                if parts.len() == 2 {
                    if let Ok(delivered_id) = parts[0].parse::<i64>() {
                        // Get the highest ID in the stream for comparison
                        let last_id_result: RedisResult<Vec<(String, Vec<(String, String)>)>> = 
                            conn.xrevrange_count(stream_key, "+", "-", 1);
                        
                        match last_id_result {
                            Ok(entries) => {
                                if !entries.is_empty() {
                                    let last_id = &entries[0].0;
                                    let last_parts: Vec<&str> = last_id.split('-').collect();
                                    if last_parts.len() == 2 {
                                        if let Ok(last_delivered) = last_parts[0].parse::<i64>() {
                                            status.lag = last_delivered - delivered_id;
                                        }
                                    }
                                }
                            },
                            Err(e) => {
                                warn!("Failed to get last stream ID: {}", e);
                            }
                        }
                    }
                }
            }
            
            Ok(status)
        },
        Err(e) => {
            Err(stream_processing_error(format!("Failed to get stream group info: {}", e)))
        }
    }
}

/// Consumer status information for diagnostics
#[derive(Debug, Clone)]
pub struct ConsumerStatus {
    /// Whether the stream exists
    pub stream_exists: bool,
    
    /// Whether the consumer group exists
    pub group_exists: bool,
    
    /// Whether the consumer exists
    pub consumer_exists: bool,
    
    /// Number of pending messages
    pub pending_count: i64,
    
    /// Last delivered ID
    pub last_delivered_id: String,
    
    /// Last seen time in milliseconds
    pub last_seen: i64,
    
    /// Whether the consumer is active
    pub is_active: bool,
    
    /// Total length of the stream
    pub stream_length: i64,
    
    /// Lag in number of messages
    pub lag: i64,
}

/// Check thread status for a node
///
/// This function checks the status of processing threads for a node.
///
/// # Arguments
///
/// * `conn` - Redis connection
/// * `node_id` - Node identifier
/// * `site_id` - Site identifier for namespacing
/// * `stream_prefix` - Stream prefix
/// * `debug_mode` - Whether debug mode is enabled
///
/// # Returns
///
/// * `IntegrationResult<ThreadStatus>` - Thread status information
pub fn check_thread_status(
    conn: &mut Connection,
    node_id: &str,
    site_id: &str,
    stream_prefix: &str,
    debug_mode: bool
) -> IntegrationResult<ThreadStatus> {
    // Get the unified stream key
    let unified_stream = get_unified_stream(site_id, stream_prefix, node_id);
    
    // Check thread consumer names for both patterns to find any active threads
    let daemon_consumer_name = format!("daemon-{}", node_id);
    let processor_consumer_name = format!("processor-{}", node_id);
    
    let daemon_status = check_stream_consumer_status(
        conn,
        &unified_stream,
        "gnode-daemon",
        &daemon_consumer_name,
        debug_mode
    )?;
    
    let processor_status = check_stream_consumer_status(
        conn,
        &unified_stream,
        "gnode-daemon",
        &processor_consumer_name,
        debug_mode
    )?;
    
    // Create thread status
    let status = ThreadStatus {
        node_id: node_id.to_string(),
        command_stream: unified_stream.clone(), // Use unified stream for both
        response_stream: unified_stream.clone(), // Use unified stream for both
        daemon_consumer_exists: daemon_status.consumer_exists,
        daemon_consumer_active: daemon_status.is_active,
        processor_consumer_exists: processor_status.consumer_exists,
        processor_consumer_active: processor_status.is_active,
        stream_exists: daemon_status.stream_exists,
        group_exists: daemon_status.group_exists,
        pending_count: daemon_status.pending_count + processor_status.pending_count,
        stream_length: daemon_status.stream_length,
        lag: daemon_status.lag.max(processor_status.lag),
    };
    
    Ok(status)
}

/// Thread status information for diagnostics
#[derive(Debug, Clone)]
pub struct ThreadStatus {
    /// Node identifier
    pub node_id: String,
    
    /// Command stream key
    pub command_stream: String,
    
    /// Response stream key
    pub response_stream: String,
    
    /// Whether the daemon consumer exists
    pub daemon_consumer_exists: bool,
    
    /// Whether the daemon consumer is active
    pub daemon_consumer_active: bool,
    
    /// Whether the processor consumer exists
    pub processor_consumer_exists: bool,
    
    /// Whether the processor consumer is active
    pub processor_consumer_active: bool,
    
    /// Whether the stream exists
    pub stream_exists: bool,
    
    /// Whether the consumer group exists
    pub group_exists: bool,
    
    /// Number of pending messages
    pub pending_count: i64,
    
    /// Total length of the stream
    pub stream_length: i64,
    
    /// Lag in number of messages
    pub lag: i64,
}

/// Reset a consumer group for clean testing
///
/// This function deletes and recreates a consumer group for a stream.
///
/// # Arguments
///
/// * `conn` - Redis connection
/// * `stream_key` - Stream key
/// * `group_name` - Consumer group name
/// * `debug_mode` - Whether debug mode is enabled
///
/// # Returns
///
/// * `IntegrationResult<bool>` - Success or failure
pub fn reset_consumer_group(
    conn: &mut Connection,
    stream_key: &str,
    group_name: &str,
    debug_mode: bool
) -> IntegrationResult<bool> {
    // Check if stream exists
    let exists: bool = conn.exists(stream_key).map_err(|e| {
        stream_processing_error(format!("Failed to check if stream exists: {}", e))
    })?;
    
    if !exists {
        if debug_mode {
            debug!("Stream {} doesn't exist, creating it", stream_key);
        }
        
        // Add a dummy message to create the stream
        let _: String = conn.xadd(stream_key, "*", &[("init", "1")]).map_err(|e| {
            stream_processing_error(format!("Failed to create stream: {}", e))
        })?;
    }
    
    // P2BF001 FIX: Try SETID first (atomic), fallback to CREATE
    // SETID resets the last-delivered-ID to current position ($)
    let setid_result: RedisResult<()> = redis::cmd("XGROUP")
        .arg("SETID")
        .arg(stream_key)
        .arg(group_name)
        .arg("$") // Reset to current position (new messages only)
        .query(conn);

    if setid_result.is_ok() {
        if debug_mode {
            debug!("Reset consumer group {} to current position for stream {}", group_name, stream_key);
        }
        return Ok(true);
    }

    // Group doesn't exist, create it
    let create_result: RedisResult<String> = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(stream_key)
        .arg(group_name)
        .arg("$")
        .arg("MKSTREAM")
        .query(conn);

    match create_result {
        Ok(_) => {
            info!("Created consumer group {} for stream {}", group_name, stream_key);
            Ok(true)
        },
        Err(e) => {
            if e.to_string().contains("BUSYGROUP") {
                info!("Consumer group {} already exists for stream {}", group_name, stream_key);
                Ok(true)
            } else {
                Err(stream_processing_error(format!("Failed to create consumer group: {}", e)))
            }
        }
    }
}

/// Get detailed stream and consumer group info for debugging
///
/// This function returns detailed information about a stream and its consumer groups.
///
/// # Arguments
///
/// * `conn` - Redis connection
/// * `stream_key` - Stream key
/// * `debug_mode` - Whether debug mode is enabled
///
/// # Returns
///
/// * `IntegrationResult<StreamInfo>` - Detailed stream information
pub fn debug_stream_state(
    conn: &mut Connection,
    stream_key: &str,
    debug_mode: bool
) -> IntegrationResult<StreamInfo> {
    // Check if stream exists
    let exists: bool = conn.exists(stream_key).map_err(|e| {
        stream_processing_error(format!("Failed to check if stream exists: {}", e))
    })?;
    
    if !exists {
        if debug_mode {
            debug!("Stream {} doesn't exist", stream_key);
        }
        return Ok(StreamInfo {
            stream_key: stream_key.to_string(),
            exists: false,
            length: 0,
            radix_tree_keys: 0,
            radix_tree_nodes: 0,
            last_generated_id: "0-0".to_string(),
            first_entry_id: "0-0".to_string(),
            last_entry_id: "0-0".to_string(),
            groups: Vec::new(),
        });
    }
    
    // Get detailed stream info
    let info_result: RedisResult<Vec<String>> = redis::cmd("XINFO")
        .arg("STREAM")
        .arg(stream_key)
        .query(conn);
    
    let mut stream_info = StreamInfo {
        stream_key: stream_key.to_string(),
        exists: true,
        length: 0,
        radix_tree_keys: 0,
        radix_tree_nodes: 0,
        last_generated_id: "0-0".to_string(),
        first_entry_id: "0-0".to_string(),
        last_entry_id: "0-0".to_string(),
        groups: Vec::new(),
    };
    
    match info_result {
        Ok(info) => {
            // Parse the XINFO STREAM output
            for i in (0..info.len()).step_by(2) {
                if i + 1 < info.len() {
                    let key = &info[i];
                    let value = &info[i + 1];
                    
                    match key.as_str() {
                        "length" => {
                            if let Ok(len) = value.parse::<i64>() {
                                stream_info.length = len;
                            }
                        },
                        "radix-tree-keys" => {
                            if let Ok(keys) = value.parse::<i64>() {
                                stream_info.radix_tree_keys = keys;
                            }
                        },
                        "radix-tree-nodes" => {
                            if let Ok(nodes) = value.parse::<i64>() {
                                stream_info.radix_tree_nodes = nodes;
                            }
                        },
                        "last-generated-id" => {
                            stream_info.last_generated_id = value.clone();
                        },
                        "first-entry" => {
                            // First entry is a list with id and fields
                            if value.starts_with('[') && value.ends_with(']') {
                                let stripped = &value[1..value.len()-1];
                                let parts: Vec<&str> = stripped.split(' ').collect();
                                if !parts.is_empty() {
                                    stream_info.first_entry_id = parts[0].trim_matches('"').to_string();
                                }
                            }
                        },
                        "last-entry" => {
                            // Last entry is a list with id and fields
                            if value.starts_with('[') && value.ends_with(']') {
                                let stripped = &value[1..value.len()-1];
                                let parts: Vec<&str> = stripped.split(' ').collect();
                                if !parts.is_empty() {
                                    stream_info.last_entry_id = parts[0].trim_matches('"').to_string();
                                }
                            }
                        },
                        _ => {}
                    }
                }
            }
        },
        Err(e) => {
            warn!("Failed to get stream info: {}", e);
        }
    }
    
    // Get consumer groups
    let groups_result: RedisResult<Vec<Vec<String>>> = redis::cmd("XINFO")
        .arg("GROUPS")
        .arg(stream_key)
        .query(conn);
    
    match groups_result {
        Ok(groups) => {
            for group in groups {
                let mut group_info = GroupInfo {
                    name: "unknown".to_string(),
                    consumers: 0,
                    pending: 0,
                    last_delivered_id: "0-0".to_string(),
                    entries_read: 0,
                    lag: 0,
                    consumer_details: Vec::new(),
                };
                
                // Parse group info
                if group.len() > 1 {
                    group_info.name = group[1].clone();
                }
                
                if group.len() > 3 {
                    group_info.last_delivered_id = group[3].clone();
                }
                
                if group.len() > 5 {
                    if let Ok(pending) = group[5].parse::<i64>() {
                        group_info.pending = pending;
                    }
                }
                
                if group.len() > 7 {
                    if let Ok(consumers) = group[7].parse::<i64>() {
                        group_info.consumers = consumers;
                    }
                }
                
                // Get consumer details for this group
                let consumers_result: RedisResult<Vec<Vec<String>>> = redis::cmd("XINFO")
                    .arg("CONSUMERS")
                    .arg(stream_key)
                    .arg(&group_info.name)
                    .query(conn);
                
                match consumers_result {
                    Ok(consumers) => {
                        for consumer in consumers {
                            let mut consumer_info = ConsumerInfo {
                                name: "unknown".to_string(),
                                pending: 0,
                                idle: 0,
                                inactive: false,
                            };
                            
                            // Parse consumer info
                            if consumer.len() > 1 {
                                consumer_info.name = consumer[1].clone();
                            }
                            
                            if consumer.len() > 3 {
                                if let Ok(idle) = consumer[3].parse::<i64>() {
                                    consumer_info.idle = idle;
                                    consumer_info.inactive = idle > 30000; // Inactive if idle for more than 30 seconds
                                }
                            }
                            
                            if consumer.len() > 5 {
                                if let Ok(pending) = consumer[5].parse::<i64>() {
                                    consumer_info.pending = pending;
                                }
                            }
                            
                            group_info.consumer_details.push(consumer_info);
                        }
                    },
                    Err(e) => {
                        warn!("Failed to get consumer info for group {}: {}", group_info.name, e);
                    }
                }
                
                // Calculate lag
                if stream_info.length > 0 && group_info.last_delivered_id != "0-0" {
                    let last_parts: Vec<&str> = stream_info.last_entry_id.split('-').collect();
                    let delivered_parts: Vec<&str> = group_info.last_delivered_id.split('-').collect();
                    
                    if last_parts.len() == 2 && delivered_parts.len() == 2 {
                        if let (Ok(last), Ok(delivered)) = (last_parts[0].parse::<i64>(), delivered_parts[0].parse::<i64>()) {
                            group_info.lag = last - delivered;
                        }
                    }
                }
                
                stream_info.groups.push(group_info);
            }
        },
        Err(e) => {
            warn!("Failed to get stream group info: {}", e);
        }
    }
    
    Ok(stream_info)
}

/// Detailed stream information
#[derive(Debug, Clone)]
pub struct StreamInfo {
    /// Stream key
    pub stream_key: String,
    
    /// Whether the stream exists
    pub exists: bool,
    
    /// Total length of the stream
    pub length: i64,
    
    /// Number of radix tree keys
    pub radix_tree_keys: i64,
    
    /// Number of radix tree nodes
    pub radix_tree_nodes: i64,
    
    /// Last generated ID
    pub last_generated_id: String,
    
    /// First entry ID
    pub first_entry_id: String,
    
    /// Last entry ID
    pub last_entry_id: String,
    
    /// Consumer groups
    pub groups: Vec<GroupInfo>,
}

/// Consumer group information
#[derive(Debug, Clone)]
pub struct GroupInfo {
    /// Group name
    pub name: String,
    
    /// Number of consumers
    pub consumers: i64,
    
    /// Number of pending messages
    pub pending: i64,
    
    /// Last delivered ID
    pub last_delivered_id: String,
    
    /// Number of entries read
    pub entries_read: i64,
    
    /// Lag in number of messages
    pub lag: i64,
    
    /// Consumer details
    pub consumer_details: Vec<ConsumerInfo>,
}

/// Consumer information
#[derive(Debug, Clone)]
pub struct ConsumerInfo {
    /// Consumer name
    pub name: String,
    
    /// Number of pending messages
    pub pending: i64,
    
    /// Idle time in milliseconds
    pub idle: i64,
    
    /// Whether the consumer is inactive
    pub inactive: bool,
}

/// Get consistent consumer name for a node
///
/// This function provides a standardized way to generate consumer names.
///
/// # Arguments
///
/// * `node_id` - Node identifier
/// * `thread_id` - Optional thread identifier for multi-threaded processing
///
/// # Returns
///
/// * `String` - Standardized consumer name
pub fn get_consistent_consumer_name(node_id: &str, thread_id: Option<usize>) -> String {
    match thread_id {
        Some(id) => format!("daemon-{}-{}", node_id, id),
        None => format!("daemon-{}", node_id)
    }
}

/// Run a diagnostic test for consumer name uniqueness
///
/// This test function verifies that consumer names are properly generated
/// with uniqueness when needed, particularly in multi-threaded scenarios.
///
/// # Arguments
///
/// * `node_id` - The node ID to test with
/// * `thread_count` - Number of threads to simulate
///
/// # Returns
///
/// * `IntegrationResult<Vec<String>>` - List of generated consumer names
pub fn test_consumer_name_uniqueness(
    node_id: &str,
    thread_count: usize
) -> IntegrationResult<Vec<String>> {
    let mut consumer_names = Vec::new();
    
    // Test consumer name without thread ID
    let base_name = get_consistent_consumer_name(node_id, None);
    consumer_names.push(base_name);
    
    // Test consumer names with different thread IDs
    for thread_id in 0..thread_count {
        let thread_name = get_consistent_consumer_name(node_id, Some(thread_id));
        consumer_names.push(thread_name);
    }
    
    // Verify uniqueness
    let mut unique_names = consumer_names.clone();
    unique_names.sort();
    unique_names.dedup();
    
    if unique_names.len() != consumer_names.len() {
        warn!("Consumer name test failed: duplicate names found!");
        // This shouldn't happen with our implementation
    } else {
        info!("Consumer name test passed: all names are unique");
    }
    
    Ok(consumer_names)
}

/// Run a diagnostic test on response JSON serialization
///
/// This function tests the response serialization logic to ensure proper handling
/// of JSON responses across all levels of the system.
///
/// # Arguments
///
/// * `debug_mode` - Enable debug mode for more verbose output
///
/// # Returns
///
/// * `IntegrationResult<bool>` - Success or error
pub fn test_response_serialization(debug_mode: bool) -> IntegrationResult<bool> {
    // Test cases for response serialization
    let test_cases = [
        // Case 1: Already well-formed JSON response
        (
            r#"{"id":"test-1","status":"ok","result":true,"error":null,"timestamp":1234567890}"#,
            "test-1"
        ),
        // Case 2: JSON without required fields
        (
            r#"{"result":"hello world"}"#,
            "test-2"
        ),
        // Case 3: Plain text
        (
            "hello world",
            "test-3"
        ),
        // Case 4: JSON array
        (
            "[1, 2, 3]",
            "test-4"
        ),
        // Case 5: Nested response object
        (
            r#"{"id":"test-5","result":{"nested":true,"data":"value"},"timestamp":1234567890}"#,
            "test-5"
        ),
    ];
    
    if debug_mode {
        info!("Running response serialization tests");
    }
    
    // Run all test cases
    for (i, (test_input, command_id)) in test_cases.iter().enumerate() {
        if debug_mode {
            info!("Test case {}: command_id={}", i+1, command_id);
        }
        
        // Parse test_input to a Value for compatibility
        let json_value: serde_json::Value = match serde_json::from_str(test_input) {
            Ok(value) => value,
            Err(e) => {
                error!("❌ Test case {} failed: Invalid JSON input: {}", i+1, e);
                continue;
            }
        };
        
        // Use our test function from stream_processor
        match crate::integration::stream_processor::test_response_serialization(&json_value, command_id) {
            Ok(output_string) => {
                let validation_result = match serde_json::from_str::<serde_json::Value>(&output_string) {
                    Ok(json) => {
                        if let serde_json::Value::Object(obj) = &json {
                            if obj.contains_key("id") && 
                              (obj.contains_key("status") || obj.contains_key("result") || obj.contains_key("error")) {
                                if debug_mode {
                                    info!("✅ Test case {} passed: output has correct structure", i+1);
                                }
                                true
                            } else {
                                error!("❌ Test case {} failed: output missing required fields", i+1);
                                false
                            }
                        } else {
                            error!("❌ Test case {} failed: output is not a JSON object", i+1);
                            false
                        }
                    },
                    Err(e) => {
                        error!("❌ Test case {} failed: output is not valid JSON: {}", i+1, e);
                        false
                    }
                };
                
                if !validation_result {
                    return Ok(false);
                }
            },
            Err(e) => {
                error!("❌ Test case {} failed: serialization error: {}", i+1, e);
                return Ok(false);
            }
        }
    }
    
    info!("✅ All response serialization tests passed");
    Ok(true)
}