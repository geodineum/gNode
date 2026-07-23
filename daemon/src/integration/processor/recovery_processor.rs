// Recovery Processor Module for gNode
//
// This module provides recovery mechanisms for the unified stream approach,
// including handling of failed messages, error recovery, and system restarts.

use log::{info, warn, debug, error};
use redis::{Connection, RedisResult};
use crate::integration::processor::stream_utils::current_timestamp;
use crate::daemon::GNodeDaemon;
use crate::integration::{
    IntegrationResult,
    error_handlings::IntegrationError,
};

use super::unified_stream_processor::initialize_unified_stream;
// Import and re-export process_pending_commands from pending_processor
pub use super::pending_processor::process_pending_commands;
use super::stream_utils;

/// Recover from system restart
///
/// This function performs recovery operations after a system restart,
/// ensuring that processing continues from where it left off.
///
/// # Arguments
///
/// * `conn` - Redis connection
/// * `stream_key` - Unified stream key
/// * `group_name` - Consumer group name
/// * `consumer_name` - Consumer name
/// * `site_id` - Site identifier for namespacing
/// * `debug_mode` - Whether debug mode is enabled
///
/// # Returns
///
/// * `IntegrationResult<usize>` - Number of recovered messages or error
pub fn recover_from_restart(
    conn: &mut Connection,
    stream_key: &str,
    group_name: &str,
    consumer_name: &str,
    site_id: &str,
    debug_mode: bool
) -> IntegrationResult<usize> {
    if debug_mode {
        debug!("Starting recovery operations for stream {}, group {}", stream_key, group_name);
    }
    
    info!("Performing recovery operations after system restart");
    
    // Check if the stream exists
    let stream_exists = stream_utils::stream_exists(conn, stream_key)?;
    
    if !stream_exists {
        warn!("Stream {} does not exist, creating it", stream_key);
        // Extract node_id from stream key
        let node_id = stream_key.split(':').next_back().unwrap_or("unknown");
        // Initialize the stream
        let init_result = initialize_unified_stream(
            conn,
            node_id,
            site_id,
            "gnode", // Standard prefix
            debug_mode
        );
        
        if let Err(e) = init_result {
            error!("Failed to initialize stream during recovery: {}", e);
            return Err(e);
        }
        
        // No messages to recover
        return Ok(0);
    }
    
    // Check consumer group
    let consumer_groups = stream_utils::get_consumer_groups(conn, stream_key, debug_mode)?;
    
    let group_exists = consumer_groups.iter().any(|group| {
        if let Some(name) = group.get("name") {
            if let Some(name_str) = name.as_str() {
                return name_str == group_name;
            }
        }
        false
    });
    
    if !group_exists {
        warn!("Consumer group {} does not exist for stream {}, creating it", group_name, stream_key);
        
        // Create consumer group
        let create_result = stream_utils::create_consumer_group(
            conn,
            stream_key,
            group_name,
            "0", // Start from beginning to recover all messages
            true, // Create stream if it doesn't exist
            debug_mode
        );
        
        if let Err(e) = create_result {
            error!("Failed to create consumer group during recovery: {}", e);
            return Err(e);
        }
    }
    
    // Process pending messages
    let topology = GNodeDaemon::get_topology_ref();
    let registry = crate::integration::command_handler::get_command_registry();
    
    // Process all pending messages with no time limit (0 ms idle time)
    let processed = process_pending_commands(
        conn,
        &topology,
        stream_key,
        group_name,
        consumer_name,
        0, // Process all pending messages
        registry,
        site_id,
        "daemon",
        debug_mode
    )?;
    
    if processed > 0 {
        info!("Recovered {} pending messages after system restart", processed);
    } else {
        info!("No pending messages to recover after system restart");
    }
    
    Ok(processed)
}

/// Repair a corrupted stream
///
/// This function attempts to repair a corrupted stream by recreating
/// consumer groups and recovering pending messages.
///
/// # Arguments
///
/// * `conn` - Redis connection
/// * `stream_key` - Unified stream key
/// * `site_id` - Site identifier for namespacing
/// * `node_id` - Node identifier
/// * `debug_mode` - Whether debug mode is enabled
///
/// # Returns
///
/// * `IntegrationResult<bool>` - True if repair was successful
pub fn repair_stream(
    conn: &mut Connection,
    stream_key: &str,
    site_id: &str,
    node_id: &str,
    debug_mode: bool
) -> IntegrationResult<bool> {
    info!("Attempting to repair stream: {}", stream_key);
    
    // Check if stream exists and has content
    let stream_info_result = stream_utils::get_stream_info(conn, stream_key, debug_mode);
    
    if stream_info_result.is_err() {
        warn!("Stream {} does not exist or is corrupted, recreating it", stream_key);
        
        // Initialize the stream
        let init_result = initialize_unified_stream(
            conn,
            node_id,
            site_id,
            "gnode", // Standard prefix
            debug_mode
        );
        
        if let Err(e) = init_result {
            error!("Failed to initialize stream during repair: {}", e);
            return Err(e);
        }
        
        return Ok(true);
    }
    
    // Reset or create consumer groups (P2BF001 FIX: SETID-first approach).
    // gnode-client (the former response group) is retired — responses use keyed
    // rendezvous, not a consumer group — so recovery no longer recreates it.
    for group_name in ["gnode-daemon"].iter() {
        // Try SETID first (atomic, works if group exists)
        let setid_result: RedisResult<()> = redis::cmd("XGROUP")
            .arg("SETID")
            .arg(stream_key)
            .arg(group_name)
            .arg("0") // Reset to beginning
            .query(conn);

        if setid_result.is_ok() {
            debug!("Reset consumer group {} to beginning during repair", group_name);
            continue;
        }

        // Group doesn't exist, create it
        let create_result = stream_utils::create_consumer_group(
            conn,
            stream_key,
            group_name,
            "0", // Start from beginning to process all messages
            true, // Create stream if it doesn't exist
            debug_mode
        );

        if let Err(e) = create_result {
            error!("Failed to create consumer group {} during repair: {}", group_name, e);
            return Err(e);
        }
    }
    
    info!("Stream {} repaired successfully", stream_key);
    Ok(true)
}

/// Detect and repair orphaned consumer groups
///
/// This function detects consumer groups with a high number of pending
/// messages that haven't been processed for a long time, and repairs them.
///
/// # Arguments
///
/// * `conn` - Redis connection
/// * `stream_key` - Unified stream key
/// * `idle_threshold_ms` - Idle time threshold in milliseconds
/// * `site_id` - Site identifier for namespacing
/// * `debug_mode` - Whether debug mode is enabled
///
/// # Returns
///
/// * `IntegrationResult<usize>` - Number of repaired consumer groups
pub fn repair_orphaned_groups(
    conn: &mut Connection,
    stream_key: &str,
    idle_threshold_ms: u64,
    site_id: &str,
    debug_mode: bool
) -> IntegrationResult<usize> {
    if debug_mode {
        debug!("Checking for orphaned consumer groups in stream {}", stream_key);
    }
    
    // Get consumer groups
    let consumer_groups = stream_utils::get_consumer_groups(conn, stream_key, debug_mode)?;
    
    let mut repaired_count = 0;
    
    for group in consumer_groups {
        let name = match group.get("name") {
            Some(n) => {
                if let Some(s) = n.as_str() {
                    s.to_string()
                } else {
                    continue;
                }
            },
            None => continue,
        };
        
        let pending_count = match group.get("pending") {
            Some(p) => {
                if let Some(s) = p.as_str() {
                    s.parse::<usize>().unwrap_or(0)
                } else {
                    0
                }
            },
            None => 0,
        };
        
        // Skip groups with no pending messages
        if pending_count == 0 {
            continue;
        }
        
        // Get the last delivered ID to check group activity
        let _last_delivered = match group.get("last-delivered-id") {
            Some(id) => {
                if let Some(s) = id.as_str() {
                    s.to_string()
                } else {
                    continue;
                }
            },
            None => continue,
        };

        // Check the oldest pending message idle time
        let pending_summary: RedisResult<Vec<String>> = redis::cmd("XPENDING")
            .arg(stream_key)
            .arg(&name)
            .query(conn);
        
        if let Ok(summary) = pending_summary {
            if summary.len() >= 4 {
                // Get oldest pending message details
                let pending_details: RedisResult<Vec<Vec<String>>> = redis::cmd("XPENDING")
                    .arg(stream_key)
                    .arg(&name)
                    .arg("-")  // Start with any ID
                    .arg("+")  // End with any ID
                    .arg(1)    // Just get the oldest message
                    .query(conn);
                
                if let Ok(details) = pending_details {
                    if !details.is_empty() && details[0].len() >= 3 {
                        let idle_time: u64 = details[0][2].parse().unwrap_or(0);
                        
                        // If the message has been idle for longer than the threshold, repair the group
                        if idle_time > idle_threshold_ms {
                            info!("Repairing orphaned consumer group {} with {} pending messages, idle for {}ms",
                                name, pending_count, idle_time);
                            
                            // Create a recovery consumer name
                            let recovery_consumer = format!("recovery-{}", current_timestamp());
                            
                            // Process the pending messages with a dedicated consumer
                            let processed = super::pending_processor::process_pending_commands(
                                conn,
                                &GNodeDaemon::get_topology_ref(),
                                stream_key,
                                &name,
                                &recovery_consumer,
                                0, // Process all pending messages
                                crate::integration::command_handler::get_command_registry(),
                                site_id,
                                "daemon",
                                debug_mode
                            )?;
                            
                            info!("Processed {} orphaned messages from group {}", processed, name);
                            repaired_count += 1;
                        }
                    }
                }
            }
        }
    }
    
    if debug_mode {
        debug!("Repaired {} orphaned consumer groups", repaired_count);
    }
    
    Ok(repaired_count)
}

/// Recover the unified stream processor
///
/// This function performs recovery operations for the unified stream processor,
/// attempting to recover from errors or system restarts.
///
/// # Arguments
///
/// * `conn` - Redis connection
/// * `stream_key` - Unified stream key
/// * `group_name` - Consumer group name
/// * `consumer_name` - Consumer name
/// * `site_id` - Site identifier for namespacing
/// * `node_id` - Node identifier
/// * `debug_mode` - Whether debug mode is enabled
///
/// # Returns
///
/// * `IntegrationResult<usize>` - Number of recovered messages or error
pub fn recover_unified_stream_processor(
    conn: &mut Connection,
    stream_key: &str,
    group_name: &str,
    consumer_name: &str,
    site_id: &str,
    node_id: &str,
    debug_mode: bool
) -> IntegrationResult<usize> {
    info!("Recovering unified stream processor for stream: {}", stream_key);
    
    // First try to repair the stream if needed
    let repair_result = repair_stream(conn, stream_key, site_id, node_id, debug_mode);
    
    if let Err(e) = repair_result {
        error!("Failed to repair stream during recovery: {}", e);
        // Continue with recovery despite repair failure
    }
    
    // Recover from restart (process pending messages)
    let recovery_result = recover_from_restart(
        conn,
        stream_key,
        group_name,
        consumer_name,
        site_id,
        debug_mode
    );
    
    // Check for orphaned groups with idle threshold of 30 minutes
    let orphaned_result = repair_orphaned_groups(
        conn,
        stream_key,
        1800000, // 30 minutes in milliseconds
        site_id,
        debug_mode
    );
    
    if let Err(e) = orphaned_result {
        warn!("Failed to repair orphaned groups: {}", e);
        // Continue with recovery despite orphaned group repair failure
    }
    
    recovery_result
}

/// Recover the unified stream processor with redis client
///
/// This function is a convenience wrapper that creates a connection from the client
/// and delegates to recover_unified_stream_processor.
///
/// # Arguments
///
/// * `client` - Redis client
/// * `node_id` - Node identifier
/// * `site_id` - Site identifier for namespacing
/// * `stream_prefix` - Stream prefix
/// * `debug_mode` - Whether debug mode is enabled
///
/// # Returns
///
/// * `IntegrationResult<usize>` - Number of recovered messages or error
pub fn recover_with_client(
    client: redis::Client,
    node_id: &str,
    site_id: &str,
    stream_prefix: &str,
    debug_mode: bool
) -> IntegrationResult<usize> {
    // Get a connection from the client
    let mut conn = match client.get_connection() {
        Ok(conn) => conn,
        Err(e) => {
            return Err(IntegrationError::new(
                crate::integration::error_handlings::IntegrationErrorKind::Redis,
                format!("Failed to get Redis connection: {}", e)
            ));
        }
    };
    
    // Calculate stream key and consumer group/name
    let stream_key = format!("{{{0}}}:{1}:stream:{2}", site_id, stream_prefix, node_id);
    let group_name = "gnode-daemon".to_string();
    let consumer_name = format!("recovery-processor-{}", current_timestamp());
    
    // Delegate to the main recovery function
    recover_unified_stream_processor(
        &mut conn,
        &stream_key,
        &group_name,
        &consumer_name,
        site_id,
        node_id,
        debug_mode
    )
}

/// Additional function with exact signature needed by daemon.rs
/// This is the function that daemon.rs expects to call for recovery
pub fn recover_unified_stream_processor_client(
    client: redis::Client,
    node_id: &str,
    site_id: &str,
    stream_prefix: &str,
    debug_mode: bool
) -> IntegrationResult<usize> {
    // Delegates to the recovery function with connection handling
    recover_with_client(client, node_id, site_id, stream_prefix, debug_mode)
}

/// DTAP wrapper: Recover stream processor using environment as namespace
/// Stream pattern: {environment}:gnode:unified:{node_id}
pub fn recover_dtap_with_client(
    client: redis::Client,
    environment: &str,
    node_id: &str,
    stream_prefix: &str,
    debug_mode: bool
) -> IntegrationResult<usize> {
    // DTAP mode: Use environment as the site_id
    recover_with_client(client, node_id, environment, stream_prefix, debug_mode)
}

/// Recover environment-based stream processor for multi-node architecture
///
/// Stream pattern: {site_id}:{stream_prefix}:{environment}:unified
/// Consumer group: gnode-workers (shared across all nodes)
/// Consumer name: gnode-{node_id} (unique per node)
pub fn recover_environment_with_client(
    client: redis::Client,
    site_id: &str,
    environment: &str,
    node_id: &str,
    stream_prefix: &str,
    debug_mode: bool
) -> IntegrationResult<usize> {
    // Get a connection from the client
    let mut conn = match client.get_connection() {
        Ok(conn) => conn,
        Err(e) => {
            return Err(IntegrationError::new(
                crate::integration::error_handlings::IntegrationErrorKind::Redis,
                format!("Failed to get Redis connection: {}", e)
            ));
        }
    };

    // Calculate environment-based stream key
    let stream_key = crate::integration::processor::unified_stream_processor::get_environment_unified_stream(
        site_id,
        stream_prefix,
        environment
    );

    // Use the shared consumer group
    let group_name = crate::integration::processor::unified_stream_processor::ENVIRONMENT_CONSUMER_GROUP.to_string();

    // Use the node-specific consumer name
    let consumer_name = crate::integration::processor::unified_stream_processor::get_environment_consumer_name(node_id);

    info!("Attempting recovery for environment-based stream:");
    info!("  Stream: {}", stream_key);
    info!("  Group: {}, Consumer: {}", group_name, consumer_name);

    // Delegate to the main recovery function
    recover_unified_stream_processor(
        &mut conn,
        &stream_key,
        &group_name,
        &consumer_name,
        site_id,
        node_id,
        debug_mode
    )
}
