// Unified Stream Processor Module for gNode
//
// This module provides a implementation of the unified stream
// approach, where commands and responses share a single stream with optimized
// RESP3 representation. It handles message encoding/decoding, stream operations,
// and consumer group management.

use log::{info, warn, debug, trace};
use redis::{Connection, RedisResult};
use crate::integration::processor::circuit_breakers::get_initialization_state;
use crate::integration::current_timestamp_ms;

use crate::config::{GNodeSettings};
use crate::integration::{
    IntegrationResult,
    error_handlings::stream_processing_error,
};


/// Unified stream name generator
///
/// This helper function generates standardized unified stream names
/// for the gNode protocol using the provided parameters.
///
/// # Arguments
///
/// * `site_id` - Site identifier for namespacing
/// * `stream_prefix` - Stream prefix to use in the stream name
/// * `node_id` - Node identifier
///
/// # Returns
///
/// * `String` - Unified stream name
pub fn get_unified_stream(site_id: &str, stream_prefix: &str, node_id: &str) -> String {
    format!("{{{0}}}:{1}:unified:{2}", site_id, stream_prefix, node_id)
}

// ============================================================================
// Environment-based stream key functions (Multi-node architecture)
// ============================================================================
// These functions generate stream keys using the environment dimension,
// enabling multiple nodes to share streams via consumer groups.
//
// Canonical stream pattern: {site_id}:{stream_prefix}:unified:{environment}
// Matches Lua (gnode_stream.lua) and PHP (gNodeClient.php) key construction.
// The {site_id} hash tag ensures all keys for a site hash to the same slot
// in a ValKey Cluster deployment.
//
// Consumer group: gnode-workers (shared across all nodes in environment)
// Consumer name: gnode-{node_id} (unique per node)
// ============================================================================

/// Generate unified stream key with environment dimension
/// Pattern: {site_id}:{stream_prefix}:unified:{environment}
///
/// All nodes in the same environment share this stream via consumer groups,
/// enabling automatic load distribution across workers.
pub fn get_environment_unified_stream(site_id: &str, stream_prefix: &str, environment: &str) -> String {
    format!("{{{0}}}:{1}:unified:{2}", site_id, stream_prefix, environment)
}

/// Generate health stream key with environment dimension
/// Pattern: {site_id}:{stream_prefix}:health:{environment}
pub fn get_environment_health_stream(site_id: &str, stream_prefix: &str, environment: &str) -> String {
    format!("{{{0}}}:{1}:health:{2}", site_id, stream_prefix, environment)
}

/// Generate broadcast stream key with environment dimension
/// Pattern: {site_id}:{stream_prefix}:broadcast:{environment}
pub fn get_environment_broadcast_stream(site_id: &str, stream_prefix: &str, environment: &str) -> String {
    format!("{{{0}}}:{1}:broadcast:{2}", site_id, stream_prefix, environment)
}

/// Generate topology storage key with environment dimension
/// Pattern: {site_id}:{stream_prefix}:topology:{environment}
pub fn get_environment_topology_key(site_id: &str, stream_prefix: &str, environment: &str) -> String {
    format!("{{{0}}}:{1}:topology:{2}", site_id, stream_prefix, environment)
}

/// Generate consumer name for a node in the environment
/// Pattern: gnode-{node_id}
///
/// Each node in the environment has a unique consumer name within
/// the shared consumer group (gnode-workers).
pub fn get_environment_consumer_name(node_id: &str) -> String {
    format!("gnode-{}", node_id)
}

/// Fixed consumer group name for environment-based streams
/// All nodes in the same environment share this consumer group
pub const ENVIRONMENT_CONSUMER_GROUP: &str = "gnode-workers";

/// DTAP wrapper: Initialize streams using environment as the namespace
/// Stream pattern: {environment}:gnode:unified:{node_id}
pub fn initialize_dtap_streams(
    conn: &mut Connection,
    environment: &str,
    node_id: &str,
    stream_prefix: &str,
    debug_mode: bool
) -> IntegrationResult<()> {
    // DTAP mode: Use environment as the site_id
    initialize_unified_stream(conn, node_id, environment, stream_prefix, debug_mode)?;

    // Initialize health stream for this environment
    let health_stream = format!("{{{0}}}:{1}:health:{2}", environment, stream_prefix, node_id);
    debug!("Initializing DTAP health stream: {}", health_stream);

    // Create the health stream consumer group if it doesn't exist
    let _: RedisResult<()> = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(&health_stream)
        .arg("gnode-daemon")
        .arg("0")
        .arg("MKSTREAM")
        .query(conn);

    info!("DTAP streams initialized for environment: {}, node: {}", environment, node_id);
    Ok(())
}

/// Initialize unified stream processor for a node
///
/// This function creates a unified stream and its consumer groups for
/// both command processing and response delivery.
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
/// * `IntegrationResult<()>` - Success or error
pub fn initialize_unified_stream(
    conn: &mut Connection,
    node_id: &str,
    site_id: &str,
    stream_prefix: &str,
    debug_mode: bool
) -> IntegrationResult<()> {
    // Get stream name
    let stream_key = get_unified_stream(site_id, stream_prefix, node_id);
    
    // Get initialization state (handles lock poisoning gracefully)
    let mut states = get_initialization_state(node_id);
    let state = match states.get_mut(node_id) {
        Some(s) => s,
        None => return Err(stream_processing_error(
            format!("Node state missing for '{}' after initialization", node_id)
        )),
    };
    let config = GNodeSettings::default();
    
    // Check if initialization is already complete
    if state.completed {
        if debug_mode {
            debug!("Unified stream for node {} is already initialized", node_id);
        }
        return Ok(());
    }
    
    // Check if we should attempt initialization based on circuit breaker
    if !state.should_attempt(config.circuit_breaker_cooldown_secs) {
        debug!("Circuit breaker active for node {}, skipping initialization", node_id);
        return Err(stream_processing_error(
            format!("Circuit breaker active for node {}", node_id)
        ));
    }
    
    // Record initialization attempt
    state.record_attempt();
    
    info!("Initializing unified stream processor for node {}: stream={}", 
        node_id, stream_key);
    
    // Check if consumer groups already exist (single query for both - P3AF001 fix)
    let (cmd_group_exists, client_group_exists): (bool, bool) = redis::cmd("XINFO")
        .arg("GROUPS")
        .arg(&stream_key)
        .query::<Vec<redis::Value>>(conn)
        .map_or((false, false), |groups| {
            let mut daemon_found = false;
            let mut client_found = false;
            for group in &groups {
                if let redis::Value::Array(items) = group {
                    for (i, item) in items.iter().enumerate() {
                        if i % 2 == 0 && item == &redis::Value::BulkString(b"name".to_vec()) {
                            if let Some(redis::Value::BulkString(name)) = items.get(i + 1) {
                                let name_str = String::from_utf8_lossy(name);
                                if name_str == "gnode-daemon" {
                                    daemon_found = true;
                                } else if name_str == "gnode-client" {
                                    client_found = true;
                                }
                            }
                        }
                    }
                }
            }
            (daemon_found, client_found)
        });
    
    // If both groups exist, consider initialization complete and return early
    if cmd_group_exists && client_group_exists {
        if debug_mode {
            debug!("Both consumer groups already exist for stream {}", stream_key);
        }
        state.record_success();
        info!("Unified stream processor initialization complete for node {}", node_id);
        return Ok(());
    }
    
    // Ensure the stream exists with an initial message
    let stream_exists: bool = redis::cmd("EXISTS")
        .arg(&stream_key)
        .query(conn)
        .map_err(|e| {
            state.record_failure(config.circuit_breaker_threshold);
            stream_processing_error(format!("Failed to check if stream exists: {}", e))
        })?;
    
    if !stream_exists {
        if debug_mode {
            debug!("Creating unified stream {}", stream_key);
        }
        
        let _: String = redis::cmd("XADD")
            .arg(&stream_key)
            .arg("*")
            .arg("init")
            .arg("true")
            .arg("t")
            .arg("i") // i = initialization
            .arg("ss")
            .arg(site_id)
            .arg("sn")
            .arg("system")
            .arg("ts")
            .arg(current_timestamp_ms().to_string())
            .arg("_gh")  // Special marker to indicate consumer group assignment
            .arg("none")         // This message should not be processed by either consumer group
            .query(conn)
            .map_err(|e| {
                state.record_failure(config.circuit_breaker_threshold);
                stream_processing_error(format!("Failed to create stream: {}", e))
            })?;
    }
    
    // Create command processor consumer group if it doesn't exist
    if !cmd_group_exists {
        let cmd_group_result: RedisResult<String> = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(&stream_key)
            .arg("gnode-daemon")
            .arg("$")
            .arg("MKSTREAM")
            .query(conn);
        
        match cmd_group_result {
            Ok(_) => {
                info!("Created command processor consumer group for {}", node_id);
                
                // Configure the command processor consumer group to only read command messages (t=c)
                trace!("Configuring command processor consumer group to only see command (t=c) messages");
                let _ = redis::cmd("XAUTOCLAIM")
                    .arg(&stream_key)
                    .arg("gnode-daemon")
                    .arg("INITIALIZER")
                    .arg("0")  // Claim all pending messages regardless of idle time
                    .arg("0")  // Start from the beginning
                    .arg("COUNT").arg(1000)  // Reasonable batch size
                    .query::<()>(conn);
            },
            Err(e) => {
                let error_str = e.to_string();
                if error_str.contains("BUSYGROUP") {
                    if debug_mode {
                        debug!("Command processor consumer group already exists for {}", node_id);
                    }
                } else {
                    warn!("Failed to create command processor consumer group: {}", e);
                    state.record_failure(config.circuit_breaker_threshold);
                }
            }
        }
    }
    
    // Create client consumer group if it doesn't exist
    if !client_group_exists {
        // CRITICAL FIX: Use "0" instead of "$" so the group reads ALL messages from beginning
        let client_group_result: RedisResult<String> = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(&stream_key)
            .arg("gnode-client")
            .arg("0")  // FIXED: Read ALL messages from the beginning, not just new ones
            .arg("MKSTREAM")
            .query(conn);
        
        match client_group_result {
            Ok(_) => {
                info!("Created client consumer group for {} starting from beginning (ID 0)", node_id);
                
                // Configure the client consumer group to read both regular and batch response messages (t=r & t=br)
                trace!("Configuring client consumer group to see all response messages (t=r and t=br)");
                let _ = redis::cmd("XAUTOCLAIM")
                    .arg(&stream_key)
                    .arg("gnode-client")
                    .arg("INITIALIZER")
                    .arg("0")  // Claim all pending messages regardless of idle time
                    .arg("0")  // Start from the beginning
                    .arg("COUNT").arg(1000)  // Reasonable batch size
                    .query::<()>(conn);
            },
            Err(e) => {
                let error_str = e.to_string();
                if error_str.contains("BUSYGROUP") {
                    if debug_mode {
                        debug!("Client consumer group already exists for {} - recreating from beginning", node_id);
                    }
                    
                    // For existing client groups that might have been created with "$" (only new messages),
                    // we use SETID to atomically reset to beginning (P2BF001 FIX)

                    // Use XGROUP SETID to atomically reset the last-delivered-ID
                    let setid_result: RedisResult<()> = redis::cmd("XGROUP")
                        .arg("SETID")
                        .arg(&stream_key)
                        .arg("gnode-client")
                        .arg("0")  // Reset to beginning
                        .query(conn);

                    match setid_result {
                        Ok(_) => {
                            trace!("Successfully reset client consumer group for {} to beginning (ID 0)", node_id);
                        },
                        Err(set_err) => {
                            warn!("Failed to reset client consumer group ID: {}", set_err);
                            state.record_failure(config.circuit_breaker_threshold);
                        }
                    }
                } else {
                    warn!("Failed to create client consumer group: {}", e);
                    state.record_failure(config.circuit_breaker_threshold);
                }
            }
        }
    } else if debug_mode {
        debug!("Client consumer group exists - ensuring it's configured properly");
        
        // For existing groups, verify they can read from beginning
        // This is done by creating a test consumer and reading from ID 0,
        // then destroying the test consumer
        
        let test_consumer = format!("test-config-{}", current_timestamp_ms());
        
        // Try reading from the beginning with the test consumer
        let test_read: RedisResult<()> = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg("gnode-client")
            .arg(&test_consumer)
            .arg("COUNT")
            .arg(1)
            .arg("STREAMS")
            .arg(&stream_key)
            .arg("0")  // Try reading from beginning
            .query(conn);
        
        // If reading from beginning fails, reset the group ID (P2BF001 FIX)
        if let Err(read_err) = test_read {
            warn!("Client consumer group exists but cannot read from beginning: {}", read_err);
            warn!("Resetting client consumer group ID to fix the issue");

            // Use XGROUP SETID to atomically reset the last-delivered-ID
            let setid_result: RedisResult<()> = redis::cmd("XGROUP")
                .arg("SETID")
                .arg(&stream_key)
                .arg("gnode-client")
                .arg("0")  // Reset to beginning
                .query(conn);

            match setid_result {
                Ok(_) => {
                    trace!("Successfully reset client consumer group for {} to beginning (ID 0)", node_id);
                },
                Err(set_err) => {
                    warn!("Failed to reset client consumer group ID: {}", set_err);
                    state.record_failure(config.circuit_breaker_threshold);
                }
            }
        } else if debug_mode {
            debug!("Verified client consumer group can read from beginning");
        }
    }
    
    // Record successful initialization
    state.record_success();
    
    info!("Unified stream processor initialization complete for node {}", node_id);
    Ok(())
}

/// Health stream name generator
///
/// This helper function generates standardized health stream names
/// for the gNode load-aware service discovery protocol.
///
/// # Arguments
///
/// * `site_id` - Site identifier for namespacing
/// * `stream_prefix` - Stream prefix to use in the stream name
/// * `node_id` - Node identifier
///
/// # Returns
///
/// * `String` - Health stream name
pub fn get_health_stream(site_id: &str, stream_prefix: &str, node_id: &str) -> String {
    format!("{{{0}}}:{1}:health:{2}", site_id, stream_prefix, node_id)
}

/// Initialize health stream for load-aware service discovery
///
/// This function creates a health stream and its consumer group for
/// receiving load metric updates from services.
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
/// * `IntegrationResult<String>` - Health stream name or error
pub fn initialize_health_stream(
    conn: &mut Connection,
    node_id: &str,
    site_id: &str,
    stream_prefix: &str,
    debug_mode: bool
) -> IntegrationResult<String> {
    let health_stream = get_health_stream(site_id, stream_prefix, node_id);
    let _config = GNodeSettings::default();

    info!("Initializing health stream for node {}: stream={}", node_id, health_stream);

    // Check if consumer group already exists
    let group_exists: bool = redis::cmd("XINFO")
        .arg("GROUPS")
        .arg(&health_stream)
        .query::<Vec<redis::Value>>(conn)
        .is_ok_and(|groups| {
            groups.iter().any(|group| {
                match group {
                    redis::Value::Array(items) => {
                        items.iter().enumerate().any(|(i, item)| {
                            if i % 2 == 0 && item == &redis::Value::BulkString(b"name".to_vec()) {
                                if let Some(redis::Value::BulkString(name)) = items.get(i+1) {
                                    return String::from_utf8_lossy(name) == "gnode-daemon";
                                }
                            }
                            false
                        })
                    },
                    _ => false
                }
            })
        });

    if group_exists {
        if debug_mode {
            debug!("Health stream consumer group already exists for {}", node_id);
        }
        return Ok(health_stream);
    }

    // Create the health stream with an initial message
    let stream_exists: bool = redis::cmd("EXISTS")
        .arg(&health_stream)
        .query(conn)
        .map_err(|e| {
            stream_processing_error(format!("Failed to check if health stream exists: {}", e))
        })?;

    if !stream_exists {
        if debug_mode {
            debug!("Creating health stream {}", health_stream);
        }

        let _: String = redis::cmd("XADD")
            .arg(&health_stream)
            .arg("*")
            .arg("init")
            .arg("true")
            .arg("t")
            .arg("hi") // hi = health initialization
            .arg("ss")
            .arg(site_id)
            .arg("ts")
            .arg(current_timestamp_ms().to_string())
            .query(conn)
            .map_err(|e| {
                stream_processing_error(format!("Failed to create health stream: {}", e))
            })?;
    }

    // Create daemon consumer group (starts from ID "0" to read all messages)
    let group_result: RedisResult<String> = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(&health_stream)
        .arg("gnode-daemon")
        .arg("0") // Start from beginning to process all health updates
        .arg("MKSTREAM")
        .query(conn);

    match group_result {
        Ok(_) => {
            info!("Created health stream consumer group for {} (daemon-only, start ID: 0)", node_id);
        },
        Err(e) => {
            let error_str = e.to_string();
            if error_str.contains("BUSYGROUP") {
                if debug_mode {
                    debug!("Health stream consumer group already exists for {}", node_id);
                }
            } else {
                warn!("Failed to create health stream consumer group: {}", e);
                return Err(stream_processing_error(format!(
                    "Failed to create health stream consumer group: {}", e
                )));
            }
        }
    }

    info!("Health stream initialization complete for node {}", node_id);
    Ok(health_stream)
}

/// Initialize both unified and health streams
///
/// This function creates both the unified command/response stream and the
/// dedicated health metrics stream, returning both stream keys.
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
/// * `IntegrationResult<(String, String)>` - Tuple of (unified_stream, health_stream) or error
pub fn initialize_streams(
    conn: &mut Connection,
    node_id: &str,
    site_id: &str,
    stream_prefix: &str,
    debug_mode: bool
) -> IntegrationResult<(String, String)> {
    // Initialize unified stream first
    initialize_unified_stream(conn, node_id, site_id, stream_prefix, debug_mode)?;
    let unified_stream = get_unified_stream(site_id, stream_prefix, node_id);

    // Initialize health stream
    let health_stream = initialize_health_stream(conn, node_id, site_id, stream_prefix, debug_mode)?;

    info!("Both streams initialized successfully for node {}: unified={}, health={}",
        node_id, unified_stream, health_stream);

    Ok((unified_stream, health_stream))
}

// ============================================================================
// Environment-based stream initialization (Multi-node architecture)
// ============================================================================

/// Initialize environment-based streams for multi-node architecture
///
/// This function creates shared streams and consumer groups for an environment,
/// enabling multiple nodes to share the same streams via consumer groups.
///
/// Stream pattern: {site_id}:{stream_prefix}:{environment}:unified
/// Consumer group: gnode-workers (shared across all nodes)
/// Consumer name: gnode-{node_id} (unique per node)
///
/// # Arguments
///
/// * `conn` - Redis connection
/// * `site_id` - Site identifier for namespacing
/// * `environment` - DTAP environment (testing, staging, acceptance, production)
/// * `node_id` - Unique node identifier (used for consumer name)
/// * `stream_prefix` - Stream prefix
/// * `debug_mode` - Whether debug mode is enabled
///
/// # Returns
///
/// * `IntegrationResult<(String, String)>` - Tuple of (unified_stream, health_stream) or error
pub fn initialize_environment_streams(
    conn: &mut Connection,
    site_id: &str,
    environment: &str,
    node_id: &str,
    stream_prefix: &str,
    debug_mode: bool
) -> IntegrationResult<(String, String)> {
    let unified_stream = get_environment_unified_stream(site_id, stream_prefix, environment);
    let health_stream = get_environment_health_stream(site_id, stream_prefix, environment);
    let consumer_name = get_environment_consumer_name(node_id);

    info!("Initializing environment-based streams:");
    info!("  Site: {}, Environment: {}, Node: {}", site_id, environment, node_id);
    info!("  Unified stream: {}", unified_stream);
    info!("  Health stream: {}", health_stream);
    info!("  Consumer group: {}, Consumer: {}", ENVIRONMENT_CONSUMER_GROUP, consumer_name);

    // Initialize unified stream with environment-based consumer groups
    initialize_environment_unified_stream(conn, site_id, environment, node_id, stream_prefix, debug_mode)?;

    // Initialize health stream with environment-based consumer groups
    initialize_environment_health_stream(conn, site_id, environment, node_id, stream_prefix, debug_mode)?;

    info!("Environment-based streams initialized successfully");
    Ok((unified_stream, health_stream))
}

/// Initialize environment-based unified stream
fn initialize_environment_unified_stream(
    conn: &mut Connection,
    site_id: &str,
    environment: &str,
    node_id: &str,
    stream_prefix: &str,
    debug_mode: bool
) -> IntegrationResult<()> {
    let stream_key = get_environment_unified_stream(site_id, stream_prefix, environment);
    let consumer_name = get_environment_consumer_name(node_id);

    if debug_mode {
        debug!("Initializing environment unified stream: {}", stream_key);
    }

    // Check if stream exists
    let stream_exists: bool = redis::cmd("EXISTS")
        .arg(&stream_key)
        .query(conn)
        .map_err(|e| stream_processing_error(format!("Failed to check if stream exists: {}", e)))?;

    if !stream_exists {
        if debug_mode {
            debug!("Creating environment unified stream {}", stream_key);
        }

        let _: String = redis::cmd("XADD")
            .arg(&stream_key)
            .arg("*")
            .arg("init")
            .arg("true")
            .arg("t")
            .arg("i") // i = initialization
            .arg("ss")
            .arg(site_id)
            .arg("env")
            .arg(environment)
            .arg("sn")
            .arg("system")
            .arg("ts")
            .arg(current_timestamp_ms().to_string())
            .arg("_gh")
            .arg("none")
            .query(conn)
            .map_err(|e| stream_processing_error(format!("Failed to create stream: {}", e)))?;
    }

    // Create shared consumer group (gnode-workers) if it doesn't exist
    let daemon_group_result: RedisResult<String> = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(&stream_key)
        .arg(ENVIRONMENT_CONSUMER_GROUP)
        .arg("$")
        .arg("MKSTREAM")
        .query(conn);

    match daemon_group_result {
        Ok(_) => {
            info!("Created environment consumer group '{}' for stream {}", ENVIRONMENT_CONSUMER_GROUP, stream_key);
        },
        Err(e) => {
            let error_str = e.to_string();
            if error_str.contains("BUSYGROUP") {
                if debug_mode {
                    debug!("Consumer group '{}' already exists for stream {}", ENVIRONMENT_CONSUMER_GROUP, stream_key);
                }
            } else {
                return Err(stream_processing_error(format!(
                    "Failed to create consumer group '{}': {}", ENVIRONMENT_CONSUMER_GROUP, e
                )));
            }
        }
    }

    // Create client consumer group (for responses) if it doesn't exist
    let client_group_result: RedisResult<String> = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(&stream_key)
        .arg("gnode-client")
        .arg("0") // Read from beginning for responses
        .arg("MKSTREAM")
        .query(conn);

    match client_group_result {
        Ok(_) => {
            info!("Created client consumer group for stream {}", stream_key);
        },
        Err(e) => {
            let error_str = e.to_string();
            if !error_str.contains("BUSYGROUP") {
                warn!("Failed to create client consumer group: {}", e);
            }
        }
    }

    info!("Environment unified stream ready: {} (consumer: {})", stream_key, consumer_name);
    Ok(())
}

/// Initialize environment-based health stream
fn initialize_environment_health_stream(
    conn: &mut Connection,
    site_id: &str,
    environment: &str,
    node_id: &str,
    stream_prefix: &str,
    debug_mode: bool
) -> IntegrationResult<()> {
    let health_stream = get_environment_health_stream(site_id, stream_prefix, environment);
    let consumer_name = get_environment_consumer_name(node_id);

    if debug_mode {
        debug!("Initializing environment health stream: {}", health_stream);
    }

    // Check if stream exists
    let stream_exists: bool = redis::cmd("EXISTS")
        .arg(&health_stream)
        .query(conn)
        .map_err(|e| stream_processing_error(format!("Failed to check if health stream exists: {}", e)))?;

    if !stream_exists {
        if debug_mode {
            debug!("Creating environment health stream {}", health_stream);
        }

        let _: String = redis::cmd("XADD")
            .arg(&health_stream)
            .arg("*")
            .arg("init")
            .arg("true")
            .arg("t")
            .arg("hi") // hi = health initialization
            .arg("ss")
            .arg(site_id)
            .arg("env")
            .arg(environment)
            .arg("ts")
            .arg(current_timestamp_ms().to_string())
            .query(conn)
            .map_err(|e| stream_processing_error(format!("Failed to create health stream: {}", e)))?;
    }

    // Create shared consumer group for health stream
    let group_result: RedisResult<String> = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(&health_stream)
        .arg(ENVIRONMENT_CONSUMER_GROUP)
        .arg("0") // Start from beginning for health updates
        .arg("MKSTREAM")
        .query(conn);

    match group_result {
        Ok(_) => {
            info!("Created environment health consumer group for {}", health_stream);
        },
        Err(e) => {
            let error_str = e.to_string();
            if !error_str.contains("BUSYGROUP") {
                warn!("Failed to create health consumer group: {}", e);
            }
        }
    }

    info!("Environment health stream ready: {} (consumer: {})", health_stream, consumer_name);
    Ok(())
}

/// Broadcast stream name generator
///
/// This helper function generates standardized broadcast stream names
/// for the gNode protocol. Broadcast streams are GLOBAL (not per-node) and
/// use XREAD (not XREADGROUP) for pub-sub semantics.
///
/// # Arguments
///
/// * `site_id` - Site identifier for namespacing
/// * `stream_prefix` - Stream prefix to use in the stream name
///
/// # Returns
///
/// * `String` - Broadcast stream name
pub fn get_broadcast_stream(site_id: &str, stream_prefix: &str) -> String {
    format!("{{{0}}}:{1}:broadcast:global", site_id, stream_prefix)
}

/// Initialize broadcast stream for pub-sub messaging
///
/// This function creates a broadcast stream for messages that should be
/// seen by ALL nodes/clients. It does NOT create consumer groups since
/// broadcast uses XREAD for replication semantics (not work distribution).
///
/// Broadcast stream characteristics:
/// - No consumer groups (XREAD-based, not XREADGROUP)
/// - No PEL (no acknowledgments needed)
/// - Auto-XTRIM based on retention time (not ACK status)
/// - All nodes read independently from their last-seen-ID
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
/// * `IntegrationResult<String>` - Broadcast stream name or error
pub fn initialize_broadcast_stream(
    conn: &mut Connection,
    site_id: &str,
    stream_prefix: &str,
    debug_mode: bool
) -> IntegrationResult<String> {
    let broadcast_stream = get_broadcast_stream(site_id, stream_prefix);

    info!("Initializing broadcast stream: stream={}", broadcast_stream);

    // Check if stream exists
    let stream_exists: bool = redis::cmd("EXISTS")
        .arg(&broadcast_stream)
        .query(conn)
        .map_err(|e| {
            stream_processing_error(format!("Failed to check if broadcast stream exists: {}", e))
        })?;

    if !stream_exists {
        if debug_mode {
            debug!("Creating broadcast stream {}", broadcast_stream);
        }

        // Create the broadcast stream with an initial message
        let _: String = redis::cmd("XADD")
            .arg(&broadcast_stream)
            .arg("*")
            .arg("init")
            .arg("true")
            .arg("t")
            .arg("bi") // bi = broadcast initialization
            .arg("ss")
            .arg(site_id)
            .arg("ts")
            .arg(current_timestamp_ms().to_string())
            .arg("msg")
            .arg("Broadcast stream initialized")
            .query(conn)
            .map_err(|e| {
                stream_processing_error(format!("Failed to create broadcast stream: {}", e))
            })?;

        info!("Created broadcast stream {}", broadcast_stream);
    } else if debug_mode {
        debug!("Broadcast stream already exists: {}", broadcast_stream);
    }

    info!("Broadcast stream initialization complete");
    Ok(broadcast_stream)
}