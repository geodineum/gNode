//! Consumer groups module for Geodineum Service Daemon
//!
//! This module provides high-throughput operations using ValKey/Redis consumer groups.
//! Consumer groups offer significant performance advantages (4-8x) over traditional
//! stream processing methods by eliminating polling and providing efficient message
//! distribution across consumers.
//!
//! ## Orphan recovery and consumer lifetime
//!
//! Ownership of a stream entry lives in the group's Pending Entries List until it
//! is acknowledged. A consumer that stops without acknowledging leaves the entry
//! owned by a name that will never return, and ValKey does not redeliver it. So
//! recovery is a liveness property every node must perform, and consumers must be
//! removed once they are gone — otherwise group metadata grows with every
//! reconnection of an intermittent participant.
//!
//! Two rules keep recovery from causing what it prevents:
//!   * inspect before claiming — ownership must not move for work this node will
//!     not perform, so pending entries are read first and only exposed ones claimed
//!   * never remove a consumer holding pending entries — DELCONSUMER discards
//!     them, turning a stuck entry into a lost one
//!
//! The module has three layers of functionality:
//! 1. Core consumer group operations (create, read, ack, etc.)
//! 2. High-level batch processing utilities
//! 3. Adaptive backoff and recovery strategies

use std::time::{Instant, Duration};
use log::{info, warn, debug, error, trace};
use redis::{Connection, RedisResult, Value};
use serde_json;
use crate::integration::{
    error_handlings::{IntegrationResult, consumer_group_error, log_error, stream_processing_error},
    valkey_functions::execute_function,
    connection_manager,
};
use crate::config::GNodeSettings;
use crate::daemon::GNodeDaemon;


/// How often a node looks for entries whose owner stopped.
const RECLAIM_INTERVAL_MS: u64 = 5_000;

/// How long an entry must be idle before it is treated as orphaned. Must exceed
/// the slowest legitimate processing time: reclaim below it steals live work and
/// the entry runs twice.
const RECLAIM_MIN_IDLE_MS: u64 = 30_000;

/// Entries reclaimed per pass. Bounded so recovery cannot monopolise a cycle.
const RECLAIM_BATCH: usize = 10;

/// How often stale consumers are swept. Bookkeeping, not liveness, so far less
/// frequent than reclaim.
const REAP_INTERVAL_MS: u64 = 300_000;

/// Idle time after which a consumer holding nothing is considered gone. Well
/// beyond any normal quiet period, so a merely idle node is never reaped.
const REAP_IDLE_MS: u64 = 3_600_000;

/// State for a consumer group processor
#[derive(Clone, Debug)]
pub struct ConsumerGroupState {
    /// Current batch size (dynamically adjusted based on load)
    pub batch_size: usize,
    
    /// Last time a message was processed
    pub last_activity: Instant,
    
    /// Last time pending messages were checked
    pub last_pending_check: Instant,
    
    /// Current backoff time in milliseconds
    pub current_backoff_ms: u64,
    
    /// Base backoff time in milliseconds
    pub base_backoff_ms: u64,
    
    /// Maximum backoff time in milliseconds
    pub max_backoff_ms: u64,
    
    /// Number of consecutive errors
    pub consecutive_errors: usize,
    
    /// Maximum consecutive errors before recovery action
    pub max_consecutive_errors: usize,
    
    /// Last time commands were empty
    pub last_empty_time: Instant,
}

impl ConsumerGroupState {
    /// Create a new consumer group state
    pub fn new(initial_batch_size: usize, base_backoff_ms: u64) -> Self {
        let now = Instant::now();
        ConsumerGroupState {
            batch_size: initial_batch_size,
            last_activity: now,
            last_pending_check: now,
            current_backoff_ms: base_backoff_ms,
            base_backoff_ms,
            max_backoff_ms: 10000, // Default 10 seconds
            consecutive_errors: 0,
            max_consecutive_errors: 5, // Default 5 consecutive errors before recovery
            last_empty_time: now,
        }
    }
    
    /// Create a new consumer group state with custom configuration
    pub fn with_config(initial_batch_size: usize, base_backoff_ms: u64, max_backoff_ms: u64, max_consecutive_errors: usize) -> Self {
        let now = Instant::now();
        ConsumerGroupState {
            batch_size: initial_batch_size,
            last_activity: now,
            last_pending_check: now,
            current_backoff_ms: base_backoff_ms,
            base_backoff_ms,
            max_backoff_ms,
            consecutive_errors: 0,
            max_consecutive_errors,
            last_empty_time: now,
        }
    }
    
    /// Check if pending messages should be processed
    pub fn should_check_pending(&self, interval_ms: u64) -> bool {
        self.last_pending_check.elapsed().as_millis() as u64 >= interval_ms
    }
    
    /// Update the pending check timestamp
    pub fn update_pending_check(&mut self) {
        self.last_pending_check = Instant::now();
    }
    
    /// Update last activity timestamp
    pub fn update_activity(&mut self) {
        self.last_activity = Instant::now();
    }
    
    /// Adjust batch size based on message count
    pub fn adjust_batch_size(&mut self, messages_processed: usize, min_batch_size: usize, max_batch_size: usize) {
        // If we're processing at max capacity, increase batch size
        if messages_processed >= self.batch_size {
            self.batch_size = std::cmp::min(self.batch_size * 2, max_batch_size);
            debug!("Increased batch size to {}", self.batch_size);
        } else if messages_processed < self.batch_size / 4 && self.batch_size > min_batch_size {
            // If we're processing less than 25% of batch, decrease size
            self.batch_size = std::cmp::max(self.batch_size / 2, min_batch_size);
            debug!("Decreased batch size to {}", self.batch_size);
        }
        
        // Always update activity when processing messages
        if messages_processed > 0 {
            self.update_activity();
        }
    }
    
    /// Apply exponential backoff
    pub fn apply_backoff(&mut self, max_backoff_ms: u64) {
        self.current_backoff_ms = std::cmp::min(self.current_backoff_ms * 2, max_backoff_ms);
        debug!("Applied backoff: {}ms", self.current_backoff_ms);
    }
    
    /// Get current backoff duration
    pub fn backoff_duration(&self) -> Duration {
        Duration::from_millis(self.current_backoff_ms)
    }
    
    /// Reset error count and backoff after successful operation
    pub fn reset_after_success(&mut self) {
        self.consecutive_errors = 0;
        self.current_backoff_ms = self.base_backoff_ms;
        self.update_activity();
    }
    
    /// Register an error
    pub fn register_error(&mut self) {
        self.consecutive_errors += 1;
        // Apply backoff when an error occurs
        self.apply_backoff(self.max_backoff_ms);
    }
    
    /// Check if we should attempt recovery based on consecutive errors
    pub fn should_attempt_recovery(&self) -> bool {
        self.consecutive_errors >= self.max_consecutive_errors
    }
    
    /// Get time since last activity
    pub fn time_since_last_activity(&self) -> Duration {
        self.last_activity.elapsed()
    }
    
    /// Check if this consumer has been inactive for too long
    pub fn is_inactive(&self, threshold_ms: u64) -> bool {
        self.time_since_last_activity().as_millis() as u64 > threshold_ms
    }
    
    /// Update empty time
    pub fn update_empty_time(&mut self) {
        self.last_empty_time = Instant::now();
    }
    
    /// Time since last empty result
    pub fn time_since_empty(&self) -> Duration {
        self.last_empty_time.elapsed()
    }
}

/// Consumer Group Node Processor State for multi-threaded processing
#[derive(Clone)]
pub struct ConsumerGroupNodeState {
    /// Last time a message was processed
    pub last_empty_time: Instant,
    
    /// Number of consecutive errors 
    pub consecutive_errors: usize,
    
    /// Current batch size (dynamically adjusted based on load)
    pub batch_size: usize,
    
    /// Current backoff time in milliseconds
    pub backoff_ms: u64,
}

impl ConsumerGroupNodeState {
    /// Create a new ConsumerGroupNodeState with default values
    pub fn new(initial_batch_size: usize, base_backoff_ms: u64) -> Self {
        Self {
            last_empty_time: Instant::now(),
            consecutive_errors: 0,
            batch_size: initial_batch_size,
            backoff_ms: base_backoff_ms,
        }
    }
    
    /// Reset consecutive errors after successful processing
    pub fn reset_errors(&mut self) {
        self.consecutive_errors = 0;
    }
    
    /// Apply backoff strategy when there are no messages (for ConsumerGroupNodeState)
    pub fn apply_backoff(&mut self, max_backoff_ms: u64) {
        self.backoff_ms = std::cmp::min(self.backoff_ms * 2, max_backoff_ms);
    }
    
    
    /// Adjust batch size based on processing results
    pub fn adjust_batch_size(&mut self, processed_count: usize, min_batch_size: usize, max_batch_size: usize) {
        if processed_count >= self.batch_size && self.batch_size < max_batch_size {
            self.batch_size = std::cmp::min(self.batch_size * 2, max_batch_size);
        } else if processed_count < self.batch_size / 4 && self.batch_size > min_batch_size {
            self.batch_size = std::cmp::max(self.batch_size / 2, min_batch_size);
        }
    }
}

// Use the ConsumerGroupState defined above

/// Create a consumer group for a stream
///
/// This function creates a consumer group for a stream, using the specified
/// configuration. It will create the stream if it doesn't exist.
///
/// # Arguments
///
/// * `conn` - Redis connection
/// * `stream_key` - Stream key
/// * `group_name` - Consumer group name
/// * `start_id` - Start ID for the group ($ for only new messages)
/// * `mkstream` - Whether to create the stream if it doesn't exist
/// * `site_id` - Site identifier for namespacing
/// * `debug_mode` - Whether debug mode is enabled
///
/// # Returns
///
/// * `IntegrationResult<()>` - Success or error with context
pub fn create_consumer_group(
    conn: &mut Connection,
    stream_key: &str,
    group_name: &str,
    start_id: &str,
    mkstream: bool,
    site_id: &str,
    debug_mode: bool
) -> IntegrationResult<()> {
    // Try ValKey function first
    let result = execute_function(
        conn,
        "GNODE_STREAM_GROUP",
        &[stream_key],
        &[
            group_name,
            start_id,
            if mkstream { "MKSTREAM" } else { "NOMKSTREAM" },
            "CREATE"
        ],
        site_id,
        debug_mode
    );
    
    match result {
        Ok(_) => {
            if debug_mode {
                debug!("Created consumer group {} for stream {}", group_name, stream_key);
            }
            Ok(())
        },
        Err(e) => {
            warn!("ValKey function failed to create consumer group: {}", e);

            // Fallback to direct Redis command
            let mut cmd = redis::cmd("XGROUP");
            cmd.arg("CREATE")
                .arg(stream_key)
                .arg(group_name)
                .arg(start_id);

            if mkstream {
                cmd.arg("MKSTREAM");
            }

            match cmd.query::<String>(conn) {
                Ok(_) => {
                    if debug_mode {
                        debug!("Created consumer group using direct Redis command");
                    }
                    Ok(())
                },
                Err(direct_error) => {
                    // Ignore BUSYGROUP error - group already exists
                    if direct_error.to_string().contains("BUSYGROUP") {
                        if debug_mode {
                            debug!("Consumer group already exists");
                        }
                        Ok(())
                    } else {
                        let error = consumer_group_error(
                            format!("Failed to create consumer group: {}", direct_error)
                        );
                        log_error(&error, "creating consumer group");
                        Err(error)
                    }
                }
            }
        }
    }
}

/// Read messages from a consumer group
///
/// This function reads messages from a stream using a consumer group,
/// with support for batching and blocking.
///
/// # Arguments
///
/// * `conn` - Redis connection
/// * `stream_key` - Stream key
/// * `group_name` - Consumer group name
/// * `consumer_name` - Consumer name
/// * `count` - Maximum number of messages to read
/// * `block_ms` - Block timeout in milliseconds
/// * `site_id` - Site identifier for namespacing
/// * `debug_mode` - Whether debug mode is enabled
///
/// # Returns
///
/// * `IntegrationResult<String>` - JSON-encoded stream data or error
#[allow(clippy::too_many_arguments, clippy::type_complexity)]
pub fn read_group_messages(
    conn: &mut Connection,
    stream_key: &str,
    group_name: &str,
    consumer_name: &str,
    count: usize,
    block_ms: u64,
    site_id: &str,
    debug_mode: bool
) -> IntegrationResult<String> {
    // Try ValKey function first
    let result = execute_function(
        conn,
        "GNODE_STREAM_GROUP_READ",
        &[stream_key],
        &[
            group_name,
            consumer_name,
            &count.to_string(),
            &block_ms.to_string(),
            ">"  // Only new messages
        ],
        site_id,
        debug_mode
    );
    
    match result {
        Ok(json_response) => {
            // The json_response is already a String from execute_function
            let result_str = json_response;
            
            Ok(result_str)
        },
        Err(e) => {
            warn!("ValKey function failed to read group messages: {}", e);

            // Fallback - direct Redis command (Tier 3)
            let result: RedisResult<Vec<(String, Vec<(String, Vec<(String, String)>)>)>> =
                if block_ms > 0 {
                    redis::cmd("XREADGROUP")
                        .arg("GROUP")
                        .arg(group_name)
                        .arg(consumer_name)
                        .arg("COUNT")
                        .arg(count)
                        .arg("BLOCK")
                        .arg(block_ms)
                        .arg("STREAMS")
                        .arg(stream_key)
                        .arg(">")
                        .query(conn)
                } else {
                    redis::cmd("XREADGROUP")
                        .arg("GROUP")
                        .arg(group_name)
                        .arg(consumer_name)
                        .arg("COUNT")
                        .arg(count)
                        .arg("STREAMS")
                        .arg(stream_key)
                        .arg(">")
                        .query(conn)
                };

            match result {
                Ok(msgs) => {
                    // Format output to match ValKey function format
                    let mut formatted_result = Vec::new();

                    for (stream_name, messages) in msgs {
                        let mut formatted_messages = Vec::new();

                        for (id, fields) in messages {
                            let mut formatted_fields = Vec::new();
                            for (field, value) in fields {
                                formatted_fields.push(field);
                                formatted_fields.push(value);
                            }

                            // Wrap formatted fields in a JSON array
                            let fields_str = serde_json::to_string(&formatted_fields)
                                .unwrap_or_else(|_| "[]".to_string());
                            formatted_messages.push(vec![id, fields_str]);
                        }

                        // Convert formatted_messages to JSON string
                        let messages_json = serde_json::to_string(&formatted_messages)
                            .unwrap_or_else(|_| "[]".to_string());
                        formatted_result.push(vec![stream_name, messages_json]);
                    }

                    // Convert to JSON string
                    match serde_json::to_string(&formatted_result) {
                        Ok(json) => Ok(json),
                        Err(_) => Ok("[]".to_string())
                    }
                },
                Err(direct_error) => {
                    let error = consumer_group_error(
                        format!("Failed to read group messages with direct command: {}", direct_error)
                    );
                    log_error(&error, "reading group messages");

                    // Timeout errors are normal and expected
                    if direct_error.to_string().contains("timeout") {
                        return Ok("[]".to_string());
                    }

                    Err(error)
                }
            }
        }
    }
}

/// Delete a consumer from a consumer group
///
/// This function deletes a consumer from a consumer group,
/// reassigning its pending messages to other consumers.
///
/// # Arguments
///
/// * `conn` - Redis connection
/// * `stream_key` - Stream key
/// * `group_name` - Consumer group name
/// * `consumer_name` - Consumer name
/// * `site_id` - Site identifier for namespacing
/// * `debug_mode` - Whether debug mode is enabled
///
/// # Returns
///
/// * `IntegrationResult<usize>` - Number of pending messages reassigned or error
pub fn delete_consumer(
    conn: &mut Connection,
    stream_key: &str,
    group_name: &str,
    consumer_name: &str,
    site_id: &str,
    debug_mode: bool
) -> IntegrationResult<usize> {
    // Try ValKey function first
    let result = execute_function(
        conn,
        "GNODE_STREAM_DELCONSUMER",
        &[stream_key],
        &[
            group_name,
            consumer_name
        ],
        site_id,
        debug_mode
    );
    
    match result {
        Ok(pending_str) => {
            // Parse pending count
            match pending_str.parse::<usize>() {
                Ok(count) => Ok(count),
                Err(_) => Ok(0) // Default to 0 if parsing fails
            }
        },
        Err(e) => {
            if debug_mode {
                debug!("ValKey function failed to delete consumer: {}", e);
            }
            
            // Direct Redis command
            let result: RedisResult<i64> = redis::cmd("XGROUP")
                .arg("DELCONSUMER")
                .arg(stream_key)
                .arg(group_name)
                .arg(consumer_name)
                .query(conn);
            
            match result {
                Ok(pending) => Ok(pending as usize),
                Err(direct_error) => {
                    let error = consumer_group_error(
                        format!("Failed to delete consumer with direct command: {}", direct_error)
                    );
                    log_error(&error, "deleting consumer");
                    Err(error)
                }
            }
        }
    }
}


/// Ensure consumer group exists and has proper start position
///
/// This helper function checks if a consumer group exists for a stream,
/// and creates it if not. It allows specifying whether the group should
/// start reading from the beginning of the stream (ID "0") or only new
/// messages (ID "$").
///
/// # Arguments
///
/// * `conn` - Redis connection
/// * `stream_key` - Stream key
/// * `group_name` - Consumer group name
/// * `from_beginning` - Whether to start from the beginning of the stream
/// * `site_id` - Site identifier for namespacing
/// * `debug_mode` - Whether debug mode is enabled
///
/// # Returns
///
/// * `IntegrationResult<bool>` - Success (with created flag) or error
pub fn ensure_consumer_group(
    conn: &mut Connection,
    stream_key: &str,
    group_name: &str,
    start_from_beginning: bool,
    site_id: &str,
    debug_mode: bool
) -> IntegrationResult<bool> {
    if debug_mode {
        debug!("Ensuring consumer group '{}' exists for stream '{}'", group_name, stream_key);
    }
    
    // Check if the group exists
    let groups_result: RedisResult<Vec<Value>> = redis::cmd("XINFO")
        .arg("GROUPS")
        .arg(stream_key)
        .query(conn);
    
    let mut group_exists = false;
    let mut wrong_start_id = false;
    
    if let Ok(groups) = groups_result {
        for group in groups {
            if let Value::Array(items) = group {
                // Variable to track if we found the group name
                let mut found_name = false;
                let mut found_last_id = false;
                let mut last_id = String::new();
                
                // Look through the items to find the group name
                for i in 0..items.len().saturating_sub(1) {
                    // Look for the "name" key
                    if let Value::BulkString(key_bytes) = &items[i] {
                        let key = String::from_utf8_lossy(key_bytes);
                        
                        if key == "name" {
                            // Check the next item for the value
                            if let Value::BulkString(name_bytes) = &items[i+1] {
                                let name = String::from_utf8_lossy(name_bytes);
                                if name == group_name {
                                    found_name = true;
                                    group_exists = true;
                                    if debug_mode {
                                        debug!("Consumer group '{}' already exists", group_name);
                                    }
                                }
                            }
                        } else if key == "last-id" && found_name {
                            // For gnode-client group, check if it was created with correct start position
                            found_last_id = true;
                            if let Value::BulkString(id_bytes) = &items[i+1] {
                                last_id = String::from_utf8_lossy(id_bytes).to_string();
                                
                                // For gnode-client group, check if it should read from beginning but doesn't
                                if group_name == "gnode-client" && start_from_beginning && last_id != "0" && last_id != "0-0" {
                                    wrong_start_id = true;
                                    if debug_mode {
                                        debug!("Consumer group '{}' exists but has wrong start ID: {}", group_name, last_id);
                                    }
                                }
                            }
                        }
                    }
                }
                
                // Print information when we find the group
                if found_name && found_last_id {
                    info!("Found consumer group '{}' with last-id: {}", group_name, last_id);
                }
            }
            
            // Early break if we found the group and it has correct configuration
            if group_exists && !wrong_start_id {
                break;
            }
        }
    }
    
    // For 'gnode-client' group with incorrect start ID, use SETID (atomic, no race condition)
    // P2BF001 FIX: Replace DESTROY+CREATE with XGROUP SETID for atomicity
    if group_exists && wrong_start_id && group_name == "gnode-client" {
        info!("Consumer group 'gnode-client' exists but doesn't read from beginning, resetting ID to '0' atomically");

        // Use XGROUP SETID to atomically reset the last-delivered-ID
        // This avoids the race condition of DESTROY followed by CREATE
        let setid_result: RedisResult<()> = redis::cmd("XGROUP")
            .arg("SETID")
            .arg(stream_key)
            .arg(group_name)
            .arg("0") // Reset to beginning
            .query(conn);

        match setid_result {
            Ok(_) => {
                info!("Successfully reset consumer group '{}' to read from beginning (ID '0')", group_name);
                return Ok(true); // Group ID was successfully reset
            },
            Err(e) => {
                warn!("Failed to reset consumer group '{}' ID: {}", group_name, e);
                // Fall through to try other recovery approaches
            }
        }
    }
    
    // If group doesn't exist, create it
    if !group_exists {
        // For new groups, allow choosing to start from beginning or current position
        let start_id = if start_from_beginning { "0" } else { "$" };
        
        info!("Creating consumer group '{}' with start ID '{}'", group_name, start_id);
        
        // Create the consumer group with the specified start ID
        let result = create_consumer_group(
            conn,
            stream_key,
            group_name,
            start_id, 
            true, // Create stream if needed
            site_id,
            debug_mode
        );
        
        match result {
            Ok(_) => {
                info!("Created consumer group '{}' for stream '{}' starting from ID '{}'", 
                      group_name, stream_key, start_id);
                
                // Verify the group was created correctly
                if start_from_beginning {
                    // Test reading with the group
                    let test_result: RedisResult<Vec<Value>> = redis::cmd("XREADGROUP")
                        .arg("GROUP")
                        .arg(group_name)
                        .arg("verifier")
                        .arg("COUNT")
                        .arg(1)
                        .arg("STREAMS")
                        .arg(stream_key)
                        .arg("0") // Try reading from beginning
                        .query(conn);

                    // P2BF002 FIX: Clean up verifier consumer to prevent orphan accumulation
                    let _ = redis::cmd("XGROUP")
                        .arg("DELCONSUMER")
                        .arg(stream_key)
                        .arg(group_name)
                        .arg("verifier")
                        .query::<i64>(conn);

                    match test_result {
                        Ok(_) => {
                            info!("Verified new consumer group '{}' can read from beginning", group_name);
                        },
                        Err(e) => {
                            warn!("New consumer group '{}' verification failed: {}", group_name, e);
                        }
                    }
                }

                return Ok(true); // Group was created
            },
            Err(e) => {
                // Check if error is BUSYGROUP (which is not really an error)
                if e.to_string().contains("BUSYGROUP") {
                    if debug_mode {
                        debug!("Consumer group already exists (BUSYGROUP)");
                    }
                    return Ok(false); // Group already existed
                }
                
                // Actual error
                error!("Failed to create consumer group '{}': {}", group_name, e);
                return Err(consumer_group_error(
                    format!("Failed to create consumer group '{}': {}", group_name, e)
                ));
            }
        }
    }
    
    // For existing client groups, verify they can read from the beginning
    if group_exists && group_name == "gnode-client" && start_from_beginning {
        info!("Verifying existing client consumer group can read from beginning");
        
        // Try reading from the beginning
        let test_result: RedisResult<Vec<Value>> = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg(group_name)
            .arg("verifier")
            .arg("COUNT")
            .arg(1)
            .arg("STREAMS")
            .arg(stream_key)
            .arg("0") // Try reading from beginning
            .query(conn);

        // P2BF002 FIX: Clean up verifier consumer to prevent orphan accumulation
        let _ = redis::cmd("XGROUP")
            .arg("DELCONSUMER")
            .arg(stream_key)
            .arg(group_name)
            .arg("verifier")
            .query::<i64>(conn);

        match test_result {
            Ok(_) => {
                info!("Verified consumer group '{}' can read from beginning", group_name);
            },
            Err(e) => {
                warn!("Consumer group '{}' cannot read from beginning: {}", group_name, e);
                
                // Last resort: try to delete all pending messages and reset the group
                info!("Attempting to reset consumer group's last delivered ID to 0");
                
                // Create a special test consumer to help reset
                let reset_consumer = format!("reset-{}", std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() % 10000);
                
                // Try to read all pending messages and acknowledge them
                let _ = redis::cmd("XREADGROUP")
                    .arg("GROUP")
                    .arg(group_name)
                    .arg(&reset_consumer)
                    .arg("COUNT")
                    .arg(1000) // Read a large batch
                    .arg("STREAMS")
                    .arg(stream_key)
                    .arg(">") // Read pending messages
                    .query::<Vec<Value>>(conn);
                
                // Then try to delete the consumer to reset
                let _ = redis::cmd("XGROUP")
                    .arg("DELCONSUMER")
                    .arg(stream_key)
                    .arg(group_name)
                    .arg(&reset_consumer)
                    .query::<i64>(conn);
            }
        }
    }
    
    Ok(false) // Group already existed and was valid
}


/// Create a unified stream worker thread
///
/// This function creates a worker thread that processes commands
/// from the unified stream using the provided configuration.
/// It ensures proper initialization of the unified stream and
/// consumer groups before starting the worker loop.
///
/// # Arguments
///
/// * `node_id` - Node identifier
/// * `site_id` - Site identifier for namespacing
/// * `stream_prefix` - Stream prefix
/// * `config` - Unified stream configuration
/// * `debug_mode` - Whether debug mode is enabled
///
/// # Returns
///
/// * `IntegrationResult<std::thread::JoinHandle<()>>` - Thread handle or error
pub fn create_unified_stream_worker(
    node_id: &str,
    site_id: &str,
    stream_prefix: &str,
    config: &GNodeSettings,
    debug_mode: bool
) -> IntegrationResult<std::thread::JoinHandle<()>> {
    // Create owned copies for the thread
    let node_id_owned = node_id.to_string();
    let site_id_owned = site_id.to_string();
    let stream_prefix_owned = stream_prefix.to_string();
    let config_clone = config.clone();
    
    // Initialize the stream before creating the worker thread
    match connection_manager::with_connection(|conn| {
        crate::integration::processor::unified_stream_processor::initialize_unified_stream(
            conn,
            node_id,
            site_id,
            stream_prefix,
            debug_mode
        )
    }) {
        Ok(_) => {
            trace!("Pre-initialized unified stream processor for node {}", node_id);
        },
        Err(e) => {
            // Non-fatal, we'll retry in the worker thread
            if !e.to_string().contains("Circuit breaker active") {
                warn!("Failed to pre-initialize unified stream processor for node {}: {}", node_id, e);
            }
        }
    }
    
    // Create thread
    let handle = std::thread::spawn(move || {
        // Initialize state
        let mut state = ConsumerGroupState::new(
            config_clone.initial_batch_size,
            config_clone.base_backoff_ms
        );
        
        // Get command registry
        let registry = crate::integration::command_handler::get_command_registry();
        
        // Get unified stream key
        let stream_key = crate::integration::processor::unified_stream_processor::get_unified_stream(
            &site_id_owned,
            &stream_prefix_owned,
            &node_id_owned
        );
        
        // Get consumer name (include node_id to make it unique across nodes)
        let consumer_name = format!("worker-{}-{}", node_id_owned, thread_id::get());
        
        trace!("Unified stream worker started for node {} (consumer: {})", 
              node_id_owned, consumer_name);
        
        // Track initialization status locally to avoid constant re-initialization
        let mut initialization_completed = false;

        // Processing loop (checks shutdown flag)
        while !crate::daemon::is_shutdown_requested() {
            // Get a connection from the pool
            match connection_manager::get_connection() {
                Ok(mut conn) => {
                    // Only initialize if not completed already
                    if !initialization_completed {
                        let init_result = crate::integration::processor::unified_stream_processor::initialize_unified_stream(
                            &mut conn,
                            &node_id_owned,
                            &site_id_owned,
                            &stream_prefix_owned,
                            debug_mode
                        );
                        
                        match init_result {
                            Ok(_) => {
                                initialization_completed = true;
                            },
                            Err(e) => {
                                if !e.to_string().contains("Circuit breaker active") {
                                    error!("Failed to initialize unified stream: {}", e);
                                }
                                std::thread::sleep(Duration::from_secs(1));
                                continue;
                            }
                        }
                    }
                    
                    // Get topology reference
                    let topology = GNodeDaemon::get_topology_ref();

                    // Get load manager reference for health updates
                    // P2BF003 FIX: Always warn on fallback (invariant violation)
                    let load_manager = match crate::daemon::GNodeDaemon::get_load_metrics_manager_ref() {
                        Some(lm) => lm,
                        None => {
                            // This should not happen - daemon should initialize LoadMetricsManager before workers
                            error!("LoadMetricsManager not initialized - worker started before daemon init complete");
                            warn!("Health metrics will be tracked in isolated instance (not shared with daemon)");
                            std::sync::Arc::new(crate::integration::load_metrics::LoadMetricsManager::new(30))
                        }
                    };

                    // Get health stream key
                    let health_stream = crate::integration::processor::unified_stream_processor::get_health_stream(
                        &site_id_owned,
                        &stream_prefix_owned,
                        &node_id_owned
                    );

                    // Read from both streams simultaneously
                    match crate::integration::processor::read_multi_stream(
                        &mut conn,
                        &stream_key,      // unified stream
                        &health_stream,   // health stream
                        "gnode-daemon",
                        &consumer_name,
                        state.batch_size,
                        1000, // block_ms
                        debug_mode
                    ) {
                        Ok((commands, health_messages, unified_message_ids, _health_message_ids)) => {
                            if debug_mode && (!commands.is_empty() || !health_messages.is_empty()) {
                                debug!("Multi-stream read: {} commands, {} health updates",
                                    commands.len(), health_messages.len());
                            }

                            let mut total_processed = 0;

                            // Log unified stream message IDs (for debugging XACK)
                            if !unified_message_ids.is_empty() {
                                info!("Read {} unified stream message IDs (commands + responses): {:?}",
                                    unified_message_ids.len(), unified_message_ids);
                            }

                            // Process command messages
                            if !commands.is_empty() {

                                match crate::integration::command_processor::process_command_batch(
                                    &mut conn,
                                    &topology,
                                    &stream_key,
                                    &commands,
                                    registry,
                                    &site_id_owned,
                                    "daemon",
                                    debug_mode,
                                    crate::daemon::LogLevel::Info
                                ) {
                                    Ok(processed) => {
                                        if processed > 0 {
                                            trace!("Processed {} commands successfully", processed);
                                            total_processed += processed;
                                        }

                                        state.reset_after_success();
                                    },
                                    Err(e) => {
                                        warn!("Error processing command batch: {}", e);
                                        state.register_error();
                                    }
                                }
                            }

                            // Process health messages
                            if !health_messages.is_empty() {
                                match crate::integration::processor::process_health_updates(
                                    &load_manager,
                                    health_messages,
                                    &mut conn,
                                    &health_stream,
                                    debug_mode
                                ) {
                                    Ok(processed) => {
                                        if debug_mode && processed > 0 {
                                            debug!("Processed {} health updates successfully", processed);
                                        }
                                        total_processed += processed;
                                    },
                                    Err(e) => {
                                        warn!("Error processing health updates: {}", e);
                                    }
                                }
                            }

                            // Acknowledge unified stream messages (commands + responses) to remove from PEL
                            // Do this AFTER processing, regardless of whether we had commands or not
                            if !unified_message_ids.is_empty() {
                                info!("Attempting to acknowledge {} unified stream message IDs", unified_message_ids.len());
                                match acknowledge_messages(
                                    &mut conn,
                                    &stream_key,
                                    "gnode-daemon",
                                    &unified_message_ids,
                                    &site_id_owned,
                                    debug_mode
                                ) {
                                    Ok(acked) => {
                                        info!("Successfully acknowledged {} messages", acked);
                                    },
                                    Err(e) => {
                                        warn!("Failed to acknowledge messages: {}", e);
                                        // Non-fatal - continue processing
                                    }
                                }
                            }

                            // Adjust batch size based on total processed
                            state.adjust_batch_size(total_processed, config_clone.min_batch_size, config_clone.max_batch_size);

                            // Periodically trim the unified stream
                            if state.last_empty_time.elapsed().as_secs() >= config_clone.trim_interval_secs {
                                let _ = crate::integration::trim_unified_stream(
                                    &mut conn,
                                    &stream_key,
                                    config_clone.max_stream_length,
                                    config_clone.approximate_trim,
                                    &site_id_owned,
                                    debug_mode
                                );

                                // Reset last empty time
                                state.last_empty_time = Instant::now();
                            }
                        },
                        Err(e) => {
                            warn!("Error reading from multiple streams: {}", e);
                            state.register_error();
                            // Apply backoff on error
                            std::thread::sleep(Duration::from_millis(
                                state.current_backoff_ms.max(config_clone.base_backoff_ms)
                            ));
                        }
                    }
                },
                Err(e) => {
                    error!("Failed to get connection from pool: {}", e);
                    // Back off on connection error
                    std::thread::sleep(Duration::from_secs(1));
                }
            }
        }
        // Log shutdown message when loop exits
        info!("Unified stream worker shutting down (node: {})", node_id_owned);
    });

    Ok(handle)
}

/// DTAP wrapper: Create a unified stream worker for a DTAP environment
/// Stream pattern: {environment}:gnode:unified:{node_id}
pub fn create_dtap_stream_worker(
    environment: &str,
    node_id: &str,
    stream_prefix: &str,
    config: &GNodeSettings,
    debug_mode: bool
) -> IntegrationResult<std::thread::JoinHandle<()>> {
    // DTAP mode: Use environment as the site_id
    info!("Creating DTAP stream worker for environment: {}, node: {}", environment, node_id);
    create_unified_stream_worker(node_id, environment, stream_prefix, config, debug_mode)
}

/// Create an environment-based stream worker for multi-node architecture
///
/// Stream pattern: {site_id}:{stream_prefix}:{environment}:unified
/// Consumer group: gnode-workers (shared across all nodes in environment)
/// Consumer name: gnode-{node_id} (unique per node)
///
/// Message routing via node_type:
/// - "general": Processes messages without _gh field or _gh != "inference"
/// - "inference": Only processes messages with _gh:"inference"
/// - "all": Processes all messages regardless of _gh field
///
/// This enables multiple nodes to share the same stream via consumer groups,
/// with intelligent routing based on message type.
///
/// Dynamic Stream Discovery (Phase 2):
/// If `shared_discovery` is provided, the worker will dynamically subscribe to
/// newly registered site streams. The worker syncs with the shared StreamDiscoveryManager
/// every 60 seconds to pick up new sites and their streams.
pub fn create_environment_stream_worker(
    site_id: &str,
    environment: &str,
    node_id: &str,
    node_type: &str,
    stream_prefix: &str,
    config: &GNodeSettings,
    debug_mode: bool
) -> IntegrationResult<std::thread::JoinHandle<()>> {
    // Call the dynamic version without shared discovery for backward compatibility
    create_environment_stream_worker_dynamic(
        site_id,
        environment,
        node_id,
        node_type,
        stream_prefix,
        config,
        debug_mode,
        None
    )
}

/// Create an environment-based stream worker with dynamic stream discovery
///
/// This is the full-featured version that accepts a shared StreamDiscoveryManager
/// for dynamic subscription to newly registered site streams.
#[allow(clippy::too_many_arguments)]
pub fn create_environment_stream_worker_dynamic(
    site_id: &str,
    environment: &str,
    node_id: &str,
    node_type: &str,
    stream_prefix: &str,
    config: &GNodeSettings,
    debug_mode: bool,
    shared_discovery: Option<std::sync::Arc<std::sync::RwLock<crate::integration::stream_discovery::StreamDiscoveryManager>>>
) -> IntegrationResult<std::thread::JoinHandle<()>> {
    // Create owned copies for the thread
    let site_id_owned = site_id.to_string();
    let environment_owned = environment.to_string();
    let node_id_owned = node_id.to_string();
    let node_type_owned = node_type.to_string();
    let stream_prefix_owned = stream_prefix.to_string();
    let config_clone = config.clone();

    let dynamic_mode = shared_discovery.is_some();
    info!("Creating environment-based stream worker:");
    info!("  Site: {}, Environment: {}, Node: {}, Type: {}", site_id, environment, node_id, node_type);
    info!("  Dynamic discovery: {}", if dynamic_mode { "ENABLED" } else { "disabled (single-site mode)" });

    // Pre-initialize the streams before creating the worker thread
    match connection_manager::with_connection(|conn| {
        crate::integration::processor::unified_stream_processor::initialize_environment_streams(
            conn,
            site_id,
            environment,
            node_id,
            stream_prefix,
            debug_mode
        )
    }) {
        Ok(_) => {
            trace!("Pre-initialized environment streams");
        },
        Err(e) => {
            if !e.to_string().contains("Circuit breaker active") {
                warn!("Failed to pre-initialize environment streams: {}", e);
            }
        }
    }

    // Create thread
    let handle = std::thread::spawn(move || {
        // Initialize state
        let mut state = ConsumerGroupState::new(
            config_clone.initial_batch_size,
            config_clone.base_backoff_ms
        );

        // Get command registry
        let registry = crate::integration::command_handler::get_command_registry();

        // Get initial environment-based stream keys (fallback for single-site mode)
        let fallback_stream_key = crate::integration::processor::unified_stream_processor::get_environment_unified_stream(
            &site_id_owned,
            &stream_prefix_owned,
            &environment_owned
        );

        let fallback_health_stream = crate::integration::processor::unified_stream_processor::get_environment_health_stream(
            &site_id_owned,
            &stream_prefix_owned,
            &environment_owned
        );

        // Track active streams (for dynamic mode)
        // Try to use discovered streams immediately at startup
        let (mut active_unified_streams, mut active_health_streams) = if let Some(ref discovery) = shared_discovery {
            match discovery.read() {
                Ok(disc) => {
                    let unified: Vec<String> = disc.get_unified_streams()
                        .into_iter()
                        .map(|s| s.key)
                        .collect();
                    let health: Vec<String> = disc.get_health_streams()
                        .into_iter()
                        .map(|s| s.key)
                        .collect();

                    if !unified.is_empty() {
                        info!("🚀 Using {} discovered unified streams at startup", unified.len());
                        (unified, health)
                    } else {
                        info!("⚠️ No discovered streams yet, using fallback");
                        (vec![fallback_stream_key.clone()], vec![fallback_health_stream.clone()])
                    }
                },
                Err(_) => {
                    warn!("Failed to read discovery at startup, using fallback streams");
                    (vec![fallback_stream_key.clone()], vec![fallback_health_stream.clone()])
                }
            }
        } else {
            // Static mode - use fallback
            (vec![fallback_stream_key.clone()], vec![fallback_health_stream.clone()])
        };
        let mut last_stream_sync = Instant::now();
        let stream_sync_interval_secs = 60; // Sync with discovery every 60 seconds

        // Get consumer name (unique per node in the shared consumer group)
        let consumer_name = crate::integration::processor::unified_stream_processor::get_environment_consumer_name(&node_id_owned);

        // Use the shared consumer group
        let consumer_group = crate::integration::processor::unified_stream_processor::ENVIRONMENT_CONSUMER_GROUP;

        info!("Environment stream worker started:");
        if dynamic_mode {
            info!("  Mode: DYNAMIC (subscribing to ALL discovered site streams)");
        } else {
            info!("  Mode: STATIC (single site: {})", site_id_owned);
        }
        info!("  Initial unified streams ({}):", active_unified_streams.len());
        for stream in &active_unified_streams {
            info!("    → {}", stream);
        }
        info!("  Consumer group: {}, Consumer: {}, Type: {}", consumer_group, consumer_name, node_type_owned);

        // Ensure consumer groups exist on all initial streams
        if let Ok(mut conn) = connection_manager::get_connection() {
            for stream_key in &active_unified_streams {
                if let Err(e) = ensure_consumer_group(
                    &mut conn,
                    stream_key,
                    consumer_group,
                    false, // Start from new messages only
                    &site_id_owned,
                    debug_mode
                ) {
                    // Only warn if it's not a "group already exists" error
                    let err_str = format!("{:?}", e);
                    if !err_str.contains("BUSYGROUP") {
                        warn!("Failed to create consumer group on {}: {:?}", stream_key, e);
                    }
                }
            }
            for stream_key in &active_health_streams {
                if let Err(e) = ensure_consumer_group(
                    &mut conn,
                    stream_key,
                    consumer_group,
                    false,
                    &site_id_owned,
                    debug_mode
                ) {
                    let err_str = format!("{:?}", e);
                    if !err_str.contains("BUSYGROUP") {
                        warn!("Failed to create consumer group on {}: {:?}", stream_key, e);
                    }
                }
            }
            info!("✅ Consumer groups initialized on {} unified, {} health streams",
                active_unified_streams.len(), active_health_streams.len());
        }

        // Track initialization status
        let mut initialization_completed = false;

        // Orphan recovery cadence. Every node reclaims; see the call site below.
        let mut last_xclaim_check = Instant::now();
        let xclaim_interval_ms = RECLAIM_INTERVAL_MS;
        let mut last_reap_check = Instant::now();

        // Track last staleness check for service topology entities
        let mut last_staleness_check = Instant::now();
        let staleness_check_interval_ms: u64 = 30_000; // Every 30 seconds

        // Relay tracker: thread-local tracker for pending relay commands awaiting responses
        let mut relay_tracker = crate::integration::relay::RelayTracker::new(30_000);

        // Relay telemetry: thread-local metrics collector, flushed to ValKey every 30s
        let mut relay_telemetry = crate::integration::relay::RelayTelemetry::new();

        // Processing loop (checks shutdown flag)
        while !crate::daemon::is_shutdown_requested() {
            // Dynamic stream sync: check for newly discovered streams OR immediate sync signal
            if let Some(ref discovery) = shared_discovery {
                // Check if immediate sync was signaled (e.g., from environment_changed broadcast)
                let immediate_sync_needed = discovery.read()
                    .map(|d| d.check_and_clear_sync_signal())
                    .unwrap_or(false);

                let time_for_sync = last_stream_sync.elapsed().as_secs() >= stream_sync_interval_secs;

                if immediate_sync_needed || time_for_sync {
                    if immediate_sync_needed {
                        info!("🔔 Immediate stream sync triggered (environment change detected)");
                    }
                    last_stream_sync = Instant::now();

                    // Read from shared discovery manager
                    match discovery.read() {
                        Ok(disc) => {
                            // Get all unified streams from discovery
                            let discovered_unified: Vec<String> = disc.get_unified_streams()
                                .into_iter()
                                .map(|s| s.key)
                                .collect();

                            let discovered_health: Vec<String> = disc.get_health_streams()
                                .into_iter()
                                .map(|s| s.key)
                                .collect();

                            // Check for new streams
                            let new_unified: Vec<String> = discovered_unified.iter()
                                .filter(|s| !active_unified_streams.contains(s))
                                .cloned()
                                .collect();

                            let new_health: Vec<String> = discovered_health.iter()
                                .filter(|s| !active_health_streams.contains(s))
                                .cloned()
                                .collect();

                            if !new_unified.is_empty() || !new_health.is_empty() {
                                info!("🔍 Dynamic discovery: found {} new unified, {} new health streams",
                                    new_unified.len(), new_health.len());

                                // Ensure consumer groups on new streams
                                if let Ok(mut conn) = connection_manager::get_connection() {
                                    for stream_key in &new_unified {
                                        info!("  + Subscribing to new unified stream: {}", stream_key);
                                        if let Err(e) = ensure_consumer_group(
                                            &mut conn,
                                            stream_key,
                                            consumer_group,
                                            false, // Start from new messages only
                                            &site_id_owned,
                                            debug_mode
                                        ) {
                                            warn!("Failed to create consumer group on {}: {:?}", stream_key, e);
                                        }
                                    }

                                    for stream_key in &new_health {
                                        info!("  + Subscribing to new health stream: {}", stream_key);
                                        if let Err(e) = ensure_consumer_group(
                                            &mut conn,
                                            stream_key,
                                            consumer_group,
                                            false,
                                            &site_id_owned,
                                            debug_mode
                                        ) {
                                            warn!("Failed to create consumer group on {}: {:?}", stream_key, e);
                                        }
                                    }
                                }

                                // Update active streams
                                active_unified_streams = discovered_unified;
                                active_health_streams = discovered_health;

                                info!("✅ Now subscribed to {} unified, {} health streams",
                                    active_unified_streams.len(), active_health_streams.len());
                            }
                        },
                        Err(e) => {
                            warn!("Failed to read from shared discovery: {}", e);
                        }
                    }
                }
            }

            // Get a connection from the pool
            match connection_manager::get_connection() {
                Ok(mut conn) => {
                    // Only initialize if not completed already
                    if !initialization_completed {
                        let init_result = crate::integration::processor::unified_stream_processor::initialize_environment_streams(
                            &mut conn,
                            &site_id_owned,
                            &environment_owned,
                            &node_id_owned,
                            &stream_prefix_owned,
                            debug_mode
                        );

                        match init_result {
                            Ok(_) => {
                                initialization_completed = true;
                            },
                            Err(e) => {
                                if !e.to_string().contains("Circuit breaker active") {
                                    error!("Failed to initialize environment streams: {}", e);
                                }
                                std::thread::sleep(Duration::from_secs(1));
                                continue;
                            }
                        }
                    }

                    // Get topology reference
                    let topology = GNodeDaemon::get_topology_ref();

                    // Get load manager reference for health updates
                    // P2BF003 FIX: Always warn on fallback (invariant violation)
                    let load_manager = match crate::daemon::GNodeDaemon::get_load_metrics_manager_ref() {
                        Some(lm) => lm,
                        None => {
                            // This should not happen - daemon should initialize LoadMetricsManager before workers
                            error!("LoadMetricsManager not initialized - worker started before daemon init complete");
                            warn!("Health metrics will be tracked in isolated instance (not shared with daemon)");
                            std::sync::Arc::new(crate::integration::load_metrics::LoadMetricsManager::new(30))
                        }
                    };

                    // Orphan recovery runs on EVERY node. An entry whose owner
                    // stopped is not redelivered on its own, so if no live node
                    // reclaims it the request is silently swallowed and the caller
                    // waits forever. This was previously gated to specialised node
                    // types, which meant a constellation of default "general" nodes
                    // performed no recovery at all.
                    //
                    // Each node only reclaims what its exposure covers, so widening
                    // this does not let a node take work it may not process.
                    if last_xclaim_check.elapsed().as_millis() as u64 >= xclaim_interval_ms {
                        last_xclaim_check = Instant::now();
                        for stream_key in &active_unified_streams {
                            if let Ok(claimed) = reclaim_exposed_pending_messages(
                                &mut conn,
                                stream_key,
                                consumer_group,
                                &consumer_name,
                                &node_type_owned,
                                RECLAIM_BATCH,
                                RECLAIM_MIN_IDLE_MS,
                                debug_mode
                            ) {
                                if claimed > 0 {
                                    info!("Node '{}' reclaimed {} orphaned entries from {}", node_type_owned, claimed, stream_key);
                                }
                            }
                        }
                    }

                    // Consumers accumulate with every reconnection of an
                    // intermittent node; nothing else removes them. Swept far less
                    // often than reclaim because it is bookkeeping, not liveness.
                    if last_reap_check.elapsed().as_millis() as u64 >= REAP_INTERVAL_MS {
                        last_reap_check = Instant::now();
                        for stream_key in &active_unified_streams {
                            let _ = reap_stale_consumers(
                                &mut conn,
                                stream_key,
                                consumer_group,
                                &consumer_name,
                                REAP_IDLE_MS,
                                debug_mode
                            );
                        }
                    }

                    // Multi-site stream processing: iterate over ALL discovered stream pairs
                    // Each site has its own unified+health stream pair
                    let stream_count = active_unified_streams.len().min(active_health_streams.len());
                    let mut total_processed_all_streams = 0;
                    let mut had_any_error = false;
                    let mut stale_stream_indices: Vec<usize> = Vec::new();

                    // Calculate block time per stream: short blocks for low latency
                    // Prefer fast response over CPU efficiency - brief blocks only
                    let block_ms_per_stream = if stream_count > 0 {
                        (50 / stream_count as u64).max(10) // 10-50ms per stream (fast)
                    } else {
                        50
                    };

                    for stream_idx in 0..stream_count {
                        let current_unified_stream = &active_unified_streams[stream_idx];
                        let current_health_stream = &active_health_streams[stream_idx];

                        // Read from this site's stream pair
                        match crate::integration::processor::read_multi_stream(
                            &mut conn,
                            current_unified_stream,
                            current_health_stream,
                            consumer_group,   // gnode-workers (shared)
                            &consumer_name,   // gnode-{node_id} (unique)
                            state.batch_size / stream_count.max(1), // Distribute batch across streams
                            block_ms_per_stream,
                            debug_mode
                        ) {
                            Ok((commands, health_messages, unified_message_ids, _health_message_ids)) => {
                                if debug_mode && (!commands.is_empty() || !health_messages.is_empty()) {
                                    debug!("Stream[{}] {}: {} commands, {} health updates",
                                        stream_idx, current_unified_stream, commands.len(), health_messages.len());
                                }

                                let mut stream_processed = 0;

                                // Filter commands based on node_type and _gh field
                                let (commands_to_process, commands_to_skip, mut ids_to_ack, ids_to_skip) =
                                    filter_commands_by_node_type(&commands, &unified_message_ids, &node_type_owned, debug_mode);

                                // Compute non-command message IDs (responses, PHP key-based notifications, init messages)
                                // These are in unified_message_ids but NOT in commands (only t=c/t=bc become commands).
                                // They must always be ACKed — no node processes them, so leaving them in PEL causes unbounded growth.
                                let command_msg_ids: std::collections::HashSet<&String> = commands.iter()
                                    .map(|(id, _)| id).collect();
                                let non_command_ids: Vec<String> = unified_message_ids.iter()
                                    .filter(|id| !command_msg_ids.contains(id))
                                    .cloned().collect();
                                if !non_command_ids.is_empty() {
                                    if debug_mode {
                                        debug!("Found {} non-command messages to ACK (responses/notifications)", non_command_ids.len());
                                    }
                                    ids_to_ack.extend(non_command_ids);
                                }

                                if debug_mode && !commands_to_skip.is_empty() {
                                    debug!("Node type '{}' skipping {} messages (not matching routing)", node_type_owned, commands_to_skip.len());
                                }

                                // Separate relay commands from local commands
                                let (relay_commands, local_commands): (Vec<_>, Vec<_>) = commands_to_process
                                    .into_iter()
                                    .partition(|(_, cmd)| cmd.relay_target.is_some());

                                // Process relay commands: resolve target, forward via XADD
                                if !relay_commands.is_empty() {
                                    if debug_mode {
                                        info!("Processing {} relay command(s) from {}", relay_commands.len(), current_unified_stream);
                                    }

                                    // Extract site_id from current stream for relay resolution
                                    let stream_site_id = current_unified_stream.split(":gnode:").next().unwrap_or(&site_id_owned);

                                    for (_msg_id, cmd) in &relay_commands {
                                        let relay_target = match cmd.relay_target.as_ref() {
                                            Some(t) => t,
                                            None => continue, // skip: missing relay target
                                        };

                                        let decision = crate::integration::relay::resolve_relay_target(
                                            &mut conn,
                                            relay_target,
                                            stream_site_id,
                                            current_unified_stream,
                                            shared_discovery.as_ref(),
                                            debug_mode,
                                        );

                                        match decision {
                                            crate::integration::relay::RelayDecision::Forward { ref target_site_id, ref target_stream_key, ref target_entity_id } => {
                                                // --- Phase 5B: ACL policy check ---
                                                let ns = crate::daemon::GNodeDaemon::get_topology_namespace();
                                                let policy = crate::integration::relay::check_relay_policy(
                                                    &mut conn, ns, stream_site_id, target_site_id,
                                                    &cmd.command, debug_mode,
                                                );
                                                if !policy.is_allowed() {
                                                    if let crate::integration::relay::PolicyDecision::Deny(ref reason) = policy {
                                                        warn!("Relay DENIED: {} -> {} cmd='{}': {}", stream_site_id, target_site_id, cmd.command, reason);
                                                        let err_response = crate::daemon::Response {
                                                            id: cmd.id.clone(),
                                                            status: "error".to_string(),
                                                            result: None,
                                                            error: Some(format!("Relay denied by policy: {}", reason)),
                                                            timestamp: crate::integration::processor::stream_utils::current_timestamp(),
                                                            batch_id: None,
                                                            sequence: None,
                                                        };
                                                        let _ = crate::integration::send_response_with_routing(
                                                            &mut conn, &err_response, current_unified_stream,
                                                            "gNode", "daemon", &cmd.source_site, &cmd.source_node,
                                                            &site_id_owned, debug_mode
                                                        );
                                                        continue; // Skip this relay, process next command
                                                    }
                                                }

                                                // --- Phase 5C: Start telemetry ---
                                                let mut relay_metrics = crate::integration::relay::RelayTelemetry::record_start(
                                                    stream_site_id, target_site_id, &cmd.command,
                                                );

                                                // --- Phase 5E: Start distributed trace span ---
                                                let trace_span_id = start_relay_trace_span(
                                                    &mut conn, &cmd.id, &cmd.command,
                                                    stream_site_id, target_site_id, debug_mode,
                                                );

                                                // Build forwarded command fields
                                                let mut forwarded = cmd.clone();
                                                forwarded.dest_site = target_site_id.clone();
                                                forwarded.dest_node = target_entity_id.clone();
                                                forwarded.relay_target = None; // Clear _rt to prevent re-relay at target

                                                // Set reply-to if not already set
                                                if forwarded.relay_reply_to.is_none() {
                                                    forwarded.relay_reply_to = Some(current_unified_stream.to_string());
                                                }

                                                // --- Phase 5A: Format translation (best-effort) ---
                                                // Translation is opt-in: only runs if FormatProcessor Lua funcs are loaded
                                                // and both source/target have declared format preferences.
                                                let topology_key = crate::GeometricTopology::get_services_topology_key(stream_site_id);
                                                let params_str = forwarded.parameters_as_json_string();
                                                let translation = crate::integration::relay::translate_for_relay(
                                                    &mut conn, &params_str, stream_site_id,
                                                    target_entity_id, &topology_key, debug_mode,
                                                );
                                                match &translation {
                                                    crate::integration::relay::TranslationResult::Translated { params_json, source_format, target_format } => {
                                                        forwarded.set_parameters_from_json(params_json);
                                                        info!("Relay format translation applied: {} -> {} for '{}'", source_format, target_format, cmd.command);
                                                    }
                                                    crate::integration::relay::TranslationResult::Failed(reason) => {
                                                        warn!("Relay format translation failed, forwarding original: {}", reason);
                                                    }
                                                    crate::integration::relay::TranslationResult::NoOp => {}
                                                }
                                                let relay_translated = matches!(&translation, crate::integration::relay::TranslationResult::Translated { .. });

                                                // XADD to target stream
                                                let fields = forwarded.to_resp3_fields();
                                                let field_pairs: Vec<(String, String)> = fields.into_iter().collect();

                                                let relay_success = match redis::cmd("XADD")
                                                    .arg(target_stream_key)
                                                    .arg("*")
                                                    .arg(&field_pairs)
                                                    .query::<String>(&mut conn) {
                                                    Ok(_msg_id) => {
                                                        info!("Relayed '{}' -> {} on {}", cmd.command, target_entity_id, target_stream_key);

                                                        // Track for response forwarding
                                                        relay_tracker.track(crate::integration::relay::PendingRelay {
                                                            command_id: cmd.id.clone(),
                                                            source_stream: current_unified_stream.to_string(),
                                                            source_site: cmd.source_site.clone(),
                                                            source_node: cmd.source_node.clone(),
                                                            target_stream: target_stream_key.clone(),
                                                            relayed_at: Instant::now(),
                                                        });

                                                        stream_processed += 1;
                                                        true
                                                    },
                                                    Err(e) => {
                                                        warn!("Relay XADD failed for '{}' -> {}: {}", cmd.command, target_stream_key, e);
                                                        false
                                                    }
                                                };

                                                // --- Phase 5C: Complete telemetry ---
                                                relay_metrics.translated = relay_translated;
                                                relay_telemetry.record_complete(relay_metrics, relay_success);

                                                // --- Phase 5E: Finish trace span ---
                                                if let Some(ref span_id) = trace_span_id {
                                                    finish_relay_trace_span(
                                                        &mut conn, span_id, relay_success, debug_mode,
                                                    );
                                                }

                                                // Send relay-accepted acknowledgment to source
                                                let ack_response = crate::daemon::Response {
                                                    id: cmd.id.clone(),
                                                    status: "ok".to_string(),
                                                    result: Some(serde_json::json!({
                                                        "relayed": true,
                                                        "target_site": target_site_id,
                                                        "target_entity": target_entity_id,
                                                        "target_stream": target_stream_key
                                                    })),
                                                    error: None,
                                                    timestamp: crate::integration::processor::stream_utils::current_timestamp(),
                                                    batch_id: None,
                                                    sequence: None,
                                                };
                                                let _ = crate::integration::send_response_with_routing(
                                                    &mut conn, &ack_response, current_unified_stream,
                                                    "gNode", "daemon", &cmd.source_site, &cmd.source_node,
                                                    &site_id_owned, debug_mode
                                                );
                                            },
                                            crate::integration::relay::RelayDecision::Local => {
                                                // Target is on same site — will be processed locally below
                                                // (handled by falling through to local_commands processing)
                                                if debug_mode {
                                                    debug!("Relay target '{}' resolved to local site, processing locally", relay_target);
                                                }
                                            },
                                            crate::integration::relay::RelayDecision::NotFound(ref reason) => {
                                                warn!("Relay target not found: {} ({})", relay_target, reason);
                                                let err_response = crate::daemon::Response {
                                                    id: cmd.id.clone(),
                                                    status: "error".to_string(),
                                                    result: None,
                                                    error: Some(format!("Relay target not found: {}", reason)),
                                                    timestamp: crate::integration::processor::stream_utils::current_timestamp(),
                                                    batch_id: None,
                                                    sequence: None,
                                                };
                                                let _ = crate::integration::send_response_with_routing(
                                                    &mut conn, &err_response, current_unified_stream,
                                                    "gNode", "daemon", &cmd.source_site, &cmd.source_node,
                                                    &site_id_owned, debug_mode
                                                );
                                            },
                                            crate::integration::relay::RelayDecision::Error(ref e) => {
                                                error!("Relay resolution error for '{}': {}", relay_target, e);
                                                let err_response = crate::daemon::Response {
                                                    id: cmd.id.clone(),
                                                    status: "error".to_string(),
                                                    result: None,
                                                    error: Some(format!("Relay error: {}", e)),
                                                    timestamp: crate::integration::processor::stream_utils::current_timestamp(),
                                                    batch_id: None,
                                                    sequence: None,
                                                };
                                                let _ = crate::integration::send_response_with_routing(
                                                    &mut conn, &err_response, current_unified_stream,
                                                    "gNode", "daemon", &cmd.source_site, &cmd.source_node,
                                                    &site_id_owned, debug_mode
                                                );
                                            }
                                        }
                                    }
                                }

                                // Collect any relay-local commands back into local processing
                                // (commands where relay resolved to Local get processed normally)
                                let mut local_commands_final: Vec<(String, crate::integration::processor::OptimizedCommand)> = local_commands;
                                for (msg_id, cmd) in &relay_commands {
                                    if let Some(relay_target) = cmd.relay_target.as_ref() {
                                        let stream_site_id = current_unified_stream.split(":gnode:").next().unwrap_or(&site_id_owned);
                                        let decision = crate::integration::relay::resolve_relay_target(
                                            &mut conn, relay_target, stream_site_id,
                                            current_unified_stream, shared_discovery.as_ref(), false,
                                        );
                                        if matches!(decision, crate::integration::relay::RelayDecision::Local) {
                                            let mut local_cmd = cmd.clone();
                                            local_cmd.relay_target = None;
                                            local_commands_final.push((msg_id.clone(), local_cmd));
                                        }
                                    }
                                }

                                // Process local command messages
                                if !local_commands_final.is_empty() {
                                    match crate::integration::command_processor::process_command_batch(
                                        &mut conn,
                                        &topology,
                                        current_unified_stream,
                                        &local_commands_final,
                                        registry,
                                        &site_id_owned,
                                        "daemon",
                                        debug_mode,
                                        crate::daemon::LogLevel::Info
                                    ) {
                                        Ok(processed) => {
                                            if processed > 0 {
                                                trace!("Processed {} commands from stream {}", processed, current_unified_stream);
                                                stream_processed += processed;
                                            }
                                        },
                                        Err(e) => {
                                            warn!("Error processing command batch from {}: {}", current_unified_stream, e);
                                            had_any_error = true;
                                        }
                                    }
                                }

                                // Process health messages (all nodes process health updates)
                                if !health_messages.is_empty() {
                                    match crate::integration::processor::process_health_updates(
                                        &load_manager,
                                        health_messages,
                                        &mut conn,
                                        current_health_stream,
                                        debug_mode
                                    ) {
                                        Ok(processed) => {
                                            if debug_mode && processed > 0 {
                                                debug!("Processed {} health updates from {}", processed, current_health_stream);
                                            }
                                            stream_processed += processed;
                                        },
                                        Err(e) => {
                                            warn!("Error processing health updates from {}: {}", current_health_stream, e);
                                        }
                                    }
                                }

                                // Acknowledge only processed messages (not skipped ones)
                                // Skipped messages remain in PEL for other nodes to claim
                                if !ids_to_ack.is_empty() {
                                    match acknowledge_messages(
                                        &mut conn,
                                        current_unified_stream,
                                        consumer_group,
                                        &ids_to_ack,
                                        &site_id_owned,
                                        debug_mode
                                    ) {
                                        Ok(acked) => {
                                            if debug_mode {
                                                debug!("Acknowledged {} messages on {}", acked, current_unified_stream);
                                            }
                                        },
                                        Err(e) => {
                                            warn!("Failed to acknowledge messages on {}: {}", current_unified_stream, e);
                                        }
                                    }
                                }

                                // For skipped messages: Don't ACK them - they'll stay in PEL
                                if !ids_to_skip.is_empty() && debug_mode {
                                    debug!("Left {} messages unacknowledged on {} for other nodes", ids_to_skip.len(), current_unified_stream);
                                }

                                total_processed_all_streams += stream_processed;
                            },
                            Err(e) => {
                                let error_str = e.to_string();

                                // Check for stale stream (NOGROUP with "No such key")
                                // This happens when a site was removed but we still have its streams cached
                                if error_str.contains("NOGROUP") && error_str.contains("No such key") {
                                    info!("🗑️  Detected stale stream (site removed): {} - will remove from active list",
                                        current_unified_stream);
                                    stale_stream_indices.push(stream_idx);
                                } else if !error_str.contains("Circuit breaker") {
                                    // Don't log circuit breaker errors repeatedly
                                    warn!("Error reading from stream {}: {}", current_unified_stream, e);
                                }
                                had_any_error = true;
                            }
                        }
                    }

                    // Remove stale streams from active lists (in reverse order to preserve indices)
                    if !stale_stream_indices.is_empty() {
                        info!("🧹 Removing {} stale stream(s) from subscription", stale_stream_indices.len());
                        for idx in stale_stream_indices.into_iter().rev() {
                            if idx < active_unified_streams.len() {
                                let removed = active_unified_streams.remove(idx);
                                info!("   - Removed: {}", removed);
                            }
                            if idx < active_health_streams.len() {
                                active_health_streams.remove(idx);
                            }
                        }
                        // Signal discovery manager to refresh on next cycle
                        if let Some(ref discovery) = shared_discovery {
                            if let Ok(disc) = discovery.read() {
                                disc.signal_immediate_sync();
                            }
                        }
                    }

                    // Update state based on overall results
                    if had_any_error {
                        state.register_error();
                    } else if total_processed_all_streams > 0 {
                        state.reset_after_success();
                    }

                    // Adjust batch size based on total processed across all streams
                    state.adjust_batch_size(total_processed_all_streams, config_clone.min_batch_size, config_clone.max_batch_size);

                    // Periodically trim all active unified streams
                    if state.last_empty_time.elapsed().as_secs() >= config_clone.trim_interval_secs {
                        for stream in &active_unified_streams {
                            let _ = crate::integration::trim_unified_stream(
                                &mut conn,
                                stream,
                                config_clone.max_stream_length,
                                config_clone.approximate_trim,
                                &site_id_owned,
                                debug_mode
                            );
                        }
                        state.last_empty_time = Instant::now();
                    }

                    // Periodic staleness check: scan service topologies for stale/dead entities
                    if last_staleness_check.elapsed().as_millis() as u64 >= staleness_check_interval_ms {
                        last_staleness_check = Instant::now();

                        // Evict stale relay tracker entries (pending relays older than timeout)
                        let evicted = relay_tracker.evict_stale();
                        if evicted > 0 {
                            warn!("Evicted {} stale pending relay(s) (no response received)", evicted);
                        } else if debug_mode && relay_tracker.pending_count() > 0 {
                            debug!("Relay tracker: {} pending relay(s)", relay_tracker.pending_count());
                        }

                        // Flush relay telemetry to ValKey (Phase 5C)
                        if relay_telemetry.has_pending() {
                            let ns = crate::daemon::GNodeDaemon::get_topology_namespace();
                            relay_telemetry.flush(&mut conn, ns, debug_mode);
                        }

                        // Check for expired/idle direct channels
                        {
                            let ns = crate::daemon::GNodeDaemon::get_topology_namespace();
                            let now_s = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs();
                            let _ = crate::integration::direct::check_expiry(
                                &mut conn, ns, now_s, 3600, debug_mode
                            );
                        }

                        // Deduplicate site_ids from active streams to avoid checking the same topology twice
                        let mut checked_sites: std::collections::HashSet<String> = std::collections::HashSet::new();
                        for stream_key in &active_unified_streams {
                            // Extract site_id from "{site_id}:gnode:unified:{env}" pattern
                            if let Some(site_id) = stream_key.split(":gnode:").next() {
                                if !site_id.is_empty() && checked_sites.insert(site_id.to_string()) {
                                    let topology_key = crate::GeometricTopology::get_services_topology_key(site_id);
                                    let now_s = std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .unwrap_or_default()
                                        .as_secs();

                                    let staleness_result: redis::RedisResult<String> = redis::cmd("FCALL")
                                        .arg("GNODE_TOPO_CHECK_STALENESS")
                                        .arg(1)
                                        .arg(&topology_key)
                                        .arg(60)    // staleness_threshold_s
                                        .arg(300)   // deregister_threshold_s
                                        .arg(now_s)
                                        .query(&mut conn);

                                    match staleness_result {
                                        Ok(json_str) => {
                                            if let Ok(result) = serde_json::from_str::<serde_json::Value>(&json_str) {
                                                let stale = result.get("stale").and_then(|v| v.as_i64()).unwrap_or(0);
                                                let deregistered = result.get("deregistered")
                                                    .and_then(|v| v.as_array())
                                                    .map(|a| a.len())
                                                    .unwrap_or(0);
                                                if stale > 0 || deregistered > 0 {
                                                    warn!("Staleness check [{}]: {} stale, {} deregistered",
                                                        site_id, stale, deregistered);
                                                } else if debug_mode {
                                                    let checked = result.get("checked").and_then(|v| v.as_i64()).unwrap_or(0);
                                                    debug!("Staleness check [{}]: {} checked, all healthy", site_id, checked);
                                                }
                                            }
                                        },
                                        Err(e) => {
                                            if debug_mode {
                                                debug!("Staleness check skipped for {}: {}", site_id, e);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // If no streams had data, minimal sleep to prevent tight spinning
                    // Keep it short for fast response when data arrives
                    if total_processed_all_streams == 0 && !had_any_error {
                        std::thread::sleep(Duration::from_millis(5));
                    } else if had_any_error {
                        std::thread::sleep(Duration::from_millis(
                            state.current_backoff_ms.max(config_clone.base_backoff_ms)
                        ));
                    }
                },
                Err(e) => {
                    error!("Failed to get connection from pool: {}", e);
                    std::thread::sleep(Duration::from_secs(1));
                }
            }
        }
        // Depart cleanly: remove this consumer from every group it joined, so the
        // group reflects live participants rather than every connection ever made.
        // Only safe when nothing is pending — DELCONSUMER discards pending entries,
        // and an entry discarded here is one no reclaim can ever recover. Anything
        // still held is left for another node to reclaim once it goes idle.
        info!("Environment stream worker shutting down (node: {}, type: {})", node_id_owned, node_type_owned);
        if let Ok(mut conn) = connection_manager::get_connection() {
            let mut departed = 0usize;
            let mut retained = 0usize;
            for stream_key in &active_unified_streams {
                let pending: redis::RedisResult<i64> = redis::cmd("XGROUP")
                    .arg("DELCONSUMER").arg(stream_key).arg(consumer_group).arg(&consumer_name)
                    .query(&mut conn);
                match pending {
                    Ok(0) => departed += 1,
                    Ok(n) => {
                        retained += 1;
                        warn!("Departed {} holding {} pending entries — they were discarded; \
                               another node cannot reclaim what no longer exists", stream_key, n);
                    },
                    Err(_) => {}
                }
            }
            if departed > 0 || retained > 0 {
                info!("Consumer {} removed from {} stream group(s)", consumer_name, departed + retained);
            }
        }
    });

    Ok(handle)
}

/// Filter commands based on node_type and group_hint field using dynamic RoutingConfig
///
/// The routing configuration is fetched from the global cache, which was populated
/// from ValKey during daemon startup. This allows nodes to dynamically learn their
/// routing rules without hardcoding.
///
/// Returns (commands_to_process, commands_to_skip, ids_to_ack, ids_to_skip)
#[allow(clippy::type_complexity)]
fn filter_commands_by_node_type(
    commands: &[(String, crate::integration::processor::resp3_protocol::OptimizedCommand)],
    _message_ids: &[String],
    node_type: &str,
    debug_mode: bool
) -> (
    Vec<(String, crate::integration::processor::resp3_protocol::OptimizedCommand)>,
    Vec<(String, crate::integration::processor::resp3_protocol::OptimizedCommand)>,
    Vec<String>,
    Vec<String>
) {
    // Get routing config from cache (populated during daemon startup)
    let routing_config = crate::routing_config::get_cached_routing_config(node_type);

    if debug_mode {
        debug!("Using routing config for node_type '{}': mode={:?}, hints={:?}",
            node_type, routing_config.routing.mode, routing_config.routing.group_hints);
    }

    let mut to_process = Vec::new();
    let mut to_skip = Vec::new();
    let mut ids_to_ack = Vec::new();
    let mut ids_to_skip = Vec::new();

    for (id, cmd) in commands.iter() {
        // Use the command's own ID — commands is a subset of message_ids
        // (only t=c/t=bc), so positional indexing into message_ids is wrong
        let msg_id = id.clone();
        let group_hint = cmd.group_hint.as_deref();

        // Use the dynamic routing config to determine if we should process this message
        let should_process = routing_config.should_process_message(group_hint);

        if should_process {
            to_process.push((id.clone(), cmd.clone()));
            if !msg_id.is_empty() {
                ids_to_ack.push(msg_id);
            }
        } else {
            if debug_mode {
                debug!("Skipping message {} with group_hint='{:?}' (node_type='{}')",
                    id, group_hint, node_type);
            }
            to_skip.push((id.clone(), cmd.clone()));
            if !msg_id.is_empty() {
                ids_to_skip.push(msg_id);
            }
        }
    }

    (to_process, to_skip, ids_to_ack, ids_to_skip)
}

/// Claim pending messages that match this node's routing config
///
/// This is used by specialized nodes (e.g., inference nodes) to claim messages
/// that were read by general nodes but not acknowledged because they didn't
/// match the general node's routing config.
///
/// The function uses the node's routing config to determine which group_hints
/// to look for when claiming messages.
#[allow(clippy::too_many_arguments, clippy::type_complexity)]
/// Reclaim pending entries whose owner stopped, restricted to this node's exposure.
///
/// An entry taken by `XREADGROUP` stays owned by that consumer until acknowledged.
/// If the consumer disappears — process killed, host slept, network dropped — the
/// entry is never redelivered on its own. Recovering it is a liveness property of
/// every node, not a feature of specialised ones.
///
/// Ownership is inspected BEFORE it is transferred. `XAUTOCLAIM` moves ownership of
/// everything it returns, so filtering afterwards leaves this consumer holding work
/// it will not perform, which is worse than not reclaiming at all. Here `XPENDING`
/// and `XRANGE` are reads; only entries this node is exposed to are then `XCLAIM`ed.
///
/// `min_idle_ms` must exceed the slowest legitimate processing time, or reclaim
/// steals entries that are still being worked on and they run twice.
fn reclaim_exposed_pending_messages(
    conn: &mut redis::Connection,
    stream_key: &str,
    group_name: &str,
    consumer_name: &str,
    node_type: &str,
    max_count: usize,
    min_idle_ms: u64,
    debug_mode: bool
) -> IntegrationResult<usize> {
    let routing_config = crate::routing_config::get_cached_routing_config(node_type);

    // READ ONLY: which entries are idle past the threshold, and who holds them.
    let pending: redis::RedisResult<Vec<(String, String, u64, u64)>> = redis::cmd("XPENDING")
        .arg(stream_key)
        .arg(group_name)
        .arg("IDLE")
        .arg(min_idle_ms)
        .arg("-")
        .arg("+")
        .arg(max_count)
        .query(conn);

    let pending = match pending {
        Ok(p) if !p.is_empty() => p,
        Ok(_) => return Ok(0),
        Err(e) => {
            if debug_mode {
                debug!("XPENDING on {} returned error (normal when the group is new): {}", stream_key, e);
            }
            return Ok(0);
        }
    };

    // READ ONLY: inspect each entry's routing hint before deciding to take it.
    let mut claimable: Vec<String> = Vec::new();
    let mut skipped_unexposed = 0usize;
    for (msg_id, owner, idle_ms, _delivered) in &pending {
        // Our own in-flight entries are not orphans.
        if owner == consumer_name {
            continue;
        }

        let entry: redis::RedisResult<Vec<(String, Vec<(String, String)>)>> = redis::cmd("XRANGE")
            .arg(stream_key)
            .arg(msg_id)
            .arg(msg_id)
            .query(conn);

        let fields = match entry {
            Ok(rows) => match rows.into_iter().next() {
                Some((_, f)) => f,
                // Entry gone from the stream (trimmed) but still in the PEL: the
                // owner can never complete it. Claiming would inherit a phantom, so
                // leave it for the group's own bookkeeping.
                None => continue,
            },
            Err(_) => continue,
        };

        let gh_value = fields.iter().find(|(k, _)| k == "_gh").map(|(_, v)| v.as_str());

        if routing_config.should_process_message(gh_value) {
            claimable.push(msg_id.clone());
        } else {
            skipped_unexposed += 1;
            if debug_mode {
                debug!("Leaving {} (hint {:?}, idle {}ms) — outside this node's exposure",
                    msg_id, gh_value, idle_ms);
            }
        }
    }

    if skipped_unexposed > 0 && debug_mode {
        debug!("{} orphaned entries on {} left for an exposed node", skipped_unexposed, stream_key);
    }

    if claimable.is_empty() {
        return Ok(0);
    }

    // Ownership moves only now, and only for entries this node will process.
    let mut claim = redis::cmd("XCLAIM");
    claim.arg(stream_key).arg(group_name).arg(consumer_name).arg(min_idle_ms);
    for id in &claimable {
        claim.arg(id);
    }
    let claimed: redis::RedisResult<Vec<(String, Vec<(String, String)>)>> = claim.query(conn);

    match claimed {
        Ok(rows) => {
            if !rows.is_empty() {
                info!("Reclaimed {} orphaned entries from {} (idle >= {}ms)",
                    rows.len(), stream_key, min_idle_ms);
            }
            Ok(rows.len())
        },
        Err(e) => {
            if debug_mode {
                debug!("XCLAIM on {} failed: {}", stream_key, e);
            }
            Ok(0)
        }
    }
}

/// Remove consumers that stopped and left nothing behind.
///
/// Every reconnection of an intermittent node can create a consumer, and nothing
/// removes them, so group metadata grows with historical connections rather than
/// live participants.
///
/// A consumer holding pending entries is never removed: `DELCONSUMER` discards
/// those entries, turning "stuck" into "lost". Reclaim empties a consumer first;
/// this only sweeps what is already empty.
fn reap_stale_consumers(
    conn: &mut redis::Connection,
    stream_key: &str,
    group_name: &str,
    self_consumer: &str,
    idle_threshold_ms: u64,
    debug_mode: bool
) -> IntegrationResult<usize> {
    let consumers: redis::RedisResult<Vec<std::collections::HashMap<String, redis::Value>>> =
        redis::cmd("XINFO").arg("CONSUMERS").arg(stream_key).arg(group_name).query(conn);

    let consumers = match consumers {
        Ok(c) => c,
        Err(e) => {
            if debug_mode {
                debug!("XINFO CONSUMERS on {} failed: {}", stream_key, e);
            }
            return Ok(0);
        }
    };

    let as_string = |v: &redis::Value| -> Option<String> {
        match v {
            redis::Value::BulkString(b) => String::from_utf8(b.clone()).ok(),
            redis::Value::SimpleString(s) => Some(s.clone()),
            _ => None,
        }
    };
    let as_u64 = |v: &redis::Value| -> Option<u64> {
        match v {
            redis::Value::Int(i) => Some(*i as u64),
            other => as_string(other).and_then(|s| s.parse().ok()),
        }
    };

    let mut removed = 0usize;
    for c in &consumers {
        let name = match c.get("name").and_then(as_string) {
            Some(n) => n,
            None => continue,
        };
        // Never reap ourselves, however idle this stream happens to be.
        if name == self_consumer {
            continue;
        }
        let pending = c.get("pending").and_then(as_u64).unwrap_or(1);
        let idle = c.get("idle").and_then(as_u64).unwrap_or(0);

        if pending == 0 && idle >= idle_threshold_ms {
            let res: redis::RedisResult<i64> = redis::cmd("XGROUP")
                .arg("DELCONSUMER").arg(stream_key).arg(group_name).arg(&name)
                .query(conn);
            if let Ok(discarded) = res {
                // Belt and braces: XINFO said zero pending, so this must be zero.
                // If it is not, the consumer became active between the two calls
                // and we have just discarded live work — say so loudly.
                if discarded > 0 {
                    warn!("DELCONSUMER {} on {} discarded {} pending entries — it became active mid-sweep",
                        name, stream_key, discarded);
                } else {
                    removed += 1;
                    if debug_mode {
                        debug!("Reaped stale consumer {} from {} (idle {}ms)", name, stream_key, idle);
                    }
                }
            }
        }
    }

    if removed > 0 {
        info!("Reaped {} stale consumers from {}", removed, stream_key);
    }
    Ok(removed)
}

/// Acknowledge messages in the unified stream
///
/// This function acknowledges messages in a consumer group to mark them as processed.
///
/// # Arguments
///
/// * `conn` - Redis connection
/// * `stream_key` - Unified stream key
/// * `group_name` - Consumer group name
/// * `message_ids` - Message IDs to acknowledge
/// * `site_id` - Site identifier for namespacing
/// * `debug_mode` - Whether debug mode is enabled
///
/// # Returns
///
/// * `IntegrationResult<usize>` - Number of acknowledged messages or error
pub fn acknowledge_messages(
    conn: &mut Connection,
    stream_key: &str,
    group_name: &str,
    message_ids: &[String],
    _site_id: &str,
    debug_mode: bool
) -> IntegrationResult<usize> {
    if message_ids.is_empty() {
        return Ok(0);
    }
    
    if debug_mode {
        debug!("Acknowledging {} messages in unified stream {}", 
            message_ids.len(), stream_key);
    }
    
    // Execute XACK command
    let result: RedisResult<i64> = redis::cmd("XACK")
        .arg(stream_key)
        .arg(group_name)
        .arg(message_ids)
        .query(conn);
    
    match result {
        Ok(count) => {
            if debug_mode {
                debug!("Acknowledged {} messages", count);
            }
            Ok(count as usize)
        },
        Err(e) => {
            let error = stream_processing_error(format!("Failed to acknowledge messages: {}", e));
            log_error(&error, "acknowledging messages in unified stream");
            Err(error)
        }
    }
}

// =========================================================================
// Relay tracing helpers (Phase 5E)
// =========================================================================

/// Start a W3C-compatible trace span for a relay operation.
/// Uses FCALL GNODE_SPAN_START if available (pro tracing library).
/// Returns the span_id on success, None if tracing is unavailable.
fn start_relay_trace_span(
    conn: &mut Connection,
    command_id: &str,
    command_name: &str,
    source_site: &str,
    target_site: &str,
    debug_mode: bool,
) -> Option<String> {
    // Generate trace_id from command_id (deterministic for correlation)
    let trace_id = format!("{:0>32}", &command_id.replace('-', ""));
    let operation = format!("relay:{}", command_name);

    let result: redis::RedisResult<String> = redis::cmd("FCALL")
        .arg("GNODE_SPAN_START")
        .arg(1)
        .arg(format!("{{{}}}:tracing", source_site))
        .arg(&trace_id)
        .arg("") // no parent span (root)
        .arg(&operation)
        .query(conn);

    match result {
        Ok(json_str) => {
            match serde_json::from_str::<serde_json::Value>(&json_str) {
                Ok(val) => {
                    let span_id = val.get("span_id")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());

                    // Annotate span with relay metadata
                    if let Some(ref sid) = span_id {
                        let _ = redis::cmd("FCALL")
                            .arg("GNODE_SPAN_ANNOTATE")
                            .arg(1)
                            .arg(format!("{{{}}}:tracing", source_site))
                            .arg(sid)
                            .arg("relay.source")
                            .arg(source_site)
                            .query::<String>(conn);
                        let _ = redis::cmd("FCALL")
                            .arg("GNODE_SPAN_ANNOTATE")
                            .arg(1)
                            .arg(format!("{{{}}}:tracing", source_site))
                            .arg(sid)
                            .arg("relay.target")
                            .arg(target_site)
                            .query::<String>(conn);
                    }

                    if debug_mode {
                        debug!("Trace span started: trace={} span={:?} op={}",
                            trace_id, span_id, operation);
                    }
                    span_id
                }
                Err(_) => None,
            }
        }
        Err(e) => {
            let err_str = e.to_string();
            if !err_str.contains("Function not found") && !err_str.contains("NOSCRIPT")
                && debug_mode {
                    debug!("GNODE_SPAN_START failed: {}", e);
                }
            None
        }
    }
}

/// Finish a trace span for a relay operation.
fn finish_relay_trace_span(
    conn: &mut Connection,
    span_id: &str,
    success: bool,
    debug_mode: bool,
) {
    // Use a generic site key — span_id is globally unique
    // The tracing library stores spans by span_id, not by site
    let status = if success { "OK" } else { "ERROR" };

    let result: redis::RedisResult<String> = redis::cmd("FCALL")
        .arg("GNODE_SPAN_FINISH")
        .arg(1)
        .arg("tracing") // generic key — span storage is keyed by span_id
        .arg(span_id)
        .arg(status)
        .query(conn);

    if let Err(e) = result {
        let err_str = e.to_string();
        if !err_str.contains("Function not found") && !err_str.contains("NOSCRIPT") && debug_mode {
            debug!("GNODE_SPAN_FINISH failed for span {}: {}", span_id, e);
        }
    } else if debug_mode {
        debug!("Trace span finished: span={} status={}", span_id, status);
    }
}