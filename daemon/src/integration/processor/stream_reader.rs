// Stream reader module for gNode
//
// This module provides functionality for reading command and response
// messages from unified streams using Redis consumer groups.

use std::collections::HashMap;
use log::{debug, warn, trace};
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

/// Stream reader for unified streams
pub struct StreamReader {}

impl Default for StreamReader {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamReader{
    pub fn new() -> Self {
        StreamReader {
            // Initialize fields as needed
        }
    }

    /// Read commands from the unified stream
    ///
    /// This function reads command messages from the unified stream using
    /// the specified consumer group.
    ///
    /// # Arguments
    ///
    /// * `conn` - Redis connection
    /// * `stream_key` - Unified stream key
    /// * `group_name` - Consumer group name
    /// * `consumer_name` - Consumer name
    /// * `count` - Maximum number of messages to read
    /// * `block_ms` - Time to block for new messages in milliseconds
    /// * `site_id` - Site identifier for namespacing
    /// * `debug_mode` - Whether debug mode is enabled
    ///
    /// # Returns
    ///
    /// * `IntegrationResult<Vec<(String, OptimizedCommand)>>` - Message IDs and commands or error
    #[allow(clippy::too_many_arguments)]
    pub fn read_commands(
        conn: &mut Connection,
        stream_key: &str,
        group_name: &str,
        consumer_name: &str,
        count: usize,
        block_ms: u64,
        _site_id: &str,
        debug_mode: bool
    ) -> IntegrationResult<Vec<(String, OptimizedCommand)>> {
        if debug_mode {
            debug!("Reading commands from unified stream {} with consumer {}", 
                stream_key, consumer_name);
        }
        
        // Execute XREADGROUP command using a more flexible approach
        let result: RedisResult<Value> =
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
                .arg(">") // Only new messages
                .query(conn);
        
        match result {
            Ok(value) => {
                // Process the Redis Value manually to handle differences in response format
                Self::process_xreadgroup_response(value, debug_mode)
            },
            Err(e) => {
                let error = stream_processing_error(format!("Failed to read commands: {}", e));
                log_error(&error, "reading commands from unified stream");
                Err(error)
            }
        }
    }

    /// Process XREADGROUP response value to extract command messages
    ///
    /// This function processes the raw Redis value returned by XREADGROUP,
    /// handling different format possibilities and extracting command messages.
    ///
    /// # Arguments
    ///
    /// * `value` - Redis value from XREADGROUP response
    /// * `debug_mode` - Whether debug mode is enabled
    ///
    /// # Returns
    ///
    /// * `IntegrationResult<Vec<(String, OptimizedCommand)>>` - Message IDs and commands or error
    fn process_xreadgroup_response(
        value: Value,
        debug_mode: bool
    ) -> IntegrationResult<Vec<(String, OptimizedCommand)>> {
        // Handle empty response
        if let Value::Nil = value {
            return Ok(Vec::new());
        }
        
        if debug_mode {
            trace!("XREADGROUP response type: {:?}", value);
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
                    format!("Unexpected XREADGROUP simple response type: {:?}", value)
                ));
            },
            // redis 0.26 added RESP3 variants (Map, Set, Attribute, Double,
            // Boolean, VerbatimString, BigNumber, Push, ServerError) that
            // we don't expect from XREADGROUP on RESP2 streams. Treat as
            // protocol anomaly, same as other unexpected types.
            Value::Map(_) | Value::Set(_) | Value::Attribute { .. } | Value::Double(_)
            | Value::Boolean(_) | Value::VerbatimString { .. } | Value::BigNumber(_)
            | Value::Push { .. } | Value::ServerError(_) => {
                return Err(stream_processing_error(
                    format!("Unexpected RESP3 XREADGROUP response type: {:?}", value)
                ));
            },
            Value::Array(entries) => {
                if entries.is_empty() {
                    return Ok(Vec::new());
                }
                
                // First element is the stream entry
                if let Some(Value::Array(stream_parts)) = entries.first() {
                        // Skip stream name (first element), get messages (second element)
                        if stream_parts.len() >= 2 {
                            if let Value::Array(messages) = &stream_parts[1] {
                                // Process each message
                                for message in messages {
                                    if let Value::Array(message_parts) = message {
                                        if message_parts.len() >= 2 {
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
                                                        warn!("Unexpected field format: {:?}", message_parts[1]);
                                                    }
                                                    continue;
                                                }
                                            };
                                            
                                            // Check for typed format (t field) or plain JSON format (command field)
                                            if let Some(msg_type) = fields.get("t") {
                                                trace!("Processing message with type: {}", msg_type);
                                                if msg_type == "c" || msg_type == "bc" {
                                                    // For regular commands, validate command field (support multiple field names)
                                                    if msg_type == "c" {
                                                        let cmd_value = fields.get("c")
                                                            .or_else(|| fields.get("command"))
                                                            .or_else(|| fields.get("command_name"));

                                                        if let Some(cmd) = cmd_value {
                                                            if cmd.is_empty() {
                                                                // Silently skip empty commands without any logging
                                                                continue;
                                                            }
                                                        } else {
                                                            // Silently skip messages without any command fields
                                                            continue;
                                                        }
                                                    }
                                                    
                                                    // For regular commands, parse and add to commands vector
                                                    if msg_type == "c" {
                                                        // Parse command from fields
                                                        match OptimizedCommand::from_resp3_fields(msg_id.clone(), fields.clone()) {
                                                            Ok(cmd) => {
                                                                if debug_mode {
                                                                    debug!("Successfully parsed regular command - ID: {}, command: {}", 
                                                                        msg_id, cmd.command);
                                                                }
                                                                commands.push((msg_id.clone(), cmd));
                                                            },
                                                            Err(e) => {
                                                                warn!("Failed to parse regular command from message {}: {}", msg_id, e);
                                                                if debug_mode {
                                                                    warn!("Regular command fields: {:?}", fields);
                                                                }
                                                            }
                                                        }
                                                    }
                                                    
                                                    // For batch commands, validate the batch_id and messages
                                                    if msg_type == "bc" {
                                                        trace!("Found batch command (bc) with ID: {}", msg_id);
                                                        
                                                        if let Some(batch_id) = fields.get("bi") {
                                                            if batch_id.is_empty() {
                                                                warn!("Skipping batch command - empty batch_id");
                                                                continue;
                                                            }
                                                            trace!("Batch command has batch_id: {}", batch_id);
                                                        } else {
                                                            warn!("Skipping batch command - missing batch_id field");
                                                            continue;
                                                        }
                                                        
                                                        // Ensure batch has messages array
                                                        if !fields.contains_key("m") {
                                                            warn!("Skipping batch command - missing messages array");
                                                            continue;
                                                        }
                                                        
                                                        // Log all fields in the batch command
                                                        trace!("Batch command fields:");
                                                        for (key, value) in &fields {
                                                            if key == "m" && value.len() > 100 {
                                                                trace!("  {} = {} (truncated from {} chars)", key, &value[0..100], value.len());
                                                            } else {
                                                                trace!("  {} = {}", key, value);
                                                            }
                                                        }
                                                    }
                                                    
                                                    // Parse command from fields
                                                    match OptimizedCommand::from_resp3_fields(msg_id.clone(), fields.clone()) {
                                                        Ok(cmd) => {
                                                            // Always log batch commands for debugging
                                                            trace!("Successfully parsed batch command - ID: {}, batch_id: {}", 
                                                                msg_id,
                                                                cmd.batch_id.as_ref().unwrap_or(&"none".to_string()));
                                                            
                                                            // For batch commands, ensure we set the correct message type
                                                            let mut cmd_modified = cmd.clone();
                                                            if cmd_modified.message_type != "bc" {
                                                                warn!("Correcting batch command message type from '{}' to 'bc'", cmd_modified.message_type);
                                                                cmd_modified.message_type = "bc".to_string();
                                                            }
                                                            
                                                            commands.push((msg_id, cmd_modified));
                                                        },
                                                        Err(e) => {
                                                            // Always log batch command parsing errors as they're critical
                                                            warn!("Failed to parse batch command from message {}: {}", msg_id, e);
                                                            warn!("Batch command fields: {:?}", fields);
                                                        }
                                                    }
                                                } else {
                                                    // Specifically ignore 'b' type messages to prevent infinite loops
                                                    if msg_type == "b"
                                                        && debug_mode {
                                                            debug!("Skipping generic batch message (type 'b') to prevent processing loops");
                                                        }
                                                    
                                                    // Silently skip non-command messages (including 'r' and 'br' responses)
                                                    continue;
                                                }
                                            } else if fields.contains_key("command") {
                                                // Plain format: command field with JSON body
                                                if let Some(cmd_json) = fields.get("command") {
                                                    if !cmd_json.is_empty() {
                                                        if let Ok(parsed_cmd) = serde_json::from_str::<serde_json::Value>(cmd_json) {
                                                            // Create a synthetic OptimizedCommand from the JSON
                                                            let cmd = OptimizedCommand {
                                                                id: msg_id.clone(),
                                                                message_type: "c".to_string(),
                                                                source_site: "default".to_string(),
                                                                source_node: "default".to_string(),
                                                                dest_site: "default".to_string(),
                                                                dest_node: "default".to_string(),
                                                                command: parsed_cmd.get("command")
                                                                    .and_then(|v| v.as_str())
                                                                    .unwrap_or("unknown")
                                                                    .to_string(),
                                                                parameters: crate::integration::processor::resp3_protocol::Resp3Value::Map(
                                                                    parsed_cmd.get("params")
                                                                        .and_then(|v| v.as_object())
                                                                        .map(|m| m.iter()
                                                                            .map(|(k, v)| (k.clone(), crate::integration::processor::resp3_protocol::Resp3Value::String(v.to_string())))
                                                                            .collect())
                                                                        .unwrap_or_default()
                                                                ),
                                                                request_id: parsed_cmd.get("id")
                                                                    .and_then(|v| v.as_str())
                                                                    .map(|s| s.to_string()),
                                                                batch_id: None,
                                                                sequence: None,
                                                                status: None,
                                                                result: None,
                                                                error: None,
                                                                total_count: None,
                                                                messages: None,
                                                                timestamp: crate::utils::current_timestamp_ms(),
                                                                path: None,
                                                                category: None,
                                                                load: None,
                                                                version: None,
                                                                signature: None,
                                                                group_hint: fields.get("_gh").map(|s| s.to_string()),
                                                                relay_target: fields.get("_rt").map(|s| s.to_string()),
                                                                relay_reply_to: fields.get("_rr").map(|s| s.to_string()),
                                                                _formatted_messages: None,
                                                            };

                                                            if debug_mode {
                                                                debug!("Successfully parsed plain JSON command - ID: {}, command: {}",
                                                                    msg_id, cmd.command);
                                                            }
                                                            commands.push((msg_id.clone(), cmd));
                                                        } else {
                                                            warn!("Failed to parse plain JSON command from message {}: {}", msg_id, cmd_json);
                                                        }
                                                    }
                                                }
                                            } else {
                                                // Silently skip messages without type field or command field
                                                continue;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                }
            }
        }

        Ok(commands)
    }


    /// Read responses from the unified stream
    ///
    /// This function reads response messages from the unified stream using
    /// the specified consumer group.
    ///
    /// # Arguments
    ///
    /// * `conn` - Redis connection
    /// * `stream_key` - Unified stream key
    /// * `group_name` - Consumer group name
    /// * `consumer_name` - Consumer name
    /// * `count` - Maximum number of messages to read
    /// * `block_ms` - Time to block for new messages in milliseconds
    /// * `site_id` - Site identifier for namespacing
    /// * `debug_mode` - Whether debug mode is enabled
    ///
    /// # Returns
    ///
    /// * `IntegrationResult<Vec<(String, OptimizedCommand)>>` - Message IDs and responses or error
    #[allow(clippy::too_many_arguments)]
    pub fn read_responses(
        conn: &mut Connection,
        stream_key: &str,
        group_name: &str,
        consumer_name: &str,
        count: usize,
        block_ms: u64,
        _site_id: &str,
        debug_mode: bool
    ) -> IntegrationResult<Vec<(String, OptimizedCommand)>> {
        if debug_mode {
            debug!("Reading responses from unified stream {} with consumer {}", 
                stream_key, consumer_name);
        }
        
        // Execute XREADGROUP command using more flexible approach
        let result: RedisResult<Value> = 
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
                .arg(">") // Only new messages
                .query(conn);
        
        match result {
            Ok(value) => {
                // Process the Redis Value to extract response messages
                Self::process_xreadgroup_responses(value, debug_mode)
            },
            Err(e) => {
                let error = stream_processing_error(format!("Failed to read responses: {}", e));
                log_error(&error, "reading responses from unified stream");
                Err(error)
            }
        }
    }

    /// Process XREADGROUP response value to extract response messages
    ///
    /// This function processes the raw Redis value returned by XREADGROUP,
    /// handling different format possibilities and extracting response messages.
    ///
    /// # Arguments
    ///
    /// * `value` - Redis value from XREADGROUP response
    /// * `debug_mode` - Whether debug mode is enabled
    ///
    /// # Returns
    ///
    /// * `IntegrationResult<Vec<(String, OptimizedCommand)>>` - Message IDs and responses or error
    fn process_xreadgroup_responses(
        value: Value,
        debug_mode: bool
    ) -> IntegrationResult<Vec<(String, OptimizedCommand)>> {
        // Handle empty response
        if let Value::Nil = value {
            return Ok(Vec::new());
        }
        
        if debug_mode {
            trace!("XREADGROUP response type for responses: {:?}", value);
        }
        
        let mut responses = Vec::new();
        
        match value {
            Value::Nil => {
                // No messages available
                return Ok(Vec::new());
            },
            Value::BulkString(_) | Value::Int(_) | Value::SimpleString(_) | Value::Okay => {
                // Unexpected simple response type
                return Err(stream_processing_error(
                    format!("Unexpected XREADGROUP simple response type for responses: {:?}", value)
                ));
            },
            // redis 0.26 RESP3 variants we don't expect on XREADGROUP over RESP2.
            Value::Map(_) | Value::Set(_) | Value::Attribute { .. } | Value::Double(_)
            | Value::Boolean(_) | Value::VerbatimString { .. } | Value::BigNumber(_)
            | Value::Push { .. } | Value::ServerError(_) => {
                return Err(stream_processing_error(
                    format!("Unexpected RESP3 XREADGROUP response type for responses: {:?}", value)
                ));
            },
            Value::Array(entries) => {
                if entries.is_empty() {
                    return Ok(Vec::new());
                }
                
                // First element is the stream entry
                if let Some(Value::Array(stream_parts)) = entries.first() {
                        // Skip stream name (first element), get messages (second element)
                        if stream_parts.len() >= 2 {
                            if let Value::Array(messages) = &stream_parts[1] {
                                // Process each message
                                for message in messages {
                                    if let Value::Array(message_parts) = message {
                                        if message_parts.len() >= 2 {
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
                                                        warn!("Unexpected field format for response: {:?}", 
                                                            message_parts[1]);
                                                    }
                                                    continue;
                                                }
                                            };
                                            
                                            // STRICT FILTERING: Only process messages of type 'r' (response) or 'br' (batch response)
                                            // Skip any messages that don't have a type field or aren't responses
                                            if let Some(msg_type) = fields.get("t") {
                                                if msg_type == "r" || msg_type == "br" {
                                                    // For batch responses, validate batch_id
                                                    if msg_type == "br" {
                                                        if debug_mode {
                                                            trace!("Found a batch response message with ID: {}", msg_id);
                                                        }
                                                        
                                                        if let Some(batch_id) = fields.get("bi") {
                                                            if debug_mode {
                                                                trace!("Batch response has batch_id: {}", batch_id);
                                                            }
                                                            
                                                            if batch_id.is_empty() {
                                                                if debug_mode {
                                                                    trace!("Skipping batch response with empty batch_id");
                                                                }
                                                                // Silently skip empty batch IDs
                                                                continue;
                                                            }
                                                        } else {
                                                            if debug_mode {
                                                                trace!("Skipping batch response without batch_id");
                                                            }
                                                            // Silently skip batch responses without batch_id
                                                            continue;
                                                        }
                                                        
                                                        // Log all fields in the batch response
                                                        if debug_mode {
                                                            trace!("Batch response fields:");
                                                            for (key, value) in &fields {
                                                                trace!("  {} = {}", key, value);
                                                            }
                                                        }
                                                    }
                                                    
                                                    // Parse response from fields
                                                    match OptimizedCommand::from_resp3_fields(msg_id.clone(), fields) {
                                                        Ok(resp) => {
                                                            responses.push((msg_id, resp));
                                                        },
                                                        Err(e) => {
                                                            // Only log parse errors in debug mode
                                                            if debug_mode {
                                                                warn!("Failed to parse response from message {}: {}", msg_id, e);
                                                            }
                                                        }
                                                    }
                                                } else {
                                                    // Specifically ignore 'b' type messages to prevent infinite loops
                                                    if msg_type == "b" {
                                                        if debug_mode {
                                                            debug!("Skipping generic batch message (type 'b') to prevent processing loops");
                                                        }
                                                    } else if debug_mode {
                                                        trace!("Skipping message with type '{}', only accepting 'r' and 'br' types", msg_type);
                                                    }
                                                    
                                                    // Silently skip non-response messages without any logging
                                                    continue;
                                                }
                                            } else {
                                                // Silently skip messages without type field
                                                continue;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                }
            }
        }

        Ok(responses)
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
/// * `IntegrationResult<(Vec<(String, OptimizedCommand)>, Vec<(String, HashMap<String, String>)>, Vec<String>, Vec<String>)>` -
///   Tuple of (command messages, health messages, unified stream message IDs, health stream message IDs) or error
#[allow(clippy::too_many_arguments, clippy::type_complexity)]
pub fn read_multi_stream(
    conn: &mut Connection,
    unified_stream: &str,
    health_stream: &str,
    group_name: &str,
    consumer_name: &str,
    count: usize,
    block_ms: u64,
    debug_mode: bool
) -> IntegrationResult<(Vec<(String, OptimizedCommand)>, Vec<(String, HashMap<String, String>)>, Vec<String>, Vec<String>)> {
    if debug_mode {
        debug!("Reading from multiple streams: unified={}, health={}", unified_stream, health_stream);
    }

    // Execute XREADGROUP for multiple streams
    let result: RedisResult<Value> =
        redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg(group_name)
            .arg(consumer_name)
            .arg("COUNT")
            .arg(count)
            .arg("BLOCK")
            .arg(block_ms)
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
/// * `IntegrationResult<(Vec<(String, OptimizedCommand)>, Vec<(String, HashMap<String, String>)>, Vec<String>, Vec<String>)>` -
///   Tuple of (command messages, health messages, unified stream message IDs, health stream message IDs) or error
#[allow(clippy::type_complexity)]
fn process_multi_stream_response(
    value: Value,
    unified_stream: &str,
    health_stream: &str,
    debug_mode: bool
) -> IntegrationResult<(Vec<(String, OptimizedCommand)>, Vec<(String, HashMap<String, String>)>, Vec<String>, Vec<String>)> {
    // Handle empty response
    if let Value::Nil = value {
        return Ok((Vec::new(), Vec::new(), Vec::new(), Vec::new()));
    }

    let mut command_messages = Vec::new();
    let mut health_messages = Vec::new();
    let mut unified_message_ids = Vec::new();
    let mut health_message_ids = Vec::new();

    match value {
        Value::Nil => {
            return Ok((Vec::new(), Vec::new(), Vec::new(), Vec::new()));
        },
        Value::Array(stream_entries) => {
            if stream_entries.is_empty() {
                return Ok((Vec::new(), Vec::new(), Vec::new(), Vec::new()));
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
                                    // Process as command message
                                    if let Some(msg_type) = fields.get("t") {
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
        debug!("Multi-stream read complete: {} commands, {} health updates, {} unified IDs, {} health IDs for ACK",
            command_messages.len(), health_messages.len(), unified_message_ids.len(), health_message_ids.len());
    }

    Ok((command_messages, health_messages, unified_message_ids, health_message_ids))
}