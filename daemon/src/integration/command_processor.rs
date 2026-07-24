// Command Processor Module for gNode
//
// This module provides a consolidated, standardized approach to command processing
// in the gNode daemon. It handles command parsing, execution, and response sending,
// eliminating duplication across multiple modules.
//
// The command processor addresses several key challenges:
// 1. Unifies parsing logic from multiple sources
// 2. Provides a consistent pipeline for processing commands
// 3. Standardizes error handling and response generation
// 4. Eliminates redundant code paths across the codebase
// 5. Ensures consistent behavior regardless of the source of commands

use log::{debug, error, warn, info};
use std::sync::{Arc, RwLock};
use redis::{Connection, Commands};
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;
use regex::Regex;
use crate::config::GNodeSettings;
use crate::integration::ConsumerGroupState;
use crate::integration::CommandHandlerRegistry;
use crate::integration::claim_pending_messages;
use crate::daemon::LogLevel;
use crate::integration::processor::stream_reader::StreamReader;
use crate::integration::OptimizedCommand;
use crate::utils::current_timestamp_ms;
use crate::integration::command_handler::unknown_command_error;
use crate::daemon::Response;
use crate::integration::error_handlings::log_error;

use crate::{
    daemon::Command,
    GeometricTopology
};

use crate::integration::error_handlings::{IntegrationResult, stream_processing_error};
use crate::utils::{get_field, get_field_opt};

// Commit 1.5.a (GN-D2.02): per-message size cap for tenant-submitted JSON
// arriving via ValKey stream bodies. XADD stream MAXLEN bounds total messages
// but NOT per-message nesting / payload size, so a malicious tenant could pin
// a worker thread parsing a deeply nested or huge `parameters` blob and break
// multi-tenant isolation. serde_json's default recursion limit (128) already
// caps depth; this helper adds the missing size check before the parse.
const MAX_PARAMS_BYTES: usize = 64 * 1024;

/// Parse tenant-submitted JSON with a per-message size cap enforced before
/// deserialization. Returns `Err(String)` with a short reason on size or
/// parse failure; callers translate that to their existing log-and-fallback
/// idiom (Value::Null, empty object, etc.).
fn parse_params_safely(s: &str) -> Result<Value, String> {
    if s.len() > MAX_PARAMS_BYTES {
        return Err(format!(
            "payload {} bytes exceeds per-message cap {} bytes",
            s.len(),
            MAX_PARAMS_BYTES
        ));
    }
    serde_json::from_str(s).map_err(|e| e.to_string())
}

// Pre-XADD validate: every emission to the unified stream is checked
// against the published `unified_command` contract before the XADD hits
// ValKey. Fails loud by design — never emit an off-contract message.
// Contract is loaded once at first use; subsequent calls hit the OnceLock
// fast path.
use std::sync::OnceLock;

static UNIFIED_CONTRACT: OnceLock<Option<geodineum_schema::StreamContract>> = OnceLock::new();

/// Resolve the gNode schemas dir the same way daemon.rs:run() does — env var
/// with a compile-time CARGO_MANIFEST_DIR fallback. Keeps the two sides in
/// sync without a cross-module coupling.
fn unified_command_contract() -> Option<&'static geodineum_schema::StreamContract> {
    UNIFIED_CONTRACT
        .get_or_init(|| {
            let schemas_dir = std::env::var("GNODE_SCHEMAS_DIR").unwrap_or_else(|_| {
                concat!(env!("CARGO_MANIFEST_DIR"), "/../config/schemas").to_string()
            });
            let contracts = geodineum_schema::load_contracts(std::path::Path::new(&schemas_dir));
            contracts
                .into_iter()
                .find(|c| c.name == "unified_command")
        })
        .as_ref()
}

/// Convert (key, value) pairs into the HashMap shape validate() expects and
/// run validation against the cached contract. Returns Ok(()) when the
/// contract is unavailable (dev-box without GNODE_SCHEMAS_DIR) — the daemon
/// startup path already fail-fasts on missing schemas dir, so this is only
/// reachable in unit tests.
fn validate_pre_xadd(
    field_pairs: &[(String, String)],
    site: &str,
) -> Result<(), String> {
    let Some(contract) = unified_command_contract() else {
        debug!("pre-XADD validate: unified_command contract not loaded; skipping at {}", site);
        return Ok(());
    };
    let fields: HashMap<String, String> = field_pairs
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let errors = geodineum_schema::validate(contract, &fields);
    if errors.is_empty() {
        return Ok(());
    }
    let summary = errors
        .iter()
        .map(|e| format!("{}: {}", e.field, e.reason))
        .collect::<Vec<_>>()
        .join("; ");
    Err(format!("unified_command validate failed at {}: {}", site, summary))
}

// Re-export Command for compatibility with modules that
// were previously using format_handler::Command
pub use crate::daemon::Command as FormatCommand;

/// Simple Response format for command processing results
#[derive(Debug, Clone)]
pub struct FormatResponse {
    /// Status of the command execution ("ok" or "error")
    pub status: String,
    
    /// Result data (if successful)
    pub result: Option<Value>,
    
    /// Error message (if failed)
    pub error: Option<String>,
}

impl FormatResponse {
    /// Create a success response with any value
    pub fn success<T: Into<Value>>(result: T) -> Self {
        Self {
            status: "ok".to_string(),
            result: Some(result.into()),
            error: None,
        }
    }
    
    /// Create an error response with a message
    pub fn error(message: &str) -> Self {
        Self {
            status: "error".to_string(),
            result: None,
            error: Some(message.to_string()),
        }
    }
}

/// Process a command and send a response
  ///
  /// This function provides a standardized pipeline for processing commands and
  /// sending responses. It handles error recovery, batch operations, and provides
  /// consistent logging.
  ///
  /// # Arguments
  ///
  /// * `connection` - Redis connection
  /// * `topology` - Shared geometric topology
  /// * `command` - Command to process
  /// * `response_stream` - Stream to send the response to
  /// * `site_id` - Site identifier for namespacing
  /// * `debug_mode` - Whether debug mode is enabled
  ///
  /// # Returns
  ///
  /// * `IntegrationResult<String>` - Message ID of the response or error
  pub fn process_command(
    connection: &mut Connection,
    topology: &Arc<RwLock<GeometricTopology>>,
    command: &Command,
    response_stream: &str,
    site_id: &str,
    debug_mode: bool
) -> IntegrationResult<String> {
    // Per-site rate limit (GN-D2.03). Rejection short-circuits before any
    // expensive work — topology read, FCALL, format dispatch — so a
    // flooding tenant can't monopolize this worker. Empty site_id (system
    // commands) bypasses the limit by design; see ratelimit.rs.
    crate::integration::ratelimit::try_acquire(site_id)?;

    // Log command processing
    if debug_mode {
        debug!("Processing command: {} (ID: {})", command.command, command.id);
    }

    // Special handling for format-related commands
    let is_format_command = matches!(command.command.as_str(),
        "register_format" | "list_formats" | "detect_format" | "convert_format" |
        "lf" | "df" | "cf" | "rf"
    );

    if is_format_command && debug_mode {
        debug!("Processing format-related command: {}", command.command);

        // Verify format processor accessibility (CMS extension)
        {
            use crate::daemon::GNodeDaemon;
            if let Some(_processor) = GNodeDaemon::get_format_processor_ref() {
                debug!("Format processor is accessible for command: {}", command.command);
            } else {
                debug!("Format processor not yet initialized");
            }
        }
    }

    // Use the unified stream implementation from command_handler
    crate::integration::command_handler::process_command_unified_stream(
        connection,
        topology,
        command,
        response_stream, // We reuse the response_stream as the unified stream key
        site_id,
        debug_mode
    )
}


// Duplicate functions have been moved up to prevent name collisions

/// Parse a command from script format (flattened field-value pairs)
///
/// This function takes a vector of field-value pairs and extracts a command from them.
/// It's specifically designed for the format returned by ValKey functions and Lua scripts.
///
/// # Arguments
///
/// * `fields` - Vector of field-value pairs from the script
///
/// # Returns
///
/// * `Option<Command>` - The parsed command or None if parsing failed
pub fn parse_command_from_script_format(fields: Vec<(String, String)>) -> Option<Command> {
    if fields.is_empty() {
        return None;
    }

    // Process fields in pairs
    let mut field_map = HashMap::new();

    // If fields are already in key-value pairs
    for (key, value) in &fields {
        field_map.insert(key.clone(), value.clone());
    }

    // Resolve fields through the central canonical alias lists so this
    // parser, the RESP3 parser, and the key-based compute reader all
    // accept identical wire formats. See utils::field_names for the
    // single source of truth.
    use crate::utils::field_names;
    let command_id     = get_field(&field_map, field_names::ID);
    let command_name   = get_field(&field_map, field_names::CMD);
    let parameters_str = get_field(&field_map, field_names::PARAMS);
    let site_id        = get_field(&field_map, field_names::SOURCE_SITE);
    let node_id        = get_field(&field_map, field_names::SOURCE_NODE);
    let timestamp: f64 = get_field_opt(&field_map, field_names::TIMESTAMP)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.0);
    
    // Parse parameters JSON with size cap (GN-D2.02)
    let parameters_value = match parse_params_safely(&parameters_str) {
        Ok(v) => v,
        Err(e) => {
            warn!("Failed to parse parameters '{}': {}", parameters_str, e);
            serde_json::Value::Object(serde_json::Map::new())
        },
    };
    
    // Create command if we have required fields
    if !command_id.is_empty() && !command_name.is_empty() {
        let command = Command {
            id: command_id,
            command: command_name,
            parameters: parameters_value,
            site_id,
            node_id,
            timestamp,
        };
        
        Some(command)
    } else {
        None
    }
}

/// Parse a response from script read operation
///
/// Format: [["message_id", ["key1", "value1", "key2", "value2", ...]], ...]
///
/// # Arguments
///
/// * `json_str` - JSON string to parse
///
/// # Returns
///
/// * `Result<Vec<Command>, serde_json::Error>` - Parsed commands or error
pub fn parse_script_read_response(json_str: &str) -> serde_json::Result<Vec<Command>> {
    // Parse the JSON string
    let entries: Vec<Vec<Value>> = serde_json::from_str(json_str)?;
    let mut commands = Vec::new();
    
    // Process each message entry
    for entry in entries {
        if entry.len() == 2 {
            let _id = entry[0].as_str().unwrap_or("").to_string();
            
            // Get the fields array
            if let Some(fields) = entry[1].as_array() {
                // Process fields in pairs (key, value)
                let mut command_data = HashMap::new();
                let mut i = 0;
                
                while i + 1 < fields.len() {
                    let key = fields[i].as_str().unwrap_or("").to_string();
                    let value = fields[i+1].as_str().unwrap_or("").to_string();
                    command_data.insert(key, value);
                    i += 2;
                }
                
                // Resolve fields via central canonical lists so this parser
                // accepts the same wire format as parse_command_from_script_format
                // and the RESP3/key-based readers. See utils::field_names.
                use crate::utils::field_names;
                let command_id      = get_field(&command_data, field_names::ID);
                let command_name    = get_field(&command_data, field_names::CMD);
                let parameters_json = get_field(&command_data, field_names::PARAMS);
                let site_id         = get_field(&command_data, field_names::SOURCE_SITE);
                let node_id         = get_field(&command_data, field_names::SOURCE_NODE);
                let timestamp: f64  = get_field_opt(&command_data, field_names::TIMESTAMP)
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0);
                
                // Parse parameters JSON with size cap (GN-D2.02)
                let parameters = match parse_params_safely(&parameters_json) {
                    Ok(v) => v,
                    Err(e) => {
                        debug!("Failed to parse parameters JSON: {}", e);
                        Value::Null
                    },
                };

                // Create command object
                let command = Command {
                    id: command_id,
                    command: command_name,
                    parameters,
                    site_id,
                    node_id,
                    timestamp,
                };
                
                commands.push(command);
            }
        }
    }
    
    Ok(commands)
}


/// Parse a response from script group read operation
///
/// Format: [["stream_name", [["message_id", ["key1", "value1", ...]], ...]], ...]
///
/// # Arguments
///
/// * `json_str` - JSON string to parse
///
/// # Returns
///
/// * `Result<HashMap<String, Vec<Command>>, serde_json::Error>` - Parsed commands by stream or error
pub fn parse_script_group_read_response(json_str: &str) -> serde_json::Result<HashMap<String, Vec<Command>>> {
    // Handle empty or nil responses from ValKey function
    if json_str.trim().is_empty() || json_str.trim() == "false" || json_str.trim() == "nil" || json_str.trim() == "[]" {
        return Ok(HashMap::new());
    }
    
    // Check if the response might be a RESP3 format response
    let resp3_patterns = [
        "bulk(bulk(string-data", 
        "RESP3_FORMAT",
        "int(",
        ">> RESP3_FORMAT"
    ];
    for pattern in resp3_patterns {
        if json_str.contains(pattern) {
            debug!("Detected RESP3 format response with pattern '{}' - using enhanced RESP3 parser", pattern);
            
            match parse_script_read_response(json_str) {
                Ok(commands) => {
                    let mut result = HashMap::new();
                    result.insert("default".to_string(), commands);
                    return Ok(result);
                },
                Err(e) => return Err(e),
            }
        }
    }
    
    // Check if the response is a valid JSON array
    if !json_str.trim().starts_with('[') {
        // Direct format from ValKey (not JSON-wrapped) - convert it to a proper stream entry format
        // This is mainly for compatibility with direct XREADGROUP results that weren't JSON encoded
        let mut result = HashMap::new();
        // Extract any messages that look like commands
        let command_pattern = r#"id"#;
        if json_str.contains(command_pattern) {
            // Crude parsing as fallback - look for command patterns in the text
            // This is not ideal but provides a fallback for direct ValKey responses
            let commands = Vec::new();
            if let Some(stream_name) = json_str.lines().next() {
                result.insert(stream_name.trim().to_string(), commands);
            }
        }
        return Ok(result);
    }
    
    // Standard parsing path - parse as JSON array
    let stream_entries: Vec<Vec<Value>> = match serde_json::from_str(json_str) {
        Ok(entries) => entries,
        Err(e) => {
            // Try to handle boolean/nil values wrapped as strings
            if json_str.trim() == "\"false\"" || json_str.trim() == "\"nil\"" {
                return Ok(HashMap::new());
            }
            
            // Try an alternative parsing approach for quote-escaped JSON
            if json_str.contains("\\\"") {
                // Try to unescape the JSON string
                let unescaped = json_str.replace("\\\"", "\"").replace("\\\\", "\\");
                match serde_json::from_str(&unescaped) {
                    Ok(entries) => entries,
                    Err(_) => return Err(e), // If that fails too, return the original error
                }
            } else {
                return Err(e);
            }
        }
    };
    
    let mut result = HashMap::new();
    
    // Process each stream entry
    for stream_entry in stream_entries {
        if stream_entry.len() == 2 {
            let stream_name = stream_entry[0].as_str().unwrap_or("").to_string();
            debug!("Processing stream: {}", stream_name);
            
            // Get the messages array
            if let Some(messages) = stream_entry[1].as_array() {
                let mut commands = Vec::new();
                debug!("Found {} messages in stream {}", messages.len(), stream_name);
                
                // Process each message
                for (msg_idx, message) in messages.iter().enumerate() {
                    debug!("Processing message {}/{} in stream {}", msg_idx+1, messages.len(), stream_name);
                    if let Some(msg_entry) = message.as_array() {
                        if msg_entry.len() == 2 {
                            let msg_id = msg_entry[0].as_str().unwrap_or("").to_string();
                            debug!("Message ID: {}", msg_id);
                            
                            // Get the fields array
                            if let Some(fields) = msg_entry[1].as_array() {
                                // Process fields in pairs (key, value)
                                let mut command_data = HashMap::new();
                                let mut i = 0;
                                debug!("Processing {} field pairs", fields.len() / 2);
                                
                                while i + 1 < fields.len() {
                                    let key = fields[i].as_str().unwrap_or("").to_string();
                                    let value = fields[i+1].as_str().unwrap_or("").to_string();
                                    command_data.insert(key, value);
                                    i += 2;
                                }
                                
                                // Extract command fields from the map (short-form preferred)
                                let command_id = get_field(&command_data, &["id", "request_id"]);
                                let command_name = get_field(&command_data, &["cmd", "c", "command"]);
                                let parameters_json = get_field(&command_data, &["params", "p", "parameters"]);
                                let site_id = get_field(&command_data, &["st", "site_id"]);
                                let node_id = get_field(&command_data, &["n", "node_id"]);
                                let timestamp: f64 = get_field_opt(&command_data, &["ts", "t", "timestamp"])
                                    .map(|s| s.parse().unwrap_or(0.0))
                                    .unwrap_or(0.0);

                                // Parse parameters JSON with size cap (GN-D2.02)
                                let parameters = match parse_params_safely(&parameters_json) {
                                    Ok(v) => v,
                                    Err(e) => {
                                        debug!("Failed to parse parameters JSON: {}", e);
                                        Value::Null
                                    },
                                };

                                // Create command object
                                let command = Command {
                                    id: command_id,
                                    command: command_name,
                                    parameters,
                                    site_id,
                                    node_id,
                                    timestamp,
                                };
                                
                                commands.push(command);
                            }
                        }
                    }
                }
                
                // Add commands for this stream
                result.insert(stream_name, commands);
            }
        }
    }
    
    Ok(result)
}

/// Parse script JSON response (combination of other parsing methods)
///
/// This is a convenience function that tries multiple parsing methods in sequence.
///
/// # Arguments
///
/// * `json_str` - JSON string to parse
///
/// # Returns
///
/// * `Vec<Command>` - Parsed commands
pub fn parse_script_json_response(json_str: &str) -> Vec<Command> {
    // First check if the data is empty or not valid JSON
    if json_str.trim().is_empty() || !json_str.trim().starts_with('[') {
        // Check if this might be a RESP3 format response
        if json_str.contains("bulk(") || json_str.contains("string-data") {
            debug!("Detected possible RESP3 format in parse_script_json_response");
            
            // Try RESP3 parser
            if let Ok(stream_commands) = parse_script_read_response(json_str) {
                return stream_commands;
            }
        }
        
        return Vec::new();
    }
    
    // First try regular read response format
    if let Ok(commands) = parse_script_read_response(json_str) {
        if !commands.is_empty() {
            return commands;
        }
    }
    
    // If unsuccessful or empty, try group read response format
    if let Ok(stream_commands) = parse_script_group_read_response(json_str) {
        let mut all_commands = Vec::new();
        for commands in stream_commands.values() {
            all_commands.extend(commands.clone());
        }
        if !all_commands.is_empty() {
            return all_commands;
        }
    }
    
    // If all previous attempts failed, return empty vector
    Vec::new()
}

/// Parse RESP3 protocol formatted responses from ValKey
/// 
/// This function extracts stream names, message IDs, and field-value pairs to build Command objects
/// from the RESP3 protocol format used by ValKey.
///
/// # Arguments
///
/// * `resp3_str` - RESP3 formatted string
///
/// # Returns
///
/// * `Result<HashMap<String, Vec<Command>>, serde_json::Error>` - Parsed commands by stream or error
/// 
/// Parse RESP3 stream response into structured commands.
/// Uses regex for reliable RESP3 field extraction.
pub fn parse_resp3_stream_response(resp3_str: &str) -> serde_json::Result<HashMap<String, Vec<Command>>> {
    // Check for integer responses - these are probably RESP3 numeric values
    if resp3_str.contains("int(") {
        debug!("Detected numeric RESP3 response: {}", resp3_str);
        // For integer responses, parse the integer value for better diagnostics
        if let Some(int_value) = resp3_str.find("int(").map(|idx| {
            // Extract the value between "int(" and ")"
            let start = idx + 4; // Skip "int("
            if let Some(end) = resp3_str[start..].find(')') {
                resp3_str[start..start+end].trim().to_string()
            } else {
                "0".to_string() // Default if no closing parenthesis
            }
        }) {
            debug!("RESP3 integer value: {}", int_value);
        }
        // Return an empty result for integer responses
        return Ok(HashMap::new());
    }
    
    // Check for RESP3_FORMAT = numeric value pattern
    if resp3_str.contains(">> RESP3_FORMAT = ") && !resp3_str.contains("[") {
        debug!("Detected RESP3_FORMAT with numeric value: {}", resp3_str);
        // Extract the numeric value after the marker
        if let Some(value_start) = resp3_str.find(">> RESP3_FORMAT = ").map(|idx| idx + ">> RESP3_FORMAT = ".len()) {
            let value_str = &resp3_str[value_start..].trim();
            debug!("Extracted numeric value from RESP3_FORMAT: {}", value_str);
        }
        // Return an empty result for numeric responses
        return Ok(HashMap::new());
    }
    
    // Check for RESP3_FORMAT marker
    if resp3_str.contains("RESP3_FORMAT") || resp3_str.contains(">> RESP3_FORMAT") {
        debug!("Detected special RESP3_FORMAT marker, attempting special parsing");
        
        // Check for the specific format pattern with array [["stream_name", [[...]]]]
        if resp3_str.contains(">> RESP3_FORMAT = [[") {
            debug!("Detected specialized array format with RESP3_FORMAT marker");
            
            // Extract the JSON array part after the marker
            if let Some(json_start) = resp3_str.find(">> RESP3_FORMAT = ") {
                let json_str = &resp3_str[json_start + ">> RESP3_FORMAT = ".len()..];
                
                // Try to parse as a standard JSON array
                if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                    debug!("Successfully extracted JSON array from RESP3 response");
                    
                    // Enhanced parsing for RESP3 batch responses
                    if let Some(outer_array) = json_value.as_array() {
                        let mut stream_commands = HashMap::new();
                        
                        // Process each stream entry (there's typically only one)
                        for stream_entry in outer_array {
                            if let Some(stream_array) = stream_entry.as_array() {
                                // The stream array format should be [stream_name, messages]
                                if stream_array.len() >= 2 {
                                    // Extract stream name
                                    let stream_name = match stream_array[0].as_str() {
                                        Some(name) => name.to_string(),
                                        None => {
                                            debug!("Invalid stream name format in RESP3 response");
                                            continue;
                                        }
                                    };
                                    
                                    // The messages array is the second element of the stream array
                                    if let Some(messages_array) = stream_array[1].as_array() {
                                        // Create collection for commands from this stream
                                        let mut commands = Vec::new();
                                        
                                        // Process each message entry
                                        for (idx, message) in messages_array.iter().enumerate() {
                                            // Message format should be [message_id, fields]
                                            if let Some(msg_array) = message.as_array() {
                                                if msg_array.len() >= 2 {
                                                    // Get the message ID (this is the XADD ID)
                                                    let msg_id = match msg_array[0].as_str() {
                                                        Some(id) => id.to_string(),
                                                        None => {
                                                            debug!("Invalid message ID format at index {}", idx);
                                                            continue;
                                                        }
                                                    };
                                                    
                                                    // Get the fields array for this message
                                                    if let Some(fields) = msg_array[1].as_array() {
                                                        // Process field/value pairs
                                                        let mut command_data = HashMap::new();
                                                        let mut i = 0;
                                                        
                                                        // Process fields in pairs
                                                        while i + 1 < fields.len() {
                                                            // Extract field name
                                                            let key = match fields[i].as_str() {
                                                                Some(k) => k.to_string(),
                                                                None => {
                                                                    debug!("Invalid field key at index {}", i);
                                                                    i += 2;
                                                                    continue;
                                                                }
                                                            };
                                                            
                                                            // Extract field value
                                                            let value = match fields[i+1].as_str() {
                                                                Some(v) => v.to_string(),
                                                                None => {
                                                                    debug!("Invalid field value at index {}", i+1);
                                                                    i += 2;
                                                                    continue;
                                                                }
                                                            };
                                                            
                                                            // Store the field-value pair
                                                            command_data.insert(key, value);
                                                            i += 2;
                                                        }
                                                        
                                                        // Extract command fields
                                                        let command_id = command_data.get("id").cloned().unwrap_or_default();
                                                        let command_name = command_data.get("command").cloned().unwrap_or_default();
                                                        let parameters_json = command_data.get("parameters").cloned().unwrap_or_default();
                                                        let site_id = command_data.get("site_id").cloned().unwrap_or_default();
                                                        let node_id = command_data.get("node_id").cloned().unwrap_or_default();
                                                        let timestamp: f64 = command_data.get("timestamp")
                                                            .map(|s| s.parse().unwrap_or(0.0))
                                                            .unwrap_or(0.0);
                                                        
                                                        // Parse parameters JSON with size cap (GN-D2.02)
                                                        let parameters = match parse_params_safely(&parameters_json) {
                                                            Ok(v) => v,
                                                            Err(e) => {
                                                                debug!("Failed to parse parameters JSON: {}", e);
                                                                Value::Null
                                                            },
                                                        };
                                                        
                                                        // Create command if we have required fields
                                                        if !command_id.is_empty() && !command_name.is_empty() {
                                                            // Store both IDs for proper processing
                                                            // command_id - The logical ID of the command (for correlation/tracking)
                                                            // msg_id - The physical stream message ID (for ACKing)
                                                            
                                                            // Create a mutable copy of parameters to add the original command_id
                                                            let mut enhanced_params = parameters.clone();
                                                            
                                                            // Add original command_id as a parameter for response correlation
                                                            if let Some(obj) = enhanced_params.as_object_mut() {
                                                                obj.insert("command_id".to_string(), 
                                                                          serde_json::Value::String(command_id.clone()));
                                                            }
                                                            
                                                            let command = Command {
                                                                id: msg_id.clone(),  // CRITICAL FIX: Use the stream message ID for ACK
                                                                command: command_name,
                                                                parameters: enhanced_params,  // Use enhanced parameters with command_id
                                                                site_id,
                                                                node_id,
                                                                timestamp,
                                                            };
                                                            
                                                            commands.push(command);
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        
                                        // Store commands for this stream
                                        if !commands.is_empty() {
                                            stream_commands.insert(stream_name, commands);
                                        }
                                    }
                                }
                            }
                        }
                        
                        // Return parsed commands if we found any
                        if !stream_commands.is_empty() {
                            return Ok(stream_commands);
                        }
                    }
                    
                    // Fallback - try to parse this array using the standard GROUP_READ parser
                    if let Ok(stream_commands) = parse_script_group_read_response(json_str) {
                        if !stream_commands.is_empty() {
                            return Ok(stream_commands);
                        }
                    }
                }
            }
        }
    }
    
    // Fall back to regex-based RESP3 parsing
    let mut result = HashMap::new();
    
    // Regex to match stream names in RESP3 format: string-data('stream_name')
    static STREAM_RE: once_cell::sync::Lazy<Regex> = once_cell::sync::Lazy::new(|| {
        Regex::new(r"bulk\(\d+\)\(\s*string-data\('([^']*)'\)").unwrap()
    });
    let stream_re = &*STREAM_RE;
    
    // Find all stream names
    for stream_cap in stream_re.captures_iter(resp3_str) {
        let stream_name = stream_cap[1].to_string();
        debug!("Processing stream: {}", stream_name);
        
        // Extract the section for this stream
        if let Some(stream_section_start) = resp3_str.find(&format!("string-data('{}')", stream_name)) {
            let stream_section = &resp3_str[stream_section_start..];
            
            // Regex to find message IDs in this stream section
            static MSG_ID_RE: once_cell::sync::Lazy<Regex> = once_cell::sync::Lazy::new(|| {
                Regex::new(r"string-data\('(\d+-\d+)'\)").unwrap()
            });
            let msg_id_re = &*MSG_ID_RE;
            let mut commands = Vec::new();
            
            // Process each message in the stream
            for msg_cap in msg_id_re.captures_iter(stream_section) {
                let msg_id = msg_cap[1].to_string();
                debug!("Found message ID: {}", msg_id);
                
                // Find the message data section that follows this ID
                if let Some(msg_data_start) = stream_section.find(&format!("string-data('{}')", msg_id)) {
                    let msg_data_section = &stream_section[msg_data_start..];
                    
                    // Extract the bulk array that contains field-value pairs
                    if let Some(bulk_start) = msg_data_section.find("bulk(") {
                        // Find the first bulk array after the message ID
                        let fields_section = &msg_data_section[bulk_start..];
                        
                        // Extract fields and values
                        let fields = crate::integration::processor::stream_utils::extract_message_fields(fields_section);
                        
                        // Extract command fields
                        let command_id = fields.get("id").cloned().unwrap_or_default();
                        let command_name = fields.get("command").cloned().unwrap_or_default();
                        let parameters_json = fields.get("parameters").cloned().unwrap_or_default();
                        let site_id = fields.get("site_id").cloned().unwrap_or_default();
                        let node_id = fields.get("node_id").cloned().unwrap_or_default();
                        let timestamp: f64 = fields.get("timestamp")
                            .map(|s| s.parse().unwrap_or(0.0))
                            .unwrap_or(0.0);
                        
                        // Parse parameters JSON with size cap (GN-D2.02)
                        let parameters = match parse_params_safely(&parameters_json) {
                            Ok(v) => v,
                            Err(e) => {
                                warn!("Failed to parse parameters JSON: {}", e);
                                Value::Null
                            },
                        };
                        
                        // Create command object if we have the required fields
                        if !command_id.is_empty() && !command_name.is_empty() {
                            debug!("Creating command: id={}, command={}", command_id, command_name);
                            
                            let command = Command {
                                id: command_id.clone(),
                                command: command_name,
                                parameters,
                                site_id,
                                node_id,
                                timestamp,
                            };
                            
                            commands.push(command);
                            debug!("Successfully parsed command from RESP3 format: {}", command_id);
                        }
                    }
                }
            }
            
            // Add commands for this stream if we found any
            if !commands.is_empty() {
                result.insert(stream_name, commands);
            }
        }
    }
    
    Ok(result)
}

/// Process commands from the unified stream
///
/// This function reads and processes commands from the unified stream,
/// handling batch operations, error recovery, and response generation.
///
/// # Arguments
///
/// * `conn` - Redis connection
/// * `topology` - Shared geometric topology
/// * `stream_key` - Unified stream key
/// * `group_name` - Consumer group name
/// * `consumer_name` - Consumer name
/// * `config` - Unified stream configuration
/// * `state` - Consumer group state
/// * `registry` - Command handler registry
/// * `site_id` - Site identifier for namespacing
/// * `debug_mode` - Whether debug mode is enabled
///
/// # Returns
///
/// * `IntegrationResult<usize>` - Number of processed commands or error
#[allow(clippy::too_many_arguments)]
pub fn process_commands(
    conn: &mut Connection,
    topology: &Arc<RwLock<GeometricTopology>>,
    stream_key: &str,
    group_name: &str,
    consumer_name: &str,
    config: &GNodeSettings,
    state: &mut ConsumerGroupState,
    registry: &CommandHandlerRegistry,
    site_id: &str,
    debug_mode: bool
) -> IntegrationResult<usize> {
    // Check for pending messages if needed
    if state.should_check_pending(config.pending_check_interval_ms) {
        let pending_result = claim_pending_messages(
            conn,
            stream_key,
            group_name,
            consumer_name,
            config.idle_time_ms,
            state.batch_size,
            site_id,
            debug_mode
        );
        
        match pending_result {
            Ok(pending_commands) => {
                if !pending_commands.is_empty() {
                    if debug_mode {
                        debug!("Processing {} pending commands", pending_commands.len());
                    }
                    
                    let processed = process_command_batch(
                        conn,
                        topology,
                        stream_key,
                        &pending_commands,
                        registry,
                        site_id,
                        "daemon",
                        debug_mode,
                        LogLevel::Info // Default to Info level
                    )?;
                    
                    // Update pending check timestamp
                    state.update_pending_check();
                    
                    // If we processed pending messages, return early
                    if processed > 0 {
                        // Update state
                        state.reset_after_success();
                        state.adjust_batch_size(processed, config.min_batch_size, config.max_batch_size);
                        
                        return Ok(processed);
                    }
                }
            },
            Err(e) => {
                warn!("Failed to claim pending messages: {}", e);
                // Continue with new messages
            }
        }
        
        // Update pending check timestamp even if no messages were processed
        state.update_pending_check();
    }
    
    // Read new commands
    let commands_result = StreamReader::read_commands(
        conn,
        stream_key,
        group_name,
        consumer_name,
        state.batch_size,
        1000, // Block for 1 second
        site_id,
        debug_mode
    );
    
    match commands_result {
        Ok(commands) => {
            if commands.is_empty() {
                // Apply backoff if there were no messages
                state.apply_backoff(config.max_backoff_ms);
                
                // Sleep for backoff duration
                if state.current_backoff_ms > 0 {
                    std::thread::sleep(Duration::from_millis(state.current_backoff_ms));
                }
                
                return Ok(0);
            }
            
            // Process commands
            let processed = process_command_batch(
                conn,
                topology,
                stream_key,
                &commands,
                registry,
                site_id,
                "daemon",
                debug_mode,
                LogLevel::Info // Default to Info level
            )?;
            
            // Update state
            state.reset_after_success();
            state.adjust_batch_size(processed, config.min_batch_size, config.max_batch_size);
            
            Ok(processed)
        },
        Err(e) => {
            warn!("Failed to read commands: {}", e);
            
            // Register error
            state.register_error();
            
            Err(e)
        }
    }
}

/// Process a batch of commands (PREFERRED METHOD)
///
/// This function is the PREFERRED and RECOMMENDED way to process commands in the gNode system.
/// It provides optimal performance, reliability, and consistency for command processing.
/// All new code should use this function instead of individual command processing methods.
///
/// Key advantages:
/// - Batched processing (fewer round-trips than individual processing)
/// - Reduced network overhead with single batch response
/// - Better error handling and recovery
/// - Consistent logging and metrics
/// - Lower memory usage
/// - Proper stream consumer group integration
///
/// This function processes multiple commands in a batch, executing handlers
/// and sending a single batch response to the unified stream.
///
/// # Arguments
///
/// * `conn` - Redis connection
/// * `topology` - Shared geometric topology
/// * `stream_key` - Unified stream key
/// * `commands` - Commands to process
/// * `registry` - Command handler registry
/// * `site_id` - Site identifier for namespacing
/// * `source_node` - Source node identifier
/// * `debug_mode` - Whether debug mode is enabled
/// * `debug_level` - Debug level for conditional logging
///
/// # Returns
///
/// * `IntegrationResult<usize>` - Number of processed commands or error
///
/// # Performance Metrics
/// - Metric: command_batch_size - Number of commands in the batch
/// - Metric: command_batch_processing_time_ms - Time taken to process the batch
/// - Metric: command_batch_success_count - Number of successfully processed commands
#[allow(clippy::too_many_arguments)]
pub fn process_command_batch(
    conn: &mut Connection,
    topology: &Arc<RwLock<GeometricTopology>>,
    stream_key: &str,
    commands: &[(String, OptimizedCommand)],
    registry: &CommandHandlerRegistry,
    site_id: &str,
    source_node: &str,
    debug_mode: bool,
    debug_level: LogLevel
) -> IntegrationResult<usize> {
    if commands.is_empty() {
        return Ok(0);
    }

    // Start metrics collection
    let _start_time = std::time::Instant::now();
    let batch_size = commands.len();

    if debug_level >= LogLevel::Info {
        info!("Processing batch of {} commands", batch_size);
    }

    // Log metrics for monitoring
    debug!("METRIC command_batch_size={}", batch_size);
    
    let mut processed_count = 0;
    let mut message_ids = Vec::new();
    let mut batch_commands = Vec::new();
    let mut regular_commands = Vec::new();
    let mut optimized_responses = Vec::new();
    let mut batch_id = String::new();
    
    // Step 1: Categorize commands into batch and regular
    for (msg_id, optimized) in commands {
        // Add all message IDs for acknowledgment regardless of type
        message_ids.push(msg_id.clone());

        // Extract batch_id from the FIRST command that carries one —
        // batches arrive here already EXPANDED into individual commands
        // (the "bc" envelope never reaches this loop), so `bi` rides on
        // the sub-commands. The old extraction lived only inside the
        // dead `== "bc"` branch below, leaving batch_id empty for every
        // real batch: the batch response went out with `bi=""` and id
        // `br--<ts>` (empty middle segment).
        if batch_id.is_empty() {
            if let Some(bi) = optimized.batch_id.as_deref() {
                if !bi.is_empty() {
                    batch_id = bi.to_string();
                    if debug_level >= LogLevel::Info {
                        info!("Found batch command with batch_id: {}", batch_id);
                    }
                }
            }
        }

        if optimized.message_type == "bc" {
            // Add to batch commands for separate processing
            batch_commands.push((msg_id.clone(), optimized.clone()));
        } else {
            // Add to regular commands
            regular_commands.push((msg_id.clone(), optimized.clone()));
        }
    }
    
    // Step 2: Process batch commands if any
    if !batch_commands.is_empty() {
        if debug_level >= LogLevel::Info {
            info!("Processing {} batch command(s)", batch_commands.len());
        }
        
        // If we didn't extract a batch_id yet, generate one
        if batch_id.is_empty() {
            batch_id = format!("batch-{}", current_timestamp_ms());
            if debug_level >= LogLevel::Info {
                info!("Generated batch_id: {}", batch_id);
            }
        }
        
        // Process each batch command and its contained messages
        for (_, batch_cmd) in &batch_commands {
            // Get the messages array from the batch command
            if let Some(batch_messages) = &batch_cmd.messages {
                if debug_level >= LogLevel::Info {
                    info!("Processing {} messages in batch command", batch_messages.len());
                }
                
                // Process each message in the batch
                for (idx, batch_msg) in batch_messages.iter().enumerate() {
                    if debug_level >= LogLevel::Info {
                        info!("Processing batch sub-command {}/{}: {}", 
                              idx + 1, batch_messages.len(), batch_msg.command);
                    }
                    
                    // Convert to standard command
                    let sub_command = Command::from_optimized(batch_msg);
                    
                    // Skip empty commands to avoid logging errors
                    if sub_command.command.trim().is_empty() {
                        continue;
                    }
                    
                    // Get handler for command
                    let handler_opt = registry.get_handler(&sub_command.command);
                    
                    // Execute handler
                    let result = match handler_opt {
                        Some(handler) => {
                            if debug_level >= LogLevel::Info {
                                info!("Found registered handler for batch sub-command: {}", sub_command.command);
                            }
                            // Call handler with proper arguments
                            handler(&sub_command, conn, topology, site_id, debug_mode)
                        },
                        None => {
                            unknown_command_error(&sub_command.command)
                        }
                    };
                    
                    // Convert result to response
                    let response = result.to_response(&sub_command.id);
                    
                    // Create optimized response
                    let mut optimized_response = response.to_optimized(
                        site_id,
                        source_node,
                        &batch_cmd.source_site,
                        &batch_cmd.source_node
                    );
                    
                    // Add sequence and batch_id to the response
                    optimized_response.sequence = batch_msg.sequence;
                    optimized_response.batch_id = Some(batch_id.clone());
                    optimized_response.command = sub_command.command.clone();
                    
                    // Add to responses collection
                    optimized_responses.push(optimized_response);
                    processed_count += 1;
                }
            } else {
                // Fallback: If messages array is missing from the 'messages' field
                if let Some(m_field) = batch_cmd.to_resp3_fields().get("m") {
                    // Try to parse from the raw m field
                    if let Ok(msg_array) = serde_json::from_str::<Vec<Vec<String>>>(m_field) {
                        if debug_level >= LogLevel::Info {
                            info!("Extracted {} commands from message array", msg_array.len());
                        }
                        
                        for (idx, msg_parts) in msg_array.iter().enumerate() {
                            if msg_parts.len() >= 3 {
                                // Extract command and params
                                let _cmd_type = &msg_parts[0];
                                let cmd_name = &msg_parts[1];
                                let cmd_params = &msg_parts[2];
                                let sequence = if msg_parts.len() >= 4 {
                                    msg_parts[3].parse::<u32>().unwrap_or(idx as u32)
                                } else {
                                    idx as u32
                                };
                                
                                if debug_level >= LogLevel::Info {
                                    info!("Processing batch sub-command {}/{} from raw array: {}", 
                                        idx + 1, msg_array.len(), cmd_name);
                                }
                                
                                // Create a command
                                let sub_command = Command {
                                    id: format!("{}-{}", batch_cmd.id, idx),
                                    command: cmd_name.clone(),
                                    parameters: parse_params_safely(cmd_params).unwrap_or_else(|_| serde_json::json!({})),
                                    site_id: batch_cmd.source_site.clone(),
                                    node_id: batch_cmd.source_node.clone(),
                                    timestamp: (batch_cmd.timestamp as f64) / 1000.0
                                };
                                
                                // Skip empty commands to avoid logging errors
                                if sub_command.command.trim().is_empty() {
                                    continue;
                                }
                                
                                // Get handler for command
                                let handler_opt = registry.get_handler(&sub_command.command);
                                
                                // Execute handler
                                let result = match handler_opt {
                                    Some(handler) => {
                                        if debug_level >= LogLevel::Info {
                                            info!("Found registered handler for raw batch command: {}", sub_command.command);
                                        }
                                        // Call handler with proper arguments
                                        handler(&sub_command, conn, topology, site_id, debug_mode)
                                    },
                                    None => {
                                        unknown_command_error(&sub_command.command)
                                    }
                                };
                                
                                // Convert result to response
                                let response = result.to_response(&sub_command.id);
                                
                                // Create optimized response
                                let mut optimized_response = response.to_optimized(
                                    site_id,
                                    source_node,
                                    &batch_cmd.source_site,
                                    &batch_cmd.source_node
                                );
                                
                                // Add sequence and batch_id to the response
                                optimized_response.sequence = Some(sequence);
                                optimized_response.batch_id = Some(batch_id.clone());
                                optimized_response.command = sub_command.command.clone();
                                
                                // Add to responses collection
                                optimized_responses.push(optimized_response);
                                processed_count += 1;
                            }
                        }
                    }
                }
            }
        }
    }
    
    // Step 3: Process regular (non-batch) commands
    if !regular_commands.is_empty() {
        if debug_level >= LogLevel::Info {
            info!("Processing {} regular command(s)", regular_commands.len());
        }
        
        for (_, command_obj) in &regular_commands {
            // Convert to standard command
            let command = Command::from_optimized(command_obj);
            
            // Skip empty commands to avoid logging errors
            if command.command.trim().is_empty() {
                continue;
            }
            
            if debug_level >= LogLevel::Info {
                info!("Processing regular command: {} (ID: {})", command.command, command.id);
            }
            
            // Lane lookup — every command declares its execution lane
            // via CommandDescriptor.lane, wired into
            // actual routing:
            //   Lane::Fast    — if an async handler exists AND the
            //                   Fast-lane runtime is initialized, hand
            //                   the command off to tokio::spawn and
            //                   move on immediately. Response gets
            //                   written by the spawned task.
            //   Lane::Ordered — keep the synchronous inline call as
            //                   before; ordering preserved across the
            //                   batch.
            //   Fallback      — if Fast-lane runtime isn't initialized
            //                   (tests, single-shot tools), or if no
            //                   async handler is registered, fall
            //                   through to the synchronous path.
            let lane = registry.get_lane(&command.command);
            let async_available = registry.has_async(&command.command);
            if debug_level >= LogLevel::Info {
                info!(
                    "Dispatching {} (lane={:?}, async_available={}, fast_lane_initialized={})",
                    command.command,
                    lane,
                    async_available,
                    crate::integration::fast_lane::is_initialized()
                );
            }

            // Fast-lane spawn — fire-and-forget. The spawned task
            // writes its own response to the polling key; nothing more
            // to do here. Increment counter and skip to next message.
            if matches!(lane, crate::integration::handlers::types::Lane::Fast)
                && async_available
                && crate::integration::fast_lane::is_initialized()
            {
                let cmd_owned = command.clone();
                let site_owned = site_id.to_string();
                let topology_clone = topology.clone();
                let env_owned = crate::integration::receipt::env_from_stream_key(stream_key)
                    .map(String::from);
                let spawned = crate::integration::fast_lane::try_spawn_fast(
                    crate::integration::fast_lane::dispatch(
                        cmd_owned,
                        site_owned,
                        topology_clone,
                        env_owned,
                        debug_mode,
                    ),
                );
                if spawned {
                    processed_count += 1;
                    continue;
                }
                // try_spawn_fast returned false (race: runtime dropped
                // between is_initialized() and the spawn). Fall through
                // to the synchronous path as a safety net.
            }

            // Synchronous (Ordered lane, or Fast fallback) dispatch.
            let handler_opt = registry.get_handler(&command.command);
            let result = match handler_opt {
                Some(handler) => {
                    if debug_level >= LogLevel::Info {
                        info!("Found registered handler for command: {}", command.command);
                    }
                    handler(&command, conn, topology, site_id, debug_mode)
                },
                None => {
                    unknown_command_error(&command.command)
                }
            };
            
            // Convert result to response
            let response = result.to_response(&command.id);

            // Write response to ValKey key for poll-based clients. Prefer the
            // client-supplied _request_id in parameters (what pollForResponse
            // keys on); fall back to command.id — the wire "id" the reader now
            // parses — so top-level-id callers get a response too. This mirrors
            // the Fast lane; without the fallback, any Ordered-lane command sent
            // with a top-level id but no _request_id hangs the poll to timeout.
            let rid: Option<String> = command
                .parameters
                .get("_request_id")
                .and_then(|v| v.as_str())
                .map(String::from)
                .or_else(|| {
                    if !command.id.is_empty() {
                        Some(command.id.clone())
                    } else {
                        None
                    }
                });
            if let Some(request_id) = rid {
                let key_site = if command.site_id.is_empty() { site_id } else { &command.site_id };
                let response_key = format!("{{{}}}:res:{}", key_site, request_id);
                let response_json = serde_json::json!({
                    "id": response.id,
                    "status": response.status,
                    "result": response.result,
                    "error": response.error,
                    "timestamp": response.timestamp
                }).to_string();
                let _ = redis::cmd("SET")
                    .arg(&response_key)
                    .arg(&response_json)
                    .arg("EX").arg(10) // 10 second TTL
                    .query::<()>(conn);
                if debug_level >= LogLevel::Info {
                    info!("Wrote response to polling key: {}", response_key);
                }

                // Durable channel: a signed receipt beside the ephemeral reply.
                // Additive — the t=r/br stream writes below are untouched until
                // the receipt is verified live (contract §6, emit-then-remove).
                let now = crate::integration::receipt::now_ms();
                let env = crate::integration::receipt::env_from_stream_key(stream_key)
                    .map(String::from)
                    .or_else(|| crate::integration::receipt::receipt_context()
                        .map(|c| c.environment.clone()));
                if let (Some(env), Some(receipt)) = (env, crate::integration::receipt::signed_response_receipt(
                    &request_id,
                    &command.command,
                    &response.status,
                    response.error.clone(),
                    key_site,
                    &response_key,
                    &response_json,
                    now,
                )) {
                    match crate::integration::receipt::emit_receipt(conn, &receipt, &env, now) {
                        Ok(id) => crate::integration::receipt::log_first_emission(
                            &crate::integration::receipt::receipt_stream_key(key_site, &env), &id),
                        Err(e) => warn!("receipt emit failed for {}: {}", request_id, e),
                    }
                }
            }

            // Create optimized response
            let optimized_response = response.to_optimized(
                site_id,
                source_node,
                &command_obj.source_site,
                &command_obj.source_node
            );

            // Add to responses collection
            optimized_responses.push(optimized_response);
            processed_count += 1;
        }
    }

    // Step 4: Send responses - batch response for batch commands or multiple responses, individual responses otherwise
    if !optimized_responses.is_empty() {
        // Relay response routing: if the command was relayed (has _rr field),
        // send the response to the relay_reply_to stream instead of the local stream.
        // This is the "belt" in the belt-and-suspenders relay response routing.
        let response_stream = commands.first()
            .and_then(|(_, cmd)| cmd.relay_reply_to.as_deref())
            .unwrap_or(stream_key);
        let is_relay_response = response_stream != stream_key;
        // Determine if we need to use batch response format
        let force_batch_response = !batch_commands.is_empty() || optimized_responses.len() > 1;
        
        if force_batch_response {
            // Last-resort fallback: never emit a batch response with an
            // empty batch_id (the upstream generate-one lives in the
            // dead bc branch and never runs for expanded batches).
            if batch_id.is_empty() {
                batch_id = format!("batch-{}", current_timestamp_ms());
                if debug_level >= LogLevel::Info {
                    info!("Generated batch_id: {}", batch_id);
                }
            }

            // Ensure all responses have the batch_id and command set
            for resp in &mut optimized_responses {
                // Set batch_id if not already set
                if resp.batch_id.is_none() {
                    resp.batch_id = Some(batch_id.clone());
                }
                
                // Ensure command is not empty for proper response association
                if resp.command.is_empty() && resp.request_id.is_some() {
                    // Try to extract command from request ID if possible
                    if let Some(ref req_id) = resp.request_id {
                        if let Some(cmd_name) = req_id.split('-').nth(1) {
                            resp.command = cmd_name.to_string();
                        }
                    }
                }
            }
            
            if debug_level >= LogLevel::Info {
                info!("Creating batch response with {} individual responses, batch_id: {}", 
                      optimized_responses.len(), batch_id);
            }
            
            // Create the batch response using create_batch_response function
            let batch_response = OptimizedCommand::create_batch_response(
                &optimized_responses,
                &batch_id
            );
            
            // Log the message type of the batch response for debugging
            if debug_level >= LogLevel::Info {
                info!("Batch response created with message_type: {}", batch_response.message_type);
            }
            
            // Convert batch response to fields and send to stream
            let mut fields = batch_response.to_resp3_fields();
            
            // Add validation for message format
            if debug_level >= LogLevel::Debug {
                if let Some(messages) = fields.get("m") {
                    if let Ok(parsed) = serde_json::from_str::<Vec<Vec<String>>>(messages) {
                        for (i, msg) in parsed.iter().enumerate() {
                            if (msg.len() != 4 || msg[0] != "r")
                                && debug_level >= LogLevel::Warning {
                                    warn!("Invalid message format at index {}: {:?}", i, msg);
                                }
                        }
                    } else if debug_level >= LogLevel::Warning {
                        warn!("Failed to parse messages array for validation");
                    }
                }
            }

            // Set required fields for proper consumer group routing
            fields.insert("_gh".to_string(), "gnode-client".to_string());
            
            // Extract origin info from the first command for routing
            if let Some((_, first_cmd)) = commands.first() {
                // Set destination from the source of the original command
                if !first_cmd.source_site.is_empty() {
                    fields.insert("ds".to_string(), first_cmd.source_site.clone());
                } else {
                    fields.insert("ds".to_string(), "client".to_string());
                }
                
                if !first_cmd.source_node.is_empty() {
                    fields.insert("dn".to_string(), first_cmd.source_node.clone());
                } else {
                    fields.insert("dn".to_string(), "*".to_string());
                }
            } else {
                // Fallback defaults
                fields.insert("ds".to_string(), "client".to_string());
                fields.insert("dn".to_string(), "*".to_string());
            }
            
            // Set the source as gNode daemon
            fields.insert("ss".to_string(), "gNode".to_string());
            fields.insert("sn".to_string(), "daemon".to_string());
            
            // Add critical metadata for consumer group routing
            fields.insert("_cr".to_string(), "1".to_string());
            
            // Ensure message type is 'br'
            if !fields.contains_key("t") || fields.get("t") != Some(&"br".to_string()) {
                fields.insert("t".to_string(), "br".to_string());
                if debug_level >= LogLevel::Info {
                    info!("Ensured 'type' field is set to 'br'");
                }
            }
            
            // Ensure batch_id is set
            if !fields.contains_key("bi") {
                fields.insert("bi".to_string(), batch_id.clone());
                if debug_level >= LogLevel::Info {
                    info!("Ensured 'batch_id' field is set");
                }
            }
            
            // Add metadata for tracking/debugging
            let timestamp = current_timestamp_ms().to_string();
            fields.insert("btm".to_string(), timestamp);
            fields.insert("tc".to_string(), optimized_responses.len().to_string());
            
            // Print all fields being sent in the batch response
            if debug_level >= LogLevel::Info {
                info!("Prepared batch response with fields count: {}", fields.len());
                
                if debug_level >= LogLevel::Debug {
                    debug!("Batch response fields:");
                    for (key, value) in &fields {
                        if key == "m" && value.len() > 100 {
                            debug!("  {} = {} (truncated from {} chars)", key, &value[0..100], value.len());
                        } else {
                            debug!("  {} = {}", key, value);
                        }
                    }
                }
            }
            
            if debug_level >= LogLevel::Info {
                if is_relay_response {
                    info!("Sending batch response to relay source stream: {}", response_stream);
                } else {
                    info!("Sending batch response to stream: {}", response_stream);
                }
            }

            // No client/response consumer group ensure: batch responses are read
            // by keyed rendezvous ({ss}:res:{id}), not from a group. The batch
            // response is XADDed below AND written to the response key by the
            // response writer; gnode-client had no reader and is retired.

            // Convert fields to field pairs
            let mut field_pairs = Vec::new();
            for (key, value) in &fields {
                field_pairs.push((key.clone(), value.clone()));
            }

            // Pre-XADD validate against unified_command contract.
            // Fail-loud: log + return Err, skip the xadd.
            if let Err(msg) = validate_pre_xadd(&field_pairs, "batch_response") {
                error!("{}", msg);
                return Err(stream_processing_error(msg));
            }

            // Implement retry mechanism with progressive backoff
            const MAX_RETRIES: usize = 3;
            let mut retry_count = 0;
            let mut success = false;
            let mut _batch_response_id = String::new();

            while retry_count < MAX_RETRIES && !success {
                if retry_count > 0 {
                    let backoff_ms = 50 * (1 << retry_count);
                    if debug_level >= LogLevel::Warning {
                        warn!("Retrying batch response send (attempt {}/{}, backoff {} ms)", 
                             retry_count + 1, MAX_RETRIES, backoff_ms);
                    }
                    std::thread::sleep(std::time::Duration::from_millis(backoff_ms));
                }
                
                // Attempt to add batch response to stream
                match conn.xadd::<_, _, _, _, String>(response_stream, "*", &field_pairs) {
                    Ok(msg_id) => {
                        if debug_level >= LogLevel::Info {
                            info!("Batch response added to stream with ID: {}", msg_id);
                        }
                        _batch_response_id = msg_id;
                        
                        // Verify the message exists in the stream (simplified)
                        let mut cmd: redis::Cmd = redis::cmd("XINFO");
                        cmd.arg("STREAM").arg(stream_key);
                        
                        match cmd.query::<redis::Value>(conn) {
                            Ok(_) => {
                                // If we can get stream info, assume the message was added successfully
                                if debug_level >= LogLevel::Info {
                                    info!("Skipping batch response verification - batch response assumed valid");
                                }
                                success = true;
                                break;
                            },
                            Err(e) => {
                                if debug_level >= LogLevel::Warning {
                                    warn!("Error checking stream info: {}", e);
                                }
                                // Continue with retry logic
                                retry_count += 1;
                            }
                        }
                        
                        if !success {
                            retry_count += 1;
                        }
                    },
                    Err(e) => {
                        if debug_level >= LogLevel::Warning {
                            warn!("Failed to send batch response: {}", e);
                        }
                        retry_count += 1;
                    }
                }
            }
            
            // If batch response failed, fall back to individual responses
            if !success {
                if debug_level >= LogLevel::Warning {
                    warn!("Batch response sending failed after {} attempts, falling back to individual responses", MAX_RETRIES);
                }
                
                // Send individual responses instead
                for (idx, response) in optimized_responses.iter().enumerate() {
                    if debug_level >= LogLevel::Info {
                        info!("Sending individual response {}/{}", idx + 1, optimized_responses.len());
                    }
                    
                    let mut response_fields = response.to_resp3_fields();
                    
                    // Ensure key fields are set properly
                    response_fields.insert("t".to_string(), "r".to_string());  // Regular response type
                    
                    // Set routing information
                    if let Some((_, first_cmd)) = commands.first() {
                        // Set destination from the source of the original command
                        response_fields.insert("ds".to_string(), first_cmd.source_site.clone());
                        response_fields.insert("dn".to_string(), first_cmd.source_node.clone());
                    }
                    
                    // Set source as gNode daemon
                    response_fields.insert("ss".to_string(), "gNode".to_string());
                    response_fields.insert("sn".to_string(), "daemon".to_string());
                    
                    // Convert to field pairs
                    let mut response_pairs = Vec::new();
                    for (key, value) in response_fields {
                        response_pairs.push((key, value));
                    }

                    // Pre-XADD validate (Commit 0.5.d). Fallback path after
                    // batch failure; skip this specific response on contract
                    // violation and continue to the next one.
                    if let Err(msg) = validate_pre_xadd(&response_pairs, "fallback_individual_response") {
                        error!("{}", msg);
                        continue;
                    }

                    // Send with retries
                    let mut resp_retry = 0;
                    while resp_retry < 2 {
                        match conn.xadd::<_, _, _, _, String>(response_stream, "*", &response_pairs) {
                            Ok(msg_id) => {
                                if debug_level >= LogLevel::Info {
                                    info!("Individual response sent with ID: {}", msg_id);
                                }
                                break;
                            },
                            Err(e) => {
                                if debug_level >= LogLevel::Warning {
                                    warn!("Failed to send individual response: {}", e);
                                }
                                resp_retry += 1;
                                std::thread::sleep(std::time::Duration::from_millis(50));
                            }
                        }
                    }
                }
            }
        } else {
            // For non-batch commands with single response, send individual response
            if debug_level >= LogLevel::Info {
                info!("Sending {} individual response(s)", optimized_responses.len());
            }
            
            for (idx, response) in optimized_responses.iter().enumerate() {
                let mut individual_fields = response.to_resp3_fields();
                
                // Set source/destination explicitly
                individual_fields.insert("ss".to_string(), "gNode".to_string()); // Source: gNode
                individual_fields.insert("sn".to_string(), "daemon".to_string()); // Source: daemon
                
                if let Some((_, cmd)) = commands.first() {
                    individual_fields.insert("ds".to_string(), cmd.source_site.clone()); // Dest: client site
                    individual_fields.insert("dn".to_string(), cmd.source_node.clone()); // Dest: client node
                } else {
                    individual_fields.insert("ds".to_string(), "client".to_string());
                    individual_fields.insert("dn".to_string(), "*".to_string());
                }
                
                let mut individual_pairs = Vec::new();
                for (key, value) in individual_fields {
                    individual_pairs.push((key, value));
                }

                // Pre-XADD validate (Commit 0.5.d). Non-batch single-response
                // path; skip and move to next response on contract violation.
                if let Err(msg) = validate_pre_xadd(&individual_pairs, "nonbatch_individual_response") {
                    error!("{}", msg);
                    continue;
                }

                // Try to send individual response with retry
                let mut retry = 0;
                let max_retry = 2;

                while retry < max_retry {
                    match conn.xadd::<_, _, _, _, String>(response_stream, "*", &individual_pairs) {
                        Ok(msg_id) => {
                            if debug_level >= LogLevel::Info {
                                info!("Individual response {} sent with ID: {}", idx + 1, msg_id);
                            }
                            break;
                        },
                        Err(e) => {
                            if debug_level >= LogLevel::Warning {
                                warn!("Failed to send individual response (attempt {}/{}): {}", 
                                     retry + 1, max_retry, e);
                            }
                            retry += 1;
                            if retry < max_retry {
                                std::thread::sleep(std::time::Duration::from_millis(50 * (1 << retry)));
                            }
                        }
                    }
                }
            }
        }
    }
    
    // Acknowledge processed messages
    if !message_ids.is_empty() {
        match crate::integration::consumer_groups::acknowledge_messages(
            conn,
            stream_key,
            "gnode-daemon",
            &message_ids,
            site_id,
            debug_mode
        ) {
            Ok(ack_count) => {
                if debug_level >= LogLevel::Debug {
                    debug!("Acknowledged {} messages", ack_count);
                }
            },
            Err(e) => {
                warn!("Failed to acknowledge messages: {}", e);
            }
        }
    }
    
    Ok(processed_count)
}

/// Send a command to the unified stream
///
/// This function encodes a command to RESP3 format and adds it to the unified stream.
///
/// # Arguments
///
/// * `conn` - Redis connection
/// * `command` - Command to send
/// * `stream_key` - Unified stream key
/// * `site_id` - Site identifier for namespacing
/// * `debug_mode` - Whether debug mode is enabled
///
/// # Returns
///
/// * `IntegrationResult<String>` - Message ID or error
pub fn send_command(
    conn: &mut Connection,
    command: &Command,
    stream_key: &str,
    _site_id: &str,
    debug_mode: bool
) -> IntegrationResult<String> {
    if debug_mode {
        debug!("Sending command {} to unified stream {}", command.command, stream_key);
    }
    
    // Convert command to OptimizedCommand
    let optimized = command.to_optimized();
    
    // Convert to field map
    let fields = optimized.to_resp3_fields();
    
    // Build field pairs for XADD
    let mut field_pairs = Vec::new();
    for (key, value) in fields {
        field_pairs.push((key, value));
    }

    // Pre-XADD validate. This is the command-emission path (t=c); shape
    // drift here would corrupt the bidirectional stream. Fail-loud.
    if let Err(msg) = validate_pre_xadd(&field_pairs, "send_command") {
        let err = stream_processing_error(msg);
        log_error(&err, "pre-XADD validate (send_command)");
        return Err(err);
    }

    // Add command to stream
    match conn.xadd(stream_key, "*", &field_pairs) {
        Ok(msg_id) => {
            if debug_mode {
                debug!("Command sent successfully with ID: {}", msg_id);
            }
            Ok(msg_id)
        },
        Err(e) => {
            let error = stream_processing_error(format!("Failed to send command: {}", e));
            log_error(&error, "sending command to unified stream");
            Err(error)
        }
    }
}

/// Send a response to the unified stream (with explicit source/destination)
///
/// This function encodes a response to RESP3 format and adds it to the unified stream.
/// It allows specifying the source and destination for the response.
///
/// # Arguments
///
/// * `conn` - Redis connection
/// * `response` - Response to send
/// * `stream_key` - Unified stream key
/// * `source_site` - Source site identifier
/// * `source_node` - Source node identifier
/// * `dest_site` - Destination site identifier
/// * `dest_node` - Destination node identifier
/// * `site_id` - Site identifier for namespacing
/// * `debug_mode` - Whether debug mode is enabled
///
/// # Returns
///
/// * `IntegrationResult<String>` - Message ID or error
#[allow(clippy::too_many_arguments)]
pub fn send_response_with_routing(
    conn: &mut Connection,
    response: &Response,
    stream_key: &str,
    source_site: &str,
    source_node: &str,
    dest_site: &str,
    dest_node: &str,
    _site_id: &str,
    debug_mode: bool
) -> IntegrationResult<String> {
    if debug_mode {
        debug!("Sending response for command {} to unified stream {}", 
            response.id, stream_key);
    }
    
    // Convert response to OptimizedCommand
    let mut optimized = response.to_optimized(source_site, source_node, dest_site, dest_node);
    
    // Explicitly ensure message type is set to 'r' for responses
    optimized.message_type = "r".to_string();
    
    // Convert to field map
    let fields = optimized.to_resp3_fields();
    
    // Build field pairs for XADD
    let mut field_pairs = Vec::new();
    for (key, value) in fields {
        field_pairs.push((key, value));
    }

    // Pre-XADD validate (Commit 0.5.d). Response-with-routing path; fail-loud.
    if let Err(msg) = validate_pre_xadd(&field_pairs, "send_response_with_routing") {
        let err = stream_processing_error(msg);
        log_error(&err, "pre-XADD validate (send_response_with_routing)");
        return Err(err);
    }

    // Add response to stream
    match conn.xadd(stream_key, "*", &field_pairs) {
        Ok(msg_id) => {
            if debug_mode {
                debug!("Response sent successfully with ID: {}", msg_id);
            }
            Ok(msg_id)
        },
        Err(e) => {
            let error = stream_processing_error(format!("Failed to send response: {}", e));
            log_error(&error, "sending response to unified stream");
            Err(error)
        }
    }
}
