// RESP3 Protocol implementation for gNode
//
// This module provides the RESP3 value representation and optimized command
// structure for the unified stream approach. It handles conversions between
// standard commands and the optimized RESP3 format.

use std::collections::HashMap;
use serde_json::{Value, Map};
use log::{debug, info, warn};
use crate::integration::processor::stream_utils::current_timestamp;
use crate::integration::current_timestamp_ms;
/// RESP3 value representation for optimized protocol
#[derive(Debug, Clone)]
pub enum Resp3Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Double(f64),
    String(String),
    Array(Vec<Resp3Value>),
    Map(HashMap<String, Resp3Value>),
    // Additional RESP3 types as needed
}

/// Command optimized for RESP3 processing
#[derive(Debug, Clone)]
pub struct OptimizedCommand {
    pub id: String,                // Message ID
    pub message_type: String,      // Message type (c,r,bc,br,m,e)
    pub source_site: String,       // Source site
    pub source_node: String,       // Source node
    pub dest_site: String,         // Destination site
    pub dest_node: String,         // Destination node
    pub command: String,           // Command name (for command types)
    pub parameters: Resp3Value,    // Parameters (for command types)
    pub request_id: Option<String>, // Request ID (for response types)
    pub batch_id: Option<String>,  // Batch ID (for batch types)
    pub sequence: Option<u32>,     // Sequence number
    pub status: Option<String>,    // Status (for response types)
    pub result: Option<Resp3Value>, // Result (for response types)
    pub error: Option<String>,     // Error message (for response types)
    pub total_count: Option<u32>,  // Total count (for batch types)
    pub messages: Option<Vec<OptimizedCommand>>, // Messages (for batch types)
    pub timestamp: i64,            // Timestamp in milliseconds

    // New optional fields for enhanced protocol
    pub path: Option<String>,      // Path for API routing
    pub category: Option<String>,  // Category for message classification
    pub load: Option<f64>,         // Load factor for resource distribution
    pub version: Option<String>,   // Version for versioning support
    pub signature: Option<String>, // Signature for verification

    /// Group hint for message routing (_gh field)
    /// Used for node-type based message routing:
    /// - "inference": Message should be processed by inference nodes
    /// - "general" or empty: Message should be processed by general nodes
    /// - Other values: Custom routing
    pub group_hint: Option<String>,

    /// Relay target: entity_id, site_id, or capability query JSON (_rt field)
    /// Presence of this field indicates the command should be relayed to another site.
    pub relay_target: Option<String>,

    /// Reply-to stream override (_rr field)
    /// If set, the target daemon sends the response to this stream instead of its own.
    pub relay_reply_to: Option<String>,

    // Internal field for proper batch response formatting
    pub _formatted_messages: Option<Vec<Vec<String>>>, // For storing formatted messages
}

// Extension for standard Command
use crate::daemon::{Command, Response};

impl OptimizedCommand {
    /// Convert from standard Command to OptimizedCommand
    pub fn from_standard(command: &Command) -> Self {
        // Convert timestamp from float to milliseconds
        let timestamp_ms = (command.timestamp * 1000.0) as i64;
        
        // Convert parameters to RESP3Value
        let parameters = json_to_resp3(&command.parameters);
        
        // Determine if this is a batch command
        let (batch_id, sequence, messages, total_count) = if let Some(batch) = command.parameters.get("batch_id") {
            // This is part of a batch
            let batch_id = batch.as_str().map(|s| s.to_string());
            let sequence = command.parameters.get("sequence")
                .and_then(|v| v.as_u64())
                .map(|n| n as u32);
            (batch_id, sequence, None, None)
        } else {
            (None, None, None, None)
        };
        
        // Shorten command name if possible
        let shortened_command = shorten_command_name(&command.command);
        
        // Extract optional routing fields from parameters if present
        let path = command.parameters.get("path").and_then(|v| v.as_str().map(|s| s.to_string()));
        let category = command.parameters.get("category").and_then(|v| v.as_str().map(|s| s.to_string()));
        let load = command.parameters.get("load").and_then(|v| v.as_f64());
        let version = command.parameters.get("version").and_then(|v| v.as_str().map(|s| s.to_string()));
        let signature = command.parameters.get("signature").and_then(|v| v.as_str().map(|s| s.to_string()));
        
        OptimizedCommand {
            id: command.id.clone(),
            message_type: "c".to_string(), // c = command
            source_site: command.site_id.clone(),
            source_node: command.node_id.clone(),
            dest_site: "gNode".to_string(), // Default destination is gNode
            dest_node: "*".to_string(),   // Default is all nodes
            command: shortened_command,
            parameters,
            request_id: None, // Not applicable for commands
            batch_id,
            sequence,
            status: None,      // Not applicable for commands
            result: None,      // Not applicable for commands
            error: None,       // Not applicable for commands
            total_count,
            messages,
            timestamp: timestamp_ms,
            path,
            category,
            load,
            version,
            signature,
            group_hint: None, // Will be set when writing to stream with _gh field
            relay_target: None,
            relay_reply_to: None,
            _formatted_messages: None,
        }
    }

    /// Convert to RESP3 field map for stream storage
    pub fn to_resp3_fields(&self) -> HashMap<String, String> {
        let mut fields = HashMap::new();

        // id is universally required by the unified_command schema —
        // commands correlate via id, responses target a request's id, and
        // batch responses use their own id as the wrapping identifier.
        // Dropping it here would fail every batch-response XADD at
        // pre-validation.
        fields.insert("id".to_string(), self.id.clone());

        // Add message type
        fields.insert("t".to_string(), self.message_type.clone());

        // Add source info
        fields.insert("ss".to_string(), self.source_site.clone());
        fields.insert("sn".to_string(), self.source_node.clone());

        // Add destination info
        fields.insert("ds".to_string(), self.dest_site.clone());
        fields.insert("dn".to_string(), self.dest_node.clone());
        
        // Type-specific fields
        if self.message_type == "c" {
            // Command
            fields.insert("c".to_string(), self.command.clone());
            fields.insert("p".to_string(), resp3_to_json_string(&self.parameters));
        } else if self.message_type == "r" {
            // Response
            if let Some(ref req_id) = self.request_id {
                fields.insert("ri".to_string(), req_id.clone());
            }
            if let Some(ref status) = self.status {
                fields.insert("st".to_string(), status.clone());
            }
            if let Some(ref result) = self.result {
                fields.insert("r".to_string(), resp3_to_json_string(result));
            }
            if let Some(ref error) = self.error {
                fields.insert("e".to_string(), error.clone());
            }
        } else if self.message_type == "bc" || self.message_type == "br" {
            // Batch command or batch response
            if let Some(ref batch_id) = self.batch_id {
                fields.insert("bi".to_string(), batch_id.clone());
            }
            if let Some(total) = self.total_count {
                fields.insert("tc".to_string(), total.to_string());
            }
            
            // Use formatted messages if available for batch responses
            if self.message_type == "br" {
                if let Some(ref formatted_msgs) = self._formatted_messages {
                    // For batch responses, use the properly formatted messages array
                    fields.insert("m".to_string(), serde_json::to_string(formatted_msgs)
                        .unwrap_or_else(|_| "[]".to_string()));
                    
                    // Payload dump is debug-level info, not a warning.
                    // Operators investigating a batch problem will raise RUST_LOG
                    // to debug; routine operation should stay silent.
                    if log::log_enabled!(log::Level::Debug) {
                        let msg_str = serde_json::to_string(formatted_msgs).unwrap_or_default();
                        if msg_str.len() > 100 {
                            debug!("Formatted message array (truncated): {}...", &msg_str[0..100]);
                        } else {
                            debug!("Formatted message array: {}", msg_str);
                        }
                    }
                } else if let Some(ref messages) = self.messages {
                    // Fallback for batch responses without formatted messages
                    let json_messages = messages.iter()
                        .map(|msg| {
                            let mut message_parts = Vec::new();
                            message_parts.push("r".to_string()); // Always r for responses
                            message_parts.push(msg.command.clone());
                            
                            // Use result field for responses
                            let content = match msg.result.as_ref() {
                                Some(r) => resp3_to_json_string(r),
                                None => "{}".to_string(),
                            };
                            
                            message_parts.push(content);
                            message_parts.push(msg.sequence.unwrap_or(0).to_string());
                            serde_json::Value::Array(message_parts.into_iter()
                                .map(serde_json::Value::String)
                                .collect())
                        })
                        .collect::<Vec<_>>();
                    
                    fields.insert("m".to_string(), serde_json::to_string(&json_messages)
                        .unwrap_or_else(|_| "[]".to_string()));
                }
            } else if let Some(ref messages) = self.messages {
                // For batch commands or fallback for responses
                let json_messages = messages.iter()
                    .map(|msg| {
                        let mut message_parts = Vec::new();
                        message_parts.push(msg.message_type.clone());
                        message_parts.push(msg.command.clone());
                        
                        // Use result field for responses or parameters for commands
                        let content = if msg.message_type == "r" {
                            match msg.result.as_ref() {
                                Some(r) => resp3_to_json_string(r),
                                None => resp3_to_json_string(&msg.parameters),
                            }
                        } else {
                            resp3_to_json_string(&msg.parameters)
                        };
                        
                        message_parts.push(content);
                        message_parts.push(msg.sequence.unwrap_or(0).to_string());
                        serde_json::Value::Array(message_parts.into_iter()
                            .map(serde_json::Value::String)
                            .collect())
                    })
                    .collect::<Vec<_>>();
                
                fields.insert("m".to_string(), serde_json::to_string(&json_messages)
                    .unwrap_or_else(|_| "[]".to_string()));
            }
        }
        
        // Add correlation fields
        if let Some(ref batch_id) = self.batch_id {
            fields.insert("bi".to_string(), batch_id.clone());
        }
        if let Some(sequence) = self.sequence {
            fields.insert("sq".to_string(), sequence.to_string());
        }
        
        // Add timestamp
        fields.insert("ts".to_string(), self.timestamp.to_string());
        
        // Add new optional fields if present
        if let Some(ref path) = self.path {
            fields.insert("pa".to_string(), path.clone());
        }
        if let Some(ref category) = self.category {
            fields.insert("ca".to_string(), category.clone());
        }
        if let Some(load) = self.load {
            fields.insert("lo".to_string(), load.to_string());
        }
        if let Some(ref version) = self.version {
            fields.insert("ve".to_string(), version.clone());
        }
        if let Some(ref signature) = self.signature {
            fields.insert("si".to_string(), signature.clone());
        }

        // Relay fields
        if let Some(ref rt) = self.relay_target {
            fields.insert("_rt".to_string(), rt.clone());
        }
        if let Some(ref rr) = self.relay_reply_to {
            fields.insert("_rr".to_string(), rr.clone());
        }

        fields
    }

    /// Get the command parameters as a JSON string.
    /// Used by relay translator to detect and convert formats.
    pub fn parameters_as_json_string(&self) -> String {
        resp3_to_json_string(&self.parameters)
    }

    /// Replace the command parameters from a translated JSON string.
    /// Used by relay format translation to apply converted parameters.
    pub fn set_parameters_from_json(&mut self, json_str: &str) {
        self.parameters = json_to_resp3(&parse_json(json_str));
    }

    /// Parse from RESP3 field map from stream.
    ///
    /// Field-name resolution mirrors `utils::field_names`. Where this
    /// parser hard-codes alias lists (because crate::utils is not in
    /// scope here), the lists MUST match the canonical set. If you
    /// change one, change the other.
    pub fn from_resp3_fields(id: String, fields: HashMap<String, String>) -> Result<Self, String> {
        // Required: message type (TYPE = ["t", "type"])
        let mut message_type = fields.get("t")
            .or_else(|| fields.get("type"))
            .ok_or_else(|| "Missing message type".to_string())?
            .clone();

        // Routing — canonical compact first, long-form aliases as fallbacks.
        // Order MUST match utils::field_names (see doc-comment above).
        // SOURCE_SITE: ss > source_site > service_id > site_id > st.
        // "service_id" is the preferred long form; "st" is a legacy alias and
        // also means status in t=r, but those are disjoint contexts.
        let source_site = fields.get("ss")
            .or_else(|| fields.get("source_site"))
            .or_else(|| fields.get("service_id"))
            .or_else(|| fields.get("site_id"))
            .or_else(|| fields.get("st"))
            .cloned()
            .unwrap_or_default();

        // SOURCE_NODE: sn > source_node > node_id > n.
        let source_node = fields.get("sn")
            .or_else(|| fields.get("source_node"))
            .or_else(|| fields.get("node_id"))
            .or_else(|| fields.get("n"))
            .cloned()
            .unwrap_or_default();

        let dest_site = fields.get("ds")
            .or_else(|| fields.get("dest_site"))
            .cloned()
            .unwrap_or_default();

        let dest_node = fields.get("dn")
            .or_else(|| fields.get("dest_node"))
            .cloned()
            .unwrap_or_default();

        // TIMESTAMP = ["ts", "timestamp"] — "t" intentionally excluded
        // (collides with TYPE).
        let timestamp = fields.get("ts")
            .or_else(|| fields.get("timestamp"))
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or_else(|| {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default();
                now.as_millis() as i64
            });
        
        // Initialize type-specific fields
        let mut command = String::new();
        let mut parameters = Resp3Value::Null;
        let mut request_id = None;
        let mut batch_id = None;
        let mut sequence = None;
        let mut status = None;
        let mut result = None;
        let mut error = None;
        let mut total_count = None;
        let mut messages = None;

        // Extract type-specific fields
        if message_type == "c" {
            // Command - support multiple field name formats for maximum compatibility
            // CMD: c > cmd > command > command_name (utils::field_names::CMD).
            command = fields.get("c")
                .or_else(|| fields.get("cmd"))
                .or_else(|| fields.get("command"))
                .or_else(|| fields.get("command_name")).cloned()
                .unwrap_or_default();

            parameters = fields.get("p")
                .or_else(|| fields.get("params"))
                .or_else(|| fields.get("parameters")) // Support test file format
                .map(|v| json_to_resp3(&parse_json(v)))
                .unwrap_or(Resp3Value::Null);

            // Wire ID = ["id", "request_id"] (utils::field_names contract).
            // The struct `id` stays the stream entry id (XACK/dedupe);
            // request correlation — and therefore the {ss}:res:{id} polling
            // key — must come from the client-supplied field, or polled
            // responses land on a key no client ever reads.
            request_id = fields.get("id")
                .or_else(|| fields.get("request_id"))
                .cloned();
        } else if message_type == "r" {
            // Response
            request_id = fields.get("ri").cloned();
            status = fields.get("st").cloned();
            
            result = fields.get("r")
                .map(|v| json_to_resp3(&parse_json(v)));
            
            error = fields.get("e").cloned();
        } else if message_type == "bc" || message_type == "br" || message_type == "b" {
            // Batch command (bc), batch response (br), or generic batch (b)
            batch_id = fields.get("bi").cloned();
            
            total_count = fields.get("tc")
                .and_then(|v| v.parse::<u32>().ok());
            
            // Parse messages array
            if let Some(msgs_json) = fields.get("m") {
                if let Ok(msgs_value) = serde_json::from_str::<serde_json::Value>(msgs_json) {
                    if let Some(msgs_array) = msgs_value.as_array() {
                        let mut batch_messages = Vec::new();
                        
                        for (i, msg) in msgs_array.iter().enumerate() {
                            if let Some(msg_array) = msg.as_array() {
                                if msg_array.len() >= 4 {
                                    // Extract message parts
                                    let msg_type = msg_array[0].as_str().unwrap_or("c").to_string();
                                    let msg_command = msg_array[1].as_str().unwrap_or("").to_string();
                                    let msg_params_json = msg_array[2].as_str().unwrap_or("{}").to_string();
                                    let msg_sequence = msg_array[3].as_str()
                                        .and_then(|s| s.parse::<u32>().ok())
                                        .unwrap_or(i as u32);
                                    
                                    // Create optimized command for this message
                                    let msg_params = json_to_resp3(&parse_json(&msg_params_json));
                                    
                                    let batch_message = OptimizedCommand {
                                        id: format!("{}-{}", id, i),
                                        message_type: msg_type,
                                        source_site: source_site.clone(),
                                        source_node: source_node.clone(),
                                        dest_site: dest_site.clone(),
                                        dest_node: dest_node.clone(),
                                        command: msg_command,
                                        parameters: msg_params,
                                        request_id: None,
                                        batch_id: batch_id.clone(),
                                        sequence: Some(msg_sequence),
                                        status: None,
                                        result: None,
                                        error: None,
                                        total_count: None,
                                        messages: None,
                                        timestamp,
                                        path: None,
                                        category: None,
                                        load: None,
                                        version: None,
                                        signature: None,
                                        group_hint: None,
                                        relay_target: None,
                                        relay_reply_to: None,
                                        _formatted_messages: None,
                                    };

                                    batch_messages.push(batch_message);
                                }
                            }
                        }
                        
                        if !batch_messages.is_empty() {
                            messages = Some(batch_messages);
                        }
                    }
                }
            }
            
            // For generic 'b' type, infer specific batch type from context
            if message_type == "b" {
                // Determine if this is a command or response based on context
                // If it has a request_id, it's likely a response, otherwise a command
                if fields.contains_key("ri") {
                    // Convert to batch response type
                    message_type = "br".to_string();
                } else {
                    // Convert to batch command type
                    message_type = "bc".to_string();
                }
            }
        }
        
        // Extract correlation fields
        if batch_id.is_none() {
            batch_id = fields.get("bi").cloned();
        }

        if sequence.is_none() {
            sequence = fields.get("sq")
                .and_then(|v| v.parse::<u32>().ok());
        }

        // Extract new optional fields
        let path = fields.get("pa").cloned();
        let category = fields.get("ca").cloned();
        let load = fields.get("lo").and_then(|v| v.parse::<f64>().ok());
        let version = fields.get("ve").cloned();
        let signature = fields.get("si").cloned();
        // Extract group hint for message routing (_gh field)
        let group_hint = fields.get("_gh").cloned();

        // Extract relay fields
        let relay_target = fields.get("_rt").cloned();
        let relay_reply_to = fields.get("_rr").cloned();

        // Create and return the optimized command
        Ok(OptimizedCommand {
            id,
            message_type,
            source_site,
            source_node,
            dest_site,
            dest_node,
            command,
            parameters,
            request_id,
            batch_id,
            sequence,
            status,
            result,
            error,
            total_count,
            messages,
            timestamp,
            path,
            category,
            load,
            version,
            signature,
            group_hint,
            relay_target,
            relay_reply_to,
            _formatted_messages: None,
        })
    }

    /// Convert to standard Command
    pub fn to_command(&self) -> Command {
        // Convert parameters to JSON Value
        let mut parameters = resp3_to_json(&self.parameters);
        
        // Ensure parameters is an object
        if !parameters.is_object() {
            parameters = Value::Object(Map::new());
        }
        
        // Add batch metadata if applicable
        if let Some(ref batch_id) = self.batch_id {
            if let Some(obj) = parameters.as_object_mut() {
                obj.insert("batch_id".to_string(), Value::String(batch_id.clone()));
            }
        }
        
        if let Some(sequence) = self.sequence {
            if let Some(obj) = parameters.as_object_mut() {
                obj.insert("sequence".to_string(), Value::Number((sequence as u64).into()));
            }
        }
        
        // Add extended fields if present
        if let Some(obj) = parameters.as_object_mut() {
            if let Some(ref path) = self.path {
                obj.insert("path".to_string(), Value::String(path.clone()));
            }
            
            if let Some(ref category) = self.category {
                obj.insert("category".to_string(), Value::String(category.clone()));
            }
            
            if let Some(load) = self.load {
                if let Some(load_num) = serde_json::Number::from_f64(load) {
                    obj.insert("load".to_string(), Value::Number(load_num));
                }
            }
            
            if let Some(ref version) = self.version {
                obj.insert("version".to_string(), Value::String(version.clone()));
            }
            
            if let Some(ref signature) = self.signature {
                obj.insert("signature".to_string(), Value::String(signature.clone()));
            }
        }
        
        // Expand shortened command name
        let expanded_command = expand_command_name(&self.command);
        
        // Convert timestamp from milliseconds to float seconds
        let timestamp = (self.timestamp as f64) / 1000.0;
        
        Command {
            // Client-supplied request id when present (drives the
            // {ss}:res:{id} polling key); stream entry id only as fallback.
            id: self.request_id.clone().unwrap_or_else(|| self.id.clone()),
            command: expanded_command,
            parameters,
            site_id: self.source_site.clone(),
            node_id: self.source_node.clone(),
            timestamp,
        }
    }
    
    /// Create a response for this command
    pub fn create_response(&self, status: &str, result: Option<Value>, error: Option<String>) -> Self {
        let source_site = self.dest_site.clone();
        let source_node = "daemon".to_string();
        let dest_site = self.source_site.clone();
        let dest_node = self.source_node.clone();
        
        // Convert result to RESP3Value if present
        let resp3_result = result.map(|r| json_to_resp3(&r));
        
        // Generate a unique response ID
        let resp_id = format!("resp-{}-{}", self.id, current_timestamp());
        
        // Determine message type based on whether this is part of a batch
        let message_type = "r".to_string(); // r = normal response (same for batch items and standalone)
        
        // Copy extended fields if present
        let path = self.path.clone();
        let category = self.category.clone();
        let load = self.load;
        let version = self.version.clone();
        let signature = self.signature.clone();
        
        OptimizedCommand {
            id: resp_id,
            message_type,
            source_site,
            source_node,
            dest_site,
            dest_node,
            command: String::new(), // Not applicable for responses
            parameters: Resp3Value::Null, // Not applicable for responses
            request_id: Some(self.id.clone()),
            batch_id: self.batch_id.clone(),
            sequence: self.sequence,
            status: Some(status.to_string()),
            result: resp3_result,
            error,
            total_count: None,
            messages: None,
            timestamp: current_timestamp_ms() as i64,
            path,
            category,
            load,
            version,
            signature,
            group_hint: None,
            relay_target: None,
            relay_reply_to: None,
            _formatted_messages: None,
        }
    }

    /// Create a batch command
    pub fn create_batch(commands: &[Command], batch_id: &str) -> Self {
        if commands.is_empty() {
            return OptimizedCommand {
                id: batch_id.to_string(),
                message_type: "bc".to_string(), // bc = batch command
                source_site: String::new(),
                source_node: String::new(),
                dest_site: "gNode".to_string(),
                dest_node: "*".to_string(),
                command: String::new(),
                parameters: Resp3Value::Null,
                request_id: None,
                batch_id: Some(batch_id.to_string()),
                sequence: None,
                status: None,
                result: None,
                error: None,
                total_count: Some(0),
                messages: None,
                timestamp: current_timestamp_ms() as i64,
                path: None,
                category: None,
                load: None,
                version: None,
                signature: None,
                group_hint: None,
                relay_target: None,
                relay_reply_to: None,
                _formatted_messages: None,
            };
        }
        
        // Use source from first command
        let source_site = commands[0].site_id.clone();
        let source_node = commands[0].node_id.clone();
        
        // Convert commands to optimized format
        let optimized_commands: Vec<OptimizedCommand> = commands.iter()
            .enumerate()
            .map(|(i, cmd)| {
                let mut opt_cmd = OptimizedCommand::from_standard(cmd);
                opt_cmd.batch_id = Some(batch_id.to_string());
                opt_cmd.sequence = Some(i as u32);
                opt_cmd
            })
            .collect();
        
        OptimizedCommand {
            id: batch_id.to_string(),
            message_type: "bc".to_string(), // bc = batch command
            source_site,
            source_node,
            dest_site: "gNode".to_string(),
            dest_node: "*".to_string(),
            command: String::new(), // Not applicable for batch
            parameters: Resp3Value::Null, // Not applicable for batch
            request_id: None,
            batch_id: Some(batch_id.to_string()),
            sequence: None,
            status: None,
            result: None,
            error: None,
            total_count: Some(optimized_commands.len() as u32),
            messages: Some(optimized_commands),
            timestamp: current_timestamp_ms() as i64,
            path: None,
            category: None,
            load: None,
            version: None,
            signature: None,
            group_hint: None,
            relay_target: None,
            relay_reply_to: None,
            _formatted_messages: None,
        }
    }

    /// Create a batch response from individual responses
    pub fn create_batch_response(responses: &[OptimizedCommand], batch_id: &str) -> Self {
        if responses.is_empty() {
            return OptimizedCommand {
                id: format!("br-{}-{}", batch_id, current_timestamp()),
                message_type: "br".to_string(), // br = batch response
                source_site: "gNode".to_string(),
                source_node: "daemon".to_string(),
                dest_site: String::new(),
                dest_node: String::new(),
                command: String::new(),
                parameters: Resp3Value::Null,
                request_id: None,
                batch_id: Some(batch_id.to_string()),
                sequence: None,
                status: Some("ok".to_string()),
                result: None,
                error: None,
                total_count: Some(0),
                messages: Some(Vec::new()),
                timestamp: current_timestamp_ms() as i64,
                path: None,
                category: None,
                load: None,
                version: None,
                signature: None,
                group_hint: None,
                relay_target: None,
                relay_reply_to: None,
                _formatted_messages: Some(Vec::new()), // Empty formatted messages array
            };
        }

        // CRITICAL: Ensure source/destination are properly set for consumer group filtering
        // For batch responses, source should be the daemon, destination should be the client
        
        // Set source as gNode daemon (source of the response)
        let source_site = "gNode".to_string();
        let source_node = "daemon".to_string();
        
        // Set destination as the client (destination of the response)
        // Extract from the SOURCE of the individual responses - this is critical
        // because the original source of the commands is where we need to send the responses
        // Per-batch routing decisions are debug-level detail, not
        // warnings. The "using default" branches are mild anomalies that
        // should fall under info if anything (the daemon doesn't actually
        // fail — it routes to a default destination).
        let dest_site = if !responses[0].source_site.is_empty() {
            debug!("Batch response - destination site: {}", responses[0].source_site);
            responses[0].source_site.clone()
        } else {
            info!("Batch response - no source site in responses, falling back to default");
            "default".to_string()
        };

        let dest_node = if !responses[0].source_node.is_empty() {
            debug!("Batch response - destination node: {}", responses[0].source_node);
            responses[0].source_node.clone()
        } else {
            info!("Batch response - no source node in responses, falling back to default");
            "default".to_string()
        };
        
        // Create formatted messages array - critically important format
        let formatted_messages = responses.iter().enumerate().map(|(i, response)| {
            // Create a message entry in format: ["r", command_name, result_json, sequence]
            let result_json = if let Some(ref result) = response.result {
                resp3_to_json_string(result)
            } else {
                "{}".to_string()
            };
            
            // Use the command name from the original command or default to empty
            let command_name = if !response.command.is_empty() {
                response.command.clone()
            } else {
                // Try to extract command from request_id if available
                response.request_id.as_ref()
                    .and_then(|id| id.split('-').next())
                    .map(|s| s.to_string())
                    .unwrap_or_default()
            };
            
            // Create the array format expected by clients: ["r", command_name, result_json, sequence]
            vec![
                "r".to_string(),                      // Type is always "r" for individual responses
                command_name,                         // Command name 
                result_json,                          // Result as JSON string
                response.sequence.unwrap_or(i as u32).to_string() // Sequence number
            ]
        }).collect::<Vec<_>>();
        
        // Batch-response creation is the normal happy path — fires every
        // time the daemon consolidates responses, so debug! is the
        // correct level.
        debug!("Creating batch response with {} responses for batch_id: {}", responses.len(), batch_id);
        
        let optimized_cmd = OptimizedCommand {
            id: format!("br-{}-{}", batch_id, current_timestamp()),
            message_type: "br".to_string(), // br = batch response
            source_site,
            source_node,
            dest_site,
            dest_node,
            command: String::new(), // Not applicable for batch response
            parameters: Resp3Value::Null, // Not applicable for batch response
            request_id: None,
            batch_id: Some(batch_id.to_string()),
            sequence: None,
            status: Some("ok".to_string()),
            result: None,
            error: None,
            total_count: Some(responses.len() as u32),
            messages: Some(responses.to_vec()),
            timestamp: current_timestamp_ms() as i64,
            path: None,
            category: None,
            load: None,
            version: None,
            signature: None,
            group_hint: None,
            relay_target: None,
            relay_reply_to: None,
            _formatted_messages: Some(formatted_messages), // Store the properly formatted messages
        };

        // Type-tag confirmation fires per-batch — debug, not warn.
        debug!("Batch response created with message_type: {}", optimized_cmd.message_type);
        optimized_cmd
    }
}

impl Command {
    /// Convert to OptimizedCommand
    pub fn to_optimized(&self) -> OptimizedCommand {
        OptimizedCommand::from_standard(self)
    }
    
    /// Create from OptimizedCommand
    pub fn from_optimized(optimized: &OptimizedCommand) -> Self {
        optimized.to_command()
    }
}

impl Response {
    /// Convert to OptimizedCommand
    pub fn to_optimized(&self, source_site: &str, source_node: &str, dest_site: &str, dest_node: &str) -> OptimizedCommand {
        // Convert timestamp from float to milliseconds
        let timestamp_ms = (self.timestamp * 1000.0) as i64;
        
        // Convert result to RESP3Value if present
        let resp3_result = self.result.as_ref().map(json_to_resp3);
        
        // Extract batch_id and sequence from response if present
        let (batch_id, sequence) = match (&self.batch_id, self.sequence) {
            (Some(bid), Some(seq)) => (Some(bid.clone()), Some(seq)),
            (Some(bid), None) => (Some(bid.clone()), None),
            _ => (None, None),
        };
        
        // Determine message type based on batch_id (use "br" for batch responses)
        let message_type = if batch_id.is_some() {
            "br".to_string() // br = batch response
        } else {
            "r".to_string() // r = normal response
        };
        
        OptimizedCommand {
            id: self.id.clone(),
            message_type,
            source_site: source_site.to_string(),
            source_node: source_node.to_string(),
            dest_site: dest_site.to_string(),
            dest_node: dest_node.to_string(),
            command: String::new(), // Not applicable for responses
            parameters: Resp3Value::Null, // Not applicable for responses
            request_id: Some(self.id.clone()), // Use same ID for correlation
            batch_id,
            sequence,
            status: Some(self.status.clone()),
            result: resp3_result,
            error: self.error.clone(),
            total_count: None,
            messages: None,
            timestamp: timestamp_ms,
            path: None,
            category: None,
            load: None,
            version: None,
            signature: None,
            group_hint: None,
            relay_target: None,
            relay_reply_to: None,
            _formatted_messages: None,
        }
    }

    /// Create from OptimizedCommand
    pub fn from_optimized(optimized: &OptimizedCommand) -> Self {
        // Convert timestamp from milliseconds to float seconds
        let timestamp = (optimized.timestamp as f64) / 1000.0;
        
        // Extract request ID or fallback to command ID
        let id = optimized.request_id.clone().unwrap_or_else(|| optimized.id.clone());
        
        // Extract status or default to "ok"
        let status = optimized.status.clone().unwrap_or_else(|| "ok".to_string());
        
        // Convert result if present
        let result = optimized.result.as_ref().map(resp3_to_json);
        
        // Extract batch information
        let batch_id = optimized.batch_id.clone();
        let sequence = optimized.sequence;
        
        Response {
            id,
            status,
            result,
            error: optimized.error.clone(),
            timestamp,
            batch_id,
            sequence
        }
    }
}

/// Convert JSON Value to RESP3Value
fn json_to_resp3(value: &Value) -> Resp3Value {
    match value {
        Value::Null => Resp3Value::Null,
        Value::Bool(b) => Resp3Value::Boolean(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Resp3Value::Integer(i)
            } else if let Some(f) = n.as_f64() {
                Resp3Value::Double(f)
            } else {
                Resp3Value::Null
            }
        },
        Value::String(s) => Resp3Value::String(s.clone()),
        Value::Array(arr) => {
            let values: Vec<Resp3Value> = arr.iter()
                .map(json_to_resp3)
                .collect();
            Resp3Value::Array(values)
        },
        Value::Object(obj) => {
            let mut map = HashMap::new();
            for (k, v) in obj {
                map.insert(k.clone(), json_to_resp3(v));
            }
            Resp3Value::Map(map)
        },
    }
}

/// Convert RESP3Value to JSON Value
fn resp3_to_json(value: &Resp3Value) -> Value {
    match value {
        Resp3Value::Null => Value::Null,
        Resp3Value::Boolean(b) => Value::Bool(*b),
        Resp3Value::Integer(i) => Value::Number((*i).into()),
        Resp3Value::Double(f) => {
            // P3AF002 fix: handle NaN/Infinity explicitly instead of silent Null
            if f.is_nan() {
                Value::String("NaN".to_string())
            } else if f.is_infinite() {
                Value::String(if *f > 0.0 { "Infinity" } else { "-Infinity" }.to_string())
            } else if let Some(n) = serde_json::Number::from_f64(*f) {
                Value::Number(n)
            } else {
                // Fallback for any other edge cases (shouldn't happen)
                Value::String(f.to_string())
            }
        },
        Resp3Value::String(s) => Value::String(s.clone()),
        Resp3Value::Array(arr) => {
            let values: Vec<Value> = arr.iter()
                .map(resp3_to_json)
                .collect();
            Value::Array(values)
        },
        Resp3Value::Map(map) => {
            let mut obj = Map::new();
            for (k, v) in map {
                obj.insert(k.clone(), resp3_to_json(v));
            }
            Value::Object(obj)
        },
    }
}

/// Convert RESP3Value to JSON string
fn resp3_to_json_string(value: &Resp3Value) -> String {
    let json_value = resp3_to_json(value);
    serde_json::to_string(&json_value).unwrap_or_else(|_| "null".to_string())
}

/// Parse JSON string to Value with error handling
fn parse_json(json_str: &str) -> Value {
    match serde_json::from_str(json_str) {
        Ok(value) => value,
        Err(e) => {
            warn!("Failed to parse JSON: {}", e);
            Value::Null
        }
    }
}

/// Shorten command name according to mapping
fn shorten_command_name(command: &str) -> String {
    match command {
        "geometric_discover" => "geo_disc".to_string(),
        "geometric_store_topology" => "geo_store".to_string(),
        "geometric_load_sequence" => "geo_seq".to_string(),
        "geometric_distance" => "geo_dist".to_string(),
        "geometric_dimensions" => "geo_dim".to_string(),
        "stream_info" => "str_info".to_string(),
        "stream_group_info" => "str_group".to_string(),
        "stream_consumer_info" => "str_cons".to_string(),
        "stream_pending" => "str_pend".to_string(),
        "get_node_info" => "node_info".to_string(),
        "get_site_info" => "site_info".to_string(),
        _ => command.to_string(),
    }
}

/// Expand shortened command name
fn expand_command_name(short: &str) -> String {
    match short {
        "geo_disc" => "geometric_discover".to_string(),
        "geo_store" => "geometric_store_topology".to_string(),
        "geo_seq" => "geometric_load_sequence".to_string(),
        "geo_dist" => "geometric_distance".to_string(),
        "geo_dim" => "geometric_dimensions".to_string(),
        "str_info" => "stream_info".to_string(),
        "str_group" => "stream_group_info".to_string(),
        "str_cons" => "stream_consumer_info".to_string(),
        "str_pend" => "stream_pending".to_string(),
        "node_info" => "get_node_info".to_string(),
        "site_info" => "get_site_info".to_string(),
        _ => short.to_string(),
    }
}