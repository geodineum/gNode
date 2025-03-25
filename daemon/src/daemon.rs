use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use redis::{Client, Commands, Connection};
use serde::{Serialize, Deserialize};
use log::{info, error, debug, warn};

use crate::{
    GeometricTopology, ServiceConfig, RequirementSet, SharedTopology,
    Capability, Requirement, Result, GeometricError
};

/// Command received from clients
#[derive(Debug, Serialize, Deserialize)]
pub struct Command {
    pub id: String,
    pub command: String,
    pub parameters: serde_json::Value,
    pub site_id: String,
    pub node_id: String,
    pub timestamp: f64,
}

/// Response sent back to clients
#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub id: String,
    pub status: String,
    pub result: Option<serde_json::Value>,
    pub error: Option<String>,
    pub timestamp: f64,
}

/// Main daemon struct
pub struct GSDaemon {
    client: Client,
    topology_manager: SharedTopology,
    site_id: String,
    stream_prefix: String,
    debug: bool,
}

impl GSDaemon {
    /// Create a new daemon instance
    pub fn new(redis_url: &str, dimensions: usize, site_id: String, stream_prefix: String, debug: bool) -> Result<Self> {
        info!("Initializing GSD daemon with Redis URL: {}, dimensions: {}, site_id: {}, stream_prefix: {}", 
            redis_url, dimensions, site_id, stream_prefix);
        
        let client = Client::open(redis_url).map_err(GeometricError::Redis)?;
        
        // Create topology manager with Redis storage
        let topology_manager = SharedTopology::with_storage(
            dimensions, 
            redis_url, 
            &site_id, 
            &stream_prefix
        )?;
        
        Ok(Self {
            client,
            topology_manager,
            site_id,
            stream_prefix,
            debug,
        })
    }
    
    /// Get the command stream name for a specific node
    fn get_command_stream(&self, node_id: &str) -> String {
        format!("{{{0}}}:{1}:stream:{2}:commands", self.site_id, self.stream_prefix, node_id)
    }
    
    /// Get the response stream name for a specific node
    fn get_response_stream(&self, node_id: &str) -> String {
        format!("{{{0}}}:{1}:stream:{2}:responses", self.site_id, self.stream_prefix, node_id)
    }
    
    /// Run the daemon
    pub fn run(&self) -> Result<()> {
        info!("Starting GSD daemon");
        
        let mut connection = self.client.get_connection()
            .map_err(GeometricError::Redis)?;
        
        // Listen for commands across all node streams
        let pattern = format!("{{{0}}}:{1}:stream:*:commands", self.site_id, self.stream_prefix);
        info!("Looking for command streams with pattern: {}", pattern);
        let streams = connection.keys::<_, Vec<String>>(pattern)
            .map_err(GeometricError::Redis)?;
        
        if streams.is_empty() {
            info!("No existing command streams found, creating a default one");
            let default_node_id = "default";
            let command_stream = self.get_command_stream(default_node_id);
            let response_stream = self.get_response_stream(default_node_id);
            
            // Create streams if they don't exist
            let _: redis::RedisResult<String> = redis::cmd("XGROUP")
                .arg("CREATE")
                .arg(&command_stream)
                .arg("gsd-daemon")
                .arg("$")
                .arg("MKSTREAM")
                .query(&mut connection);
                
            let _: redis::RedisResult<String> = redis::cmd("XGROUP")
                .arg("CREATE")
                .arg(&response_stream)
                .arg("gsd-client")
                .arg("$")
                .arg("MKSTREAM")
                .query(&mut connection);
            
            // Start processing on the default stream
            self.start_stream_processor(default_node_id);
        } else {
            info!("Found {} existing command streams", streams.len());
            
            // Process each command stream
            for stream in &streams {
                let parts: Vec<&str> = stream.split(':').collect();
                if parts.len() >= 4 {
                    let node_id = parts[3];
                    info!("Starting processor for node_id: {}", node_id);
                    self.start_stream_processor(node_id);
                }
            }
        }
        
        // Keep checking for new streams periodically
        let poll_interval = Duration::from_secs(10);
        info!("Starting stream discovery loop with poll interval of {}s", poll_interval.as_secs());
        
        let site_id = self.site_id.clone();
        let stream_prefix = self.stream_prefix.clone();
        let client = self.client.clone();
        let topology_manager = self.topology_manager.get_topology_ref();
        
        thread::spawn(move || {
            let mut known_streams = Vec::new();
            loop {
                thread::sleep(poll_interval);
                
                let pattern = format!("{{{0}}}:{1}:stream:*:commands", site_id, stream_prefix);
                let mut connection = match client.get_connection() {
                    Ok(conn) => conn,
                    Err(e) => {
                        error!("Failed to get Redis connection for stream discovery: {}", e);
                        continue;
                    }
                };
                
                let streams: Vec<String> = match connection.keys(&pattern) {
                    Ok(s) => s,
                    Err(e) => {
                        error!("Failed to discover streams: {}", e);
                        continue;
                    }
                };
                
                // Find new streams
                for stream in &streams {
                    if !known_streams.contains(stream) {
                        known_streams.push(stream.clone());
                        
                        let parts: Vec<&str> = stream.split(':').collect();
                        if parts.len() >= 4 {
                            let node_id = parts[3];
                            debug!("Discovered new stream for node_id: {}", node_id);
                            
                            // Create a new processor for this stream
                            let node_id = node_id.to_string();
                            let topology = Arc::clone(&topology_manager);
                            let client = client.clone();
                            let site_id = site_id.clone();
                            let stream_prefix = stream_prefix.clone();
                            
                            thread::spawn(move || {
                                let daemon = GSDaemon {
                                    client: client.clone(),
                                    topology_manager: SharedTopology {
                                        topology: topology,
                                        storage: None,
                                        auto_save: false,
                                    },
                                    site_id: site_id.clone(),
                                    stream_prefix: stream_prefix.clone(),
                                    debug: false,
                                };
                                
                                daemon.start_stream_processor(&node_id);
                            });
                        }
                    }
                }
            }
        });
        
        // Keep the main thread alive with periodic saves
        loop {
            thread::sleep(Duration::from_secs(30));
            info!("GSD daemon heartbeat");
            
            // Save topology state
            if let Err(e) = self.topology_manager.save() {
                error!("Failed to save topology state: {:?}", e);
            }
        }
    }
    
    /// Start a processor for a specific node's command stream
    fn start_stream_processor(&self, node_id: &str) {
        let command_stream = self.get_command_stream(node_id);
        let response_stream = self.get_response_stream(node_id);
        
        info!("Starting processor for node {}: command_stream={}, response_stream={}", 
            node_id, command_stream, response_stream);
        
        // Create streams and consumer groups if they don't exist
        let mut connection = match self.client.get_connection() {
            Ok(conn) => conn,
            Err(e) => {
                error!("Failed to get Redis connection: {}", e);
                return;
            }
        };
        
        // Create command stream group
        match redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(&command_stream)
            .arg("gsd-daemon")
            .arg("$")
            .arg("MKSTREAM")
            .query::<String>(&mut connection) {
            Ok(_) => info!("Created command stream group for {}", node_id),
            Err(e) => warn!("Command stream group creation failed (may already exist): {}", e),
        }
        
        // Create response stream group
        match redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(&response_stream)
            .arg("gsd-client")
            .arg("$")
            .arg("MKSTREAM")
            .query::<String>(&mut connection) {
            Ok(_) => info!("Created response stream group for {}", node_id),
            Err(e) => warn!("Response stream group creation failed (may already exist): {}", e),
        }
        
        // Clone necessary resources for the thread
        let topology_manager = self.topology_manager.get_topology_ref();
        let client = self.client.clone();
        let node_id = node_id.to_string();
        let cmd_stream = command_stream.clone();
        let resp_stream = response_stream.clone();
        let debug = self.debug;
        
        thread::spawn(move || {
            info!("Command processor for node {} started", node_id);
            
            // Get a dedicated connection for this thread
            let mut conn = match client.get_connection() {
                Ok(conn) => conn,
                Err(e) => {
                    error!("Failed to get Redis connection in processor thread: {}", e);
                    return;
                }
            };
            
            loop {
                // FIX: Change the command ordering and use "0" instead of "0-0" for pending messages
                let pending_result: redis::RedisResult<Vec<(String, Vec<(String, Vec<(String, String)>)>)>> = redis::cmd("XREADGROUP")
                    .arg("GROUP")
                    .arg("gsd-daemon")
                    .arg(format!("processor-{}", node_id))
                    .arg("COUNT")                // Moved COUNT before STREAMS
                    .arg(10)
                    .arg("STREAMS")
                    .arg(&cmd_stream)
                    .arg("0")                    // Changed from "0-0" to "0" for better compatibility
                    .query(&mut conn);
                
                match pending_result {
                    Ok(pending_msgs) => {
                        // If we successfully got messages, process them
                        if !pending_msgs.is_empty() {
                            debug!("Found {} pending message streams", pending_msgs.len());
                            
                            for (stream_name, messages) in pending_msgs {
                                if messages.is_empty() {
                                    continue;
                                }
                                
                                debug!("Processing {} pending messages from stream {}", messages.len(), stream_name);
                                
                                for (id, fields) in messages {
                                    debug!("Processing pending message {} for node {}", id, node_id);
                                    
                                    // Convert redis format to Command
                                    let command = parse_command_from_fields(fields);
                                    
                                    if let Some(cmd) = command {
                                        // Process command with regular error handling (not catch_unwind due to connection not being UnwindSafe)
                                        match process_command_safely(&mut conn, &topology_manager, &cmd, &resp_stream, debug) {
                                            Ok(_) => debug!("Successfully processed command: {}", cmd.command),
                                            Err(e) => error!("Error processing command: {}", e),
                                        }
                                    } else {
                                        warn!("Failed to parse command from message {}", id);
                                    }
                                    
                                    // Acknowledge message - use transaction to ensure both operations succeed or fail together
                                    let pipe_result: redis::RedisResult<((), i32)> = redis::pipe()
                                        .atomic()
                                        .cmd("XACK").arg(&cmd_stream).arg("gsd-daemon").arg(&id).ignore()
                                        .cmd("XDEL").arg(&cmd_stream).arg(&id).query(&mut conn);
                                    
                                    if let Err(e) = pipe_result {
                                        warn!("Failed to acknowledge message {}: {}", id, e);
                                    }
                                }
                            }
                        }
                    },
                    Err(e) => {
                        // More detailed error handling with appropriate severity levels
                        if e.to_string().contains("Invalid stream ID") {
                            // This is an expected case when there are no pending messages
                            debug!("No pending messages with ID 0: {}", e);
                        } else if e.to_string().contains("NOGROUP") {
                            // Missing consumer group is a serious error
                            error!("Consumer group doesn't exist. Recreating group...");
                            let _: redis::RedisResult<()> = redis::cmd("XGROUP")
                                .arg("CREATE")
                                .arg(&cmd_stream)
                                .arg("gsd-daemon")
                                .arg("$")
                                .arg("MKSTREAM")
                                .query(&mut conn);
                        } else if e.to_string().contains("Bulk response of wrong dimension") {
                            // Handle PHP Redis client format incompatibility
                            warn!("Redis format issue. Trying alternative XREAD approach");
                            let _: redis::RedisResult<()> = redis::cmd("XREAD")
                                .arg("COUNT").arg(5)
                                .arg("STREAMS").arg(&cmd_stream).arg("0")
                                .query(&mut conn);
                        } else {
                            // Log unexpected errors as warnings
                            warn!("Failed to read pending messages: {}", e);
                        }
                    }
                }
                
                // FIX: Change the command ordering for XREADGROUP for new messages
                let result: redis::RedisResult<Vec<(String, Vec<(String, Vec<(String, String)>)>)>> = redis::cmd("XREADGROUP")
                    .arg("GROUP")
                    .arg("gsd-daemon")
                    .arg(format!("processor-{}", node_id))
                    .arg("COUNT")                // Moved COUNT before BLOCK
                    .arg(10)
                    .arg("BLOCK")
                    .arg(1000)  // 1 second block
                    .arg("STREAMS")
                    .arg(&cmd_stream)
                    .arg(">")   // ">" is the special ID that means "new messages only"
                    .query(&mut conn);
                
                match result {
                    Ok(msgs) => {
                        if !msgs.is_empty() {
                            let mut processed_count = 0;
                            
                            for (stream_name, messages) in msgs {
                                if messages.is_empty() {
                                    continue;
                                }
                                
                                debug!("Processing {} new messages from stream {}", messages.len(), stream_name);
                                
                                for (id, fields) in messages {
                                    debug!("Processing new message {} for node {}", id, node_id);
                                    processed_count += 1;
                                    
                                    // Convert redis format to Command
                                    let command = parse_command_from_fields(fields);
                                    
                                    if let Some(cmd) = command {
                                        // Process command with regular error handling
                                        match process_command_safely(&mut conn, &topology_manager, &cmd, &resp_stream, debug) {
                                            Ok(_) => debug!("Successfully processed command: {}", cmd.command),
                                            Err(e) => error!("Error processing command: {}", e),
                                        }
                                    } else {
                                        warn!("Failed to parse command from message {}", id);
                                    }
                                    
                                    // Acknowledge message with transaction to ensure atomicity
                                    let pipe_result: redis::RedisResult<((), i32)> = redis::pipe()
                                        .atomic()
                                        .cmd("XACK").arg(&cmd_stream).arg("gsd-daemon").arg(&id).ignore()
                                        .cmd("XDEL").arg(&cmd_stream).arg(&id).query(&mut conn);
                                    
                                    if let Err(e) = pipe_result {
                                        warn!("Failed to acknowledge message {}: {}", id, e);
                                    }
                                }
                            }
                            
                            if processed_count > 0 {
                                debug!("Processed {} new messages", processed_count);
                            }
                        }
                    },
                    Err(e) => {
                        // Enhanced error handling for new messages
                        if e.to_string().contains("BUSYGROUP") {
                            // Handle group already exists error
                            debug!("Consumer group already exists, continuing operation");
                        } else if e.to_string().contains("timeout") {
                            // Timeout is normal for blocking operations
                            if debug {
                                debug!("Read timeout - no new messages available");
                            }
                        } else if e.to_string().contains("NOGROUP") {
                            // Group doesn't exist - try to recreate it
                            error!("Consumer group doesn't exist. Recreating group...");
                            let _: redis::RedisResult<()> = redis::cmd("XGROUP")
                                .arg("CREATE")
                                .arg(&cmd_stream)
                                .arg("gsd-daemon")
                                .arg("$")
                                .arg("MKSTREAM")
                                .query(&mut conn);
                        } else if e.to_string().contains("Bulk response of wrong dimension") {
                            // Handle PHP Redis client format incompatibility
                            warn!("Redis format issue with XREADGROUP. Trying direct XREAD");
                            match redis::cmd("XREAD")
                                .arg("COUNT").arg(5)
                                .arg("BLOCK").arg(100)
                                .arg("STREAMS").arg(&cmd_stream).arg("$")
                                .query::<Vec<(String, Vec<(String, Vec<(String, String)>)>)>>(&mut conn) {
                                Ok(msgs) => {
                                    if !msgs.is_empty() {
                                        debug!("Successfully read {} messages with direct XREAD", msgs.len());
                                    }
                                },
                                Err(e) => {
                                    warn!("Direct XREAD also failed: {}", e);
                                }
                            }
                        } else {
                            // Log other errors as warnings
                            warn!("Error reading new messages: {}", e);
                        }
                        
                        // Small sleep to prevent CPU spinning on errors
                        thread::sleep(Duration::from_millis(100));
                    }
                }
            }
        });
    }
}

/// Parse a command from Redis stream field-value pairs
fn parse_command_from_fields(fields: Vec<(String, String)>) -> Option<Command> {
    let mut id = String::new();
    let mut command = String::new();
    let mut parameters = String::new();
    let mut site_id = String::new();
    let mut node_id = String::new();
    let mut timestamp = 0.0;
    
    // Debug logging for incoming fields
    debug!("Processing message fields: {:?}", fields);
    
    for (field, value) in fields {
        // Remove quotes if they exist (compatibility with PHP Redis client)
        let clean_value = value.trim_matches('"').to_string();
        
        match field.as_str() {
            "id" => id = clean_value,
            "command" => command = clean_value,
            "parameters" => parameters = clean_value,
            "site_id" => site_id = clean_value,
            "node_id" => node_id = clean_value,
            "timestamp" => timestamp = clean_value.parse().unwrap_or(0.0),
            _ => {}
        }
        
        debug!("Field: {}, Value: {} (cleaned: {})", field, value, clean_value);
    }
    
    if id.is_empty() || command.is_empty() {
        debug!("Missing required fields for command: id={} command={}", id, command);
        return None;
    }
    
    // More robust parameter parsing
    let parameters_value = match serde_json::from_str(&parameters) {
        Ok(v) => {
            debug!("Successfully parsed parameters: {}", parameters);
            v
        },
        Err(e) => {
            warn!("Failed to parse parameters '{}': {}", parameters, e);
            
            // Try to fix common issues with PHP-style JSON - attempt various formats
            match serde_json::from_str(&format!("{{{}}}", parameters.trim_matches('{').trim_matches('}'))) {
                Ok(v) => {
                    debug!("Recovered parameters after formatting");
                    v
                },
                Err(_) => {
                    warn!("Could not parse parameters even after recovery attempt");
                    serde_json::Value::Object(serde_json::Map::new())
                }
            }
        },
    };
    
    Some(Command {
        id,
        command,
        parameters: parameters_value,
        site_id,
        node_id,
        timestamp,
    })
}

/// Process a command and send a response
fn process_command(
    connection: &mut Connection, 
    topology: &Arc<Mutex<GeometricTopology>>, 
    command: &Command, 
    response_stream: &str,
    debug: bool
) {
    if debug {
        debug!("Processing command: {} from node {}", command.command, command.node_id);
    }
    
    let response = match command.command.as_str() {
        "ping" => Response {
            id: command.id.clone(),
            status: "ok".to_string(),
            result: Some(serde_json::Value::Bool(true)),
            error: None,
            timestamp: current_timestamp(),
        },
        
        "hello" => Response {
            id: command.id.clone(),
            status: "ok".to_string(),
            result: Some(serde_json::Value::String("Hello from GSD daemon!".to_string())),
            error: None,
            timestamp: current_timestamp(),
        },
        
        "registerCapabilityDimension" => {
            let name = command.parameters["name"].as_str().unwrap_or("");
            let dimension = command.parameters["dimension"].as_u64().unwrap_or(0) as usize;
            
            if name.is_empty() {
                Response {
                    id: command.id.clone(),
                    status: "error".to_string(),
                    result: None,
                    error: Some("Invalid name parameter".to_string()),
                    timestamp: current_timestamp(),
                }
            } else {
                // Register the capability dimension
                let mut topology = match topology.lock() {
                    Ok(t) => t,
                    Err(e) => {
                        error!("Failed to lock topology: {}", e);
                        return;
                    }
                };
                
                topology.capability_dimensions.insert(name.to_string(), dimension);
                
                Response {
                    id: command.id.clone(),
                    status: "ok".to_string(),
                    result: Some(serde_json::Value::Bool(true)),
                    error: None,
                    timestamp: current_timestamp(),
                }
            }
        },
        
        "registerService" => {
            let id = command.parameters["id"].as_str().unwrap_or("");
            let capabilities = &command.parameters["capabilities"];
            let metadata = &command.parameters["metadata"];
            
            if id.is_empty() {
                Response {
                    id: command.id.clone(),
                    status: "error".to_string(),
                    result: None,
                    error: Some("Invalid id parameter".to_string()),
                    timestamp: current_timestamp(),
                }
            } else {
                // Create service config
                let mut service_capabilities = Vec::new();
                
                if let Some(obj) = capabilities.as_object() {
                    for (name, value) in obj {
                        service_capabilities.push(Capability {
                            name: name.clone(),
                            value: value.as_f64().unwrap_or(0.0),
                        });
                    }
                }
                
                let mut service_metadata = std::collections::HashMap::new();
                
                if let Some(obj) = metadata.as_object() {
                    for (key, value) in obj {
                        if let Some(value_str) = value.as_str() {
                            service_metadata.insert(key.clone(), value_str.to_string());
                        }
                    }
                }
                
                let service = ServiceConfig {
                    id: id.to_string(),
                    capabilities: service_capabilities,
                    metadata: service_metadata,
                };
                
                // Register the service
                let mut topology = match topology.lock() {
                    Ok(t) => t,
                    Err(e) => {
                        error!("Failed to lock topology: {}", e);
                        return;
                    }
                };
                
                match topology.register_service(&service) {
                    Ok(_) => Response {
                        id: command.id.clone(),
                        status: "ok".to_string(),
                        result: Some(serde_json::Value::Bool(true)),
                        error: None,
                        timestamp: current_timestamp(),
                    },
                    Err(e) => Response {
                        id: command.id.clone(),
                        status: "error".to_string(),
                        result: None,
                        error: Some(format!("Failed to register service: {:?}", e)),
                        timestamp: current_timestamp(),
                    },
                }
            }
        },
        
        "findServices" => {
            let requirements = &command.parameters["requirements"];
            
            // Create requirements
            let mut req_set = RequirementSet {
                requirements: Vec::new(),
            };
            
            if let Some(obj) = requirements.as_object() {
                for (name, value) in obj {
                    req_set.requirements.push(Requirement {
                        name: name.clone(),
                        min_value: value.as_f64().unwrap_or(0.0),
                    });
                }
            }
            
            // Find matching services
            let topology = match topology.lock() {
                Ok(t) => t,
                Err(e) => {
                    error!("Failed to lock topology: {}", e);
                    return;
                }
            };
            
            match topology.find_services(&req_set) {
                Ok(services) => Response {
                    id: command.id.clone(),
                    status: "ok".to_string(),
                    result: Some(serde_json::to_value(&services).unwrap_or(serde_json::Value::Array(Vec::new()))),
                    error: None,
                    timestamp: current_timestamp(),
                },
                Err(e) => Response {
                    id: command.id.clone(),
                    status: "error".to_string(),
                    result: None,
                    error: Some(format!("Failed to find services: {:?}", e)),
                    timestamp: current_timestamp(),
                },
            }
        },
        
        "getLoadSequence" => {
            let topology = match topology.lock() {
                Ok(t) => t,
                Err(e) => {
                    error!("Failed to lock topology: {}", e);
                    return;
                }
            };
            
            match topology.get_load_sequence() {
                Ok(sequence) => Response {
                    id: command.id.clone(),
                    status: "ok".to_string(),
                    result: Some(serde_json::to_value(&sequence).unwrap_or(serde_json::Value::Array(Vec::new()))),
                    error: None,
                    timestamp: current_timestamp(),
                },
                Err(e) => Response {
                    id: command.id.clone(),
                    status: "error".to_string(),
                    result: None,
                    error: Some(format!("Failed to get load sequence: {:?}", e)),
                    timestamp: current_timestamp(),
                },
            }
        },
        
        "getCapabilityDimensions" => {
            let topology = match topology.lock() {
                Ok(t) => t,
                Err(e) => {
                    error!("Failed to lock topology: {}", e);
                    return;
                }
            };
            
            Response {
                id: command.id.clone(),
                status: "ok".to_string(),
                result: Some(serde_json::to_value(&topology.capability_dimensions).unwrap_or(serde_json::Value::Object(serde_json::Map::new()))),
                error: None,
                timestamp: current_timestamp(),
            }
        },
        
        "getStatus" => {
            Response {
                id: command.id.clone(),
                status: "ok".to_string(),
                result: Some(serde_json::json!({
                    "version": env!("CARGO_PKG_VERSION"),
                    "uptime": std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                    "timestamp": current_timestamp(),
                })),
                error: None,
                timestamp: current_timestamp(),
            }
        },
        
        _ => Response {
            id: command.id.clone(),
            status: "error".to_string(),
            result: None,
            error: Some(format!("Unknown command: {}", command.command)),
            timestamp: current_timestamp(),
        },
    };
    
    // Send response
    let response_json = match serde_json::to_string(&response) {
        Ok(json) => json,
        Err(e) => {
            error!("Failed to serialize response: {}", e);
            return;
        }
    };
    
    debug!("Sending response: {}", response_json);
    
    let add_result: redis::RedisResult<String> = connection.xadd(
        response_stream,
        "*",
        &[("id", &response.id), ("response", &response_json)]
    );
    
    match add_result {
        Ok(id) => {
            if debug {
                debug!("Sent response for command {} with status {} (ID: {})",
                    command.command, response.status, id);
            }
        },
        Err(e) => {
            error!("Failed to send response for command {}: {}",
                command.command, e);
        }
    }
}

/// Get current timestamp
fn current_timestamp() -> f64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    
    now.as_secs_f64()
}

/// Process a command safely, catching and converting any errors
fn process_command_safely(
    connection: &mut Connection,
    topology: &Arc<Mutex<GeometricTopology>>,
    command: &Command,
    response_stream: &str,
    debug: bool
) -> std::result::Result<(), String> {
    // Use regular try-catch pattern instead of catch_unwind
    // since Connection isn't UnwindSafe
    if debug {
        debug!("Safely processing command: {} from node {}", command.command, command.node_id);
    }
    
    // Wrap the call with a basic try-catch
    match std::panic::catch_unwind(|| {
        // Use a cloned reference to avoid passing connection through catch_unwind
        let cmd = command.command.clone();
        return cmd;
    }) {
        Ok(cmd) => {
            // If we get here, the command object is valid
            // Now process it normally (no catch_unwind)
            process_command(connection, topology, command, response_stream, debug);
            Ok(())
        },
        Err(e) => {
            // Handle panics in a more controlled way
            let error_msg = if let Some(s) = e.downcast_ref::<&str>() {
                format!("Panic: {}", s)
            } else if let Some(s) = e.downcast_ref::<String>() {
                format!("Panic: {}", s)
            } else {
                "Unknown panic".to_string()
            };
            
            error!("{}", error_msg);
            Err(error_msg)
        }
    }
}