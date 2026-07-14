// Template Engine Module for gNode
//
// This module provides the template engine for gNode, which applies templates
// to messages and enables extensible message formats.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use log::{info, warn};
use serde_json::{Value, Map, json};

use super::template_manager::{TemplateManager, TemplateError};

/// Template engine for gNode
pub struct TemplateEngine {
    /// Template manager
    template_manager: Arc<TemplateManager>,
    
    /// Command schemas cache
    command_schemas: Arc<RwLock<HashMap<String, Value>>>,
    
    /// RESP3 mappings cache
    resp3_mappings: Arc<RwLock<HashMap<String, Value>>>,
}

/// Template engine error type
#[derive(Debug)]
pub enum TemplateEngineError {
    /// Template error
    Template(TemplateError),
    /// Validation error
    Validation(String),
    /// Conversion error
    Conversion(String),
}

impl From<TemplateError> for TemplateEngineError {
    fn from(err: TemplateError) -> Self {
        TemplateEngineError::Template(err)
    }
}

impl std::fmt::Display for TemplateEngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TemplateEngineError::Template(err) => write!(f, "Template error: {}", err),
            TemplateEngineError::Validation(msg) => write!(f, "Validation error: {}", msg),
            TemplateEngineError::Conversion(msg) => write!(f, "Conversion error: {}", msg),
        }
    }
}

impl TemplateEngine {
    /// Create a new template engine
    pub fn new(template_manager: Arc<TemplateManager>) -> Self {
        Self {
            template_manager,
            command_schemas: Arc::new(RwLock::new(HashMap::new())),
            resp3_mappings: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    /// Initialize the template engine
    pub fn initialize(&self) -> Result<(), TemplateEngineError> {
        info!("Initializing template engine");
        
        // Cache command schemas
        match self.template_manager.get_json_schema("command_schema") {
            Ok(schema) => {
                if let Ok(mut guard) = self.command_schemas.write() {
                    guard.insert("command".to_string(), schema);
                } else {
                    warn!("Failed to acquire write lock for command_schemas");
                }
            },
            Err(e) => {
                warn!("Command schema not found: {}", e);
            }
        }

        match self.template_manager.get_json_schema("response_schema") {
            Ok(schema) => {
                if let Ok(mut guard) = self.command_schemas.write() {
                    guard.insert("response".to_string(), schema);
                } else {
                    warn!("Failed to acquire write lock for command_schemas");
                }
            },
            Err(e) => {
                warn!("Response schema not found: {}", e);
            }
        }

        // Cache RESP3 mappings
        match self.template_manager.get_resp3_mapping("field_mapping") {
            Ok(mapping) => {
                if let Ok(mut guard) = self.resp3_mappings.write() {
                    guard.insert("field_mapping".to_string(), mapping);
                } else {
                    warn!("Failed to acquire write lock for resp3_mappings");
                }
            },
            Err(e) => {
                warn!("Field mapping not found: {}", e);
            }
        }

        match self.template_manager.get_resp3_mapping("redis_mapping") {
            Ok(mapping) => {
                if let Ok(mut guard) = self.resp3_mappings.write() {
                    guard.insert("redis_mapping".to_string(), mapping);
                } else {
                    warn!("Failed to acquire write lock for resp3_mappings");
                }
            },
            Err(e) => {
                warn!("Redis mapping not found: {}", e);
            }
        }

        info!("Template engine initialized with {} command schemas and {} RESP3 mappings",
            self.command_schemas.read().map(|g| g.len()).unwrap_or(0),
            self.resp3_mappings.read().map(|g| g.len()).unwrap_or(0));
        
        Ok(())
    }
    
    /// Validate a command against the command schema
    pub fn validate_command(&self, command: &Value) -> Result<(), TemplateEngineError> {
        match self.template_manager.validate(command, "command_schema") {
            Ok(_) => Ok(()),
            Err(e) => Err(TemplateEngineError::Validation(format!("Command validation failed: {}", e))),
        }
    }
    
    /// Validate a response against the response schema
    pub fn validate_response(&self, response: &Value) -> Result<(), TemplateEngineError> {
        match self.template_manager.validate(response, "response_schema") {
            Ok(_) => Ok(()),
            Err(e) => Err(TemplateEngineError::Validation(format!("Response validation failed: {}", e))),
        }
    }
    
    /// Convert a command to optimized RESP3 format
    pub fn command_to_resp3(&self, command: &Value) -> Result<HashMap<String, String>, TemplateEngineError> {
        // Validate command
        self.validate_command(command)?;

        // Get field mapping
        let mappings = self.resp3_mappings.read()
            .map_err(|e| TemplateEngineError::Conversion(format!("Lock poisoned: {}", e)))?;
        let field_mapping = mappings.get("field_mapping")
            .ok_or_else(|| TemplateEngineError::Conversion("Field mapping not found".to_string()))?;

        // Get common fields
        let common_fields = field_mapping.get("commonFields")
            .and_then(|v| v.as_object())
            .ok_or_else(|| TemplateEngineError::Conversion("Common fields not found or invalid in mapping".to_string()))?;

        // Get type values
        let type_values = field_mapping.get("typeValues")
            .and_then(|v| v.as_object())
            .ok_or_else(|| TemplateEngineError::Conversion("Type values not found or invalid in mapping".to_string()))?;

        // Get command short names
        let command_short_names = field_mapping.get("commandShortNames")
            .and_then(|v| v.as_object())
            .ok_or_else(|| TemplateEngineError::Conversion("Command short names not found or invalid in mapping".to_string()))?;

        // Helper to get string field from mapping (returns default if missing)
        let get_field = |obj: &Map<String, Value>, key: &str| -> String {
            obj.get(key).and_then(|v| v.as_str()).unwrap_or("_unknown").to_string()
        };

        // Create RESP3 fields
        let mut resp3_fields = HashMap::new();

        // Set message type (command)
        let type_field = get_field(common_fields, "type");
        let cmd_type = type_values.get("command").and_then(|v| v.as_str()).unwrap_or("c");
        resp3_fields.insert(type_field, cmd_type.to_string());

        // Set ID
        if let Some(id) = command.get("id") {
            if let Some(id_str) = id.as_str() {
                resp3_fields.insert(get_field(common_fields, "id"), id_str.to_string());
            }
        }

        // Set command (shortened if possible)
        if let Some(cmd) = command.get("command") {
            if let Some(cmd_str) = cmd.as_str() {
                let short_name = command_short_names.get(cmd_str)
                    .and_then(|v| v.as_str())
                    .unwrap_or(cmd_str);
                resp3_fields.insert(get_field(common_fields, "command"), short_name.to_string());
            }
        }

        // Set parameters
        if let Some(params) = command.get("parameters") {
            resp3_fields.insert(get_field(common_fields, "parameters"), params.to_string());
        }

        // Set site_id
        if let Some(site_id) = command.get("site_id") {
            if let Some(site_id_str) = site_id.as_str() {
                resp3_fields.insert(get_field(common_fields, "site_id"), site_id_str.to_string());
            }
        }

        // Set node_id
        if let Some(node_id) = command.get("node_id") {
            if let Some(node_id_str) = node_id.as_str() {
                resp3_fields.insert(get_field(common_fields, "node_id"), node_id_str.to_string());
            }
        }

        // Set timestamp
        if let Some(timestamp) = command.get("timestamp") {
            if let Some(ts) = timestamp.as_f64() {
                resp3_fields.insert(get_field(common_fields, "timestamp"), format!("{}", (ts * 1000.0) as i64));
            }
        }

        // Set destination_site (default: gNode)
        resp3_fields.insert(get_field(common_fields, "destination_site"), "gNode".to_string());

        // Set destination_node (default: *)
        resp3_fields.insert(get_field(common_fields, "destination_node"), "*".to_string());

        // Set batch fields if present
        if let Some(batch_id) = command.get("batch_id") {
            if let Some(batch_id_str) = batch_id.as_str() {
                resp3_fields.insert(get_field(common_fields, "batch_id"), batch_id_str.to_string());
            }
        }

        if let Some(sequence) = command.get("sequence") {
            resp3_fields.insert(get_field(common_fields, "sequence"), sequence.to_string());
        }

        Ok(resp3_fields)
    }
    
    /// Convert a response to optimized RESP3 format
    pub fn response_to_resp3(&self, response: &Value, source_site: &str, source_node: &str, dest_site: &str, dest_node: &str) -> Result<HashMap<String, String>, TemplateEngineError> {
        // Validate response
        self.validate_response(response)?;

        // Get field mapping
        let mappings = self.resp3_mappings.read()
            .map_err(|e| TemplateEngineError::Conversion(format!("Lock poisoned: {}", e)))?;
        let field_mapping = mappings.get("field_mapping")
            .ok_or_else(|| TemplateEngineError::Conversion("Field mapping not found".to_string()))?;

        // Get common fields
        let common_fields = field_mapping.get("commonFields")
            .and_then(|v| v.as_object())
            .ok_or_else(|| TemplateEngineError::Conversion("Common fields not found or invalid in mapping".to_string()))?;

        // Get type values
        let type_values = field_mapping.get("typeValues")
            .and_then(|v| v.as_object())
            .ok_or_else(|| TemplateEngineError::Conversion("Type values not found or invalid in mapping".to_string()))?;

        // Helper to get string field from mapping (returns default if missing)
        let get_field = |obj: &Map<String, Value>, key: &str| -> String {
            obj.get(key).and_then(|v| v.as_str()).unwrap_or("_unknown").to_string()
        };

        // Create RESP3 fields
        let mut resp3_fields = HashMap::new();

        // Set message type (response)
        let type_field = get_field(common_fields, "type");
        let resp_type = type_values.get("response").and_then(|v| v.as_str()).unwrap_or("r");
        resp3_fields.insert(type_field, resp_type.to_string());

        // Set ID
        if let Some(id) = response.get("id") {
            if let Some(id_str) = id.as_str() {
                resp3_fields.insert(get_field(common_fields, "id"), id_str.to_string());
                // Also set request_id for correlation
                resp3_fields.insert(get_field(common_fields, "request_id"), id_str.to_string());
            }
        }

        // Set status
        if let Some(status) = response.get("status") {
            if let Some(status_str) = status.as_str() {
                resp3_fields.insert(get_field(common_fields, "status"), status_str.to_string());
            }
        }

        // Set result
        if let Some(result) = response.get("result") {
            resp3_fields.insert(get_field(common_fields, "result"), result.to_string());
        }

        // Set error
        if let Some(error) = response.get("error") {
            if let Some(error_str) = error.as_str() {
                resp3_fields.insert(get_field(common_fields, "error"), error_str.to_string());
            }
        }

        // Set source info
        resp3_fields.insert(get_field(common_fields, "site_id"), source_site.to_string());
        resp3_fields.insert(get_field(common_fields, "node_id"), source_node.to_string());

        // Set destination info
        resp3_fields.insert(get_field(common_fields, "destination_site"), dest_site.to_string());
        resp3_fields.insert(get_field(common_fields, "destination_node"), dest_node.to_string());

        // Set timestamp
        if let Some(timestamp) = response.get("timestamp") {
            if let Some(ts) = timestamp.as_f64() {
                resp3_fields.insert(get_field(common_fields, "timestamp"), format!("{}", (ts * 1000.0) as i64));
            }
        } else {
            // Use current timestamp
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64;
            resp3_fields.insert(get_field(common_fields, "timestamp"), now.to_string());
        }

        // Set batch fields if present
        if let Some(batch_id) = response.get("batch_id") {
            if let Some(batch_id_str) = batch_id.as_str() {
                resp3_fields.insert(get_field(common_fields, "batch_id"), batch_id_str.to_string());
            }
        }

        if let Some(sequence) = response.get("sequence") {
            resp3_fields.insert(get_field(common_fields, "sequence"), sequence.to_string());
        }

        Ok(resp3_fields)
    }
    
    /// Convert RESP3 fields to a command
    pub fn resp3_to_command(&self, fields: &HashMap<String, String>) -> Result<Value, TemplateEngineError> {
        // Get field mapping
        let mappings = self.resp3_mappings.read()
            .map_err(|e| TemplateEngineError::Conversion(format!("Lock poisoned: {}", e)))?;
        let field_mapping = mappings.get("field_mapping")
            .ok_or_else(|| TemplateEngineError::Conversion("Field mapping not found".to_string()))?;

        // Get common fields
        let common_fields = field_mapping.get("commonFields")
            .and_then(|v| v.as_object())
            .ok_or_else(|| TemplateEngineError::Conversion("Common fields not found or invalid in mapping".to_string()))?;

        // Get command short names (for reversal)
        let command_short_names = field_mapping.get("commandShortNames")
            .and_then(|v| v.as_object())
            .ok_or_else(|| TemplateEngineError::Conversion("Command short names not found or invalid in mapping".to_string()))?;

        // Helper to get string field from mapping (returns default if missing)
        let get_field_key = |key: &str| -> Option<&str> {
            common_fields.get(key).and_then(|v| v.as_str())
        };

        // Create reverse mapping for command names
        let mut short_to_full = HashMap::new();
        for (full, short_value) in command_short_names.iter() {
            if let Some(short) = short_value.as_str() {
                short_to_full.insert(short.to_string(), full.clone());
            }
        }

        // Create field name mapping
        let mut field_name_mapping = HashMap::new();
        for (name, short_value) in common_fields.iter() {
            if let Some(short) = short_value.as_str() {
                field_name_mapping.insert(short.to_string(), name.clone());
            }
        }

        // Create command object
        let mut command = Map::new();

        // Set ID
        if let Some(id_key) = get_field_key("id") {
            if let Some(id) = fields.get(id_key) {
                command.insert("id".to_string(), Value::String(id.clone()));
            }
        }

        // Set command (expanded if needed)
        if let Some(cmd_key) = get_field_key("command") {
            if let Some(cmd) = fields.get(cmd_key) {
                let full_name = short_to_full.get(cmd).cloned().unwrap_or_else(|| cmd.clone());
                command.insert("command".to_string(), Value::String(full_name));
            }
        }

        // Set parameters
        if let Some(params_key) = get_field_key("parameters") {
            if let Some(params_str) = fields.get(params_key) {
                match serde_json::from_str(params_str) {
                    Ok(params) => {
                        command.insert("parameters".to_string(), params);
                    },
                    Err(e) => {
                        return Err(TemplateEngineError::Conversion(format!("Failed to parse parameters JSON: {}", e)));
                    }
                }
            } else {
                // Default empty parameters
                command.insert("parameters".to_string(), Value::Object(Map::new()));
            }
        } else {
            // Default empty parameters
            command.insert("parameters".to_string(), Value::Object(Map::new()));
        }

        // Set site_id
        if let Some(site_key) = get_field_key("site_id") {
            if let Some(site_id) = fields.get(site_key) {
                command.insert("site_id".to_string(), Value::String(site_id.clone()));
            }
        }

        // Set node_id
        if let Some(node_key) = get_field_key("node_id") {
            if let Some(node_id) = fields.get(node_key) {
                command.insert("node_id".to_string(), Value::String(node_id.clone()));
            }
        }

        // Set timestamp
        if let Some(ts_key) = get_field_key("timestamp") {
            if let Some(ts_str) = fields.get(ts_key) {
                if let Ok(ts) = ts_str.parse::<i64>() {
                    // Convert millisecond timestamp to seconds
                    let timestamp = ts as f64 / 1000.0;

                    // Use string serialization to avoid precision issues
                    let timestamp_str = format!("{}", timestamp);
                    match timestamp_str.parse::<f64>() {
                        Ok(parsed_timestamp) => {
                            // Create json object with the number directly
                            command.insert("timestamp".to_string(), json!(parsed_timestamp));
                        },
                        Err(_) => {
                            // Fallback to integer timestamp
                            command.insert("timestamp".to_string(), Value::Number(serde_json::Number::from(0)));
                        }
                    }
                }
            }
        }

        // Set batch fields if present
        if let Some(batch_key) = get_field_key("batch_id") {
            if let Some(batch_id) = fields.get(batch_key) {
                command.insert("batch_id".to_string(), Value::String(batch_id.clone()));
            }
        }

        if let Some(seq_key) = get_field_key("sequence") {
            if let Some(sequence) = fields.get(seq_key) {
                if let Ok(seq) = sequence.parse::<i64>() {
                    command.insert("sequence".to_string(), Value::Number(serde_json::Number::from(seq)));
                }
            }
        }

        Ok(Value::Object(command))
    }
    
    /// Convert RESP3 fields to a response
    pub fn resp3_to_response(&self, fields: &HashMap<String, String>) -> Result<Value, TemplateEngineError> {
        // Get field mapping
        let mappings = self.resp3_mappings.read()
            .map_err(|e| TemplateEngineError::Conversion(format!("Lock poisoned: {}", e)))?;
        let field_mapping = mappings.get("field_mapping")
            .ok_or_else(|| TemplateEngineError::Conversion("Field mapping not found".to_string()))?;

        // Get common fields
        let common_fields = field_mapping.get("commonFields")
            .and_then(|v| v.as_object())
            .ok_or_else(|| TemplateEngineError::Conversion("Common fields not found or invalid in mapping".to_string()))?;

        // Helper to get string field from mapping (returns None if missing)
        let get_field_key = |key: &str| -> Option<&str> {
            common_fields.get(key).and_then(|v| v.as_str())
        };

        // Create field name mapping
        let mut field_name_mapping = HashMap::new();
        for (name, short_value) in common_fields.iter() {
            if let Some(short) = short_value.as_str() {
                field_name_mapping.insert(short.to_string(), name.clone());
            }
        }

        // Create response object
        let mut response = Map::new();

        // Set ID (from request_id if available, otherwise from id)
        let mut id_set = false;
        if let Some(req_key) = get_field_key("request_id") {
            if let Some(req_id) = fields.get(req_key) {
                response.insert("id".to_string(), Value::String(req_id.clone()));
                id_set = true;
            }
        }
        if !id_set {
            if let Some(id_key) = get_field_key("id") {
                if let Some(id) = fields.get(id_key) {
                    response.insert("id".to_string(), Value::String(id.clone()));
                }
            }
        }

        // Set status
        if let Some(status_key) = get_field_key("status") {
            if let Some(status) = fields.get(status_key) {
                response.insert("status".to_string(), Value::String(status.clone()));
            } else {
                // Default status
                response.insert("status".to_string(), Value::String("ok".to_string()));
            }
        } else {
            // Default status if key not found
            response.insert("status".to_string(), Value::String("ok".to_string()));
        }

        // Set result
        if let Some(result_key) = get_field_key("result") {
            if let Some(result_str) = fields.get(result_key) {
                match serde_json::from_str(result_str) {
                    Ok(result) => {
                        response.insert("result".to_string(), result);
                    },
                    Err(e) => {
                        warn!("Failed to parse result JSON: {}", e);
                        response.insert("result".to_string(), Value::Null);
                    }
                }
            }
        }

        // Set error
        if let Some(error_key) = get_field_key("error") {
            if let Some(error) = fields.get(error_key) {
                response.insert("error".to_string(), Value::String(error.clone()));
            }
        }

        // Set timestamp
        if let Some(ts_key) = get_field_key("timestamp") {
            if let Some(ts_str) = fields.get(ts_key) {
                if let Ok(ts) = ts_str.parse::<i64>() {
                    // Convert millisecond timestamp to seconds
                    let timestamp = ts as f64 / 1000.0;

                    // Use string serialization to avoid precision issues
                    let timestamp_str = format!("{}", timestamp);
                    match timestamp_str.parse::<f64>() {
                        Ok(parsed_timestamp) => {
                            // Create json object with the number directly
                            response.insert("timestamp".to_string(), json!(parsed_timestamp));
                        },
                        Err(_) => {
                            // Fallback to integer timestamp
                            response.insert("timestamp".to_string(), Value::Number(serde_json::Number::from(0)));
                        }
                    }
                }
            } else {
                // Use current timestamp
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs_f64();

                // Use json! macro which handles floating point values properly
                response.insert("timestamp".to_string(), json!(now));
            }
        } else {
            // Use current timestamp if key not found
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs_f64();
            response.insert("timestamp".to_string(), json!(now));
        }

        // Set batch fields if present
        if let Some(batch_key) = get_field_key("batch_id") {
            if let Some(batch_id) = fields.get(batch_key) {
                response.insert("batch_id".to_string(), Value::String(batch_id.clone()));
            }
        }

        if let Some(seq_key) = get_field_key("sequence") {
            if let Some(sequence) = fields.get(seq_key) {
                if let Ok(seq) = sequence.parse::<i64>() {
                    response.insert("sequence".to_string(), Value::Number(seq.into()));
                }
            }
        }

        Ok(Value::Object(response))
    }
}