// Format Serializer Module for gNode
//
// This module provides serialization capabilities for user-defined message formats.
// It supports serializing internal representations to various output formats.

use serde_json::Value;
use std::sync::Arc;

use super::format_registry::{FormatRegistry, RegistryError};
use super::format_schema::SchemaError;

/// Serializer error type
#[derive(Debug)]
pub enum SerializerError {
    /// Registry error
    Registry(RegistryError),
    /// Schema error
    Schema(SchemaError),
    /// Format not found
    FormatNotFound(String),
    /// Serialization error
    SerializationError(String),
}

impl std::fmt::Display for SerializerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SerializerError::Registry(e) => write!(f, "Registry error: {}", e),
            SerializerError::Schema(e) => write!(f, "Schema error: {}", e),
            SerializerError::FormatNotFound(e) => write!(f, "Format not found: {}", e),
            SerializerError::SerializationError(e) => write!(f, "Serialization error: {}", e),
        }
    }
}

impl From<RegistryError> for SerializerError {
    fn from(err: RegistryError) -> Self {
        SerializerError::Registry(err)
    }
}

impl From<SchemaError> for SerializerError {
    fn from(err: SchemaError) -> Self {
        SerializerError::Schema(err)
    }
}

/// Format serializer
pub struct FormatSerializer {
    /// Format registry
    registry: Arc<FormatRegistry>,
}

impl FormatSerializer {
    /// Create a new format serializer
    pub fn new(registry: Arc<FormatRegistry>) -> Self {
        Self {
            registry,
        }
    }
    
    /// Serialize a value to a specific format
    pub fn serialize(&self, value: &Value, format_name: &str, version: Option<&str>) -> Result<Vec<u8>, SerializerError> {
        // Get format
        let format = match self.registry.get_format(format_name, version) {
            Ok(f) => f,
            Err(e) => return Err(SerializerError::Registry(e)),
        };
        
        // Apply field mapping
        let transformed_value = format.apply_reverse_mapping(value)?;
        
        // Serialize based on content type
        let definition = format.get_definition();
        
        match definition.content_type.as_str() {
            "application/json" => {
                // Serialize to JSON
                match serde_json::to_vec(&transformed_value) {
                    Ok(data) => Ok(data),
                    Err(e) => Err(SerializerError::SerializationError(format!("JSON serialization error: {}", e))),
                }
            },
            "application/messagepack" | "application/msgpack" => {
                // Serialize to MessagePack
                match rmp_serde::to_vec(&transformed_value) {
                    Ok(data) => Ok(data),
                    Err(e) => Err(SerializerError::SerializationError(format!("MessagePack serialization error: {}", e))),
                }
            },
            "application/resp3" => {
                // Serialize to RESP3
                self.serialize_to_resp3(&transformed_value)
            },
            _ => {
                // Unknown content type
                Err(SerializerError::SerializationError(format!("Unsupported content type: {}", definition.content_type)))
            }
        }
    }
    
    /// Serialize a value to RESP3 format
    fn serialize_to_resp3(&self, value: &Value) -> Result<Vec<u8>, SerializerError> {
        // Implement RESP3 serialization
        if let Some(obj) = value.as_object() {
            let mut result = Vec::new();
            
            // Start with array header
            result.extend_from_slice(format!("*{}\r\n", obj.len() * 2).as_bytes());
            
            // Add each key-value pair
            for (key, value) in obj {
                // Add key
                result.extend_from_slice(format!("${}\r\n{}\r\n", key.len(), key).as_bytes());
                
                // Add value based on type
                match value {
                    Value::String(s) => {
                        result.extend_from_slice(format!("${}\r\n{}\r\n", s.len(), s).as_bytes());
                    },
                    Value::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            result.extend_from_slice(format!(":{}\r\n", i).as_bytes());
                        } else if let Some(f) = n.as_f64() {
                            result.extend_from_slice(format!(",{}\r\n", f).as_bytes());
                        } else {
                            // Fallback to string
                            let s = n.to_string();
                            result.extend_from_slice(format!("${}\r\n{}\r\n", s.len(), s).as_bytes());
                        }
                    },
                    Value::Bool(b) => {
                        if *b {
                            result.extend_from_slice("#t\r\n".as_bytes());
                        } else {
                            result.extend_from_slice("#f\r\n".as_bytes());
                        }
                    },
                    Value::Null => {
                        result.extend_from_slice("_\r\n".as_bytes());
                    },
                    Value::Array(a) => {
                        // Create array header
                        result.extend_from_slice(format!("*{}\r\n", a.len()).as_bytes());
                        
                        // Add each array element
                        for item in a {
                            match item {
                                Value::String(s) => {
                                    result.extend_from_slice(format!("${}\r\n{}\r\n", s.len(), s).as_bytes());
                                },
                                Value::Number(n) => {
                                    if let Some(i) = n.as_i64() {
                                        result.extend_from_slice(format!(":{}\r\n", i).as_bytes());
                                    } else if let Some(f) = n.as_f64() {
                                        result.extend_from_slice(format!(",{}\r\n", f).as_bytes());
                                    } else {
                                        // Fallback to string
                                        let s = n.to_string();
                                        result.extend_from_slice(format!("${}\r\n{}\r\n", s.len(), s).as_bytes());
                                    }
                                },
                                Value::Bool(b) => {
                                    if *b {
                                        result.extend_from_slice("#t\r\n".as_bytes());
                                    } else {
                                        result.extend_from_slice("#f\r\n".as_bytes());
                                    }
                                },
                                Value::Null => {
                                    result.extend_from_slice("_\r\n".as_bytes());
                                },
                                _ => {
                                    // Convert complex types to JSON string
                                    let s = item.to_string();
                                    result.extend_from_slice(format!("${}\r\n{}\r\n", s.len(), s).as_bytes());
                                }
                            }
                        }
                    },
                    Value::Object(o) => {
                        // Convert to map format
                        result.extend_from_slice(format!("%{}\r\n", o.len()).as_bytes());
                        
                        // Add each key-value pair
                        for (k, v) in o {
                            // Add key
                            result.extend_from_slice(format!("${}\r\n{}\r\n", k.len(), k).as_bytes());
                            
                            // Add value (simplified, just convert to string)
                            let s = v.to_string();
                            result.extend_from_slice(format!("${}\r\n{}\r\n", s.len(), s).as_bytes());
                        }
                    },
                }
            }
            
            Ok(result)
        } else {
            // For non-object values, serialize as simple string
            let s = value.to_string();
            Ok(format!("${}\r\n{}\r\n", s.len(), s).as_bytes().to_vec())
        }
    }
    
    /// Serialize a value to JSON format
    pub fn to_json(&self, value: &Value, pretty: bool) -> Result<String, SerializerError> {
        if pretty {
            match serde_json::to_string_pretty(value) {
                Ok(s) => Ok(s),
                Err(e) => Err(SerializerError::SerializationError(format!("JSON serialization error: {}", e))),
            }
        } else {
            match serde_json::to_string(value) {
                Ok(s) => Ok(s),
                Err(e) => Err(SerializerError::SerializationError(format!("JSON serialization error: {}", e))),
            }
        }
    }
    
    /// Serialize a value to MessagePack format
    pub fn to_messagepack(&self, value: &Value) -> Result<Vec<u8>, SerializerError> {
        match rmp_serde::to_vec(value) {
            Ok(data) => Ok(data),
            Err(e) => Err(SerializerError::SerializationError(format!("MessagePack serialization error: {}", e))),
        }
    }
    
    /// Get the content type for a format
    pub fn get_content_type(&self, format_name: &str, version: Option<&str>) -> Result<String, SerializerError> {
        // Get format metadata
        let metadata = match self.registry.get_format_metadata(format_name, version) {
            Ok(m) => m,
            Err(e) => return Err(SerializerError::Registry(e)),
        };
        
        Ok(metadata.content_type)
    }
}