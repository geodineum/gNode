// Format Schema Module for gNode
//
// This module provides schema validation for user-defined message formats.
// It supports JSON Schema Draft-07 for validating format definitions and messages.

use serde::{Serialize, Deserialize};
use serde_json::{Value, Map};
use jsonschema::JSONSchema;
use log::warn;
use std::fmt;
use std::collections::HashMap;

/// Maximum regex pattern size to prevent ReDoS attacks (512 bytes)
const MAX_REGEX_PATTERN_SIZE: usize = 512;

/// Maximum regex size limit for compiled patterns (1MB)
const MAX_REGEX_SIZE_LIMIT: usize = 1024 * 1024;

/// Safely compile a regex pattern with size limits to prevent ReDoS attacks.
/// Returns None if pattern is too long or fails to compile within limits.
fn safe_regex_compile(pattern: &str) -> Option<regex::Regex> {
    if pattern.len() > MAX_REGEX_PATTERN_SIZE {
        warn!("Regex pattern too long ({} bytes), max {} bytes", pattern.len(), MAX_REGEX_PATTERN_SIZE);
        return None;
    }

    match regex::RegexBuilder::new(pattern)
        .size_limit(MAX_REGEX_SIZE_LIMIT)
        .dfa_size_limit(MAX_REGEX_SIZE_LIMIT)
        .build()
    {
        Ok(regex) => Some(regex),
        Err(e) => {
            warn!("Failed to compile regex pattern: {}", e);
            None
        }
    }
}

/// Error type for format schema operations
#[derive(Debug)]
pub enum SchemaError {
    /// JSON parsing error
    Json(serde_json::Error),
    /// Schema compilation error
    Compilation(String),
    /// Schema validation error
    Validation(String),
    /// Field mapping error
    Mapping(String),
    /// Schema not found
    NotFound(String),
}

impl fmt::Display for SchemaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SchemaError::Json(e) => write!(f, "JSON error: {}", e),
            SchemaError::Compilation(e) => write!(f, "Schema compilation error: {}", e),
            SchemaError::Validation(e) => write!(f, "Schema validation error: {}", e),
            SchemaError::Mapping(e) => write!(f, "Field mapping error: {}", e),
            SchemaError::NotFound(e) => write!(f, "Schema not found: {}", e),
        }
    }
}

impl std::error::Error for SchemaError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            SchemaError::Json(e) => Some(e),
            SchemaError::Compilation(_) => None,
            SchemaError::Validation(_) => None,
            SchemaError::Mapping(_) => None,
            SchemaError::NotFound(_) => None,
        }
    }
}

impl From<serde_json::Error> for SchemaError {
    fn from(err: serde_json::Error) -> Self {
        SchemaError::Json(err)
    }
}

/// Represents a detection pattern for a format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetectionPattern {
    /// Type of pattern (prefix, regex, magic)
    pub pattern_type: String,
    /// Pattern string or bytes
    pub pattern: String,
    /// Confidence level (0.0 to 1.0)
    #[serde(default = "default_confidence")]
    pub confidence: f64,
}

fn default_confidence() -> f64 {
    0.8
}

/// Field mapping definition
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum FieldMapping {
    /// Simple string mapping (source to target)
    Simple(String),
    /// Complex mapping with transformations
    Complex(ComplexMapping),
}

/// Complex field mapping with additional properties
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComplexMapping {
    /// Target field name
    pub target: String,
    /// Optional transformation function
    #[serde(default)]
    pub transform: Option<String>,
    /// Whether the field is required
    #[serde(default)]
    pub required: bool,
    /// Default value if field is missing
    #[serde(default)]
    pub default: Option<Value>,
}

/// Format definition schema
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FormatDefinition {
    /// Format name
    pub format_name: String,
    /// Format version
    pub version: String,
    /// Format description
    #[serde(default)]
    pub description: String,
    /// Content type (json, messagepack, etc.)
    pub content_type: String,
    /// Whether the format is binary
    #[serde(default)]
    pub binary: bool,
    /// Detection patterns for auto-detection
    #[serde(default)]
    pub detection_patterns: Vec<DetectionPattern>,
    /// Field mapping between source and target
    pub field_mapping: HashMap<String, FieldMapping>,
    /// JSON Schema for validating messages
    #[serde(default)]
    pub schema: Option<Value>,
    /// Default values for fields
    #[serde(default)]
    pub defaults: HashMap<String, Value>,
}

/// Format schema validator
pub struct FormatSchema {
    /// Format definition
    definition: FormatDefinition,
    /// Compiled JSON Schema for validation
    compiled_schema: Option<JSONSchema>,
}

impl FormatSchema {
    /// Create a new format schema from a JSON definition
    pub fn new(definition_json: &Value) -> Result<Self, SchemaError> {
        // Parse the definition
        let definition: FormatDefinition = serde_json::from_value(definition_json.clone())?;

        // Compile the schema if present
        let compiled_schema = match &definition.schema {
            Some(schema) => {
                match JSONSchema::compile(schema) {
                    Ok(compiled) => Some(compiled),
                    Err(e) => {
                        return Err(SchemaError::Compilation(e.to_string()));
                    }
                }
            },
            None => None,
        };

        Ok(Self {
            definition,
            compiled_schema,
        })
    }
    
    /// Create a new format schema from a FormatDefinition
    pub fn from_definition(definition: FormatDefinition) -> Result<Self, SchemaError> {
        // Compile the schema if present
        let compiled_schema = match &definition.schema {
            Some(schema) => {
                match JSONSchema::compile(schema) {
                    Ok(compiled) => Some(compiled),
                    Err(e) => {
                        return Err(SchemaError::Compilation(e.to_string()));
                    }
                }
            },
            None => None,
        };

        Ok(Self {
            definition,
            compiled_schema,
        })
    }
    
    /// Validate a message against the format schema
    pub fn validate(&self, message: &Value) -> Result<(), SchemaError> {
        // If we have a compiled schema, validate against it
        if let Some(schema) = &self.compiled_schema {
            // Clone the message to avoid lifetime issues
            let message_owned = message.clone();

            // Validate and collect errors immediately while message_owned is alive
            let validation_result = schema.validate(&message_owned);
            match validation_result {
                Ok(()) => Ok(()),
                Err(error_iter) => {
                    // Collect error messages while message_owned is still in scope
                    let error_messages: Vec<String> = error_iter
                        .map(|e| format!("{}: {:?}", e.instance_path, e))
                        .collect();
                    Err(SchemaError::Validation(error_messages.join(", ")))
                }
            }
        } else {
            // No schema, assume valid
            Ok(())
        }
    }
    
    /// Get the format definition
    pub fn get_definition(&self) -> &FormatDefinition {
        &self.definition
    }
    
    /// Check if a message can be parsed by this format
    pub fn can_parse(&self, message: &[u8]) -> bool {
        // If no detection patterns, assume we can't parse
        if self.definition.detection_patterns.is_empty() {
            return false;
        }
        
        // Check each detection pattern
        for pattern in &self.definition.detection_patterns {
            match pattern.pattern_type.as_str() {
                "prefix" => {
                    // Check if message starts with pattern
                    let pattern_bytes = pattern.pattern.as_bytes();
                    if message.len() >= pattern_bytes.len() && &message[..pattern_bytes.len()] == pattern_bytes {
                        return true;
                    }
                },
                "regex" => {
                    // Try to convert message to string and check regex with ReDoS protection
                    if let Ok(message_str) = std::str::from_utf8(message) {
                        if let Some(regex) = safe_regex_compile(&pattern.pattern) {
                            if regex.is_match(message_str) {
                                return true;
                            }
                        }
                    }
                },
                "magic" => {
                    // Convert pattern to bytes and check
                    if let Ok(magic_bytes) = hex::decode(&pattern.pattern) {
                        if message.len() >= magic_bytes.len() && message[..magic_bytes.len()] == magic_bytes[..] {
                            return true;
                        }
                    }
                },
                _ => {
                    // Unknown pattern type
                    warn!("Unknown pattern type: {}", pattern.pattern_type);
                }
            }
        }
        
        // No patterns matched
        false
    }
    
    /// Calculate confidence score for this format parsing the message
    pub fn confidence_score(&self, message: &[u8]) -> f64 {
        // If no detection patterns, return 0.0
        if self.definition.detection_patterns.is_empty() {
            return 0.0;
        }
        
        // Check each detection pattern and find the highest confidence
        let mut max_confidence: f64 = 0.0;
        
        for pattern in &self.definition.detection_patterns {
            match pattern.pattern_type.as_str() {
                "prefix" => {
                    // Check if message starts with pattern
                    let pattern_bytes = pattern.pattern.as_bytes();
                    if message.len() >= pattern_bytes.len() && &message[..pattern_bytes.len()] == pattern_bytes {
                        max_confidence = max_confidence.max(pattern.confidence);
                    }
                },
                "regex" => {
                    // Try to convert message to string and check regex with ReDoS protection
                    if let Ok(message_str) = std::str::from_utf8(message) {
                        if let Some(regex) = safe_regex_compile(&pattern.pattern) {
                            if regex.is_match(message_str) {
                                max_confidence = max_confidence.max(pattern.confidence);
                            }
                        }
                    }
                },
                "magic" => {
                    // Convert pattern to bytes and check
                    if let Ok(magic_bytes) = hex::decode(&pattern.pattern) {
                        if message.len() >= magic_bytes.len() && message[..magic_bytes.len()] == magic_bytes[..] {
                            max_confidence = max_confidence.max(pattern.confidence);
                        }
                    }
                },
                _ => {
                    // Unknown pattern type
                    warn!("Unknown pattern type: {}", pattern.pattern_type);
                }
            }
        }
        
        max_confidence
    }
    
    /// Apply field mapping to transform a message to internal format
    pub fn apply_mapping(&self, message: &Value) -> Result<Value, SchemaError> {
        // Create output object
        let mut output = Map::new();
        
        // Apply field mapping
        for (source_field, mapping) in &self.definition.field_mapping {
            match mapping {
                FieldMapping::Simple(target_field) => {
                    // Simple mapping
                    if let Some(value) = message.get(source_field) {
                        output.insert(target_field.clone(), value.clone());
                    }
                },
                FieldMapping::Complex(complex) => {
                    // Complex mapping
                    let target_field = &complex.target;
                    let value = match message.get(source_field) {
                        Some(v) => {
                            // Apply transformation if specified
                            if let Some(transform) = &complex.transform {
                                self.apply_transform(v, transform)?
                            } else {
                                v.clone()
                            }
                        },
                        None => {
                            // Use default value if specified and field is missing
                            if complex.required && complex.default.is_none() {
                                return Err(SchemaError::Mapping(format!("Required field {} is missing", source_field)));
                            }
                            
                            match &complex.default {
                                Some(default) => default.clone(),
                                None => continue, // Skip this field
                            }
                        }
                    };
                    
                    output.insert(target_field.clone(), value);
                }
            }
        }
        
        // Apply default values for fields not specified in mapping
        for (field, value) in &self.definition.defaults {
            if !output.contains_key(field) {
                output.insert(field.clone(), value.clone());
            }
        }
        
        Ok(Value::Object(output))
    }
    
    /// Apply reverse field mapping to transform internal format to user format
    pub fn apply_reverse_mapping(&self, message: &Value) -> Result<Value, SchemaError> {
        // Create output object
        let mut output = Map::new();
        
        // Create reverse mapping
        let mut reverse_mapping = HashMap::new();
        for (source, mapping) in &self.definition.field_mapping {
            match mapping {
                FieldMapping::Simple(target) => {
                    reverse_mapping.insert(target.clone(), (source.clone(), None));
                },
                FieldMapping::Complex(complex) => {
                    reverse_mapping.insert(complex.target.clone(), (source.clone(), complex.transform.clone()));
                }
            }
        }
        
        // Apply reverse mapping
        if let Some(obj) = message.as_object() {
            for (field, value) in obj {
                match reverse_mapping.get(field) {
                    Some((source_field, transform)) => {
                        // Apply reverse transformation if specified
                        let target_value = match transform {
                            Some(t) => self.apply_reverse_transform(value, t)?,
                            None => value.clone(),
                        };
                        
                        output.insert(source_field.clone(), target_value);
                    },
                    None => {
                        // Field not in mapping, copy as-is
                        output.insert(field.clone(), value.clone());
                    }
                }
            }
        }
        
        Ok(Value::Object(output))
    }
    
    /// Apply a transformation to a value
    fn apply_transform(&self, value: &Value, transform: &str) -> Result<Value, SchemaError> {
        // Implement basic transformations
        match transform {
            "string" => {
                match value {
                    Value::String(s) => Ok(Value::String(s.clone())),
                    Value::Number(n) => Ok(Value::String(n.to_string())),
                    Value::Bool(b) => Ok(Value::String(b.to_string())),
                    Value::Null => Ok(Value::String("null".to_string())),
                    _ => Ok(Value::String(value.to_string())),
                }
            },
            "number" => {
                match value {
                    Value::Number(n) => Ok(Value::Number(n.clone())),
                    Value::String(s) => {
                        match s.parse::<f64>() {
                            Ok(n) => Ok(Value::Number(serde_json::Number::from_f64(n).unwrap_or_else(|| serde_json::Number::from(0)))),
                            Err(_) => Err(SchemaError::Mapping(format!("Cannot convert string '{}' to number", s))),
                        }
                    },
                    _ => Err(SchemaError::Mapping(format!("Cannot convert {:?} to number", value))),
                }
            },
            "boolean" => {
                match value {
                    Value::Bool(b) => Ok(Value::Bool(*b)),
                    Value::String(s) => {
                        match s.to_lowercase().as_str() {
                            "true" | "yes" | "1" | "on" => Ok(Value::Bool(true)),
                            "false" | "no" | "0" | "off" => Ok(Value::Bool(false)),
                            _ => Err(SchemaError::Mapping(format!("Cannot convert string '{}' to boolean", s))),
                        }
                    },
                    Value::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            Ok(Value::Bool(i != 0))
                        } else {
                            Ok(Value::Bool(n.as_f64().unwrap_or_default() != 0.0))
                        }
                    },
                    _ => Err(SchemaError::Mapping(format!("Cannot convert {:?} to boolean", value))),
                }
            },
            "timestamp_ms" => {
                // Convert to timestamp in milliseconds
                match value {
                    Value::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            Ok(Value::Number(serde_json::Number::from_f64((i as f64) / 1000.0).unwrap_or_else(|| serde_json::Number::from(0))))
                        } else {
                            Ok(Value::Number(serde_json::Number::from_f64(n.as_f64().unwrap_or(0.0) / 1000.0).unwrap_or_else(|| serde_json::Number::from(0))))
                        }
                    },
                    Value::String(s) => {
                        match s.parse::<i64>() {
                            Ok(n) => Ok(Value::Number(serde_json::Number::from_f64((n as f64) / 1000.0).unwrap_or_else(|| serde_json::Number::from(0)))),
                            Err(_) => Err(SchemaError::Mapping(format!("Cannot convert string '{}' to timestamp", s))),
                        }
                    },
                    _ => Err(SchemaError::Mapping(format!("Cannot convert {:?} to timestamp", value))),
                }
            },
            "timestamp_s" => {
                // Convert to timestamp in seconds
                match value {
                    Value::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            Ok(Value::Number(serde_json::Number::from_f64(i as f64).unwrap_or_else(|| serde_json::Number::from(0))))
                        } else {
                            Ok(Value::Number(serde_json::Number::from_f64(n.as_f64().unwrap_or(0.0)).unwrap_or_else(|| serde_json::Number::from(0))))
                        }
                    },
                    Value::String(s) => {
                        match s.parse::<f64>() {
                            Ok(n) => Ok(Value::Number(serde_json::Number::from_f64(n).unwrap_or_else(|| serde_json::Number::from(0)))),
                            Err(_) => Err(SchemaError::Mapping(format!("Cannot convert string '{}' to timestamp", s))),
                        }
                    },
                    _ => Err(SchemaError::Mapping(format!("Cannot convert {:?} to timestamp", value))),
                }
            },
            "iso_date" => {
                // Convert ISO date string to timestamp
                match value {
                    Value::String(s) => {
                        // Parse ISO date string
                        match chrono::DateTime::parse_from_rfc3339(s) {
                            Ok(dt) => {
                                let timestamp = dt.timestamp() as f64 + dt.timestamp_subsec_millis() as f64 / 1000.0;
                                Ok(Value::Number(serde_json::Number::from_f64(timestamp).unwrap_or_else(|| serde_json::Number::from(0))))
                            },
                            Err(_) => Err(SchemaError::Mapping(format!("Cannot parse '{}' as ISO date", s))),
                        }
                    },
                    _ => Err(SchemaError::Mapping(format!("Cannot convert {:?} to ISO date", value))),
                }
            },
            "json" => {
                // Parse JSON string to object
                match value {
                    Value::String(s) => {
                        match serde_json::from_str(s) {
                            Ok(json) => Ok(json),
                            Err(e) => Err(SchemaError::Mapping(format!("Cannot parse '{}' as JSON: {}", s, e))),
                        }
                    },
                    _ => Ok(value.clone()), // Already parsed, return as-is
                }
            },
            _ => {
                // Unknown transformation
                warn!("Unknown transformation: {}", transform);
                Ok(value.clone())
            }
        }
    }
    
    /// Apply a reverse transformation to a value
    fn apply_reverse_transform(&self, value: &Value, transform: &str) -> Result<Value, SchemaError> {
        // Implement basic reverse transformations
        match transform {
            "string" => {
                // Convert back to original type based on context, but for now, keep as string
                Ok(Value::String(value.to_string()))
            },
            "number" => {
                // Keep as number
                match value {
                    Value::Number(n) => Ok(Value::Number(n.clone())),
                    _ => Err(SchemaError::Mapping(format!("Expected number, got {:?}", value))),
                }
            },
            "boolean" => {
                // Keep as boolean
                match value {
                    Value::Bool(b) => Ok(Value::Bool(*b)),
                    _ => Err(SchemaError::Mapping(format!("Expected boolean, got {:?}", value))),
                }
            },
            "timestamp_ms" => {
                // Convert from seconds to milliseconds
                match value {
                    Value::Number(n) => {
                        if let Some(f) = n.as_f64() {
                            Ok(Value::Number(serde_json::Number::from_f64(f * 1000.0).unwrap_or_else(|| serde_json::Number::from(0))))
                        } else {
                            Err(SchemaError::Mapping(format!("Cannot convert {:?} to milliseconds", value)))
                        }
                    },
                    _ => Err(SchemaError::Mapping(format!("Expected number, got {:?}", value))),
                }
            },
            "timestamp_s" => {
                // Keep as seconds
                match value {
                    Value::Number(n) => Ok(Value::Number(n.clone())),
                    _ => Err(SchemaError::Mapping(format!("Expected number, got {:?}", value))),
                }
            },
            "iso_date" => {
                // Convert from timestamp to ISO date string
                match value {
                    Value::Number(n) => {
                        if let Some(f) = n.as_f64() {
                            let secs = f.trunc() as i64;
                            let nanos = ((f.fract() * 1_000_000_000.0) as u32).min(999_999_999);
                            let dt = chrono::DateTime::from_timestamp(secs, nanos)
                                .unwrap_or(chrono::DateTime::<chrono::Utc>::UNIX_EPOCH);
                            Ok(Value::String(dt.to_rfc3339()))
                        } else {
                            Err(SchemaError::Mapping(format!("Cannot convert {:?} to ISO date", value)))
                        }
                    },
                    _ => Err(SchemaError::Mapping(format!("Expected number, got {:?}", value))),
                }
            },
            "json" => {
                // Convert to JSON string
                Ok(Value::String(value.to_string()))
            },
            _ => {
                // Unknown transformation
                warn!("Unknown reverse transformation: {}", transform);
                Ok(value.clone())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Schema exercising all seven built-in transforms (wire field -> internal).
    fn transform_schema() -> FormatSchema {
        let def = json!({
            "format_name": "xform_test",
            "version": "1.0.0",
            "content_type": "application/json",
            "field_mapping": {
                "s_in":    {"target": "s_out",    "transform": "string"},
                "n_in":    {"target": "n_out",    "transform": "number"},
                "b_in":    {"target": "b_out",    "transform": "boolean"},
                "tms_in":  {"target": "tms_out",  "transform": "timestamp_ms"},
                "ts_in":   {"target": "ts_out",   "transform": "timestamp_s"},
                "iso_in":  {"target": "iso_out",  "transform": "iso_date"},
                "json_in": {"target": "json_out", "transform": "json"}
            }
        });
        FormatSchema::new(&def).unwrap()
    }

    #[test]
    fn all_seven_transforms_apply_forward() {
        let schema = transform_schema();
        let input = json!({
            "s_in": 42,
            "n_in": "3.5",
            "b_in": "yes",
            "tms_in": 1500,
            "ts_in": 1500,
            "iso_in": "2020-01-01T00:00:00Z",
            "json_in": "{\"a\":1}"
        });
        let out = schema.apply_mapping(&input).unwrap();

        assert_eq!(out["s_out"], json!("42"));
        assert_eq!(out["n_out"].as_f64().unwrap(), 3.5);
        assert_eq!(out["b_out"], json!(true));
        assert_eq!(out["tms_out"].as_f64().unwrap(), 1.5); // 1500ms -> 1.5s
        assert_eq!(out["ts_out"].as_f64().unwrap(), 1500.0);
        assert_eq!(out["iso_out"].as_f64().unwrap(), 1_577_836_800.0);
        assert_eq!(out["json_out"], json!({"a": 1})); // parsed, not a string
    }

    #[test]
    fn invertible_transforms_round_trip() {
        let schema = transform_schema();
        // Use native-typed inputs so forward+reverse is a true identity for the
        // invertible transforms (number, boolean, timestamp_ms/s, json).
        let input = json!({
            "n_in": 3.5,
            "b_in": true,
            "tms_in": 1500,
            "ts_in": 2.0,
            "json_in": "{\"a\":1}"
        });
        let internal = schema.apply_mapping(&input).unwrap();
        let wire = schema.apply_reverse_mapping(&internal).unwrap();

        assert_eq!(wire["n_in"].as_f64().unwrap(), 3.5);
        assert_eq!(wire["b_in"], json!(true));
        assert_eq!(wire["tms_in"].as_f64().unwrap(), 1500.0); // 1.5s -> 1500ms
        assert_eq!(wire["ts_in"].as_f64().unwrap(), 2.0);
        assert_eq!(wire["json_in"], json!("{\"a\":1}"));
    }

    #[test]
    fn iso_date_round_trips_to_same_instant() {
        let schema = transform_schema();
        let internal = schema.apply_mapping(&json!({"iso_in": "2020-01-01T00:00:00Z"})).unwrap();
        let wire = schema.apply_reverse_mapping(&internal).unwrap();
        let restored = chrono::DateTime::parse_from_rfc3339(wire["iso_in"].as_str().unwrap()).unwrap();
        assert_eq!(restored.timestamp(), 1_577_836_800);
    }

    #[test]
    fn schema_validation_accepts_and_rejects() {
        let def = json!({
            "format_name": "v",
            "version": "1.0.0",
            "content_type": "application/json",
            "field_mapping": {},
            "schema": {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "required": ["x"],
                "properties": {"x": {"type": "string"}}
            }
        });
        let schema = FormatSchema::new(&def).unwrap();
        assert!(schema.validate(&json!({"x": "ok"})).is_ok());
        assert!(schema.validate(&json!({"x": 5})).is_err());   // wrong type
        assert!(schema.validate(&json!({"y": "z"})).is_err());  // missing required
    }

    #[test]
    fn required_missing_field_errors() {
        let def = json!({
            "format_name": "req",
            "version": "1.0.0",
            "content_type": "application/json",
            "field_mapping": {
                "a": {"target": "a_out", "required": true}
            }
        });
        let schema = FormatSchema::new(&def).unwrap();
        assert!(schema.apply_mapping(&json!({"b": 1})).is_err());
        assert!(schema.apply_mapping(&json!({"a": 1})).is_ok());
    }
}