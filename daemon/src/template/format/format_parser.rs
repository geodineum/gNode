// Format Parser Module for gNode
//
// This module provides parsing capabilities for user-defined message formats.
// It supports parsing from various formats into a standardized internal representation.

use serde_json::Value;
use log::debug;
use std::sync::Arc;

use super::format_schema::{FormatSchema, SchemaError};
use super::format_registry::{FormatRegistry, RegistryError};

/// Parser error type
#[derive(Debug)]
pub enum ParserError {
    /// Registry error
    Registry(RegistryError),
    /// Schema error
    Schema(SchemaError),
    /// Format not detected
    FormatNotDetected,
    /// Parse error
    ParseError(String),
    /// Binary format error
    BinaryFormatError(String),
}

impl std::fmt::Display for ParserError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParserError::Registry(e) => write!(f, "Registry error: {}", e),
            ParserError::Schema(e) => write!(f, "Schema error: {}", e),
            ParserError::FormatNotDetected => write!(f, "Format not detected"),
            ParserError::ParseError(e) => write!(f, "Parse error: {}", e),
            ParserError::BinaryFormatError(e) => write!(f, "Binary format error: {}", e),
        }
    }
}

impl From<RegistryError> for ParserError {
    fn from(err: RegistryError) -> Self {
        ParserError::Registry(err)
    }
}

impl From<SchemaError> for ParserError {
    fn from(err: SchemaError) -> Self {
        ParserError::Schema(err)
    }
}

/// Format parser
pub struct FormatParser {
    /// Format registry
    registry: Arc<FormatRegistry>,
}

impl FormatParser {
    /// Create a new format parser
    pub fn new(registry: Arc<FormatRegistry>) -> Self {
        Self {
            registry,
        }
    }
    
    /// Parse a message with automatic format detection
    pub fn parse(&self, data: &[u8]) -> Result<Value, ParserError> {
        // Detect format
        let format_info = match self.registry.detect_format(data)? {
            Some((name, version, confidence)) => {
                debug!("Detected format {} version {} with confidence {}", name, version, confidence);
                (name, version)
            },
            None => return Err(ParserError::FormatNotDetected),
        };
        
        // Parse using detected format
        self.parse_with_format(&format_info.0, Some(&format_info.1), data)
    }
    
    /// Parse a message with a specific format
    pub fn parse_with_format(&self, format_name: &str, version: Option<&str>, data: &[u8]) -> Result<Value, ParserError> {
        // Get format
        let format = self.registry.get_format(format_name, version)?;
        
        // Parse based on content type
        let definition = format.get_definition();
        
        match definition.content_type.as_str() {
            "application/json" => {
                // Parse JSON
                self.parse_json(data, &format)
            },
            "application/messagepack" | "application/msgpack" => {
                // Parse MessagePack
                self.parse_messagepack(data, &format)
            },
            "application/resp3" => {
                // Parse RESP3
                self.parse_resp3(data, &format)
            },
            _ => {
                // Unknown content type
                Err(ParserError::ParseError(format!("Unsupported content type: {}", definition.content_type)))
            }
        }
    }
    
    /// Parse JSON data
    fn parse_json(&self, data: &[u8], format: &Arc<FormatSchema>) -> Result<Value, ParserError> {
        // Parse JSON
        let raw_value = match serde_json::from_slice::<Value>(data) {
            Ok(value) => value,
            Err(e) => return Err(ParserError::ParseError(format!("JSON parse error: {}", e))),
        };
        
        // Validate against schema
        format.validate(&raw_value)?;
        
        // Apply field mapping
        let mapped_value = format.apply_mapping(&raw_value)?;
        
        Ok(mapped_value)
    }
    
    /// Parse MessagePack data
    fn parse_messagepack(&self, data: &[u8], format: &Arc<FormatSchema>) -> Result<Value, ParserError> {
        // Parse MessagePack
        let raw_value = match rmp_serde::from_slice::<Value>(data) {
            Ok(value) => value,
            Err(e) => return Err(ParserError::ParseError(format!("MessagePack parse error: {}", e))),
        };
        
        // Validate against schema
        format.validate(&raw_value)?;
        
        // Apply field mapping
        let mapped_value = format.apply_mapping(&raw_value)?;
        
        Ok(mapped_value)
    }
    
    /// Parse RESP3 data
    fn parse_resp3(&self, data: &[u8], format: &Arc<FormatSchema>) -> Result<Value, ParserError> {
        // We need to convert RESP3 to a map of fields
        let fields = self.parse_resp3_to_fields(data)?;
        
        // Convert fields to a JSON object
        let mut raw_value = serde_json::Map::new();
        for (key, value) in fields {
            raw_value.insert(key, Value::String(value));
        }
        
        // Apply field mapping
        let mapped_value = format.apply_mapping(&Value::Object(raw_value))?;
        
        Ok(mapped_value)
    }
    
    /// Parse RESP3 data to fields with proper bounds checking
    fn parse_resp3_to_fields(&self, data: &[u8]) -> Result<Vec<(String, String)>, ParserError> {
        // We need to parse RESP3 format (simplified implementation)
        // This is a basic parser for key-value pairs in RESP3 array format

        let data_str = match std::str::from_utf8(data) {
            Ok(s) => s,
            Err(_) => return Err(ParserError::ParseError("Invalid UTF-8 in RESP3 data".to_string())),
        };

        // Collect lines for bounds-safe access
        let lines: Vec<&str> = data_str.lines().collect();

        if lines.is_empty() {
            return Err(ParserError::ParseError("RESP3 data is empty".to_string()));
        }

        // Check if it's an array
        let first_line = lines[0];
        if !first_line.starts_with('*') {
            return Err(ParserError::ParseError("RESP3 data must start with array type".to_string()));
        }

        // Parse array size with bounds check
        if first_line.len() < 2 {
            return Err(ParserError::ParseError("Invalid array header in RESP3 data".to_string()));
        }

        let size = match first_line[1..].parse::<usize>() {
            Ok(n) => n,
            Err(_) => return Err(ParserError::ParseError("Invalid array size in RESP3 data".to_string())),
        };

        if size % 2 != 0 {
            return Err(ParserError::ParseError("RESP3 array size must be even for key-value pairs".to_string()));
        }

        // Validate we have enough lines for the declared array size
        // Each key-value pair needs: key_type + key + value_type + value = 4 lines per pair
        // Plus 1 for the array header
        let expected_lines = 1 + (size * 2); // size/2 pairs * 4 lines = size * 2
        if lines.len() < expected_lines {
            return Err(ParserError::ParseError(format!(
                "RESP3 data truncated: expected {} lines for {} elements, got {}",
                expected_lines, size, lines.len()
            )));
        }

        let mut fields = Vec::new();
        let mut line_idx = 1; // Skip the array header

        // Parse key-value pairs with bounds checking
        let num_pairs = size / 2;
        for _ in 0..num_pairs {
            // Bounds check before accessing
            if line_idx + 3 >= lines.len() {
                return Err(ParserError::ParseError("RESP3 data truncated during parsing".to_string()));
            }

            // Parse key type
            let key_type = lines[line_idx];
            if !key_type.starts_with('$') && !key_type.starts_with('+') {
                return Err(ParserError::ParseError(format!(
                    "RESP3 key must be a string, got: '{}'", key_type
                )));
            }
            line_idx += 1;

            // Parse key value
            let key = lines[line_idx];
            line_idx += 1;

            // Parse value type
            let value_type = lines[line_idx];
            if !value_type.starts_with('$') && !value_type.starts_with('+') && !value_type.starts_with(':') {
                return Err(ParserError::ParseError(format!(
                    "RESP3 value must be a string or integer, got: '{}'", value_type
                )));
            }
            line_idx += 1;

            // Parse value
            let value = lines[line_idx];
            line_idx += 1;

            // Add to fields
            fields.push((key.to_string(), value.to_string()));
        }

        Ok(fields)
    }
    
    /// Determine if data is likely to be in a specific format
    pub fn is_likely_format(&self, data: &[u8], format_name: &str, version: Option<&str>) -> Result<bool, ParserError> {
        // Get format
        let format = self.registry.get_format(format_name, version)?;
        
        // Check if format can parse this data
        Ok(format.can_parse(data))
    }
    
    /// Get format confidence score
    pub fn format_confidence(&self, data: &[u8], format_name: &str, version: Option<&str>) -> Result<f64, ParserError> {
        // Get format
        let format = self.registry.get_format(format_name, version)?;
        
        // Get confidence score
        Ok(format.confidence_score(data))
    }
}