// Format Transformer Module for gNode
//
// This module provides transformation capabilities between different message formats.
// It supports bidirectional transformations with field mapping and validation.

use serde_json::Value;
use std::sync::Arc;

use super::format_registry::{FormatRegistry, RegistryError};
use super::format_schema::SchemaError;
use super::format_parser::{FormatParser, ParserError};
use super::format_serializer::{FormatSerializer, SerializerError};

/// Transformer error type
#[derive(Debug)]
pub enum TransformerError {
    /// Registry error
    Registry(RegistryError),
    /// Schema error
    Schema(SchemaError),
    /// Parser error
    Parser(ParserError),
    /// Serializer error
    Serializer(SerializerError),
    /// Format not found
    FormatNotFound(String),
    /// Transformation error
    TransformationError(String),
    /// Source format not detected
    SourceFormatNotDetected,
}

impl std::fmt::Display for TransformerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransformerError::Registry(e) => write!(f, "Registry error: {}", e),
            TransformerError::Schema(e) => write!(f, "Schema error: {}", e),
            TransformerError::Parser(e) => write!(f, "Parser error: {}", e),
            TransformerError::Serializer(e) => write!(f, "Serializer error: {}", e),
            TransformerError::FormatNotFound(e) => write!(f, "Format not found: {}", e),
            TransformerError::TransformationError(e) => write!(f, "Transformation error: {}", e),
            TransformerError::SourceFormatNotDetected => write!(f, "Source format not detected"),
        }
    }
}

impl From<SerializerError> for TransformerError {
    fn from(err: SerializerError) -> Self {
        TransformerError::Serializer(err)
    }
}

impl From<RegistryError> for TransformerError {
    fn from(err: RegistryError) -> Self {
        TransformerError::Registry(err)
    }
}

impl From<SchemaError> for TransformerError {
    fn from(err: SchemaError) -> Self {
        TransformerError::Schema(err)
    }
}

impl From<ParserError> for TransformerError {
    fn from(err: ParserError) -> Self {
        TransformerError::Parser(err)
    }
}

/// Format transformer
pub struct FormatTransformer {
    /// Format registry
    registry: Arc<FormatRegistry>,
    /// Format parser
    parser: FormatParser,
    /// Format serializer (single source of truth for all output encodings)
    serializer: FormatSerializer,
}

impl FormatTransformer {
    /// Create a new format transformer
    pub fn new(registry: Arc<FormatRegistry>) -> Self {
        let parser = FormatParser::new(Arc::clone(&registry));
        let serializer = FormatSerializer::new(Arc::clone(&registry));

        Self {
            registry,
            parser,
            serializer,
        }
    }
    
    /// Transform a message from one format to another
    pub fn transform(&self, source_data: &[u8], target_format: &str, target_version: Option<&str>) -> Result<Vec<u8>, TransformerError> {
        // Parse the source data to internal format
        let parsed = match self.parser.parse(source_data) {
            Ok(value) => value,
            Err(ParserError::FormatNotDetected) => return Err(TransformerError::SourceFormatNotDetected),
            Err(e) => return Err(TransformerError::Parser(e)),
        };
        
        // Transform to target format
        self.transform_value(&parsed, target_format, target_version)
    }
    
    /// Transform from one known format to another
    pub fn transform_from_to(&self, source_data: &[u8], source_format: &str, source_version: Option<&str>,
                           target_format: &str, target_version: Option<&str>) -> Result<Vec<u8>, TransformerError> {
        // Parse the source data with the specified format
        let parsed = self.parser.parse_with_format(source_format, source_version, source_data)?;
        
        // Transform to target format
        self.transform_value(&parsed, target_format, target_version)
    }
    
    /// Transform a value to a target format.
    ///
    /// Serialization is delegated to `FormatSerializer` — the single source of
    /// truth for every output encoding (JSON / MessagePack / RESP3). The
    /// serializer applies the target's reverse field-mapping itself, so the
    /// internal `value` is passed straight through. This collapses the former
    /// duplicate encoder here, whose RESP3 path wrongly emitted nested
    /// aggregates as JSON strings instead of real RESP3 arrays/maps.
    fn transform_value(&self, value: &Value, target_format: &str, target_version: Option<&str>) -> Result<Vec<u8>, TransformerError> {
        Ok(self.serializer.serialize(value, target_format, target_version)?)
    }

    /// Transform a value between internal formats
    pub fn transform_internal(&self, value: &Value, source_format: &str, source_version: Option<&str>,
                              target_format: &str, target_version: Option<&str>) -> Result<Value, TransformerError> {
        // Get source format
        let source = self.registry.get_format(source_format, source_version)?;
        
        // Get target format
        let target = self.registry.get_format(target_format, target_version)?;
        
        // Apply reverse mapping to transform from internal format to source format
        let source_value = source.apply_reverse_mapping(value)?;
        
        // Then apply forward mapping to transform from source format to target format
        let target_value = target.apply_mapping(&source_value)?;
        
        Ok(target_value)
    }
}