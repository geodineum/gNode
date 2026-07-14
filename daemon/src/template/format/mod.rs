// Format Module for gNode
//
// This module provides user-defined message formats for the gNode daemon.
// It enables flexible message formats while maintaining the efficiency of
// the unified stream approach.

mod format_schema;
mod format_registry;
mod format_parser;
mod format_transformer;
mod format_serializer;

pub use format_schema::{
    FormatSchema, FormatDefinition, SchemaError,
    DetectionPattern, FieldMapping, ComplexMapping
};
pub use format_registry::{
    FormatRegistry, FormatMetadata, RegistryError
};
pub use format_parser::{
    FormatParser, ParserError
};
pub use format_transformer::{
    FormatTransformer, TransformerError
};
pub use format_serializer::{
    FormatSerializer, SerializerError
};

use std::sync::Arc;

/// Create a new format registry with the given base directory
pub fn create_format_registry(base_dir: Option<&str>) -> Arc<FormatRegistry> {
    let registry = match base_dir {
        Some(dir) => FormatRegistry::with_base_dir(dir),
        None => FormatRegistry::new(),
    };
    
    Arc::new(registry)
}

/// Initialize a format registry
pub fn initialize_format_registry(registry: &Arc<FormatRegistry>) -> Result<(), RegistryError> {
    registry.initialize()
}

