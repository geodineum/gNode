// Template Module for gNode (CMS extension)
//
// This module provides template management for extensible message formats
// and protocol optimizations.

mod template_manager;
mod template_engine;

// Format sub-module (always available when CMS is compiled)
pub mod format;

pub use template_manager::{TemplateManager, TemplateError, TemplateType, TemplateMetadata};
pub use template_engine::{TemplateEngine, TemplateEngineError};

pub use format::{
    FormatRegistry, FormatSchema, FormatDefinition, FormatMetadata,
    FormatParser, FormatSerializer, FormatTransformer,
    SchemaError, RegistryError, ParserError, SerializerError, TransformerError,
};

/// Create a template manager with the given base directory
pub fn create_template_manager(base_dir: &str) -> std::sync::Arc<TemplateManager> {
    std::sync::Arc::new(TemplateManager::new(base_dir))
}

/// Create a template engine with the given template manager
pub fn create_template_engine(template_manager: std::sync::Arc<TemplateManager>) -> TemplateEngine {
    TemplateEngine::new(template_manager)
}

/// Initialize the template manager and engine
pub fn initialize_templates(base_dir: &str) -> Result<(std::sync::Arc<TemplateManager>, TemplateEngine), String> {
    let template_manager = create_template_manager(base_dir);

    match template_manager.initialize() {
        Ok(_) => {},
        Err(e) => {
            return Err(format!("Failed to initialize template manager: {:?}", e));
        }
    }

    let template_engine = create_template_engine(template_manager.clone());

    match template_engine.initialize() {
        Ok(_) => {},
        Err(e) => {
            return Err(format!("Failed to initialize template engine: {:?}", e));
        }
    }

    Ok((template_manager, template_engine))
}

pub fn initialize_formats(base_dir: &str) -> Result<std::sync::Arc<FormatRegistry>, String> {
    let format_registry = format::create_format_registry(Some(&format!("{}/format", base_dir)));

    match format::initialize_format_registry(&format_registry) {
        Ok(_) => {},
        Err(e) => {
            return Err(format!("Failed to initialize format registry: {:?}", e));
        }
    }

    Ok(format_registry)
}
