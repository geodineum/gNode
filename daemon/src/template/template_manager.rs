// Template Manager Module for gNode
//
// This module provides management for JSON schemas and RESP3 mappings,
// enabling extensible message formats and protocol optimizations.

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Read;
use std::path::Path;
use std::sync::{Arc, RwLock};
use log::{info, warn, debug, error};
use serde_json::Value;
use jsonschema::JSONSchema;

/// Template manager error type
#[derive(Debug)]
pub enum TemplateError {
    /// File system error
    IO(std::io::Error),
    /// JSON parsing error
    Parse(serde_json::Error),
    /// Schema validation error
    Validation(String),
    /// Template not found
    NotFound(String),
}

impl From<std::io::Error> for TemplateError {
    fn from(err: std::io::Error) -> Self {
        TemplateError::IO(err)
    }
}

impl From<serde_json::Error> for TemplateError {
    fn from(err: serde_json::Error) -> Self {
        TemplateError::Parse(err)
    }
}

impl std::fmt::Display for TemplateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TemplateError::IO(err) => write!(f, "IO error: {}", err),
            TemplateError::Parse(err) => write!(f, "Parse error: {}", err),
            TemplateError::Validation(msg) => write!(f, "Validation error: {}", msg),
            TemplateError::NotFound(name) => write!(f, "Template not found: {}", name),
        }
    }
}

impl std::error::Error for TemplateError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            TemplateError::IO(err) => Some(err),
            TemplateError::Parse(err) => Some(err),
            TemplateError::Validation(_) => None,
            TemplateError::NotFound(_) => None,
        }
    }
}

/// Validate schema/mapping name for safe filesystem operations
///
/// Prevents path traversal attacks by rejecting names containing:
/// - Path separators (/, \)
/// - Parent directory references (..)
/// - Null bytes
/// - Empty strings
fn validate_schema_name(name: &str) -> Result<(), TemplateError> {
    if name.is_empty() {
        return Err(TemplateError::Validation("Schema name cannot be empty".to_string()));
    }
    if name.contains('/') || name.contains('\\') {
        return Err(TemplateError::Validation(
            "Schema name cannot contain path separators".to_string()
        ));
    }
    if name.contains("..") {
        return Err(TemplateError::Validation(
            "Schema name cannot contain parent directory references".to_string()
        ));
    }
    if name.contains('\0') {
        return Err(TemplateError::Validation(
            "Schema name cannot contain null bytes".to_string()
        ));
    }
    Ok(())
}

/// Template type
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TemplateType {
    /// JSON schema template
    JsonSchema,
    /// RESP3 mapping template
    Resp3Mapping,
}

/// Template metadata
#[derive(Debug, Clone)]
pub struct TemplateMetadata {
    /// Template name
    pub name: String,
    /// Template type
    pub template_type: TemplateType,
    /// Template version
    pub version: String,
    /// Template description
    pub description: String,
    /// Template path
    pub path: String,
    /// Last modified timestamp
    pub last_modified: u64,
}

/// Template manager for gNode
pub struct TemplateManager {
    /// Base directory for templates
    base_dir: String,
    
    /// JSON schemas
    json_schemas: Arc<RwLock<HashMap<String, Value>>>,
    
    /// RESP3 mappings
    resp3_mappings: Arc<RwLock<HashMap<String, Value>>>,
    
    /// Template metadata
    metadata: Arc<RwLock<HashMap<String, TemplateMetadata>>>,
    
    /// Compiled JSON schemas (wrapped in Arc to avoid cloning)
    compiled_schemas: Arc<RwLock<HashMap<String, Arc<JSONSchema>>>>,
}

impl TemplateManager {
    /// Create a new template manager
    pub fn new(base_dir: &str) -> Self {
        Self {
            base_dir: base_dir.to_string(),
            json_schemas: Arc::new(RwLock::new(HashMap::new())),
            resp3_mappings: Arc::new(RwLock::new(HashMap::new())),
            metadata: Arc::new(RwLock::new(HashMap::new())),
            compiled_schemas: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Initialize the template manager
    pub fn initialize(&self) -> Result<(), TemplateError> {
        info!("Initializing template manager with base directory: {}", self.base_dir);
        
        // Load JSON schemas
        self.load_json_schemas()?;
        
        // Load RESP3 mappings
        self.load_resp3_mappings()?;
        
        // Compile JSON schemas
        self.compile_schemas()?;
        
        info!("Template manager initialized with {} JSON schemas and {} RESP3 mappings",
            self.json_schemas.read().map(|g| g.len()).unwrap_or(0),
            self.resp3_mappings.read().map(|g| g.len()).unwrap_or(0));
        
        Ok(())
    }
    
    /// Load JSON schemas from the template directory
    fn load_json_schemas(&self) -> Result<(), TemplateError> {
        let json_dir = format!("{}/json", self.base_dir);
        
        // Check if directory exists
        if !Path::new(&json_dir).exists() {
            warn!("JSON schema directory not found: {}", json_dir);
            return Ok(());
        }
        
        // Read all JSON files in the directory
        for entry in fs::read_dir(json_dir)? {
            let entry = entry?;
            let path = entry.path();
            
            // Skip non-JSON files
            if !path.is_file() || path.extension().unwrap_or_default() != "json" {
                continue;
            }
            
            // Read the file
            let mut file = File::open(&path)?;
            let mut contents = String::new();
            file.read_to_string(&mut contents)?;
            
            // Parse JSON
            let schema: Value = serde_json::from_str(&contents)?;
            
            // Extract schema name
            let name = path.file_stem().unwrap_or_default().to_string_lossy().to_string();
            
            // Extract metadata
            let metadata = TemplateMetadata {
                name: name.clone(),
                template_type: TemplateType::JsonSchema,
                version: schema.get("$schema")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .to_string(),
                description: schema.get("description")
                    .and_then(|v| v.as_str())
                    .unwrap_or("No description")
                    .to_string(),
                path: path.to_string_lossy().to_string(),
                last_modified: entry.metadata()?.modified()?
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
            };
            
            // Store schema and metadata
            if let Ok(mut guard) = self.json_schemas.write() {
                guard.insert(name.clone(), schema);
            } else {
                warn!("Failed to acquire write lock for json_schemas");
                continue;
            }
            if let Ok(mut guard) = self.metadata.write() {
                guard.insert(name.clone(), metadata);
            } else {
                warn!("Failed to acquire write lock for metadata");
            }

            debug!("Loaded JSON schema: {}", name);
        }

        info!("Loaded {} JSON schemas", self.json_schemas.read().map(|g| g.len()).unwrap_or(0));
        Ok(())
    }
    
    /// Load RESP3 mappings from the template directory
    fn load_resp3_mappings(&self) -> Result<(), TemplateError> {
        let resp3_dir = format!("{}/resp3", self.base_dir);
        
        // Check if directory exists
        if !Path::new(&resp3_dir).exists() {
            warn!("RESP3 mapping directory not found: {}", resp3_dir);
            return Ok(());
        }
        
        // Read all JSON files in the directory
        for entry in fs::read_dir(resp3_dir)? {
            let entry = entry?;
            let path = entry.path();
            
            // Skip non-JSON files
            if !path.is_file() || path.extension().unwrap_or_default() != "json" {
                continue;
            }
            
            // Read the file
            let mut file = File::open(&path)?;
            let mut contents = String::new();
            file.read_to_string(&mut contents)?;
            
            // Parse JSON
            let mapping: Value = serde_json::from_str(&contents)?;
            
            // Extract mapping name
            let name = path.file_stem().unwrap_or_default().to_string_lossy().to_string();
            
            // Extract metadata
            let metadata = TemplateMetadata {
                name: name.clone(),
                template_type: TemplateType::Resp3Mapping,
                version: mapping.get("version")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .to_string(),
                description: mapping.get("description")
                    .and_then(|v| v.as_str())
                    .unwrap_or("No description")
                    .to_string(),
                path: path.to_string_lossy().to_string(),
                last_modified: entry.metadata()?.modified()?
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
            };
            
            // Store mapping and metadata
            if let Ok(mut guard) = self.resp3_mappings.write() {
                guard.insert(name.clone(), mapping);
            } else {
                warn!("Failed to acquire write lock for resp3_mappings");
                continue;
            }
            if let Ok(mut guard) = self.metadata.write() {
                guard.insert(name.clone(), metadata);
            } else {
                warn!("Failed to acquire write lock for metadata");
            }

            debug!("Loaded RESP3 mapping: {}", name);
        }

        info!("Loaded {} RESP3 mappings", self.resp3_mappings.read().map(|g| g.len()).unwrap_or(0));
        Ok(())
    }
    
    /// Compile JSON schemas
    fn compile_schemas(&self) -> Result<(), TemplateError> {
        let schemas = self.json_schemas.read()
            .map_err(|_| TemplateError::Validation("Lock poisoned on json_schemas".to_string()))?;
        let mut compiled = self.compiled_schemas.write()
            .map_err(|_| TemplateError::Validation("Lock poisoned on compiled_schemas".to_string()))?;
        
        for (name, schema) in schemas.iter() {
            // Compile schema and wrap in Arc
            match JSONSchema::compile(schema) {
                Ok(compiled_schema) => {
                    compiled.insert(name.clone(), Arc::new(compiled_schema));
                    debug!("Compiled JSON schema: {}", name);
                },
                Err(e) => {
                    error!("Failed to compile JSON schema {}: {}", name, e);
                    return Err(TemplateError::Validation(format!("Failed to compile JSON schema {}: {}", name, e)));
                }
            }
        }
        
        info!("Compiled {} JSON schemas", compiled.len());
        Ok(())
    }
    
    /// Get a JSON schema by name
    pub fn get_json_schema(&self, name: &str) -> Result<Value, TemplateError> {
        let schemas = self.json_schemas.read()
            .map_err(|_| TemplateError::Validation("Lock poisoned on json_schemas".to_string()))?;

        match schemas.get(name) {
            Some(schema) => Ok(schema.clone()),
            None => Err(TemplateError::NotFound(format!("JSON schema not found: {}", name))),
        }
    }

    /// Get a RESP3 mapping by name
    pub fn get_resp3_mapping(&self, name: &str) -> Result<Value, TemplateError> {
        let mappings = self.resp3_mappings.read()
            .map_err(|_| TemplateError::Validation("Lock poisoned on resp3_mappings".to_string()))?;

        match mappings.get(name) {
            Some(mapping) => Ok(mapping.clone()),
            None => Err(TemplateError::NotFound(format!("RESP3 mapping not found: {}", name))),
        }
    }

    /// Get a compiled JSON schema by name (returns cloned Arc, not cloned schema)
    pub fn get_compiled_schema(&self, name: &str) -> Result<Arc<JSONSchema>, TemplateError> {
        let schemas = self.compiled_schemas.read()
            .map_err(|_| TemplateError::Validation("Lock poisoned on compiled_schemas".to_string()))?;

        match schemas.get(name) {
            Some(schema_arc) => Ok(Arc::clone(schema_arc)),
            None => Err(TemplateError::NotFound(format!("Compiled JSON schema not found: {}", name))),
        }
    }
    
    /// Validate a value against a JSON schema
    pub fn validate(&self, value: &Value, schema_name: &str) -> Result<(), TemplateError> {
        // Get the schema directly using our Arc-wrapped method
        let schema_arc = self.get_compiled_schema(schema_name)?;

        // Clone the value to avoid lifetime issues
        let value_owned = value.clone();

        // Validate value against schema and collect errors immediately
        let validation_result = schema_arc.validate(&value_owned);
        match validation_result {
            Ok(()) => Ok(()),
            Err(error_iter) => {
                // Collect error messages while schema_arc and value_owned are still alive
                let error_messages: Vec<String> = error_iter
                    .map(|e| format!("{}: {:?}", e.instance_path, e))
                    .collect();
                Err(TemplateError::Validation(error_messages.join(", ")))
            }
        }
    }

    /// Get all template metadata
    pub fn get_all_metadata(&self) -> Result<Vec<TemplateMetadata>, TemplateError> {
        let metadata = self.metadata.read()
            .map_err(|_| TemplateError::Validation("Lock poisoned on metadata".to_string()))?;
        let mut result = Vec::new();

        for (_, meta) in metadata.iter() {
            result.push(meta.clone());
        }

        Ok(result)
    }

    /// Get metadata for a specific template
    pub fn get_metadata(&self, name: &str) -> Result<TemplateMetadata, TemplateError> {
        let metadata = self.metadata.read()
            .map_err(|_| TemplateError::Validation("Lock poisoned on metadata".to_string()))?;

        match metadata.get(name) {
            Some(meta) => Ok(meta.clone()),
            None => Err(TemplateError::NotFound(format!("Template metadata not found: {}", name))),
        }
    }
    
    /// Add a new JSON schema
    pub fn add_json_schema(&self, name: &str, schema: Value) -> Result<(), TemplateError> {
        // Validate schema
        match JSONSchema::compile(&schema) {
            Ok(compiled_schema) => {
                // Store schema and compiled schema (wrapped in Arc)
                self.json_schemas.write()
                    .map_err(|_| TemplateError::Validation("Lock poisoned on json_schemas".to_string()))?
                    .insert(name.to_string(), schema.clone());
                self.compiled_schemas.write()
                    .map_err(|_| TemplateError::Validation("Lock poisoned on compiled_schemas".to_string()))?
                    .insert(name.to_string(), Arc::new(compiled_schema));

                // Create metadata
                let metadata = TemplateMetadata {
                    name: name.to_string(),
                    template_type: TemplateType::JsonSchema,
                    version: schema.get("$schema")
                        .and_then(|v| v.as_str())
                        .unwrap_or("unknown")
                        .to_string(),
                    description: schema.get("description")
                        .and_then(|v| v.as_str())
                        .unwrap_or("No description")
                        .to_string(),
                    path: "memory".to_string(),
                    last_modified: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                };

                // Store metadata
                self.metadata.write()
                    .map_err(|_| TemplateError::Validation("Lock poisoned on metadata".to_string()))?
                    .insert(name.to_string(), metadata);

                info!("Added JSON schema: {}", name);
                Ok(())
            },
            Err(e) => {
                error!("Failed to compile JSON schema {}: {}", name, e);
                Err(TemplateError::Validation(format!("Failed to compile JSON schema {}: {}", name, e)))
            }
        }
    }

    /// Add a new RESP3 mapping
    pub fn add_resp3_mapping(&self, name: &str, mapping: Value) -> Result<(), TemplateError> {
        // Store mapping
        self.resp3_mappings.write()
            .map_err(|_| TemplateError::Validation("Lock poisoned on resp3_mappings".to_string()))?
            .insert(name.to_string(), mapping.clone());

        // Create metadata
        let metadata = TemplateMetadata {
            name: name.to_string(),
            template_type: TemplateType::Resp3Mapping,
            version: mapping.get("version")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string(),
            description: mapping.get("description")
                .and_then(|v| v.as_str())
                .unwrap_or("No description")
                .to_string(),
            path: "memory".to_string(),
            last_modified: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        };

        // Store metadata
        self.metadata.write()
            .map_err(|_| TemplateError::Validation("Lock poisoned on metadata".to_string()))?
            .insert(name.to_string(), metadata);

        info!("Added RESP3 mapping: {}", name);
        Ok(())
    }
    
    /// Save a JSON schema to disk
    pub fn save_json_schema(&self, name: &str) -> Result<(), TemplateError> {
        // Validate name to prevent path traversal (P4AF002 fix)
        validate_schema_name(name)?;

        let schemas = self.json_schemas.read()
            .map_err(|_| TemplateError::Validation("Lock poisoned on json_schemas".to_string()))?;

        if let Some(schema) = schemas.get(name) {
            // Create file path (safe after validation)
            let file_path = format!("{}/json/{}.json", self.base_dir, name);

            // Create parent directory if it doesn't exist
            if let Some(parent) = Path::new(&file_path).parent() {
                if !parent.exists() {
                    fs::create_dir_all(parent)?;
                }
            }

            // Write schema to file
            let json = serde_json::to_string_pretty(schema)?;
            fs::write(&file_path, json)?;

            // Update metadata (drop schemas lock first to avoid deadlock)
            drop(schemas);

            if let Ok(mut metadata) = self.metadata.write() {
                if let Some(meta) = metadata.get_mut(name) {
                    meta.path = file_path.clone();
                    meta.last_modified = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                }
            } else {
                warn!("Failed to acquire write lock for metadata");
            }

            info!("Saved JSON schema {} to {}", name, file_path);
            Ok(())
        } else {
            Err(TemplateError::NotFound(format!("JSON schema not found: {}", name)))
        }
    }

    /// Save a RESP3 mapping to disk
    pub fn save_resp3_mapping(&self, name: &str) -> Result<(), TemplateError> {
        // Validate name to prevent path traversal (P4AF002 fix)
        validate_schema_name(name)?;

        let mappings = self.resp3_mappings.read()
            .map_err(|_| TemplateError::Validation("Lock poisoned on resp3_mappings".to_string()))?;

        if let Some(mapping) = mappings.get(name) {
            // Create file path (safe after validation)
            let file_path = format!("{}/resp3/{}.json", self.base_dir, name);

            // Create parent directory if it doesn't exist
            if let Some(parent) = Path::new(&file_path).parent() {
                if !parent.exists() {
                    fs::create_dir_all(parent)?;
                }
            }

            // Write mapping to file
            let json = serde_json::to_string_pretty(mapping)?;
            fs::write(&file_path, json)?;

            // Update metadata (drop mappings lock first to avoid deadlock)
            drop(mappings);

            if let Ok(mut metadata) = self.metadata.write() {
                if let Some(meta) = metadata.get_mut(name) {
                    meta.path = file_path.clone();
                    meta.last_modified = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                }
            } else {
                warn!("Failed to acquire write lock for metadata");
            }

            info!("Saved RESP3 mapping {} to {}", name, file_path);
            Ok(())
        } else {
            Err(TemplateError::NotFound(format!("RESP3 mapping not found: {}", name)))
        }
    }
}