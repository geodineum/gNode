// Format Registry Module for gNode
//
// This module provides registration and management of user-defined message formats.
// It supports versioning, discovery, and retrieval of format definitions.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::path::{Path, PathBuf};
use std::fs::{self, File};
use std::io::Read;
use log::{info, warn, debug};
use serde_json::Value;
use semver::Version;

use super::format_schema::{FormatSchema, FormatDefinition, SchemaError};

/// Error type for format registry operations
#[derive(Debug)]
pub enum RegistryError {
    /// Schema error
    Schema(SchemaError),
    /// IO error
    IO(std::io::Error),
    /// JSON parsing error
    Json(serde_json::Error),
    /// YAML parsing error
    Yaml(String),
    /// Format not found
    NotFound(String),
    /// Version not found
    VersionNotFound(String),
    /// Registration error
    Registration(String),
    /// Lock error
    LockError,
}

impl std::fmt::Display for RegistryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RegistryError::Schema(e) => write!(f, "Schema error: {}", e),
            RegistryError::IO(e) => write!(f, "IO error: {}", e),
            RegistryError::Json(e) => write!(f, "JSON error: {}", e),
            RegistryError::Yaml(e) => write!(f, "YAML error: {}", e),
            RegistryError::NotFound(e) => write!(f, "Format not found: {}", e),
            RegistryError::VersionNotFound(e) => write!(f, "Version not found: {}", e),
            RegistryError::Registration(e) => write!(f, "Registration error: {}", e),
            RegistryError::LockError => write!(f, "Lock error"),
        }
    }
}

impl From<SchemaError> for RegistryError {
    fn from(err: SchemaError) -> Self {
        RegistryError::Schema(err)
    }
}

impl From<std::io::Error> for RegistryError {
    fn from(err: std::io::Error) -> Self {
        RegistryError::IO(err)
    }
}

impl From<serde_json::Error> for RegistryError {
    fn from(err: serde_json::Error) -> Self {
        RegistryError::Json(err)
    }
}

/// Format metadata
#[derive(Debug, Clone)]
pub struct FormatMetadata {
    /// Format name
    pub name: String,
    /// Format version
    pub version: String,
    /// Format description
    pub description: String,
    /// Content type
    pub content_type: String,
    /// Whether the format is binary
    pub binary: bool,
    /// File path (if loaded from file)
    pub path: Option<String>,
    /// Last modified timestamp
    pub last_modified: u64,
}

impl FormatMetadata {
    /// Create new metadata from a format definition
    pub fn from_definition(definition: &FormatDefinition, path: Option<String>) -> Self {
        Self {
            name: definition.format_name.clone(),
            version: definition.version.clone(),
            description: definition.description.clone(),
            content_type: definition.content_type.clone(),
            binary: definition.binary,
            path,
            last_modified: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }
}

/// Format registry for storing and retrieving format definitions
#[derive(Clone)]
#[allow(clippy::type_complexity)]
pub struct FormatRegistry {
    /// Base directory for format definitions
    base_dir: Option<PathBuf>,
    
    /// Formats by name and version
    formats: Arc<RwLock<HashMap<String, HashMap<String, Arc<FormatSchema>>>>>,
    
    /// Format metadata by name and version
    metadata: Arc<RwLock<HashMap<String, HashMap<String, FormatMetadata>>>>,
    
    /// Default formats for auto-detection
    default_formats: Arc<RwLock<Vec<(String, String)>>>,
}

impl Default for FormatRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl FormatRegistry {
    /// Create a new format registry
    pub fn new() -> Self {
        Self {
            base_dir: None,
            formats: Arc::new(RwLock::new(HashMap::new())),
            metadata: Arc::new(RwLock::new(HashMap::new())),
            default_formats: Arc::new(RwLock::new(Vec::new())),
        }
    }
    
    /// Create a new format registry with a base directory
    pub fn with_base_dir<P: AsRef<Path>>(base_dir: P) -> Self {
        Self {
            base_dir: Some(base_dir.as_ref().to_path_buf()),
            formats: Arc::new(RwLock::new(HashMap::new())),
            metadata: Arc::new(RwLock::new(HashMap::new())),
            default_formats: Arc::new(RwLock::new(Vec::new())),
        }
    }
    
    /// Initialize the registry
    pub fn initialize(&self) -> Result<(), RegistryError> {
        info!("Initializing format registry");
        
        // Load format definitions from base directory
        if let Some(base_dir) = &self.base_dir {
            if base_dir.exists() {
                info!("Loading format definitions from {}", base_dir.display());
                self.load_formats_from_directory(base_dir)?;
            } else {
                warn!("Format directory {} does not exist", base_dir.display());
            }
        }
        
        // Register built-in formats
        self.register_builtin_formats()?;
        
        // Log initialization status
        let formats = self.formats.read().map_err(|_| RegistryError::LockError)?;
        let format_count = formats.values().map(|v| v.len()).sum::<usize>();
        info!("Format registry initialized with {} format definitions", format_count);
        
        Ok(())
    }
    
    /// Register built-in formats
    fn register_builtin_formats(&self) -> Result<(), RegistryError> {
        // Register standard JSON format
        let standard_json = r#"{
            "format_name": "standard_json",
            "version": "1.0.0",
            "description": "Standard JSON format for gNode commands",
            "content_type": "application/json",
            "binary": false,
            "detection_patterns": [
                {
                    "pattern_type": "prefix",
                    "pattern": "{",
                    "confidence": 0.7
                },
                {
                    "pattern_type": "regex",
                    "pattern": "^\\s*\\{",
                    "confidence": 0.8
                }
            ],
            "field_mapping": {
                "id": "id",
                "command": "command",
                "parameters": "parameters",
                "site_id": "site_id",
                "node_id": "node_id",
                "timestamp": "timestamp",
                "batch_id": "batch_id",
                "sequence": "sequence"
            },
            "schema": {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "required": ["id", "command", "parameters", "site_id", "node_id", "timestamp"],
                "properties": {
                    "id": {
                        "type": "string",
                        "description": "Unique identifier for the command"
                    },
                    "command": {
                        "type": "string",
                        "description": "Command name to execute"
                    },
                    "parameters": {
                        "type": "object",
                        "description": "Command parameters"
                    },
                    "site_id": {
                        "type": "string",
                        "description": "Site identifier for namespacing"
                    },
                    "node_id": {
                        "type": "string",
                        "description": "Node identifier"
                    },
                    "timestamp": {
                        "type": "number",
                        "description": "Unix timestamp in seconds with millisecond precision"
                    },
                    "batch_id": {
                        "type": "string",
                        "description": "Optional batch identifier for batch operations"
                    },
                    "sequence": {
                        "type": "integer",
                        "description": "Optional sequence number for ordered processing"
                    }
                }
            }
        }"#;
        
        // Register RESP3 format
        let resp3_format = r#"{
            "format_name": "resp3",
            "version": "1.0.0",
            "description": "RESP3 protocol format for gNode commands",
            "content_type": "application/resp3",
            "binary": false,
            "detection_patterns": [
                {
                    "pattern_type": "prefix",
                    "pattern": "*",
                    "confidence": 0.9
                },
                {
                    "pattern_type": "prefix",
                    "pattern": "+",
                    "confidence": 0.7
                },
                {
                    "pattern_type": "prefix",
                    "pattern": ":",
                    "confidence": 0.7
                },
                {
                    "pattern_type": "prefix",
                    "pattern": "$",
                    "confidence": 0.7
                }
            ],
            "field_mapping": {
                "i": "id",
                "c": "command",
                "p": "parameters",
                "ss": "site_id",
                "sn": "node_id",
                "ts": {
                    "target": "timestamp",
                    "transform": "timestamp_ms"
                },
                "bi": "batch_id",
                "sq": "sequence"
            }
        }"#;
        
        // Register compact JSON format
        let compact_json = r#"{
            "format_name": "compact_json",
            "version": "1.0.0",
            "description": "Compact JSON format for bandwidth-constrained environments",
            "content_type": "application/json",
            "binary": false,
            "detection_patterns": [
                {
                    "pattern_type": "regex",
                    "pattern": "^\\s*\\{\\s*\"i\":",
                    "confidence": 0.9
                }
            ],
            "field_mapping": {
                "i": "id",
                "c": "command",
                "p": "parameters",
                "s": "site_id",
                "n": "node_id",
                "t": {
                    "target": "timestamp",
                    "transform": "timestamp_s"
                },
                "b": "batch_id",
                "q": "sequence"
            },
            "defaults": {
                "timestamp": 0.0
            }
        }"#;
        
        // Parse and register formats
        let json_schema = serde_json::from_str::<Value>(standard_json)?;
        self.register_format(&json_schema)?;
        
        let resp3_schema = serde_json::from_str::<Value>(resp3_format)?;
        self.register_format(&resp3_schema)?;
        
        let compact_schema = serde_json::from_str::<Value>(compact_json)?;
        self.register_format(&compact_schema)?;

        // No explicit default-format push needed: register_format_with_metadata
        // already registers each format (json, resp3, compact — in that order)
        // as an auto-detection candidate.

        Ok(())
    }
    
    /// Load formats from a directory
    fn load_formats_from_directory<P: AsRef<Path>>(&self, dir: P) -> Result<(), RegistryError> {
        let dir = dir.as_ref();
        
        // Check if directory exists
        if !dir.exists() || !dir.is_dir() {
            return Err(RegistryError::IO(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Directory not found: {}", dir.display())
            )));
        }
        
        // Read all files in the directory
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            
            if path.is_file() {
                // Check file extension
                if let Some(ext) = path.extension() {
                    let ext_str = ext.to_string_lossy().to_lowercase();
                    
                    match ext_str.as_str() {
                        "json" => {
                            // Load JSON format
                            self.load_json_format(&path)?;
                        },
                        "yaml" | "yml" => {
                            // Load YAML format
                            self.load_yaml_format(&path)?;
                        },
                        _ => {
                            // Ignore other file types
                            continue;
                        }
                    }
                }
            } else if path.is_dir() {
                // Recursively load formats from subdirectories
                self.load_formats_from_directory(&path)?;
            }
        }
        
        Ok(())
    }
    
    /// Load a JSON format definition
    fn load_json_format<P: AsRef<Path>>(&self, path: P) -> Result<(), RegistryError> {
        let path = path.as_ref();
        debug!("Loading JSON format from {}", path.display());
        
        // Read the file
        let mut file = File::open(path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        
        // Parse JSON
        let value: Value = serde_json::from_str(&contents)?;
        
        // Register format with path
        self.register_format_with_path(&value, path.to_string_lossy().to_string())?;
        
        Ok(())
    }
    
    /// Load a YAML format definition
    fn load_yaml_format<P: AsRef<Path>>(&self, path: P) -> Result<(), RegistryError> {
        let path = path.as_ref();
        debug!("Loading YAML format from {}", path.display());
        
        // Read the file
        let mut file = File::open(path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        
        // Parse YAML
        let value = match serde_yaml::from_str::<Value>(&contents) {
            Ok(v) => v,
            Err(e) => return Err(RegistryError::Yaml(e.to_string())),
        };
        
        // Register format with path
        self.register_format_with_path(&value, path.to_string_lossy().to_string())?;
        
        Ok(())
    }
    
    /// Register a format from a JSON value with a file path
    fn register_format_with_path(&self, value: &Value, path: String) -> Result<(), RegistryError> {
        // Create format schema
        let schema = FormatSchema::new(value)?;
        
        // Get definition
        let definition = schema.get_definition();
        
        // Create metadata with path
        let metadata = FormatMetadata {
            name: definition.format_name.clone(),
            version: definition.version.clone(),
            description: definition.description.clone(),
            content_type: definition.content_type.clone(),
            binary: definition.binary,
            path: Some(path),
            last_modified: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        };
        
        // Register format
        self.register_format_with_metadata(Arc::new(schema), metadata)?;
        
        Ok(())
    }
    
    /// Register a format from a JSON value
    pub fn register_format(&self, value: &Value) -> Result<(), RegistryError> {
        // Create format schema
        let schema = FormatSchema::new(value)?;

        // Get definition
        let definition = schema.get_definition();

        // Create metadata without path
        let metadata = FormatMetadata::from_definition(definition, None);

        // Register format
        self.register_format_with_metadata(Arc::new(schema), metadata)?;

        Ok(())
    }
    
    /// Register a format from a definition
    pub fn register_format_definition(&self, definition: FormatDefinition) -> Result<(), RegistryError> {
        // Create format schema
        let schema = FormatSchema::from_definition(definition.clone())?;
        
        // Create metadata without path
        let metadata = FormatMetadata::from_definition(&definition, None);
        
        // Register format
        self.register_format_with_metadata(Arc::new(schema), metadata)?;
        
        Ok(())
    }
    
    /// Register a format with metadata
    fn register_format_with_metadata(&self, schema: Arc<FormatSchema>, metadata: FormatMetadata) -> Result<(), RegistryError> {
        let name = metadata.name.clone();
        let version = metadata.version.clone();

        debug!("Registering format {} version {}", name, version);

        // Add to formats registry (guard scoped so it is released before
        // set_default_format re-acquires a read lock below).
        {
            let mut formats = self.formats.write().map_err(|_| RegistryError::LockError)?;
            let versions = formats.entry(name.clone()).or_insert_with(HashMap::new);

            // Check if version already exists
            if versions.contains_key(&version) {
                warn!("Format {} version {} already registered, overwriting", name, version);
            }

            versions.insert(version.clone(), schema);
        }

        // Add to metadata registry
        {
            let mut metadata_map = self.metadata.write().map_err(|_| RegistryError::LockError)?;
            let version_meta = metadata_map.entry(name.clone()).or_insert_with(HashMap::new);
            version_meta.insert(version.clone(), metadata);
        }

        // Every registered format becomes an auto-detection candidate.
        // detect_format only scans default_formats, so without this a
        // custom-registered format could never be detected — only the
        // built-ins were. set_default_format dedups by name (replacing the
        // version), so re-registering a format is idempotent here.
        self.set_default_format(&name, &version)?;

        Ok(())
    }
    
    /// Get a format by name and version
    pub fn get_format(&self, name: &str, version: Option<&str>) -> Result<Arc<FormatSchema>, RegistryError> {
        let formats = self.formats.read().map_err(|_| RegistryError::LockError)?;
        
        // Get versions for this format
        let versions = match formats.get(name) {
            Some(v) => v,
            None => return Err(RegistryError::NotFound(format!("Format not found: {}", name))),
        };
        
        // If version is specified, get that version
        if let Some(version) = version {
            match versions.get(version) {
                Some(format) => Ok(Arc::clone(format)),
                None => Err(RegistryError::VersionNotFound(format!("Version {} not found for format {}", version, name))),
            }
        } else {
            // Get the latest version
            let latest_version = self.get_latest_version(versions.keys())?;
            
            match versions.get(&latest_version) {
                Some(format) => Ok(Arc::clone(format)),
                None => Err(RegistryError::VersionNotFound(format!("Latest version not found for format {}", name))),
            }
        }
    }
    
    /// Get the latest version from a set of version strings
    fn get_latest_version<'a, I>(&self, versions: I) -> Result<String, RegistryError>
    where
        I: Iterator<Item = &'a String>,
    {
        let mut latest: Option<(Version, String)> = None;
        
        for version_str in versions {
            // Parse version
            match Version::parse(version_str) {
                Ok(version) => {
                    // Compare with latest
                    if let Some((latest_ver, _)) = &latest {
                        if version > *latest_ver {
                            latest = Some((version, version_str.clone()));
                        }
                    } else {
                        latest = Some((version, version_str.clone()));
                    }
                },
                Err(_) => {
                    // If not semver, use string comparison
                    if let Some((_, latest_str)) = &latest {
                        if version_str > latest_str {
                            latest = Some((Version::new(0, 0, 0), version_str.clone()));
                        }
                    } else {
                        latest = Some((Version::new(0, 0, 0), version_str.clone()));
                    }
                }
            }
        }
        
        match latest {
            Some((_, version_str)) => Ok(version_str),
            None => Err(RegistryError::VersionNotFound("No versions found".to_string())),
        }
    }
    
    /// Get all format metadata
    pub fn get_all_metadata(&self) -> Result<Vec<FormatMetadata>, RegistryError> {
        let metadata_map = self.metadata.read().map_err(|_| RegistryError::LockError)?;
        let mut result = Vec::new();
        
        for versions in metadata_map.values() {
            for metadata in versions.values() {
                result.push(metadata.clone());
            }
        }
        
        Ok(result)
    }
    
    /// Get metadata for a specific format
    pub fn get_format_metadata(&self, name: &str, version: Option<&str>) -> Result<FormatMetadata, RegistryError> {
        let metadata_map = self.metadata.read().map_err(|_| RegistryError::LockError)?;
        
        // Get versions for this format
        let versions = match metadata_map.get(name) {
            Some(v) => v,
            None => return Err(RegistryError::NotFound(format!("Format not found: {}", name))),
        };
        
        // If version is specified, get that version
        if let Some(version) = version {
            match versions.get(version) {
                Some(metadata) => Ok(metadata.clone()),
                None => Err(RegistryError::VersionNotFound(format!("Version {} not found for format {}", version, name))),
            }
        } else {
            // Get the latest version
            let latest_version = self.get_latest_version(versions.keys())?;
            
            match versions.get(&latest_version) {
                Some(metadata) => Ok(metadata.clone()),
                None => Err(RegistryError::VersionNotFound(format!("Latest version not found for format {}", name))),
            }
        }
    }
    
    /// Set a format as default for auto-detection
    pub fn set_default_format(&self, name: &str, version: &str) -> Result<(), RegistryError> {
        // Verify format exists
        self.get_format(name, Some(version))?;
        
        // Add to default formats
        let mut defaults = self.default_formats.write().map_err(|_| RegistryError::LockError)?;
        
        // Check if already in defaults
        for (i, (default_name, _default_version)) in defaults.iter().enumerate() {
            if default_name == name {
                // Replace version
                defaults[i] = (name.to_string(), version.to_string());
                return Ok(());
            }
        }
        
        // Add new default
        defaults.push((name.to_string(), version.to_string()));
        
        Ok(())
    }
    
    /// Get default formats for auto-detection
    pub fn get_default_formats(&self) -> Result<Vec<(String, String)>, RegistryError> {
        let defaults = self.default_formats.read().map_err(|_| RegistryError::LockError)?;
        Ok(defaults.clone())
    }
    
    /// Detect format for a message
    pub fn detect_format(&self, message: &[u8]) -> Result<Option<(String, String, f64)>, RegistryError> {
        // Get default formats
        let defaults = self.default_formats.read().map_err(|_| RegistryError::LockError)?;
        
        // Try each default format
        let mut best_match: Option<(String, String, f64)> = None;
        
        for (name, version) in defaults.iter() {
            // Get format
            match self.get_format(name, Some(version)) {
                Ok(format) => {
                    // Check if this format can parse the message
                    let confidence = format.confidence_score(message);
                    
                    if confidence > 0.0 {
                        // Update best match if this has higher confidence
                        if let Some((_, _, best_confidence)) = &best_match {
                            if confidence > *best_confidence {
                                best_match = Some((name.clone(), version.clone(), confidence));
                            }
                        } else {
                            best_match = Some((name.clone(), version.clone(), confidence));
                        }
                    }
                },
                Err(_) => {
                    // Skip this format
                    continue;
                }
            }
        }
        
        Ok(best_match)
    }
    
    /// Save a format definition to disk
    pub fn save_format(&self, name: &str, version: Option<&str>) -> Result<String, RegistryError> {
        // Get format
        let format = self.get_format(name, version)?;
        let definition = format.get_definition();
        
        // Check if base_dir is set
        if self.base_dir.is_none() {
            return Err(RegistryError::IO(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Base directory not set"
            )));
        }
        
        // Create filename
        let version_str = version.unwrap_or(&definition.version);
        let filename = format!("{}-{}.json", name, version_str);
        let path = match self.base_dir.as_ref() {
            Some(dir) => dir.join(filename),
            None => return Err(RegistryError::IO(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Base directory not set"
            ))),
        };
        
        // Create JSON
        let json = serde_json::to_string_pretty(&definition)
            .map_err(RegistryError::Json)?;
        
        // Write to file
        fs::write(&path, json)?;

        // Update metadata
        let mut metadata_map = self.metadata.write().map_err(|_| RegistryError::LockError)?;
        if let Some(versions) = metadata_map.get_mut(name) {
            if let Some(metadata) = versions.get_mut(version_str) {
                metadata.path = Some(path.to_string_lossy().to_string());
                metadata.last_modified = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
            }
        }

        Ok(path.to_string_lossy().to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::template::format::{FormatParser, FormatSerializer, FormatTransformer};
    use serde_json::json;

    fn registry_with_builtins() -> Arc<FormatRegistry> {
        let reg = Arc::new(FormatRegistry::new());
        reg.initialize().expect("builtin registry init");
        reg
    }

    #[test]
    fn detect_picks_highest_confidence() {
        let reg = registry_with_builtins();
        // standard_json matches "{" (0.8 via regex); compact_json matches the
        // tighter {"i": pattern (0.9) and should win.
        let (name, _ver, conf) = reg
            .detect_format(br#"{"i":"x","c":"ping"}"#)
            .unwrap()
            .expect("a format should be detected");
        assert_eq!(name, "compact_json");
        assert!((conf - 0.9).abs() < f64::EPSILON);

        // A plain object that does not match the compact pattern -> standard.
        let (name, _v, _c) = reg
            .detect_format(br#"{"id":"x"}"#)
            .unwrap()
            .expect("standard json detected");
        assert_eq!(name, "standard_json");
    }

    #[test]
    fn detect_tie_break_favours_first_registered() {
        let reg = registry_with_builtins();
        // Two custom formats with an identical pattern + confidence; the first
        // registered must win the tie (detect uses a strict > comparison).
        for n in ["tie_a", "tie_b"] {
            reg.register_format(&json!({
                "format_name": n,
                "version": "1.0.0",
                "content_type": "application/json",
                "detection_patterns": [{"pattern_type": "prefix", "pattern": "~", "confidence": 0.95}],
                "field_mapping": {}
            })).unwrap();
        }
        let (name, _v, _c) = reg.detect_format(b"~payload").unwrap().expect("tie detected");
        assert_eq!(name, "tie_a");
    }

    #[test]
    fn custom_format_becomes_detectable() {
        // The ph4 fix: a freshly registered custom format must be added to the
        // auto-detection set, not only the built-ins.
        let reg = registry_with_builtins();
        reg.register_format(&json!({
            "format_name": "pipe_fmt",
            "version": "2.1.0",
            "content_type": "application/json",
            "detection_patterns": [{"pattern_type": "prefix", "pattern": "|>", "confidence": 0.99}],
            "field_mapping": {}
        })).unwrap();

        let (name, ver, _c) = reg.detect_format(b"|> hello").unwrap().expect("custom detected");
        assert_eq!(name, "pipe_fmt");
        assert_eq!(ver, "2.1.0");
    }

    #[test]
    fn parse_standard_json_validates_schema() {
        let reg = registry_with_builtins();
        let parser = FormatParser::new(Arc::clone(&reg));
        let good = br#"{"id":"x","command":"ping","parameters":{},"site_id":"s","node_id":"n","timestamp":1.5}"#;
        let internal = parser.parse_with_format("standard_json", Some("1.0.0"), good).unwrap();
        assert_eq!(internal["command"], json!("ping"));

        // Missing required fields must be rejected by schema validation.
        let bad = br#"{"id":"x"}"#;
        assert!(parser.parse_with_format("standard_json", Some("1.0.0"), bad).is_err());
    }

    #[test]
    fn parse_compact_json_applies_timestamp_transform() {
        let reg = registry_with_builtins();
        let parser = FormatParser::new(Arc::clone(&reg));
        let internal = parser
            .parse_with_format("compact_json", Some("1.0.0"), br#"{"i":"x","c":"ping","t":1.5}"#)
            .unwrap();
        assert_eq!(internal["id"], json!("x"));
        assert_eq!(internal["command"], json!("ping"));
        assert_eq!(internal["timestamp"].as_f64().unwrap(), 1.5);
    }

    #[test]
    fn parse_resp3_flat_message() {
        let reg = registry_with_builtins();
        let parser = FormatParser::new(Arc::clone(&reg));
        let msg = b"*4\r\n$1\r\ni\r\n$3\r\nabc\r\n$1\r\nc\r\n$4\r\nping\r\n";
        let internal = parser.parse_with_format("resp3", Some("1.0.0"), msg).unwrap();
        assert_eq!(internal["id"], json!("abc"));
        assert_eq!(internal["command"], json!("ping"));
    }

    #[test]
    fn convert_standard_to_compact_round_trips_fields() {
        let reg = registry_with_builtins();
        let transformer = FormatTransformer::new(Arc::clone(&reg));
        let parser = FormatParser::new(Arc::clone(&reg));

        let source = br#"{"id":"x","command":"ping","parameters":{},"site_id":"s","node_id":"n","timestamp":1.5}"#;
        let compact = transformer
            .transform_from_to(source, "standard_json", Some("1.0.0"), "compact_json", Some("1.0.0"))
            .unwrap();

        // Re-parse the compact output and confirm the canonical fields survived.
        let internal = parser.parse_with_format("compact_json", Some("1.0.0"), &compact).unwrap();
        assert_eq!(internal["id"], json!("x"));
        assert_eq!(internal["command"], json!("ping"));
        assert_eq!(internal["timestamp"].as_f64().unwrap(), 1.5);
    }

    #[test]
    fn resp3_serializes_nested_array_as_real_aggregate() {
        // The ph4 RESP3-unify fix: nested arrays must be emitted as RESP3
        // aggregates (*N + typed elements), not as a JSON-encoded bulk string.
        let reg = registry_with_builtins();
        reg.register_format(&json!({
            "format_name": "resp3_pass",
            "version": "1.0.0",
            "content_type": "application/resp3",
            "field_mapping": {}
        })).unwrap();
        let serializer = FormatSerializer::new(Arc::clone(&reg));

        let value = json!({"arr": [1, 2, 3], "flag": true});
        let bytes = serializer.serialize(&value, "resp3_pass", Some("1.0.0")).unwrap();
        let out = String::from_utf8(bytes).unwrap();

        assert!(out.contains("*3\r\n:1\r\n:2\r\n:3\r\n"), "array not emitted as RESP3 aggregate: {:?}", out);
        assert!(out.contains("#t\r\n"), "bool not emitted as RESP3 boolean: {:?}", out);
        assert!(!out.contains("[1,2,3]"), "array wrongly emitted as JSON string: {:?}", out);
    }

    #[test]
    fn transformer_resp3_matches_serializer() {
        // transform_value now delegates to the serializer, so both paths must
        // produce byte-identical RESP3.
        let reg = registry_with_builtins();
        let transformer = FormatTransformer::new(Arc::clone(&reg));
        let serializer = FormatSerializer::new(Arc::clone(&reg));

        let source = br#"{"id":"x","command":"ping","parameters":{},"site_id":"s","node_id":"n","timestamp":1.5}"#;
        let via_transformer = transformer
            .transform_from_to(source, "standard_json", Some("1.0.0"), "resp3", Some("1.0.0"))
            .unwrap();

        // Mirror the transformer's internal step: parse source -> internal,
        // then serialize internal to resp3 directly.
        let parser = FormatParser::new(Arc::clone(&reg));
        let internal = parser.parse_with_format("standard_json", Some("1.0.0"), source).unwrap();
        let via_serializer = serializer.serialize(&internal, "resp3", Some("1.0.0")).unwrap();

        assert_eq!(via_transformer, via_serializer);
    }
}