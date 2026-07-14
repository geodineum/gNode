use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::fs;
use redis::{Client, Connection, RedisResult};
use log::{info, debug, warn, error};

use crate::{Result, GeometricError};

/// ValKey Function Manager
///
/// Manages loading, registering, and executing ValKey functions
pub struct ValKeyFunctionManager {
    /// ValKey client
    client: Client,
    
    /// Path to function directory
    function_dir: PathBuf,
    
    /// Site ID for namespacing
    site_id: String,
    
    /// Debug mode
    debug: bool,
    
    /// Loaded function list
    functions: HashMap<String, String>,
}

impl ValKeyFunctionManager {
    /// Create a new ValKey function manager
    pub fn new(
        client: Client,
        function_dir: &Path,
        site_id: &str,
        debug: bool
    ) -> Result<Self> {
        // Check if function directory exists
        if !function_dir.exists() {
            warn!("ValKey function directory does not exist: {:?}", function_dir);
        } else if debug {
            // Log found functions
            if let Ok(entries) = std::fs::read_dir(function_dir) {
                let functions: Vec<_> = entries
                    .filter_map(|e| e.ok())
                    .filter(|entry| {
                        let path = entry.path();
                        path.is_file() && path.extension().is_some_and(|ext| ext == "lua")
                    })
                    .map(|entry| entry.file_name().to_string_lossy().to_string())
                    .collect();
                
                if !functions.is_empty() {
                    info!("Found {} ValKey function files: {}", functions.len(), functions.join(", "));
                } else {
                    warn!("No ValKey function files found in directory");
                }
            }
        }
        
        let manager = Self {
            client,
            function_dir: function_dir.to_path_buf(),
            site_id: site_id.to_string(),
            debug,
            functions: HashMap::new(),
        };
        
        Ok(manager)
    }
    
    /// Load and register all ValKey functions
    pub fn load_functions(&mut self) -> Result<HashMap<String, String>> {
        info!("Loading ValKey functions from {:?}", self.function_dir);
        
        // Get connection to ValKey
        let mut conn = self.client.get_connection()
            .map_err(GeometricError::Redis)?;
        
        // List all Lua files in the function directory
        let mut function_paths = Vec::new();
        if self.function_dir.exists() {
            if let Ok(entries) = fs::read_dir(&self.function_dir) {
                for entry in entries.filter_map(|e| e.ok()) {
                    let path = entry.path();
                    if path.is_file() && path.extension().is_some_and(|ext| ext == "lua") {
                        function_paths.push(path);
                    }
                }
            }
        }
        
        if function_paths.is_empty() {
            warn!("No ValKey function files found in {:?}", self.function_dir);
            return Ok(HashMap::new());
        }
        
        // Store function registry in ValKey
        let registry_key = format!("{{{0}}}:gcore:gnode:valkey_functions", self.site_id);
        
        // Clear existing registry
        let _: RedisResult<()> = redis::cmd("DEL")
            .arg(&registry_key)
            .query(&mut conn);
        
        // Store metadata
        let metadata = serde_json::json!({
            "version": env!("CARGO_PKG_VERSION"),
            "loaded_at": std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            "territory": self.site_id,
        });
        
        let _: RedisResult<()> = redis::cmd("HSET")
            .arg(&registry_key)
            .arg("_metadata")
            .arg(metadata.to_string())
            .query(&mut conn);
        
        // Process each function file
        for path in function_paths {
            let function_name = path.file_stem()
                .and_then(|stem| stem.to_str())
                .unwrap_or_default();
            
            // Skip unsupported files
            if function_name.is_empty() {
                continue;
            }
            
            // Normalize function name
            let library_name: String = match function_name {
                "gnode_stream" => "GNODE_STREAM".to_string(),
                "gnode_geometric" => "GNODE_GEOMETRIC".to_string(),
                "gnode_core" => "GNODE_CORE".to_string(),
                "gnode_batch" => "GNODE_BATCH".to_string(),
                "gnode_hash" => "GNODE_HASH".to_string(),
                _ => {
                    let upper = function_name.to_uppercase();
                    if upper.starts_with("GNODE_") {
                        upper
                    } else {
                        continue; // Skip non-gNode files
                    }
                }
            };
            
            // Read function content
            let function_content = match fs::read_to_string(&path) {
                Ok(content) => content,
                Err(e) => {
                    warn!("Failed to read ValKey function file {:?}: {}", path, e);
                    continue;
                }
            };
            
            // Register function with ValKey
            let load_result: RedisResult<String> = redis::cmd("FUNCTION")
                .arg("LOAD")
                .arg("REPLACE")
                .arg(&function_content)
                .query(&mut conn);
            
            match load_result {
                Ok(_) => {
                    info!("Successfully loaded ValKey function: {}", library_name);
                    
                    // Register in local hashmap
                    self.functions.insert(library_name.to_string(), "loaded".to_string());
                    
                    // Register in ValKey
                    let _: RedisResult<()> = redis::cmd("HSET")
                        .arg(&registry_key)
                        .arg(library_name)
                        .arg("loaded")
                        .query(&mut conn);
                },
                Err(e) => {
                    error!("Failed to load ValKey function {}: {}", library_name, e);
                }
            }
        }
        
        info!("Loaded {} ValKey functions", self.functions.len());
        
        // List loaded functions in debug mode
        if self.debug {
            let list_result: RedisResult<Vec<String>> = redis::cmd("FUNCTION")
                .arg("LIST")
                .query(&mut conn);
            
            match list_result {
                Ok(functions) => {
                    debug!("ValKey functions currently loaded:");
                    for func in functions {
                        debug!("  {}", func);
                    }
                },
                Err(e) => warn!("Failed to list ValKey functions: {}", e)
            }
        }
        
        Ok(self.functions.clone())
    }
    
    /// Check if a specific function is available
    pub fn is_function_available(&self, name: &str) -> bool {
        self.functions.contains_key(name)
    }
    
    /// Call a ValKey function
    pub fn call_function(
        &self,
        conn: &mut Connection,
        function_name: &str,
        keys: &[&str],
        args: &[&str]
    ) -> Result<String> {
        if !self.is_function_available(function_name) {
            return Err(GeometricError::InvalidState(format!(
                "ValKey function not loaded: {}", function_name
            )));
        }
        
        let mut cmd = redis::cmd("FCALL");
        cmd.arg(function_name).arg(keys.len());
        
        // Add keys
        for key in keys {
            cmd.arg(key);
        }
        
        // Add args
        for arg in args {
            cmd.arg(arg);
        }
        
        // Execute function
        let result: RedisResult<String> = cmd.query(conn);
        
        match result {
            Ok(output) => Ok(output),
            Err(e) => Err(GeometricError::Redis(e))
        }
    }
    
    /// Get the currently loaded functions
    pub fn get_functions(&self) -> &HashMap<String, String> {
        &self.functions
    }
    
    /// Set the function map with a new collection of functions
    pub fn set_functions(&mut self, functions: HashMap<String, String>) {
        self.functions = functions;
    }
    
    /// Check if the manager is initialized
    pub fn is_initialized(&self) -> bool {
        !self.functions.is_empty()
    }
    
    /// Set the initialized state
    pub fn set_initialized(&mut self, function_names: Vec<String>) {
        // Create a HashMap with function names as keys and empty strings as values
        let mut new_functions = HashMap::new();
        for name in function_names {
            new_functions.insert(name, String::new());
        }
        self.functions = new_functions;
    }
}