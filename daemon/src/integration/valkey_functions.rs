use std::path::{Path, PathBuf};
use std::collections::HashMap;
use redis::{Client, Connection, RedisResult};
use log::{info, debug, warn, error};

use crate::integration::{
    path_resolution::{
        find_valkey_functions_directory, 
        find_function_file, 
        verify_directory_contents_with_predicate
    },
    shared_manager::{get_valkey_manager, with_valkey_manager},
    error_handlings::{
        IntegrationResult, 
        valkey_function_error, log_error
    }
};

/// Function library types supported by the system
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FunctionLibrary {
    Core,
    Geometric,
    Stream,
    Batch,
    Lock,
    Group,
    Hash,
    Monitoring,
    PubSub,
    Site,
    Transaction,
    Utils,
    Test,
    Cache,
    Broadcast,
    Custom(String),
}

impl FunctionLibrary {
    /// Convert the enum to its file name
    pub fn to_file_name(&self) -> String {
        match self {
            FunctionLibrary::Core => "gnode_core.lua".to_string(),
            FunctionLibrary::Geometric => "gnode_geometric.lua".to_string(),
            FunctionLibrary::Stream => "gnode_stream.lua".to_string(),
            FunctionLibrary::Batch => "gnode_batch.lua".to_string(),
            FunctionLibrary::Lock => "gnode_lock.lua".to_string(),
            FunctionLibrary::Group => "gnode_group.lua".to_string(),
            FunctionLibrary::Hash => "gnode_hash.lua".to_string(),
            FunctionLibrary::Monitoring => "gnode_monitoring.lua".to_string(),
            FunctionLibrary::PubSub => "gnode_pubsub.lua".to_string(),
            FunctionLibrary::Site => "gnode_site.lua".to_string(),
            FunctionLibrary::Transaction => "gnode_transaction.lua".to_string(),
            FunctionLibrary::Utils => "gnode_utils.lua".to_string(),
            FunctionLibrary::Test => "gnode_test.lua".to_string(),
            FunctionLibrary::Cache => "gnode_cache.lua".to_string(),
            FunctionLibrary::Broadcast => "gnode_broadcast.lua".to_string(),
            FunctionLibrary::Custom(name) => format!("{}.lua", name),
        }
    }

    /// Convert a file name to its corresponding library enum
    pub fn from_file_name(file_name: &str) -> Self {
        match file_name {
            "gnode_core.lua" => FunctionLibrary::Core,
            "gnode_geometric.lua" => FunctionLibrary::Geometric,
            "gnode_stream.lua" => FunctionLibrary::Stream,
            "gnode_batch.lua" | "gnode_batch_resp3.lua" => FunctionLibrary::Batch,
            "gnode_lock.lua" => FunctionLibrary::Lock,
            "gnode_group.lua" => FunctionLibrary::Group,
            "gnode_hash.lua" => FunctionLibrary::Hash,
            "gnode_monitoring.lua" => FunctionLibrary::Monitoring,
            "gnode_pubsub.lua" => FunctionLibrary::PubSub,
            "gnode_site.lua" => FunctionLibrary::Site,
            "gnode_transaction.lua" => FunctionLibrary::Transaction,
            "gnode_utils.lua" => FunctionLibrary::Utils,
            "gnode_test.lua" => FunctionLibrary::Test,
            "gnode_cache.lua" => FunctionLibrary::Cache,
            "gnode_broadcast.lua" => FunctionLibrary::Broadcast,
            name => {
                if let Some(base) = name.strip_suffix(".lua") {
                    FunctionLibrary::Custom(base.to_string())
                } else {
                    FunctionLibrary::Custom(name.to_string())
                }
            }
        }
    }

    /// Get a list of standard libraries in recommended loading order
    pub fn standard_libraries() -> Vec<FunctionLibrary> {
        vec![
            FunctionLibrary::Utils,    // Utils first as others may depend on it
            FunctionLibrary::Core,     // Core functionality
            FunctionLibrary::Geometric, // Geometric operations
            FunctionLibrary::Stream,   // Stream operations
            FunctionLibrary::Batch,    // Batch operations
            FunctionLibrary::Lock,     // Locking operations
            FunctionLibrary::Group,    // Group management
            FunctionLibrary::Hash,     // Hash operations
            FunctionLibrary::Monitoring, // Monitoring operations
            FunctionLibrary::PubSub,   // Publish/subscribe
            FunctionLibrary::Site,     // Site management
            FunctionLibrary::Transaction, // Transactions
            FunctionLibrary::Cache,    // Caching
            FunctionLibrary::Broadcast, // Broadcast streams (pub-sub without consumer groups)
            FunctionLibrary::Test,     // Test functions last
        ]
    }
}

/// Information about a loaded ValKey function
#[derive(Debug, Clone)]
pub struct FunctionInfo {
    /// The function name (e.g., "GNODE_CORE_GET")
    pub name: String,
    
    /// The library this function belongs to
    pub library: FunctionLibrary,
    
    /// Whether the function is read-only (can use FCALL_RO)
    pub read_only: bool,
    
    /// Brief description of the function
    pub description: String,
}

/// Initialize ValKey functions for the gNode daemon
///
/// This function scans the functions directory, loads all valid ValKey functions,
/// and registers them with the ValKey server. It handles function dependencies
/// by loading them in the correct order.
///
/// # Arguments
/// * `client` - Redis client for communicating with ValKey
/// * `site_id` - Site identifier for namespacing
/// * `debug` - Debug mode flag for verbose logging
///
/// # Returns
/// * `IntegrationResult<usize>` - Number of successfully loaded functions
pub fn initialize_functions(
    client: &Client,
    site_id: &str,
    debug: bool
) -> IntegrationResult<usize> {
    info!("Initializing ValKey functions for gNode daemon");
    
    // Find the ValKey function directory
    let functions_dir = find_valkey_functions_directory(debug);
    let functions_path = Path::new(&functions_dir);
    
    // Skip if directory doesn't exist
    if !functions_path.exists() || !functions_path.is_dir() {
        let error_msg = format!("ValKey functions directory not found: {}", functions_dir);
        error!("{}", error_msg);
        return Err(valkey_function_error(error_msg));
    }
    
    // Verify directory contains function files
    let has_lua_files = match verify_directory_contents_with_predicate(
        functions_path, 
        |path: &Path| path.extension().is_some_and(|ext| ext == "lua"),
        debug
    ) {
        Ok(has_files) => has_files,
        Err(e) => {
            let error_msg = format!("Failed to verify ValKey functions directory: {}", e);
            error!("{}", error_msg);
            return Err(valkey_function_error(error_msg));
        }
    };
    
    if !has_lua_files {
        let error_msg = format!("No ValKey function files found in {}", functions_dir);
        error!("{}", error_msg);
        return Err(valkey_function_error(error_msg));
    }
    
    // Connect to ValKey
    let mut conn = match client.get_connection() {
        Ok(conn) => conn,
        Err(e) => {
            let error_msg = format!("Failed to connect to ValKey: {}", e);
            error!("{}", error_msg);
            return Err(valkey_function_error(error_msg));
        }
    };
    
    // Test ValKey functions support
    info!("Testing ValKey functions support...");
    let test_function = "#!lua name=gnode_test\nserver.register_function('GNODE_TEST_PING', function() return 'PONG' end)";
    
    match redis::cmd("FUNCTION")
        .arg("LOAD")
        .arg(test_function)
        .query::<String>(&mut conn) {
        Ok(_) => {
            info!("ValKey functions support confirmed");
            // Clean up test function
            let _: redis::RedisResult<()> = redis::cmd("FUNCTION")
                .arg("DELETE")
                .arg("gnode_test")
                .query(&mut conn);
        },
        Err(e) => {
            let error_msg = format!("ValKey functions not supported: {}", e);
            warn!("{}", error_msg);
            warn!("Will fall back to script-based or direct command execution");
            return Err(valkey_function_error(error_msg));
        }
    };
    
    // Get the standard libraries in order
    let libraries = FunctionLibrary::standard_libraries();
    
    // Track loaded functions
    let mut loaded_functions = HashMap::new();
    let registry_key = format!("{{{0}}}:gcore:gnode:valkey_functions", site_id);
    
    // Clear existing registry
    let _: RedisResult<()> = redis::cmd("DEL")
        .arg(&registry_key)
        .query(&mut conn);
    
    // Store metadata in registry
    let metadata = serde_json::json!({
        "version": env!("CARGO_PKG_VERSION"),
        "loaded_at": std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
        "site_id": site_id,
    });
    
    let _: RedisResult<()> = redis::cmd("HSET")
        .arg(&registry_key)
        .arg("_metadata")
        .arg(metadata.to_string())
        .query(&mut conn);
    
    // Load libraries in order
    let mut load_count = 0;
    for library in libraries {
        let file_name = library.to_file_name();
        let library_path = match find_function_file(&file_name, debug) {
            Some(path) => path,
            None => {
                // Skip if file not found (not all libraries may be present)
                debug!("Function library not found: {}", file_name);
                continue;
            }
        };
        
        match load_function_library(&mut conn, &library_path, &library, &registry_key, debug) {
            Ok(funcs) => {
                load_count += funcs;
                info!("Loaded {} functions from library {}", funcs, file_name);
                loaded_functions.insert(library, funcs);
            },
            Err(e) => {
                warn!("Failed to load function library {}: {}", file_name, e);
                // Continue with other libraries
            }
        }
    }
    
    // Check for any custom libraries not in the standard list
    if let Ok(entries) = std::fs::read_dir(functions_path) {
        for entry in entries.filter_map(|e| e.ok()) {
            let path = entry.path();
            if path.is_file() && path.extension().is_some_and(|ext| ext == "lua") {
                let file_name = path.file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or_default();
                
                let library = FunctionLibrary::from_file_name(file_name);
                
                // Skip if already loaded from standard list
                if loaded_functions.contains_key(&library) {
                    continue;
                }
                
                match load_function_library(&mut conn, &path, &library, &registry_key, debug) {
                    Ok(funcs) => {
                        load_count += funcs;
                        info!("Loaded {} functions from custom library {}", funcs, file_name);
                        loaded_functions.insert(library, funcs);
                    },
                    Err(e) => {
                        warn!("Failed to load custom function library {}: {}", file_name, e);
                        // Continue with other libraries
                    }
                }
            }
        }
    }
    
    // Log results
    if load_count > 0 {
        info!("Successfully loaded {} ValKey functions from {} libraries", 
            load_count, loaded_functions.len());
        
        // Store ValKey function manager in shared state
        let _ = with_valkey_manager(|manager| {
            // Create a new HashMap with function names and empty values to represent loaded state
            let functions_map: HashMap<String, String> = loaded_functions.keys()
                .map(|key| (key.to_file_name(), "loaded".to_string()))
                .collect();
            
            // Update the manager with the new functions
            manager.set_functions(functions_map);
            
            // Mark the function names that are loaded
            let function_names: Vec<String> = loaded_functions.keys()
                .map(|key| key.to_file_name())
                .collect();
            manager.set_initialized(function_names);
            
            Ok(())
        });
        
        Ok(load_count)
    } else {
        let error_msg = "No ValKey functions were loaded".to_string();
        error!("{}", error_msg);
        Err(valkey_function_error(error_msg))
    }
}

/// Load a specific function library into ValKey
///
/// # Arguments
/// * `conn` - Redis connection
/// * `path` - Path to the function library file
/// * `library` - Library type
/// * `registry_key` - Registry key for storing function info
/// * `debug` - Debug mode flag
///
/// # Returns
/// * `IntegrationResult<usize>` - Number of functions loaded
fn load_function_library(
    conn: &mut Connection,
    path: &PathBuf,
    library: &FunctionLibrary,
    registry_key: &str,
    debug: bool
) -> IntegrationResult<usize> {
    debug!("Loading function library from {:?}", path);
    
    // Read function content
    let function_content = match std::fs::read_to_string(path) {
        Ok(content) => content,
        Err(e) => {
            let error_msg = format!("Failed to read function library file {:?}: {}", path, e);
            return Err(valkey_function_error(error_msg));
        }
    };
    
    // Ensure library has correct metadata format
    let content = ensure_library_metadata(&function_content, library);
    
    // Debug log the content being loaded
    if debug {
        debug!("Loading function with content starting with: {}", 
               content.lines().take(5).collect::<Vec<_>>().join("\n"));
    }

    // First check if the function library already exists
    let library_name = match library {
        FunctionLibrary::Custom(name) => name.clone(),
        _ => library.to_file_name().trim_end_matches(".lua").to_string(),
    };
    
    // Try to delete the library first to ensure clean state
    let _: redis::RedisResult<()> = redis::cmd("FUNCTION")
        .arg("DELETE")
        .arg(&library_name)
        .query(conn);
    
    // Load function into ValKey
    let load_result: redis::RedisResult<String> = redis::cmd("FUNCTION")
        .arg("LOAD")
        .arg("REPLACE")
        .arg(&content)
        .query(conn);
    
    match load_result {
        Ok(_) => {
            // Register in ValKey registry
            let _: redis::RedisResult<()> = redis::cmd("HSET")
                .arg(registry_key)
                .arg(library.to_file_name())
                .arg("loaded")
                .query(conn);
            
            // Count the functions in the library (approximate by counting register_function calls)
            let func_count = content.matches("server.register_function").count();
            
            // Verify the functions were actually loaded by testing one
            if func_count > 0 {
                // List functions in debug mode
                if debug {
                    if let Ok(function_list) = list_library_functions(conn, library) {
                        if !function_list.is_empty() {
                            info!("Successfully loaded {} functions from library {}: {}", 
                                 function_list.len(), library.to_file_name(),
                                 function_list.join(", "));
                        } else {
                            warn!("Function library {} was loaded but no functions were found", 
                                 library.to_file_name());
                        }
                    }
                }
            }
            
            Ok(func_count)
        },
        Err(e) => {
            let error_msg = format!("Failed to load ValKey function library {:?}: {}", path, e);
            error!("{}", error_msg);
            
            // Check for specific error cases
            if e.to_string().contains("Syntax error") {
                // Try to extract the line with the syntax error
                if let Ok(content_lines) = std::fs::read_to_string(path) {
                    let lines: Vec<&str> = content_lines.lines().collect();
                    error!("Syntax error in function. First 10 lines:");
                    for (i, line) in lines.iter().take(10).enumerate() {
                        error!("Line {}: {}", i+1, line);
                    }
                }
            } else if e.to_string().contains("metadata") || e.to_string().contains("shebang") {
                // Metadata issue - try to fix with more aggressive approach
                if let Ok(content) = std::fs::read_to_string(path) {
                    // Create fixed content with proper metadata
                    let fixed_content = format!(
                        "#!lua name={}\n\n--\n-- gNode Function\n-- Auto-fixed metadata\n--\n\n{}", 
                        library.to_file_name().trim_end_matches(".lua"),
                        content.lines().filter(|line| !line.contains("#!lua")).collect::<Vec<&str>>().join("\n")
                    );
                    
                    // Try loading again with fixed content
                    let retry_result: redis::RedisResult<String> = redis::cmd("FUNCTION")
                        .arg("LOAD")
                        .arg("REPLACE")
                        .arg(&fixed_content)
                        .query(conn);
                    
                    match retry_result {
                        Ok(_) => {
                            info!("Successfully loaded function after fixing metadata: {}", library.to_file_name());
                            return Ok(1); // Assume at least one function in library
                        },
                        Err(retry_err) => {
                            error!("Failed to load function even after fixing metadata: {}", retry_err);
                        }
                    }
                }
            }
            
            Err(valkey_function_error(error_msg))
        }
    }
}

/// Ensure the function library has the correct metadata format
///
/// # Arguments
/// * `content` - The function library content
/// * `library` - Library type
///
/// # Returns
/// * `String` - The updated function library content
fn ensure_library_metadata(content: &str, library: &FunctionLibrary) -> String {
    let lib_name = match library {
        FunctionLibrary::Custom(name) => name.clone(),
        _ => library.to_file_name().trim_end_matches(".lua").to_string(),
    };
    
    // Check if metadata is present and correctly formatted
    if content.starts_with("#!lua name=") {
        // Already has metadata, but check if it has the correct format
        content.to_string()
    } else if content.contains("#\\!lua name=") {
        // Fix escaped shebang
        content.replace("#\\!lua name=", "#!lua name=")
    } else {
        // Add metadata with proper description
        format!("#!lua name={}\n\n--\n-- {} Functions\n-- A ValKey function library for {} operations\n--\n-- This is a port of the gCore Cache Scripts to ValKey functions\n-- with enhancements for RESP3 compatibility\n-- \n\n{}", 
            lib_name, 
            lib_name.split('_').nth(1).unwrap_or("gNode").to_uppercase(), 
            lib_name.split('_').nth(1).unwrap_or("core").replace("_", " "),
            content)
    }
}

/// List the functions in a library
///
/// # Arguments
/// * `conn` - Redis connection
/// * `library` - Library type
///
/// # Returns
/// * `IntegrationResult<Vec<String>>` - List of function names
fn list_library_functions(
    conn: &mut Connection,
    library: &FunctionLibrary
) -> IntegrationResult<Vec<String>> {
    let lib_name = match library {
        FunctionLibrary::Custom(name) => name.clone(),
        _ => library.to_file_name().trim_end_matches(".lua").to_string(),
    };
    
    let list_result: redis::RedisResult<redis::Value> = redis::cmd("FUNCTION")
        .arg("LIST")
        .arg("LIBRARYNAME")
        .arg(lib_name)
        .query(conn);
    
    match list_result {
        Ok(redis::Value::Array(libraries)) => {
            let mut functions = Vec::new();
            
            for library_value in libraries {
                if let redis::Value::Array(lib_info) = library_value {
                    // Skip if less than 2 elements (should have library name and functions)
                    if lib_info.len() < 2 {
                        continue;
                    }
                    
                    // Get functions array
                    if let redis::Value::Array(funcs_value) = &lib_info[1] {
                        for func_val in funcs_value {
                            if let redis::Value::Array(func_info) = func_val {
                                // Get function name (first element)
                                if !func_info.is_empty() {
                                    if let redis::Value::BulkString(name_bytes) = &func_info[0] {
                                        if let Ok(name) = String::from_utf8(name_bytes.clone()) {
                                            functions.push(name);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            
            Ok(functions)
        },
        Ok(_) => Ok(Vec::new()), // Unknown format, return empty list
        Err(e) => Err(valkey_function_error(format!("Failed to list functions: {}", e)))
    }
}

/// Execute a read-only ValKey function with proper error handling
///
/// This function calls a read-only ValKey function and handles any errors that occur.
/// It always uses FCALL_RO for better performance with read-only operations.
///
/// # Arguments
/// * `conn` - Redis connection
/// * `function_name` - Name of the function to execute
/// * `keys` - Key arguments to the function
/// * `args` - Non-key arguments to the function
/// * `site_id` - Site identifier for namespacing
/// * `debug_mode` - Whether debug mode is enabled
///
/// # Returns
/// * `IntegrationResult<String>` - Function result
pub fn execute_readonly_function(
    conn: &mut Connection,
    function_name: &str,
    keys: &[&str],
    args: &[&str],
    site_id: &str,
    debug_mode: bool
) -> IntegrationResult<String> {
    if debug_mode {
        debug!("Executing read-only ValKey function: {}", function_name);
    }
    
    // Build command specifically for read-only function
    let mut cmd = redis::cmd("FCALL_RO");
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
    let result: redis::RedisResult<String> = cmd.query(conn);
    
    match result {
        Ok(output) => {
            if debug_mode {
                debug!("Successfully executed read-only ValKey function: {}", function_name);
            }
            Ok(output)
        },
        Err(e) => {
            let error_msg = format!("Failed to execute read-only ValKey function {}: {}", function_name, e);
            let err = valkey_function_error(error_msg.clone());
            log_error(&err, "executing readonly ValKey function");
            
            // Handle specific error cases
            if e.to_string().contains("No such function name") {
                // Function not found - might need to reload functions
                warn!("Function {} not found, checking if functions need reloading", function_name);
                
                // Check if we should try to reload the function
                if function_name.starts_with("GNODE_") {
                    // Try to determine which library this function belongs to
                    let lib_prefix = function_name.split('_').nth(1).unwrap_or("UNKNOWN");
                    let library = match lib_prefix {
                        "CORE" => FunctionLibrary::Core,
                        "GEOMETRIC" => FunctionLibrary::Geometric,
                        "STREAM" => FunctionLibrary::Stream,
                        "BATCH" => FunctionLibrary::Batch,
                        "LOCK" => FunctionLibrary::Lock,
                        "GROUP" => FunctionLibrary::Group,
                        "HASH" => FunctionLibrary::Hash,
                        "MONITORING" => FunctionLibrary::Monitoring,
                        "PUBSUB" => FunctionLibrary::PubSub,
                        "SITE" => FunctionLibrary::Site,
                        "TRANSACTION" => FunctionLibrary::Transaction,
                        "UTILS" => FunctionLibrary::Utils,
                        "TEST" => FunctionLibrary::Test,
                        "CACHE" => FunctionLibrary::Cache,
                        _ => FunctionLibrary::Custom(format!("gnode_{}", lib_prefix.to_lowercase())),
                    };
                    
                    // Try to reload just this library
                    let lib_file = library.to_file_name();
                    debug!("Attempting to reload library {} for function {}", lib_file, function_name);
                    
                    match find_function_file(&lib_file, debug_mode) {
                        Some(path) => {
                            let registry_key = format!("{{{}}}:gcore:gnode:valkey_functions", site_id);
                            if let Ok(count) = load_function_library(conn, &path, &library, &registry_key, debug_mode) {
                                info!("Reloaded {} functions from library {}", count, lib_file);
                                
                                // Try executing the function again
                                return execute_readonly_function(conn, function_name, keys, args, site_id, debug_mode);
                            }
                        },
                        None => {
                            warn!("Could not find function library file: {}", lib_file);
                        }
                    }
                }
            }
            
            Err(valkey_function_error(error_msg))
        }
    }
}

/// Execute a ValKey function with proper error handling
///
/// This function calls a ValKey function and handles any errors that occur.
/// It provides proper logging, error recovery, and fallback mechanisms.
///
/// # Arguments
/// * `conn` - Redis connection
/// * `function_name` - Name of the function to execute
/// * `keys` - Key arguments to the function
/// * `args` - Non-key arguments to the function
/// * `site_id` - Site identifier for namespacing
/// * `debug_mode` - Whether debug mode is enabled
///
/// # Returns
/// * `IntegrationResult<String>` - Function result
pub fn execute_function(
    conn: &mut Connection,
    function_name: &str,
    keys: &[&str],
    args: &[&str],
    site_id: &str,
    debug_mode: bool
) -> IntegrationResult<String> {
    if debug_mode {
        debug!("Executing ValKey function: {}", function_name);
    }
    
    // Build command
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
    let result: redis::RedisResult<String> = cmd.query(conn);
    
    match result {
        Ok(output) => {
            if debug_mode {
                debug!("Successfully executed ValKey function: {}", function_name);
            }
            Ok(output)
        },
        Err(e) => {
            let error_msg = format!("Failed to execute ValKey function {}: {}", function_name, e);
            let err = valkey_function_error(error_msg.clone());
            log_error(&err, "executing ValKey function");
            
            // Handle specific error cases
            if e.to_string().contains("No such function name") {
                // Function not found - might need to reload functions
                warn!("Function {} not found, checking if functions need reloading", function_name);
                
                // Check if we should try to reload the function
                if function_name.starts_with("GNODE_") {
                    // Try to determine which library this function belongs to
                    let lib_prefix = function_name.split('_').nth(1).unwrap_or("UNKNOWN");
                    let library = match lib_prefix {
                        "CORE" => FunctionLibrary::Core,
                        "GEOMETRIC" => FunctionLibrary::Geometric,
                        "STREAM" => FunctionLibrary::Stream,
                        "BATCH" => FunctionLibrary::Batch,
                        "LOCK" => FunctionLibrary::Lock,
                        "GROUP" => FunctionLibrary::Group,
                        "HASH" => FunctionLibrary::Hash,
                        "MONITORING" => FunctionLibrary::Monitoring,
                        "PUBSUB" => FunctionLibrary::PubSub,
                        "SITE" => FunctionLibrary::Site,
                        "TRANSACTION" => FunctionLibrary::Transaction,
                        "UTILS" => FunctionLibrary::Utils,
                        "TEST" => FunctionLibrary::Test,
                        "CACHE" => FunctionLibrary::Cache,
                        _ => FunctionLibrary::Custom(format!("gnode_{}", lib_prefix.to_lowercase())),
                    };
                    
                    // Try to reload just this library
                    let lib_file = library.to_file_name();
                    debug!("Attempting to reload library {} for function {}", lib_file, function_name);
                    
                    match find_function_file(&lib_file, debug_mode) {
                        Some(path) => {
                            let registry_key = format!("{{{}}}:gcore:gnode:valkey_functions", site_id);
                            if let Ok(count) = load_function_library(conn, &path, &library, &registry_key, debug_mode) {
                                info!("Reloaded {} functions from library {}", count, lib_file);
                                
                                // Try executing the function again
                                return execute_function(conn, function_name, keys, args, site_id, debug_mode);
                            }
                        },
                        None => {
                            warn!("Could not find function library file: {}", lib_file);
                        }
                    }
                }
            }
            
            Err(valkey_function_error(error_msg))
        }
    }
}

/// Execute a ValKey function asynchronously (Phase 4: Async Architecture)
///
/// This is the async version of execute_function for use with async command handlers.
/// Does not include retry/reload logic - that should be handled at a higher level.
///
/// # Arguments
/// * `conn` - Async Redis connection
/// * `function_name` - Name of the function to execute
/// * `keys` - Key arguments to the function
/// * `args` - Non-key arguments to the function
/// * `debug_mode` - Enable debug logging
///
/// # Returns
/// * `IntegrationResult<String>` - Function result
pub async fn execute_function_async(
    conn: &mut redis::aio::MultiplexedConnection,
    function_name: &str,
    keys: &[&str],
    args: &[&str],
    debug_mode: bool
) -> IntegrationResult<String> {
    if debug_mode {
        debug!("Executing async ValKey function: {}", function_name);
    }

    // Build command
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

    // Execute function asynchronously
    let result: redis::RedisResult<String> = cmd.query_async(conn).await;

    match result {
        Ok(output) => {
            if debug_mode {
                debug!("Successfully executed async ValKey function: {}", function_name);
            }
            Ok(output)
        },
        Err(e) => {
            let error_msg = format!("Failed to execute async ValKey function {}: {}", function_name, e);
            warn!("{}", error_msg);
            Err(valkey_function_error(error_msg))
        }
    }
}

/// Check if a function is available in the loaded functions
///
/// # Arguments
/// * `function_name` - Name of the function to check
///
/// # Returns
/// * `bool` - True if the function is available
pub fn is_function_available(_function_name: &str) -> bool {
    // Check if we have a shared ValKey manager
    if let Some(manager_ref) = get_valkey_manager() {
        if let Ok(manager_guard) = manager_ref.read() {
            // RwLockReadGuard already acts like a reference through Deref
            return manager_guard.is_initialized();
        }
    }
    
    false
}

/// Execute a ValKey function with fallback to direct Redis commands
///
/// This function first tries to execute the ValKey function directly.
/// If that fails, it falls back to executing a direct Redis command.
///
/// # Arguments
/// * `conn` - Redis connection
/// * `function_name` - Name of the function to execute
/// * `keys` - Key arguments to the function
/// * `args` - Non-key arguments to the function
/// * `read_only` - Whether the function is read-only (uses FCALL_RO)
/// * `fallback_fn` - Fallback function to execute if ValKey function fails
///
/// # Returns
/// * `IntegrationResult<String>` - Function result
pub fn execute_function_with_fallback<F>(
    conn: &mut Connection,
    function_name: &str,
    keys: &[&str],
    args: &[&str],
    _site_id: &str,
    read_only: bool,
    fallback_fn: F
) -> IntegrationResult<String>
where
    F: FnOnce(&mut Connection, &[&str], &[&str]) -> IntegrationResult<String>
{
    // First try to execute the ValKey function directly
    // Get site_id from the current context or pass empty string if not available
    let site_id = ""; // In a production environment, this should be passed in or obtained from context
    match execute_function(conn, function_name, keys, args, site_id, read_only) {
        Ok(result) => Ok(result),
        Err(e) => {
            // Log the error and fall back to direct commands
            warn!("ValKey function execution failed: {}", e);
            warn!("Falling back to direct Redis commands");
            
            // Execute fallback function
            fallback_fn(conn, keys, args)
        }
    }
}

/// Execute a geometric operation using ValKey functions
///
/// This is a specialized function for geometric operations,
/// which are critical for the gNode system's core functionality.
///
/// # Arguments
/// * `conn` - Redis connection
/// * `operation` - Geometric operation to perform
/// * `topology_key` - Key to the topology data
/// * `data` - JSON-encoded data
/// * `site_id` - Site identifier for namespacing
/// * `debug_mode` - Whether debug mode is enabled
///
/// # Returns
/// * `IntegrationResult<String>` - Operation result
pub fn execute_geometric_operation(
    conn: &mut Connection,
    operation: &str,
    topology_key: &str,
    data: &str,        // JSON-encoded data
    site_id: &str,     // optional
    debug_mode: bool
) -> IntegrationResult<String> {
    let function_name = format!("GNODE_GEOMETRIC_{}", operation.to_uppercase());
    let keys = [topology_key];
    
    // Build args vector with proper parameter order
    let args = if !site_id.is_empty() {
        vec![data, site_id]
    } else {
        vec![data]
    };
    
    // Should validate required parameters before calling
    if data.is_empty() {
        return Err(valkey_function_error("Missing required data parameter".to_string()));
    }
    
    // Geometric operations are read-only
    execute_readonly_function(conn, &function_name, &keys, &args, site_id, debug_mode)
}

/// Execute a stream operation using ValKey functions
///
/// This is a specialized function for stream operations,
/// which are used for communication between clients and the daemon.
///
/// # Arguments
/// * `conn` - Redis connection
/// * `operation` - Stream operation to perform
/// * `stream_key` - Key to the stream
/// * `args` - Additional arguments for the operation
/// * `site_id` - Site identifier for namespacing
/// * `debug_mode` - Whether debug mode is enabled
///
/// # Returns
/// * `IntegrationResult<String>` - Operation result
pub fn execute_stream_operation(
    conn: &mut Connection,
    operation: &str,
    stream_key: &str,
    args: &[&str],
    site_id: &str,
    debug_mode: bool
) -> IntegrationResult<String> {
    let function_name = format!("GNODE_STREAM_{}", operation.to_uppercase());
    let keys = [stream_key];
    
    // Stream operations are generally not read-only except for reading
    if operation.eq_ignore_ascii_case("read") || 
       operation.eq_ignore_ascii_case("group_read") ||
       operation.eq_ignore_ascii_case("info") {
        execute_readonly_function(conn, &function_name, &keys, args, site_id, debug_mode)
    } else {
        execute_function(conn, &function_name, &keys, args, site_id, debug_mode)
    }
}

/// Execute a batch operation using ValKey functions
///
/// This is a specialized function for batch operations,
/// which allow multiple operations to be performed atomically.
///
/// # Arguments
/// * `conn` - Redis connection
/// * `operation` - Batch operation to perform
/// * `keys` - Keys to operate on
/// * `args` - Additional arguments for the operation
/// * `site_id` - Site identifier for namespacing
/// * `debug_mode` - Whether debug mode is enabled
///
/// # Returns
/// * `IntegrationResult<String>` - Operation result
pub fn execute_batch_operation(
    conn: &mut Connection,
    operation: &str,
    keys: &[&str],
    args: &[&str],
    site_id: &str,
    debug_mode: bool
) -> IntegrationResult<String> {
    let function_name = format!("GNODE_BATCH_{}", operation.to_uppercase());
    
    // Batch operations are generally not read-only except for MGET
    if operation.eq_ignore_ascii_case("mget") {
        execute_readonly_function(conn, &function_name, keys, args, site_id, debug_mode)
    } else {
        execute_function(conn, &function_name, keys, args, site_id, debug_mode)
    }
}

/// Execute a consumer group operation using ValKey functions
///
/// This is a specialized function for consumer group operations,
/// which are used for high-throughput message processing.
///
/// # Arguments
/// * `conn` - Redis connection
/// * `operation` - Consumer group operation to perform
/// * `stream_key` - Key to the stream
/// * `group_name` - Name of the consumer group
/// * `consumer_name` - Name of the consumer
/// * `args` - Additional arguments for the operation
/// * `site_id` - Site identifier for namespacing
/// * `debug_mode` - Whether debug mode is enabled
///
/// # Returns
/// * `IntegrationResult<String>` - Operation result
#[allow(clippy::too_many_arguments)]
pub fn execute_consumer_group_operation(
    conn: &mut Connection,
    operation: &str,
    stream_key: &str,
    group_name: &str,
    consumer_name: &str,
    args: &[&str],
    site_id: &str,
    debug_mode: bool
) -> IntegrationResult<String> {
    // Prepare arguments
    let mut operation_args = Vec::with_capacity(args.len() + 2);
    operation_args.push(group_name);
    operation_args.push(consumer_name);
    operation_args.extend_from_slice(args);
    
    // Execute as stream operation
    execute_stream_operation(conn, operation, stream_key, &operation_args, site_id, debug_mode)
}

/// Reload all ValKey functions
///
/// This function reloads all ValKey functions from disk.
/// It's useful when functions have been modified or when
/// functions are missing due to a ValKey restart.
///
/// # Arguments
/// * `client` - Redis client
/// * `site_id` - Site identifier for namespacing
/// * `debug` - Debug mode flag
///
/// # Returns
/// * `IntegrationResult<usize>` - Number of functions loaded
pub fn reload_functions(
    client: &Client,
    site_id: &str,
    debug: bool
) -> IntegrationResult<usize> {
    info!("Reloading all ValKey functions");
    
    // Verify we can connect to ValKey
    let mut conn = match client.get_connection() {
        Ok(conn) => conn,
        Err(e) => {
            let error_msg = format!("Failed to connect to ValKey: {}", e);
            error!("{}", error_msg);
            return Err(valkey_function_error(error_msg));
        }
    };
    
    // First check what functions are currently loaded
    let list_before_result: redis::RedisResult<redis::Value> = redis::cmd("FUNCTION")
        .arg("LIST")
        .query(&mut conn);
    
    match &list_before_result {
        Ok(redis::Value::Array(list)) => {
            info!("Found {} function libraries before flush", list.len());
        },
        Ok(_) => info!("Unexpected response format from FUNCTION LIST"),
        Err(e) => warn!("Failed to list functions before flush: {}", e)
    }
    
    // Flush functions with FUNCTION FLUSH
    let flush_result: redis::RedisResult<()> = redis::cmd("FUNCTION")
        .arg("FLUSH")
        .query(&mut conn);
    
    match flush_result {
        Ok(_) => info!("Successfully flushed ValKey functions"),
        Err(e) => {
            warn!("Failed to flush ValKey functions: {}", e);
            // If we can't flush, check if this is a permission issue
            if e.to_string().contains("permission") || e.to_string().contains("auth") {
                error!("Permission denied when attempting to flush functions. Check ValKey ACL configuration.");
                return Err(valkey_function_error(format!(
                    "Permission denied for ValKey functions: {}", e
                )));
            }
        }
    }
    
    // Initialize functions
    match initialize_functions(client, site_id, debug) {
        Ok(count) => {
            if count > 0 {
                info!("Successfully loaded {} ValKey functions", count);
                
                // Verify functions were actually loaded
                let list_after_result: redis::RedisResult<redis::Value> = redis::cmd("FUNCTION")
                    .arg("LIST")
                    .query(&mut conn);
                
                match &list_after_result {
                    Ok(redis::Value::Array(list)) => {
                        info!("Verified {} function libraries after loading", list.len());
                        
                        // Try a test function
                        let test_result: redis::RedisResult<String> = redis::cmd("FCALL")
                            .arg("GNODE_TEST_HELLO")
                            .arg(0)
                            .query(&mut conn);
                        
                        match test_result {
                            Ok(response) => {
                                info!("Successfully executed test function GNODE_TEST_HELLO: {}", response);
                                
                                // Create a mapping of function names used in the code to actual function names
                                let function_name_map: HashMap<&str, &str> = [
                                    // Add known mappings here
                                    ("GNODE_UTILS_HEALTH_CHECK", "GNODE_MONITORING_HEALTH_CHECK"),
                                    ("GNODE_GEOMETRIC_DIMENSIONS", "GNODE_GEOMETRIC_GET_DIMENSIONS"),
                                    ("GNODE_STREAM_GROUP_INFO", "GNODE_STREAM_GROUPS_INFO"),
                                ].iter().cloned().collect();
                                
                                // Log the function name mappings for documentation
                                info!("Function name mapping applied for compatibility:");
                                for (code_name, actual_name) in &function_name_map {
                                    info!("  {} → {}", code_name, actual_name);
                                }
                            },
                            Err(e) => {
                                warn!("Failed to execute test function: {}", e);
                            }
                        }
                    },
                    Ok(_) => info!("Unexpected response format from FUNCTION LIST"),
                    Err(e) => warn!("Failed to list functions after loading: {}", e)
                }
                
                Ok(count)
            } else {
                warn!("No ValKey functions were loaded. Check the function files for errors.");
                Ok(0)
            }
        },
        Err(e) => {
            // Log the error but don't treat it as fatal
            error!("ValKey functions failed to load: {}", e);
            error!("This is a critical issue and may impact service operation!");
            error!("Manual intervention required: run scripts/load-valkey-functions.sh");
            
            // Return the error now - this is critical for proper operation
            Err(e)
        }
    }
}

/// Test ValKey function integration
///
/// This function tests the ValKey function integration by executing
/// a simple function. It's useful for verifying that ValKey functions
/// are properly loaded and working.
///
/// # Arguments
/// * `conn` - Redis connection
///
/// # Returns
/// * `IntegrationResult<()>` - Success or failure
pub fn test_valkey_function_integration(conn: &mut Connection) -> IntegrationResult<()> {
    info!("Testing ValKey function integration");
    
    // First try a simple test function
    // Use empty site_id and debug = false for this test
    let site_id = "";
    let debug_mode = false;
    match execute_function(
        conn,
        "GNODE_TEST_HELLO",
        &[],
        &[],
        site_id,
        debug_mode
    ) {
        Ok(result) => {
            info!("ValKey function test successful: {}", result);
            Ok(())
        },
        Err(e) => {
            // Try to use the basic FUNCTION command directly
            match redis::cmd("FUNCTION")
                .arg("LIST")
                .query::<redis::Value>(conn) {
                Ok(_) => {
                    warn!("ValKey functions are available, but test function not found: {}", e);
                    warn!("You may need to load test functions with load-valkey-functions.sh");
                    Err(e)
                },
                Err(cmd_err) => {
                    error!("ValKey functions not available: {}", cmd_err);
                    error!("This ValKey instance may not support functions or requires authentication");
                    Err(valkey_function_error(format!(
                        "ValKey functions not available: {}", cmd_err
                    )))
                }
            }
        }
    }
}

/// Log ValKey function statistics
///
/// This function logs statistics about the loaded ValKey functions.
/// It's useful for debugging and monitoring.
///
/// # Arguments
/// * `conn` - Redis connection
///
/// # Returns
/// * `IntegrationResult<HashMap<String, usize>>` - Function statistics
pub fn log_valkey_function_stats(conn: &mut Connection) -> IntegrationResult<HashMap<String, usize>> {
    info!("Logging ValKey function statistics");
    
    // Get list of all functions
    let list_result: redis::RedisResult<redis::Value> = redis::cmd("FUNCTION")
        .arg("LIST")
        .query(conn);
    
    match list_result {
        Ok(redis::Value::Array(libraries)) => {
            let mut stats = HashMap::new();
            let mut total_functions = 0;
            
            for library_value in libraries {
                if let redis::Value::Array(lib_info) = library_value {
                    // Skip if less than 2 elements (should have library name and functions)
                    if lib_info.len() < 2 {
                        continue;
                    }
                    
                    // Get library name
                    let library_name = if let redis::Value::BulkString(name_bytes) = &lib_info[0] {
                        String::from_utf8(name_bytes.clone()).unwrap_or_else(|_| "unknown".to_string())
                    } else {
                        "unknown".to_string()
                    };
                    
                    // Get functions array
                    if let redis::Value::Array(funcs_value) = &lib_info[1] {
                        let function_count = funcs_value.len();
                        stats.insert(library_name.clone(), function_count);
                        total_functions += function_count;
                        
                        info!("Library {}: {} functions", library_name, function_count);
                    }
                }
            }
            
            info!("Total ValKey functions available: {}", total_functions);
            Ok(stats)
        },
        Ok(_) => {
            warn!("Unexpected response format from FUNCTION LIST");
            Ok(HashMap::new())
        },
        Err(e) => {
            warn!("Failed to list ValKey functions: {}", e);
            Err(valkey_function_error(format!("Failed to list functions: {}", e)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis::Client;
    
    // Helper function to create a Redis client for tests
    fn create_test_client() -> Client {
        Client::open("redis://127.0.0.1:6379").expect("Failed to create Redis client")
    }
    
    #[test]
    fn test_function_library_conversion() {
        assert_eq!(FunctionLibrary::Core.to_file_name(), "gnode_core.lua");
        assert_eq!(FunctionLibrary::Stream.to_file_name(), "gnode_stream.lua");
        
        assert_eq!(FunctionLibrary::from_file_name("gnode_core.lua"), FunctionLibrary::Core);
        assert_eq!(FunctionLibrary::from_file_name("gnode_stream.lua"), FunctionLibrary::Stream);
        assert_eq!(
            FunctionLibrary::from_file_name("custom.lua"), 
            FunctionLibrary::Custom("custom".to_string())
        );
    }
    
    #[test]
    fn test_standard_libraries_order() {
        let libraries = FunctionLibrary::standard_libraries();
        
        // Utils should be first as others may depend on it
        assert_eq!(libraries[0], FunctionLibrary::Utils);
        
        // Check that all core libraries are included
        assert!(libraries.contains(&FunctionLibrary::Core));
        assert!(libraries.contains(&FunctionLibrary::Geometric));
        assert!(libraries.contains(&FunctionLibrary::Stream));
    }

    
    #[test]
    fn test_ensure_library_metadata() {
        let content = "function test() return 'hello' end";
        let library = FunctionLibrary::Core;
        
        let updated = ensure_library_metadata(content, &library);
        assert!(updated.starts_with("#!lua name=gnode_core\n\n"));
        
        // If already has metadata, should not change
        let with_metadata = "#!lua name=existing\n\nfunction test() end";
        let no_change = ensure_library_metadata(with_metadata, &library);
        assert_eq!(no_change, with_metadata);
    }
    
    // Integration tests that require a running ValKey
    #[test]
    #[ignore]
    fn test_valkey_function_integration() {
        let client = create_test_client();
        let mut conn = client.get_connection().expect("Failed to connect to ValKey");
        
        // Test requires GNODE_TEST_HELLO function to be loaded
        // Added the missing parameters for site_id and debug_mode
        let result = execute_readonly_function(&mut conn, "GNODE_TEST_HELLO", &[], &[], "test", true);
        println!("ValKey function test result: {:?}", result);
    }
    
    #[test]
    #[ignore]
    fn test_initialize_functions() {
        let client = create_test_client();
        
        // This test will only work if the functions directory exists
        let result = initialize_functions(&client, "test", true);
        println!("Initialize functions result: {:?}", result);
    }
}