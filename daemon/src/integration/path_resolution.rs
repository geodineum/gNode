// Path Resolution Module for gNode
//
// This module provides functions for resolving paths to ValKey functions
// used by the gNode (Geodineum Service Daemon). It implements multiple resolution strategies
// to locate these files in different environments including development, testing, and production.
//
// Main functions:
// - find_valkey_functions_directory: Resolves the path to ValKey function files
// - find_function_file: Resolves the path to a specific ValKey function file
// - verify_directory_contents: Verifies that a directory contains the expected file types

use std::path::{Path, PathBuf};
use std::fs;
use log::{info, warn, debug};

/// Find the ValKey functions directory
pub fn find_valkey_functions_directory(debug_mode: bool) -> String {
    // List of potential function locations to try
    let mut search_paths = Vec::new();
    
    // 1. Check environment variable
    if let Ok(env_dir) = std::env::var("GNODE_FUNCTIONS_DIR") {
        let env_path = PathBuf::from(&env_dir);
        search_paths.push(env_path);
    }
    
    // 2. Get executable directory and check for functions directory
    if let Ok(exe_path) = std::env::current_exe() {
        if let Some(exe_dir) = exe_path.parent() {
            search_paths.push(exe_dir.join("functions"));
            
            // Check one level up from executable
            if let Some(parent_dir) = exe_dir.parent() {
                search_paths.push(parent_dir.join("functions"));
                
                // Check for daemon/functions in project structure
                if exe_dir.ends_with("release") || exe_dir.ends_with("debug") {
                    if let Some(target_dir) = exe_dir.parent() {
                        if let Some(project_dir) = target_dir.parent() {
                            search_paths.push(project_dir.join("functions"));
                            
                            // Also check for daemon/functions structure
                            search_paths.push(project_dir.join("daemon/functions"));
                        }
                    }
                }
            }
        }
    }
    
    // 3. Check relative to current working directory
    if let Ok(current_dir) = std::env::current_dir() {
        search_paths.push(current_dir.join("functions"));
        
        // Check one level up from current directory
        if let Some(parent_dir) = current_dir.parent() {
            search_paths.push(parent_dir.join("functions"));
            
            // Check for daemon/functions structure
            search_paths.push(parent_dir.join("daemon/functions"));
        }
    }
    
    // 4. Check standard installation locations
    search_paths.push(PathBuf::from("/usr/local/share/gnode/functions"));
    search_paths.push(PathBuf::from("/usr/share/gnode/functions"));
    search_paths.push(PathBuf::from("/opt/gnode/functions"));
    
    // 5. Check GNODE_DIR environment variable
    if let Ok(gnode_dir) = std::env::var("GNODE_DIR") {
        search_paths.push(PathBuf::from(gnode_dir).join("daemon/functions"));
    }

    // Try each path and find the first one that exists and contains Lua files
    for path in &search_paths {
        if verify_directory_contents(path, "lua", debug_mode) {
            if debug_mode {
                info!("Found ValKey function directory at: {:?}", path);
            }
            return path.to_string_lossy().to_string();
        }
    }
    
    // Fallback: Log all paths searched and return a not-found path
    if debug_mode {
        warn!("No valid ValKey function directory found. Searched in:");
        for path in &search_paths {
            warn!("  - {:?}", path);
        }
    }
    
    if let Ok(current_dir) = std::env::current_dir() {
        format!("{}/NOT_FOUND_functions", current_dir.to_string_lossy())
    } else {
        "/NOT_FOUND/searched_paths_in_log/functions".to_string()
    }
}

/// Find a specific ValKey function file
///
/// Resolves the path to a specific ValKey function file based on its name
/// Returns Option<PathBuf> containing the resolved path if found
pub fn find_function_file(name: &str, debug_mode: bool) -> Option<PathBuf> {
    let functions_dir = find_valkey_functions_directory(debug_mode);
    let functions_path = Path::new(&functions_dir);
    
    // Skip if directory doesn't exist
    if !functions_path.exists() || !functions_path.is_dir() {
        if debug_mode {
            warn!("Functions directory not found: {:?}", functions_path);
        }
        return None;
    }
    
    // Normalize function name to filename
    let file_name = match name {
        "GNODE_CORE" => String::from("gnode_core.lua"),
        "GNODE_STREAM" => String::from("gnode_stream.lua"),
        "GNODE_GEOMETRIC" => String::from("gnode_geometric.lua"),
        "GNODE_BATCH" => String::from("gnode_batch.lua"),
        "GNODE_BATCH_RESP3" => String::from("gnode_batch_resp3.lua"),
        "GNODE_HASH" => String::from("gnode_hash.lua"),
        "GNODE_GROUP" => String::from("gnode_group.lua"),
        "GNODE_LOCK" => String::from("gnode_lock.lua"),
        "GNODE_MONITORING" => String::from("gnode_monitoring.lua"),
        "GNODE_PUBSUB" => String::from("gnode_pubsub.lua"),
        "GNODE_SITE" => String::from("gnode_site.lua"),
        "GNODE_TEST" => String::from("gnode_test.lua"),
        "GNODE_TRANSACTION" => String::from("gnode_transaction.lua"),
        "GNODE_UTILS" => String::from("gnode_utils.lua"),
        "GNODE_CACHE" => String::from("gnode_cache.lua"),
        _ => {
            // If not recognized, try lowercase version with .lua extension
            let lowercase = name.to_lowercase();
            if lowercase.ends_with(".lua") {
                lowercase
            } else {
                format!("{}.lua", lowercase)
            }
        }
    };
    
    let file_path = functions_path.join(file_name);
    if file_path.exists() && file_path.is_file() {
        if debug_mode {
            debug!("Found function file at: {:?}", file_path);
        }
        Some(file_path)
    } else {
        if debug_mode {
            warn!("Function file not found: {:?}", file_path);
        }
        None
    }
}

/// Verify that a directory exists and contains files with the specified extension
///
/// Returns true if the directory exists and contains at least one file with the specified extension
pub fn verify_directory_contents(path: &Path, extension: &str, debug_mode: bool) -> bool {
    if !path.exists() || !path.is_dir() {
        return false;
    }

    // Check if any file with the specified extension exists in the directory
    match fs::read_dir(path) {
        Ok(entries) => {
            let has_files = entries
                .filter_map(|e| e.ok()) // Use closure instead of Result::ok
                .any(|entry| {
                    let file_path = entry.path();
                    file_path.is_file() && 
                    file_path.extension().is_some_and(|ext| ext == extension)
                });
            
            if has_files {
                true
            } else {
                if debug_mode {
                    debug!("Directory exists but contains no *.{} files: {:?}", extension, path);
                }
                false
            }
        },
        Err(e) => {
            if debug_mode {
                warn!("Failed to read directory {:?}: {}", path, e);
            }
            false
        }
    }
}

/// Verifies that a directory contains at least one file that matches the given predicate
/// 
/// This function provides more flexibility than verify_directory_contents by allowing
/// custom predicates for file validation.
/// 
/// # Arguments
/// 
/// * `path` - Path to the directory to check
/// * `predicate` - Function that takes a reference to a Path and returns a boolean
/// * `debug_mode` - Whether to output debug messages
/// 
/// # Returns
/// 
/// * `Result<bool, std::io::Error>` - Ok(true) if matching files exist, Ok(false) if none found, Err on error
pub fn verify_directory_contents_with_predicate<F>(
    path: &Path, 
    predicate: F,
    debug_mode: bool
) -> Result<bool, std::io::Error> 
where 
    F: Fn(&Path) -> bool 
{
    if !path.exists() || !path.is_dir() {
        return Ok(false);
    }

    // Read the directory and check if any file matches the predicate
    let entries = fs::read_dir(path)?;
    
    let has_files = entries
        .filter_map(|e| e.ok())
        .any(|entry| {
            let file_path = entry.path();
            file_path.is_file() && predicate(&file_path)
        });
    
    if !has_files && debug_mode {
        debug!("Directory exists but contains no matching files: {:?}", path);
    }
    
    Ok(has_files)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::tempdir;
    use std::env;

    #[test]
    fn test_verify_directory_contents() {
        // Create a temporary directory
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.lua");

        // Create a test file
        let mut file = File::create(&file_path).unwrap();
        writeln!(file, "-- Test Lua file").unwrap();

        // Test with debug mode off
        assert!(verify_directory_contents(dir.path(), "lua", false));
        assert!(!verify_directory_contents(dir.path(), "rs", false));

        // Test with debug mode on
        assert!(verify_directory_contents(dir.path(), "lua", true));
        assert!(!verify_directory_contents(dir.path(), "rs", true));
    }

    #[test]
    fn test_find_function_file() {
        let _env = crate::test_env_guard();
        // Test with non-existent directory
        assert!(find_function_file("test", false).is_none());

        // Create a temporary directory with a test function file
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("gnode_test.lua");

        // Create a test file
        let mut file = File::create(&file_path).unwrap();
        writeln!(file, "-- Test Lua function").unwrap();

        // Set environment variable to point to the temporary directory
        env::set_var("GNODE_FUNCTIONS_DIR", dir.path().to_string_lossy().to_string());

        // Test function file lookup
        let result = find_function_file("GNODE_TEST", true);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), file_path);

        // Test with non-existent function
        assert!(find_function_file("NONEXISTENT", true).is_none());

        // Clean up
        env::remove_var("GNODE_FUNCTIONS_DIR");
    }

    #[test]
    fn test_path_normalization() {
        let _env = crate::test_env_guard();
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("gnode_core.lua");

        // Create a test file
        let mut file = File::create(&file_path).unwrap();
        writeln!(file, "-- Test Lua function").unwrap();

        // Set environment variable
        env::set_var("GNODE_FUNCTIONS_DIR", dir.path().to_string_lossy().to_string());

        // Test various forms of the same function name
        assert!(find_function_file("GNODE_CORE", false).is_some());
        assert!(find_function_file("gnode_core", false).is_some());
        assert!(find_function_file("gnode_core.lua", false).is_some());

        // Clean up
        env::remove_var("GNODE_FUNCTIONS_DIR");
    }
}