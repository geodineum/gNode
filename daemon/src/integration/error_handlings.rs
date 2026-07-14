//! Error Handling Module for gNode Integration
//!
//! This module provides specialized error types and handling utilities for the integration
//! components of the gNode daemon. It defines a consistent error handling approach for
//! ValKey functions, script execution, stream processing, and other integration concerns.
//!
//! The module includes:
//! - Custom error types for integration operations
//! - Error conversion functions for external error types
//! - Error recovery strategies
//! - Logging utilities for errors

use std::fmt;
use std::error::Error;
use std::sync::PoisonError;
use log::{error, warn};
use redis::RedisError;

use crate::GeometricError;

/// Error kind for integration operations
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum IntegrationErrorKind {
    /// Path resolution error
    PathResolution,
    
    /// ValKey function error
    ValkeyFunction,
    
    /// Script execution error
    ScriptExecution,
    
    /// Stream processing error
    StreamProcessing,
    
    /// Consumer group error
    ConsumerGroup,
    
    /// Thread pool error
    ThreadPool,
    
    /// Redis error
    Redis,
    
    /// Generic error
    Generic,
}

/// Error type for integration operations
#[derive(Debug, Clone)]
pub struct IntegrationError {
    /// The kind of error
    pub kind: IntegrationErrorKind,
    
    /// Error message
    pub message: String,
}

impl IntegrationError {
    /// Create a new integration error
    pub fn new(kind: IntegrationErrorKind, message: String) -> Self {
        Self { kind, message }
    }
}

/// Integration result type
pub type IntegrationResult<T> = Result<T, IntegrationError>;

/// Display implementation for IntegrationError
impl fmt::Display for IntegrationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.message)
    }
}

/// Error implementation for IntegrationError
impl Error for IntegrationError {}

/// Create a path resolution error
pub fn path_resolution_error(message: String) -> IntegrationError {
    IntegrationError::new(IntegrationErrorKind::PathResolution, message)
}

/// Create a ValKey function error
pub fn valkey_function_error(message: String) -> IntegrationError {
    IntegrationError::new(IntegrationErrorKind::ValkeyFunction, message)
}

/// Create a script execution error
pub fn script_execution_error(message: String) -> IntegrationError {
    IntegrationError::new(IntegrationErrorKind::ScriptExecution, message)
}

/// Create a stream processing error
pub fn stream_processing_error(message: String) -> IntegrationError {
    IntegrationError::new(IntegrationErrorKind::StreamProcessing, message)
}

/// Create a consumer group error
pub fn consumer_group_error(message: String) -> IntegrationError {
    IntegrationError::new(IntegrationErrorKind::ConsumerGroup, message)
}

/// Create a thread pool error
pub fn thread_pool_error(message: String) -> IntegrationError {
    IntegrationError::new(IntegrationErrorKind::ThreadPool, message)
}

/// Create a Redis error
pub fn redis_error(message: String) -> IntegrationError {
    IntegrationError::new(IntegrationErrorKind::Redis, message)
}

/// Log an error
pub fn log_error(error: &IntegrationError, context: &str) {
    // Log with different levels based on error kind
    match error.kind {
        IntegrationErrorKind::PathResolution => warn!("[{}] Path resolution error: {}", context, error.message),
        IntegrationErrorKind::ValkeyFunction => warn!("[{}] ValKey function error: {}", context, error.message),
        IntegrationErrorKind::ScriptExecution => warn!("[{}] Script execution error: {}", context, error.message),
        IntegrationErrorKind::StreamProcessing => warn!("[{}] Stream processing error: {}", context, error.message),
        IntegrationErrorKind::ConsumerGroup => warn!("[{}] Consumer group error: {}", context, error.message),
        IntegrationErrorKind::ThreadPool => error!("[{}] Thread pool error: {}", context, error.message),
        IntegrationErrorKind::Redis => warn!("[{}] Redis error: {}", context, error.message),
        IntegrationErrorKind::Generic => error!("[{}] Error: {}", context, error.message),
    }
}

/// Convert a Redis error to an IntegrationError
pub fn redis_to_integration_error(
    error: RedisError, 
    kind: IntegrationErrorKind,
    context: &str
) -> IntegrationError {
    IntegrationError::new(
        kind,
        format!("{} error: {} ({})", context, error, error.category())
    )
}

/// Convert a GeometricError to an IntegrationError
pub fn geometric_to_integration_error(
    error: GeometricError,
    kind: IntegrationErrorKind,
    context: &str
) -> IntegrationError {
    IntegrationError::new(
        kind,
        format!("{} error: {}", context, error)
    )
}

/// Convert a PoisonError to an IntegrationError for script manager
pub fn script_manager_poison_error<T>(
    _error: PoisonError<T>,
    lock_type: &str
) -> IntegrationError {
    IntegrationError::new(
        IntegrationErrorKind::ScriptExecution,
        format!("Script manager {} lock was poisoned", lock_type)
    )
}

/// Convert a PoisonError to an IntegrationError for ValKey function manager
pub fn valkey_manager_poison_error<T>(
    _error: PoisonError<T>,
    lock_type: &str
) -> IntegrationError {
    IntegrationError::new(
        IntegrationErrorKind::ValkeyFunction,
        format!("ValKey function manager {} lock was poisoned", lock_type)
    )
}

/// Handle a ValKey function error
pub fn handle_valkey_function_error(
    error: IntegrationError,
    function_name: &str,
    retry_attempt: usize
) -> IntegrationResult<bool> {
    // Log the error with context
    let context = format!("ValKey function {} (attempt {})", function_name, retry_attempt);
    log_error(&error, &context);
    
    if retry_attempt > 2 {
        // After multiple retries, fail fast
        error!("ValKey function {} failed after {} attempts: {}", function_name, retry_attempt, error);
        Err(error)
    } else {
        // On first attempts, allow retry or fallback
        warn!("ValKey function {} failed (attempt {}): {}. Will use fallback.", 
            function_name, retry_attempt, error);
        Ok(true) // Use fallback
    }
}

/// Handle a script execution error
pub fn handle_script_execution_error(
    error: IntegrationError,
    script_name: &str,
    retry_attempt: usize
) -> IntegrationResult<bool> {
    // Log the error with context
    let context = format!("Script {} (attempt {})", script_name, retry_attempt);
    log_error(&error, &context);
    
    if retry_attempt > 2 {
        // After multiple retries, fail fast
        error!("Script {} failed after {} attempts: {}", script_name, retry_attempt, error);
        Err(error)
    } else {
        // On first attempts, allow retry or fallback
        warn!("Script {} failed (attempt {}): {}. Will use direct Redis commands.", 
            script_name, retry_attempt, error);
        Ok(true) // Use direct Redis commands
    }
}

/// Handle a stream processing error
pub fn handle_stream_processing_error(
    error: IntegrationError,
    stream_name: &str,
    retry_attempt: usize
) -> IntegrationResult<bool> {
    // Log the error with context
    let context = format!("Stream {} (attempt {})", stream_name, retry_attempt);
    log_error(&error, &context);
    
    if retry_attempt > 3 {
        // After multiple retries, fail fast
        error!("Stream processing for {} failed after {} attempts: {}", 
            stream_name, retry_attempt, error);
        Err(error)
    } else {
        // On first attempts, allow retry with backoff
        warn!("Stream processing for {} failed (attempt {}): {}. Will retry with backoff.", 
            stream_name, retry_attempt, error);
        Ok(true) // Retry with backoff
    }
}

/// Handle a consumer group error
pub fn handle_consumer_group_error(
    error: IntegrationError,
    group_name: &str,
    retry_attempt: usize
) -> IntegrationResult<bool> {
    // Log the error with context
    let context = format!("Consumer group {} (attempt {})", group_name, retry_attempt);
    log_error(&error, &context);
    
    if retry_attempt > 2 {
        // After multiple retries, fail fast
        error!("Consumer group {} failed after {} attempts: {}", 
            group_name, retry_attempt, error);
        Err(error)
    } else {
        // On first attempts, allow retry or fallback
        warn!("Consumer group {} failed (attempt {}): {}. Will use fallback.", 
            group_name, retry_attempt, error);
        Ok(true) // Use fallback
    }
}

/// Handle a thread pool error
pub fn handle_thread_pool_error(
    error: IntegrationError,
    thread_pool_name: &str,
    retry_attempt: usize
) -> IntegrationResult<bool> {
    // Log the error with context
    let context = format!("Thread pool {} (attempt {})", thread_pool_name, retry_attempt);
    log_error(&error, &context);
    
    if retry_attempt > 1 {
        // After multiple retries, fail fast
        error!("Thread pool {} failed after {} attempts: {}", 
            thread_pool_name, retry_attempt, error);
        Err(error)
    } else {
        // On first attempt, allow retry with fallback
        warn!("Thread pool {} failed (attempt {}): {}. Will use fallback.", 
            thread_pool_name, retry_attempt, error);
        Ok(true) // Use fallback
    }
}

/// Handle a path resolution error
pub fn handle_path_resolution_error(
    error: IntegrationError,
    path: &str,
    retry_attempt: usize
) -> IntegrationResult<bool> {
    // Log the error with context
    let context = format!("Path resolution for {} (attempt {})", path, retry_attempt);
    log_error(&error, &context);
    
    if retry_attempt > 1 {
        // After multiple retries, fail fast
        error!("Path resolution for {} failed after {} attempts: {}", 
            path, retry_attempt, error);
        Err(error)
    } else {
        // On first attempt, allow retry with fallback
        warn!("Path resolution for {} failed (attempt {}): {}. Will use fallback path.", 
            path, retry_attempt, error);
        Ok(true) // Use fallback path
    }
}

/// Handle a Redis error
pub fn handle_redis_error(
    error: IntegrationError,
    operation: &str,
    retry_attempt: usize
) -> IntegrationResult<bool> {
    // Log the error with context
    let context = format!("Redis operation {} (attempt {})", operation, retry_attempt);
    log_error(&error, &context);
    
    if retry_attempt > 3 {
        // After multiple retries, fail fast
        error!("Redis operation {} failed after {} attempts: {}", 
            operation, retry_attempt, error);
        Err(error)
    } else {
        // On first attempts, allow retry with backoff
        warn!("Redis operation {} failed (attempt {}): {}. Will retry with backoff.", 
            operation, retry_attempt, error);
        Ok(true) // Retry with backoff
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_error_creation() {
        let err = path_resolution_error("test error".to_string());
        assert_eq!(err.kind, IntegrationErrorKind::PathResolution);
        assert_eq!(err.message, "test error");
        
        let err = valkey_function_error("test error".to_string());
        assert_eq!(err.kind, IntegrationErrorKind::ValkeyFunction);
        assert_eq!(err.message, "test error");
        
        let err = script_execution_error("test error".to_string());
        assert_eq!(err.kind, IntegrationErrorKind::ScriptExecution);
        assert_eq!(err.message, "test error");
    }
    
    #[test]
    fn test_error_display() {
        let err = path_resolution_error("test error".to_string());
        assert_eq!(format!("{}", err), "PathResolution: test error");
    }
    
    #[test]
    fn test_log_error() {
        // This is mostly for coverage, actual logging is not tested
        let err = path_resolution_error("test error".to_string());
        log_error(&err, "test context");
        
        // Test various error kinds
        let err = valkey_function_error("test error".to_string());
        log_error(&err, "valkey test");
        
        let err = script_execution_error("test error".to_string());
        log_error(&err, "script test");
        
        let err = stream_processing_error("test error".to_string());
        log_error(&err, "stream test");
    }
    
    #[test]
    fn test_handle_valkey_function_error() {
        let int_err = valkey_function_error("test valkey error".to_string());
        
        // First attempt should allow fallback
        let result = handle_valkey_function_error(int_err.clone(), "test_function", 1);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), true); // Should use fallback
        
        // After multiple retries, should fail
        let result = handle_valkey_function_error(int_err, "test_function", 3);
        assert!(result.is_err());
    }
    
    #[test]
    fn test_handle_script_execution_error() {
        let int_err = script_execution_error("test script error".to_string());
        
        // First attempt should allow direct Redis commands
        let result = handle_script_execution_error(int_err, "test_script", 0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), true); // Should use direct Redis commands
    }
    
    #[test]
    fn test_handle_path_resolution_error() {
        let int_err = path_resolution_error("test path resolution error".to_string());
        
        // First attempt should allow fallback
        let result = handle_path_resolution_error(int_err.clone(), "/some/path", 1);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), true); // Should use fallback path
        
        // After multiple retries, should fail
        let result = handle_path_resolution_error(int_err, "/some/path", 2);
        assert!(result.is_err());
    }
    
    #[test]
    fn test_redis_to_integration_error() {
        // Create a Redis error
        let redis_err = RedisError::from(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused, 
            "Connection refused"
        ));
        
        // Convert to integration error
        let int_err = redis_to_integration_error(
            redis_err,
            IntegrationErrorKind::StreamProcessing,
            "test context"
        );
        
        assert_eq!(int_err.kind, IntegrationErrorKind::StreamProcessing);
        assert!(int_err.message.contains("test context error"));
    }
}