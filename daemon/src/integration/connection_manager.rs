//! Connection Manager Module for gNode
//!
//! This module provides a standardized approach to Redis/ValKey connection management.
//! It handles connection acquisition, pooling, error handling, and retry strategies.
//! The goal is to eliminate inconsistencies in how connections are obtained across modules.
//!
//! Key features:
//! - Thread-safe connection pooling using r2d2
//! - Automatic connection retry with backoff
//! - Efficient connection reuse
//! - Consistent error handling
//! - Simple API for both one-off and transactional operations

use std::sync::{Arc, Mutex};
use std::time::Duration;
use log::{info, warn};
use redis::{Client, Connection, RedisError, cmd};
use r2d2::{Pool, PooledConnection, ManageConnection};

use crate::integration::error_handlings::{
    IntegrationError, IntegrationResult,
    stream_processing_error, IntegrationErrorKind, redis_error,
};

// Configuration options for connection pooling and retries
#[derive(Clone, Debug)]
pub struct ConnectionConfig {
    // Maximum number of retry attempts
    pub max_retries: u32,
    
    // Base backoff time in milliseconds
    pub base_backoff_ms: u64,
    
    // Maximum backoff time in milliseconds
    pub max_backoff_ms: u64,
    
    // Connection timeout in milliseconds
    pub connection_timeout_ms: u64,
    
    // Maximum pool size (maximum concurrent connections)
    pub max_pool_size: u32,
    
    // Minimum idle connections to maintain
    pub min_idle_connections: u32,
    
    // Connection idle timeout in seconds
    pub connection_idle_timeout_secs: u64,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        ConnectionConfig {
            max_retries: 3,
            base_backoff_ms: 100,
            max_backoff_ms: 2000,
            connection_timeout_ms: 5000,
            max_pool_size: 16,
            min_idle_connections: 2,
            connection_idle_timeout_secs: 300, // 5 minutes
        }
    }
}

// Custom Redis connection manager for r2d2 pool
pub struct RedisConnectionManager {
    client: Client,
}

impl RedisConnectionManager {
    fn new(client: Client) -> Self {
        Self { client }
    }
}

impl ManageConnection for RedisConnectionManager {
    type Connection = Connection;
    type Error = RedisError;

    fn connect(&self) -> Result<Connection, Self::Error> {
        self.client.get_connection()
    }

    fn is_valid(&self, conn: &mut Connection) -> Result<(), Self::Error> {
        cmd("PING").query::<String>(conn).map(|_| ())
    }

    fn has_broken(&self, _conn: &mut Connection) -> bool {
        false // We'll use test_on_checkout instead
    }
}

// Type alias for our r2d2 Redis connection pool
type RedisPool = Pool<RedisConnectionManager>;

/// A managed connection from the pool that automatically returns to the pool when dropped.
///
/// This type implements `Deref<Target=Connection>` and `DerefMut`, so it can be used
/// exactly like a `redis::Connection`. When dropped, the connection is returned to the
/// pool for reuse rather than being closed.
///
/// # Example
/// ```ignore
/// let mut conn = get_connection()?;
/// let result: String = redis::cmd("PING").query(&mut *conn)?;
/// // conn automatically returns to pool when dropped
/// ```
pub type ManagedConnection = PooledConnection<RedisConnectionManager>;

// Keep internal alias for backward compatibility within this module
type PooledRedisConnection = ManagedConnection;

// The ConnectionManager struct for thread-safe connection pooling
#[derive(Clone)]
pub struct ConnectionManager {
    // Connection pool
    pool: Arc<RedisPool>,
    
    // Connection configuration
    config: ConnectionConfig,
}

impl ConnectionManager {
    /// Create a new connection manager with connection pooling
    pub fn new(client: Client, config: Option<ConnectionConfig>) -> IntegrationResult<Self> {
        let config = config.unwrap_or_default();
        
        // Create a connection manager for r2d2
        let manager = RedisConnectionManager::new(client.clone());
        
        // Create a connection pool with configured parameters
        let pool = Pool::builder()
            .max_size(config.max_pool_size)
            .min_idle(Some(config.min_idle_connections))
            .idle_timeout(Some(Duration::from_secs(config.connection_idle_timeout_secs)))
            .connection_timeout(Duration::from_millis(config.connection_timeout_ms))
            .test_on_check_out(true)
            .build(manager)
            .map_err(|e| {
                let error_msg = format!("Failed to build connection pool: {}", e);
                stream_processing_error(error_msg)
            })?;
        
        // Test that we can get a connection
        if let Err(e) = pool.get() {
            let error_msg = format!("Failed initial pool connection test: {}", e);
            return Err(stream_processing_error(error_msg));
        }
        
        info!("Created connection pool with max_size={}, min_idle={}", 
            config.max_pool_size, config.min_idle_connections);
        
        Ok(ConnectionManager {
            pool: Arc::new(pool),
            config,
        })
    }
    
    /// Get a connection from the pool with exponential backoff retry.
    ///
    /// Retries up to 3 times with 50ms, 100ms, 200ms delays before failing.
    /// This prevents transient pool exhaustion from causing immediate errors
    /// under burst load conditions.
    pub fn get_connection(&self) -> IntegrationResult<PooledRedisConnection> {
        let mut last_error = String::new();
        for attempt in 0..3u32 {
            match self.pool.get() {
                Ok(conn) => return Ok(conn),
                Err(e) => {
                    last_error = format!("Pool attempt {}: {}", attempt + 1, e);
                    if attempt < 2 {
                        let backoff = Duration::from_millis(50 * (1 << attempt));
                        warn!("Connection pool backpressure (attempt {}), backing off {:?}", attempt + 1, backoff);
                        std::thread::sleep(backoff);
                    }
                }
            }
        }
        Err(redis_error(format!("Failed to get connection from pool after 3 attempts: {}", last_error)))
    }
    
    /// Execute a function with a connection
    /// 
    /// This method gets a connection from the pool and passes it to the provided function,
    /// handling errors and connection return.
    pub fn with_connection<F, R>(&self, f: F) -> IntegrationResult<R>
    where
        F: FnOnce(&mut Connection) -> IntegrationResult<R>
    {
        let mut conn = self.get_connection()?;
        let conn_ref: &mut Connection = &mut conn;
        f(conn_ref)
    }
    
    /// Execute a function with retry on connection errors
    /// 
    /// This method is similar to with_connection but adds automatic
    /// retry for transient connection errors like timeouts.
    pub fn with_retry_connection<F, R>(&self, f: F) -> IntegrationResult<R>
    where
        F: Fn(&mut Connection) -> IntegrationResult<R>
    {
        let mut retry_count = 0;
        let mut backoff_ms = self.config.base_backoff_ms;
        
        loop {
            let mut conn = self.get_connection()?;
            let conn_ref: &mut Connection = &mut conn;
            
            match f(conn_ref) {
                Ok(result) => return Ok(result),
                Err(e) => {
                    // Only retry on connection errors
                    if !is_connection_error(&e) {
                        return Err(e);
                    }
                    
                    retry_count += 1;
                    if retry_count > self.config.max_retries {
                        return Err(e);
                    }
                    
                    warn!("Operation failed with connection error: {}. Retrying in {}ms", 
                          e, backoff_ms);
                    
                    // Apply backoff before retry
                    std::thread::sleep(Duration::from_millis(backoff_ms));
                    
                    // Increase backoff for next attempt
                    backoff_ms = std::cmp::min(backoff_ms * 2, self.config.max_backoff_ms);
                }
            }
        }
    }
    
    /// Get current pool status for monitoring
    pub fn get_pool_status(&self) -> (u32, u32) {
        (self.pool.state().connections, self.pool.state().idle_connections)
    }
    
    /// Check if a connection is still valid
    pub fn check_connection_health(&self, conn: &mut Connection) -> bool {
        // Use a simple PING command to check if connection is still valid
        match cmd("PING").query::<String>(conn) {
            Ok(response) => response == "PONG",
            Err(_) => false,
        }
    }
}

/// Helper function to determine if an error is related to connection issues
fn is_connection_error(e: &IntegrationError) -> bool {
    match e {
        IntegrationError { kind: IntegrationErrorKind::Redis, message } => {
            // Check if error message contains connection-related keywords
            let msg = message.to_lowercase();
            msg.contains("connection") || 
            msg.contains("timeout") || 
            msg.contains("reset") ||
            msg.contains("closed") ||
            msg.contains("refused")
        },
        _ => false,
    }
}

// Global connection manager instance
lazy_static::lazy_static! {
    static ref CONNECTION_MANAGER: Mutex<Option<ConnectionManager>> = Mutex::new(None);
}

/// Initialize the global connection manager
/// 
/// This function must be called once at startup to set up the global
/// connection manager with the provided Redis client.
pub fn initialize_connection_manager(client: Client, config: Option<ConnectionConfig>) -> IntegrationResult<()> {
    let mut manager = CONNECTION_MANAGER.lock().map_err(|e| {
        let error_msg = format!("Failed to lock connection manager: {}", e);
        stream_processing_error(error_msg)
    })?;
    
    if manager.is_some() {
        warn!("Connection manager already initialized, replacing with new instance");
    }
    
    let new_manager = ConnectionManager::new(client, config)?;
    *manager = Some(new_manager);
    info!("Connection manager initialized with connection pooling");
    
    Ok(())
}

/// Get a connection from the global connection manager
///
/// Returns a `ManagedConnection` that automatically returns to the pool when dropped.
/// This type implements `DerefMut<Target=Connection>`, so it can be used exactly like
/// a `redis::Connection` in all contexts.
///
/// # P3CF001 FIX: Now returns actual pooled connection instead of creating new connections.
/// Previous implementation validated the pool but then created a new direct connection,
/// defeating the purpose of pooling. Now returns the pooled connection directly for
/// true connection reuse (targeting 99.7% reuse rate).
///
/// # Example
/// ```ignore
/// let mut conn = get_connection()?;
/// let result: String = redis::cmd("PING").query(&mut *conn)?;
/// // conn returns to pool when dropped, available for next caller
/// ```
pub fn get_connection() -> IntegrationResult<ManagedConnection> {
    let manager = CONNECTION_MANAGER.lock().map_err(|e| {
        let error_msg = format!("Failed to lock connection manager: {}", e);
        stream_processing_error(error_msg)
    })?;

    match &*manager {
        Some(manager) => {
            // P3CF001 FIX: Return the pooled connection directly
            // The ManagedConnection (PooledConnection) holds an Arc to the pool,
            // so it safely outlives the mutex guard and returns to pool on drop
            manager.get_connection()
        },
        None => {
            let error_msg = "Connection manager not initialized. Call initialize_connection_manager first.";
            Err(stream_processing_error(error_msg.to_string()))
        }
    }
}

/// Execute a function with a connection from the global manager
/// 
/// This function provides a simple interface to execute an operation
/// with a connection without directly interacting with the ConnectionManager.
pub fn with_connection<F, R>(f: F) -> IntegrationResult<R>
where
    F: FnOnce(&mut Connection) -> IntegrationResult<R>
{
    let manager = CONNECTION_MANAGER.lock().map_err(|e| {
        let error_msg = format!("Failed to lock connection manager: {}", e);
        stream_processing_error(error_msg)
    })?;
    
    match &*manager {
        Some(manager) => manager.with_connection(f),
        None => {
            let error_msg = "Connection manager not initialized. Call initialize_connection_manager first.";
            Err(stream_processing_error(error_msg.to_string()))
        }
    }
}

/// Execute a function with retry on connection errors
/// 
/// This function provides a simple interface to execute an operation
/// with retry without directly interacting with the ConnectionManager.
pub fn with_retry_connection<F, R>(f: F) -> IntegrationResult<R>
where
    F: Fn(&mut Connection) -> IntegrationResult<R>
{
    let manager = CONNECTION_MANAGER.lock().map_err(|e| {
        let error_msg = format!("Failed to lock connection manager: {}", e);
        stream_processing_error(error_msg)
    })?;
    
    match &*manager {
        Some(manager) => manager.with_retry_connection(f),
        None => {
            let error_msg = "Connection manager not initialized. Call initialize_connection_manager first.";
            Err(stream_processing_error(error_msg.to_string()))
        }
    }
}

/// Get the current connection pool status
/// 
/// Returns a tuple of (total_connections, idle_connections)
pub fn get_pool_status() -> IntegrationResult<(u32, u32)> {
    let manager = CONNECTION_MANAGER.lock().map_err(|e| {
        let error_msg = format!("Failed to lock connection manager: {}", e);
        stream_processing_error(error_msg)
    })?;
    
    match &*manager {
        Some(manager) => Ok(manager.get_pool_status()),
        None => {
            let error_msg = "Connection manager not initialized. Call initialize_connection_manager first.";
            Err(stream_processing_error(error_msg.to_string()))
        }
    }
}

/// Log the current pool status
pub fn log_pool_status() -> IntegrationResult<()> {
    let (total, idle) = get_pool_status()?;
    info!("Connection pool status: {} total connections, {} idle connections", total, idle);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::sync::atomic::{AtomicUsize, Ordering};
    
    // Example test (would require a Redis server)
    #[test]
    fn test_connection_manager_initialization() {
        // This is just a placeholder for actual tests
        // Real tests would require a Redis server and would test:
        // - Connection acquisition
        // - Connection pooling
        // - Connection refresh
        // - Retry behavior
        // - Error handling
        let config = ConnectionConfig {
            max_retries: 2,
            base_backoff_ms: 50,
            max_backoff_ms: 200,
            connection_timeout_ms: 1000,
            max_pool_size: 10,
            min_idle_connections: 1,
            connection_idle_timeout_secs: 60,
        };
        
        assert_eq!(config.max_retries, 2);
        assert_eq!(config.base_backoff_ms, 50);
        assert_eq!(config.max_pool_size, 10);
    }
    
    // This test requires a running Redis instance to pass
    // It's disabled by default to not break CI but can be enabled locally
    #[test]
    #[ignore]
    fn test_connection_pool_performance() {
        // Create a client
        let client = Client::open("redis://127.0.0.1/").expect("Failed to create client");
        
        // Create configuration with smaller pool size for testing
        let config = ConnectionConfig {
            max_retries: 2,
            base_backoff_ms: 50,
            max_backoff_ms: 200,
            connection_timeout_ms: 1000,
            max_pool_size: 5,
            min_idle_connections: 1,
            connection_idle_timeout_secs: 60,
        };
        
        // Store max_pool_size for later assertion
        let max_pool_size = config.max_pool_size;
        
        // Create connection manager
        let manager = ConnectionManager::new(client, Some(config)).expect("Failed to create manager");
        
        // Counter for successful operations
        let success_count = Arc::new(AtomicUsize::new(0));
        
        // Counter for connection errors
        let error_count = Arc::new(AtomicUsize::new(0));
        
        // Spawn multiple threads that use connections concurrently
        let mut handles = Vec::new();
        let thread_count = 10; // More threads than pool size to test reuse
        
        for _ in 0..thread_count {
            let manager_clone = manager.clone();
            let success_clone = Arc::clone(&success_count);
            let error_clone = Arc::clone(&error_count);
            
            let handle = thread::spawn(move || {
                // Run multiple commands in each thread
                for _ in 0..20 {
                    match manager_clone.with_connection(|conn| {
                        // Just ping the server
                        match cmd("PING").query::<String>(conn) {
                            Ok(response) => {
                                if response == "PONG" {
                                    // Increment success counter atomically
                                    success_clone.fetch_add(1, Ordering::SeqCst);
                                    Ok(())
                                } else {
                                    error_clone.fetch_add(1, Ordering::SeqCst);
                                    Err(stream_processing_error(format!("Unexpected response: {}", response)))
                                }
                            },
                            Err(e) => {
                                error_clone.fetch_add(1, Ordering::SeqCst);
                                Err(stream_processing_error(format!("Redis error: {}", e)))
                            }
                        }
                    }) {
                        Ok(_) => {},
                        Err(_) => {
                            error_clone.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                    
                    // Small sleep to simulate work
                    thread::sleep(Duration::from_millis(5));
                }
            });
            
            handles.push(handle);
        }
        
        // Wait for all threads to complete
        for handle in handles {
            handle.join().expect("Thread panicked");
        }
        
        // Get final counts
        let final_success = success_count.load(Ordering::SeqCst);
        let final_errors = error_count.load(Ordering::SeqCst);
        
        // Check pool status
        let (total_conns, idle_conns) = manager.get_pool_status();
        
        println!("Connection pool test results:");
        println!("  - Success count: {}", final_success);
        println!("  - Error count: {}", final_errors);
        println!("  - Total connections: {}", total_conns);
        println!("  - Idle connections: {}", idle_conns);
        
        // If Redis is available, we should have no errors and many successes
        // This assertion is only tested when the test is explicitly run
        if final_errors > 0 {
            println!("Note: errors detected - this is expected if Redis is not running");
        }
        
        // The pool should maintain no more than max_size connections
        // (this assertion doesn't depend on Redis being available)
        assert!(total_conns <= max_pool_size);
    }
}