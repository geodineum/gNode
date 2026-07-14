//! Shared Managers Module for gNode
//!
//! This module provides functionality for creating and accessing shared instances
//! of the ValKey function manager using the standardized thread safety patterns.

use log::debug;

use crate::valkey_function_manager::ValKeyFunctionManager;
use crate::integration::{
    error_handlings::{
        IntegrationResult,
        valkey_function_error
    },
    thread_safety::{ThreadSafeSingleton, ThreadSafetyError},
};

// Define singleton for ValKey manager
static VALKEY_MANAGER: ThreadSafeSingleton<ValKeyFunctionManager> = ThreadSafeSingleton::new();

/// Initialize shared ValKey function manager
///
/// This function creates a new ValKey function manager and stores it in a
/// static singleton for later use by other components.
///
/// # Arguments
///
/// * `manager` - The ValKey function manager to store
pub fn initialize_shared_valkey_manager(manager: ValKeyFunctionManager) {
    debug!("Initializing shared ValKey function manager");

    if VALKEY_MANAGER.initialize(manager) {
        debug!("Shared ValKey function manager initialized");
    } else {
        debug!("Shared ValKey function manager already initialized");
    }
}

/// Get shared ValKey function manager
///
/// This function returns the shared ValKey function manager if it exists.
///
/// # Returns
///
/// * `Option<std::sync::Arc<std::sync::RwLock<ValKeyFunctionManager>>>` - The shared manager or None
pub fn get_valkey_manager() -> Option<std::sync::Arc<std::sync::RwLock<ValKeyFunctionManager>>> {
    VALKEY_MANAGER.get()
}

/// Execute a function with write access to the ValKey function manager
///
/// This function provides safe access to the shared ValKey function manager for writing.
/// It handles locks and error conversions automatically.
///
/// # Arguments
///
/// * `f` - Function to execute with the ValKey function manager
///
/// # Returns
///
/// * `IntegrationResult<T>` - Result of the function or error
pub fn with_valkey_manager<T, F>(f: F) -> IntegrationResult<T>
where
    F: FnOnce(&mut ValKeyFunctionManager) -> IntegrationResult<T>
{
    // Get write access to the ValKey function manager
    match VALKEY_MANAGER.with_write(|manager| {
        f(manager)
    }) {
        Ok(result) => result,
        Err(e) => {
            let error = match e {
                ThreadSafetyError::Poisoned(msg) => {
                    valkey_function_error(format!("ValKey function manager lock was poisoned: {}", msg))
                },
                ThreadSafetyError::NotInitialized => {
                    valkey_function_error("ValKey function manager not initialized".to_string())
                },
                _ => valkey_function_error(format!("ValKey function manager access error: {}", e)),
            };
            Err(error)
        }
    }
}

/// Clear shared ValKey function manager (for testing)
///
/// This function is primarily used for testing to reset state.
pub fn clear_valkey_manager() {
    debug!("Note: OnceCell-based singletons cannot be cleared at runtime");
    debug!("Instead, create a new instance per test with unique identifiers");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_singleton_access_patterns() {
        // Note: In actual tests, we'd use test-specific singletons with unique names
        // rather than the global ones to ensure test isolation

        // Create a local singleton for testing
        let test_singleton = ThreadSafeSingleton::<usize>::new();
        assert!(!test_singleton.is_initialized());
        assert!(test_singleton.initialize(42));
        assert!(test_singleton.with_read(|v| *v == 42).unwrap());

        // Test write patterns
        assert!(test_singleton.with_write(|v| *v = 100).is_ok());
        assert_eq!(test_singleton.with_read(|v| *v).unwrap(), 100);
    }
}
