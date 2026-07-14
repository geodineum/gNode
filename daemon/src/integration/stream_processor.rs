// Stream Processor Module for gNode
//
// This is a compatibility module that re-exports components from the processor module
// to fix import errors in code that expects a stream_processor module.

use std::sync::{Arc, Mutex};

use crate::integration::error_handlings::IntegrationResult;
pub use crate::config::GNodeSettings as StreamProcessorConfig;
pub use crate::integration::ConsumerGroupState;

/// Reexport StateManager as StreamProcessorStateManager for backward compatibility
#[derive(Clone)]
pub struct StreamProcessorStateManager {
    inner: crate::integration::processor::state_manager::UnifiedStreamStateManager,
}

impl Default for StreamProcessorStateManager {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamProcessorStateManager {
    pub fn new() -> Self {
        Self {
            inner: crate::integration::processor::state_manager::UnifiedStreamStateManager::new()
        }
    }
    
    /// Get or create state for a node, forwarding to inner implementation
    pub fn get_or_create_state(&mut self, node_id: &str, config: &StreamProcessorConfig) -> Arc<Mutex<ConsumerGroupState>> {
        self.inner.get_or_create_state(node_id, config)
    }
    
    /// Execute function with mutable state reference for a specific node
    pub fn with_state<F, R>(&mut self, node_id: &str, config: &StreamProcessorConfig, f: F) -> IntegrationResult<R>
    where
        F: FnOnce(&mut ConsumerGroupState) -> IntegrationResult<R>
    {
        self.inner.with_state(node_id, config, f)
    }
    
    /// Update state after successful processing
    pub fn record_success(&mut self, node_id: &str, config: &StreamProcessorConfig, processed_count: usize) -> IntegrationResult<()> {
        self.inner.record_success(node_id, config, processed_count)
    }
    
    /// Update state after empty batch (no messages)
    pub fn record_empty(&mut self, node_id: &str, config: &StreamProcessorConfig) -> IntegrationResult<()> {
        self.inner.record_empty(node_id, config)
    }
    
    /// Update state after error
    pub fn record_error(&mut self, node_id: &str, config: &StreamProcessorConfig) -> IntegrationResult<usize> {
        self.inner.record_error(node_id, config)
    }
}


/// Test response serialization
/// 
/// This function tests serializing a response for a command.
///
/// This is a wrapper function for backward compatibility.
pub fn test_response_serialization(
    test_input: &serde_json::Value,
    command_id: &str
) -> IntegrationResult<String> {
    // Create a response using the command handler module
    let response = crate::integration::command_handler::CommandResult {
        status: "ok".to_string(),
        result: Some(test_input.clone()),
        error: None,
    }.to_response(command_id);
    
    // Serialize the response
    serde_json::to_string(&response)
        .map_err(|e| crate::integration::error_handlings::stream_processing_error(
            format!("Failed to serialize response: {}", e)
        ))
} 