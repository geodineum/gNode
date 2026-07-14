// State Manager Module for gNode
//
// This module provides state management for stream processing, including
// batch size adjustment, backoff, and error handling.

use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use std::time::Instant;

use crate::integration::{
    IntegrationResult,
    error_handlings::stream_processing_error,
};

use crate::integration::ConsumerGroupState;
pub use crate::config::GNodeSettings;

/// Generic state manager for stream processors
///
/// This struct provides a simpler and more generic state management interface
/// for older code that hasn't migrated to UnifiedStreamStateManager yet.
#[derive(Clone)]
pub struct StateManager {
    /// Batch size for each node
    batch_sizes: HashMap<String, usize>,
    
    /// Backoff times for each node
    backoff_times: HashMap<String, u64>,
    
    /// Error counts for each node
    error_counts: HashMap<String, usize>,
    
    /// Default batch size
    default_batch_size: usize,
    
    /// Default backoff time
    default_backoff_time: u64,
}

impl Default for StateManager {
    fn default() -> Self {
        Self::new()
    }
}

impl StateManager {
    /// Create a new state manager
    pub fn new() -> Self {
        Self {
            batch_sizes: HashMap::new(),
            backoff_times: HashMap::new(), 
            error_counts: HashMap::new(),
            default_batch_size: 100,
            default_backoff_time: 100,
        }
    }
    
    /// Get batch size for a node
    pub fn get_batch_size(&self, node_id: &str) -> usize {
        *self.batch_sizes.get(node_id).unwrap_or(&self.default_batch_size)
    }
    
    /// Get backoff time for a node
    pub fn get_backoff_time(&self, node_id: &str) -> u64 {
        *self.backoff_times.get(node_id).unwrap_or(&self.default_backoff_time)
    }
    
    /// Set batch size for a node
    pub fn set_batch_size(&mut self, node_id: &str, batch_size: usize) {
        self.batch_sizes.insert(node_id.to_string(), batch_size);
    }
    
    /// Set backoff time for a node
    pub fn set_backoff_time(&mut self, node_id: &str, backoff_time: u64) {
        self.backoff_times.insert(node_id.to_string(), backoff_time);
    }
    
    /// Record successful processing
    pub fn record_success(&mut self, node_id: &str, processed_count: usize) {
        // Reset error count
        self.error_counts.insert(node_id.to_string(), 0);
        
        // Reset backoff time
        self.backoff_times.insert(node_id.to_string(), self.default_backoff_time);
        
        // Adjust batch size based on processed count
        let current_batch_size = self.get_batch_size(node_id);
        
        if processed_count >= current_batch_size {
            // Increase batch size if we're using full capacity
            let new_batch_size = current_batch_size * 2;
            self.set_batch_size(node_id, std::cmp::min(new_batch_size, 500));
        } else if processed_count < current_batch_size / 4 && current_batch_size > 10 {
            // Decrease batch size if we're using less than 25% of capacity
            let new_batch_size = current_batch_size / 2;
            self.set_batch_size(node_id, std::cmp::max(new_batch_size, 10));
        }
    }
    
    /// Record error
    pub fn record_error(&mut self, node_id: &str) {
        // Increment error count
        let error_count = self.error_counts.entry(node_id.to_string()).or_insert(0);
        *error_count += 1;
        
        // Apply exponential backoff
        let backoff_time = self.get_backoff_time(node_id);
        let new_backoff_time = std::cmp::min(backoff_time * 2, 5000);
        self.set_backoff_time(node_id, new_backoff_time);
    }
}

/// State manager for unified stream processors
#[derive(Clone)]
pub struct UnifiedStreamStateManager {
    /// State for each node, keyed by node_id
    states: HashMap<String, Arc<Mutex<ConsumerGroupState>>>,
}

impl Default for UnifiedStreamStateManager {
    fn default() -> Self {
        Self::new()
    }
}

impl UnifiedStreamStateManager {
    /// Create a new unified stream state manager
    pub fn new() -> Self {
        Self {
            states: HashMap::new()
        }
    }
    
    /// Get or create state for a node, ensuring thread-safety
    pub fn get_or_create_state(&mut self, node_id: &str, config: &GNodeSettings) -> Arc<Mutex<ConsumerGroupState>> {
        self.states.entry(node_id.to_string()).or_insert_with(|| {
            Arc::new(Mutex::new(ConsumerGroupState::new(
                config.initial_batch_size,
                config.base_backoff_ms
            )))
        }).clone()
    }
    
    /// Execute function with mutable state reference for a specific node
    pub fn with_state<F, R>(&mut self, node_id: &str, config: &GNodeSettings, f: F) -> IntegrationResult<R>
    where
        F: FnOnce(&mut ConsumerGroupState) -> IntegrationResult<R>
    {
        let state_arc = self.get_or_create_state(node_id, config);
        let mut state = state_arc.lock().map_err(|e| {
            stream_processing_error(format!("Failed to lock state for node {}: {}", node_id, e))
        })?;
        
        f(&mut state)
    }
    
    /// Update state after successful processing
    pub fn record_success(&mut self, node_id: &str, config: &GNodeSettings, processed_count: usize) -> IntegrationResult<()> {
        self.with_state(node_id, config, |state| {
            state.reset_after_success();
            state.adjust_batch_size(processed_count, config.min_batch_size, config.max_batch_size);
            state.last_empty_time = Instant::now();
            Ok(())
        })
    }
    
    /// Update state after empty batch (no messages)
    pub fn record_empty(&mut self, node_id: &str, config: &GNodeSettings) -> IntegrationResult<()> {
        self.with_state(node_id, config, |state| {
            // Only apply backoff if enough time has passed since last empty
            if state.last_empty_time.elapsed().as_secs() >= 3 {
                state.apply_backoff(config.max_backoff_ms);
            }
            Ok(())
        })
    }
    
    /// Update state after error
    pub fn record_error(&mut self, node_id: &str, config: &GNodeSettings) -> IntegrationResult<usize> {
        self.with_state(node_id, config, |state| {
            state.register_error();
            Ok(state.consecutive_errors)
        })
    }
}