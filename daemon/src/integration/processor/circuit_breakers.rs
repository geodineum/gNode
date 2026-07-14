// Circuit breakers module for gNode
//
// This module provides circuit breaker patterns for
// preventing repeated operation attempts when failures occur.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;
use log::{debug, warn};

/// Stream processor initialization state
/// 
/// This struct is used to track initialization attempts and
/// prevent repeated initialization in case of errors
pub struct InitializationState {
    /// Number of consecutive initialization attempts
    attempts: usize,
    
    /// Last initialization attempt timestamp
    last_attempt: Instant,
    
    /// Whether initialization has been completed successfully
    pub completed: bool,
    
    /// Count of consecutive failures
    consecutive_failures: usize,
    
    /// Whether the circuit breaker is open (initialization should be paused)
    circuit_breaker_open: bool,
    
    /// When the circuit breaker was opened
    circuit_breaker_opened_at: Option<Instant>,
}

impl InitializationState {
    /// Create a new initialization state
    fn new() -> Self {
        InitializationState {
            attempts: 0,
            last_attempt: Instant::now(),
            completed: false,
            consecutive_failures: 0,
            circuit_breaker_open: false,
            circuit_breaker_opened_at: None,
        }
    }
    
    /// Record an initialization attempt
    pub fn record_attempt(&mut self) {
        self.attempts += 1;
        self.last_attempt = Instant::now();
    }
    
    /// Record a successful initialization
    pub fn record_success(&mut self) {
        self.completed = true;
        self.consecutive_failures = 0;
        self.circuit_breaker_open = false;
        self.circuit_breaker_opened_at = None;
    }
    
    /// Record a failed initialization
    pub fn record_failure(&mut self, threshold: usize) {
        self.consecutive_failures += 1;
        
        // Check if circuit breaker should be opened
        if self.consecutive_failures >= threshold && !self.circuit_breaker_open {
            self.circuit_breaker_open = true;
            self.circuit_breaker_opened_at = Some(Instant::now());
            warn!("Circuit breaker opened after {} consecutive initialization failures", 
                  self.consecutive_failures);
        }
    }
    
    /// Check if initialization should be attempted
    /// based on circuit breaker state and cool-down period
    pub fn should_attempt(&self, cooldown_secs: u64) -> bool {
        if !self.circuit_breaker_open {
            return true;
        }
        
        if let Some(opened_at) = self.circuit_breaker_opened_at {
            // Check if cool-down period has elapsed
            if opened_at.elapsed().as_secs() >= cooldown_secs {
                debug!("Circuit breaker cool-down period elapsed, allowing initialization attempt");
                return true;
            }
        }
        
        false
    }
    
    /// Reset circuit breaker state
    pub fn reset_circuit_breaker(&mut self) {
        self.circuit_breaker_open = false;
        self.circuit_breaker_opened_at = None;
        self.consecutive_failures = 0;
    }
}

// Global initialization state (thread-safe)
lazy_static::lazy_static! {
    static ref INITIALIZATION_STATES: Mutex<HashMap<String, InitializationState>> = Mutex::new(HashMap::new());
}

/// Get or create initialization state for a node
///
/// Handles lock poisoning gracefully by recovering the guard.
/// Lock poisoning indicates a prior thread panicked while holding the lock,
/// but we recover and continue since circuit breaker state is non-critical.
pub fn get_initialization_state(node_id: &str) -> std::sync::MutexGuard<'static, HashMap<String, InitializationState>> {
    let mut states = match INITIALIZATION_STATES.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            // Lock poisoned - prior thread panicked while holding lock
            // Recover the guard anyway (circuit breaker state is non-critical)
            warn!("Circuit breaker lock was poisoned, recovering");
            poisoned.into_inner()
        }
    };

    if !states.contains_key(node_id) {
        states.insert(node_id.to_string(), InitializationState::new());
    }

    states
}