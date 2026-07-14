//! Worker abstraction for gNode daemon
//!
//! This module provides a unified interface for daemon workers that can operate
//! in both multi-threaded (spawned) and single-threaded (tick-based) modes.
//!
//! # Architecture
//!
//! The `DaemonWorker` trait defines a cooperative worker interface where each
//! worker can perform a single unit of work via `tick()`. In multi-threaded mode,
//! workers are spawned in separate threads with their own loops. In single-threaded
//! mode, all workers are ticked cooperatively from the main thread.
//!
//! # Example
//!
//! ```ignore
//! struct MyWorker { /* state */ }
//!
//! impl DaemonWorker for MyWorker {
//!     fn name(&self) -> &str { "my-worker" }
//!     fn tick(&mut self) -> TickResult {
//!         // Do work, return Idle/Busy/Error
//!         TickResult::Idle
//!     }
//! }
//! ```

use std::time::{Duration, Instant};
use std::sync::Arc;
use std::thread;
use log::{info, warn, error, debug};

use crate::daemon::is_shutdown_requested;

/// Result of a single worker tick
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TickResult {
    /// Worker had no work to do
    Idle,
    /// Worker processed some work
    Busy,
    /// Worker encountered an error and should back off
    Error,
    /// Worker wants to shut down (e.g., critical failure)
    Shutdown,
}

/// Configuration for worker timing behavior
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// Minimum interval between ticks when idle (prevents tight spinning)
    pub idle_interval: Duration,
    /// Base backoff duration after errors
    pub error_backoff: Duration,
    /// Maximum backoff duration
    pub max_backoff: Duration,
    /// How often to check for shutdown in long operations
    pub shutdown_check_interval: Duration,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            idle_interval: Duration::from_millis(10),
            error_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(5),
            shutdown_check_interval: Duration::from_secs(1),
        }
    }
}

/// Trait for daemon workers that can operate in both multi-threaded and single-threaded modes
pub trait DaemonWorker: Send + 'static {
    /// Worker name for logging and identification
    fn name(&self) -> &str;

    /// Perform a single unit of work
    ///
    /// This method should be non-blocking and complete relatively quickly.
    /// For long-running operations, break them into smaller chunks or
    /// use the shutdown check internally.
    fn tick(&mut self) -> TickResult;

    /// Get worker configuration
    fn config(&self) -> WorkerConfig {
        WorkerConfig::default()
    }

    /// Initialize the worker (called once before first tick)
    fn init(&mut self) -> Result<(), String> {
        Ok(())
    }

    /// Cleanup when shutting down (called once after last tick)
    fn shutdown(&mut self) {
        // Default: no cleanup needed
    }
}

/// Registry for managing multiple workers in single-threaded mode
pub struct WorkerRegistry {
    workers: Vec<Box<dyn DaemonWorker>>,
    backoffs: Vec<(Duration, Instant)>, // (current_backoff, last_error_time)
    config: WorkerConfig,
}

impl WorkerRegistry {
    /// Create a new worker registry with default config
    pub fn new() -> Self {
        Self::with_config(WorkerConfig::default())
    }

    /// Create a new worker registry with custom config
    pub fn with_config(config: WorkerConfig) -> Self {
        Self {
            workers: Vec::new(),
            backoffs: Vec::new(),
            config,
        }
    }

    /// Register a worker
    pub fn register<W: DaemonWorker>(&mut self, worker: W) {
        self.workers.push(Box::new(worker));
        self.backoffs.push((self.config.error_backoff, Instant::now()));
    }

    /// Initialize all workers
    pub fn init_all(&mut self) -> Result<(), String> {
        for worker in &mut self.workers {
            if let Err(e) = worker.init() {
                return Err(format!("Worker '{}' failed to initialize: {}", worker.name(), e));
            }
            info!("[{}] Worker initialized", worker.name());
        }
        Ok(())
    }

    /// Get the number of registered workers
    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }

    /// Tick all workers once (single-threaded cooperative scheduling)
    ///
    /// Returns true if any worker was busy (did work)
    pub fn tick_all(&mut self) -> bool {
        let mut any_busy = false;

        for (i, worker) in self.workers.iter_mut().enumerate() {
            // Check if this worker is in backoff
            let (ref mut backoff, ref mut last_error) = self.backoffs[i];
            if last_error.elapsed() < *backoff {
                // Skip this worker, still in backoff
                continue;
            }

            // Perform tick
            let result = worker.tick();

            match result {
                TickResult::Idle => {
                    // Reset backoff
                    *backoff = self.config.error_backoff;
                }
                TickResult::Busy => {
                    // Reset backoff, mark as busy
                    *backoff = self.config.error_backoff;
                    any_busy = true;
                }
                TickResult::Error => {
                    // Enter backoff
                    debug!("[{}] Error, entering backoff for {:?}", worker.name(), *backoff);
                    *last_error = Instant::now();
                    *backoff = (*backoff * 2).min(self.config.max_backoff);
                }
                TickResult::Shutdown => {
                    // Mark for removal (handled separately)
                    warn!("[{}] Worker requested shutdown", worker.name());
                }
            }
        }

        any_busy
    }

    /// Shutdown all workers
    pub fn shutdown_all(&mut self) {
        for worker in &mut self.workers {
            debug!("[{}] Shutting down worker", worker.name());
            worker.shutdown();
        }
        info!("All {} workers shut down", self.workers.len());
    }

    /// Run the main loop for single-threaded mode
    ///
    /// This cooperative scheduler ticks all workers in round-robin fashion,
    /// sleeping briefly when all workers are idle.
    pub fn run_single_threaded(&mut self) {
        info!("Starting single-threaded worker loop with {} workers", self.workers.len());

        // Initialize all workers
        if let Err(e) = self.init_all() {
            error!("Failed to initialize workers: {}", e);
            return;
        }

        let mut last_activity = Instant::now();
        let max_idle_sleep = Duration::from_millis(50); // Cap idle sleep

        while !is_shutdown_requested() {
            let any_busy = self.tick_all();

            if any_busy {
                last_activity = Instant::now();
                // No sleep when busy - maximize throughput
            } else {
                // All workers idle - sleep briefly
                // Adaptive: longer sleep when idle for a while
                let idle_duration = last_activity.elapsed();
                let sleep_duration = if idle_duration > Duration::from_secs(5) {
                    max_idle_sleep
                } else {
                    self.config.idle_interval
                };
                thread::sleep(sleep_duration);
            }
        }

        // Shutdown sequence
        info!("Shutdown requested, stopping single-threaded worker loop");
        self.shutdown_all();
    }
}

impl Default for WorkerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Concrete Worker Implementations
// ============================================================================

/// Health metrics cleanup worker
pub struct HealthCleanupWorker {
    load_manager: Arc<crate::integration::load_metrics::LoadMetricsManager>,
    last_cleanup: Instant,
    cleanup_interval: Duration,
}

impl HealthCleanupWorker {
    pub fn new(load_manager: Arc<crate::integration::load_metrics::LoadMetricsManager>) -> Self {
        Self {
            load_manager,
            last_cleanup: Instant::now(),
            cleanup_interval: Duration::from_secs(10),
        }
    }
}

impl DaemonWorker for HealthCleanupWorker {
    fn name(&self) -> &str {
        "health-cleanup"
    }

    fn tick(&mut self) -> TickResult {
        if self.last_cleanup.elapsed() >= self.cleanup_interval {
            let now = crate::utils::current_timestamp_ms();
            let removed = self.load_manager.cleanup_stale(now);
            if removed > 0 {
                debug!("[health-cleanup] Cleaned up {} stale health metrics", removed);
            }
            self.last_cleanup = Instant::now();
            TickResult::Busy
        } else {
            TickResult::Idle
        }
    }

    fn config(&self) -> WorkerConfig {
        WorkerConfig {
            idle_interval: Duration::from_secs(1), // Check every second
            ..Default::default()
        }
    }
}

/// Stream discovery refresh worker
pub struct DiscoveryRefreshWorker {
    stream_discovery: Arc<std::sync::RwLock<crate::integration::stream_discovery::StreamDiscoveryManager>>,
    environment: String,
    last_refresh: Instant,
    refresh_interval: Duration,
    debug_mode: bool,
}

impl DiscoveryRefreshWorker {
    pub fn new(
        stream_discovery: Arc<std::sync::RwLock<crate::integration::stream_discovery::StreamDiscoveryManager>>,
        environment: String,
        debug_mode: bool,
    ) -> Self {
        Self {
            stream_discovery,
            environment,
            last_refresh: Instant::now(),
            refresh_interval: Duration::from_secs(60),
            debug_mode,
        }
    }
}

impl DaemonWorker for DiscoveryRefreshWorker {
    fn name(&self) -> &str {
        "discovery-refresh"
    }

    fn tick(&mut self) -> TickResult {
        if self.last_refresh.elapsed() < self.refresh_interval {
            return TickResult::Idle;
        }

        self.last_refresh = Instant::now();

        match crate::integration::connection_manager::get_connection() {
            Ok(mut conn) => {
                match self.stream_discovery.write() {
                    Ok(discovery) => {
                        if discovery.needs_refresh() {
                            if self.debug_mode {
                                debug!("[discovery-refresh] Refreshing for environment: {}", self.environment);
                            }

                            match discovery.refresh(&mut conn) {
                                Ok(()) => {
                                    if discovery.has_new_streams() {
                                        let new_streams = discovery.take_newly_added();
                                        info!("[discovery-refresh] {} new streams detected!", new_streams.len());
                                    }
                                    TickResult::Busy
                                }
                                Err(e) => {
                                    warn!("[discovery-refresh] Failed: {:?}", e);
                                    TickResult::Error
                                }
                            }
                        } else {
                            TickResult::Idle
                        }
                    }
                    Err(e) => {
                        warn!("[discovery-refresh] Failed to acquire lock: {}", e);
                        TickResult::Error
                    }
                }
            }
            Err(e) => {
                warn!("[discovery-refresh] Failed to get connection: {:?}", e);
                TickResult::Error
            }
        }
    }

    fn config(&self) -> WorkerConfig {
        WorkerConfig {
            idle_interval: Duration::from_secs(5), // Check every 5 seconds
            error_backoff: Duration::from_secs(5),
            max_backoff: Duration::from_secs(30),
            ..Default::default()
        }
    }
}

/// Service discovery worker — periodic scan of config files for service registration
pub struct ServiceDiscoveryWorker {
    service_discovery: Arc<std::sync::RwLock<crate::integration::service_discovery::ServiceDiscoveryManager>>,
    last_tick: Instant,
    scan_interval: Duration,
}

impl ServiceDiscoveryWorker {
    pub fn new(
        service_discovery: Arc<std::sync::RwLock<crate::integration::service_discovery::ServiceDiscoveryManager>>,
        scan_interval_secs: u64,
    ) -> Self {
        Self {
            service_discovery,
            last_tick: Instant::now(),
            scan_interval: Duration::from_secs(scan_interval_secs),
        }
    }
}

impl DaemonWorker for ServiceDiscoveryWorker {
    fn name(&self) -> &str {
        "service-discovery"
    }

    fn tick(&mut self) -> TickResult {
        if self.last_tick.elapsed() < self.scan_interval {
            return TickResult::Idle;
        }

        self.last_tick = Instant::now();

        match crate::integration::connection_manager::get_connection() {
            Ok(mut conn) => {
                match self.service_discovery.write() {
                    Ok(mut discovery) => {
                        if discovery.needs_scan() {
                            match discovery.discover_and_register(&mut conn) {
                                Ok(result) => {
                                    if !result.skipped && result.registered > 0 {
                                        info!("[service-discovery] Registered {} services for {} sites",
                                              result.registered, result.sites);
                                    }
                                    TickResult::Busy
                                }
                                Err(e) => {
                                    warn!("[service-discovery] Failed: {:?}", e);
                                    TickResult::Error
                                }
                            }
                        } else {
                            TickResult::Idle
                        }
                    }
                    Err(e) => {
                        warn!("[service-discovery] Failed to acquire lock: {}", e);
                        TickResult::Error
                    }
                }
            }
            Err(e) => {
                warn!("[service-discovery] Failed to get connection: {:?}", e);
                TickResult::Error
            }
        }
    }

    fn config(&self) -> WorkerConfig {
        WorkerConfig {
            idle_interval: Duration::from_secs(10),
            error_backoff: Duration::from_secs(10),
            max_backoff: Duration::from_secs(60),
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestWorker {
        tick_count: usize,
        max_ticks: usize,
    }

    impl DaemonWorker for TestWorker {
        fn name(&self) -> &str {
            "test-worker"
        }

        fn tick(&mut self) -> TickResult {
            self.tick_count += 1;
            if self.tick_count >= self.max_ticks {
                TickResult::Shutdown
            } else {
                TickResult::Busy
            }
        }
    }

    #[test]
    fn test_worker_registry() {
        let mut registry = WorkerRegistry::new();
        registry.register(TestWorker { tick_count: 0, max_ticks: 5 });

        assert_eq!(registry.worker_count(), 1);

        // Tick until shutdown
        for _ in 0..5 {
            registry.tick_all();
        }
    }
}
