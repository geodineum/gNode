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

/// Load sampler worker: pools observations, derives dimension 16, writes it.
///
/// Reads every site's health stream through its OWN consumer group
/// (`gnode-sampler`), not the workers' group. That is what makes the derivation
/// independent of which node holds the lease: the workers' group splits messages
/// between nodes, so a holder reading through it would aggregate a fraction of the
/// traffic and write it as the truth. A separate group gets every observation.
///
/// Only the lease holder reads and writes. Every node PUBLISHES observations (see
/// relay telemetry), so the input is pooled while the axis has one writer.
pub struct LoadSamplerWorker {
    sampler: crate::integration::sampler::Sampler,
    stream_discovery: Arc<std::sync::RwLock<crate::integration::stream_discovery::StreamDiscoveryManager>>,
    topology_namespace: String,
    node_id: String,
    mode: SamplerMode,
    last_tick: Instant,
    tick_interval: Duration,
    /// Baselines are reloaded when the lease is taken, so a new holder does not
    /// relearn every provider's normal from scratch.
    seeded: bool,
}

/// What the sampler is allowed to do. `Shadow` computes and logs without writing —
/// the 48h gate before the axis carries weight.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SamplerMode { Off, Shadow, Write }

impl SamplerMode {
    pub fn from_env() -> Self {
        match std::env::var("GNODE_SAMPLER").unwrap_or_default().to_ascii_lowercase().as_str() {
            "write" | "on" => SamplerMode::Write,
            "off" => SamplerMode::Off,
            _ => SamplerMode::Shadow,
        }
    }
}

impl LoadSamplerWorker {
    pub fn new(
        stream_discovery: Arc<std::sync::RwLock<crate::integration::stream_discovery::StreamDiscoveryManager>>,
        topology_namespace: String,
        node_id: String,
    ) -> Self {
        let cfg = crate::integration::sampler::SamplerConfig::from_env();
        let tick_interval = Duration::from_secs(cfg.tick_secs);
        Self {
            sampler: crate::integration::sampler::Sampler::new(cfg),
            stream_discovery,
            topology_namespace,
            node_id,
            mode: SamplerMode::from_env(),
            last_tick: Instant::now(),
            tick_interval,
            seeded: false,
        }
    }

    fn health_streams(&self) -> Vec<String> {
        let mut streams: Vec<String> = self.stream_discovery.read()
            .map(|d| d.get_health_streams().into_iter().map(|s| s.key).collect())
            .unwrap_or_default();
        // Discovery lists registered SITES. The daemons' own observations land in
        // the namespace's stream, because that is where their entities live, and
        // the namespace is not a site.
        let ns = crate::config::build_health_stream_key(&self.topology_namespace);
        if !streams.contains(&ns) {
            streams.push(ns);
        }
        streams
    }

    /// Which topology an observation on this stream belongs to. The namespace's
    /// stream carries node observations; a site's carries its services'.
    fn tier_for(&self, site: &str) -> crate::integration::sampler::Tier {
        if site == self.topology_namespace {
            crate::integration::sampler::Tier::Node
        } else {
            crate::integration::sampler::Tier::Service
        }
    }

    /// Drain the observations waiting in one site's health stream.
    fn read_observations(&mut self, conn: &mut redis::Connection, stream: &str) -> usize {
        use crate::integration::sampler::Observation;
        let Some(site) = crate::config::site_of_stream_key(stream).map(|s| s.to_string()) else { return 0 };

        // The group is ours alone; MKSTREAM so a site with no traffic yet is not an error.
        let _: redis::RedisResult<String> = redis::cmd("XGROUP").arg("CREATE").arg(stream)
            .arg(SAMPLER_GROUP).arg("0").arg("MKSTREAM").query(conn);

        let reply: redis::RedisResult<redis::streams::StreamReadReply> = redis::cmd("XREADGROUP")
            .arg("GROUP").arg(SAMPLER_GROUP).arg(&self.node_id)
            .arg("COUNT").arg(500).arg("STREAMS").arg(stream).arg(">")
            .query(conn);
        let Ok(reply) = reply else { return 0 };

        let mut ids = Vec::new();
        let mut taken = 0usize;
        for key in reply.keys {
            for entry in key.ids {
                ids.push(entry.id.clone());
                let field = |k: &str| entry.get::<String>(k);
                // `t=rq` carries an observed latency; anything else on this stream
                // is not ours to interpret, and is acknowledged so it cannot pile
                // up in our group the way 114 records did in the workers'.
                if field("t").as_deref() != Some("rq") { continue; }
                let Some(entity) = field("si").filter(|s| !s.is_empty()) else { continue };
                let Some(lat) = field("lat").and_then(|v| v.parse::<f64>().ok()) else { continue };
                if !lat.is_finite() || lat < 0.0 { continue }
                let ts_ms = crate::integration::sampler::observed_at_ms(
                    field("ts").as_deref(), &entry.id);
                let now_ms = crate::utils::current_timestamp_ms().max(0) as u64;
                if !self.sampler.is_current(ts_ms, now_ms) {
                    // Acknowledged above, deliberately not observed: a stream's
                    // backlog is history, not the present.
                    continue;
                }
                self.sampler.observe(Observation {
                    tier: self.tier_for(&site),
                    site: site.clone(),
                    entity,
                    command: field("cmd").unwrap_or_else(|| "unknown".into()),
                    elapsed_ms: lat as u64,
                    ok: field("ok").map(|v| v != "0").unwrap_or(true),
                    ts_ms,
                });
                taken += 1;
            }
        }
        if !ids.is_empty() {
            let mut ack = redis::cmd("XACK");
            ack.arg(stream).arg(SAMPLER_GROUP);
            for id in &ids { ack.arg(id); }
            let _: redis::RedisResult<i64> = ack.query(conn);
        }
        taken
    }

    fn baseline_key(&self, site: &str) -> String { format!("{{{}}}:gnode:baseline", site) }

    fn seed_baselines(&mut self, conn: &mut redis::Connection) {
        for stream in self.health_streams() {
            let Some(site) = crate::config::site_of_stream_key(&stream).map(|s| s.to_string()) else { continue };
            let stored: redis::RedisResult<std::collections::HashMap<String, String>> =
                redis::cmd("HGETALL").arg(self.baseline_key(&site)).query(conn);
            if let Ok(map) = stored {
                let tier = self.tier_for(&site);
                for (field, value) in map {
                    if let (Some((entity, command)), Ok(p50)) = (field.split_once('|'), value.parse::<f64>()) {
                        self.sampler.seed_baseline(tier, &site, entity, command, p50);
                    }
                }
            }
        }
        self.seeded = true;
    }

    fn save_baselines(&self, conn: &mut redis::Connection) {
        let mut per_site: std::collections::HashMap<String, Vec<(String, String)>> = std::collections::HashMap::new();
        for (_tier, site, entity, command, p50) in self.sampler.baselines() {
            per_site.entry(site).or_default().push((format!("{}|{}", entity, command), format!("{:.3}", p50)));
        }
        for (site, fields) in per_site {
            let mut cmd = redis::cmd("HSET");
            cmd.arg(self.baseline_key(&site));
            for (f, v) in fields { cmd.arg(f).arg(v); }
            let _: redis::RedisResult<i64> = cmd.query(conn);
        }
    }
}

/// The sampler's own consumer group, separate from the workers' by design.
const SAMPLER_GROUP: &str = "gnode-sampler";

impl DaemonWorker for LoadSamplerWorker {
    fn name(&self) -> &str { "load-sampler" }

    fn tick(&mut self) -> TickResult {
        if self.mode == SamplerMode::Off || self.last_tick.elapsed() < self.tick_interval {
            return TickResult::Idle;
        }
        self.last_tick = Instant::now();

        let Ok(mut conn) = crate::integration::connection_manager::get_connection() else {
            return TickResult::Error;
        };
        // One writer of the axis. A node that does not hold the lease does not even
        // read: the group's position would then advance on a node that cannot write.
        if !crate::integration::lease::hold_writer_lease(&mut conn, &self.node_id) {
            return TickResult::Idle;
        }
        if !self.seeded {
            self.seed_baselines(&mut conn);
        }

        let mut observed = 0usize;
        for stream in self.health_streams() {
            observed += self.read_observations(&mut conn, &stream);
        }

        let writes = self.sampler.tick(crate::utils::current_timestamp_ms().max(0) as u64);
        if writes.is_empty() {
            return if observed > 0 { TickResult::Busy } else { TickResult::Idle };
        }

        for w in &writes {
            if self.mode == SamplerMode::Shadow {
                // Quiet by construction: a write only happens on a band change or a
                // stamp refresh, so this is not a per-tick line.
                info!("[load-sampler] shadow: {:?} {} in {} would read {:.4} ({})",
                      w.tier, w.entity, w.site, w.pd, w.reason);
                continue;
            }
            let updates = format!(r#"{{"{}":{{"pd":{:.4},"pr":"{}"}}}}"#,
                w.index, w.pd,
                crate::geometric_precision::FixedPoint::from_f64(w.pd).raw());
            let topology_key = match w.tier {
                crate::integration::sampler::Tier::Node =>
                    format!("{{{}}}:gnode:constellation", self.topology_namespace),
                crate::integration::sampler::Tier::Service =>
                    crate::GeometricTopology::get_services_topology_key(&w.site),
            };
            let result: redis::RedisResult<String> = redis::cmd("FCALL")
                .arg("GNODE_TOPO_SET_DERIVED").arg(1)
                .arg(&topology_key)
                .arg(&w.entity).arg(&updates).arg(w.zs)
                .arg(format!("{{{}}}:gnode:topology:services", self.topology_namespace))
                .arg(crate::integration::processor::stream_utils::current_timestamp())
                .arg(crate::integration::handlers::types::HASHED_DIMENSIONS)
                .arg(&self.node_id)
                .query(&mut conn);
            match result {
                Ok(_) => info!("[load-sampler] {:?} {} in {} → {:.4} ({})",
                               w.tier, w.entity, w.site, w.pd, w.reason),
                // A provider can be observed before it is registered; that is the
                // publisher's business, not a reason to stop sampling.
                Err(e) => debug!("[load-sampler] write for {} refused: {}", w.entity, e),
            }
        }
        self.save_baselines(&mut conn);
        TickResult::Busy
    }

    fn config(&self) -> WorkerConfig {
        WorkerConfig { idle_interval: Duration::from_secs(1), ..Default::default() }
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
