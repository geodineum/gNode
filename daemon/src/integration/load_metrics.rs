// Load Metrics Module for gNode
//
// This module provides runtime operational metrics for service instances,
// enabling load-aware service discovery. It maintains a thread-safe cache
// of service health and load information with automatic TTL-based cleanup.
//
// Key Features:
// - Composite load scoring (CPU, memory, latency, error rate)
// - Automatic stale metric cleanup (30s TTL default)
// - Health-based filtering (excludes overloaded services)
// - Thundering herd prevention (jitter among top candidates)

use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use log::{debug, warn};

/// Runtime operational metrics for a service instance
///
/// This structure holds real-time performance and load data for a service,
/// separate from its geometric capability representation. Load metrics are
/// ephemeral (TTL-based) while capabilities are persistent.
#[derive(Debug, Clone)]
pub struct LoadMetrics {
    pub service_id: String,
    pub load_factor: f64,           // 0.0-1.0 composite score (lower is better)
    pub cpu_usage: Option<f64>,     // 0.0-1.0 CPU utilization
    pub memory_usage: Option<f64>,  // 0.0-1.0 memory utilization
    pub active_requests: Option<u32>, // Current request count
    pub avg_latency_ms: Option<u64>,  // Average latency (milliseconds)
    pub error_rate: Option<f64>,      // 0.0-1.0 error percentage
    pub last_update: i64,             // Unix timestamp (ms)
    pub ttl_seconds: u64,             // Time-to-live (default: 30)
}

impl LoadMetrics {
    /// Check if metrics are stale based on TTL
    ///
    /// # Arguments
    ///
    /// * `now` - Current timestamp in milliseconds
    ///
    /// # Returns
    ///
    /// * `bool` - True if metrics have exceeded TTL
    pub fn is_stale(&self, now: i64) -> bool {
        now - self.last_update > (self.ttl_seconds * 1000) as i64
    }

    /// Check if service is healthy based on load and error thresholds
    ///
    /// A service is considered unhealthy if:
    /// - Load factor >= 0.95 (95% capacity)
    /// - Error rate >= 0.05 (5% errors)
    ///
    /// # Returns
    ///
    /// * `bool` - True if service is healthy
    pub fn is_healthy(&self) -> bool {
        self.load_factor < 0.95 &&
        self.error_rate.unwrap_or(0.0) < 0.05
    }

    /// Calculate composite score for ranking (lower is better)
    ///
    /// Scoring formula:
    /// - Load factor: 60% weight
    /// - CPU usage: 20% weight
    /// - Memory usage: 10% weight
    /// - Latency: 10% weight (normalized to 0-1 scale)
    ///
    /// # Returns
    ///
    /// * `f64` - Composite score (0.0-1.0+, lower is better)
    pub fn score(&self) -> f64 {
        self.load_factor * 0.6 +
        self.cpu_usage.unwrap_or(0.5) * 0.2 +
        self.memory_usage.unwrap_or(0.5) * 0.1 +
        (self.avg_latency_ms.unwrap_or(100) as f64 / 1000.0) * 0.1
    }
}

/// Manager for runtime load metrics with automatic cleanup
///
/// This structure maintains a thread-safe cache of service load metrics
/// with TTL-based expiration. It provides methods for updating metrics,
/// selecting optimal services, and cleaning up stale entries.
pub struct LoadMetricsManager {
    metrics: Arc<RwLock<HashMap<String, LoadMetrics>>>,
    default_ttl: u64,
}

impl LoadMetricsManager {
    /// Create a new LoadMetricsManager
    ///
    /// # Arguments
    ///
    /// * `default_ttl` - Default TTL in seconds for metrics (recommended: 30)
    ///
    /// # Returns
    ///
    /// * `Self` - New manager instance
    pub fn new(default_ttl: u64) -> Self {
        Self {
            metrics: Arc::new(RwLock::new(HashMap::new())),
            default_ttl,
        }
    }

    /// Update metrics for a service
    ///
    /// If the metrics have a TTL of 0, the default TTL will be used.
    /// This method is thread-safe and can be called concurrently.
    ///
    /// # Arguments
    ///
    /// * `metrics` - LoadMetrics instance to update/insert
    pub fn update(&self, mut metrics: LoadMetrics) {
        if metrics.ttl_seconds == 0 {
            metrics.ttl_seconds = self.default_ttl;
        }

        let mut map = match self.metrics.write() {
            Ok(guard) => guard,
            Err(poisoned) => {
                warn!("LoadMetricsManager lock poisoned during update, recovering");
                poisoned.into_inner()
            }
        };

        debug!("Updating load metrics for service: {} (load: {:.2}, score: {:.2})",
            metrics.service_id, metrics.load_factor, metrics.score());

        map.insert(metrics.service_id.clone(), metrics);
    }

    /// Get metrics for a service
    ///
    /// # Arguments
    ///
    /// * `service_id` - Service identifier
    ///
    /// # Returns
    ///
    /// * `Option<LoadMetrics>` - Cloned metrics if found, None otherwise
    pub fn get(&self, service_id: &str) -> Option<LoadMetrics> {
        let map = match self.metrics.read() {
            Ok(guard) => guard,
            Err(poisoned) => {
                warn!("LoadMetricsManager lock poisoned during get, recovering");
                poisoned.into_inner()
            }
        };

        map.get(service_id).cloned()
    }

    /// Remove stale entries based on TTL
    ///
    /// This method should be called periodically (e.g., every 10 seconds)
    /// to prevent unbounded growth of the metrics cache.
    ///
    /// # Arguments
    ///
    /// * `now` - Current timestamp in milliseconds
    ///
    /// # Returns
    ///
    /// * `usize` - Number of stale entries removed
    pub fn cleanup_stale(&self, now: i64) -> usize {
        let mut map = match self.metrics.write() {
            Ok(guard) => guard,
            Err(poisoned) => {
                warn!("LoadMetricsManager lock poisoned during cleanup, recovering");
                poisoned.into_inner()
            }
        };

        let before_count = map.len();
        map.retain(|_, metrics| !metrics.is_stale(now));
        let removed = before_count - map.len();

        if removed > 0 {
            debug!("Cleaned up {} stale load metrics", removed);
        }

        removed
    }

    /// Select optimal service from candidates based on load
    ///
    /// Two-phase selection:
    /// 1. Filter to healthy services only
    /// 2. Select service with lowest composite score
    ///
    /// If no load data exists for any candidate, falls back to first candidate.
    ///
    /// # Arguments
    ///
    /// * `candidates` - Vector of service IDs from capability-based discovery
    ///
    /// # Returns
    ///
    /// * `Option<String>` - Service ID of optimal service, or None if no candidates
    pub fn select_optimal(&self, candidates: Vec<String>) -> Option<String> {
        if candidates.is_empty() {
            return None;
        }

        let map = match self.metrics.read() {
            Ok(guard) => guard,
            Err(poisoned) => {
                warn!("LoadMetricsManager lock poisoned during select_optimal, recovering");
                poisoned.into_inner()
            }
        };

        // Find best service among candidates with load data
        let optimal = candidates.iter()
            .filter_map(|id| {
                map.get(id).map(|m| (id, m))
            })
            .filter(|(_, metrics)| metrics.is_healthy())
            .min_by(|(_, a), (_, b)| {
                a.score().partial_cmp(&b.score()).unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|(id, _)| id.clone());

        // Fallback: if no load data or all unhealthy, return first candidate
        optimal.or_else(|| {
            debug!("No load data available for candidates, using fallback selection");
            Some(candidates[0].clone())
        })
    }

    /// Select optimal service with jitter to prevent thundering herd
    ///
    /// Instead of always selecting the single best service, this method
    /// randomly selects from the top 3 services by score. This distributes
    /// load more evenly when multiple clients are making concurrent selections.
    ///
    /// # Arguments
    ///
    /// * `candidates` - Vector of service IDs from capability-based discovery
    ///
    /// # Returns
    ///
    /// * `Option<String>` - Service ID of selected service, or None if no candidates
    pub fn select_optimal_with_jitter(&self, candidates: Vec<String>) -> Option<String> {
        if candidates.is_empty() {
            return None;
        }

        let map = match self.metrics.read() {
            Ok(guard) => guard,
            Err(poisoned) => {
                warn!("LoadMetricsManager lock poisoned during select_optimal_with_jitter, recovering");
                poisoned.into_inner()
            }
        };

        // Get top services by score
        let mut ranked: Vec<_> = candidates.iter()
            .filter_map(|id| map.get(id).map(|m| (id, m)))
            .filter(|(_, m)| m.is_healthy())
            .collect();

        ranked.sort_by(|(_, a), (_, b)| {
            a.score().partial_cmp(&b.score()).unwrap_or(std::cmp::Ordering::Equal)
        });

        // Add jitter: randomly select from top 3
        let top_count = ranked.len().min(3);
        if top_count == 0 {
            debug!("No healthy services with load data, using fallback selection");
            return Some(candidates[0].clone()); // Fallback
        }

        use rand::Rng;
        let mut rng = rand::thread_rng();
        let index = rng.gen_range(0..top_count);

        debug!("Selected service {} from top {} candidates with jitter",
            ranked[index].0, top_count);

        Some(ranked[index].0.clone())
    }

    /// Get count of tracked services
    ///
    /// # Returns
    ///
    /// * `usize` - Number of services with load metrics
    pub fn count(&self) -> usize {
        let map = match self.metrics.read() {
            Ok(guard) => guard,
            Err(poisoned) => {
                warn!("LoadMetricsManager lock poisoned during count, recovering");
                poisoned.into_inner()
            }
        };

        map.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_metrics_is_stale() {
        let metrics = LoadMetrics {
            service_id: "test".to_string(),
            load_factor: 0.5,
            cpu_usage: None,
            memory_usage: None,
            active_requests: None,
            avg_latency_ms: None,
            error_rate: None,
            last_update: 1000,
            ttl_seconds: 30,
        };

        assert!(!metrics.is_stale(1000)); // Same time
        assert!(!metrics.is_stale(30999)); // Just before TTL
        assert!(metrics.is_stale(31001)); // After TTL
    }

    #[test]
    fn test_load_metrics_is_healthy() {
        let healthy = LoadMetrics {
            service_id: "test".to_string(),
            load_factor: 0.8,
            cpu_usage: None,
            memory_usage: None,
            active_requests: None,
            avg_latency_ms: None,
            error_rate: Some(0.02),
            last_update: 1000,
            ttl_seconds: 30,
        };

        assert!(healthy.is_healthy());

        let overloaded = LoadMetrics {
            load_factor: 0.96,
            error_rate: Some(0.02),
            ..healthy.clone()
        };

        assert!(!overloaded.is_healthy());

        let high_errors = LoadMetrics {
            load_factor: 0.8,
            error_rate: Some(0.06),
            ..healthy.clone()
        };

        assert!(!high_errors.is_healthy());
    }

    #[test]
    fn test_load_metrics_score() {
        let metrics = LoadMetrics {
            service_id: "test".to_string(),
            load_factor: 0.5,
            cpu_usage: Some(0.4),
            memory_usage: Some(0.6),
            active_requests: None,
            avg_latency_ms: Some(200),
            error_rate: None,
            last_update: 1000,
            ttl_seconds: 30,
        };

        // Score = 0.5*0.6 + 0.4*0.2 + 0.6*0.1 + 0.2*0.1 = 0.3 + 0.08 + 0.06 + 0.02 = 0.46
        let score = metrics.score();
        assert!((score - 0.46).abs() < 0.01);
    }

    #[test]
    fn test_manager_update_and_get() {
        let manager = LoadMetricsManager::new(30);

        let metrics = LoadMetrics {
            service_id: "service1".to_string(),
            load_factor: 0.5,
            cpu_usage: None,
            memory_usage: None,
            active_requests: None,
            avg_latency_ms: None,
            error_rate: None,
            last_update: 1000,
            ttl_seconds: 0, // Should use default
        };

        manager.update(metrics.clone());

        let retrieved = manager.get("service1").unwrap();
        assert_eq!(retrieved.service_id, "service1");
        assert_eq!(retrieved.ttl_seconds, 30); // Default applied
    }

    #[test]
    fn test_manager_cleanup_stale() {
        let manager = LoadMetricsManager::new(30);

        let metrics1 = LoadMetrics {
            service_id: "service1".to_string(),
            load_factor: 0.5,
            cpu_usage: None,
            memory_usage: None,
            active_requests: None,
            avg_latency_ms: None,
            error_rate: None,
            last_update: 1000,
            ttl_seconds: 30,
        };

        let metrics2 = LoadMetrics {
            service_id: "service2".to_string(),
            last_update: 50000, // More recent
            ..metrics1.clone()
        };

        manager.update(metrics1);
        manager.update(metrics2);

        assert_eq!(manager.count(), 2);

        // Cleanup at time 40000 should remove service1 (1000 + 30000 = 31000 < 40000)
        let removed = manager.cleanup_stale(40000);
        assert_eq!(removed, 1);
        assert_eq!(manager.count(), 1);
        assert!(manager.get("service2").is_some());
        assert!(manager.get("service1").is_none());
    }

    #[test]
    fn test_manager_select_optimal() {
        let manager = LoadMetricsManager::new(30);

        let metrics1 = LoadMetrics {
            service_id: "service1".to_string(),
            load_factor: 0.8,
            cpu_usage: Some(0.7),
            memory_usage: Some(0.6),
            active_requests: None,
            avg_latency_ms: Some(150),
            error_rate: Some(0.02),
            last_update: 1000,
            ttl_seconds: 30,
        };

        let metrics2 = LoadMetrics {
            service_id: "service2".to_string(),
            load_factor: 0.3,
            cpu_usage: Some(0.2),
            memory_usage: Some(0.4),
            avg_latency_ms: Some(50),
            ..metrics1.clone()
        };

        manager.update(metrics1);
        manager.update(metrics2);

        let candidates = vec!["service1".to_string(), "service2".to_string()];
        let optimal = manager.select_optimal(candidates).unwrap();

        // service2 should have lower score
        assert_eq!(optimal, "service2");
    }

    #[test]
    fn test_manager_select_optimal_fallback() {
        let manager = LoadMetricsManager::new(30);

        // No metrics loaded
        let candidates = vec!["service1".to_string(), "service2".to_string()];
        let optimal = manager.select_optimal(candidates).unwrap();

        // Should fallback to first candidate
        assert_eq!(optimal, "service1");
    }
}
