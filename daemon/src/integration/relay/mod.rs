//! Relay module for inter-service message forwarding
//!
//! When a command arrives with the `_rt` (relay_target) field, the daemon resolves
//! the target site/entity and forwards the command to the target's unified stream.
//! Responses are routed back via the `_rr` (relay_reply_to) field or the
//! RelayTracker safety net.

pub mod router;
pub mod translator;
pub mod policy;
pub mod telemetry;

pub use router::{RelayDecision, resolve_relay_target};
pub use translator::{TranslationResult, translate_for_relay, detect_format, convert_format};
pub use policy::{PolicyDecision, check_relay_policy};
pub use telemetry::{RelayMetrics, RelayTelemetry, get_relay_stats};

use std::collections::HashMap;
use std::time::Instant;

/// Runtime config for relay behavior
pub struct RelayConfig {
    pub enabled: bool,
    /// Prevent infinite relay loops (default: 3)
    pub max_hop_count: u8,
    /// How long to wait for target response before eviction (default: 30000ms)
    pub relay_timeout_ms: u64,
}

impl Default for RelayConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_hop_count: 3,
            relay_timeout_ms: 30_000,
        }
    }
}

/// Tracks a relayed command awaiting a response from the target
#[derive(Debug, Clone)]
pub struct PendingRelay {
    /// Original command ID (matches response ri field)
    pub command_id: String,
    /// Where to forward the response
    pub source_stream: String,
    /// Original sender's site
    pub source_site: String,
    /// Original sender's node
    pub source_node: String,
    /// Where the command was relayed to
    pub target_stream: String,
    /// For TTL eviction
    pub relayed_at: Instant,
}

/// Thread-local (per-worker) pending relay tracker.
/// NOT Arc<RwLock<>> — each worker thread owns its own tracker.
pub struct RelayTracker {
    pending: HashMap<String, PendingRelay>,
    timeout_ms: u64,
}

impl RelayTracker {
    pub fn new(timeout_ms: u64) -> Self {
        Self {
            pending: HashMap::new(),
            timeout_ms,
        }
    }

    /// Track a relayed command for response forwarding
    pub fn track(&mut self, relay: PendingRelay) {
        self.pending.insert(relay.command_id.clone(), relay);
    }

    /// Check if a response message matches a pending relay.
    /// Returns the PendingRelay if matched (and removes it from tracking).
    pub fn match_response(&mut self, request_id: &str) -> Option<PendingRelay> {
        self.pending.remove(request_id)
    }

    /// Evict entries older than timeout_ms. Call periodically (e.g., every 30s).
    /// Returns the number of evicted entries.
    pub fn evict_stale(&mut self) -> usize {
        let timeout = self.timeout_ms;
        let before = self.pending.len();
        self.pending.retain(|_, relay| {
            relay.relayed_at.elapsed().as_millis() as u64 <= timeout
        });
        before - self.pending.len()
    }

    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }
}
