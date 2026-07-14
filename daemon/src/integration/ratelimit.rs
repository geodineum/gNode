//! Per-site rate limiting for the unified stream consumer loop.
//!
//! GN-D2.03 closure (Tier-2 commit 2.1.c). Without this, a single tenant
//! flooding the unified consumer can monopolize the worker pool and starve
//! every other site sharing the same daemon.
//!
//! Implementation: in-process token bucket per `site_id`. One token == one
//! command processed. Tokens replenish at a configurable rate up to a
//! configurable burst capacity. Rejection returns an explicit
//! `IntegrationError` with kind `Generic` and a `[ratelimit]`-prefixed
//! message so log filtering can distinguish rate-limit rejections from
//! generic command failures.
//!
//! Per-process state is correct for single-node deployments: a flood that
//! starves others on the same WORKER concerns the same process. Multi-node
//! tenancy should swap to a Lua-atomic ValKey token bucket keyed at
//! `{site_id}:gnode:ratelimit:tokens` so the rate is shared across the
//! constellation. Until then, in-process is the right choke point and
//! avoids one ValKey round-trip per command.
//!
//! Configuration via environment variable (consumed at module init):
//!
//!   `GNODE_RATELIMIT_CAPACITY`       — max tokens per bucket (default 100)
//!   `GNODE_RATELIMIT_REFILL_PER_SEC` — tokens added per second (default 50)
//!
//! Both are also published in the gNode `config_schema.yaml` so wp-admin
//! surfaces them as operator-tunable.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;

use once_cell::sync::Lazy;

use crate::integration::error_handlings::{IntegrationError, IntegrationErrorKind};

#[derive(Debug)]
struct TokenBucket {
    /// Current token count (fractional — replenishes continuously).
    tokens: f64,
    /// Last instant we refilled the bucket.
    last_refill: Instant,
}

impl TokenBucket {
    fn new(initial: f64) -> Self {
        Self {
            tokens: initial,
            last_refill: Instant::now(),
        }
    }

    /// Try to consume one token. Returns `true` on success, `false` if
    /// the bucket is empty after the just-applied refill.
    fn try_take(&mut self, capacity: f64, refill_per_sec: f64) -> bool {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * refill_per_sec).min(capacity);
        self.last_refill = now;
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

#[derive(Debug)]
struct Config {
    capacity: f64,
    refill_per_sec: f64,
}

impl Config {
    fn from_env() -> Self {
        let capacity = std::env::var("GNODE_RATELIMIT_CAPACITY")
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
            .filter(|n| *n > 0.0)
            .unwrap_or(100.0);
        let refill_per_sec = std::env::var("GNODE_RATELIMIT_REFILL_PER_SEC")
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
            .filter(|n| *n > 0.0)
            .unwrap_or(50.0);
        Self {
            capacity,
            refill_per_sec,
        }
    }
}

static CONFIG: Lazy<Config> = Lazy::new(Config::from_env);
static BUCKETS: Lazy<Mutex<HashMap<String, TokenBucket>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

/// Try to acquire one token for `site_id`.
///
/// Returns `Ok(())` if a token was available (command may proceed).
/// Returns `Err(IntegrationError)` with `[ratelimit]`-prefixed message
/// if the bucket is exhausted.
///
/// Empty `site_id` short-circuits to `Ok(())` — internal/system commands
/// that don't carry a site identity are not rate-limited.
pub fn try_acquire(site_id: &str) -> Result<(), IntegrationError> {
    if site_id.is_empty() {
        return Ok(());
    }

    let mut buckets = BUCKETS.lock().unwrap_or_else(|p| {
        // Mutex poisoned by a panic in another thread — recover the guard
        // rather than propagating the panic. Better to over-rate-limit for
        // one request than to crash the whole daemon.
        p.into_inner()
    });

    let bucket = buckets
        .entry(site_id.to_string())
        .or_insert_with(|| TokenBucket::new(CONFIG.capacity));

    if bucket.try_take(CONFIG.capacity, CONFIG.refill_per_sec) {
        Ok(())
    } else {
        Err(IntegrationError::new(
            IntegrationErrorKind::Generic,
            format!(
                "[ratelimit] site '{}' exceeded {} tokens/sec sustained / {} burst",
                site_id, CONFIG.refill_per_sec, CONFIG.capacity
            ),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh_bucket() -> TokenBucket {
        TokenBucket::new(5.0)
    }

    #[test]
    fn empty_site_short_circuits() {
        // Even with the global bucket exhausted, empty site_id always proceeds.
        assert!(try_acquire("").is_ok());
    }

    #[test]
    fn bucket_drains_then_refills() {
        let mut b = fresh_bucket();
        // 5 tokens available — drain them.
        for _ in 0..5 {
            assert!(b.try_take(5.0, 1.0));
        }
        // 6th attempt fails immediately (no time elapsed).
        assert!(!b.try_take(5.0, 1.0));
        // Sleep ~100ms — at 100 tok/sec refill, ~10 tokens accumulate (capped at 5).
        std::thread::sleep(std::time::Duration::from_millis(100));
        assert!(b.try_take(5.0, 100.0));
    }

    #[test]
    fn distinct_sites_have_independent_buckets() {
        // Drain site A.
        for _ in 0..(CONFIG.capacity as usize) {
            let _ = try_acquire("test-site-a");
        }
        // Site B should still have a full bucket.
        assert!(try_acquire("test-site-b").is_ok());
    }
}
