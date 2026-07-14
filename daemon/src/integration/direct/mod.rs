//! Direct Channel Module for Geodineum Service Daemon
//!
//! Provides gNode-provisioned direct inter-service communication channels.
//! gNode creates the stream and consumer groups, then steps out of the hot path —
//! services XADD/XREADGROUP directly on the channel stream.
//!
//! Two modes:
//! - **Temporary**: TTL-based, auto-expires after ttl_seconds or max_idle_seconds
//! - **Persistent**: No TTL, survives daemon restarts, closed explicitly or on service deregistration

pub mod provisioner;

pub use provisioner::{
    provision_channel,
    close_channel,
    get_channel_info,
    list_channels,
    check_expiry,
};

use std::fmt;

/// Channel mode: temporary (TTL) or persistent (explicit close)
#[derive(Debug, Clone, PartialEq)]
pub enum ChannelMode {
    /// TTL-based, auto-expires after configured seconds
    Temporary,
    /// No TTL, explicit close only — survives daemon restarts
    Persistent,
}

impl ChannelMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            ChannelMode::Temporary => "temporary",
            ChannelMode::Persistent => "persistent",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "temporary" | "temp" => Some(ChannelMode::Temporary),
            "persistent" | "perm" => Some(ChannelMode::Persistent),
            _ => None,
        }
    }
}

impl fmt::Display for ChannelMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Channel info returned from provisioning
#[derive(Debug, Clone)]
pub struct ChannelInfo {
    pub channel_id: String,
    pub stream_key: String,
    pub source_site: String,
    pub target_site: String,
    pub environment: String,
    pub mode: ChannelMode,
    pub created_at: u64,
    pub expires_at: Option<u64>,
    pub consumer_groups: Vec<String>,
}

/// Runtime configuration for direct channels
pub struct DirectConfig {
    /// Whether direct channels are enabled
    pub enabled: bool,
    /// Default TTL for temporary channels (seconds)
    pub default_ttl_seconds: u64,
    /// Maximum allowed TTL (seconds, cap at 24h)
    pub max_ttl_seconds: u64,
    /// Max idle time before staleness cleanup (seconds)
    pub max_idle_seconds: u64,
    /// Maximum channels per site (abuse prevention)
    pub max_channels_per_site: usize,
}

impl Default for DirectConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            default_ttl_seconds: 300,
            max_ttl_seconds: 86400,
            max_idle_seconds: 3600,
            max_channels_per_site: 50,
        }
    }
}
