//! Direct channel provisioner — Rust-side wrappers for GNODE_DIRECT_* Lua FCALL operations.
//!
//! Each function constructs a FCALL command, executes it, and parses the JSON result.

use log::{info, warn, debug};
use redis::Connection;
use serde_json;

use crate::integration::error_handlings::{IntegrationResult, stream_processing_error};

/// Construct the base key for direct channel operations.
/// Format: `{topology_namespace}:gnode:direct`
fn direct_base_key(topology_namespace: &str) -> String {
    format!("{{{}}}:gnode:direct", topology_namespace)
}

/// Generate a channel ID: `ch_{8-hex}`
/// Uses timestamp XOR with a simple hash for uniqueness.
fn generate_channel_id() -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    // Mix timestamp with thread ID for cross-thread uniqueness
    let thread_hash = std::thread::current().id();
    let mixed = now ^ (format!("{:?}", thread_hash).len() as u128 * 0x517cc1b727220a95);
    format!("ch_{:08x}", (mixed & 0xFFFFFFFF) as u32)
}

/// Provision a new direct channel between two sites.
///
/// Returns the channel info including stream key and consumer groups.
#[allow(clippy::too_many_arguments)]
pub fn provision_channel(
    conn: &mut Connection,
    topology_namespace: &str,
    source_site: &str,
    target_site: &str,
    mode: &str,
    ttl_seconds: u64,
    metadata: &serde_json::Value,
    environment: &str,
    debug_mode: bool,
) -> IntegrationResult<serde_json::Value> {
    let base_key = direct_base_key(topology_namespace);
    let channel_id = generate_channel_id();
    let metadata_json = serde_json::to_string(metadata).unwrap_or_else(|_| "{}".to_string());

    if debug_mode {
        info!("Provisioning direct channel {} ({}) between {} and {} [env: {}]",
            channel_id, mode, source_site, target_site, environment);
    }

    let result: redis::RedisResult<String> = redis::cmd("FCALL")
        .arg("GNODE_DIRECT_PROVISION")
        .arg(1)
        .arg(&base_key)
        .arg(&channel_id)
        .arg(source_site)
        .arg(target_site)
        .arg(mode)
        .arg(ttl_seconds)
        .arg(&metadata_json)
        .arg(environment)
        .query(conn);

    match result {
        Ok(json_str) => {
            match serde_json::from_str::<serde_json::Value>(&json_str) {
                Ok(val) => {
                    if val.get("ok").and_then(|v| v.as_bool()) == Some(true) {
                        info!("Direct channel provisioned: {} ({}) {} <-> {}",
                            channel_id, mode, source_site, target_site);
                        Ok(val)
                    } else {
                        let err = val.get("error")
                            .and_then(|v| v.as_str())
                            .unwrap_or("Unknown provisioning error");
                        Err(stream_processing_error(format!("Channel provision failed: {}", err)))
                    }
                },
                Err(e) => Err(stream_processing_error(format!("Failed to parse provision response: {}", e))),
            }
        },
        Err(e) => Err(stream_processing_error(format!("GNODE_DIRECT_PROVISION FCALL failed: {}", e))),
    }
}

/// Close a direct channel (delete stream + metadata + index entry).
pub fn close_channel(
    conn: &mut Connection,
    topology_namespace: &str,
    channel_id: &str,
    debug_mode: bool,
) -> IntegrationResult<serde_json::Value> {
    let base_key = direct_base_key(topology_namespace);

    if debug_mode {
        debug!("Closing direct channel: {}", channel_id);
    }

    let result: redis::RedisResult<String> = redis::cmd("FCALL")
        .arg("GNODE_DIRECT_CLOSE")
        .arg(1)
        .arg(&base_key)
        .arg(channel_id)
        .query(conn);

    match result {
        Ok(json_str) => {
            match serde_json::from_str::<serde_json::Value>(&json_str) {
                Ok(val) => {
                    if val.get("ok").and_then(|v| v.as_bool()) == Some(true) {
                        info!("Direct channel closed: {}", channel_id);
                        Ok(val)
                    } else {
                        let err = val.get("error")
                            .and_then(|v| v.as_str())
                            .unwrap_or("Unknown close error");
                        Err(stream_processing_error(format!("Channel close failed: {}", err)))
                    }
                },
                Err(e) => Err(stream_processing_error(format!("Failed to parse close response: {}", e))),
            }
        },
        Err(e) => Err(stream_processing_error(format!("GNODE_DIRECT_CLOSE FCALL failed: {}", e))),
    }
}

/// Get channel info (metadata + stream stats).
pub fn get_channel_info(
    conn: &mut Connection,
    topology_namespace: &str,
    channel_id: &str,
    debug_mode: bool,
) -> IntegrationResult<serde_json::Value> {
    let base_key = direct_base_key(topology_namespace);

    if debug_mode {
        debug!("Getting direct channel info: {}", channel_id);
    }

    let result: redis::RedisResult<String> = redis::cmd("FCALL")
        .arg("GNODE_DIRECT_INFO")
        .arg(1)
        .arg(&base_key)
        .arg(channel_id)
        .query(conn);

    match result {
        Ok(json_str) => {
            serde_json::from_str::<serde_json::Value>(&json_str)
                .map_err(|e| stream_processing_error(format!("Failed to parse info response: {}", e)))
        },
        Err(e) => Err(stream_processing_error(format!("GNODE_DIRECT_INFO FCALL failed: {}", e))),
    }
}

/// List channels, optionally filtered by participant site and/or environment.
pub fn list_channels(
    conn: &mut Connection,
    topology_namespace: &str,
    site_filter: Option<&str>,
    env_filter: Option<&str>,
    debug_mode: bool,
) -> IntegrationResult<serde_json::Value> {
    let base_key = direct_base_key(topology_namespace);

    if debug_mode {
        debug!("Listing direct channels (site: {:?}, env: {:?})", site_filter, env_filter);
    }

    let mut cmd = redis::cmd("FCALL");
    cmd.arg("GNODE_DIRECT_LIST")
        .arg(1)
        .arg(&base_key);

    // Always pass both args (empty string = no filter) for positional ordering
    cmd.arg(site_filter.unwrap_or(""));
    cmd.arg(env_filter.unwrap_or(""));

    let result: redis::RedisResult<String> = cmd.query(conn);

    match result {
        Ok(json_str) => {
            serde_json::from_str::<serde_json::Value>(&json_str)
                .map_err(|e| stream_processing_error(format!("Failed to parse list response: {}", e)))
        },
        Err(e) => Err(stream_processing_error(format!("GNODE_DIRECT_LIST FCALL failed: {}", e))),
    }
}

/// Check for expired/idle channels and auto-close them.
/// Called periodically from the staleness check in consumer_groups.rs.
pub fn check_expiry(
    conn: &mut Connection,
    topology_namespace: &str,
    now_seconds: u64,
    max_idle_seconds: u64,
    debug_mode: bool,
) -> IntegrationResult<serde_json::Value> {
    let base_key = direct_base_key(topology_namespace);

    let result: redis::RedisResult<String> = redis::cmd("FCALL")
        .arg("GNODE_DIRECT_CHECK_EXPIRY")
        .arg(1)
        .arg(&base_key)
        .arg(now_seconds)
        .arg(max_idle_seconds)
        .query(conn);

    match result {
        Ok(json_str) => {
            match serde_json::from_str::<serde_json::Value>(&json_str) {
                Ok(val) => {
                    let expired = val.get("expired").and_then(|v| v.as_i64()).unwrap_or(0);
                    let idle_closed = val.get("idle_closed").and_then(|v| v.as_i64()).unwrap_or(0);
                    if expired > 0 || idle_closed > 0 {
                        warn!("Direct channel expiry check: {} expired, {} idle-closed", expired, idle_closed);
                    } else if debug_mode {
                        let checked = val.get("checked").and_then(|v| v.as_i64()).unwrap_or(0);
                        if checked > 0 {
                            debug!("Direct channel expiry check: {} checked, all healthy", checked);
                        }
                    }
                    Ok(val)
                },
                Err(e) => Err(stream_processing_error(format!("Failed to parse expiry response: {}", e))),
            }
        },
        Err(e) => {
            if debug_mode {
                debug!("Direct channel expiry check skipped: {}", e);
            }
            Err(stream_processing_error(format!("GNODE_DIRECT_CHECK_EXPIRY FCALL failed: {}", e)))
        },
    }
}
