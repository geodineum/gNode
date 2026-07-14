//! Relay format translation module
//!
//! Uses the base-tier native FormatProcessor to translate commands between
//! formats in the relay path. When Service A speaks JSON and Service B expects
//! compact_json (or vice versa), the translator converts transparently.
//!
//! Format detection and conversion are a BASE capability (native Rust engine),
//! so the relay no longer depends on the premium gNode-BROKER Lua functions.
//! If the processor is not yet initialized, translation degrades to a no-op.
//!
//! Translation only occurs when source and target formats differ. If both
//! services use the same format (common case), this is a no-op.

use log::{debug, info, warn};
use redis::Connection;
use serde_json::Value;

/// Result of a format translation attempt
#[derive(Debug)]
pub enum TranslationResult {
    /// Translation performed — params were converted
    Translated {
        params_json: String,
        source_format: String,
        target_format: String,
    },
    /// No translation needed — formats match or detection unavailable
    NoOp,
    /// Translation failed — return original params, log warning
    Failed(String),
}

/// Detect the format of a message payload via the native FormatProcessor.
///
/// Returns (format_name, confidence) or None if detection fails or the
/// processor is not yet initialized.
pub fn detect_format(
    _conn: &mut Connection,
    message_json: &str,
    debug_mode: bool,
) -> Option<(String, f64)> {
    let processor = match crate::daemon::GNodeDaemon::get_format_processor_ref() {
        Some(p) => p,
        None => {
            if debug_mode {
                debug!("Format processor not initialized; skipping relay detection");
            }
            return None;
        }
    };

    match processor.detect(message_json.as_bytes()) {
        Ok(Some((format_name, _version, confidence))) => {
            if debug_mode {
                debug!("Detected format: {} (confidence: {:.2})", format_name, confidence);
            }
            Some((format_name, confidence))
        }
        Ok(None) => {
            if debug_mode {
                debug!("Relay format detection found no matching format");
            }
            None
        }
        Err(e) => {
            if debug_mode {
                debug!("Relay format detection failed: {}", e);
            }
            None
        }
    }
}

/// Convert a message between formats via the native FormatProcessor.
///
/// # Arguments
/// * `conn` — ValKey connection (unused; kept for call-site stability)
/// * `source_format` — Source format name (e.g., "standard_json")
/// * `source_version` — Source format version (e.g., "1.0.0")
/// * `target_format` — Target format name (e.g., "compact_json")
/// * `target_version` — Target format version (e.g., "1.0.0")
/// * `message_json` — The message payload as JSON string
/// * `debug_mode` — Verbose logging
pub fn convert_format(
    _conn: &mut Connection,
    source_format: &str,
    source_version: &str,
    target_format: &str,
    target_version: &str,
    message_json: &str,
    debug_mode: bool,
) -> TranslationResult {
    if source_format == target_format {
        return TranslationResult::NoOp;
    }

    if debug_mode {
        info!(
            "Relay format translation: {} v{} -> {} v{}",
            source_format, source_version, target_format, target_version
        );
    }

    let processor = match crate::daemon::GNodeDaemon::get_format_processor_ref() {
        Some(p) => p,
        None => {
            if debug_mode {
                debug!("Format processor not initialized; skipping relay translation");
            }
            return TranslationResult::NoOp;
        }
    };

    match processor.convert(
        message_json.as_bytes(),
        source_format,
        Some(source_version),
        target_format,
        Some(target_version),
    ) {
        Ok(bytes) => match String::from_utf8(bytes) {
            Ok(params_json) => TranslationResult::Translated {
                params_json,
                source_format: source_format.to_string(),
                target_format: target_format.to_string(),
            },
            Err(e) => {
                warn!(
                    "Relay conversion produced non-UTF8 output ({} -> {}): {}",
                    source_format, target_format, e
                );
                TranslationResult::Failed(e.to_string())
            }
        },
        Err(e) => {
            warn!(
                "Format conversion failed ({} -> {}): {}",
                source_format, target_format, e
            );
            TranslationResult::Failed(e.to_string())
        }
    }
}

/// Translate a relay command's parameters between source and target formats.
///
/// This is the main entry point called from consumer_groups.rs relay processing.
/// It attempts to detect the source format and look up the target entity's preferred
/// format from topology metadata, then converts if they differ.
///
/// # Arguments
/// * `conn` — ValKey connection
/// * `params_json` — The command parameters as a JSON string
/// * `source_site` — Source site ID (for format preference lookup)
/// * `target_entity_id` — Target entity ID (for format preference lookup)
/// * `topology_key` — Topology key for entity metadata lookup
/// * `debug_mode` — Verbose logging
///
/// # Returns
/// `TranslationResult` — Translated params, NoOp, or Failed
pub fn translate_for_relay(
    conn: &mut Connection,
    params_json: &str,
    _source_site: &str,
    target_entity_id: &str,
    topology_key: &str,
    debug_mode: bool,
) -> TranslationResult {
    // Step 1: Detect source format
    let source_format = match detect_format(conn, params_json, debug_mode) {
        Some((fmt, confidence)) if confidence >= 0.5 => fmt,
        _ => return TranslationResult::NoOp, // Can't detect → no translation
    };

    // Step 2: Look up target entity's preferred format from topology metadata
    let target_format = match get_entity_format_preference(conn, topology_key, target_entity_id, debug_mode) {
        Some(fmt) => fmt,
        None => return TranslationResult::NoOp, // No preference declared → no translation
    };

    // Step 3: If formats match, no translation needed
    if source_format == target_format {
        if debug_mode {
            debug!("Source and target both use '{}', no translation needed", source_format);
        }
        return TranslationResult::NoOp;
    }

    // Step 4: Convert
    convert_format(
        conn,
        &source_format,
        "1.0.0",
        &target_format,
        "1.0.0",
        params_json,
        debug_mode,
    )
}

/// Look up a target entity's preferred format from its topology metadata.
///
/// The entity's metadata (`m` field in topology HASH) may contain:
/// - `native_format`: the format name this service prefers
///
/// Returns the format name if found, None otherwise.
fn get_entity_format_preference(
    conn: &mut Connection,
    topology_key: &str,
    entity_id: &str,
    debug_mode: bool,
) -> Option<String> {
    if entity_id.is_empty() {
        return None;
    }

    let entities_key = format!("{}:entities", topology_key);
    let result: redis::RedisResult<Option<String>> = redis::cmd("HGET")
        .arg(&entities_key)
        .arg(entity_id)
        .query(conn);

    match result {
        Ok(Some(json_str)) => {
            match serde_json::from_str::<Value>(&json_str) {
                Ok(entity) => {
                    // Check metadata.native_format
                    let format = entity
                        .get("m")
                        .and_then(|m| m.get("native_format"))
                        .and_then(|f| f.as_str())
                        .map(|s| s.to_string());

                    if debug_mode {
                        if let Some(ref fmt) = format {
                            debug!("Entity '{}' prefers format: {}", entity_id, fmt);
                        }
                    }
                    format
                }
                Err(_) => None,
            }
        }
        Ok(None) => None,
        Err(e) => {
            if debug_mode {
                debug!("Failed to lookup entity format preference for '{}': {}", entity_id, e);
            }
            None
        }
    }
}
