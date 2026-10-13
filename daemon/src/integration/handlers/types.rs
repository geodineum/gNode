// Shared types, constants, and utilities for command handlers
//
// This module contains all types shared across handler modules:
// - CommandResult struct
// - Handler function type aliases
// - Utility functions for parameter parsing

use std::pin::Pin;
use std::future::Future;
use redis::Connection;
use redis::aio::MultiplexedConnection as AsyncConnection;
use serde::{Deserialize, Serialize};
use log::warn;
use serde_json::Value;
use crate::daemon::{Command, Response};
use crate::integration::processor::stream_utils::current_timestamp;

/// Indices whose value belongs to the sampler, not to the registering provider: the first derived axis above the hashed cut is where the sampler writes load.
pub const SERVICE_SAMPLER_AXES: [usize; 2] = [16, 17];

// ============================================================================
// Command Result Type
// ============================================================================

/// Result type returned by command handlers
#[derive(Debug, Clone)]
pub struct CommandResult {
    pub status: String,
    pub result: Option<Value>,
    pub error: Option<String>,
}

impl CommandResult {
    /// Create a success result with a value
    pub fn success(result: impl Into<Value>) -> Self {
        Self {
            status: "ok".to_string(),
            result: Some(result.into()),
            error: None,
        }
    }

    /// Create a success result with a JSON value
    pub fn success_json(json_str: String) -> Self {
        match serde_json::from_str(&json_str) {
            Ok(value) => Self {
                status: "ok".to_string(),
                result: Some(value),
                error: None,
            },
            Err(e) => {
                warn!("Failed to parse JSON result: {}", e);
                Self {
                    status: "ok".to_string(),
                    result: Some(Value::String(json_str)),
                    error: None,
                }
            }
        }
    }

    /// Create an error result with an error message
    pub fn error(error: impl Into<String>) -> Self {
        Self {
            status: "error".to_string(),
            result: None,
            error: Some(error.into()),
        }
    }

    /// Convert to a Response object
    pub fn to_response(&self, command_id: &str) -> Response {
        Response {
            id: command_id.to_string(),
            status: self.status.clone(),
            result: self.result.clone(),
            error: self.error.clone(),
            timestamp: current_timestamp(),
            batch_id: None,
            sequence: None,
        }
    }
}

// ============================================================================
// Handler Type Aliases
// ============================================================================

/// Type alias for synchronous command handler functions
pub type CommandHandlerFn = fn(&Command, &mut Connection, &str, bool) -> CommandResult;

/// Type alias for asynchronous command handler functions
/// Uses Pin<Box<dyn Future>> to allow async fn with references
pub type AsyncCommandHandlerFn = for<'a> fn(
    &'a Command,
    &'a mut AsyncConnection,
    &'a str,  // site_id
    bool,     // debug
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>>;

// ============================================================================
// Parameter Parsing
// ============================================================================

/// Parse parameters for a specific type
pub fn parse_parameters<T: for<'de> Deserialize<'de>>(command: &Command) -> Result<T, String> {
    match serde_json::from_value::<T>(command.parameters.clone()) {
        Ok(params) => Ok(params),
        Err(e) => Err(format!("Invalid parameters: {}", e)),
    }
}

// ============================================================================
// Utility Functions
// ============================================================================

/// Default group name for serde defaults
pub fn default_group() -> String {
    "default".to_string()
}

/// Asserts a descriptor's params name only fields its parser reads (all of them when
/// `exact`), its `required` list is among them, and its example parses. `P` derives
/// `Serialize + Default` under `cfg(test)` so its field names can be read.
#[cfg(test)]
pub fn assert_descriptor_fields<P>(descriptors: &[CommandDescriptor], command: &str, exact: bool)
where
    P: Serialize + Default + for<'de> Deserialize<'de>,
{
    let d = descriptors.iter().find(|d| d.name == command)
        .unwrap_or_else(|| panic!("no descriptor for {command}"));
    let keys = |v: &Value| -> Vec<String> {
        let mut k: Vec<String> = v.as_object().map(|o| o.keys().cloned().collect()).unwrap_or_default();
        k.sort();
        k
    };
    let declared = keys(&d.params_schema["properties"]);
    let parsed = keys(&serde_json::to_value(P::default()).unwrap());
    if exact {
        assert_eq!(declared, parsed, "{command}: descriptor properties vs parser fields");
    } else {
        let unknown: Vec<&String> = declared.iter().filter(|f| !parsed.contains(f)).collect();
        assert!(unknown.is_empty(), "{command}: descriptor names fields the parser never reads: {unknown:?}");
    }
    for required in d.params_schema["required"].as_array().into_iter().flatten() {
        let name = required.as_str().unwrap_or_default();
        assert!(declared.iter().any(|f| f == name), "{command}: required '{name}' is not a declared property");
    }
    let example: Value = serde_json::from_str(d.example)
        .unwrap_or_else(|e| panic!("{command}: example is not JSON: {e}"));
    assert_eq!(example["cmd"], command, "{command}: example names another command");
    for field in keys(&example["params"]) {
        assert!(declared.contains(&field), "{command}: example uses undeclared '{field}'");
    }
    serde_json::from_value::<P>(example["params"].clone())
        .unwrap_or_else(|e| panic!("{command}: example params do not parse: {e}"));
}

/// Get current memory usage in KB (rough estimate)
pub fn get_memory_usage_kb() -> u64 {
    // Simple estimation based on typical gNode usage
    4600 // ~4.6MB as observed in production
}

// ============================================================================
// Command Descriptor (Autodocumentation)
// ============================================================================

/// Execution lane for a command.
///
/// The daemon executes commands through one of two pipelines, declared
/// per-command rather than per-code-path:
///
/// - `Concurrent`     — async-spawned execution. The consumer-group reader
///                hands the command to a tokio task and immediately
///                reads the next batch. Many in-flight per consumer
///                thread, no ordering guarantee between requests.
///                Default for idempotent FCALL wrappers and read-only
///                operations (registerService, geometric_discover,
///                template_fragment, ping, etc.).
///
/// - `Ordered`  — synchronous in-thread execution. The handler runs
///                inline before the consumer reads the next message;
///                ordering preserved across the batch. For commands
///                with cross-key transactional semantics or commands
///                whose effects subsequent reads must observe (site
///                provisioning, deprovisioning, relay policy writes,
///                topo_delete, batch when caller flags ordered).
///
/// See `COMMAND_SCHEMA.md` § Lane Semantics for the full rationale +
/// per-command assignment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Lane {
    /// Async-spawned, unordered, high-throughput. Safe default.
    Concurrent,
    /// Synchronous inline, ordering preserved. Use only when caller
    /// semantics demand it.
    Ordered,
}

impl Default for Lane {
    fn default() -> Self {
        // Concurrent is the safe default — most commands are idempotent
        // FCALL wrappers or read-only and don't need ordering. Opt
        // into Ordered explicitly in the handler registration.
        Lane::Concurrent
    }
}

/// Schema descriptor for a command, enabling runtime API discovery.
///
/// Each handler module registers descriptors alongside its command handlers.
/// Clients can query these via the `describe` command to learn parameter
/// formats without external documentation.
#[derive(Debug, Clone, Serialize)]
pub struct CommandDescriptor {
    /// Canonical command name (lowercase, no aliases)
    pub name: &'static str,
    /// Handler category (e.g. "system", "geometric", "topology")
    pub category: &'static str,
    /// Human-readable description of what the command does
    pub description: &'static str,
    /// JSON Schema for command parameters
    pub params_schema: Value,
    /// JSON Schema for the result payload (inside status:"ok")
    pub returns_schema: Value,
    /// Example invocation as a JSON string
    pub example: &'static str,
    /// Whether the command has an async handler
    pub async_capable: bool,
    /// Execution lane (Concurrent = async-spawned, Ordered = synchronous inline).
    /// Defaults to Concurrent — opt into Ordered for commands with
    /// cross-request ordering semantics. See `Lane` doc above.
    #[serde(default)]
    pub lane: Lane,
}

impl CommandDescriptor {
    /// Convert to a JSON value for API responses
    pub fn to_json(&self) -> Value {
        serde_json::to_value(self).unwrap_or_default()
    }
}
