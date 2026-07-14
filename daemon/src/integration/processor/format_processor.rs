// Format Processor Module for gNode
//
// The daemon-facing facade over the user-defined format engine: detection,
// conversion, registration, listing, and ValKey persistence/hydration of
// custom format definitions. Consumed by the format commands, the relay
// translator, and daemon startup.

use std::sync::Arc;
use serde_json::{Value, Map};
use log::{debug, warn};
use redis::Connection;

use crate::template::format::{
    FormatRegistry, FormatTransformer, TransformerError, RegistryError
};

/// ValKey key for the custom-format index SET, hash-tagged on the topology
/// namespace so it shares a cluster slot with every definition key.
fn format_index_key(namespace: &str) -> String {
    format!("{{{}}}:gnode:format:_index", namespace)
}

/// ValKey key for a single format definition STRING. `member` is the
/// `<name>:<version>` index member, appended verbatim so the index and the
/// definition keys stay in lockstep.
fn format_def_key(namespace: &str, member: &str) -> String {
    format!("{{{}}}:gnode:format:{}", namespace, member)
}

/// Format processor error type
#[derive(Debug)]
pub enum FormatProcessorError {
    /// Transformer error
    Transformer(TransformerError),
    /// Registry error
    Registry(RegistryError),
    /// Command processing error
    CommandProcessing(String),
    /// Redis error
    Redis(redis::RedisError),
    /// JSON error
    Json(serde_json::Error),
}

impl std::fmt::Display for FormatProcessorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FormatProcessorError::Transformer(e) => write!(f, "Transformer error: {}", e),
            FormatProcessorError::Registry(e) => write!(f, "Registry error: {}", e),
            FormatProcessorError::CommandProcessing(e) => write!(f, "Command processing error: {}", e),
            FormatProcessorError::Redis(e) => write!(f, "Redis error: {}", e),
            FormatProcessorError::Json(e) => write!(f, "JSON error: {}", e),
        }
    }
}

impl From<TransformerError> for FormatProcessorError {
    fn from(err: TransformerError) -> Self {
        FormatProcessorError::Transformer(err)
    }
}

impl From<RegistryError> for FormatProcessorError {
    fn from(err: RegistryError) -> Self {
        FormatProcessorError::Registry(err)
    }
}

impl From<redis::RedisError> for FormatProcessorError {
    fn from(err: redis::RedisError) -> Self {
        FormatProcessorError::Redis(err)
    }
}

impl From<serde_json::Error> for FormatProcessorError {
    fn from(err: serde_json::Error) -> Self {
        FormatProcessorError::Json(err)
    }
}

/// Format processor
pub struct FormatProcessor {
    /// Format registry
    registry: Arc<FormatRegistry>,
    /// Bidirectional format converter (JSON v1 <-> v2 <-> compact); drives `convert`
    transformer: FormatTransformer,
}

impl FormatProcessor {
    /// Create a new format processor
    pub fn new(registry: Arc<FormatRegistry>) -> Self {
        let transformer = FormatTransformer::new(Arc::clone(&registry));

        Self {
            registry,
            transformer,
        }
    }

    /// Get a reference to the format registry for dynamic format registration
    pub fn get_registry(&self) -> &Arc<FormatRegistry> {
        &self.registry
    }

    /// Detect the wire format of a raw message — native equivalent of the
    /// former `FCALL GNODE_DETECT_FORMAT`. Returns the best-scoring default
    /// format as (name, version, confidence), or None if undetectable.
    pub fn detect(&self, data: &[u8]) -> Result<Option<(String, String, f64)>, FormatProcessorError> {
        self.registry.detect_format(data).map_err(FormatProcessorError::Registry)
    }

    /// Convert a message between two known formats via the canonical internal
    /// form — native equivalent of `FCALL GNODE_CONVERT_FORMAT`.
    pub fn convert(
        &self,
        source_data: &[u8],
        source_format: &str,
        source_version: Option<&str>,
        target_format: &str,
        target_version: Option<&str>,
    ) -> Result<Vec<u8>, FormatProcessorError> {
        self.transformer
            .transform_from_to(source_data, source_format, source_version, target_format, target_version)
            .map_err(FormatProcessorError::Transformer)
    }

    /// Register a custom format from a definition value — native equivalent of
    /// `FCALL GNODE_REGISTER_FORMAT`. Returns the registered format name.
    pub fn register(&self, definition: &Value) -> Result<String, FormatProcessorError> {
        self.registry.register_format(definition).map_err(FormatProcessorError::Registry)?;
        let name = definition
            .get("format_name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        Ok(name)
    }

    /// Persist a custom format definition to ValKey so it survives daemon
    /// restarts and is restored by `hydrate_formats`. Replaces the former
    /// `FCALL GNODE_PERSIST_FORMATS` Lua path. Keys are hash-tagged on the
    /// topology namespace so a single tag owns both the index and every
    /// definition (one cluster slot, multi-tenant safe). Built-in formats
    /// self-register at startup and must NOT be persisted here.
    pub fn persist_format(
        &self,
        connection: &mut Connection,
        namespace: &str,
        definition: &Value,
    ) -> Result<(), FormatProcessorError> {
        let name = definition
            .get("format_name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| FormatProcessorError::CommandProcessing(
                "format definition missing format_name".to_string(),
            ))?;
        let version = definition
            .get("version")
            .and_then(|v| v.as_str())
            .unwrap_or("1.0.0");

        let member = format!("{}:{}", name, version);
        let def_key = format_def_key(namespace, &member);
        let index_key = format_index_key(namespace);
        let payload = serde_json::to_string(definition)?;

        redis::cmd("SET").arg(&def_key).arg(&payload).query::<()>(connection)?;
        redis::cmd("SADD").arg(&index_key).arg(&member).query::<()>(connection)?;

        debug!("Persisted format {} to ValKey ({})", member, def_key);
        Ok(())
    }

    /// Async counterpart of `persist_format`, for the fast-lane async command
    /// handlers (which hold a multiplexed async connection). Same key scheme.
    pub async fn persist_format_async(
        &self,
        connection: &mut redis::aio::MultiplexedConnection,
        namespace: &str,
        definition: &Value,
    ) -> Result<(), FormatProcessorError> {
        let name = definition
            .get("format_name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| FormatProcessorError::CommandProcessing(
                "format definition missing format_name".to_string(),
            ))?;
        let version = definition
            .get("version")
            .and_then(|v| v.as_str())
            .unwrap_or("1.0.0");

        let member = format!("{}:{}", name, version);
        let def_key = format_def_key(namespace, &member);
        let index_key = format_index_key(namespace);
        let payload = serde_json::to_string(definition)?;

        let _: () = redis::cmd("SET").arg(&def_key).arg(&payload).query_async(connection).await?;
        let _: () = redis::cmd("SADD").arg(&index_key).arg(&member).query_async(connection).await?;

        debug!("Persisted format {} to ValKey async ({})", member, def_key);
        Ok(())
    }

    /// Restore custom format definitions from ValKey into the in-memory
    /// registry. Replaces the former `FCALL GNODE_LOAD_FORMATS` Lua path.
    /// Built-ins are already self-registered by the time this runs, so only
    /// custom formats are read back. Returns the count restored.
    pub fn hydrate_formats(
        &self,
        connection: &mut Connection,
        namespace: &str,
    ) -> Result<usize, FormatProcessorError> {
        let index_key = format_index_key(namespace);
        let members: Vec<String> = redis::cmd("SMEMBERS").arg(&index_key).query(connection)?;

        let mut restored = 0usize;
        for member in members {
            let def_key = format_def_key(namespace, &member);
            let payload: Option<String> = redis::cmd("GET").arg(&def_key).query(connection)?;
            let payload = match payload {
                Some(p) => p,
                None => {
                    warn!("Format index references missing definition {}; skipping", def_key);
                    continue;
                }
            };
            match serde_json::from_str::<Value>(&payload) {
                Ok(value) => match self.registry.register_format(&value) {
                    Ok(()) => restored += 1,
                    Err(e) => warn!("Failed to restore format {} from ValKey: {:?}", member, e),
                },
                Err(e) => warn!("Failed to parse persisted format {}: {}", member, e),
            }
        }
        Ok(restored)
    }

    /// List available formats
    pub fn list_formats(&self) -> Result<Value, FormatProcessorError> {
        // Get all metadata
        let metadata = self.registry.get_all_metadata()?;

        // Convert to value
        let mut formats = Vec::new();

        for meta in metadata {
            let mut format = Map::new();
            format.insert("name".to_string(), Value::String(meta.name.clone()));
            format.insert("version".to_string(), Value::String(meta.version.clone()));
            format.insert("description".to_string(), Value::String(meta.description));
            format.insert("content_type".to_string(), Value::String(meta.content_type));
            format.insert("binary".to_string(), Value::Bool(meta.binary));

            // Get the full format definition to include the schema
            if let Ok(format_schema) = self.registry.get_format(&meta.name, Some(&meta.version)) {
                let definition = format_schema.get_definition();

                // Add schema if present
                if let Some(schema) = &definition.schema {
                    format.insert("schema".to_string(), schema.clone());
                }

                // Add detection patterns
                if !definition.detection_patterns.is_empty() {
                    let patterns: Vec<Value> = definition.detection_patterns.iter().map(|p| {
                        let mut pattern_map = Map::new();
                        pattern_map.insert("pattern_type".to_string(), Value::String(p.pattern_type.clone()));
                        pattern_map.insert("pattern".to_string(), Value::String(p.pattern.clone()));
                        pattern_map.insert("confidence".to_string(),
                            Value::Number(serde_json::Number::from_f64(p.confidence).unwrap_or_else(|| serde_json::Number::from(0))));
                        Value::Object(pattern_map)
                    }).collect();
                    format.insert("detection_patterns".to_string(), Value::Array(patterns));
                }
            }

            formats.push(Value::Object(format));
        }

        Ok(Value::Array(formats))
    }
}