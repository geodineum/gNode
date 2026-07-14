// Compute Request Handler
//
// Implements request/response processing for gNode daemon.
// Uses XREADGROUP on per-site/environment streams for high-throughput
// multi-tenant request processing with consumer group load balancing.
//
// Stream pattern: {site_id}:gnode:unified:{environment}
// Dynamic site discovery via GNODE_SERVICE_GET_DAEMON_STREAMS
// Automatic subscription to newly registered sites
//
// Flow:
// 1. Discover registered sites and their streams
// 2. XREADGROUP from all site/environment unified streams
// 3. Parse site_id and request_id from stream message
// 4. Execute command using existing command handlers
// 5. XADD response back to the same unified stream OR SET response key

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::collections::HashSet;
use redis::{Client, AsyncCommands, Value as RedisValue};
use redis::streams::{StreamReadReply, StreamKey, StreamId};
use serde::{Serialize, Deserialize};
use serde_json::{Value, json};
use log::{info, error, debug, warn};
use crate::utils::field_names;
use tokio::task::JoinHandle;
use tokio::sync::RwLock as TokioRwLock;

use crate::daemon::Command;
use crate::integration::command_handler::{CommandHandlerRegistry, CommandResult};
use crate::integration::stream_discovery::StreamDiscoveryManager;
use crate::config::{build_request_key, build_response_key};
use crate::GeometricTopology;
use std::sync::RwLock;

/// Request received from KeyBasedClient (PHP)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComputeRequest {
    pub command: String,
    pub parameters: Value,
    pub requested_at: f64,
    pub timeout_ms: u64,
    pub site_id: String,
}

/// Response sent back to KeyBasedClient (PHP)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComputeResponse {
    pub status: String,
    pub result: Option<Value>,
    pub error: Option<String>,
    pub error_code: Option<String>,
    pub computed_at: f64,
    pub compute_time_ms: u64,
    pub request_id: String,
}

impl ComputeResponse {
    /// Create success response
    pub fn success(request_id: String, result: Value, compute_time_ms: u64) -> Self {
        Self {
            status: "ok".to_string(),
            result: Some(result),
            error: None,
            error_code: None,
            computed_at: current_timestamp(),
            compute_time_ms,
            request_id,
        }
    }

    /// Create error response
    pub fn error(request_id: String, error: String, error_code: Option<String>) -> Self {
        Self {
            status: "error".to_string(),
            result: None,
            error: Some(error),
            error_code,
            computed_at: current_timestamp(),
            compute_time_ms: 0,
            request_id,
        }
    }
}

/// Compute handler context (shared state)
pub struct ComputeHandler {
    redis_client: Client,
    /// Node ID for consumer naming
    node_id: String,
    command_registry: Arc<CommandHandlerRegistry>,
    topology: Arc<RwLock<GeometricTopology>>,
    /// SHARED stream discovery manager from daemon - single source of truth
    /// Uses std::sync::RwLock for compatibility with daemon's synchronous discovery
    shared_discovery: Arc<RwLock<StreamDiscoveryManager>>,
    /// Currently subscribed stream keys (local cache, refreshed from shared_discovery)
    active_streams: Arc<TokioRwLock<HashSet<String>>>,
    debug: bool,
    /// Track if we've logged the "waiting for discovery" message to avoid spam
    logged_waiting: Arc<TokioRwLock<bool>>,
}

impl ComputeHandler {
    /// Create new compute handler with SHARED stream discovery from daemon
    ///
    /// IMPORTANT: This uses the daemon's StreamDiscoveryManager as single source of truth.
    /// The daemon discovers ALL sites and ALL environments, and this handler subscribes
    /// to the unified streams discovered by the daemon.
    ///
    /// # Arguments
    /// * `redis_client` - ValKey client for connections
    /// * `node_id` - Unique node identifier for consumer naming
    /// * `topology` - Shared topology reference
    /// * `shared_discovery` - SHARED StreamDiscoveryManager from daemon (single source of truth)
    /// * `debug` - Enable debug logging
    pub fn new(
        redis_client: Client,
        node_id: String,
        topology: Arc<RwLock<GeometricTopology>>,
        shared_discovery: Arc<RwLock<StreamDiscoveryManager>>,
        debug: bool,
    ) -> Self {
        Self {
            redis_client,
            node_id,
            command_registry: Arc::new(CommandHandlerRegistry::new()),
            topology,
            shared_discovery,
            active_streams: Arc::new(TokioRwLock::new(HashSet::new())),
            debug,
            logged_waiting: Arc::new(TokioRwLock::new(false)),
        }
    }

    /// Consumer group name
    const CONSUMER_GROUP: &'static str = "gnode-daemon";

    /// Start compute request listener (spawns async task)
    ///
    /// Uses the SHARED StreamDiscoveryManager from daemon to discover ALL streams
    /// across ALL registered sites and ALL DTAP environments.
    pub async fn start_listener(self) -> Result<JoinHandle<()>, Box<dyn std::error::Error + Send + Sync>> {
        let handler = Arc::new(self);

        // Get environment from shared discovery for logging
        let env_info = {
            let discovery = handler.shared_discovery.read()
                .map_err(|e| format!("Failed to read shared discovery: {}", e))?;
            discovery.environment().to_string()
        };

        info!("🔑 KeyBased: Starting compute listener (environment: '{}', multi-tenant: true)", env_info);

        // Initial stream refresh from shared discovery
        handler.refresh_streams_from_shared().await?;

        // Ensure consumer groups exist on discovered streams
        handler.ensure_consumer_groups().await?;

        // Spawn async task for stream listener
        let task = tokio::spawn(async move {
            loop {
                if let Err(e) = handler.run_listener_loop().await {
                    error!("❌ KeyBased: Compute listener failed: {}, restarting in 5s...", e);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        });

        Ok(task)
    }

    /// Refresh active streams from the SHARED StreamDiscoveryManager
    ///
    /// This reads the already-discovered streams from the daemon's shared discovery manager.
    /// The daemon is responsible for refreshing the discovery; this just syncs the local cache.
    async fn refresh_streams_from_shared(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Read from shared discovery (sync RwLock - use spawn_blocking if needed for long ops)
        let unified_streams: Vec<String> = {
            let discovery = self.shared_discovery.read()
                .map_err(|e| format!("Failed to read shared discovery: {}", e))?;

            // Get only unified streams (we don't process health/broadcast here)
            discovery.get_unified_streams()
                .into_iter()
                .map(|s| s.key)
                .collect()
        };

        // Update local active streams cache
        {
            let mut active = self.active_streams.write().await;
            active.clear();
            for stream in &unified_streams {
                active.insert(stream.clone());
            }
        }

        if unified_streams.is_empty() {
            // Only log once when transitioning to waiting state
            let mut logged = self.logged_waiting.write().await;
            if !*logged {
                info!("⏳ KeyBased: Waiting for daemon stream discovery...");
                *logged = true;
            }
        } else {
            // Log when we transition from waiting to discovered
            let mut logged = self.logged_waiting.write().await;
            if *logged {
                debug!("🔍 KeyBased: Synced {} unified streams from shared discovery", unified_streams.len());
                *logged = false;
            }
            if self.debug {
                for stream in &unified_streams {
                    debug!("  → {}", stream);
                }
            }
        }

        Ok(())
    }

    /// Ensure consumer groups exist on all discovered streams
    async fn ensure_consumer_groups(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut conn = self.redis_client.get_multiplexed_async_connection().await?;
        let active = self.active_streams.read().await;

        if active.is_empty() {
            // Silent return - waiting message already logged by refresh_streams
            return Ok(());
        }

        let mut created = 0;
        let mut existing = 0;

        for stream in active.iter() {
            // Try to create the consumer group (MKSTREAM creates stream if not exists)
            let result: redis::RedisResult<()> = redis::cmd("XGROUP")
                .arg("CREATE")
                .arg(stream)
                .arg(Self::CONSUMER_GROUP)
                .arg("$")  // Start from new messages only
                .arg("MKSTREAM")
                .query_async(&mut conn)
                .await;

            match result {
                Ok(_) => {
                    created += 1;
                    debug!("✅ KeyBased: Created consumer group '{}' on '{}'", Self::CONSUMER_GROUP, stream);
                }
                Err(e) => {
                    // BUSYGROUP means group already exists - that's fine
                    if e.to_string().contains("BUSYGROUP") {
                        existing += 1;
                    } else {
                        warn!("⚠️ KeyBased: Failed to create consumer group on '{}': {}", stream, e);
                    }
                }
            }
        }

        debug!("✅ KeyBased: Consumer groups ready on {} streams (created: {}, existing: {})",
            active.len(), created, existing);
        Ok(())
    }

    /// Main listener loop using XREADGROUP (internal)
    ///
    /// Listens to ALL discovered unified streams from the shared discovery manager.
    /// Periodically syncs with the daemon's discovery to pick up newly registered sites.
    async fn run_listener_loop(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut conn = self.redis_client.get_multiplexed_async_connection().await?;
        let consumer_name = format!("daemon-{}", self.node_id);
        let mut refresh_counter = 0u32;

        info!("✅ KeyBased: Starting listener loop as consumer '{}'", consumer_name);

        loop {
            // Periodically sync with shared discovery (every ~60 iterations = ~60 seconds with 1s block)
            // The daemon handles the actual discovery refresh; we just sync our local cache
            refresh_counter += 1;
            if refresh_counter >= 60 {
                refresh_counter = 0;
                if let Err(e) = self.refresh_streams_from_shared().await {
                    debug!("KeyBased: Periodic stream sync failed (will retry): {}", e);
                }
                // Ensure consumer groups on any newly discovered streams
                if let Err(e) = self.ensure_consumer_groups().await {
                    debug!("KeyBased: Failed to ensure consumer groups (will retry): {}", e);
                }
            }

            // Get current active streams from local cache
            let active_streams: Vec<String> = {
                let active = self.active_streams.read().await;
                active.iter().cloned().collect()
            };

            if active_streams.is_empty() {
                // Message already logged by refresh_streams_from_shared - just wait
                tokio::time::sleep(Duration::from_secs(5)).await;
                // Try to sync with shared discovery
                if let Err(e) = self.refresh_streams_from_shared().await {
                    debug!("KeyBased: Stream sync failed (retrying): {}", e);
                }
                continue;
            }

            // Build XREADGROUP command dynamically
            let mut cmd = redis::cmd("XREADGROUP");
            cmd.arg("GROUP")
                .arg(Self::CONSUMER_GROUP)
                .arg(&consumer_name)
                .arg("BLOCK")
                .arg(1000)  // 1 second timeout
                .arg("COUNT")
                .arg(100)  // Max 100 messages per poll
                .arg("STREAMS");

            // Add all stream keys
            for stream in &active_streams {
                cmd.arg(stream);
            }

            // Add ">" for each stream (read new messages only)
            for _ in &active_streams {
                cmd.arg(">");
            }

            let result: redis::RedisResult<StreamReadReply> = cmd.query_async(&mut conn).await;

            match result {
                Ok(reply) => {
                    for StreamKey { key: stream_name, ids } in reply.keys {
                        for StreamId { id: message_id, map: fields } in ids {
                            // Field-name resolution goes through the central
                            // canonical alias lists in utils::field_names. The
                            // local helper takes &[&str] (same type as the
                            // constants), so we can pass them directly — no
                            // duplicate-list-with-sync-note dance.
                            let site_id = Self::get_field_with_fallbacks(&fields, field_names::SOURCE_SITE)
                                .filter(|s| !s.is_empty())
                                .unwrap_or_else(|| Self::extract_site_from_stream(&stream_name));

                            let request_id = Self::get_field_with_fallbacks(&fields, field_names::ID);
                            let msg_type   = Self::get_field_with_fallbacks(&fields, field_names::TYPE);

                            // The unified streams carry ALL wire traffic —
                            // commands, responses, batches (t = c|r|bc|br|b|i).
                            // KeyBased must only act on actual KeyBased
                            // announcements: the legacy "batch" envelope
                            // (request_ids array) and id-only notifications
                            // WITHOUT an inline command body. Everything else
                            // belongs to the unified command_processor.
                            // Pre-filter rationale: without it the daemon
                            // consumed its OWN batch responses (t=br) here,
                            // GET'd {site}:req:br-… (nil), logged ERROR and
                            // wrote a junk error-response key for every batch
                            // it had just answered (~60 ERRORs/hour).
                            let has_inline_cmd = Self::get_field_with_fallbacks(&fields, field_names::CMD).is_some();
                            match msg_type.as_deref() {
                                // Responses, canonical batches, info frames —
                                // consumed (XACK below) but never treated as
                                // compute requests.
                                Some("r") | Some("br") | Some("b") | Some("bc") | Some("i") => {}
                                Some("batch") => {
                                    // Legacy KeyBased batch - parse request_ids JSON array
                                    if let Some(ids_json) = Self::get_field_string(&fields, "request_ids") {
                                        if let Ok(request_ids) = serde_json::from_str::<Vec<String>>(&ids_json) {
                                            debug!("📦 KeyBased: Received batch of {} requests from {} via {}", request_ids.len(), site_id, stream_name);
                                            for req_id in request_ids {
                                                self.spawn_request_handler(&site_id, &req_id);
                                            }
                                        }
                                    }
                                }
                                // Unified inline command (t=c with body) —
                                // command_processor's job; a KeyBased GET on
                                // its id would nil-error identically.
                                Some("c") if has_inline_cmd => {}
                                _ => {
                                    if let Some(req_id) = request_id {
                                        // KeyBased single announcement (id, no inline body)
                                        debug!("🔔 KeyBased: Received compute request {} from {} via {}", req_id, site_id, stream_name);
                                        self.spawn_request_handler(&site_id, &req_id);
                                    } else {
                                        warn!("⚠️ KeyBased: Message missing request_id on {}: message_id={}", stream_name, message_id);
                                    }
                                }
                            }

                            // Acknowledge the message
                            let _: redis::RedisResult<i64> = redis::cmd("XACK")
                                .arg(&stream_name)
                                .arg(Self::CONSUMER_GROUP)
                                .arg(&message_id)
                                .query_async(&mut conn)
                                .await;
                        }
                    }
                }
                Err(e) => {
                    // Timeout is normal (nil response), only log actual errors
                    let err_str = e.to_string();
                    if !err_str.contains("nil") && !err_str.contains("response was nil") {
                        warn!("⚠️ KeyBased: XREADGROUP error: {}", e);
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        }
    }

    /// Extract a string value from the StreamId field map
    fn get_field_string(fields: &std::collections::HashMap<String, RedisValue>, key: &str) -> Option<String> {
        fields.get(key).and_then(|v| match v {
            RedisValue::BulkString(data) => String::from_utf8(data.clone()).ok(),
            RedisValue::SimpleString(s) => Some(s.clone()),
            _ => None,
        })
    }

    /// Extract a string value trying multiple field names (short-form preferred)
    /// Canonical short form: id, cmd/c, params/p, st, ts/t
    fn get_field_with_fallbacks(fields: &std::collections::HashMap<String, RedisValue>, keys: &[&str]) -> Option<String> {
        for key in keys {
            if let Some(value) = Self::get_field_string(fields, key) {
                return Some(value);
            }
        }
        None
    }

    /// Extract site_id from a stream key
    /// Pattern: {site_id}:gnode:unified:{environment} -> extracts site_id
    fn extract_site_from_stream(stream_key: &str) -> String {
        if stream_key.contains(":gnode:unified:") || stream_key.contains(":gnode:health:") {
            if let Some(site_id) = stream_key.split(":gnode:").next() {
                return site_id.to_string();
            }
        }
        "default".to_string()
    }

    /// Spawn a task to handle a single compute request
    fn spawn_request_handler(&self, site_id: &str, request_id: &str) {
        let redis_client = self.redis_client.clone();
        let command_registry = self.command_registry.clone();
        let topology = self.topology.clone();
        let debug = self.debug;
        let site = site_id.to_string();
        let req_id = request_id.to_string();

        tokio::spawn(async move {
            if let Err(e) = Self::handle_compute_request_static(
                &redis_client,
                &command_registry,
                &topology,
                &site,
                &req_id,
                debug
            ).await {
                error!("❌ KeyBased: Failed to handle request {}: {}", req_id, e);
            }
        });
    }

    /// Handle single compute request (static version for spawned tasks)
    async fn handle_compute_request_static(
        redis_client: &Client,
        command_registry: &Arc<CommandHandlerRegistry>,
        topology: &Arc<RwLock<GeometricTopology>>,
        site_id: &str,
        request_id: &str,
        debug_mode: bool,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let start = Instant::now();

        // 1. GET request from ValKey (ASYNC - non-blocking)
        let request_key = build_request_key(site_id, request_id);
        let mut conn = redis_client.get_multiplexed_async_connection().await?;

        let request_json: String = match conn.get(&request_key).await {
            Ok(json) => json,
            Err(e) => {
                error!("❌ KeyBased: Failed to GET request key {}: {}", request_key, e);
                // Store error response
                Self::store_error_response_static(redis_client, site_id, request_id, format!("Request not found: {}", e), Some("REQUEST_NOT_FOUND".to_string())).await?;
                return Ok(());
            }
        };

        // 2. Parse request
        let request: ComputeRequest = match serde_json::from_str(&request_json) {
            Ok(req) => req,
            Err(e) => {
                error!("❌ KeyBased: Failed to parse request {}: {}", request_id, e);
                Self::store_error_response_static(redis_client, site_id, request_id, format!("Invalid request JSON: {}", e), Some("INVALID_JSON".to_string())).await?;
                return Ok(());
            }
        };

        debug!("📥 KeyBased: Processing command '{}' for request {}", request.command, request_id);

        // 3. Execute command using existing command handler registry
        let result = Self::execute_command_static(redis_client, command_registry, topology, &request, request_id, site_id, debug_mode).await;

        let compute_time_ms = start.elapsed().as_millis() as u64;

        // 4. Build response
        let response = match result {
            Ok(cmd_result) => {
                if cmd_result.status == "ok" {
                    ComputeResponse::success(
                        request_id.to_string(),
                        cmd_result.result.unwrap_or(json!(null)),
                        compute_time_ms
                    )
                } else {
                    ComputeResponse::error(
                        request_id.to_string(),
                        cmd_result.error.unwrap_or_else(|| "Unknown error".to_string()),
                        Some("COMMAND_ERROR".to_string())
                    )
                }
            }
            Err(e) => {
                ComputeResponse::error(
                    request_id.to_string(),
                    e.to_string(),
                    Some("EXECUTION_ERROR".to_string())
                )
            }
        };

        // 5. Store response in ValKey with 10s TTL (ASYNC - non-blocking)
        Self::store_response_static(redis_client, site_id, request_id, &response).await?;

        if debug_mode {
            info!("✅ KeyBased: Computed '{}' in {}ms → {}", request.command, compute_time_ms, request_id);
        } else {
            debug!("✅ KeyBased: Computed '{}' in {}ms", request.command, compute_time_ms);
        }

        Ok(())
    }

    /// Execute command using async handler (preferred) or sync fallback (static version)
    async fn execute_command_static(
        redis_client: &Client,
        command_registry: &Arc<CommandHandlerRegistry>,
        topology: &Arc<RwLock<GeometricTopology>>,
        request: &ComputeRequest,
        request_id: &str,
        site_id: &str,
        debug_mode: bool,
    ) -> Result<CommandResult, Box<dyn std::error::Error + Send + Sync>> {
        // Create Command struct compatible with existing handler registry
        let command = Command {
            id: request_id.to_string(),
            command: request.command.clone(),
            parameters: request.parameters.clone(),
            site_id: request.site_id.clone(),
            node_id: "keybased".to_string(), // Marker for key-based requests
            timestamp: request.requested_at,
        };

        // Prefer async handlers for non-blocking execution (Phase 2: Async Architecture)
        if let Some(async_handler) = command_registry.get_async_handler(&request.command) {
            debug!("⚡ KeyBased: Using async handler for '{}'", request.command);
            let mut conn = redis_client.get_multiplexed_async_connection().await?;
            let result = async_handler(&command, &mut conn, topology, site_id, debug_mode).await;
            return Ok(result);
        }

        // Fallback to sync handler (commands not yet converted to async)
        let handler = command_registry.get_handler(&request.command)
            .ok_or_else(|| format!("Unknown command: {}", request.command))?;

        // Execute command (using sync connection for existing handlers)
        // NOTE: This still blocks but it's unavoidable without rewriting all command handlers
        // However, the GET/SET operations are now async which is the main bottleneck
        let mut conn = redis_client.get_connection()?;
        let result = handler(&command, &mut conn, topology, site_id, debug_mode);

        Ok(result)
    }

    /// Store response in ValKey (ASYNC - non-blocking, static version)
    async fn store_response_static(
        redis_client: &Client,
        site_id: &str,
        request_id: &str,
        response: &ComputeResponse
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let response_key = build_response_key(site_id, request_id);
        let response_json = serde_json::to_string(response)?;

        let mut conn = redis_client.get_multiplexed_async_connection().await?;

        // SETEX with 10 second TTL (ASYNC - non-blocking)
        redis::cmd("SETEX")
            .arg(&response_key)
            .arg(10)
            .arg(response_json)
            .query_async::<()>(&mut conn)
            .await?;

        debug!("💾 KeyBased: Stored response → {}", response_key);

        Ok(())
    }

    /// Store error response (ASYNC, static version)
    async fn store_error_response_static(
        redis_client: &Client,
        site_id: &str,
        request_id: &str,
        error: String,
        error_code: Option<String>
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let response = ComputeResponse::error(request_id.to_string(), error, error_code);
        Self::store_response_static(redis_client, site_id, request_id, &response).await
    }
}

/// Get current timestamp in seconds (with fractional part)
/// Returns 0.0 if system time is before UNIX_EPOCH (should never happen in practice)
fn current_timestamp() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_compute_response_success() {
        let response = ComputeResponse::success(
            "test_req_123".to_string(),
            json!({"result": "ok"}),
            15
        );

        assert_eq!(response.status, "ok");
        assert!(response.result.is_some());
        assert!(response.error.is_none());
        assert_eq!(response.compute_time_ms, 15);
    }

    #[tokio::test]
    async fn test_compute_response_error() {
        let response = ComputeResponse::error(
            "test_req_456".to_string(),
            "Test error".to_string(),
            Some("TEST_ERROR".to_string())
        );

        assert_eq!(response.status, "error");
        assert!(response.result.is_none());
        assert_eq!(response.error, Some("Test error".to_string()));
        assert_eq!(response.error_code, Some("TEST_ERROR".to_string()));
    }
}
