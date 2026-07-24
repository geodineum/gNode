// Fast Lane — async-spawned command dispatch
//
// The "Fast lane" is one of two execution lanes the daemon supports (the
// other is "Ordered", which runs handlers synchronously inline in the
// consumer-group thread). The lane choice is declared per command via
// `CommandDescriptor.lane` (see handlers/types.rs::Lane) — most commands
// are idempotent FCALL wrappers or read-only and live on the Fast lane;
// a small set with cross-request causal semantics opt into Ordered.
//
// This module owns a single multi-threaded tokio runtime, lazily
// initialized at daemon startup via `init()`. Worker threads in the
// consumer-group dispatch loop call `try_spawn_fast()` to hand a
// command's async handler over to this runtime. The spawn is
// fire-and-forget: the handler writes its own response to the
// `{ss}:res:{request_id}` polling key and the worker thread immediately
// reads the next stream message without waiting.
//
// Why a single static runtime instead of one-per-worker:
//   - Reduces total thread count (one shared async worker pool vs. one
//     per consumer thread).
//   - Simpler graceful-shutdown story — the daemon owns the runtime,
//     drops it once during teardown.
//   - Async tasks can outlive the consumer thread that spawned them
//     (useful for slow-but-not-blocking commands like geometric_discover
//     against a large topology).
//
// Why fail-soft (init() is optional):
//   - Tests, single-shot CLI tools, and the asset_builder path call
//     process_command_batch without setting up a runtime — they should
//     keep working, just always-synchronous. `try_spawn_fast` returns
//     false in that case and the caller falls back to the sync handler.

use std::sync::Arc;
use std::sync::OnceLock;
use std::future::Future;

use log::{debug, error, info, warn};
use redis::Client;
use tokio::runtime::Runtime;

use crate::daemon::Command;
use crate::GeometricTopology;
use std::sync::RwLock;

/// The Fast-lane runtime. Initialized once at daemon startup via `init()`.
static FAST_LANE_RUNTIME: OnceLock<Runtime> = OnceLock::new();

/// Shared redis::Client for async-connection construction inside spawned
/// tasks. Cheap to clone (just an Arc<...> internally). Initialized
/// alongside the runtime.
static FAST_LANE_CLIENT: OnceLock<Arc<Client>> = OnceLock::new();

/// Initialize the Fast-lane runtime + client. Call ONCE at daemon
/// startup, before any consumer worker spawns.
///
/// Idempotent: subsequent calls are no-ops (logs a warning).
///
/// # Arguments
///
/// * `client` - Shared redis::Client; spawned tasks use this to acquire
///              MultiplexedConnections for response writes.
/// * `worker_threads` - Size of the runtime's worker pool. 2-4 is
///                      typically sufficient — Fast-lane handlers are
///                      mostly I/O-bound FCALLs.
///
/// # Returns
///
/// `Ok(())` on success, `Err(...)` if the runtime couldn't be built.
pub fn init(client: Arc<Client>, worker_threads: usize) -> Result<(), String> {
    if FAST_LANE_RUNTIME.get().is_some() {
        warn!("fast_lane::init() called more than once — ignoring");
        return Ok(());
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads.max(1))
        .thread_name("gnode-fast-lane")
        .enable_all()
        .build()
        .map_err(|e| format!("Failed to build Fast-lane tokio runtime: {}", e))?;

    FAST_LANE_RUNTIME
        .set(rt)
        .map_err(|_| "Fast-lane runtime was set concurrently".to_string())?;
    FAST_LANE_CLIENT
        .set(client)
        .map_err(|_| "Fast-lane client was set concurrently".to_string())?;

    info!("Fast lane initialized ({} worker threads)", worker_threads);
    Ok(())
}

/// Check whether the Fast lane has been initialized.
pub fn is_initialized() -> bool {
    FAST_LANE_RUNTIME.get().is_some()
}

/// Spawn a future onto the Fast-lane runtime. Returns `true` if the
/// runtime was available and the future was queued, `false` otherwise
/// (caller should fall back to synchronous dispatch).
///
/// The future must be `'static + Send` — own everything it uses.
pub fn try_spawn_fast<F>(future: F) -> bool
where
    F: Future<Output = ()> + Send + 'static,
{
    match FAST_LANE_RUNTIME.get() {
        Some(rt) => {
            rt.spawn(future);
            true
        }
        None => false,
    }
}

/// Dispatch a command via the Fast lane. Used by command_processor's
/// dispatch loop for `Lane::Fast` commands that have an async handler
/// registered.
///
/// The function:
///   1. Acquires an async connection from the shared client.
///   2. Looks up the async handler in the global registry.
///   3. Awaits the handler.
///   4. Writes the response JSON to the `{ss}:res:{request_id}` polling
///      key (10 s TTL) — same key shape PHP clients poll on.
///
/// Errors are logged but never propagated — the worker thread that
/// spawned this task has already moved on, so there's no caller to
/// surface to. Failures here are operationally visible via the
/// "[fast_lane]" log prefix.
pub async fn dispatch(
    command: Command,
    site_id: String,
    topology: Arc<RwLock<GeometricTopology>>,
    environment: Option<String>,
    debug_mode: bool,
) {
    let client = match FAST_LANE_CLIENT.get() {
        Some(c) => c,
        None => {
            error!("[fast_lane] dispatch called but client not initialized — dropping command {}", command.command);
            return;
        }
    };

    let mut conn = match client.get_multiplexed_async_connection().await {
        Ok(c) => c,
        Err(e) => {
            error!(
                "[fast_lane] failed to acquire async connection for command {}: {}",
                command.command, e
            );
            return;
        }
    };

    let registry = crate::integration::command_handler::get_command_registry();

    let result = match registry.get_async_handler(&command.command) {
        Some(handler) => {
            if debug_mode {
                debug!(
                    "[fast_lane] dispatching {} (id={}, site={})",
                    command.command, command.id, site_id
                );
            }
            handler(&command, &mut conn, &topology, &site_id, debug_mode).await
        }
        None => {
            // Defensive: try_spawn_fast should only be reached after
            // has_async() returned true, so this is a logic bug.
            error!(
                "[fast_lane] no async handler for {} despite has_async=true at dispatch time",
                command.command
            );
            return;
        }
    };

    let response = result.to_response(&command.id);

    // Write response to the polling key — same shape PHP clients consume.
    // Resolve the request id exactly like the synchronous path does:
    // parameters._request_id first (what pollForResponse keys on), then
    // command.id. command.id is only trustworthy when the reader parsed
    // the wire "id" field into it — a reader that falls back to the
    // stream entry id would send responses to a key no client ever
    // polls, hanging every polled fast-lane command to timeout.
    let rid: Option<String> = command
        .parameters
        .get("_request_id")
        .and_then(|v| v.as_str())
        .map(String::from)
        .or_else(|| {
            if !command.id.is_empty() {
                Some(command.id.clone())
            } else {
                None
            }
        });

    if let Some(request_id) = rid {
        let key_site = if command.site_id.is_empty() {
            site_id.as_str()
        } else {
            command.site_id.as_str()
        };
        let response_key = format!("{{{}}}:res:{}", key_site, request_id);
        let response_json = serde_json::json!({
            "id": response.id,
            "status": response.status,
            "result": response.result,
            "error": response.error,
            "timestamp": response.timestamp,
        })
        .to_string();

        let write_result: redis::RedisResult<()> = redis::cmd("SET")
            .arg(&response_key)
            .arg(&response_json)
            .arg("EX")
            .arg(10)
            .query_async(&mut conn)
            .await;

        if let Err(e) = write_result {
            error!(
                "[fast_lane] failed to write response to {}: {}",
                response_key, e
            );
        } else if debug_mode {
            debug!("[fast_lane] response written to {}", response_key);
        }

        // Durable channel: a signed receipt beside the ephemeral reply
        // (additive; see contract §6, emit-then-remove).
        let now = crate::integration::receipt::now_ms();
        let env = environment.or_else(|| {
            crate::integration::receipt::receipt_context().map(|c| c.environment.clone())
        });
        if let (Some(env), Some(receipt)) = (env, crate::integration::receipt::signed_response_receipt(
            &request_id,
            &command.command,
            &response.status,
            response.error.clone(),
            key_site,
            &response_key,
            &response_json,
            now,
        )) {
            if let Err(e) =
                crate::integration::receipt::emit_receipt_async(&mut conn, &receipt, &env, now).await
            {
                warn!("[fast_lane] receipt emit failed for {}: {}", request_id, e);
            }
        }
    } else {
        if debug_mode {
            debug!(
                "[fast_lane] command {} had no id and no _request_id — skipping response write",
                command.command
            );
        }
    }
}
