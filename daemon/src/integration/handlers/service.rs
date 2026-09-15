// Service Management Command Handlers
//
// Handles: registerService, deregisterService, discover_with_endpoints
// These manage service registration in the geometric topology and provide
// service discovery with endpoint enrichment.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use std::pin::Pin;
use std::future::Future;
use redis::Connection;
use redis::aio::MultiplexedConnection as AsyncConnection;
use serde::Deserialize;
use log::{debug, info, error};
use serde_json::{Value, json};
use crate::daemon::Command;
use crate::GeometricTopology;

use super::types::{
    CommandResult, CommandHandlerFn, AsyncCommandHandlerFn, CommandDescriptor,
    parse_parameters, build_service_capability_vector, discovery_point_from_full, Lane,
};

/// Register all service command handlers
pub fn register(
    handlers: &mut HashMap<String, CommandHandlerFn>,
    async_handlers: &mut HashMap<String, AsyncCommandHandlerFn>,
    descriptors: &mut Vec<CommandDescriptor>,
) {
    // Sync handlers
    handlers.insert("registerService".to_string(), handle_register_service as CommandHandlerFn);
    handlers.insert("deregisterService".to_string(), handle_deregister_service as CommandHandlerFn);
    handlers.insert("discover_with_endpoints".to_string(), handle_discover_with_endpoints as CommandHandlerFn);
    handlers.insert("DISCOVER_WITH_ENDPOINTS".to_string(), handle_discover_with_endpoints as CommandHandlerFn);
    handlers.insert("service_endpoints".to_string(), handle_discover_with_endpoints as CommandHandlerFn);

    // Async handlers - Phase 2 multi-node critical
    async_handlers.insert("register_service".to_string(), handle_register_service_async as AsyncCommandHandlerFn);
    async_handlers.insert("registerService".to_string(), handle_register_service_async as AsyncCommandHandlerFn);
    async_handlers.insert("REGISTER_SERVICE".to_string(), handle_register_service_async as AsyncCommandHandlerFn);
    async_handlers.insert("deregister_service".to_string(), handle_deregister_service_async as AsyncCommandHandlerFn);
    async_handlers.insert("deregisterService".to_string(), handle_deregister_service_async as AsyncCommandHandlerFn);
    async_handlers.insert("DEREGISTER_SERVICE".to_string(), handle_deregister_service_async as AsyncCommandHandlerFn);

    // Async handlers - discover_with_endpoints
    async_handlers.insert("discover_with_endpoints".to_string(), handle_discover_with_endpoints_async as AsyncCommandHandlerFn);
    async_handlers.insert("DISCOVER_WITH_ENDPOINTS".to_string(), handle_discover_with_endpoints_async as AsyncCommandHandlerFn);
    async_handlers.insert("service_endpoints".to_string(), handle_discover_with_endpoints_async as AsyncCommandHandlerFn);

    // Command descriptors
    descriptors.push(CommandDescriptor {
        name: "registerService",
        category: "service",
        description: "Register a service with capabilities into the geometric topology",
        params_schema: json!({
            "type": "object",
            "properties": {
                "id": {"type": "string", "description": "Unique service identifier"},
                "capabilities": {
                    "type": "object",
                    "description": "Service-tier axis name → value in [0, 1] (names and codes: GNODE_SCHEMA_GET); axes left out are stored as 0",
                    "additionalProperties": {"type": "number", "minimum": 0.0, "maximum": 1.0}
                },
                "metadata": {
                    "type": "object",
                    "description": "Optional service metadata (type, url, version, etc.)",
                    "additionalProperties": {"type": "string"}
                }
            },
            "required": ["id", "capabilities"]
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "status": {"type": "string"},
                "service_id": {"type": "string"},
                "registered": {"type": "boolean"},
                "topology_key": {"type": "string"},
                "bucket_key": {"type": "string"},
                "z_score": {"type": "integer"}
            }
        }),
        example: r#"{"cmd":"registerService","params":{"id":"my-svc","capabilities":{"protocol":0.5,"service_scope":0.3},"metadata":{"type":"worker"}}}"#,
        async_capable: true,
        lane: Lane::Concurrent,
    });

    descriptors.push(CommandDescriptor {
        name: "deregisterService",
        category: "service",
        description: "Remove a service from the geometric topology",
        params_schema: json!({
            "type": "object",
            "properties": {
                "service_id": {"type": "string", "description": "Service identifier to remove"}
            },
            "required": ["service_id"]
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "status": {"type": "string", "enum": ["deregistered", "not_found"]},
                "service_id": {"type": "string"}
            }
        }),
        example: r#"{"cmd":"deregisterService","params":{"service_id":"my-svc"}}"#,
        async_capable: true,
        lane: Lane::Concurrent,
    });

    descriptors.push(CommandDescriptor {
        name: "discover_with_endpoints",
        category: "service",
        description: "Discover services and include endpoint translation info",
        params_schema: json!({
            "type": "object",
            "properties": {
                "capabilities": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Capability names every match must hold with a value > 0; empty matches all"
                },
                "endpoint_registry": {"type": "string", "description": "Endpoint registry key; default {site_id}:gnode:endpoints"},
                "limit": {"type": "integer", "default": 10, "description": "Maximum services, taken after sorting by id; 0 is unlimited"}
            }
        }),
        returns_schema: json!({
            "type": "object",
            "properties": {
                "count": {"type": "integer"},
                "services": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "service_id": {"type": "string"},
                            "endpoints": {"type": "array", "description": "From GNODE_ENDPOINT_LIST (gNode-BROKER); null when unavailable"}
                        }
                    }
                },
                "endpoints_error": {"type": "string", "description": "Present when endpoints could not be listed, with the reason"}
            }
        }),
        example: r#"{"cmd":"discover_with_endpoints","params":{"capabilities":["protocol","domain_primary"],"limit":5}}"#,
        async_capable: true,
        lane: Lane::Concurrent,
    });
}

// =========================================================================
// Parameter structs
// =========================================================================

/// Parameters for the registerService command
#[derive(Debug, Deserialize)]
#[cfg_attr(test, derive(serde::Serialize, Default))]
struct RegisterServiceParams {
    id: String,
    capabilities: HashMap<String, f64>,
    #[serde(default)]
    metadata: HashMap<String, String>,
}

/// Parameters for the deregisterService command
#[derive(Debug, Deserialize)]
#[cfg_attr(test, derive(serde::Serialize, Default))]
struct DeregisterServiceParams {
    /// The service ID to deregister (can be passed as 'id' or 'service_id')
    #[serde(alias = "id")]
    service_id: String,
}

/// Parameters for the discover_with_endpoints command
#[derive(Debug, Deserialize)]
#[cfg_attr(test, derive(serde::Serialize, Default))]
struct DiscoverWithEndpointsParams {
    /// Capability names every match must hold with a value > 0
    #[serde(default)]
    capabilities: Vec<String>,
    /// Endpoint registry key (default: {site_id}:gnode:endpoints)
    #[serde(default)]
    endpoint_registry: Option<String>,
    /// Maximum services to return (0 = unlimited)
    #[serde(default = "default_limit")]
    limit: usize,
}

fn default_limit() -> usize { 10 }

// =========================================================================
// Shared stateless registration compute (connection-agnostic)
//
// Rust/gMath does the heavy lifting (capability vector, bucket key, z-score);
// the result is captured in ValKey. Reused by BOTH the sync handler (batch/
// pending dispatch) and the async handler (concurrent-lane) so all transports write
// the SAME canonical (C) representation — `{site_id}:gnode:services:entities`
// + voxel. The connection I/O (sync `query` vs async `query_async`) is NOT
// shared because the sync/async connection types differ; only the COMPUTE is.
// =========================================================================

/// Global derived-snapshot hash key: `{topology_namespace}:gnode:topology:services`.
/// Field = service_id, value = `{point, metadata}` — the read-shape PHP
/// `getTopology()` consumes. A projection of (C), never authoritative.
fn topology_snapshot_key() -> String {
    format!("{{{}}}:gnode:topology:services", crate::daemon::GNodeDaemon::get_topology_namespace())
}

/// Pre-computed registration write plan (connection-agnostic).
struct RegistrationPlan {
    topology_key: String,
    entity_json: String,
    bucket_key: String,
    z_score: i64,
}

/// Validate params and build the (C) entity write + (B) snapshot entry.
/// All Q64.64 arithmetic (vector, bucket key, z-score) is gMath.
fn plan_registration(params: &RegisterServiceParams, site_id: &str) -> Result<RegistrationPlan, String> {
    if params.id.is_empty() {
        return Err("Service ID cannot be empty".to_string());
    }
    for (cap_name, cap_value) in &params.capabilities {
        if *cap_value < 0.0 || *cap_value > 1.0 {
            return Err(format!(
                "Invalid capability value for '{}': {} (must be between 0.0 and 1.0)",
                cap_name, cap_value
            ));
        }
    }

    // Service-tier capability vector (30D, Q64.64 via gMath)
    let point = build_service_capability_vector(&params.capabilities);

    // Bucket key from discovery dims (25D for service tier), z-score from dim#16
    let disc_point = discovery_point_from_full(&point);
    let bucket_key = GeometricTopology::point_to_bucket_key(&disc_point, 10);
    let z_score = GeometricTopology::compute_service_z_score(&point);

    let point_raw: Vec<String> = (0..point.len()).map(|i| point[i].raw().to_string()).collect();
    let point_display: Vec<f64> = (0..point.len())
        .map(|i| (point[i].to_f64() * 1000.0).round() / 1000.0)
        .collect();

    // (C) authoritative entity — abbreviated fields (see gnode_topo.lua header)
    let entity_json = json!({
        "pr": point_raw,
        "pd": point_display,
        "c": params.capabilities,
        "m": params.metadata
    }).to_string();

    // NOTE: (B) snapshot is maintained by the Lua primitive (passed snapshot_key),
    // so EVERY transport — not just this handler — keeps it current.
    Ok(RegistrationPlan {
        topology_key: GeometricTopology::get_services_topology_key(site_id),
        entity_json,
        bucket_key,
        z_score,
    })
}

/// Ids of entities in a GNODE_TOPO_GET_ENTITIES reply (`{ents:{id:{c,...}}}`) that hold
/// every requested capability name with a value > 0, sorted, then cut to `limit`
/// (0 = unlimited). Empty `caps` → all entities.
fn filter_entities_by_capabilities(entities_json: &str, caps: &[String], limit: usize) -> Result<Vec<String>, String> {
    let v: Value = serde_json::from_str(entities_json).map_err(|e| format!("unreadable topology reply: {}", e))?;
    let mut out = Vec::new();
    if let Some(ents) = v.get("ents").and_then(|e| e.as_object()) {
        for (id, data) in ents {
            let c = data.get("c").and_then(|x| x.as_object());
            let has_all = caps.iter().all(|cap|
                c.and_then(|m| m.get(cap)).and_then(|x| x.as_f64()).map(|val| val > 0.0).unwrap_or(false));
            if has_all {
                out.push(id.clone());
            }
        }
    }
    out.sort();
    if limit > 0 {
        out.truncate(limit);
    }
    Ok(out)
}

/// Endpoints from a GNODE_ENDPOINT_LIST reply (gNode-BROKER: `{status, result: {count, endpoints}}`),
/// or why there are none.
fn endpoints_from_reply(reply: redis::RedisResult<String>) -> Result<Value, String> {
    let json_str = reply.map_err(|e| format!("GNODE_ENDPOINT_LIST failed: {}", e))?;
    let v: Value = serde_json::from_str(&json_str).map_err(|e| format!("unreadable GNODE_ENDPOINT_LIST reply: {}", e))?;
    if v.get("status").and_then(|s| s.as_str()) != Some("ok") {
        return Err(v.get("error").and_then(|e| e.as_str()).unwrap_or("GNODE_ENDPOINT_LIST refused").to_string());
    }
    Ok(v.get("result").and_then(|r| r.get("endpoints")).cloned().unwrap_or_else(|| json!([])))
}

fn endpoints_reply(services: Vec<(String, Result<Value, String>)>) -> CommandResult {
    let endpoints_error = services.iter().find_map(|(_, r)| r.as_ref().err().cloned());
    let services: Vec<Value> = services
        .into_iter()
        .map(|(service_id, r)| json!({ "service_id": service_id, "endpoints": r.unwrap_or(Value::Null) }))
        .collect();
    let mut reply = json!({ "count": services.len(), "services": services });
    if let Some(e) = endpoints_error {
        reply["endpoints_error"] = json!(e);
    }
    CommandResult::success(reply)
}

// =========================================================================
// Sync handlers
// =========================================================================

/// Handle 'registerService' command - Register external service in topology
///
/// This bridges external stream-based registration (from PHP gNode-Client, WordPress, etc.)
/// to the internal register_service() method used by the daemon itself.
///
/// Expected parameters:
/// - id: Service identifier (e.g., "my_app", "inference_service_1")
/// - capabilities: Map of capability name => value (0.0-1.0)
/// - metadata: Optional metadata (type, url, version, etc.)
pub fn handle_register_service(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,  // UNUSED - stateless architecture
    site_id: &str,
    debug_mode: bool
) -> CommandResult {
    if debug_mode {
        debug!("[STATELESS] Handling registerService command: {}", command.id);
    }

    // Parse parameters
    let params = match parse_parameters::<RegisterServiceParams>(command) {
        Ok(p) => p,
        Err(e) => {
            error!("Failed to parse registerService parameters: {}", e);
            return CommandResult::error(format!("Invalid parameters: {}", e));
        }
    };

    // Validate + compute (shared with async concurrent-lane)
    let plan = match plan_registration(&params, site_id) {
        Ok(p) => p,
        Err(e) => return CommandResult::error(e),
    };

    // STATELESS: ensure services topology exists (sync FCALL)
    if let Err(e) = redis::cmd("FCALL")
        .arg("GNODE_ENSURE_TOPOLOGY").arg(1).arg(site_id)
        .query::<String>(conn)
    {
        return CommandResult::error(format!("Failed to ensure services topology: {:?}", e));
    }

    // STATELESS: register entity into the canonical (C) representation (sync FCALL).
    // args[5] = snapshot_key → the Lua primitive maintains the (B) snapshot.
    // args[6..7] = where and how wide the registration_order axis is.
    let register_result = redis::cmd("FCALL")
        .arg("GNODE_REGISTER_CAPABILITY_VECTOR").arg(1)
        .arg(&plan.topology_key)
        .arg(&params.id)
        .arg(&plan.entity_json)
        .arg(&plan.bucket_key)
        .arg(plan.z_score)
        .arg(topology_snapshot_key())
        .arg(super::types::registration_order_index(super::types::get_service_dimensions()))
        .arg(super::types::POINT_FRAC_BITS)
        .query::<String>(conn);

    match register_result {
        Ok(result_json) => {
            let ok = serde_json::from_str::<Value>(&result_json)
                .ok()
                .and_then(|v| v.get("ok").and_then(|b| b.as_bool()))
                .unwrap_or(true);
            if !ok {
                return CommandResult::error(format!("Registration failed: {}", result_json));
            }

            CommandResult::success(json!({
                "status": "ok",
                "service_id": params.id,
                "registered": true,
                "topology_key": plan.topology_key,
                "bucket_key": plan.bucket_key,
                "z_score": plan.z_score,
                "stateless": true
            }))
        },
        Err(e) => CommandResult::error(format!("Registration FCALL failed: {:?}", e)),
    }
}

/// Handle 'deregisterService' command - Remove a service from the topology
///
/// This removes a service from:
/// - The topology's services HashMap
/// - The spatial hash index
/// - The dependencies map
///
/// The removal is persisted to ValKey immediately.
pub fn handle_deregister_service(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,  // UNUSED - stateless architecture
    site_id: &str,
    debug_mode: bool
) -> CommandResult {
    if debug_mode {
        debug!("[STATELESS] Handling deregisterService command: {}", command.id);
    }

    // Parse parameters
    let params = match parse_parameters::<DeregisterServiceParams>(command) {
        Ok(p) => p,
        Err(e) => {
            error!("Failed to parse deregisterService parameters: {}", e);
            return CommandResult::error(format!("Invalid parameters: {}", e));
        }
    };

    // Validate service ID
    if params.service_id.is_empty() {
        return CommandResult::error("Service ID cannot be empty");
    }

    // Prevent deregistration of the daemon itself
    if params.service_id.starts_with("gnode-daemon") {
        return CommandResult::error("Cannot deregister the gNode daemon itself");
    }

    // STATELESS: deregister entity from the canonical (C) representation (sync FCALL).
    // args[2] = snapshot_key → the Lua primitive mirrors removal from the (B) snapshot.
    let topology_key = GeometricTopology::get_services_topology_key(site_id);
    let deregister_result = redis::cmd("FCALL")
        .arg("GNODE_DEREGISTER_CAPABILITY_VECTOR").arg(1)
        .arg(&topology_key)
        .arg(&params.service_id)
        .arg(topology_snapshot_key())
        .query::<String>(conn);

    match deregister_result {
        Ok(result_json) => {
            let result = serde_json::from_str::<Value>(&result_json).unwrap_or(Value::Null);
            let was_found = result.get("ok").and_then(|v| v.as_bool()) == Some(true);
            if was_found {
                info!("Deregistered service '{}'", params.service_id);
                CommandResult::success(json!({
                    "status": "deregistered",
                    "service_id": params.service_id,
                    "stateless": true
                }))
            } else {
                let error_msg = result.get("error").and_then(|v| v.as_str()).unwrap_or("");
                if error_msg.contains("not found") || error_msg.contains("Entity not found") {
                    CommandResult::success(json!({
                        "status": "not_found",
                        "service_id": params.service_id,
                        "message": "Service was not registered",
                        "stateless": true
                    }))
                } else {
                    CommandResult::error(format!("Deregistration failed: {}", result_json))
                }
            }
        },
        Err(e) => CommandResult::error(format!("Deregistration FCALL failed: {:?}", e)),
    }
}

/// STATELESS: discover services by capability presence over the (C) entities
/// (via FCALL GNODE_TOPO_GET_ENTITIES), then enrich each with its endpoints.
pub fn handle_discover_with_endpoints(
    command: &Command,
    conn: &mut Connection,
    _topology: &Arc<RwLock<GeometricTopology>>,
    site_id: &str,
    debug_mode: bool,
) -> CommandResult {
    if debug_mode { debug!("Handling discover_with_endpoints command"); }
    let params: DiscoverWithEndpointsParams = match serde_json::from_value(command.parameters.clone()) {
        Ok(p) => p,
        Err(e) => return CommandResult::error(format!("Invalid parameters: {}", e)),
    };

    let topology_key = GeometricTopology::get_services_topology_key(site_id);
    let service_ids = match redis::cmd("FCALL")
        .arg("GNODE_TOPO_GET_ENTITIES").arg(1).arg(&topology_key).arg("*")
        .query::<String>(conn)
        .map_err(|e| format!("Discovery FCALL failed: {:?}", e))
        .and_then(|j| filter_entities_by_capabilities(&j, &params.capabilities, params.limit))
    {
        Ok(ids) => ids,
        Err(e) => return CommandResult::error(e),
    };

    let endpoint_registry = params.endpoint_registry
        .unwrap_or_else(|| format!("{{{}}}:gnode:endpoints", site_id));
    let mut services = Vec::with_capacity(service_ids.len());
    for service_id in service_ids {
        let reply = redis::cmd("FCALL")
            .arg("GNODE_ENDPOINT_LIST").arg(1).arg(&endpoint_registry).arg(&service_id)
            .query::<String>(conn);
        services.push((service_id, endpoints_from_reply(reply)));
    }
    endpoints_reply(services)
}

// =========================================================================
// Async handlers
// =========================================================================

/// Async version of handle_register_service (STATELESS Architecture)
///
/// Uses FCALL to GNODE_REGISTER_CAPABILITY_VECTOR - NO in-memory state.
/// All topology data lives in ValKey as single source of truth.
///
/// Flow:
///   1. Parse and validate parameters
///   2. Build service-tier capability vector (Q64.64)
///   3. Compute bucket_key (76 chars; computed from 25 discovery dims for service tier)
///   4. Compute z_score (dimension 16: current_load)
///   5. Build entity JSON with abbreviated fields
///   6. FCALL GNODE_ENSURE_TOPOLOGY (create if needed)
///   7. FCALL GNODE_REGISTER_CAPABILITY_VECTOR (store in ValKey)
pub fn handle_register_service_async<'a>(
    command: &'a Command,
    conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,  // UNUSED - stateless architecture
    site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("[STATELESS] Handling async registerService command: {}", command.id);
        }

        // Parse parameters
        let params: RegisterServiceParams = match serde_json::from_value(command.parameters.clone()) {
            Ok(p) => p,
            Err(e) => return CommandResult::error(format!("Invalid parameters: {}", e)),
        };

        // Validate + compute (shared with sync batch/pending path)
        let plan = match plan_registration(&params, site_id) {
            Ok(p) => p,
            Err(e) => return CommandResult::error(e),
        };

        // ====================================================================
        // STATELESS: Ensure services topology exists for this site
        // ====================================================================
        let ensure_result: redis::RedisResult<String> = redis::cmd("FCALL")
            .arg("GNODE_ENSURE_TOPOLOGY")
            .arg(1)
            .arg(site_id)
            .query_async(conn)
            .await;

        if let Err(e) = ensure_result {
            return CommandResult::error(format!("Failed to ensure services topology: {:?}", e));
        }

        // ====================================================================
        // STATELESS: Register entity into the canonical (C) representation
        // ====================================================================
        let register_result: redis::RedisResult<String> = redis::cmd("FCALL")
            .arg("GNODE_REGISTER_CAPABILITY_VECTOR")
            .arg(1)
            .arg(&plan.topology_key)
            .arg(&params.id)
            .arg(&plan.entity_json)
            .arg(&plan.bucket_key)
            .arg(plan.z_score)
            .arg(topology_snapshot_key())  // args[5]: Lua maintains (B) snapshot
            .arg(super::types::registration_order_index(super::types::get_service_dimensions()))  // args[6]
            .arg(super::types::POINT_FRAC_BITS)  // args[7]
            .query_async(conn)
            .await;

        match register_result {
            Ok(result_json) => {
                // Parse Lua response to check success
                if let Ok(result) = serde_json::from_str::<Value>(&result_json) {
                    if result.get("ok").and_then(|v| v.as_bool()) == Some(true) {
                        CommandResult::success(json!({
                            "status": "ok",
                            "service_id": params.id,
                            "registered": true,
                            "topology_key": plan.topology_key,
                            "bucket_key": plan.bucket_key,
                            "z_score": plan.z_score,
                            "stateless": true,
                            "precision": "Q64.64"
                        }))
                    } else {
                        CommandResult::error(format!("Registration failed: {}", result_json))
                    }
                } else {
                    // Non-JSON response, assume success
                    CommandResult::success(json!({
                        "status": "ok",
                        "service_id": params.id,
                        "registered": true,
                        "stateless": true
                    }))
                }
            },
            Err(e) => CommandResult::error(format!("Registration FCALL failed: {:?}", e))
        }
    })
}

/// Async version of handle_deregister_service (STATELESS Architecture)
///
/// Uses FCALL to GNODE_DEREGISTER_CAPABILITY_VECTOR - NO in-memory state.
/// All topology data lives in ValKey as single source of truth.
///
/// Flow:
///   1. Parse and validate service_id
///   2. FCALL GNODE_DEREGISTER_CAPABILITY_VECTOR (removes from ValKey)
pub fn handle_deregister_service_async<'a>(
    command: &'a Command,
    conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,  // UNUSED - stateless architecture
    site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("[STATELESS] Handling async deregisterService command: {}", command.id);
        }

        // Parse parameters
        let service_id = match command.parameters.get("service_id")
            .or_else(|| command.parameters.get("id"))
            .and_then(|v| v.as_str())
        {
            Some(id) if !id.is_empty() => id.to_string(),
            _ => return CommandResult::error("Missing or empty service_id parameter"),
        };

        // Prevent deregistration of daemon
        if service_id.starts_with("gnode-daemon") {
            return CommandResult::error("Cannot deregister the gNode daemon itself");
        }

        // ====================================================================
        // STATELESS: Deregister entity via FCALL (no in-memory update)
        // ====================================================================
        let topology_key = GeometricTopology::get_services_topology_key(site_id);

        let deregister_result: redis::RedisResult<String> = redis::cmd("FCALL")
            .arg("GNODE_DEREGISTER_CAPABILITY_VECTOR")
            .arg(1)
            .arg(&topology_key)
            .arg(&service_id)
            .arg(topology_snapshot_key())  // args[2]: Lua mirrors (B) snapshot removal
            .query_async(conn)
            .await;

        match deregister_result {
            Ok(result_json) => {
                // Parse Lua response to check result
                if let Ok(result) = serde_json::from_str::<Value>(&result_json) {
                    let was_found = result.get("ok").and_then(|v| v.as_bool()) == Some(true);

                    if was_found {
                        CommandResult::success(json!({
                            "status": "deregistered",
                            "service_id": service_id,
                            "topology_key": topology_key,
                            "stateless": true
                        }))
                    } else {
                        // Check if it's a "not found" vs error
                        let error_msg = result.get("error")
                            .and_then(|v| v.as_str())
                            .unwrap_or("Unknown error");

                        if error_msg.contains("not found") || error_msg.contains("Entity not found") {
                            CommandResult::success(json!({
                                "status": "not_found",
                                "service_id": service_id,
                                "message": "Service was not registered",
                                "stateless": true
                            }))
                        } else {
                            CommandResult::error(format!("Deregistration failed: {}", error_msg))
                        }
                    }
                } else {
                    // Non-JSON response
                    CommandResult::success(json!({
                        "status": "deregistered",
                        "service_id": service_id,
                        "stateless": true
                    }))
                }
            },
            Err(e) => CommandResult::error(format!("Deregistration FCALL failed: {:?}", e))
        }
    })
}

/// Async handler for discover_with_endpoints command
/// Combines capability-presence discovery with endpoint listing in a single call
pub fn handle_discover_with_endpoints_async<'a>(
    command: &'a Command,
    conn: &'a mut AsyncConnection,
    _topology: &'a Arc<RwLock<GeometricTopology>>,
    site_id: &'a str,
    debug_mode: bool,
) -> Pin<Box<dyn Future<Output = CommandResult> + Send + 'a>> {
    Box::pin(async move {
        if debug_mode {
            debug!("Handling discover_with_endpoints command: {}", command.id);
        }

        let params: DiscoverWithEndpointsParams = match serde_json::from_value(command.parameters.clone()) {
            Ok(p) => p,
            Err(e) => return CommandResult::error(format!("Invalid parameters: {}", e)),
        };

        let topology_key = GeometricTopology::get_services_topology_key(site_id);
        let entities: redis::RedisResult<String> = redis::cmd("FCALL")
            .arg("GNODE_TOPO_GET_ENTITIES").arg(1).arg(&topology_key).arg("*")
            .query_async(conn)
            .await;
        let service_ids = match entities
            .map_err(|e| format!("Discovery FCALL failed: {:?}", e))
            .and_then(|j| filter_entities_by_capabilities(&j, &params.capabilities, params.limit))
        {
            Ok(ids) => ids,
            Err(e) => return CommandResult::error(e),
        };

        let endpoint_registry = params.endpoint_registry
            .unwrap_or_else(|| format!("{{{}}}:gnode:endpoints", site_id));
        let mut services = Vec::with_capacity(service_ids.len());
        for service_id in service_ids {
            let reply: redis::RedisResult<String> = redis::cmd("FCALL")
                .arg("GNODE_ENDPOINT_LIST").arg(1).arg(&endpoint_registry).arg(&service_id)
                .query_async(conn)
                .await;
            services.push((service_id, endpoints_from_reply(reply)));
        }
        endpoints_reply(services)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::types::assert_descriptor_fields;

    #[test]
    fn descriptors_name_exactly_the_fields_the_parsers_read() {
        let (mut handlers, mut async_handlers, mut descriptors) = (HashMap::new(), HashMap::new(), Vec::new());
        register(&mut handlers, &mut async_handlers, &mut descriptors);
        assert_descriptor_fields::<RegisterServiceParams>(&descriptors, "registerService", true);
        assert_descriptor_fields::<DeregisterServiceParams>(&descriptors, "deregisterService", true);
        assert_descriptor_fields::<DiscoverWithEndpointsParams>(&descriptors, "discover_with_endpoints", true);
    }

    #[test]
    fn capability_matches_are_sorted_before_the_limit() {
        let reply = json!({"ents": {
            "c": {"c": {"protocol": 0.1}},
            "a": {"c": {"protocol": 0.1}},
            "b": {"c": {"protocol": 0.0}}
        }}).to_string();
        let caps = vec!["protocol".to_string()];
        assert_eq!(filter_entities_by_capabilities(&reply, &caps, 1).unwrap(), vec!["a"]);
        assert_eq!(filter_entities_by_capabilities(&reply, &caps, 0).unwrap(), vec!["a", "c"]);
    }

    #[test]
    fn endpoint_replies_unwrap_the_broker_envelope_and_name_failures() {
        let listed = Ok(json!({"status": "ok", "result": {"count": 1, "endpoints": [{"path": "/x"}]}}).to_string());
        assert_eq!(endpoints_from_reply(listed).unwrap()[0]["path"], "/x");
        let refused = Ok(json!({"status": "error", "error": "no registry"}).to_string());
        assert_eq!(endpoints_from_reply(refused).unwrap_err(), "no registry");

        let reply = endpoints_reply(vec![("a".into(), Err("no BROKER".into()))]).result.unwrap();
        assert_eq!(reply["endpoints_error"], "no BROKER");
        assert!(reply["services"][0]["endpoints"].is_null());
    }
}
