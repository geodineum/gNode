// Integration Module for gNode
//
// This module contains components for integrating the gNode daemon with external systems,
// particularly ValKey/Redis for communication between clients and the daemon.
//
// The module is organized into several sub-modules:
// - path_resolution: Functions for resolving paths to ValKey functions
// - shared_manager: Shared instance of ValKey function manager
// - valkey_functions: Functions for initializing and executing ValKey functions (FCALL)
// - stream_processor: Functions for processing command streams
// - processor: New unified stream approach with RESP3 optimization
// - consumer_groups: Functions for working with ValKey/Redis consumer groups
// - error_handlings: Functions for handling and recovering from errors
// - connection_manager: Functions for managing Redis connections
// - command_processor: Unified command processing pipeline
// - diagnostics: Functions for diagnosing and fixing stream processing issues

// Re-export path resolution
pub mod path_resolution;
pub use path_resolution::{
    find_valkey_functions_directory,
    find_function_file,
    verify_directory_contents,
    verify_directory_contents_with_predicate,
};

pub use crate::config::GNodeSettings;
pub use crate::integration::processor::stream_utils::{
    current_timestamp,
    current_timestamp_ms,
    trim_unified_stream
};

pub use crate::integration::consumer_groups::{
    create_unified_stream_worker, ConsumerGroupState
};

// Re-export processor (unified stream approach)
pub use processor::{
    // RESP3 protocol
    Resp3Value, OptimizedCommand,

    //STREAM READER
    StreamReader,
    StreamReaderError,
    StreamReaderResult,

    // Unified stream processor
    initialize_unified_stream,
};

// Shared manager for ValKey functions
pub mod shared_manager;
pub use shared_manager::{
    get_valkey_manager,
    initialize_shared_valkey_manager,
    with_valkey_manager,
};

// ValKey functions module (FCALL - primary execution path)
pub mod valkey_functions;

// Processor module (unified stream approach)
pub mod processor;

// Stream processor compatibility module
pub mod stream_processor;

// Consumer groups module
pub mod consumer_groups;
pub use consumer_groups::{
    ConsumerGroupNodeState,
    acknowledge_messages,
};

// Error handling module
pub mod error_handlings;
pub use error_handlings::{
    // Error types and aliases
    IntegrationError, IntegrationResult, IntegrationErrorKind,

    // Error creation functions
    path_resolution_error,
    valkey_function_error,
    stream_processing_error,
    consumer_group_error,
    thread_pool_error,
    redis_error,

    // Error handling functions
    log_error,
    handle_path_resolution_error,
    handle_valkey_function_error,
    handle_stream_processing_error,
    handle_consumer_group_error,
    handle_thread_pool_error,
    handle_redis_error,

    // Error conversion functions
    redis_to_integration_error,
    geometric_to_integration_error,
    valkey_manager_poison_error,
};

// Command handler modules (decomposed into SRP-compliant submodules)
pub mod handlers;
pub mod command_handler;
pub use command_handler::{
    CommandHandlerRegistry,
    CommandResult,
    get_command_registry,
    initialize_command_registry,
    process_command_with_registry,
};

pub mod command_processor;

// Fast lane: async-spawned dispatch for Lane::Fast commands.
// Owns a shared tokio runtime + redis Client. See module doc-comment
// for the Fast vs Ordered design rationale.
pub mod fast_lane;

// Per-site rate limiting (GN-D2.03 — Tier-2 commit 2.1.c).
pub mod ratelimit;
pub use command_processor::{
    process_commands,
    process_command,
    send_command,
    send_response_with_routing,
};
pub use crate::integration::processor::unified_stream_processor::get_unified_stream;

pub use crate::integration::processor::pending_processor::{
    claim_pending_messages,
};
// Diagnostics module
pub mod diagnostics;
pub use diagnostics::{
    check_stream_consumer_status,
    check_thread_status,
    reset_consumer_group,
    debug_stream_state,
    get_consistent_consumer_name,
    ConsumerStatus,
    ThreadStatus,
    StreamInfo,
    GroupInfo,
    ConsumerInfo,
};

// Thread safety module
pub mod thread_safety;
pub use thread_safety::{
    ThreadSafeSingleton,
    ThreadSafetyError,
    ThreadSafetyResult,
    SharedMutex,
    SharedRwLock,
    SharedImmutable,
    new_shared_mutex,
    new_shared_rwlock,
    new_shared_immutable,
    with_mutex,
    with_mutex_timeout,
    with_read,
    with_write,
    with_shared_mutex,
    with_shared_read,
    with_shared_write,
    with_arc_mutex,
    with_arc_read,
    with_arc_write,
    convert_mutex_to_rwlock,
    get_thread_id,
    get_consistent_consumer_name as get_thread_consumer_name,
};

// Load metrics module for load-aware service discovery
pub mod load_metrics;
pub use load_metrics::{
    LoadMetrics,
    LoadMetricsManager,
};

// Connection manager module
pub mod connection_manager;
pub use connection_manager::{
    ConnectionManager,
    ManagedConnection,  // P3CF001: Pooled connection type for explicit type annotations
    get_connection,
    with_connection,
    with_retry_connection,
};

// Content processing modules (CMS extension)
pub mod content_minifier;
pub mod content_compressor;
pub use content_minifier::{minify_safe, MinifyStats, MinifyError};
pub use content_compressor::{compress_smart, compress_and_encode, decode_and_decompress, CompressionStats, CompressionError};

// Template rendering module (CMS extension)
pub mod template_renderer;
pub use template_renderer::{
    register_template,
    render_template,
    render_string,
    delete_template,
    invalidate_template,
    list_templates,
    get_template_metadata,
    get_template_dependencies,
    TemplateError,
};

// Relay module (inter-service message forwarding)
pub mod relay;

// Direct channel module (gNode-provisioned inter-service streams)
pub mod direct;

// Stream discovery module (topology-driven site/stream discovery)
pub mod stream_discovery;
pub use stream_discovery::{
    StreamDiscoveryManager,
    StreamDiscoveryConfig,
    DiscoveredStream,
    RegisteredSite,
    create_site_streams,
    ensure_consumer_groups,
    get_site_streams,
};

// Service discovery module (daemon-driven periodic registration from config files)
pub mod service_discovery;
pub use service_discovery::{
    ServiceDiscoveryManager,
    ServiceDiscoveryConfig,
};

/// Process node with unified stream approach
///
/// This function processes messages using the unified stream approach, with
/// optimized RESP3 protocol for efficient storage and communication.
///
/// # Arguments
///
/// * `conn` - Redis connection
/// * `node_id` - Node identifier
/// * `site_id` - Site identifier for namespacing
/// * `stream_prefix` - Stream prefix
/// * `debug_mode` - Whether debug mode is enabled
///
/// # Returns
///
/// * `IntegrationResult<usize>` - Number of processed messages or error
pub fn process_node_unified_stream(
    conn: &mut redis::Connection,
    node_id: &str,
    site_id: &str,
    stream_prefix: &str,
    debug_mode: bool
) -> IntegrationResult<usize> {
    // Get registry
    let registry = command_handler::get_command_registry();
    
    // Get unified stream
    let stream_key = processor::unified_stream_processor::get_unified_stream(site_id, stream_prefix, node_id);
    
    // Get consumer name
    let consumer_name = diagnostics::get_consistent_consumer_name(node_id, None);
    
    // Create configuration
    let config = crate::config::GNodeSettings::default();
    
    // Create state
    let mut state = consumer_groups::ConsumerGroupState::new(
        config.initial_batch_size,
        config.base_backoff_ms
    );
    
    // Get topology reference
    let topology = crate::daemon::GNodeDaemon::get_topology_ref();
    
    // Process commands
    command_processor::process_commands(
        conn,
        &topology,
        &stream_key,
        "gnode-daemon",
        &consumer_name,
        &config,
        &mut state,
        registry,
        site_id,
        debug_mode
    )
}



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_unified_stream() {
        let site_id = "test";
        let stream_prefix = "gnode";
        let node_id = "node1";
        
        let stream = processor::get_unified_stream(site_id, stream_prefix, node_id);
        assert_eq!(stream, "{test}:gnode:unified:node1");
    }

    #[test]
    fn test_current_timestamp() {
        let timestamp = current_timestamp();
        
        // Should be a positive number representing current time
        assert!(timestamp > 0.0);
        
        // Should be a recent timestamp (within last hour)
        let one_hour_ago = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs_f64() - 3600.0;
            
        assert!(timestamp > one_hour_ago);
    }
}