// Processor module for gNode
//
// This module provides processing capabilities for streams and commands,
// including the unified stream approach with optimized RESP3 protocol.

pub mod resp3_protocol;
pub mod unified_stream_processor;  // Made public to fix visibility issues
pub mod stream_utils;
pub mod stream_reader;
pub mod state_manager;
pub mod recovery_processor;
pub mod pending_processor;
// Format processor is conditional based on CMS feature flag
pub mod format_processor;

pub mod circuit_breakers;
pub use circuit_breakers::InitializationState;

pub mod health_processor;
pub use health_processor::process_health_updates;

pub mod broadcast_reader;
pub use broadcast_reader::{
    BroadcastReader,
    BroadcastMessage,
    trim_broadcast_stream,
    is_broadcast_message,
};

// Re-export main components for use in other modules
pub use stream_reader::{
    StreamReader,
    StreamReaderError,
    StreamReaderResult,
    read_multi_stream,
};
pub use resp3_protocol::{Resp3Value, OptimizedCommand};
pub use crate::config::{GNodeSettings};
pub use crate::integration::consumer_groups::{create_unified_stream_worker};
pub use unified_stream_processor::{
    initialize_unified_stream,
    get_unified_stream,
    initialize_health_stream,
    get_health_stream,
    initialize_streams,
    initialize_broadcast_stream,
    get_broadcast_stream,
};
pub use pending_processor::claim_pending_messages;
pub use stream_utils::{
    find_existing_nodes, trim_unified_stream
};
pub use recovery_processor::recover_with_client;
// Format processor exports are conditional based on CMS feature flag
pub use format_processor::{FormatProcessor, FormatProcessorError};