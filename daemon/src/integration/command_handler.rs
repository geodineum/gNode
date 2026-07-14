// Command Handler Registry for gNode Daemon
//
// This module implements a registry pattern for command handlers, allowing
// dynamically mapping command names to handler functions. It provides a consistent
// interface for processing different command types while allowing the codebase
// to be extended with new commands.
//
// Command handlers follow a standard pattern:
// 1. Validate command parameters
// 2. Execute the command logic
// 3. Return a structured CommandResult
//
// The registry manages all available command handlers and provides a clean interface
// for retrieving and executing the appropriate handler for a given command.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use redis::Connection;
use log::{debug, warn};

use crate::daemon::Command;
use crate::GeometricTopology;
use crate::integration::IntegrationResult;
use crate::integration::send_response_with_routing;

// Import and re-export shared types from the decomposed handlers module.
// pub use ensures the existing public API (e.g., command_handler::CommandResult) is preserved.
pub use super::handlers::types::*;

/// Command handler registry supporting both sync and async handlers
pub struct CommandHandlerRegistry {
    handlers: HashMap<String, CommandHandlerFn>,
    async_handlers: HashMap<String, AsyncCommandHandlerFn>,
    descriptors: HashMap<String, CommandDescriptor>,
}

impl Default for CommandHandlerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl CommandHandlerRegistry {
    /// Create a new command handler registry with default handlers
    pub fn new() -> Self {
        let mut handlers = HashMap::new();
        let mut async_handlers = HashMap::new();
        let mut desc_vec: Vec<CommandDescriptor> = Vec::new();

        // ---- Base handlers (always registered) ----
        super::handlers::system::register(&mut handlers, &mut async_handlers, &mut desc_vec);
        super::handlers::stream::register(&mut handlers, &mut async_handlers, &mut desc_vec);
        super::handlers::config::register(&mut handlers, &mut async_handlers, &mut desc_vec);
        super::handlers::diagnostic::register(&mut handlers, &mut async_handlers, &mut desc_vec);
        super::handlers::geometric::register(&mut handlers, &mut async_handlers, &mut desc_vec);
        super::handlers::service::register(&mut handlers, &mut async_handlers, &mut desc_vec);
        super::handlers::introspection::register(&mut handlers, &mut async_handlers, &mut desc_vec);
        super::handlers::topology_custom::register(&mut handlers, &mut async_handlers, &mut desc_vec);
        super::handlers::topology_unified::register(&mut handlers, &mut async_handlers, &mut desc_vec);
        super::handlers::direct_channel::register(&mut handlers, &mut async_handlers, &mut desc_vec);
        super::handlers::relay_ops::register(&mut handlers, &mut async_handlers, &mut desc_vec);

        // CMS handlers (content/format/template/asset) and all
        // other extension handlers come via the signed-extension path
        // exclusively. They're discovered at build time from
        // GNODE_EXT_DIR, verified against AUTHOR_PUBKEY, and
        // dispatched here via register_signed_extensions() below.
        // Previously, a `cfg(feature = "cms")` block here directly invoked
        // super::handlers::{content,format,template,asset}::register —
        // those modules don't exist in gNode core (they live in
        // gNode-CMS extension repo and get staged into OUT_DIR by
        // build.rs::verify_and_stage). The direct calls only worked
        // when the now-removed `cms` Cargo feature was active.

        // ---- Signed extensions (build.rs codegen, closes GN-D1.01) ----
        // register_signed_extensions() is emitted by build.rs into
        // OUT_DIR/ext_handlers.rs and include!()'d by
        // integration::handlers::mod. It invokes register() on every
        // verified extension's staged handler modules. Before Commit 0.2
        // the include! expansion was a no-op dead wire; now it dispatches.
        let before = handlers.len();
        super::handlers::register_signed_extensions(
            &mut handlers,
            &mut async_handlers,
            &mut desc_vec,
        );
        let added = handlers.len() - before;
        if added > 0 {
            debug!("Signed extensions registered: {} command(s)", added);
        }

        // Convert descriptor Vec to HashMap keyed by lowercase name for case-insensitive lookup
        let descriptors = desc_vec.into_iter()
            .map(|d| (d.name.to_lowercase(), d))
            .collect();

        Self { handlers, async_handlers, descriptors }
    }
    
    /// Get a handler for a command
    pub fn get_handler(&self, command_name: &str) -> Option<&CommandHandlerFn> {
        self.handlers.get(command_name)
    }
    
    /// Add a new handler to the registry
    pub fn register_handler(&mut self, command_name: &str, handler: CommandHandlerFn) {
        self.handlers.insert(command_name.to_string(), handler);
    }
    
    /// Get all registered command names
    pub fn get_command_names(&self) -> Vec<String> {
        self.handlers.keys().cloned().collect()
    }

    //-------------------------------------------------------------------------
    // Async Handler Methods (Phase 1: Async Architecture)
    //-------------------------------------------------------------------------

    /// Get an async handler for a command (preferred over sync)
    pub fn get_async_handler(&self, command_name: &str) -> Option<&AsyncCommandHandlerFn> {
        self.async_handlers.get(command_name)
    }

    /// Register a new async handler
    pub fn register_async_handler(&mut self, command_name: &str, handler: AsyncCommandHandlerFn) {
        self.async_handlers.insert(command_name.to_string(), handler);
    }

    /// Check if command has an async handler (for routing decisions)
    pub fn has_async(&self, command_name: &str) -> bool {
        self.async_handlers.contains_key(command_name)
    }

    /// Get all async command names
    pub fn get_async_command_names(&self) -> Vec<String> {
        self.async_handlers.keys().cloned().collect()
    }

    //-------------------------------------------------------------------------
    // Descriptor Methods (Command Schema Autodocumentation)
    //-------------------------------------------------------------------------

    /// Get a command descriptor by name (case-insensitive)
    pub fn get_descriptor(&self, name: &str) -> Option<&CommandDescriptor> {
        self.descriptors.get(&name.to_lowercase())
    }

    /// Get the execution lane declared for a command.
    ///
    /// Returns the lane from the registered CommandDescriptor, or
    /// `Lane::Fast` as a safe default if the command has no descriptor
    /// (defensive — every base-catalog command HAS a descriptor; this
    /// only matters for signed extensions that may register handlers
    /// without descriptors).
    ///
    /// Dispatchers use this to route messages: Fast commands get
    /// tokio::spawn'd via the async handler path; Ordered commands
    /// run synchronously inline. See `handlers::types::Lane` for the
    /// full semantic contract.
    pub fn get_lane(&self, command_name: &str) -> Lane {
        self.get_descriptor(command_name)
            .map(|d| d.lane)
            .unwrap_or(Lane::Fast)
    }

    /// Get all descriptors grouped by category
    pub fn get_descriptors_by_category(&self) -> HashMap<String, Vec<&CommandDescriptor>> {
        let mut by_cat: HashMap<String, Vec<&CommandDescriptor>> = HashMap::new();
        for desc in self.descriptors.values() {
            by_cat.entry(desc.category.to_string())
                .or_default()
                .push(desc);
        }
        // Sort within each category by name for deterministic output
        for descs in by_cat.values_mut() {
            descs.sort_by_key(|d| d.name);
        }
        by_cat
    }

    /// Get all descriptor names (sorted)
    pub fn get_descriptor_names(&self) -> Vec<&str> {
        let mut names: Vec<&str> = self.descriptors.values().map(|d| d.name).collect();
        names.sort();
        names
    }

    /// Machine-readable inventory of the compiled command surface.
    ///
    /// Emitted by `gnode-daemon dump-schema` and consumed by the
    /// COMMAND_SCHEMA.md drift-checker (`scripts/check-command-schema.sh`):
    /// the canonical descriptors (name/category/lane/async) plus the full set
    /// of accepted command tokens (canonical names + every registered alias,
    /// upper/lower/camel). What's compiled in defines the inventory, so the
    /// checker fails loudly the moment a `descriptors.push(...)` or an
    /// `insert(alias)` lands without a matching doc row.
    pub fn command_inventory(&self) -> serde_json::Value {
        let mut commands: Vec<&CommandDescriptor> = self.descriptors.values().collect();
        commands.sort_by_key(|d| d.name);
        let commands: Vec<serde_json::Value> = commands.iter().map(|d| serde_json::json!({
            "name": d.name,
            "category": d.category,
            "async_capable": d.async_capable,
            "lane": d.lane,
        })).collect();

        // Accepted tokens = union of both dispatch maps. Lookup is
        // case-sensitive (handlers.get(name), no folding) and some aliases are
        // registered only on the async side (e.g. CONFIG_GET), so a token the
        // daemon actually accepts may live in either map.
        let mut tokens: Vec<&str> = self.handlers.keys()
            .chain(self.async_handlers.keys())
            .map(String::as_str)
            .collect();
        tokens.sort_unstable();
        tokens.dedup();

        serde_json::json!({
            "command_count": commands.len(),
            "commands": commands,
            "tokens": tokens,
        })
    }

}

// Create a static registry instance for global access using thread-safe patterns
use std::sync::OnceLock;

static REGISTRY: OnceLock<CommandHandlerRegistry> = OnceLock::new();

/// Initialize the global command handler registry
pub fn initialize_command_registry() {
    let _ = REGISTRY.set(CommandHandlerRegistry::new());
}

/// Get a reference to the global command handler registry
pub fn get_command_registry() -> &'static CommandHandlerRegistry {
    REGISTRY.get_or_init(CommandHandlerRegistry::new)
}

/// Process a command with the registry
///
/// This function processes a command using the command registry,
/// providing a standardized interface for command execution.
pub fn process_command_with_registry(
    command: &Command,
    conn: &mut Connection,
    topology: &Arc<RwLock<GeometricTopology>>,
    site_id: &str,
    debug_mode: bool
) -> CommandResult {
    let registry = get_command_registry();
    let handler_opt = registry.get_handler(&command.command);

    match handler_opt {
        Some(handler) => {
            if debug_mode {
                debug!("Executing handler for command: {}", command.command);
            }
            handler(command, conn, topology, site_id, debug_mode)
        },
        None => {
            unknown_command_error(&command.command)
        }
    }
}

/// Return a generic error for an unregistered command.
pub fn unknown_command_error(command_name: &str) -> CommandResult {
    warn!("No handler found for command: {}", command_name);
    CommandResult::error(format!("Unknown command: {}", command_name))
}

/// Process a command using the unified stream approach
///
/// This function processes a command and sends the response
/// to the unified stream.
pub fn process_command_unified_stream(
    conn: &mut Connection,
    topology: &Arc<RwLock<GeometricTopology>>,
    command: &Command,
    stream_key: &str,
    site_id: &str,
    debug_mode: bool
) -> IntegrationResult<String> {
    let result = process_command_with_registry(command, conn, topology, site_id, debug_mode);
    let response = result.to_response(&command.id);

    send_response_with_routing(
        conn,
        &response,
        stream_key,
        site_id,     // source_site
        "daemon",    // source_node
        &command.site_id,  // dest_site
        &command.node_id,  // dest_node
        site_id,     // site_id for namespacing
        debug_mode
    )
}

