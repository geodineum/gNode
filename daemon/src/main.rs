use log::{info, error, warn, LevelFilter};
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::fs::OpenOptions;

use gnode::{Result, GeometricError, ThreadConfig, GNodeDaemon};
use gnode::config::{GNodeArgs, load_config};
use gnode::unified_config::{UnifiedConfig, update_config};

// Global flag for log rotation signal (USR1)
static LOG_ROTATION_REQUESTED: AtomicBool = AtomicBool::new(false);

// Log file path captured at startup; used by the SIGUSR1 rotation handler
// if it later needs to reopen the file. Set once, then immutable for the
// lifetime of the process — safe to read from any thread without locking.
static LOG_FILE_PATH: OnceLock<String> = OnceLock::new();

// ========================================
// Signal Handling for Graceful Shutdown
// ========================================

/// Set up Unix signal handlers for graceful shutdown and log rotation
/// Handles SIGTERM (systemd stop), SIGINT (Ctrl+C), SIGHUP (config reload), SIGUSR1 (log rotation)
fn setup_signal_handlers() -> Result<()> {
    use signal_hook::consts::signal::*;
    use signal_hook::iterator::Signals;

    let mut signals = Signals::new([SIGTERM, SIGINT, SIGHUP, SIGUSR1])
        .map_err(GeometricError::Io)?;

    std::thread::spawn(move || {
        for sig in signals.forever() {
            match sig {
                SIGTERM | SIGINT => {
                    info!("═══════════════════════════════════════════════════════════════");
                    info!("  SHUTDOWN SIGNAL RECEIVED (signal {})", sig);
                    info!("  Initiating graceful shutdown...");
                    info!("═══════════════════════════════════════════════════════════════");
                    gnode::daemon::request_shutdown();
                    break;
                },
                SIGHUP => {
                    info!("═══════════════════════════════════════════════════════════════");
                    info!("  SIGHUP RECEIVED - Reloading configuration...");
                    info!("═══════════════════════════════════════════════════════════════");
                    match reload_config() {
                        Ok(()) => info!("Configuration reloaded successfully"),
                        Err(e) => error!("Config reload failed: {}. Keeping previous config.", e),
                    }
                },
                SIGUSR1 => {
                    info!("Received SIGUSR1 - log rotation signal");
                    LOG_ROTATION_REQUESTED.store(true, Ordering::SeqCst);
                    // Note: With fern + copytruncate logrotate mode, explicit reopen
                    // is not strictly necessary, but we log the event for visibility
                },
                _ => {}
            }
        }
    });

    info!("Signal handlers installed (SIGTERM, SIGINT, SIGHUP, SIGUSR1)");
    Ok(())
}

/// Reload configuration from environment variables and update hot-reloadable sections
///
/// This function is called when SIGHUP is received. It:
/// 1. Re-reads environment variables (they may have changed)
/// 2. Creates a new UnifiedConfig with env overrides
/// 3. Validates the new configuration
/// 4. Updates only hot-reloadable sections (stream, consumer, performance, routing, health, features)
/// 5. Clears caches to pick up new routing/node configs
fn reload_config() -> std::result::Result<(), String> {
    // Re-load ecosystem config (disk-minimal bootstrap.env + ValKey-resident
    // tier). Single canonical route — no dotenv, no source-bootstrap.env.
    gnode::ecosystem_config::load()
        .map_err(|e| format!("ecosystem_config::load() failed during SIGHUP reload: {}", e))?;

    // Create new config with defaults and apply env overrides
    let mut new_config = UnifiedConfig::default();
    new_config.apply_env_overrides();

    // Validate the new configuration
    new_config.validate()?;

    // Update the global config (only hot-reloadable sections)
    update_config(new_config)?;

    // Clear caches to pick up new configurations
    gnode::routing_config::clear_routing_cache();
    gnode::node_config::clear_node_cache();

    info!("Caches cleared - new routing and node configs will be loaded on next access");

    Ok(())
}

/// Initialize fern logger with dual output (stdout for journald + optional file)
///
/// Log format: [TIMESTAMP LEVEL module] message
/// Example: [2026-02-02T13:21:45.123Z INFO gnode_daemon] Starting gNode service
fn setup_logger(level: LevelFilter, log_file: Option<&PathBuf>) -> std::result::Result<(), fern::InitError> {
    use chrono::Local;

    // Build base dispatch with formatting
    let mut dispatch = fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "[{} {} {}] {}",
                Local::now().format("%Y-%m-%dT%H:%M:%S%.3f"),
                record.level(),
                record.target(),
                message
            ))
        })
        .level(level)
        // Filter out noisy modules
        .level_for("rustls", LevelFilter::Warn)
        .level_for("tokio_util", LevelFilter::Warn)
        .level_for("mio", LevelFilter::Warn);

    // Always output to stdout (captured by journald when running as systemd service)
    dispatch = dispatch.chain(std::io::stdout());

    // Optionally also output to file
    if let Some(path) = log_file {
        // Record the path for the SIGUSR1 rotation handler. set() is a
        // no-op if called twice, which matches this function's invariant
        // (called once during startup before any signal thread spawns).
        let _ = LOG_FILE_PATH.set(path.to_string_lossy().to_string());

        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| {
                        eprintln!("Warning: Failed to create log directory {:?}: {}", parent, e);
                    })
                    .ok();
            }
        }

        // Open log file in append mode
        match OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
        {
            Ok(file) => {
                eprintln!("Logging to file: {:?}", path);
                dispatch = dispatch.chain(file);
            }
            Err(e) => {
                eprintln!("Warning: Failed to open log file {:?}: {}. Continuing with stdout only.", path, e);
            }
        }
    }

    dispatch.apply()?;
    Ok(())
}


/// Geodineum Service Daemon - Standalone service for gCore capability-based service discovery
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    /// Command to execute
    #[clap(subcommand)]
    command: Option<Commands>,
    
    /// ValKey host (ecosystem_config populates from disk tier)
    #[clap(long, default_value = "127.0.0.1", env = "VALKEY_HOST")]
    redis_host: String,

    /// ValKey port (standardized to 47445 for Geodineum ecosystem)
    #[clap(long, default_value = "47445", env = "VALKEY_PORT")]
    redis_port: String,

    /// ValKey username for ACL authentication (ecosystem_config may set VALKEY_USER)
    #[clap(long, env = "VALKEY_USER")]
    redis_user: Option<String>,

    /// ValKey auth password. Prefer `--redis-auth-file` (GNODE_REDIS_AUTH_FILE)
    /// for production — passing the password inline via env or flag leaves it
    /// in `/proc/<pid>/environ` + `/proc/<pid>/cmdline` where any local process
    /// running as the same uid can read it. This flag is retained for dev /
    /// test convenience; production installers set the path flag instead.
    #[clap(long, default_value = "", env = "GNODE_REDIS_AUTH", hide_env_values = true)]
    redis_auth: String,

    /// Path to a file containing the ValKey auth password. Preferred over
    /// `--redis-auth` in production. The file is read once at startup, its
    /// contents trimmed, and the password used for ValKey authentication.
    /// The path itself is not sensitive (it is just metadata); the file
    /// must be 0600 / owned by the daemon-running uid.
    ///
    /// Closes NC-D2.05: prior to this flag, gNode-Client's startDaemon path
    /// shell-interpolated the password into the child env, leaving it in
    /// `/proc/<pid>/environ`. Client now passes the path via
    /// `GNODE_REDIS_AUTH_FILE`; the daemon reads the file itself.
    ///
    /// If both `--redis-auth` and `--redis-auth-file` are set, the file
    /// wins (production-over-dev preference).
    #[clap(long, env = "GNODE_REDIS_AUTH_FILE")]
    redis_auth_file: Option<String>,
    
    /// Topology namespace - the shared namespace for service registration and discovery
    /// All services across all sites register to {topology_namespace}:gnode:topology
    /// Default: "geodineum" → creates key {geodineum}:gnode:topology
    #[clap(long, default_value = "geodineum", env = "GNODE_TOPOLOGY_NAMESPACE")]
    topology_namespace: String,

    /// Daemon name - unique identifier for this daemon instance
    /// Used for consumer group membership and node registration
    #[clap(long, default_value = "default", visible_alias = "node-id")]
    daemon_name: String,

    /// Daemon alias - human-readable name for this daemon (optional)
    /// Used for display/logging purposes only, not for identification
    #[clap(long)]
    daemon_alias: Option<String>,
    
    /// Stream prefix
    #[clap(long, default_value = "gnode", env = "GNODE_STREAM_PREFIX")]
    stream_prefix: String,

    /// DTAP environment for stream isolation
    /// - "all": Listen to ALL environments (testing, staging, acceptance, production) - RECOMMENDED
    /// - Specific environment: Listen only to that environment's streams
    ///   Default is "all" so daemon discovers and processes messages from all DTAP environments
    #[clap(long, default_value = "all",
           value_parser = ["testing", "staging", "acceptance", "production", "all"])]
    environment: String,

    /// Stream refresh interval in seconds
    /// How often the daemon re-discovers registered sites and their streams from ValKey.
    /// Lower values = faster detection of new sites, higher values = less overhead.
    /// Default: 60 seconds
    #[clap(long, default_value = "60")]
    stream_refresh_secs: u64,

    /// Node type for message routing within consumer groups
    /// Built-in types:
    /// - general: Processes all non-specialized messages (default)
    /// - inference: Only processes messages with _gh:"inference" routing hint
    /// - gpu_compute: Only processes GPU-bound messages (_gh:"gpu_compute", "tensor_ops", etc.)
    /// - all: Processes all messages regardless of routing hint
    ///   Custom types can be defined in daemon/config/nodes/*.yaml
    #[clap(long, default_value = "general")]
    node_type: String,

    /// Number of dimensions in the SERVICE TIER capability space.
    /// Default: 30 (25 discovery + 5 storage) per daemon/config/service_schema.yaml.
    /// Discovery dims 0-24 feed the spatial-hash bucket key; dims 25-29 are storage-only.
    /// Other tiers (tool/constellation/galaxy) and custom topologies (created via
    /// topo_create / gNode-TOPO) load their own dim count from their tier schema.
    #[clap(long, default_value = "30")]
    dimensions: usize,
    
    /// Number of worker threads per stream processor
    /// Use "auto" for automatic CPU-based configuration, or a specific number
    #[clap(long, default_value = "auto")]
    threads: String,
    
    /// Maximum number of threads per node (when using auto configuration)
    #[clap(long, default_value = "16")]
    max_threads: usize,
    
    /// Enable debug mode
    #[clap(long)]
    debug: bool,

    /// Run as master node (loads config from YAML files and stores to ValKey)
    /// Master nodes load node type configurations from the node config directory
    /// and store them to ValKey for worker nodes to fetch.
    /// Alternative to using --node-id=master
    #[clap(long)]
    master: bool,

    /// Directory containing node type configuration files (*.yaml)
    /// Checked in order: 1) This flag, 2) GNODE_NODE_CONFIG_DIR env var,
    /// 3) /etc/geodineum/components/gnode-daemon/nodes/, 4) daemon/config/nodes/
    #[clap(long)]
    node_config_dir: Option<PathBuf>,

    /// Run in single-threaded mode (cooperative tick-based scheduling)
    /// Instead of spawning worker threads, all workers run cooperatively
    /// in the main thread. Useful for debugging, profiling, and resource-
    /// constrained environments.
    #[clap(long)]
    single_threaded: bool,

    /// Set log level (error, warn, info, debug, trace)
    #[clap(long, default_value = "info", env = "GNODE_LOG_LEVEL")]
    log_level: String,

    /// Log file path for file-based logging (in addition to stdout/journald)
    /// If not specified, logs only go to stdout (captured by journald when running as service)
    /// Example: --log-file /var/log/geodineum/gnode/daemon.log
    #[clap(long)]
    log_file: Option<PathBuf>,

    /// Path to unified stream configuration file (YAML)
    #[clap(long)]
    stream_config: Option<PathBuf>,

    /// Base backoff time in milliseconds for stream operations
    #[clap(long)]
    base_backoff_ms: Option<u64>,

    /// Maximum backoff time in milliseconds for stream operations
    #[clap(long)]
    max_backoff_ms: Option<u64>,

    /// Initial batch size for stream operations
    #[clap(long)]
    initial_batch_size: Option<usize>,

    /// Maximum batch size for stream operations
    #[clap(long)]
    max_batch_size: Option<usize>,

    /// Minimum batch size for stream operations
    #[clap(long)]
    min_batch_size: Option<usize>,

    /// Time in milliseconds a message must be idle before being claimed
    #[clap(long)]
    idle_time_ms: Option<u64>,

    /// Stream trim interval in seconds
    #[clap(long)]
    trim_interval_secs: Option<u64>,

    /// Stream maximum length
    #[clap(long)]
    max_stream_length: Option<usize>,

    /// Use approximate trimming for stream
    #[clap(long)]
    approximate_trim: Option<bool>,

    /// Time in milliseconds between pending message checks
    #[clap(long)]
    pending_check_interval_ms: Option<u64>,

    /// Circuit breaker threshold for consecutive initialization failures
    #[clap(long)]
    circuit_breaker_threshold: Option<usize>,

    /// Circuit breaker cool-down period in seconds
    #[clap(long)]
    circuit_breaker_cooldown_secs: Option<u64>,

    /// Optional extension directory override (points at an extension repo
    /// with `extension.yaml`/`extensions.yaml` and `src/handlers/` or
    /// `functions/`). Takes precedence over GNODE_EXT_<NAME>_PATH
    /// env vars and GNODE_EXT_DIR scanning.
    #[clap(long)]
    ext_path: Option<PathBuf>,

    /// Enable automatic service discovery from YAML config files.
    /// When enabled, the daemon periodically scans geometric_topology.yaml
    /// and registers discovered services for all known sites.
    /// Local services use discovery; remote services still use stream registration.
    #[clap(long, default_value = "true", env = "GNODE_SERVICE_DISCOVERY")]
    service_discovery: bool,

    /// Service discovery scan interval in seconds.
    /// How often the daemon checks config files for changes.
    /// Only re-registers when file content actually changes (mtime-based).
    #[clap(long, default_value = "120", env = "GNODE_DISCOVERY_INTERVAL_SECS")]
    discovery_interval_secs: u64,

    /// Additional config paths for service discovery (comma-separated).
    /// Whitelisted directories or files containing gnode_services.yaml / geometric_topology.yaml.
    /// Added to the default search paths (GCORE_DIR, /opt/geodineum/gCore/config/).
    #[clap(long, env = "GNODE_DISCOVERY_CONFIG_PATHS")]
    discovery_config_paths: Option<String>,

    /// Path to discovery-paths.conf manifest file for dynamic path management.
    /// The daemon re-reads this file on each scan cycle when its mtime changes,
    /// allowing new service paths to be added without daemon restart.
    /// Format: one path per line, # comments, empty lines ignored.
    #[clap(long, env = "GNODE_DISCOVERY_PATHS_FILE")]
    discovery_paths_file: Option<String>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Start the daemon
    Start,

    /// Stop the daemon
    Stop,

    /// Check daemon status
    Status,

    /// Register tier services to the geometric topology (deploy-time)
    ///
    /// Reads geometric_topology.yaml and the active tier's schema
    /// (service_schema.yaml by default; tool_schema.yaml / constellation_schema.yaml /
    /// galaxy_schema.yaml for those tiers), translates human-readable capability
    /// names to N-D Q64.64 coordinates (N = tier's total_dimensions), and registers
    /// each entity via FCALL GNODE_REGISTER_CAPABILITY_VECTOR.
    RegisterTools {
        /// Topology tier: "service" (per-site, 30D) or "tool" (ecosystem, 16D)
        #[clap(long, default_value = "service")]
        tier: String,

        /// Target site ID (if omitted, discovers all registered sites; ignored for --tier tool)
        #[clap(long)]
        site: Option<String>,

        /// Path to config YAML (auto-detected per tier if omitted)
        #[clap(long)]
        config: Option<PathBuf>,

        /// Path to schema YAML (auto-detected per tier if omitted)
        #[clap(long)]
        schema: Option<PathBuf>,

        /// Service profile (web|headless|service|system|component): register ONE
        /// site entity from the schema's profile defaults (use with --tier service
        /// --site <id>) instead of looping geometric_topology.yaml.
        #[clap(long)]
        profile: Option<String>,

        /// DTAP environment to embed in dim-20 (testing|staging|acceptance|production).
        /// Overrides the profile default (production) so a non-prod site's geometric
        /// placement matches its active_environment. Used by `geodineum env set` to
        /// re-embed env on promotion.
        #[clap(long)]
        environment: Option<String>,

        /// Dry run — show what would be registered without writing
        #[clap(long)]
        dry_run: bool,
    },

    /// Verify a signed extension directory (extension.yaml + extension.sig)
    /// against the baked-in author key — the runtime counterpart of the
    /// build-time check, used by load-valkey-functions.sh so the loader and
    /// build.rs share ONE signing scheme. Pure crypto; no ValKey needed.
    VerifyExtension {
        /// Path to the extension directory (contains extension.yaml + .sig)
        dir: PathBuf,
    },

    /// Print the compiled command inventory as JSON (canonical descriptors +
    /// every accepted command token). The COMMAND_SCHEMA.md drift-checker
    /// (scripts/check-command-schema.sh) diffs this against the doc so the
    /// same binary that serves the commands defines what's documented.
    /// Reflects the build: base → 60 commands; built with the CMS extension
    /// staged into GNODE_EXT_DIR → 83. Offline; no ValKey needed.
    DumpSchema,
}

// Main entry point
fn main() -> Result<()> {
    // Parse CLI early. `verify-extension` is a pure signed-manifest check (no
    // ValKey) — handle it before ecosystem_config::load() so the Lua loader can
    // call it without daemon credentials.
    let mut cli = Cli::parse();
    if let Some(Commands::VerifyExtension { dir }) = &cli.command {
        match gnode::ext_verify::verify_extension(dir) {
            Ok(name) => {
                println!("verified extension '{}'", name);
                std::process::exit(0);
            }
            Err(e) => {
                eprintln!("verify-extension failed: {}", e);
                std::process::exit(1);
            }
        }
    }
    // dump-schema is a pure in-memory registry serialization (no ValKey, no
    // config) — handle it here alongside verify-extension so the doc checker
    // can run against a bare binary.
    if let Some(Commands::DumpSchema) = &cli.command {
        let registry = gnode::integration::CommandHandlerRegistry::new();
        match serde_json::to_string_pretty(&registry.command_inventory()) {
            Ok(json) => {
                println!("{}", json);
                std::process::exit(0);
            }
            Err(e) => {
                eprintln!("dump-schema failed to serialize: {}", e);
                std::process::exit(1);
            }
        }
    }

    // Load ecosystem config: disk-minimal bootstrap.env (3 keys, strict
    // ownership/mode/whitelist parse) followed by ValKey-resident tier
    // (geodineum:bootstrap:_index). Single canonical route — no dotenv,
    // no systemd EnvironmentFile= (the daemon binary self-loads).
    if let Err(e) = gnode::ecosystem_config::load() {
        eprintln!("FATAL: ecosystem_config::load() failed: {}", e);
        eprintln!("       Ensure /etc/geodineum/bootstrap.env exists with");
        eprintln!("       root 0640/0600 ownership and the 3 required keys");
        eprintln!("       (VALKEY_HOST, VALKEY_PORT, VALKEY_CREDS_PATH).");
        std::process::exit(1);
    }

    // cli parsed BEFORE ecosystem_config::load() (verify-extension needs it
    // credential-free), so clap's env-backed args captured the environment
    // from before bootstrap.env was loaded. Re-apply the canonical ValKey
    // vars here or a constellation worker silently dials clap's 127.0.0.1
    // default instead of the master (connection-refused crash loop on join;
    // masked on the master where 127.0.0.1 is correct). The systemd unit
    // passes no --redis-* flags, so env-over-flag precedence is not a
    // concern in practice.
    if let Ok(h) = std::env::var("VALKEY_HOST") {
        cli.redis_host = h;
    }
    if let Ok(p) = std::env::var("VALKEY_PORT") {
        cli.redis_port = p;
    }
    if cli.redis_user.is_none() {
        if let Ok(u) = std::env::var("VALKEY_USER") {
            cli.redis_user = Some(u);
        }
    }

    // Initialize logger with the specified log level
    let log_level = match cli.log_level.to_lowercase().as_str() {
        "error" => LevelFilter::Error,
        "warn" | "warning" => LevelFilter::Warn,
        "info" => LevelFilter::Info,
        "debug" => LevelFilter::Debug,
        "trace" => LevelFilter::Trace,
        _ => {
            eprintln!("Warning: Invalid log level '{}', defaulting to 'info'", cli.log_level);
            LevelFilter::Info
        }
    };

    // Initialize fern logger with dual output (stdout + optional file)
    setup_logger(log_level, cli.log_file.as_ref())
        .expect("Failed to initialize logger");
    
    // NC-D2.05: resolve ValKey auth password. `--redis-auth-file` wins over
    // `--redis-auth` when both are set. File path is just metadata; reading
    // it here (in-process) keeps the secret off the command line and out of
    // the child env.
    let redis_auth = if let Some(path) = cli.redis_auth_file.as_ref() {
        match std::fs::read_to_string(path) {
            Ok(contents) => {
                let trimmed = contents.trim().to_string();
                if trimmed.is_empty() {
                    eprintln!(
                        "FATAL: --redis-auth-file {} exists but yielded empty password after trim",
                        path
                    );
                    std::process::exit(1);
                }
                trimmed
            }
            Err(e) => {
                eprintln!(
                    "FATAL: --redis-auth-file {} could not be read: {}. \
                     Check the file exists, is readable by the daemon uid, \
                     and contains the ValKey password.",
                    path, e
                );
                std::process::exit(1);
            }
        }
    } else {
        cli.redis_auth.clone()
    };

    // Construct Redis URL with properly encoded password and optional username
    let redis_url = if redis_auth.is_empty() {
        format!("redis://{}:{}", cli.redis_host, cli.redis_port)
    } else {
        // URL-encode the password to handle special characters like / and =
        let encoded_password = urlencoding::encode(&redis_auth);

        // Support both ACL (username:password) and requirepass (password only) authentication
        if let Some(username) = &cli.redis_user {
            let encoded_username = urlencoding::encode(username);
            format!("redis://{}:{}@{}:{}", encoded_username, encoded_password, cli.redis_host, cli.redis_port)
        } else {
            format!("redis://:{}@{}:{}", encoded_password, cli.redis_host, cli.redis_port)
        }
    };
    
    // Determine master status: explicit --master flag OR --daemon-name=master
    let is_master = cli.master || cli.daemon_name == "master";

    // Compute display name (alias if provided, otherwise daemon_name)
    let display_name = cli.daemon_alias.as_ref()
        .map(|a| format!("{} ({})", a, cli.daemon_name))
        .unwrap_or_else(|| cli.daemon_name.clone());

    // Log configuration
    info!("Starting gNode service with configuration:");
    info!("  Redis URL: {}", redis_url.replace(&redis_auth, "***"));
    info!("  Topology Namespace: {} → {{{}}}:gnode:topology", cli.topology_namespace, cli.topology_namespace);
    info!("  Environment: {}", cli.environment);
    info!("  Daemon Name: {}", display_name);
    info!("  Master mode: {} {}", is_master, if cli.master { "(--master flag)" } else if is_master { "(--daemon-name=master)" } else { "" });
    info!("  Node Type: {} (message routing filter)", cli.node_type);
    info!("  Stream prefix: {}", cli.stream_prefix);
    info!("  Dimensions: {}", cli.dimensions);
    info!("  Debug mode: {}", cli.debug);
    info!("  Log level: {}", cli.log_level);
    info!("  Single-threaded mode: {}", cli.single_threaded);
    info!("  Stream discovery: DYNAMIC (sites discovered from topology)");

    // Initialize extension manager (discovers optional extensions from
    // --ext-path, GNODE_EXT_<NAME>_PATH, and GNODE_EXT_DIR). GN-D3.06:
    // surface double-init as an explicit error rather than silently
    // swallowing — without this, an earlier get_extension_manager() call
    // could lock the manager to discover(None) and the operator's
    // --ext-path override would be silently dropped.
    let ext_path_str = cli.ext_path.as_ref().map(|p| p.to_string_lossy().to_string());
    if let Err(e) = gnode::extensions::initialize_extension_manager(ext_path_str.as_deref()) {
        warn!("Extension manager init returned: {} — using whatever was previously set", e);
    }

    // FUNCTION LOAD REPLACE each operational extension's Lua libraries into
    // ValKey. Best-effort — the installer's scripts/load-valkey-functions.sh
    // is the primary loader (runs as admin); this is the safety net when
    // daemon ACL allows FUNCTION LOAD (e.g., after extension hot-swap via
    // SIGHUP). Failures are logged, not fatal.
    match redis::Client::open(redis_url.clone())
        .and_then(|c| c.get_connection_with_timeout(std::time::Duration::from_secs(5)))
    {
        Ok(mut conn) => {
            let (loaded, failed) =
                gnode::extensions::load_lua_libraries_into_valkey(&mut conn);
            if failed > 0 && loaded == 0 {
                warn!(
                    "No extension Lua libraries loaded ({} failed). The installer's \
                     scripts/load-valkey-functions.sh may need to run as admin.",
                    failed
                );
            }
        }
        Err(e) => {
            warn!(
                "Skipping extension FUNCTION LOAD: cannot reach ValKey for pre-flight load: {}",
                e
            );
        }
    }

    match cli.command {
        Some(Commands::Start) => {
            info!("Starting daemon...");
            start_daemon(&redis_url, cli.dimensions, &cli.environment, &cli.daemon_name, &cli.node_type, &cli.stream_prefix, &cli.threads, cli.max_threads, cli.debug, &cli.log_level, is_master, &cli)?;
        },
        Some(Commands::Stop) => {
            info!("Stopping daemon...");
            stop_daemon(&redis_url, &cli.environment, &cli.stream_prefix)?;
        },
        Some(Commands::Status) => {
            info!("Checking daemon status...");
            check_daemon_status(&redis_url, &cli.environment, &cli.stream_prefix)?;
        },
        Some(Commands::RegisterTools { tier, site, config, schema, profile, environment, dry_run }) => {
            info!("Registering {} tier...", tier);
            gnode::tool_registration::run(gnode::tool_registration::RegisterToolsArgs {
                site,
                config_path: config,
                schema_path: schema,
                dry_run,
                redis_url: redis_url.clone(),
                topology_namespace: cli.topology_namespace.clone(),
                tier,
                profile,
                environment,
            })?;
        },
        Some(Commands::VerifyExtension { .. }) => {
            // Handled early (before ecosystem_config::load()); never reached here.
            unreachable!("verify-extension is dispatched before the ValKey load");
        },
        Some(Commands::DumpSchema) => {
            // Handled early (before ecosystem_config::load()); never reached here.
            unreachable!("dump-schema is dispatched before the ValKey load");
        },
        None => {
            // Default command is to start the daemon
            info!("Starting daemon (default command)...");
            start_daemon(&redis_url, cli.dimensions, &cli.environment, &cli.daemon_name, &cli.node_type, &cli.stream_prefix, &cli.threads, cli.max_threads, cli.debug, &cli.log_level, is_master, &cli)?;
        }
    }
    
    Ok(())
}

// Function to start the daemon
#[allow(clippy::too_many_arguments)]
fn start_daemon(redis_url: &str, dimensions: usize, environment: &str, daemon_name: &str, node_type: &str, stream_prefix: &str, threads: &str, max_threads: usize, debug: bool, log_level: &str, is_master: bool, cli: &Cli) -> Result<()> {
    // Parse thread configuration
    let thread_config = match threads {
        "auto" => {
            // Automatic configuration based on CPU cores
            let num_cores = num_cpus::get();
            let thread_count = std::cmp::min(num_cores, max_threads);
            info!("Auto-configuring with {} worker threads per node (based on {} CPU cores, max: {})",
                  thread_count, num_cores, max_threads);
            ThreadConfig::Auto(thread_count)
        },
        _ => {
            // Parse specific thread count
            match threads.parse::<usize>() {
                Ok(count) if count > 0 => {
                    info!("Using {} worker threads per node as specified", count);
                    ThreadConfig::Fixed(count)
                },
                _ => {
                    warn!("Invalid thread count: '{}', falling back to auto configuration", threads);
                    let num_cores = num_cpus::get();
                    let thread_count = std::cmp::min(num_cores, max_threads);
                    info!("Auto-configuring with {} worker threads per node (based on {} CPU cores, max: {})", 
                          thread_count, num_cores, max_threads);
                    ThreadConfig::Auto(thread_count)
                }
            }
        }
    };

    // Load unified stream configuration
    let stream_args = GNodeArgs {
        base_backoff_ms: cli.base_backoff_ms,
        max_backoff_ms: cli.max_backoff_ms,
        initial_batch_size: cli.initial_batch_size,
        max_batch_size: cli.max_batch_size,
        min_batch_size: cli.min_batch_size,
        idle_time_ms: cli.idle_time_ms,
        trim_interval_secs: cli.trim_interval_secs,
        max_stream_length: cli.max_stream_length,
        approximate_trim: cli.approximate_trim,
        pending_check_interval_ms: cli.pending_check_interval_ms,
        circuit_breaker_threshold: cli.circuit_breaker_threshold,
        circuit_breaker_cooldown_secs: cli.circuit_breaker_cooldown_secs,
        stream_refresh_secs: Some(cli.stream_refresh_secs),
    };

    let config_from_file = match load_config(cli.stream_config.as_ref(), &stream_args) {
        Ok(config) => {
            info!("Loaded unified stream configuration");
            config
        },
        Err(e) => {
            warn!("Failed to load unified stream configuration: {}. Using defaults.", e);
            gnode::config::GNodeSettings::default()
        }
    };
    
    // Create daemon with the specified log level and stream config
    // Note: daemon_name is passed to GNodeDaemon as node_id internally (for backward compatibility)
    // topology_namespace creates the shared topology key {topology_namespace}:gnode:topology
    // Site streams are discovered dynamically via StreamDiscoveryManager
    let mut daemon = GNodeDaemon::new_with_config(
        redis_url,
        dimensions,
        cli.topology_namespace.clone(),  // Shared topology namespace (all services register here)
        environment.to_string(),
        daemon_name.to_string(),  // daemon_name used as node_id internally
        node_type.to_string(),
        stream_prefix.to_string(),
        debug,
        log_level,
        is_master,
        config_from_file
    )?;
    
    // Set thread configuration
    daemon.set_thread_config(thread_config);

    // Set single-threaded mode if requested
    daemon.set_single_threaded(cli.single_threaded);

    // Set node config directory if provided (uses fallback chain otherwise)
    daemon.set_node_config_dir(cli.node_config_dir.as_ref().map(|p| p.to_string_lossy().to_string()));

    // Configure service discovery from CLI flags
    daemon.set_service_discovery_config(
        cli.service_discovery,
        cli.discovery_interval_secs,
        cli.discovery_config_paths.clone(),
        cli.discovery_paths_file.clone(),
    );

    // Set up signal handlers for graceful shutdown BEFORE running daemon
    setup_signal_handlers()?;

    // Run daemon with integration module
    daemon.run()
}

// Thread configuration is now re-exported from lib.rs

// Cursor-iterated SCAN (KEYS is ACL-denied for the daemon tier on worker
// nodes, and a single SCAN batch is partial by design).
fn scan_keys(conn: &mut redis::Connection, pattern: &str) -> Result<Vec<String>> {
    let mut keys = Vec::new();
    let mut cursor: u64 = 0;
    loop {
        let (next, batch): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(pattern)
            .arg("COUNT")
            .arg(500)
            .query(conn)
            .map_err(GeometricError::Redis)?;
        keys.extend(batch);
        cursor = next;
        if cursor == 0 {
            break;
        }
    }
    Ok(keys)
}

// Function to stop the daemon (will send a signal via Redis)
// Note: Daemon is now site-agnostic, searches for all daemon PIDs in the environment
fn stop_daemon(redis_url: &str, environment: &str, stream_prefix: &str) -> Result<()> {
    // Connect to Redis
    let client = redis::Client::open(redis_url)
        .map_err(GeometricError::Redis)?;

    let mut conn = client.get_connection()
        .map_err(GeometricError::Redis)?;

    // Check if daemon is running by querying PID keys (search all sites in environment)
    // Pattern: {*}:gnode:{environment}:daemon:pid:* or gnode:daemon:pid:{environment}:*
    let pattern = format!("{}:daemon:pid:{}:*", stream_prefix, environment);
    let pid_keys: Vec<String> = scan_keys(&mut conn, &pattern)?;

    if pid_keys.is_empty() {
        info!("No running daemons found");
        return Ok(());
    }

    // Stop each daemon
    for key in pid_keys {
        // Get PID
        let pid: String = redis::cmd("GET")
            .arg(&key)
            .query(&mut conn)
            .map_err(GeometricError::Redis)?;

        info!("Stopping daemon with PID {}", pid);

        // Send SIGTERM signal
        if cfg!(unix) {
            use std::process::Command;
            let output = Command::new("kill")
                .arg(&pid)
                .output()
                .map_err(GeometricError::Io)?;

            if !output.status.success() {
                let error = String::from_utf8_lossy(&output.stderr);
                error!("Failed to stop daemon: {}", error);
            } else {
                // Delete PID key
                let _: () = redis::cmd("DEL")
                    .arg(&key)
                    .query(&mut conn)
                    .map_err(GeometricError::Redis)?;

                info!("Daemon stopped successfully");
            }
        } else {
            error!("Stopping daemons is only supported on Unix-like systems");
        }
    }

    Ok(())
}

// Function to check daemon status
// Note: Daemon is now site-agnostic, discovers all active streams
fn check_daemon_status(redis_url: &str, environment: &str, stream_prefix: &str) -> Result<()> {
    // Connect to Redis
    let client = redis::Client::open(redis_url)
        .map_err(GeometricError::Redis)?;

    let mut conn = client.get_connection()
        .map_err(GeometricError::Redis)?;

    // Check if daemon is running by querying PID keys
    let pattern = format!("{}:daemon:pid:{}:*", stream_prefix, environment);
    let pid_keys: Vec<String> = scan_keys(&mut conn, &pattern)?;

    if pid_keys.is_empty() {
        info!("No running daemons found");
        return Ok(());
    }

    // Check each daemon
    for key in pid_keys {
        // Get PID
        let pid: String = redis::cmd("GET")
            .arg(&key)
            .query(&mut conn)
            .map_err(GeometricError::Redis)?;

        // Extract node ID from key
        let parts: Vec<&str> = key.split(':').collect();
        let node_id = parts.last().unwrap_or(&"unknown");

        // Check if process is running
        if cfg!(unix) {
            use std::process::Command;
            let output = Command::new("ps")
                .arg("-p")
                .arg(&pid)
                .arg("-o")
                .arg("pid,cmd,etime")
                .output()
                .map_err(GeometricError::Io)?;

            if !output.status.success() || output.stdout.len() <= 10 {
                info!("Daemon for node {} (PID {}) is not running", node_id, pid);
            } else {
                let output_str = String::from_utf8_lossy(&output.stdout);
                info!("Daemon for node {} is running:", node_id);
                info!("{}", output_str);
            }
        } else {
            info!("Daemon for node {} has PID {}", node_id, pid);
            info!("Process status check not available on this platform");
        }
    }

    // Discover the site streams that actually exist.
    //
    // The pattern here was `*:{prefix}:{env}:unified`, with the last two
    // segments transposed relative to what every producer writes
    // (`{site_id}:gnode:unified:{env}` — gnode_site.lua:935). It therefore
    // matched nothing, and status reported "Found 0 active command streams"
    // on an instance carrying 19 of them. Silent because an empty match is
    // indistinguishable from an idle system.
    //
    // SCAN rather than KEYS: KEYS is O(N) and blocks the server for the whole
    // keyspace. Same fix already applied in
    // integration/processor/stream_utils.rs.
    let stream_pattern = format!("*:{}:unified:{}", stream_prefix, environment);
    let mut cursor = "0".to_string();
    let mut streams: Vec<String> = Vec::new();
    loop {
        let scan_result: redis::RedisResult<(String, Vec<String>)> = redis::cmd("SCAN")
            .arg(&cursor)
            .arg("MATCH")
            .arg(&stream_pattern)
            .arg("COUNT")
            .arg(100)
            .query(&mut conn);
        match scan_result {
            Ok((next, batch)) => {
                streams.extend(batch);
                cursor = next;
                if cursor == "0" { break; }
            },
            Err(e) => return Err(GeometricError::Redis(e)),
        }
    }
    streams.sort();

    info!("Found {} active command streams:", streams.len());
    for stream in streams {
        let parts: Vec<&str> = stream.split(':').collect();
        let site_id = if !parts.is_empty() { parts[0].trim_matches(|c| c == '{' || c == '}') } else { "unknown" };

        // Get stream length
        let len: i64 = redis::cmd("XLEN")
            .arg(&stream)
            .query(&mut conn)
            .map_err(GeometricError::Redis)?;

        info!("  Stream for site {}: {} messages pending", site_id, len);
    }

    Ok(())
}