use std::env;
use env_logger;
use log::{info, error};
use clap::{Parser, Subcommand};

use gsd::{Result, GeometricError};
use gsd::daemon::GSDaemon;

/// Geometric Service Daemon - Standalone service for nCore capability-based service discovery
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    /// Command to execute
    #[clap(subcommand)]
    command: Option<Commands>,
    
    /// Redis host
    #[clap(long, default_value = "127.0.0.1")]
    redis_host: String,
    
    /// Redis port
    #[clap(long, default_value = "6379")]
    redis_port: String,
    
    /// Redis auth
    #[clap(long, default_value = "")]
    redis_auth: String,
    
    /// Site ID
    #[clap(long, default_value = "default")]
    site_id: String,
    
    /// Node ID
    #[clap(long, default_value = "default")]
    node_id: String,
    
    /// Stream prefix
    #[clap(long, default_value = "gsd")]
    stream_prefix: String,
    
    /// Number of dimensions in capability space
    #[clap(long, default_value = "8")]
    dimensions: usize,
    
    /// Enable debug mode
    #[clap(long)]
    debug: bool,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Start the daemon
    Start,
    
    /// Stop the daemon
    Stop,
    
    /// Check daemon status
    Status,
}

// Main entry point
fn main() -> Result<()> {
    // Load .env file if it exists
    let _ = dotenv::dotenv();
    
    // Initialize logger
    env_logger::init();
    
    // Parse command line arguments
    let cli = Cli::parse();
    
    // Construct Redis URL
    let redis_url = if cli.redis_auth.is_empty() {
        format!("redis://{}:{}", cli.redis_host, cli.redis_port)
    } else {
        format!("redis://:{}@{}:{}", cli.redis_auth, cli.redis_host, cli.redis_port)
    };
    
    // Log configuration
    info!("Starting GSD service with configuration:");
    info!("  Redis URL: {}", redis_url.replace(&cli.redis_auth, "***"));
    info!("  Site ID: {}", cli.site_id);
    info!("  Node ID: {}", cli.node_id);
    info!("  Stream prefix: {}", cli.stream_prefix);
    info!("  Dimensions: {}", cli.dimensions);
    info!("  Debug mode: {}", cli.debug);
    
    match cli.command {
        Some(Commands::Start) => {
            info!("Starting daemon...");
            start_daemon(&redis_url, cli.dimensions, &cli.site_id, &cli.stream_prefix, cli.debug)?;
        },
        Some(Commands::Stop) => {
            info!("Stopping daemon...");
            stop_daemon(&redis_url, &cli.site_id, &cli.stream_prefix)?;
        },
        Some(Commands::Status) => {
            info!("Checking daemon status...");
            check_daemon_status(&redis_url, &cli.site_id, &cli.stream_prefix)?;
        },
        None => {
            // Default command is to start the daemon
            info!("Starting daemon (default command)...");
            start_daemon(&redis_url, cli.dimensions, &cli.site_id, &cli.stream_prefix, cli.debug)?;
        }
    }
    
    Ok(())
}

// Function to start the daemon
fn start_daemon(redis_url: &str, dimensions: usize, site_id: &str, stream_prefix: &str, debug: bool) -> Result<()> {
    // Create daemon
    let daemon = GSDaemon::new(
        redis_url, 
        dimensions, 
        site_id.to_string(), 
        stream_prefix.to_string(), 
        debug
    )?;
    
    // Run daemon
    daemon.run()
}

// Function to stop the daemon (will send a signal via Redis)
fn stop_daemon(redis_url: &str, site_id: &str, stream_prefix: &str) -> Result<()> {
    // Connect to Redis
    let client = redis::Client::open(redis_url)
        .map_err(GeometricError::Redis)?;
    
    let mut conn = client.get_connection()
        .map_err(GeometricError::Redis)?;
    
    // Check if daemon is running by querying PID keys
    let pattern = format!("{{{0}}}:{1}:daemon:pid:*", site_id, stream_prefix);
    let pid_keys: Vec<String> = redis::cmd("KEYS")
        .arg(&pattern)
        .query(&mut conn)
        .map_err(GeometricError::Redis)?;
    
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
                .map_err(|e| GeometricError::Io(e))?;
            
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
fn check_daemon_status(redis_url: &str, site_id: &str, stream_prefix: &str) -> Result<()> {
    // Connect to Redis
    let client = redis::Client::open(redis_url)
        .map_err(GeometricError::Redis)?;
    
    let mut conn = client.get_connection()
        .map_err(GeometricError::Redis)?;
    
    // Check if daemon is running by querying PID keys
    let pattern = format!("{{{0}}}:{1}:daemon:pid:*", site_id, stream_prefix);
    let pid_keys: Vec<String> = redis::cmd("KEYS")
        .arg(&pattern)
        .query(&mut conn)
        .map_err(GeometricError::Redis)?;
    
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
        let node_id = if parts.len() >= 4 { parts[3] } else { "unknown" };
        
        // Check if process is running
        if cfg!(unix) {
            use std::process::Command;
            let output = Command::new("ps")
                .arg("-p")
                .arg(&pid)
                .arg("-o")
                .arg("pid,cmd,etime")
                .output()
                .map_err(|e| GeometricError::Io(e))?;
            
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
    
    // Also check active streams
    let stream_pattern = format!("{{{0}}}:{1}:stream:*:commands", site_id, stream_prefix);
    let streams: Vec<String> = redis::cmd("KEYS")
        .arg(&stream_pattern)
        .query(&mut conn)
        .map_err(GeometricError::Redis)?;
    
    info!("Found {} active command streams:", streams.len());
    for stream in streams {
        let parts: Vec<&str> = stream.split(':').collect();
        let node_id = if parts.len() >= 4 { parts[3] } else { "unknown" };
        
        // Get stream length
        let len: i64 = redis::cmd("XLEN")
            .arg(&stream)
            .query(&mut conn)
            .map_err(GeometricError::Redis)?;
        
        info!("  Stream for node {}: {} messages pending", node_id, len);
    }
    
    Ok(())
}