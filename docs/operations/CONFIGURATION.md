# gNode Configuration Guide

Complete reference for configuring the Geodineum Service Daemon (gNode).

---

## Table of Contents

- [Configuration Overview](#configuration-overview)
- [Default Settings](#default-settings)
- [Configuration Priority](#configuration-priority)
- [YAML Configuration](#yaml-configuration)
- [Environment Variables](#environment-variables)
- [Command-Line Arguments](#command-line-arguments)
- [Connection Settings](#connection-settings)
- [Configuration Files](#configuration-files)
- [Example Configurations](#example-configurations)
- [Advanced Settings](#advanced-settings)

---

## Configuration Overview

gNode uses a hierarchical configuration system with four sources:

1. **Defaults** (hardcoded in `daemon/src/config.rs`)
2. **YAML File** (optional, `daemon/config/stream-config.yaml`)
3. **Environment Variables** (`/etc/geodineum/` config or shell environment)
4. **Command-Line Arguments** (highest priority)

### Configuration Loading Process

```
Defaults → YAML File → Environment Variables → CLI Args
(lowest priority)                       (highest priority)
```

**Example**: If you set `max_batch_size` in all four places:
- Default: 500
- YAML: 400
- Environment: 300
- CLI: 250

**Result**: CLI wins, `max_batch_size = 250`

---

## Default Settings

### Core Stream Processing Settings

These defaults are defined in `daemon/src/config.rs`:

```rust
// Backoff and Retry
base_backoff_ms: 100              // Base retry delay (milliseconds)
max_backoff_ms: 1000              // Maximum retry delay (milliseconds)

// Batch Processing
initial_batch_size: 250           // Starting batch size
max_batch_size: 500               // Maximum messages per batch
min_batch_size: 50                // Minimum messages per batch

// Stream Management
idle_time_ms: 30000               // Claim idle messages after 30s
trim_interval_secs: 60            // Trim streams every 60s
max_stream_length: 10000          // Maximum stream length
approximate_trim: true            // Use ~MAXLEN for efficiency

// Health Checks and Recovery
pending_check_interval_ms: 5000   // Check pending messages every 5s
circuit_breaker_threshold: 5      // Failures before circuit opens
circuit_breaker_cooldown_secs: 30 // Circuit breaker cooldown period

// Consumer Group Settings
group_name: "gnode-daemon"          // Consumer group name
consumer_prefix: "consumer-"      // Consumer name prefix
block_timeout_ms: 1000            // XREADGROUP block timeout
claim_interval_ms: 5000           // Claim pending messages interval
batch_acknowledge: true           // Batch ACK messages
max_pending_claim: 50             // Maximum pending to claim per cycle

// Geometric Precision (always enabled)
// Q64.64 fixed-point arithmetic is used for all geometric operations
// See: daemon/src/geometric_precision.rs
```

### Connection Defaults

```rust
redis_host: "127.0.0.1"          // ValKey host
redis_port: 47445                // ValKey port
redis_auth: ""                   // Authentication (use ACL user + password)
topology_namespace: "geodineum"  // Shared namespace for topology
node_id: "default"               // Unique node identifier
stream_prefix: "gnode"           // Stream naming prefix
```

### Operational Defaults

```rust
dimensions: 23                   // Capability space dimensions (19 discovery + 4 storage-only)
threads: "auto"                  // Worker thread configuration
max_threads: 16                  // Maximum auto threads
debug: false                     // Debug mode disabled
log_level: "info"                // Log verbosity level
```

---

## Configuration Priority

### Priority Order (High to Low)

1. **Command-Line Arguments** (--max-batch-size=1000)
2. **Environment Variables** (gNode_MAX_BATCH_SIZE=800)
3. **YAML Configuration File** (max_batch_size: 600)
4. **Hardcoded Defaults** (500)

### Example Scenario

Given these configurations:

**daemon/config/stream-config.yaml**:
```yaml
max_batch_size: 400
initial_batch_size: 200
```

**.env file**:
```bash
GNODE_MAX_BATCH_SIZE=300
```

**Command**:
```bash
./daemon/target/release/gnode-daemon \
  --max-batch-size 250 \
  start
```

**Resulting Configuration**:
```
max_batch_size: 250         # CLI wins
initial_batch_size: 200     # YAML (no CLI/env override)
min_batch_size: 50          # Default (no override)
```

---

## YAML Configuration

### Location

Default location: `daemon/config/stream-config.yaml`

Custom location: Use `--stream-config /path/to/config.yaml`

### Full YAML Template

File: `daemon/config/stream-config.yaml`

```yaml
# Unified Stream Processor Configuration
#
# This file configures the behavior of the unified stream processor.
# All settings are optional and will use defaults if not specified.
#
# Command line arguments take precedence over this configuration.

# Base backoff time in milliseconds for stream operations
# Default: 100
base_backoff_ms: 100

# Maximum backoff time in milliseconds for stream operations
# Default: 1000
max_backoff_ms: 1000

# Initial batch size for stream operations
# Default: 250 (was incorrectly documented as 100 in YAML file)
initial_batch_size: 250

# Maximum batch size for stream operations
# Default: 500
max_batch_size: 500

# Minimum batch size for stream operations
# Default: 50
min_batch_size: 50

# Time in milliseconds a message must be idle before being claimed
# Default: 30000 (30 seconds)
idle_time_ms: 30000

# Stream trim interval in seconds
# Default: 60 (1 minute)
trim_interval_secs: 60

# Stream maximum length
# Default: 10000
max_stream_length: 10000

# Use approximate trimming for stream
# Default: true
approximate_trim: true

# Time in milliseconds between pending message checks
# Default: 5000 (5 seconds)
pending_check_interval_ms: 5000

# Circuit breaker threshold for consecutive initialization failures
# Default: 5
circuit_breaker_threshold: 5

# Circuit breaker cool-down period in seconds
# Default: 30
circuit_breaker_cooldown_secs: 30
```

### Using Custom YAML Config

```bash
# Specify custom config file
./daemon/target/release/gnode-daemon \
  --stream-config /path/to/custom-config.yaml \
  start

# With systemd service, edit gnode-daemon.service:
[Service]
ExecStart=/opt/gNode/daemon/target/release/gnode-daemon \
  --stream-config /opt/gNode/daemon/config/stream-config.yaml \
  ...
```

---

## Environment Variables

### Environment File Locations

Configuration is centralized at `/etc/geodineum/`:

```
/etc/geodineum/bootstrap.env                              # Ecosystem-wide (world-readable, NO SECRETS)
/etc/geodineum/components/gnode-daemon/daemon.env          # Daemon tuning (gnode:gnode 640)
/opt/geodineum/gNode/.env                                  # Development overrides (optional)
```

The systemd service loads these in order (later files override earlier).

### Connection Environment Variables

```bash
# ValKey Connection (used by scripts and setup)
VALKEY_HOST=127.0.0.1
VALKEY_PORT=47445
VALKEY_PASSWORD=your_password_here

# Authentication (always use REDISCLI_AUTH, never --pass flag)
REDISCLI_AUTH=your_password_here
```

### gNode Configuration Environment Variables

```bash
# Stream Processing
GNODE_BASE_BACKOFF_MS=100
GNODE_MAX_BACKOFF_MS=1000
GNODE_MAX_BATCH_SIZE=500

# Consumer Group
GNODE_GROUP_NAME=gnode-daemon

# Add more as needed in daemon/src/config.rs:load_config_from_env()
```

### Example .env File

```bash
# Valkey Configuration (auto-generated by setup-valkey-smart.sh)
VALKEY_HOST=127.0.0.1
VALKEY_PORT=47445
VALKEY_PASSWORD=<your-valkey-password>

# Optional: gNode-specific overrides
# GNODE_MAX_BATCH_SIZE=400
# GNODE_GROUP_NAME=custom-group
```

### Loading .env File

The daemon automatically loads `.env` via the `dotenv` crate:

```rust
// In main.rs
let _ = dotenv::dotenv();  // Loads .env from current directory
```

Scripts can source it:

```bash
# Load environment variables
source .env

# Then use them
echo $VALKEY_PASSWORD
```

---

## Command-Line Arguments

### Complete CLI Reference

```bash
gnode-daemon [OPTIONS] <COMMAND>

Commands:
  start   Start the daemon
  stop    Stop the daemon
  status  Check daemon status

Connection Options:
  --redis-host <HOST>          ValKey host [default: 127.0.0.1]
  --redis-port 47445]
  --redis-auth <PASSWORD>      ValKey password [default: ""]
  --redis-user <USER>          ACL username (e.g., gnode_daemon)
  --topology-namespace <NS>    Shared topology namespace [default: geodineum]
  --daemon-name|--node-id <ID> Unique node identifier [default: default]
  --node-type <TYPE>           Message routing type [default: general]
  --environment <ENV>          DTAP isolation [default: all]
  --master                     Run as config master (loads YAML, stores to ValKey)
  --stream-prefix <PREFIX>     Stream naming prefix [default: gnode]

Operational Options:
  --dimensions <N>             Capability space dimensions [default: 23]
  --threads <auto|N>           Worker threads (auto or number) [default: auto]
  --max-threads <N>            Max auto threads [default: 16]
  --debug                      Enable debug mode
  --log-level <LEVEL>          Log level (error|warn|info|debug|trace) [default: info]

Configuration Options:
  --stream-config <PATH>       Path to YAML config file

Stream Processing Options:
  --base-backoff-ms <MS>              Base retry delay [default: 100]
  --max-backoff-ms <MS>               Max retry delay [default: 1000]
  --initial-batch-size <N>            Initial batch size [default: 250]
  --max-batch-size <N>                Max batch size [default: 500]
  --min-batch-size <N>                Min batch size [default: 50]
  --idle-time-ms <MS>                 Idle message claim time [default: 30000]
  --trim-interval-secs <SECS>         Stream trim interval [default: 60]
  --max-stream-length <N>             Max stream length [default: 10000]
  --approximate-trim <BOOL>           Use approximate trim [default: true]
  --pending-check-interval-ms <MS>    Pending check interval [default: 5000]
  --circuit-breaker-threshold <N>     Circuit breaker threshold [default: 5]
  --circuit-breaker-cooldown-secs <S> Circuit breaker cooldown [default: 30]
```

### Example Commands

**Start with defaults**:
```bash
./daemon/target/release/gnode-daemon start
```

**Start with authentication**:
```bash
./daemon/target/release/gnode-daemon \
  --redis-auth "$(cat .gnode/valkey_daemon.password)" \
  start
```

**Start with custom batch size**:
```bash
./daemon/target/release/gnode-daemon \
  --redis-auth "$(cat .gnode/valkey_daemon.password)" \
  --max-batch-size 1000 \
  --initial-batch-size 500 \
  start
```

**Start with debug logging**:
```bash
./daemon/target/release/gnode-daemon \
  --redis-auth "$(cat .gnode/valkey_daemon.password)" \
  --log-level debug \
  --debug \
  start
```

**Start with custom config**:
```bash
./daemon/target/release/gnode-daemon \
  --redis-auth "$(cat .gnode/valkey_daemon.password)" \
  --stream-config /path/to/config.yaml \
  start
```

**Multi-tenant setup**:
```bash
# Site 1, Node 1
./daemon/target/release/gnode-daemon \
  --topology-namespace production \
  --node-id node1 \
  --redis-auth "$(cat .gnode/valkey_daemon.password)" \
  start

# Site 1, Node 2
./daemon/target/release/gnode-daemon \
  --topology-namespace production \
  --node-id node2 \
  --redis-auth "$(cat .gnode/valkey_daemon.password)" \
  start
```

---

## Connection Settings

### ValKey Authentication

**Priority order**:
1. CLI: `--redis-auth "password"` (avoid — visible in `ps`)
2. Environment: `$GNODE_REDIS_AUTH` (recommended — used by systemd service)
3. File: `/etc/geodineum/credentials/valkey_daemon.password` (read by service on start)

**Recommended**: Let the systemd service handle authentication via `GNODE_REDIS_AUTH` env var (loaded from `daemon.env`). For manual CLI usage, pass the password file inline.

### Password Storage

**Centralized credentials** (production):
```
/etc/geodineum/credentials/
├── valkey.password                     # Admin (ACL management)
├── valkey_daemon.password              # Daemon (gnode_daemon ACL user)
└── valkey_client_{service_id}.password    # Per-site client passwords
```

**Dev credentials** (development):
```
/opt/gNode/.gnode/
├── valkey_daemon.password              # Daemon password
└── valkey_client_{service_id}.password    # Per-site client passwords
```

**Permissions**: `600` (owner read/write only), owned by `gnode:gnode`

**Usage**:
```bash
# Systemd service (automatic — reads from daemon.env)
# No manual password passing needed

# Manual CLI
--redis-auth "$(cat /etc/geodineum/credentials/valkey_daemon.password)"
```

**Security notes**:
- Never commit credential files to git (in `.gitignore`)
- Client passwords are `gnode:www-data` (640) so PHP can read them
- Daemon password is `gnode:gnode` (600) — only daemon needs it

### Connection Examples

**Local development**:
```bash
--redis-host 127.0.0.1 \
--redis-port 47445 \
--redis-auth "dev_password"
```

**Production (systemd)**:
The systemd service handles authentication automatically via `GNODE_REDIS_AUTH` env var — see `daemon/config/gnode-daemon.service` for the full ExecStart configuration.

**Remote ValKey**:
```bash
--redis-host valkey.example.com \
--redis-port 47445 \
--redis-auth "$(cat .gnode/valkey_daemon.password)"
```

---

## Node Configuration

### Overview

gNode uses a unified node configuration system. On the master node, YAML files in the node config directory define node types. The master loads these and stores them to ValKey; worker nodes fetch from ValKey (no local YAML needed). Each YAML file defines a node type with:
- **Routing rules**: Which messages the node processes (include/exclude by group hint)
- **Resources**: CPU cores, memory limits, thread pool size
- **Performance**: Batch sizes, timeouts, circuit breaker settings
- **Capabilities**: Geometric dimensions for service discovery
- **Health**: Metrics reporting interval and retention

### Master vs Worker Nodes

| Mode | Behavior | CLI Flag |
|------|----------|----------|
| **Master** | Loads node YAML configs, stores to ValKey | `--master` or `--node-id=master` |
| **Worker** | Fetches configs from ValKey (no local YAML needed) | (default, no flag) |

### Node Configuration Schema

```yaml
node_type: string           # Unique identifier (e.g., "general", "inference")
description: string         # Human-readable description

routing:
  mode: include|exclude|all # include=only matching, exclude=all except, all=everything
  group_hints:              # Message routing hints to match
    - inference
    - gpu_compute

resources:
  cores: 0                  # 0 = auto-detect
  max_memory_mb: 0          # 0 = no limit
  thread_pool_size: 0       # 0 = auto (matches cores)

performance:
  batch_size:
    initial: 250            # Starting batch size
    min: 50                 # Minimum batch size
    max: 500                # Maximum batch size
  timeouts:
    idle_ms: 30000          # Claim idle messages after this time
    block_ms: 1000          # XREADGROUP block timeout
  circuit_breaker:
    threshold: 5            # Failures before circuit opens
    cooldown_secs: 30       # Cooldown before retry

capabilities:
  dimensions:               # Geometric capability dimensions (0.0-1.0)
    general: 1.0
    caching: 0.9

health:
  report_interval_ms: 5000  # Heartbeat frequency
  metrics_enabled: true     # Enable metrics collection
  metrics_retention_secs: 3600

metadata:
  version: "1.0.0"
  created_by: "system"
```

### Built-in Node Types

| Type | Mode | Processes | Ignores |
|------|------|-----------|---------|
| `general` | exclude | Everything else | inference, gpu_compute, batch_heavy |
| `inference` | include | inference, ml_predict, embedding | Everything else |
| `gpu_compute` | include | gpu_compute, tensor_ops, matrix_mult | Everything else |
| `all` | all | Everything | Nothing (dev/test only) |

### Creating Custom Node Types

1. Copy a template:
   ```bash
   cp daemon/config/nodes/general.yaml daemon/config/nodes/my_custom.yaml
   ```

2. Edit the file to set `node_type`, routing rules, and capabilities

3. Restart master node to load:
   ```bash
   sudo systemctl restart gnode-daemon
   ```

4. Start workers with:
   ```bash
   ./gnode-daemon --node-type my_custom --node-id worker1
   ```

See [NODE_QUICKSTART.md](NODE_QUICKSTART.md) for detailed examples.

---

## Configuration Files

### File Locations

**Centralized config** (`/etc/geodineum/`):
```
/etc/geodineum/
├── bootstrap.env                                      # Ecosystem-wide settings (644)
├── components/
│   └── gnode-daemon/
│       ├── daemon.env                                 # Daemon tuning (gnode:gnode 640)
│       └── nodes/                                     # Node type configurations (YAML)
└── credentials/
    ├── valkey.password                                # Admin password
    ├── valkey_daemon.password                         # Daemon ACL password (600)
    └── valkey_client_{service_id}.password               # Per-site client passwords (640)
```

**Repo config** (`daemon/config/`):
```
daemon/config/
├── stream-config.yaml                 # Stream processor config (DTAP environments)
├── service_schema.yaml                # service-tier (30D = 25 discovery + 5 storage) dimension name → coordinate mapping (canonical tier; legacy capability_schema.yaml/23D removed pre-launch)
├── lua-libraries.yaml                 # Lua library load list
├── gnode-daemon.service               # Systemd service template
├── valkey-gnode.conf                  # Production ValKey config
└── nodes/                             # Node type configs (master loads → stores to ValKey)

# Note: ValKey backup units live at Geodineum-BAK/config/systemd/ post-0.6.
# The Geodineum installer's phase_bak provisions them in /etc/systemd/system/.
```

**Dev credentials** (`.gnode/` — gitignored):
```
.gnode/
├── valkey_daemon.password             # Daemon ACL password
└── valkey_client_{service_id}.password   # Per-site client passwords
```

---

## Example Configurations

### Development Configuration

**daemon/config/dev.yaml**:
```yaml
# Development configuration
max_batch_size: 100
initial_batch_size: 50
min_batch_size: 10
trim_interval_secs: 30
max_stream_length: 5000
circuit_breaker_threshold: 10
```

**Start command**:
```bash
./daemon/target/release/gnode-daemon \
  --redis-auth "dev_password" \
  --stream-config daemon/config/dev.yaml \
  --log-level debug \
  --debug \
  start
```

### High-Throughput Production

**daemon/config/production-high-throughput.yaml**:
```yaml
# High-throughput production configuration
initial_batch_size: 500
max_batch_size: 1000
min_batch_size: 100
base_backoff_ms: 50
max_backoff_ms: 500
idle_time_ms: 15000
trim_interval_secs: 30
max_stream_length: 50000
pending_check_interval_ms: 2000
```

**Start command**:
```bash
./daemon/target/release/gnode-daemon \
  --redis-auth "$(cat .gnode/valkey_daemon.password)" \
  --stream-config daemon/config/production-high-throughput.yaml \
  --threads 16 \
  start
```

### Resource-Constrained Environment

**daemon/config/low-resource.yaml**:
```yaml
# Resource-constrained configuration
initial_batch_size: 50
max_batch_size: 100
min_batch_size: 10
max_stream_length: 2000
trim_interval_secs: 120
circuit_breaker_threshold: 3
```

**Start command**:
```bash
./daemon/target/release/gnode-daemon \
  --redis-auth "$(cat .gnode/valkey_daemon.password)" \
  --stream-config daemon/config/low-resource.yaml \
  --threads 2 \
  start
```

### Multi-Tenant Production

**daemon/config/multitenant.yaml**:
```yaml
# Multi-tenant configuration
max_batch_size: 500
initial_batch_size: 250
idle_time_ms: 30000
trim_interval_secs: 60
max_stream_length: 20000
circuit_breaker_threshold: 5
```

**Start multiple nodes**:
```bash
# Tenant 1, Node 1
./daemon/target/release/gnode-daemon \
  --topology-namespace tenant1 \
  --node-id node1 \
  --redis-auth "$(cat .gnode/valkey_daemon.password)" \
  --stream-config daemon/config/multitenant.yaml \
  start

# Tenant 1, Node 2
./daemon/target/release/gnode-daemon \
  --topology-namespace tenant1 \
  --node-id node2 \
  --redis-auth "$(cat .gnode/valkey_daemon.password)" \
  --stream-config daemon/config/multitenant.yaml \
  start

# Tenant 2, Node 1
./daemon/target/release/gnode-daemon \
  --topology-namespace tenant2 \
  --node-id node1 \
  --redis-auth "$(cat .gnode/valkey_daemon.password)" \
  --stream-config daemon/config/multitenant.yaml \
  start
```

---

## Advanced Settings

### Thread Configuration

**Auto configuration** (recommended):
```bash
--threads auto --max-threads 16
```

This uses `min(CPU_cores, max_threads)` worker threads.

**Fixed configuration**:
```bash
--threads 8
```

This uses exactly 8 worker threads.

**Examples**:
```bash
# Let gNode choose based on CPU cores (up to 16)
--threads auto --max-threads 16

# Use exactly 4 threads
--threads 4

# Use all CPU cores (up to 32)
--threads auto --max-threads 32
```

### Capability Dimensions

Default: 23 dimensions (19 discovery + 4 storage-only). This matches the
Service-tier semantic topology (30D = 25 discovery + 5 storage) defined in `daemon/config/service_schema.yaml`. Other tiers: tool_schema.yaml (16D), constellation_schema.yaml (20D), galaxy_schema.yaml (20D).

Changing this value is only needed for custom topology configurations
outside the standard service discovery model.

### Circuit Breaker Tuning

**Aggressive** (fail fast):
```yaml
circuit_breaker_threshold: 3
circuit_breaker_cooldown_secs: 60
```

**Lenient** (tolerate transient errors):
```yaml
circuit_breaker_threshold: 10
circuit_breaker_cooldown_secs: 15
```

**Default** (balanced):
```yaml
circuit_breaker_threshold: 5
circuit_breaker_cooldown_secs: 30
```

### Batch Size Optimization

**For low latency**:
```yaml
initial_batch_size: 50
max_batch_size: 100
```

**For high throughput**:
```yaml
initial_batch_size: 500
max_batch_size: 1000
```

**Adaptive (default)**:
```yaml
initial_batch_size: 250
max_batch_size: 500
min_batch_size: 50
```

### Stream Trimming

**Aggressive trimming** (save memory):
```yaml
max_stream_length: 5000
trim_interval_secs: 30
approximate_trim: true
```

**Lenient trimming** (keep more history):
```yaml
max_stream_length: 50000
trim_interval_secs: 300
approximate_trim: true
```

**Exact trimming** (slower but precise):
```yaml
approximate_trim: false
```

---

## Configuration Validation

### Checking Current Configuration

```bash
# Start daemon with --debug to see loaded config
./daemon/target/release/gnode-daemon \
  --debug \
  start

# Look for log line:
# [DEBUG] Final unified stream configuration: ...
```

### Configuration Recommendations

**Development**:
- `log_level: debug`
- `debug: true`
- Smaller batch sizes (faster feedback)
- More frequent trimming

**Production**:
- `log_level: info`
- `debug: false`
- Optimized batch sizes (250-500)
- Balanced trimming (60s interval)
- Enable systemd auto-restart

**High-Performance**:
- Larger batch sizes (500-1000)
- More threads (16+)
- Larger stream lengths

**Low-Resource**:
- Smaller batch sizes (50-100)
- Fewer threads (2-4)
- Aggressive trimming

---

## Related Documentation

- **[SYSTEMD_SERVICES.md](SYSTEMD_SERVICES.md)** - Systemd service management
- **[README.md](../README.md)** - Installation and usage
- **[CLAUDE.md](../CLAUDE.md)** - Architecture reference
- **[daemon/config/stream-config.yaml](../../daemon/config/stream-config.yaml)** - Default YAML config
- **[daemon/src/config.rs](../daemon/src/config.rs)** - Configuration code

---

**Last Updated**: 2026-03-11
