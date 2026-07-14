# gNode Node Quickstart Guide

Quick setup guide for adding new gNode nodes to an existing deployment.

## Prerequisites

- Existing gNode master node running with ValKey
- Network access to master's ValKey instance (port 47445)
- ValKey daemon password from master: `.gnode/valkey_daemon.password`

## Understanding Master vs Worker Nodes

| Node Type | Behavior | Use Case |
|-----------|----------|----------|
| **Master** | Loads node configs from YAML, stores to ValKey | First node in cluster, config source of truth |
| **Worker** | Fetches configs from ValKey (no local YAML needed) | Additional nodes, remote servers |

**Master mode is enabled by:**
- `--master` flag (explicit, recommended)
- `--node-id=master` (implicit, backward compatible)

---

## 1. Master Node Setup

The first node should be the master. It loads YAML configs and stores them to ValKey.

```bash
# 1. Clone gNode
git clone https://github.com/geodineum/gNode.git
cd gNode

# 2. Run full setup (builds daemon, starts ValKey, loads functions)
./setup-gnode.sh

# 3. Start as master
./daemon/target/release/gnode-daemon \
  --master \
  --topology-namespace <YOUR_SITE_ID> \
  --environment production \
  --node-id master \
  --node-type general \
  --redis-auth "$(cat .gnode/valkey_daemon.password)" \
  --redis-user gnode_daemon
```

**What this does:**
- Loads node type configurations from the node config directory (see `--node-config-dir`)
- Stores configurations to ValKey for workers to fetch
- Registers itself in the node topology
- Starts processing messages for `--node-type` (general by default)

---

## 2. Worker Node Setup (Remote Server)

Worker nodes only need the binary and password file - configs come from ValKey.

```bash
# 1. Clone and build
git clone https://github.com/geodineum/gNode.git
cd gNode/daemon && cargo build --release && cd ..

# 2. Copy password from master
mkdir -p .gnode
scp master:/opt/gNode/.gnode/valkey_daemon.password .gnode/

# 3. Start worker (fetches config from ValKey)
./daemon/target/release/gnode-daemon \
  --redis-host <MASTER_IP> \
  --redis-port 47445 \
  --redis-user gnode_daemon \
  --redis-auth "$(cat .gnode/valkey_daemon.password)" \
  --topology-namespace <YOUR_SITE_ID> \
  --environment production \
  --node-id worker1 \
  --node-type general
```

---

## 3. Inference Node Setup (5 minutes)

For AI/ML inference on dedicated hardware:

```bash
# 1. Clone and build (same as above)
git clone https://github.com/geodineum/gNode.git
cd gNode/daemon && cargo build --release && cd ..

# 2. Copy password from master
mkdir -p .gnode
scp master:/opt/gNode/.gnode/valkey_daemon.password .gnode/

# 3. Start inference node
./daemon/target/release/gnode-daemon \
  --redis-host <MASTER_IP> \
  --redis-port 47445 \
  --redis-user gnode_daemon \
  --redis-auth "$(cat .gnode/valkey_daemon.password)" \
  --topology-namespace <YOUR_SITE_ID> \
  --environment production \
  --node-id inference1 \
  --node-type inference
```

**What this does**: Only processes messages with `_gh:"inference"` routing hint.

---

## 4. GPU Compute Node Setup

For CUDA/OpenCL workloads:

```bash
./daemon/target/release/gnode-daemon \
  --redis-host <MASTER_IP> \
  --redis-user gnode_daemon \
  --redis-auth "$(cat .gnode/valkey_daemon.password)" \
  --topology-namespace <YOUR_SITE_ID> \
  --environment production \
  --node-id gpu1 \
  --node-type gpu_compute
```

**What this does**: Only processes `_gh:"gpu_compute"`, `_gh:"tensor_ops"`, `_gh:"matrix_mult"`.

---

## 5. Creating Custom Node Types

To add a new specialized node type:

### On the Master Server:

Create a YAML file in the node config directory (default: `/etc/geodineum/components/gnode-daemon/nodes/`):

```bash
sudo nano /etc/geodineum/components/gnode-daemon/nodes/my_custom.yaml
```

**Example config:**
```yaml
node_type: my_custom
description: "Custom node for specialized workloads"

routing:
  mode: include
  group_hints:
    - custom_task
    - special_op

resources:
  cores: 0
  max_memory_mb: 0
  thread_pool_size: 0

performance:
  batch_size: {initial: 250, min: 50, max: 500}
  timeouts: {idle_ms: 30000, block_ms: 1000}
  circuit_breaker: {threshold: 5, cooldown_secs: 30}

capabilities:
  dimensions:
    custom_capability: 1.0

health:
  report_interval_ms: 5000
  metrics_enabled: true
```

```bash
# 3. Restart master to load new config
sudo systemctl restart gnode-daemon
```

### On Worker Servers:

```bash
# Workers automatically get the new config from ValKey
./gnode-daemon --node-type my_custom --node-id custom1 \
  --redis-host <MASTER_IP> \
  --redis-auth "$(cat .gnode/valkey_daemon.password)"
```

---

## Quick Reference

| Node Type | Processes | Ignores |
|-----------|-----------|---------|
| `general` | Everything else | `inference`, `gpu_compute` |
| `inference` | `_gh:"inference"` only | Everything else |
| `gpu_compute` | `_gh:"gpu_compute"`, `tensor_ops`, `matrix_mult` | Everything else |
| `all` | Everything | Nothing (dev only) |

---

## systemd Service (Optional)

Create `/etc/systemd/system/gnode-worker.service`:

```ini
[Unit]
Description=gNode Worker Node
After=network.target

[Service]
Type=simple
User=gnode
WorkingDirectory=/opt/geodineum/gNode
ExecStart=/opt/geodineum/gNode/daemon/target/release/gnode-daemon \
  --redis-host <MASTER_IP> \
  --redis-user gnode_daemon \
  --redis-auth "$(cat /opt/gNode/.gnode/valkey_daemon.password)" \
  --topology-namespace <YOUR_SITE_ID> \
  --environment production \
  --node-id worker1 \
  --node-type general
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

For inference nodes, create `/etc/systemd/system/gnode-inference.service`:

```ini
[Unit]
Description=gNode Inference Node
After=network.target

[Service]
Type=simple
User=gnode
WorkingDirectory=/opt/geodineum/gNode
ExecStart=/opt/geodineum/gNode/daemon/target/release/gnode-daemon \
  --redis-host <MASTER_IP> \
  --redis-user gnode_daemon \
  --redis-auth "$(cat /opt/gNode/.gnode/valkey_daemon.password)" \
  --topology-namespace <YOUR_SITE_ID> \
  --environment production \
  --node-id inference1 \
  --node-type inference
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable gnode-worker
sudo systemctl start gnode-worker
```

---

## Verify Connection

```bash
# Check node is connected to consumer group
REDISCLI_AUTH="$(cat .gnode/valkey_daemon.password)" \
  valkey-cli -h <MASTER_IP> --user gnode_daemon \
  XINFO GROUPS {<YOUR_SITE_ID>}:gnode:unified:production

# View all registered nodes
REDISCLI_AUTH="$(cat .gnode/valkey_daemon.password)" \
  valkey-cli -h <MASTER_IP> --user gnode_daemon \
  FCALL GNODE_NODE_GET_TOPOLOGY 0 true

# Check configs were loaded
REDISCLI_AUTH="$(cat .gnode/valkey_daemon.password)" \
  valkey-cli -h <MASTER_IP> --user gnode_daemon \
  SMEMBERS gnode:node_config:_types
```

---

## Sending Messages to Specific Node Types

```bash
# Send to inference nodes only
XADD {site}:gnode:unified:production * id "req-1" cmd inference_request params '{"model":"gpt"}' _gh inference

# Send to GPU nodes only
XADD {site}:gnode:unified:production * id "req-2" cmd matrix_multiply params '{"size":1000}' _gh gpu_compute

# Send to general nodes (no _gh field)
XADD {site}:gnode:unified:production * id "req-3" cmd ping params '{}'

# Send to custom nodes
XADD {site}:gnode:unified:production * id "req-4" cmd custom_task params '{}' _gh custom_task
```

---

## Troubleshooting

**"WRONGPASS" error**:
```bash
# Use REDISCLI_AUTH, not --pass flag
REDISCLI_AUTH="$(cat .gnode/valkey_daemon.password)" valkey-cli -h <MASTER_IP> --user gnode_daemon PING
```

**Node not processing messages**:
```bash
# Check routing config was loaded
REDISCLI_AUTH="$(cat .gnode/valkey_daemon.password)" \
  valkey-cli -h <MASTER_IP> --user gnode_daemon \
  GET gnode:routing:inference
```

**Config not available for worker**:
```bash
# Ensure master was started with --master flag
# Check configs exist in ValKey:
REDISCLI_AUTH="$(cat .gnode/valkey_daemon.password)" \
  valkey-cli -h <MASTER_IP> --user gnode_daemon \
  SMEMBERS gnode:node_config:_types
```

**Messages stuck in PEL**:
```bash
# View pending messages
REDISCLI_AUTH="$(cat .gnode/valkey_daemon.password)" \
  valkey-cli -h <MASTER_IP> --user gnode_daemon \
  XPENDING {site}:gnode:unified:production gnode-workers
```

---

## Related Documentation

- [CONFIGURATION.md](CONFIGURATION.md) - Configuration reference
- [../CLAUDE.md](../../CLAUDE.md) - Architecture reference
