# Geometric Service Daemon (GSD)

## Overview

The Geometric Service Daemon (GSD) is a high-performance, capability-based service discovery system for the nCore framework. It enables efficient service discovery using n-dimensional capability vectors in a geometric space, allowing services to be discovered by their capabilities rather than by direct references.

This implementation consists of:

1. A Rust daemon (`gsd-daemon`) that manages the capability space and service registry
2. A PHP client (`GSDClient`) that communicates with the daemon through ValKey/Redis streams
3. A fallback implementation for environments where the daemon cannot run

## Architecture

The GSD system is built on a client-server model with the following components:

- **GSD Daemon**: A Rust-based daemon that manages the service registry and capability space
- **GSD Client**: A PHP client that communicates with the daemon using ValKey streams
- **ValKey/Redis**: The communication layer between client and daemon using streams
- **Geometric Topology**: The mathematical model for capability-based service discovery

Communication between client and daemon occurs through Redis Streams, with two streams for each node:
- `{site_id}:{stream_prefix}:stream:{node_id}:commands` - Commands from client to daemon
- `{site_id}:{stream_prefix}:stream:{node_id}:responses` - Responses from daemon to client

## Installation

### Prerequisites

- ValKey or Redis server (version 5.0+)
- PHP 7.4+ with Redis extension
- Rust 1.50+ (for building the daemon)

### Building the Daemon

```bash
cd TBD/daemon
cargo build --release
```

The compiled binary will be placed in `TBD/daemon/target/release/gsd-daemon`.

### Setting Up the Framework

1. Copy the daemon binary to your preferred location:
   ```bash
   cp TBD/daemon/target/release/gsd-daemon bin/
   ```

2. Include the GSDClient in your PHP application:
   ```php
   use nCore\Modules\Core\Client\GSDClient;
   ```

## Usage

### Starting the Daemon

The daemon can be started directly or through the PHP client's auto-start mechanism:

```bash
SITE_ID=default NODE_ID=default STREAM_PREFIX=gsd ./bin/gsd-daemon --debug
```

Command-line options:
- `--redis-host`: Redis host (default: 127.0.0.1)
- `--redis-port`: Redis port (default: 6379)
- `--redis-auth`: Redis auth password (default: empty)
- `--site-id`: Site identifier (default: default)
- `--node-id`: Node identifier (default: default)
- `--stream-prefix`: Stream prefix (default: gsd)
- `--dimensions`: Number of dimensions (default: 8)
- `--debug`: Enable debug mode

### Using the Client

```php
use nCore\Modules\Core\Client\GSDClient;
use nCore\Modules\Core\Adapters\Shared\ValKeyStorage;

// Create ValKey storage connection
$storage = new ValKeyStorage([
    'host' => '127.0.0.1',
    'port' => 6379
]);

// Create GSD client
$client = new GSDClient(
    $storage,
    'default', // site_id
    'default', // node_id
    [
        'stream_prefix' => 'gsd',
        'auto_start' => true,
        'daemon_path' => '/path/to/gsd-daemon',
        'debug' => true
    ]
);

// Register a capability dimension
$client->registerCapabilityDimension('performance', 0);

// Register a service
$client->registerService('my-service', [
    'performance' => 0.9,
    'reliability' => 0.8
]);

// Find services matching requirements
$services = $client->findServices([
    'performance' => 0.7
]);

// Get the load sequence for dependency ordering
$sequence = $client->getLoadSequence();
```

## Troubleshooting

### Stream ID Format Issues

If you encounter errors with stream IDs, ensure:

1. The daemon is using `0` instead of `0-0` for reading pending messages
2. The `XREADGROUP` command has arguments in the correct order (`COUNT` before `STREAMS`)

### Parameter Serialization

For proper communication:

1. The client must JSON-encode parameters before sending
2. The daemon must properly handle quoted strings and parameter parsing

### Redis Connection

Ensure Redis is running and accessible:

```bash
redis-cli ping
```

For connection issues, check:
- Redis is running on the expected host/port
- Authentication is configured correctly
- No firewall blocking the connection

### Debug Logging

Enable debug mode for detailed logging:

```bash
RUST_LOG=debug ./bin/gsd-daemon --debug
```

In the PHP client:

```php
$client = new GSDClient($storage, 'default', 'default', [
    'debug' => true
]);
```

## Performance Considerations

- The daemon is designed for high throughput with minimal latency
- Service discovery operations are O(1) complexity
- The client includes caching for read-only operations
- Batch operations can be used for registering multiple services

## Security

- Ensure Redis is properly secured, especially in production environments
- The daemon should run with restricted privileges
- Consider using SSL/TLS for Redis connections in production
- Always validate inputs to prevent injection attacks

## License

This software is part of the nCore framework and is subject to its licensing terms.

---

© 2025 nCore Framework