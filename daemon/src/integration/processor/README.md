# Unified Stream Processor for gNode

This directory contains the implementation of the unified stream approach for the Geodineum Service Daemon (gNode). The unified stream approach replaces the previous dual-stream architecture with a more efficient single-stream design using optimized RESP3 protocol.

## Overview

The unified stream approach has the following key characteristics:

1. **Single Stream**: Both commands and responses are sent through a single ValKey stream, reducing overhead and simplifying management.

2. **RESP3 Optimization**: The stream uses an optimized RESP3 format for efficient storage and communication, with shortened field names and native type representation.

3. **Protocol Conversion**: ValKey functions handle conversion between JSON (for clients) and RESP3 (for internal storage), bridging the two protocols.

4. **Memory Efficiency**: The optimized format reduces memory usage by 65-70% compared to the previous approach.

## Key Components

1. **resp3_protocol.rs**: Defines the RESP3 value representation and conversion between standard commands and optimized format.

2. **unified_stream_processor.rs**: Core implementation of the unified stream approach, handling stream operations, consumer groups, and message processing.

3. **stream_utils.rs**: Utility functions for common stream operations and error handling.

4. **state_manager.rs**: State management for stream processing, including batch size adjustment and backoff.

5. **broadcast_reader.rs**: XREAD-based pub-sub reader (no consumer groups).

6. **health_processor.rs**: Health stream processing.

7. **pending_processor.rs**: Pending message recovery (XAUTOCLAIM).

8. **circuit_breakers.rs**: Circuit breaker state management.

9. **gnode_protocol.lua** (in `daemon/functions/`): ValKey functions for protocol conversion between JSON and RESP3.

## Architecture

The unified stream approach uses a layered architecture:

1. **Client Layer**: Clients use native JSON for communication, with no protocol awareness needed.

2. **Conversion Layer**: ValKey functions convert between JSON and RESP3 at stream boundaries.

3. **Transport Layer**: Streams use optimized RESP3 format for efficient storage.

4. **Processing Layer**: The Rust daemon processes messages directly in RESP3 format.

## Usage

To use the unified stream approach:

1. **Initialize**: Call `initialize_unified_stream` to set up the unified stream and consumer groups.

2. **Send Commands**: Use `send_command` to send a command to the unified stream.

3. **Read Commands**: Use `read_commands` to read commands from the unified stream.

4. **Process Commands**: Use `process_commands` to process commands and send responses.

5. **Read Responses**: Use `read_responses` to read responses from the unified stream.

## Metrics and Monitoring

The unified stream approach provides several metrics for monitoring:

1. **Storage Efficiency**: Track stream size reduction with the optimized format.

2. **Type Distribution**: Analyze message types and sizes in the stream.

3. **Operation Throughput**: Measure operations per second with the unified approach.

4. **Memory Usage**: Track memory consumption over time.

5. **Protocol Conversion Overhead**: Measure ValKey function execution time.

