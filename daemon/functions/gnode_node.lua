#!lua name=gnode_node

--
-- gNode NODE Functions
-- A ValKey function library for node registration, configuration, and metrics
--
-- This library handles:
--   - Node self-registration with configuration from YAML
--   - Topology discovery when connecting to existing clusters
--   - Usage metrics capture and aggregation
--   - Health reporting for load balancing
--
-- Key patterns:
--   gnode:node:{node_id}:config     - Node configuration (from YAML)
--   gnode:node:{node_id}:metrics    - Runtime metrics (load, cpu, mem, etc.)
--   gnode:node:{node_id}:health     - Health status and timestamps
--   gnode:nodes:registry            - Set of all registered node IDs
--   gnode:nodes:by_type:{type}      - Set of node IDs by node type
--   gnode:nodes:topology            - Cached topology for discovery

-- Helper: Track node metric
local function track_node_metric(node_id, metric, value)
    local metrics_key = 'gnode:node:' .. node_id .. ':metrics'
    server.call('HINCRBY', metrics_key, metric, value or 1)
    server.call('HSET', metrics_key, 'last_updated', server.call('TIME')[1])
end

-- Helper: JSON encode using cjson (ValKey built-in)
local function safe_json_encode(value)
    if value == nil then
        return "null"
    end
    local success, result = pcall(cjson.encode, value)
    if success then
        return result
    end
    -- Fallback for encoding errors (e.g., circular references)
    return '{"error":"encoding_failed"}'
end

-- Helper: JSON decode using cjson (ValKey built-in)
local function safe_json_decode(value)
    if not value then return nil, "No value to decode" end
    local success, result = pcall(cjson.decode, value)
    if success then
        return result
    end
    return nil, "JSON decoding error: " .. tostring(result)
end

--
-- GNODE_REGISTER_NODE: Register a node with its configuration
--
-- Args:
--   node_id: Unique node identifier
--   node_type: Node type (general, inference, gpu_compute, custom)
--   config_json: Full node configuration as JSON
--   site_id: Site identifier (optional)
--   hostname: Node hostname (optional)
--   ip_address: Node IP address (optional)
--
-- Returns: "OK" or error
--
server.register_function{
    function_name = 'GNODE_REGISTER_NODE',
    callback = function(keys, args)
        -- Input validation
        if not args[1] or args[1] == '' then
            return server.error_reply("Node ID required")
        end
        if not args[2] or args[2] == '' then
            return server.error_reply("Node type required")
        end

        local node_id = args[1]
        local node_type = args[2]
        local config_json = args[3] or '{}'
        local site_id = args[4] or 'default'
        local hostname = args[5] or node_id
        local ip_address = args[6] or '127.0.0.1'

        local now = server.call('TIME')[1]

        -- Parse config if provided
        local config = {}
        if config_json ~= '' and config_json ~= '{}' then
            config = safe_json_decode(config_json) or {}
        end

        -- Key patterns
        local config_key = 'gnode:node:' .. node_id .. ':config'
        local health_key = 'gnode:node:' .. node_id .. ':health'
        local metrics_key = 'gnode:node:' .. node_id .. ':metrics'
        local registry_key = 'gnode:nodes:registry'
        local type_key = 'gnode:nodes:by_type:' .. node_type

        -- Check if node already exists (update vs new registration)
        local existing = server.call('EXISTS', config_key) == 1
        local status = existing and 'updated' or 'registered'

        -- Store node configuration
        server.call('HMSET', config_key,
            'node_id', node_id,
            'node_type', node_type,
            'site_id', site_id,
            'hostname', hostname,
            'ip_address', ip_address,
            'registered_at', existing and (server.call('HGET', config_key, 'registered_at') or now) or now,
            'updated_at', now,
            'status', 'active',
            'version', config.metadata and config.metadata.version or '1.0.0'
        )

        -- Store routing config if provided
        if config.routing then
            server.call('HSET', config_key, 'routing_mode', config.routing.mode or 'all')
            if config.routing.group_hints then
                local hints = table.concat(config.routing.group_hints, ',')
                server.call('HSET', config_key, 'routing_hints', hints)
            end
        end

        -- Store resource config if provided
        if config.resources then
            server.call('HSET', config_key, 'cores', config.resources.cores or 0)
            server.call('HSET', config_key, 'max_memory_mb', config.resources.max_memory_mb or 0)
            server.call('HSET', config_key, 'thread_pool_size', config.resources.thread_pool_size or 0)
        end

        -- Store performance config if provided
        if config.performance and config.performance.batch_size then
            server.call('HSET', config_key, 'batch_initial', config.performance.batch_size.initial or 250)
            server.call('HSET', config_key, 'batch_min', config.performance.batch_size.min or 50)
            server.call('HSET', config_key, 'batch_max', config.performance.batch_size.max or 500)
        end

        -- Store capabilities if provided
        if config.capabilities and config.capabilities.dimensions then
            local caps = {}
            for k, v in pairs(config.capabilities.dimensions) do
                table.insert(caps, k .. ':' .. tostring(v))
            end
            server.call('HSET', config_key, 'capabilities', table.concat(caps, ','))
        end

        -- Initialize health status
        server.call('HMSET', health_key,
            'status', 'healthy',
            'last_heartbeat', now,
            'heartbeat_count', 0,
            'startup_time', now
        )

        -- Initialize metrics if not existing
        if not existing then
            server.call('HMSET', metrics_key,
                'commands_processed', 0,
                'commands_failed', 0,
                'bytes_in', 0,
                'bytes_out', 0,
                'avg_latency_ms', 0,
                'load_factor', 0,
                'cpu_usage', 0,
                'memory_usage', 0,
                'last_updated', now
            )
        end

        -- Add to registries
        server.call('SADD', registry_key, node_id)
        server.call('SADD', type_key, node_id)

        -- Track registration
        track_node_metric(node_id, 'registrations', 1)

        -- Publish registration event to broadcast stream
        local event = {
            type = 'node_' .. status,
            node_id = node_id,
            node_type = node_type,
            site_id = site_id,
            timestamp = now
        }
        local event_json = safe_json_encode(event)

        -- Add to broadcast stream if it exists
        local broadcast_stream = site_id .. ':gnode:broadcast:global'
        pcall(function()
            server.call('XADD', broadcast_stream, 'MAXLEN', '~', 1000, '*',
                't', 'node_' .. status,
                'ss', site_id,
                'ts', tostring(now),
                'node_id', node_id,
                'node_type', node_type
            )
        end)

        return server.status_reply("OK")
    end,
    description = 'Registers a gNode node with its configuration'
}

--
-- GNODE_NODE_DEREGISTER: Remove a node from the registry
--
server.register_function{
    function_name = 'GNODE_NODE_DEREGISTER',
    callback = function(keys, args)
        if not args[1] or args[1] == '' then
            return server.error_reply("Node ID required")
        end

        local node_id = args[1]
        local now = server.call('TIME')[1]

        -- Get node type before deletion
        local config_key = 'gnode:node:' .. node_id .. ':config'
        local node_type = server.call('HGET', config_key, 'node_type') or 'unknown'
        local site_id = server.call('HGET', config_key, 'site_id') or 'default'

        -- Mark as inactive (don't delete config, for history)
        server.call('HSET', config_key, 'status', 'inactive')
        server.call('HSET', config_key, 'deregistered_at', now)

        -- Remove from registries
        server.call('SREM', 'gnode:nodes:registry', node_id)
        server.call('SREM', 'gnode:nodes:by_type:' .. node_type, node_id)

        -- Publish deregistration event
        local broadcast_stream = site_id .. ':gnode:broadcast:global'
        pcall(function()
            server.call('XADD', broadcast_stream, 'MAXLEN', '~', 1000, '*',
                't', 'node_deregistered',
                'ss', site_id,
                'ts', tostring(now),
                'node_id', node_id,
                'node_type', node_type
            )
        end)

        return server.status_reply("OK")
    end,
    description = 'Deregisters a gNode node from the cluster'
}

--
-- GNODE_NODE_HEARTBEAT: Update node health status (high-frequency)
--
-- Args:
--   node_id: Node identifier
--   load_factor: Current load (0.0-1.0)
--   cpu_usage: CPU usage percentage (0.0-1.0) [optional]
--   memory_usage: Memory usage percentage (0.0-1.0) [optional]
--   active_requests: Number of active requests [optional]
--   avg_latency_ms: Average latency in milliseconds [optional]
--
server.register_function{
    function_name = 'GNODE_NODE_HEARTBEAT',
    callback = function(keys, args)
        if not args[1] or args[1] == '' then
            return server.error_reply("Node ID required")
        end

        local node_id = args[1]
        local load_factor = tonumber(args[2]) or 0
        local cpu_usage = tonumber(args[3])
        local memory_usage = tonumber(args[4])
        local active_requests = tonumber(args[5])
        local avg_latency_ms = tonumber(args[6])

        local now = server.call('TIME')[1]

        local health_key = 'gnode:node:' .. node_id .. ':health'
        local metrics_key = 'gnode:node:' .. node_id .. ':metrics'

        -- Update health
        server.call('HMSET', health_key,
            'status', 'healthy',
            'last_heartbeat', now
        )
        server.call('HINCRBY', health_key, 'heartbeat_count', 1)

        -- Update metrics
        server.call('HSET', metrics_key, 'load_factor', load_factor)
        server.call('HSET', metrics_key, 'last_updated', now)

        if cpu_usage then
            server.call('HSET', metrics_key, 'cpu_usage', cpu_usage)
        end
        if memory_usage then
            server.call('HSET', metrics_key, 'memory_usage', memory_usage)
        end
        if active_requests then
            server.call('HSET', metrics_key, 'active_requests', active_requests)
        end
        if avg_latency_ms then
            server.call('HSET', metrics_key, 'avg_latency_ms', avg_latency_ms)
        end

        -- Return the heartbeat count as a plain integer. NOTE: `{ integer = N }`
        -- is NOT a valid ValKey/Redis Lua return shape — table conversion only
        -- reads integer-indexed elements [1..], so a lone string key converts to
        -- an empty array, which the daemon's i64 read rejects ("array([])").
        return server.call('HINCRBY', health_key, 'heartbeat_count', 0)
    end,
    description = 'Updates node health status via heartbeat'
}

--
-- GNODE_NODE_RECORD_METRICS: Record command processing metrics
--
-- Args:
--   node_id: Node identifier
--   commands_processed: Number of commands processed in batch
--   commands_failed: Number of failed commands [optional]
--   latency_ms: Processing latency in ms [optional]
--   bytes_in: Bytes received [optional]
--   bytes_out: Bytes sent [optional]
--
server.register_function{
    function_name = 'GNODE_NODE_RECORD_METRICS',
    callback = function(keys, args)
        if not args[1] or args[1] == '' then
            return server.error_reply("Node ID required")
        end

        local node_id = args[1]
        local commands_processed = tonumber(args[2]) or 0
        local commands_failed = tonumber(args[3]) or 0
        local latency_ms = tonumber(args[4])
        local bytes_in = tonumber(args[5])
        local bytes_out = tonumber(args[6])

        local now = server.call('TIME')[1]
        local metrics_key = 'gnode:node:' .. node_id .. ':metrics'

        -- Increment counters
        server.call('HINCRBY', metrics_key, 'commands_processed', commands_processed)
        if commands_failed > 0 then
            server.call('HINCRBY', metrics_key, 'commands_failed', commands_failed)
        end
        if bytes_in then
            server.call('HINCRBY', metrics_key, 'bytes_in', bytes_in)
        end
        if bytes_out then
            server.call('HINCRBY', metrics_key, 'bytes_out', bytes_out)
        end

        -- Update latency with rolling average
        if latency_ms then
            local current = tonumber(server.call('HGET', metrics_key, 'avg_latency_ms')) or 0
            local count = tonumber(server.call('HGET', metrics_key, 'latency_samples')) or 0

            -- Exponential moving average (alpha = 0.1)
            local new_avg
            if count == 0 then
                new_avg = latency_ms
            else
                new_avg = current * 0.9 + latency_ms * 0.1
            end

            server.call('HSET', metrics_key, 'avg_latency_ms', new_avg)
            server.call('HINCRBY', metrics_key, 'latency_samples', 1)
        end

        server.call('HSET', metrics_key, 'last_updated', now)

        return server.status_reply("OK")
    end,
    description = 'Records command processing metrics for a node'
}

--
-- GNODE_NODE_GET_INFO: Getnode information
--
server.register_function{
    function_name = 'GNODE_NODE_GET_INFO',
    callback = function(keys, args)
        if not args[1] or args[1] == '' then
            return server.error_reply("Node ID required")
        end

        local node_id = args[1]

        local config_key = 'gnode:node:' .. node_id .. ':config'
        local health_key = 'gnode:node:' .. node_id .. ':health'
        local metrics_key = 'gnode:node:' .. node_id .. ':metrics'

        -- Check if node exists
        if server.call('EXISTS', config_key) == 0 then
            return server.error_reply("Node not found: " .. node_id)
        end

        -- Get all data
        local config = server.call('HGETALL', config_key)
        local health = server.call('HGETALL', health_key)
        local metrics = server.call('HGETALL', metrics_key)

        -- Build response
        local response = {
            node_id = node_id,
            config = {},
            health = {},
            metrics = {}
        }

        -- Convert arrays to maps
        for i = 1, #config, 2 do
            response.config[config[i]] = config[i+1]
        end
        for i = 1, #health, 2 do
            response.health[health[i]] = health[i+1]
        end
        for i = 1, #metrics, 2 do
            local v = tonumber(metrics[i+1])
            response.metrics[metrics[i]] = v or metrics[i+1]
        end

        local json = safe_json_encode(response)
        return json
    end,
    flags = {'no-writes'},
    description = 'Gets information about a node'
}

--
-- GNODE_NODE_GET_TOPOLOGY: Get all registered nodes and their status
--
server.register_function{
    function_name = 'GNODE_NODE_GET_TOPOLOGY',
    callback = function(keys, args)
        local include_metrics = args[1] == 'true' or args[1] == '1'
        local filter_type = args[2] -- optional: filter by node type

        local registry_key = 'gnode:nodes:registry'
        local now = server.call('TIME')[1]

        -- Get all node IDs
        local node_ids
        if filter_type and filter_type ~= '' then
            node_ids = server.call('SMEMBERS', 'gnode:nodes:by_type:' .. filter_type)
        else
            node_ids = server.call('SMEMBERS', registry_key)
        end

        local topology = {
            timestamp = now,
            node_count = #node_ids,
            nodes = {}
        }

        -- Collect info for each node
        for _, node_id in ipairs(node_ids) do
            local config_key = 'gnode:node:' .. node_id .. ':config'
            local health_key = 'gnode:node:' .. node_id .. ':health'

            local node = {
                node_id = node_id,
                node_type = server.call('HGET', config_key, 'node_type'),
                site_id = server.call('HGET', config_key, 'site_id'),
                hostname = server.call('HGET', config_key, 'hostname'),
                ip_address = server.call('HGET', config_key, 'ip_address'),
                status = server.call('HGET', config_key, 'status'),
                registered_at = server.call('HGET', config_key, 'registered_at'),
                health_status = server.call('HGET', health_key, 'status'),
                last_heartbeat = server.call('HGET', health_key, 'last_heartbeat')
            }

            -- Check if node is stale (no heartbeat in 60 seconds)
            local last_hb = tonumber(node.last_heartbeat) or 0
            if now - last_hb > 60 then
                node.health_status = 'stale'
            end

            -- Include metrics if requested
            if include_metrics then
                local metrics_key = 'gnode:node:' .. node_id .. ':metrics'
                node.load_factor = tonumber(server.call('HGET', metrics_key, 'load_factor')) or 0
                node.cpu_usage = tonumber(server.call('HGET', metrics_key, 'cpu_usage')) or 0
                node.memory_usage = tonumber(server.call('HGET', metrics_key, 'memory_usage')) or 0
                node.commands_processed = tonumber(server.call('HGET', metrics_key, 'commands_processed')) or 0
            end

            table.insert(topology.nodes, node)
        end

        -- Group by type for summary
        topology.by_type = {}
        for _, node in ipairs(topology.nodes) do
            local t = node.node_type or 'unknown'
            topology.by_type[t] = (topology.by_type[t] or 0) + 1
        end

        local json = safe_json_encode(topology)
        return json
    end,
    flags = {'no-writes'},
    description = 'Gets topology of all registered nodes'
}

--
-- GNODE_NODE_CLEANUP_STALE: Remove stale nodes from registry
--
server.register_function{
    function_name = 'GNODE_NODE_CLEANUP_STALE',
    callback = function(keys, args)
        local stale_threshold = tonumber(args[1]) or 300  -- 5 minutes default
        local dry_run = args[2] == 'true' or args[2] == '1'

        local now = server.call('TIME')[1]
        local registry_key = 'gnode:nodes:registry'
        local node_ids = server.call('SMEMBERS', registry_key)

        local stale_nodes = {}
        local cleaned = 0

        for _, node_id in ipairs(node_ids) do
            local health_key = 'gnode:node:' .. node_id .. ':health'
            local last_hb = tonumber(server.call('HGET', health_key, 'last_heartbeat')) or 0

            if now - last_hb > stale_threshold then
                table.insert(stale_nodes, node_id)

                if not dry_run then
                    -- Get node type for removal from type index
                    local config_key = 'gnode:node:' .. node_id .. ':config'
                    local node_type = server.call('HGET', config_key, 'node_type') or 'unknown'

                    -- Mark as stale and remove from active registries
                    server.call('HSET', config_key, 'status', 'stale')
                    server.call('SREM', registry_key, node_id)
                    server.call('SREM', 'gnode:nodes:by_type:' .. node_type, node_id)

                    cleaned = cleaned + 1
                end
            end
        end

        local response = {
            stale_threshold_secs = stale_threshold,
            stale_count = #stale_nodes,
            cleaned_count = cleaned,
            dry_run = dry_run,
            stale_nodes = stale_nodes
        }

        return safe_json_encode(response)
    end,
    description = 'Cleans up stale nodes from the registry'
}

--
-- GNODE_NODE_LIST_TYPES: List all node types and their counts
--
server.register_function{
    function_name = 'GNODE_NODE_LIST_TYPES',
    callback = function(keys, args)
        -- Scan for all type keys
        local cursor = "0"
        local types = {}

        repeat
            local result = server.call('SCAN', cursor, 'MATCH', 'gnode:nodes:by_type:*', 'COUNT', 100)
            cursor = result[1]
            local batch = result[2]

            for _, key in ipairs(batch) do
                local node_type = key:match('gnode:nodes:by_type:(.+)$')
                if node_type then
                    local count = server.call('SCARD', key)
                    types[node_type] = count
                end
            end
        until cursor == "0"

        -- Also include global stats
        local total = server.call('SCARD', 'gnode:nodes:registry')

        local response = {
            total_nodes = total,
            types = types
        }

        return safe_json_encode(response)
    end,
    flags = {'no-writes'},
    description = 'Lists all node types and their counts'
}

--
-- GNODE_NODE_STORE_CONFIG: Store node type configuration (from YAML)
--
-- This is called by the master node to store a node type's configuration
-- so that worker nodes can fetch it without local YAML files.
--
server.register_function{
    function_name = 'GNODE_NODE_STORE_CONFIG',
    callback = function(keys, args)
        if not args[1] or args[1] == '' then
            return server.error_reply("Node type required")
        end
        if not args[2] or args[2] == '' then
            return server.error_reply("Config JSON required")
        end

        local node_type = args[1]
        local config_json = args[2]

        local now = server.call('TIME')[1]

        -- Store the config
        local config_key = 'gnode:node_config:' .. node_type
        server.call('SET', config_key, config_json)

        -- Update the list of available node types
        server.call('SADD', 'gnode:node_config:_types', node_type)

        -- Store metadata
        server.call('HSET', 'gnode:node_config:_metadata:' .. node_type,
            'stored_at', now,
            'size_bytes', #config_json
        )

        return server.status_reply("OK")
    end,
    description = 'Stores node type configuration from YAML'
}

--
-- GNODE_NODE_FETCH_CONFIG: Fetch node type configuration
--
server.register_function{
    function_name = 'GNODE_NODE_FETCH_CONFIG',
    callback = function(keys, args)
        if not args[1] or args[1] == '' then
            return server.error_reply("Node type required")
        end

        local node_type = args[1]
        local config_key = 'gnode:node_config:' .. node_type

        local config = server.call('GET', config_key)
        if not config then
            return server.error_reply("Config not found for node type: " .. node_type)
        end

        return config
    end,
    flags = {'no-writes'},
    description = 'Fetches node type configuration'
}

--
-- GNODE_NODE_LIST_CONFIGS: List all available node type configurations
--
server.register_function{
    function_name = 'GNODE_NODE_LIST_CONFIGS',
    callback = function(keys, args)
        local types = server.call('SMEMBERS', 'gnode:node_config:_types')

        local configs = {}
        for _, node_type in ipairs(types) do
            local meta_key = 'gnode:node_config:_metadata:' .. node_type
            configs[node_type] = {
                stored_at = server.call('HGET', meta_key, 'stored_at'),
                size_bytes = tonumber(server.call('HGET', meta_key, 'size_bytes')) or 0
            }
        end

        return safe_json_encode({
            count = #types,
            configs = configs
        })
    end,
    flags = {'no-writes'},
    description = 'Lists all available node type configurations'
}

--
-- GNODE_NODE_AGGREGATE_METRICS: Aggregate metrics across all nodes or by type
--
server.register_function{
    function_name = 'GNODE_NODE_AGGREGATE_METRICS',
    callback = function(keys, args)
        local filter_type = args[1]  -- optional: filter by node type

        local registry_key = 'gnode:nodes:registry'
        local now = server.call('TIME')[1]

        -- Get node IDs
        local node_ids
        if filter_type and filter_type ~= '' then
            node_ids = server.call('SMEMBERS', 'gnode:nodes:by_type:' .. filter_type)
        else
            node_ids = server.call('SMEMBERS', registry_key)
        end

        -- Aggregate metrics
        local agg = {
            node_count = #node_ids,
            active_count = 0,
            total_commands = 0,
            total_failed = 0,
            total_bytes_in = 0,
            total_bytes_out = 0,
            avg_load = 0,
            avg_cpu = 0,
            avg_memory = 0,
            avg_latency_ms = 0
        }

        local load_sum, cpu_sum, mem_sum, lat_sum = 0, 0, 0, 0
        local active_for_avg = 0

        for _, node_id in ipairs(node_ids) do
            local health_key = 'gnode:node:' .. node_id .. ':health'
            local metrics_key = 'gnode:node:' .. node_id .. ':metrics'

            local last_hb = tonumber(server.call('HGET', health_key, 'last_heartbeat')) or 0
            if now - last_hb <= 60 then
                agg.active_count = agg.active_count + 1
                active_for_avg = active_for_avg + 1

                load_sum = load_sum + (tonumber(server.call('HGET', metrics_key, 'load_factor')) or 0)
                cpu_sum = cpu_sum + (tonumber(server.call('HGET', metrics_key, 'cpu_usage')) or 0)
                mem_sum = mem_sum + (tonumber(server.call('HGET', metrics_key, 'memory_usage')) or 0)
                lat_sum = lat_sum + (tonumber(server.call('HGET', metrics_key, 'avg_latency_ms')) or 0)
            end

            agg.total_commands = agg.total_commands + (tonumber(server.call('HGET', metrics_key, 'commands_processed')) or 0)
            agg.total_failed = agg.total_failed + (tonumber(server.call('HGET', metrics_key, 'commands_failed')) or 0)
            agg.total_bytes_in = agg.total_bytes_in + (tonumber(server.call('HGET', metrics_key, 'bytes_in')) or 0)
            agg.total_bytes_out = agg.total_bytes_out + (tonumber(server.call('HGET', metrics_key, 'bytes_out')) or 0)
        end

        if active_for_avg > 0 then
            agg.avg_load = load_sum / active_for_avg
            agg.avg_cpu = cpu_sum / active_for_avg
            agg.avg_memory = mem_sum / active_for_avg
            agg.avg_latency_ms = lat_sum / active_for_avg
        end

        agg.timestamp = now
        agg.filter_type = filter_type or 'all'

        return safe_json_encode(agg)
    end,
    flags = {'no-writes'},
    description = 'Aggregates metrics across nodes'
}
