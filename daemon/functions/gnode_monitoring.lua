#!lua name=gnode_monitoring

--
-- gNode MONITORING Functions
-- A ValKey function library for monitoring operations
--
-- This is a port of the gCore Cache Scripts to ValKey functions
-- with enhancements for RESP3 compatibility
--

-- Note: PRNG initialization moved to lazy init inside functions
-- server.call() cannot be used at module load time in ValKey functions
local prng_initialized = false
local function ensure_prng_init()
    if not prng_initialized then
        math.randomseed(os.time() + os.clock() * 1000000)
        prng_initialized = true
    end
end 


-- Helper function for metric tracking (used by other functions)
local function track_metric(site_id, metric_type, value, extra)
    -- Site-specific metrics
    local site_metrics = '{' .. site_id .. '}:metrics'
    server.call('HINCRBY', site_metrics, metric_type, value or 1)
    
    -- Store additional metric data if provided
    if extra then
        -- Convert complex objects to JSON
        local success, extra_json = pcall(cjson.encode, extra)
        if success and extra_json then
            local details_key = site_metrics .. ':details:' .. metric_type
            server.call('LPUSH', details_key, extra_json)
            server.call('LTRIM', details_key, 0, 999)  -- Keep last 1000 entries
        end
    end
    
    -- Global metrics if enabled
    if server.call('GET', 'global_metrics_enabled') == '1' then
        server.call('HINCRBY', '{global}:metrics', metric_type, value or 1)
    end
    
    -- Performance tracking
    if metric_type:match('^latency:') then
        local latency_key = '{' .. site_id .. '}:latency'
        server.call('ZADD', latency_key, value, tostring(server.call('TIME')[1]))
        server.call('ZREMRANGEBYRANK', latency_key, 0, -10001)  -- Keep last 10K samples
    end
end

-- Register metric tracking function (mirroring CacheScriptsMonitoring::TRACK_METRIC)
server.register_function{
    function_name = 'GNODE_MONITORING_TRACK_METRIC',
    callback = function(keys, args)
        -- Input validation
        if not args[1] or args[1] == '' then
            return server.error_reply("Site ID required")
        end
        if not args[2] then
            return server.error_reply("Metric type required")
        end
        
        local site_id = args[1]
        local metric_type = args[2]
        local value = tonumber(args[3]) or 1
        local extra_json = args[4]
        
        -- Parse extra data if provided
        local extra = nil
        if extra_json then
            local success, parsed = pcall(cjson.decode, extra_json)
            if success then
                extra = parsed
            else
                return server.error_reply("Invalid JSON for extra data")
            end
        end
        
        -- Track the metric
        track_metric(site_id, metric_type, value, extra)
        
        return server.status_reply("OK")
    end,
    description = 'Tracks a metric with optional additional data'
}

-- Register metrics aggregation function (mirroring CacheScriptsMonitoring::METRICS_AGGREGATE)
server.register_function{
    function_name = 'GNODE_MONITORING_METRICS_AGGREGATE',
    callback = function(keys, args)
        -- Input validation
        if not args[1] then
            return server.error_reply("Site ID required")
        end
        
        local site_id = args[1]
        local window = tonumber(args[2] or 300)  -- Default window of 5 minutes
        
        -- Track operation timing
        local now = server.call('TIME')[1]
        local start_time = now - window
        
        -- Build metrics keys in site's slot
        local base = '{' .. site_id .. '}:metrics'
        local keys = {
            operations = base .. ':ops',
            latency = base .. ':latency',
            errors = base .. ':errors',
            storage = base .. ':storage'
        }
        
        -- Aggregate operation metrics
        local ops = server.call('HGETALL', keys.operations)
        local op_metrics = {}
        for i = 1, #ops, 2 do
            op_metrics[ops[i]] = tonumber(ops[i + 1])
        end
        
        -- Calculate latency percentiles
        local latencies = server.call('ZRANGEBYSCORE', keys.latency, start_time, now, 'WITHSCORES')
        local latency_values = {}
        for i = 2, #latencies, 2 do
            table.insert(latency_values, tonumber(latencies[i]))
        end
        table.sort(latency_values)
        
        -- RESP3 response structure
        local response = { 
            map = {
                site_id = site_id,
                timestamp = now,
                window = window,
                operations = op_metrics,
                latency = {},
                errors = {},
                storage = {}
            }
        }
        
        -- Add latency percentiles if available
        if #latency_values > 0 then
            response.map.latency = {
                p50 = latency_values[math.ceil(#latency_values * 0.5)] or 0,
                p90 = latency_values[math.ceil(#latency_values * 0.9)] or 0,
                p95 = latency_values[math.ceil(#latency_values * 0.95)] or 0,
                p99 = latency_values[math.ceil(#latency_values * 0.99)] or 0
            }
        end
        
        -- Add errors
        local errors = server.call('HGETALL', keys.errors)
        for i = 1, #errors, 2 do
            response.map.errors[errors[i]] = tonumber(errors[i + 1])
        end
        
        -- Add storage metrics
        response.map.storage = {
            keys = tonumber(server.call('HGET', keys.storage, 'keys') or 0),
            bytes = tonumber(server.call('HGET', keys.storage, 'bytes') or 0)
        }
        
        -- Track aggregation metrics
        track_metric(site_id, 'metrics_aggregated', 1, {
            window = window,
            latency_samples = #latency_values,
            error_types = #errors / 2
        })
        
        -- Store aggregated metrics with TTL
        local agg_key = string.format('{%s}:metrics:agg:%s', site_id, now)

        -- Convert to JSON for storage
        local success, response_json = pcall(cjson.encode, response.map)
        if success and response_json then
            server.call('SET', agg_key, response_json)
            server.call('EXPIRE', agg_key, 86400) -- Keep for 24 hours
        end
        
        return response
    end,
    flags = {},  -- This function modifies data (SET, EXPIRE, track_metric)
    description = 'Aggregates metrics over a time window'
}

-- Register cleanup function (mirroring CacheScriptsMonitoring::CLEANUP)
server.register_function{
    function_name = 'GNODE_MONITORING_CLEANUP',
    callback = function(keys, args)
        -- Input validation
        if not args[1] then
            return server.error_reply("Site ID required")
        end
        
        local site_id = args[1]
        local batch_size = tonumber(args[2] or 1000)
        
        -- Track operation timing
        local start_time = server.call('TIME')[1]
        
        -- RESP3 response structure
        local cleaned = { 
            map = {
                keys = 0,
                locks = 0,
                transactions = 0,
                metrics = 0,
                latency = 0,
                duration = 0
            }
        }
        
        -- Build base key in site's slot
        local base = '{' .. site_id .. '}'
        
        -- Cleanup expired locks
        local lock_pattern = base .. ':lock:*'
        local cursor = '0'
        repeat
            local result = server.call('SCAN', cursor, 'MATCH', lock_pattern, 'COUNT', batch_size)
            cursor = result[1]
            local keys = result[2]
            
            for _, key in ipairs(keys) do
                if server.call('TTL', key) <= 0 then
                    server.call('DEL', key)
                    cleaned.map.locks = cleaned.map.locks + 1
                end
            end
        until cursor == '0'
        
        -- Cleanup stale transactions
        local tx_pattern = base .. ':tx:*'
        cursor = '0'
        repeat
            local result = server.call('SCAN', cursor, 'MATCH', tx_pattern, 'COUNT', batch_size)
            cursor = result[1]
            local keys = result[2]
            
            for _, key in ipairs(keys) do
                if server.call('TTL', key) <= 0 then
                    server.call('DEL', key)
                    cleaned.map.transactions = cleaned.map.transactions + 1
                end
            end
        until cursor == '0'
        
        -- Trim metrics data
        local metrics_base = base .. ':metrics'
        
        -- Keep last 24 hours of detailed metrics
        local removed = server.call('ZREMRANGEBYSCORE', metrics_base .. ':latency', 0, start_time - 86400)
        cleaned.map.latency = removed
        cleaned.map.metrics = server.call('ZCARD', metrics_base .. ':latency')
        
        -- Calculate cleanup duration
        cleaned.map.duration = server.call('TIME')[1] - start_time
        
        -- Track cleanup metrics
        track_metric(site_id, 'cleanup_executed', 1, {
            duration = cleaned.map.duration,
            locks_cleaned = cleaned.map.locks,
            transactions_cleaned = cleaned.map.transactions,
            metrics_trimmed = cleaned.map.latency
        })
        
        return cleaned
    end,
    description = 'Performs system cleanup operations'
}

-- Register health check function (mirroring CacheScriptsMonitoring::HEALTH_CHECK)
server.register_function{
    function_name = 'GNODE_MONITORING_HEALTH_CHECK',
    callback = function(keys, args)
        -- Input validation
        if not args[1] then
            return server.error_reply("Site ID required")
        end
        
        local site_id = args[1]
        
        -- Track operation timing
        local start_time = server.call('TIME')[1]
        
        -- Build base key in site's slot
        local base = '{' .. site_id .. '}'
        
        -- RESP3 response structure
        local health = {
            map = {
                status = 'healthy',
                timestamp = start_time,
                checks = {},
                metrics = {}
            }
        }
        
        -- Check basic connectivity
        health.map.checks.connectivity = {
            status = 'ok',
            latency = 0
        }
        
        -- Check storage metrics
        health.map.metrics.storage = {
            keys = tonumber(server.call('HGET', base .. ':metrics:storage', 'keys') or 0),
            bytes = tonumber(server.call('HGET', base .. ':metrics:storage', 'bytes') or 0)
        }
        
        -- Check recent errors
        local recent_errors = server.call('ZCOUNT', base .. ':errors', start_time - 300, start_time)
        health.map.checks.errors = {
            status = recent_errors > 100 and 'warning' or 'ok',
            count = recent_errors
        }
        
        -- Check lock status
        local active_locks = server.call('HLEN', base .. ':locks:active')
        health.map.checks.locks = {
            status = active_locks > 1000 and 'warning' or 'ok',
            count = active_locks
        }
        
        -- Check transaction status
        local active_tx = server.call('HLEN', base .. ':transactions:active')
        health.map.checks.transactions = {
            status = active_tx > 100 and 'warning' or 'ok',
            count = active_tx
        }
        
        -- Get operation rates
        local ops = server.call('HGETALL', base .. ':metrics:ops')
        health.map.metrics.operations = {}
        for i = 1, #ops, 2 do
            health.map.metrics.operations[ops[i]] = tonumber(ops[i + 1])
        end
        
        -- Calculate operation rate
        local window_ops = server.call('ZCOUNT', base .. ':metrics:ops:history', start_time - 60, start_time)
        health.map.metrics.operation_rate = window_ops / 60.0
        
        -- Get recent latencies
        local latencies = server.call('ZRANGEBYSCORE', base .. ':metrics:latency', start_time - 60, start_time, 'WITHSCORES')
        if #latencies > 0 then
            local values = {}
            for i = 2, #latencies, 2 do
                table.insert(values, tonumber(latencies[i]))
            end
            table.sort(values)
            
            -- Add latency metrics
            health.map.metrics.latency = {
                p50 = values[math.ceil(#values * 0.5)] or 0,
                p95 = values[math.ceil(#values * 0.95)] or 0,
                p99 = values[math.ceil(#values * 0.99)] or 0
            }
            
            -- Set status based on latency
            if health.map.metrics.latency.p95 > 100 then
                health.map.checks.latency = {
                    status = 'warning',
                    threshold = 100
                }
            else
                health.map.checks.latency = {
                    status = 'ok',
                    threshold = 100
                }
            end
        end
        
        -- Check circuit breakers
        local breakers = server.call('HGETALL', base .. ':circuit_breakers')
        health.map.checks.circuit_breakers = {
            status = 'ok',
            open = 0
        }
        
        for i = 1, #breakers, 2 do
            local state = breakers[i + 1]
            if state == 'open' then
                health.map.checks.circuit_breakers.open = health.map.checks.circuit_breakers.open + 1
            end
        end
        
        if health.map.checks.circuit_breakers.open > 0 then
            health.map.checks.circuit_breakers.status = 'warning'
        end
        
        -- Set overall status based on checks
        for check_name, check in pairs(health.map.checks) do
            if check.status == 'warning' then
                health.map.status = 'degraded'
                break
            end
        end
        
        -- Update health check metrics
        track_metric(site_id, 'health_check', 1, {
            status = health.map.status,
            duration = server.call('TIME')[1] - start_time
        })
        
        return { map = health.map }
    end,
    flags = {},  -- Tracks health check metrics (writes via track_metric)
    description = 'Performs a health check'
}

-- Register error tracking function (mirroring CacheScriptsMonitoring::TRACK_ERROR)
server.register_function{
    function_name = 'GNODE_MONITORING_TRACK_ERROR',
    callback = function(keys, args)
        -- Input validation
        if not args[1] then
            return server.error_reply("Site ID required")
        end
        if not args[2] then
            return server.error_reply("Error type required")
        end
        if not args[3] then
            return server.error_reply("Error message required")
        end
        
        local site_id = args[1]
        local error_type = args[2]
        local message = args[3]
        local context_json = args[4]
        
        -- Parse context if provided
        local context = {}
        if context_json then
            local success, parsed = pcall(cjson.decode, context_json)
            if success then
                context = parsed
            end
        end
        
        -- Track operation timing
        ensure_prng_init()
        local _t = server.call('TIME')
        local start_time = _t[1] * 1000000 + _t[2]

        -- Build error tracking keys in site's slot
        local base = '{' .. site_id .. '}:errors'
        local keys = {
            recent = base .. ':recent',
            counts = base .. ':counts',
            details = base .. ':details'
        }
        
        -- Create error entry
        local error_id = site_id .. ':' .. start_time .. ':' .. string.format("%x", math.random(1000000))
        local error_data = {
            id = error_id,
            type = error_type,
            message = message,
            context = context,
            timestamp = start_time
        }
        
        -- Convert to JSON for storage
        local success, error_data_json = pcall(cjson.encode, error_data)
        if not success or not error_data_json then
            return server.error_reply("Failed to encode error data: " .. tostring(error_data_json))
        end
        
        -- Track recent errors with automatic cleanup
        server.call('ZADD', keys.recent, start_time, error_id)
        server.call('ZREMRANGEBYRANK', keys.recent, 0, -1001)  -- Keep last 1000
        
        -- Store error details
        server.call('HSET', keys.details, error_id, error_data_json)
        server.call('EXPIRE', keys.details, 86400)  -- Keep for 24 hours
        
        -- Update error counts
        server.call('HINCRBY', keys.counts, error_type, 1)
        
        -- Track in metrics
        local _t2 = server.call('TIME')
        local elapsed = _t2[1] * 1000000 + _t2[2] - start_time
        track_metric(site_id, 'errors', 1, {
            type = error_type,
            latency = elapsed
        })
        
        -- RESP3 response structure
        local response = { verbatim_string = {
            format = "txt",
            string = error_id
        }}
        
        return response
    end,
    description = 'Tracks an error with optional context'
}

-- Register scan cluster function (mirroring CacheScriptsMonitoring::SCAN_CLUSTER)
server.register_function{
    function_name = 'GNODE_MONITORING_SCAN_CLUSTER',
    callback = function(keys, args)
        -- Input validation
        if not args[1] then
            return server.error_reply("Site ID required")
        end
        if not args[2] then
            return server.error_reply("Pattern required")
        end
        
        local site_id = args[1]
        local pattern = args[2]
        local cursor = tonumber(args[3] or 0)
        local count = tonumber(args[4] or 10)
        
        -- Track operation timing
        local _t = server.call('TIME')
        local start_time = _t[1] * 1000000 + _t[2]

        -- Ensure pattern has proper site isolation
        if not pattern:match('^{' .. site_id .. '}') then
            pattern = '{' .. site_id .. '}:' .. pattern
        end
        
        -- Perform scan with count limit
        local result = server.call('SCAN', cursor, 'MATCH', pattern, 'COUNT', count)
        
        -- RESP3 response structure
        local response = {
            map = {
                cursor = result[1],
                keys = {}
            }
        }
        
        -- Convert keys to array
        for _, key in ipairs(result[2]) do
            table.insert(response.map.keys, key)
        end
        
        -- Track scan metrics
        local _t2 = server.call('TIME')
        local end_time = _t2[1] * 1000000 + _t2[2]
        track_metric(site_id, 'scans', 1, {
            keys_found = #result[2],
            cursor = result[1],
            latency = (end_time - start_time) / 1000000.0
        })
        
        return response
    end,
    flags = {},  -- Tracks scan metrics (writes via track_metric)
    description = 'Performs a cluster-safe pattern scan'
}

-- Enhanced function: Get metric history (extends original functionality)
server.register_function{
    function_name = 'GNODE_MONITORING_GET_METRIC_HISTORY',
    callback = function(keys, args)
        -- Input validation
        if not args[1] then
            return server.error_reply("Site ID required")
        end
        if not args[2] then
            return server.error_reply("Metric name required")
        end
        
        local site_id = args[1]
        local metric_name = args[2]
        local count = tonumber(args[3] or 10)
        
        -- Track operation timing
        local start_time = server.call('TIME')[1]
        
        -- Build key in site's slot
        local details_key = '{' .. site_id .. '}:metrics:details:' .. metric_name
        
        -- Get recent metric details
        local details = server.call('LRANGE', details_key, 0, count - 1)
        
        -- RESP3 response structure
        local response = {
            map = {
                metric = metric_name,
                site_id = site_id,
                history = {},
                count = #details
            }
        }
        
        -- Parse each detail entry
        for _, detail_json in ipairs(details) do
            local success, detail = pcall(cjson.decode, detail_json)
            if success and detail then
                table.insert(response.map.history, detail)
            end
        end
        
        -- Get current metric value
        local metrics_key = '{' .. site_id .. '}:metrics'
        local current_value = server.call('HGET', metrics_key, metric_name)
        if current_value then
            response.map.current_value = tonumber(current_value) or current_value
        end
        
        return response
    end,
    flags = {'no-writes'}, -- This function only reads data
    description = 'Gets detailed history for a specific metric'
}