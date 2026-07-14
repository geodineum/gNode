#!lua name=gnode_site

--
-- gNode SITE Functions
-- A ValKey function library for site operations
--
-- This is a port of the gCore Cache Scripts to ValKey functions
-- with enhancements for RESP3 compatibility
--

-- Note: PRNG initialization moved to lazy init inside functions
-- server.call() cannot be used at module load time in ValKey functions
local prng_initialized = false
local function ensure_prng_init()
    if not prng_initialized then
        -- Use os.time() as fallback seed (available at module load)
        math.randomseed(os.time() + os.clock() * 1000000)
        prng_initialized = true
    end
end

-- Simple JSON utilities with proper error handling
local function safe_json_encode(value)
    -- Use pcall-wrapped cjson.encode for safety
    local success, result = pcall(function()
        return cjson.encode(value)
    end)

    if success and result then
        return result
    else
        return nil, "JSON encoding error: " .. tostring(result)
    end
end

local function safe_json_decode(value)
    if not value then return nil, "No value to decode" end

    -- Use pcall-wrapped cjson.decode for safety
    local success, result = pcall(function()
        return cjson.decode(value)
    end)

    if success and result then
        return result
    else
        return nil, "JSON decoding error: " .. tostring(result)
    end
end

local function track_metric(site_id, metric, value)
    local metrics_key = '{' .. site_id .. '}:metrics'
    server.call('HINCRBY', metrics_key, metric, value or 1)
end

-- Register site registration function (mirroring CacheScriptsSiteManager::SITE_REGISTER)
server.register_function{
    function_name = 'GNODE_SITE_REGISTER',
    callback = function(keys, args)
        -- Input validation
        if not args[1] then
            return server.error_reply("Site ID required")
        end
        if not args[2] then
            return server.error_reply("Config required")
        end

        local site_id = args[1]
        local config_str = args[2]
        
        -- Parse config JSON
        local config, err = safe_json_decode(config_str)
        if not config then
            return server.error_reply(err)
        end
        
        -- Track operation timing
        local start_time = server.call('TIME')[1]
        
        -- Ensure consistent hash slot
        local base_key = '{gcore:sites}'
        local keys = {
            registry = base_key .. ':registry',
            config = base_key .. ':config:' .. site_id,
            metrics = base_key .. ':metrics:' .. site_id
        }
        
        -- Check if site already exists
        if server.call('HEXISTS', keys.registry, site_id) == 1 then
            local current_str = server.call('HGET', keys.registry, site_id)
            local current, err = safe_json_decode(current_str)
            if not current then
                return server.error_reply(err)
            end
            
            if current.status == 'active' then
                return server.error_reply("Site already registered")
            end
        end
        
        -- Register site with metadata
        local site_data = {
            id = site_id,
            status = 'active',
            registered_at = server.call('TIME')[1],
            config = config
        }
        
        local site_json, err = safe_json_encode(site_data)
        if not site_json then
            return server.error_reply(err)
        end
        
        server.call('HSET', keys.registry, site_id, site_json)
        server.call('HMSET', keys.config,
            'quota', config.quota or 1000000,
            'rate_limit', config.rate_limit or 10000,
            'max_keys', config.max_keys or 100000
        )
        
        -- Initialize metrics
        server.call('HMSET', keys.metrics,
            'operations', 0,
            'storage_used', 0,
            'last_active', server.call('TIME')[1]
        )
        
        -- Publish site registration event
        local event = {
            type = 'register',
            site = site_id,
            timestamp = server.call('TIME')[1]
        }
        
        local event_json, err = safe_json_encode(event)
        if not event_json then
            return server.error_reply(err)
        end
        
        server.call('PUBLISH', 'gcore:sites:events', event_json)
        
        -- Return success
        return server.status_reply("OK")
    end,
    description = 'Registers a site with the specified configuration'
}

-- Register rate limiting function (mirroring CacheScriptsSiteManager::RATE_LIMIT)
server.register_function{
    function_name = 'GNODE_SERVICE_RATE_LIMIT',
    callback = function(keys, args)
        -- Input validation
        if not args[1] then
            return server.error_reply("Site ID required")
        end
        if not args[2] then
            return server.error_reply("Operation required")
        end

        local site_id = args[1]
        local operation = args[2]
        local limit = tonumber(args[3] or 1000)  -- Default 1000
        local window = tonumber(args[4] or 60)   -- Default 60 seconds
        
        -- Track operation timing
        local start_time = server.call('TIME')[1]
        local now = start_time
        
        -- Build rate limit key in site's slot
        local rate_key = '{' .. site_id .. '}:ratelimit:' .. operation
        
        -- Get current counter
        local current = tonumber(server.call('GET', rate_key)) or 0
        
        -- Track metrics
        track_metric(site_id, 'ratelimit_checks', 1)

        -- Check if rate limited
        if current >= limit then
            -- Check if window has passed
            local window_start = tonumber(server.call('HGET', rate_key .. ':meta', 'window_start')) or 0
            if now - window_start >= window then
                -- Reset window
                server.call('SET', rate_key, 1)
                server.call('HSET', rate_key .. ':meta', 'window_start', now)
                track_metric(site_id, 'ratelimit_resets', 1)
                -- Allowed after reset
                return 1
            else
                -- Rate limit exceeded
                track_metric(site_id, 'ratelimit_exceeded', 1)
                return 0
            end
        else
            -- Increment counter
            server.call('INCR', rate_key)

            -- Set expiration and window start if new
            if current == 0 then
                server.call('EXPIRE', rate_key, window)
                server.call('HSET', rate_key .. ':meta', 'window_start', now)
            end
        end

        -- Allowed
        return 1
    end,
    flags = {},  -- This function modifies data (INCR, SET, HSET, EXPIRE)
    description = 'Performs rate limiting for the specified operation'
}

-- Register circuit breaker function (mirroring CacheScriptsSiteManager::CIRCUIT_BREAKER)
server.register_function{
    function_name = 'GNODE_SERVICE_CIRCUIT_BREAKER',
    callback = function(keys, args)
        -- Input validation
        if not args[1] then
            return server.error_reply("Site ID required")
        end
        if not args[2] then
            return server.error_reply("Service required")
        end

        local site_id = args[1]
        local service = args[2]
        local threshold = tonumber(args[3] or 5)    -- Default 5
        local window = tonumber(args[4] or 60)      -- Default 60 seconds
        local reset_timeout = tonumber(args[5] or 300)  -- Default 300 seconds
        
        -- Track operation timing
        local start_time = server.call('TIME')[1]
        local now = start_time
        
        -- Build circuit breaker keys in site's slot
        local base = '{' .. site_id .. '}:circuit:' .. service
        local keys = {
            state = base .. ':state',
            failures = base .. ':failures',
            metrics = base .. ':metrics'
        }
        
        -- Track metrics
        track_metric(site_id, 'circuit_checks', 1)
        
        -- Get current state
        local state = server.call('GET', keys.state) or 'closed'
        
        -- RESP3 response structure
        local response = { map = {
            allowed = true,
            state = state
        }}
        
        if state == 'open' then
            -- Check if reset timeout has passed
            local last_opened = tonumber(server.call('HGET', keys.metrics, 'last_opened')) or 0
            if now - last_opened >= reset_timeout then
                -- Move to half-open state
                server.call('SET', keys.state, 'half-open')
                server.call('HSET', keys.metrics, 'transitions', 
                    tonumber(server.call('HGET', keys.metrics, 'transitions') or 0) + 1
                )
                response.map.state = 'half-open'
                track_metric(site_id, 'circuit_transitions', 1)
            else
                response.map.allowed = false
                track_metric(site_id, 'circuit_blocks', 1)
            end
            
            return response
        end
        
        if state == 'half-open' then
            -- Allow limited traffic through
            local attempts = tonumber(server.call('HGET', keys.metrics, 'half_open_attempts') or 0)
            if attempts >= 3 then
                response.map.allowed = false
                track_metric(site_id, 'circuit_blocks', 1)
            else
                server.call('HINCRBY', keys.metrics, 'half_open_attempts', 1)
                track_metric(site_id, 'circuit_tests', 1)
            end
            
            return response
        end
        
        -- Circuit is closed, check failure count
        local failures = server.call('ZCOUNT', keys.failures, now - window, '+inf')
        response.map.failures = failures
        
        if failures >= threshold then
            -- Open circuit
            server.call('SET', keys.state, 'open')
            server.call('HSET', keys.metrics,
                'last_opened', now,
                'total_opens', tonumber(server.call('HGET', keys.metrics, 'total_opens') or 0) + 1
            )
            response.map.allowed = false
            response.map.state = 'open'
            track_metric(site_id, 'circuit_opens', 1)
        end
        
        return response
    end,
    flags = {},  -- This function modifies data (SET, HSET, HINCRBY)
    description = 'Checks circuit breaker status for a service'
}

-- Register circuit breaker failure recording function (mirroring CacheScriptsSiteManager::CIRCUIT_BREAKER_RECORD_FAILURE)
server.register_function{
    function_name = 'GNODE_SERVICE_CIRCUIT_RECORD_FAILURE',
    callback = function(keys, args)
        -- Input validation
        if not args[1] then
            return server.error_reply("Service required")
        end
        if not args[2] then
            return server.error_reply("Site ID required")
        end

        local service = args[1]
        local site_id = args[2]
        
        -- Track operation timing
        ensure_prng_init()
        local start_time = server.call('TIME')[1]
        local now = start_time

        -- Build circuit breaker keys
        local base_key = '{' .. site_id .. '}:circuit:' .. service

        -- Update failure counters and timestamp
        server.call('HINCRBY', base_key .. ':stats', 'failures', 1)
        server.call('HSET', base_key .. ':stats', 'last_failure', now)

        -- Get current failure count in window
        local window_key = base_key .. ':failures'
        server.call('ZADD', window_key, now, now .. ':' .. math.random())
        
        -- Set expiration if not set
        server.call('EXPIRE', window_key, 86400)  -- 24 hours
        
        -- Update metrics
        track_metric(site_id, 'circuit_failures', 1)
        
        -- Return the failure count as a plain integer. `{ integer = N }` is NOT
        -- a valid ValKey/Redis Lua return shape (it converts to an empty array,
        -- which an i64 read rejects) — return the number directly.
        local failures = tonumber(server.call('HGET', base_key .. ':stats', 'failures')) or 0
        return failures
    end,
    description = 'Records a failure for circuit breaker tracking'
}

-- Register circuit breaker reset function (mirroring CacheScriptsSiteManager::CIRCUIT_BREAKER_RESET)
server.register_function{
    function_name = 'GNODE_SERVICE_CIRCUIT_RESET',
    callback = function(keys, args)
        -- Input validation
        if not args[1] then
            return server.error_reply("Service required")
        end
        if not args[2] then
            return server.error_reply("Site ID required")
        end

        local service = args[1]
        local site_id = args[2]
        
        -- Track operation timing
        local start_time = server.call('TIME')[1]
        local now = start_time
        
        -- Build circuit breaker keys
        local base_key = '{' .. site_id .. '}:circuit:' .. service
        
        -- Reset all breaker state
        server.call('DEL', base_key .. ':failures')
        server.call('SET', base_key .. ':state', 'closed')
        server.call('HMSET', base_key .. ':stats',
            'failures', 0,
            'tripped', 0,
            'last_failure', 0,
            'reset_time', now,
            'status', 'closed'
        )
        
        -- Reset half-open attempts
        server.call('HSET', base_key .. ':metrics', 'half_open_attempts', 0)
        
        -- Track reset in metrics
        track_metric(site_id, 'circuit_resets', 1)
        server.call('HSET', base_key .. ':history', now, 'reset')
        
        return server.status_reply("OK")
    end,
    description = 'Resets a circuit breaker to closed state'
}

-- Get site information function
server.register_function{
    function_name = 'GNODE_SERVICE_GET_INFO',
    callback = function(keys, args)
        -- Input validation
        if not args[1] then
            return server.error_reply("Site ID required")
        end

        local site_id = args[1]
        
        -- Track operation timing
        local start_time = server.call('TIME')[1]
        
        -- IMPLEMENTATION PRIORITY: First check for data in the client key format
        -- This is the modern format used by the client and should be preferred
        local client_key = 'gnode:site:' .. site_id
        
        -- Log site key check
        server.log(server.LOG_NOTICE, "Checking for site data at key: " .. client_key)
        
        local client_site_exists = server.call('EXISTS', client_key) == 1
        
        if client_site_exists then
            -- Get site data from client format
            local site_data = {}
            local site_fields = server.call('HGETALL', client_key)
            server.log(server.LOG_NOTICE, "Found site data with " .. #site_fields/2 .. " fields")
            
            for i = 1, #site_fields, 2 do
                local key = site_fields[i]
                local value = site_fields[i+1]
                -- Try to convert numeric values
                local num_value = tonumber(value)
                site_data[key] = num_value or value
            end
            
            -- Track metrics
            track_metric(site_id, 'site_info_requests', 1)
            
            -- Build response from client data format
            local response = {
                id = site_id,
                name = site_data.name or site_id,
                status = site_data.status or "unknown",
                created = site_data.created,
                config = site_data
            }
            
            -- Check for nodes
            local node_pattern = 'gnode:site:' .. site_id .. ':node:*'
            
            -- Use SCAN instead of KEYS for better performance in production
            local cursor = "0"
            local node_keys = {}
            
            repeat
                local result = server.call('SCAN', cursor, 'MATCH', node_pattern, 'COUNT', 100)
                cursor = result[1]
                local batch = result[2]
                
                for _, key in ipairs(batch) do
                    table.insert(node_keys, key)
                end
            until cursor == "0"
            
            server.log(server.LOG_NOTICE, "Found " .. #node_keys .. " node keys for site " .. site_id)
            
            if #node_keys > 0 then
                response.nodes = {}
                for _, node_key in ipairs(node_keys) do
                    local node_id = node_key:match('node:([^:]+)$')
                    if node_id then
                        local node_data = {}
                        local node_fields = server.call('HGETALL', node_key)
                        for i = 1, #node_fields, 2 do
                            local key = node_fields[i]
                            local value = node_fields[i+1]
                            -- Try to convert numeric values
                            local num_value = tonumber(value)
                            node_data[key] = num_value or value
                        end
                        response.nodes[node_id] = {
                            id = node_id,
                            name = node_data.name or node_id,
                            status = node_data.status or "unknown",
                            created = node_data.created,
                            ip = node_data.ip
                        }
                    end
                end
                response.node_count = #node_keys
            end
            
            -- Return JSON response
            local json_response, err = safe_json_encode(response)
            if not json_response then
                return server.error_reply("Failed to encode response: " .. (err or "unknown error"))
            end
            
            -- Successfully found and processed client format data, return immediately
            return json_response
        else
            server.log(server.LOG_NOTICE, "Site data not found at key: " .. client_key)
        end
        
        -- Fall back to standard format if client key doesn't exist
        -- Ensure consistent hash slot
        local base_key = '{gcore:sites}'
        local registry_key = base_key .. ':registry'
        local config_key = base_key .. ':config:' .. site_id
        local metrics_key = base_key .. ':metrics:' .. site_id
        
        -- Check if site exists
        if server.call('HEXISTS', registry_key, site_id) == 0 then
            return server.error_reply("Site: not found")
        end
        
        -- Get site data from registry
        local site_data_str = server.call('HGET', registry_key, site_id)
        local site_data, err = safe_json_decode(site_data_str)
        if not site_data then
            return server.error_reply("Failed to decode site data: " .. (err or "unknown error"))
        end
        
        -- Get site configuration
        local config = {}
        local config_fields = server.call('HGETALL', config_key)
        for i = 1, #config_fields, 2 do
            local key = config_fields[i]
            local value = config_fields[i+1]
            -- Try to convert numeric values
            local num_value = tonumber(value)
            config[key] = num_value or value
        end
        
        -- Get site metrics
        local metrics = {}
        local metrics_fields = server.call('HGETALL', metrics_key)
        for i = 1, #metrics_fields, 2 do
            local key = metrics_fields[i]
            local value = metrics_fields[i+1]
            -- Try to convert numeric values
            local num_value = tonumber(value)
            metrics[key] = num_value or value
        end
        
        -- Update last active time
        server.call('HSET', metrics_key, 'last_active', server.call('TIME')[1])
        
        -- Track metrics
        track_metric(site_id, 'site_info_requests', 1)
        
        -- Build response
        local response = {
            id = site_id,
            status = site_data.status or "unknown",
            registered_at = site_data.registered_at,
            config = config,
            metrics = metrics,
            nodes = {},  -- Will be populated if available
            version = site_data.version or "1.0",
            service_count = 0
        }
        
        -- Get node information if available
        local nodes_key = '{' .. site_id .. '}:nodes'
        local node_ids = server.call('SMEMBERS', nodes_key)
        
        for _, node_id in ipairs(node_ids) do
            local node_key = '{' .. site_id .. '}:node:' .. node_id
            local node_data_str = server.call('GET', node_key)
            if node_data_str then
                local node_data, _ = safe_json_decode(node_data_str)
                if node_data then
                    response.nodes[node_id] = node_data
                    
                    -- Count services
                    if node_data.services and type(node_data.services) == "table" then
                        response.service_count = response.service_count + #node_data.services
                    end
                end
            end
        end
        
        -- Get endpoint count if available
        local endpoints_key = '{' .. site_id .. '}:endpoints'
        local endpoint_count = server.call('SCARD', endpoints_key)
        response.endpoint_count = endpoint_count
        
        -- Return JSON response
        local json_response, err = safe_json_encode(response)
        if not json_response then
            return server.error_reply("Failed to encode response: " .. (err or "unknown error"))
        end
        
        return json_response
    end,
    -- Function performs some writes for tracking metrics
    description = 'Gets information about a site'
}

-- Get node information function
server.register_function{
    function_name = 'GNODE_SERVICE_GET_NODE_INFO',
    callback = function(keys, args)
        -- Input validation
        if not args[1] then
            return server.error_reply("Site ID required")
        end
        if not args[2] then
            return server.error_reply("Node ID required")
        end

        local site_id = args[1]
        local node_id = args[2]
        
        -- Track operation timing
        local start_time = server.call('TIME')[1]
        
        -- IMPLEMENTATION PRIORITY: First check for data in client key format
        -- This is the modern format used by the client and should be preferred
        local client_node_key = 'gnode:site:' .. site_id .. ':node:' .. node_id
        
        -- Log node key check
        server.log(server.LOG_NOTICE, "Checking for node data at key: " .. client_node_key)
        
        local client_node_exists = server.call('EXISTS', client_node_key) == 1
        
        if client_node_exists then
            -- Get node data from client format
            local node_data = {}
            local node_fields = server.call('HGETALL', client_node_key)
            server.log(server.LOG_NOTICE, "Found node data with " .. #node_fields/2 .. " fields")
            
            for i = 1, #node_fields, 2 do
                local key = node_fields[i]
                local value = node_fields[i+1]
                -- Log each field for debugging
                server.log(server.LOG_NOTICE, "Node field: " .. key .. " = " .. value)
                -- Try to convert numeric values
                local num_value = tonumber(value)
                node_data[key] = num_value or value
            end
            
            -- Track metrics
            track_metric(site_id, 'node_info_requests', 1)
            
            -- Build response from client data format
            local response = {
                site_id = site_id,
                node_id = node_id,
                name = node_data.name or node_id,
                status = node_data.status or "unknown",
                created = node_data.created,
                ip_address = node_data.ip,
                config = node_data
            }
            
            -- Return JSON response
            local json_response, err = safe_json_encode(response)
            if not json_response then
                return server.error_reply("Failed to encode response: " .. (err or "unknown error"))
            end
            
            -- Successfully found and processed client format data, return immediately
            return json_response
        else
            server.log(server.LOG_NOTICE, "Node data not found at key: " .. client_node_key)
        end
        
        -- Fall back to standard format
        -- Check if site exists
        local base_key = '{gcore:sites}'
        local registry_key = base_key .. ':registry'
        
        if server.call('HEXISTS', registry_key, site_id) == 0 then
            return server.error_reply("Site: not found")
        end
        
        -- Get node data
        local node_key = '{' .. site_id .. '}:node:' .. node_id
        local node_data_str = server.call('GET', node_key)
        
        if not node_data_str then
            return server.error_reply("Node: not found")
        end
        
        local node_data, err = safe_json_decode(node_data_str)
        if not node_data then
            return server.error_reply("Failed to decode node data: " .. (err or "unknown error"))
        end
        
        -- Get node metrics if available
        local metrics_key = '{' .. site_id .. '}:node:' .. node_id .. ':metrics'
        local metrics = {}
        local metrics_fields = server.call('HGETALL', metrics_key)
        for i = 1, #metrics_fields, 2 do
            local key = metrics_fields[i]
            local value = metrics_fields[i+1]
            -- Try to convert numeric values
            local num_value = tonumber(value)
            metrics[key] = num_value or value
        end
        
        -- Update last queried timestamp
        server.call('HSET', metrics_key, 'last_queried', server.call('TIME')[1])
        
        -- Get service details if available
        local services = {}
        if node_data.services and type(node_data.services) == "table" then
            for _, service_id in ipairs(node_data.services) do
                local service_key = '{' .. site_id .. '}:service:' .. service_id
                local service_data_str = server.call('GET', service_key)
                if service_data_str then
                    local service_data, _ = safe_json_decode(service_data_str)
                    if service_data then
                        services[service_id] = service_data
                    end
                end
            end
        end
        
        -- Track metrics
        track_metric(site_id, 'node_info_requests', 1)
        
        -- Build response
        local response = {
            site_id = site_id,
            node_id = node_id,
            status = node_data.status or "unknown",
            registered_at = node_data.registered_at,
            last_seen = node_data.last_seen or 0,
            hostname = node_data.hostname or "unknown",
            ip_address = node_data.ip_address,
            version = node_data.version or "1.0",
            capabilities = node_data.capabilities or {},
            services = services,
            endpoints = node_data.endpoints or {},
            metrics = metrics,
            config = node_data.config or {}
        }
        
        -- Return JSON response
        local json_response, err = safe_json_encode(response)
        if not json_response then
            return server.error_reply("Failed to encode response: " .. (err or "unknown error"))
        end
        
        return json_response
    end,
    -- Function performs some writes for tracking metrics
    description = 'Gets information about a node within a site'
}

-- =============================================================================
-- SITE DISCOVERY FUNCTIONS
-- =============================================================================
-- These functions support daemon stream discovery and multi-site management
-- =============================================================================

--- List all registered sites from the global registry
-- Returns all site IDs that have been registered via GNODE_PROVISION_SERVICE
-- or other registration mechanisms.
--
-- @param args[1] include_meta - Optional: "true" to include site metadata
-- @return JSON array of site IDs or objects with metadata
server.register_function{
    function_name = 'GNODE_SERVICE_LIST_ALL',
    callback = function(keys, args)
        local include_meta = args[1] == "true"

        -- Get all registered sites from the primary registry
        local registry_key = 'gnode:sites:registry'
        local site_ids = server.call('SMEMBERS', registry_key)

        -- Also check the gcore sites registry for backward compatibility
        local gcore_registry = '{gcore:sites}:registry'
        local gcore_sites_raw = server.call('HKEYS', gcore_registry)

        -- Merge into a set to avoid duplicates
        local site_set = {}
        for _, site_id in ipairs(site_ids) do
            site_set[site_id] = true
        end
        for _, site_id in ipairs(gcore_sites_raw) do
            site_set[site_id] = true
        end

        -- Convert set back to list
        local all_sites = {}
        for site_id, _ in pairs(site_set) do
            table.insert(all_sites, site_id)
        end

        -- Sort for consistent ordering
        table.sort(all_sites)

        if not include_meta then
            local result, err = safe_json_encode({
                sites = all_sites,
                count = #all_sites
            })
            if not result then
                return server.error_reply("Failed to encode response: " .. (err or "unknown error"))
            end
            return result
        end

        -- Include metadata for each site
        local sites_with_meta = {}
        for _, site_id in ipairs(all_sites) do
            local site_info = {
                id = site_id,
                status = "unknown",
                environments = cjson.empty_array,  -- Force JSON array encoding
                stream_count = 0
            }

            -- Try to get metadata from gnode:site:{id}:meta
            local meta_key = 'gnode:site:' .. site_id .. ':meta'
            local meta_exists = server.call('EXISTS', meta_key) == 1

            if meta_exists then
                local meta_fields = server.call('HGETALL', meta_key)
                for i = 1, #meta_fields, 2 do
                    local key = meta_fields[i]
                    local value = meta_fields[i+1]

                    if key == 'status' then
                        site_info.status = value
                    elseif key == 'created_at' then
                        site_info.created_at = tonumber(value)
                    elseif key == 'environments' then
                        local ok, envs = pcall(cjson.decode, value)
                        if ok then
                            site_info.environments = envs
                        end
                    end
                end
            end

            -- Count existing streams for this site
            local stream_count = 0
            local environments = {"testing", "staging", "acceptance", "production"}
            local stream_types = {"unified", "health"}

            for _, env in ipairs(environments) do
                for _, stype in ipairs(stream_types) do
                    local stream_key = '{' .. site_id .. '}:gnode:' .. stype .. ':' .. env
                    if server.call('EXISTS', stream_key) == 1 then
                        stream_count = stream_count + 1
                    end
                end
            end

            -- Check broadcast stream
            if server.call('EXISTS', '{' .. site_id .. '}:gnode:broadcast') == 1 then
                stream_count = stream_count + 1
            end

            site_info.stream_count = stream_count

            table.insert(sites_with_meta, site_info)
        end

        local result, err = safe_json_encode({
            sites = sites_with_meta,
            count = #sites_with_meta
        })
        if not result then
            return server.error_reply("Failed to encode response: " .. (err or "unknown error"))
        end
        return result
    end,
    flags = {'no-writes'},
    description = 'Lists all registered sites from the global registry with optional metadata'
}

--- Get streams that the daemon should subscribe to for a given environment
-- This is the primary discovery function for the daemon to find which streams to process.
--
-- Stream Architecture:
--   Per site:
--     {site_id}:gnode:unified:{env}  - unified stream for this environment
--     {site_id}:gnode:health         - health stream (NO environment suffix)
--   Shared (always included):
--     {topology_namespace}:gnode:broadcast:global  - shared broadcast stream
--     {topology_namespace}:gnode:unified           - service registration stream
--     geodineum:unified:stream                   - global network stream
--
-- @param args[1] environment - The DTAP environment to get streams for
-- @param args[2] stream_type - Optional: filter by stream type (unified/health)
-- @param args[3] topology_namespace - Optional: namespace for shared streams (default: "geodineum")
-- @return JSON object with list of stream keys to subscribe to
server.register_function{
    function_name = 'GNODE_SERVICE_GET_DAEMON_STREAMS',
    callback = function(keys, args)
        local environment = args[1]
        local filter_type = args[2]
        local topology_namespace = args[3] or "geodineum"

        if not environment or environment == "" then
            return server.error_reply("Environment required (testing/staging/acceptance/production)")
        end

        -- Get all registered sites
        local registry_key = 'gnode:sites:registry'
        local site_ids = server.call('SMEMBERS', registry_key)

        -- Also check gcore registry
        local gcore_registry = '{gcore:sites}:registry'
        local gcore_sites = server.call('HKEYS', gcore_registry)

        -- Merge into set
        local site_set = {}
        for _, site_id in ipairs(site_ids) do
            site_set[site_id] = true
        end
        for _, site_id in ipairs(gcore_sites) do
            site_set[site_id] = true
        end

        local streams = {
            unified = {},
            health = {},
            broadcast = {},
            registration = {},
            global = {}
        }

        -- For each site, check if the streams exist
        for site_id, _ in pairs(site_set) do
            -- Unified stream: {site_id}:gnode:unified:{environment}
            if not filter_type or filter_type == "" or filter_type == "unified" then
                local unified_key = '{' .. site_id .. '}:gnode:unified:' .. environment
                if server.call('EXISTS', unified_key) == 1 then
                    table.insert(streams.unified, {
                        key = unified_key,
                        site_id = site_id,
                        environment = environment,
                        type = "unified"
                    })
                end
            end

            -- Health stream: {site_id}:gnode:health (NO environment suffix)
            if not filter_type or filter_type == "" or filter_type == "health" then
                local health_key = '{' .. site_id .. '}:gnode:health'
                if server.call('EXISTS', health_key) == 1 then
                    table.insert(streams.health, {
                        key = health_key,
                        site_id = site_id,
                        type = "health"
                    })
                end
            end
        end

        -- Shared broadcast stream (all sites use this one)
        if not filter_type or filter_type == "" or filter_type == "broadcast" then
            local broadcast_key = '{' .. topology_namespace .. '}:gnode:broadcast:global'
            if server.call('EXISTS', broadcast_key) == 1 then
                table.insert(streams.broadcast, {
                    key = broadcast_key,
                    topology_namespace = topology_namespace,
                    type = "broadcast"
                })
            end
        end

        -- Shared registration stream (for service registrations from gNode-Client)
        if not filter_type or filter_type == "" or filter_type == "registration" then
            local registration_key = '{' .. topology_namespace .. '}:gnode:unified'
            if server.call('EXISTS', registration_key) == 1 then
                table.insert(streams.registration, {
                    key = registration_key,
                    topology_namespace = topology_namespace,
                    type = "registration"
                })
            end
        end

        -- Global network stream (future multi-topology coordination)
        if not filter_type or filter_type == "" or filter_type == "global" then
            local global_key = 'geodineum:unified:stream'
            if server.call('EXISTS', global_key) == 1 then
                table.insert(streams.global, {
                    key = global_key,
                    type = "global"
                })
            end
        end

        -- Calculate totals
        local total = #streams.unified + #streams.health + #streams.broadcast + #streams.registration + #streams.global
        local site_count = 0
        for _ in pairs(site_set) do site_count = site_count + 1 end

        local result, err = safe_json_encode({
            environment = environment,
            topology_namespace = topology_namespace,
            streams = streams,
            total_streams = total,
            site_count = site_count
        })
        if not result then
            return server.error_reply("Failed to encode response: " .. (err or "unknown error"))
        end
        return result
    end,
    flags = {'no-writes'},
    description = 'Gets all streams the daemon should subscribe to for a given environment'
}

-- ============================================================================
-- PER-SITE ENVIRONMENT CONFIGURATION
-- ============================================================================

--- Set the active environment for a site
-- Sites can exist in multiple DTAP environments but only ONE is "active" at a time.
-- The daemon subscribes to the site's active environment streams.
--
-- @param args[1] site_id - The site identifier
-- @param args[2] environment - The DTAP environment (testing/staging/acceptance/production)
-- @return JSON with success status
server.register_function{
    function_name = 'GNODE_SERVICE_SET_ENVIRONMENT',
    callback = function(keys, args)
        local site_id = args[1]
        local environment = args[2]

        if not site_id or site_id == "" then
            return server.error_reply("Site ID required")
        end
        if not environment or environment == "" then
            return server.error_reply("Environment required (testing/staging/acceptance/production)")
        end

        -- Validate environment
        local valid_envs = {testing=true, staging=true, acceptance=true, production=true}
        if not valid_envs[environment] then
            return server.error_reply("Invalid environment. Must be: testing, staging, acceptance, or production")
        end

        -- Get old environment before updating (for change detection)
        local meta_key = 'gnode:site:' .. site_id .. ':meta'
        local old_environment = server.call('HGET', meta_key, 'active_environment')
        if not old_environment or old_environment == "" then
            old_environment = "production"  -- default
        end

        local changed = (old_environment ~= environment)
        local timestamp = server.call('TIME')[1]

        -- Store in site meta key
        server.call('HSET', meta_key, 'active_environment', environment)
        server.call('HSET', meta_key, 'environment_updated_at', timestamp)

        -- Ensure the environments array includes this environment
        local existing_envs_raw = server.call('HGET', meta_key, 'environments')
        local envs_set = {}
        local envs_list = {}
        if existing_envs_raw and existing_envs_raw ~= "" then
            local ok, decoded = pcall(cjson.decode, existing_envs_raw)
            if ok and type(decoded) == "table" then
                for _, e in ipairs(decoded) do
                    if not envs_set[e] then
                        envs_set[e] = true
                        table.insert(envs_list, e)
                    end
                end
            end
        end
        if not envs_set[environment] then
            envs_set[environment] = true
            table.insert(envs_list, environment)
        end
        local ok_enc, envs_json = pcall(cjson.encode, envs_list)
        if ok_enc then
            server.call('HSET', meta_key, 'environments', envs_json)
        end

        -- Ensure status is set if missing
        local existing_status = server.call('HGET', meta_key, 'status')
        if not existing_status or existing_status == "" then
            server.call('HSET', meta_key, 'status', 'active')
        end

        -- Also register site if not in registry
        local registry_key = 'gnode:sites:registry'
        server.call('SADD', registry_key, site_id)

        -- If environment changed, broadcast notification for immediate daemon refresh
        if changed then
            local broadcast_key = '{' .. site_id .. '}:gnode:broadcast:global'
            local event_data, err = safe_json_encode({
                type = "environment_changed",
                site_id = site_id,
                old_environment = old_environment,
                new_environment = environment,
                timestamp = timestamp
            })
            if event_data then
                server.call('XADD', broadcast_key, '*',
                    'type', 'environment_changed',
                    'data', event_data)
            end
        end

        local result, err = safe_json_encode({
            ok = true,
            site_id = site_id,
            active_environment = environment,
            changed = changed,
            old_environment = old_environment
        })
        if not result then
            return server.error_reply("Failed to encode response: " .. (err or "unknown"))
        end
        return result
    end,
    description = 'Sets the active DTAP environment for a site. Broadcasts notification if changed.'
}

--- Get the active environment for a site
-- @param args[1] site_id - The site identifier
-- @return JSON with site's active environment (defaults to "production" if not set)
server.register_function{
    function_name = 'GNODE_SERVICE_GET_ENVIRONMENT',
    callback = function(keys, args)
        local site_id = args[1]

        if not site_id or site_id == "" then
            return server.error_reply("Site ID required")
        end

        local meta_key = 'gnode:site:' .. site_id .. ':meta'
        local env = server.call('HGET', meta_key, 'active_environment')

        -- Default to production if not set
        if not env or env == "" then
            env = "production"
        end

        local result, err = safe_json_encode({
            site_id = site_id,
            active_environment = env
        })
        if not result then
            return server.error_reply("Failed to encode response: " .. (err or "unknown"))
        end
        return result
    end,
    flags = {'no-writes'},
    description = 'Gets the active DTAP environment for a site'
}

--- Get ALL daemon streams based on per-site environment configuration
-- This is the PRIMARY function for stream discovery. It returns exactly the streams
-- the daemon should subscribe to:
--
-- Stream Architecture:
--   Per site:
--     {site_id}:gnode:unified:{active_env}  - unified stream for site's active environment
--     {site_id}:gnode:health                - health stream (NO environment suffix)
--   Shared (always included once):
--     {topology_namespace}:gnode:broadcast:global  - shared broadcast stream
--     {topology_namespace}:gnode:unified           - service registration stream
--     geodineum:unified:stream                   - global network stream
--
-- No environment parameter needed - reads each site's active_environment from metadata.
-- This avoids the duplication bug when iterating DTAP environments.
--
-- @param args[1] topology_namespace - Optional: namespace for shared streams (default: "geodineum")
-- @return JSON with all streams organized by type
server.register_function{
    function_name = 'GNODE_SERVICE_GET_ALL_STREAMS',
    callback = function(keys, args)
        local topology_namespace = args[1] or "geodineum"

        -- Get all registered sites from both registries
        local registry_key = 'gnode:sites:registry'
        local site_ids = server.call('SMEMBERS', registry_key)

        local gcore_registry = '{gcore:sites}:registry'
        local gcore_sites = server.call('HKEYS', gcore_registry)

        -- Merge into set (deduplicate)
        local site_set = {}
        for _, site_id in ipairs(site_ids) do
            site_set[site_id] = true
        end
        for _, site_id in ipairs(gcore_sites) do
            site_set[site_id] = true
        end

        local streams = {
            unified = {},
            health = {},
            broadcast = {},
            registration = {},
            global = {}
        }

        local sites_info = {}

        -- For each site, get its active environment and check for streams
        for site_id, _ in pairs(site_set) do
            -- Get site's active environment (default: production)
            local meta_key = 'gnode:site:' .. site_id .. ':meta'
            local active_env = server.call('HGET', meta_key, 'active_environment')
            if not active_env or active_env == "" then
                active_env = "production"
            end

            -- Track site info for response
            sites_info[site_id] = {
                active_environment = active_env,
                streams = {}
            }

            -- Unified stream: {site_id}:gnode:unified:{active_env}
            local unified_key = '{' .. site_id .. '}:gnode:unified:' .. active_env
            if server.call('EXISTS', unified_key) == 1 then
                table.insert(streams.unified, {
                    key = unified_key,
                    site_id = site_id,
                    environment = active_env,
                    type = "unified"
                })
                table.insert(sites_info[site_id].streams, "unified")
            end

            -- Health stream: {site_id}:gnode:health (NO environment suffix)
            local health_key = '{' .. site_id .. '}:gnode:health'
            if server.call('EXISTS', health_key) == 1 then
                table.insert(streams.health, {
                    key = health_key,
                    site_id = site_id,
                    type = "health"
                })
                table.insert(sites_info[site_id].streams, "health")
            end
        end

        -- Shared broadcast stream (all sites use this one)
        local broadcast_key = '{' .. topology_namespace .. '}:gnode:broadcast:global'
        if server.call('EXISTS', broadcast_key) == 1 then
            table.insert(streams.broadcast, {
                key = broadcast_key,
                topology_namespace = topology_namespace,
                type = "broadcast"
            })
        end

        -- Shared registration stream (for service registrations from gNode-Client)
        local registration_key = '{' .. topology_namespace .. '}:gnode:unified'
        if server.call('EXISTS', registration_key) == 1 then
            table.insert(streams.registration, {
                key = registration_key,
                topology_namespace = topology_namespace,
                type = "registration"
            })
        end

        -- Global network stream (future multi-topology coordination)
        local global_key = 'geodineum:unified:stream'
        if server.call('EXISTS', global_key) == 1 then
            table.insert(streams.global, {
                key = global_key,
                type = "global"
            })
        end

        -- Calculate totals
        local total = #streams.unified + #streams.health + #streams.broadcast + #streams.registration + #streams.global
        local site_count = 0
        for _ in pairs(site_set) do
            site_count = site_count + 1
        end

        -- Shared streams (broadcast/registration/global) are CONDITIONAL
        -- singletons — discovery only counts them when they exist. The
        -- expectation must mirror that same rule; a hardcoded +3 made
        -- the daemon's count-mismatch warn fire forever on topologies
        -- where registration/global streams were never created. The
        -- meaningful invariant is per-site: every registered site
        -- contributes exactly unified + health.
        local shared_count = #streams.broadcast + #streams.registration + #streams.global

        local result, err = safe_json_encode({
            topology_namespace = topology_namespace,
            streams = streams,
            total_streams = total,
            site_count = site_count,
            sites = sites_info,
            expected_per_site = 2,  -- unified + health (broadcast is shared)
            expected_shared = shared_count,
            expected_total = (site_count * 2) + shared_count
        })
        if not result then
            return server.error_reply("Failed to encode response: " .. (err or "unknown"))
        end
        return result
    end,
    flags = {'no-writes'},
    description = 'Gets ALL streams for daemon subscription based on per-site environment configuration'
}

-- =============================================================================
-- TENANT GROUP OPERATIONS
-- =============================================================================
-- Lightweight multi-tenant grouping via owner metadata + index SET.
-- Sites with the same owner can discover each other's services through
-- GNODE_TENANT_DISCOVER (daemon-mediated, respects ACL isolation).

-- GNODE_TENANT_LIST_SITES: List all sites belonging to a tenant/owner group.
-- Usage: FCALL GNODE_TENANT_LIST_SITES 0 <owner_id>
-- Returns: {"owner":"acme","sites":["my_app","staging_my_app"],"count":2}
server.register_function{
    function_name = 'GNODE_TENANT_LIST_SITES',
    callback = function(keys, args)
        if not args[1] or args[1] == '' then
            return server.error_reply("Owner ID required")
        end

        local owner_id = args[1]
        local tenant_key = 'gnode:tenant:' .. owner_id .. ':sites'
        local sites = server.call('SMEMBERS', tenant_key)

        local result, err = safe_json_encode({
            owner = owner_id,
            sites = sites,
            count = #sites
        })
        if not result then
            return server.error_reply("Failed to encode response: " .. (err or "unknown"))
        end
        return result
    end,
    flags = {'no-writes'},
    description = 'List all sites belonging to a tenant/owner group'
}

-- GNODE_TENANT_DISCOVER: Cross-site service discovery within a tenant group.
-- Queries the services topology of every site owned by the same tenant,
-- aggregates results, and returns them annotated with site_id.
-- Usage: FCALL GNODE_TENANT_DISCOVER 0 <owner_id> <capabilities_json> [limit]
-- Returns: {"owner":"acme","results":[...],"total":N,"sites_queried":M}
server.register_function{
    function_name = 'GNODE_TENANT_DISCOVER',
    callback = function(keys, args)
        if not args[1] or args[1] == '' then
            return server.error_reply("Owner ID required")
        end
        if not args[2] then
            return server.error_reply("Capabilities JSON required")
        end

        local owner_id = args[1]
        local capabilities_str = args[2]
        local limit = tonumber(args[3]) or 10

        -- Parse capabilities
        local ok, capabilities = pcall(cjson.decode, capabilities_str)
        if not ok or type(capabilities) ~= 'table' then
            return server.error_reply("Invalid capabilities JSON")
        end

        -- Get all sites for this tenant
        local tenant_key = 'gnode:tenant:' .. owner_id .. ':sites'
        local sites = server.call('SMEMBERS', tenant_key)
        if #sites == 0 then
            local result, _ = safe_json_encode({
                owner = owner_id,
                results = {},
                total = 0,
                sites_queried = 0
            })
            return result or '{"owner":"' .. owner_id .. '","results":[],"total":0,"sites_queried":0}'
        end

        -- Query each site's services topology
        local all_results = {}
        local sites_queried = 0

        for _, site_id in ipairs(sites) do
            -- Check if the site has a services topology
            local topo_key = '{' .. site_id .. '}:gnode:services'
            local entities_key = topo_key .. ':entities'

            if server.call('EXISTS', entities_key) == 1 then
                sites_queried = sites_queried + 1

                -- Get all entities for this site
                local entities = server.call('HGETALL', entities_key)
                for i = 1, #entities, 2 do
                    local entity_id = entities[i]
                    local entity_json = entities[i + 1]
                    local parse_ok, entity = pcall(cjson.decode, entity_json)
                    if parse_ok and entity then
                        entity.site_id = site_id
                        entity.entity_id = entity_id
                        table.insert(all_results, entity)
                    end
                end
            end
        end

        -- Trim to limit
        if #all_results > limit then
            local trimmed = {}
            for i = 1, limit do
                trimmed[i] = all_results[i]
            end
            all_results = trimmed
        end

        local result, err = safe_json_encode({
            owner = owner_id,
            results = all_results,
            total = #all_results,
            sites_queried = sites_queried
        })
        if not result then
            return server.error_reply("Failed to encode response: " .. (err or "unknown"))
        end
        return result
    end,
    flags = {'no-writes'},
    description = 'Cross-site service discovery within a tenant/owner group'
}