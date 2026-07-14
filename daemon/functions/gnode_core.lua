#!lua name=gnode_core

--
-- gNode CORE Functions
-- A ValKey function library for core operations
--
-- This is a port of the gCore Cache Scripts to ValKey functions
-- with enhancements for RESP3 compatibility
-- 
-- Capabilities:
--  - Incrementing and decrementing values
--  - Atomic get-and-set operations
--  - Conditional set operations
--  
-- Usage:
--  - GNODE_CORE_GET(key, site_id)
--  - GNODE_CORE_SET_WITH_TTL(key, value, ttl, site_id)
--  - GNODE_CORE_DELETE(key, site_id)
--  - GNODE_CORE_EXISTS(key, site_id)
--  - GNODE_CORE_TTL(key, site_id)
--  - GNODE_CORE_INCREMENT(key, amount, ttl, site_id)
--  - GNODE_CORE_DECREMENT(key, amount, ttl, site_id)
--  - GNODE_CORE_GET_SET(key, value, ttl, site_id)
--  - GNODE_CORE_SET_IF_NOT_EXISTS(key, value, ttl, site_id)
--

-- Function to build a properly namespaced key
local function build_key(key, site_id, prefix)
    if not site_id or site_id == "" then
        site_id = "default"
    end
    
    prefix = prefix or "cache:"
    
    -- Enable key to be already prefixed with site ID slot for optimized calls
    if key:find("^{" .. site_id .. "}") then
        return key
    end
    
    return '{' .. site_id .. '}:' .. prefix .. key
end

-- Function to track metrics
local function track_metric(site_id, metric_type, value, details, read_only)
    -- Skip if site_id not provided or in read-only mode
    if not site_id or site_id == "" or read_only then
        return
    end
    
    -- Default to increment by 1
    value = value or 1
    
    -- Build metrics key with site isolation
    local metrics_key = '{' .. site_id .. '}:metrics'
    
    -- Track the metric
    server.call('HINCRBY', metrics_key, metric_type, value)
    
    -- Track details if provided
    if details then
        -- Convert details to JSON
        local ok, details_json = pcall(function()
            return cjson.encode(details)
        end)
        
        if ok and details_json then
            -- Store in detail log with timestamp
            local details_key = metrics_key .. ':' .. metric_type .. ':details'
            server.call('LPUSH', details_key, details_json)
            server.call('LTRIM', details_key, 0, 999)  -- Keep last 1000 entries
        end
    end
end

-- Register core GET function (direct mirror of CacheScriptsCoreOperations::GET)
server.register_function{
    function_name = 'GNODE_CORE_GET',
    callback = function(keys, args)
        -- Validate inputs
        if #args < 1 then
            return server.error_reply("Missing key argument")
        end
        
        local key = args[1]
        local site_id = args[2] or "default"
        
        -- Build the fully namespaced key
        local cache_key = build_key(key, site_id)
        
        -- Get the value - exact mirror of the original script
        local value = server.call('GET', cache_key)
        
        -- Track cache hit/miss metrics (with read_only flag to prevent writes)
        if value then
            track_metric(site_id, 'cache_hits', 1, nil, true)
        else
            track_metric(site_id, 'cache_misses', 1, nil, true)
        end
        
        return value
    end,
    flags = {'no-writes'}, -- This function only reads data
    description = 'Gets a cached value with proper site namespacing'
}

-- Register core SET_WITH_TTL function (direct mirror of CacheScriptsCoreOperations::SET_WITH_TTL)
server.register_function{
    function_name = 'GNODE_CORE_SET_WITH_TTL',
    callback = function(keys, args)
        -- Validate inputs
        if #args < 2 then
            return server.error_reply("Missing required arguments")
        end
        
        local key = args[1]
        local value = args[2]
        local ttl = tonumber(args[3] or '0')
        local site_id = args[4] or "default"
        
        -- Build the fully namespaced key
        local cache_key = build_key(key, site_id)
        
        -- Set the value - exact mirror of the original script
        server.call('SET', cache_key, value)
        if ttl > 0 then
            server.call('EXPIRE', cache_key, ttl)
        end
        
        -- Track cache write metrics
        track_metric(site_id, 'cache_writes', 1, {
            key = key,
            ttl = ttl,
            size = #tostring(value)
        })
        
        return true
    end,
    description = 'Sets a cached value with TTL and proper site namespacing'
}

-- Register core DELETE function (direct mirror of CacheScriptsCoreOperations::DELETE)
server.register_function{
    function_name = 'GNODE_CORE_DELETE',
    callback = function(keys, args)
        -- Validate inputs
        if #args < 1 then
            return server.error_reply("Missing key argument")
        end
        
        local key = args[1]
        local site_id = args[2] or "default"
        
        -- Build the fully namespaced key
        local cache_key = build_key(key, site_id)
        
        -- Delete the key - exact mirror of the original script
        local deleted = server.call('DEL', cache_key)
        
        -- Track deletion metrics
        if deleted > 0 then
            track_metric(site_id, 'cache_deletes', 1, {
                key = key
            })
        end
        
        return deleted
    end,
    description = 'Deletes a cached value with proper site namespacing'
}

-- Register core EXISTS function (direct mirror of CacheScriptsCoreOperations::EXISTS)
server.register_function{
    function_name = 'GNODE_CORE_EXISTS',
    callback = function(keys, args)
        -- Validate inputs
        if #args < 1 then
            return server.error_reply("Missing key argument")
        end
        
        local key = args[1]
        local site_id = args[2] or "default"
        
        -- Build the fully namespaced key
        local cache_key = build_key(key, site_id)
        
        -- Check if key exists - exact mirror of the original script
        local result = server.call('EXISTS', cache_key)
        
        -- No metrics tracking for read-only function
        
        return result
    end,
    flags = {'no-writes'}, -- This function only reads data
    description = 'Checks if a cached key exists with proper site namespacing'
}

-- Register core TTL function (direct mirror of CacheScriptsCoreOperations::TTL)
server.register_function{
    function_name = 'GNODE_CORE_TTL',
    callback = function(keys, args)
        -- Validate inputs
        if #args < 1 then
            return server.error_reply("Missing key argument")
        end
        
        local key = args[1]
        local site_id = args[2] or "default"
        
        -- Build the fully namespaced key
        local cache_key = build_key(key, site_id)
        
        -- Get TTL of the key - exact mirror of the original script
        local result = server.call('TTL', cache_key)
        
        -- No metrics tracking for read-only function
        
        return result
    end,
    flags = {'no-writes'}, -- This function only reads data
    description = 'Gets the TTL of a cached key with proper site namespacing'
}

-- Register core INCREMENT function (direct mirror of CacheScriptsCoreOperations::INCREMENT)
server.register_function{
    function_name = 'GNODE_CORE_INCREMENT',
    callback = function(keys, args)
        -- Validate inputs
        if #args < 1 then
            return server.error_reply("Missing key argument")
        end
        
        local key = args[1]
        local amount = tonumber(args[2] or '1')
        local ttl = tonumber(args[3] or '0')
        local site_id = args[4] or "default"
        
        -- Build the fully namespaced key
        local cache_key = build_key(key, site_id)
        
        -- Increment the value - exact mirror of the original script
        local value = server.call('INCRBY', cache_key, amount)
        if ttl > 0 then
            server.call('EXPIRE', cache_key, ttl)
        end
        
        -- Track increment metrics
        track_metric(site_id, 'cache_increments', 1, {
            key = key,
            amount = amount,
            new_value = value
        })
        
        return value
    end,
    description = 'Increments a cached counter with proper site namespacing'
}

-- Register core DECREMENT function (direct mirror of CacheScriptsCoreOperations::DECREMENT)
server.register_function{
    function_name = 'GNODE_CORE_DECREMENT',
    callback = function(keys, args)
        -- Validate inputs
        if #args < 1 then
            return server.error_reply("Missing key argument")
        end
        
        local key = args[1]
        local amount = tonumber(args[2] or '1')
        local ttl = tonumber(args[3] or '0')
        local site_id = args[4] or "default"
        
        -- Build the fully namespaced key
        local cache_key = build_key(key, site_id)
        
        -- Decrement the value - exact mirror of the original script
        local value = server.call('DECRBY', cache_key, amount)
        if ttl > 0 then
            server.call('EXPIRE', cache_key, ttl)
        end
        
        -- Track decrement metrics
        track_metric(site_id, 'cache_decrements', 1, {
            key = key,
            amount = amount,
            new_value = value
        })
        
        return value
    end,
    description = 'Decrements a cached counter with proper site namespacing'
}

-- Register core GET_SET function (direct mirror of CacheScriptsCoreOperations::GET_SET)
server.register_function{
    function_name = 'GNODE_CORE_GET_SET',
    callback = function(keys, args)
        -- Validate inputs
        if #args < 2 then
            return server.error_reply("Missing required arguments")
        end
        
        local key = args[1]
        local value = args[2]
        local ttl = tonumber(args[3] or '0')
        local site_id = args[4] or "default"
        
        -- Build the fully namespaced key
        local cache_key = build_key(key, site_id)
        
        -- Get old value and set new value - exact mirror of the original script
        local old = server.call('GET', cache_key)
        server.call('SET', cache_key, value)
        if ttl > 0 then
            server.call('EXPIRE', cache_key, ttl)
        end
        
        -- Track cache operation metrics
        track_metric(site_id, 'cache_get_sets', 1, {
            key = key,
            had_old_value = old ~= nil,
            ttl = ttl
        })
        
        return old
    end,
    description = 'Gets old value and sets new value atomically with proper site namespacing'
}

-- Register core SET_IF_NOT_EXISTS function (direct mirror of CacheScriptsCoreOperations::SET_IF_NOT_EXISTS)
server.register_function{
    function_name = 'GNODE_CORE_SET_IF_NOT_EXISTS',
    callback = function(keys, args)
        -- Validate inputs
        if #args < 2 then
            return server.error_reply("Missing required arguments")
        end
        
        local key = args[1]
        local value = args[2]
        local ttl = tonumber(args[3] or '0')
        local site_id = args[4] or "default"
        
        -- Build the fully namespaced key
        local cache_key = build_key(key, site_id)
        
        -- Set value if not exists - exact mirror of the original script
        local result = server.call('SETNX', cache_key, value)
        if result == 1 and ttl > 0 then
            server.call('EXPIRE', cache_key, ttl)
        end
        
        -- Track cache operation metrics
        track_metric(site_id, 'cache_setnx', 1, {
            key = key,
            success = result == 1,
            ttl = ttl
        })
        
        return result
    end,
    description = 'Sets value only if key does not exist with proper site namespacing'
}