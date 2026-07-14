#!lua name=gnode_cache

--
-- gNode CACHE Functions
-- A ValKey function library for cache operations
--
-- This is a port of the gCore Cache Scripts to ValKey functions
-- with enhancements for RESP3 compatibility
-- 


--[[
  This implementation is specifically designed for ValKey compatibility and
  provides proper error handling and performance optimizations.
  
  Usage:
  - GNODE_CACHE_GET(key, site_id)
  - GNODE_CACHE_SET(key, value, ttl, site_id)
  - GNODE_CACHE_DEL(key, site_id)
  - GNODE_CACHE_EXISTS(key, site_id)
  - GNODE_CACHE_INCR(key, amount, site_id)
  - GNODE_CACHE_DECR(key, amount, site_id)
  - GNODE_CACHE_TTL(key, site_id)
  - GNODE_CACHE_PERSIST(key, site_id)
  - GNODE_CACHE_STATS(site_id)
  
  All functions use proper error handling and follow ValKey best practices.
]]

-- Function to build a properly namespaced key
--
-- Key format rules:
-- 1. If key starts with {site_id}: -> return as-is (already has hash tag)
-- 2. If key starts with site_id: (no braces) -> add hash tag braces, preserve namespace
-- 3. Otherwise -> add {site_id}:cache: prefix (generic cache key)
--
-- Examples with site_id = "my_app":
--   "{my_app}:gnode:face_mapping" -> "{my_app}:gnode:face_mapping" (unchanged)
--   "my_app:gnode:face_mapping"   -> "{my_app}:gnode:face_mapping" (add braces)
--   "my_cache_key"                -> "{my_app}:cache:my_cache_key" (add prefix)
--
local function build_key(key, site_id)
    if not site_id or site_id == "" then
        site_id = "default"
    end

    -- Case 1: Already has hash tag with site_id - return as-is
    if key:find("^{" .. site_id .. "}") then
        return key
    end

    -- Case 2: Key starts with site_id: (no braces) - add hash tag, preserve structure
    -- This handles keys like "my_app:gnode:face_mapping" -> "{my_app}:gnode:face_mapping"
    local site_prefix = site_id .. ":"
    if key:sub(1, #site_prefix) == site_prefix then
        -- Replace "site_id:" with "{site_id}:" to add hash tag
        return "{" .. site_id .. "}:" .. key:sub(#site_prefix + 1)
    end

    -- Case 3: Generic key without site prefix - add full cache namespace
    return '{' .. site_id .. '}:cache:' .. key
end

-- Safe JSON encode/decode functions
local function safe_json_encode(value)
    local ok, result = pcall(function()
        return cjson.encode(value)
    end)
    
    if not ok then
        return nil, "JSON encoding failed: " .. tostring(result)
    end
    
    return result
end

local function safe_json_decode(json_str)
    if not json_str then
        return nil, "JSON string is nil"
    end
    
    local ok, result = pcall(function()
        return cjson.decode(json_str)
    end)
    
    if not ok then
        return nil, "JSON decoding failed: " .. tostring(result)
    end
    
    return result
end

-- Register cache get function
server.register_function{
    function_name = 'GNODE_CACHE_GET',
    callback = function(keys, args)
        -- Validate inputs
        if #args < 1 then
            return server.error_reply("Missing key argument")
        end
        
        local key = args[1]
        local site_id = args[2] or "default"
        
        -- Build the fully namespaced key
        local cache_key = build_key(key, site_id)
        
        -- Get the value from cache
        local value = server.call('GET', cache_key)
        
        -- Track cache hit/miss for metrics
        local metrics_key = '{' .. site_id .. '}:metrics:cache'
        if value then
            server.call('HINCRBY', metrics_key, 'hits', 1)
        else
            server.call('HINCRBY', metrics_key, 'misses', 1)
        end
        
        -- Check if value is JSON encoded
        if value and (value:sub(1,1) == '{' or value:sub(1,1) == '[') then
            -- Attempt to decode JSON
            local decoded, err = safe_json_decode(value)
            if decoded then
                -- Return decoded value as JSON string for cross-language compatibility
                return value
            end
        end
        
        -- Return raw value if not JSON or JSON decoding failed
        return value
    end,
    -- Note: metrics tracking requires write access
    description = 'Gets a cached value with proper site namespacing'
}

-- Default TTL constants (in seconds)
local DEFAULT_TTL = 3600           -- 1 hour default for general cache
local MAX_TTL = 86400 * 30         -- 30 days maximum
local REQUEST_TTL = 60             -- 1 minute for request tracking keys
local ERROR_TTL = 86400 * 7        -- 7 days for error keys

-- Register cache set function
server.register_function{
    function_name = 'GNODE_CACHE_SET',
    callback = function(keys, args)
        -- Validate inputs
        if #args < 2 then
            return server.error_reply("Missing required arguments")
        end

        local key = args[1]
        local value = args[2]
        local ttl = tonumber(args[3] or '0')
        local site_id = args[4] or "default"
        local nx = args[5] == 'NX'
        local xx = args[6] == 'XX'

        -- SAFETY: Enforce TTL to prevent orphaned keys
        -- Apply smart defaults based on key pattern
        if ttl == 0 or ttl == nil then
            if key:find(':req:') or key:find(':request:') then
                ttl = REQUEST_TTL        -- Request tracking: 1 minute
            elseif key:find(':error:') or key:find(':errors:') then
                ttl = ERROR_TTL          -- Error cache: 7 days
            else
                ttl = DEFAULT_TTL        -- General cache: 1 hour
            end
        end

        -- Cap TTL at maximum to prevent runaway keys
        if ttl > MAX_TTL then
            ttl = MAX_TTL
        end

        -- Build the fully namespaced key
        local cache_key = build_key(key, site_id)

        -- Set the value in cache
        local result
        if nx then
            result = server.call('SET', cache_key, value, 'NX')
        elseif xx then
            result = server.call('SET', cache_key, value, 'XX')
        else
            result = server.call('SET', cache_key, value)
        end

        -- Set TTL (now always applied due to safety defaults above)
        -- Note: In ValKey 7.2+ functions, SET returns a status table, not string 'OK'
        local success = result and (result.ok or result == 'OK' or type(result) == 'table')
        if success and ttl > 0 then
            server.call('EXPIRE', cache_key, ttl)
        end

        -- Track cache write for metrics
        if success then
            local metrics_key = '{' .. site_id .. '}:metrics:cache'
            server.call('HINCRBY', metrics_key, 'writes', 1)
            
            -- Track cache size metrics
            local size = #value
            server.call('HINCRBY', metrics_key, 'total_size', size)
            server.call('HINCRBY', metrics_key, 'items', 1)
            
            local avg_size = tonumber(server.call('HGET', metrics_key, 'total_size')) / 
                tonumber(server.call('HGET', metrics_key, 'items'))
            server.call('HSET', metrics_key, 'avg_size', avg_size)
        end
        
        return result
    end,
    description = 'Sets a cached value with optional TTL and site namespacing'
}

-- Register cache delete function
server.register_function{
    function_name = 'GNODE_CACHE_DEL',
    callback = function(keys, args)
        -- Validate inputs
        if #args < 1 then
            return server.error_reply("Missing key argument")
        end
        
        local key = args[1]
        local site_id = args[2] or "default"
        
        -- Build the fully namespaced key
        local cache_key = build_key(key, site_id)
        
        -- Delete the key
        local deleted = server.call('DEL', cache_key)
        
        -- Track cache deletion for metrics
        if deleted > 0 then
            local metrics_key = '{' .. site_id .. '}:metrics:cache'
            server.call('HINCRBY', metrics_key, 'deletes', 1)
            server.call('HINCRBY', metrics_key, 'items', -1)
        end
        
        return deleted
    end,
    description = 'Deletes a cached value with proper site namespacing'
}

-- Register cache exists function
server.register_function{
    function_name = 'GNODE_CACHE_EXISTS',
    callback = function(keys, args)
        -- Validate inputs
        if #args < 1 then
            return server.error_reply("Missing key argument")
        end
        
        local key = args[1]
        local site_id = args[2] or "default"
        
        -- Build the fully namespaced key
        local cache_key = build_key(key, site_id)
        
        -- Check if key exists
        return server.call('EXISTS', cache_key)
    end,
    flags = {'no-writes'}, -- This function only reads data
    description = 'Checks if a cached key exists with proper site namespacing'
}

-- Register cache increment function
server.register_function{
    function_name = 'GNODE_CACHE_INCR',
    callback = function(keys, args)
        -- Validate inputs
        if #args < 1 then
            return server.error_reply("Missing key argument")
        end
        
        local key = args[1]
        local amount = tonumber(args[2] or '1')
        local site_id = args[3] or "default"
        
        -- Build the fully namespaced key
        local cache_key = build_key(key, site_id)
        
        -- Increment the value
        local result
        if amount == 1 then
            result = server.call('INCR', cache_key)
        else
            result = server.call('INCRBY', cache_key, amount)
        end
        
        -- Track cache operation for metrics
        local metrics_key = '{' .. site_id .. '}:metrics:cache'
        server.call('HINCRBY', metrics_key, 'increments', 1)
        
        return result
    end,
    description = 'Increments a cached counter with proper site namespacing'
}

-- Register cache decrement function
server.register_function{
    function_name = 'GNODE_CACHE_DECR',
    callback = function(keys, args)
        -- Validate inputs
        if #args < 1 then
            return server.error_reply("Missing key argument")
        end
        
        local key = args[1]
        local amount = tonumber(args[2] or '1')
        local site_id = args[3] or "default"
        
        -- Build the fully namespaced key
        local cache_key = build_key(key, site_id)
        
        -- Decrement the value
        local result
        if amount == 1 then
            result = server.call('DECR', cache_key)
        else
            result = server.call('DECRBY', cache_key, amount)
        end
        
        -- Track cache operation for metrics
        local metrics_key = '{' .. site_id .. '}:metrics:cache'
        server.call('HINCRBY', metrics_key, 'decrements', 1)
        
        return result
    end,
    description = 'Decrements a cached counter with proper site namespacing'
}

-- Register cache TTL function
server.register_function{
    function_name = 'GNODE_CACHE_TTL',
    callback = function(keys, args)
        -- Validate inputs
        if #args < 1 then
            return server.error_reply("Missing key argument")
        end
        
        local key = args[1]
        local site_id = args[2] or "default"
        
        -- Build the fully namespaced key
        local cache_key = build_key(key, site_id)
        
        -- Get TTL of the key
        return server.call('TTL', cache_key)
    end,
    flags = {'no-writes'}, -- This function only reads data
    description = 'Gets the TTL of a cached key with proper site namespacing'
}

-- Register cache persist function
server.register_function{
    function_name = 'GNODE_CACHE_PERSIST',
    callback = function(keys, args)
        -- Validate inputs
        if #args < 1 then
            return server.error_reply("Missing key argument")
        end
        
        local key = args[1]
        local site_id = args[2] or "default"
        
        -- Build the fully namespaced key
        local cache_key = build_key(key, site_id)
        
        -- Remove expiration from the key
        local result = server.call('PERSIST', cache_key)
        
        -- Track cache operation for metrics
        if result == 1 then
            local metrics_key = '{' .. site_id .. '}:metrics:cache'
            server.call('HINCRBY', metrics_key, 'persists', 1)
        end
        
        return result
    end,
    description = 'Removes expiration from a cached key with proper site namespacing'
}

-- Register cache stats function
server.register_function{
    function_name = 'GNODE_CACHE_STATS',
    callback = function(keys, args)
        local site_id = args[1] or "default"
        
        -- Get cache metrics for the site
        local metrics_key = '{' .. site_id .. '}:metrics:cache'
        local stats = server.call('HGETALL', metrics_key)
        
        -- Convert array to object
        local stats_obj = {}
        for i = 1, #stats, 2 do
            local key = stats[i]
            local value = stats[i+1]
            -- Convert numeric values
            if key == "hits" or key == "misses" or key == "writes" or 
               key == "deletes" or key == "items" or key == "total_size" or
               key == "increments" or key == "decrements" or key == "persists" or
               key == "avg_size" then
                stats_obj[key] = tonumber(value) or 0
            else
                stats_obj[key] = value
            end
        end
        
        -- Calculate additional stats
        if stats_obj.hits and stats_obj.misses then
            local total = stats_obj.hits + stats_obj.misses
            if total > 0 then
                stats_obj.hit_ratio = stats_obj.hits / total
            else
                stats_obj.hit_ratio = 0
            end
        end
        
        -- Get sample of keys for analysis. SCAN/MEMORY may be denied by the
        -- per-site ACL (least-privilege grants +fcall +scan but not +memory);
        -- a raw NOPERM there would abort the whole FCALL and the dashboard would
        -- show "not available". pcall so stats degrade gracefully instead.
        local cache_keys_pattern = '{' .. site_id .. '}:cache:*'
        local scan_ok, scan_res = pcall(function()
            return server.call('SCAN', '0', 'MATCH', cache_keys_pattern, 'COUNT', '10')
        end)
        stats_obj.sample_keys = (scan_ok and scan_res and scan_res[2]) or {}

        -- Add memory usage stats if available (MEMORY often not granted).
        local mem_ok, mem_res = pcall(function()
            return server.call('MEMORY', 'USAGE', metrics_key)
        end)
        stats_obj.metrics_memory_usage = (mem_ok and mem_res) or 0
        
        -- Encode as JSON for cross-language compatibility
        local json, err = safe_json_encode(stats_obj)
        if not json then
            return server.error_reply("Failed to encode stats: " .. err)
        end
        
        return json
    end,
    flags = {'no-writes'}, -- This function only reads data
    description = 'Gets cache statistics for a site'
}