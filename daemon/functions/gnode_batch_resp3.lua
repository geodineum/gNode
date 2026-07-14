#!lua name=gnode_batch_resp3

--
-- gNode BATCH_RESP3 Functions
-- A ValKey function library for batch resp3 operations
--
-- This is a port of the gCore Cache Scripts to ValKey functions
-- with enhancements for RESP3 compatibility
-- 


--[[
  - MGET: Multi-get with RESP3 array returns
  - MSET: Multi-set with TTL support
  - MDEL: Multi-delete with detailed results
  
  All functions match the gCore implementation while adding ValKey best practices.
  
  Usage:
  - GNODE_BATCH_MGET_RESP3(keys[], site_id, [group])
  - GNODE_BATCH_MSET_RESP3(keys[], values[], site_id, ttl, [group])
  - GNODE_BATCH_MDEL_RESP3(keys[], site_id, [group])
  
  All functions include proper site isolation, error handling, and metrics.
]]

-- Function to build a properly namespaced key
local function build_key(key, site_id, group, prefix)
    if not site_id or site_id == "" then
        site_id = "default"
    end
    
    prefix = prefix or ""
    
    -- Enable key to be already prefixed with site ID slot for optimized calls
    if key:find("^{" .. site_id .. "}") then
        return key
    end
    
    if group and group ~= "" then
        return '{' .. site_id .. '}:' .. prefix .. group .. ':' .. key
    else
        return '{' .. site_id .. '}:' .. prefix .. key
    end
end

-- Function to track metrics with detail recording
local function track_metric(site_id, metric_type, value, details)
    -- Skip if site_id not provided
    if not site_id or site_id == "" then
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

-- Register MGET function (mirroring CacheScriptsBatchOperations::MGET with RESP3)
server.register_function{
    function_name = 'GNODE_BATCH_MGET_RESP3',
    callback = function(keys, args)
        -- Input validation
        if #keys == 0 then
            return server.error_reply("At least one key required")
        end
        if not args[1] then
            return server.error_reply("Site ID required")
        end
        
        -- Extract parameters
        local batch_keys = keys
        local site_id = args[1]
        local group = args[2]
        
        -- Track operation timing
        local start_time = server.call('TIME')[1] * 1000000 + server.call('TIME')[2]

        -- Validate all keys hash to same slot (for cluster compatibility)
        -- Skip validation if cluster mode is disabled
        local cluster_ok, base_slot = pcall(function()
            return server.call('CLUSTER', 'KEYSLOT', batch_keys[1])
        end)
        if cluster_ok then
            for i=2, #batch_keys do
                local _, slot = pcall(function()
                    return server.call('CLUSTER', 'KEYSLOT', batch_keys[i])
                end)
                if slot ~= base_slot then
                    return server.error_reply("Keys must hash to same slot")
                end
            end
        end
        -- If cluster is disabled, we skip the slot validation

        -- Build fully namespaced keys
        local full_keys = {}
        for i, key in ipairs(batch_keys) do
            local full_key = build_key(key, site_id, group, '')
            if type(full_key) ~= "string" then
                return server.error_reply("Invalid key construction: " .. key)
            end
            full_keys[i] = full_key
        end

        -- Perform batch get
        local values = server.call('MGET', unpack(full_keys))
        
        -- Track metrics
        local end_time = server.call('TIME')[1] * 1000000 + server.call('TIME')[2]
        local latency = end_time - start_time
        
        local hits = 0
        local misses = 0
        for _, v in ipairs(values) do
            if v then hits = hits + 1
            else misses = misses + 1 end
        end
        
        track_metric(site_id, 'batch_gets', 1, {
            keys = #batch_keys,
            hits = hits,
            misses = misses,
            latency = latency
        })
        
        -- Return array directly for RESP3
        return values
    end,
    -- Note: No no-writes flag because track_metric writes to metrics hash
    description = 'Gets multiple keys with RESP3 array returns (matching CacheScriptsBatchOperations::MGET)'
}

-- Register MSET function (mirroring CacheScriptsBatchOperations::MSET with RESP3)
server.register_function{
    function_name = 'GNODE_BATCH_MSET_RESP3',
    callback = function(keys, args)
        -- Input validation
        if #keys == 0 then
            return server.error_reply("At least one key required")
        end
        
        -- We need args[1] (site_id), args[2] (ttl), args[3] (group), and then values for each key
        if #args < 3 + #keys then
            return server.error_reply("Key/value count mismatch")
        end
        
        -- Extract parameters
        local batch_keys = keys
        local site_id = args[1]
        local ttl = tonumber(args[2] or '0')
        local group = args[3]
        
        -- Extract values from args (skip site_id, ttl, group)
        local values = {}
        for i=4, 3+#batch_keys do
            values[i-3] = args[i]
        end
        
        -- Track operation timing
        local start_time = server.call('TIME')[1] * 1000000 + server.call('TIME')[2]

        -- Validate all keys hash to same slot (for cluster compatibility)
        -- Skip validation if cluster mode is disabled
        local cluster_ok, base_slot = pcall(function()
            return server.call('CLUSTER', 'KEYSLOT', batch_keys[1])
        end)
        if cluster_ok then
            for i=2, #batch_keys do
                local _, slot = pcall(function()
                    return server.call('CLUSTER', 'KEYSLOT', batch_keys[i])
                end)
                if slot ~= base_slot then
                    return server.error_reply("Keys must hash to same slot")
                end
            end
        end
        -- If cluster is disabled, we skip the slot validation

        -- Build fully namespaced keys
        local full_keys = {}
        for i, key in ipairs(batch_keys) do
            local full_key = build_key(key, site_id, group, '')
            if type(full_key) ~= "string" then
                return server.error_reply("Invalid key construction: " .. key)
            end
            full_keys[i] = full_key
        end

        -- Build MSET arguments (alternating key-value)
        local mset_args = {}
        for i=1, #full_keys do
            mset_args[i*2-1] = full_keys[i]
            mset_args[i*2] = values[i]
        end
        
        -- Perform batch set
        local success = server.call('MSET', unpack(mset_args))
        
        -- Apply TTL if specified
        if ttl and ttl > 0 then
            for _, key in ipairs(full_keys) do
                server.call('EXPIRE', key, ttl)
            end
        end
        
        -- Track metrics
        local end_time = server.call('TIME')[1] * 1000000 + server.call('TIME')[2]
        local latency = end_time - start_time
        
        track_metric(site_id, 'batch_sets', 1, {
            keys = #batch_keys,
            ttl = ttl,
            latency = latency
        })
        
        -- Return status reply for RESP3 compatibility
        return "OK"
    end,
    description = 'Sets multiple key/value pairs with TTL (matching CacheScriptsBatchOperations::MSET)'
}

-- Register MDEL function (mirroring CacheScriptsBatchOperations::MDEL with RESP3)
server.register_function{
    function_name = 'GNODE_BATCH_MDEL_RESP3',
    callback = function(keys, args)
        -- Input validation
        if #keys == 0 then
            return server.error_reply("At least one key required")
        end
        if not args[1] then
            return server.error_reply("Site ID required")
        end
        
        -- Extract parameters
        local batch_keys = keys
        local site_id = args[1]
        local group = args[2]
        
        -- Track operation timing
        local start_time = server.call('TIME')[1] * 1000000 + server.call('TIME')[2]

        -- Validate all keys hash to same slot (for cluster compatibility)
        -- Skip validation if cluster mode is disabled
        local cluster_ok, base_slot = pcall(function()
            return server.call('CLUSTER', 'KEYSLOT', batch_keys[1])
        end)
        if cluster_ok then
            for i=2, #batch_keys do
                local _, slot = pcall(function()
                    return server.call('CLUSTER', 'KEYSLOT', batch_keys[i])
                end)
                if slot ~= base_slot then
                    return server.error_reply("Keys must hash to same slot")
                end
            end
        end
        -- If cluster is disabled, we skip the slot validation

        -- Build fully namespaced keys
        local full_keys = {}
        for i, key in ipairs(batch_keys) do
            local full_key = build_key(key, site_id, group, '')
            if type(full_key) ~= "string" then
                return server.error_reply("Invalid key construction: " .. key)
            end
            full_keys[i] = full_key
        end
        
        -- Perform batch delete
        local deleted = 0
        for _, key in ipairs(full_keys) do
            deleted = deleted + server.call('DEL', key)
        end
        
        -- Track metrics
        local end_time = server.call('TIME')[1] * 1000000 + server.call('TIME')[2]
        local latency = end_time - start_time
        
        track_metric(site_id, 'batch_deletes', 1, {
            attempted = #batch_keys,
            deleted = deleted,
            latency = latency
        })
        
        -- Return integer for RESP3
        return deleted
    end,
    description = 'Deletes multiple keys (matching CacheScriptsBatchOperations::MDEL)'
}