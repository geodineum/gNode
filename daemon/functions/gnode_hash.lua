#!lua name=gnode_hash

--
-- gNode HASH Functions
-- A ValKey function library for hash operations
--
-- This is a port of the gCore Cache Scripts to ValKey functions
-- with enhancements for RESP3 compatibility
-- 


local function track_metric(site_id, metric, value, context)
    local metrics_key = '{' .. site_id .. '}:metrics'
    server.call('HINCRBY', metrics_key, metric, value or 1)

    -- If context provided, record detailed metrics
    if context then
        local ctx_key = '{' .. site_id .. '}:metrics:detailed:' .. metric
        local timestamp = server.call('TIME')[1]

        -- Store context as JSON using Lua cjson library (not a Redis command)
        local ok, ctx_json = pcall(function()
            return cjson.encode(context)
        end)
        if not ok or not ctx_json then
            -- Log error but don't fail the operation
            server.call('HINCRBY', metrics_key, 'metric_encode_errors', 1)
            return
        end

        server.call('ZADD', ctx_key, timestamp, timestamp .. ':' .. ctx_json)

        -- Keep only recent metrics (last 1000)
        local count = server.call('ZCARD', ctx_key)
        if count > 1000 then
            server.call('ZREMRANGEBYRANK', ctx_key, 0, count - 1001)
        end
    end
end

-- Register hash increment function (mirroring CacheScriptsHashOperations::HINCRBY)
server.register_function{
    function_name = 'GNODE_HASH_HINCRBY',
    callback = function(keys, args)
        -- Input validation
        if #keys < 1 then
            return server.error_reply("Key required")
        end
        if not args[1] then
            return server.error_reply("Field required")
        end
        if not args[2] then
            return server.error_reply("Increment required")
        end
        if not args[3] then
            return server.error_reply("Site ID required")
        end
        
        local key = keys[1]
        local field = args[1]
        local increment = tonumber(args[2])
        if not increment then
            return server.error_reply("Increment must be a number")
        end
        local site_id = args[3]
        
        -- Track operation timing
        local start_time = server.call('TIME')[1]
        local start_us = start_time * 1000000 + server.call('TIME')[2]
        
        -- Execute increment
        local new_value = server.call('HINCRBY', key, field, increment)
        
        -- Track operation metrics
        local end_time = server.call('TIME')[1]
        local end_us = end_time * 1000000 + server.call('TIME')[2]
        local latency = end_us - start_us
        
        track_metric(site_id, 'hash_increments', 1, {
            key = key,
            field = field,
            increment = increment,
            latency = latency
        })

        -- Return simple integer (PHP Redis ext doesn't parse RESP3 maps correctly)
        return new_value
    end,
    description = 'Increments a hash field with metric tracking'
}

-- Register hash get all function (mirroring CacheScriptsHashOperations::HGETALL)
server.register_function{
    function_name = 'GNODE_HASH_HGETALL',
    callback = function(keys, args)
        -- Input validation
        if #keys < 1 then
            return server.error_reply("Key required")
        end
        if not args[1] then
            return server.error_reply("Site ID required")
        end
        
        local key = keys[1]
        local site_id = args[1]
        
        -- Track operation timing
        local start_time = server.call('TIME')[1]
        local start_us = start_time * 1000000 + server.call('TIME')[2]
        
        -- Get all fields
        local result = server.call('HGETALL', key)

        -- Track operation metrics
        local end_time = server.call('TIME')[1]
        local end_us = end_time * 1000000 + server.call('TIME')[2]
        local latency = end_us - start_us

        track_metric(site_id, 'hash_retrievals', 1, {
            key = key,
            fields_count = #result / 2,
            latency = latency
        })

        -- Return flat array [field1, value1, field2, value2, ...]
        -- PHP will convert this to associative array
        return result
    end,
    flags = {},  -- Tracks retrieval metrics (writes via track_metric)
    description = 'Gets all hash fields with metric tracking'
}

-- Register hash get single field function (mirroring CacheScriptsHashOperations::HGET)
server.register_function{
    function_name = 'GNODE_HASH_HGET',
    callback = function(keys, args)
        -- Input validation
        if #keys < 1 then
            return server.error_reply("Key required")
        end
        if not args[1] then
            return server.error_reply("Field required")
        end
        if not args[2] then
            return server.error_reply("Site ID required")
        end

        local key = keys[1]
        local field = args[1]
        local site_id = args[2]

        -- Track operation timing
        local start_time = server.call('TIME')[1]
        local start_us = start_time * 1000000 + server.call('TIME')[2]

        -- Get field value
        local value = server.call('HGET', key, field)

        -- Track operation metrics
        local end_time = server.call('TIME')[1]
        local end_us = end_time * 1000000 + server.call('TIME')[2]
        local latency = end_us - start_us

        track_metric(site_id, 'hash_gets', 1, {
            key = key,
            field = field,
            hit = value ~= false,
            latency = latency
        })

        -- Return value directly (nil if not found)
        return value
    end,
    flags = {},  -- Tracks retrieval metrics (writes via track_metric)
    description = 'Gets single hash field with metric tracking'
}

-- Register hash exists function (mirroring CacheScriptsHashOperations::HEXISTS)
server.register_function{
    function_name = 'GNODE_HASH_HEXISTS',
    callback = function(keys, args)
        -- Input validation
        if #keys < 1 then
            return server.error_reply("Key required")
        end
        if not args[1] then
            return server.error_reply("Field required")
        end
        if not args[2] then
            return server.error_reply("Site ID required")
        end
        
        local key = keys[1]
        local field = args[1]
        local site_id = args[2]
        
        -- Track operation timing
        local start_time = server.call('TIME')[1]
        local start_us = start_time * 1000000 + server.call('TIME')[2]
        
        -- Check existence
        local exists = server.call('HEXISTS', key, field)
        
        -- Track operation metrics
        local end_time = server.call('TIME')[1]
        local end_us = end_time * 1000000 + server.call('TIME')[2]
        local latency = end_us - start_us
        
        track_metric(site_id, 'hash_exists_checks', 1, {
            key = key,
            field = field,
            exists = exists == 1,
            latency = latency
        })

        -- Return simple integer (1 exists, 0 not exists)
        return exists
    end,
    flags = {},  -- Tracks existence check metrics (writes via track_metric)
    description = 'Checks hash field existence with metric tracking'
}

-- Register hash set function (mirroring CacheScriptsHashOperations::HSET)
server.register_function{
    function_name = 'GNODE_HASH_HSET',
    callback = function(keys, args)
        -- Input validation
        if #keys < 1 then
            return server.error_reply("Key required")
        end
        if not args[1] then
            return server.error_reply("Field required")
        end
        if not args[2] then
            return server.error_reply("Value required")
        end
        if not args[3] then
            return server.error_reply("Site ID required")
        end
        
        local key = keys[1]
        local field = args[1]
        local value = args[2]
        local site_id = args[3]
        
        -- Track operation timing
        local start_time = server.call('TIME')[1]
        local start_us = start_time * 1000000 + server.call('TIME')[2]
        
        -- Set field
        local result = server.call('HSET', key, field, value)
        
        -- Track operation metrics
        local end_time = server.call('TIME')[1]
        local end_us = end_time * 1000000 + server.call('TIME')[2]
        local latency = end_us - start_us
        
        track_metric(site_id, 'hash_sets', 1, {
            key = key,
            field = field,
            new_field = result == 1,
            latency = latency
        })

        -- Return 1 for success (field was set)
        -- HSET returns number of fields added, but we always set 1 field
        return 1
    end,
    description = 'Sets hash field with metric tracking'
}

-- Register list push function (mirroring CacheScriptsHashOperations::LPUSH)
server.register_function{
    function_name = 'GNODE_HASH_LPUSH',
    callback = function(keys, args)
        -- Input validation
        if #keys < 1 then
            return server.error_reply("Key required")
        end
        if not args[1] then
            return server.error_reply("Value required")
        end
        if not args[2] then
            return server.error_reply("Site ID required")
        end
        
        local key = keys[1]
        local value = args[1]
        local site_id = args[2]
        
        -- Track operation timing
        local start_time = server.call('TIME')[1]
        local start_us = start_time * 1000000 + server.call('TIME')[2]
        
        -- Push value
        local list_length = server.call('LPUSH', key, value)
        
        -- Track operation metrics
        local end_time = server.call('TIME')[1]
        local end_us = end_time * 1000000 + server.call('TIME')[2]
        local latency = end_us - start_us
        
        track_metric(site_id, 'list_pushes', 1, {
            key = key,
            list_size = list_length,
            latency = latency
        })

        -- Return list length directly
        return list_length
    end,
    description = 'Pushes to list with metric tracking'
}

-- Extended function for hash operations (enhanced beyond original scripts)
server.register_function{
    function_name = 'GNODE_HASH_HMSET',
    callback = function(keys, args)
        -- Input validation
        if #keys < 1 then
            return server.error_reply("Key required")
        end
        if #args < 3 or #args % 2 == 0 then
            return server.error_reply("Arguments must be: field1, value1, field2, value2, ..., site_id")
        end
        
        local key = keys[1]
        local site_id = args[#args]
        
        -- Track operation timing
        local start_time = server.call('TIME')[1]
        local start_us = start_time * 1000000 + server.call('TIME')[2]
        
        -- Prepare field-value pairs for HMSET
        local fields_count = (#args - 1) / 2
        local hmset_args = {key}
        for i = 1, #args - 1, 2 do
            table.insert(hmset_args, args[i])
            table.insert(hmset_args, args[i+1])
        end
        
        -- Set fields
        server.call('HMSET', unpack(hmset_args))
        
        -- Track operation metrics
        local end_time = server.call('TIME')[1]
        local end_us = end_time * 1000000 + server.call('TIME')[2]
        local latency = end_us - start_us
        
        track_metric(site_id, 'hash_msets', 1, {
            key = key,
            fields_count = fields_count,
            latency = latency
        })

        -- Return number of fields set
        return fields_count
    end,
    description = 'Sets multiple hash fields with metric tracking'
}