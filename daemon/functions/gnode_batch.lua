#!lua name=gnode_batch

--
-- gNode BATCH Functions
-- A ValKey function library for batch operations
--
-- This is a port of the gCore Cache Scripts to ValKey functions
-- with enhancements for RESP3 compatibility
-- 

--[[
  This implementation is fully compatible with ValKey and provides
  significant performance benefits for multi-operation transactions.
  
  Usage:
  - GNODE_BATCH_EXEC(site_id, operations_json) - Execute JSON batch
  - GNODE_BATCH_MGET(site_id, key1, key2, ...) - Multi-GET
  - GNODE_BATCH_MSET(site_id, key1, value1, key2, value2, ...) - Multi-SET
  - GNODE_BATCH_MDEL(site_id, key1, key2, ...) - Multi-DEL
  - Additional batch operations for hash, expiry, etc.
  
  All functions ensure proper key namespacing and error handling.
]]

-- Helper function to build site-specific key
local function build_key(site_id, key)
    if key:find("^{" .. site_id .. "}") then
        -- Key already has site ID namespace
        return key
    end
    return '{' .. site_id .. '}:' .. key
end

-- Helper function to encode JSON
local function json_encode(obj)
    local ok, result = pcall(function()
        return cjson.encode(obj)
    end)
    
    if not ok then
        return '{"error":"JSON encoding failed"}'
    end
    
    return result
end

-- Helper function to decode JSON
local function json_decode(json_str)
    local ok, result = pcall(function()
        return cjson.decode(json_str)
    end)

    if not ok then
        return nil, "JSON decoding failed"
    end

    return result
end

-- Safe server.call wrapper (P2CF002 fix)
local function safe_call(...)
    local ok, result = pcall(server.call, ...)
    if ok then
        return result, nil
    else
        return nil, tostring(result)
    end
end

-- Main batch execution function (JSON mode)
server.register_function{
    function_name = 'GNODE_BATCH_EXEC',
    callback = function(keys, args)
        -- Validate inputs
        if #args < 2 then
            return server.error_reply("Missing required arguments")
        end
        
        local site_id = args[1]
        local operations_json = args[2]
        
        -- Parse operations JSON
        local operations, err = json_decode(operations_json)
        if not operations then
            return server.error_reply("Invalid operations JSON: " .. err)
        end
        
        -- Process each operation
        local results = {}
        
        for i, op in ipairs(operations) do
            -- Each operation is an array: [command, arg1, arg2, ...]
            if type(op) ~= "table" or #op < 1 then
                results[i] = {err = "Invalid operation format"}
            else
            
            -- Extract command and args
            local cmd = op[1]:upper()
            local args = {}
            
            -- Process command args with proper key namespacing
            for j = 2, #op do
                if j == 2 and cmd ~= "EVAL" and cmd ~= "EVALSHA" and cmd ~= "FCALL" and cmd ~= "FCALL_RO" then
                    -- Add site ID namespace to keys except for script/function execution
                    table.insert(args, build_key(site_id, op[j]))
                else
                    table.insert(args, op[j])
                end
            end
            
            -- Execute command based on type
            local ok, result
            
            if cmd == "GET" then
                ok, result = pcall(function() return server.call("GET", args[1]) end)
                
            elseif cmd == "SET" then
                if #args >= 3 and tonumber(args[3]) and tonumber(args[3]) > 0 then
                    -- SET with TTL
                    ok, result = pcall(function()
                        server.call("SET", args[1], args[2])
                        return server.call("EXPIRE", args[1], tonumber(args[3]))
                    end)
                else
                    ok, result = pcall(function() return server.call("SET", args[1], args[2]) end)
                end
                
            elseif cmd == "DEL" then
                ok, result = pcall(function() return server.call("DEL", args[1]) end)
                
            elseif cmd == "EXISTS" then
                ok, result = pcall(function() return server.call("EXISTS", args[1]) end)
                
            elseif cmd == "EXPIRE" then
                ok, result = pcall(function() return server.call("EXPIRE", args[1], tonumber(args[2])) end)
                
            elseif cmd == "TTL" then
                ok, result = pcall(function() return server.call("TTL", args[1]) end)
                
            elseif cmd == "INCR" then
                ok, result = pcall(function() return server.call("INCR", args[1]) end)
                
            elseif cmd == "DECR" then
                ok, result = pcall(function() return server.call("DECR", args[1]) end)
                
            elseif cmd == "HINCRBY" then
                ok, result = pcall(function() return server.call("HINCRBY", args[1], args[2], tonumber(args[3])) end)
                
            elseif cmd == "HGET" then
                ok, result = pcall(function() return server.call("HGET", args[1], args[2]) end)
                
            elseif cmd == "HSET" then
                ok, result = pcall(function() return server.call("HSET", args[1], args[2], args[3]) end)
                
            elseif cmd == "HDEL" then
                ok, result = pcall(function() return server.call("HDEL", args[1], args[2]) end)
                
            else
                results[i] = {err = "Unsupported command: " .. cmd}
            end
            
            if ok then
                results[i] = result
            else
                results[i] = {err = tostring(result)}
            end
            
            end -- End of the else block
        end
        
        -- Return JSON-encoded results
        return json_encode(results)
    end,
    description = 'Executes a batch of operations in JSON format'
}

-- Multi-GET operation
server.register_function{
    function_name = 'GNODE_BATCH_MGET',
    callback = function(keys, args)
        -- Validate inputs
        if #args < 1 then
            return server.error_reply("Missing site ID")
        end
        if #keys < 1 then
            return server.error_reply("No keys provided")
        end
        
        local site_id = args[1]
        local results = {}
        local success_count = 0
        
        -- Process each key (P2CF002 fix: using safe_call)
        for i, key in ipairs(keys) do
            local namespaced_key = build_key(site_id, key)
            local value, err = safe_call('GET', namespaced_key)

            if err then
                results[i] = {error = err}
            elseif value then
                results[i] = value
                success_count = success_count + 1
            else
                results[i] = false
            end
        end

        -- Return result as JSON array with success count and values
        return json_encode({success_count, results})
    end,
    flags = {'no-writes'},
    description = 'Gets multiple keys in a single operation'
}

-- Multi-SET operation
server.register_function{
    function_name = 'GNODE_BATCH_MSET',
    callback = function(keys, args)
        -- Validate inputs
        if #args < 1 then
            return server.error_reply("Missing site ID")
        end
        if #keys < 1 then
            return server.error_reply("No keys provided")
        end
        if #args < 1 + #keys then
            return server.error_reply("Missing values for keys")
        end
        
        local site_id = args[1]
        local success_count = 0
        
        -- Get values from args (starting from index 2)
        local values = {}
        for i = 2, 1 + #keys do
            values[i-1] = args[i]
        end
        
        -- Get TTLs if provided (after values)
        local ttls = {}
        for i = 2 + #keys, 1 + 2*#keys do
            if i <= #args then
                ttls[i-(1+#keys)] = tonumber(args[i]) or 0
            else
                ttls[i-(1+#keys)] = 0  -- Default TTL is 0 (no expiry)
            end
        end
        
        -- Process each key-value pair (P2CF002 fix: using safe_call)
        for i, key in ipairs(keys) do
            local namespaced_key = build_key(site_id, key)
            local value = values[i]
            local ttl = ttls[i]

            -- Set the value
            local _, err = safe_call('SET', namespaced_key, value)
            if not err then
                -- Apply TTL if specified
                if ttl > 0 then
                    safe_call('EXPIRE', namespaced_key, ttl)
                end
                success_count = success_count + 1
            end
        end

        return success_count
    end,
    description = 'Sets multiple key-value pairs in a single operation'
}

-- Multi-DEL operation
server.register_function{
    function_name = 'GNODE_BATCH_MDEL',
    callback = function(keys, args)
        -- Validate inputs
        if #args < 1 then
            return server.error_reply("Missing site ID")
        end
        if #keys < 1 then
            return server.error_reply("No keys provided")
        end
        
        local site_id = args[1]
        local total_deleted = 0
        
        -- Process each key (P2CF002 fix: using safe_call)
        for _, key in ipairs(keys) do
            local namespaced_key = build_key(site_id, key)
            local deleted, err = safe_call('DEL', namespaced_key)
            if not err and deleted then
                total_deleted = total_deleted + deleted
            end
        end

        return total_deleted
    end,
    description = 'Deletes multiple keys in a single operation'
}

-- Multi-EXISTS operation
server.register_function{
    function_name = 'GNODE_BATCH_MEXISTS',
    callback = function(keys, args)
        -- Validate inputs
        if #args < 1 then
            return server.error_reply("Missing site ID")
        end
        if #keys < 1 then
            return server.error_reply("No keys provided")
        end
        
        local site_id = args[1]
        local results = {}
        local success_count = 0
        
        -- Process each key (P2CF002 fix: using safe_call)
        for i, key in ipairs(keys) do
            local namespaced_key = build_key(site_id, key)
            local result, err = safe_call('EXISTS', namespaced_key)
            if err then
                results[i] = {error = err}
            else
                local exists = (result == 1)
                results[i] = exists
                if exists then success_count = success_count + 1 end
            end
        end

        -- Return result as JSON array with success count and values
        return json_encode({success_count, results})
    end,
    flags = {'no-writes'},
    description = 'Checks existence of multiple keys in a single operation'
}

-- Multi-EXPIRE operation
server.register_function{
    function_name = 'GNODE_BATCH_MEXPIRE',
    callback = function(keys, args)
        -- Validate inputs
        if #args < 1 then
            return server.error_reply("Missing site ID")
        end
        if #keys < 1 then
            return server.error_reply("No keys provided")
        end
        if #args < 1 + #keys then
            return server.error_reply("Missing TTL values for keys")
        end
        
        local site_id = args[1]
        local success_count = 0
        
        -- Get TTLs from args (starting from index 2)
        local ttls = {}
        for i = 2, 1 + #keys do
            ttls[i-1] = tonumber(args[i]) or 0
        end
        
        -- Process each key (P2CF002 fix: using safe_call)
        for i, key in ipairs(keys) do
            local namespaced_key = build_key(site_id, key)
            local ttl = ttls[i]

            -- Only apply TTL if key exists
            local exists, exists_err = safe_call('EXISTS', namespaced_key)
            if not exists_err and exists == 1 then
                local _, expire_err = safe_call('EXPIRE', namespaced_key, ttl)
                if not expire_err then
                    success_count = success_count + 1
                end
            end
        end

        return success_count
    end,
    description = 'Sets expiration on multiple keys in a single operation'
}

-- Multi-PERSIST operation
server.register_function{
    function_name = 'GNODE_BATCH_MPERSIST',
    callback = function(keys, args)
        -- Validate inputs
        if #args < 1 then
            return server.error_reply("Missing site ID")
        end
        if #keys < 1 then
            return server.error_reply("No keys provided")
        end
        
        local site_id = args[1]
        local success_count = 0
        
        -- Process each key (P2CF002 fix: using safe_call)
        for _, key in ipairs(keys) do
            local namespaced_key = build_key(site_id, key)
            local persisted, err = safe_call('PERSIST', namespaced_key)
            if not err and persisted then
                success_count = success_count + persisted
            end
        end

        return success_count
    end,
    description = 'Removes expiration from multiple keys in a single operation'
}

-- Multi-INCR operation
server.register_function{
    function_name = 'GNODE_BATCH_MINCR',
    callback = function(keys, args)
        -- Validate inputs
        if #args < 1 then
            return server.error_reply("Missing site ID")
        end
        if #keys < 1 then
            return server.error_reply("No keys provided")
        end
        
        local site_id = args[1]
        local results = {}
        local success_count = 0
        
        -- Get increment values from args (starting from index 2)
        local increments = {}
        for i = 2, 1 + #keys do
            if i <= #args then
                increments[i-1] = tonumber(args[i]) or 1
            else
                increments[i-1] = 1  -- Default increment is 1
            end
        end
        
        -- Process each key (P2CF002 fix: using safe_call)
        for i, key in ipairs(keys) do
            local namespaced_key = build_key(site_id, key)
            local increment = increments[i]

            local new_value, err = safe_call('INCRBY', namespaced_key, increment)
            if err then
                results[i] = {error = err}
            else
                results[i] = new_value
                success_count = success_count + 1
            end
        end

        -- Return result as JSON array with success count and values
        return json_encode({success_count, results})
    end,
    description = 'Increments multiple keys in a single operation'
}

-- Multi-TTL operation
server.register_function{
    function_name = 'GNODE_BATCH_MTTL',
    callback = function(keys, args)
        -- Validate inputs
        if #args < 1 then
            return server.error_reply("Missing site ID")
        end
        if #keys < 1 then
            return server.error_reply("No keys provided")
        end
        
        local site_id = args[1]
        local results = {}
        local success_count = 0
        
        -- Process each key
        for i, key in ipairs(keys) do
            local namespaced_key = build_key(site_id, key)
            local remaining = server.call('TTL', namespaced_key)
            
            results[i] = remaining
            if remaining > -2 then  -- -2 means key doesn't exist
                success_count = success_count + 1
            end
        end
        
        -- Return result as JSON array with success count and values
        return json_encode({success_count, results})
    end,
    flags = {'no-writes'},
    description = 'Gets time-to-live for multiple keys in a single operation'
}

-- Multi-HGET operation
server.register_function{
    function_name = 'GNODE_BATCH_MHGET',
    callback = function(keys, args)
        -- Validate inputs
        if #args < 1 then
            return server.error_reply("Missing site ID")
        end
        if #keys < 1 then
            return server.error_reply("No keys provided")
        end
        if #args < 1 + #keys then
            return server.error_reply("Missing field names for keys")
        end
        
        local site_id = args[1]
        local results = {}
        local success_count = 0
        
        -- Get field names from args (starting from index 2)
        local fields = {}
        for i = 2, 1 + #keys do
            fields[i-1] = args[i]
        end
        
        -- Process each key-field pair
        for i, key in ipairs(keys) do
            local namespaced_key = build_key(site_id, key)
            local field = fields[i]
            
            local value = server.call('HGET', namespaced_key, field)
            results[i] = value
            
            if value ~= false then
                success_count = success_count + 1
            end
        end
        
        -- Return result as JSON array with success count and values
        return json_encode({success_count, results})
    end,
    flags = {'no-writes'},
    description = 'Gets hash fields for multiple keys in a single operation'
}

-- Multi-HSET operation
server.register_function{
    function_name = 'GNODE_BATCH_MHSET',
    callback = function(keys, args)
        -- Validate inputs
        if #args < 1 then
            return server.error_reply("Missing site ID")
        end
        if #keys < 1 then
            return server.error_reply("No keys provided")
        end
        if #args < 1 + 2*#keys then
            return server.error_reply("Missing field names or values for keys")
        end
        
        local site_id = args[1]
        local success_count = 0
        
        -- Get field names and values from args
        local fields = {}
        local values = {}
        
        for i = 1, #keys do
            fields[i] = args[1 + i]
            values[i] = args[1 + #keys + i]
        end
        
        -- Process each key-field-value trio
        for i, key in ipairs(keys) do
            local namespaced_key = build_key(site_id, key)
            local field = fields[i]
            local value = values[i]
            
            server.call('HSET', namespaced_key, field, value)
            success_count = success_count + 1
        end
        
        return success_count
    end,
    description = 'Sets hash fields for multiple keys in a single operation'
}