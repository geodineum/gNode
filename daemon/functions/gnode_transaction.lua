#!lua name=gnode_transaction

--
-- gNode TRANSACTION Functions
-- A ValKey function library for transaction operations
--
-- This is a port of the gCore Cache Scripts to ValKey functions
-- with enhancements for RESP3 compatibility
-- 

--[[
  - Transaction initialization
  - Adding operations to transactions
  - Committing transactions
  - Rolling back transactions
  - Transaction status and monitoring
  
  All functions use RESP3 compatible responses with proper error handling.
  
  Usage:
  - GNODE_TRANSACTION_BEGIN(tx_id, site_id, timeout)
  - GNODE_TRANSACTION_ADD_OP(tx_id, op_type, key, value, site_id)
  - GNODE_TRANSACTION_COMMIT(tx_id, site_id)
  - GNODE_TRANSACTION_ROLLBACK(tx_id, site_id)
  - GNODE_TRANSACTION_STATUS(tx_id, site_id)
]]

-- Function to track metrics
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

-- Helper function to encode JSON
local function safe_json_encode(value)
    local ok, result = pcall(function()
        return cjson.encode(value)
    end)
    
    if not ok then
        return nil, "JSON encoding failed: " .. tostring(result)
    end
    
    return result
end

-- Helper function to decode JSON
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

-- Register transaction begin function (mirroring CacheScriptsTransactionManager::TRANSACTION_BEGIN)
server.register_function{
    function_name = 'GNODE_TRANSACTION_BEGIN',
    callback = function(keys, args)
        -- Input validation
        if #args < 2 then
            return server.error_reply("Missing required arguments")
        end
        
        local tx_id = args[1]
        local site_id = args[2]
        local timeout = tonumber(args[3] or '300')  -- Default 5 min timeout
        
        -- Generate slot-aligned transaction keys
        local base = '{' .. site_id .. ':tx:' .. tx_id .. '}'
        local keys_map = {
            meta = base .. ':meta',
            changes = base .. ':changes',
            locks = base .. ':locks'
        }
        
        -- Check if transaction already exists
        if server.call('EXISTS', keys_map.meta) == 1 then
            return server.error_reply("Transaction already exists: " .. tx_id)
        end
        
        -- Current time for tracking
        local timestamp = server.call('TIME')
        local current_time = tonumber(timestamp[1])
        
        -- Register transaction
        server.call('HMSET', keys_map.meta,
            'id', tx_id,
            'site', site_id,
            'status', 'active',
            'start_time', current_time,
            'op_count', 0
        )
        
        -- Set expiration on all transaction keys
        for _, key in pairs(keys_map) do
            server.call('EXPIRE', key, timeout)
        end
        
        -- Update metrics
        track_metric(site_id, 'transactions_started', 1, {
            tx_id = tx_id,
            timeout = timeout
        })
        
        -- Build RESP3-compatible response
        local response = {
            map = {
                transaction_id = tx_id,
                status = "active",
                start_time = current_time,
                timeout = timeout,
                expires_at = current_time + timeout
            }
        }
        
        -- Convert to RESP3 or JSON depending on server capability
        if server.response_version and server.response_version() >= 3 then
            return response
        else
            local json, err = safe_json_encode(response)
            if not json then
                return server.error_reply("JSON encoding error: " .. (err or "unknown"))
            end
            return json
        end
    end,
    description = 'Begins a transaction with proper site isolation and key alignment'
}

-- Register transaction add operation function
server.register_function{
    function_name = 'GNODE_TRANSACTION_ADD_OP',
    callback = function(keys, args)
        -- Input validation
        if #args < 5 then
            return server.error_reply("Missing required arguments")
        end
        
        local tx_id = args[1]
        local op_type = args[2]
        local key = args[3]
        local value = args[4]
        local site_id = args[5]
        
        -- Generate slot-aligned transaction keys
        local base = '{' .. site_id .. ':tx:' .. tx_id .. '}'
        local keys_map = {
            meta = base .. ':meta',
            changes = base .. ':changes',
            locks = base .. ':locks'
        }
        
        -- Check if transaction exists and is active
        local exists = server.call('EXISTS', keys_map.meta)
        if exists == 0 then
            return server.error_reply("Transaction not found: " .. tx_id)
        end
        
        local status = server.call('HGET', keys_map.meta, 'status')
        if status ~= 'active' then
            return server.error_reply("Transaction not active: " .. tx_id .. " (Status: " .. status .. ")")
        end
        
        -- Get current operation count
        local op_count = tonumber(server.call('HGET', keys_map.meta, 'op_count') or '0')
        local op_key = keys_map.changes .. ':' .. op_count
        
        -- Validate operation type
        local valid_ops = {
            SET = true,
            DEL = true,
            INCR = true,
            DECR = true,
            EXPIRE = true,
            PERSIST = true,
            HSET = true,
            HDEL = true,
            HINCRBY = true,
            ZADD = true,
            ZREM = true,
            ZINCRBY = true
        }
        
        if not valid_ops[op_type] then
            return server.error_reply("Invalid operation type: " .. op_type)
        end
        
        -- If needed, get current value for rollback
        local current_value = nil
        if op_type == 'SET' or op_type == 'DEL' or op_type == 'INCR' or op_type == 'DECR' then
            current_value = server.call('GET', key)
        elseif op_type == 'EXPIRE' or op_type == 'PERSIST' then
            current_value = server.call('TTL', key)
        elseif op_type == 'HSET' or op_type == 'HDEL' then
            local field = value
            current_value = server.call('HGET', key, field)
        elseif op_type == 'HINCRBY' then
            local field = value:match("^([^:]+):")
            current_value = server.call('HGET', key, field)
        elseif op_type == 'ZADD' or op_type == 'ZREM' then
            local member = value:match("^[^:]+:(.+)")
            current_value = server.call('ZSCORE', key, member)
        elseif op_type == 'ZINCRBY' then
            local member = value:match("^[^:]+:(.+)")
            current_value = server.call('ZSCORE', key, member)
        end
        
        -- Store operation with current value for potential rollback
        server.call('HMSET', op_key,
            'type', op_type,
            'key', key,
            'value', value,
            'current', current_value or ''
        )
        
        -- Update operation count
        server.call('HSET', keys_map.meta, 'op_count', op_count + 1)
        
        -- Update metrics
        track_metric(site_id, 'transaction_operations', 1, {
            tx_id = tx_id,
            op_type = op_type,
            key = key
        })
        
        -- Build RESP3-compatible response
        local response = {
            map = {
                transaction_id = tx_id,
                operation = {
                    map = {
                        id = op_count,
                        type = op_type,
                        key = key
                    }
                },
                success = true
            }
        }
        
        -- Convert to RESP3 or JSON depending on server capability
        if server.response_version and server.response_version() >= 3 then
            return response
        else
            local json, err = safe_json_encode(response)
            if not json then
                return server.error_reply("JSON encoding error: " .. (err or "unknown"))
            end
            return json
        end
    end,
    description = 'Adds an operation to a transaction with rollback information'
}

-- Register transaction commit function
server.register_function{
    function_name = 'GNODE_TRANSACTION_COMMIT',
    callback = function(keys, args)
        -- Input validation
        if #args < 2 then
            return server.error_reply("Missing required arguments")
        end
        
        local tx_id = args[1]
        local site_id = args[2]
        
        -- Generate slot-aligned transaction keys
        local base = '{' .. site_id .. ':tx:' .. tx_id .. '}'
        local keys_map = {
            meta = base .. ':meta',
            changes = base .. ':changes',
            locks = base .. ':locks'
        }
        
        -- Check if transaction exists and is active
        local exists = server.call('EXISTS', keys_map.meta)
        if exists == 0 then
            return server.error_reply("Transaction not found: " .. tx_id)
        end
        
        local status = server.call('HGET', keys_map.meta, 'status')
        if status ~= 'active' then
            return server.error_reply("Transaction not active: " .. tx_id .. " (Status: " .. status .. ")")
        end
        
        -- Get operation count
        local op_count = tonumber(server.call('HGET', keys_map.meta, 'op_count') or '0')
        
        -- Execute all operations
        local executed = 0
        local errors = {}
        
        for i = 0, op_count - 1 do
            local op_key = keys_map.changes .. ':' .. i
            local op = server.call('HGETALL', op_key)
            
            if #op > 0 then
                -- Convert to a table
                local op_data = {}
                for j = 1, #op, 2 do
                    op_data[op[j]] = op[j+1]
                end
                
                -- Execute operation
                local ok, result = pcall(function()
                    if op_data.type == 'SET' then
                        return server.call('SET', op_data.key, op_data.value)
                    elseif op_data.type == 'DEL' then
                        return server.call('DEL', op_data.key)
                    elseif op_data.type == 'INCR' then
                        return server.call('INCRBY', op_data.key, op_data.value)
                    elseif op_data.type == 'DECR' then
                        return server.call('DECRBY', op_data.key, op_data.value)
                    elseif op_data.type == 'EXPIRE' then
                        return server.call('EXPIRE', op_data.key, op_data.value)
                    elseif op_data.type == 'PERSIST' then
                        return server.call('PERSIST', op_data.key)
                    elseif op_data.type == 'HSET' then
                        local field, value = op_data.value:match("^([^:]+):(.+)")
                        return server.call('HSET', op_data.key, field, value)
                    elseif op_data.type == 'HDEL' then
                        return server.call('HDEL', op_data.key, op_data.value)
                    elseif op_data.type == 'HINCRBY' then
                        local field, amount = op_data.value:match("^([^:]+):(.+)")
                        return server.call('HINCRBY', op_data.key, field, amount)
                    elseif op_data.type == 'ZADD' then
                        local score, member = op_data.value:match("^([^:]+):(.+)")
                        return server.call('ZADD', op_data.key, score, member)
                    elseif op_data.type == 'ZREM' then
                        return server.call('ZREM', op_data.key, op_data.value)
                    elseif op_data.type == 'ZINCRBY' then
                        local amount, member = op_data.value:match("^([^:]+):(.+)")
                        return server.call('ZINCRBY', op_data.key, amount, member)
                    else
                        return nil, "Unknown operation type: " .. op_data.type
                    end
                end)
                
                if ok then
                    executed = executed + 1
                else
                    table.insert(errors, {
                        op_id = i,
                        op_type = op_data.type,
                        key = op_data.key,
                        error = tostring(result)
                    })
                end
            end
        end
        
        -- Update transaction status
        local new_status = #errors > 0 and 'partial' or 'committed'
        server.call('HSET', keys_map.meta, 'status', new_status)
        server.call('HSET', keys_map.meta, 'commit_time', server.call('TIME')[1])
        server.call('HSET', keys_map.meta, 'executed_ops', executed)
        
        if #errors > 0 then
            server.call('HSET', keys_map.meta, 'errors', safe_json_encode(errors))
        end
        
        -- Update metrics
        track_metric(site_id, 'transactions_committed', 1, {
            tx_id = tx_id,
            operations = op_count,
            executed = executed,
            errors = #errors,
            status = new_status
        })
        
        -- Build RESP3-compatible response
        local response = {
            map = {
                transaction_id = tx_id,
                status = new_status,
                operations = {
                    total = op_count,
                    executed = executed,
                    failed = #errors
                },
                success = new_status == 'committed'
            }
        }
        
        if #errors > 0 then
            response.map.errors = { set = {} }
            for _, err in ipairs(errors) do
                response.map.errors.set[tostring(err.op_id)] = {
                    map = {
                        op_type = err.op_type,
                        key = err.key,
                        error = err.error
                    }
                }
            end
        end
        
        -- Convert to RESP3 or JSON depending on server capability
        if server.response_version and server.response_version() >= 3 then
            return response
        else
            local json, err = safe_json_encode(response)
            if not json then
                return server.error_reply("JSON encoding error: " .. (err or "unknown"))
            end
            return json
        end
    end,
    description = 'Commits a transaction and executes all operations'
}

-- Register transaction rollback function
server.register_function{
    function_name = 'GNODE_TRANSACTION_ROLLBACK',
    callback = function(keys, args)
        -- Input validation
        if #args < 2 then
            return server.error_reply("Missing required arguments")
        end
        
        local tx_id = args[1]
        local site_id = args[2]
        
        -- Generate slot-aligned transaction keys
        local base = '{' .. site_id .. ':tx:' .. tx_id .. '}'
        local keys_map = {
            meta = base .. ':meta',
            changes = base .. ':changes',
            locks = base .. ':locks'
        }
        
        -- Check if transaction exists
        local exists = server.call('EXISTS', keys_map.meta)
        if exists == 0 then
            return server.error_reply("Transaction not found: " .. tx_id)
        end
        
        -- Get transaction status
        local status = server.call('HGET', keys_map.meta, 'status')
        
        -- Only active transactions can be rolled back
        if status ~= 'active' then
            return server.error_reply("Cannot rollback transaction with status: " .. status)
        end
        
        -- Mark transaction as rolled back
        server.call('HSET', keys_map.meta, 'status', 'rolled_back')
        server.call('HSET', keys_map.meta, 'rollback_time', server.call('TIME')[1])
        
        -- Update metrics
        track_metric(site_id, 'transactions_rolled_back', 1, {
            tx_id = tx_id
        })
        
        -- Build RESP3-compatible response
        local response = {
            map = {
                transaction_id = tx_id,
                status = 'rolled_back',
                success = true
            }
        }
        
        -- Convert to RESP3 or JSON depending on server capability
        if server.response_version and server.response_version() >= 3 then
            return response
        else
            local json, err = safe_json_encode(response)
            if not json then
                return server.error_reply("JSON encoding error: " .. (err or "unknown"))
            end
            return json
        end
    end,
    description = 'Rolls back a transaction and prevents further operations'
}

-- Register transaction status function
server.register_function{
    function_name = 'GNODE_TRANSACTION_STATUS',
    callback = function(keys, args)
        -- Input validation
        if #args < 2 then
            return server.error_reply("Missing required arguments")
        end
        
        local tx_id = args[1]
        local site_id = args[2]
        
        -- Generate slot-aligned transaction keys
        local base = '{' .. site_id .. ':tx:' .. tx_id .. '}'
        local keys_map = {
            meta = base .. ':meta',
            changes = base .. ':changes',
            locks = base .. ':locks'
        }
        
        -- Check if transaction exists
        local exists = server.call('EXISTS', keys_map.meta)
        if exists == 0 then
            return server.error_reply("Transaction not found: " .. tx_id)
        end
        
        -- Get transaction metadata
        local meta = server.call('HGETALL', keys_map.meta)
        
        -- Convert to a table
        local meta_data = {}
        for i = 1, #meta, 2 do
            meta_data[meta[i]] = meta[i+1]
        end
        
        -- Get operation count
        local op_count = tonumber(meta_data.op_count or '0')
        
        -- Build list of operations
        local operations = {}
        for i = 0, op_count - 1 do
            local op_key = keys_map.changes .. ':' .. i
            local op = server.call('HGETALL', op_key)
            
            if #op > 0 then
                -- Convert to a table
                local op_data = {}
                for j = 1, #op, 2 do
                    op_data[op[j]] = op[j+1]
                end
                
                operations[i+1] = op_data
            end
        end
        
        -- Build RESP3-compatible response
        local response = {
            map = {
                transaction_id = tx_id,
                status = meta_data.status or 'unknown',
                start_time = tonumber(meta_data.start_time or '0'),
                op_count = op_count,
                operations = { set = {} }
            }
        }
        
        -- Add commit time if available
        if meta_data.commit_time then
            response.map.commit_time = tonumber(meta_data.commit_time)
        end
        
        -- Add rollback time if available
        if meta_data.rollback_time then
            response.map.rollback_time = tonumber(meta_data.rollback_time)
        end
        
        -- Add executed ops if available
        if meta_data.executed_ops then
            response.map.executed_ops = tonumber(meta_data.executed_ops)
        end
        
        -- Add operations
        for i, op in ipairs(operations) do
            response.map.operations.set[tostring(i-1)] = {
                map = {
                    type = op.type,
                    key = op.key,
                    value = op.value
                }
            }
        end
        
        -- Add errors if available
        if meta_data.errors then
            local ok, errors = pcall(function()
                return cjson.decode(meta_data.errors)
            end)
            
            if ok and errors then
                response.map.errors = { set = {} }
                for _, err in ipairs(errors) do
                    response.map.errors.set[tostring(err.op_id)] = {
                        map = {
                            op_type = err.op_type,
                            key = err.key,
                            error = err.error
                        }
                    }
                end
            end
        end
        
        -- Update metrics
        track_metric(site_id, 'transaction_status_checks', 1, {
            tx_id = tx_id,
            status = meta_data.status
        })
        
        -- Convert to RESP3 or JSON depending on server capability
        if server.response_version and server.response_version() >= 3 then
            return response
        else
            local json, err = safe_json_encode(response)
            if not json then
                return server.error_reply("JSON encoding error: " .. (err or "unknown"))
            end
            return json
        end
    end,
    -- Note: No no-writes flag because track_metric writes to metrics hash
    description = 'Gets transaction status and details'
}