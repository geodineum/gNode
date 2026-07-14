#!lua name=gnode_lock

--
-- gNode LOCK Functions
-- A ValKey function library for lock operations
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

        -- Store context as JSON in a sorted set for time-ordered access
        local ok, ctx_json = pcall(cjson.encode, context)
        if ok and ctx_json then
            server.call('ZADD', ctx_key, timestamp, timestamp .. ':' .. ctx_json)

            -- Keep only recent metrics (last 1000)
            local count = server.call('ZCARD', ctx_key)
            if count > 1000 then
                server.call('ZREMRANGEBYRANK', ctx_key, 0, count - 1001)
            end
        end
    end
end

-- Helper function for formatting lock state
local function format_lock_state(state)
    if not state or #state == 0 then
        return nil
    end
    
    -- Convert array to dictionary
    local lock_state = {}
    for i = 1, #state, 2 do
        lock_state[state[i]] = state[i + 1]
    end
    
    return lock_state
end

-- Register lock acquisition function (mirroring CacheScriptsLockManager::LOCK_ACQUIRE)
-- NOTE: This is an atomic single-attempt acquisition. Retries must be handled client-side.
server.register_function{
    function_name = 'GNODE_LOCK_ACQUIRE',
    callback = function(keys, args)
        -- Input validation
        if #keys < 1 then
            return server.error_reply("Resource key required")
        end
        if not args[1] then
            return server.error_reply("Site ID required")
        end
        if not args[2] then
            return server.error_reply("Token required")
        end

        local resource = keys[1]
        local site_id = args[1]
        local token = args[2]
        local timeout = tonumber(args[3] or 30)  -- Default 30 seconds lock TTL

        -- Track operation timing
        local start_time = server.call('TIME')[1]

        -- Build lock key in same slot as resource
        local lock_key = '{' .. site_id .. '}:lock:' .. resource
        local lock_meta = '{' .. site_id .. '}:lock:meta:' .. resource

        -- RESP3 response structure
        local response = { boolean = false }

        -- Atomic single-attempt lock acquisition using SET NX EX
        if server.call('SET', lock_key, token, 'NX', 'EX', timeout) then
            -- Record complete lock state atomically
            server.call('HMSET', lock_meta,
                'token', token,
                'acquired_at', start_time,
                'timeout', timeout,
                'site', site_id
            )
            server.call('EXPIRE', lock_meta, timeout)

            -- Update metrics
            track_metric(site_id, 'locks_acquired', 1, {
                resource = resource,
                latency = server.call('TIME')[1] - start_time
            })

            response.boolean = true
        else
            -- Lock already held - return failure (client should retry if needed)
            track_metric(site_id, 'locks_contended', 1, {
                resource = resource
            })
        end

        return response
    end,
    description = 'Acquires a distributed lock (single atomic attempt, client handles retry)'
}

-- Register lock release function (mirroring CacheScriptsLockManager::LOCK_RELEASE)
server.register_function{
    function_name = 'GNODE_LOCK_RELEASE',
    callback = function(keys, args)
        -- Input validation
        if #keys < 1 then
            return server.error_reply("Resource key required")
        end
        if not args[1] then
            return server.error_reply("Site ID required")
        end
        if not args[2] then
            return server.error_reply("Token required")
        end
        
        local resource = keys[1]
        local site_id = args[1]
        local token = args[2]
        
        -- Track operation timing
        local start_time = server.call('TIME')[1]
        
        -- Build lock keys in same slot as resource
        local lock_key = '{' .. site_id .. '}:lock:' .. resource
        local lock_meta = '{' .. site_id .. '}:lock:meta:' .. resource
        
        -- RESP3 response structure
        local response = { boolean = false }
        
        -- Get full lock state
        local state = server.call('HGETALL', lock_meta)
        if #state == 0 then
            -- Lock doesn't exist or expired
            return response
        end
        
        -- Format lock state
        local lock_state = format_lock_state(state)
        
        -- Verify ownership
        if lock_state['token'] == token then
            -- Cleanup all lock state atomically
            server.call('DEL', lock_key)
            server.call('DEL', lock_meta)
            
            -- Update metrics
            local held_time = server.call('TIME')[1] - tonumber(lock_state['acquired_at'] or start_time)
            track_metric(site_id, 'locks_released', 1, {
                resource = resource,
                held_time = held_time
            })
            
            response.boolean = true
        else
            -- Token mismatch - potential security issue
            track_metric(site_id, 'lock_mismatches', 1, {
                resource = resource,
                expected = token,
                actual = lock_state['token']
            })
        end
        
        return response
    end,
    description = 'Releases a distributed lock with token verification'
}

-- Register lock status function (mirroring CacheScriptsLockManager::IS_LOCKED)
server.register_function{
    function_name = 'GNODE_LOCK_IS_LOCKED',
    callback = function(keys, args)
        -- Input validation
        if #keys < 1 then
            return server.error_reply("Resource key required")
        end
        if not args[1] then
            return server.error_reply("Site ID required")
        end
        
        local resource = keys[1]
        local site_id = args[1]
        
        -- Build lock key in same slot as resource
        local lock_key = '{' .. site_id .. '}:lock:' .. resource
        
        -- Track operation timing
        local start_time = server.call('TIME')[1]
        
        -- RESP3 response structure
        local response = { boolean = false }
        
        -- Check if lock exists and is valid
        local exists = server.call('EXISTS', lock_key)
        
        if exists == 1 then
            -- Track lock check metric
            track_metric(site_id, 'lock_checks', 1)
            response.boolean = true
        end
        
        return response
    end,
    -- Note: No no-writes flag because track_metric writes to metrics hash
    description = 'Checks if a resource is currently locked'
}

-- Register lock info function (extended functionality beyond original scripts)
server.register_function{
    function_name = 'GNODE_LOCK_INFO',
    callback = function(keys, args)
        -- Input validation
        if #keys < 1 then
            return server.error_reply("Resource key required")
        end
        if not args[1] then
            return server.error_reply("Site ID required")
        end
        
        local resource = keys[1]
        local site_id = args[1]
        
        -- Build lock keys in same slot as resource
        local lock_key = '{' .. site_id .. '}:lock:' .. resource
        local lock_meta = '{' .. site_id .. '}:lock:meta:' .. resource
        
        -- Track operation timing
        local start_time = server.call('TIME')[1]
        
        -- RESP3 response structure with map
        local response = { map = {
            locked = false,
            exists = false,
            info = {}
        }}
        
        -- Check if lock exists
        local exists = server.call('EXISTS', lock_key)
        
        if exists == 1 then
            response.map.locked = true
            response.map.exists = true
            
            -- Get detailed lock metadata
            local state = server.call('HGETALL', lock_meta)
            if #state > 0 then
                local lock_state = format_lock_state(state)
                
                response.map.info = {
                    acquired_at = tonumber(lock_state['acquired_at'] or 0),
                    timeout = tonumber(lock_state['timeout'] or 0),
                    attempts = tonumber(lock_state['attempts'] or 1),
                    site = lock_state['site'] or site_id,
                    ttl = server.call('TTL', lock_key)
                }
            end
            
            -- Track lock check metric
            track_metric(site_id, 'lock_checks', 1)
        end
        
        return response
    end,
    -- Note: No no-writes flag because track_metric writes to metrics hash
    description = 'Gets detailed information about a lock'
}