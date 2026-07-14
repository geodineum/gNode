#!lua name=gnode_utils

--
-- gNode UTILS Functions
-- A ValKey function library for utility operations
--
-- This is a port of the gCore Cache Scripts to ValKey functions
-- with enhancements for RESP3 compatibility
--

-- Function to detect if we're running on ValKey vs Redis
local function detect_server()
    local ok, info = pcall(function()
        return server.call('INFO', 'SERVER')
    end)
    
    local is_valkey = false
    local server_version = "unknown"
    
    if ok and type(info) == "string" then
        -- Check if this is ValKey
        if info:find("valkey_version") then
            is_valkey = true
            server_version = info:match("valkey_version:([%d%.]+)")
        else
            -- Redis OSS likely
            server_version = info:match("redis_version:([%d%.]+)")
        end
    end
    
    return { 
        is_valkey = is_valkey,
        version = server_version
    }
end

-- Function to parse version string to numeric components
local function parse_version(version_str)
    local major, minor, patch = version_str:match("(%d+)%.(%d+)%.(%d+)")
    if major then
        return {
            major = tonumber(major) or 0,
            minor = tonumber(minor) or 0,
            patch = tonumber(patch) or 0,
            raw = version_str
        }
    end
    return { major = 0, minor = 0, patch = 0, raw = version_str }
end

-- Function to check if current version is at least the required version
local function version_at_least(required)
    local server_info = detect_server()
    local current = parse_version(server_info.version)
    local req = parse_version(required)
    
    if current.major > req.major then return true end
    if current.major < req.major then return false end
    if current.minor > req.minor then return true end
    if current.minor < req.minor then return false end
    return current.patch >= req.patch
end

-- Function to check if cluster mode is enabled
local function is_cluster_enabled()
    local ok, result = pcall(function()
        return server.call('CLUSTER', 'INFO')
    end)
    
    if ok and type(result) == "string" and result:find("cluster_state:") then
        return result:find("cluster_state:ok") ~= nil
    end
    return false
end

-- Function to safely encode/decode data in different formats
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
    local ok, result = pcall(function()
        return cjson.decode(json_str)
    end)
    
    if not ok then
        return nil, "JSON decoding failed: " .. tostring(result)
    end
    
    return result
end

-- Function to parse data in various formats (JSON, MessagePack, etc.)
local function parse_data(data_str)
    -- Try JSON first (most common)
    local ok, result = pcall(function()
        return cjson.decode(data_str)
    end)
    
    if ok then 
        return result, nil, "json" 
    end
    
    -- Try MessagePack as fallback
    ok, result = pcall(function()
        return cmsgpack.unpack(data_str)
    end)
    
    if ok then 
        return result, nil, "msgpack" 
    end
    
    -- Neither format worked
    return nil, "Failed to parse data as JSON or MessagePack", nil
end

-- Register build key function
server.register_function{
    function_name = 'GNODE_UTILS_BUILD_KEY',
    callback = function(keys, args)
        -- Validate inputs
        if #args < 2 then
            return server.error_reply("Missing required arguments")
        end
        
        local site_id = args[1]
        local key = args[2]
        local group = args[3]
        local prefix = args[4] or ""
        
        -- Validate arguments
        if not key or key == "" then
            return server.error_reply("Key required")
        end
        
        if not site_id or site_id == "" then
            return server.error_reply("Site ID required")
        end
        
        -- Enable key to be already prefixed with site ID slot for optimized calls
        if key:find("^{" .. site_id .. "}") then
            return key
        end
        
        -- Fast path for simple keys (most common case)
        if not group then
            return '{' .. site_id .. '}:' .. prefix .. key
        end
        
        -- Group path needs validation
        local group_key = '{' .. site_id .. '}:groups'
        local exists = server.call('EXISTS', group_key)
        if exists == 0 then
            -- Create groups key if it doesn't exist
            server.call('HSET', group_key, group, '{}')
        else
            -- Verify group exists
            if server.call('HEXISTS', group_key, group) == 0 then
                return server.error_reply("Invalid group: " .. group)
            end
        end
        
        -- Return fully namespaced key with group
        return '{' .. site_id .. '}:' .. prefix .. group .. ':' .. key
    end,
    flags = {},  -- This function can write (HSET for group creation at line 163)
    description = 'Builds a properly namespaced key with site isolation'
}

-- Register track metric function
server.register_function{
    function_name = 'GNODE_UTILS_TRACK_METRIC',
    callback = function(keys, args)
        -- Validate inputs
        if #args < 2 then
            return server.error_reply("Missing required arguments")
        end
        
        local site_id = args[1]
        local metric_type = args[2]
        local value = tonumber(args[3]) or 1
        local extra_json = args[4]
        
        -- Guard against missing arguments
        if not site_id or site_id == "" then
            return false
        end
        
        if not metric_type then
            return false
        end
        
        -- Track site-specific metric
        local site_metrics = '{' .. site_id .. '}:metrics'
        server.call('HINCRBY', site_metrics, metric_type, value)
        
        -- Store additional metric data if provided
        if extra_json then
            local details_key = site_metrics .. ':details:' .. metric_type
            
            -- Validate JSON
            local extra, err = safe_json_decode(extra_json)
            if extra then
                local encoded = safe_json_encode(extra)
                if encoded then
                    server.call('LPUSH', details_key, encoded)
                    server.call('LTRIM', details_key, 0, 999)  -- Keep last 1000 entries
                end
            end
        end
        
        -- Global metrics if enabled
        local global_metrics_enabled = server.call('GET', 'global_metrics_enabled')
        if global_metrics_enabled == '1' then
            server.call('HINCRBY', '{global}:metrics', metric_type, value)
        end
        
        -- Performance tracking for latency metrics
        if metric_type:match('^latency:') then
            local latency_key = '{' .. site_id .. '}:latency'
            local time = server.call('TIME')
            server.call('ZADD', latency_key, value, tostring(time[1]))
            server.call('ZREMRANGEBYRANK', latency_key, 0, -10001)  -- Keep last 10K samples
        end
        
        return true
    end,
    description = 'Tracks metrics with proper namespacing'
}

-- Register validate keys function
server.register_function{
    function_name = 'GNODE_UTILS_VALIDATE_KEYS',
    callback = function(keys, args)
        -- Validate inputs
        if #args < 2 then
            return server.error_reply("Missing required arguments")
        end
        
        local site_id = args[1]
        local keys_json = args[2]
        local max_batch_size = tonumber(args[3]) or 1000
        local max_key_length = tonumber(args[4]) or 256
        
        -- Parse keys JSON
        local keys_list, err = safe_json_decode(keys_json)
        if not keys_list then
            return server.error_reply("Invalid keys JSON: " .. err)
        end
        
        -- Check batch size
        if #keys_list > max_batch_size then
            return server.error_reply("Batch size exceeds limit of " .. max_batch_size)
        end
        
        -- Check key lengths
        for _, key in ipairs(keys_list) do
            if type(key) ~= "string" then
                return server.error_reply("Keys must be strings")
            end
            
            if #key > max_key_length then
                return server.error_reply("Key length exceeds " .. max_key_length .. " bytes")
            end
        end
        
        -- Validate slot consistency for cluster mode
        if is_cluster_enabled() and #keys_list > 1 then
            -- Get slot of first key
            local first_key = '{' .. site_id .. '}:' .. keys_list[1]
            local base_slot = server.call('CLUSTER', 'KEYSLOT', first_key)
            
            -- Check if all keys hash to the same slot
            for i=2, #keys_list do
                local key = '{' .. site_id .. '}:' .. keys_list[i]
                local slot = server.call('CLUSTER', 'KEYSLOT', key)
                
                if slot ~= base_slot then
                    return server.error_reply("Keys must hash to same slot for atomic operations")
                end
            end
        end
        
        return "OK"
    end,
    flags = {'no-writes'}, -- This function only validates, doesn't write
    description = 'Validates a batch of keys for security and performance constraints'
}

-- Register server info function
server.register_function{
    function_name = 'GNODE_UTILS_SERVER_INFO',
    callback = function(keys, args)
        local server_info = detect_server()
        local cluster_enabled = is_cluster_enabled()
        
        -- Gather additional information
        local memory_info
        local ok, info = pcall(function()
            return server.call('INFO', 'MEMORY')
        end)
        
        if ok and type(info) == "string" then
            memory_info = {
                used_memory = info:match("used_memory:(%d+)"),
                used_memory_peak = info:match("used_memory_peak:(%d+)"),
                maxmemory = info:match("maxmemory:(%d+)")
            }
        end
        
        -- Get current time
        local time = server.call('TIME')
        
        -- Build server info
        local result = {
            server = {
                type = server_info.is_valkey and "valkey" or "redis",
                version = server_info.version
            },
            cluster = {
                enabled = cluster_enabled
            },
            memory = memory_info,
            time = {
                seconds = tonumber(time[1]),
                microseconds = tonumber(time[2])
            },
            functions = {
                available = true,
                library = "gnode_utils"
            }
        }
        
        -- Return as JSON
        local json, err = safe_json_encode(result)
        if not json then
            return server.error_reply("Failed to encode server info: " .. err)
        end
        
        return json
    end,
    flags = {'no-writes'}, -- This function only reads info
    description = 'Returns information about the ValKey server'
}

-- Register detect cycles function
server.register_function{
    function_name = 'GNODE_UTILS_DETECT_CYCLES',
    callback = function(keys, args)
        -- Validate inputs
        if #args < 2 then
            return server.error_reply("Missing required arguments")
        end
        
        local site_id = args[1]
        local node = args[2]
        
        -- Setup recursive cycle detection
        local function detect_cycles_recursive(current_node, ancestors)
            -- Initialize ancestor tracking
            ancestors = ancestors or {}
            
            -- Check immediate cycle
            if ancestors[current_node] then
                local path = {}
                for k, _ in pairs(ancestors) do
                    table.insert(path, k)
                end
                table.insert(path, current_node)
                return { has_cycle = true, path = path }
            end
            
            -- Get parent with proper site isolation
            local parent_key = '{' .. site_id .. '}:groups:' .. current_node .. ':parent'
            local parent = server.call('GET', parent_key)
            
            -- No parent means no cycle possible
            if not parent then
                return { has_cycle = false }
            end
            
            -- Build new ancestors set with current node
            local new_ancestors = { [current_node] = true }
            for k, v in pairs(ancestors) do
                new_ancestors[k] = v
            end
            
            -- Recursive check
            return detect_cycles_recursive(parent, new_ancestors)
        end
        
        -- Run the cycle detection
        local result = detect_cycles_recursive(node, {})
        
        -- Convert to JSON for response
        local json, err = safe_json_encode(result)
        if not json then
            return server.error_reply("Failed to encode cycle detection result: " .. err)
        end
        
        return json
    end,
    flags = {'no-writes'}, -- This function only reads data
    description = 'Detects cycles in a graph-like structure'
}

-- Register validate permissions function
server.register_function{
    function_name = 'GNODE_UTILS_VALIDATE_PERMISSIONS',
    callback = function(keys, args)
        -- Validate inputs
        if #args < 1 then
            return server.error_reply("Missing permissions JSON")
        end
        
        local permissions_json = args[1]
        
        -- Parse permissions JSON
        local permissions, err = safe_json_decode(permissions_json)
        if not permissions then
            return server.error_reply("Invalid permissions JSON: " .. err)
        end
        
        -- Valid permission types
        local valid_types = {
            read = true,
            write = true,
            delete = true,
            admin = true
        }
        
        -- Valid permission values
        local valid_values = {
            allow = true,
            deny = true,
            inherit = true
        }
        
        -- Validate structure
        if type(permissions) ~= 'table' then
            return server.error_reply("Permissions must be a table")
        end
        
        -- Validate each permission
        for perm_type, value in pairs(permissions) do
            if not valid_types[perm_type] then
                return server.error_reply("Invalid permission type: " .. perm_type)
            end
            if not valid_values[value] then
                return server.error_reply("Invalid permission value for " .. perm_type)
            end
        end
        
        -- Return success
        return "valid"
    end,
    flags = {'no-writes'}, -- This function only validates
    description = 'Validates permission settings'
}

-- Register keys pattern function (using SCAN for safety - doesn't block server)
server.register_function{
    function_name = 'GNODE_KEYS_PATTERN',
    callback = function(keys, args)
        -- Validate inputs
        if #args < 1 then
            return server.error_reply("Missing pattern argument")
        end

        local pattern = args[1]
        local site_id = args[2] or ''
        local limit = tonumber(args[3]) or 1000
        local count_per_scan = tonumber(args[4]) or 100

        -- If site_id provided but pattern doesn't include it, prefix with site_id
        if site_id ~= '' and not pattern:find("^{" .. site_id .. "}") and not pattern:find("^" .. site_id .. ":") then
            pattern = '{' .. site_id .. '}:' .. pattern
        end

        -- Use SCAN for safety (not KEYS which blocks the server)
        local cursor = '0'
        local results = {}
        local iterations = 0
        local max_iterations = 10000  -- Safety limit to prevent infinite loops

        repeat
            local scan_result = server.call('SCAN', cursor, 'MATCH', pattern, 'COUNT', count_per_scan)
            cursor = scan_result[1]

            for _, key in ipairs(scan_result[2]) do
                table.insert(results, key)
                if #results >= limit then
                    break
                end
            end

            iterations = iterations + 1
            if iterations >= max_iterations then
                -- Safety: stop after too many iterations
                break
            end
        until cursor == '0' or #results >= limit

        -- Return results as JSON array for consistent parsing
        local json, err = safe_json_encode(results)
        if not json then
            return server.error_reply("Failed to encode keys: " .. (err or "unknown error"))
        end

        return json
    end,
    flags = {'no-writes'},
    description = 'Finds keys matching pattern using SCAN (safe for production)'
}

-- Register get time function
server.register_function{
    function_name = 'GNODE_UTILS_GET_TIME',
    callback = function(keys, args)
        -- Get current time from ValKey's TIME command
        local time = server.call('TIME')
        local seconds = tonumber(time[1])
        local microseconds = tonumber(time[2])
        
        -- Create ISO-8601 timestamp manually without using os.date
        -- which isn't available in ValKey Lua environment
        local epoch = seconds
        local days_since_epoch = math.floor(epoch / 86400)
        local seconds_today = epoch % 86400
        
        -- Base date: 1970-01-01
        local year = 1970
        local month = 1
        local day = 1
        
        -- Add days to base date
        for i = 1, days_since_epoch do
            day = day + 1
            
            -- Check month lengths, including leap years
            local month_days = 31
            if month == 4 or month == 6 or month == 9 or month == 11 then
                month_days = 30
            elseif month == 2 then
                if (year % 4 == 0 and year % 100 ~= 0) or (year % 400 == 0) then
                    month_days = 29
                else
                    month_days = 28
                end
            end
            
            -- Roll over to next month if needed
            if day > month_days then
                day = 1
                month = month + 1
                
                -- Roll over to next year if needed
                if month > 12 then
                    month = 1
                    year = year + 1
                end
            end
        end
        
        -- Calculate hours, minutes, seconds
        local hours = math.floor(seconds_today / 3600)
        local minutes = math.floor((seconds_today % 3600) / 60)
        local secs = seconds_today % 60
        
        -- Format components to ensure leading zeros
        local year_str = tostring(year)
        local month_str = string.format("%02d", month)
        local day_str = string.format("%02d", day)
        local hours_str = string.format("%02d", hours)
        local minutes_str = string.format("%02d", minutes)
        local seconds_str = string.format("%02d", secs)
        
        -- Assemble ISO-8601 timestamp
        local iso8601 = year_str .. "-" .. month_str .. "-" .. day_str .. "T" .. 
                        hours_str .. ":" .. minutes_str .. ":" .. seconds_str .. "Z"
        
        -- Return formatted time
        local result = {
            seconds = seconds,
            microseconds = microseconds,
            total_microseconds = seconds * 1000000 + microseconds,
            iso8601 = iso8601,
            timestamp = seconds
        }
        
        -- Convert to JSON
        local json, err = safe_json_encode(result)
        if not json then
            return server.error_reply("Failed to encode time: " .. err)
        end
        
        return json
    end,
    flags = {'no-writes'}, -- This function only reads time
    description = 'Gets current time with microsecond precision'
}