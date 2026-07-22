#!lua name=gnode_stream

--
-- gNode STREAM Functions
-- A ValKey function library for stream operations
--
-- This is a port of the gCore Cache Scripts to ValKey functions
-- with enhancements for RESP3 compatibility
-- 

--[[
  This implementation directly mirrors functionality from gCore's CacheScriptsStreamOperations.php
  while adding ValKey compatibility enhancements and RESP3 optimizations.
  
  Usage:
  - GNODE_STREAM_ADD(stream_key, request_id, command, parameters, [site_id], [node_id], [timestamp])
      Emits both 'command'/'parameters' and 'cmd'/'params' for the same values;
      consumers disagree on the spelling and the disagreement fails silently.
      For caller-defined fields use GNODE_STREAM_ADD_RESP3, which flattens a
      JSON entry as given: GNODE_STREAM_ADD_RESP3(stream_key, entry_json, site_id, max_len)
  - GNODE_STREAM_CREATE_GROUP(stream_key, group, site_id, start_position)
  - GNODE_STREAM_READ_GROUP(stream_key, group, consumer, site_id, count)
  - GNODE_STREAM_ACK(stream_key, group, message_id, site_id)
  - GNODE_STREAM_INFO(stream_key, site_id)
  - GNODE_STREAM_TRIM(stream_key, site_id, max_len)
  - GNODE_STREAM_PENDING(stream_key, group, site_id, start, end, count, consumer)
  - GNODE_STREAM_CLAIM(stream_key, group, consumer, min_idle_time, site_id, start, end, count)
  - GNODE_STREAM_DEL(stream_key, site_id, message_ids_json)
  - GNODE_STREAM_RESPOND(stream_key, message_id, response_json)
  - Original basic functions: GROUP_READ, READ, GROUP, BATCH_READ, BATCH_GROUP_READ, etc.
  
  All functions use proper error handling with RESP3 output when available and follow ValKey best practices.
  
  === STANDARDIZED PARAMETER ORDERING ===
  To ensure consistency between daemon.rs and ValKey functions, we follow these parameter conventions:
  
  For GNODE_STREAM_RESPOND:
  1. stream_key: The stream to send the response to (key parameter)
  2. message_id: The ID of the original command to correlate responses with (arg parameter)
  3. response_json: The JSON response data (arg parameter)
  
  This standardization ensures proper alignment between daemon response handling via:
  - Direct XADD in the daemon
  - ValKey GNODE_STREAM_RESPOND function calls
  - Fallback script execution
]]

-- ------------------------------------------------------------------------
-- JSON functions (safe implementation)
-- ------------------------------------------------------------------------

local function json_encode(obj)
    local ok, result = pcall(cjson.encode, obj)
    if not ok then
        return "null"
    end
    return result
end

local function json_decode(str)
    local ok, result = pcall(function()
        return cjson.decode(str)
    end)
    
    if not ok then
        return nil, "JSON decode error: " .. tostring(result)
    end
    
    return result
end

local function parse_json_array(json_str)
    -- Simple JSON array parser for message IDs
    local items = {}
    local json = json_str:gsub('[%[%]" ]', '')
    for item in string.gmatch(json, '[^,]+') do
        table.insert(items, item)
    end
    return items
end

-- Register stream consumer group read function
server.register_function{
    function_name = 'GNODE_STREAM_GROUP_READ',
    callback = function(keys, args)
        -- Validate inputs
        if #keys < 1 then
            return server.error_reply('Missing stream key')
        end
        if #args < 3 then
            return server.error_reply('Insufficient arguments')
        end
        
        -- Extract parameters with proper type handling
        local stream_key = keys[1]
        local group_name = args[1]
        local consumer_name = args[2]
        -- Use requested batch size to respect the daemon's dynamic batch sizing
        local count = tonumber(args[3] or '100')
        local block = tonumber(args[4] or 0)
        -- ">" is the default for normal operation, "0" for recovering all pending messages
        local id = args[5] or '>'  -- Use provided ID or default to new messages only
        local site_id = args[6] or "default" -- Extract site_id for batch metrics
        
        -- First, ensure the stream and group exist to prevent NOGROUP errors
        -- This checks atomically if the stream exists, and if not, creates it
        local stream_exists = server.call('EXISTS', stream_key) == 1
        
        if not stream_exists then
            -- Create an empty stream with a minimal message
            server.call('XADD', stream_key, '*', 'init', 'true')
        end
        
        -- Check if the group exists
        local group_exists = false
        local success, groups = pcall(function()
            return server.call('XINFO', 'GROUPS', stream_key)
        end)
        
        if success and groups then
            for _, group in ipairs(groups) do
                if group[1] == 'name' and group[2] == group_name then
                    group_exists = true
                    break
                end
            end
        end
        
        -- Create the group if it doesn't exist
        if not group_exists then
            local create_result = server.pcall('XGROUP', 'CREATE', stream_key, group_name, '$', 'MKSTREAM')
            if create_result.err and not create_result.err:find('BUSYGROUP') then
                return server.error_reply("Failed to create consumer group: " .. create_result.err)
            end
        end
        
        -- Prepare XREADGROUP command with proper parameter types
        local xreadgroup_args = {'XREADGROUP', 'GROUP', group_name, consumer_name}
        
        -- Add COUNT parameter
        table.insert(xreadgroup_args, 'COUNT')
        table.insert(xreadgroup_args, count)
        
        -- Add BLOCK parameter - mandatory for proper stream behavior to avoid polling
        -- Block with a default timeout if none provided
        local block_timeout = block and block > 0 and block or 1889 -- Default 1889ms for optimal responsiveness
        table.insert(xreadgroup_args, 'BLOCK')
        table.insert(xreadgroup_args, block_timeout)
        
        -- Add STREAMS and key/ID parameters
        table.insert(xreadgroup_args, 'STREAMS')
        table.insert(xreadgroup_args, stream_key)
        table.insert(xreadgroup_args, id)
        
        -- Execute with proper error handling
        local success, result = pcall(function()
            return server.call(unpack(xreadgroup_args))
        end)
        
        if not success then
            return server.error_reply("Error reading group: " .. tostring(result))
        end
        
        -- Most important fix: XREADGROUP with no messages returns nil
        -- ValKey protocol converts nil to false in some clients
        -- This was causing parsing errors in the daemon
        if result == nil or result == false then
            -- Return empty array for compatibility with JSON format expected by daemon
            return json_encode({})
        end

        -- Check if we have a RESP3 protocol format
        -- Use safer approach to check version as server.info() might not be available in all ValKey versions
        local has_resp3 = false
        local server_info = server.call("INFO", "server")
        if server_info and server_info:find("redis_version:7%.") then
            has_resp3 = true
        end
        
        if has_resp3 then
            -- For RESP3 protocol, provide additional debug information in the response
            -- to help the client correctly parse the nested format
            local debug_prefix = ">> RESP3_FORMAT = "
            
            -- Fix for batch processing: We need to process multiple entries in a single read
            -- Check for multiple entries in the command stream
            if result and type(result) == 'table' and #result > 0 then
                -- Format all messages into a single batch for easier processing
                local all_stream_messages = {}
                for i, stream_data in ipairs(result) do
                    local stream_name = tostring(stream_data[1])
                    local messages = stream_data[2] or {}
                    
                    -- Add all messages in this batch for the current stream
                    all_stream_messages[i] = {stream_name, messages}
                end
                
                if #all_stream_messages > 0 then
                    -- Log the batch size for debugging with enhanced information
                    local total_messages = 0
                    for _, stream_entry in ipairs(all_stream_messages) do
                        if stream_entry[2] and type(stream_entry[2]) == 'table' then
                            total_messages = total_messages + #stream_entry[2]
                        end
                    end
                    
                    -- Add diagnostics for batch debugging
                    server.call('HSET', '{' .. site_id .. '}:metrics:batch', 'last_batch_size', total_messages)
                    server.call('HINCRBY', '{' .. site_id .. '}:metrics:batch', 'total_batches', 1)
                    server.call('HINCRBY', '{' .. site_id .. '}:metrics:batch', 'total_messages', total_messages)
                    
                    -- Debug output to diagnose batch issue
                    for i, stream_entry in ipairs(all_stream_messages) do
                        local stream_name = stream_entry[1]
                        local messages = stream_entry[2]
                        server.call('HINCRBY', '{' .. site_id .. '}:metrics:batch:streams', stream_name, #messages)
                        server.call('HSET', '{' .. site_id .. '}:metrics:batch:last_count', stream_name, #messages)
                    end
                    
                    -- Return all messages in a single batch
                    return debug_prefix .. json_encode(all_stream_messages)
                end
            end
            
            -- Fallback to original behavior
            return debug_prefix .. json_encode(result)
        else
            -- Standard RESP2 processing for backward compatibility
            -- Convert the response to a compatible JSON format for the Rust client
            local formatted_result = {}
            if type(result) == 'table' and #result > 0 then
                for i, stream_data in ipairs(result) do
                    local stream_name = stream_data[1]
                    local messages = stream_data[2]
                    local formatted_messages = {}
                    
                    for j, msg in ipairs(messages) do
                        local id = msg[1]
                        local fields = {}
                        for k = 1, #msg[2], 2 do
                            fields[k] = tostring(msg[2][k])
                            fields[k+1] = tostring(msg[2][k+1])
                        end
                        formatted_messages[j] = {id, fields}
                    end
                    
                    formatted_result[i] = {stream_name, formatted_messages}
                end
            end
            
            -- Return the formatted result as JSON
            return json_encode(formatted_result)
        end
    end,
    description = 'Reads messages from a stream using consumer groups with improved parameter handling'
}

-- Register stream read function
server.register_function{
    function_name = 'GNODE_STREAM_READ',
    callback = function(keys, args)
        -- Validate inputs
        if #keys < 1 then
            return server.error_reply("Missing stream key")
        end
        
        -- Extract parameters
        local stream_key = keys[1]
        local min_id = args[1] or '-'
        local max_id = args[2] or '+'
        local count = tonumber(args[3] or '100')
        
        -- Execute the XRANGE command
        local messages = server.call('XRANGE', stream_key, min_id, max_id, 'COUNT', count)
        
        -- Format the results
        local formatted_messages = {}
        for i, msg in ipairs(messages) do
            local id = msg[1]
            local fields = {}
            for j = 1, #msg[2], 2 do
                fields[j] = tostring(msg[2][j])
                fields[j+1] = tostring(msg[2][j+1])
            end
            formatted_messages[i] = {id, fields}
        end
        
        return json_encode(formatted_messages)
    end,
    flags = {'no-writes'},
    description = 'Reads messages from a stream'
}

-- Register stream add function
server.register_function{
    function_name = 'GNODE_STREAM_ADD',
    callback = function(keys, args)
        -- Validate inputs
        if #keys < 1 then
            return server.error_reply("Missing stream key")
        end
        if #args < 3 then
            return server.error_reply("Insufficient arguments")
        end
        
        -- Extract parameters
        local stream_key = keys[1]
        local request_id = args[1]
        local command = args[2]
        local parameters = args[3]
        local site_id = args[4] or "default"
        local node_id = args[5] or "default"
        local timestamp = args[6] or tostring(server.call('TIME')[1])

        -- Emit BOTH the long ('command'/'parameters') and canonical
        -- ('cmd'/'params') field names for the same values.
        --
        -- WHY: consumers of this stream do not agree on the spelling, and the
        -- disagreement fails SILENTLY.
        --
        --   * The gNode daemon is tolerant — utils.rs field_names accepts
        --     CMD = c|cmd|command|command_name and PARAMS = p|params|parameters
        --     — so it reads either form.
        --   * command_processor.rs (the RESP2 stream path) is strict the OTHER
        --     way: fields.get("command") / fields.get("parameters") with
        --     unwrap_or_default(). Dropping the long names would hand it empty
        --     strings, so they must stay.
        --   * Strict consumers of the CANONICAL form -- Geodine's
        --     pipeline_runner.php is the live example -- test
        --     `empty($fields['cmd'])` to decide whether an entry is a command
        --     at all. Given only 'command', they classify it as a response or
        --     metadata, ACK it, and DISCARD it. No error is raised anywhere:
        --     the sender waits out its timeout and the symptom is
        --     indistinguishable from the service never having answered.
        --
        -- Writing both is additive and costs two field/value pairs per entry.
        -- Renaming would trade one silent failure for another.
        --
        -- Deliberately NOT added here: 't' (message type). Adding it would
        -- change how the daemon's type dispatch routes these entries, which is
        -- a behavioural change and not what this fix is for.
        local msg_id = server.call('XADD', stream_key, '*',
            'id', request_id,
            'command', command,
            'cmd', command,
            'parameters', parameters,
            'params', parameters,
            'site_id', site_id,
            'node_id', node_id,
            'timestamp', timestamp
        )

        return msg_id
    end,
    description = 'Adds a message to a stream (emits both command/parameters and cmd/params)'
}

-- Register stream acknowledge function
server.register_function{
    function_name = 'GNODE_STREAM_ACK',
    callback = function(keys, args)
        -- Validate inputs
        if #keys < 1 then
            return server.error_reply("Missing stream key")
        end
        if #args < 2 then
            return server.error_reply("Insufficient arguments")
        end
        
        -- Extract parameters
        local stream_key = keys[1]
        local group_name = args[1]
        local msg_ids_json = args[2]
        local thread_id = args[3] or "worker-unknown"  -- Worker thread identifier
        
        -- Parse message IDs
        local msg_ids = {}
        
        -- Handle JSON parsing with multiple fallbacks
        if type(msg_ids_json) == 'string' then
            -- First try cjson decode with protection
            local ok, decoded = pcall(function() 
                return cjson.decode(msg_ids_json) 
            end)
            
            if ok and type(decoded) == 'table' then
                msg_ids = decoded
            else
                -- Clean input for simple parsing as fallback
                local cleaned = msg_ids_json:gsub('[%[%]" ]', '')
                for item in string.gmatch(cleaned, '[^,]+') do
                    if item and #item > 0 then
                        table.insert(msg_ids, item)
                    end
                end
            end
        elseif type(msg_ids_json) == 'table' then
            -- Direct table input
            msg_ids = msg_ids_json
        end
        
        -- Filter for valid message IDs
        local valid_ids = {}
        for _, id in ipairs(msg_ids) do
            -- Check for valid format (timestamp-sequence)
            if type(id) == 'string' and string.match(id, '%d+%-%d+') then
                table.insert(valid_ids, id)
            end
        end
        
        -- Early return if no valid IDs
        if #valid_ids == 0 then
            -- Format the response for RESP3 compatibility
            local resp3_str = ">> RESP3_FORMAT = " .. tostring(0)
            return resp3_str
        end
        
        -- Create a distributed lock key specific to these message IDs
        local lock_key = stream_key .. ":ack-lock:" .. group_name
        local lock_token = thread_id .. ":" .. server.call('TIME')[1]
        local lock_timeout = 5000  -- 5 seconds in milliseconds
        
        -- Try to acquire the lock
        local locked = server.call('SET', lock_key, lock_token, 'NX', 'PX', lock_timeout)
        
        -- If we couldn't get the lock, another thread is processing these messages
        if not locked then
            -- Check who holds the lock (for logging)
            local lock_holder = server.call('GET', lock_key)
            local debug_key = stream_key .. ":ack-conflicts:" .. group_name 
            server.call('HINCRBY', debug_key, thread_id, 1)
            
            -- Return 0 to indicate no messages were acknowledged by this thread
            return ">> RESP3_FORMAT = " .. tostring(0)
        end
        
        -- We have the lock, process acknowledgments with better error handling
        local ack_count = 0
        
        -- Use batching for efficiency with more than 10 IDs
        if #valid_ids > 10 then
            -- Build arguments array for batch acknowledgment
            local xack_args = {'XACK', stream_key, group_name}
            for _, id in ipairs(valid_ids) do
                table.insert(xack_args, id)
            end
            
            -- Protected call using standard Lua pcall
            local success, result = pcall(function()
                return server.call(unpack(xack_args))
            end)
            
            if success then
                ack_count = result
            else
                -- Release the lock before returning error
                server.call('DEL', lock_key)
                return server.error_reply("Error acknowledging messages: " .. tostring(result))
            end
        else
            -- Process IDs individually for better resilience
            for _, id in ipairs(valid_ids) do
                local success, result = pcall(function()
                    return server.call('XACK', stream_key, group_name, id)
                end)
                
                if success then
                    ack_count = ack_count + (result or 0)
                end
                -- Continue with other IDs even if one fails
            end
        end
        
        -- Track successful batch acknowledgments for metrics
        local metrics_key = stream_key .. ":metrics:ack_batches"
        server.call('HINCRBY', metrics_key, "count", 1)
        server.call('HINCRBY', metrics_key, "total_messages", ack_count)
        server.call('HSET', metrics_key, "last_batch_size", ack_count)
        server.call('HSET', metrics_key, "last_thread", thread_id)
        
        -- Release the lock when done
        server.call('DEL', lock_key)
        
        -- Format the response for RESP3 compatibility
        local resp3_str = ">> RESP3_FORMAT = " .. tostring(ack_count)
        return resp3_str
    end,
    description = 'Atomically acknowledges messages in a consumer group with distributed locking to prevent race conditions between workers'
}

-- Register stream group management function
server.register_function{
    function_name = 'GNODE_STREAM_GROUP',
    callback = function(keys, args)
        -- Validate inputs
        if #keys < 1 then
            return server.error_reply('Missing stream key')
        end
        if #args < 3 then
            return server.error_reply('Insufficient arguments')
        end
        
        -- Extract parameters - Handle the invocation from Rust code
        local stream_key = keys[1]
        local group_name = args[1]
        local id = "$"
        local operation = "CREATE"
        
        -- Detect if parameters are in a different order
        for i=1, #args do
            if args[i] == "CREATE" then
                operation = "CREATE"
            elseif args[i] == "DESTROY" then
                operation = "DESTROY"
            elseif args[i] == "DELCONSUMER" then
                operation = "DELCONSUMER"
            elseif args[i] == "SETID" then
                operation = "SETID"
            end
        end
        
        -- Look for ID parameter (any parameter that starts with '$' or a timestamp)
        for i=1, #args do
            if args[i] and (args[i]:sub(1,1) == '$' or args[i]:match('%d+%-%d+')) then
                id = args[i]
                break
            end
        end
        
        -- Handle CREATE operation
        if operation == "CREATE" then
            -- Look for MKSTREAM flag
            local mkstream = false
            for i = 1, #args do
                if args[i] == "MKSTREAM" then
                    mkstream = true
                    break
                end
            end
            
            local command_args
            if mkstream then
                command_args = {'XGROUP', 'CREATE', stream_key, group_name, id, 'MKSTREAM'}
            else
                command_args = {'XGROUP', 'CREATE', stream_key, group_name, id}
            end
            
            -- Execute with error handling using pcall
            local result = server.pcall(unpack(command_args))
            
            if result.err then
                if result.err:find('BUSYGROUP') then
                    return 'OK'  -- Group already exists, which is fine
                else
                    return server.error_reply(result.err)
                end
            end
            
            return 'OK'
        elseif operation == "DESTROY" then
            return server.call('XGROUP', 'DESTROY', stream_key, group_name)
        elseif operation == "DELCONSUMER" then
            local consumer = nil
            -- Find the consumer parameter
            for i=1, #args do
                if args[i] ~= "DELCONSUMER" and args[i] ~= group_name and args[i] ~= id then
                    consumer = args[i]
                    break
                end
            end
            
            if not consumer then
                return server.error_reply('Missing consumer argument')
            end
            
            return server.call('XGROUP', 'DELCONSUMER', stream_key, group_name, consumer)
        elseif operation == "SETID" then
            return server.call('XGROUP', 'SETID', stream_key, group_name, id)
        else
            return server.error_reply('Unknown group operation: ' .. operation)
        end
    end,
    description = 'Manages consumer groups for streams with flexible parameter ordering'
}

-- Register stream delete function
server.register_function{
    function_name = 'GNODE_STREAM_DEL',
    callback = function(keys, args)
        -- Validate inputs
        if #keys < 1 then
            return server.error_reply("Missing stream key")
        end
        if #args < 1 then
            return server.error_reply("Missing message IDs")
        end
        
        -- Extract parameters
        local stream_key = keys[1]
        local msg_ids_json = args[1]
        
        -- Parse the message IDs JSON
        local msg_ids = parse_json_array(msg_ids_json)
        
        -- Delete the messages
        local deleted = 0
        if #msg_ids > 10 then
            local xdel_args = {'XDEL', stream_key}
            for _, id in ipairs(msg_ids) do
                table.insert(xdel_args, id)
            end
            deleted = server.call(unpack(xdel_args))
        else
            for _, id in ipairs(msg_ids) do
                deleted = deleted + server.call('XDEL', stream_key, id)
            end
        end
        
        return deleted
    end,
    description = 'Deletes messages from a stream'
}

-- Register stream pending messages function
server.register_function{
    function_name = 'GNODE_STREAM_PENDING',
    callback = function(keys, args)
        -- Validate inputs
        if #keys < 1 then
            return server.error_reply("Missing stream key")
        end
        if #args < 1 then
            return server.error_reply("Missing group name")
        end
        
        -- Extract parameters
        local stream_key = keys[1]
        local group_name = args[1]
        local start_id = args[2] or '-'
        local end_id = args[3] or '+'
        local count = tonumber(args[4] or '100')
        local consumer = args[5]
        
        -- Get the pending messages
        local result
        if consumer then
            result = server.call('XPENDING', stream_key, group_name, start_id, end_id, count, consumer)
        else
            result = server.call('XPENDING', stream_key, group_name, start_id, end_id, count)
        end
        
        return json_encode(result)
    end,
    flags = {'no-writes'},
    description = 'Gets pending messages for a consumer group'
}

-- Register stream respond function
server.register_function{
    function_name = 'GNODE_STREAM_RESPOND',
    callback = function(keys, args)
        -- Validate inputs
        if #keys < 1 then
            return server.error_reply("Missing stream key")
        end
        if #args < 2 then
            return server.error_reply("Insufficient arguments")
        end
        
        -- Extract parameters with STANDARDIZED ORDERING
        local stream_key = keys[1]
        local message_id = args[1]  -- Now contains a unique ID with batch context
        local response_json = args[2]
        
        -- Standardized parameters for both daemon and ValKey
        -- stream_key - The stream to send the response to
        -- message_id - The ID of the original command (to correlate responses)
        -- response_json - The JSON response data
        
        -- Extract batch context from message_id if present (format: original_id_batch_seq)
        local original_id = message_id
        local batch_id = nil
        local sequence_num = nil
        
        -- Check if this is a batch-formatted ID (contains underscores)
        local parts = {}
        for part in string.gmatch(message_id, "[^_]+") do
            table.insert(parts, part)
        end
        
        -- If we have the expected batch format with at least 3 parts
        if #parts >= 3 then
            -- Parse the components (we expect original_id_batch-id_sequence)
            -- The original_id itself might contain underscores, so we need to be careful
            sequence_num = tonumber(parts[#parts]) -- Last part is sequence
            batch_id = parts[#parts-1] -- Second-to-last is batch ID
            -- Everything else is the original ID
            local original_parts = {}
            for i = 1, #parts - 2 do
                table.insert(original_parts, parts[i])
            end
            original_id = table.concat(original_parts, "_")
        end
        
        -- Validate that we have valid JSON
        local is_valid_json = true
        local decoded_response
        
        -- Check if response_json is valid JSON
        local ok, result = pcall(function() 
            return cjson.decode(response_json) 
        end)
        
        if not ok or type(result) ~= 'table' then
            -- Invalid JSON, create a valid error response
            is_valid_json = false
            response_json = string.format('{"id":"%s","status":"error","error":"Invalid JSON","timestamp":%s}',
                message_id, server.call('TIME')[1])
        else
            decoded_response = result
            
            -- Ensure ID in the response JSON matches our unique ID
            if type(decoded_response) == 'table' then
                if decoded_response.id then
                    -- Keep the unique ID format (original_id_batch_id_sequence) in the response
                    -- This is crucial for response correlation and prevents overwriting
                    decoded_response.id = message_id
                end
                
                -- Always preserve batch context metadata in response to help with debugging
                if batch_id and sequence_num then
                    decoded_response.batch_id = batch_id
                    decoded_response.sequence = sequence_num
                    
                    -- Also store the original command ID for easier correlation in client code
                    decoded_response.original_id = original_id
                end
                
                -- Re-encode with our updates
                response_json = json_encode(decoded_response)
            end
        end
        
        -- Always add enhanced logging for batch debugging
        if batch_id and sequence_num then
            -- Log batch processing info to help with troubleshooting
            local debug_key = string.format('{%s}:metrics:batch:debug', batch_id)
            local debug_info = string.format(
                "RESP: batch=%s, seq=%d, id=%s, unique_id=%s", 
                batch_id, sequence_num, original_id, message_id
            )
            
            -- Store debug info in ValKey
            server.call('HSET', debug_key, message_id, debug_info)
            server.call('EXPIRE', debug_key, 3600) -- Keep for 1 hour
        end
        
        -- Handle case where response_json is a JSON array with field/value pairs
        if is_valid_json and response_json:sub(1,1) == '[' then
            -- Try to parse as a field/value array
            local parsed_fields = {}
            
            if type(decoded_response) == 'table' then
                -- If successful parsing, use the fields directly
                local xadd_args = {'XADD', stream_key, '*'}
                for i = 1, #decoded_response, 2 do
                    if i+1 <= #decoded_response then
                        table.insert(xadd_args, decoded_response[i])
                        table.insert(xadd_args, decoded_response[i+1])
                    end
                end
                
                local msg_id = server.call(unpack(xadd_args))
                return msg_id
            end
        end
        
        -- Always log every response attempt for debugging
        local debug_key = '{default}:metrics:batch:debug:responses'
        local debug_info = string.format(
            "RESPONSE_ATTEMPT: stream=%s, id=%s, seq=%s, batch=%s, time=%s", 
            stream_key, 
            message_id, 
            sequence_num or "none",
            batch_id or "none",
            server.call('TIME')[1]
        )
        server.call('HSET', debug_key, message_id, debug_info)
        server.call('EXPIRE', debug_key, 3600) -- Keep for 1 hour
        
        -- Log the full response JSON for debugging
        local response_debug_key = '{default}:metrics:batch:debug:response_json'
        server.call('HSET', response_debug_key, message_id, response_json)
        server.call('EXPIRE', response_debug_key, 3600) -- Keep for 1 hour

        -- Use the unique message_id to prevent overwriting in batch operations
        -- The key change: Always use '*' to generate a new stream message ID and
        -- store our unique formatted ID in the 'id' field to prevent collisions
        local msg_id = server.call('XADD', stream_key, '*', 
            'id', message_id,
            'response', response_json,
            'timestamp', server.call('TIME')[1],
            'debug', 'true' -- Add debug flag for better tracking
        )
        
        -- Record success info for debugging
        local success_key = '{default}:metrics:batch:debug:successes'
        local success_info = string.format(
            "RESPONSE_SUCCESS: stream=%s, id=%s, msg_id=%s, time=%s", 
            stream_key, message_id, msg_id, server.call('TIME')[1]
        )
        server.call('HSET', success_key, message_id, success_info)
        server.call('EXPIRE', success_key, 3600) -- Keep for 1 hour
        
        -- Record metrics for the batch response
        if batch_id and sequence_num then
            local metrics_key = '{default}:metrics:batch:responses'
            server.call('HINCRBY', metrics_key, batch_id, 1)
            server.call('HINCRBY', metrics_key, 'total', 1)
            server.call('HSET', metrics_key, 'last_batch', batch_id)
            server.call('HSET', metrics_key, 'last_message_id', message_id)
        end
        
        return msg_id
    end,
    description = 'Adds a response to a stream with batch context support to prevent response overwriting'
}

-- Register stream claim function
server.register_function{
    function_name = 'GNODE_STREAM_CLAIM',
    callback = function(keys, args)
        -- Validate inputs
        if #keys < 1 then
            return server.error_reply("Missing stream key")
        end
        if #args < 3 then
            return server.error_reply("Insufficient arguments")
        end
        
        -- Extract parameters
        local stream_key = keys[1]
        local group_name = args[1]
        local consumer_name = args[2]
        local min_idle_time = tonumber(args[3] or '30000')
        local msg_ids_json = args[4]
        
        -- Parse the message IDs JSON if provided
        local msg_ids = {}
        if msg_ids_json and msg_ids_json ~= "" then
            msg_ids = parse_json_array(msg_ids_json)
        end
        
        -- Claim the messages
        local result
        if #msg_ids == 0 then
            -- Use XAUTOCLAIM for automatic claiming of all pending messages
            result = server.call('XAUTOCLAIM', stream_key, group_name, consumer_name, min_idle_time, '0-0', 'COUNT', 100)
        else
            -- Use XCLAIM for specific message IDs
            local xclaim_args = {'XCLAIM', stream_key, group_name, consumer_name, min_idle_time}
            for _, id in ipairs(msg_ids) do
                table.insert(xclaim_args, id)
            end
            result = server.call(unpack(xclaim_args))
        end
        
        -- Format the results
        local formatted_messages = {}
        for i, msg in ipairs(result) do
            if type(msg) == 'table' then
                local id = msg[1]
                local fields = {}
                for j = 1, #msg[2], 2 do
                    fields[j] = tostring(msg[2][j])
                    fields[j+1] = tostring(msg[2][j+1])
                end
                formatted_messages[i] = {id, fields}
            end
        end
        
        return json_encode(formatted_messages)
    end,
    description = 'Claims pending messages for a consumer'
}

-- Register stream trim function
server.register_function{
    function_name = 'GNODE_STREAM_TRIM',
    callback = function(keys, args)
        -- Validate inputs
        if #keys < 1 then
            return server.error_reply("Missing stream key")
        end
        
        -- Extract parameters
        local stream_key = keys[1]
        local max_len = tonumber(args[1] or '1000')
        local approximate = args[2] == '~'
        
        -- Trim the stream
        local result
        if approximate then
            result = server.call('XTRIM', stream_key, 'MAXLEN', '~', max_len)
        else
            result = server.call('XTRIM', stream_key, 'MAXLEN', max_len)
        end
        
        return result
    end,
    description = 'Trims a stream to a maximum length'
}

-- Register batch stream read function
server.register_function{
    function_name = 'GNODE_STREAM_BATCH_READ',
    callback = function(keys, args)
        -- Validate inputs
        if #keys < 1 then
            return server.error_reply("Missing stream keys")
        end
        
        -- Extract parameters
        local count = tonumber(args[1] or '100')
        local block = tonumber(args[2] or '100')
        
        -- Prepare arguments
        local streams = {}
        for i=1, #keys do
            table.insert(streams, keys[i])
        end
        
        -- Get IDs for each stream
        local ids = {}
        for i=3, 2+#streams do
            if i-2 <= #streams then
                table.insert(ids, args[i] or '$')
            end
        end
        
        -- Ensure we have an ID for each stream
        while #ids < #streams do
            table.insert(ids, '$')
        end
        
        -- Execute the batch read
        local result
        if block > 0 then
            local xread_args = {'XREAD', 'COUNT', count, 'BLOCK', block, 'STREAMS'}
            for _, stream in ipairs(streams) do
                table.insert(xread_args, stream)
            end
            for _, id in ipairs(ids) do
                table.insert(xread_args, id)
            end
            result = server.call(unpack(xread_args))
        else
            local xread_args = {'XREAD', 'COUNT', count, 'STREAMS'}
            for _, stream in ipairs(streams) do
                table.insert(xread_args, stream)
            end
            for _, id in ipairs(ids) do
                table.insert(xread_args, id)
            end
            result = server.call(unpack(xread_args))
        end
        
        -- Format the results
        local formatted_result = {}
        if result and #result > 0 then
            for i, stream_data in ipairs(result) do
                local stream_name = stream_data[1]
                local messages = stream_data[2]
                local formatted_messages = {}
                
                for j, msg in ipairs(messages) do
                    local id = msg[1]
                    local fields = {}
                    for k = 1, #msg[2], 2 do
                        fields[k] = tostring(msg[2][k])
                        fields[k+1] = tostring(msg[2][k+1])
                    end
                    formatted_messages[j] = {id, fields}
                end
                
                formatted_result[i] = {stream_name, formatted_messages}
            end
        end
        
        return json_encode(formatted_result)
    end,
    flags = {'no-writes'},
    description = 'Reads messages from multiple streams'
}

-- Register batch stream group read function
server.register_function{
    function_name = 'GNODE_STREAM_BATCH_GROUP_READ',
    callback = function(keys, args)
        -- Validate inputs
        if #keys < 1 then
            return server.error_reply("Missing stream keys")
        end
        if #args < 3 then
            return server.error_reply("Insufficient arguments")
        end
        
        -- Extract parameters
        local group_name = args[1]
        local consumer_name = args[2]
        local count = tonumber(args[3] or '100')
        local block = tonumber(args[4] or '100')
        
        -- Prepare arguments
        local streams = {}
        for i=1, #keys do
            table.insert(streams, keys[i])
        end
        
        -- Get IDs for each stream
        local ids = {}
        for i=5, 4+#streams do
            if i-4 <= #streams then
                table.insert(ids, args[i] or '>')
            end
        end
        
        -- Ensure we have an ID for each stream
        while #ids < #streams do
            table.insert(ids, '>')
        end
        
        -- Execute the batch group read
        local result
        if block > 0 then
            local xreadgroup_args = {'XREADGROUP', 'GROUP', group_name, consumer_name, 
                'COUNT', count, 'BLOCK', block, 'STREAMS'}
            for _, stream in ipairs(streams) do
                table.insert(xreadgroup_args, stream)
            end
            for _, id in ipairs(ids) do
                table.insert(xreadgroup_args, id)
            end
            result = server.call(unpack(xreadgroup_args))
        else
            local xreadgroup_args = {'XREADGROUP', 'GROUP', group_name, consumer_name, 
                'COUNT', count, 'STREAMS'}
            for _, stream in ipairs(streams) do
                table.insert(xreadgroup_args, stream)
            end
            for _, id in ipairs(ids) do
                table.insert(xreadgroup_args, id)
            end
            result = server.call(unpack(xreadgroup_args))
        end
        
        -- Format the results
        local formatted_result = {}
        if result and #result > 0 then
            for i, stream_data in ipairs(result) do
                local stream_name = stream_data[1]
                local messages = stream_data[2]
                local formatted_messages = {}
                
                for j, msg in ipairs(messages) do
                    local id = msg[1]
                    local fields = {}
                    for k = 1, #msg[2], 2 do
                        fields[k] = tostring(msg[2][k])
                        fields[k+1] = tostring(msg[2][k+1])
                    end
                    formatted_messages[j] = {id, fields}
                end
                
                formatted_result[i] = {stream_name, formatted_messages}
            end
        end
        
        return json_encode(formatted_result)
    end,
    description = 'Reads messages from multiple streams using consumer groups'
}

-- Register stream info function
server.register_function{
    function_name = 'GNODE_STREAM_INFO',
    callback = function(keys, args)
        -- Validate inputs
        if #keys < 1 then
            return server.error_reply('Missing stream key')
        end
        
        -- Extract parameters
        local stream_key = keys[1]
        
        -- Check if stream exists first to avoid error on non-existent stream
        local exists = server.call('EXISTS', stream_key)
        if exists == 0 then
            -- Return empty info for non-existent stream rather than error
            return json_encode({
                length = 0,
                radix_tree_keys = 0,
                radix_tree_nodes = 0,
                groups = 0,
                last_generated_id = "0-0",
                first_entry = nil,
                last_entry = nil
            })
        end

        -- Try XINFO with proper error handling
        local success, info
        success, info = pcall(function()
            return server.call('XINFO', 'STREAM', stream_key)
        end)
        
        if not success or not info then
            -- If XINFO fails, try to get minimal info via other commands
            local err = tostring(info or "Unknown error")
            
            if err:find("ERR no such key") then
                -- Stream doesn't exist, return empty info
                return json_encode({
                    length = 0,
                    radix_tree_keys = 0,
                    radix_tree_nodes = 0,
                    groups = 0,
                    last_generated_id = "0-0",
                    first_entry = nil,
                    last_entry = nil
                })
            elseif err:find("WRONGTYPE") then
                -- Not a stream
                return server.error_reply("Key exists but is not a stream: " .. stream_key)
            else
                -- Try to get minimal info with error protection
                local len = 0
                success, len = pcall(function()
                    return server.call('XLEN', stream_key)
                end)
                
                if not success then
                    len = 0
                end
                
                return json_encode({
                    length = len,
                    radix_tree_keys = 0,  -- Unknown
                    radix_tree_nodes = 0, -- Unknown
                    groups = 0,           -- Unknown without XINFO
                    last_generated_id = "0-0", -- Unknown without XINFO
                    first_entry = nil,    -- Unknown without XRANGE
                    last_entry = nil      -- Unknown without XRANGE
                })
            end
        end
        
        -- Convert the raw XINFO response to a structured map for JSON encoding
        local result = {}
        if type(info) == 'table' then
            -- XINFO returns an array of field/value pairs
            for i = 1, #info, 2 do
                local field = info[i]
                local value = info[i+1]
                
                -- Handle nested arrays and special fields
                if type(value) == 'table' and #value >= 2 and type(value[1]) == 'string' and value[1]:match('%d+%-%d+') then
                    -- This is likely a message entry with ID and fields
                    local entry = {}
                    entry.id = value[1]
                    entry.fields = {}
                    
                    if type(value[2]) == 'table' then
                        for j = 1, #value[2], 2 do
                            entry.fields[value[2][j]] = value[2][j+1]
                        end
                    end
                    
                    result[field] = entry
                else
                    result[field] = value
                end
            end
        else
            -- Fallback if structure is unexpected
            result.length = 0
            result.error = "Invalid XINFO response format"
        end
        
        -- Ensure JSON encoding doesn't fail
        local ok, json = pcall(function()
            return cjson.encode(result)
        end)
        
        if not ok then
            -- Ultimate fallback
            return json_encode({
                length = 0,
                error = "JSON encoding error"
            })
        end
        
        return json
    end,
    flags = {'no-writes'},
    description = 'Gets information about a stream and consistent JSON output'
}

-- Register stream groups info function
server.register_function{
    function_name = 'GNODE_STREAM_GROUPS_INFO',
    callback = function(keys, args)
        -- Validate inputs
        if #keys < 1 then
            return server.error_reply("Missing stream key")
        end
        
        -- Extract parameters
        local stream_key = keys[1]
        
        -- Get stream groups info
        local info = server.call('XINFO', 'GROUPS', stream_key)
        
        -- Format as JSON for cross-language compatibility
        return json_encode(info)
    end,
    flags = {'no-writes'},
    description = 'Gets information about consumer groups for a stream'
}

-- Register stream consumers info function
server.register_function{
    function_name = 'GNODE_STREAM_CONSUMERS_INFO',
    callback = function(keys, args)
        -- Validate inputs
        if #keys < 1 then
            return server.error_reply("Missing stream key")
        end
        if #args < 1 then
            return server.error_reply("Missing group name")
        end
        
        -- Extract parameters
        local stream_key = keys[1]
        local group_name = args[1]
        
        -- Get stream consumers info
        local info = server.call('XINFO', 'CONSUMERS', stream_key, group_name)
        
        -- Format as JSON for cross-language compatibility
        return json_encode(info)
    end,
    flags = {'no-writes'},
    description = 'Gets information about consumers in a group'
}

-- Register STREAM_ADD function (mirroring CacheScriptsStreamOperations::STREAM_ADD)
server.register_function{
    function_name = 'GNODE_STREAM_ADD_RESP3',
    callback = function(keys, args)
        -- Input validation
        if #keys < 1 then
            return server.error_reply("Stream required")
        end
        if #args < 2 then
            return server.error_reply("Entry data and site ID required")
        end
        
        local stream = keys[1]
        local entry_data = args[1]
        local site_id = args[2]
        local max_len = tonumber(args[3] or '10000')  -- Default to 10K
        
        -- Set a reasonable entry size limit (default = 1MB)
        local max_entry_size = 1048576
        
        -- Estimate entry size
        local entry_size = 0
        if entry_data then
            entry_size = #entry_data
        end
        
        if entry_size > max_entry_size then
            return server.error_reply("Entry size exceeds limit")
        end
        
        local start_time = server.call('TIME')[1]
        
        -- Build RESP3-style response structure
        local response = {
            map = {
                stream = stream,
                success = false,
                timestamp = { double = start_time },
                metrics = { map = {} }
            }
        }
        
        -- Check length and apply backpressure
        local length = server.call('XLEN', stream)
        if length >= max_len then
            -- Trim stream to 50% of max length with metrics
            local trim_target = math.floor(max_len * 0.5)
            server.call('XTRIM', stream, 'MAXLEN', '~', trim_target)
            
            response.map.metrics.trimmed = {
                map = {
                    original_length = length,
                    new_target = trim_target
                }
            }
            -- Track trimming metrics
            local metrics_key = '{' .. site_id .. '}:metrics'
            server.call('HINCRBY', metrics_key, 'stream_trimmed', 1)
        end

        -- Parse entry data safely
        local ok, entry = pcall(cjson.decode, entry_data)
        if not ok then
            response.map.error = "Invalid entry data format"
            return response
        end
        
        -- Flatten the entry for XADD (convert from map to array of field/value pairs)
        local flattened_entry = {}
        for k, v in pairs(entry) do
            table.insert(flattened_entry, k)
            if type(v) == "table" then
                -- Convert nested tables to JSON
                local ok, json_val = pcall(cjson.encode, v)
                if ok then
                    table.insert(flattened_entry, json_val)
                else
                    table.insert(flattened_entry, tostring(v))
                end
            else
                table.insert(flattened_entry, tostring(v))
            end
        end
        
        -- Add entry to stream
        local id = server.call('XADD', stream, 'MAXLEN', '~', max_len, '*', unpack(flattened_entry))
        
        if id then
            response.map.success = true
            response.map.entry_id = { verbatim_string = { format = "txt", string = id } }
            response.map.metrics.produced = 1
            
            -- Track metrics
            local metrics_key = '{' .. site_id .. '}:metrics'
            server.call('HINCRBY', metrics_key, 'stream_produced', 1)
        end
        
        -- Add timing information
        response.map.duration = { double = server.call('TIME')[1] - start_time }
        
        return response
    end,
    description = 'Adds a message to a stream with backpressure handling and RESP3 response'
}

-- =============================================================================
-- SITE STREAM LIFECYCLE FUNCTIONS
-- =============================================================================
-- These functions support the refactored stream architecture where:
-- - Each site has its own streams: {site_id}:gnode:unified:{environment}
-- - Streams are created atomically when sites register
-- - Consumer groups are managed per-site, per-environment
-- =============================================================================

--- Create all DTAP streams for a site with proper consumer groups
-- This function atomically creates the required streams for a site across all environments.
-- Stream pattern: {site_id}:gnode:{stream_type}:{environment}
--
-- @param args[1] site_id - The site identifier (e.g., 'my_app')
-- @param args[2] environments_json - JSON array of environments (default: ["testing","staging","acceptance","production"])
-- @param args[3] topology_namespace - Namespace for shared streams (default: "geodineum")
-- @return JSON object with created streams and consumer groups
--
-- Stream Architecture:
--   Per site:
--     {site_id}:gnode:unified:{env}  - 4 unified streams (one per DTAP environment)
--     {site_id}:gnode:health         - 1 health stream (NO environment suffix)
--   Shared (all sites use):
--     {topology_namespace}:gnode:broadcast:global  - shared broadcast stream
--     {topology_namespace}:gnode:unified           - service registration stream
--     geodineum:unified:stream                   - future global network stream
--
server.register_function{
    function_name = 'GNODE_PROVISION_SERVICE',
    callback = function(keys, args)
        -- Input validation
        if not args[1] then
            return server.error_reply("Site ID required")
        end

        local site_id = args[1]
        local environments = {"testing", "staging", "acceptance", "production"}
        local topology_namespace = args[3] or "geodineum"

        -- Parse custom environments if provided
        if args[2] and args[2] ~= "" then
            local ok, parsed = pcall(cjson.decode, args[2])
            if ok and type(parsed) == 'table' then
                environments = parsed
            end
        end

        local start_time = server.call('TIME')[1]
        local created_streams = {}
        local created_groups = {}
        local errors = {}

        -- =============================================================================
        -- 1. Create unified streams for each DTAP environment (4 per site)
        -- =============================================================================
        for _, env in ipairs(environments) do
            -- Pattern: {site_id}:gnode:unified:{environment}
            -- The {} around site_id ensures all site keys hash to same slot in cluster
            local stream_key = '{' .. site_id .. '}:gnode:unified:' .. env

            -- Check if stream already exists
            local exists = server.call('EXISTS', stream_key) == 1

            if not exists then
                -- Create stream with initial placeholder message
                local ok, result = pcall(function()
                    return server.call('XADD', stream_key, '*',
                        '_init', 'true',
                        '_site_id', site_id,
                        '_environment', env,
                        '_stream_type', 'unified',
                        '_created_at', tostring(start_time)
                    )
                end)

                if ok then
                    table.insert(created_streams, stream_key)
                else
                    table.insert(errors, "Failed to create stream " .. stream_key .. ": " .. tostring(result))
                end
            end

            -- Create consumer groups for unified stream
            for _, group_name in ipairs({"gnode-daemon", "gnode-client"}) do
                local group_ok = server.pcall('XGROUP', 'CREATE', stream_key, group_name, '$', 'MKSTREAM')
                if group_ok.err then
                    if not group_ok.err:find('BUSYGROUP') then
                        table.insert(errors, "Failed to create group " .. group_name .. " on " .. stream_key .. ": " .. group_ok.err)
                    end
                else
                    table.insert(created_groups, stream_key .. ":" .. group_name)
                end
            end
        end

        -- =============================================================================
        -- 2. Create ONE health stream per site (NO environment suffix)
        -- =============================================================================
        local health_key = '{' .. site_id .. '}:gnode:health'
        local health_exists = server.call('EXISTS', health_key) == 1

        if not health_exists then
            local ok, result = pcall(function()
                return server.call('XADD', health_key, '*',
                    '_init', 'true',
                    '_site_id', site_id,
                    '_stream_type', 'health',
                    '_created_at', tostring(start_time)
                )
            end)

            if ok then
                table.insert(created_streams, health_key)
            else
                table.insert(errors, "Failed to create health stream: " .. tostring(result))
            end
        end

        -- Health stream only needs daemon group
        local health_group_ok = server.pcall('XGROUP', 'CREATE', health_key, 'gnode-daemon', '$', 'MKSTREAM')
        if health_group_ok.err and not health_group_ok.err:find('BUSYGROUP') then
            table.insert(errors, "Failed to create group gnode-daemon on " .. health_key .. ": " .. health_group_ok.err)
        elseif not health_group_ok.err then
            table.insert(created_groups, health_key .. ":gnode-daemon")
        end

        -- =============================================================================
        -- 3. Ensure shared broadcast stream exists (all sites use the same one)
        -- =============================================================================
        local broadcast_key = '{' .. topology_namespace .. '}:gnode:broadcast:global'
        local broadcast_exists = server.call('EXISTS', broadcast_key) == 1

        if not broadcast_exists then
            local ok, result = pcall(function()
                return server.call('XADD', broadcast_key, '*',
                    '_init', 'true',
                    '_topology_namespace', topology_namespace,
                    '_stream_type', 'broadcast',
                    '_created_at', tostring(start_time)
                )
            end)

            if ok then
                table.insert(created_streams, broadcast_key)
            else
                table.insert(errors, "Failed to create broadcast stream: " .. tostring(result))
            end
        end

        -- =============================================================================
        -- 4. Ensure topology namespace registration stream exists
        -- =============================================================================
        local registration_key = '{' .. topology_namespace .. '}:gnode:unified'
        local registration_exists = server.call('EXISTS', registration_key) == 1

        if not registration_exists then
            local ok, result = pcall(function()
                return server.call('XADD', registration_key, '*',
                    '_init', 'true',
                    '_topology_namespace', topology_namespace,
                    '_stream_type', 'registration',
                    '_purpose', 'service_registrations',
                    '_created_at', tostring(start_time)
                )
            end)

            if ok then
                table.insert(created_streams, registration_key)
            else
                table.insert(errors, "Failed to create registration stream: " .. tostring(result))
            end
        end

        -- Create consumer groups for registration stream
        for _, group_name in ipairs({"gnode-daemon", "gnode-client"}) do
            local group_ok = server.pcall('XGROUP', 'CREATE', registration_key, group_name, '$', 'MKSTREAM')
            if group_ok.err and not group_ok.err:find('BUSYGROUP') then
                table.insert(errors, "Failed to create group " .. group_name .. " on " .. registration_key .. ": " .. group_ok.err)
            elseif not group_ok.err then
                table.insert(created_groups, registration_key .. ":" .. group_name)
            end
        end

        -- =============================================================================
        -- 5. Ensure global geodineum network stream exists (future multi-topology)
        -- =============================================================================
        local global_key = 'geodineum:unified:stream'
        local global_exists = server.call('EXISTS', global_key) == 1

        if not global_exists then
            local ok, result = pcall(function()
                return server.call('XADD', global_key, '*',
                    '_init', 'true',
                    '_stream_type', 'global_network',
                    '_purpose', 'multi_topology_coordination',
                    '_created_at', tostring(start_time)
                )
            end)

            if ok then
                table.insert(created_streams, global_key)
            else
                table.insert(errors, "Failed to create global network stream: " .. tostring(result))
            end
        end

        -- Create consumer groups for global stream
        for _, group_name in ipairs({"gnode-daemon", "gnode-client"}) do
            local group_ok = server.pcall('XGROUP', 'CREATE', global_key, group_name, '$', 'MKSTREAM')
            if group_ok.err and not group_ok.err:find('BUSYGROUP') then
                table.insert(errors, "Failed to create group " .. group_name .. " on " .. global_key .. ": " .. group_ok.err)
            elseif not group_ok.err then
                table.insert(created_groups, global_key .. ":" .. group_name)
            end
        end

        -- =============================================================================
        -- 6. Register site in the global site registry
        -- =============================================================================
        local registry_key = 'gnode:sites:registry'
        server.call('SADD', registry_key, site_id)

        -- Store site metadata
        local site_meta_key = 'gnode:site:' .. site_id .. ':meta'
        server.call('HSET', site_meta_key,
            'created_at', start_time,
            'environments', json_encode(environments),
            'topology_namespace', topology_namespace,
            'status', 'active'
        )

        -- Optional: set tenant/owner group (args[4])
        local owner = args[4]
        if owner and owner ~= '' then
            server.call('HSET', site_meta_key, 'owner', owner)
            server.call('SADD', 'gnode:tenant:' .. owner .. ':sites', site_id)
        end

        -- Build response
        local response = {
            success = #errors == 0,
            site_id = site_id,
            topology_namespace = topology_namespace,
            created_streams = created_streams,
            created_groups = created_groups,
            stream_counts = {
                unified = #environments,  -- 4 per site (one per DTAP env)
                health = 1,               -- 1 per site
                broadcast = 1,            -- 1 shared
                registration = 1,         -- 1 shared
                global = 1                -- 1 shared
            },
            errors = errors,
            duration_ms = (server.call('TIME')[1] - start_time) * 1000
        }

        local ok, result = pcall(cjson.encode, response)
        if not ok then
            return server.error_reply("Failed to encode response: " .. (result or "unknown error"))
        end
        return result
    end,
    description = 'Creates DTAP unified streams, single health stream, and ensures shared broadcast/registration streams exist'
}

-- =============================================================================
-- SERVICE DEPROVISIONING
-- =============================================================================
-- Complete cleanup when a service is removed from the system.
-- Removes all service-specific keys while preserving shared infrastructure.
--
-- Keys removed per service:
--   {service_id}:gnode:unified:{env}    - unified streams (all DTAP envs)
--   {service_id}:gnode:health           - health stream
--   {service_id}:gnode:broadcast        - per-service broadcast (if exists)
--   gnode:site:{service_id}:meta        - metadata
--   {service_id}:*                    - any other service-namespaced keys
--
-- Keys NOT removed (shared infrastructure):
--   {topology_namespace}:gnode:broadcast:global
--   {topology_namespace}:gnode:unified
--   geodineum:unified:stream
--   gnode:sites:registry (only the entry is removed, not the set itself)
--
-- @param args[1] service_id - The service identifier to deprovision
-- @param args[2] options_json - Optional: {"dry_run": true, "include_cache": true}
-- @return JSON with cleanup results
-- =============================================================================
server.register_function{
    function_name = 'GNODE_DEPROVISION_SERVICE',
    callback = function(keys, args)
        -- Input validation
        if not args[1] then
            return server.error_reply("Service ID required")
        end

        local service_id = args[1]
        local options = {}

        -- Parse options if provided
        if args[2] and args[2] ~= "" then
            local ok, parsed = pcall(cjson.decode, args[2])
            if ok and type(parsed) == 'table' then
                options = parsed
            end
        end

        local dry_run = options.dry_run == true
        local include_cache = options.include_cache ~= false  -- default true
        local include_all_namespaced = options.include_all_namespaced ~= false  -- default true

        local start_time = server.call('TIME')[1]
        local deleted_keys = {}
        local skipped_keys = {}
        local errors = {}

        -- Helper function to delete a key (respects dry_run)
        local function delete_key(key, reason)
            if dry_run then
                table.insert(deleted_keys, {key = key, reason = reason, dry_run = true})
                return true
            else
                local ok, result = pcall(function()
                    return server.call('DEL', key)
                end)
                if ok then
                    if result > 0 then
                        table.insert(deleted_keys, {key = key, reason = reason})
                        return true
                    else
                        table.insert(skipped_keys, {key = key, reason = "did not exist"})
                        return false
                    end
                else
                    table.insert(errors, "Failed to delete " .. key .. ": " .. tostring(result))
                    return false
                end
            end
        end

        -- =============================================================================
        -- 1. Remove from site registry
        -- =============================================================================
        local registry_key = 'gnode:sites:registry'
        if dry_run then
            table.insert(deleted_keys, {key = registry_key .. " (SREM " .. service_id .. ")", reason = "registry entry"})
        else
            local removed = server.call('SREM', registry_key, service_id)
            if removed > 0 then
                table.insert(deleted_keys, {key = registry_key .. " (SREM " .. service_id .. ")", reason = "registry entry"})
            else
                table.insert(skipped_keys, {key = registry_key, reason = "service not in registry"})
            end
        end

        -- =============================================================================
        -- 1b. Clean up tenant group index (before metadata is deleted)
        -- =============================================================================
        local meta_key = 'gnode:site:' .. service_id .. ':meta'
        local owner = server.call('HGET', meta_key, 'owner')
        if owner and owner ~= '' then
            local tenant_key = 'gnode:tenant:' .. owner .. ':sites'
            if dry_run then
                table.insert(deleted_keys, {key = tenant_key .. " (SREM " .. service_id .. ")", reason = "tenant group"})
            else
                server.call('SREM', tenant_key, service_id)
                -- Clean up empty tenant group
                if server.call('SCARD', tenant_key) == 0 then
                    server.call('DEL', tenant_key)
                end
            end
        end

        -- =============================================================================
        -- 2. Delete site metadata
        -- =============================================================================
        delete_key(meta_key, "service metadata")

        -- =============================================================================
        -- 3. Delete unified streams (all DTAP environments)
        -- =============================================================================
        local environments = {"testing", "staging", "acceptance", "production"}
        for _, env in ipairs(environments) do
            local stream_key = '{' .. service_id .. '}:gnode:unified:' .. env
            delete_key(stream_key, "unified stream (" .. env .. ")")
        end

        -- =============================================================================
        -- 4. Delete health stream
        -- =============================================================================
        local health_key = '{' .. service_id .. '}:gnode:health'
        delete_key(health_key, "health stream")

        -- =============================================================================
        -- 5. Delete per-service broadcast stream (if exists)
        -- =============================================================================
        local broadcast_key = '{' .. service_id .. '}:gnode:broadcast'
        delete_key(broadcast_key, "service broadcast stream")

        -- =============================================================================
        -- 6. Delete cache keys (if include_cache is true)
        -- =============================================================================
        if include_cache then
            -- Cache keys use {site_id}:cache:* or cache:{site_id}:* patterns
            local cache_patterns = {
                '{' .. service_id .. '}:cache:*',
                'cache:{' .. service_id .. '}:*',
                '{' .. service_id .. '}:ratelimit:*',
                '{' .. service_id .. '}:circuit:*',
                '{' .. service_id .. '}:metrics'
            }

            for _, pattern in ipairs(cache_patterns) do
                -- Use SCAN to find matching keys
                local cursor = "0"
                repeat
                    local result = server.call('SCAN', cursor, 'MATCH', pattern, 'COUNT', 100)
                    cursor = result[1]
                    local batch = result[2]

                    for _, key in ipairs(batch) do
                        delete_key(key, "cache/rate-limit key")
                    end
                until cursor == "0"
            end
        end

        -- =============================================================================
        -- 7. Delete ALL service-namespaced keys (if include_all_namespaced is true)
        -- =============================================================================
        if include_all_namespaced then
            -- Find all keys with the service's hash tag prefix
            local pattern = '{' .. service_id .. '}:*'
            local cursor = "0"
            repeat
                local result = server.call('SCAN', cursor, 'MATCH', pattern, 'COUNT', 100)
                cursor = result[1]
                local batch = result[2]

                for _, key in ipairs(batch) do
                    delete_key(key, "namespaced key")
                end
            until cursor == "0"

            -- Also check for gnode:site:{service_id}:* pattern
            pattern = 'gnode:site:' .. service_id .. ':*'
            cursor = "0"
            repeat
                local result = server.call('SCAN', cursor, 'MATCH', pattern, 'COUNT', 100)
                cursor = result[1]
                local batch = result[2]

                for _, key in ipairs(batch) do
                    delete_key(key, "site metadata key")
                end
            until cursor == "0"
        end

        -- =============================================================================
        -- Build response
        -- =============================================================================
        local response = {
            success = #errors == 0,
            service_id = service_id,
            dry_run = dry_run,
            deleted_count = #deleted_keys,
            skipped_count = #skipped_keys,
            deleted_keys = deleted_keys,
            skipped_keys = skipped_keys,
            errors = errors,
            duration_ms = (server.call('TIME')[1] - start_time) * 1000
        }

        local ok, result = pcall(cjson.encode, response)
        if not ok then
            return server.error_reply("Failed to encode response: " .. (result or "unknown error"))
        end
        return result
    end,
    description = 'Completely removes a service and all its keys from the system. Use dry_run:true to preview.'
}

-- =============================================================================
-- SERVICE UPDATE
-- =============================================================================
-- Update service metadata and status without reprovisioning.
--
-- Updateable fields:
--   status: active|inactive|maintenance
--   active_environment: testing|staging|acceptance|production
--   topology_namespace: string
--   custom metadata: any additional fields
--
-- @param args[1] service_id - The service identifier
-- @param args[2] updates_json - JSON object with fields to update
-- @return JSON with update results
-- =============================================================================
server.register_function{
    function_name = 'GNODE_UPDATE_SERVICE',
    callback = function(keys, args)
        if not args[1] then
            return server.error_reply("Service ID required")
        end
        if not args[2] then
            return server.error_reply("Updates JSON required")
        end

        local service_id = args[1]
        local updates_str = args[2]

        -- Parse updates
        local ok, updates = pcall(cjson.decode, updates_str)
        if not ok or type(updates) ~= 'table' then
            return server.error_reply("Invalid updates JSON: " .. tostring(updates))
        end

        -- Check if service exists
        local registry_key = 'gnode:sites:registry'
        local exists = server.call('SISMEMBER', registry_key, service_id) == 1
        if not exists then
            return server.error_reply("Service not found: " .. service_id)
        end

        local meta_key = 'gnode:site:' .. service_id .. ':meta'
        local timestamp = server.call('TIME')[1]
        local updated_fields = {}
        local errors = {}

        -- Validate and apply updates
        local valid_statuses = {active = true, inactive = true, maintenance = true}
        local valid_envs = {testing = true, staging = true, acceptance = true, production = true}

        for field, value in pairs(updates) do
            if field == 'status' then
                if valid_statuses[value] then
                    server.call('HSET', meta_key, 'status', value)
                    server.call('HSET', meta_key, 'status_updated_at', timestamp)
                    table.insert(updated_fields, {field = 'status', value = value})

                    -- If setting to maintenance, broadcast notification
                    if value == 'maintenance' then
                        local broadcast_key = '{' .. service_id .. '}:gnode:broadcast'
                        local event_ok, event_json = pcall(cjson.encode, {
                            type = "service_status_changed",
                            service_id = service_id,
                            status = value,
                            timestamp = timestamp
                        })
                        if event_ok then
                            server.call('XADD', broadcast_key, 'MAXLEN', '~', 1000, '*',
                                'type', 'service_status_changed',
                                'data', event_json)
                        end
                    end
                else
                    table.insert(errors, "Invalid status: " .. tostring(value) .. ". Must be: active, inactive, or maintenance")
                end

            elseif field == 'active_environment' then
                if valid_envs[value] then
                    local old_env = server.call('HGET', meta_key, 'active_environment') or 'production'
                    server.call('HSET', meta_key, 'active_environment', value)
                    server.call('HSET', meta_key, 'environment_updated_at', timestamp)
                    table.insert(updated_fields, {field = 'active_environment', value = value, old_value = old_env})

                    -- Broadcast environment change for daemon refresh
                    if old_env ~= value then
                        local broadcast_key = '{' .. service_id .. '}:gnode:broadcast'
                        local event_ok, event_json = pcall(cjson.encode, {
                            type = "environment_changed",
                            service_id = service_id,
                            old_environment = old_env,
                            new_environment = value,
                            timestamp = timestamp
                        })
                        if event_ok then
                            server.call('XADD', broadcast_key, 'MAXLEN', '~', 1000, '*',
                                'type', 'environment_changed',
                                'data', event_json)
                        end
                    end
                else
                    table.insert(errors, "Invalid environment: " .. tostring(value) .. ". Must be: testing, staging, acceptance, or production")
                end

            elseif field == 'topology_namespace' then
                server.call('HSET', meta_key, 'topology_namespace', value)
                table.insert(updated_fields, {field = 'topology_namespace', value = value})

            elseif field == 'owner' then
                -- Owner field: also maintain tenant group index
                local old_owner = server.call('HGET', meta_key, 'owner')
                if old_owner and old_owner ~= '' and old_owner ~= value then
                    server.call('SREM', 'gnode:tenant:' .. old_owner .. ':sites', service_id)
                    -- Clean up empty tenant group
                    if server.call('SCARD', 'gnode:tenant:' .. old_owner .. ':sites') == 0 then
                        server.call('DEL', 'gnode:tenant:' .. old_owner .. ':sites')
                    end
                end
                server.call('HSET', meta_key, 'owner', value)
                if value ~= '' then
                    server.call('SADD', 'gnode:tenant:' .. value .. ':sites', service_id)
                end
                table.insert(updated_fields, {field = 'owner', value = value})

            elseif field == 'description' or field == 'display_name' or field == 'tags' then
                -- Allow common metadata fields
                local store_value = value
                local should_store = true
                if type(value) == 'table' then
                    local encode_ok, encoded = pcall(cjson.encode, value)
                    if encode_ok then
                        store_value = encoded
                    else
                        table.insert(errors, "Failed to encode " .. field)
                        should_store = false
                    end
                end
                if should_store then
                    server.call('HSET', meta_key, field, store_value)
                    table.insert(updated_fields, {field = field, value = value})
                end

            else
                -- Store custom fields with 'custom_' prefix
                local store_value = value
                local should_store = true
                if type(value) == 'table' then
                    local encode_ok, encoded = pcall(cjson.encode, value)
                    if encode_ok then
                        store_value = encoded
                    else
                        table.insert(errors, "Failed to encode custom field: " .. field)
                        should_store = false
                    end
                end
                if should_store then
                    server.call('HSET', meta_key, 'custom_' .. field, store_value)
                    table.insert(updated_fields, {field = 'custom_' .. field, value = value})
                end
            end
        end

        -- Update timestamp
        server.call('HSET', meta_key, 'updated_at', timestamp)

        local response = {
            success = #errors == 0,
            service_id = service_id,
            updated_fields = updated_fields,
            errors = errors,
            timestamp = timestamp
        }

        local encode_ok, result = pcall(cjson.encode, response)
        if not encode_ok then
            return server.error_reply("Failed to encode response: " .. tostring(result))
        end
        return result
    end,
    description = 'Updates service metadata and status'
}

-- =============================================================================
-- SERVICE ADD ENVIRONMENT
-- =============================================================================
-- Add a new DTAP environment to an existing service.
-- Creates the unified stream for that environment.
--
-- @param args[1] service_id - The service identifier
-- @param args[2] environment - The DTAP environment to add (testing/staging/acceptance/production)
-- @param args[3] set_active - Optional: "true" to also set as active environment
-- @return JSON with creation results
-- =============================================================================
server.register_function{
    function_name = 'GNODE_SERVICE_ADD_ENVIRONMENT',
    callback = function(keys, args)
        if not args[1] then
            return server.error_reply("Service ID required")
        end
        if not args[2] then
            return server.error_reply("Environment required (testing/staging/acceptance/production)")
        end

        local service_id = args[1]
        local environment = args[2]
        local set_active = args[3] == "true"

        -- Validate environment
        local valid_envs = {testing = true, staging = true, acceptance = true, production = true}
        if not valid_envs[environment] then
            return server.error_reply("Invalid environment: " .. environment .. ". Must be: testing, staging, acceptance, or production")
        end

        -- Check if service exists
        local registry_key = 'gnode:sites:registry'
        local exists = server.call('SISMEMBER', registry_key, service_id) == 1
        if not exists then
            return server.error_reply("Service not found: " .. service_id .. ". Use GNODE_PROVISION_SERVICE to create it first.")
        end

        local start_time = server.call('TIME')[1]
        local created_streams = {}
        local created_groups = {}
        local errors = {}

        -- Create unified stream for this environment
        local stream_key = '{' .. service_id .. '}:gnode:unified:' .. environment
        local stream_exists = server.call('EXISTS', stream_key) == 1

        if stream_exists then
            return server.error_reply("Environment already exists: " .. environment .. " (stream: " .. stream_key .. ")")
        end

        -- Create stream with initial placeholder message
        local ok, result = pcall(function()
            return server.call('XADD', stream_key, '*',
                '_init', 'true',
                '_site_id', service_id,
                '_environment', environment,
                '_stream_type', 'unified',
                '_created_at', tostring(start_time)
            )
        end)

        if ok then
            table.insert(created_streams, stream_key)
        else
            table.insert(errors, "Failed to create stream: " .. tostring(result))
        end

        -- Create consumer groups
        for _, group_name in ipairs({"gnode-daemon", "gnode-client"}) do
            local group_ok = server.pcall('XGROUP', 'CREATE', stream_key, group_name, '$', 'MKSTREAM')
            if group_ok.err then
                if not group_ok.err:find('BUSYGROUP') then
                    table.insert(errors, "Failed to create group " .. group_name .. ": " .. group_ok.err)
                end
            else
                table.insert(created_groups, stream_key .. ":" .. group_name)
            end
        end

        -- Update metadata with new environment list
        local meta_key = 'gnode:site:' .. service_id .. ':meta'
        local envs_str = server.call('HGET', meta_key, 'environments')
        local envs = {"production"}  -- default
        if envs_str then
            local decode_ok, decoded = pcall(cjson.decode, envs_str)
            if decode_ok and type(decoded) == 'table' then
                envs = decoded
            end
        end

        -- Add new environment if not already present
        local env_exists = false
        for _, e in ipairs(envs) do
            if e == environment then
                env_exists = true
                break
            end
        end
        if not env_exists then
            table.insert(envs, environment)
            local encode_ok, envs_json = pcall(cjson.encode, envs)
            if encode_ok then
                server.call('HSET', meta_key, 'environments', envs_json)
            end
        end

        -- Optionally set as active environment
        if set_active then
            server.call('HSET', meta_key, 'active_environment', environment)
            server.call('HSET', meta_key, 'environment_updated_at', start_time)
        end

        server.call('HSET', meta_key, 'updated_at', start_time)

        -- Broadcast for daemon to pick up new stream
        local broadcast_key = '{' .. service_id .. '}:gnode:broadcast'
        local event_ok, event_json = pcall(cjson.encode, {
            type = "environment_added",
            service_id = service_id,
            environment = environment,
            stream_key = stream_key,
            set_active = set_active,
            timestamp = start_time
        })
        if event_ok then
            server.call('XADD', broadcast_key, 'MAXLEN', '~', 1000, '*',
                'type', 'environment_added',
                'data', event_json)
        end

        local response = {
            success = #errors == 0,
            service_id = service_id,
            environment = environment,
            stream_key = stream_key,
            created_streams = created_streams,
            created_groups = created_groups,
            set_active = set_active,
            all_environments = envs,
            errors = errors,
            duration_ms = (server.call('TIME')[1] - start_time) * 1000
        }

        local encode_ok, result = pcall(cjson.encode, response)
        if not encode_ok then
            return server.error_reply("Failed to encode response: " .. tostring(result))
        end
        return result
    end,
    description = 'Adds a new DTAP environment to an existing service'
}

-- =============================================================================
-- SERVICE REMOVE ENVIRONMENT
-- =============================================================================
-- Remove a DTAP environment from a service.
-- Deletes only that environment's unified stream, preserving other environments.
--
-- @param args[1] service_id - The service identifier
-- @param args[2] environment - The DTAP environment to remove
-- @param args[3] options_json - Optional: {"force": true} to remove even if it's the active environment
-- @return JSON with removal results
-- =============================================================================
server.register_function{
    function_name = 'GNODE_SERVICE_REMOVE_ENVIRONMENT',
    callback = function(keys, args)
        if not args[1] then
            return server.error_reply("Service ID required")
        end
        if not args[2] then
            return server.error_reply("Environment required")
        end

        local service_id = args[1]
        local environment = args[2]
        local options = {}

        if args[3] and args[3] ~= "" then
            local ok, parsed = pcall(cjson.decode, args[3])
            if ok and type(parsed) == 'table' then
                options = parsed
            end
        end

        local force = options.force == true

        -- Validate environment
        local valid_envs = {testing = true, staging = true, acceptance = true, production = true}
        if not valid_envs[environment] then
            return server.error_reply("Invalid environment: " .. environment)
        end

        -- Check if service exists
        local registry_key = 'gnode:sites:registry'
        local exists = server.call('SISMEMBER', registry_key, service_id) == 1
        if not exists then
            return server.error_reply("Service not found: " .. service_id)
        end

        local meta_key = 'gnode:site:' .. service_id .. ':meta'
        local start_time = server.call('TIME')[1]

        -- Check if this is the active environment
        local active_env = server.call('HGET', meta_key, 'active_environment') or 'production'
        if active_env == environment and not force then
            return server.error_reply("Cannot remove active environment '" .. environment .. "'. Set a different active environment first, or use force:true")
        end

        local deleted_keys = {}
        local errors = {}

        -- Delete the unified stream for this environment
        local stream_key = '{' .. service_id .. '}:gnode:unified:' .. environment
        local deleted = server.call('DEL', stream_key)
        if deleted > 0 then
            table.insert(deleted_keys, stream_key)
        end

        -- Update metadata to remove this environment from the list
        local envs_str = server.call('HGET', meta_key, 'environments')
        if envs_str then
            local decode_ok, envs = pcall(cjson.decode, envs_str)
            if decode_ok and type(envs) == 'table' then
                local new_envs = {}
                for _, e in ipairs(envs) do
                    if e ~= environment then
                        table.insert(new_envs, e)
                    end
                end
                local encode_ok, new_envs_json = pcall(cjson.encode, new_envs)
                if encode_ok then
                    server.call('HSET', meta_key, 'environments', new_envs_json)
                end

                -- If we removed the active environment with force, switch to another
                if active_env == environment and #new_envs > 0 then
                    -- Prefer production, then staging, etc.
                    local new_active = new_envs[1]
                    for _, preferred in ipairs({"production", "staging", "acceptance", "testing"}) do
                        for _, e in ipairs(new_envs) do
                            if e == preferred then
                                new_active = preferred
                                break
                            end
                        end
                        if new_active ~= new_envs[1] then break end
                    end
                    server.call('HSET', meta_key, 'active_environment', new_active)
                    server.call('HSET', meta_key, 'environment_updated_at', start_time)
                end
            end
        end

        server.call('HSET', meta_key, 'updated_at', start_time)

        -- Broadcast for daemon to stop listening to removed stream
        local broadcast_key = '{' .. service_id .. '}:gnode:broadcast'
        local event_ok, event_json = pcall(cjson.encode, {
            type = "environment_removed",
            service_id = service_id,
            environment = environment,
            stream_key = stream_key,
            timestamp = start_time
        })
        if event_ok then
            server.call('XADD', broadcast_key, 'MAXLEN', '~', 1000, '*',
                'type', 'environment_removed',
                'data', event_json)
        end

        local response = {
            success = #errors == 0,
            service_id = service_id,
            environment = environment,
            deleted_keys = deleted_keys,
            errors = errors,
            duration_ms = (server.call('TIME')[1] - start_time) * 1000
        }

        local encode_ok, result = pcall(cjson.encode, response)
        if not encode_ok then
            return server.error_reply("Failed to encode response: " .. tostring(result))
        end
        return result
    end,
    description = 'Removes a DTAP environment from a service (deletes that environment stream only)'
}

-- =============================================================================
-- SERVICE GET (Read operation for CRUD completeness)
-- =============================================================================
-- Get complete service information including metadata, environments, and stream status.
--
-- @param args[1] service_id - The service identifier
-- @param args[2] options_json - Optional: {"include_stream_info": true}
-- @return JSON with complete service details
-- =============================================================================
server.register_function{
    function_name = 'GNODE_SERVICE_GET',
    callback = function(keys, args)
        if not args[1] then
            return server.error_reply("Service ID required")
        end

        local service_id = args[1]
        local options = {}

        if args[2] and args[2] ~= "" then
            local ok, parsed = pcall(cjson.decode, args[2])
            if ok and type(parsed) == 'table' then
                options = parsed
            end
        end

        local include_stream_info = options.include_stream_info == true

        -- Check if service exists
        local registry_key = 'gnode:sites:registry'
        local exists = server.call('SISMEMBER', registry_key, service_id) == 1
        if not exists then
            return server.error_reply("Service not found: " .. service_id)
        end

        local meta_key = 'gnode:site:' .. service_id .. ':meta'

        -- Get all metadata
        local meta_fields = server.call('HGETALL', meta_key)
        local metadata = {}
        for i = 1, #meta_fields, 2 do
            local key = meta_fields[i]
            local value = meta_fields[i + 1]

            -- Try to decode JSON values
            if key == 'environments' or key:match('^custom_') or key == 'tags' then
                local decode_ok, decoded = pcall(cjson.decode, value)
                if decode_ok then
                    metadata[key] = decoded
                else
                    metadata[key] = value
                end
            else
                -- Try numeric conversion
                local num = tonumber(value)
                metadata[key] = num or value
            end
        end

        -- Build response
        local response = {
            service_id = service_id,
            status = metadata.status or "active",
            active_environment = metadata.active_environment or "production",
            environments = metadata.environments or {"production"},
            topology_namespace = metadata.topology_namespace or "geodineum",
            created_at = metadata.created_at,
            updated_at = metadata.updated_at,
            metadata = metadata
        }

        -- Optionally include stream information
        if include_stream_info then
            response.streams = {}
            local envs = metadata.environments or {"production"}
            if type(envs) == "string" then
                local decode_ok, decoded = pcall(cjson.decode, envs)
                if decode_ok then envs = decoded else envs = {"production"} end
            end

            for _, env in ipairs(envs) do
                local stream_key = '{' .. service_id .. '}:gnode:unified:' .. env
                local stream_exists = server.call('EXISTS', stream_key) == 1
                local stream_len = 0
                if stream_exists then
                    stream_len = server.call('XLEN', stream_key)
                end
                response.streams[env] = {
                    key = stream_key,
                    exists = stream_exists,
                    length = stream_len
                }
            end

            -- Health stream
            local health_key = '{' .. service_id .. '}:gnode:health'
            local health_exists = server.call('EXISTS', health_key) == 1
            response.streams.health = {
                key = health_key,
                exists = health_exists,
                length = health_exists and server.call('XLEN', health_key) or 0
            }
        end

        local encode_ok, result = pcall(cjson.encode, response)
        if not encode_ok then
            return server.error_reply("Failed to encode response: " .. tostring(result))
        end
        return result
    end,
    flags = {'no-writes'},
    description = 'Gets complete service information'
}

-- =============================================================================
-- SERVICE LIST (List operation for admin)
-- =============================================================================
-- List all services with optional filtering.
--
-- @param args[1] options_json - Optional: {"status": "active", "environment": "production"}
-- @return JSON with list of services
-- =============================================================================
server.register_function{
    function_name = 'GNODE_SERVICE_LIST',
    callback = function(keys, args)
        local options = {}

        if args[1] and args[1] ~= "" then
            local ok, parsed = pcall(cjson.decode, args[1])
            if ok and type(parsed) == 'table' then
                options = parsed
            end
        end

        local filter_status = options.status
        local filter_env = options.environment
        local include_meta = options.include_meta ~= false  -- default true

        -- Get all registered services
        local registry_key = 'gnode:sites:registry'
        local service_ids = server.call('SMEMBERS', registry_key)

        local services = {}

        for _, service_id in ipairs(service_ids) do
            local meta_key = 'gnode:site:' .. service_id .. ':meta'
            local status = server.call('HGET', meta_key, 'status') or 'active'
            local active_env = server.call('HGET', meta_key, 'active_environment') or 'production'

            -- Apply filters
            local include = true
            if filter_status and status ~= filter_status then
                include = false
            end
            if filter_env and active_env ~= filter_env then
                include = false
            end

            if include then
                local service_info = {
                    id = service_id,
                    status = status,
                    active_environment = active_env
                }

                if include_meta then
                    local created_at = server.call('HGET', meta_key, 'created_at')
                    local envs_str = server.call('HGET', meta_key, 'environments')
                    local envs = {"production"}
                    if envs_str then
                        local decode_ok, decoded = pcall(cjson.decode, envs_str)
                        if decode_ok then envs = decoded end
                    end

                    service_info.created_at = tonumber(created_at)
                    service_info.environments = envs
                    service_info.environment_count = #envs
                end

                table.insert(services, service_info)
            end
        end

        -- Sort by ID for consistent ordering
        table.sort(services, function(a, b) return a.id < b.id end)

        local response = {
            services = services,
            count = #services,
            total_registered = #service_ids
        }

        if filter_status or filter_env then
            response.filters = {
                status = filter_status,
                environment = filter_env
            }
        end

        local encode_ok, result = pcall(cjson.encode, response)
        if not encode_ok then
            return server.error_reply("Failed to encode response: " .. tostring(result))
        end
        return result
    end,
    flags = {'no-writes'},
    description = 'Lists all services with optional filtering'
}

--- Ensure consumer groups exist on a stream, creating them if needed
-- This is idempotent - safe to call multiple times
--
-- @param keys[1] stream_key - The stream key to ensure groups on
-- @param args[1] groups_json - JSON array of group names to ensure exist
-- @param args[2] start_id - Starting ID for new groups (default: "$" for new messages only)
-- @return JSON object with results
server.register_function{
    function_name = 'GNODE_STREAM_ENSURE_CONSUMER_GROUPS',
    callback = function(keys, args)
        -- Input validation
        if #keys < 1 then
            return server.error_reply("Stream key required")
        end
        if not args[1] then
            return server.error_reply("Groups JSON required")
        end

        local stream_key = keys[1]
        local start_id = args[2] or '$'

        -- Parse groups
        local ok, groups = pcall(cjson.decode, args[1])
        if not ok or type(groups) ~= 'table' then
            return server.error_reply("Invalid groups JSON")
        end

        local results = {
            stream = stream_key,
            ensured = {},
            already_existed = {},
            errors = {}
        }

        -- Ensure stream exists first
        local stream_exists = server.call('EXISTS', stream_key) == 1

        for _, group_name in ipairs(groups) do
            local create_result = server.pcall('XGROUP', 'CREATE', stream_key, group_name, start_id, 'MKSTREAM')

            if create_result.err then
                if create_result.err:find('BUSYGROUP') then
                    table.insert(results.already_existed, group_name)
                else
                    table.insert(results.errors, {group = group_name, error = create_result.err})
                end
            else
                table.insert(results.ensured, group_name)
            end
        end

        results.success = #results.errors == 0
        results.stream_created = not stream_exists

        return json_encode(results)
    end,
    description = 'Ensures consumer groups exist on a stream, creating them if needed (idempotent)'
}

--- Get stream keys for a site across all environments
-- Returns the stream keys that should exist for a given site
--
-- @param args[1] site_id - The site identifier
-- @param args[2] environment - Optional: filter to specific environment
-- @param args[3] stream_type - Optional: filter to specific stream type (unified/health/broadcast)
-- @return JSON object with stream keys and their existence status
server.register_function{
    function_name = 'GNODE_STREAM_GET_SITE_STREAMS',
    callback = function(keys, args)
        -- Input validation
        if not args[1] then
            return server.error_reply("Site ID required")
        end

        local site_id = args[1]
        local filter_env = args[2]
        local filter_type = args[3]

        local environments = {"testing", "staging", "acceptance", "production"}
        local stream_types = {"unified", "health"}

        -- Apply environment filter if specified
        if filter_env and filter_env ~= "" then
            environments = {filter_env}
        end

        -- Apply stream type filter if specified
        if filter_type and filter_type ~= "" then
            if filter_type == "broadcast" then
                -- Special case for broadcast (not per-environment)
                local broadcast_key = site_id .. ':gnode:broadcast'
                local exists = server.call('EXISTS', broadcast_key) == 1
                local info = nil

                if exists then
                    local ok, stream_info = pcall(function()
                        return server.call('XINFO', 'STREAM', broadcast_key)
                    end)
                    if ok then
                        info = {length = stream_info.length or 0}
                    end
                end

                return json_encode({
                    site_id = site_id,
                    streams = {{
                        key = broadcast_key,
                        exists = exists,
                        type = "broadcast",
                        environment = "global",
                        info = info
                    }}
                })
            else
                stream_types = {filter_type}
            end
        end

        local streams = {}

        -- Check each stream combination
        for _, env in ipairs(environments) do
            for _, stream_type in ipairs(stream_types) do
                local stream_key = site_id .. ':gnode:' .. stream_type .. ':' .. env
                local exists = server.call('EXISTS', stream_key) == 1

                local stream_info = {
                    key = stream_key,
                    exists = exists,
                    type = stream_type,
                    environment = env
                }

                -- Get additional info if stream exists
                if exists then
                    local ok, info = pcall(function()
                        return server.call('XINFO', 'STREAM', stream_key)
                    end)

                    if ok and info then
                        -- Parse XINFO response (array of key-value pairs)
                        local parsed_info = {}
                        for i = 1, #info, 2 do
                            parsed_info[info[i]] = info[i+1]
                        end
                        stream_info.length = parsed_info.length or 0
                        stream_info.first_entry_id = parsed_info['first-entry'] and parsed_info['first-entry'][1] or nil
                        stream_info.last_entry_id = parsed_info['last-entry'] and parsed_info['last-entry'][1] or nil
                    end

                    -- Get consumer group info
                    local groups_ok, groups = pcall(function()
                        return server.call('XINFO', 'GROUPS', stream_key)
                    end)

                    if groups_ok and groups then
                        stream_info.groups = {}
                        for _, group in ipairs(groups) do
                            -- Parse group info (array of key-value pairs)
                            local group_info = {}
                            for i = 1, #group, 2 do
                                group_info[group[i]] = group[i+1]
                            end
                            table.insert(stream_info.groups, {
                                name = group_info.name,
                                consumers = group_info.consumers or 0,
                                pending = group_info.pending or 0,
                                last_delivered_id = group_info['last-delivered-id']
                            })
                        end
                    end
                end

                table.insert(streams, stream_info)
            end
        end

        -- Also include broadcast stream if no type filter
        if not filter_type or filter_type == "" then
            local broadcast_key = site_id .. ':gnode:broadcast'
            local exists = server.call('EXISTS', broadcast_key) == 1

            local stream_info = {
                key = broadcast_key,
                exists = exists,
                type = "broadcast",
                environment = "global"
            }

            if exists then
                local ok, info = pcall(function()
                    return server.call('XINFO', 'STREAM', broadcast_key)
                end)

                if ok and info then
                    local parsed_info = {}
                    for i = 1, #info, 2 do
                        parsed_info[info[i]] = info[i+1]
                    end
                    stream_info.length = parsed_info.length or 0
                end
            end

            table.insert(streams, stream_info)
        end

        return json_encode({
            site_id = site_id,
            streams = streams
        })
    end,
    flags = {'no-writes'},
    description = 'Gets stream keys and status for a site across all environments'
}