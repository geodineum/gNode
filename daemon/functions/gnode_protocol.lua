#!lua name=gnode_protocol

--
-- gNode PROTOCOL Functions
-- A ValKey function library for protocol conversion between JSON and RESP3
--
-- These functions support the unified stream approach for gNode
-- with optimized RESP3 representation for memory efficiency
-- 

-- ------------------------------------------------------------------------
-- JSON utility functions
-- ------------------------------------------------------------------------

local function json_encode(obj)
    local ok, result = pcall(cjson.encode, obj)
    if not ok then
        return "null"
    end
    return result
end

local function json_decode(str)
    if not str or str == "" then
        return nil, "Empty JSON string"
    end
    
    -- Safely trim whitespace
    local trimmed = string.match(str, "^%s*(.-)%s*$") or str
    
    -- Handle empty objects and arrays
    if trimmed == "{}" then
        return {}, nil
    elseif trimmed == "[]" then
        return {}, nil
    end
    
    -- Try to parse with cjson
    local ok, result = pcall(function()
        return cjson.decode(trimmed)
    end)
    
    if not ok then
        -- Log debug info about the parsing error
        local error_msg = "JSON decode error: " .. tostring(result)
        local sample = string.sub(trimmed, 1, 100) .. (string.len(trimmed) > 100 and "..." or "")
        return nil, error_msg .. " (sample: " .. sample .. ")"
    end
    
    return result
end

-- ------------------------------------------------------------------------
-- Command name shortening functions
-- ------------------------------------------------------------------------

local function shorten_command(command)
    if command == "geometric_discover" then
        return "geo_disc"
    elseif command == "geometric_store_topology" then
        return "geo_store"
    elseif command == "geometric_load_sequence" then
        return "geo_seq"
    elseif command == "geometric_distance" then
        return "geo_dist"
    elseif command == "geometric_dimensions" then
        return "geo_dim"
    elseif command == "stream_info" then
        return "str_info"
    elseif command == "stream_group_info" then
        return "str_group"
    elseif command == "stream_consumer_info" then
        return "str_cons"
    elseif command == "stream_pending" then
        return "str_pend"
    elseif command == "get_node_info" then
        return "node_info"
    elseif command == "get_site_info" then
        return "site_info"
    else
        return command
    end
end

local function expand_command(short)
    if short == "geo_disc" then
        return "geometric_discover"
    elseif short == "geo_store" then
        return "geometric_store_topology"
    elseif short == "geo_seq" then
        return "geometric_load_sequence"
    elseif short == "geo_dist" then
        return "geometric_distance"
    elseif short == "geo_dim" then
        return "geometric_dimensions"
    elseif short == "str_info" then
        return "stream_info"
    elseif short == "str_group" then
        return "stream_group_info"
    elseif short == "str_cons" then
        return "stream_consumer_info"
    elseif short == "str_pend" then
        return "stream_pending"
    elseif short == "node_info" then
        return "get_node_info"
    elseif short == "site_info" then
        return "get_site_info"
    else
        return short
    end
end

-- Helper to safely handle potential field serialization errors
local function safe_field_serialization(fields)
    local result = {}
    for k, v in pairs(fields) do
        -- Handle field serialization based on type
        if type(v) == "table" then
            -- For table values, use cjson to ensure proper encoding
            local ok, encoded = pcall(function() return cjson.encode(v) end)
            if ok then
                result[k] = encoded
            else
                -- Fallback for encoding errors
                result[k] = json_encode(v)
            end
        elseif type(v) == "string" or type(v) == "number" or type(v) == "boolean" then
            -- Simple types can be stored directly
            result[k] = v
        else
            -- Convert nil and other types to string representation
            result[k] = tostring(v)
        end
    end
    return result
end

-- ------------------------------------------------------------------------
-- Protocol conversion functions
-- ------------------------------------------------------------------------

--
-- GNODE_PROTOCOL_ENCODE: Convert JSON to optimized RESP3
-- KEYS[1]: stream_key
-- ARGV[1]: JSON string (standard format)
-- Returns: Message ID
--
server.register_function{
    function_name = 'GNODE_PROTOCOL_ENCODE',
    callback = function(keys, args)
        local stream_key = keys[1]
        local json_str = args[1]
        
        -- Parse JSON
        local message, err = json_decode(json_str)
        if not message then
            return server.error_reply("Invalid JSON: " .. (err or "unknown error"))
        end
        
        -- Extract fields according to mapping table
        local fields = {}
        
        -- Message type (single character code)
        if message.type then
            -- Map type values to the appropriate type code
            local type_value = message.type
            if type_value == "batch_command" then
                fields.t = "bc"
            elseif type_value == "batch_response" then
                fields.t = "br"
            elseif type_value == "batch" then
                -- For backward compatibility, interpret based on context
                if message.content and message.content.messages then
                    -- Default to batch command if not otherwise specified
                    fields.t = "bc"
                else
                    -- Fallback for empty batches
                    fields.t = "bc"
                end
            else
                -- Regular types: "command" -> "c", "response" -> "r"
                fields.t = string.sub(type_value, 1, 1)
            end
        end
        
        -- Source info
        if message.source then
            fields.ss = message.source.site_id
            fields.sn = message.source.node_id
        end
        
        -- Destination info
        if message.destination then
            fields.ds = message.destination.site_id
            fields.dn = message.destination.node_id
        end
        
        -- Optional extended fields if present
        if message.path then
            fields.pa = message.path
        end
        
        if message.category then
            fields.ca = message.category
        end
        
        if message.load then
            fields.lo = message.load
        end
        
        if message.version then
            fields.ve = message.version
        end
        
        if message.signature then
            fields.si = message.signature
        end
        
        -- Message type-specific fields
        if fields.t == "c" then -- Command
            fields.c = shorten_command(message.content.command)
            
            -- Convert parameters to JSON string if they're an object
            if type(message.content.parameters) == "table" then
                local ok, encoded = pcall(function() return cjson.encode(message.content.parameters) end)
                if ok then
                    fields.p = encoded
                else
                    -- Fallback for encoding errors
                    fields.p = json_encode(message.content.parameters)
                end
            else
                fields.p = message.content.parameters or "{}"
            end
        elseif fields.t == "r" then -- Response
            if message.correlation then
                fields.ri = message.correlation.request_id
            end
            fields.st = message.content.status
            if message.content.result then
                -- Convert result to JSON string if it's an object
                if type(message.content.result) == "table" then
                    local ok, encoded = pcall(function() return cjson.encode(message.content.result) end)
                    if ok then
                        fields.r = encoded
                    else
                        -- Fallback for encoding errors
                        fields.r = json_encode(message.content.result)
                    end
                else
                    fields.r = tostring(message.content.result)
                end
            end
            if message.content.error then
                fields.e = message.content.error
            end
        elseif fields.t == "bc" or fields.t == "br" then -- Batch command or batch response
            fields.bi = message.id
            fields.tc = message.correlation and message.correlation.total_messages or 0
            
            -- Process batch messages
            if message.content and message.content.messages then
                local messages = {}
                for i, msg in ipairs(message.content.messages) do
                    local params
                    if type(msg.parameters) == "table" then
                        local ok, encoded = pcall(function() return cjson.encode(msg.parameters) end)
                        if ok then
                            params = encoded
                        else
                            params = json_encode(msg.parameters)
                        end
                    else
                        params = msg.parameters or "{}"
                    end
                    
                    table.insert(messages, {
                        string.sub(msg.type or "command", 1, 1),
                        shorten_command(msg.command),
                        params,
                        msg.sequence or (i-1)
                    })
                end
                -- Store messages as JSON string to avoid complex nesting
                local ok, encoded = pcall(function() return cjson.encode(messages) end)
                if ok then
                    fields.m = encoded
                else
                    fields.m = json_encode(messages)
                end
            end
        end
        
        -- Correlation fields
        if message.correlation then
            if message.correlation.batch_id then
                fields.bi = message.correlation.batch_id
            end
            if message.correlation.sequence ~= nil then
                fields.sq = message.correlation.sequence
            end
        end
        
        -- Timestamp (convert to milliseconds)
        if message.timestamp then
            fields.ts = math.floor(message.timestamp * 1000)
        else
            local t = server.call('TIME')
            fields.ts = math.floor(t[1] * 1000 + t[2] / 1000)
        end
        
        -- Process fields for safe serialization
        local safe_fields = safe_field_serialization(fields)
        
        -- Build XADD command arguments
        local xadd_args = {'XADD', stream_key, '*'}
        for k, v in pairs(safe_fields) do
            table.insert(xadd_args, k)
            table.insert(xadd_args, v)
        end
        
        -- Add to stream
        return server.call(unpack(xadd_args))
    end,
    description = 'Encodes a JSON message to optimized RESP3 format and adds it to the unified stream'
}

--
-- GNODE_PROTOCOL_DECODE: Convert optimized RESP3 to JSON
-- KEYS[1]: stream_key
-- ARGV[1]: message_id
-- Returns: JSON string in standard format
--
server.register_function{
    function_name = 'GNODE_PROTOCOL_DECODE',
    callback = function(keys, args)
        local stream_key = keys[1]
        local message_id = args[1]
        
        -- Read the message from the stream
        local result = server.call('XRANGE', stream_key, message_id, message_id)
        if #result == 0 then
            return server.error_reply("Message not found: " .. message_id)
        end
        
        -- Extract fields from the message
        local message = result[1]
        local id = message[1]
        local fields = {}
        
        -- Convert field array to map
        for i = 1, #message[2], 2 do
            fields[message[2][i]] = message[2][i+1]
        end
        
        -- Build JSON response
        local json = {}
        
        -- Set ID and type
        json.id = id
        
        -- Message type
        if fields.t == "c" then
            json.type = "command"
        elseif fields.t == "r" then
            json.type = "response"
        elseif fields.t == "bc" then
            json.type = "batch_command"
        elseif fields.t == "br" then
            json.type = "batch_response"
        elseif fields.t == "b" then
            -- Legacy handling - convert to batch_command for backward compatibility
            json.type = "batch_command"
        else
            json.type = "unknown"
        end
        
        -- Source
        json.source = {
            site_id = fields.ss or "",
            node_id = fields.sn or ""
        }
        
        -- Destination
        json.destination = {
            site_id = fields.ds or "",
            node_id = fields.dn or ""
        }
        
        -- Add extended fields if present
        if fields.pa then
            json.path = fields.pa
        end
        
        if fields.ca then
            json.category = fields.ca
        end
        
        if fields.lo then
            json.load = tonumber(fields.lo)
        end
        
        if fields.ve then
            json.version = fields.ve
        end
        
        if fields.si then
            json.signature = fields.si
        end
        
        -- Correlation
        json.correlation = {}
        if fields.ri then
            json.correlation.request_id = fields.ri
        end
        if fields.bi then
            json.correlation.batch_id = fields.bi
        end
        if fields.sq then
            json.correlation.sequence = tonumber(fields.sq)
        end
        if fields.tc then
            json.correlation.total_messages = tonumber(fields.tc)
        end
        
        -- Content
        json.content = {}
        
        -- Message type-specific content
        if fields.t == "c" then -- Command
            json.content.command = expand_command(fields.c)
            
            -- Parse parameters from JSON string
            if fields.p then
                local ok, params = pcall(function() return cjson.decode(fields.p) end)
                if ok then
                    json.content.parameters = params
                else
                    -- If parsing fails, return as-is
                    json.content.parameters = fields.p
                end
            else
                json.content.parameters = {}
            end
        elseif fields.t == "r" then -- Response
            json.content.status = fields.st
            if fields.r then
                -- Parse result from JSON string if possible
                local ok, result_obj = pcall(function() return cjson.decode(fields.r) end)
                if ok then
                    json.content.result = result_obj
                else
                    -- If parsing fails, return as-is
                    json.content.result = fields.r
                end
            end
            if fields.e then
                json.content.error = fields.e
            end
        elseif fields.t == "b" or fields.t == "bc" or fields.t == "br" then -- Any batch type
            json.content.messages = {}
            
            if fields.m then
                -- Parse messages array from JSON string
                local ok, messages = pcall(function() return cjson.decode(fields.m) end)
                if ok and type(messages) == "table" then
                    for i, msg in ipairs(messages) do
                        local message_type = "command"
                        if msg[1] == "r" then
                            message_type = "response"
                        elseif msg[1] == "bc" then
                            message_type = "batch_command"
                        elseif msg[1] == "br" then
                            message_type = "batch_response"
                        end
                        
                        -- Parse parameters from JSON string if possible
                        local params
                        if msg[3] then
                            local ok, decoded = pcall(function() return cjson.decode(msg[3]) end)
                            if ok then
                                params = decoded
                            else
                                params = msg[3]
                            end
                        else
                            params = {}
                        end
                        
                        table.insert(json.content.messages, {
                            type = message_type,
                            command = expand_command(msg[2]),
                            parameters = params,
                            sequence = msg[4]
                        })
                    end
                end
            end
        end
        
        -- Timestamp (convert from milliseconds to seconds)
        json.timestamp = tonumber(fields.ts) / 1000
        
        -- Encode the result to JSON
        return json_encode(json)
    end,
    flags = {'no-writes'},
    description = 'Decodes a message from the unified stream to standard JSON format'
}

--
-- GNODE_PROTOCOL_READ_GROUP: Read and decode messages from a consumer group
-- KEYS[1]: stream_key
-- ARGV[1]: group_name
-- ARGV[2]: consumer_name
-- ARGV[3]: count
-- ARGV[4]: block_ms
-- Returns: JSON array of messages
--
server.register_function{
    function_name = 'GNODE_PROTOCOL_READ_GROUP',
    callback = function(keys, args)
        local stream_key = keys[1]
        local group_name = args[1]
        local consumer_name = args[2]
        local count = tonumber(args[3] or '10')
        local block_ms = tonumber(args[4] or '1000')
        
        -- Create consumer group if it doesn't exist
        local group_exists = false
        
        -- Check if the group exists
        local ok, groups = pcall(function()
            return server.call('XINFO', 'GROUPS', stream_key)
        end)
        
        if ok and groups then
            for _, group in ipairs(groups) do
                -- XINFO GROUPS returns array with name at index 2 (RESP3 format)
                if type(group) == "table" and #group >= 2 and group[2] == group_name then
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
        
        -- Read messages from the stream
        local result = server.pcall('XREADGROUP', 'GROUP', group_name, consumer_name, 
                                   'COUNT', count, 'BLOCK', block_ms, 'STREAMS', stream_key, '>')
        
        -- No messages available or error
        if result.err or not result.ok or not result.ok[1] then
            return json_encode({})
        end
        
        -- Process the messages
        local messages = {}
        
        -- The result structure: [[stream_key, [[msg_id, [field1, value1, field2, value2, ...]], ...]]]
        local stream_entries = result.ok[1][2]
        
        for _, entry in ipairs(stream_entries) do
            local msg_id = entry[1]
            local fields = {}
            
            -- Convert field array to map
            for i = 1, #entry[2], 2 do
                fields[entry[2][i]] = entry[2][i+1]
            end
            
            -- Build JSON message
            local json = {
                id = msg_id,
                internal_id = msg_id -- Keep the stream message ID for acknowledgment
            }
            
            -- Message type
            if fields.t == "c" then
                json.type = "command"
            elseif fields.t == "r" then
                json.type = "response"
            elseif fields.t == "b" then
                json.type = "command"
                json.subtype = "batch"
            else
                json.type = "unknown"
            end
            
            -- Source
            json.source = {
                site_id = fields.ss or "",
                node_id = fields.sn or ""
            }
            
            -- Destination
            json.destination = {
                site_id = fields.ds or "",
                node_id = fields.dn or ""
            }
            
            -- Correlation
            json.correlation = {}
            if fields.ri then
                json.correlation.request_id = fields.ri
            end
            if fields.bi then
                json.correlation.batch_id = fields.bi
            end
            if fields.sq then
                json.correlation.sequence = tonumber(fields.sq)
            end
            if fields.tc then
                json.correlation.total_messages = tonumber(fields.tc)
            end
            
            -- Content
            json.content = {}
            
            -- Message type-specific content
            if fields.t == "c" then -- Command
                json.content.command = expand_command(fields.c)
                
                -- Parse parameters from JSON string
                if fields.p then
                    local ok, params = pcall(function() return cjson.decode(fields.p) end)
                    if ok then
                        json.content.parameters = params
                    else
                        -- If parsing fails, return as-is
                        json.content.parameters = fields.p
                    end
                else
                    json.content.parameters = {}
                end
            elseif fields.t == "r" then -- Response
                json.content.status = fields.st
                if fields.r then
                    -- Parse result from JSON string if possible
                    local ok, result_obj = pcall(function() return cjson.decode(fields.r) end)
                    if ok then
                        json.content.result = result_obj
                    else
                        -- If parsing fails, return as-is
                        json.content.result = fields.r
                    end
                end
                if fields.e then
                    json.content.error = fields.e
                end
            elseif fields.t == "b" then -- Batch
                json.content.messages = {}
                
                if fields.m then
                    -- Parse messages array from JSON string
                    local ok, batch_messages = pcall(function() return cjson.decode(fields.m) end)
                    if ok and type(batch_messages) == "table" then
                        for i, msg in ipairs(batch_messages) do
                            local message_type = "command"
                            if msg[1] == "r" then
                                message_type = "response"
                            end
                            
                            -- Parse parameters from JSON string if possible
                            local params
                            if msg[3] then
                                local ok, decoded = pcall(function() return cjson.decode(msg[3]) end)
                                if ok then
                                    params = decoded
                                else
                                    params = msg[3]
                                end
                            else
                                params = {}
                            end
                            
                            table.insert(json.content.messages, {
                                type = message_type,
                                command = expand_command(msg[2]),
                                parameters = params,
                                sequence = msg[4]
                            })
                        end
                    end
                end
            end
            
            -- Timestamp (convert from milliseconds to seconds)
            json.timestamp = tonumber(fields.ts) / 1000
            
            table.insert(messages, json)
        end
        
        return json_encode(messages)
    end,
    description = 'Reads messages from a unified stream using a consumer group and decodes to JSON format'
}

--
-- GNODE_PROTOCOL_ACK: Acknowledge messages in a consumer group
-- KEYS[1]: stream_key
-- ARGV[1]: group_name
-- ARGV[2]: message_id (raw string) OR message_ids_json (JSON array)
-- Returns: Number of acknowledged messages
--
-- This function intelligently handles both:
--   1. Single message: pass raw message ID string (e.g., "1760453776060-0")
--   2. Batch: pass JSON array (e.g., '["1760453776060-0", "1760453776061-0"]')
--
server.register_function{
    function_name = 'GNODE_PROTOCOL_ACK',
    callback = function(keys, args)
        local stream_key = keys[1]
        local group_name = args[1]
        local message_param = args[2]

        -- Handle both single message ID (raw string) and JSON array of IDs
        -- This allows clients to pass either "1760453776060-0" or '["1760453776060-0", "1760453776061-0"]'
        local message_ids = {}

        -- Check if this looks like a JSON array (starts with '[')
        if type(message_param) == "string" and string.sub(message_param, 1, 1) == "[" then
            -- Parse as JSON array for batch acknowledgment
            local parsed, err = json_decode(message_param)
            if not parsed then
                return server.error_reply("Invalid JSON array: " .. (err or "unknown error"))
            end
            message_ids = parsed
        else
            -- Treat as single raw message ID (most common case)
            message_ids = {message_param}
        end

        -- Validate message IDs
        if #message_ids == 0 then
            return 0
        end

        -- Acknowledge all messages in a single call
        local xack_args = {'XACK', stream_key, group_name}
        for _, id in ipairs(message_ids) do
            table.insert(xack_args, id)
        end

        -- Execute command
        local result = server.pcall(unpack(xack_args))

        if result.err then
            return server.error_reply("Failed to acknowledge messages: " .. result.err)
        end

        return result.ok or 0
    end,
    description = 'Acknowledges messages in a unified stream consumer group'
}

--
-- GNODE_PROTOCOL_CLAIM: Claim pending messages
-- KEYS[1]: stream_key
-- ARGV[1]: group_name
-- ARGV[2]: consumer_name
-- ARGV[3]: min_idle_time
-- ARGV[4]: count
-- Returns: JSON array of claimed messages
--
server.register_function{
    function_name = 'GNODE_PROTOCOL_CLAIM',
    callback = function(keys, args)
        local stream_key = keys[1]
        local group_name = args[1]
        local consumer_name = args[2]
        local min_idle_time = tonumber(args[3] or '30000')
        local count = tonumber(args[4] or '10')
        
        -- Get pending messages
        local pending_result = server.pcall('XPENDING', stream_key, group_name, '-', '+', count)
        
        if pending_result.err or not pending_result.ok or #pending_result.ok == 0 then
            return json_encode({})
        end
        
        -- Extract message IDs
        local message_ids = {}
        for _, pending in ipairs(pending_result.ok) do
            -- XPENDING returns: [message_id, consumer, milliseconds idle, delivery counter]
            table.insert(message_ids, pending[1])
        end
        
        -- Claim the messages
        local xclaim_args = {'XCLAIM', stream_key, group_name, consumer_name, min_idle_time}
        for _, id in ipairs(message_ids) do
            table.insert(xclaim_args, id)
        end
        
        local claim_result = server.pcall(unpack(xclaim_args))
        
        if claim_result.err or not claim_result.ok or #claim_result.ok == 0 then
            return json_encode({})
        end
        
        -- Process the claimed messages
        local messages = {}
        
        for _, entry in ipairs(claim_result.ok) do
            local msg_id = entry[1]
            local fields = {}
            
            -- Convert field array to map
            for i = 1, #entry[2], 2 do
                fields[entry[2][i]] = entry[2][i+1]
            end
            
            -- Build JSON message
            local json = {
                id = msg_id,
                internal_id = msg_id -- Keep the stream message ID for acknowledgment
            }
            
            -- Message type
            if fields.t == "c" then
                json.type = "command"
            elseif fields.t == "r" then
                json.type = "response"
            elseif fields.t == "b" then
                json.type = "command"
                json.subtype = "batch"
            else
                json.type = "unknown"
            end
            
            -- Source
            json.source = {
                site_id = fields.ss or "",
                node_id = fields.sn or ""
            }
            
            -- Destination
            json.destination = {
                site_id = fields.ds or "",
                node_id = fields.dn or ""
            }
            
            -- Correlation
            json.correlation = {}
            if fields.ri then
                json.correlation.request_id = fields.ri
            end
            if fields.bi then
                json.correlation.batch_id = fields.bi
            end
            if fields.sq then
                json.correlation.sequence = tonumber(fields.sq)
            end
            if fields.tc then
                json.correlation.total_messages = tonumber(fields.tc)
            end
            
            -- Content
            json.content = {}
            
            -- Message type-specific content
            if fields.t == "c" then -- Command
                json.content.command = expand_command(fields.c)
                
                -- Parse parameters from JSON string
                if fields.p then
                    local ok, params = pcall(function() return cjson.decode(fields.p) end)
                    if ok then
                        json.content.parameters = params
                    else
                        -- If parsing fails, return as-is
                        json.content.parameters = fields.p
                    end
                else
                    json.content.parameters = {}
                end
            elseif fields.t == "r" then -- Response
                json.content.status = fields.st
                if fields.r then
                    -- Parse result from JSON string if possible
                    local ok, result_obj = pcall(function() return cjson.decode(fields.r) end)
                    if ok then
                        json.content.result = result_obj
                    else
                        -- If parsing fails, return as-is
                        json.content.result = fields.r
                    end
                end
                if fields.e then
                    json.content.error = fields.e
                end
            elseif fields.t == "b" then -- Batch
                json.content.messages = {}
                
                if fields.m then
                    -- Parse messages array from JSON string
                    local ok, batch_messages = pcall(function() return cjson.decode(fields.m) end)
                    if ok and type(batch_messages) == "table" then
                        for i, msg in ipairs(batch_messages) do
                            local message_type = "command"
                            if msg[1] == "r" then
                                message_type = "response"
                            end
                            
                            -- Parse parameters from JSON string if possible
                            local params
                            if msg[3] then
                                local ok, decoded = pcall(function() return cjson.decode(msg[3]) end)
                                if ok then
                                    params = decoded
                                else
                                    params = msg[3]
                                end
                            else
                                params = {}
                            end
                            
                            table.insert(json.content.messages, {
                                type = message_type,
                                command = expand_command(msg[2]),
                                parameters = params,
                                sequence = msg[4]
                            })
                        end
                    end
                end
            end
            
            -- Timestamp (convert from milliseconds to seconds)
            json.timestamp = tonumber(fields.ts) / 1000
            
            table.insert(messages, json)
        end
        
        return json_encode(messages)
    end,
    description = 'Claims pending messages from a unified stream consumer group'
}

--
-- GNODE_PROTOCOL_INFO: Get information about the protocol usage
-- Returns: JSON object with protocol statistics
--
server.register_function{
    function_name = 'GNODE_PROTOCOL_INFO',
    callback = function(keys, args)
        local info = {
            version = "2.0.0",  -- Updated version with batch type differentiation and extended fields
            description = "gNode Unified Stream Protocol",
            command_map = {
                geometric_discover = "geo_disc",
                geometric_store_topology = "geo_store",
                geometric_load_sequence = "geo_seq",
                geometric_distance = "geo_dist",
                geometric_dimensions = "geo_dim",
                stream_info = "str_info",
                stream_group_info = "str_group",
                stream_consumer_info = "str_cons",
                stream_pending = "str_pend",
                get_node_info = "node_info",
                get_site_info = "site_info"
            },
            type_map = {
                c = "command",
                r = "response",
                bc = "batch_command",
                br = "batch_response",
                m = "metadata",
                e = "error",
                i = "initialization"
            },
            field_map = {
                type = "t",
                source_site_id = "ss",
                source_node_id = "sn",
                destination_site_id = "ds",
                destination_node_id = "dn",
                command = "c",
                parameters = "p",
                request_id = "ri",
                batch_id = "bi",
                sequence = "sq",
                status = "st",
                result = "r",
                error = "e",
                total_count = "tc",
                messages = "m",
                timestamp = "ts",
                path = "pa",
                category = "ca",
                load = "lo",
                version = "ve",
                signature = "si"
            },
            extended_fields = {
                pa = "path for API routing",
                ca = "category for message classification",
                lo = "load factor for resource distribution",
                ve = "version for versioning support",
                si = "signature for verification"
            }
        }
        
        return json_encode(info)
    end,
    flags = {'no-writes'},
    description = 'Returns information about the gNode protocol conversion functions'
}