#!lua name=gnode_broadcast

-- Broadcast Stream Functions for gNode
--
-- These functions provide server-side operations for broadcast streams,
-- enabling multi-language clients to read/write broadcast messages using FCALL.
-- Broadcast streams use XREAD (no consumer groups) for 1:many pub-sub semantics.
--
-- Architecture:
-- - No consumer groups (no PEL, no XACK)
-- - Time-based retention via XTRIM
-- - Each reader tracks its own last-seen-ID
-- - Perfect for topology updates, service registrations, announcements
--
-- Functions:
-- - GNODE_BROADCAST_READ: Read messages with JSON parsing
-- - GNODE_BROADCAST_WRITE: Write broadcast message
-- - GNODE_BROADCAST_TRIM: Clean up old messages
-- - GNODE_BROADCAST_INFO: Get stream metadata

-- Safe JSON decode helper (P2CF001 fix)
local function safe_decode(json_str)
    if not json_str or json_str == "" then
        return {}, nil
    end
    local ok, result = pcall(cjson.decode, json_str)
    if ok then
        return result, nil
    else
        return nil, "JSON decode error: " .. tostring(result)
    end
end

-- Read broadcast messages (XREAD wrapper with parsing)
--
-- Arguments (keys):
--   keys[1]: stream_key - Broadcast stream key
--
-- Arguments (args):
--   args[1]: last_id - Last seen message ID ('0' for all, '$' for new only)
--   args[2]: count - Maximum number of messages to read (default: 100)
--   args[3]: block_ms - Block timeout in milliseconds (0 = don't block)
--
-- Returns:
--   msgpack array of messages: [{id, type, site_id, timestamp, fields}, ...]
--
-- Example:
--   FCALL GNODE_BROADCAST_READ 1 '{default}:gnode:broadcast:global' '$' 100 5000
server.register_function{
    function_name = 'GNODE_BROADCAST_READ',
    description = 'Read broadcast messages using XREAD with msgpack encoding',
    callback = function(keys, args)
        local stream_key = keys[1]
        local last_id = args[1] or '$'
        local count = tonumber(args[2]) or 100
        local block_ms = tonumber(args[3]) or 0

        -- Build XREAD command
        local cmd = {'XREAD'}

        if count > 0 then
            table.insert(cmd, 'COUNT')
            table.insert(cmd, count)
        end

        if block_ms > 0 then
            table.insert(cmd, 'BLOCK')
            table.insert(cmd, block_ms)
        end

        table.insert(cmd, 'STREAMS')
        table.insert(cmd, stream_key)
        table.insert(cmd, last_id)

        -- Execute XREAD
        local result = server.call(unpack(cmd))

        if not result or #result == 0 then
            return cmsgpack.pack({})
        end

        -- Parse XREAD result: [[stream_name, [[msg_id, [field, value, ...]]]]]
        local messages = {}

        for _, stream in ipairs(result) do
            local stream_msgs = stream[2]

            for _, msg in ipairs(stream_msgs) do
                local msg_id = msg[1]
                local fields = msg[2]

                -- Convert fields array to map
                local field_map = {}
                for i = 1, #fields, 2 do
                    field_map[fields[i]] = fields[i+1]
                end

                -- Build message structure with common field extraction
                table.insert(messages, {
                    id = msg_id,
                    type = field_map.t or field_map.type or 'unknown',
                    site_id = field_map.ss or field_map.site_id or '',
                    timestamp = tonumber(field_map.ts or field_map.timestamp or 0),
                    fields = field_map
                })
            end
        end

        return cmsgpack.pack(messages)
    end,
    flags = {'no-writes'}
}

-- Write broadcast message
--
-- Arguments (keys):
--   keys[1]: stream_key - Broadcast stream key
--
-- Arguments (args):
--   args[1]: message_type - Message type (topology_update, service_registered, etc.)
--   args[2]: fields_json - JSON object with additional fields
--
-- Returns:
--   Message ID from XADD
--
-- Example:
--   FCALL GNODE_BROADCAST_WRITE 1 '{default}:gnode:broadcast:global' 'topology_update' '{"msg":"Test"}'
server.register_function{
    function_name = 'GNODE_BROADCAST_WRITE',
    description = 'Write broadcast message to stream',
    callback = function(keys, args)
        local stream_key = keys[1]
        local message_type = args[1]
        local fields_json = args[2] or '{}'

        -- Parse JSON fields with error handling (P2CF001 fix)
        local fields, decode_err = safe_decode(fields_json)
        if decode_err then
            return server.error_reply("Invalid fields JSON: " .. decode_err)
        end

        -- Ensure type and timestamp fields
        fields.t = message_type
        if not fields.ts and not fields.timestamp then
            -- Get current time in milliseconds
            local time_result = server.call('TIME')
            local seconds = tonumber(time_result[1])
            local microseconds = tonumber(time_result[2])
            fields.ts = tostring((seconds * 1000) + math.floor(microseconds / 1000))
        end

        -- Build XADD command
        local cmd = {'XADD', stream_key, '*'}

        for k, v in pairs(fields) do
            table.insert(cmd, k)
            table.insert(cmd, tostring(v))
        end

        -- Execute XADD
        local msg_id = server.call(unpack(cmd))

        return msg_id
    end
}

-- Trim broadcast stream by retention time
--
-- Arguments (keys):
--   keys[1]: stream_key - Broadcast stream key
--
-- Arguments (args):
--   args[1]: retention_seconds - Keep messages newer than this (default: 300 = 5 minutes)
--
-- Returns:
--   Number of messages trimmed
--
-- Example:
--   FCALL GNODE_BROADCAST_TRIM 1 '{default}:gnode:broadcast:global' 300
server.register_function{
    function_name = 'GNODE_BROADCAST_TRIM',
    description = 'Trim broadcast stream by retention time using MAXLEN',
    callback = function(keys, args)
        local stream_key = keys[1]
        local retention_seconds = tonumber(args[1]) or 300 -- 5 minutes default

        -- Strategy: Use MAXLEN to keep recent messages
        -- Estimate max messages based on retention period and typical rate
        local estimated_rate = 10 -- messages per second
        local max_messages = math.max(retention_seconds * estimated_rate, 1000) -- minimum 1000

        -- Execute XTRIM with approximate trim for efficiency
        local trimmed = server.call('XTRIM', stream_key, 'MAXLEN', '~', max_messages)

        return trimmed
    end
}

-- Get broadcast stream info
--
-- Arguments (keys):
--   keys[1]: stream_key - Broadcast stream key
--
-- Returns:
--   msgpack map with stream metadata: {length, first_id, last_id}
--
-- Example:
--   FCALL GNODE_BROADCAST_INFO 1 '{default}:gnode:broadcast:global'
server.register_function{
    function_name = 'GNODE_BROADCAST_INFO',
    description = 'Get broadcast stream metadata',
    callback = function(keys, args)
        local stream_key = keys[1]

        -- Get stream length
        local length = server.call('XLEN', stream_key)

        -- Get first and last message IDs
        local first = server.call('XRANGE', stream_key, '-', '+', 'COUNT', 1)
        local last = server.call('XREVRANGE', stream_key, '+', '-', 'COUNT', 1)

        local info = {
            length = length,
            first_id = (first[1] and first[1][1]) or '',
            last_id = (last[1] and last[1][1]) or ''
        }

        return cmsgpack.pack(info)
    end,
    flags = {'no-writes'}
}
