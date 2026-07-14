#!lua name=gnode_pubsub

--
-- gNode PUBSUB Functions
-- A ValKey function library for pubsub operations
--
-- This is a port of the gCore Cache Scripts to ValKey functions
-- with enhancements for RESP3 compatibility
--

-- Note: PRNG initialization moved to lazy init inside functions
-- server.call() cannot be used at module load time in ValKey functions
local prng_initialized = false
local function ensure_prng_init()
    if not prng_initialized then
        math.randomseed(os.time() + os.clock() * 1000000)
        prng_initialized = true
    end
end 


local function track_metric(site_id, metric, value, context)
    local metrics_key = '{' .. site_id .. '}:metrics'
    server.call('HINCRBY', metrics_key, metric, value or 1)

    -- If context provided, record detailed metrics
    if context then
        local ctx_key = '{' .. site_id .. '}:metrics:detailed:' .. metric
        local timestamp = server.call('TIME')[1]

        -- Store context as JSON in a sorted set for time-ordered access
        local status, ctx_json = pcall(cjson.encode, context)
        if not status or not ctx_json then
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

-- Helper function for safe JSON decoding
local function safe_json_decode(json_str)
    if not json_str then return nil, "No JSON string provided" end

    local success, data = pcall(cjson.decode, json_str)
    if not success then
        return nil, "Failed to decode JSON: " .. tostring(data)
    end

    return data
end

-- Register publish function (mirroring CacheScriptsPubSub::PUBLISH)
server.register_function{
    function_name = 'GNODE_PUBSUB_PUBLISH',
    callback = function(keys, args)
        -- Input validation
        if #keys < 1 then
            return server.error_reply("Channel required")
        end
        if not args[1] then
            return server.error_reply("Site ID required")
        end
        if not args[2] then
            return server.error_reply("Message required")
        end
        
        local channel = keys[1]
        local site_id = args[1]
        local message = args[2]
        local options_json = args[3]
        
        -- Parse options
        local options = {}
        if options_json then
            local parsed_options, err = safe_json_decode(options_json)
            if not parsed_options then
                return server.error_reply("Invalid options JSON: " .. tostring(err))
            end
            options = parsed_options
        end
        
        -- Track operation timing
        ensure_prng_init()
        local start_time = server.call('TIME')[1]

        -- Generate message ID if not provided
        local msg_id = options.id or string.format(
            "%s-%s-%s",
            site_id,
            string.format("%x", start_time),
            string.format("%x", math.random(1000000))
        )
        
        -- Deduplication check with sliding window (last hour)
        local dedup_key = '{' .. site_id .. '}:msg:dedup'
        local cutoff = start_time - 3600
        
        -- Cleanup old message IDs first for memory efficiency
        server.call('ZREMRANGEBYSCORE', dedup_key, 0, cutoff)
        
        -- RESP3 response structure
        local response = { map = {
            success = true,
            receivers = 0,
            msg_id = msg_id
        }}
        
        -- Check for duplicate
        if server.call('ZSCORE', dedup_key, msg_id) then
            track_metric(site_id, 'messages_deduplicated', 1)
            response.map.success = false
            response.map.reason = 'duplicate'
            return response
        end
        
        -- Record message metadata
        server.call('ZADD', dedup_key, start_time, msg_id)
        server.call('EXPIRE', dedup_key, 3600)
        
        -- Track in stream for replay capability
        local stream_key = '{' .. site_id .. '}:stream:' .. channel
        local max_stream_len = options.max_stream_len or 10000
        
        -- Build entry fields
        local entry_fields = {
            'id', msg_id,
            'message', message,
            'site', site_id,
            'timestamp', tostring(start_time)
        }
        
        -- Add options if provided
        if options_json then
            table.insert(entry_fields, 'options')
            table.insert(entry_fields, options_json)
        end
        
        -- Add to stream
        local stream_id = server.call('XADD', stream_key, 'MAXLEN', '~', max_stream_len, '*', unpack(entry_fields))
        response.map.stream_id = stream_id
        
        -- Handle persistence if requested
        if options.persistent then
            local persist_key = '{' .. site_id .. '}:msg:store:' .. channel
            
            -- Prepare message data
            local msg_data = {
                id = msg_id,
                message = message,
                timestamp = start_time,
                stream_id = stream_id,
                options = options
            }
            
            -- Convert to JSON
            local status, msg_data_json = pcall(cjson.encode, msg_data)
            if status and msg_data_json then
                server.call('HSET', persist_key, msg_id, msg_data_json)

                if options.ttl then
                    server.call('EXPIRE', persist_key, options.ttl)
                end
            else
                -- Log the error but continue
                track_metric(site_id, 'json_encode_errors', 1)
            end
        end
        
        -- Publish message
        local receivers = server.call('PUBLISH', channel, message)
        response.map.receivers = receivers
        
        -- Update metrics
        track_metric(site_id, 'messages_published', 1, {
            channel = channel,
            receivers = receivers,
            size = #message,
            latency = server.call('TIME')[1] - start_time
        })
        
        return response
    end,
    description = 'Publishes a message with deduplication and persistence options'
}

-- Register subscribe function (mirroring CacheScriptsPubSub::SUBSCRIBE)
server.register_function{
    function_name = 'GNODE_PUBSUB_SUBSCRIBE',
    callback = function(keys, args)
        -- Input validation
        if #keys < 1 then
            return server.error_reply("Channel required")
        end
        if not args[1] then
            return server.error_reply("Consumer ID required")
        end
        if not args[2] then
            return server.error_reply("Site ID required")
        end
        
        local channel = keys[1]
        local consumer_id = args[1]
        local site_id = args[2]
        
        -- Track operation timing
        local start_time = server.call('TIME')[1]
        
        -- Registration keys
        local consumers_key = '{' .. site_id .. '}:consumers:' .. channel
        local subscriptions_key = '{' .. site_id .. '}:subscriptions:' .. channel
        
        -- Register consumer
        server.call('SADD', consumers_key, consumer_id)
        
        -- Extract node ID from consumer ID
        local node = string.match(consumer_id, "^([^:]+)")
        if not node then
            node = "unknown"
        end
        
        -- Prepare metadata
        local metadata = {
            timestamp = start_time,
            node = node,
            status = 'active'
        }

        -- Convert metadata to JSON
        local status, metadata_json = pcall(cjson.encode, metadata)
        if not status or not metadata_json then
            return server.error_reply("Failed to encode subscription metadata: " .. tostring(metadata_json))
        end
        
        -- Set metadata
        server.call('HSET', subscriptions_key, consumer_id, metadata_json)
        
        -- Track metric
        track_metric(site_id, 'subscriptions', 1, {
            channel = channel,
            consumer = consumer_id,
            node = node
        })
        
        return server.status_reply("OK")
    end,
    description = 'Registers a subscription for a consumer'
}

-- Register unsubscribe function (mirroring CacheScriptsPubSub::UNSUBSCRIBE)
server.register_function{
    function_name = 'GNODE_PUBSUB_UNSUBSCRIBE',
    callback = function(keys, args)
        -- Input validation
        if #keys < 1 then
            return server.error_reply("Channel required")
        end
        if not args[1] then
            return server.error_reply("Consumer ID required")
        end
        if not args[2] then
            return server.error_reply("Site ID required")
        end
        
        local channel = keys[1]
        local consumer_id = args[1]
        local site_id = args[2]
        
        -- Track operation timing
        local start_time = server.call('TIME')[1]
        
        -- Registration keys
        local consumers_key = '{' .. site_id .. '}:consumers:' .. channel
        local subscriptions_key = '{' .. site_id .. '}:subscriptions:' .. channel
        
        -- Remove consumer registration
        local removed = server.call('SREM', consumers_key, consumer_id)
        server.call('HDEL', subscriptions_key, consumer_id)
        
        -- RESP3 response structure
        local response = { boolean = removed > 0 }
        
        -- Track unsubscribe event if consumer existed
        if removed > 0 then
            track_metric(site_id, 'unsubscribes', 1, {
                channel = channel,
                consumer = consumer_id
            })
        end
        
        return response
    end,
    description = 'Removes a subscription for a consumer'
}

-- Extended function: List subscribers (enhanced beyond original scripts)
server.register_function{
    function_name = 'GNODE_PUBSUB_LIST_SUBSCRIBERS',
    callback = function(keys, args)
        -- Input validation
        if #keys < 1 then
            return server.error_reply("Channel required")
        end
        if not args[1] then
            return server.error_reply("Site ID required")
        end
        
        local channel = keys[1]
        local site_id = args[1]
        
        -- Track operation timing
        local start_time = server.call('TIME')[1]
        
        -- Registration keys
        local consumers_key = '{' .. site_id .. '}:consumers:' .. channel
        local subscriptions_key = '{' .. site_id .. '}:subscriptions:' .. channel
        
        -- Get all consumers
        local consumers = server.call('SMEMBERS', consumers_key)
        
        -- Get subscription metadata for each consumer
        local result = { map = {
            channel = channel,
            subscribers = {},
            count = #consumers
        }}
        
        if #consumers > 0 then
            local metadata = server.call('HMGET', subscriptions_key, unpack(consumers))
            
            for i, consumer_id in ipairs(consumers) do
                local metadata_json = metadata[i]
                local subscriber_info = { consumer_id = consumer_id }
                
                if metadata_json then
                    local parsed, err = safe_json_decode(metadata_json)
                    if parsed then
                        subscriber_info.timestamp = parsed.timestamp
                        subscriber_info.node = parsed.node
                        subscriber_info.status = parsed.status
                    end
                end
                
                table.insert(result.map.subscribers, subscriber_info)
            end
        end
        
        -- Track metric
        track_metric(site_id, 'subscriber_list_queries', 1, {
            channel = channel,
            subscriber_count = #consumers,
            latency = server.call('TIME')[1] - start_time
        })
        
        return result
    end,
    -- Note: No no-writes flag because track_metric writes to metrics hash
    description = 'Lists all subscribers for a channel'
}

-- Extended function: Get message history (enhanced beyond original scripts)
server.register_function{
    function_name = 'GNODE_PUBSUB_GET_HISTORY',
    callback = function(keys, args)
        -- Input validation
        if #keys < 1 then
            return server.error_reply("Channel required")
        end
        if not args[1] then
            return server.error_reply("Site ID required")
        end
        
        local channel = keys[1]
        local site_id = args[1]
        local count = tonumber(args[2] or "10")
        
        -- Track operation timing
        local start_time = server.call('TIME')[1]
        
        -- Stream key
        local stream_key = '{' .. site_id .. '}:stream:' .. channel
        
        -- Get the most recent messages
        local messages = server.call('XREVRANGE', stream_key, '+', '-', 'COUNT', count)
        
        -- Format the result
        local result = { map = {
            channel = channel,
            messages = {},
            count = #messages
        }}
        
        for _, message in ipairs(messages) do
            local id = message[1]
            local fields = message[2]
            
            -- Convert fields array to map
            local msg_data = {}
            for i = 1, #fields, 2 do
                msg_data[fields[i]] = fields[i + 1]
            end
            
            -- Parse options if present
            if msg_data.options then
                local parsed_options, _ = safe_json_decode(msg_data.options)
                if parsed_options then
                    msg_data.options = parsed_options
                end
            end
            
            -- Add stream ID
            msg_data.stream_id = id
            
            table.insert(result.map.messages, msg_data)
        end
        
        -- Track metric
        track_metric(site_id, 'message_history_queries', 1, {
            channel = channel,
            message_count = #messages,
            requested_count = count,
            latency = server.call('TIME')[1] - start_time
        })
        
        return result
    end,
    -- Note: No no-writes flag because track_metric writes to metrics hash
    description = 'Retrieves message history from a channel'
}