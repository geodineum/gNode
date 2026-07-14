#!lua name=gnode_direct

--
-- gNode DIRECT CHANNEL Functions
-- A ValKey function library for direct inter-service channel lifecycle
--
-- Direct channels are gNode-provisioned streams that allow two sites to
-- communicate directly (XADD/XREADGROUP) without per-message daemon relay.
-- gNode provisions the channel (creates stream, consumer groups, metadata)
-- then steps out of the hot path.
--
-- Supports two modes:
--   temporary  — TTL-based, auto-expires after ttl_seconds
--   persistent — no TTL, survives daemon restarts, explicit close only
--
-- Key patterns (all under {topology_namespace}:gnode:direct):
--   {ns}:gnode:direct:{channel_id}       — the stream (messages)
--   {ns}:gnode:direct:meta:{channel_id}  — metadata HASH
--   {ns}:gnode:direct:channels           — index HASH (channel_id → compact JSON)
--
-- Usage:
--   GNODE_DIRECT_PROVISION(key, channel_id, source_site, target_site, mode, ttl_seconds, metadata_json, environment)
--   GNODE_DIRECT_CLOSE(key, channel_id)
--   GNODE_DIRECT_INFO(key, channel_id)
--   GNODE_DIRECT_LIST(key, [site_id_filter], [environment_filter])
--   GNODE_DIRECT_CHECK_EXPIRY(key, now_seconds, max_idle_seconds)
--

-- Safe JSON encode helper (pcall-wrapped per project convention)
local function safe_json_encode(value)
    local ok, json = pcall(cjson.encode, value)
    if not ok then return nil, "encode_error" end
    return json
end

-- Safe JSON decode helper
local function safe_json_decode(str)
    local ok, data = pcall(cjson.decode, str)
    if not ok then return nil, "decode_error" end
    return data
end


-- ============================================================================
-- GNODE_DIRECT_PROVISION
-- Atomically create channel stream + metadata + consumer groups + index entry
-- ============================================================================
server.register_function{
    function_name = 'GNODE_DIRECT_PROVISION',
    description = 'Provision a direct inter-service channel (stream + metadata + consumer groups)',
    callback = function(keys, args)
        -- keys[1] = base key prefix e.g. "{geodineum}:gnode:direct"
        -- args: channel_id, source_site, target_site, mode, ttl_seconds, metadata_json, environment
        if #keys < 1 or #args < 5 then
            local err_json = safe_json_encode({ok = false, error = "Usage: FCALL GNODE_DIRECT_PROVISION 1 base_key channel_id source_site target_site mode ttl_seconds [metadata_json] [environment]"})
            return err_json or '{"ok":false,"error":"invalid_args"}'
        end

        local base_key = keys[1]
        local channel_id = args[1]
        local source_site = args[2]
        local target_site = args[3]
        local mode = args[4]           -- "temporary" or "persistent"
        local ttl_seconds = tonumber(args[5]) or 0
        local metadata_json = args[6] or '{}'
        local environment = args[7] or 'production'  -- DTAP environment

        -- Validate mode
        if mode ~= "temporary" and mode ~= "persistent" then
            local err_json = safe_json_encode({ok = false, error = "Invalid mode: must be 'temporary' or 'persistent'"})
            return err_json or '{"ok":false,"error":"invalid_mode"}'
        end

        -- Validate participants
        if not source_site or source_site == "" or not target_site or target_site == "" then
            local err_json = safe_json_encode({ok = false, error = "source_site and target_site are required"})
            return err_json or '{"ok":false,"error":"missing_participants"}'
        end

        if source_site == target_site then
            local err_json = safe_json_encode({ok = false, error = "source_site and target_site must be different"})
            return err_json or '{"ok":false,"error":"same_site"}'
        end

        local now = tonumber(server.call('TIME')[1])

        -- Compute expiry
        local expires_at = 0
        if mode == "temporary" then
            if ttl_seconds <= 0 then
                ttl_seconds = 300  -- default 5 minutes
            end
            expires_at = now + ttl_seconds
        end

        -- Key construction
        local stream_key = base_key .. ":" .. channel_id
        local meta_key = base_key .. ":meta:" .. channel_id
        local index_key = base_key .. ":channels"

        -- Check if channel already exists
        local existing = server.call('EXISTS', meta_key)
        if existing == 1 then
            local err_json = safe_json_encode({ok = false, error = "Channel already exists: " .. channel_id})
            return err_json or '{"ok":false,"error":"channel_exists"}'
        end

        -- 1. Create stream with init message
        server.call('XADD', stream_key, '*',
            '_init', '1',
            'ss', source_site,
            'ts', target_site,
            'env', environment,
            'ca', tostring(now))

        -- 2. Create consumer groups for both participants
        local source_group = "gnode-" .. source_site
        local target_group = "gnode-" .. target_site
        server.call('XGROUP', 'CREATE', stream_key, source_group, '0')
        server.call('XGROUP', 'CREATE', stream_key, target_group, '0')

        -- 3. Store metadata HASH
        server.call('HSET', meta_key,
            'id', channel_id,
            'ss', source_site,
            'ts', target_site,
            'env', environment,
            'mode', mode,
            'ca', tostring(now),
            'ea', tostring(expires_at),
            'la', tostring(now),
            'st', 'active',
            'sk', stream_key,
            'md', metadata_json)

        -- 4. Set EXPIRE on metadata for temporary channels
        if mode == "temporary" and ttl_seconds > 0 then
            server.call('EXPIRE', meta_key, ttl_seconds)
        end

        -- 5. Add to channel index
        local index_entry = safe_json_encode({
            id = channel_id,
            ss = source_site,
            ts = target_site,
            env = environment,
            mode = mode,
            ca = now,
            ea = expires_at,
            st = 'active'
        })
        if index_entry then
            server.call('HSET', index_key, channel_id, index_entry)
        end

        -- Build response
        local result = {
            ok = true,
            channel_id = channel_id,
            stream_key = stream_key,
            mode = mode,
            environment = environment,
            source_site = source_site,
            target_site = target_site,
            consumer_groups = {source_group, target_group},
            created_at = now,
            expires_at = expires_at > 0 and expires_at or cjson.null
        }

        local result_json = safe_json_encode(result)
        return result_json or '{"ok":false,"error":"encode_error"}'
    end
}


-- ============================================================================
-- GNODE_DIRECT_CLOSE
-- Atomically clean up a channel (stream + metadata + index)
-- ============================================================================
server.register_function{
    function_name = 'GNODE_DIRECT_CLOSE',
    description = 'Close and clean up a direct channel',
    callback = function(keys, args)
        if #keys < 1 or #args < 1 then
            local err_json = safe_json_encode({ok = false, error = "Usage: FCALL GNODE_DIRECT_CLOSE 1 base_key channel_id"})
            return err_json or '{"ok":false,"error":"invalid_args"}'
        end

        local base_key = keys[1]
        local channel_id = args[1]

        local stream_key = base_key .. ":" .. channel_id
        local meta_key = base_key .. ":meta:" .. channel_id
        local index_key = base_key .. ":channels"

        -- Check if channel exists
        local meta_exists = server.call('EXISTS', meta_key)
        local stream_exists = server.call('EXISTS', stream_key)

        if meta_exists == 0 and stream_exists == 0 then
            local err_json = safe_json_encode({ok = false, error = "Channel not found: " .. channel_id})
            return err_json or '{"ok":false,"error":"not_found"}'
        end

        -- Delete stream
        if stream_exists == 1 then
            server.call('DEL', stream_key)
        end

        -- Delete metadata
        if meta_exists == 1 then
            server.call('DEL', meta_key)
        end

        -- Remove from index
        server.call('HDEL', index_key, channel_id)

        local result = {
            ok = true,
            channel_id = channel_id,
            cleaned = true,
            stream_deleted = stream_exists == 1,
            meta_deleted = meta_exists == 1
        }

        local result_json = safe_json_encode(result)
        return result_json or '{"ok":false,"error":"encode_error"}'
    end
}


-- ============================================================================
-- GNODE_DIRECT_INFO
-- Get channel metadata + stream stats
-- ============================================================================
server.register_function{
    function_name = 'GNODE_DIRECT_INFO',
    description = 'Get direct channel metadata and stream stats',
    callback = function(keys, args)
        if #keys < 1 or #args < 1 then
            local err_json = safe_json_encode({ok = false, error = "Usage: FCALL GNODE_DIRECT_INFO 1 base_key channel_id"})
            return err_json or '{"ok":false,"error":"invalid_args"}'
        end

        local base_key = keys[1]
        local channel_id = args[1]

        local stream_key = base_key .. ":" .. channel_id
        local meta_key = base_key .. ":meta:" .. channel_id

        -- Get metadata
        local meta_raw = server.call('HGETALL', meta_key)
        if not meta_raw or #meta_raw == 0 then
            local err_json = safe_json_encode({ok = false, error = "Channel not found: " .. channel_id})
            return err_json or '{"ok":false,"error":"not_found"}'
        end

        -- Convert flat array to table
        local meta = {}
        for i = 1, #meta_raw, 2 do
            meta[meta_raw[i]] = meta_raw[i + 1]
        end

        -- Get stream length
        local message_count = 0
        local stream_exists = server.call('EXISTS', stream_key)
        if stream_exists == 1 then
            message_count = server.call('XLEN', stream_key)
        end

        -- Get consumer group info
        local groups = {}
        if stream_exists == 1 then
            local ok_g, group_info = pcall(server.call, 'XINFO', 'GROUPS', stream_key)
            if ok_g and group_info then
                for _, g in ipairs(group_info) do
                    -- group_info returns arrays of [field, value, field, value, ...]
                    local gdata = {}
                    if type(g) == "table" then
                        for j = 1, #g, 2 do
                            gdata[g[j]] = g[j + 1]
                        end
                    end
                    if gdata['name'] then
                        table.insert(groups, {
                            name = gdata['name'],
                            consumers = tonumber(gdata['consumers'] or 0),
                            pending = tonumber(gdata['pending'] or 0),
                            last_delivered = gdata['last-delivered-id'] or '0-0'
                        })
                    end
                end
            end
        end

        -- Parse metadata JSON if present
        local metadata = {}
        if meta['md'] and meta['md'] ~= '{}' then
            local parsed = safe_json_decode(meta['md'])
            if parsed then metadata = parsed end
        end

        local now = tonumber(server.call('TIME')[1])
        local expires_at = tonumber(meta['ea'] or '0')
        local is_expired = expires_at > 0 and now > expires_at

        local result = {
            ok = true,
            channel_id = meta['id'] or channel_id,
            stream_key = meta['sk'] or (base_key .. ":" .. channel_id),
            source_site = meta['ss'],
            target_site = meta['ts'],
            environment = meta['env'] or 'production',
            mode = meta['mode'],
            status = is_expired and 'expired' or (meta['st'] or 'unknown'),
            created_at = tonumber(meta['ca'] or '0'),
            expires_at = expires_at > 0 and expires_at or cjson.null,
            last_active = tonumber(meta['la'] or '0'),
            message_count = message_count,
            consumer_groups = groups,
            metadata = metadata,
            stream_exists = stream_exists == 1
        }

        local result_json = safe_json_encode(result)
        return result_json or '{"ok":false,"error":"encode_error"}'
    end,
    flags = {'no-writes'}
}


-- ============================================================================
-- GNODE_DIRECT_LIST
-- List channels, optionally filtered by participant site
-- ============================================================================
server.register_function{
    function_name = 'GNODE_DIRECT_LIST',
    description = 'List direct channels, optionally filtered by site and/or environment',
    callback = function(keys, args)
        if #keys < 1 then
            local err_json = safe_json_encode({ok = false, error = "Usage: FCALL GNODE_DIRECT_LIST 1 base_key [site_id_filter] [environment_filter]"})
            return err_json or '{"ok":false,"error":"invalid_args"}'
        end

        local base_key = keys[1]
        local site_filter = args[1]  -- optional
        local env_filter = args[2]   -- optional DTAP environment filter

        -- Normalize empty strings to nil
        if site_filter == "" then site_filter = nil end
        if env_filter == "" then env_filter = nil end

        local index_key = base_key .. ":channels"

        -- Get all channels from index
        local raw = server.call('HGETALL', index_key)
        if not raw or #raw == 0 then
            local result_json = safe_json_encode({ok = true, channels = {}, count = 0})
            return result_json or '{"ok":true,"channels":[],"count":0}'
        end

        local channels = {}
        for i = 1, #raw, 2 do
            local ch_id = raw[i]
            local ch_json = raw[i + 1]

            local ch_data = safe_json_decode(ch_json)
            if ch_data then
                local site_match = true
                local env_match = true

                -- Apply site filter if provided
                if site_filter then
                    site_match = (ch_data.ss == site_filter or ch_data.ts == site_filter)
                end

                -- Apply environment filter if provided
                if env_filter then
                    env_match = (ch_data.env == env_filter)
                end

                if site_match and env_match then
                    table.insert(channels, ch_data)
                end
            end
        end

        local result = {
            ok = true,
            channels = channels,
            count = #channels
        }

        local result_json = safe_json_encode(result)
        return result_json or '{"ok":false,"error":"encode_error"}'
    end,
    flags = {'no-writes'}
}


-- ============================================================================
-- GNODE_DIRECT_CHECK_EXPIRY
-- Check and close expired/idle channels (called periodically by daemon)
-- ============================================================================
server.register_function{
    function_name = 'GNODE_DIRECT_CHECK_EXPIRY',
    description = 'Check and auto-close expired or idle direct channels',
    callback = function(keys, args)
        if #keys < 1 or #args < 2 then
            local err_json = safe_json_encode({ok = false, error = "Usage: FCALL GNODE_DIRECT_CHECK_EXPIRY 1 base_key now_seconds max_idle_seconds"})
            return err_json or '{"ok":false,"error":"invalid_args"}'
        end

        local base_key = keys[1]
        local now = tonumber(args[1])
        local max_idle = tonumber(args[2])

        local index_key = base_key .. ":channels"

        -- Get all channels
        local raw = server.call('HGETALL', index_key)
        if not raw or #raw == 0 then
            local result_json = safe_json_encode({ok = true, checked = 0, expired = 0, idle_closed = 0, closed_ids = {}})
            return result_json or '{"ok":true,"checked":0,"expired":0,"idle_closed":0}'
        end

        local checked = 0
        local expired = 0
        local idle_closed = 0
        local closed_ids = {}

        for i = 1, #raw, 2 do
            local ch_id = raw[i]
            local ch_json = raw[i + 1]
            checked = checked + 1

            local ch_data = safe_json_decode(ch_json)
            if ch_data then
                local should_close = false
                local reason = ""

                -- Check TTL expiry (temporary channels only)
                local ea = tonumber(ch_data.ea or 0)
                if ea > 0 and now > ea then
                    should_close = true
                    reason = "expired"
                    expired = expired + 1
                end

                -- Check idle timeout (all channels, if max_idle > 0)
                if not should_close and max_idle > 0 then
                    -- Read last_active from metadata HASH (more accurate than index)
                    local meta_key = base_key .. ":meta:" .. ch_id
                    local la_raw = server.call('HGET', meta_key, 'la')
                    local la = tonumber(la_raw or '0')

                    -- Fallback: if metadata is gone (TTL expired), close the channel
                    if la == 0 then
                        local meta_exists = server.call('EXISTS', meta_key)
                        if meta_exists == 0 then
                            should_close = true
                            reason = "metadata_expired"
                            expired = expired + 1
                        end
                    elseif (now - la) > max_idle then
                        should_close = true
                        reason = "idle"
                        idle_closed = idle_closed + 1
                    end
                end

                if should_close then
                    -- Clean up: delete stream + metadata + index entry
                    local stream_key = base_key .. ":" .. ch_id
                    local meta_key = base_key .. ":meta:" .. ch_id

                    pcall(server.call, 'DEL', stream_key)
                    pcall(server.call, 'DEL', meta_key)
                    server.call('HDEL', index_key, ch_id)

                    table.insert(closed_ids, {id = ch_id, reason = reason})
                end
            end
        end

        local result = {
            ok = true,
            checked = checked,
            expired = expired,
            idle_closed = idle_closed,
            closed_ids = closed_ids
        }

        local result_json = safe_json_encode(result)
        return result_json or '{"ok":false,"error":"encode_error"}'
    end
}
