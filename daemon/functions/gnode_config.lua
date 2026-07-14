#!lua name=gnode_config

--[[
gNode Configuration Functions
===========================

A ValKey function library for runtime configuration management.
Enables configuration changes without PHP restart.

Key Schema:
  {site_id}:config:{category}  → Hash with key-value pairs
  {default}:config:{category}  → Global defaults (fallback)

Categories:
  - ratelimit: API rate limiting (api_limit, api_window, burst_limit)
  - cache: TTL settings (default_ttl, page_ttl, api_ttl, bundle_ttl)
  - security: Security flags (debug, rate_limiting_enabled)
  - features: Feature toggles (pwa_enabled, tera_templates)

Usage:
  FCALL GNODE_CONFIG_GET 0 my_app ratelimit api_limit 100
  FCALL GNODE_CONFIG_SET 0 my_app ratelimit api_limit 200
  FCALL GNODE_CONFIG_HGETALL 0 my_app ratelimit
  FCALL GNODE_CONFIG_SEED 0 my_app '{"ratelimit":{"api_limit":"100"}}'

Version: 1.0.0
Author: gCube/gCore Integration
]]

-- ============================================================================
-- DEFAULTS: Baked-in default values for all config categories
-- ============================================================================

local DEFAULTS = {
    ratelimit = {
        api_limit = "100",           -- Requests per window
        api_window = "60",           -- Window in seconds
        burst_limit = "10",          -- Burst allowance
        block_duration = "300",      -- Block duration after limit exceeded
        whitelist_enabled = "0",     -- IP whitelist active
        enabled = "1"                -- Rate limiting enabled
    },
    cache = {
        default_ttl = "3600",        -- 1 hour
        page_ttl = "3600",           -- Full page cache
        fragment_ttl = "1800",       -- HTML fragments (30 min)
        template_ttl = "7200",       -- Tera templates (2 hours)
        api_ttl = "300",             -- API responses (5 min)
        gnode_ttl = "600",           -- gNode responses (10 min)
        bundle_ttl = "86400",        -- Pre-rendered bundles (24 hours)
        cube_face_ttl = "3600"       -- Cube face content (1 hour)
    },
    security = {
        debug = "0",                 -- Debug mode off by default
        rate_limiting_enabled = "1", -- Rate limiting on
        firewall_enabled = "1",      -- Firewall on
        audit_enabled = "1",         -- Audit logging on
        audit_level = "detailed",    -- Audit detail level
        blocked_ips = "",            -- Comma-separated blocked IPs
        allowed_ips = ""             -- Comma-separated allowed IPs
    },
    features = {
        pwa_enabled = "1",           -- PWA support
        tera_templates = "1",        -- Tera server-side rendering
        htmx_progressive = "1",      -- HTMX lazy loading
        gpu_accelerated = "1",       -- GPU acceleration hints
        cookie_consent = "1",        -- GDPR cookie banner
        bundle_prerender = "1"       -- Pre-render cube bundles
    }
}

-- ============================================================================
-- HELPER FUNCTIONS
-- ============================================================================

-- Build config key with site_id hash tag for cluster compatibility
local function build_config_key(site_id, category)
    return '{' .. site_id .. '}:config:' .. category
end

-- Build global default key
local function build_default_key(category)
    return '{default}:config:' .. category
end

-- Track config access metrics
local function track_config_metric(site_id, operation, category)
    local metrics_key = '{' .. site_id .. '}:metrics:config'
    local field = operation .. ':' .. category
    server.call('HINCRBY', metrics_key, field, 1)
    server.call('HSET', metrics_key, 'last_access', tostring(server.call('TIME')[1]))
end

-- Get default value for a category/key
local function get_default(category, key)
    if DEFAULTS[category] and DEFAULTS[category][key] then
        return DEFAULTS[category][key]
    end
    return nil
end

-- ============================================================================
-- GNODE_CONFIG_GET: Get a single config value
-- ============================================================================
-- Args: site_id, category, key, [default_value]
-- Returns: String value or default
-- Flags: no-writes (read-only operation)
--
-- Lookup order:
-- 1. {site_id}:config:{category} HGET {key}
-- 2. {default}:config:{category} HGET {key}
-- 3. Baked-in DEFAULTS table
-- 4. Provided default_value argument

server.register_function{
    function_name = 'GNODE_CONFIG_GET',
    callback = function(keys, args)
        -- Validate args
        if not args[1] or not args[2] or not args[3] then
            return server.error_reply("Usage: GNODE_CONFIG_GET site_id category key [default]")
        end

        local site_id = args[1]
        local category = args[2]
        local key = args[3]
        local default_val = args[4]  -- Optional

        -- 1. Try site-specific config
        local site_key = build_config_key(site_id, category)
        local value = server.call('HGET', site_key, key)

        if value then
            return value
        end

        -- 2. Try global default config
        local default_key = build_default_key(category)
        value = server.call('HGET', default_key, key)

        if value then
            return value
        end

        -- 3. Try baked-in defaults
        value = get_default(category, key)
        if value then
            return value
        end

        -- 4. Return provided default or nil
        return default_val
    end,
    flags = {'no-writes'}
}

-- ============================================================================
-- GNODE_CONFIG_GET_INT: Get config value as integer
-- ============================================================================
-- Args: site_id, category, key, [default_value]
-- Returns: Integer value

server.register_function{
    function_name = 'GNODE_CONFIG_GET_INT',
    callback = function(keys, args)
        if not args[1] or not args[2] or not args[3] then
            return server.error_reply("Usage: GNODE_CONFIG_GET_INT site_id category key [default]")
        end

        local site_id = args[1]
        local category = args[2]
        local key = args[3]
        local default_val = tonumber(args[4]) or 0

        -- Same lookup chain as GNODE_CONFIG_GET
        local site_key = build_config_key(site_id, category)
        local value = server.call('HGET', site_key, key)

        if not value then
            local default_key = build_default_key(category)
            value = server.call('HGET', default_key, key)
        end

        if not value then
            value = get_default(category, key)
        end

        if value then
            local num = tonumber(value)
            if num then
                return num
            end
        end

        return default_val
    end,
    flags = {'no-writes'}
}

-- ============================================================================
-- GNODE_CONFIG_SET: Set a single config value
-- ============================================================================
-- Args: site_id, category, key, value
-- Returns: 1 on success

server.register_function{
    function_name = 'GNODE_CONFIG_SET',
    callback = function(keys, args)
        if not args[1] or not args[2] or not args[3] then
            return server.error_reply("Usage: GNODE_CONFIG_SET site_id category key value")
        end

        local site_id = args[1]
        local category = args[2]
        local key = args[3]
        local value = args[4] or ""

        -- Validate category exists in defaults (known categories only)
        if not DEFAULTS[category] then
            return server.error_reply("Unknown config category: " .. category)
        end

        local config_key = build_config_key(site_id, category)

        -- Set the value
        server.call('HSET', config_key, key, value)

        -- Update metadata
        server.call('HSET', config_key, '_updated_at', tostring(server.call('TIME')[1]))
        server.call('HSET', config_key, '_updated_by', 'GNODE_CONFIG_SET')

        -- Track metric
        track_config_metric(site_id, 'set', category)

        return 1
    end,
    flags = {}
}

-- ============================================================================
-- GNODE_CONFIG_MSET: Set multiple config values at once
-- ============================================================================
-- Args: site_id, category, key1, value1, key2, value2, ...
-- Returns: Number of fields set

server.register_function{
    function_name = 'GNODE_CONFIG_MSET',
    callback = function(keys, args)
        if not args[1] or not args[2] then
            return server.error_reply("Usage: GNODE_CONFIG_MSET site_id category key1 val1 [key2 val2 ...]")
        end

        local site_id = args[1]
        local category = args[2]

        if not DEFAULTS[category] then
            return server.error_reply("Unknown config category: " .. category)
        end

        local config_key = build_config_key(site_id, category)
        local count = 0

        -- Process key-value pairs starting at args[3]
        local i = 3
        while args[i] and args[i+1] do
            server.call('HSET', config_key, args[i], args[i+1])
            count = count + 1
            i = i + 2
        end

        -- Update metadata
        server.call('HSET', config_key, '_updated_at', tostring(server.call('TIME')[1]))
        server.call('HSET', config_key, '_updated_by', 'GNODE_CONFIG_MSET')
        server.call('HSET', config_key, '_field_count', tostring(count))

        track_config_metric(site_id, 'mset', category)

        return count
    end,
    flags = {}
}

-- ============================================================================
-- GNODE_CONFIG_HGETALL: Get all config values for a category
-- ============================================================================
-- Args: site_id, category
-- Returns: Array of [key, value, key, value, ...]
-- Merges: site config + global defaults + baked-in defaults

server.register_function{
    function_name = 'GNODE_CONFIG_HGETALL',
    callback = function(keys, args)
        if not args[1] or not args[2] then
            return server.error_reply("Usage: GNODE_CONFIG_HGETALL site_id category")
        end

        local site_id = args[1]
        local category = args[2]

        if not DEFAULTS[category] then
            return server.error_reply("Unknown config category: " .. category)
        end

        -- Start with baked-in defaults
        local result = {}
        for k, v in pairs(DEFAULTS[category]) do
            result[k] = v
        end

        -- Overlay global defaults
        local default_key = build_default_key(category)
        local global_config = server.call('HGETALL', default_key)
        if global_config then
            for i = 1, #global_config, 2 do
                local k = global_config[i]
                local v = global_config[i+1]
                if k and not k:match('^_') then  -- Skip metadata fields
                    result[k] = v
                end
            end
        end

        -- Overlay site-specific config (highest priority)
        local site_key = build_config_key(site_id, category)
        local site_config = server.call('HGETALL', site_key)
        if site_config then
            for i = 1, #site_config, 2 do
                local k = site_config[i]
                local v = site_config[i+1]
                if k and not k:match('^_') then  -- Skip metadata fields
                    result[k] = v
                end
            end
        end

        -- Convert to flat array for RESP compatibility
        local flat = {}
        for k, v in pairs(result) do
            table.insert(flat, k)
            table.insert(flat, v)
        end

        return flat
    end,
    flags = {'no-writes'}
}

-- ============================================================================
-- GNODE_CONFIG_DELETE: Delete a config key
-- ============================================================================
-- Args: site_id, category, key
-- Returns: 1 if deleted, 0 if not found

server.register_function{
    function_name = 'GNODE_CONFIG_DELETE',
    callback = function(keys, args)
        if not args[1] or not args[2] or not args[3] then
            return server.error_reply("Usage: GNODE_CONFIG_DELETE site_id category key")
        end

        local site_id = args[1]
        local category = args[2]
        local key = args[3]

        local config_key = build_config_key(site_id, category)
        local result = server.call('HDEL', config_key, key)

        server.call('HSET', config_key, '_updated_at', tostring(server.call('TIME')[1]))
        track_config_metric(site_id, 'delete', category)

        return result
    end,
    flags = {}
}

-- ============================================================================
-- GNODE_CONFIG_RESET: Reset category to defaults (delete all site-specific)
-- ============================================================================
-- Args: site_id, category
-- Returns: 1 on success

server.register_function{
    function_name = 'GNODE_CONFIG_RESET',
    callback = function(keys, args)
        if not args[1] or not args[2] then
            return server.error_reply("Usage: GNODE_CONFIG_RESET site_id category")
        end

        local site_id = args[1]
        local category = args[2]

        if not DEFAULTS[category] then
            return server.error_reply("Unknown config category: " .. category)
        end

        local config_key = build_config_key(site_id, category)

        -- Delete the entire hash
        server.call('DEL', config_key)

        track_config_metric(site_id, 'reset', category)

        return 1
    end,
    flags = {}
}

-- ============================================================================
-- GNODE_CONFIG_SEED: Seed config from JSON (for migration/initialization)
-- ============================================================================
-- Args: site_id, json_config
-- json_config format: {"category": {"key": "value", ...}, ...}
-- Returns: Number of categories seeded

server.register_function{
    function_name = 'GNODE_CONFIG_SEED',
    callback = function(keys, args)
        if not args[1] or not args[2] then
            return server.error_reply("Usage: GNODE_CONFIG_SEED site_id json_config")
        end

        local site_id = args[1]
        local json_str = args[2]

        -- Parse JSON using cjson (JSON.PARSE is not a valid ValKey command)
        local success, config = pcall(cjson.decode, json_str)

        if not success or not config or type(config) ~= 'table' then
            return server.error_reply("Invalid JSON config")
        end

        local categories_seeded = 0

        -- Iterate over categories
        for category, values in pairs(config) do
            if DEFAULTS[category] and type(values) == 'table' then
                local config_key = build_config_key(site_id, category)

                -- Set each key-value pair
                for k, v in pairs(values) do
                    if type(v) ~= 'table' then  -- Only scalar values
                        server.call('HSET', config_key, k, tostring(v))
                    end
                end

                -- Add metadata
                server.call('HSET', config_key, '_seeded_at', tostring(server.call('TIME')[1]))
                server.call('HSET', config_key, '_seeded_by', 'GNODE_CONFIG_SEED')

                categories_seeded = categories_seeded + 1
            end
        end

        track_config_metric(site_id, 'seed', 'all')

        return categories_seeded
    end,
    flags = {}
}

-- ============================================================================
-- GNODE_CONFIG_EXPORT: Export all config as JSON
-- ============================================================================
-- Args: site_id
-- Returns: JSON string with all categories

server.register_function{
    function_name = 'GNODE_CONFIG_EXPORT',
    callback = function(keys, args)
        if not args[1] then
            return server.error_reply("Usage: GNODE_CONFIG_EXPORT site_id")
        end

        local site_id = args[1]
        local export = {}

        -- Export each known category
        for category, defaults in pairs(DEFAULTS) do
            local config_key = build_config_key(site_id, category)
            local values = {}

            -- Start with defaults
            for k, v in pairs(defaults) do
                values[k] = v
            end

            -- Overlay site values
            local site_values = server.call('HGETALL', config_key)
            if site_values then
                for i = 1, #site_values, 2 do
                    local k = site_values[i]
                    local v = site_values[i+1]
                    if k and not k:match('^_') then
                        values[k] = v
                    end
                end
            end

            export[category] = values
        end

        -- Convert to JSON using cjson (JSON.STRINGIFY is not a valid ValKey command)
        local success, json = pcall(cjson.encode, export)

        if success and json then
            return json
        else
            return server.error_reply("JSON encoding failed")
        end
    end,
    flags = {'no-writes'}
}

-- ============================================================================
-- GNODE_CONFIG_LIST_CATEGORIES: List all known config categories
-- ============================================================================
-- Returns: Array of category names

server.register_function{
    function_name = 'GNODE_CONFIG_LIST_CATEGORIES',
    callback = function(keys, args)
        local categories = {}
        for category, _ in pairs(DEFAULTS) do
            table.insert(categories, category)
        end
        table.sort(categories)
        return categories
    end,
    flags = {'no-writes'}
}

-- ============================================================================
-- GNODE_CONFIG_GET_DEFAULTS: Get default values for a category
-- ============================================================================
-- Args: category
-- Returns: Flat array of [key, value, ...]

server.register_function{
    function_name = 'GNODE_CONFIG_GET_DEFAULTS',
    callback = function(keys, args)
        if not args[1] then
            return server.error_reply("Usage: GNODE_CONFIG_GET_DEFAULTS category")
        end

        local category = args[1]

        if not DEFAULTS[category] then
            return server.error_reply("Unknown config category: " .. category)
        end

        local flat = {}
        for k, v in pairs(DEFAULTS[category]) do
            table.insert(flat, k)
            table.insert(flat, v)
        end

        return flat
    end,
    flags = {'no-writes'}
}

-- ============================================================================
-- CONSTELLATION GENERATION
-- ============================================================================
-- Monotonic counter for distributed config staleness detection.
-- APCu caches store their generation — if it doesn't match, the cache is stale.
-- One INCR per config change, one GET per request (cached per-request in PHP).
-- ============================================================================

--- Increment constellation generation + broadcast config_updated
-- Called by compile-config.php or admin config changes.
-- @param args[1] site_id
-- @param args[2] version_hash (optional)
-- @return New generation value (integer)
server.register_function{
    function_name = 'GNODE_CONSTELLATION_GENERATION_INCR',
    callback = function(keys, args)
        if not args[1] then
            return server.error_reply("Usage: GNODE_CONSTELLATION_GENERATION_INCR site_id [version_hash]")
        end

        local site_id = args[1]
        local version_hash = args[2] or ''

        -- Atomic increment
        local gen_key = '{' .. site_id .. '}:constellation:generation'
        local new_gen = server.call('INCR', gen_key)

        -- Broadcast config_updated (within site ACL scope)
        local broadcast_key = '{' .. site_id .. '}:constellation:broadcast'
        local time_result = server.call('TIME')
        local ts = tostring(tonumber(time_result[1]) * 1000 + math.floor(tonumber(time_result[2]) / 1000))

        server.call('XADD', broadcast_key, 'MAXLEN', '~', 100, '*',
            't', 'config_updated',
            'ss', site_id,
            'gen', tostring(new_gen),
            'vh', version_hash,
            'ts', ts
        )

        track_config_metric(site_id, 'generation_incr', 'constellation')

        return new_gen
    end,
    description = 'Atomically increment constellation generation and broadcast config_updated event'
}

--- Get current constellation generation
-- @param args[1] site_id
-- @return Current generation (integer), 0 if not set
server.register_function{
    function_name = 'GNODE_CONSTELLATION_GENERATION_GET',
    callback = function(keys, args)
        if not args[1] then
            return server.error_reply("Usage: GNODE_CONSTELLATION_GENERATION_GET site_id")
        end

        local site_id = args[1]
        local gen_key = '{' .. site_id .. '}:constellation:generation'
        local gen = server.call('GET', gen_key)

        if gen then
            return tonumber(gen) or 0
        end

        return 0
    end,
    flags = {'no-writes'},
    description = 'Get current constellation generation for staleness detection'
}
