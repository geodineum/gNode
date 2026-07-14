#!lua name=gnode_gcore_config

--[[
gCore Manager Configuration Functions
=====================================

ValKey function library for gCore manager-config homogenization. Mirrors
the gnode_config.lua precedent but scopes keys per gCore manager
(CacheManager, ResourceManager, ErrorManager, etc.) rather than per
gNode category.

Key Schema:
  {site_id}:gcore:config:{Manager}      → HASH, per-site override
  {default}:gcore:config:{Manager}      → HASH, global default (bootloader seed)
  {site_id}:gcore:secrets:{Manager}     → HASH, per-site secrets (ACL-restricted)
  {default}:gcore:secrets:{Manager}     → HASH, global secrets

Lookup chain (for any read):
  1. {site_id}:gcore:config:{Manager} HGET {key}
  2. {default}:gcore:config:{Manager} HGET {key}
  3. caller-supplied default (passed as arg)
  4. nil

Field naming:
  - Nested config keys use dot-notation: "cors.allowed_origins",
    "rate_limit.requests", "storage.host".
  - Arrays / objects are JSON-encoded strings inside the HASH.
  - Booleans stored as "true" or "false" string — NOT "1"/"0" (avoids
    bool-vs-int decode ambiguity; PHP ManagerConfigTrait encodes/decodes
    the same way). Lua tostring(true) also produces "true".
  - Numbers stored as numeric strings.

Usage:
  FCALL GCORE_MGR_CONFIG_GET 0 example_site CacheManager default_ttl 3600
  FCALL GCORE_MGR_CONFIG_SET 0 example_site CacheManager default_ttl 7200
  FCALL GCORE_MGR_CONFIG_HGETALL 0 example_site CacheManager
  FCALL GCORE_MGR_CONFIG_SEED 0 default CacheManager '{"default_ttl":"3600","prefix":"cache_"}'
  FCALL GCORE_MGR_CONFIG_DELETE 0 example_site CacheManager default_ttl

  -- Secrets (separate keyspace, ACL-restricted via gcore_secrets_rw_<site>)
  FCALL GCORE_MGR_SECRETS_GET 0 example_site SecurityManager jwt_secret
  FCALL GCORE_MGR_SECRETS_SET 0 example_site SecurityManager jwt_secret <value>
  FCALL GCORE_MGR_SECRETS_SEED 0 default CommsManager '{"email.config.smtp_pass":"..."}'

Versioning:
  Each SET / SEED / DELETE bumps {site_id}:gcore:config:{Manager}:version
  (monotonic counter). PHP-side managers can check this to invalidate
  cached config without polling every key.

Version: 1.0.0
Companion to: gnode_config.lua (gNode-side runtime config)
]]

-- ============================================================================
-- HELPER FUNCTIONS
-- ============================================================================

-- Build per-site config key with hash-tag for cluster routing.
local function build_config_key(site_id, manager)
    return '{' .. site_id .. '}:gcore:config:' .. manager
end

-- Build per-site secrets key (separate keyspace for ACL isolation).
local function build_secrets_key(site_id, manager)
    return '{' .. site_id .. '}:gcore:secrets:' .. manager
end

-- Build global-default key (manager-namespaced).
local function build_default_config_key(manager)
    return '{default}:gcore:config:' .. manager
end

local function build_default_secrets_key(manager)
    return '{default}:gcore:secrets:' .. manager
end

-- Bump version counter for invalidation tracking.
local function bump_version(config_key)
    server.call('INCR', config_key .. ':version')
end

-- Track access metrics (best-effort — never let a metrics write
-- fail the calling FCALL function). Uses pcall so ACL restrictions
-- on the metrics keyspace don't break config reads/writes.
local function track_mgr_metric(site_id, manager, operation)
    pcall(function()
        local metrics_key = '{' .. site_id .. '}:metrics:gcore_config'
        server.call('HINCRBY', metrics_key, operation .. ':' .. manager, 1)
    end)
end

-- Validate non-empty string args.
local function require_args(args, count, usage)
    for i = 1, count do
        if not args[i] or args[i] == '' then
            return server.error_reply("Usage: " .. usage)
        end
    end
    return nil
end

-- ============================================================================
-- GCORE_MGR_CONFIG_GET: Single-key lookup with fallback chain
-- ============================================================================
-- Args: site_id, manager, key, [default]
-- Returns: String value, or default arg, or nil

server.register_function{
    function_name = 'GCORE_MGR_CONFIG_GET',
    callback = function(keys, args)
        local err = require_args(args, 3, 'GCORE_MGR_CONFIG_GET site_id manager key [default]')
        if err then return err end

        local site_id = args[1]
        local manager = args[2]
        local key = args[3]
        local caller_default = args[4]

        -- 1. Per-site override
        local site_key = build_config_key(site_id, manager)
        local value = server.call('HGET', site_key, key)
        if value then
            track_mgr_metric(site_id, manager, 'get_hit_site')
            return value
        end

        -- 2. Global default
        local default_key = build_default_config_key(manager)
        value = server.call('HGET', default_key, key)
        if value then
            track_mgr_metric(site_id, manager, 'get_hit_default')
            return value
        end

        -- 3. Caller-supplied default
        track_mgr_metric(site_id, manager, 'get_miss')
        if caller_default then
            return caller_default
        end

        return nil
    end
}

-- ============================================================================
-- GCORE_MGR_CONFIG_HGETALL: Merged view (site override OVERLAID on default)
-- ============================================================================
-- Args: site_id, manager
-- Returns: Flat array [field1, value1, field2, value2, ...] suitable for
--          PHP Redis::hGetAll consumption.
--
-- Used by manager initialize() — one round trip to fetch the full
-- effective config (defaults + site overrides) at boot.

server.register_function{
    function_name = 'GCORE_MGR_CONFIG_HGETALL',
    callback = function(keys, args)
        local err = require_args(args, 2, 'GCORE_MGR_CONFIG_HGETALL site_id manager')
        if err then return err end

        local site_id = args[1]
        local manager = args[2]

        local default_key = build_default_config_key(manager)
        local site_key = build_config_key(site_id, manager)

        -- Start with global defaults
        local merged = {}
        local default_pairs = server.call('HGETALL', default_key)
        for i = 1, #default_pairs, 2 do
            merged[default_pairs[i]] = default_pairs[i + 1]
        end

        -- Overlay per-site overrides
        local site_pairs = server.call('HGETALL', site_key)
        for i = 1, #site_pairs, 2 do
            merged[site_pairs[i]] = site_pairs[i + 1]
        end

        -- Flatten back to [k1, v1, k2, v2, ...] for response
        local flat = {}
        for k, v in pairs(merged) do
            table.insert(flat, k)
            table.insert(flat, v)
        end

        track_mgr_metric(site_id, manager, 'hgetall')
        return flat
    end
}

-- ============================================================================
-- GCORE_MGR_CONFIG_SET: Write a per-site override
-- ============================================================================
-- Args: site_id, manager, key, value
-- Returns: 1 if newly set, 0 if updated (matches HSET semantics)

server.register_function{
    function_name = 'GCORE_MGR_CONFIG_SET',
    callback = function(keys, args)
        local err = require_args(args, 4, 'GCORE_MGR_CONFIG_SET site_id manager key value')
        if err then return err end

        local site_id = args[1]
        local manager = args[2]
        local key = args[3]
        local value = args[4]

        local target_key = build_config_key(site_id, manager)
        local result = server.call('HSET', target_key, key, value)
        bump_version(target_key)
        track_mgr_metric(site_id, manager, 'set')

        return result
    end
}

-- ============================================================================
-- GCORE_MGR_CONFIG_SEED: Bulk write (used by installer bootloader)
-- ============================================================================
-- Args: site_id, manager, json_object, [mode = "NX" | "OVERWRITE"]
-- Returns: Number of fields written.
--
-- Modes:
--   NX (default)  — only write fields that don't already exist (idempotent
--                   bootloader behaviour: rerunning a clean install doesn't
--                   clobber operator-set values).
--   OVERWRITE     — unconditionally HSET every field (use for explicit
--                   config rotation / re-seeding).

server.register_function{
    function_name = 'GCORE_MGR_CONFIG_SEED',
    callback = function(keys, args)
        local err = require_args(args, 3, 'GCORE_MGR_CONFIG_SEED site_id manager json [mode=NX|OVERWRITE]')
        if err then return err end

        local site_id = args[1]
        local manager = args[2]
        local json_str = args[3]
        local mode = args[4] or 'NX'

        local ok, parsed = pcall(cjson.decode, json_str)
        if not ok or type(parsed) ~= 'table' then
            return server.error_reply("Invalid JSON object: " .. tostring(parsed))
        end

        local target_key = build_config_key(site_id, manager)
        local written = 0

        for k, v in pairs(parsed) do
            local value_str
            if type(v) == 'table' then
                value_str = cjson.encode(v)
            else
                value_str = tostring(v)
            end

            local should_write
            if mode == 'OVERWRITE' then
                should_write = true
            else
                should_write = (server.call('HEXISTS', target_key, k) == 0)
            end

            if should_write then
                server.call('HSET', target_key, k, value_str)
                written = written + 1
            end
        end

        if written > 0 then
            bump_version(target_key)
        end
        track_mgr_metric(site_id, manager, 'seed_' .. string.lower(mode))

        return written
    end
}

-- ============================================================================
-- GCORE_MGR_CONFIG_DELETE: Remove a per-site override (fall back to default)
-- ============================================================================
-- Args: site_id, manager, key
-- Returns: 1 if removed, 0 if no override existed

server.register_function{
    function_name = 'GCORE_MGR_CONFIG_DELETE',
    callback = function(keys, args)
        local err = require_args(args, 3, 'GCORE_MGR_CONFIG_DELETE site_id manager key')
        if err then return err end

        local site_id = args[1]
        local manager = args[2]
        local key = args[3]

        local target_key = build_config_key(site_id, manager)
        local result = server.call('HDEL', target_key, key)

        if result > 0 then
            bump_version(target_key)
        end
        track_mgr_metric(site_id, manager, 'delete')

        return result
    end
}

-- ============================================================================
-- GCORE_MGR_CONFIG_VERSION: Return current version counter
-- ============================================================================
-- Used by PHP-side managers to detect "config changed since I cached it"
-- without polling every key.
-- Args: site_id, manager
-- Returns: Integer version (0 if never written)

server.register_function{
    function_name = 'GCORE_MGR_CONFIG_VERSION',
    callback = function(keys, args)
        local err = require_args(args, 2, 'GCORE_MGR_CONFIG_VERSION site_id manager')
        if err then return err end

        local site_id = args[1]
        local manager = args[2]

        local target_key = build_config_key(site_id, manager)
        local v = server.call('GET', target_key .. ':version')
        return tonumber(v) or 0
    end
}

-- ============================================================================
-- GCORE_MGR_SECRETS_GET: Single-key lookup on the secrets keyspace
-- ============================================================================
-- Same fallback chain as CONFIG_GET, but reads {site}:gcore:secrets:{Mgr}.
-- Intended to be ACL-restricted: only gcore_secrets_rw_<site> + the
-- manager's own ACL user should hold read access.
--
-- INTENTIONALLY NO track_mgr_metric() call here. Tracking secret
-- access in the broadly-readable {site}:metrics:gcore_config hash
-- would leak access patterns (observers would see "SecurityManager.
-- jwt_secret read N times"). If audit logging for secrets access is
-- needed, route it to a dedicated audit stream (XADD to an ACL-
-- restricted audit stream), not to the metrics hash.

server.register_function{
    function_name = 'GCORE_MGR_SECRETS_GET',
    callback = function(keys, args)
        local err = require_args(args, 3, 'GCORE_MGR_SECRETS_GET site_id manager key [default]')
        if err then return err end

        local site_id = args[1]
        local manager = args[2]
        local key = args[3]
        local caller_default = args[4]

        local site_key = build_secrets_key(site_id, manager)
        local value = server.call('HGET', site_key, key)
        if value then
            return value
        end

        local default_key = build_default_secrets_key(manager)
        value = server.call('HGET', default_key, key)
        if value then
            return value
        end

        if caller_default then
            return caller_default
        end
        return nil
    end
}

-- ============================================================================
-- GCORE_MGR_SECRETS_SET / SEED: Mirror of CONFIG variants on secrets keyspace
-- ============================================================================

server.register_function{
    function_name = 'GCORE_MGR_SECRETS_SET',
    callback = function(keys, args)
        local err = require_args(args, 4, 'GCORE_MGR_SECRETS_SET site_id manager key value')
        if err then return err end

        local target_key = build_secrets_key(args[1], args[2])
        local result = server.call('HSET', target_key, args[3], args[4])
        bump_version(target_key)
        return result
    end
}

server.register_function{
    function_name = 'GCORE_MGR_SECRETS_SEED',
    callback = function(keys, args)
        local err = require_args(args, 3, 'GCORE_MGR_SECRETS_SEED site_id manager json [mode=NX|OVERWRITE]')
        if err then return err end

        local json_str = args[3]
        local mode = args[4] or 'NX'

        local ok, parsed = pcall(cjson.decode, json_str)
        if not ok or type(parsed) ~= 'table' then
            return server.error_reply("Invalid JSON object: " .. tostring(parsed))
        end

        local target_key = build_secrets_key(args[1], args[2])
        local written = 0

        for k, v in pairs(parsed) do
            local value_str
            if type(v) == 'table' then
                value_str = cjson.encode(v)
            else
                value_str = tostring(v)
            end

            local should_write
            if mode == 'OVERWRITE' then
                should_write = true
            else
                should_write = (server.call('HEXISTS', target_key, k) == 0)
            end

            if should_write then
                server.call('HSET', target_key, k, value_str)
                written = written + 1
            end
        end

        if written > 0 then
            bump_version(target_key)
        end
        return written
    end
}
