#!lua name=gnode_topology

--
-- gNode TOPOLOGY Functions — dimension schema + service load metrics
--
-- The live geometric Service Topology engine (register / discover / voxel) is
-- gnode_topo.lua. This library now provides only:
--   GNODE_TOPOLOGY_GET_SCHEMA / GET_FULL_SCHEMA — the capability-dimension
--     schema + named values (consumed by the wp-admin topology viewer).
--
-- GNODE_TOPOLOGY_BATCH_UPDATE_LOAD is gone: it wrote dimension 16 into
-- topology.services inside a JSON blob at the topology key — a model the live
-- store does not have (TYPE none on every site), so every load update it ever
-- received failed as "Topology not found" and was logged at trace. Derived axes
-- are written into the canonical (C) entities by GNODE_TOPO_SET_DERIVED.
--
-- The legacy string-blob semantic-discovery family (DISCOVER / BY_DOMAIN /
-- DESCRIBE_* / …) read a model the daemon stopped populating and has been
-- removed. The user-defined custom-topology builder (the multi-dimension
-- family) is a premium concern and lives in the gNode-TOPO extension, not base.
--
-- The DIMENSIONS / VALUES / QUERY_TYPES constants and the helpers below are
-- retained because the schema functions read them.
--

-- GENERATED FROM config/service_schema.yaml v4.0 — do not hand-edit.
-- assert_lua_dimension_constants() fails daemon startup if these disagree
-- with the schema, because three hand-kept copies of one map is how the
-- 23-D and 30-D layouts came to coexist.
local DIMENSIONS = {
    -- declared, hashed
    protocol = 0,
    api_version = 1,
    contract_stability = 2,
    clearance_required = 3,
    auth_method = 4,
    data_sensitivity = 5,
    service_scope = 6,
    domain_primary = 7,
    domain_secondary = 8,
    specialization = 9,
    throughput_tier = 10,
    latency_class = 11,
    reliability_tier = 12,
    pipeline_stage = 13,
    execution_priority = 14,
    environment = 15,
    -- derived, ranked, never hashed
    current_load = 16,
    health_status = 17,
    lifecycle_state = 18,
    -- storage
    native_format = 19,
    implementation_language = 20,
    data_persistence = 21,
    service_tier = 22
}

-- Two cuts, both prefix truncations: 0..15 are hashed into the bucket key
-- (64 chars = 16 x 4), 16..18 are derived and ranked but never hashed,
-- 19..22 are stored and refused in a query.
local TOTAL_DIMENSIONS = 23
local DISCOVERY_DIMENSIONS = 19
local HASHED_DIMENSIONS = 16

-- ============================================================================
-- SEMANTIC VALUE CONSTANTS
-- Named values for each dimension
-- ============================================================================

local VALUES = {
    protocol = {
        undefined = 0.00,
        http_rest = 0.10,
        graphql = 0.20,
        grpc = 0.30,
        websocket = 0.40,
        gnode_stream = 0.50,
        resp3_direct = 0.60,
        amqp = 0.70,
        kafka = 0.80,
        custom_tcp = 0.90
    },
    api_version = {
        undefined = 0.00,
        v1 = 0.10,
        v2 = 0.20,
        v3 = 0.30,
        v4 = 0.40,
        v5 = 0.50
    },
    contract_stability = {
        experimental = 0.00,
        alpha = 0.25,
        beta = 0.50,
        stable = 0.75,
        frozen = 1.00
    },
    clearance_required = {
        public = 0.00,
        authenticated = 0.20,
        authorized = 0.40,
        privileged = 0.60,
        confidential = 0.80,
        classified = 1.00
    },
    auth_method = {
        none = 0.00,
        api_key = 0.20,
        bearer_token = 0.40,
        session_cookie = 0.60,
        mtls = 0.80,
        hardware_token = 1.00
    },
    data_sensitivity = {
        public_data = 0.00,
        internal = 0.25,
        confidential = 0.50,
        pii = 0.75,
        regulated = 1.00
    },
    service_scope = {
        infrastructure = 0.00,
        daemon = 0.15,
        worker = 0.30,
        cron_scheduled = 0.45,
        internal_api = 0.60,
        bff = 0.75,
        client_facing = 0.90,
        edge = 1.00
    },
    domain_primary = {
        undefined = 0.00,
        platform = 0.05,
        identity = 0.10,
        configuration = 0.15,
        storage = 0.20,
        cache = 0.25,
        compute = 0.30,
        transform = 0.35,
        messaging = 0.40,
        workflow = 0.45,
        template = 0.50,
        content = 0.55,
        gateway = 0.60,
        integration = 0.65,
        analytics = 0.70,
        logging = 0.75,
        ml_inference = 0.80,
        search = 0.85,
        notification = 0.90,
        presentation = 0.95
    },
    domain_secondary = {
        undefined = 0.00,
        platform = 0.05,
        identity = 0.10,
        configuration = 0.15,
        storage = 0.20,
        cache = 0.25,
        compute = 0.30,
        transform = 0.35,
        messaging = 0.40,
        workflow = 0.45,
        template = 0.50,
        content = 0.55,
        gateway = 0.60,
        integration = 0.65,
        analytics = 0.70,
        logging = 0.75,
        ml_inference = 0.80,
        search = 0.85,
        notification = 0.90,
        presentation = 0.95
    },
    specialization = {
        platform = 0.00,
        generalist = 0.25,
        focused = 0.50,
        specialist = 0.75,
        single_purpose = 1.00
    },
    throughput_tier = {
        minimal = 0.00,
        standard = 0.25,
        professional = 0.50,
        enterprise = 0.75,
        hyperscale = 1.00
    },
    latency_class = {
        realtime = 0.00,
        interactive = 0.25,
        responsive = 0.50,
        patient = 0.75,
        batch = 1.00
    },
    reliability_tier = {
        best_effort = 0.00,
        standard = 0.25,
        high = 0.50,
        critical = 0.75,
        ultra = 1.00
    },
    pipeline_stage = {
        source = 0.00,
        ingest = 0.20,
        process = 0.40,
        enrich = 0.60,
        deliver = 0.80,
        sink = 1.00
    },
    execution_priority = {
        background = 0.00,
        low = 0.25,
        normal = 0.50,
        high = 0.75,
        critical = 1.00
    },
    environment = {
        global = 0.00,
        testing = 0.25,
        staging = 0.50,
        acceptance = 0.75,
        production = 1.00
    },
    current_load = {
        unknown = 0.00,
        idle = 0.20,
        light = 0.40,
        moderate = 0.60,
        heavy = 0.80,
        saturated = 1.00
    },
    health_status = {
        unknown = 0.00,
        dead = 0.33,
        degraded = 0.67,
        healthy = 1.00
    },
    lifecycle_state = {
        registering = 0.00,
        active = 0.25,
        draining = 0.50,
        stopped = 0.75,
        failed = 1.00
    },
    native_format = {
        undefined = 0.00,
        plaintext = 0.10,
        json = 0.20,
        xml = 0.30,
        yaml = 0.40,
        msgpack = 0.50,
        protobuf = 0.60,
        cbor = 0.70,
        resp3 = 0.80,
        custom_binary = 0.90
    },
    implementation_language = {
        undefined = 0.00,
        rust = 0.15,
        php = 0.30,
        lua = 0.45,
        python = 0.55,
        go = 0.65,
        javascript = 0.75,
        bash = 0.90
    },
    data_persistence = {
        stateless = 0.00,
        ephemeral = 0.33,
        persistent = 0.67,
        replicated = 1.00
    },
    service_tier = {
        tool = 0.10,
        service = 0.30,
        pipeline = 0.50,
        infrastructure = 0.70,
        orchestrator = 0.90
    }
}

-- Query types for each dimension, verbatim from the schema. The AXIS-ROLES
-- engine maps these to roles (equality->category, range->at_least/at_most/near,
-- proximity->category, informational->refused in a query).
local QUERY_TYPES = {
    protocol = "equality",
    api_version = "equality",
    contract_stability = "range",
    clearance_required = "range",
    auth_method = "equality",
    data_sensitivity = "range",
    service_scope = "equality",
    domain_primary = "proximity",
    domain_secondary = "proximity",
    specialization = "range",
    throughput_tier = "range",
    latency_class = "range",
    reliability_tier = "range",
    pipeline_stage = "equality",
    execution_priority = "range",
    environment = "equality",
    current_load = "range",
    health_status = "equality",
    lifecycle_state = "equality",
    native_format = "informational",
    implementation_language = "informational",
    data_persistence = "informational",
    service_tier = "informational"
}

-- ============================================================================
-- HELPER FUNCTIONS
-- ============================================================================

-- Safe JSON encode (P2CF001 fix)
local function safe_json_encode(value)
    local ok, result = pcall(cjson.encode, value)
    if ok then
        return result
    else
        return '{"error":"encode_error"}'
    end
end

-- Safe JSON decode (P2CF001 fix)
local function safe_json_decode(json_str)
    if not json_str or json_str == "" then
        return nil, "Empty or nil JSON string"
    end
    local ok, result = pcall(cjson.decode, json_str)
    if ok then
        return result, nil
    else
        return nil, "JSON decode error: " .. tostring(result)
    end
end

-- Parse JSON or MessagePack data
local function parse_data(data_str)
    if type(data_str) == "table" then
        return data_str, nil
    end

    local ok, result = pcall(function()
        return cjson.decode(data_str)
    end)

    if ok then
        return result, nil
    end

    ok, result = pcall(function()
        return cmsgpack.unpack(data_str)
    end)

    if ok then
        return result, nil
    end

    return nil, "Failed to parse data as JSON or MessagePack"
end

-- Get topology from ValKey
local function get_topology(topology_key)
    local topology_data = server.call('GET', topology_key)
    if not topology_data then
        return nil, "Topology not found at key: " .. topology_key
    end

    return parse_data(topology_data)
end

-- Get dimension schema (returns the dimension definitions)
server.register_function{
    function_name = 'GNODE_TOPOLOGY_GET_SCHEMA',
    callback = function(keys, args)
        return safe_json_encode({
            total_dimensions = TOTAL_DIMENSIONS,
            dimensions = DIMENSIONS,
            values = VALUES,
            query_types = QUERY_TYPES
        })
    end,
    flags = {'no-writes'},
    description = 'Returns the semantic dimension schema'
}

-- List all services with their semantic coordinates

-- Find services that support a specific format
-- Used for format-aware service discovery (gNode as format translator)

-- Register service format info in topology metadata
-- Called when a service registers its format capabilities

-- Get the dimension schema for developers
-- Returns the full schema with dimension names, indices, and valid values
server.register_function{
    function_name = 'GNODE_TOPOLOGY_GET_FULL_SCHEMA',
    callback = function(keys, args)
        local schema = {
            total_dimensions = TOTAL_DIMENSIONS,
            dimensions = {},
            query_types = QUERY_TYPES
        }

        -- Build dimension info with valid values
        for dim_name, dim_index in pairs(DIMENSIONS) do
            local dim_info = {
                name = dim_name,
                index = dim_index,
                query_type = QUERY_TYPES[dim_name] or "equality",
                values = {}
            }

            -- Add valid values if they exist
            if VALUES[dim_name] then
                for value_name, value_num in pairs(VALUES[dim_name]) do
                    dim_info.values[value_name] = value_num
                end
            end

            schema.dimensions[dim_name] = dim_info
        end

        return safe_json_encode(schema)
    end,
    flags = {'no-writes'},
    description = 'Returns the full dimension schema with valid values for each dimension'
}

-- ---------------------------------------------------------------------------
-- Canonical capability-schema lookup
-- ---------------------------------------------------------------------------
-- The dimension count is per TIER, not one global number: service 30, tool 16,
-- constellation and galaxy 20, each with its own discovery subset. That is
-- deliberate. What was not deliberate is that every consumer kept its own copy
-- of the answer — counts of 8, 9, 12, 16, 19, 23 and 25 were all present in the
-- tree at once, several stale by months, and nothing could say which was true.
--
-- The daemon publishes the active schema it actually loaded; this reads it back.
-- A caller that asks gets the same number the daemon is matching against, by
-- construction. A caller that hardcodes can only be wrong silently.
--
-- Returns the schema hash for the tier, including `dimension_index` — the
-- name→index map. The count alone is not enough to build a coordinate vector;
-- the index each named capability occupies is the part that actually drifts.
--
-- Usage: FCALL_RO GNODE_SCHEMA_GET 0 <topology_ns> [tier]
server.register_function{
    function_name = 'GNODE_SCHEMA_GET',
    callback = function(keys, args)
        if not args[1] or args[1] == '' then
            return server.error_reply("Topology namespace required")
        end

        local ns = args[1]
        local tier = args[2]
        if not tier or tier == '' then tier = 'service' end

        local key = '{' .. ns .. '}:gnode:schema:' .. tier
        local flat = server.call('HGETALL', key)

        -- Absent is a real answer, not an error: the daemon may not have
        -- published yet on a cold start. The caller decides whether to wait or
        -- fall back, and can tell "not published" from "wrong tier" because the
        -- key it looked for is named in the reply.
        if not flat or #flat == 0 then
            return server.error_reply(
                "No schema published at " .. key ..
                " — the master publishes it at startup; is the daemon running and is the tier correct?")
        end

        return flat
    end,
    flags = {'no-writes'},
    description = 'Reads the canonical capability schema (dimension counts + name->index map) for a tier'
}
