#!lua name=gnode_topology

--
-- gNode TOPOLOGY Functions — dimension schema + service load metrics
--
-- The live geometric Service Topology engine (register / discover / voxel) is
-- gnode_topo.lua. This library now provides only:
--   GNODE_TOPOLOGY_GET_SCHEMA / GET_FULL_SCHEMA — the capability-dimension
--     schema + named values (consumed by the wp-admin topology viewer), and
--   GNODE_TOPOLOGY_BATCH_UPDATE_LOAD — bulk service load updates (daemon health).
--
-- The legacy string-blob semantic-discovery family (DISCOVER / BY_DOMAIN /
-- DESCRIBE_* / …) read a model the daemon stopped populating and has been
-- removed. The user-defined custom-topology builder (the multi-dimension
-- family) is a premium concern and lives in the gNode-TOPO extension, not base.
--
-- The DIMENSIONS / VALUES / QUERY_TYPES constants and the helpers below are
-- retained because the schema functions read them.
--

-- ============================================================================
-- DIMENSION CONSTANTS
-- Maps dimension names to their 0-based indices
-- ============================================================================

local DIMENSIONS = {
    -- Layer 1: Interface Identity (0-3)
    protocol = 0,
    native_format = 1,
    api_version = 2,
    contract_stability = 3,

    -- Layer 2: Access Control (4-6)
    clearance_required = 4,
    auth_method = 5,
    data_sensitivity = 6,

    -- Layer 3: Service Scope (7)
    service_scope = 7,

    -- Layer 4: Functional Domain (8-10)
    domain_primary = 8,
    domain_secondary = 9,
    specialization = 10,

    -- Layer 5: Performance Profile (11-13)
    throughput_tier = 11,
    latency_class = 12,
    reliability_tier = 13,

    -- Layer 6: Workflow Context (14-15)
    pipeline_stage = 14,
    execution_priority = 15,

    -- Layer 7: Runtime State (16-18) - Dynamic
    current_load = 16,
    health_status = 17,
    lifecycle_state = 18,

    -- Layer 8: Classification (19-21)
    service_tier = 19,
    environment = 20,
    implementation_language = 21,

    -- Layer 9: Network Context (22-24)
    network_zone = 22,
    data_persistence = 23,
    update_channel = 24,

    -- Layer 10: Visual Topology (25-27) - Storage-only
    user_x = 25,
    user_y = 26,
    user_z = 27,

    -- Layer 11: Metadata (28-29) - Storage-only
    deployment_model = 28,
    registration_order = 29
}

-- Total dimensions (30D) — service_schema.yaml v3.0
-- Discovery dims (0-24): used for spatial hash bucket key (100 chars = 25 × 4)
-- Storage-only dims (25-29): visual topology + metadata, queryable via range filters
local TOTAL_DIMENSIONS = 30
local DISCOVERY_DIMENSIONS = 25

-- ============================================================================
-- SEMANTIC VALUE CONSTANTS
-- Named values for each dimension
-- ============================================================================

local VALUES = {
    -- Protocol values
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

    -- Native format values
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

    -- Contract stability values
    contract_stability = {
        experimental = 0.00,
        alpha = 0.25,
        beta = 0.50,
        stable = 0.75,
        frozen = 1.00
    },

    -- Clearance values
    clearance_required = {
        public = 0.00,
        authenticated = 0.20,
        authorized = 0.40,
        privileged = 0.60,
        confidential = 0.80,
        classified = 1.00
    },

    -- Auth method values
    auth_method = {
        none = 0.00,
        api_key = 0.20,
        bearer_token = 0.40,
        session_cookie = 0.60,
        mtls = 0.80,
        hardware_token = 1.00
    },

    -- Data sensitivity values
    data_sensitivity = {
        public_data = 0.00,
        internal = 0.25,
        confidential = 0.50,
        pii = 0.75,
        regulated = 1.00
    },

    -- Service scope values
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

    -- Domain primary values
    domain_primary = {
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

    -- Specialization values
    specialization = {
        platform = 0.00,
        generalist = 0.25,
        focused = 0.50,
        specialist = 0.75,
        single_purpose = 1.00
    },

    -- Throughput tier values
    throughput_tier = {
        minimal = 0.00,
        standard = 0.25,
        professional = 0.50,
        enterprise = 0.75,
        hyperscale = 1.00
    },

    -- Latency class values (LOWER = FASTER)
    latency_class = {
        realtime = 0.00,
        interactive = 0.25,
        responsive = 0.50,
        patient = 0.75,
        batch = 1.00
    },

    -- Reliability tier values
    reliability_tier = {
        best_effort = 0.00,
        standard = 0.25,
        high = 0.50,
        critical = 0.75,
        ultra = 1.00
    },

    -- Pipeline stage values
    pipeline_stage = {
        source = 0.00,
        ingest = 0.20,
        process = 0.40,
        enrich = 0.60,
        deliver = 0.80,
        sink = 1.00
    },

    -- Execution priority values
    execution_priority = {
        background = 0.00,
        low = 0.25,
        normal = 0.50,
        high = 0.75,
        critical = 1.00
    },

    -- Current load values
    current_load = {
        idle = 0.00,
        light = 0.25,
        moderate = 0.50,
        heavy = 0.75,
        saturated = 1.00
    },

    -- Health status values (Layer 7) - Dynamic
    health_status = {
        dead = 0.00,
        degraded = 0.33,
        healthy = 0.67,
        unknown = 1.00
    },

    -- Lifecycle state values (Layer 7) - Dynamic
    lifecycle_state = {
        registering = 0.00,
        active = 0.25,
        draining = 0.50,
        stopped = 0.75,
        failed = 1.00
    },

    -- Service tier values (Layer 8)
    service_tier = {
        undefined = 0.00,
        tool = 0.10,
        service = 0.30,
        pipeline = 0.50,
        infrastructure = 0.70,
        orchestrator = 0.90
    },

    -- Environment values (Layer 8)
    environment = {
        global = 0.00,
        testing = 0.25,
        staging = 0.50,
        acceptance = 0.75,
        production = 1.00
    },

    -- Implementation language values (Layer 8)
    implementation_language = {
        rust = 0.15,
        php = 0.30,
        lua = 0.45,
        python = 0.55,
        go = 0.65,
        javascript = 0.75,
        bash = 0.90
    },

    -- Network zone values (Layer 9)
    network_zone = {
        localhost = 0.00,
        vpn = 0.25,
        internal = 0.50,
        dmz = 0.75,
        public = 1.00
    },

    -- Data persistence values (Layer 9)
    data_persistence = {
        stateless = 0.00,
        ephemeral = 0.33,
        persistent = 0.67,
        replicated = 1.00
    },

    -- Update channel values (Layer 9)
    update_channel = {
        stable = 0.00,
        beta = 0.33,
        canary = 0.67,
        pinned = 1.00
    },

    -- Visual position values (Layer 10) - user-set, continuous 0.0-1.0
    visual_position = {
        left = 0.00,
        center = 0.50,
        right = 1.00
    },

    -- Deployment model values (Layer 11) - Storage-only
    deployment_model = {
        bare_metal = 0.00,
        container = 0.33,
        serverless = 0.67,
        embedded = 1.00
    },

    -- Registration order values (Layer 11) - auto-computed, normalized
    registration_order = {
        first = 0.00,
        early = 0.25,
        middle = 0.50,
        late = 0.75,
        recent = 1.00
    }
}

-- Query types for each dimension
local QUERY_TYPES = {
    protocol = "equality",
    native_format = "informational",
    api_version = "equality",
    contract_stability = "minimum",
    clearance_required = "maximum",
    auth_method = "equality",
    data_sensitivity = "informational",
    service_scope = "range",
    domain_primary = "equality",
    domain_secondary = "equality",
    specialization = "range",
    throughput_tier = "minimum",
    latency_class = "maximum",
    reliability_tier = "minimum",
    pipeline_stage = "range",
    execution_priority = "minimum",
    current_load = "maximum",
    health_status = "minimum",
    lifecycle_state = "equality",
    service_tier = "range",
    environment = "equality",
    implementation_language = "equality",
    network_zone = "equality",
    data_persistence = "equality",
    update_channel = "equality",
    user_x = "range",
    user_y = "range",
    user_z = "range",
    deployment_model = "informational",
    registration_order = "range"
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

-- Batch update load values for multiple services
-- Called from health stream processor for efficiency
server.register_function{
    function_name = 'GNODE_TOPOLOGY_BATCH_UPDATE_LOAD',
    callback = function(keys, args)
        if #keys < 1 then
            return server.error_reply("Missing topology key")
        end
        if #args < 1 then
            return server.error_reply("Missing updates JSON")
        end

        local topology_key = keys[1]
        local updates_json = args[1]

        local updates, err = parse_data(updates_json)
        if not updates then
            return server.error_reply("Invalid updates JSON: " .. (err or "parse error"))
        end

        local topology, top_err = get_topology(topology_key)
        if not topology then
            return server.error_reply(top_err)
        end

        local services = topology.services or {}
        local updated_count = 0
        local not_found = {}

        for service_id, load_value in pairs(updates) do
            local service = services[service_id]
            if service then
                -- Clamp load to [0, 1]
                load_value = math.max(0, math.min(1, tonumber(load_value) or 0))

                if not service.point then
                    service.point = {}
                end
                while #service.point < TOTAL_DIMENSIONS do
                    table.insert(service.point, 0)
                end
                service.point[DIMENSIONS.current_load + 1] = load_value
                updated_count = updated_count + 1
            else
                table.insert(not_found, service_id)
            end
        end

        -- Re-encode and store
        local updated_topology = safe_json_encode(topology)
        server.call('SET', topology_key, updated_topology)

        return safe_json_encode({
            status = "ok",
            updated = updated_count,
            not_found = not_found
        })
    end,
    description = 'Batch updates load values for multiple services'
}

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
