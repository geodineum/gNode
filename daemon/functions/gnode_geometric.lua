#!lua name=gnode_geometric

--
-- gNode GEOMETRIC Functions
-- A ValKey function library for geometric operations
--
-- All geometric calculations (distance, bucket keys) are performed in Rust
-- using Q64.64 fixed-point arithmetic (g_math crate) for deterministic
-- cross-node results.
--
-- Geometric operations are handled via unified stream commands:
--   - geometric_discover: GNODE_TOPO_QUERY_VOXEL + Rust distance ranking
--   - geometric_distance: Rust Q64.64 euclidean distance
--   - registerService: GNODE_REGISTER_CAPABILITY_VECTOR with pre-computed bucket keys
--
-- This library provides dimension metadata only. The dim count returned by
-- GNODE_GEOMETRIC_GET_DIMENSIONS comes from the active service-tier schema
-- (default 30D = 25 discovery + 5 storage; see daemon/config/service_schema.yaml).
-- Other tiers (tool/constellation/galaxy) and custom topologies (created via
-- topo_create / gNode-TOPO) load their dim metadata from their own schemas;
-- this library does not speak for them.
--

-- Helper: safe JSON encode
local function safe_json_encode(v)
    local ok, result = pcall(cjson.encode, v)
    if ok then return result end
    return nil
end

--
-- GNODE_GEOMETRIC_GET_DIMENSIONS
-- Get capability dimensions from topology metadata
-- NOTE: Returns configured dimensions from the stateless topology system
--
server.register_function{
    function_name = 'GNODE_GEOMETRIC_GET_DIMENSIONS',
    callback = function(keys, args)
        -- Return standard 23-dimension capability space
        -- This is now statically configured, not read from blob
        -- Dims 0-18: discovery (used for bucket key hashing)
        -- Dims 19-22: storage-only (visual topology + temporal)
        local dimensions = {
            "protocol", "native_format", "api_version", "contract_stability",
            "clearance_required", "auth_method", "data_sensitivity",
            "service_scope",
            "domain_primary", "domain_secondary", "specialization",
            "throughput_tier", "latency_class", "reliability_tier",
            "pipeline_stage", "execution_priority",
            "current_load",
            "service_tier", "environment",
            "user_x", "user_y", "user_z",
            "registration_order"
        }

        local result = safe_json_encode(dimensions)
        if not result then
            return server.error_reply("Failed to encode dimensions")
        end
        return result
    end,
    flags = {'no-writes'},
    description = 'Gets capability dimensions (23D stateless topology)'
}
