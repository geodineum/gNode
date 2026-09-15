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
--   - geometric_discover: GNODE_TOPO_GET_ENTITIES / GNODE_TOPO_QUERY_VOXEL + Rust ranking
--   - geometric_distance: Rust Q64.64 euclidean distance
--   - register_service: GNODE_REGISTER_CAPABILITY_VECTOR with pre-computed bucket keys
--
-- This library only names the dimensions, and reads them from the schema the
-- daemon published at startup ({ns}:gnode:schema:<tier>) instead of carrying
-- a copy of its own.
--

--
-- GNODE_GEOMETRIC_GET_DIMENSIONS
-- Dimension names of a published tier schema, in index order.
-- Usage: FCALL_RO GNODE_GEOMETRIC_GET_DIMENSIONS 0 [topology_ns] [tier]
--
server.register_function{
    function_name = 'GNODE_GEOMETRIC_GET_DIMENSIONS',
    callback = function(keys, args)
        local ns = args[1]
        if not ns or ns == '' then ns = 'geodineum' end
        local tier = args[2]
        if not tier or tier == '' then tier = 'service' end

        local key = '{' .. ns .. '}:gnode:schema:' .. tier
        local index_json = server.call('HGET', key, 'dimension_index')
        if not index_json then
            return server.error_reply("No schema published at " .. key)
        end

        local ok, index = pcall(cjson.decode, index_json)
        if not ok or type(index) ~= 'table' then
            return server.error_reply("Unreadable dimension_index at " .. key)
        end

        local names, count = {}, 0
        for name, position in pairs(index) do
            names[position + 1] = name
            count = count + 1
        end
        for position = 1, count do
            if not names[position] then
                return server.error_reply("dimension_index at " .. key .. " has no dimension at index " .. (position - 1))
            end
        end

        return cjson.encode(names)
    end,
    flags = {'no-writes'},
    description = 'Dimension names of the published tier schema, in index order'
}
