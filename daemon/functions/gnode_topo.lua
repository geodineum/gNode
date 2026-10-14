#!lua name=gnode_topo

--
-- gNode Unified Topology Functions (Stateless Persistence Layer)
--
-- This module implements a STATELESS topology persistence layer where:
--   - Daemon computes: bucket_key, z_score using Q32.32 fixed-point arithmetic
--   - Lua stores: entities, edges, indexes in ValKey (single source of truth)
--   - No floating-point math in Lua - all computation done by Rust daemon
--
-- This architecture ensures:
--   - Multi-node determinism (Q32.32 guarantees identical bucket keys)
--   - Zero daemon state (crash recovery is instant)
--   - Single source of truth (ValKey holds everything)
--   - O(1) lookups via ValKey Hash/Set operations
--
-- ============================================================================
-- FIELD ABBREVIATIONS (bandwidth optimization - every byte counts)
-- ============================================================================
-- All JSON field names use abbreviated forms to minimize storage and transfer.
-- This follows the same pattern as health stream metrics (si, l, cpu, etc.)
--
-- TOPOLOGY METADATA:
--   tk  = topology_key      # Full key like "{c}:gnode:topo:<name>"
--   si  = constellation hash tag
--   n   = name              # Human-readable topology name
--   ct  = constraint_type   # none|z_monotonic|bidirectional|custom
--   tt  = topology_type     # custom|dependency|spatial|etc
--   ax  = axis_semantics    # {x:{n,d},y:{n,d},z:{n,d}} axis meanings
--   d   = description       # Optional description text
--   ca  = created_at        # Unix timestamp of creation
--   ua  = updated_at        # Unix timestamp of last update
--   ec  = entity_count      # Number of entities
--   xc  = edge_count        # Number of edges (x for "connection")
--
-- ENTITY DATA:
--   id  = id                # Entity identifier (kept short already)
--   x   = x                 # X coordinate (kept as-is)
--   y   = y                 # Y coordinate (kept as-is)
--   z   = z                 # Z coordinate (kept as-is)
--   bk  = bucket_key        # Pre-computed Q32.32 voxel bucket key
--   zs  = z_score           # Pre-computed Z-score for sorted set
--   ra  = registered_at     # Unix timestamp of registration
--   m   = metadata          # User-provided metadata
--
-- EDGE DATA:
--   f   = from              # Source entity ID
--   t   = to                # Target entity ID
--   zd  = z_delta           # Z difference (from.z - to.z)
--   m   = metadata          # User-provided edge metadata
--
-- RESPONSE FIELDS:
--   ok  = success flag      # Boolean success indicator
--   err = error             # Error message if failed
--   ts  = timestamp         # Unix timestamp
--   cnt = count             # Count of items
--   eid = entity_id         # Single entity ID in response
--   eids = entity_ids       # Array of entity IDs
--   ents = entities         # Map of entity data
--   out = outgoing          # Outgoing edge targets
--   in  = incoming          # Incoming edge sources
--   upd = updated           # Was this an update vs create
--   bd  = by_depth          # Chain results grouped by depth
--   ch  = chain             # Chain traversal result array
--   md  = max_depth         # Maximum depth reached
--   st  = start             # Starting entity ID
--   dir = direction         # outgoing|incoming|both
--   zr  = z_range           # {mn,mx} Z score range object
--   mn  = min               # Minimum value
--   mx  = max               # Maximum value
--   ord = order             # asc|desc sort order
--   off = offset            # Pagination offset
--   req = requested         # Number of items requested
--   fnd = found             # Number of items found
--   mis = missing           # Array of missing IDs
--   xr  = edges_removed     # Number of edges removed
--   ed  = entities_deleted  # Number of entities deleted
--   vd  = voxels_deleted    # Number of voxel keys deleted
--   ex  = exists            # Boolean existence check
--   ocnt = outgoing_count   # Count of outgoing edges
--   icnt = incoming_count   # Count of incoming edges
--   tec = tracked_entity_count  # Tracked entity count
--   txc = tracked_edge_count    # Tracked edge count
--   zoc = z_order_count     # Z-order index count
--   ft  = filter_type       # Type filter applied
--   topos = topologies      # Array of topology summaries
--
-- ============================================================================
-- VALKEY SCHEMA:
--   {c}:gnode:topo:registry              → Hash: topology_id → JSON metadata
--   {c}:gnode:topo:by_type:{type}        → Set: topology_ids of this type
--   {topo_key}:entities                      → Hash: entity_id → JSON entity data
--   {topo_key}:voxel:{bucket_key}            → Set: entity_ids in this voxel
--   {topo_key}:z_order                       → Sorted Set: entity_id with z_score
--   {topo_key}:edges                         → Hash: "from:to" → JSON edge data
--   {topo_key}:out:{entity_id}               → Set: target entity_ids (outgoing)
--   {topo_key}:in:{entity_id}                → Set: source entity_ids (incoming)
--   {topo_key}:meta                          → Hash: topology metadata
--
-- CONSTRAINT TYPES:
--   none         - No edge constraints
--   z_monotonic  - Edges must flow from higher Z to lower Z (DAG)
--   bidirectional - Edges automatically create reverse edges
--   custom       - User-defined constraint (daemon validates)
--

-- ============================================================================
-- CONSTANTS
-- ============================================================================

local CONSTRAINT_TYPES = {
    none = "none",
    z_monotonic = "z_monotonic",
    bidirectional = "bidirectional",
    custom = "custom"
}

-- Service-tier width, recorded in each topology's metadata.
--
-- A local, because Lua function libraries do not share scope — gnode_topology
-- declares its own TOTAL_DIMENSIONS and this file cannot see it. That makes this
-- a second copy by necessity, so it is asserted against the schema rather than
-- trusted: the daemon compares both to service_schema.yaml at startup and
-- refuses to load on disagreement. Changing this without changing the YAML is a
-- startup failure, not a silent drift.
local TOTAL_DIMENSIONS = 23

local REGISTRY_SUFFIX = ":gnode:topo:registry"
local TYPE_INDEX_PREFIX = ":gnode:topo:by_type:"

-- ============================================================================
-- HELPER FUNCTIONS
-- ============================================================================

--- Safe JSON encode with pcall protection
-- @param value any Lua value to encode
-- @return string|nil JSON string on success, nil on failure
-- @return nil|string nil on success, error message on failure
local function safe_json_encode(value)
    local ok, result = pcall(cjson.encode, value)
    if ok then
        return result, nil
    else
        return nil, "JSON encode error: " .. tostring(result)
    end
end

--- Safe JSON decode with pcall protection
-- @param json_str string JSON string to decode
-- @return table|nil Decoded value on success, nil on failure
-- @return nil|string nil on success, error message on failure
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

--- Get current timestamp from ValKey
-- @return number Unix timestamp
local function get_timestamp()
    local time_result = server.call('TIME')
    return tonumber(time_result[1]) or 0
end

--- Validate and return the registry (index) key
-- @param index_key string Registry key {c}:gnode:topo:registry
-- @return string Registry key
local function build_registry_key(index_key)
    if type(index_key) == 'string' and string.sub(index_key, -#REGISTRY_SUFFIX) == REGISTRY_SUFFIX
        and string.match(index_key, '^%b{}') then
        return index_key
    end
    return nil
end

local function index_tag(index_key)
    return string.match(index_key, '^(%b{})')
end

local function valid_topology_key(index_key, topology_key)
    local base = string.sub(index_key, 1, -#REGISTRY_SUFFIX - 1) .. ':gnode:topo:'
    if string.sub(topology_key, 1, #base) ~= base then
        return false
    end
    local name = string.sub(topology_key, #base + 1)
    if not string.match(name, '^[a-z0-9][a-z0-9_-]*$') then
        return false
    end
    return name ~= 'registry' and name ~= 'catalog' and name ~= 'by_type'
end

--- Build type index key for site
-- @param index_key string Registry key {c}:gnode:topo:registry
-- @param topo_type string Topology type
-- @return string Type index key
local function build_type_index_key(index_key, topo_type)
    return string.sub(index_key, 1, -#REGISTRY_SUFFIX - 1) .. TYPE_INDEX_PREFIX .. topo_type
end

-- ============================================================================
-- GNODE_TOPO_CREATE
-- Create a new topology with metadata in the registry
-- Daemon calls this after validating parameters
-- ============================================================================
server.register_function{
    function_name = 'GNODE_TOPO_CREATE',
    callback = function(keys, args)
        -- keys[1] = index key "{c}:gnode:topo:registry"
        -- args[1] = topology_key "{c}:gnode:topo:<name>"
        -- args[2] = definition_json (name, constraint_type, axis_semantics, etc)

        if #keys < 1 then
            return server.error_reply("Missing registry key")
        end
        if not build_registry_key(keys[1]) then
            return server.error_reply("keys[1] must be {c}:gnode:topo:registry")
        end
        if #args < 2 then
            return server.error_reply("Missing topology_key or definition_json")
        end

        local site_id = index_tag(keys[1])
        local topology_key = args[1]
        local definition_json = args[2]

        if index_tag(topology_key) ~= site_id then
            return server.error_reply("Topology key hash tag differs from registry key: " .. topology_key)
        end
        if not valid_topology_key(keys[1], topology_key) then
            return server.error_reply("Topology key must be {c}:gnode:topo:<name>: " .. topology_key)
        end

        -- Decode and validate definition
        local def, decode_err = safe_json_decode(definition_json)
        if not def then
            return server.error_reply("Invalid definition JSON: " .. (decode_err or "unknown"))
        end

        -- Validate required fields
        if not def.name or def.name == "" then
            return server.error_reply("Definition must include 'name'")
        end

        -- Set defaults
        local constraint_type = def.constraint_type or CONSTRAINT_TYPES.none
        if not CONSTRAINT_TYPES[constraint_type] then
            return server.error_reply("Invalid constraint_type: " .. tostring(constraint_type))
        end

        -- Check if topology already exists
        local registry_key = keys[1]
        local existing = server.call('HEXISTS', registry_key, topology_key)
        if existing == 1 then
            return server.error_reply("Topology already exists: " .. topology_key)
        end

        -- Build metadata (using abbreviated field names - see header)
        local now = get_timestamp()
        local metadata = {
            tk = topology_key,       -- topology_key
            si = site_id,            -- site_id
            n = def.name,            -- name
            ct = constraint_type,    -- constraint_type
            ax = def.axis_semantics or {
                x = { n = "x", d = "X axis" },
                y = { n = "y", d = "Y axis" },
                z = { n = "z", d = "Z axis (hierarchy)" }
            },
            d = def.description or "",  -- description
            tt = def.topology_type or "custom",  -- topology_type
            ca = now,                -- created_at
            ua = now,                -- updated_at
            ec = 0,                  -- entity_count
            xc = 0                   -- edge_count
        }

        -- Store metadata
        local meta_json, encode_err = safe_json_encode(metadata)
        if not meta_json then
            return server.error_reply("Failed to encode metadata: " .. (encode_err or "unknown"))
        end

        -- Write to registry and meta key
        server.call('HSET', registry_key, topology_key, meta_json)
        server.call('HSET', topology_key .. ':meta', 'data', meta_json)

        -- Add to type index
        if metadata.tt then
            local type_key = build_type_index_key(keys[1], metadata.tt)
            server.call('SADD', type_key, topology_key)
        end

        -- Return success (abbreviated fields)
        local result = {
            ok = true,
            tk = topology_key,
            ct = constraint_type,
            ts = now
        }

        local result_json, _ = safe_json_encode(result)
        return result_json
    end
}

-- ============================================================================
-- GNODE_TOPO_SET_DERIVED
-- Write measured coordinates into a stored entity without moving it.
--
-- A derived axis (load, health) changes far more often than anything a provider
-- declares, so it is written here rather than through a re-registration: the
-- bucket key and voxel membership are untouched, and nothing re-sends a whole
-- vector to update one number. An index below the tier's hashed width is
-- refused — those coordinates ARE the bucket key, and a value that moves cannot
-- be one of them.
-- ============================================================================
server.register_function{
    function_name = 'GNODE_TOPO_SET_DERIVED',
    callback = function(keys, args)
        -- keys[1] = topology_key
        -- args[1] = entity_id
        -- args[2] = updates JSON, { "<index>": { "pd": <float>, "pr": "<raw>" } }
        --           Both scales come from the daemon: Lua has no Q64.64.
        -- args[3] = z_score (OPTIONAL) — recomputed by the daemon when an update
        --           touches the z axis; omitted leaves the stored ordering.
        -- args[4] = snapshot_key (OPTIONAL) — keeps the (B) projection current.
        -- args[5] = ts (OPTIONAL) — the measurement's time, stored as m.ds.
        -- args[6] = hashed_width (OPTIONAL) — an index below this is refused.
        -- args[7] = node_id (OPTIONAL) — the writer. When a shared-writer lease
        --           exists, only its holder may write a derived axis: two nodes
        --           observing the same provider differently would otherwise take
        --           turns overwriting the measurement, and the value would flap
        --           with whichever tick landed last. No lease = nobody claims
        --           ownership, so the write is allowed.

        if #keys < 1 then
            return server.error_reply("Missing topology_key")
        end
        if #args < 2 then
            return server.error_reply("Missing entity_id or updates JSON")
        end

        local topology_key = keys[1]
        local entity_id = args[1]
        local hashed_width = tonumber(args[6])
        local constellation = server.call('HGET', topology_key .. ':meta', 'sv') == '5'

        -- A constellation passes its own lease key as KEYS[2]; a legacy topology reads the global one.
        local lease = server.call('GET', (constellation and keys[2]) or 'gnode:cluster:writer')
        if lease and lease ~= false and args[7] and args[7] ~= lease then
            return server.error_reply(string.format(
                "Derived writes belong to the lease holder (%s); %s does not hold it", lease, args[7]))
        end

        local updates, parse_err = safe_json_decode(args[2])
        if not updates then
            return server.error_reply("Invalid updates JSON: " .. (parse_err or "unknown"))
        end

        local entity_json = server.call('HGET', topology_key .. ':entities', entity_id)
        if not entity_json or entity_json == false then
            return server.error_reply("Entity not found: " .. entity_id)
        end
        local entity, decode_err = safe_json_decode(entity_json)
        if not entity then
            return server.error_reply("Stored entity is not decodable: " .. (decode_err or "unknown"))
        end

        local schema = nil
        if constellation then
            local flat = server.call('HGETALL', topology_key .. ':schema:' .. tostring(entity.sh))
            if not flat or #flat == 0 then
                return server.error_reply("schema for shape " .. tostring(entity.sh) .. " is not published")
            end
            schema = {}
            for i = 1, #flat, 2 do schema[flat[i]] = flat[i + 1] end
            schema.slots = cjson.decode(schema.slots)
        end

        local written = 0
        for index, value in pairs(updates) do
            if constellation and index == 'm' and type(value) == 'table' then
                -- a derived fact beside the point (m.scale_out, m.share), merged, never a coordinate
                entity.m = entity.m or {}
                for k, v in pairs(value) do entity.m[k] = v end
                written = written + 1
            else
            local slot = tonumber(index)
            if not slot then
                return server.error_reply("Update key is not an axis index: " .. tostring(index))
            end
            if constellation then
                local def = schema.slots[tostring(slot)]
                if slot < 16 or slot > 19 or not def then
                    return server.error_reply(string.format(
                        "Slot %d is not a derived axis of shape %s", slot, tostring(entity.sh)))
                end
                if def.writer == 'provider' then
                    return server.error_reply(string.format("Slot %d (%s) belongs to the provider", slot, def.name))
                end
                if value.s ~= nil then
                    entity.q = entity.q or {}
                    entity.q[def.name] = string.format('%.0f', tonumber(value.s))
                    server.call('ZADD', topology_key .. ':ix:' .. def.name, entity.q[def.name], entity_id)
                end
            elseif hashed_width and slot < hashed_width then
                return server.error_reply(string.format(
                    "Axis %d is hashed into the bucket key and cannot be written as derived", slot))
            end
            slot = slot + 1
            if not entity.pd or #entity.pd < slot then
                return server.error_reply(string.format(
                    "Axis %d is outside %s's stored point", slot - 1, entity_id))
            end
            if value.pd ~= nil then
                entity.pd[slot] = tonumber(value.pd)
            end
            if value.pr ~= nil and entity.pr and #entity.pr >= slot then
                entity.pr[slot] = tostring(value.pr)
            end
            written = written + 1
            end
        end

        entity.m = entity.m or {}
        entity.m.ds = tonumber(args[5]) or get_timestamp()

        local z_score = tonumber(args[3])
        if z_score then
            entity.zs = z_score
        end

        local final_json, encode_err = safe_json_encode(entity)
        if not final_json then
            return server.error_reply("Failed to encode entity: " .. (encode_err or "unknown"))
        end
        server.call('HSET', topology_key .. ':entities', entity_id, final_json)
        if z_score then
            server.call('ZADD', topology_key .. ':z_order', z_score, entity_id)
        end
        if constellation and entity.sh == '4.2' and entity.of then
            if tonumber(entity.pd[18]) ~= 0.25 and tonumber(entity.pd[19]) == 0.25 then
                server.call('ZADD', topology_key .. ':pl:' .. entity.of, entity.ro, entity_id)
            else
                server.call('ZREM', topology_key .. ':pl:' .. entity.of, entity_id)
            end
        end

        local snapshot_key = args[4]
        if snapshot_key and snapshot_key ~= "" then
            local snap_json, _ = safe_json_encode({ point = entity.pd, metadata = entity.m })
            if snap_json then
                server.call('HSET', snapshot_key, entity_id, snap_json)
            end
        end

        return safe_json_encode({
            ok = true,
            eid = entity_id,
            n = written,
            ds = entity.m.ds,
            zs = entity.zs
        })
    end,
    description = 'Writes derived (sampler-owned) coordinates into an entity without rehashing it'
}

-- ============================================================================
-- GNODE_TOPO_ADD_EDGE
-- Store edge AFTER daemon has validated constraints
-- Daemon validates z_monotonic, cycles, etc. using Q32.32 BEFORE calling
-- ============================================================================
server.register_function{
    function_name = 'GNODE_TOPO_ADD_EDGE',
    callback = function(keys, args)
        -- keys[1] = topology_key
        -- args[1] = from_id
        -- args[2] = to_id
        -- args[3] = edge_json (metadata from daemon including z_delta, etc)

        if #keys < 1 then
            return server.error_reply("Missing topology_key")
        end
        if #args < 3 then
            return server.error_reply("Missing from_id, to_id, or edge_json")
        end

        local topology_key = keys[1]
        local from_id = args[1]
        local to_id = args[2]
        local edge_json = args[3]

        if from_id == to_id then
            return server.error_reply("Self-edges not allowed")
        end

        -- Verify both entities exist
        local from_exists = server.call('HEXISTS', topology_key .. ':entities', from_id)
        local to_exists = server.call('HEXISTS', topology_key .. ':entities', to_id)

        if from_exists ~= 1 then
            return server.error_reply("Source entity not found: " .. from_id)
        end
        if to_exists ~= 1 then
            return server.error_reply("Target entity not found: " .. to_id)
        end

        -- On a constellation key an edge exists only if phi(from) > phi(to), phi = (top-level layer
        -- digit, fundamentality): cycles are impossible by one integer comparison (P-c).
        if server.call('HGET', topology_key .. ':meta', 'sv') == '5' then
            local lf = tonumber(server.call('ZSCORE', topology_key .. ':ix:layer', from_id))
            local lt = tonumber(server.call('ZSCORE', topology_key .. ':ix:layer', to_id))
            if not lf or not lt then
                return server.error_reply("Both ends must carry a layer")
            end
            local pf = tonumber(server.call('ZSCORE', topology_key .. ':ix:fundamentality', from_id) or 0)
            local pt = tonumber(server.call('ZSCORE', topology_key .. ':ix:fundamentality', to_id) or 0)
            lf, lt = math.floor(lf / 2 ^ 40), math.floor(lt / 2 ^ 40)
            if lf < lt or (lf == lt and pf <= pt) then
                return server.error_reply(string.format(
                    "cycle: phi(%s) = (%d, %s) must exceed phi(%s) = (%d, %s)", from_id, lf, tostring(pf), to_id, lt, tostring(pt)))
            end
        end

        -- Check for existing edge
        local edge_key = from_id .. ':' .. to_id
        local existing = server.call('HEXISTS', topology_key .. ':edges', edge_key)
        local is_update = (existing == 1)

        -- Store edge data
        server.call('HSET', topology_key .. ':edges', edge_key, edge_json)

        -- Update adjacency sets
        server.call('SADD', topology_key .. ':out:' .. from_id, to_id)
        server.call('SADD', topology_key .. ':in:' .. to_id, from_id)

        -- Update edge count if new
        if not is_update then
            server.call('HINCRBY', topology_key .. ':meta', 'xc', 1)  -- xc = edge_count
        end

        -- Return success (abbreviated)
        local result = {
            ok = true,
            f = from_id,
            t = to_id,
            upd = is_update
        }

        local result_json, _ = safe_json_encode(result)
        return result_json
    end
}

-- ============================================================================
-- GNODE_TOPO_REMOVE_EDGE
-- Remove a single edge between entities
-- ============================================================================
server.register_function{
    function_name = 'GNODE_TOPO_REMOVE_EDGE',
    callback = function(keys, args)
        -- keys[1] = topology_key
        -- args[1] = from_id
        -- args[2] = to_id

        if #keys < 1 then
            return server.error_reply("Missing topology_key")
        end
        if #args < 2 then
            return server.error_reply("Missing from_id or to_id")
        end

        local topology_key = keys[1]
        local from_id = args[1]
        local to_id = args[2]

        local edge_key = from_id .. ':' .. to_id

        -- Check if edge exists
        local exists = server.call('HEXISTS', topology_key .. ':edges', edge_key)
        if exists ~= 1 then
            return server.error_reply("Edge not found: " .. from_id .. " -> " .. to_id)
        end

        -- Remove edge data
        server.call('HDEL', topology_key .. ':edges', edge_key)

        -- Update adjacency sets
        server.call('SREM', topology_key .. ':out:' .. from_id, to_id)
        server.call('SREM', topology_key .. ':in:' .. to_id, from_id)

        -- Decrement edge count
        server.call('HINCRBY', topology_key .. ':meta', 'xc', -1)  -- xc = edge_count

        -- Return success (abbreviated)
        local result = {
            ok = true,
            f = from_id,
            t = to_id
        }

        local result_json, _ = safe_json_encode(result)
        return result_json
    end
}

-- ============================================================================
-- GNODE_TOPO_GET_ENTITIES
-- Batch get entity data by IDs
-- ============================================================================
server.register_function{
    function_name = 'GNODE_TOPO_GET_ENTITIES',
    callback = function(keys, args)
        -- keys[1] = topology_key
        -- args[1] = entity_ids_json (JSON array of entity IDs), or "*" for every entity
        -- args[2] = include_edges (optional, "true" to include edge info)

        if #keys < 1 then
            return server.error_reply("Missing topology_key")
        end
        if #args < 1 then
            return server.error_reply("Missing entity_ids_json")
        end

        local topology_key = keys[1]
        local ids_json = args[1]
        local include_edges = args[2] == "true"

        local entity_ids
        if ids_json == "*" then
            if server.call('HGET', topology_key .. ':meta', 'sv') == '5' then
                return server.error_reply("A constellation is not scanned: use GNODE_TOPO_QUERY with a layer range")
            end
            entity_ids = server.call('HKEYS', topology_key .. ':entities') or {}
        else
            local decode_err
            entity_ids, decode_err = safe_json_decode(ids_json)
            if not entity_ids then
                return server.error_reply("Invalid entity_ids JSON: " .. (decode_err or "unknown"))
            end

            if type(entity_ids) ~= "table" then
                return server.error_reply("entity_ids must be a JSON array or \"*\"")
            end
        end

        -- Fetch entities
        local entities = {}
        local found = 0
        local missing = {}

        for _, entity_id in ipairs(entity_ids) do
            local entity_json = server.call('HGET', topology_key .. ':entities', entity_id)
            if entity_json then
                local entity, _ = safe_json_decode(entity_json)
                if entity then
                    -- Optionally include edge information (abbreviated)
                    if include_edges then
                        entity.out = server.call('SMEMBERS', topology_key .. ':out:' .. entity_id) or {}
                        entity["in"] = server.call('SMEMBERS', topology_key .. ':in:' .. entity_id) or {}
                    end
                    entities[entity_id] = entity
                    found = found + 1
                end
            else
                table.insert(missing, entity_id)
            end
        end

        -- Return result (abbreviated)
        local result = {
            req = #entity_ids,  -- requested
            fnd = found,        -- found
            mis = #missing > 0 and missing or nil,  -- missing
            ents = entities
        }

        local result_json, _ = safe_json_encode(result)
        return result_json
    end,
    flags = {'no-writes'}
}

-- ============================================================================
-- GNODE_TOPO_GET_ENTITY
-- Get single entity with full edge information
-- ============================================================================
server.register_function{
    function_name = 'GNODE_TOPO_GET_ENTITY',
    callback = function(keys, args)
        -- keys[1] = topology_key
        -- args[1] = entity_id

        if #keys < 1 then
            return server.error_reply("Missing topology_key")
        end
        if #args < 1 then
            return server.error_reply("Missing entity_id")
        end

        local topology_key = keys[1]
        local entity_id = args[1]

        local entity_json = server.call('HGET', topology_key .. ':entities', entity_id)
        if not entity_json then
            return server.error_reply("Entity not found: " .. entity_id)
        end

        local entity, decode_err = safe_json_decode(entity_json)
        if not entity then
            return server.error_reply("Failed to decode entity: " .. (decode_err or "unknown"))
        end

        -- Add edge information (abbreviated)
        entity.out = server.call('SMEMBERS', topology_key .. ':out:' .. entity_id) or {}
        entity["in"] = server.call('SMEMBERS', topology_key .. ':in:' .. entity_id) or {}

        local result_json, _ = safe_json_encode(entity)
        return result_json
    end,
    flags = {'no-writes'}
}

-- ============================================================================
-- GNODE_TOPO_GET_EDGE
-- Get single edge data
-- ============================================================================
server.register_function{
    function_name = 'GNODE_TOPO_GET_EDGE',
    callback = function(keys, args)
        -- keys[1] = topology_key
        -- args[1] = from_id
        -- args[2] = to_id

        if #keys < 1 then
            return server.error_reply("Missing topology_key")
        end
        if #args < 2 then
            return server.error_reply("Missing from_id or to_id")
        end

        local topology_key = keys[1]
        local from_id = args[1]
        local to_id = args[2]

        local edge_key = from_id .. ':' .. to_id
        local edge_json = server.call('HGET', topology_key .. ':edges', edge_key)

        if not edge_json then
            return server.error_reply("Edge not found: " .. from_id .. " -> " .. to_id)
        end

        return edge_json
    end,
    flags = {'no-writes'}
}

-- ============================================================================
-- GNODE_TOPO_GET_EDGES
-- Get all edges for an entity (outgoing, incoming, or both)
-- ============================================================================
server.register_function{
    function_name = 'GNODE_TOPO_GET_EDGES',
    callback = function(keys, args)
        -- keys[1] = topology_key
        -- args[1] = entity_id
        -- args[2] = direction ("outgoing", "incoming", or "both")

        if #keys < 1 then
            return server.error_reply("Missing topology_key")
        end
        if #args < 1 then
            return server.error_reply("Missing entity_id")
        end

        local topology_key = keys[1]
        local entity_id = args[1]
        local direction = args[2] or "both"

        -- Build result (abbreviated)
        local result = {
            eid = entity_id,
            dir = direction
        }

        if direction == "outgoing" or direction == "both" then
            local outgoing_ids = server.call('SMEMBERS', topology_key .. ':out:' .. entity_id) or {}
            local outgoing_edges = {}
            for _, target_id in ipairs(outgoing_ids) do
                local edge_key = entity_id .. ':' .. target_id
                local edge_json = server.call('HGET', topology_key .. ':edges', edge_key)
                if edge_json then
                    local edge, _ = safe_json_decode(edge_json)
                    outgoing_edges[target_id] = edge
                end
            end
            result.out = outgoing_edges
            result.ocnt = #outgoing_ids  -- outgoing_count
        end

        if direction == "incoming" or direction == "both" then
            local incoming_ids = server.call('SMEMBERS', topology_key .. ':in:' .. entity_id) or {}
            local incoming_edges = {}
            for _, source_id in ipairs(incoming_ids) do
                local edge_key = source_id .. ':' .. entity_id
                local edge_json = server.call('HGET', topology_key .. ':edges', edge_key)
                if edge_json then
                    local edge, _ = safe_json_decode(edge_json)
                    incoming_edges[source_id] = edge
                end
            end
            result["in"] = incoming_edges
            result.icnt = #incoming_ids  -- incoming_count
        end

        local result_json, _ = safe_json_encode(result)
        return result_json
    end,
    flags = {'no-writes'}
}

-- ============================================================================
-- GNODE_TOPO_STATS
-- Get topology statistics
-- ============================================================================
server.register_function{
    function_name = 'GNODE_TOPO_STATS',
    callback = function(keys, args)
        -- keys[1] = topology_key

        if #keys < 1 then
            return server.error_reply("Missing topology_key")
        end

        local topology_key = keys[1]

        -- Get metadata
        local meta_json = server.call('HGET', topology_key .. ':meta', 'data')
        if not meta_json then
            return server.error_reply("Topology not found: " .. topology_key)
        end

        local meta, _ = safe_json_decode(meta_json)

        -- Get actual counts (verify against meta)
        local entity_count = server.call('HLEN', topology_key .. ':entities') or 0
        local edge_count = server.call('HLEN', topology_key .. ':edges') or 0
        local z_order_count = server.call('ZCARD', topology_key .. ':z_order') or 0

        -- Get Z-range if entities exist
        local z_min, z_max
        if z_order_count > 0 then
            local min_result = server.call('ZRANGE', topology_key .. ':z_order', 0, 0, 'WITHSCORES')
            local max_result = server.call('ZREVRANGE', topology_key .. ':z_order', 0, 0, 'WITHSCORES')
            if min_result and #min_result >= 2 then
                z_min = tonumber(min_result[2])
            end
            if max_result and #max_result >= 2 then
                z_max = tonumber(max_result[2])
            end
        end

        -- Get actual counts from hash fields (the HINCRBY targets)
        local tracked_entity_count = tonumber(server.call('HGET', topology_key .. ':meta', 'ec')) or 0
        local tracked_edge_count = tonumber(server.call('HGET', topology_key .. ':meta', 'xc')) or 0

        -- Return result (abbreviated)
        local result = {
            tk = topology_key,
            n = meta and meta.n,      -- name
            ct = meta and meta.ct,    -- constraint_type
            ec = entity_count,        -- entity_count (actual)
            xc = edge_count,          -- edge_count (actual)
            tec = tracked_entity_count,  -- tracked_entity_count
            txc = tracked_edge_count,    -- tracked_edge_count
            zoc = z_order_count,      -- z_order_count
            zr = {                    -- z_range
                mn = z_min,
                mx = z_max
            },
            ca = meta and meta.ca,    -- created_at
            ok = (entity_count == tracked_entity_count and edge_count == tracked_edge_count)  -- counts_match
        }

        local result_json, _ = safe_json_encode(result)
        return result_json
    end,
    flags = {'no-writes'}
}

-- ============================================================================
-- GNODE_TOPO_LIST
-- List all topologies for a site
-- ============================================================================
server.register_function{
    function_name = 'GNODE_TOPO_LIST',
    callback = function(keys, args)
        -- keys[1] = index key "{c}:gnode:topo:registry"
        -- args[1] = topology_type (optional, filter by type)

        if #keys < 1 or not build_registry_key(keys[1]) then
            return server.error_reply("keys[1] must be {c}:gnode:topo:registry")
        end

        local site_id = index_tag(keys[1])
        local filter_type = args[1]

        local topology_keys

        if filter_type and filter_type ~= "" then
            -- Get from type index
            local type_key = build_type_index_key(keys[1], filter_type)
            topology_keys = server.call('SMEMBERS', type_key) or {}
        else
            -- Get all from registry
            topology_keys = server.call('HKEYS', keys[1]) or {}
        end

        -- Fetch summary for each
        local topologies = {}
        for _, topo_key in ipairs(topology_keys) do
            local meta_json = server.call('HGET', topo_key .. ':meta', 'data')
            if meta_json then
                local meta, _ = safe_json_decode(meta_json)
                if meta then
                    -- Get actual counts from hash fields (not stale JSON)
                    local entity_count = tonumber(server.call('HGET', topo_key .. ':meta', 'ec')) or 0
                    local edge_count = tonumber(server.call('HGET', topo_key .. ':meta', 'xc')) or 0

                    -- Build summary (abbreviated)
                    table.insert(topologies, {
                        tk = topo_key,
                        n = meta.n,       -- name
                        tt = meta.tt,     -- topology_type
                        ct = meta.ct,     -- constraint_type
                        ec = entity_count,
                        xc = edge_count,
                        ca = meta.ca      -- created_at
                    })
                end
            end
        end

        -- Return result (abbreviated)
        local result = {
            si = site_id,
            ft = filter_type,     -- filter_type
            cnt = #topologies,
            topos = topologies
        }

        local result_json, _ = safe_json_encode(result)
        return result_json
    end,
    flags = {'no-writes'}
}

-- ============================================================================
-- GNODE_TOPO_DELETE
-- Delete entire topology (DANGEROUS - requires CONFIRM flag)
-- ============================================================================
server.register_function{
    function_name = 'GNODE_TOPO_DELETE',
    callback = function(keys, args)
        -- keys[1] = index key "{c}:gnode:topo:registry"
        -- args[1] = topology_key
        -- args[2] = "CONFIRM" (required safety flag)

        if #keys < 1 or not build_registry_key(keys[1]) then
            return server.error_reply("keys[1] must be {c}:gnode:topo:registry")
        end
        if #args < 2 then
            return server.error_reply("Missing topology_key or CONFIRM flag")
        end

        local topology_key = args[1]
        local confirm = args[2]

        if index_tag(topology_key) ~= index_tag(keys[1]) or not valid_topology_key(keys[1], topology_key) then
            return server.error_reply("Topology key must be {c}:gnode:topo:<name> under the registry key's tag: " .. topology_key)
        end

        if confirm ~= "CONFIRM" then
            return server.error_reply("Must provide 'CONFIRM' flag to delete topology")
        end

        -- Get metadata to find type for index cleanup
        local meta_json = server.call('HGET', topology_key .. ':meta', 'data')
        if not meta_json then
            return server.error_reply("Topology not found: " .. topology_key)
        end

        local meta, _ = safe_json_decode(meta_json)

        -- Get all entity IDs for cleanup
        local entity_ids = server.call('HKEYS', topology_key .. ':entities') or {}

        -- Delete all voxel keys (we need to scan for pattern)
        -- Get bucket keys from entities
        local voxel_keys_deleted = 0
        for _, entity_id in ipairs(entity_ids) do
            local entity_json = server.call('HGET', topology_key .. ':entities', entity_id)
            if entity_json then
                local entity, _ = safe_json_decode(entity_json)
                if entity and entity.bk then  -- bk = bucket_key
                    server.call('DEL', topology_key .. ':voxel:' .. entity.bk)
                    voxel_keys_deleted = voxel_keys_deleted + 1
                end
            end
            -- Delete edge sets
            server.call('DEL', topology_key .. ':out:' .. entity_id)
            server.call('DEL', topology_key .. ':in:' .. entity_id)
        end

        -- Delete main structures
        server.call('DEL', topology_key .. ':entities')
        server.call('DEL', topology_key .. ':edges')
        server.call('DEL', topology_key .. ':z_order')
        server.call('DEL', topology_key .. ':meta')

        -- Remove from registry
        server.call('HDEL', keys[1], topology_key)

        -- Remove from type index
        if meta and meta.tt then  -- tt = topology_type
            local type_key = build_type_index_key(keys[1], meta.tt)
            server.call('SREM', type_key, topology_key)
        end

        -- Return result (abbreviated)
        local result = {
            ok = true,
            tk = topology_key,
            ed = #entity_ids,       -- entities_deleted
            vd = voxel_keys_deleted -- voxel_keys_deleted
        }

        local result_json, _ = safe_json_encode(result)
        return result_json
    end
}

-- ============================================================================
-- GNODE_TOPO_EXISTS
-- Check if topology exists
-- ============================================================================
server.register_function{
    function_name = 'GNODE_TOPO_EXISTS',
    callback = function(keys, args)
        -- keys[1] = topology_key

        if #keys < 1 then
            return server.error_reply("Missing topology_key")
        end

        local topology_key = keys[1]
        local exists = server.call('EXISTS', topology_key .. ':meta')

        -- Return result (abbreviated)
        local result = {
            tk = topology_key,
            ex = (exists == 1)  -- exists
        }

        local result_json, _ = safe_json_encode(result)
        return result_json
    end,
    flags = {'no-writes'}
}

-- ============================================================================
-- ONE TOPOLOGY PER CONSTELLATION (SPEC-ONE-TOPOLOGY §4). A constellation key is a
-- topology whose meta carries sv = 5; the functions below run only on one, and
-- the kept functions above branch on it. Scores are Q0.48 integers (pr >> 16)
-- kept as strings: exact in a double, exact through cjson.
-- ============================================================================

local W = 24
local Q48 = 2 ^ 48
local LIFECYCLE_ACTIVE = 0.25
local HEALTH_DEAD = 0.25
local function on_ring(h, lc) return tonumber(h) ~= HEALTH_DEAD and tonumber(lc) == LIFECYCLE_ACTIVE end

local function tag_of(tk) return string.match(tk, '^(%b{})') or '' end
local function is_constellation(tk) return server.call('HGET', tk .. ':meta', 'sv') == '5' end
local function sfmt(n) return string.format('%.0f', n) end

local function shape_map(tk, sh)
    local flat = server.call('HGETALL', tk .. ':schema:' .. sh)
    if not flat or #flat == 0 then return nil end
    local m = {}
    for i = 1, #flat, 2 do m[flat[i]] = flat[i + 1] end
    m.index = cjson.decode(m.dimension_index)
    m.values = cjson.decode(m.dimension_values)
    m.by_name = {}
    for idx, def in pairs(cjson.decode(m.slots)) do def.slot = tonumber(idx); m.by_name[def.name] = def end
    m.lo, m.hi = tonumber(m.layer_code), tonumber(m.layer_hi)
    return m
end

local function hmget_all(key, ids)
    local out = {}
    for i = 1, #ids, 1000 do
        local part = server.call('HMGET', key, unpack(ids, i, math.min(i + 999, #ids)))
        for _, r in ipairs(part) do out[#out + 1] = r end
    end
    return out
end

local function load_record(tk, id)
    local raw = server.call('HGET', tk .. ':entities', id)
    if not raw then return nil end
    local ok, rec = pcall(cjson.decode, raw)
    if ok then return rec end
    return nil
end

local function ring_sync(tk, rec)
    if rec.sh ~= '4.2' or not rec.of then return end
    if on_ring(rec.pd[18] or 0, rec.pd[19]) then
        server.call('ZADD', tk .. ':pl:' .. rec.of, rec.ro, rec.id)
    else
        server.call('ZREM', tk .. ':pl:' .. rec.of, rec.id)
    end
end

local function store(tk, rec)
    server.call('HSET', tk .. ':entities', rec.id, cjson.encode(rec))
end

-- The one write path for entities and placements.
local function register(tk, id, sh, rec, ordered, colours, preserve)
    local sm = shape_map(tk, sh)
    if not sm then return nil, 'schema for shape ' .. sh .. ' is not published at ' .. tk .. ':schema:' .. sh end
    if type(rec.pd) ~= 'table' or type(rec.pr) ~= 'table' or #rec.pd ~= W or #rec.pr ~= W then
        return nil, string.format('point width must be %d (pd %d, pr %d)', W, rec.pd and #rec.pd or 0, rec.pr and #rec.pr or 0)
    end
    local ly = tonumber(rec.ly)
    if not ly or ly < sm.lo or ly > sm.hi then
        return nil, string.format('layer code %s is outside shape %s [%s, %s]', tostring(rec.ly), sh, sfmt(sm.lo), sfmt(sm.hi))
    end
    local q = { layer = sfmt(ly) }
    for axis, score in pairs(ordered or {}) do
        local def = sm.by_name[axis]
        if not def then return nil, 'axis ' .. axis .. ' is not an axis of shape ' .. sh end
        if def.kind ~= 'ordered' then return nil, 'axis ' .. axis .. ' is a colour, not ordered' end
        if axis ~= 'layer' then
            local s = tonumber(score)
            if not s or s < 0 or s > Q48 then return nil, 'score for ' .. axis .. ' must be an integer in [0, 2^48]' end
            q[axis] = sfmt(s)
        end
    end
    if q.fundamentality == nil and sm.by_name.fundamentality and sm.fundamentality_default and sm.fundamentality_default ~= '' then
        q.fundamentality = sfmt(tonumber(sm.fundamentality_default) * Q48)
    end
    local c = {}
    for facet, vals in pairs(colours or {}) do
        local def = sm.by_name[facet]
        if not def then return nil, 'facet ' .. facet .. ' is not an axis of shape ' .. sh end
        if def.kind ~= 'colour' then return nil, 'facet ' .. facet .. ' is ordered, not a colour' end
        if type(vals) ~= 'table' then vals = { vals } end
        if #vals == 0 then return nil, 'facet ' .. facet .. ' has no value' end
        if not def.many and #vals > 1 then return nil, 'facet ' .. facet .. ' takes one value' end
        local vocab = sm.values[facet]
        for _, v in ipairs(vals) do
            if not def.freeform and vocab and vocab[v] == nil then return nil, string.format('%s = %s is not in the vocabulary', facet, tostring(v)) end
        end
        c[facet] = vals
    end
    local old = load_record(tk, id)
    rec.id, rec.sh, rec.q, rec.c = id, sh, q, c
    rec.m = rec.m or {}
    if old then
        rec.ro = old.ro
        for axis in pairs(old.q or {}) do if q[axis] == nil then server.call('ZREM', tk .. ':ix:' .. axis, id) end end
        for facet, vals in pairs(old.c or {}) do for _, v in ipairs(vals) do server.call('SREM', tk .. ':c:' .. facet .. ':' .. v, id) end end
        for idx in string.gmatch(preserve or '', '([^,]+)') do
            local slot = tonumber(idx)
            if slot and slot >= 16 and slot < W then
                rec.pd[slot + 1] = old.pd[slot + 1]; rec.pr[slot + 1] = old.pr[slot + 1]
                if old.q then for a, s in pairs(old.q) do if sm.by_name[a] and sm.by_name[a].slot == slot then q[a] = s end end end
            end
        end
        if old.m and old.m.ds then rec.m.ds = old.m.ds end
        if old.m and old.m.share then rec.m.share = old.m.share end
    else
        rec.ro = server.call('HINCRBY', tk .. ':meta', 'ro', 1)
        server.call('HINCRBY', tk .. ':meta', 'ec', 1)
    end
    rec.ra = get_timestamp()
    rec.env = rec.env or 'global'
    store(tk, rec)
    server.call('HSET', tk .. ':env', id, rec.env)
    for axis, s in pairs(q) do server.call('ZADD', tk .. ':ix:' .. axis, s, id) end
    for facet, vals in pairs(c) do for _, v in ipairs(vals) do server.call('SADD', tk .. ':c:' .. facet .. ':' .. v, id) end end
    return rec, old ~= nil
end

local function remove_indexes(tk, rec)
    for axis in pairs(rec.q or {}) do server.call('ZREM', tk .. ':ix:' .. axis, rec.id) end
    for facet, vals in pairs(rec.c or {}) do for _, v in ipairs(vals) do server.call('SREM', tk .. ':c:' .. facet .. ':' .. v, rec.id) end end
    server.call('HDEL', tk .. ':env', rec.id)
    server.call('HDEL', tk .. ':w', rec.id)
    server.call('DEL', tk .. ':holds:' .. rec.id, tk .. ':req:' .. rec.id, tk .. ':peers:' .. rec.id, tk .. ':inflight:' .. rec.id)
end

local function remove_edges(tk, id)
    local n = 0
    for _, t in ipairs(server.call('SMEMBERS', tk .. ':out:' .. id)) do
        server.call('HDEL', tk .. ':edges', id .. ':' .. t); server.call('SREM', tk .. ':in:' .. t, id); n = n + 1
    end
    for _, f in ipairs(server.call('SMEMBERS', tk .. ':in:' .. id)) do
        server.call('HDEL', tk .. ':edges', f .. ':' .. id); server.call('SREM', tk .. ':out:' .. f, id); n = n + 1
    end
    server.call('DEL', tk .. ':out:' .. id, tk .. ':in:' .. id)
    if n > 0 then server.call('HINCRBY', tk .. ':meta', 'xc', -n) end
    return n
end

local function deregister(tk, id)
    local rec = load_record(tk, id)
    if not rec then return nil end
    local removed = 0
    if rec.sh ~= '4.1' and rec.sh ~= '4.2' then
        local pls = {}
        for _, p in ipairs(server.call('SMEMBERS', tk .. ':plof:' .. id)) do pls[p] = true end
        for _, p in ipairs(server.call('SMEMBERS', tk .. ':plon:' .. id)) do pls[p] = true end
        for p in pairs(pls) do if deregister(tk, p) then removed = removed + 1 end end
        server.call('DEL', tk .. ':plof:' .. id, tk .. ':plon:' .. id, tk .. ':pl:' .. id, tk .. ':rr:' .. id)
    else
        if rec.of then server.call('SREM', tk .. ':plof:' .. rec.of, id); server.call('ZREM', tk .. ':pl:' .. rec.of, id) end
        if rec.on then server.call('SREM', tk .. ':plon:' .. rec.on, id) end
        server.call('SREM', tk .. ':placements', id)
    end
    remove_indexes(tk, rec)
    remove_edges(tk, id)
    server.call('HDEL', tk .. ':entities', id)
    server.call('HINCRBY', tk .. ':meta', 'ec', -1)
    return removed
end

local function lease_ok(keys, node_id)
    local lease_key = keys[2] or 'gnode:cluster:writer'
    local holder = server.call('GET', lease_key)
    if holder and node_id and node_id ~= '' and node_id ~= holder then
        return false, string.format('derived writes belong to the lease holder (%s); %s does not hold it', holder, node_id)
    end
    return true
end

-- GNODE_ENSURE_CONSTELLATION tk : the one key of a constellation, created once.
server.register_function{
    function_name = 'GNODE_ENSURE_CONSTELLATION',
    callback = function(keys, args)
        if #keys < 1 then return server.error_reply('Missing constellation key') end
        local tk = keys[1]
        if server.call('HEXISTS', tk .. ':meta', 'data') == 1 then
            if not is_constellation(tk) then return server.error_reply(tk .. ' exists and is not a constellation topology') end
            return cjson.encode({ ok = true, tk = tk, created = false })
        end
        local now = get_timestamp()
        server.call('HSET', tk .. ':meta',
            'data', cjson.encode({ tk = tk, n = 'constellation', ct = 'phi', tt = 'constellation', ca = now, ua = now, w = W }),
            'sv', '5', 'ec', '0', 'xc', '0', 'ro', '0')
        return cjson.encode({ ok = true, tk = tk, created = true })
    end
}

-- GNODE_TOPO_REGISTER tk | id shape record_json ordered_json colours_json [preserve_csv]
server.register_function{
    function_name = 'GNODE_TOPO_REGISTER',
    callback = function(keys, args)
        if #keys < 1 or #args < 5 then return server.error_reply('GNODE_TOPO_REGISTER tk | id shape record ordered colours [preserve]') end
        local tk = keys[1]
        if not is_constellation(tk) then return server.error_reply(tk .. ' is not a constellation topology (GNODE_ENSURE_CONSTELLATION first)') end
        local id, sh = args[1], args[2]
        if id == '' or sh == '' then return server.error_reply('id and shape are required') end
        if sh == '4.1' or sh == '4.2' then return server.error_reply('placements register through GNODE_TOPO_PLACE') end
        local ok1, rec = pcall(cjson.decode, args[3]); local ok2, ordered = pcall(cjson.decode, args[4]); local ok3, colours = pcall(cjson.decode, args[5])
        if not (ok1 and ok2 and ok3) then return server.error_reply('record, ordered and colours must be JSON') end
        local stored, upd = register(tk, id, sh, rec, ordered, colours, args[6])
        if not stored then return server.error_reply(upd) end
        return cjson.encode({ ok = true, eid = id, sh = sh, ro = stored.ro, upd = upd })
    end
}

-- GNODE_TOPO_PLACE tk | id shape of on env heartbeat_component weight record_json ordered_json colours_json [heartbeat_env]
-- heartbeat_env is the env segment of the heartbeat key when it differs from the entity's environment (the daemon writes `all`).
server.register_function{
    function_name = 'GNODE_TOPO_PLACE',
    callback = function(keys, args)
        if #keys < 1 or #args < 10 then return server.error_reply('GNODE_TOPO_PLACE tk | id shape of on env heartbeat weight record ordered colours') end
        local tk = keys[1]
        if not is_constellation(tk) then return server.error_reply(tk .. ' is not a constellation topology') end
        local id, sh, of, on, env, hb, w = args[1], args[2], args[3], args[4], args[5], args[6], tonumber(args[7])
        if sh ~= '4.1' and sh ~= '4.2' then return server.error_reply('a placement is shape 4.1 or 4.2') end
        if not w or w < 0 then return server.error_reply('weight must be a number >= 0 (0 = standby)') end
        local ofr, onr = load_record(tk, of), load_record(tk, on)
        if not ofr then return server.error_reply('unknown entity ' .. of) end
        if not onr or onr.sh ~= '1' then return server.error_reply(on .. ' is not a node') end
        if sh == '4.2' and string.sub(ofr.sh, 1, 1) ~= '3' then return server.error_reply('4.2 places a service; ' .. of .. ' is shape ' .. ofr.sh) end
        if sh == '4.1' and ofr.sh ~= '2' then return server.error_reply('4.1 places a component; ' .. of .. ' is shape ' .. ofr.sh) end
        local ok1, rec = pcall(cjson.decode, args[8]); local ok2, ordered = pcall(cjson.decode, args[9]); local ok3, colours = pcall(cjson.decode, args[10])
        if not (ok1 and ok2 and ok3) then return server.error_reply('record, ordered and colours must be JSON') end
        rec.of, rec.on, rec.hb, rec.env, rec.w = of, on, hb, env, w
        if args[11] and args[11] ~= '' then rec.hbe = args[11] end
        if sh == '4.2' and next(colours) == nil then
            for facet, vals in pairs(ofr.c or {}) do
                local sm = shape_map(tk, '4.2')
                if sm and sm.by_name[facet] then colours[facet] = vals end
            end
        end
        local stored, upd = register(tk, id, sh, rec, ordered, colours, nil)
        if not stored then return server.error_reply(upd) end
        server.call('SADD', tk .. ':plof:' .. of, id)
        server.call('SADD', tk .. ':plon:' .. on, id)
        server.call('SADD', tk .. ':placements', id)
        server.call('HSET', tk .. ':w', id, sfmt(w))
        ring_sync(tk, stored)
        return cjson.encode({ ok = true, eid = id, sh = sh, ro = stored.ro, upd = upd })
    end
}

-- GNODE_TOPO_DEREGISTER tk | id : the entity, its indexes, edges and placements.
server.register_function{
    function_name = 'GNODE_TOPO_DEREGISTER',
    callback = function(keys, args)
        if #keys < 1 or #args < 1 then return server.error_reply('GNODE_TOPO_DEREGISTER tk | id') end
        local tk = keys[1]
        if not is_constellation(tk) then return server.error_reply(tk .. ' is not a constellation topology') end
        local removed = deregister(tk, args[1])
        if removed == nil then return server.error_reply('unknown entity ' .. args[1]) end
        return cjson.encode({ ok = true, eid = args[1], placements_removed = removed })
    end
}

-- GNODE_TOPO_QUERY tk | layer_lo layer_hi filters_json [opts_json]
-- filters: { axes: { name: [lo, hi] }, facets: { name: [any-of] } }; opts: { limit }
-- The layer range drives; every other constraint is one index lookup per candidate; survivors decode.
server.register_function{
    function_name = 'GNODE_TOPO_QUERY', flags = { 'no-writes' },
    callback = function(keys, args)
        if #keys < 1 or #args < 3 then return server.error_reply('GNODE_TOPO_QUERY tk | lo hi filters [opts]') end
        local tk = keys[1]
        local ok, filt = pcall(cjson.decode, args[3])
        if not ok then return server.error_reply('filters must be JSON') end
        local opts = {}
        if args[4] and args[4] ~= '' then local ok2; ok2, opts = pcall(cjson.decode, args[4]); if not ok2 then return server.error_reply('opts must be JSON') end end
        local limit = tonumber(opts.limit) or 0
        local ids = server.call('ZRANGEBYSCORE', tk .. ':ix:layer', args[1], args[2])
        local keep = {}
        for _, id in ipairs(ids) do
            local pass = true
            for axis, r in pairs(filt.axes or {}) do
                local s = server.call('ZSCORE', tk .. ':ix:' .. axis, id)
                if not s or tonumber(s) < tonumber(r[1]) or tonumber(s) > tonumber(r[2]) then pass = false; break end
            end
            if pass then
                for facet, vals in pairs(filt.facets or {}) do
                    if type(vals) ~= 'table' then vals = { vals } end
                    local any = false
                    for _, v in ipairs(vals) do if server.call('SISMEMBER', tk .. ':c:' .. facet .. ':' .. v, id) == 1 then any = true; break end end
                    if not any then pass = false; break end
                end
            end
            if pass then keep[#keep + 1] = id; if limit > 0 and #keep >= limit then break end end
        end
        local out = {}
        if #keep > 0 then for _, r in ipairs(hmget_all(tk .. ':entities', keep)) do if r then out[#out + 1] = r end end end
        return out
    end
}

-- GNODE_TOPO_ORDER tk | axis [limit offset desc] : ids ranked by one indexed axis, with scores.
server.register_function{
    function_name = 'GNODE_TOPO_ORDER', flags = { 'no-writes' },
    callback = function(keys, args)
        if #keys < 1 or #args < 1 then return server.error_reply('GNODE_TOPO_ORDER tk | axis [limit offset desc]') end
        local limit, offset = tonumber(args[2]) or -1, tonumber(args[3]) or 0
        local stop = limit > 0 and offset + limit - 1 or -1
        if args[4] == 'desc' then return server.call('ZRANGE', keys[1] .. ':ix:' .. args[1], offset, stop, 'REV', 'WITHSCORES') end
        return server.call('ZRANGE', keys[1] .. ':ix:' .. args[1], offset, stop, 'WITHSCORES')
    end
}

-- GNODE_TOPO_ROUTE tk | service : exact weighted round-robin from one shared counter.
-- Smooth weighted order within a cycle (deterministic from the ring order); weight 0 = standby.
server.register_function{
    function_name = 'GNODE_TOPO_ROUTE',
    callback = function(keys, args)
        if #keys < 1 or #args < 1 then return server.error_reply('GNODE_TOPO_ROUTE tk | service') end
        local tk, svc = keys[1], args[1]
        local pl = server.call('ZRANGE', tk .. ':pl:' .. svc, 0, -1)
        if #pl == 0 then return server.error_reply('NOPLACEMENT ' .. svc .. ' has no healthy active placement') end
        local ws, standby, total = {}, {}, 0
        for i, p in ipairs(pl) do
            local w = tonumber(server.call('HGET', tk .. ':w', p) or 1)
            if w > 0 then ws[#ws + 1] = { p = p, w = w }; total = total + w else standby[#standby + 1] = p end
        end
        if total == 0 then
            local n = server.call('INCR', tk .. ':rr:' .. svc)
            local p = standby[((n - 1) % #standby) + 1]
            local r = load_record(tk, p)
            return { p, r and r.on or '', 'standby' }
        end
        local n = server.call('INCR', tk .. ':rr:' .. svc)
        local step = (n - 1) % total
        local cw = {}
        for i = 1, #ws do cw[i] = 0 end
        local pick
        for s = 0, step do
            local best = 1
            for i = 1, #ws do cw[i] = cw[i] + ws[i].w; if cw[i] > cw[best] then best = i end end
            cw[best] = cw[best] - total
            pick = ws[best].p
        end
        local r = load_record(tk, pick)
        return { pick, r and r.on or '', 'ring' }
    end
}

-- In-flight per placement: a sorted set of correlation ids scored by departure ms (a lost reply reaps itself).
server.register_function{
    function_name = 'GNODE_TOPO_DEPART',
    callback = function(keys, args)
        if #keys < 1 or #args < 3 then return server.error_reply('GNODE_TOPO_DEPART tk | placement cid now_ms') end
        server.call('ZADD', keys[1] .. ':inflight:' .. args[1], args[3], args[2])
        return 'OK'
    end
}
server.register_function{
    function_name = 'GNODE_TOPO_ARRIVE',
    callback = function(keys, args)
        if #keys < 1 or #args < 2 then return server.error_reply('GNODE_TOPO_ARRIVE tk | placement cid') end
        return server.call('ZREM', keys[1] .. ':inflight:' .. args[1], args[2])
    end
}
server.register_function{
    function_name = 'GNODE_TOPO_INFLIGHT',
    callback = function(keys, args)
        if #keys < 1 or #args < 3 then return server.error_reply('GNODE_TOPO_INFLIGHT tk | placement now_ms ttl_ms') end
        local k = keys[1] .. ':inflight:' .. args[1]
        server.call('ZREMRANGEBYSCORE', k, '-inf', tonumber(args[2]) - tonumber(args[3]))
        return server.call('ZCARD', k)
    end
}

-- REACH labels and the deny list (D8).
server.register_function{
    function_name = 'GNODE_TOPO_LABEL',
    callback = function(keys, args)
        if #keys < 1 or #args < 4 then return server.error_reply('GNODE_TOPO_LABEL tk | id holds_json requires_json peers_json') end
        local tk, id = keys[1], args[1]
        if server.call('HEXISTS', tk .. ':entities', id) ~= 1 then return server.error_reply('unknown entity ' .. id) end
        local ok1, holds = pcall(cjson.decode, args[2]); local ok2, req = pcall(cjson.decode, args[3]); local ok3, peers = pcall(cjson.decode, args[4])
        if not (ok1 and ok2 and ok3) then return server.error_reply('holds, requires and peers must be JSON arrays') end
        server.call('DEL', tk .. ':holds:' .. id, tk .. ':req:' .. id, tk .. ':peers:' .. id)
        for _, l in ipairs(holds) do server.call('SADD', tk .. ':holds:' .. id, l) end
        for _, l in ipairs(req) do server.call('SADD', tk .. ':req:' .. id, l) end
        for _, p in ipairs(peers) do server.call('SADD', tk .. ':peers:' .. id, p) end
        return cjson.encode({ ok = true, eid = id, holds = #holds, requires = #req, peers = #peers })
    end
}
server.register_function{
    function_name = 'GNODE_TOPO_DENY',
    callback = function(keys, args)
        if #keys < 1 or #args < 4 then return server.error_reply('GNODE_TOPO_DENY tk | caller target now_ms ttl_ms') end
        local ttl = tonumber(args[4])
        if not ttl or ttl <= 0 then return server.error_reply('ttl_ms must be > 0: a deny is always bounded') end
        server.call('ZADD', keys[1] .. ':deny', sfmt(tonumber(args[3]) + ttl), args[1] .. '>' .. args[2])
        return 'OK'
    end
}
server.register_function{
    function_name = 'GNODE_TOPO_UNDENY',
    callback = function(keys, args)
        if #keys < 1 or #args < 2 then return server.error_reply('GNODE_TOPO_UNDENY tk | caller target') end
        return server.call('ZREM', keys[1] .. ':deny', args[1] .. '>' .. args[2])
    end
}
-- GNODE_TOPO_CAN_REACH tk | caller target now_ms → {0|1, reason}. Fail closed.
server.register_function{
    function_name = 'GNODE_TOPO_CAN_REACH', flags = { 'no-writes' },
    callback = function(keys, args)
        if #keys < 1 or #args < 3 then return server.error_reply('GNODE_TOPO_CAN_REACH tk | caller target now_ms') end
        local tk, c, t, now = keys[1], args[1], args[2], tonumber(args[3])
        local lc, lt = server.call('ZSCORE', tk .. ':ix:layer', c), server.call('ZSCORE', tk .. ':ix:layer', t)
        if not lc or not lt then return { 0, 'undeclared' } end
        lc, lt = math.floor(tonumber(lc) / 2 ^ 40), math.floor(tonumber(lt) / 2 ^ 40)
        local pc = tonumber(server.call('ZSCORE', tk .. ':ix:fundamentality', c) or 0)
        local pt = tonumber(server.call('ZSCORE', tk .. ':ix:fundamentality', t) or 0)
        local below = lt < lc or (lt == lc and pt < pc)
        if not below and server.call('SISMEMBER', tk .. ':peers:' .. c, t) == 0 then return { 0, 'layer' } end
        local ec, et = server.call('HGET', tk .. ':env', c), server.call('HGET', tk .. ':env', t)
        if not ec or not et then return { 0, 'undeclared' } end
        if et ~= 'global' and et ~= ec then return { 0, 'env' } end
        for _, l in ipairs(server.call('SMEMBERS', tk .. ':req:' .. t)) do
            if server.call('SISMEMBER', tk .. ':holds:' .. c, l) == 0 then return { 0, 'label:' .. l } end
        end
        local d = server.call('ZSCORE', tk .. ':deny', c .. '>' .. t)
        if d and tonumber(d) > now then return { 0, 'deny' } end
        return { 1, 'reach' }
    end
}

-- GNODE_TOPO_HEALTH_DERIVE tk [lease_key] | now_s fresh_s [node_id]
-- Placement health from the heartbeat key it names; entity health = max over its placements;
-- a web placement (service 3.1) reads its node daemon's heartbeat (D13).
server.register_function{
    function_name = 'GNODE_TOPO_HEALTH_DERIVE',
    callback = function(keys, args)
        if #keys < 1 or #args < 2 then return server.error_reply('GNODE_TOPO_HEALTH_DERIVE tk [lease] | now_s fresh_s [node_id]') end
        local tk, now, fresh = keys[1], tonumber(args[1]), tonumber(args[2])
        local ok, why = lease_ok(keys, args[3]); if not ok then return server.error_reply(why) end
        local hbns = tag_of(tk) .. ':gnode:heartbeat:'
        local ent, nf, nt = {}, 0, 0
        for _, pid in ipairs(server.call('SMEMBERS', tk .. ':placements')) do
            local rec = load_record(tk, pid)
            if rec then
                local hb, env = rec.hb or '', rec.hbe or rec.env or 'production'
                local ofr = load_record(tk, rec.of or '')
                if ofr and ofr.sh == '3.1' then hb, env = 'gnode-daemon', 'all' end
                local h = 0
                if hb ~= '' then
                    h = HEALTH_DEAD
                    local raw = server.call('GET', hbns .. env .. ':' .. hb .. ':' .. (rec.on or ''))
                    if raw then
                        local okv, v = pcall(cjson.decode, raw)
                        if okv and v.ts and now - tonumber(v.ts) <= fresh then h = 1 end
                    end
                end
                nt = nt + 1; if h == 1 then nf = nf + 1 end
                rec.pd[18] = h; rec.pr[18] = sfmt(h * 2 ^ 64)
                rec.q.health = sfmt(h * Q48); rec.m.ds = now
                store(tk, rec); server.call('ZADD', tk .. ':ix:health', rec.q.health, pid)
                ring_sync(tk, rec)
                for _, e in ipairs({ rec.of, rec.on }) do if e and h > (ent[e] or -1) then ent[e] = h end end  -- max over placements; unknown (0) never outranks a measured value
            end
        end
        local ne = 0
        for e, h in pairs(ent) do
            local rec = load_record(tk, e)
            if rec then
                rec.pd[18] = h; rec.pr[18] = sfmt(h * 2 ^ 64); rec.q.health = sfmt(h * Q48); rec.m.ds = now
                store(tk, rec); server.call('ZADD', tk .. ':ix:health', rec.q.health, e); ne = ne + 1
            end
        end
        return { nf, nt, ne }
    end
}

-- GNODE_TOPO_VERIFY tk : the invariants, first violation or OK.
server.register_function{
    function_name = 'GNODE_TOPO_VERIFY', flags = { 'no-writes' },
    callback = function(keys, args)
        if #keys < 1 then return server.error_reply('GNODE_TOPO_VERIFY tk') end
        local tk = keys[1]
        if not is_constellation(tk) then return server.error_reply(tk .. ' is not a constellation topology') end
        local ids = server.call('HKEYS', tk .. ':entities')
        local ec = tonumber(server.call('HGET', tk .. ':meta', 'ec') or 0)
        if ec ~= #ids then return 'ec ' .. ec .. ' != entities ' .. #ids end
        local layer = {}
        for _, id in ipairs(server.call('ZRANGE', tk .. ':ix:layer', 0, -1)) do layer[id] = true end
        for _, id in ipairs(ids) do
            if not layer[id] then return 'entity ' .. id .. ' is not in ix:layer' end
            local rec = load_record(tk, id)
            if not rec then return 'entity ' .. id .. ' does not decode' end
            for axis, s in pairs(rec.q or {}) do
                if server.call('ZSCORE', tk .. ':ix:' .. axis, id) ~= s then return 'entity ' .. id .. ' axis ' .. axis .. ' index disagrees with the record' end
            end
            if rec.of and server.call('HEXISTS', tk .. ':entities', rec.of) ~= 1 then return 'placement ' .. id .. ' of unknown ' .. rec.of end
            if rec.on and server.call('HEXISTS', tk .. ':entities', rec.on) ~= 1 then return 'placement ' .. id .. ' on unknown ' .. rec.on end
        end
        for id in pairs(layer) do if server.call('HEXISTS', tk .. ':entities', id) ~= 1 then return 'ix:layer member ' .. id .. ' has no entity' end end
        local edges = server.call('HKEYS', tk .. ':edges')
        for _, e in ipairs(edges) do
            local f, t = string.match(e, '^(.-):(.+)$')
            local lf, lt = server.call('ZSCORE', tk .. ':ix:layer', f), server.call('ZSCORE', tk .. ':ix:layer', t)
            if not lf or not lt then return 'edge ' .. e .. ' has an unknown end' end
            local pf, pt = tonumber(server.call('ZSCORE', tk .. ':ix:fundamentality', f) or 0), tonumber(server.call('ZSCORE', tk .. ':ix:fundamentality', t) or 0)
            lf, lt = math.floor(tonumber(lf) / 2 ^ 40), math.floor(tonumber(lt) / 2 ^ 40)
            if lf < lt or (lf == lt and pf <= pt) then return 'edge ' .. e .. ' violates phi' end
        end
        local xc = tonumber(server.call('HGET', tk .. ':meta', 'xc') or 0)
        if xc ~= #edges then return 'xc ' .. xc .. ' != edges ' .. #edges end
        for _, pid in ipairs(server.call('SMEMBERS', tk .. ':placements')) do
            local rec = load_record(tk, pid)
            if not rec then return 'placement ' .. pid .. ' has no record' end
            if rec.sh == '4.2' then
                local inring = server.call('ZSCORE', tk .. ':pl:' .. rec.of, pid) ~= false
                local should = on_ring(rec.pd[18] or 0, rec.pd[19])
                if inring ~= should then return 'placement ' .. pid .. ' ring membership disagrees with health/lifecycle' end
            end
        end
        return 'OK'
    end
}
