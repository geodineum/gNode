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
--   tk  = topology_key      # Full key like "{site_id}:{name}"
--   si  = site_id           # Site identifier for isolation
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
--   {site_id}:gnode:topo:registry              → Hash: topology_id → JSON metadata
--   {site_id}:gnode:topo:by_type:{type}        → Set: topology_ids of this type
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

--- Build registry key for site
-- @param site_id string Site identifier
-- @return string Registry key
local function build_registry_key(site_id)
    return site_id .. REGISTRY_SUFFIX
end

--- Build type index key for site
-- @param site_id string Site identifier
-- @param topo_type string Topology type
-- @return string Type index key
local function build_type_index_key(site_id, topo_type)
    return site_id .. TYPE_INDEX_PREFIX .. topo_type
end

-- ============================================================================
-- GNODE_TOPO_CREATE
-- Create a new topology with metadata in the registry
-- Daemon calls this after validating parameters
-- ============================================================================
server.register_function{
    function_name = 'GNODE_TOPO_CREATE',
    callback = function(keys, args)
        -- keys[1] = site_id (for keyspace isolation)
        -- args[1] = topology_key (full key like "{site_id}:{name}")
        -- args[2] = definition_json (name, constraint_type, axis_semantics, etc)

        if #keys < 1 then
            return server.error_reply("Missing site_id key")
        end
        if #args < 2 then
            return server.error_reply("Missing topology_key or definition_json")
        end

        local site_id = keys[1]
        local topology_key = args[1]
        local definition_json = args[2]

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
        local registry_key = build_registry_key(site_id)
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
            local type_key = build_type_index_key(site_id, metadata.tt)
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
-- GNODE_ENSURE_TOPOLOGY
-- Ensure the default 23D service discovery topology exists for a site.
-- Creates the topology if it doesn't exist, returns existing key otherwise.
--
-- This is the canonical topology for service registration and discovery.
-- It uses a 23-dimensional capability space (see CLAUDE.md §10A):
--   Layer 1 (0-3):   Interface Identity
--   Layer 2 (4-6):   Access Control
--   Layer 3 (7):     Service Scope
--   Layer 4 (8-10):  Functional Domain
--   Layer 5 (11-13): Performance Profile
--   Layer 6 (14-15): Workflow Context
--   Layer 7 (16):    Runtime State (current_load → z_score)
--   Layer 8 (17-18): Classification (service_tier, environment)
--   Layer 9 (19-21): Visual Topology (user_x, user_y, user_z)
--   Layer 10 (22):   Temporal (registration_order)
--
-- Bucket keys are 76 characters (19 discovery dims × 4 chars) computed by Rust Q32.32
-- Visual/temporal dims (19-22) are stored but NOT included in bucket key
-- ============================================================================
server.register_function{
    function_name = 'GNODE_ENSURE_TOPOLOGY',
    callback = function(keys, args)
        -- keys[1] = site_id (for keyspace isolation)

        if #keys < 1 then
            return server.error_reply("Missing site_id key")
        end

        local site_id = keys[1]
        local topology_key = "{" .. site_id .. "}:gnode:services"

        -- Check if topology already exists in registry AND data is actually present.
        -- Both conditions must be true — registry entry alone is not sufficient.
        -- This prevents a stale registry entry (from a failed registration) from
        -- blocking future ensure calls. Truly idempotent: safe to call repeatedly.
        local registry_key = build_registry_key(site_id)
        local in_registry = server.call('HEXISTS', registry_key, topology_key)
        local has_data = server.call('EXISTS', topology_key .. ':meta')

        if in_registry == 1 and has_data == 1 then
            -- Topology exists in registry AND data is present — genuine existing topology
            local result = {
                ok = true,
                tk = topology_key,
                cr = false
            }
            local result_json, _ = safe_json_encode(result)
            return result_json
        end

        -- Stale registry entry without data — clean it up before recreating
        if in_registry == 1 and has_data == 0 then
            server.call('HDEL', registry_key, topology_key)
        end

        -- Create new 23D service topology
        local now = get_timestamp()
        local metadata = {
            tk = topology_key,       -- topology_key
            si = site_id,            -- site_id
            n = "services",          -- name
            ct = CONSTRAINT_TYPES.none,  -- no Z-monotonicity for service discovery
            tt = "service_discovery",    -- topology_type
            d = "Default 23D service discovery topology (stateless architecture)",
            ax = {
                -- 23D capability space projected to 3D for visualization
                -- Discovery uses 19D bucket keys (76 chars), dims 19-22 are storage-only
                x = { n = "interface_access", d = "Interface + Access layers (dims 0-6)" },
                y = { n = "scope_domain", d = "Scope + Domain layers (dims 7-10)" },
                z = { n = "perf_class", d = "Perf + Workflow + Runtime + Classification (dims 11-18)" }
            },
            dm = 23,                 -- dimensions (23D capability space, 19D for discovery)
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
        local type_key = build_type_index_key(site_id, "service_discovery")
        server.call('SADD', type_key, topology_key)

        -- Return success
        local result = {
            ok = true,
            tk = topology_key,  -- topology_key
            cr = true,          -- created (true = newly created)
            ts = now            -- timestamp
        }

        local result_json, _ = safe_json_encode(result)
        return result_json
    end
}

-- ============================================================================
-- GNODE_REGISTER_CAPABILITY_VECTOR
-- Register entity with PRE-COMPUTED bucket_key and z_score from daemon
-- Daemon computes these using Q32.32 fixed-point for multi-node determinism
-- ============================================================================
server.register_function{
    function_name = 'GNODE_REGISTER_CAPABILITY_VECTOR',
    callback = function(keys, args)
        -- keys[1] = topology_key
        -- args[1] = entity_id
        -- args[2] = entity_json (position, metadata - from daemon)
        -- args[3] = bucket_key (pre-computed by daemon using Q32.32)
        -- args[4] = z_score (pre-computed by daemon as integer)
        -- args[5] = snapshot_key (OPTIONAL) — global derived-snapshot hash
        --           ({ns}:gnode:topology:services). When present, this primitive
        --           also maintains a {point, metadata} projection there, so EVERY
        --           registration transport (handler, tool/manifest, provision)
        --           keeps the PHP-facing snapshot current.

        if #keys < 1 then
            return server.error_reply("Missing topology_key")
        end
        if #args < 4 then
            return server.error_reply("Missing entity_id, entity_json, bucket_key, or z_score")
        end

        local topology_key = keys[1]
        local entity_id = args[1]
        local entity_json = args[2]
        local bucket_key = args[3]
        local z_score = tonumber(args[4])

        if not entity_id or entity_id == "" then
            return server.error_reply("entity_id cannot be empty")
        end
        if not bucket_key or bucket_key == "" then
            return server.error_reply("bucket_key cannot be empty (must be pre-computed by daemon)")
        end
        if not z_score then
            return server.error_reply("z_score must be a valid number (must be pre-computed by daemon)")
        end

        -- Verify topology exists
        local meta_json = server.call('HGET', topology_key .. ':meta', 'data')
        if not meta_json then
            return server.error_reply("Topology not found: " .. topology_key)
        end

        -- Check for existing entity
        local existing = server.call('HEXISTS', topology_key .. ':entities', entity_id)
        local is_update = (existing == 1)

        -- If updating, remove from old voxel index
        if is_update then
            local old_json = server.call('HGET', topology_key .. ':entities', entity_id)
            if old_json then
                local old_entity, _ = safe_json_decode(old_json)
                if old_entity and old_entity.bk then  -- bk = bucket_key
                    server.call('SREM', topology_key .. ':voxel:' .. old_entity.bk, entity_id)
                end
            end
        end

        -- Parse entity data to add computed fields
        local entity, decode_err = safe_json_decode(entity_json)
        if not entity then
            return server.error_reply("Invalid entity_json: " .. (decode_err or "unknown"))
        end

        -- Auto-inject registration_order for NEW entities (dim 22)
        -- Uses monotonic counter (ro) on topology metadata - NOT entity_count
        -- On updates, preserve the original registration_order
        if not is_update then
            local ro = server.call('HINCRBY', topology_key .. ':meta', 'ro', 1)
            local ro_normalized = math.min(ro / 10000.0, 1.0)
            local ro_raw = math.floor(ro_normalized * 4294967296)  -- Q32.32: 2^32
            -- Inject into pr (point_raw: Q32.32 i64) and pd (point_display: float) at index 23 (1-based)
            if entity.pr then
                entity.pr[23] = ro_raw
            end
            if entity.pd then
                entity.pd[23] = ro_normalized
            end
        else
            -- Preserve existing registration_order from old entity
            local old_json = server.call('HGET', topology_key .. ':entities', entity_id)
            if old_json then
                local old_entity, _ = safe_json_decode(old_json)
                if old_entity then
                    if old_entity.pd and entity.pd then
                        entity.pd[23] = old_entity.pd[23]
                    end
                    if old_entity.pr and entity.pr then
                        entity.pr[23] = old_entity.pr[23]
                    end
                end
            end
        end

        -- Add computed fields from daemon (abbreviated)
        entity.id = entity_id
        entity.bk = bucket_key   -- bucket_key
        entity.zs = z_score      -- z_score
        entity.ra = get_timestamp()  -- registered_at

        -- Encode final entity
        local final_json, encode_err = safe_json_encode(entity)
        if not final_json then
            return server.error_reply("Failed to encode entity: " .. (encode_err or "unknown"))
        end

        -- Store entity in hash
        server.call('HSET', topology_key .. ':entities', entity_id, final_json)

        -- Add to voxel index (O(1) lookup by bucket)
        server.call('SADD', topology_key .. ':voxel:' .. bucket_key, entity_id)

        -- Add to z_order sorted set (for Z-range queries and DAG ordering)
        server.call('ZADD', topology_key .. ':z_order', z_score, entity_id)

        -- Update entity count if new
        if not is_update then
            server.call('HINCRBY', topology_key .. ':meta', 'ec', 1)  -- ec = entity_count
        end

        -- Index schema keys for schema↔topology cross-reference
        -- If entity metadata contains schema_keys (list of "{component}:{contract}")
        -- store a reverse lookup: gnode:schema:entity:{entity_id} → schema keys
        if entity.m and entity.m.schema_keys then
            local sk_key = 'gnode:schema:entity:' .. entity_id
            -- Clear old mappings on update
            server.call('DEL', sk_key)
            for _, sk in ipairs(entity.m.schema_keys) do
                server.call('SADD', sk_key, sk)
            end
        end

        -- (B) Derived snapshot projection — maintained here so ALL register
        -- transports (not just the stream-command handler) keep the PHP-facing
        -- {ns}:gnode:topology:services hash current. Value = {point, metadata}.
        local snapshot_key = args[5]
        if snapshot_key and snapshot_key ~= "" then
            local snap_json, _ = safe_json_encode({ point = entity.pd, metadata = entity.m })
            if snap_json then
                server.call('HSET', snapshot_key, entity_id, snap_json)
            end
        end

        -- Return success (abbreviated)
        local result = {
            ok = true,
            eid = entity_id,
            bk = bucket_key,
            zs = z_score,
            upd = is_update
        }

        local result_json, _ = safe_json_encode(result)
        return result_json
    end
}

-- ============================================================================
-- GNODE_DEREGISTER_CAPABILITY_VECTOR
-- Remove entity from all indexes
-- ============================================================================
server.register_function{
    function_name = 'GNODE_DEREGISTER_CAPABILITY_VECTOR',
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

        -- Get entity to find bucket_key
        local entity_json = server.call('HGET', topology_key .. ':entities', entity_id)
        if not entity_json then
            return server.error_reply("Entity not found: " .. entity_id)
        end

        local entity, _ = safe_json_decode(entity_json)
        local bucket_key = entity and entity.bk  -- bk = bucket_key

        -- Remove from entities hash
        server.call('HDEL', topology_key .. ':entities', entity_id)

        -- (B) snapshot mirror removal (optional global snapshot key, args[2])
        local snapshot_key = args[2]
        if snapshot_key and snapshot_key ~= "" then
            server.call('HDEL', snapshot_key, entity_id)
        end

        -- Remove from voxel index
        if bucket_key then
            server.call('SREM', topology_key .. ':voxel:' .. bucket_key, entity_id)
        end

        -- Remove from z_order
        server.call('ZREM', topology_key .. ':z_order', entity_id)

        -- Get and remove all outgoing edges
        local outgoing = server.call('SMEMBERS', topology_key .. ':out:' .. entity_id) or {}
        for _, target_id in ipairs(outgoing) do
            local edge_key = entity_id .. ':' .. target_id
            server.call('HDEL', topology_key .. ':edges', edge_key)
            server.call('SREM', topology_key .. ':in:' .. target_id, entity_id)
            server.call('HINCRBY', topology_key .. ':meta', 'xc', -1)  -- xc = edge_count
        end

        -- Get and remove all incoming edges
        local incoming = server.call('SMEMBERS', topology_key .. ':in:' .. entity_id) or {}
        for _, source_id in ipairs(incoming) do
            local edge_key = source_id .. ':' .. entity_id
            server.call('HDEL', topology_key .. ':edges', edge_key)
            server.call('SREM', topology_key .. ':out:' .. source_id, entity_id)
            server.call('HINCRBY', topology_key .. ':meta', 'xc', -1)  -- xc = edge_count
        end

        -- Remove edge sets for this entity
        server.call('DEL', topology_key .. ':out:' .. entity_id)
        server.call('DEL', topology_key .. ':in:' .. entity_id)

        -- Decrement entity count
        server.call('HINCRBY', topology_key .. ':meta', 'ec', -1)  -- ec = entity_count

        -- Return success (abbreviated)
        local result = {
            ok = true,
            eid = entity_id,
            xr = #outgoing + #incoming  -- xr = edges_removed
        }

        local result_json, _ = safe_json_encode(result)
        return result_json
    end
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
-- GNODE_TOPO_QUERY_VOXEL
-- Get entities in a voxel (bucket_key computed by daemon)
-- O(1) lookup via ValKey Set
-- ============================================================================
server.register_function{
    function_name = 'GNODE_TOPO_QUERY_VOXEL',
    callback = function(keys, args)
        -- keys[1] = topology_key
        -- args[1] = bucket_key (pre-computed by daemon using Q32.32)
        -- args[2] = include_data (optional, "true" to return full entity data)

        if #keys < 1 then
            return server.error_reply("Missing topology_key")
        end
        if #args < 1 then
            return server.error_reply("Missing bucket_key (must be pre-computed by daemon)")
        end

        local topology_key = keys[1]
        local bucket_key = args[1]
        local include_data = args[2] == "true"

        -- Get entity IDs in this voxel
        local entity_ids = server.call('SMEMBERS', topology_key .. ':voxel:' .. bucket_key) or {}

        -- Build result (abbreviated)
        local result
        if include_data and #entity_ids > 0 then
            -- Fetch full entity data
            local entities = {}
            for _, entity_id in ipairs(entity_ids) do
                local entity_json = server.call('HGET', topology_key .. ':entities', entity_id)
                if entity_json then
                    local entity, _ = safe_json_decode(entity_json)
                    if entity then
                        entities[entity_id] = entity
                    end
                end
            end
            result = {
                bk = bucket_key,
                cnt = #entity_ids,
                eids = entity_ids,
                ents = entities
            }
        else
            result = {
                bk = bucket_key,
                cnt = #entity_ids,
                eids = entity_ids
            }
        end

        local result_json, _ = safe_json_encode(result)
        return result_json
    end,
    flags = {'no-writes'}
}

-- ============================================================================
-- GNODE_TOPO_QUERY_Z_RANGE
-- Get entities within a Z score range
-- Uses ZRANGEBYSCORE for efficient range queries
-- ============================================================================
server.register_function{
    function_name = 'GNODE_TOPO_QUERY_Z_RANGE',
    callback = function(keys, args)
        -- keys[1] = topology_key
        -- args[1] = min_z_score (pre-computed by daemon, or "-inf")
        -- args[2] = max_z_score (pre-computed by daemon, or "+inf")
        -- args[3] = include_data (optional, "true" to return full entity data)
        -- args[4] = limit (optional, max entities to return)

        if #keys < 1 then
            return server.error_reply("Missing topology_key")
        end

        local topology_key = keys[1]
        local min_score = args[1] or "-inf"
        local max_score = args[2] or "+inf"
        local include_data = args[3] == "true"
        local limit = tonumber(args[4])

        -- Get entity IDs in Z range
        local entity_ids
        if limit and limit > 0 then
            entity_ids = server.call('ZRANGEBYSCORE', topology_key .. ':z_order',
                min_score, max_score, 'LIMIT', 0, limit) or {}
        else
            entity_ids = server.call('ZRANGEBYSCORE', topology_key .. ':z_order',
                min_score, max_score) or {}
        end

        -- Build result (abbreviated)
        local result
        if include_data and #entity_ids > 0 then
            -- Fetch full entity data
            local entities = {}
            for _, entity_id in ipairs(entity_ids) do
                local entity_json = server.call('HGET', topology_key .. ':entities', entity_id)
                if entity_json then
                    local entity, _ = safe_json_decode(entity_json)
                    if entity then
                        entities[entity_id] = entity
                    end
                end
            end
            result = {
                zr = { mn = min_score, mx = max_score },  -- z_range
                cnt = #entity_ids,
                eids = entity_ids,
                ents = entities
            }
        else
            result = {
                zr = { mn = min_score, mx = max_score },  -- z_range
                cnt = #entity_ids,
                eids = entity_ids
            }
        end

        local result_json, _ = safe_json_encode(result)
        return result_json
    end,
    flags = {'no-writes'}
}

-- ============================================================================
-- GNODE_TOPO_Z_ORDER
-- Get entities in Z-ascending order (useful for DAG load order)
-- ============================================================================
server.register_function{
    function_name = 'GNODE_TOPO_Z_ORDER',
    callback = function(keys, args)
        -- keys[1] = topology_key
        -- args[1] = limit (optional)
        -- args[2] = offset (optional)
        -- args[3] = descending (optional, "true" for high-to-low)

        if #keys < 1 then
            return server.error_reply("Missing topology_key")
        end

        local topology_key = keys[1]
        local limit = tonumber(args[1])
        local offset = tonumber(args[2]) or 0
        local descending = args[3] == "true"

        local entity_ids
        local z_order_key = topology_key .. ':z_order'

        if descending then
            if limit and limit > 0 then
                entity_ids = server.call('ZREVRANGE', z_order_key, offset, offset + limit - 1) or {}
            else
                entity_ids = server.call('ZREVRANGE', z_order_key, 0, -1) or {}
            end
        else
            if limit and limit > 0 then
                entity_ids = server.call('ZRANGE', z_order_key, offset, offset + limit - 1) or {}
            else
                entity_ids = server.call('ZRANGE', z_order_key, 0, -1) or {}
            end
        end

        -- Return result (abbreviated)
        local result = {
            ord = descending and "desc" or "asc",  -- order
            off = offset,
            cnt = #entity_ids,
            eids = entity_ids
        }

        local result_json, _ = safe_json_encode(result)
        return result_json
    end,
    flags = {'no-writes'}
}

-- ============================================================================
-- GNODE_TOPO_GET_ENTITIES
-- Batch get entity data by IDs
-- ============================================================================
server.register_function{
    function_name = 'GNODE_TOPO_GET_ENTITIES',
    callback = function(keys, args)
        -- keys[1] = topology_key
        -- args[1] = entity_ids_json (JSON array of entity IDs)
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

        -- Decode entity IDs
        local entity_ids, decode_err = safe_json_decode(ids_json)
        if not entity_ids then
            return server.error_reply("Invalid entity_ids JSON: " .. (decode_err or "unknown"))
        end

        if type(entity_ids) ~= "table" then
            return server.error_reply("entity_ids must be a JSON array")
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
-- GNODE_TOPO_CHAIN
-- Get transitive chain from entity (BFS traversal)
-- Used for dependency chains, impact analysis
-- ============================================================================
server.register_function{
    function_name = 'GNODE_TOPO_CHAIN',
    callback = function(keys, args)
        -- keys[1] = topology_key
        -- args[1] = entity_id (starting point)
        -- args[2] = direction ("outgoing" for dependencies, "incoming" for dependents)
        -- args[3] = max_depth (optional, default unlimited)

        if #keys < 1 then
            return server.error_reply("Missing topology_key")
        end
        if #args < 2 then
            return server.error_reply("Missing entity_id or direction")
        end

        local topology_key = keys[1]
        local start_id = args[1]
        local direction = args[2]
        local max_depth = tonumber(args[3]) or 100  -- safety limit

        if direction ~= "outgoing" and direction ~= "incoming" then
            return server.error_reply("direction must be 'outgoing' or 'incoming'")
        end

        -- Verify starting entity exists
        local exists = server.call('HEXISTS', topology_key .. ':entities', start_id)
        if exists ~= 1 then
            return server.error_reply("Starting entity not found: " .. start_id)
        end

        -- BFS traversal
        local visited = {}
        local chain = {}
        local by_depth = {}
        local queue = {{id = start_id, depth = 0}}
        visited[start_id] = true

        local set_suffix = direction == "outgoing" and ":out:" or ":in:"

        while #queue > 0 do
            local current = table.remove(queue, 1)

            if current.depth < max_depth then
                local neighbors = server.call('SMEMBERS',
                    topology_key .. set_suffix .. current.id) or {}

                for _, neighbor_id in ipairs(neighbors) do
                    if not visited[neighbor_id] then
                        visited[neighbor_id] = true
                        local next_depth = current.depth + 1

                        table.insert(chain, neighbor_id)

                        -- Group by depth
                        if not by_depth[next_depth] then
                            by_depth[next_depth] = {}
                        end
                        table.insert(by_depth[next_depth], neighbor_id)

                        table.insert(queue, {id = neighbor_id, depth = next_depth})
                    end
                end
            end
        end

        -- Return result (abbreviated)
        local result = {
            st = start_id,        -- start
            dir = direction,
            cnt = #chain,
            md = #by_depth,       -- max_depth_reached
            ch = chain,           -- chain
            bd = by_depth         -- by_depth
        }

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
        -- keys[1] = site_id
        -- args[1] = topology_type (optional, filter by type)

        if #keys < 1 then
            return server.error_reply("Missing site_id")
        end

        local site_id = keys[1]
        local filter_type = args[1]

        local topology_keys

        if filter_type and filter_type ~= "" then
            -- Get from type index
            local type_key = build_type_index_key(site_id, filter_type)
            topology_keys = server.call('SMEMBERS', type_key) or {}
        else
            -- Get all from registry
            local registry_key = build_registry_key(site_id)
            topology_keys = server.call('HKEYS', registry_key) or {}
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
        -- keys[1] = site_id
        -- args[1] = topology_key
        -- args[2] = "CONFIRM" (required safety flag)

        if #keys < 1 then
            return server.error_reply("Missing site_id")
        end
        if #args < 2 then
            return server.error_reply("Missing topology_key or CONFIRM flag")
        end

        local site_id = keys[1]
        local topology_key = args[1]
        local confirm = args[2]

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
        local registry_key = build_registry_key(site_id)
        server.call('HDEL', registry_key, topology_key)

        -- Remove from type index
        if meta and meta.tt then  -- tt = topology_type
            local type_key = build_type_index_key(site_id, meta.tt)
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
-- GNODE_TOPO_UPDATE_META
-- Update topology metadata
-- ============================================================================
server.register_function{
    function_name = 'GNODE_TOPO_UPDATE_META',
    callback = function(keys, args)
        -- keys[1] = site_id
        -- args[1] = topology_key
        -- args[2] = updates_json (partial update fields)

        if #keys < 1 then
            return server.error_reply("Missing site_id")
        end
        if #args < 2 then
            return server.error_reply("Missing topology_key or updates_json")
        end

        local site_id = keys[1]
        local topology_key = args[1]
        local updates_json = args[2]

        -- Get existing metadata
        local meta_json = server.call('HGET', topology_key .. ':meta', 'data')
        if not meta_json then
            return server.error_reply("Topology not found: " .. topology_key)
        end

        local meta, decode_err = safe_json_decode(meta_json)
        if not meta then
            return server.error_reply("Failed to decode metadata: " .. (decode_err or "unknown"))
        end

        -- Parse updates
        local updates, update_err = safe_json_decode(updates_json)
        if not updates then
            return server.error_reply("Invalid updates JSON: " .. (update_err or "unknown"))
        end

        -- Apply updates (only allowed fields - map verbose to abbreviated)
        -- Input can use verbose names, we store abbreviated
        if updates.name ~= nil or updates.n ~= nil then
            meta.n = updates.name or updates.n
        end
        if updates.description ~= nil or updates.d ~= nil then
            meta.d = updates.description or updates.d
        end
        if updates.axis_semantics ~= nil or updates.ax ~= nil then
            meta.ax = updates.axis_semantics or updates.ax
        end

        meta.ua = get_timestamp()  -- updated_at

        -- Save updated metadata
        local new_meta_json, encode_err = safe_json_encode(meta)
        if not new_meta_json then
            return server.error_reply("Failed to encode metadata: " .. (encode_err or "unknown"))
        end

        server.call('HSET', topology_key .. ':meta', 'data', new_meta_json)

        -- Update registry
        local registry_key = build_registry_key(site_id)
        server.call('HSET', registry_key, topology_key, new_meta_json)

        -- Return result (abbreviated)
        local result = {
            ok = true,
            tk = topology_key,
            ua = meta.ua  -- updated_at
        }

        local result_json, _ = safe_json_encode(result)
        return result_json
    end
}

-- ============================================================================
-- GNODE_TOPO_CHECK_STALENESS
-- Scan all entities in a topology and detect stale/dead services.
-- TOOL-tier entities (service_tier <= 0.15) are skipped (no heartbeat expected).
-- Entities without a 'la' (last_active) field are skipped (freshly registered).
--
-- Stale:        now - la > staleness_threshold → mark metadata stale=true
-- Deregister:   now - la > deregister_threshold → remove entity from all indexes
-- ============================================================================
server.register_function{
    function_name = 'GNODE_TOPO_CHECK_STALENESS',
    callback = function(keys, args)
        -- keys[1] = topology_key
        -- args[1] = staleness_threshold_s (seconds)
        -- args[2] = deregister_threshold_s (seconds)
        -- args[3] = current_timestamp_s (unix seconds, from daemon)

        if #keys < 1 then
            return server.error_reply("Missing topology_key")
        end
        if #args < 3 then
            return server.error_reply("Missing staleness_threshold_s, deregister_threshold_s, or current_timestamp_s")
        end

        local topology_key = keys[1]
        local staleness_threshold = tonumber(args[1]) or 60
        local deregister_threshold = tonumber(args[2]) or 300
        local now = tonumber(args[3]) or 0

        if now == 0 then
            return server.error_reply("Invalid current_timestamp_s")
        end

        -- Get all entity IDs
        local entity_ids = server.call('HKEYS', topology_key .. ':entities')
        if not entity_ids or #entity_ids == 0 then
            local empty_result, _ = safe_json_encode({
                ok = true,
                checked = 0,
                stale = 0,
                deregistered = {},
                active = 0,
                skipped = 0
            })
            return empty_result
        end

        local checked = 0
        local stale_count = 0
        local active_count = 0
        local skipped = 0
        local deregistered = {}

        for _, entity_id in ipairs(entity_ids) do
            local entity_json = server.call('HGET', topology_key .. ':entities', entity_id)
            if entity_json then
                local entity, decode_err = safe_json_decode(entity_json)
                if entity then
                    -- Check tier from pd[17] (dimension 17 = service_tier, 0-indexed → Lua pd[18])
                    local tier_val = 0.0
                    if entity.pd and type(entity.pd) == "table" then
                        tier_val = tonumber(entity.pd[18]) or 0.0  -- Lua 1-indexed
                    end

                    -- Skip TOOL tier (value <= 0.15, covers TOOL=0.10)
                    if tier_val <= 0.15 then
                        skipped = skipped + 1
                    else
                        -- Check last_active (la) field
                        local la = tonumber(entity.la)
                        if not la then
                            -- No last_active: entity never had a health update, skip
                            skipped = skipped + 1
                        else
                            checked = checked + 1
                            local age = now - la

                            if age > deregister_threshold then
                                -- DEREGISTER: entity has been dead too long
                                -- Inline deregistration (same as GNODE_DEREGISTER_CAPABILITY_VECTOR)
                                local bucket_key = entity.bk

                                -- Remove from entities hash
                                server.call('HDEL', topology_key .. ':entities', entity_id)

                                -- Remove from voxel index
                                if bucket_key then
                                    server.call('SREM', topology_key .. ':voxel:' .. bucket_key, entity_id)
                                end

                                -- Remove from z_order
                                server.call('ZREM', topology_key .. ':z_order', entity_id)

                                -- Remove outgoing edges
                                local outgoing = server.call('SMEMBERS', topology_key .. ':out:' .. entity_id) or {}
                                for _, target_id in ipairs(outgoing) do
                                    local edge_key = entity_id .. ':' .. target_id
                                    server.call('HDEL', topology_key .. ':edges', edge_key)
                                    server.call('SREM', topology_key .. ':in:' .. target_id, entity_id)
                                    server.call('HINCRBY', topology_key .. ':meta', 'xc', -1)
                                end

                                -- Remove incoming edges
                                local incoming = server.call('SMEMBERS', topology_key .. ':in:' .. entity_id) or {}
                                for _, source_id in ipairs(incoming) do
                                    local edge_key = source_id .. ':' .. entity_id
                                    server.call('HDEL', topology_key .. ':edges', edge_key)
                                    server.call('SREM', topology_key .. ':out:' .. source_id, entity_id)
                                    server.call('HINCRBY', topology_key .. ':meta', 'xc', -1)
                                end

                                -- Remove edge sets
                                server.call('DEL', topology_key .. ':out:' .. entity_id)
                                server.call('DEL', topology_key .. ':in:' .. entity_id)

                                -- Decrement entity count
                                server.call('HINCRBY', topology_key .. ':meta', 'ec', -1)

                                table.insert(deregistered, entity_id)

                            elseif age > staleness_threshold then
                                -- STALE: mark entity metadata with stale flag
                                if not entity.m then
                                    entity.m = {}
                                end
                                if type(entity.m) == "table" then
                                    entity.m.stale = true
                                    entity.m.stale_since = now - age + staleness_threshold
                                end

                                local updated_json, _ = safe_json_encode(entity)
                                if updated_json then
                                    server.call('HSET', topology_key .. ':entities', entity_id, updated_json)
                                end
                                stale_count = stale_count + 1
                            else
                                -- ACTIVE: clear stale flag if previously set
                                if entity.m and type(entity.m) == "table" and entity.m.stale then
                                    entity.m.stale = nil
                                    entity.m.stale_since = nil
                                    local updated_json, _ = safe_json_encode(entity)
                                    if updated_json then
                                        server.call('HSET', topology_key .. ':entities', entity_id, updated_json)
                                    end
                                end
                                active_count = active_count + 1
                            end
                        end
                    end
                end
            end
        end

        local result = {
            ok = true,
            checked = checked,
            stale = stale_count,
            deregistered = deregistered,
            active = active_count,
            skipped = skipped
        }

        local result_json, _ = safe_json_encode(result)
        return result_json
    end
}

-- ============================================================================
-- GNODE_TOPO_FIND_ENTITY_SITE
-- Search across multiple site topologies to find which site owns an entity.
-- Used by the relay router to resolve entity_id → site_id for inter-service relay.
--
-- Uses 0 keys (reads from multiple key patterns via gnode_daemon ACL ~*).
-- ============================================================================
server.register_function{
    function_name = 'GNODE_TOPO_FIND_ENTITY_SITE',
    callback = function(keys, args)
        -- args[1] = entity_id
        -- args[2] = site_ids_json (JSON array of site_ids to search)

        if #args < 2 then
            return server.error_reply("Missing entity_id or site_ids_json")
        end

        local entity_id = args[1]
        local site_ids_json = args[2]

        if not entity_id or entity_id == "" then
            return server.error_reply("entity_id cannot be empty")
        end

        -- Decode site IDs array
        local site_ids, decode_err = safe_json_decode(site_ids_json)
        if not site_ids then
            return server.error_reply("Invalid site_ids JSON: " .. (decode_err or "unknown"))
        end

        if type(site_ids) ~= "table" then
            return server.error_reply("site_ids must be a JSON array")
        end

        -- Search each site's services topology
        for _, site_id in ipairs(site_ids) do
            local topology_key = "{" .. site_id .. "}:gnode:services"
            local exists = server.call('HEXISTS', topology_key .. ':entities', entity_id)
            if exists == 1 then
                local result = {
                    ok = true,
                    site_id = site_id,
                    eid = entity_id
                }
                local result_json, _ = safe_json_encode(result)
                return result_json
            end
        end

        -- Entity not found in any topology
        local result = {
            ok = false,
            error = "Entity not found in any topology",
            eid = entity_id,
            searched = #site_ids
        }
        local result_json, _ = safe_json_encode(result)
        return result_json
    end,
    flags = {'no-writes'}
}
