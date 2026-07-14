#!lua name=gnode_resilience

--[[
  gNode RESILIENCE Functions
  Resilience patterns for distributed systems

  Features:
  - Advanced Circuit Breaker (sliding window, half-open state)
  - Request Idempotency (deduplication)
  - Cache Stampede Prevention (probabilistic early expiration)
  - Distributed Leader Election
  - Retry Budget Tracking

  All functions include proper site isolation and metrics tracking.
]]

-- Note: PRNG initialization moved to lazy init inside functions
-- server.call() cannot be used at module load time in ValKey functions
local prng_initialized = false
local function ensure_prng_init()
    if not prng_initialized then
        math.randomseed(os.time() + os.clock() * 1000000)
        prng_initialized = true
    end
end

-- Safe JSON encode
local function safe_json_encode(value)
    local ok, result = pcall(cjson.encode, value)
    if ok then
        return result
    else
        return '{"error":"encode_error"}'
    end
end

-- Safe JSON decode
local function safe_json_decode(str)
    if not str or str == "" then
        return nil, "Empty or nil JSON string"
    end
    local ok, result = pcall(cjson.decode, str)
    if ok then
        return result, nil
    else
        return nil, "JSON decode error: " .. tostring(result)
    end
end

-- Helper: Build namespaced key
local function build_key(key, site_id, prefix)
    if not site_id or site_id == "" then
        site_id = "default"
    end
    prefix = prefix or "resilience"
    if key:find("^{" .. site_id .. "}") then
        return key
    end
    return '{' .. site_id .. '}:' .. prefix .. ':' .. key
end

-- Helper: Get current timestamp in milliseconds
local function get_time_ms()
    local time = server.call('TIME')
    return tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
end

-- Helper: Track metrics
local function track_metric(site_id, metric, value)
    if not site_id or site_id == "" then return end
    local metrics_key = '{' .. site_id .. '}:metrics:resilience'
    server.call('HINCRBY', metrics_key, metric, value or 1)
end

--[[
  ============================================================================
  ADVANCED CIRCUIT BREAKER

  Implements a tested circuit breaker with:
  - Sliding window failure tracking (not just counters)
  - Three states: CLOSED, OPEN, HALF_OPEN
  - Configurable failure threshold, window size, recovery timeout
  - Gradual recovery in half-open state
  ============================================================================
]]

-- Circuit Breaker: Check state and record call
-- Returns: { state: "CLOSED"|"OPEN"|"HALF_OPEN", allowed: boolean, stats: {...} }
server.register_function{
    function_name = 'GNODE_RESILIENCE_CIRCUIT_CHECK',
    callback = function(keys, args)
        if #keys < 1 then
            return server.error_reply("Missing circuit key")
        end

        local circuit_name = keys[1]
        local site_id = args[1] or "default"
        local window_ms = tonumber(args[2]) or 60000      -- 60 second window
        local failure_threshold = tonumber(args[3]) or 5  -- 5 failures to open
        local recovery_ms = tonumber(args[4]) or 30000    -- 30 second recovery
        local half_open_max = tonumber(args[5]) or 3      -- 3 test requests in half-open

        local circuit_key = build_key('circuit:' .. circuit_name, site_id)
        local failures_key = circuit_key .. ':failures'
        local state_key = circuit_key .. ':state'
        local opened_at_key = circuit_key .. ':opened_at'
        local half_open_count_key = circuit_key .. ':half_open_count'

        local now = get_time_ms()
        local window_start = now - window_ms

        -- Clean old failures outside window
        server.call('ZREMRANGEBYSCORE', failures_key, '-inf', window_start)

        -- Get current state
        local state = server.call('GET', state_key) or 'CLOSED'
        local opened_at = tonumber(server.call('GET', opened_at_key) or '0')

        -- State machine logic
        if state == 'OPEN' then
            -- Check if recovery timeout has passed
            if now - opened_at >= recovery_ms then
                -- Transition to HALF_OPEN
                server.call('SET', state_key, 'HALF_OPEN')
                server.call('SET', half_open_count_key, '0')
                state = 'HALF_OPEN'
                track_metric(site_id, 'circuit_half_open', 1)
            else
                -- Still open, reject request
                track_metric(site_id, 'circuit_rejected', 1)
                return safe_json_encode({
                    state = 'OPEN',
                    allowed = false,
                    remaining_ms = recovery_ms - (now - opened_at),
                    failure_count = server.call('ZCARD', failures_key)
                })
            end
        end

        if state == 'HALF_OPEN' then
            -- Allow limited test requests
            local half_open_count = tonumber(server.call('GET', half_open_count_key) or '0')
            if half_open_count >= half_open_max then
                track_metric(site_id, 'circuit_rejected_half_open', 1)
                return safe_json_encode({
                    state = 'HALF_OPEN',
                    allowed = false,
                    test_requests_exhausted = true
                })
            end
            server.call('INCR', half_open_count_key)
        end

        -- CLOSED or HALF_OPEN with capacity - allow request
        local failure_count = server.call('ZCARD', failures_key)

        track_metric(site_id, 'circuit_allowed', 1)
        return safe_json_encode({
            state = state,
            allowed = true,
            failure_count = failure_count,
            threshold = failure_threshold
        })
    end,
    description = 'Check circuit breaker state and get permission to proceed'
}

-- Circuit Breaker: Record success (resets half-open to closed)
server.register_function{
    function_name = 'GNODE_RESILIENCE_CIRCUIT_SUCCESS',
    callback = function(keys, args)
        if #keys < 1 then
            return server.error_reply("Missing circuit key")
        end

        local circuit_name = keys[1]
        local site_id = args[1] or "default"

        local circuit_key = build_key('circuit:' .. circuit_name, site_id)
        local state_key = circuit_key .. ':state'

        local state = server.call('GET', state_key) or 'CLOSED'

        if state == 'HALF_OPEN' then
            -- Success in half-open means we can close the circuit
            server.call('SET', state_key, 'CLOSED')
            server.call('DEL', circuit_key .. ':failures')
            server.call('DEL', circuit_key .. ':opened_at')
            server.call('DEL', circuit_key .. ':half_open_count')
            track_metric(site_id, 'circuit_closed', 1)
            return safe_json_encode({ state = 'CLOSED', recovered = true })
        end

        track_metric(site_id, 'circuit_success', 1)
        return safe_json_encode({ state = state, recovered = false })
    end,
    description = 'Record successful call (may close circuit from half-open)'
}

-- Circuit Breaker: Record failure
server.register_function{
    function_name = 'GNODE_RESILIENCE_CIRCUIT_FAILURE',
    callback = function(keys, args)
        if #keys < 1 then
            return server.error_reply("Missing circuit key")
        end

        local circuit_name = keys[1]
        local site_id = args[1] or "default"
        local failure_threshold = tonumber(args[2]) or 5
        local error_type = args[3] or "unknown"

        local circuit_key = build_key('circuit:' .. circuit_name, site_id)
        local failures_key = circuit_key .. ':failures'
        local state_key = circuit_key .. ':state'
        local opened_at_key = circuit_key .. ':opened_at'

        local now = get_time_ms()
        local state = server.call('GET', state_key) or 'CLOSED'

        -- Record failure with timestamp and error type
        server.call('ZADD', failures_key, now, now .. ':' .. error_type)
        server.call('EXPIRE', failures_key, 300)  -- 5 minute max retention

        local failure_count = server.call('ZCARD', failures_key)

        if state == 'HALF_OPEN' then
            -- Any failure in half-open immediately reopens
            server.call('SET', state_key, 'OPEN')
            server.call('SET', opened_at_key, now)
            track_metric(site_id, 'circuit_reopened', 1)
            return safe_json_encode({
                state = 'OPEN',
                failure_count = failure_count,
                reopened = true
            })
        end

        if state == 'CLOSED' and failure_count >= failure_threshold then
            -- Open the circuit
            server.call('SET', state_key, 'OPEN')
            server.call('SET', opened_at_key, now)
            track_metric(site_id, 'circuit_opened', 1)
            return safe_json_encode({
                state = 'OPEN',
                failure_count = failure_count,
                opened = true
            })
        end

        track_metric(site_id, 'circuit_failure', 1)
        return safe_json_encode({
            state = state,
            failure_count = failure_count,
            threshold = failure_threshold
        })
    end,
    description = 'Record failed call (may open circuit)'
}

--[[
  ============================================================================
  IDEMPOTENCY / REQUEST DEDUPLICATION

  Prevents duplicate processing of commands using idempotency keys.
  Implements "exactly-once" semantics for command processing.
  ============================================================================
]]

-- Check idempotency key and optionally lock for processing
-- Returns: { status: "new"|"processing"|"completed", result: ... }
server.register_function{
    function_name = 'GNODE_RESILIENCE_IDEMPOTENT_CHECK',
    callback = function(keys, args)
        if #keys < 1 then
            return server.error_reply("Missing idempotency key")
        end

        local idempotency_key = keys[1]
        local site_id = args[1] or "default"
        local lock_ttl = tonumber(args[2]) or 30  -- 30 second processing lock
        local result_ttl = tonumber(args[3]) or 86400  -- 24 hour result retention

        local key = build_key('idempotent:' .. idempotency_key, site_id)
        local lock_key = key .. ':lock'
        local result_key = key .. ':result'
        local status_key = key .. ':status'

        -- Check if already completed
        local status = server.call('GET', status_key)
        if status == 'completed' then
            local result = server.call('GET', result_key)
            track_metric(site_id, 'idempotent_hit', 1)
            return safe_json_encode({
                status = 'completed',
                cached = true,
                result = result
            })
        end

        -- Try to acquire processing lock
        local acquired = server.call('SET', lock_key, 'locked', 'NX', 'EX', lock_ttl)
        if not acquired then
            track_metric(site_id, 'idempotent_processing', 1)
            return safe_json_encode({
                status = 'processing',
                cached = false,
                message = 'Request is being processed by another worker'
            })
        end

        -- Mark as processing
        server.call('SET', status_key, 'processing', 'EX', lock_ttl + 10)

        track_metric(site_id, 'idempotent_new', 1)
        return safe_json_encode({
            status = 'new',
            cached = false,
            lock_acquired = true
        })
    end,
    description = 'Check idempotency key and acquire processing lock'
}

-- Store idempotency result after successful processing
server.register_function{
    function_name = 'GNODE_RESILIENCE_IDEMPOTENT_COMPLETE',
    callback = function(keys, args)
        if #keys < 1 then
            return server.error_reply("Missing idempotency key")
        end
        if #args < 2 then
            return server.error_reply("Missing result data")
        end

        local idempotency_key = keys[1]
        local site_id = args[1] or "default"
        local result = args[2]
        local result_ttl = tonumber(args[3]) or 86400  -- 24 hour retention

        local key = build_key('idempotent:' .. idempotency_key, site_id)
        local lock_key = key .. ':lock'
        local result_key = key .. ':result'
        local status_key = key .. ':status'

        -- Store result and mark completed
        server.call('SET', result_key, result, 'EX', result_ttl)
        server.call('SET', status_key, 'completed', 'EX', result_ttl)
        server.call('DEL', lock_key)

        track_metric(site_id, 'idempotent_completed', 1)
        return safe_json_encode({ stored = true, ttl = result_ttl })
    end,
    description = 'Store idempotency result after processing'
}

-- Release idempotency lock on failure (allows retry)
server.register_function{
    function_name = 'GNODE_RESILIENCE_IDEMPOTENT_RELEASE',
    callback = function(keys, args)
        if #keys < 1 then
            return server.error_reply("Missing idempotency key")
        end

        local idempotency_key = keys[1]
        local site_id = args[1] or "default"

        local key = build_key('idempotent:' .. idempotency_key, site_id)
        local lock_key = key .. ':lock'
        local status_key = key .. ':status'

        server.call('DEL', lock_key)
        server.call('DEL', status_key)

        track_metric(site_id, 'idempotent_released', 1)
        return safe_json_encode({ released = true })
    end,
    description = 'Release idempotency lock (allows retry after failure)'
}

--[[
  ============================================================================
  CACHE STAMPEDE PREVENTION

  Implements probabilistic early expiration (PER) to prevent cache stampedes.
  When many requests hit an expired key simultaneously, only one regenerates.
  ============================================================================
]]

-- Get with stampede prevention
-- Uses probabilistic early expiration + locking
server.register_function{
    function_name = 'GNODE_RESILIENCE_CACHE_GET_SAFE',
    callback = function(keys, args)
        if #keys < 1 then
            return server.error_reply("Missing cache key")
        end

        local cache_key = keys[1]
        local site_id = args[1] or "default"
        local beta = tonumber(args[2]) or 1.0  -- Early expiration factor (higher = earlier)
        local lock_ttl = tonumber(args[3]) or 10  -- Lock TTL for regeneration

        local key = build_key('cache:' .. cache_key, site_id)
        local data_key = key .. ':data'
        local expiry_key = key .. ':expiry'
        local lock_key = key .. ':regen_lock'

        -- Get cached data and expiry
        local data = server.call('GET', data_key)
        local expiry = tonumber(server.call('GET', expiry_key) or '0')
        local now = get_time_ms() / 1000  -- Convert to seconds

        if not data then
            -- Cache miss - try to acquire regeneration lock
            local acquired = server.call('SET', lock_key, 'locked', 'NX', 'EX', lock_ttl)
            track_metric(site_id, 'cache_miss', 1)
            return safe_json_encode({
                hit = false,
                should_regenerate = acquired ~= nil,
                lock_acquired = acquired ~= nil
            })
        end

        -- Cache hit - check for probabilistic early expiration
        local ttl = expiry - now
        if ttl > 0 then
            -- Apply probabilistic early expiration
            -- Formula: ttl - beta * log(random())
            -- This gives higher probability of regeneration as TTL approaches 0
            ensure_prng_init()
            local random_factor = -beta * math.log(math.random())

            if random_factor > ttl then
                -- Probabilistic early expiration triggered
                local acquired = server.call('SET', lock_key, 'locked', 'NX', 'EX', lock_ttl)
                if acquired then
                    track_metric(site_id, 'cache_early_regen', 1)
                    return safe_json_encode({
                        hit = true,
                        data = data,
                        should_regenerate = true,
                        lock_acquired = true,
                        ttl = ttl,
                        early_expiration = true
                    })
                end
            end
        end

        track_metric(site_id, 'cache_hit', 1)
        return safe_json_encode({
            hit = true,
            data = data,
            should_regenerate = false,
            ttl = ttl
        })
    end,
    description = 'Get cached value with stampede prevention'
}

-- Set with stampede prevention metadata
server.register_function{
    function_name = 'GNODE_RESILIENCE_CACHE_SET_SAFE',
    callback = function(keys, args)
        if #keys < 1 then
            return server.error_reply("Missing cache key")
        end
        if #args < 2 then
            return server.error_reply("Missing data")
        end

        local cache_key = keys[1]
        local site_id = args[1] or "default"
        local data = args[2]
        local ttl = tonumber(args[3]) or 3600  -- 1 hour default

        local key = build_key('cache:' .. cache_key, site_id)
        local data_key = key .. ':data'
        local expiry_key = key .. ':expiry'
        local lock_key = key .. ':regen_lock'

        local now = get_time_ms() / 1000
        local expiry = now + ttl

        -- Store data with expiry tracking
        server.call('SET', data_key, data, 'EX', ttl + 60)  -- Extra buffer for early regen
        server.call('SET', expiry_key, tostring(expiry), 'EX', ttl + 60)
        server.call('DEL', lock_key)  -- Release regeneration lock

        track_metric(site_id, 'cache_set', 1)
        return safe_json_encode({ stored = true, expiry = expiry, ttl = ttl })
    end,
    description = 'Set cached value with stampede prevention metadata'
}

--[[
  ============================================================================
  DISTRIBUTED LEADER ELECTION

  Implements leader election using ValKey with:
  - Lease-based leadership with automatic renewal
  - Graceful leadership transfer
  - Leader heartbeat monitoring
  ============================================================================
]]

-- Try to become leader or renew leadership
server.register_function{
    function_name = 'GNODE_RESILIENCE_LEADER_ACQUIRE',
    callback = function(keys, args)
        if #keys < 1 then
            return server.error_reply("Missing election key")
        end
        if #args < 2 then
            return server.error_reply("Missing node_id")
        end

        local election_name = keys[1]
        local site_id = args[1] or "default"
        local node_id = args[2]
        local lease_ttl = tonumber(args[3]) or 30  -- 30 second lease

        local key = build_key('leader:' .. election_name, site_id)
        local leader_key = key .. ':current'
        local heartbeat_key = key .. ':heartbeat'

        local now = get_time_ms()

        -- Check current leader
        local current_leader = server.call('GET', leader_key)

        if current_leader == node_id then
            -- Renew our lease
            server.call('SET', leader_key, node_id, 'EX', lease_ttl)
            server.call('SET', heartbeat_key, now, 'EX', lease_ttl)
            track_metric(site_id, 'leader_renewed', 1)
            return safe_json_encode({
                is_leader = true,
                renewed = true,
                node_id = node_id,
                lease_ttl = lease_ttl
            })
        end

        if current_leader then
            -- Someone else is leader
            track_metric(site_id, 'leader_exists', 1)
            return safe_json_encode({
                is_leader = false,
                current_leader = current_leader,
                message = 'Another node is leader'
            })
        end

        -- Try to become leader
        local acquired = server.call('SET', leader_key, node_id, 'NX', 'EX', lease_ttl)
        if acquired then
            server.call('SET', heartbeat_key, now, 'EX', lease_ttl)
            track_metric(site_id, 'leader_elected', 1)
            return safe_json_encode({
                is_leader = true,
                elected = true,
                node_id = node_id,
                lease_ttl = lease_ttl
            })
        end

        -- Lost race
        current_leader = server.call('GET', leader_key)
        return safe_json_encode({
            is_leader = false,
            current_leader = current_leader,
            message = 'Lost election race'
        })
    end,
    description = 'Try to acquire or renew leadership'
}

-- Gracefully release leadership
server.register_function{
    function_name = 'GNODE_RESILIENCE_LEADER_RELEASE',
    callback = function(keys, args)
        if #keys < 1 then
            return server.error_reply("Missing election key")
        end
        if #args < 2 then
            return server.error_reply("Missing node_id")
        end

        local election_name = keys[1]
        local site_id = args[1] or "default"
        local node_id = args[2]

        local key = build_key('leader:' .. election_name, site_id)
        local leader_key = key .. ':current'
        local heartbeat_key = key .. ':heartbeat'

        -- Only release if we're the current leader
        local current_leader = server.call('GET', leader_key)
        if current_leader ~= node_id then
            return safe_json_encode({
                released = false,
                message = 'Not the current leader'
            })
        end

        server.call('DEL', leader_key)
        server.call('DEL', heartbeat_key)

        track_metric(site_id, 'leader_released', 1)
        return safe_json_encode({
            released = true,
            former_leader = node_id
        })
    end,
    description = 'Gracefully release leadership'
}

-- Check who is current leader
server.register_function{
    function_name = 'GNODE_RESILIENCE_LEADER_WHO',
    callback = function(keys, args)
        if #keys < 1 then
            return server.error_reply("Missing election key")
        end

        local election_name = keys[1]
        local site_id = args[1] or "default"

        local key = build_key('leader:' .. election_name, site_id)
        local leader_key = key .. ':current'
        local heartbeat_key = key .. ':heartbeat'

        local current_leader = server.call('GET', leader_key)
        local heartbeat = server.call('GET', heartbeat_key)
        local ttl = server.call('TTL', leader_key)

        return safe_json_encode({
            leader = current_leader,
            last_heartbeat = heartbeat,
            lease_remaining = ttl > 0 and ttl or 0,
            has_leader = current_leader ~= nil
        })
    end,
    flags = {'no-writes'},
    description = 'Check current leader status'
}

--[[
  ============================================================================
  RETRY BUDGET TRACKING

  Implements retry budgets to prevent retry storms.
  Each service gets a budget of retries per time window.
  ============================================================================
]]

-- Check and consume retry budget
server.register_function{
    function_name = 'GNODE_RESILIENCE_RETRY_CHECK',
    callback = function(keys, args)
        if #keys < 1 then
            return server.error_reply("Missing service key")
        end

        local service_name = keys[1]
        local site_id = args[1] or "default"
        local max_retries = tonumber(args[2]) or 10  -- Max retries per window
        local window_seconds = tonumber(args[3]) or 60  -- Window size

        local key = build_key('retry:' .. service_name, site_id)
        local count_key = key .. ':count'
        local window_key = key .. ':window'

        local now = math.floor(get_time_ms() / 1000)
        local window_start = now - (now % window_seconds)

        -- Check if we're in a new window
        local current_window = tonumber(server.call('GET', window_key) or '0')
        if current_window ~= window_start then
            -- New window - reset counter
            server.call('SET', window_key, window_start, 'EX', window_seconds * 2)
            server.call('SET', count_key, '0', 'EX', window_seconds * 2)
        end

        local current_count = tonumber(server.call('GET', count_key) or '0')

        if current_count >= max_retries then
            track_metric(site_id, 'retry_budget_exhausted', 1)
            return safe_json_encode({
                allowed = false,
                remaining = 0,
                budget = max_retries,
                window_resets_in = window_seconds - (now - window_start)
            })
        end

        -- Consume one retry
        server.call('INCR', count_key)

        track_metric(site_id, 'retry_consumed', 1)
        return safe_json_encode({
            allowed = true,
            remaining = max_retries - current_count - 1,
            budget = max_retries,
            window_resets_in = window_seconds - (now - window_start)
        })
    end,
    description = 'Check and consume retry budget'
}

-- Get retry budget status without consuming
server.register_function{
    function_name = 'GNODE_RESILIENCE_RETRY_STATUS',
    callback = function(keys, args)
        if #keys < 1 then
            return server.error_reply("Missing service key")
        end

        local service_name = keys[1]
        local site_id = args[1] or "default"
        local max_retries = tonumber(args[2]) or 10
        local window_seconds = tonumber(args[3]) or 60

        local key = build_key('retry:' .. service_name, site_id)
        local count_key = key .. ':count'

        local current_count = tonumber(server.call('GET', count_key) or '0')
        local now = math.floor(get_time_ms() / 1000)
        local window_start = now - (now % window_seconds)

        return safe_json_encode({
            used = current_count,
            remaining = math.max(0, max_retries - current_count),
            budget = max_retries,
            window_resets_in = window_seconds - (now - window_start)
        })
    end,
    flags = {'no-writes'},
    description = 'Get retry budget status'
}
