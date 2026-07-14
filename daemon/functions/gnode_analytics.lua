#!lua name=gnode_analytics

--
-- gNode ANALYTICS Functions
-- Server-side visitor analytics that run at the data.
--
-- Replaces the AnalyticsManagerPro direct zAdd/hIncrBy/expire calls (which
-- throw on the storage adapter) with two atomic FCALLs. A front-end JS beacon
-- posts to a gCore/gTemplate REST endpoint, which resolves the site + visitor
-- hash + bot flag server-side and calls GNODE_ANALYTICS_HIT. The operator
-- dashboard reads everything back through GNODE_ANALYTICS_SUMMARY.
--
-- All keys are hash-tagged {site_id}: so per-site ACLs (~{site}:*) can serve
-- them, and every key carries a 90-day EXPIRE.
--
-- Usage:
--   GNODE_ANALYTICS_HIT(site_id, visitor_hash, page, referrer_host, is_bot, ts, ymd)
--   GNODE_ANALYTICS_SUMMARY(site_id, ymd1, ymd2, ... ymdN)   -- read-only
--
-- Schema (per site):
--   {site}:bothits:{Ymd}         HASH  {bot, human} per-hit counters
--   {site}:pagecounts:{Ymd}      HASH  page -> count           (human only)
--   {site}:pageviews:{Ymd}       ZSET  member=page score=ts    (human only)
--   {site}:visits:{Ymd}          ZSET  member=vhash score=ts   (unique humans)
--   {site}:visitor_requests:{Ymd} HASH vhash -> request count  (human only)
--   {site}:referrers:{Ymd}       HASH  referrer_host -> count  (human only)
--   {site}:transitions:{Ymd}     HASH  "from > to" -> count    (human paths)
--   {site}:visitors:{vhash}      HASH  first_seen/last_seen/page_count/
--                                      first_page/last_page/is_bot
--

local RETENTION = 7776000  -- 90 days

-- Sum a HASH returned as a flat {k1,v1,k2,v2,...} array into an accumulator map.
local function fold_hash(flat, acc)
    for i = 1, #flat, 2 do
        local k = flat[i]
        local v = tonumber(flat[i + 1]) or 0
        acc[k] = (acc[k] or 0) + v
    end
end

-- Top-N of a name->count map as a JSON-array-friendly sequence of {name,count}.
local function top_n(map, n)
    local arr = {}
    for k, v in pairs(map) do
        arr[#arr + 1] = { k, v }
    end
    table.sort(arr, function(a, b) return a[2] > b[2] end)
    local out = {}
    local limit = math.min(n, #arr)
    for i = 1, limit do
        out[#out + 1] = { name = arr[i][1], count = arr[i][2] }
    end
    return out
end

-- ─── WRITE: one visitor hit ──────────────────────────────────────────────────
server.register_function{
    function_name = 'GNODE_ANALYTICS_HIT',
    callback = function(keys, args)
        local site = args[1]
        if not site or site == '' then
            return server.error_reply('GNODE_ANALYTICS_HIT: missing site_id')
        end
        local vhash    = args[2] or 'anon'
        local page     = args[3] or '/'
        local referrer = args[4] or ''
        local is_bot   = (args[5] == '1')
        local ts       = tonumber(args[6]) or 0
        local ymd      = args[7]
        if not ymd or ymd == '' then
            return server.error_reply('GNODE_ANALYTICS_HIT: missing date')
        end

        local p = '{' .. site .. '}:'
        local vkey = p .. 'visitors:' .. vhash

        -- Every hit counts toward the bot/human split.
        server.call('HINCRBY', p .. 'bothits:' .. ymd, is_bot and 'bot' or 'human', 1)
        server.call('EXPIRE', p .. 'bothits:' .. ymd, RETENTION)

        -- Bots get a minimal footprint and nothing in the human-facing metrics.
        if is_bot then
            server.call('HSET', vkey, 'last_seen', ts, 'is_bot', '1')
            server.call('EXPIRE', vkey, RETENTION)
            return 'OK'
        end

        if referrer ~= '' then
            server.call('HINCRBY', p .. 'referrers:' .. ymd, referrer, 1)
            server.call('EXPIRE', p .. 'referrers:' .. ymd, RETENTION)
        end

        server.call('HINCRBY', p .. 'pagecounts:' .. ymd, page, 1)
        server.call('EXPIRE', p .. 'pagecounts:' .. ymd, RETENTION)
        server.call('ZADD', p .. 'pageviews:' .. ymd, ts, page)
        server.call('EXPIRE', p .. 'pageviews:' .. ymd, RETENTION)

        server.call('ZADD', p .. 'visits:' .. ymd, ts, vhash)
        server.call('EXPIRE', p .. 'visits:' .. ymd, RETENTION)

        server.call('HINCRBY', p .. 'visitor_requests:' .. ymd, vhash, 1)
        server.call('EXPIRE', p .. 'visitor_requests:' .. ymd, RETENTION)

        local prev = server.call('HGET', vkey, 'last_page')
        if server.call('EXISTS', vkey) == 0 then
            server.call('HSET', vkey,
                'first_seen', ts, 'last_seen', ts, 'page_count', 1,
                'first_page', page, 'last_page', page, 'is_bot', '0')
        else
            server.call('HSET', vkey, 'last_seen', ts, 'last_page', page)
            server.call('HINCRBY', vkey, 'page_count', 1)
            -- A -> B navigation, cheap top-paths without session reconstruction.
            if type(prev) == 'string' and prev ~= '' and prev ~= page then
                server.call('HINCRBY', p .. 'transitions:' .. ymd, prev .. ' > ' .. page, 1)
                server.call('EXPIRE', p .. 'transitions:' .. ymd, RETENTION)
            end
        end
        server.call('EXPIRE', vkey, RETENTION)

        return 'OK'
    end,
    description = 'Records one visitor hit (page, referrer, bot flag) into per-site analytics'
}

-- ─── READ: aggregate summary across a set of days ────────────────────────────
server.register_function{
    function_name = 'GNODE_ANALYTICS_SUMMARY',
    callback = function(keys, args)
        local site = args[1]
        if not site or site == '' then
            return server.error_reply('GNODE_ANALYTICS_SUMMARY: missing site_id')
        end
        local p = '{' .. site .. '}:'

        local human_hits, bot_hits = 0, 0
        local uv = {}            -- distinct human visitor hashes across the window
        local pages, refs, trans = {}, {}, {}
        local daily = {}

        for i = 2, #args do
            local d = args[i]

            local bh = server.call('HGETALL', p .. 'bothits:' .. d)
            local dh, db = 0, 0
            for j = 1, #bh, 2 do
                if bh[j] == 'human' then dh = tonumber(bh[j + 1]) or 0
                elseif bh[j] == 'bot' then db = tonumber(bh[j + 1]) or 0 end
            end
            human_hits = human_hits + dh
            bot_hits = bot_hits + db

            local members = server.call('ZRANGE', p .. 'visits:' .. d, 0, -1)
            for _, m in ipairs(members) do uv[m] = true end

            fold_hash(server.call('HGETALL', p .. 'pagecounts:' .. d), pages)
            fold_hash(server.call('HGETALL', p .. 'referrers:' .. d), refs)
            fold_hash(server.call('HGETALL', p .. 'transitions:' .. d), trans)

            daily[#daily + 1] = { date = d, human = dh, bot = db, visitors = #members }
        end

        local unique_visitors = 0
        for _ in pairs(uv) do unique_visitors = unique_visitors + 1 end

        local pages_served = 0
        for _, v in pairs(pages) do pages_served = pages_served + v end

        local avg = 0
        if unique_visitors > 0 then
            avg = pages_served / unique_visitors
        end

        return cjson.encode({
            pages_served          = pages_served,
            unique_visitors       = unique_visitors,
            avg_pages_per_visitor = avg,
            human_hits            = human_hits,
            bot_hits              = bot_hits,
            top_pages             = top_n(pages, 10),
            top_referrers         = top_n(refs, 8),
            top_paths             = top_n(trans, 6),
            daily                 = daily
        })
    end,
    flags = { 'no-writes' },
    description = 'Aggregates per-site analytics across a set of days for the operator dashboard'
}
