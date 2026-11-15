#!/bin/bash
# ACL command sets of the ValKey users gNode mints. Sourced by onboard-service.sh,
# constellation-setup.sh and mint-node-identity.sh; not run.

[[ -n "${_GNODE_ACL_TIERS_SH:-}" ]] && return 0
_GNODE_ACL_TIERS_SH=1

# A joined node's daemon login is the master's daemon tier (acl-daemon-tier.rule) minus
# every write to the Lua library namespace, which all constellations on the ValKey share.
ACL_NODE_TIER_DENY=("-function|load" "-function|delete" "-function|flush" "-function|restore")

# The node tier as ACL SETUSER tokens, from the daemon-tier rule file.
acl_node_tier() {
    local rule
    rule="$(cat "$1" 2>/dev/null)" && [[ -n "$rule" ]] || return 1
    printf '%s %s\n' "$rule" "${ACL_NODE_TIER_DENY[*]}"
}

# Every registered site's and service's commands. SCAN is not among them: key patterns do
# not hide key names, so SCAN lists every tenant's keys (acl_client_scan decides it).
ACL_CLIENT_COMMANDS=(
    +xread +xreadgroup +xadd +xack +xclaim +xpending +xinfo +xlen +xtrim +xrange +xrevrange +xgroup +xdel
    +fcall +fcall_ro
    +get +set +setex +setnx +del +exists +ttl +expire +mget +mset +incr +decr +incrby +decrby
    +hget +hset +hgetall +hdel +hexists +hkeys +hvals +hincrby +hmget +hmset
    +sadd +smembers +sismember +srem +scard
    +lpush +rpush +lpop +rpop +lrange +llen +lindex +ltrim
    +zadd +zrange +zrevrange +zrem +zscore +zcard
    +ping +publish +auth +select +info "+client|id" "+client|getname" "+client|setname" "+client|setinfo"
    +multi +exec +discard +time +type +object
)

# "+scan" or "-scan" for the manifest at $1 and the shape at $2. The manifest's
# `keyspace_scan: true|false` decides; without it a site keeps SCAN (gCore's cache
# invalidation and admin pages list the site's keys through it) and any other shape does not.
acl_client_scan() {
    local v=""
    [[ -n "${1:-}" && -r "$1" ]] && command -v yq >/dev/null 2>&1 \
        && v="$(yq eval '.keyspace_scan' "$1" 2>/dev/null)"
    case "$v" in
        true) echo "+scan" ;;
        false) echo "-scan" ;;
        *) [[ "${2:-}" == site ]] && echo "+scan" || echo "-scan" ;;
    esac
}
