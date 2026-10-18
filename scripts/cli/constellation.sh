# Sourced. Which constellation(s) and node a CLI call acts on.
GNODE_DAEMON_ENV="${GNODE_DAEMON_ENV:-/etc/geodineum/components/gnode-daemon/daemon.env}"

_gnode_daemon_env_value() {
    local line=""
    [[ -r "$GNODE_DAEMON_ENV" ]] && line="$(grep -m1 "^$1=" "$GNODE_DAEMON_ENV" 2>/dev/null)" || true
    line="${line#*=}"; line="${line//\"/}"; line="${line//\'/}"
    echo "${line// /}"
}

# Every constellation this node's daemon serves, one per line, the explicit one
# (GNODE_TOPOLOGY_NAMESPACE, set by the CLI's --constellation) first.
gnode_cli_constellations() {
    local served c out=() seen=" "
    served="$(_gnode_daemon_env_value GNODE_CONSTELLATIONS)"
    for c in ${GNODE_TOPOLOGY_NAMESPACE:-} ${served//,/ }; do
        [[ "$seen" == *" $c "* ]] && continue
        seen+="$c "; out+=("$c")
    done
    [[ ${#out[@]} -gt 0 ]] || out=(geodineum)
    printf '%s\n' "${out[@]}"
}

# The one a call acts on: the explicit one, else the first the daemon serves, else geodineum.
gnode_cli_constellation() {
    local first
    first="$(gnode_cli_constellations)"
    echo "${first%%$'\n'*}"
}

# This node's id: GNODE_NODE_ID from daemon.env, else the short hostname (CONTRACTS/heartbeat.md).
gnode_cli_node_id() {
    local id
    id="$(_gnode_daemon_env_value GNODE_NODE_ID)"
    echo "${id:-$(hostname -s)}"
}

# The shape a registration declares: the manifest's gnode.shape, else the profile's
# (web → site, headless → app, the rest → daemon). A site is placed on the node that
# serves it; an app or daemon is placed by its own heartbeat.
gnode_cli_shape() {
    local profile="${1:-web}" manifest="${2:-}" shape=""
    if [[ -n "$manifest" && -r "$manifest" ]] && command -v yq >/dev/null 2>&1; then
        shape="$(yq eval '.gnode.shape // ""' "$manifest" 2>/dev/null | tr -d '"')"
        [[ "$shape" == null ]] && shape=""
        [[ -z "$shape" ]] && profile="$(yq eval '.profile // ""' "$manifest" 2>/dev/null | tr -d '"')" && profile="${profile/null/}"
    fi
    if [[ -z "$shape" ]]; then
        case "${profile:-web}" in web) shape=site ;; headless) shape=app ;; *) shape=daemon ;; esac
    fi
    echo "$shape"
}
