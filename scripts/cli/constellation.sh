# Sourced. The constellation a CLI call acts on: GNODE_TOPOLOGY_NAMESPACE (the CLI's
# --constellation sets it), else the first one this node's daemon serves, else geodineum.
gnode_cli_constellation() {
    if [[ -n "${GNODE_TOPOLOGY_NAMESPACE:-}" ]]; then echo "$GNODE_TOPOLOGY_NAMESPACE"; return; fi
    local env="${GNODE_DAEMON_ENV:-/etc/geodineum/components/gnode-daemon/daemon.env}" line=""
    [[ -r "$env" ]] && line="$(grep -m1 '^GNODE_CONSTELLATIONS=' "$env" 2>/dev/null)"
    line="${line#*=}"; line="${line//\"/}"; line="${line%%,*}"; line="${line// /}"
    echo "${line:-geodineum}"
}
