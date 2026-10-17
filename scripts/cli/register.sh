#!/bin/bash
# geodineum topology register — register components and sites on the one topology.
# (`geodineum register`, without `topology`, is the installer's manifest generator.)
#
#   sudo geodineum topology register components                            (re)register the ecosystem components (shape 2)
#   sudo geodineum topology register service <site> [profile] [--env <dtap>] [--manifest <path>] [--on <node>]   register <site>'s OWN entity by name
#   sudo geodineum topology register node <id> [--role <word>] [--specialization <word>] [--constellation <c>]
#                                                                          declare a node that runs no daemon (observer
#                                                                          laptop, app box behind a tunnel), so its
#                                                                          services' heartbeats become placements
#
#   profiles → shapes: web (default) → site 3.1 · headless → app 3.2 · service | system | component → daemon 3.3
#
# Wraps `gnode-daemon register-tools`, supplying the daemon credential + binary
# so you never type the VALKEY_USER / GNODE_REDIS_AUTH_FILE dance. Reads the
# daemon credential (root-only) → requires_sudo.
#
# Everything registers into {<constellation>}:gnode:constellation by name: the
# daemon codes the words, the derived slots belong to its sampler, and a
# re-registration preserves whatever it last measured.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GNODE_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"
DAEMON_BIN="${GNODE_DAEMON_BIN:-${GNODE_ROOT}/daemon/target/release/gnode-daemon}"
[[ -x "$DAEMON_BIN" ]] || DAEMON_BIN="$(command -v gnode-daemon 2>/dev/null || true)"
CRED_DIR="${GEODINEUM_CREDENTIALS_DIR:-/etc/geodineum/credentials}"
DAEMON_CRED="${DAEMON_CRED:-${CRED_DIR}/valkey_daemon.password}"

usage() {
    echo "Usage:"
    echo "  geodineum topology register components                              register the ecosystem components (shape 2)"
    echo "  geodineum topology register service <site> [profile] [--env <dtap>] [--manifest <path>] [--on <node>]"
    echo "      register <site> by name from a profile (default: web → site) or its manifest; a site is placed on this node"
    echo "      (its health is this node's daemon heartbeat); --on <node> places it elsewhere, --on '' not at all"
    echo "  geodineum topology register node <id> [--role observer|headless|edge|replica|full_node] [--specialization <word>] [--constellation <c>]"
    echo
    echo "  profiles → shapes: web → site · headless → app · service | system | component → daemon"
    exit "${1:-0}"
}

# Usage/help works without the daemon binary or credential present.
case "${1:-}" in ""|-h|--help|help) usage 0 ;; esac

[[ -x "$DAEMON_BIN" ]] || { echo "register: gnode-daemon binary not found ($DAEMON_BIN)" >&2; exit 1; }
[[ -r "$DAEMON_CRED" ]] || { echo "register: cannot read $DAEMON_CRED — run with sudo" >&2; exit 1; }

# Run the daemon subcommand with the daemon ValKey identity.
run_reg() { VALKEY_USER=gnode_daemon GNODE_REDIS_AUTH_FILE="$DAEMON_CRED" "$DAEMON_BIN" "$@"; }

sub="${1:-}"; shift || true
case "$sub" in
    components|component|tool|tools)
        echo "Registering ecosystem components (shape 2) on the one topology…"
        run_reg register-tools --tier tool
        ;;
    service|site)
        # register service <site> [profile] [--env <dtap>]
        site="${1:-}"; shift || true
        profile="web"; environment=""; manifest=""; on="__default__"
        while [[ $# -gt 0 ]]; do
            case "$1" in
                --env)    environment="${2:-}"; shift 2 ;;
                --env=*)  environment="${1#*=}"; shift ;;
                --on)     on="${2:-}"; shift 2 ;;
                --manifest) manifest="${2:-}"; shift 2 ;;
                -*)       echo "register service: unknown option '$1'" >&2; usage 1 ;;
                *)        profile="$1"; shift ;;
            esac
        done
        [[ -n "$site" ]] || { echo "register service: missing <site>" >&2; usage 1; }
        env_arg=(); [[ -n "$environment" ]] && env_arg=(--environment "$environment")
        if [[ "$on" == "__default__" ]]; then
            on=""
            [[ "$profile" == web ]] && on="$(sed -n 's/^GNODE_NODE_ID="\{0,1\}\([^"]*\)"\{0,1\}/\1/p' /etc/geodineum/components/gnode-daemon/daemon.env 2>/dev/null | sed -n 1p)" && on="${on:-$(hostname -s)}"
        fi
        on_arg=(); [[ -n "$on" ]] && on_arg=(--on "$on")
        src_arg=(--profile "$profile"); [[ -n "$manifest" ]] && src_arg=(--manifest "$manifest")
        echo "Registering '$site' by name from the ${manifest:+manifest $manifest}${manifest:-'$profile' profile}${environment:+ (env=$environment)}${on:+, placed on $on}…"
        run_reg register-tools --tier service --site "$site" "${src_arg[@]}" "${env_arg[@]}" "${on_arg[@]}"
        ;;
    node)
        # register node <id> [--role <word>] [--specialization <word>] [--constellation <c>]
        node="${1:-}"; shift || true
        role="observer"; spec=""; c="${GNODE_TOPOLOGY_NAMESPACE:-geodineum}"
        while [[ $# -gt 0 ]]; do
            case "$1" in
                --role)            role="${2:-}"; shift 2 ;;
                --role=*)          role="${1#*=}"; shift ;;
                --specialization)  spec="${2:-}"; shift 2 ;;
                --specialization=*) spec="${1#*=}"; shift ;;
                --constellation)   c="${2:-}"; shift 2 ;;
                --constellation=*) c="${1#*=}"; shift ;;
                *) echo "register node: unknown option '$1'" >&2; usage 1 ;;
            esac
        done
        [[ -n "$node" ]] || { echo "register node: missing <id>" >&2; usage 1; }
        spec_arg=(); [[ -n "$spec" ]] && spec_arg=(--specialization "$spec")
        echo "Declaring node '$node' (role $role) in constellation '$c'…"
        VALKEY_USER=gnode_daemon GNODE_REDIS_AUTH_FILE="$DAEMON_CRED"  \
            "$DAEMON_BIN" --topology-namespace "$c" register-tools --tier node --site "$node" --node-role "$role" "${spec_arg[@]}"
        ;;
    ""|-h|--help)
        usage 0 ;;
    *)
        echo "register: unknown subcommand '$sub'" >&2; usage 1 ;;
esac
