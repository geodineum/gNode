#!/bin/bash
# geodineum register — register services/tools into the canonical (C) topology.
#
#   sudo geodineum register tool                       (re)register the ecosystem tools (global, once)
#   sudo geodineum register service <site> [profile]   register <site>'s OWN entity from a profile
#
#   profiles: web (default) | headless | service | system | component
#
# Wraps `gnode-daemon register-tools`, supplying the daemon credential + binary
# so you never type the VALKEY_USER / GNODE_REDIS_AUTH_FILE dance. Reads the
# daemon credential (root-only) → requires_sudo.
#
# Tools register ONCE globally into {ecosystem}:gnode:services. A service
# registers its own single entity into {site}:gnode:services from the profile's
# 30-dim defaults (gMath computes the vector; Lua stores).
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GNODE_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"
DAEMON_BIN="${GNODE_DAEMON_BIN:-${GNODE_ROOT}/daemon/target/release/gnode-daemon}"
[[ -x "$DAEMON_BIN" ]] || DAEMON_BIN="$(command -v gnode-daemon 2>/dev/null || true)"
CRED_DIR="${GEODINEUM_CREDENTIALS_DIR:-/etc/geodineum/credentials}"
DAEMON_CRED="${DAEMON_CRED:-${CRED_DIR}/valkey_daemon.password}"

usage() {
    echo "Usage:"
    echo "  geodineum register tool                      register the ecosystem tools (global, once)"
    echo "  geodineum register service <site> [profile]  register <site> from a profile (default: web)"
    echo
    echo "  profiles: web | headless | service | system | component"
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
    tool|tools)
        echo "Registering ecosystem tools (global tool tier)…"
        run_reg register-tools --tier tool
        ;;
    service|site)
        # register service <site> [profile] [--env <dtap>]
        site="${1:-}"; shift || true
        profile="web"; environment=""
        while [[ $# -gt 0 ]]; do
            case "$1" in
                --env)    environment="${2:-}"; shift 2 ;;
                --env=*)  environment="${1#*=}"; shift ;;
                -*)       echo "register service: unknown option '$1'" >&2; usage 1 ;;
                *)        profile="$1"; shift ;;
            esac
        done
        [[ -n "$site" ]] || { echo "register service: missing <site>" >&2; usage 1; }
        env_arg=(); [[ -n "$environment" ]] && env_arg=(--environment "$environment")
        echo "Registering '$site' as a '$profile'-profile service entity${environment:+ (env=$environment)}…"
        run_reg register-tools --tier service --site "$site" --profile "$profile" "${env_arg[@]}"
        ;;
    ""|-h|--help)
        usage 0 ;;
    *)
        echo "register: unknown subcommand '$sub'" >&2; usage 1 ;;
esac
