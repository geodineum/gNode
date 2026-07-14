#!/bin/bash
#
# Build the gNode daemon.
#
# Sources configuration from the centralized config store. Skips rebuild
# if the binary is already up-to-date (use --force to override).
#
# SINGLE extension model — all extensions discovered via GNODE_EXT_DIR. Each subdirectory
# under that path is one extension; build.rs verifies its
# extension.sig against daemon/src/ext_author.rs::AUTHOR_PUBKEY and
# stages its handlers into OUT_DIR. No more Cargo feature special
# cases.
#
# Environment:
#   GNODE_EXT_DIR=/path/to/extensions  Parent dir containing signed
#                                      extension subdirectories. If
#                                      unset, no extensions are loaded
#                                      (lean core build).
#
# Usage:
#   ./scripts/build.sh [OPTIONS]
#
# Options:
#   --force          Rebuild even if the binary is up-to-date

set -euo pipefail

# Source cargo environment (required when run via sudo -u or cron)
[ -f "$HOME/.cargo/env" ] && source "$HOME/.cargo/env"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
DAEMON_DIR="$PROJECT_ROOT/daemon"
BINARY="$DAEMON_DIR/target/release/gnode-daemon"

# Canonical ecosystem config loader (installed by Geodineum installer).
#
# Build-time config loading: only the disk tier is required (3 whitelisted
# keys: VALKEY_HOST, VALKEY_PORT, VALKEY_CREDS_PATH). The full
# load_ecosystem_config wrapper ALSO calls load_bootstrap_valkey_tier which
# requires the daemon to be running + admin credentials at
# ${VALKEY_CREDS_PATH}/valkey.password — both of which are
# post-build state (the daemon is what we're building right now;
# valkey.password is generated when the daemon first registers with
# ValKey). load_ecosystem_config at build time was a chicken-and-egg trap
# that bricked first-install on every fresh machine.
#
# Resolution: load only the disk tier. cargo build doesn't read any
# VALKEY_* variables anyway — that surface is consumed at *runtime* by
# the compiled daemon, not by build.rs. If a future build step does need
# ValKey-tier config (e.g. compile-time codegen from a ValKey-stored
# schema), it should call load_bootstrap_valkey_tier explicitly with a
# clear failure mode that says "ValKey must be populated first" rather
# than the current opaque "populate_valkey_tier requires admin creds".
GEODINEUM_LIB="${GEODINEUM_LIB:-/usr/local/lib/geodineum}"
# The loader supplies disk-tier VALKEY_* config. As noted above, `cargo
# build` itself does NOT consume it — so a missing loader must NOT block
# the build (a hard FATAL here meant any host without the installed lib
# fell back to a bare `cargo build` that built lean, silently dropping
# every signed extension). Try the canonical lib path, then the in-repo
# Geodineum lib, then warn-and-continue.
_loader=""
for _cand in "$GEODINEUM_LIB/bootstrap-loader.sh" \
             "/opt/geodineum/Geodineum/lib/bootstrap-loader.sh"; do
    [[ -r "$_cand" ]] && { _loader="$_cand"; break; }
done
if [[ -n "$_loader" ]]; then
    # shellcheck source=/usr/local/lib/geodineum/bootstrap-loader.sh
    source "$_loader"
    load_bootstrap_disk_tier
else
    echo "WARNING: bootstrap-loader.sh not found (looked in $GEODINEUM_LIB and" >&2
    echo "         /opt/geodineum/Geodineum/lib); building without disk-tier" >&2
    echo "         config — not required for cargo build." >&2
fi

# Flags
FORCE=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --force)      FORCE=true; shift ;;
        -h|--help)    head -25 "$0" | tail -21; exit 0 ;;
        *)            echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

# Freshness check: skip if binary is newer than all source files
if [[ "$FORCE" == "false" && -f "$BINARY" ]]; then
    bin_mtime=$(stat -c %Y "$BINARY" 2>/dev/null || echo 0)
    src_mtime=$(find "$DAEMON_DIR/src" -name "*.rs" -printf '%T@\n' 2>/dev/null | sort -n | tail -1 | cut -d. -f1 || echo 0)
    cargo_mtime=$(stat -c %Y "$DAEMON_DIR/Cargo.toml" 2>/dev/null || echo 0)
    newest=$((src_mtime > cargo_mtime ? src_mtime : cargo_mtime))
    if [[ "$newest" -le "$bin_mtime" ]]; then
        echo "gNode daemon binary is up-to-date (use --force to rebuild)"
        exit 0
    fi
fi

# Default the signed-extension discovery dir to the sibling `pro/gNode`
# tree when the caller didn't set it. Makes extension inclusion the
# DEFAULT for every build path through this script (install, geodeploy,
# daemon rebuild) instead of an install.sh-only opt-in — closing the
# class of bug where a routine rebuild silently dropped CMS (and every
# other signed extension) by building lean. A lean core is now explicit:
# set GNODE_EXT_DIR to a path with no extension subdirectories.
if [[ -z "${GNODE_EXT_DIR:-}" ]]; then
    _ext_default="$(dirname "$PROJECT_ROOT")/pro/gNode"
    if [[ -d "$_ext_default" ]] && [[ -n "$(ls -A "$_ext_default" 2>/dev/null)" ]]; then
        export GNODE_EXT_DIR="$_ext_default"
    fi
fi

# Single unified build. Extension discovery via GNODE_EXT_DIR;
# no Cargo feature flags for extensions.
if [[ -n "${GNODE_EXT_DIR:-}" ]] && [[ -d "${GNODE_EXT_DIR}" ]]; then
    echo "Building gNode daemon (extensions: scanning ${GNODE_EXT_DIR}/)..."
else
    echo "Building gNode daemon (no extensions — GNODE_EXT_DIR unset or absent)..."
fi

cd "$DAEMON_DIR"
cargo build --release

echo "gNode daemon built successfully!"
echo "Binary: $BINARY"
