#!/bin/bash
# =============================================================================
# geodineum-sign-extensions.sh — sign a gNode extension with Ed25519.
# =============================================================================
#
# Produces an `extension.sig` file next to the target extension's
# `extension.yaml`. The signature is over the canonical-hashes form
# documented in gNode/daemon/build.rs:
#
#   format-version: 1
#   extension: <name>
#   extension-yaml-sha256: <hex>
#   handler: <filename> <hex>          (sorted by filename)
#   lua-library: <name> <hex>          (sorted by name)
#
# This MUST stay byte-identical to the verifier in build.rs and
# daemon/src/extensions/mod.rs — any divergence silently rejects valid
# signatures or accepts invalid ones.
#
# Usage:
#   ./scripts/geodineum-sign-extensions.sh <ext-dir> --key <priv.pem> [--force]
#
# Exits non-zero on any error; the operator sees a clear diagnostic and
# no sig is written.
#
# Requires: openssl 3.0+ (ed25519 + rawin), sha256sum, python3 or yq for
# YAML parsing. Uses python3 (present on Ubuntu 22.04 / Debian 12 by
# default); no third-party yq dependency needed.
# =============================================================================

set -euo pipefail

EXT_DIR=""
KEY_PATH=""
FORCE=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --key)
            KEY_PATH="$2"; shift 2 ;;
        --key=*)
            KEY_PATH="${1#*=}"; shift ;;
        --force)
            FORCE=true; shift ;;
        -h|--help)
            sed -n '2,30p' "$0"; exit 0 ;;
        -*)
            echo "Unknown option: $1" >&2; exit 1 ;;
        *)
            if [[ -z "$EXT_DIR" ]]; then EXT_DIR="$1"
            else echo "Unexpected positional arg: $1" >&2; exit 1
            fi
            shift ;;
    esac
done

if [[ -z "$EXT_DIR" || -z "$KEY_PATH" ]]; then
    echo "Usage: $0 <ext-dir> --key <priv.pem> [--force]" >&2
    exit 1
fi
if [[ ! -d "$EXT_DIR" ]]; then
    echo "FATAL: not a directory: $EXT_DIR" >&2; exit 1
fi
if [[ ! -r "$KEY_PATH" ]]; then
    echo "FATAL: private key unreadable: $KEY_PATH" >&2; exit 1
fi

MANIFEST="$EXT_DIR/extension.yaml"
SIG="$EXT_DIR/extension.sig"
if [[ ! -r "$MANIFEST" ]]; then
    echo "FATAL: extension.yaml not found at $MANIFEST" >&2; exit 1
fi
if [[ -e "$SIG" && "$FORCE" != "true" ]]; then
    echo "FATAL: $SIG already exists. Use --force to overwrite." >&2; exit 1
fi

# Parse extension.yaml fields we need (name, handler_files, lua_libraries)
# using python3 (ships with every supported Ubuntu + Debian). Avoids a yq
# dependency; the output is a strict-format shell-safe blob.
PARSE_OUT=$(python3 - "$MANIFEST" <<'PYEOF'
import sys, yaml
path = sys.argv[1]
try:
    with open(path, 'r') as f:
        m = yaml.safe_load(f) or {}
except Exception as e:
    sys.stderr.write(f"FATAL: parse {path}: {e}\n")
    sys.exit(1)
name = m.get('name', '')
if not name:
    sys.stderr.write("FATAL: extension.yaml missing required 'name' field\n")
    sys.exit(1)
handlers = m.get('handler_files', []) or []
lua = m.get('lua_libraries', []) or []
# Validate no dangerous characters in names.
import re
ident_re = re.compile(r'^[a-z_][a-z0-9_]*$')
if not ident_re.match(name):
    sys.stderr.write(f"FATAL: name '{name}' must match [a-z_][a-z0-9_]*\n")
    sys.exit(1)
for h in handlers:
    if not isinstance(h, str) or '/' in h or '\\' in h or '..' in h \
       or not h.endswith('.rs'):
        sys.stderr.write(f"FATAL: handler '{h}' invalid\n")
        sys.exit(1)
    if not ident_re.match(h[:-3]):
        sys.stderr.write(f"FATAL: handler name '{h}' invalid\n")
        sys.exit(1)
for lib in lua:
    if not isinstance(lib, str) or not ident_re.match(lib):
        sys.stderr.write(f"FATAL: lua library '{lib}' invalid\n")
        sys.exit(1)
# Shell-safe quoting: identifiers are already validated [a-z_][a-z0-9_]*,
# but eval requires VAR="value" form so multi-word values don't splat.
print(f'NAME="{name}"')
print(f'HANDLERS="{" ".join(sorted(handlers))}"')
print(f'LUA_LIBS="{" ".join(sorted(lua))}"')
PYEOF
)

# shellcheck disable=SC2046  # word-split intended
eval "$PARSE_OUT"

# Build canonical-hashes text exactly as build.rs + extensions/mod.rs do.
CANONICAL_TMP=$(mktemp)
trap 'rm -f "$CANONICAL_TMP"' EXIT

{
    printf 'format-version: 1\n'
    printf 'extension: %s\n' "$NAME"
    # sha256(extension.yaml)
    YAML_SHA=$(sha256sum "$MANIFEST" | cut -d' ' -f1)
    printf 'extension-yaml-sha256: %s\n' "$YAML_SHA"
    # Handlers sorted already
    for h in $HANDLERS; do
        HANDLER_PATH="$EXT_DIR/src/handlers/$h"
        if [[ ! -r "$HANDLER_PATH" ]]; then
            echo "FATAL: handler file not readable: $HANDLER_PATH" >&2
            exit 1
        fi
        HANDLER_SHA=$(sha256sum "$HANDLER_PATH" | cut -d' ' -f1)
        printf 'handler: %s %s\n' "$h" "$HANDLER_SHA"
    done
    # Lua libs sorted already
    for lib in $LUA_LIBS; do
        LUA_PATH="$EXT_DIR/functions/${lib}.lua"
        if [[ ! -r "$LUA_PATH" ]]; then
            echo "FATAL: lua library not readable: $LUA_PATH" >&2
            exit 1
        fi
        LUA_SHA=$(sha256sum "$LUA_PATH" | cut -d' ' -f1)
        printf 'lua-library: %s %s\n' "$lib" "$LUA_SHA"
    done
} > "$CANONICAL_TMP"

# Sign the canonical bytes. openssl -rawin passes the file content as-is
# to ed25519 (ed25519 doesn't pre-hash — it hashes internally in Ed25519ph
# mode, or consumes raw input in pure Ed25519). ed25519-dalek verify_strict
# uses the pure-Ed25519 consumption matching `pkeyutl -rawin`.
openssl pkeyutl \
    -sign \
    -inkey "$KEY_PATH" \
    -rawin \
    -in "$CANONICAL_TMP" \
    -out "$SIG" 2>/dev/null

# Verify length + round-trip via the derived public key (sanity check).
SIG_LEN=$(stat -c '%s' "$SIG")
if [[ "$SIG_LEN" -ne 64 ]]; then
    echo "FATAL: signature is $SIG_LEN bytes, expected 64 (Ed25519)" >&2
    rm -f "$SIG"
    exit 1
fi

PUB_TMP=$(mktemp); trap 'rm -f "$CANONICAL_TMP" "$PUB_TMP"' EXIT
openssl pkey -in "$KEY_PATH" -pubout -out "$PUB_TMP" 2>/dev/null
if ! openssl pkeyutl \
        -verify \
        -pubin -inkey "$PUB_TMP" \
        -rawin -in "$CANONICAL_TMP" \
        -sigfile "$SIG" \
        >/dev/null 2>&1; then
    echo "FATAL: self-verify of freshly written $SIG failed. Check openssl version." >&2
    rm -f "$SIG"
    exit 1
fi

# Fingerprint (sha256-16 of raw pubkey) for operator reference.
RAW_PUB_HEX=$(openssl pkey -in "$KEY_PATH" -pubout -outform DER | tail -c 32 | xxd -p -c 32)
FINGERPRINT=$(printf '%s' "$RAW_PUB_HEX" | xxd -r -p | sha256sum | cut -c1-16)

echo "OK — signed '$NAME'."
echo "  Extension dir : $EXT_DIR"
echo "  Signer        : $KEY_PATH  (fp: $FINGERPRINT)"
echo "  Signature     : $SIG  (64 bytes)"
echo "  Canonical form:"
sed 's/^/    /' "$CANONICAL_TMP"
