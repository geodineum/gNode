#!/bin/bash
# =============================================================================
# geodineum-gen-ext-keys.sh — generate an Ed25519 signing keypair for
# Geodineum extensions.
# =============================================================================
#
# Produces:
#   <outdir>/ext_signer.key     priv key (PEM, 0600). Store offline.
#   <outdir>/ext_signer.pub     pub key  (PEM, 0644).
#   <outdir>/ext_author.rs      drop-in replacement for
#                               gNode/daemon/src/ext_author.rs. Contains
#                               AUTHOR_PUBKEY as a 32-byte raw array plus
#                               the sha256-16 fingerprint for reference.
#
# The existing gNode release bakes in one signer pubkey. This script is
# for initial key ceremony and for rotation. Rotation requires re-releasing
# the daemon binary with the new ext_author.rs baked in; all previously
# signed extensions must be re-signed by the new signer.
#
# Usage:
#   ./scripts/geodineum-gen-ext-keys.sh <outdir>
#
# Requires: openssl 3.0+ (ed25519 + rawin support), sha256sum.
# =============================================================================

set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "Usage: $0 <outdir>" >&2
    echo "       Writes ext_signer.key, ext_signer.pub, ext_author.rs to <outdir>." >&2
    exit 1
fi

OUTDIR="$1"
mkdir -p "$OUTDIR"
chmod 700 "$OUTDIR"

KEY_PATH="$OUTDIR/ext_signer.key"
PUB_PATH="$OUTDIR/ext_signer.pub"
RS_PATH="$OUTDIR/ext_author.rs"

if [[ -e "$KEY_PATH" ]]; then
    echo "Refusing to overwrite existing key at $KEY_PATH" >&2
    echo "Move or delete it manually, then re-run." >&2
    exit 1
fi

# Verify openssl supports ed25519 + rawin (needed by signing script).
if ! openssl list -public-key-algorithms 2>/dev/null | grep -qi ed25519; then
    echo "FATAL: openssl does not report ed25519 support. Install openssl 3.0+." >&2
    exit 1
fi

echo "Generating Ed25519 keypair..."
openssl genpkey -algorithm ed25519 -out "$KEY_PATH" 2>/dev/null
chmod 600 "$KEY_PATH"
openssl pkey -in "$KEY_PATH" -pubout -out "$PUB_PATH"
chmod 644 "$PUB_PATH"

# Extract the raw 32-byte public key. The PEM-to-raw conversion extracts
# the last 32 bytes of the DER-encoded SubjectPublicKeyInfo; ed25519 PEM
# public keys always have the raw pub at the tail.
RAW_PUB_HEX=$(openssl pkey -in "$KEY_PATH" -pubout -outform DER \
    | tail -c 32 \
    | xxd -p -c 32)

if [[ -z "$RAW_PUB_HEX" ]] || [[ ${#RAW_PUB_HEX} -ne 64 ]]; then
    echo "FATAL: failed to extract raw public key (got ${#RAW_PUB_HEX} hex chars, want 64)" >&2
    exit 1
fi

# Fingerprint: sha256 of the raw pubkey, first 16 hex chars.
FINGERPRINT=$(printf '%s' "$RAW_PUB_HEX" | xxd -r -p | sha256sum | cut -c1-16)

# Emit ext_author.rs in the format daemon/src/ext_author.rs uses.
{
    echo "// Authorized signer for verified extensions."
    echo "//"
    echo "// This public key is the sole identity the build-time extension verifier"
    echo "// (see \`build.rs\`) and the runtime Lua loader (see \`src/extensions/\`) will"
    echo "// accept. Matching private key is held offline by the project author and"
    echo "// is never committed."
    echo "//"
    echo "// Rotation requires a daemon re-release with an updated pubkey baked in."
    echo "// Losing the private key permanently prevents signing new extensions;"
    echo "// keep an off-machine backup."
    echo "//"
    echo "// NOTE: This file is \`include!\`d by build.rs; top-level inner doc comments"
    echo "// (\`//!\`) break that inclusion. Keep file-level notes as \`//\` comments."
    echo ""
    echo "/// Ed25519 public key (32 bytes, raw)."
    echo "///"
    echo "/// Fingerprint (sha256-16): \`${FINGERPRINT}\`."
    echo "pub const AUTHOR_PUBKEY: [u8; 32] = ["
    # Emit 4 rows of 8 hex bytes each: "    0xAA, 0xBB, ..., 0xHH,"
    for row in 0 1 2 3; do
        printf '    '
        for col in 0 1 2 3 4 5 6 7; do
            idx=$((row * 8 + col))
            byte=${RAW_PUB_HEX:$((idx * 2)):2}
            printf '0x%s, ' "$byte"
        done
        printf '\n'
    done
    echo "];"
} > "$RS_PATH"
chmod 644 "$RS_PATH"

echo ""
echo "OK — Ed25519 keypair generated."
echo ""
echo "  Private key : ${KEY_PATH}   (MODE 0600 — move to offline storage)"
echo "  Public key  : ${PUB_PATH}"
echo "  Rust array  : ${RS_PATH}    (replace gNode/daemon/src/ext_author.rs)"
echo ""
echo "  Fingerprint : ${FINGERPRINT}  (sha256-16 of raw pubkey)"
echo ""
echo "Next steps:"
echo "  1. BACKUP ${KEY_PATH} to an offline medium. Losing it locks out"
echo "     all future extension signing under this identity."
echo "  2. Replace gNode/daemon/src/ext_author.rs with ${RS_PATH}."
echo "  3. Rebuild the daemon; new builds will only accept extensions"
echo "     signed by this new key."
echo "  4. Re-sign existing extensions with:"
echo "     ./scripts/geodineum-sign-extensions.sh <ext-dir> --key ${KEY_PATH}"
