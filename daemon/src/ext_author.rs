// Authorized signer for verified extensions.
//
// This public key is the sole identity the build-time extension verifier
// (see `build.rs`) and the runtime Lua loader (see `src/extensions/`) will
// accept. Matching private key is held offline by the project author and
// is never committed.
//
// Rotation requires a daemon re-release with an updated pubkey baked in.
// Losing the private key permanently prevents signing new extensions;
// keep an off-machine backup.
//
// NOTE: This file is `include!`d by build.rs; top-level inner doc comments
// (`//!`) break that inclusion. Keep file-level notes as `//` comments.

/// Ed25519 public key (32 bytes, raw).
///
/// Fingerprint (sha256-16): `2ff9966fcad06b6d`.
pub const AUTHOR_PUBKEY: [u8; 32] = [
    0xf5, 0x64, 0xac, 0x05, 0x93, 0x9c, 0x1f, 0x55,
    0xc4, 0xfd, 0x75, 0x98, 0x3c, 0xd8, 0x05, 0x7d,
    0x52, 0x2f, 0x94, 0x18, 0x81, 0x1d, 0x14, 0x96,
    0x88, 0xbf, 0xc5, 0xce, 0x87, 0xf9, 0x9a, 0x29,
];
