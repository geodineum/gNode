//! Runtime verification of signed extensions — the EXACT counterpart of the
//! build-time check in `build.rs` (`verify_and_stage` / `build_canonical_hashes`).
//!
//! Both verifiers MUST agree byte-for-byte on the canonical-hashes form and use
//! the same Ed25519 author key (`ext_author::AUTHOR_PUBKEY`). This module lets
//! the Lua loader (`scripts/load-valkey-functions.sh`) verify an extension via
//! `gnode-daemon verify-extension <dir>` — one signing scheme across build and
//! load — instead of a divergent openssl/`manifest.yaml` scheme that the
//! extensions never carried (which silently skipped every extension's Lua libs).
//!
//! If you change the canonical form here, change `build.rs::build_canonical_hashes`
//! identically (and re-sign every extension), or signatures will diverge.

use std::path::Path;

use ed25519_dalek::{Signature, VerifyingKey};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::ext_author::AUTHOR_PUBKEY;

/// Subset of `extension.yaml` that participates in the canonical hashes.
#[derive(Debug, Deserialize)]
struct ExtensionManifest {
    name: String,
    #[serde(default)]
    handler_files: Vec<String>,
    #[serde(default)]
    lua_libraries: Vec<String>,
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hasher.finalize().into()
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

fn validate_identifier(s: &str) -> Result<(), String> {
    if s.is_empty() {
        return Err("empty identifier".into());
    }
    let first = s.chars().next().unwrap();
    if !(first.is_ascii_lowercase() || first == '_') {
        return Err(format!(
            "identifier '{}' must start with lowercase ASCII letter or underscore",
            s
        ));
    }
    if !s
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
    {
        return Err(format!("identifier '{}' must match [a-z0-9_]+", s));
    }
    Ok(())
}

fn validate_handler_filename(name: &str) -> Result<(), String> {
    if name.contains('/') || name.contains('\\') || name.contains("..") {
        return Err(format!("handler filename '{}' contains path separators", name));
    }
    if !name.ends_with(".rs") {
        return Err(format!("handler filename '{}' must end with .rs", name));
    }
    validate_identifier(name.trim_end_matches(".rs"))
}

/// Build the canonical-hashes text over extension.yaml + handler_files +
/// lua_libraries. MUST match `build.rs::build_canonical_hashes` byte-for-byte.
fn build_canonical_hashes(
    manifest: &ExtensionManifest,
    manifest_bytes: &[u8],
    ext_dir: &Path,
) -> Result<String, String> {
    let mut out = String::new();
    out.push_str("format-version: 1\n");
    out.push_str(&format!("extension: {}\n", manifest.name));
    out.push_str(&format!(
        "extension-yaml-sha256: {}\n",
        hex(&sha256(manifest_bytes))
    ));

    let mut handlers = manifest.handler_files.clone();
    handlers.sort();
    for handler in &handlers {
        validate_handler_filename(handler)?;
        let path = ext_dir.join("src/handlers").join(handler);
        let bytes = std::fs::read(&path)
            .map_err(|e| format!("read handler {}: {}", path.display(), e))?;
        out.push_str(&format!("handler: {} {}\n", handler, hex(&sha256(&bytes))));
    }

    let mut lua_libs = manifest.lua_libraries.clone();
    lua_libs.sort();
    for lua in &lua_libs {
        validate_identifier(lua)?;
        let path = ext_dir.join("functions").join(format!("{}.lua", lua));
        let bytes = std::fs::read(&path)
            .map_err(|e| format!("read lua library {}: {}", path.display(), e))?;
        out.push_str(&format!("lua-library: {} {}\n", lua, hex(&sha256(&bytes))));
    }

    Ok(out)
}

/// Verify a signed extension directory exactly as build.rs does. Returns the
/// verified extension name on success, or a human-readable reason on failure.
pub fn verify_extension(ext_dir: &Path) -> Result<String, String> {
    let manifest_path = ext_dir.join("extension.yaml");
    let sig_path = ext_dir.join("extension.sig");

    if !manifest_path.is_file() {
        return Err("extension.yaml missing".into());
    }
    if !sig_path.is_file() {
        return Err("extension.sig missing".into());
    }

    let manifest_bytes =
        std::fs::read(&manifest_path).map_err(|e| format!("read extension.yaml: {}", e))?;
    let sig_bytes = std::fs::read(&sig_path).map_err(|e| format!("read extension.sig: {}", e))?;

    if sig_bytes.len() != Signature::BYTE_SIZE {
        return Err(format!(
            "signature is {} bytes; Ed25519 expects {}",
            sig_bytes.len(),
            Signature::BYTE_SIZE
        ));
    }
    let sig_arr: [u8; 64] = sig_bytes
        .as_slice()
        .try_into()
        .map_err(|_| "signature byte count mismatch".to_string())?;
    let signature = Signature::from_bytes(&sig_arr);

    let manifest: ExtensionManifest = serde_yaml::from_slice(&manifest_bytes)
        .map_err(|e| format!("parse extension.yaml: {}", e))?;
    validate_identifier(&manifest.name)?;

    let canonical = build_canonical_hashes(&manifest, &manifest_bytes, ext_dir)?;

    let verifying_key = VerifyingKey::from_bytes(&AUTHOR_PUBKEY)
        .map_err(|e| format!("AUTHOR_PUBKEY rejected by ed25519-dalek: {}", e))?;
    verifying_key
        .verify_strict(canonical.as_bytes(), &signature)
        .map_err(|e| format!("signature rejected over canonical hashes: {}", e))?;

    Ok(manifest.name)
}
