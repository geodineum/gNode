// gNode daemon build script.
//
// SINGLE extension-discovery channel (unified model):
//   Signed extensions (ed25519-verified).
//      GNODE_EXT_DIR (optional) points to a directory of extension
//      subdirectories. Each must contain:
//        - extension.yaml (the mature format: rust_feature, lua_libraries,
//          handler_files, commands, etc.)
//        - extension.sig  (64-byte raw Ed25519 signature over the canonical
//          hashes manifest described below)
//
//      Signature scope (Commit 0.2, fixes GN-D2.01):
//        The signature is NOT over extension.yaml alone. It is over a
//        deterministic canonical text manifesting sha256 hashes of:
//          - extension.yaml itself
//          - every handler_files entry (sorted by filename)
//          - every lua_libraries entry (sorted by name; file
//            functions/<name>.lua)
//        An attacker with write access to GNODE_EXT_DIR can no longer swap
//        handler .rs or functions/*.lua bodies while keeping the signature
//        valid — every byte that gets executed is covered.
//
//      The canonical hashes form (text, line-separated, LF only) is:
//          format-version: 1
//          extension: <name>
//          extension-yaml-sha256: <hex>
//          handler: <filename> <hex>
//          ...
//          lua-library: <filename> <hex>
//          ...
//
//      Signatures are verified against
//      `daemon/src/ext_author.rs::AUTHOR_PUBKEY`. Invalid or unsigned
//      extensions are skipped with a `cargo:warning`.
//
//      Verified extensions have their Rust handler files copied into
//      OUT_DIR; `OUT_DIR/ext_handlers.rs` is generated with a
//      `register_signed_extensions()` function that the daemon's
//      `CommandHandlerRegistry::new()` invokes, closing GN-D1.01.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use ed25519_dalek::{Signature, VerifyingKey};
use serde::Deserialize;
use sha2::{Digest, Sha256};

// Pull AUTHOR_PUBKEY in at compile time; keeps one source of truth for the
// signer identity between the build binary and the daemon binary.
include!("src/ext_author.rs");

/// Parsed subset of the mature `extension.yaml` schema. The runtime
/// `ExtensionManager` parses the full shape; build.rs only needs the fields
/// that influence codegen and hash-scope.
#[derive(Debug, Deserialize)]
#[allow(dead_code)] // `rust_feature` is declarative-only at build time.
struct ExtensionManifest {
    /// Lowercase snake_case identifier (used to generate module names).
    name: String,
    /// Rust feature flag that gates this extension's compiled handlers.
    /// Present when `rust_feature` is set in extension.yaml; absent for
    /// pure-Lua extensions.
    #[serde(default)]
    rust_feature: Option<String>,
    /// Rust handler source files (relative to `src/handlers/`).
    #[serde(default)]
    handler_files: Vec<String>,
    /// Lua libraries this extension ships (file: functions/<name>.lua).
    #[serde(default)]
    lua_libraries: Vec<String>,
}

/// Summary row emitted for each signed extension into OUT_DIR/ext_handlers.rs.
struct StagedExtension {
    /// Extension `name`.
    name: String,
    /// Staged handler filenames (already copied into OUT_DIR/<name>_handlers).
    handlers: Vec<String>,
}

// --------------------------------------------------------------------------
// Entry point
// --------------------------------------------------------------------------

fn main() {
    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR not set by cargo"));

    // Single discovery channel: GNODE_EXT_CMS_PATH retained as a
    // back-compat env-var alias so existing scripts setting it still
    // trigger a re-run, but it doesn't drive its own discovery path
    // anymore — the canonical lookup is GNODE_EXT_DIR pointing at a
    // directory of (signed) extension subdirectories.
    println!("cargo:rerun-if-env-changed=GNODE_EXT_CMS_PATH");
    println!("cargo:rerun-if-env-changed=GNODE_EXT_DIR");

    let (ext_rs, staged) = discover_signed_extensions(&out_dir);
    let registration_rs = emit_register_signed_extensions(&staged);
    let full_rs = format!("{}{}", ext_rs, registration_rs);
    fs::write(out_dir.join("ext_handlers.rs"), full_rs).unwrap_or_else(|e| {
        panic!(
            "Failed to write {}: {}",
            out_dir.join("ext_handlers.rs").display(),
            e
        );
    });
}

// --------------------------------------------------------------------------
// Signed-extension discovery
// --------------------------------------------------------------------------
//
// This is the ONLY discovery channel. CMS used to have a
// separate compile-time path (Cargo `cms` feature + GNODE_EXT_CMS_PATH
// env var + unsigned discover_cms_path()). Removed — CMS goes through
// the signed-extension pipeline like every other extension.

fn discover_signed_extensions(out_dir: &Path) -> (String, Vec<StagedExtension>) {
    let mut out = String::from(
        "// Generated by build.rs — signed-extension handler modules.\n\
         // Empty when GNODE_EXT_DIR is unset or contains no verified extensions.\n",
    );
    let mut staged: Vec<StagedExtension> = Vec::new();

    let Ok(ext_dir) = env::var("GNODE_EXT_DIR") else {
        return (out, staged);
    };
    let ext_dir = PathBuf::from(ext_dir);
    if !ext_dir.is_dir() {
        println!(
            "cargo:warning=GNODE_EXT_DIR is not a directory: {}",
            ext_dir.display()
        );
        return (out, staged);
    }
    println!("cargo:rerun-if-changed={}", ext_dir.display());

    let entries = match fs::read_dir(&ext_dir) {
        Ok(e) => e,
        Err(e) => {
            println!(
                "cargo:warning=cannot read GNODE_EXT_DIR {}: {}",
                ext_dir.display(),
                e
            );
            return (out, staged);
        }
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        match verify_and_stage(&path, out_dir) {
            Ok(Some((module_rs, staged_ext))) => {
                out.push_str(&module_rs);
                staged.push(staged_ext);
            }
            Ok(None) => {} // verified but no Rust handlers (Lua-only)
            Err(msg) => {
                println!(
                    "cargo:warning=skipping extension at {}: {}",
                    path.display(),
                    msg
                );
            }
        }
    }

    (out, staged)
}

fn verify_and_stage(
    ext_dir: &Path,
    out_dir: &Path,
) -> Result<Option<(String, StagedExtension)>, String> {
    let manifest_path = ext_dir.join("extension.yaml");
    let sig_path = ext_dir.join("extension.sig");

    if !manifest_path.is_file() {
        return Err("extension.yaml missing".into());
    }
    if !sig_path.is_file() {
        return Err("extension.sig missing".into());
    }

    let manifest_bytes =
        fs::read(&manifest_path).map_err(|e| format!("read extension.yaml: {}", e))?;
    let sig_bytes = fs::read(&sig_path).map_err(|e| format!("read extension.sig: {}", e))?;

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

    // Build the canonical-hashes form over extension.yaml + handler_files +
    // lua_libraries. An attacker swapping any file on disk after signing
    // causes hash-mismatch, rebuild-canonical-bytes differ, signature fails.
    let canonical =
        build_canonical_hashes(&manifest, &manifest_bytes, ext_dir).map_err(|e| e)?;

    let verifying_key = VerifyingKey::from_bytes(&AUTHOR_PUBKEY)
        .map_err(|e| format!("AUTHOR_PUBKEY rejected by ed25519-dalek: {}", e))?;
    verifying_key
        .verify_strict(canonical.as_bytes(), &signature)
        .map_err(|e| format!("signature rejected over canonical hashes: {}", e))?;

    // Mark every hashed file as a cargo build dependency so edits trigger
    // a rebuild (and re-verification, which will then fail loudly if the
    // extension hasn't been re-signed).
    for handler in &manifest.handler_files {
        println!(
            "cargo:rerun-if-changed={}",
            ext_dir.join("src/handlers").join(handler).display()
        );
    }
    for lua in &manifest.lua_libraries {
        let path = ext_dir.join("functions").join(format!("{}.lua", lua));
        println!("cargo:rerun-if-changed={}", path.display());
    }
    println!("cargo:rerun-if-changed={}", manifest_path.display());
    println!("cargo:rerun-if-changed={}", sig_path.display());

    // Pure-Lua extensions verify successfully but contribute no Rust modules.
    if manifest.handler_files.is_empty() {
        println!(
            "cargo:warning=verified extension '{}' (Lua-only, no Rust handlers)",
            manifest.name
        );
        return Ok(None);
    }

    let staging = out_dir.join(format!("{}_handlers", manifest.name));
    fs::create_dir_all(&staging).map_err(|e| format!("mkdir {}: {}", staging.display(), e))?;

    // Re-export `types` inside the ext_<name> namespace so handler
    // source files written against `super::types::{...}` resolve. Previously
    // the wrapping was `pub mod ext_cms { pub mod template {
    // include!(...) } }`; from inside template.rs, `super` is `ext_cms`,
    // which has no `types` submodule → unresolved import.
    // Previously (cms-feature path) the handlers were included directly at
    // `integration::handlers::` level so `super::types` worked. Namespaced
    // wrapping was adopted for collision-safety; this re-export plugs the
    // visibility gap.
    let mut module_rs = format!(
        "\npub mod ext_{} {{\n    // Re-export so handlers' `super::types::{{...}}` resolves\n    pub use crate::integration::handlers::types;\n",
        manifest.name
    );
    let mut staged_handlers: Vec<String> = Vec::new();
    for handler in &manifest.handler_files {
        validate_handler_filename(handler)?;
        let src = ext_dir.join("src/handlers").join(handler);
        let dst = staging.join(handler);
        fs::copy(&src, &dst)
            .map_err(|e| format!("copy {} → {}: {}", src.display(), dst.display(), e))?;

        let module_name = handler.trim_end_matches(".rs");
        validate_identifier(module_name)?;
        module_rs.push_str(&format!(
            "    pub mod {} {{\n        include!(concat!(env!(\"OUT_DIR\"), \"/{}_handlers/{}\"));\n    }}\n",
            module_name, manifest.name, handler
        ));
        staged_handlers.push(handler.clone());
    }
    module_rs.push_str("}\n");

    println!(
        "cargo:warning=verified extension '{}' loaded ({} handler{}, {} lua lib{})",
        manifest.name,
        manifest.handler_files.len(),
        if manifest.handler_files.len() == 1 { "" } else { "s" },
        manifest.lua_libraries.len(),
        if manifest.lua_libraries.len() == 1 { "" } else { "s" },
    );
    Ok(Some((
        module_rs,
        StagedExtension {
            name: manifest.name,
            handlers: staged_handlers,
        },
    )))
}

/// Build the deterministic signing manifest over extension.yaml plus every
/// handler and lua library file. Identical canonical form reproducible by
/// `scripts/geodineum-sign-extensions.sh`.
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

    // Handlers — sorted by filename for reproducibility.
    let mut handlers = manifest.handler_files.clone();
    handlers.sort();
    for handler in &handlers {
        validate_handler_filename(handler)?;
        let path = ext_dir.join("src/handlers").join(handler);
        let bytes = fs::read(&path)
            .map_err(|e| format!("read handler {}: {}", path.display(), e))?;
        out.push_str(&format!("handler: {} {}\n", handler, hex(&sha256(&bytes))));
    }

    // Lua libs — sorted by name.
    let mut lua_libs = manifest.lua_libraries.clone();
    lua_libs.sort();
    for lua in &lua_libs {
        validate_identifier(lua)?;
        let path = ext_dir.join("functions").join(format!("{}.lua", lua));
        let bytes = fs::read(&path)
            .map_err(|e| format!("read lua library {}: {}", path.display(), e))?;
        out.push_str(&format!("lua-library: {} {}\n", lua, hex(&sha256(&bytes))));
    }

    Ok(out)
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(bytes);
    h.finalize().into()
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

// --------------------------------------------------------------------------
// Generated register_signed_extensions()
// --------------------------------------------------------------------------
//
// Emits a single function the daemon calls from
// CommandHandlerRegistry::new(). It invokes the `register()` function each
// staged handler module exposes, using the same signature as base + CMS
// handlers in `integration::handlers::*::register`.
//
// Closes GN-D1.01: signed-extension handler modules are now reachable
// through the registry rather than being compiled-but-unused.

fn emit_register_signed_extensions(staged: &[StagedExtension]) -> String {
    let mut out = String::from(
        "\n\
         /// Call-site generated by build.rs. Invokes `register()` on every\n\
         /// signed-extension handler module that was staged into OUT_DIR.\n\
         ///\n\
         /// Each handler module is expected to expose:\n\
         ///   pub fn register(\n\
         ///       handlers: &mut std::collections::HashMap<String, super::super::command_handler::CommandHandlerFn>,\n\
         ///       async_handlers: &mut std::collections::HashMap<String, super::super::command_handler::AsyncCommandHandlerFn>,\n\
         ///       desc_vec: &mut Vec<super::super::command_handler::CommandDescriptor>,\n\
         ///   );\n\
         ///\n\
         /// matching the signature of base and CMS handler modules.\n\
         #[allow(dead_code, unused_variables)]\n\
         pub fn register_signed_extensions(\n\
             handlers: &mut std::collections::HashMap<String, crate::integration::command_handler::CommandHandlerFn>,\n\
             async_handlers: &mut std::collections::HashMap<String, crate::integration::command_handler::AsyncCommandHandlerFn>,\n\
             desc_vec: &mut Vec<crate::integration::command_handler::CommandDescriptor>,\n\
         ) {\n",
    );
    for ext in staged {
        for handler in &ext.handlers {
            let module_name = handler.trim_end_matches(".rs");
            out.push_str(&format!(
                "    ext_{name}::{module}::register(handlers, async_handlers, desc_vec);\n",
                name = ext.name,
                module = module_name
            ));
        }
    }
    if staged.is_empty() {
        out.push_str("    // No signed extensions staged — no-op.\n");
    }
    out.push_str("}\n");
    out
}

// --------------------------------------------------------------------------
// Safety: reject identifiers and filenames that could escape or inject code
// --------------------------------------------------------------------------

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
        return Err(format!(
            "identifier '{}' must match [a-z0-9_]+",
            s
        ));
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
