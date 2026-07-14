// gNode Extension Manager
//
// Runtime-queryable extension registry. Extensions are optional modules
// discovered from:
//
//   1. Per-extension path env var: `GNODE_EXT_<NAME>_PATH` points at an
//      extension repo containing `extension.yaml` or `extensions.yaml`
//      plus `src/handlers/` or `functions/`.
//
//   2. Signed extension directory: `GNODE_EXT_DIR` contains one
//      subdirectory per extension. Each subdirectory carries a signed
//      `manifest.yaml` + `manifest.sig` verified at build time against
//      `daemon/src/ext_author.rs::AUTHOR_PUBKEY`. This module validates
//      the same signature at runtime before trusting the manifest.
//
// The manager surfaces which extensions are present, what commands and
// Lua libraries each provides, and whether every declared Rust feature
// is compiled into the current binary.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use ed25519_dalek::{Signature, VerifyingKey};
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::ext_author::AUTHOR_PUBKEY;

// ============================================================================
// Extension manifest
// ============================================================================

/// Manifest entry for a single extension, parsed from `extension.yaml` or an
/// entry of `extensions.yaml`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtensionManifest {
    /// Unique extension identifier (lowercase snake_case).
    pub name: String,
    /// Human-readable display name.
    pub display_name: String,
    /// Extension version (semver).
    pub version: String,
    /// Human-readable description.
    pub description: String,
    /// Lua library names this extension provides (loaded from the
    /// extension's `functions/` directory).
    #[serde(default)]
    pub lua_libraries: Vec<String>,
    /// Rust feature flag that gates this extension's compiled-in handlers.
    #[serde(default)]
    pub rust_feature: Option<String>,
    /// Command names this extension provides.
    #[serde(default)]
    pub commands: Vec<String>,
    /// Rust handler source files this extension provides (`foo.rs`, ...).
    /// Used by `build.rs` to determine which files to stage into OUT_DIR.
    #[serde(default)]
    pub handler_files: Vec<String>,
    /// Whether the extension is enabled (config override).
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

fn default_enabled() -> bool {
    true
}

/// Top-level `extensions.yaml` schema for repos that bundle several
/// extensions under a single manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtensionsConfig {
    #[serde(default = "default_schema_version")]
    pub schema_version: String,
    pub extensions: Vec<ExtensionManifest>,
}

fn default_schema_version() -> String {
    "1.0".to_string()
}

// ============================================================================
// Extension runtime status
// ============================================================================

#[derive(Debug, Clone, Serialize)]
pub struct ExtensionStatus {
    pub name: String,
    pub display_name: String,
    pub version: String,
    pub description: String,
    /// Whether the declared Rust feature flag is compiled into this binary.
    pub rust_compiled: bool,
    /// Whether all declared Lua libraries are available on disk.
    pub lua_available: bool,
    /// Whether the extension is enabled by its manifest.
    pub config_enabled: bool,
    /// `rust_compiled && lua_available && config_enabled`.
    pub operational: bool,
    pub lua_libraries: Vec<String>,
    pub commands: Vec<String>,
}

// ============================================================================
// Extension manager
// ============================================================================

/// Manages discovery, verification, and introspection of optional extensions.
pub struct ExtensionManager {
    extensions: Vec<ExtensionManifest>,
    /// Discovered extension paths, keyed by repo directory name
    /// (e.g. `gNode-CMS`, or the operator's opaque identifier for a signed
    /// extension).
    extension_paths: HashMap<String, PathBuf>,
    compiled_features: Vec<String>,
}

impl ExtensionManager {
    /// Build a manager by discovering extensions.
    ///
    /// Discovery order (first found per name wins):
    ///   1. `ext_path_override` — optional CLI-provided path, registered
    ///      under the key `"default"`.
    ///   2. Per-name env vars matching `GNODE_EXT_<NAME>_PATH`.
    ///   3. Subdirectories of `GNODE_EXT_DIR` carrying `manifest.yaml` +
    ///      `manifest.sig` verified against `AUTHOR_PUBKEY`.
    pub fn discover(ext_path_override: Option<&str>) -> Self {
        let compiled_features = Self::detect_compiled_features();
        let extension_paths = Self::discover_extension_paths(ext_path_override);

        let mut extensions = Vec::new();
        for (repo_name, repo_path) in &extension_paths {
            if let Some(manifest) = Self::load_single_manifest(repo_path) {
                info!(
                    "Extension '{}' loaded from {} (repo: {})",
                    manifest.name,
                    repo_path.display(),
                    repo_name
                );
                extensions.push(manifest);
            } else {
                let multi = Self::load_manifest(repo_path);
                if !multi.is_empty() {
                    info!(
                        "{} extension(s) loaded from {} (repo: {})",
                        multi.len(),
                        repo_path.display(),
                        repo_name
                    );
                    extensions.extend(multi);
                }
            }
        }

        let manager = ExtensionManager {
            extensions,
            extension_paths,
            compiled_features,
        };

        let statuses = manager.list();
        let operational_count = statuses.iter().filter(|s| s.operational).count();
        let total = statuses.len();
        if total > 0 {
            info!(
                "gNode Extension Manager: {}/{} extensions operational",
                operational_count, total
            );
            for status in &statuses {
                if status.operational {
                    info!("  [OK] {} v{}", status.display_name, status.version);
                } else {
                    let reason = if !status.rust_compiled {
                        "Rust feature not compiled"
                    } else if !status.lua_available {
                        "Lua libraries not found"
                    } else {
                        "disabled in config"
                    };
                    warn!("  [--] {} v{} ({})", status.display_name, status.version, reason);
                }
            }
        } else {
            info!("gNode Extension Manager: no extensions discovered");
        }

        manager
    }

    /// List every registered extension's runtime status.
    pub fn list(&self) -> Vec<ExtensionStatus> {
        self.extensions
            .iter()
            .map(|ext| {
                let rust_compiled = ext
                    .rust_feature
                    .as_ref()
                    .map(|f| self.compiled_features.contains(f))
                    .unwrap_or(true); // no feature gate ⇒ always compiled

                let lua_available = self.check_lua_available(ext);
                let operational = rust_compiled && lua_available && ext.enabled;

                ExtensionStatus {
                    name: ext.name.clone(),
                    display_name: ext.display_name.clone(),
                    version: ext.version.clone(),
                    description: ext.description.clone(),
                    rust_compiled,
                    lua_available,
                    config_enabled: ext.enabled,
                    operational,
                    lua_libraries: ext.lua_libraries.clone(),
                    commands: ext.commands.clone(),
                }
            })
            .collect()
    }

    /// Look up a single extension by name.
    pub fn get(&self, name: &str) -> Option<ExtensionStatus> {
        self.list().into_iter().find(|s| s.name == name)
    }

    /// Resolve the filesystem path of a registered extension repo.
    pub fn extension_path(&self, repo_name: &str) -> Option<&Path> {
        self.extension_paths.get(repo_name).map(|p| p.as_path())
    }

    /// All discovered extension paths, keyed by repo name.
    pub fn all_extension_paths(&self) -> &HashMap<String, PathBuf> {
        &self.extension_paths
    }

    /// Convenience accessor for the CMS companion repo path.
    pub fn cms_path(&self) -> Option<&Path> {
        self.extension_paths.get("gNode-CMS").map(|p| p.as_path())
    }

    /// `functions/` directories of every discovered extension.
    pub fn lua_library_paths(&self) -> Vec<PathBuf> {
        self.extension_paths
            .values()
            .map(|p| p.join("functions"))
            .filter(|p| p.is_dir())
            .collect()
    }

    /// Names of every Lua library required by an operational extension.
    pub fn operational_lua_libraries(&self) -> Vec<String> {
        self.list()
            .into_iter()
            .filter(|s| s.operational)
            .flat_map(|s| s.lua_libraries)
            .collect()
    }

    /// Whether a specific Rust feature flag is compiled into this binary.
    pub fn is_feature_compiled(&self, feature: &str) -> bool {
        self.compiled_features.contains(&feature.to_string())
    }

    pub fn operational_count(&self) -> usize {
        self.list().iter().filter(|s| s.operational).count()
    }

    pub fn total_count(&self) -> usize {
        self.extensions.len()
    }

    // ---------------------------------------------------------------------
    // Private helpers
    // ---------------------------------------------------------------------

    fn detect_compiled_features() -> Vec<String> {
        #[allow(unused_mut)]
        let mut features = vec!["base".to_string()];

        features.push("cms".to_string());

        features
    }

    /// Collect extension paths from `ext_path_override`, `GNODE_EXT_<NAME>_PATH`
    /// env vars, and verified subdirectories of `GNODE_EXT_DIR`.
    fn discover_extension_paths(ext_path_override: Option<&str>) -> HashMap<String, PathBuf> {
        let mut paths: HashMap<String, PathBuf> = HashMap::new();

        // Signed-extensions-only mode: when on, EVERY source (incl. the per-path
        // dev overrides #1/#2) must carry a valid author Ed25519 signature, not
        // just GNODE_EXT_DIR (#3). Off by default for back-compat with dev
        // override paths; production should set GNODE_SIGNED_EXT_ONLY=1.
        let signed_only = std::env::var("GNODE_SIGNED_EXT_ONLY")
            .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
            .unwrap_or(false);
        if signed_only {
            info!("GNODE_SIGNED_EXT_ONLY active — only author-signed extensions will load");
        }

        // 1. Explicit CLI override
        if let Some(p) = ext_path_override {
            let path = PathBuf::from(p);
            if path.is_dir() {
                if signed_only {
                    if let Err(e) = Self::verify_extension_signature(&path) {
                        warn!("Rejecting override extension at {} (signed-only mode): {}", path.display(), e);
                    } else {
                        debug!("Extension override at {} (signature verified)", path.display());
                        paths.insert("default".to_string(), path);
                    }
                } else {
                    debug!("Extension override at {}", path.display());
                    paths.insert("default".to_string(), path);
                }
            } else {
                warn!("Extension path override does not exist: {}", p);
            }
        }

        // 2. GNODE_EXT_<NAME>_PATH env vars (scan env for the prefix)
        for (key, value) in std::env::vars() {
            let Some(stripped) = key
                .strip_prefix("GNODE_EXT_")
                .and_then(|s| s.strip_suffix("_PATH"))
            else {
                continue;
            };
            if stripped.is_empty() {
                continue;
            }
            let path = PathBuf::from(&value);
            if !path.is_dir() || !Self::is_valid_extension_repo(&path) {
                debug!("{}='{}' skipped (not a valid extension repo)", key, value);
                continue;
            }
            // Repo-directory name is authoritative for the hashmap key, not
            // the env-var token — preserves case/hyphenation used by callers.
            let repo_name = path
                .file_name()
                .and_then(|n| n.to_str())
                .map(String::from)
                .unwrap_or_else(|| stripped.to_ascii_lowercase());
            if !paths.contains_key(&repo_name) {
                if signed_only {
                    if let Err(e) = Self::verify_extension_signature(&path) {
                        warn!("{}='{}' rejected (signed-only mode): {}", key, value, e);
                        continue;
                    }
                }
                debug!("Extension '{}' from {}: {}", repo_name, key, path.display());
                paths.insert(repo_name, path);
            }
        }

        // 3. Signed extensions in GNODE_EXT_DIR
        if let Ok(dir) = std::env::var("GNODE_EXT_DIR") {
            let dir = PathBuf::from(dir);
            if let Ok(entries) = fs::read_dir(&dir) {
                for entry in entries.flatten() {
                    let ext_dir = entry.path();
                    if !ext_dir.is_dir() {
                        continue;
                    }
                    let name = match ext_dir.file_name().and_then(|n| n.to_str()) {
                        Some(n) => n.to_string(),
                        None => continue,
                    };
                    if paths.contains_key(&name) {
                        continue; // explicit override takes precedence
                    }
                    match Self::verify_extension_signature(&ext_dir) {
                        Ok(()) => {
                            debug!("Signed extension '{}' verified at {}", name, ext_dir.display());
                            paths.insert(name, ext_dir);
                        }
                        Err(e) => {
                            warn!(
                                "Ignoring unverified extension at {}: {}",
                                ext_dir.display(),
                                e
                            );
                        }
                    }
                }
            }
        }

        if paths.is_empty() {
            debug!("No extensions discovered");
        } else {
            info!(
                "Discovered {} extension path(s): {}",
                paths.len(),
                paths.keys().cloned().collect::<Vec<_>>().join(", ")
            );
        }

        paths
    }

    /// Verify the Ed25519 signature on an extension using the canonical-
    /// hashes form — identical to `build.rs`. The signature covers
    /// extension.yaml PLUS sha256 of every handler_files entry PLUS
    /// sha256 of every lua_libraries entry. An attacker swapping any
    /// of these files after signing causes hash-mismatch and rejection.
    ///
    /// Runtime re-verification is required because Lua libraries are read
    /// from disk at `FUNCTION LOAD` time; trusting build.rs alone would
    /// leave a TOCTOU window against the `functions/` directory.
    fn verify_extension_signature(ext_dir: &Path) -> Result<(), String> {
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
        let sig_arr: [u8; 64] = sig_bytes
            .as_slice()
            .try_into()
            .map_err(|_| "signature length != 64".to_string())?;
        let signature = Signature::from_bytes(&sig_arr);

        let manifest: ExtensionManifest = serde_yaml::from_slice(&manifest_bytes)
            .map_err(|e| format!("parse extension.yaml: {}", e))?;

        let canonical = Self::build_canonical_hashes(&manifest, &manifest_bytes, ext_dir)?;

        let vk = VerifyingKey::from_bytes(&AUTHOR_PUBKEY)
            .map_err(|e| format!("AUTHOR_PUBKEY invalid: {}", e))?;
        vk.verify_strict(canonical.as_bytes(), &signature)
            .map_err(|e| format!("signature rejected over canonical hashes: {}", e))
    }

    /// Mirrors the canonical-hashes form produced by `build.rs`. Keeping
    /// both implementations byte-identical is essential; divergence here
    /// would silently accept or reject valid signatures.
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
            hex_lowercase(&sha256_bytes(manifest_bytes))
        ));

        let mut handlers = manifest.handler_files.clone();
        handlers.sort();
        for handler in &handlers {
            let path = ext_dir.join("src/handlers").join(handler);
            let bytes = fs::read(&path)
                .map_err(|e| format!("read handler {}: {}", path.display(), e))?;
            out.push_str(&format!(
                "handler: {} {}\n",
                handler,
                hex_lowercase(&sha256_bytes(&bytes))
            ));
        }

        let mut lua_libs = manifest.lua_libraries.clone();
        lua_libs.sort();
        for lua in &lua_libs {
            let path = ext_dir.join("functions").join(format!("{}.lua", lua));
            let bytes = fs::read(&path)
                .map_err(|e| format!("read lua library {}: {}", path.display(), e))?;
            out.push_str(&format!(
                "lua-library: {} {}\n",
                lua,
                hex_lowercase(&sha256_bytes(&bytes))
            ));
        }

        Ok(out)
    }

    fn is_valid_extension_repo(path: &Path) -> bool {
        // Only extension.yaml is authoritative; extensions.yaml
        // multi-extension bundles remain supported.
        let has_manifest =
            path.join("extension.yaml").exists() || path.join("extensions.yaml").exists();
        let has_code =
            path.join("src/handlers").is_dir() || path.join("functions").is_dir();
        has_manifest && has_code
    }

    fn load_single_manifest(path: &Path) -> Option<ExtensionManifest> {
        let manifest_path = path.join("extension.yaml");
        if !manifest_path.exists() {
            return None;
        }
        match fs::read_to_string(&manifest_path) {
            Ok(content) => match serde_yaml::from_str::<ExtensionManifest>(&content) {
                Ok(manifest) => {
                    info!(
                        "Loaded extension '{}' from {}",
                        manifest.name,
                        manifest_path.display()
                    );
                    Some(manifest)
                }
                Err(e) => {
                    warn!(
                        "Failed to parse extension.yaml at {}: {}",
                        manifest_path.display(),
                        e
                    );
                    None
                }
            },
            Err(e) => {
                warn!(
                    "Failed to read extension.yaml at {}: {}",
                    manifest_path.display(),
                    e
                );
                None
            }
        }
    }

    fn load_manifest(path: &Path) -> Vec<ExtensionManifest> {
        let manifest_path = path.join("extensions.yaml");
        if !manifest_path.exists() {
            return Vec::new();
        }
        match fs::read_to_string(&manifest_path) {
            Ok(content) => match serde_yaml::from_str::<ExtensionsConfig>(&content) {
                Ok(config) => {
                    info!(
                        "Loaded {} extensions from {}",
                        config.extensions.len(),
                        manifest_path.display()
                    );
                    config.extensions
                }
                Err(e) => {
                    warn!("Failed to parse extensions.yaml: {}", e);
                    Vec::new()
                }
            },
            Err(e) => {
                warn!("Failed to read extensions.yaml: {}", e);
                Vec::new()
            }
        }
    }

    fn check_lua_available(&self, ext: &ExtensionManifest) -> bool {
        if ext.lua_libraries.is_empty() {
            return true;
        }
        let search_dirs: Vec<PathBuf> = self
            .extension_paths
            .values()
            .map(|p| p.join("functions"))
            .filter(|p| p.is_dir())
            .collect();
        if search_dirs.is_empty() {
            return false;
        }
        ext.lua_libraries.iter().all(|lib| {
            let filename = format!("{}.lua", lib);
            search_dirs.iter().any(|dir| dir.join(&filename).exists())
        })
    }
}

// ============================================================================
// Hashing helpers (module-level; shared by build-time and runtime verifiers)
// ============================================================================

fn sha256_bytes(input: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(input);
    h.finalize().into()
}

fn hex_lowercase(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

// ============================================================================
// FUNCTION LOAD: ship operational extension Lua libs into ValKey on startup
// ============================================================================

/// For every operational extension, `FUNCTION LOAD REPLACE` every declared
/// Lua library into ValKey. The library content is the `.lua` file bytes
/// found under the extension's `functions/` directory.
///
/// Best-effort — each library is attempted independently; failures are
/// logged as warnings and do not abort daemon startup (the installer's
/// `load-valkey-functions.sh` is the primary pre-flight load path; this
/// runtime call is a safety net that also catches hot-swapped extensions
/// after a SIGHUP reload).
///
/// Returns `(loaded_count, failed_count)`.
pub fn load_lua_libraries_into_valkey(conn: &mut redis::Connection) -> (usize, usize) {
    let mgr = get_extension_manager();
    let mut loaded = 0usize;
    let mut failed = 0usize;
    let search_dirs: Vec<PathBuf> = mgr
        .extension_paths
        .values()
        .map(|p| p.join("functions"))
        .filter(|p| p.is_dir())
        .collect();

    if search_dirs.is_empty() {
        debug!("No extension functions/ directories discovered; skipping FUNCTION LOAD");
        return (0, 0);
    }

    for status in mgr.list() {
        if !status.operational {
            continue;
        }
        for lib_name in &status.lua_libraries {
            let filename = format!("{}.lua", lib_name);
            let Some(lib_path) = search_dirs.iter().find_map(|d| {
                let candidate = d.join(&filename);
                if candidate.is_file() {
                    Some(candidate)
                } else {
                    None
                }
            }) else {
                warn!(
                    "Lua library '{}' declared by extension '{}' not found on disk; skipping",
                    lib_name, status.name
                );
                failed += 1;
                continue;
            };

            let bytes = match fs::read(&lib_path) {
                Ok(b) => b,
                Err(e) => {
                    error!(
                        "Failed to read {} for extension '{}': {}",
                        lib_path.display(),
                        status.name,
                        e
                    );
                    failed += 1;
                    continue;
                }
            };

            // ValKey FUNCTION LOAD REPLACE <code> returns the library name on
            // success. ACL restriction on the daemon user (`gnode_daemon`)
            // without FUNCTION LOAD permission will surface as an error here
            // rather than at first FCALL — log + continue; the installer's
            // `scripts/load-valkey-functions.sh` runs as admin and is the
            // authoritative loader.
            let result: Result<String, redis::RedisError> = redis::cmd("FUNCTION")
                .arg("LOAD")
                .arg("REPLACE")
                .arg(&bytes)
                .query(conn);
            match result {
                Ok(loaded_name) => {
                    info!(
                        "FUNCTION LOAD REPLACE: extension '{}' library '{}' -> {}",
                        status.name, lib_name, loaded_name
                    );
                    loaded += 1;
                }
                Err(e) => {
                    warn!(
                        "FUNCTION LOAD failed for extension '{}' library '{}' ({}): {}",
                        status.name,
                        lib_name,
                        lib_path.display(),
                        e
                    );
                    failed += 1;
                }
            }
        }
    }

    if loaded > 0 || failed > 0 {
        info!(
            "Extension Lua libraries: {} loaded, {} failed into ValKey",
            loaded, failed
        );
    }
    (loaded, failed)
}

// ============================================================================
// Global singleton
// ============================================================================

use std::sync::OnceLock;

static EXTENSION_MANAGER: OnceLock<ExtensionManager> = OnceLock::new();

/// Initialize the global extension manager.
///
/// MUST be called BEFORE any code path that calls `get_extension_manager()` —
/// the latter falls back to `discover(None)` on first access, which permanently
/// fixes the manager state and would silently shadow the operator-supplied
/// `--ext-path` override (GN-D3.06 closure: returns `Result` to surface the
/// double-init case to the caller instead of swallowing it via `let _ =`).
///
/// # Errors
/// Returns `Err` if the manager has already been set — by an earlier
/// `initialize_extension_manager(...)` call OR by a `get_extension_manager()`
/// call that triggered the lazy-init fallback. The caller is responsible for
/// deciding whether a double-init is fatal (operator misconfiguration) or
/// recoverable (test harness re-init).
pub fn initialize_extension_manager(ext_path: Option<&str>) -> Result<(), &'static str> {
    EXTENSION_MANAGER
        .set(ExtensionManager::discover(ext_path))
        .map_err(|_| {
            "extension manager already initialized; \
             initialize_extension_manager() must run before any get_extension_manager() call"
        })
}

pub fn get_extension_manager() -> &'static ExtensionManager {
    EXTENSION_MANAGER.get_or_init(|| ExtensionManager::discover(None))
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_compiled_features() {
        let features = ExtensionManager::detect_compiled_features();
        assert!(features.contains(&"base".to_string()));
    }

    #[test]
    fn test_manager_with_no_extensions() {
        let manager = ExtensionManager {
            extensions: vec![],
            extension_paths: HashMap::new(),
            compiled_features: vec!["base".to_string()],
        };
        assert_eq!(manager.total_count(), 0);
        assert_eq!(manager.operational_count(), 0);
        assert!(manager.cms_path().is_none());
        assert!(manager.extension_path("anything").is_none());
        assert!(manager.all_extension_paths().is_empty());
    }

    #[test]
    fn test_single_extension_manifest_deserialize() {
        let yaml = r#"
name: cms
display_name: Content Management System
version: "1.0.0"
description: Template rendering and content management
lua_libraries: ["gnode_asset"]
rust_feature: cms
commands: ["render_template", "content_store"]
enabled: true
"#;
        let manifest: ExtensionManifest = serde_yaml::from_str(yaml).expect("parse yaml");
        assert_eq!(manifest.name, "cms");
        assert_eq!(manifest.lua_libraries, vec!["gnode_asset"]);
        assert_eq!(manifest.rust_feature, Some("cms".to_string()));
        assert_eq!(manifest.commands.len(), 2);
    }

    #[test]
    fn test_handler_files_deserialize() {
        let yaml = r#"
name: cms
display_name: Content Management System
version: "1.0.0"
description: CMS extension with handler files
lua_libraries: ["gnode_asset"]
rust_feature: cms
commands: ["render_template"]
handler_files: ["template.rs", "content.rs", "asset.rs"]
enabled: true
"#;
        let manifest: ExtensionManifest = serde_yaml::from_str(yaml).expect("parse yaml");
        assert_eq!(manifest.handler_files, vec!["template.rs", "content.rs", "asset.rs"]);
    }

    #[test]
    fn test_handler_files_default_empty() {
        let yaml = r#"
name: simple
display_name: Simple Extension
version: "1.0.0"
description: Extension without handler_files field
"#;
        let manifest: ExtensionManifest = serde_yaml::from_str(yaml).expect("parse yaml");
        assert!(manifest.handler_files.is_empty());
    }

    #[test]
    fn test_extension_status_operational_logic() {
        let manager = ExtensionManager {
            extensions: vec![ExtensionManifest {
                name: "test".to_string(),
                display_name: "Test".to_string(),
                version: "1.0.0".to_string(),
                description: "Test".to_string(),
                lua_libraries: vec![],
                rust_feature: None,
                commands: vec![],
                handler_files: vec![],
                enabled: true,
            }],
            extension_paths: HashMap::new(),
            compiled_features: vec!["base".to_string()],
        };

        let statuses = manager.list();
        assert_eq!(statuses.len(), 1);
        assert!(statuses[0].operational);
    }

    #[test]
    fn test_is_valid_extension_repo() {
        let fake_path = PathBuf::from("/tmp/nonexistent_extension_repo_test");
        assert!(!ExtensionManager::is_valid_extension_repo(&fake_path));
    }

    #[test]
    fn test_path_accessors() {
        let mut paths = HashMap::new();
        paths.insert("gNode-CMS".to_string(), PathBuf::from("/srv/ext/gNode-CMS"));

        let manager = ExtensionManager {
            extensions: vec![],
            extension_paths: paths,
            compiled_features: vec!["base".to_string()],
        };

        assert_eq!(
            manager.cms_path(),
            Some(Path::new("/srv/ext/gNode-CMS"))
        );
        assert_eq!(
            manager.extension_path("gNode-CMS"),
            Some(Path::new("/srv/ext/gNode-CMS"))
        );
        assert_eq!(manager.all_extension_paths().len(), 1);
    }
}
