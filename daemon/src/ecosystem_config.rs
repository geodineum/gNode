//! Geodineum ecosystem bootstrap loader (Rust).
//!
//! Single-route config loader. Mirrors `lib/bootstrap-loader.sh` (Bash) and
//! `gCore/Bootstrap/EcosystemConfigLoader.php` (PHP).
//!
//! - Tier 1 (disk): `/etc/geodineum/bootstrap.env` — owner `root`, group
//!   `geodineum-bootstrap` (a narrow group containing only the legitimate
//!   readers: gnode + deploy user + www-data + service users), mode `0640` or
//!   stricter (`0600` also accepted for installs that want root-only).
//!   World-readable (`0644`) and group-writable bits are REJECTED.
//!   Strict-deny posture: no information leakage about deployment
//!   topology via the file's existence/contents. Exactly 3 whitelisted
//!   keys (`VALKEY_HOST`, `VALKEY_PORT`, `VALKEY_CREDS_PATH`), strict
//!   regex parse, fail-fast on drift.
//! - Tier 2 (ValKey): `geodineum:bootstrap:<KEY>` plus
//!   `geodineum:bootstrap:_index`. Iterated and exported into the process
//!   environment.
//!
//! Public API:
//!   ecosystem_config::load() -> gnode::Result<()>
//!   ecosystem_config::load_disk_tier() -> gnode::Result<()>
//!   ecosystem_config::load_valkey_tier() -> gnode::Result<()>

use crate::{GeometricError, Result};
use redis::Commands;
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::time::Duration;

const DEFAULT_FILE: &str = "/etc/geodineum/bootstrap.env";
const DISK_KEYS: &[&str] = &["VALKEY_HOST", "VALKEY_PORT", "VALKEY_CREDS_PATH"];
const VK_PREFIX: &str = "geodineum:bootstrap:";
const VK_INDEX: &str = "geodineum:bootstrap:_index";

/// Canonical entry point. Loads the disk tier then the ValKey tier.
pub fn load() -> Result<()> {
    load_disk_tier()?;
    load_valkey_tier()?;
    Ok(())
}

/// Verify ownership/mode of the disk file, parse the whitelisted KEY=value
/// lines, set them into the process environment. Fail-fast on any drift.
pub fn load_disk_tier() -> Result<()> {
    let path = std::env::var("GEODINEUM_BOOTSTRAP_FILE")
        .unwrap_or_else(|_| DEFAULT_FILE.to_string());

    let meta = fs::metadata(&path).map_err(|e| {
        GeometricError::Other(format!(
            "bootstrap-loader: {} missing or unreadable ({})",
            path, e
        ))
    })?;

    let dev_mode = std::env::var("GEODINEUM_BOOTSTRAP_DEV").as_deref() == Ok("1");
    if !dev_mode {
        // Owner MUST be root. Group identity is install-defined (canonically
        // `geodineum-bootstrap`, a narrow group containing only the legitimate
        // readers) — we don't hardcode the gid here because the daemon's
        // runtime user joins that group at install time; if the group is
        // wrong, fs::read_to_string below will fail with EACCES and produce
        // a clearer error than a hardcoded gid comparison.
        if meta.uid() != 0 {
            return Err(GeometricError::Other(format!(
                "bootstrap-loader: {} ownership drift (got uid={}, want 0; \
                 owner must be root, group must be geodineum-bootstrap)",
                path,
                meta.uid()
            )));
        }
        // Strict-deny mode policy (operator security stance, 2026-06-03):
        // bootstrap.env never world-readable, never group-writable.
        // Accept exactly 0640 (root:geodineum-bootstrap rw-r-----) or
        // 0600 (root-only rw-------). Reject everything else, including
        // the legacy 0644 that exposed deployment topology to "others".
        let mode = meta.mode() & 0o777;
        if mode != 0o640 && mode != 0o600 {
            return Err(GeometricError::Other(format!(
                "bootstrap-loader: {} mode drift (got {:o}, want 0640 or 0600 \
                 — strict-deny on world-readable / group-writable)",
                path, mode
            )));
        }
    }

    let content = fs::read_to_string(&path).map_err(|e| {
        GeometricError::Other(format!("bootstrap-loader: cannot read {}: {}", path, e))
    })?;

    // Reset whitelisted vars so a partial parse can't inherit stale values.
    for key in DISK_KEYS {
        std::env::remove_var(key);
    }

    let mut parsed: std::collections::HashMap<&'static str, String> =
        std::collections::HashMap::new();

    for (idx, raw) in content.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let (k, v) = line.split_once('=').ok_or_else(|| {
            GeometricError::Other(format!(
                "bootstrap-loader: {} line {} rejected (no '='): {}",
                path,
                idx + 1,
                raw
            ))
        })?;

        // Whitelist check.
        let canonical = DISK_KEYS.iter().find(|w| **w == k).ok_or_else(|| {
            GeometricError::Other(format!(
                "bootstrap-loader: {} line {} rejected (key '{}' not whitelisted)",
                path,
                idx + 1,
                k
            ))
        })?;

        // Value safety: no whitespace, no shell metacharacters.
        if v.is_empty()
            || v.chars()
                .any(|c| c.is_whitespace() || matches!(c, '"' | '\'' | '$' | '`' | '\\'))
        {
            return Err(GeometricError::Other(format!(
                "bootstrap-loader: {} line {} rejected (unsafe value for {}): {}",
                path,
                idx + 1,
                k,
                raw
            )));
        }

        parsed.insert(*canonical, v.to_string());
    }

    for key in DISK_KEYS {
        let value = parsed.get(*key).ok_or_else(|| {
            GeometricError::Other(format!(
                "bootstrap-loader: {} missing required key: {}",
                path, key
            ))
        })?;
        std::env::set_var(key, value);
    }

    // VALKEY_PORT must be a positive integer.
    let port_str = std::env::var("VALKEY_PORT").unwrap_or_default();
    if port_str.parse::<u16>().ok().filter(|p| *p > 0).is_none() {
        return Err(GeometricError::Other(format!(
            "bootstrap-loader: VALKEY_PORT not a valid u16 > 0: {}",
            port_str
        )));
    }

    Ok(())
}

/// Iterate `geodineum:bootstrap:_index`, GET each key, set env under bare name.
/// Empty index (first-boot) is silently accepted; any other failure raises.
pub fn load_valkey_tier() -> Result<()> {
    let host = std::env::var("VALKEY_HOST").map_err(|_| {
        GeometricError::Other(
            "bootstrap-loader: load_disk_tier must run before load_valkey_tier".into(),
        )
    })?;
    let port: u16 = std::env::var("VALKEY_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| {
            GeometricError::Other(
                "bootstrap-loader: VALKEY_PORT not set or invalid (load_disk_tier first)".into(),
            )
        })?;
    let creds = std::env::var("VALKEY_CREDS_PATH").map_err(|_| {
        GeometricError::Other(
            "bootstrap-loader: VALKEY_CREDS_PATH not set (load_disk_tier first)".into(),
        )
    })?;

    let (auth_user, auth_password) = resolve_credentials(&creds)?;

    // Build redis:// URL with userinfo. Use urlencoding to be safe with passwords
    // containing reserved chars.
    let auth_segment = match auth_user.as_deref() {
        Some(user) => format!(
            "{}:{}@",
            urlencoding::encode(user),
            urlencoding::encode(&auth_password)
        ),
        None => format!(":{}@", urlencoding::encode(&auth_password)),
    };
    let url = format!("redis://{}{}:{}/", auth_segment, host, port);

    let client = redis::Client::open(url.as_str()).map_err(|e| {
        GeometricError::Other(format!("bootstrap-loader: invalid redis URL: {}", e))
    })?;
    let mut conn = client
        .get_connection_with_timeout(Duration::from_secs(2))
        .map_err(|e| {
            GeometricError::Other(format!(
                "bootstrap-loader: ValKey unreachable at {}:{}: {}",
                host, port, e
            ))
        })?;

    // Probe + auth via PING (auth happened during URL parse + connect).
    let pong: String = redis::cmd("PING").query(&mut conn).map_err(|e| {
        GeometricError::Other(format!("bootstrap-loader: PING failed: {}", e))
    })?;
    if pong != "PONG" {
        return Err(GeometricError::Other(format!(
            "bootstrap-loader: PING returned unexpected: {}",
            pong
        )));
    }

    let members: Vec<String> = conn.smembers(VK_INDEX).map_err(|e| {
        GeometricError::Other(format!(
            "bootstrap-loader: SMEMBERS {} failed: {}",
            VK_INDEX, e
        ))
    })?;

    if members.is_empty() {
        // First-boot tolerance: index empty means installer hasn't populated yet.
        return Ok(());
    }

    let key_re = regex::Regex::new(r"^[A-Z_][A-Z0-9_]*$").unwrap();
    for key in members {
        if !key_re.is_match(&key) {
            return Err(GeometricError::Other(format!(
                "bootstrap-loader: indexed key name rejected: {}",
                key
            )));
        }
        let value: String = conn.get(format!("{}{}", VK_PREFIX, key)).map_err(|e| {
            GeometricError::Other(format!(
                "bootstrap-loader: GET {}{} failed: {}",
                VK_PREFIX, key, e
            ))
        })?;
        std::env::set_var(&key, value);
    }

    Ok(())
}

/// Resolve (user, password) for ValKey auth. Honors VALKEY_PASSWORD_FILE
/// override; otherwise tries `valkey_daemon.password` then `valkey.password`.
fn resolve_credentials(creds_dir: &str) -> Result<(Option<String>, String)> {
    let override_path = std::env::var("VALKEY_PASSWORD_FILE").ok();
    if let Some(p) = override_path.as_deref() {
        if !p.is_empty() {
            let pw = read_password(p)?;
            // Override path defaults to the daemon user; callers can change
            // by setting VALKEY_USER explicitly.
            let user = std::env::var("VALKEY_USER").ok();
            return Ok((user, pw));
        }
    }

    let daemon_path = format!("{}/valkey_daemon.password", creds_dir);
    if std::fs::metadata(&daemon_path).is_ok() {
        let pw = read_password(&daemon_path)?;
        return Ok((Some("gnode_daemon".to_string()), pw));
    }

    let admin_path = format!("{}/valkey.password", creds_dir);
    if std::fs::metadata(&admin_path).is_ok() {
        let pw = read_password(&admin_path)?;
        return Ok((None, pw));
    }

    Err(GeometricError::Other(format!(
        "bootstrap-loader: no readable ValKey password under {} \
         (set VALKEY_PASSWORD_FILE, or run as a user with creds access)",
        creds_dir
    )))
}

fn read_password(path: &str) -> Result<String> {
    let raw = std::fs::read_to_string(path).map_err(|e| {
        GeometricError::Other(format!("bootstrap-loader: cannot read {}: {}", path, e))
    })?;
    Ok(raw.trim_end_matches(&['\r', '\n'][..]).to_string())
}
