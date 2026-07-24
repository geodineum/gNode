//! Response receipts — durable, observer-consumable records of command outcomes.
//!
//! A receipt is the second of the three response-delivery channels (see
//! CONTRACTS/receipt-stream.md in the Geodineum repo):
//!   * immediate reply  → keyed rendezvous `{ss}:res:{id}`, ephemeral
//!   * durable receipt  → THIS module, `{ss}:gnode:receipts:{env}`, retained weeks
//!   * command delivery → the unified stream, unchanged
//!
//! A receipt is METADATA + a REFERENCE, never the body: status, timing,
//! command, correlation, lineage, plus `body_ref` (where the full result lives)
//! and `body_hash` (a content-anomaly signal without reading the body). It
//! generalises two receipts already live in production — COMMS delivery receipts
//! and gFlow run receipts — so the schema below is deliberately a superset of
//! both, with gFlow-specific fields optional.
//!
//! Principle 4 of the contract is "provenance replaces secrecy": a receipt in
//! the shared commons is trustworthy only because it is SIGNED. Each node holds
//! its own signing key (`load_or_generate_signer`; the private key never leaves
//! the node) and publishes only the pubkey to the topology registry, where
//! verifiers resolve a receipt's `signer` fingerprint. Live emission goes
//! through [`ReceiptContext`] — set once at daemon startup — and the
//! `signed_response_receipt` builder, which refuses to produce an unsigned
//! receipt: no context or a signing failure means no receipt, never an
//! unverifiable one on a shared stream.

use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use redis::Connection;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::io;
use std::path::Path;

/// How long receipts are retained. gFlow workflows carry deadlines "measured in
/// weeks"; COMMS keeps its delivery status 30 days. 30 days covers both and is
/// the age floor for the receipt stream's trim.
pub const RECEIPT_RETENTION_MS: u64 = 30 * 24 * 60 * 60 * 1000;

/// Current receipt schema version. Bump on any wire-field change.
pub const RECEIPT_SCHEMA_VERSION: u32 = 1;

/// A response receipt. Core fields are always present; gFlow/inference-specific
/// fields are optional and populated only by producers that have them.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Receipt {
    /// The request this is a receipt for — the universal join across reply,
    /// receipt, and body. (gFlow's `correlation_id`, COMMS's stream entry id.)
    pub correlation_id: String,
    /// The command / action this receipt is for.
    pub command: String,
    /// `ok` | `failed` | `refused`.
    pub status: String,
    /// Failure reason when `status != ok`.
    pub error: Option<String>,
    /// Producing site and node.
    pub site: String,
    pub node: String,
    /// Completion timestamp, ms since epoch.
    pub ts_ms: u64,
    /// Where the full result lives (e.g. the `{ss}:res:{id}` reply key). The
    /// receipt never inlines the body.
    pub body_ref: String,
    /// Hex sha256 of the result body — a content-anomaly signal (deterministic
    /// inference ⇒ a hash mismatch is a real anomaly) that needs no body read.
    pub body_hash: String,

    // ── lineage (optional — for flows/chains, e.g. gFlow) ──
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flow_id: Option<String>,

    // ── operational extras (optional) ──
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wait_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub work_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,

    // ── signature envelope ──
    /// Schema version.
    pub v: u32,
    /// Signature ALGORITHM id (e.g. `ed25519`). Named on the wire and folded
    /// into the signed bytes so the scheme is swappable and un-downgradeable —
    /// a future post-quantum signature (ML-DSA / Dilithium) is another `alg`,
    /// not a schema break. Empty ⇒ unsigned.
    pub alg: String,
    /// Signature over `canonical_bytes()`, hex. Empty ⇒ unsigned.
    pub sig: String,
    /// Signer id — a fingerprint of the producing node's public key. Empty ⇒
    /// unsigned. Verifiers resolve this to the node's published pubkey.
    pub signer: String,
}

impl Receipt {
    /// Minimal receipt for a command response. Optional fields default to None;
    /// producers with lineage/timing/model fill them in afterward.
    pub fn for_response(
        correlation_id: impl Into<String>,
        command: impl Into<String>,
        status: impl Into<String>,
        error: Option<String>,
        site: impl Into<String>,
        node: impl Into<String>,
        ts_ms: u64,
        body_ref: impl Into<String>,
        body: &str,
    ) -> Self {
        Receipt {
            correlation_id: correlation_id.into(),
            command: command.into(),
            status: status.into(),
            error,
            site: site.into(),
            node: node.into(),
            ts_ms,
            body_ref: body_ref.into(),
            body_hash: body_hash(body),
            parent_id: None,
            flow_id: None,
            wait_ms: None,
            work_ms: None,
            model: None,
            v: RECEIPT_SCHEMA_VERSION,
            alg: String::new(),
            sig: String::new(),
            signer: String::new(),
        }
    }

    /// Sign this receipt with a node's signer. Sets `alg`, `sig`, `signer`.
    /// `alg` is part of `canonical_bytes()`, so the scheme cannot be silently
    /// downgraded by an attacker without invalidating the signature.
    pub fn sign(&mut self, signer: &NodeSigner) -> Result<(), String> {
        // alg must be set BEFORE canonical_bytes so it is covered by the sig.
        self.alg = signer.alg_id().to_string();
        let sig = signer.sign(&self.canonical_bytes())?;
        self.sig = hex(&sig);
        self.signer = signer.signer_id();
        Ok(())
    }

    /// Verify this receipt's signature against a producer's public key bytes.
    /// Resolves the receipt's declared `alg` to a scheme; an unknown or empty
    /// alg fails closed.
    pub fn verify(&self, pubkey: &[u8]) -> bool {
        if self.alg.is_empty() || self.sig.is_empty() {
            return false;
        }
        let scheme = match scheme_for(&self.alg) {
            Some(s) => s,
            None => return false,
        };
        let sig_bytes = match unhex(&self.sig) {
            Some(b) => b,
            None => return false,
        };
        scheme.verify(pubkey, &self.canonical_bytes(), &sig_bytes)
    }

    /// The exact bytes a signature covers: every field EXCEPT `sig`/`signer`,
    /// in a fixed order, so signing and verification agree byte-for-byte. This
    /// is defined now so signing is purely additive later.
    pub fn canonical_bytes(&self) -> Vec<u8> {
        // Deliberately explicit + ordered, not serde (whose field order/format
        // could drift). Anything a verifier must trust goes here.
        let mut s = String::new();
        s.push_str(&format!("v={}\n", self.v));
        s.push_str(&format!("alg={}\n", self.alg));
        s.push_str(&format!("cid={}\n", self.correlation_id));
        s.push_str(&format!("cmd={}\n", self.command));
        s.push_str(&format!("st={}\n", self.status));
        s.push_str(&format!("e={}\n", self.error.as_deref().unwrap_or("")));
        s.push_str(&format!("ss={}\n", self.site));
        s.push_str(&format!("sn={}\n", self.node));
        s.push_str(&format!("ts={}\n", self.ts_ms));
        s.push_str(&format!("bref={}\n", self.body_ref));
        s.push_str(&format!("bh={}\n", self.body_hash));
        s.push_str(&format!("pid={}\n", self.parent_id.as_deref().unwrap_or("")));
        s.push_str(&format!("fid={}\n", self.flow_id.as_deref().unwrap_or("")));
        s.into_bytes()
    }

    /// Wire fields for XADD. Short names, consistent with the command schema.
    pub fn to_fields(&self) -> Vec<(String, String)> {
        let mut f = vec![
            ("v".to_string(), self.v.to_string()),
            ("cid".to_string(), self.correlation_id.clone()),
            ("cmd".to_string(), self.command.clone()),
            ("st".to_string(), self.status.clone()),
            ("ss".to_string(), self.site.clone()),
            ("sn".to_string(), self.node.clone()),
            ("ts".to_string(), self.ts_ms.to_string()),
            ("bref".to_string(), self.body_ref.clone()),
            ("bh".to_string(), self.body_hash.clone()),
        ];
        if let Some(e) = &self.error { f.push(("e".to_string(), e.clone())); }
        if let Some(p) = &self.parent_id { f.push(("pid".to_string(), p.clone())); }
        if let Some(fl) = &self.flow_id { f.push(("fid".to_string(), fl.clone())); }
        if let Some(w) = self.wait_ms { f.push(("wait_ms".to_string(), w.to_string())); }
        if let Some(w) = self.work_ms { f.push(("work_ms".to_string(), w.to_string())); }
        if let Some(m) = &self.model { f.push(("model".to_string(), m.clone())); }
        if !self.alg.is_empty() { f.push(("alg".to_string(), self.alg.clone())); }
        if !self.sig.is_empty() { f.push(("sig".to_string(), self.sig.clone())); }
        if !self.signer.is_empty() { f.push(("signer".to_string(), self.signer.clone())); }
        f
    }
}

// ─────────────────────────── signature adapter ─────────────────────────────
// Interface/adapter over crypto schemes, byte-oriented so the receipt, wire
// format, and key files stay algorithm-agnostic. Ed25519 today; a post-quantum
// signature (ML-DSA / Dilithium) is a new impl + a resolver arm, no schema
// change. NOTE: Kyber is a KEM, not a signature scheme — the successor here is a
// PQ *signature*.

/// A signature scheme, defined over raw bytes (keys and signatures differ
/// wildly in size between schemes, so nothing typed leaks past this boundary).
pub trait SignatureScheme: Send + Sync {
    /// Wire id, e.g. `ed25519`. Folded into the signed bytes.
    fn alg_id(&self) -> &'static str;
    /// Generate a keypair: (private_bytes, public_bytes).
    fn generate(&self) -> (Vec<u8>, Vec<u8>);
    /// Public key bytes derived from a private key.
    fn public_from_private(&self, private_bytes: &[u8]) -> Result<Vec<u8>, String>;
    /// Sign a message with a private key.
    fn sign(&self, private_bytes: &[u8], msg: &[u8]) -> Result<Vec<u8>, String>;
    /// Verify a signature against a public key. Fails closed on any malformed input.
    fn verify(&self, public_bytes: &[u8], msg: &[u8], sig: &[u8]) -> bool;
}

/// Resolve an algorithm id to its scheme. The single place algorithms are
/// registered — add a variant here to add a scheme.
pub fn scheme_for(alg_id: &str) -> Option<Box<dyn SignatureScheme>> {
    match alg_id {
        "ed25519" => Some(Box::new(Ed25519Scheme)),
        _ => None,
    }
}

/// The default scheme for new signers until an operator selects otherwise.
pub fn default_scheme() -> Box<dyn SignatureScheme> {
    Box::new(Ed25519Scheme)
}

/// Ed25519 adapter.
pub struct Ed25519Scheme;

impl SignatureScheme for Ed25519Scheme {
    fn alg_id(&self) -> &'static str { "ed25519" }

    fn generate(&self) -> (Vec<u8>, Vec<u8>) {
        use rand::RngCore;
        let mut seed = [0u8; 32];
        rand::rngs::OsRng.fill_bytes(&mut seed);
        let sk = SigningKey::from_bytes(&seed);
        let pk = sk.verifying_key();
        (seed.to_vec(), pk.to_bytes().to_vec())
    }

    fn public_from_private(&self, private_bytes: &[u8]) -> Result<Vec<u8>, String> {
        let seed: [u8; 32] = private_bytes.try_into()
            .map_err(|_| "ed25519 private key must be 32 bytes".to_string())?;
        Ok(SigningKey::from_bytes(&seed).verifying_key().to_bytes().to_vec())
    }

    fn sign(&self, private_bytes: &[u8], msg: &[u8]) -> Result<Vec<u8>, String> {
        let seed: [u8; 32] = private_bytes.try_into()
            .map_err(|_| "ed25519 private key must be 32 bytes".to_string())?;
        Ok(SigningKey::from_bytes(&seed).sign(msg).to_bytes().to_vec())
    }

    fn verify(&self, public_bytes: &[u8], msg: &[u8], sig: &[u8]) -> bool {
        let pk: [u8; 32] = match public_bytes.try_into() { Ok(b) => b, Err(_) => return false };
        let vk = match VerifyingKey::from_bytes(&pk) { Ok(v) => v, Err(_) => return false };
        let sig: [u8; 64] = match sig.try_into() { Ok(b) => b, Err(_) => return false };
        vk.verify(msg, &Signature::from_bytes(&sig)).is_ok()
    }
}

/// A node's signing identity: an algorithm + its private key material. The
/// private key never leaves the node — the master/topology only ever holds the
/// published public key.
pub struct NodeSigner {
    scheme: Box<dyn SignatureScheme>,
    private_bytes: Vec<u8>,
    public_bytes: Vec<u8>,
}

impl NodeSigner {
    pub fn alg_id(&self) -> &'static str { self.scheme.alg_id() }
    pub fn public_bytes(&self) -> &[u8] { &self.public_bytes }
    pub fn sign(&self, msg: &[u8]) -> Result<Vec<u8>, String> {
        self.scheme.sign(&self.private_bytes, msg)
    }
    /// Short fingerprint of the public key (first 8 bytes of its sha256, hex) —
    /// the `signer` value verifiers resolve to the published pubkey.
    pub fn signer_id(&self) -> String {
        let mut h = Sha256::new();
        h.update(&self.public_bytes);
        hex(&h.finalize()[..8])
    }
}

/// Load this node's signer from disk, generating and persisting a keypair on
/// first use. The private key is written `0600`; only the node holds it.
///
/// File format: one line `<alg>:<private_hex>`. Algorithm-tagged so a key file
/// declares its own scheme — a future re-key to a PQ scheme rewrites this file
/// with a new alg, and old receipts still verify against their own `alg`.
pub fn load_or_generate_signer(path: &Path) -> io::Result<NodeSigner> {
    if let Ok(contents) = std::fs::read_to_string(path) {
        let line = contents.trim();
        if let Some((alg, hexkey)) = line.split_once(':') {
            if let (Some(scheme), Some(priv_bytes)) = (scheme_for(alg), unhex(hexkey)) {
                let public_bytes = scheme.public_from_private(&priv_bytes)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                return Ok(NodeSigner { scheme, private_bytes: priv_bytes, public_bytes });
            }
        }
        return Err(io::Error::new(io::ErrorKind::InvalidData,
            format!("unrecognised or corrupt signing key at {}", path.display())));
    }

    // Generate on first use.
    let scheme = default_scheme();
    let (private_bytes, public_bytes) = scheme.generate();
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    std::fs::write(path, format!("{}:{}\n", scheme.alg_id(), hex(&private_bytes)))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(NodeSigner { scheme, private_bytes, public_bytes })
}

/// Default path for this node's receipt signing key. The daemon's own
/// component dir is gnode-owned (writable by the daemon), unlike the credential
/// dir (root:geodineum-creds, not gnode-writable). Overridable via
/// `GNODE_RECEIPT_KEY_FILE`.
pub fn default_signer_path() -> std::path::PathBuf {
    if let Ok(p) = std::env::var("GNODE_RECEIPT_KEY_FILE") {
        return std::path::PathBuf::from(p);
    }
    std::path::PathBuf::from("/etc/geodineum/components/gnode-daemon/receipt_signing.key")
}

/// The shared registry of node receipt pubkeys. Placed in the topology
/// namespace's gnode bus — a PUBLIC key, safe in the commons, resolvable by any
/// verifier (GeoV, gFlow, dashboard). Field = signer_id (the fingerprint a
/// receipt carries); value = `<alg>:<pubkey_hex>`.
pub fn pubkey_registry_key(topology_ns: &str) -> String {
    format!("{{{}}}:gnode:receipt_pubkeys", topology_ns)
}

/// Publish this node's receipt public key so verifiers can resolve its
/// `signer_id`. Additive and idempotent — republishing the same key is a no-op.
pub fn publish_pubkey(
    conn: &mut Connection,
    signer: &NodeSigner,
    topology_ns: &str,
) -> redis::RedisResult<()> {
    let registry = pubkey_registry_key(topology_ns);
    let value = format!("{}:{}", signer.alg_id(), hex(signer.public_bytes()));
    let _: i64 = redis::cmd("HSET")
        .arg(&registry)
        .arg(signer.signer_id())
        .arg(&value)
        .query(conn)?;
    Ok(())
}

/// Verifier helper: resolve a receipt's `signer` id to `(alg, pubkey_bytes)`.
/// A receipt then verifies via `receipt.verify(&pubkey)` after checking its
/// `alg` matches. Returns None if the signer is unknown or the entry malformed.
pub fn resolve_pubkey(
    conn: &mut Connection,
    topology_ns: &str,
    signer_id: &str,
) -> Option<(String, Vec<u8>)> {
    let registry = pubkey_registry_key(topology_ns);
    let v: Option<String> = redis::cmd("HGET")
        .arg(&registry)
        .arg(signer_id)
        .query(conn)
        .ok()?;
    let v = v?;
    let (alg, hexkey) = v.split_once(':')?;
    Some((alg.to_string(), unhex(hexkey)?))
}

/// Lowercase hex encode.
pub fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes { s.push_str(&format!("{:02x}", b)); }
    s
}

/// Decode lowercase/uppercase hex; None on any non-hex or odd length.
pub fn unhex(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 { return None; }
    (0..s.len()).step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}

/// The receipt stream key for a site + environment. Hash-tagged, and placed
/// under `{site}:gnode:*` so the existing per-site dashboard ACL grants read
/// access (the property COMMS relies on today for its status hash).
pub fn receipt_stream_key(site: &str, environment: &str) -> String {
    format!("{{{}}}:gnode:receipts:{}", site, environment)
}

/// Hex sha256 of a body — the content-anomaly signal.
pub fn body_hash(body: &str) -> String {
    let mut h = Sha256::new();
    h.update(body.as_bytes());
    let digest = h.finalize();
    let mut out = String::with_capacity(64);
    for b in digest.iter() {
        out.push_str(&format!("{:02x}", b));
    }
    out
}

/// Emit a receipt to the site's receipt stream, then trim by age (MINID).
///
/// Additive and non-destructive: this does not touch the reply key or the
/// command stream. Trim uses MINID (by age), never MAXLEN (by count) — the
/// resource is bytes over weeks, not a message count.
pub fn emit_receipt(
    conn: &mut Connection,
    receipt: &Receipt,
    environment: &str,
    now_ms: u64,
) -> redis::RedisResult<String> {
    let key = receipt_stream_key(&receipt.site, environment);
    let fields = receipt.to_fields();

    let id: String = {
        let mut cmd = redis::cmd("XADD");
        cmd.arg(&key).arg("*");
        for (k, v) in &fields {
            cmd.arg(k).arg(v);
        }
        cmd.query(conn)?
    };

    // Age-based trim: drop receipts older than the retention floor.
    let cutoff_ms = now_ms.saturating_sub(RECEIPT_RETENTION_MS);
    let cutoff_id = format!("{}-0", cutoff_ms);
    let _: redis::RedisResult<i64> = redis::cmd("XTRIM")
        .arg(&key)
        .arg("MINID")
        .arg("~")
        .arg(&cutoff_id)
        .query(conn);

    Ok(id)
}

/// Async twin of [`emit_receipt`] for the Fast lane's multiplexed connection.
pub async fn emit_receipt_async(
    conn: &mut redis::aio::MultiplexedConnection,
    receipt: &Receipt,
    environment: &str,
    now_ms: u64,
) -> redis::RedisResult<String> {
    let key = receipt_stream_key(&receipt.site, environment);
    let fields = receipt.to_fields();

    let id: String = {
        let mut cmd = redis::cmd("XADD");
        cmd.arg(&key).arg("*");
        for (k, v) in &fields {
            cmd.arg(k).arg(v);
        }
        cmd.query_async(conn).await?
    };

    let cutoff_ms = now_ms.saturating_sub(RECEIPT_RETENTION_MS);
    let cutoff_id = format!("{}-0", cutoff_ms);
    let _: redis::RedisResult<i64> = redis::cmd("XTRIM")
        .arg(&key)
        .arg("MINID")
        .arg("~")
        .arg(&cutoff_id)
        .query_async(conn)
        .await;

    Ok(id)
}

// ───────────────────────────── emission context ─────────────────────────────

/// The daemon-wide emission identity: this node's signer plus the identity and
/// default environment receipts carry. Set once at startup, read by every
/// response path (the Fast lane's spawned tasks included, hence the static).
pub struct ReceiptContext {
    pub signer: NodeSigner,
    pub node_id: String,
    /// Fallback environment when one cannot be parsed from a stream key.
    pub environment: String,
}

static RECEIPT_CONTEXT: std::sync::OnceLock<ReceiptContext> = std::sync::OnceLock::new();

/// Install the emission context. Idempotent; the first caller wins.
pub fn init_receipt_context(signer: NodeSigner, node_id: String, environment: String) {
    let _ = RECEIPT_CONTEXT.set(ReceiptContext { signer, node_id, environment });
}

/// The emission context, if the daemon initialized one. None ⇒ no receipts
/// (single-shot tools, tests, or a node whose signer failed to load).
pub fn receipt_context() -> Option<&'static ReceiptContext> {
    RECEIPT_CONTEXT.get()
}

/// The environment segment of a unified command stream key
/// (`{site}:gnode:unified:{env}`); None for any other shape.
pub fn env_from_stream_key(stream_key: &str) -> Option<&str> {
    stream_key
        .rsplit_once(":unified:")
        .map(|(_, env)| env)
        .filter(|e| !e.is_empty())
}

/// Build and SIGN a response receipt from the emission context. Returns None —
/// emit nothing — when no context is installed or signing fails: an unsigned
/// receipt never enters the shared stream. `response_status` is the reply's
/// wire status (`ok` passes through; anything else records as `failed`).
#[allow(clippy::too_many_arguments)]
pub fn signed_response_receipt(
    correlation_id: &str,
    command: &str,
    response_status: &str,
    error: Option<String>,
    site: &str,
    body_ref: &str,
    body_json: &str,
    now_ms: u64,
) -> Option<Receipt> {
    let ctx = receipt_context()?;
    let status = if response_status == "ok" { "ok" } else { "failed" };
    let mut receipt = Receipt::for_response(
        correlation_id,
        command,
        status,
        error,
        site,
        ctx.node_id.as_str(),
        now_ms,
        body_ref,
        body_json,
    );
    if let Err(e) = receipt.sign(&ctx.signer) {
        log::warn!("receipt signing failed for {} — receipt suppressed: {}", correlation_id, e);
        return None;
    }
    Some(receipt)
}

/// One-time emission proof: the first receipt a daemon process emits is logged
/// at info so live wiring is verifiable from the journal alone (per-request
/// logging stays off).
pub fn log_first_emission(stream_key: &str, entry_id: &str) {
    static FIRST: std::sync::Once = std::sync::Once::new();
    FIRST.call_once(|| {
        log::info!("First receipt emitted to {} (id {})", stream_key, entry_id);
    });
}

/// Milliseconds since the epoch.
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_env_from_stream_key() {
        assert_eq!(
            env_from_stream_key("{nierto_com}:gnode:unified:production"),
            Some("production")
        );
        assert_eq!(env_from_stream_key("{nierto_com}:gnode:unified:"), None);
        assert_eq!(env_from_stream_key("{geodineum}:gnode:broadcast"), None);
    }

    #[test]
    fn test_stream_key_hash_tagged_under_gnode() {
        assert_eq!(
            receipt_stream_key("nierto_com", "production"),
            "{nierto_com}:gnode:receipts:production"
        );
    }

    #[test]
    fn test_body_hash_is_deterministic_sha256() {
        // sha256("") known vector
        assert_eq!(
            body_hash(""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(body_hash("abc"), body_hash("abc"));
        assert_ne!(body_hash("abc"), body_hash("abd"));
    }

    #[test]
    fn test_for_response_populates_core_and_hashes_body() {
        let r = Receipt::for_response(
            "req-1", "get_node_info", "ok", None,
            "nierto_com", "master", 1_700_000_000_000,
            "{nierto_com}:res:req-1", "{\"result\":42}",
        );
        assert_eq!(r.correlation_id, "req-1");
        assert_eq!(r.status, "ok");
        assert_eq!(r.v, RECEIPT_SCHEMA_VERSION);
        assert!(r.sig.is_empty(), "unsigned in v1");
        assert_eq!(r.body_hash, body_hash("{\"result\":42}"));
    }

    #[test]
    fn test_to_fields_omits_empty_optionals() {
        let r = Receipt::for_response(
            "req-1", "cmd", "ok", None, "s", "n", 1, "ref", "body",
        );
        let fields = r.to_fields();
        let keys: Vec<&str> = fields.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>();
        assert!(keys.contains(&"cid"));
        assert!(keys.contains(&"st"));
        assert!(!keys.contains(&"e"), "no error field when error is None");
        assert!(!keys.contains(&"sig"), "no sig field when unsigned");
    }

    #[test]
    fn test_canonical_bytes_stable_and_excludes_sig() {
        let mut r = Receipt::for_response(
            "req-1", "cmd", "ok", None, "s", "n", 1, "ref", "body",
        );
        let before = r.canonical_bytes();
        r.sig = "deadbeef".to_string();
        r.signer = "key1".to_string();
        // Setting sig/signer must not change what was signed (they are excluded).
        assert_eq!(before, r.canonical_bytes());
    }

    #[test]
    fn test_sign_then_verify_roundtrip() {
        let scheme = Ed25519Scheme;
        let (priv_b, pub_b) = scheme.generate();
        let signer = NodeSigner {
            scheme: Box::new(Ed25519Scheme),
            private_bytes: priv_b,
            public_bytes: pub_b.clone(),
        };
        let mut r = Receipt::for_response(
            "req-1", "get_node_info", "ok", None, "nierto_com", "master",
            1_700_000_000_000, "{nierto_com}:res:req-1", "{\"r\":1}",
        );
        r.sign(&signer).unwrap();
        assert_eq!(r.alg, "ed25519");
        assert!(!r.sig.is_empty() && !r.signer.is_empty());
        assert!(r.verify(&pub_b), "valid signature must verify");
    }

    #[test]
    fn test_verify_fails_on_tamper_and_wrong_key() {
        let scheme = Ed25519Scheme;
        let (priv_b, pub_b) = scheme.generate();
        let (_, other_pub) = scheme.generate();
        let signer = NodeSigner {
            scheme: Box::new(Ed25519Scheme), private_bytes: priv_b, public_bytes: pub_b.clone(),
        };
        let mut r = Receipt::for_response(
            "req-1", "cmd", "ok", None, "s", "n", 1, "ref", "body",
        );
        r.sign(&signer).unwrap();
        assert!(r.verify(&pub_b));
        // wrong key
        assert!(!r.verify(&other_pub));
        // tamper the covered content
        let mut tampered = r.clone();
        tampered.status = "failed".to_string();
        assert!(!tampered.verify(&pub_b), "tampering must break verification");
        // downgrade the alg
        let mut downgraded = r.clone();
        downgraded.alg = "bogus".to_string();
        assert!(!downgraded.verify(&pub_b), "unknown alg fails closed");
    }

    #[test]
    fn test_unsigned_receipt_does_not_verify() {
        let r = Receipt::for_response("req-1", "cmd", "ok", None, "s", "n", 1, "ref", "body");
        assert!(r.alg.is_empty() && r.sig.is_empty());
        assert!(!r.verify(&[0u8; 32]), "an unsigned receipt never verifies");
    }

    #[test]
    fn test_pubkey_registry_key_shape() {
        assert_eq!(
            pubkey_registry_key("geodineum"),
            "{geodineum}:gnode:receipt_pubkeys"
        );
    }

    #[test]
    fn test_signer_id_is_pubkey_fingerprint() {
        let (priv_b, pub_b) = Ed25519Scheme.generate();
        let s = NodeSigner {
            scheme: Box::new(Ed25519Scheme), private_bytes: priv_b, public_bytes: pub_b.clone(),
        };
        // 8 bytes → 16 hex chars, and stable for the same key.
        assert_eq!(s.signer_id().len(), 16);
        let s2 = NodeSigner {
            scheme: Box::new(Ed25519Scheme),
            private_bytes: s.private_bytes.clone(),
            public_bytes: pub_b,
        };
        assert_eq!(s.signer_id(), s2.signer_id());
    }

    #[test]
    fn test_load_or_generate_roundtrips_from_disk() {
        // generate → reload → same key → a receipt signed by one verifies with
        // the other's pubkey.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("receipt_signing.key");
        let s1 = load_or_generate_signer(&path).unwrap();
        let s2 = load_or_generate_signer(&path).unwrap(); // loads, does not regenerate
        assert_eq!(s1.signer_id(), s2.signer_id());
        assert_eq!(s1.public_bytes(), s2.public_bytes());
        let mut r = Receipt::for_response("req", "cmd", "ok", None, "s", "n", 1, "ref", "b");
        r.sign(&s1).unwrap();
        assert!(r.verify(s2.public_bytes()));
    }

    #[test]
    fn test_hex_roundtrip() {
        assert_eq!(hex(&[0xde, 0xad, 0xbe, 0xef]), "deadbeef");
        assert_eq!(unhex("deadbeef"), Some(vec![0xde, 0xad, 0xbe, 0xef]));
        assert_eq!(unhex("xyz"), None);
        assert_eq!(unhex("abc"), None); // odd length
    }

    #[test]
    fn test_error_appears_in_fields_and_canonical() {
        let r = Receipt::for_response(
            "req-1", "cmd", "failed", Some("boom".to_string()),
            "s", "n", 1, "ref", "body",
        );
        let f = r.to_fields();
        assert!(f.iter().any(|(k, v)| k == "e" && v == "boom"));
        let canon = String::from_utf8(r.canonical_bytes()).unwrap();
        assert!(canon.contains("e=boom\n"));
    }
}
