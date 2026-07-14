// Asset Builder — Manifest-Driven Background Bundle Builder
//
// CMS-agnostic, manifest-driven bundle assembly. Reads manifest definitions from
// ValKey and assembles bundles according to their specifications.
//
// Architecture:
// 1. Timer-based rebuild (every 5 minutes, configurable)
// 2. Event-based rebuild (on PubSub invalidation notifications)
// 3. Manifest-driven assembly (any layout: cube, tesseract, grid, custom)
// 4. Gzip compression (level 9) for bandwidth optimization
// 5. Sites without manifests fall back to face_mapping-based builds
//
// Key patterns:
//   {site_id}:asset:manifests                — SET: registered manifest IDs
//   {site_id}:asset:manifest:{id}            — HASH: manifest definition
//   {site_id}:gnode:bundle:{manifest_id}       — STRING: gzip-compressed bundle
//   {site_id}:gnode:bundle:{manifest_id}:meta  — HASH: build metadata
//   {site_id}:gnode:face_mapping               — STRING: face mapping data
//   {site_id}:gnode:bundle:full                — STRING: full bundle output

use std::io::Write;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use redis::Client;
use serde::{Serialize, Deserialize};
use serde_json::{Value, json};
use log::{info, error, debug, warn};
use tokio::task::JoinHandle;
use flate2::write::GzEncoder;
use flate2::Compression;
use futures::StreamExt;

use crate::GeometricTopology;
use std::sync::RwLock;

/// A content slot in a manifest bundle (face, cell, page, etc.)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlotContent {
    pub id: String,
    #[serde(default)]
    pub position: String,
    #[serde(default)]
    pub html: String,
    pub css: Option<String>,
    pub js: Option<String>,
    #[serde(default = "default_content_type")]
    pub content_type: String,
}

fn default_content_type() -> String {
    "text/html".to_string()
}

/// A manifest-driven bundle (replaces the fixed Bundle struct)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestBundle {
    pub manifest_id: String,
    pub manifest_version: String,
    pub layout: String,
    pub slots: Vec<SlotContent>,
    pub sections: HashMap<String, Value>,
    pub built_at: f64,
    pub builder_version: String,
}

/// Face-based bundle structure for sites using face_mapping
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FaceBundle {
    pub faces: Vec<FaceData>,
    pub posts: Value,
    pub navigation: Value,
    pub metadata: Value,
    pub built_at: f64,
    pub version: String,
}

/// Individual face data (id, html, optional css/js)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FaceData {
    pub id: u8,
    pub html: String,
    pub css: Option<String>,
    pub js: Option<String>,
}

/// Asset builder context
pub struct AssetBuilder {
    redis_client: Client,
    #[allow(dead_code)]
    topology: Arc<RwLock<GeometricTopology>>,
    rebuild_interval_secs: u64,
    #[allow(dead_code)]
    debug: bool,
}

impl AssetBuilder {
    pub fn new(
        redis_client: Client,
        topology: Arc<RwLock<GeometricTopology>>,
        rebuild_interval_secs: u64,
        debug: bool,
    ) -> Self {
        Self {
            redis_client,
            topology,
            rebuild_interval_secs,
            debug,
        }
    }

    /// Start asset builder (spawns async task)
    pub async fn start_builder(self) -> Result<JoinHandle<()>, Box<dyn std::error::Error + Send + Sync>> {
        let builder = Arc::new(self);

        info!("Starting asset builder (interval: {}s)", builder.rebuild_interval_secs);

        // Initial build
        if let Err(e) = builder.rebuild_all_sites().await {
            error!("Initial asset build failed: {}", e);
        }

        let task = tokio::spawn(async move {
            if let Err(e) = builder.run_builder_loop().await {
                error!("Asset builder failed: {}", e);
            }
        });

        Ok(task)
    }

    /// Main builder loop — dual-trigger: timer + PubSub invalidation
    async fn run_builder_loop(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut interval = tokio::time::interval(Duration::from_secs(self.rebuild_interval_secs));

        // Subscribe to invalidation events across all sites
        // Uses a wildcard-friendly pattern: we subscribe to all known site channels
        let client = self.redis_client.clone();
        #[allow(deprecated)] // PubSub requires non-multiplexed connection (redis crate API)
        let conn = client.get_async_connection().await?;
        let mut pubsub = conn.into_pubsub();

        // Subscribe to a general invalidation pattern
        // Individual site channels are subscribed as sites are discovered
        let sites = self.get_registered_sites().await.unwrap_or_default();
        for site_id in &sites {
            let channel = format!("{}:events:invalidate", site_id);
            if let Err(e) = pubsub.subscribe(&channel).await {
                warn!("Failed to subscribe to {}: {}", channel, e);
            }
        }

        if sites.is_empty() {
            debug!("No sites discovered yet, will check on timer tick");
        } else {
            info!("Asset builder subscribed to {} site invalidation channel(s)", sites.len());
        }

        let mut message_stream = pubsub.on_message();

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    debug!("Scheduled asset rebuild");
                    if let Err(e) = self.rebuild_all_sites().await {
                        error!("Scheduled asset rebuild failed: {}", e);
                    }
                }
                msg_result = message_stream.next() => {
                    if let Some(msg) = msg_result {
                        match msg.get_payload::<String>() {
                            Ok(payload_str) => match serde_json::from_str::<Value>(&payload_str) {
                                Ok(payload) => {
                                    if let Some(event) = payload["event"].as_str() {
                                        if event == "bundle_invalidated"
                                            || event == "bundle_rebuild_requested"
                                            || event == "manifest_updated"
                                        {
                                            let site = payload["site_id"].as_str().unwrap_or("unknown");
                                            info!("Invalidation event '{}' for site {}", event, site);
                                            if let Err(e) = self.rebuild_all_sites().await {
                                                error!("Event-triggered rebuild failed: {}", e);
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    warn!("Failed to parse invalidation event: {}", e);
                                }
                            },
                            Err(e) => {
                                warn!("Failed to get payload: {:?}", e);
                            }
                        }
                    } else {
                        warn!("Invalidation channel closed, reconnecting...");
                        tokio::time::sleep(Duration::from_secs(5)).await;
                        return Err("Invalidation channel closed".into());
                    }
                }
            }
        }
    }

    /// Rebuild bundles for all registered sites
    async fn rebuild_all_sites(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let start = Instant::now();

        let registered_sites = self.get_registered_sites().await?;

        if registered_sites.is_empty() {
            debug!("No registered sites, skipping asset build");
            return Ok(());
        }

        debug!("Building assets for {} site(s)", registered_sites.len());

        let mut built = 0;
        let mut failed = 0;

        for site_id in &registered_sites {
            match self.rebuild_site(site_id).await {
                Ok(_) => built += 1,
                Err(e) => {
                    error!("Failed to build assets for site {}: {}", site_id, e);
                    failed += 1;
                }
            }
        }

        let elapsed = start.elapsed().as_millis();

        if failed > 0 {
            warn!("Built {} site bundle(s) ({} failed) in {}ms", built, failed, elapsed);
        } else {
            debug!("Built {} site bundle(s) in {}ms", built, elapsed);
        }

        Ok(())
    }

    /// Rebuild bundles for a single site
    ///
    /// Strategy:
    /// 1. Check for registered manifests in {site_id}:asset:manifests SET
    /// 2. If manifests found: build each one from its manifest definition
    /// 3. If no manifests: fall back to face_mapping-based build
    async fn rebuild_site(&self, site_id: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut conn = self.redis_client.get_multiplexed_async_connection().await?;

        // Check for registered manifests
        let manifests_key = format!("{{{}}}:asset:manifests", site_id);
        let manifest_ids: Vec<String> = redis::cmd("SMEMBERS")
            .arg(&manifests_key)
            .query_async(&mut conn)
            .await
            .unwrap_or_default();

        if !manifest_ids.is_empty() {
            debug!("Site {} has {} manifest(s), building from manifests", site_id, manifest_ids.len());
            for manifest_id in &manifest_ids {
                if let Err(e) = self.build_from_manifest(site_id, manifest_id).await {
                    error!("Failed to build manifest '{}' for site {}: {}", manifest_id, site_id, e);
                }
            }

            // Also build the compat bundle if "main" manifest exists
            // This ensures {site_id}:gnode:bundle:full stays populated
            if manifest_ids.contains(&"main".to_string()) {
                if let Err(e) = self.copy_manifest_to_compat(site_id, "main").await {
                    warn!("Failed to copy main manifest to compat bundle: {}", e);
                }
            }
        } else {
            // No manifests registered — use face_mapping-based build
            self.build_compat_bundle(site_id).await?;
        }

        Ok(())
    }

    /// Build a bundle from a manifest definition
    async fn build_from_manifest(&self, site_id: &str, manifest_id: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let start = Instant::now();
        let mut conn = self.redis_client.get_multiplexed_async_connection().await?;

        // Read manifest HASH
        let manifest_key = format!("{{{}}}:asset:manifest:{}", site_id, manifest_id);
        let fields_raw: Vec<String> = redis::cmd("HGETALL")
            .arg(&manifest_key)
            .query_async(&mut conn)
            .await?;

        if fields_raw.is_empty() {
            return Err(format!("Manifest '{}' not found for site {}", manifest_id, site_id).into());
        }

        // Convert to HashMap
        let mut manifest_data: HashMap<String, String> = HashMap::new();
        let mut i = 0;
        while i + 1 < fields_raw.len() {
            manifest_data.insert(fields_raw[i].clone(), fields_raw[i + 1].clone());
            i += 2;
        }


        let layout = manifest_data.get("layout").cloned().unwrap_or_else(|| "custom".to_string());
        let version = manifest_data.get("v").cloned().unwrap_or_else(|| "1.0.0".to_string());
        let slot_count: usize = manifest_data.get("sc").and_then(|s| s.parse().ok()).unwrap_or(0);

        // Parse slots JSON
        let slots_json = manifest_data.get("slots").cloned().unwrap_or_else(|| "[]".to_string());
        let slot_defs: Vec<Value> = serde_json::from_str(&slots_json).unwrap_or_default();

        // Resolve slot content
        let mut slots = Vec::with_capacity(slot_count);
        for slot_def in &slot_defs {
            let slot = self.resolve_slot(site_id, slot_def, &mut conn).await;
            slots.push(slot);
        }

        // Parse sections JSON
        let sections_json = manifest_data.get("sections").cloned().unwrap_or_else(|| "{}".to_string());
        let mut sections: HashMap<String, Value> = serde_json::from_str(&sections_json).unwrap_or_default();

        // Resolve section references (sections can point to stored keys)
        let section_keys: Vec<String> = sections.keys().cloned().collect();
        for key in section_keys {
            if let Some(section) = sections.get(&key).cloned() {
                if let Some(source) = section.get("source").and_then(|v| v.as_str()) {
                    if source == "key" {
                        if let Some(ref_key) = section.get("key").and_then(|v| v.as_str()) {
                            let resolved_key = if ref_key.contains('{') {
                                ref_key.to_string()
                            } else {
                                format!("{{{}}}{}", site_id, ref_key)
                            };
                            let value: Option<String> = redis::cmd("GET")
                                .arg(&resolved_key)
                                .query_async(&mut conn)
                                .await
                                .unwrap_or(None);
                            if let Some(json_str) = value {
                                if let Ok(parsed) = serde_json::from_str::<Value>(&json_str) {
                                    sections.insert(key, parsed);
                                }
                            }
                        }
                    } else if source == "inline" {
                        // Inline data — use the "data" field directly
                        if let Some(data) = section.get("data").cloned() {
                            sections.insert(key, data);
                        }
                    }
                }
            }
        }

        // Parse build options
        let build_options_json = manifest_data.get("bo").cloned().unwrap_or_else(|| "{}".to_string());
        let build_options: Value = serde_json::from_str(&build_options_json).unwrap_or(json!({}));
        let ttl: usize = build_options.get("ttl").and_then(|v| v.as_u64()).unwrap_or(300) as usize;
        let compress = build_options.get("compress").and_then(|v| v.as_bool()).unwrap_or(true);

        // Determine output key
        let output_key = build_options.get("output_key")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("{{{}}}:gnode:bundle:{}", site_id, manifest_id));

        // Assemble bundle
        let bundle = ManifestBundle {
            manifest_id: manifest_id.to_string(),
            manifest_version: version,
            layout,
            slots,
            sections,
            built_at: current_timestamp(),
            builder_version: "3.0.0".to_string(),
        };

        // Serialize
        let json_str = serde_json::to_string(&bundle).map_err(|e| format!("Serialization failed: {}", e))?;
        let json_len = json_str.len();

        // Optionally compress
        let (stored_data, stored_len) = if compress {
            let mut encoder = GzEncoder::new(Vec::new(), Compression::best());
            encoder.write_all(json_str.as_bytes()).map_err(|e| format!("Compression failed: {}", e))?;
            let compressed = encoder.finish().map_err(|e| format!("Compression finish failed: {}", e))?;
            let len = compressed.len();
            (compressed, len)
        } else {
            let len = json_str.len();
            (json_str.into_bytes(), len)
        };

        // Store bundle
        let _: () = redis::cmd("SETEX")
            .arg(&output_key)
            .arg(ttl)
            .arg(&stored_data)
            .query_async(&mut conn)
            .await
            .map_err(|e| format!("Failed to store bundle: {}", e))?;

        // Store build metadata
        let meta_key = format!("{}:meta", output_key);
        let _: () = redis::cmd("HSET")
            .arg(&meta_key)
            .arg("ba").arg(current_timestamp().to_string())
            .arg("sz").arg(json_len.to_string())
            .arg("csz").arg(stored_len.to_string())
            .arg("ac").arg(bundle.slots.len().to_string())
            .arg("bv").arg("3.0.0")
            .query_async(&mut conn)
            .await
            .map_err(|e| format!("Failed to store build metadata: {}", e))?;

        if ttl > 0 {
            let _: () = redis::cmd("EXPIRE").arg(&meta_key).arg(ttl)
                .query_async(&mut conn).await.unwrap_or_default();
        }

        let elapsed = start.elapsed().as_millis();
        let ratio = if json_len > 0 { (stored_len as f64 / json_len as f64) * 100.0 } else { 0.0 };

        debug!(
            "Built manifest '{}' for {}: {}KB -> {}KB ({:.1}%) in {}ms",
            manifest_id, site_id,
            json_len / 1024, stored_len / 1024,
            ratio, elapsed
        );

        Ok(())
    }

    /// Resolve a single slot definition into content
    async fn resolve_slot(
        &self,
        site_id: &str,
        slot_def: &Value,
        conn: &mut redis::aio::MultiplexedConnection,
    ) -> SlotContent {
        let id = slot_def.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let position = slot_def.get("position").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let content_type = slot_def.get("content_type").and_then(|v| v.as_str()).unwrap_or("text/html").to_string();

        // Try inline content first
        let html = if let Some(content) = slot_def.get("content").and_then(|v| v.as_str()) {
            content.to_string()
        } else if let Some(asset_key) = slot_def.get("asset_key").and_then(|v| v.as_str()) {
            // Dereference from stored asset
            let resolved_key = if asset_key.contains('{') {
                asset_key.to_string()
            } else {
                format!("{{{}}}:asset:{}", site_id, asset_key)
            };
            {
                let val: Option<String> = redis::cmd("GET")
                    .arg(&resolved_key)
                    .query_async(conn)
                    .await
                    .unwrap_or(None);
                val.unwrap_or_default()
            }
        } else {
            String::new()
        };

        let css = slot_def.get("css").and_then(|v| v.as_str()).map(|s| s.to_string())
            .or_else(|| {
                slot_def.get("css_key").and_then(|v| v.as_str()).map(|_| String::new())
                // Future: resolve css_key from ValKey
            });

        let js = slot_def.get("js").and_then(|v| v.as_str()).map(|s| s.to_string());

        SlotContent { id, position, html, css, js, content_type }
    }

    /// Copy a manifest-built bundle to the bundle:full key
    async fn copy_manifest_to_compat(&self, site_id: &str, manifest_id: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut conn = self.redis_client.get_multiplexed_async_connection().await?;

        let source_key = format!("{{{}}}:gnode:bundle:{}", site_id, manifest_id);
        let compat_key = format!("{{{}}}:gnode:bundle:full", site_id);

        // Read the manifest bundle
        let data: Option<Vec<u8>> = redis::cmd("GET")
            .arg(&source_key)
            .query_async(&mut conn)
            .await?;

        if let Some(bundle_data) = data {
            let ttl: i64 = redis::cmd("TTL")
                .arg(&source_key)
                .query_async(&mut conn)
                .await
                .unwrap_or(300);

            let ttl = if ttl > 0 { ttl as usize } else { 300 };

            let _: () = redis::cmd("SETEX")
                .arg(&compat_key)
                .arg(ttl)
                .arg(&bundle_data)
                .query_async(&mut conn)
                .await?;

            debug!("Copied manifest '{}' bundle to compat key for {}", manifest_id, site_id);
        }

        Ok(())
    }

    /// Build bundle from face_mapping data
    ///
    /// For sites without manifests. Reads {site_id}:gnode:face_mapping,
    /// assembles a FaceBundle, gzip-compresses, stores at {site_id}:gnode:bundle:full.
    async fn build_compat_bundle(&self, site_id: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let start = Instant::now();
        let mut conn = self.redis_client.get_multiplexed_async_connection().await?;

        // Read face mapping (PHP-synced)
        let mapping_key = format!("{{{}}}:gnode:face_mapping", site_id);
        let mapping_json: Option<String> = redis::cmd("GET")
            .arg(&mapping_key)
            .query_async(&mut conn)
            .await?;

        let mapping_json = match mapping_json {
            Some(json) => json,
            None => {
                debug!("No face mapping found for {}, skipping compat build", site_id);
                return Ok(());
            }
        };

        let mapping: Value = serde_json::from_str(&mapping_json)
            .map_err(|e| format!("Failed to parse face mapping for {}: {}", site_id, e))?;

        // Extract faces
        let empty_faces = json!([]);
        let faces_data = mapping.get("faces").unwrap_or(&empty_faces);
        let faces_array = faces_data.as_array();

        // Determine face count from data (support both 6-face cube and 8-cell tesseract)
        let face_count = if let Some(arr) = faces_array {
            arr.len().min(8)
        } else {
            6 // default to cube
        };

        let mut faces = Vec::with_capacity(face_count);
        for face_id in 0..face_count {
            let face_obj = if let Some(arr) = faces_array {
                arr.get(face_id)
            } else {
                let face_key = face_id.to_string();
                faces_data.get(&face_key)
            };

            if let Some(face_obj) = face_obj {
                let html = face_obj.get("html")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();

                let css = face_obj.get("css")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());

                let js = face_obj.get("js")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());

                if !html.is_empty() {
                    faces.push(FaceData { id: face_id as u8, html, css, js });
                } else {
                    faces.push(FaceData {
                        id: face_id as u8,
                        html: format!("<div class='face face-{}' data-site='{}'>Face {} for {}</div>",
                            face_id, site_id, face_id, site_id),
                        css, js,
                    });
                }
            } else {
                faces.push(FaceData {
                    id: face_id as u8,
                    html: format!("<div class='face face-{}' data-site='{}'>Face {} for {}</div>",
                        face_id, site_id, face_id, site_id),
                    css: None, js: None,
                });
            }
        }

        // Assemble bundle with remaining sections passed through as-is
        let bundle = FaceBundle {
            faces,
            posts: mapping.get("posts").cloned().unwrap_or(json!({"list": [], "by_id": {}})),
            navigation: mapping.get("navigation").cloned().unwrap_or(json!({"menu": [], "breadcrumbs": []})),
            metadata: mapping.get("metadata").cloned().unwrap_or(json!({})),
            built_at: current_timestamp(),
            version: "2.0.0".to_string(),
        };

        // Serialize + compress + store
        let json_str = serde_json::to_string(&bundle)?;
        let json_len = json_str.len();

        let mut encoder = GzEncoder::new(Vec::new(), Compression::best());
        encoder.write_all(json_str.as_bytes())?;
        let compressed = encoder.finish()?;
        let compressed_len = compressed.len();

        let bundle_key = format!("{{{}}}:gnode:bundle:full", site_id);
        let _: () = redis::cmd("SETEX")
            .arg(&bundle_key)
            .arg(300)
            .arg(&compressed)
            .query_async(&mut conn)
            .await?;

        let elapsed = start.elapsed().as_millis();
        let ratio = if json_len > 0 { (compressed_len as f64 / json_len as f64) * 100.0 } else { 0.0 };

        debug!(
            "Compat bundle for {}: {}KB -> {}KB ({:.1}%) in {}ms ({} faces)",
            site_id, json_len / 1024, compressed_len / 1024, ratio, elapsed, face_count
        );

        Ok(())
    }

    /// Get registered sites from Lua registry
    async fn get_registered_sites(&self) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
        let mut conn = self.redis_client.get_multiplexed_async_connection().await?;

        let result: String = redis::cmd("FCALL")
            .arg("GNODE_SERVICE_LIST_ALL")
            .arg(0)
            .query_async(&mut conn)
            .await?;

        let response: Value = serde_json::from_str(&result)?;

        let sites: Vec<String> = response.get("sites")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();

        Ok(sites)
    }
}

/// Get current timestamp in seconds
fn current_timestamp() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_manifest_bundle_serialization() {
        let bundle = ManifestBundle {
            manifest_id: "main".to_string(),
            manifest_version: "1.0.0".to_string(),
            layout: "cube".to_string(),
            slots: vec![
                SlotContent {
                    id: "face_0".to_string(),
                    position: "front".to_string(),
                    html: "<div>Front</div>".to_string(),
                    css: Some(".face-0 { color: red; }".to_string()),
                    js: None,
                    content_type: "text/html".to_string(),
                },
            ],
            sections: HashMap::from([
                ("metadata".to_string(), json!({"site_name": "Test"})),
            ]),
            built_at: 1700000000.0,
            builder_version: "3.0.0".to_string(),
        };

        let json = serde_json::to_string(&bundle).unwrap();
        assert!(json.contains("\"builder_version\":\"3.0.0\""));
        assert!(json.contains("\"layout\":\"cube\""));

        // Round-trip
        let parsed: ManifestBundle = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.manifest_id, "main");
        assert_eq!(parsed.slots.len(), 1);
        assert_eq!(parsed.slots[0].id, "face_0");
    }

    #[test]
    fn test_face_bundle_serialization() {
        let bundle = FaceBundle {
            faces: vec![
                FaceData { id: 0, html: "<div>Front</div>".to_string(), css: None, js: None },
            ],
            posts: json!({"list": [], "by_id": {}}),
            navigation: json!({"menu": [], "breadcrumbs": []}),
            metadata: json!({"site_name": "Test"}),
            built_at: 1700000000.0,
            version: "2.0.0".to_string(),
        };

        let json = serde_json::to_string(&bundle).unwrap();
        assert!(json.contains("\"version\":\"2.0.0\""));
    }

    #[test]
    fn test_gzip_compression_ratio() {
        let test_data = "Hello, World!".repeat(100);
        let original_len = test_data.len();

        let mut encoder = GzEncoder::new(Vec::new(), Compression::best());
        encoder.write_all(test_data.as_bytes()).unwrap();
        let compressed = encoder.finish().unwrap();
        let compressed_len = compressed.len();

        assert!(compressed_len < original_len);
        let ratio = (compressed_len as f64 / original_len as f64) * 100.0;
        assert!(ratio < 50.0, "Compression ratio should be < 50% for repetitive data");
    }
}
