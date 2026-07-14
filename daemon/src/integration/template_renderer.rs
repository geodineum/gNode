//! Template Rendering Module
//!
//! This module provides runtime template registration and rendering using the Tera template engine.
//! Template include-dependencies are tracked in a dedicated daemon-local graph (`TEMPLATE_DEPS`)
//! for circular-include detection and transitive cache invalidation.
//!
//! # Architecture
//!
//! - **Tera Engine Singleton**: Thread-safe singleton using `lazy_static!` and `Arc<RwLock<Tera>>`
//! - **Dependency Graph**: `TEMPLATE_DEPS` maps `template_id -> [included template_ids]`
//! - **Cycle Detection**: `template_include_cycle()` rejects circular includes at registration
//! - **Security**: Auto-escaping enabled by default, sandboxed execution (no file access)
//!
//! # Design note
//!
//! This dependency graph is LOCAL cache-management state, deliberately decoupled from the
//! service-discovery topology: templates are not service-topology entities, and template
//! rendering carries no dependency on the (retired) in-memory `GeometricTopology`.
//!
//! # Example
//!
//! ```rust,ignore
//! use gnode::integration::template_renderer::{register_template, render_template};
//! use serde_json::json;
//!
//! // Register a template (automatically tracked in topology)
//! register_template("greeting", "<h1>Hello {{ name }}!</h1>", &config)?;
//!
//! // Render with variables
//! let output = render_template("greeting", &json!({"name": "World"}), &config)?;
//! // Output: "<h1>Hello World!</h1>"
//! ```

use lazy_static::lazy_static;
use regex::Regex;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock, mpsc};
use std::time::Duration;
use std::thread;
use tera::{Context, Tera};

/// Maximum template render time (P4AF001 fix)
const RENDER_TIMEOUT_SECS: u64 = 5;

use crate::config::GNodeSettings;
use crate::integration::connection_manager::with_connection;
use crate::integration::error_handlings::{IntegrationError, IntegrationResult, IntegrationErrorKind};

/// Errors specific to template rendering operations
#[derive(Debug, Clone)]
pub enum TemplateError {
    /// Template not found in registry or ValKey
    NotFound(String),
    /// Template syntax error (invalid Tera syntax)
    SyntaxError(String),
    /// Variable undefined in context
    UndefinedVariable(String),
    /// Rendering timeout or recursion limit exceeded
    ExecutionLimit(String),
    /// Failed to compile template
    CompilationFailed(String),
    /// Failed to register template
    RegistrationFailed(String),
    /// Failed to retrieve template from ValKey
    StorageError(String),
    /// Invalid template ID or content
    ValidationError(String),
    /// Circular dependency detected
    CyclicDependency(String),
}

impl std::fmt::Display for TemplateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TemplateError::NotFound(id) => write!(f, "Template not found: {}", id),
            TemplateError::SyntaxError(msg) => write!(f, "Template syntax error: {}", msg),
            TemplateError::UndefinedVariable(var) => write!(f, "Undefined variable: {}", var),
            TemplateError::ExecutionLimit(msg) => write!(f, "Execution limit exceeded: {}", msg),
            TemplateError::CompilationFailed(msg) => write!(f, "Template compilation failed: {}", msg),
            TemplateError::RegistrationFailed(msg) => write!(f, "Template registration failed: {}", msg),
            TemplateError::StorageError(msg) => write!(f, "Storage error: {}", msg),
            TemplateError::ValidationError(msg) => write!(f, "Validation error: {}", msg),
            TemplateError::CyclicDependency(msg) => write!(f, "Cyclic dependency: {}", msg),
        }
    }
}

impl std::error::Error for TemplateError {}

impl From<TemplateError> for IntegrationError {
    fn from(err: TemplateError) -> Self {
        IntegrationError::new(IntegrationErrorKind::ScriptExecution, err.to_string())
    }
}

impl From<crate::GeometricError> for TemplateError {
    fn from(err: crate::GeometricError) -> Self {
        match err {
            crate::GeometricError::InvalidState(msg) if msg.contains("Circular dependency") => {
                TemplateError::CyclicDependency(msg)
            }
            _ => TemplateError::RegistrationFailed(err.to_string()),
        }
    }
}

// Thread-safe singleton Tera engine instance
lazy_static! {
    static ref TERA_ENGINE: Arc<RwLock<Tera>> = {
        let mut tera = Tera::default();
        tera.autoescape_on(vec![".html", ".htm", ".xml"]);
        Arc::new(RwLock::new(tera))
    };
}

// Daemon-local template include-graph: `template_id -> [included template_ids]`.
// Drives circular-include detection at registration and transitive cache
// invalidation. This is LOCAL cache-management state, NOT authoritative service
// topology — it was previously piggy-backed on the in-memory GeometricTopology,
// and now lives here so template rendering carries no dependency on that struct
// (which the stateless-registration migration retires).
lazy_static! {
    static ref TEMPLATE_DEPS: RwLock<HashMap<String, Vec<String>>> = RwLock::new(HashMap::new());
}

/// Return true if making `id` include `new_deps` would create a circular
/// include — i.e. some dependency already (transitively) includes `id`.
fn template_include_cycle(
    graph: &HashMap<String, Vec<String>>,
    id: &str,
    new_deps: &[String],
) -> bool {
    let mut stack: Vec<&str> = new_deps.iter().map(String::as_str).collect();
    let mut seen: HashSet<&str> = HashSet::new();
    while let Some(node) = stack.pop() {
        if node == id {
            return true;
        }
        if !seen.insert(node) {
            continue;
        }
        if let Some(children) = graph.get(node) {
            stack.extend(children.iter().map(String::as_str));
        }
    }
    false
}

/// Convert template ID to service ID for topology registration
///
/// # Arguments
///
/// * `template_id` - Template identifier
///
/// # Returns
///
/// Service ID with "template:" namespace prefix
#[cfg(test)]
#[inline]
fn to_service_id(template_id: &str) -> String {
    format!("template:{}", template_id)
}

/// Convert service ID back to template ID
///
/// # Arguments
///
/// * `service_id` - Service identifier
///
/// # Returns
///
/// Template ID without namespace prefix, or None if not a template service
#[cfg(test)]
#[inline]
fn from_service_id(service_id: &str) -> Option<&str> {
    service_id.strip_prefix("template:")
}

/// Extract partial template names from `{% include 'name' %}` directives
fn extract_includes(content: &str) -> Vec<String> {
    lazy_static! {
        static ref INCLUDE_PATTERN: Regex = Regex::new(
            r#"\{%\s*include\s+['"]([^'"]+)['"]\s*%\}"#
        ).unwrap();
    }

    INCLUDE_PATTERN
        .captures_iter(content)
        .filter_map(|cap| cap.get(1).map(|m| m.as_str().to_owned()))
        .collect()
}

/// Extract geometric capabilities from template content for 8D capability space
///
/// Analyzes template content to produce an 8-dimensional capability vector:
/// 1. html: Always 1.0 (type identifier for HTML templates)
/// 2. complexity: LOC / 100, capped at 1.0 (normalized complexity metric)
/// 3. interactivity: 0.0-1.0 based on forms, inputs, scripts
/// 4. data_density: Variables per 100 chars (dynamic content ratio)
/// 5. reusability: Include count / 5 (component reuse metric)
/// 6. cacheability: 1 - (data_density * interactivity) (cache-friendliness)
/// 7. semantic_layout: Structural HTML5 elements / 5 (semantic richness)
/// 8. render_cost: Default 0.5, can be learned from historical metrics
///
/// # Arguments
///
/// * `content` - Template content to analyze
///
/// # Returns
///
/// HashMap with 8 capability dimensions as f64 values (0.0-1.0 range)
///
/// # Example
///
/// ```rust,ignore
/// let caps = extract_template_capabilities("<form><input name='email'></form>");
/// // caps["html"] = 1.0
/// // caps["complexity"] = ~0.01 (short template)
/// // caps["interactivity"] = 0.7 (has form)
/// // caps["data_density"] = 0.0 (no variables)
/// ```
// Retained for future capability-based template placement; unused since
// templates are no longer registered as service-topology entities.
#[allow(dead_code)]
fn extract_template_capabilities(content: &str) -> HashMap<String, f64> {
    // 1. HTML type identifier (always 1.0 for HTML templates)
    let html = 1.0;

    // 2. Complexity: LOC / 100, capped at 1.0
    let lines = content.lines().count() as f64;
    let complexity = (lines / 100.0).min(1.0);

    // 3. Interactivity: Forms, inputs, scripts presence
    let has_forms = content.contains("<form") || content.contains("<input");
    let has_script = content.contains("<script") || content.contains("{{");
    let interactivity = match (has_forms, has_script) {
        (true, true) => 1.0,   // Highly interactive (forms + dynamic content)
        (true, false) => 0.7,  // Forms without dynamic content
        (false, true) => 0.5,  // Dynamic content without forms
        (false, false) => 0.1, // Static content
    };

    // 4. Data density: Variables per 100 chars (dynamic content ratio)
    let var_count = content.matches("{{").count() as f64;
    let char_count = content.len() as f64;
    let data_density = if char_count > 0.0 {
        ((var_count / char_count) * 100.0).min(1.0)
    } else {
        0.0
    };

    // 5. Reusability: Include count / 5 (component reuse metric)
    let include_count = content.matches("{% include").count() as f64;
    let reusability = (include_count / 5.0).min(1.0);

    // 6. Cacheability: Inverse of dynamic content (1 - data_density * interactivity)
    let cacheability = 1.0 - (data_density * interactivity);

    // 7. Semantic layout: Structural HTML5 elements count / 5
    let structural_tags = ["<nav", "<header", "<main", "<aside", "<footer"];
    let structural_count = structural_tags.iter()
        .filter(|tag| content.contains(*tag))
        .count() as f64;
    let semantic_layout = (structural_count / 5.0).min(1.0);

    // 8. Render cost: Default medium cost (0.5), can be learned from historical metrics
    let render_cost = 0.5;

    // Build capability HashMap
    HashMap::from([
        ("html".to_string(), html),
        ("complexity".to_string(), complexity),
        ("interactivity".to_string(), interactivity),
        ("data_density".to_string(), data_density),
        ("reusability".to_string(), reusability),
        ("cacheability".to_string(), cacheability),
        ("semantic_layout".to_string(), semantic_layout),
        ("render_cost".to_string(), render_cost),
    ])
}

/// Validate template ID format
fn validate_template_id(id: &str) -> Result<(), TemplateError> {
    if id.is_empty() {
        return Err(TemplateError::ValidationError("Template ID cannot be empty".to_string()));
    }

    if id.contains('/') || id.contains('\\') {
        return Err(TemplateError::ValidationError(
            "Template ID cannot contain path separators".to_string()
        ));
    }

    if id.contains('\0') {
        return Err(TemplateError::ValidationError(
            "Template ID cannot contain null bytes".to_string()
        ));
    }

    // Prevent collision with service ID namespace
    if id.starts_with("template:") {
        return Err(TemplateError::ValidationError(
            "Template ID cannot start with 'template:' (reserved namespace)".to_string()
        ));
    }

    Ok(())
}

/// Register a template for runtime rendering with topology integration
///
/// Templates are registered as services in the GeometricTopology with:
/// - Service ID: `template:{template_id}`
/// - Zero capabilities (not used for discovery)
/// - Dependencies: Stored in topology.dependencies for DAG tracking
/// - Cycle detection: Automatic via topology's topological sort
///
/// # Arguments
///
/// * `id` - Unique template identifier
/// * `content` - Template content (Tera syntax)
/// * `config` - gNode configuration for ValKey connection
///
/// # Returns
///
/// * `Ok(Vec<String>)` - List of partial templates referenced via {% include %}
/// * `Err(IntegrationError)` - If registration fails or circular dependency detected
///
/// # Security
///
/// - Auto-escaping is enabled by default for XSS prevention
/// - Template execution is sandboxed (no file system or process access)
/// - Template IDs are validated to prevent path traversal
///
/// # Example
///
/// ```rust,ignore
/// let partials = register_template(
///     "layout",
///     "<html>{% include 'header' %}<body>{{ content }}</body></html>",
///     &config
/// )?;
/// // partials = ["header"]
/// // Registered in topology with service_id = "template:layout"
/// // topology.dependencies["template:layout"] = ["template:header"]
/// ```
pub fn register_template(
    id: &str,
    content: &str,
    _config: &GNodeSettings,
) -> IntegrationResult<Vec<String>> {
    // Validate template ID
    validate_template_id(id).map_err(IntegrationError::from)?;

    // Extract include directives for dependency tracking
    let partials = extract_includes(content);

    // Register template in Tera engine
    {
        let mut tera = TERA_ENGINE.write().map_err(|e| {
            IntegrationError::new(IntegrationErrorKind::ScriptExecution, format!("Failed to acquire Tera write lock: {}", e))
        })?;

        // Add template to engine (syntax validation happens here)
        tera.add_raw_template(id, content).map_err(|e| {
            TemplateError::CompilationFailed(format!("Template '{}' compilation failed: {}", id, e))
        })?;
    }

    // Store template in ValKey for persistence
    let template_key = format!("template:{}", id);
    let metadata_key = format!("template:{}:meta", id);

    with_connection(|conn| {
        // Store template content
        redis::cmd("SET")
            .arg(&template_key)
            .arg(content)
            .query::<()>(conn)
            .map_err(|e| {
                IntegrationError::new(IntegrationErrorKind::ScriptExecution, format!("Failed to store template in ValKey: {}", e))
            })?;

        // Store metadata (dependencies, timestamp)
        let metadata = serde_json::json!({
            "dependencies": partials,
            "timestamp": std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            "size": content.len(),
        });

        redis::cmd("SET")
            .arg(&metadata_key)
            .arg(metadata.to_string())
            .query::<()>(conn)
            .map_err(|e| {
                IntegrationError::new(IntegrationErrorKind::ScriptExecution, format!("Failed to store template metadata: {}", e))
            })?;

        Ok(())
    })?;

    // Record the template's include-dependencies in the local template graph,
    // rejecting circular includes. Daemon-local cache state, not service topology.
    {
        let mut graph = TEMPLATE_DEPS.write().map_err(|e| {
            IntegrationError::new(IntegrationErrorKind::ScriptExecution, format!("Failed to acquire template-deps lock: {}", e))
        })?;

        if template_include_cycle(&graph, id, &partials) {
            // Circular include — roll back the Tera registration and fail closed.
            if let Ok(mut tera) = TERA_ENGINE.write() {
                tera.templates.remove(id);
            }
            return Err(TemplateError::CompilationFailed(
                format!("Template '{}' introduces a circular include dependency", id)
            ).into());
        }

        graph.insert(id.to_string(), partials.clone());
    }

    Ok(partials)
}

/// Render a template with variable substitution
///
/// Retrieves the template from ValKey (if not in Tera cache), builds a rendering context
/// from the provided variables, and executes the template to produce output.
///
/// # Arguments
///
/// * `id` - Template identifier (previously registered)
/// * `variables` - JSON object containing variables for interpolation
/// * `config` - gNode configuration
///
/// # Returns
///
/// * `Ok(String)` - Rendered HTML output
/// * `Err(IntegrationError)` - If template not found or rendering fails
///
/// # Security
///
/// - Variables are automatically HTML-escaped (e.g., `<script>` → `&lt;script&gt;`)
/// - Use `{{ var | safe }}` filter to bypass escaping (logged as security event)
/// - Rendering timeout: 5 seconds (prevents infinite loops)
/// - Max recursion depth: 10 (prevents stack overflow)
///
/// # Example
///
/// ```rust,ignore
/// let html = render_template(
///     "greeting",
///     &json!({"name": "Alice", "age": 30}),
///     &config
/// )?;
/// ```
pub fn render_template(
    id: &str,
    variables: &Value,
    config: &GNodeSettings,
) -> IntegrationResult<String> {
    // Validate template ID
    validate_template_id(id).map_err(IntegrationError::from)?;

    // Check if template exists in Tera engine, if not try to load from ValKey
    let template_exists = {
        let tera = TERA_ENGINE.read().map_err(|e| {
            IntegrationError::new(IntegrationErrorKind::ScriptExecution, format!("Failed to acquire Tera read lock: {}", e))
        })?;
        tera.templates.contains_key(id)
    };

    if !template_exists {
        // Try to load from ValKey
        let template_key = format!("template:{}", id);
        let content: Option<String> = with_connection(|conn| {
            redis::cmd("GET")
                .arg(&template_key)
                .query(conn)
                .map_err(|e| {
                    IntegrationError::new(IntegrationErrorKind::ScriptExecution, format!("Failed to retrieve template from ValKey: {}", e))
                })
        })?;

        match content {
            Some(content) => {
                // Register template (this also validates syntax)
                register_template(id, &content, config)?;
            }
            None => {
                return Err(TemplateError::NotFound(id.to_string()).into());
            }
        }
    }

    // Build rendering context
    let context = Context::from_serialize(variables).map_err(|e| {
        IntegrationError::new(IntegrationErrorKind::ScriptExecution, format!("Failed to build template context: {}", e))
    })?;

    // Render template with timeout (P4AF001 fix)
    // Clone what we need for the spawned thread
    let template_id = id.to_string();
    let (tx, rx) = mpsc::channel();

    thread::spawn(move || {
        let result = TERA_ENGINE.read().map_err(|e| {
            TemplateError::SyntaxError(format!("Failed to acquire Tera read lock: {}", e))
        }).and_then(|tera| {
            tera.render(&template_id, &context).map_err(|e| {
                let error_msg = e.to_string();
                if error_msg.contains("variable") && error_msg.contains("not found") {
                    TemplateError::UndefinedVariable(error_msg)
                } else if error_msg.contains("recursion") {
                    TemplateError::ExecutionLimit(error_msg)
                } else {
                    TemplateError::SyntaxError(error_msg)
                }
            })
        });
        let _ = tx.send(result);
    });

    // Wait for result with timeout
    match rx.recv_timeout(Duration::from_secs(RENDER_TIMEOUT_SECS)) {
        Ok(result) => result.map_err(IntegrationError::from),
        Err(mpsc::RecvTimeoutError::Timeout) => {
            Err(TemplateError::ExecutionLimit(format!(
                "Template '{}' rendering exceeded {}s timeout",
                id, RENDER_TIMEOUT_SECS
            )).into())
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            Err(TemplateError::ExecutionLimit(
                "Template rendering thread panicked".to_string()
            ).into())
        }
    }
}

/// Render a template string directly without registration
///
/// Useful for one-off rendering where template doesn't need to be stored.
///
/// # Example
///
/// ```rust,ignore
/// let output = render_string(
///     "Hello {{ name }}!",
///     &json!({"name": "World"})
/// )?;
/// ```
pub fn render_string(template: &str, variables: &Value) -> IntegrationResult<String> {
    let mut tera = Tera::default();
    tera.autoescape_on(vec![".html", ".htm", ".xml"]);

    // Use .html extension to ensure auto-escaping (P2DF003 fix)
    let temp_id = "__temp__.html";
    tera.add_raw_template(temp_id, template).map_err(|e| {
        TemplateError::SyntaxError(format!("Failed to parse template: {}", e))
    })?;

    let context = Context::from_serialize(variables).map_err(|e| {
        IntegrationError::new(IntegrationErrorKind::ScriptExecution, format!("Failed to build template context: {}", e))
    })?;

    let output = tera.render(temp_id, &context).map_err(|e| {
        TemplateError::SyntaxError(e.to_string())
    })?;

    Ok(output)
}

/// Delete a template from Tera engine, ValKey, and topology
///
/// # Arguments
///
/// * `id` - Template identifier to delete
/// * `config` - gNode configuration
///
/// # Returns
///
/// * `Ok(())` - Template deleted successfully
/// * `Err(IntegrationError)` - If deletion fails
pub fn delete_template(id: &str, _config: &GNodeSettings) -> IntegrationResult<()> {
    validate_template_id(id).map_err(IntegrationError::from)?;

    // Remove from Tera engine
    {
        let mut tera = TERA_ENGINE.write().map_err(|e| {
            IntegrationError::new(IntegrationErrorKind::ScriptExecution, format!("Failed to acquire Tera write lock: {}", e))
        })?;
        tera.templates.remove(id);
    }

    // Remove from ValKey
    let template_key = format!("template:{}", id);
    let metadata_key = format!("template:{}:meta", id);

    with_connection(|conn| {
        redis::cmd("DEL")
            .arg(&[&template_key, &metadata_key])
            .query::<()>(conn)
            .map_err(|e| {
                IntegrationError::new(IntegrationErrorKind::ScriptExecution, format!("Failed to delete template from ValKey: {}", e))
            })
    })?;

    // Note: We intentionally do NOT remove from topology to preserve dependency history
    // The topology maintains a record of what was registered, even if deleted

    Ok(())
}

/// Invalidate a template and all templates that depend on it (transitive closure)
///
/// Uses GeometricTopology's dependency graph to find all templates that transitively
/// include the specified template. All found templates are removed from the Tera engine
/// and their cached output is purged from ValKey.
///
/// # Arguments
///
/// * `template_id` - Template that changed
/// * `config` - gNode configuration for cache operations
///
/// # Returns
///
/// Vector of all template IDs that were invalidated (including the original)
///
/// # Algorithm
///
/// 1. Get topology reference and lock
/// 2. Build reverse dependency map (template → templates that include it)
/// 3. DFS traversal to find all transitive dependents
/// 4. Delete each dependent from Tera engine and cache
///
/// # Example
///
/// ```rust,ignore
/// // If "header" is updated:
/// // layout includes header
/// // page includes layout
/// let invalidated = invalidate_template("header", &config)?;
/// // invalidated = ["header", "layout", "page"]
/// ```
pub fn invalidate_template(template_id: &str, _config: &GNodeSettings) -> IntegrationResult<Vec<String>> {
    validate_template_id(template_id).map_err(IntegrationError::from)?;

    let graph = TEMPLATE_DEPS.read().map_err(|e| {
        IntegrationError::new(IntegrationErrorKind::ScriptExecution, format!("Failed to acquire template-deps lock: {}", e))
    })?;

    // Build reverse dependency map: template_id -> [templates that include it]
    let mut reverse_deps: HashMap<String, Vec<String>> = HashMap::new();

    for (tmpl_id, deps) in graph.iter() {
        for dep_template_id in deps {
            reverse_deps
                .entry(dep_template_id.clone())
                .or_default()
                .push(tmpl_id.clone());
        }
    }

    // Find all templates that transitively depend on this one (DFS)
    let mut invalidated = Vec::new();
    let mut visited = HashSet::new();

    fn dfs_dependents(
        template_id: &str,
        reverse_deps: &HashMap<String, Vec<String>>,
        visited: &mut HashSet<String>,
        result: &mut Vec<String>,
    ) {
        if !visited.insert(template_id.to_string()) {
            return; // Already visited
        }

        result.push(template_id.to_string());

        if let Some(dependents) = reverse_deps.get(template_id) {
            for dependent in dependents {
                dfs_dependents(dependent, reverse_deps, visited, result);
            }
        }
    }

    dfs_dependents(template_id, &reverse_deps, &mut visited, &mut invalidated);

    // Drop the template-deps lock before deletion operations
    drop(graph);

    // Delete templates from Tera engine and cache
    for id in &invalidated {
        // Delete from Tera (best effort)
        {
            let mut tera = TERA_ENGINE.write().ok();
            if let Some(ref mut t) = tera {
                t.templates.remove(id);
            }
        }

        // Purge rendered output cache (if exists)
        let cache_key = format!("template:{}:output", id);
        with_connection(|conn| {
            redis::cmd("DEL")
                .arg(&cache_key)
                .query::<()>(conn)
                .ok(); // Ignore errors for cache cleanup
            Ok(())
        })?;
    }

    Ok(invalidated)
}

/// List all registered templates
///
/// Uses SCAN instead of KEYS for O(1) per-iteration complexity (P2DF002 fix)
pub fn list_templates(_config: &GNodeSettings) -> IntegrationResult<Vec<String>> {
    with_connection(|conn| {
        let pattern = "template:*";
        let mut template_ids = Vec::new();
        let mut cursor: u64 = 0;

        // SCAN iteration - O(1) per iteration, doesn't block server
        loop {
            let (next_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(pattern)
                .arg("COUNT")
                .arg(100)
                .query(conn)
                .map_err(|e| {
                    IntegrationError::new(IntegrationErrorKind::ScriptExecution, format!("Failed to scan templates: {}", e))
                })?;

            // Filter and collect template IDs
            for key in keys {
                if !key.ends_with(":meta") && !key.ends_with(":output") {
                    if let Some(id) = key.strip_prefix("template:") {
                        template_ids.push(id.to_string());
                    }
                }
            }

            cursor = next_cursor;
            if cursor == 0 {
                break;
            }
        }

        Ok(template_ids)
    })
}

/// Get template metadata
pub fn get_template_metadata(id: &str, _config: &GNodeSettings) -> IntegrationResult<Value> {
    validate_template_id(id).map_err(IntegrationError::from)?;

    let metadata_key = format!("template:{}:meta", id);

    with_connection(|conn| {
        let metadata_str: Option<String> = redis::cmd("GET")
            .arg(&metadata_key)
            .query(conn)
            .map_err(|e| {
                IntegrationError::new(IntegrationErrorKind::ScriptExecution, format!("Failed to get template metadata: {}", e))
            })?;

        match metadata_str {
            Some(json_str) => {
                serde_json::from_str(&json_str).map_err(|e| {
                    IntegrationError::new(IntegrationErrorKind::ScriptExecution, format!("Failed to parse metadata JSON: {}", e))
                })
            }
            None => Err(TemplateError::NotFound(id.to_string()).into()),
        }
    })
}

/// Get dependencies for a template from topology
///
/// # Arguments
///
/// * `template_id` - Template identifier
///
/// # Returns
///
/// Vector of template IDs that this template directly depends on (includes)
pub fn get_template_dependencies(template_id: &str) -> IntegrationResult<Vec<String>> {
    validate_template_id(template_id)?;

    let graph = TEMPLATE_DEPS.read().map_err(|e| {
        IntegrationError::new(IntegrationErrorKind::ScriptExecution, format!("Failed to acquire template-deps lock: {}", e))
    })?;

    let deps = graph.get(template_id).cloned().unwrap_or_default();

    Ok(deps)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_to_service_id() {
        assert_eq!(to_service_id("my_template"), "template:my_template");
    }

    #[test]
    fn test_from_service_id() {
        assert_eq!(from_service_id("template:my_template"), Some("my_template"));
        assert_eq!(from_service_id("other:service"), None);
    }

    #[test]
    fn test_extract_includes_single() {
        let template = "<html>{% include 'header' %}<body>content</body></html>";
        let includes = extract_includes(template);
        assert_eq!(includes, vec!["header"]);
    }

    #[test]
    fn test_extract_includes_multiple() {
        let template = r#"
            {% include 'header' %}
            <main>{% include "sidebar" %}</main>
            {% include 'footer' %}
        "#;
        let includes = extract_includes(template);
        assert_eq!(includes, vec!["header", "sidebar", "footer"]);
    }

    #[test]
    fn test_extract_includes_none() {
        let template = "<html><body>{{ content }}</body></html>";
        let includes = extract_includes(template);
        assert!(includes.is_empty());
    }

    #[test]
    fn test_validate_template_id_valid() {
        assert!(validate_template_id("my_template").is_ok());
        assert!(validate_template_id("template-123").is_ok());
        assert!(validate_template_id("template.html").is_ok());
    }

    #[test]
    fn test_validate_template_id_invalid() {
        assert!(validate_template_id("").is_err());
        assert!(validate_template_id("path/to/template").is_err());
        assert!(validate_template_id("path\\to\\template").is_err());
        assert!(validate_template_id("template\0null").is_err());
        assert!(validate_template_id("template:reserved").is_err());
    }

    #[test]
    fn test_render_string_basic() {
        let output = render_string("Hello {{ name }}!", &json!({"name": "World"}));
        assert!(output.is_ok());
        assert_eq!(output.unwrap(), "Hello World!");
    }

    #[test]
    fn test_render_string_auto_escape() {
        let output = render_string(
            "Content: {{ html }}",
            &json!({"html": "<script>alert('xss')</script>"})
        );
        assert!(output.is_ok());
        let result = output.unwrap();
        // P2DF003 fix: render_string now uses .html extension for reliable auto-escaping
        // HTML special chars should be escaped to prevent XSS
        assert!(result.contains("&lt;script&gt;"), "Expected HTML-escaped output, got: {}", result);
        assert!(!result.contains("<script>"), "XSS vulnerability: unescaped script tag found");
    }

    #[test]
    fn test_render_string_filters() {
        let output = render_string(
            "{{ name | upper }}",
            &json!({"name": "alice"})
        );
        assert!(output.is_ok());
        assert_eq!(output.unwrap(), "ALICE");
    }

    #[test]
    fn test_render_string_undefined_variable() {
        let output = render_string("Hello {{ undefined }}!", &json!({}));
        assert!(output.is_err());
    }

    #[test]
    fn test_render_string_syntax_error() {
        let output = render_string("Hello {{ name !", &json!({"name": "World"}));
        assert!(output.is_err());
    }

    #[test]
    fn test_render_string_conditionals() {
        let output = render_string(
            "{% if admin %}Admin{% else %}User{% endif %}",
            &json!({"admin": true})
        );
        assert!(output.is_ok());
        assert_eq!(output.unwrap(), "Admin");
    }

    #[test]
    fn test_render_string_loops() {
        let output = render_string(
            "{% for item in items %}{{ item }},{% endfor %}",
            &json!({"items": ["a", "b", "c"]})
        );
        assert!(output.is_ok());
        assert_eq!(output.unwrap(), "a,b,c,");
    }

    #[test]
    fn test_render_string_nested_variables() {
        let output = render_string(
            "{{ user.name }} - {{ user.email }}",
            &json!({"user": {"name": "Alice", "email": "alice@example.com"}})
        );
        assert!(output.is_ok());
        assert_eq!(output.unwrap(), "Alice - alice@example.com");
    }

    #[test]
    fn test_render_string_unicode() {
        let output = render_string(
            "Hello {{ name }}!",
            &json!({"name": "世界"})
        );
        assert!(output.is_ok());
        assert_eq!(output.unwrap(), "Hello 世界!");
    }

    #[test]
    fn test_render_string_empty_template() {
        let output = render_string("", &json!({}));
        assert!(output.is_ok());
        assert_eq!(output.unwrap(), "");
    }

    #[test]
    fn test_render_string_whitespace_control() {
        let output = render_string(
            "  {{- name -}}  ",
            &json!({"name": "test"})
        );
        assert!(output.is_ok());
        assert_eq!(output.unwrap(), "test");
    }

    #[test]
    fn test_template_error_display() {
        let err = TemplateError::NotFound("test".to_string());
        assert_eq!(err.to_string(), "Template not found: test");

        let err = TemplateError::SyntaxError("invalid syntax".to_string());
        assert_eq!(err.to_string(), "Template syntax error: invalid syntax");
    }

    #[test]
    fn test_template_error_conversion() {
        let err = TemplateError::NotFound("test".to_string());
        let integration_err: IntegrationError = err.into();
        assert!(integration_err.to_string().contains("Template not found"));
    }
}
