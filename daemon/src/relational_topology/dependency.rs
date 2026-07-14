//! Dependency Topology - Service dependency management with geometric encoding.
//!
//! This module provides a specialized topology for managing service dependencies
//! where the Z-axis encodes the dependency hierarchy, making cycles geometrically
//! impossible and enabling O(1) cycle detection.
//!
//! # Axis Semantics
//!
//! - **X-axis**: Functional domain (data, messaging, auth, compute, web, integration)
//! - **Y-axis**: Operational scope (internal, external, hybrid, infrastructure)
//! - **Z-axis**: Fundamentality (0.0 = foundation, 1.0 = endpoint)
//!
//! # Key Insight
//!
//! ```text
//! "A DAG is not a graph - it's a GEOMETRY with a monotonic coordinate constraint"
//!
//! DAG-property: ∀ edge(A→B): Z(A) > Z(B)
//! Result: Cycle-detection = O(1) coordinate comparison
//! ```

use super::constraint::{ConstraintError, DependencyConstraint};
use super::edge::{DependencyMeta, DependencyType};
use super::point::RelPoint3D;
use super::topology::{EntityId, EntityMetadata, RelationalTopology, RelEntity};
use super::voxel::VoxelKey;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Type alias for dependency topology.
pub type DependencyTopology = RelationalTopology<DependencyConstraint, DependencyMeta>;

/// Functional domains for X-axis clustering.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FunctionalDomain {
    /// Data storage and persistence (databases, file systems)
    Data,
    /// Message queues, event buses, pub/sub
    Messaging,
    /// Authentication, authorization, identity
    Auth,
    /// Computation, processing, ML/AI
    Compute,
    /// Web servers, HTTP APIs, GraphQL
    Web,
    /// External integrations, third-party APIs
    Integration,
    /// Custom domain
    Custom(u8),
}

impl FunctionalDomain {
    /// Convert domain to X coordinate (0.0 - 1.0).
    pub fn to_coordinate(&self) -> f64 {
        match self {
            Self::Data => 0.1,
            Self::Messaging => 0.25,
            Self::Auth => 0.4,
            Self::Compute => 0.55,
            Self::Web => 0.7,
            Self::Integration => 0.85,
            Self::Custom(v) => (*v as f64) / 255.0,
        }
    }

    /// Parse a domain name into a FunctionalDomain variant.
    pub fn parse(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "data" | "database" | "storage" | "persistence" => Self::Data,
            "messaging" | "queue" | "pubsub" | "events" => Self::Messaging,
            "auth" | "authentication" | "authorization" | "identity" => Self::Auth,
            "compute" | "processing" | "ml" | "ai" => Self::Compute,
            "web" | "http" | "api" | "graphql" => Self::Web,
            "integration" | "external" | "thirdparty" => Self::Integration,
            _ => Self::Custom(128), // Default to middle
        }
    }
}

impl Default for FunctionalDomain {
    fn default() -> Self {
        Self::Custom(128)
    }
}

/// Operational scopes for Y-axis clustering.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationalScope {
    /// Infrastructure services (OS, networking, monitoring)
    Infrastructure,
    /// Internal services (not exposed externally)
    Internal,
    /// Hybrid services (internal + external access)
    Hybrid,
    /// External-facing services (public APIs, frontends)
    External,
    /// Custom scope
    Custom(u8),
}

impl OperationalScope {
    /// Convert scope to Y coordinate (0.0 - 1.0).
    pub fn to_coordinate(&self) -> f64 {
        match self {
            Self::Infrastructure => 0.15,
            Self::Internal => 0.4,
            Self::Hybrid => 0.65,
            Self::External => 0.85,
            Self::Custom(v) => (*v as f64) / 255.0,
        }
    }

    /// Parse a scope name into an OperationalScope variant.
    pub fn parse(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "infrastructure" | "infra" | "system" => Self::Infrastructure,
            "internal" | "private" => Self::Internal,
            "hybrid" | "mixed" => Self::Hybrid,
            "external" | "public" | "exposed" => Self::External,
            _ => Self::Custom(128),
        }
    }
}

impl Default for OperationalScope {
    fn default() -> Self {
        Self::Internal
    }
}

/// Service definition for registration.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServiceDefinition {
    /// Unique service identifier.
    pub id: EntityId,

    /// Human-readable name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    /// Functional domain for X-axis positioning.
    #[serde(default)]
    pub domain: FunctionalDomain,

    /// Operational scope for Y-axis positioning.
    #[serde(default)]
    pub scope: OperationalScope,

    /// Capabilities this service provides.
    #[serde(default)]
    pub provides: Vec<String>,

    /// Capabilities this service requires.
    #[serde(default)]
    pub requires: Vec<String>,

    /// Direct dependencies (service IDs).
    #[serde(default)]
    pub dependencies: Vec<DependencySpec>,

    /// Optional explicit Z position (overrides computed Z).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub explicit_z: Option<f64>,
}

impl ServiceDefinition {
    /// Create a new service definition.
    pub fn new(id: impl Into<EntityId>) -> Self {
        Self {
            id: id.into(),
            name: None,
            domain: FunctionalDomain::default(),
            scope: OperationalScope::default(),
            provides: Vec::new(),
            requires: Vec::new(),
            dependencies: Vec::new(),
            explicit_z: None,
        }
    }

    /// Set the name.
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Set the domain.
    pub fn with_domain(mut self, domain: FunctionalDomain) -> Self {
        self.domain = domain;
        self
    }

    /// Set the scope.
    pub fn with_scope(mut self, scope: OperationalScope) -> Self {
        self.scope = scope;
        self
    }

    /// Add a provided capability.
    pub fn provides(mut self, cap: impl Into<String>) -> Self {
        self.provides.push(cap.into());
        self
    }

    /// Add a required capability.
    pub fn requires_cap(mut self, cap: impl Into<String>) -> Self {
        self.requires.push(cap.into());
        self
    }

    /// Add a hard dependency.
    pub fn depends_on(mut self, service_id: impl Into<EntityId>) -> Self {
        self.dependencies.push(DependencySpec {
            service_id: service_id.into(),
            dep_type: DependencyType::Hard,
            capability: None,
        });
        self
    }

    /// Add a soft dependency.
    pub fn soft_depends_on(mut self, service_id: impl Into<EntityId>) -> Self {
        self.dependencies.push(DependencySpec {
            service_id: service_id.into(),
            dep_type: DependencyType::Soft,
            capability: None,
        });
        self
    }

    /// Add a dependency with full specification.
    pub fn with_dependency(mut self, spec: DependencySpec) -> Self {
        self.dependencies.push(spec);
        self
    }

    /// Set explicit Z position.
    pub fn at_z(mut self, z: f64) -> Self {
        self.explicit_z = Some(z);
        self
    }
}

/// Dependency specification.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DependencySpec {
    /// Target service ID.
    pub service_id: EntityId,

    /// Type of dependency.
    #[serde(default)]
    pub dep_type: DependencyType,

    /// Optional capability requirement.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capability: Option<String>,
}

/// Result of service registration.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RegistrationResult {
    /// Service ID.
    pub service_id: EntityId,

    /// Computed position.
    pub position: (f64, f64, f64),

    /// Voxel key.
    pub voxel_key: VoxelKey,

    /// Z-slice for load ordering.
    pub z_slice: usize,

    /// Number of dependencies added.
    pub dependency_count: usize,

    /// Any warnings during registration.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

/// Missing dependency information.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MissingDependency {
    /// The capability that is required but not provided.
    pub capability: String,

    /// Suggested providers (services that provide this capability).
    pub suggested_providers: Vec<EntityId>,
}

/// Z-epsilon for separating services at the same computed Z level.
const Z_EPSILON: f64 = 0.01;

/// Minimum Z value (foundation layer).
const Z_MIN: f64 = 0.05;

/// Maximum Z value (endpoint layer).
const Z_MAX: f64 = 0.95;

/// Extension trait for DependencyTopology with specialized operations.
pub trait DependencyTopologyExt {
    /// Register a service with automatic Z-computation based on dependencies.
    ///
    /// The Z coordinate is computed as: `max(Z(dep) for dep in dependencies) + ε`
    /// This ensures the service is always above its dependencies in the hierarchy.
    fn register_service(&mut self, def: ServiceDefinition) -> Result<RegistrationResult, DependencyError>;

    /// Compute the Z position for a service based on its dependencies.
    fn compute_z(&self, dependencies: &[EntityId]) -> f64;

    /// Detect missing dependencies (required capabilities without providers).
    fn detect_missing_dependencies(&self, service_id: &EntityId) -> Vec<MissingDependency>;

    /// Find services that provide a specific capability.
    fn find_capability_providers(&self, capability: &str) -> Vec<&EntityId>;

    /// Get the load order as a list of levels (services that can start in parallel).
    fn load_order_levels(&self) -> Vec<Vec<&EntityId>>;

    /// Validate all dependencies are satisfied.
    fn validate_dependencies(&self) -> Vec<DependencyError>;

    /// Get dependency chain for a service (transitive dependencies).
    fn dependency_chain(&self, service_id: &EntityId) -> Vec<&EntityId>;

    /// Get dependent chain for a service (transitive dependents).
    fn dependent_chain(&self, service_id: &EntityId) -> Vec<&EntityId>;

    /// Check if adding a dependency would be valid (O(1) check).
    fn can_add_dependency(&self, from: &EntityId, to: &EntityId) -> bool;

    /// Get services at the foundation level (no dependencies).
    fn foundation_services(&self) -> Vec<&EntityId>;

    /// Get services at the endpoint level (no dependents).
    fn endpoint_services(&self) -> Vec<&EntityId>;
}

/// Errors specific to dependency topology operations.
#[derive(Debug, Clone)]
pub enum DependencyError {
    /// Service already exists.
    AlreadyExists(EntityId),

    /// Dependency not found.
    DependencyNotFound(EntityId),

    /// Would create a cycle.
    WouldCreateCycle { from: EntityId, to: EntityId },

    /// Constraint error.
    ConstraintError(String),

    /// Invalid Z position.
    InvalidZPosition { service_id: EntityId, z: f64, reason: String },
}

impl std::fmt::Display for DependencyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlreadyExists(id) => write!(f, "Service already exists: {}", id),
            Self::DependencyNotFound(id) => write!(f, "Dependency not found: {}", id),
            Self::WouldCreateCycle { from, to } => {
                write!(f, "Would create cycle: {} → {}", from, to)
            }
            Self::ConstraintError(msg) => write!(f, "Constraint error: {}", msg),
            Self::InvalidZPosition { service_id, z, reason } => {
                write!(f, "Invalid Z position for {}: {} ({})", service_id, z, reason)
            }
        }
    }
}

impl std::error::Error for DependencyError {}

impl From<ConstraintError> for DependencyError {
    fn from(e: ConstraintError) -> Self {
        Self::ConstraintError(e.to_string())
    }
}

impl DependencyTopologyExt for DependencyTopology {
    fn register_service(&mut self, def: ServiceDefinition) -> Result<RegistrationResult, DependencyError> {
        // Check if service already exists
        if self.contains(&def.id) {
            return Err(DependencyError::AlreadyExists(def.id));
        }

        let mut warnings = Vec::new();

        // Validate all dependencies exist
        for dep in &def.dependencies {
            if !self.contains(&dep.service_id) {
                return Err(DependencyError::DependencyNotFound(dep.service_id.clone()));
            }
        }

        // Compute Z position
        let dep_ids: Vec<_> = def.dependencies.iter().map(|d| d.service_id.clone()).collect();
        let z = if let Some(explicit_z) = def.explicit_z {
            // Validate explicit Z is above all dependencies
            let min_z = self.compute_z(&dep_ids);
            if explicit_z <= min_z {
                return Err(DependencyError::InvalidZPosition {
                    service_id: def.id.clone(),
                    z: explicit_z,
                    reason: format!("Must be > {} (max dependency Z)", min_z - Z_EPSILON),
                });
            }
            explicit_z.clamp(Z_MIN, Z_MAX)
        } else {
            self.compute_z(&dep_ids)
        };

        // Compute X and Y from domain and scope
        let x = def.domain.to_coordinate();
        let y = def.scope.to_coordinate();

        // Create position
        let position = RelPoint3D::from_f64(x, y, z);

        // Create entity metadata
        let mut metadata = EntityMetadata::new();
        if let Some(name) = &def.name {
            metadata = metadata.with_name(name.clone());
        }
        if let Some(domain_str) = Some(format!("{:?}", def.domain)) {
            metadata = metadata.with_domain(domain_str);
        }
        if let Some(scope_str) = Some(format!("{:?}", def.scope)) {
            metadata = metadata.with_scope(scope_str);
        }
        for cap in &def.provides {
            metadata.provides.push(cap.clone());
        }
        for cap in &def.requires {
            metadata.requires.push(cap.clone());
        }

        // Register entity
        let entity = RelEntity::with_metadata(def.id.clone(), position, metadata);
        let (voxel_key, z_slice) = self.register(entity)?;

        // Add dependency edges
        let mut dependency_count = 0;
        for dep_spec in &def.dependencies {
            let meta = DependencyMeta {
                dep_type: dep_spec.dep_type,
                capability: dep_spec.capability.clone(),
                ..Default::default()
            };

            match self.add_edge(def.id.clone(), dep_spec.service_id.clone(), meta) {
                Ok(()) => dependency_count += 1,
                Err(e) => {
                    warnings.push(format!("Failed to add edge to {}: {}", dep_spec.service_id, e));
                }
            }
        }

        Ok(RegistrationResult {
            service_id: def.id,
            position: (x, y, z),
            voxel_key,
            z_slice,
            dependency_count,
            warnings,
        })
    }

    fn compute_z(&self, dependencies: &[EntityId]) -> f64 {
        if dependencies.is_empty() {
            return Z_MIN;
        }

        let max_dep_z = dependencies
            .iter()
            .filter_map(|id| self.get(id))
            .map(|e| e.position.z.to_f64())
            .fold(0.0f64, |a, b| a.max(b));

        // New service is placed above the highest dependency
        (max_dep_z + Z_EPSILON).clamp(Z_MIN, Z_MAX)
    }

    fn detect_missing_dependencies(&self, service_id: &EntityId) -> Vec<MissingDependency> {
        let entity = match self.get(service_id) {
            Some(e) => e,
            None => return Vec::new(),
        };

        let mut missing = Vec::new();

        // Check each required capability
        for required_cap in &entity.metadata.requires {
            // Find current dependencies that provide this capability
            let deps = self.dependencies(service_id);
            let is_satisfied = deps.iter().any(|dep_id| {
                self.get(dep_id)
                    .map(|dep| dep.metadata.provides.contains(required_cap))
                    .unwrap_or(false)
            });

            if !is_satisfied {
                // Find potential providers
                let providers = self.find_capability_providers(required_cap);

                // Filter to only services below this one in Z
                let z_threshold = entity.position.z;
                let suggested: Vec<_> = providers
                    .iter()
                    .filter(|id| {
                        self.get(id)
                            .map(|e| e.position.z < z_threshold)
                            .unwrap_or(false)
                    })
                    .map(|id| (*id).clone())
                    .collect();

                missing.push(MissingDependency {
                    capability: required_cap.clone(),
                    suggested_providers: suggested,
                });
            }
        }

        missing
    }

    fn find_capability_providers(&self, capability: &str) -> Vec<&EntityId> {
        self.entities()
            .filter(|e| e.metadata.provides.contains(&capability.to_string()))
            .map(|e| &e.id)
            .collect()
    }

    fn load_order_levels(&self) -> Vec<Vec<&EntityId>> {
        let mut levels: Vec<Vec<&EntityId>> = vec![Vec::new(); 10];

        for id in self.load_order() {
            if let Some(entity) = self.get(id) {
                let z_slice = entity.position.z_slice(10);
                if z_slice < levels.len() {
                    levels[z_slice].push(id);
                }
            }
        }

        // Remove empty levels
        levels.into_iter().filter(|l| !l.is_empty()).collect()
    }

    fn validate_dependencies(&self) -> Vec<DependencyError> {
        let mut errors = Vec::new();

        for entity in self.entities() {
            // Check for missing required capabilities
            for missing in self.detect_missing_dependencies(&entity.id) {
                if missing.suggested_providers.is_empty() {
                    errors.push(DependencyError::DependencyNotFound(
                        format!("capability:{}", missing.capability)
                    ));
                }
            }
        }

        errors
    }

    fn dependency_chain(&self, service_id: &EntityId) -> Vec<&EntityId> {
        let mut visited = HashSet::new();
        let mut chain = Vec::new();
        let mut stack = vec![service_id];

        while let Some(current) = stack.pop() {
            if visited.contains(current) {
                continue;
            }
            visited.insert(current.clone());

            for dep_id in self.dependencies(current) {
                if !visited.contains(dep_id) {
                    chain.push(dep_id);
                    stack.push(dep_id);
                }
            }
        }

        chain
    }

    fn dependent_chain(&self, service_id: &EntityId) -> Vec<&EntityId> {
        let mut visited = HashSet::new();
        let mut chain = Vec::new();
        let mut stack = vec![service_id];

        while let Some(current) = stack.pop() {
            if visited.contains(current) {
                continue;
            }
            visited.insert(current.clone());

            for dep_id in self.dependents(current) {
                if !visited.contains(dep_id) {
                    chain.push(dep_id);
                    stack.push(dep_id);
                }
            }
        }

        chain
    }

    fn can_add_dependency(&self, from: &EntityId, to: &EntityId) -> bool {
        self.can_connect(from, to).is_ok()
    }

    fn foundation_services(&self) -> Vec<&EntityId> {
        self.entity_ids()
            .filter(|id| self.out_degree(id) == 0)
            .collect()
    }

    fn endpoint_services(&self) -> Vec<&EntityId> {
        self.entity_ids()
            .filter(|id| self.in_degree(id) == 0)
            .collect()
    }
}

/// Create a new DependencyTopology with default settings.
pub fn create_dependency_topology() -> DependencyTopology {
    DependencyTopology::new(DependencyConstraint::new())
}

/// Create a DependencyTopology with minimum Z-delta requirement.
pub fn create_dependency_topology_with_delta(min_delta: f64) -> DependencyTopology {
    DependencyTopology::new(DependencyConstraint::with_min_delta(min_delta))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_service_definition_builder() {
        let def = ServiceDefinition::new("api-server")
            .with_name("REST API Server")
            .with_domain(FunctionalDomain::Web)
            .with_scope(OperationalScope::External)
            .provides("http-api")
            .requires_cap("database")
            .depends_on("postgres");

        assert_eq!(def.id, "api-server");
        assert_eq!(def.domain, FunctionalDomain::Web);
        assert_eq!(def.scope, OperationalScope::External);
        assert_eq!(def.provides, vec!["http-api"]);
        assert_eq!(def.requires, vec!["database"]);
        assert_eq!(def.dependencies.len(), 1);
    }

    #[test]
    fn test_domain_coordinates() {
        assert!((FunctionalDomain::Data.to_coordinate() - 0.1).abs() < 0.01);
        assert!((FunctionalDomain::Web.to_coordinate() - 0.7).abs() < 0.01);
    }

    #[test]
    fn test_scope_coordinates() {
        assert!((OperationalScope::Infrastructure.to_coordinate() - 0.15).abs() < 0.01);
        assert!((OperationalScope::External.to_coordinate() - 0.85).abs() < 0.01);
    }

    #[test]
    fn test_register_service_with_z_computation() {
        let mut topo = create_dependency_topology();

        // Register foundation service
        let db_result = topo.register_service(
            ServiceDefinition::new("postgres")
                .with_domain(FunctionalDomain::Data)
                .with_scope(OperationalScope::Infrastructure)
                .provides("sql-database")
        ).unwrap();

        assert!(db_result.position.2 < 0.1); // Near bottom

        // Register service depending on postgres
        let api_result = topo.register_service(
            ServiceDefinition::new("api")
                .with_domain(FunctionalDomain::Web)
                .requires_cap("sql-database")
                .depends_on("postgres")
        ).unwrap();

        // API should be above postgres
        assert!(api_result.position.2 > db_result.position.2);
    }

    #[test]
    fn test_z_computation_chain() {
        let mut topo = create_dependency_topology();

        // Build a chain: foundation → middleware → app → frontend
        topo.register_service(ServiceDefinition::new("foundation").at_z(0.1)).unwrap();
        topo.register_service(ServiceDefinition::new("middleware").depends_on("foundation")).unwrap();
        topo.register_service(ServiceDefinition::new("app").depends_on("middleware")).unwrap();
        topo.register_service(ServiceDefinition::new("frontend").depends_on("app")).unwrap();

        // Verify Z increases along the chain
        let z_foundation = topo.get(&"foundation".to_string()).unwrap().position.z.to_f64();
        let z_middleware = topo.get(&"middleware".to_string()).unwrap().position.z.to_f64();
        let z_app = topo.get(&"app".to_string()).unwrap().position.z.to_f64();
        let z_frontend = topo.get(&"frontend".to_string()).unwrap().position.z.to_f64();

        assert!(z_foundation < z_middleware);
        assert!(z_middleware < z_app);
        assert!(z_app < z_frontend);
    }

    #[test]
    fn test_load_order_levels() {
        let mut topo = create_dependency_topology();

        // Create services at different levels
        topo.register_service(ServiceDefinition::new("db1").at_z(0.1)).unwrap();
        topo.register_service(ServiceDefinition::new("db2").at_z(0.1)).unwrap();
        topo.register_service(ServiceDefinition::new("cache").at_z(0.2)).unwrap();
        topo.register_service(ServiceDefinition::new("api").at_z(0.5)).unwrap();

        let levels = topo.load_order_levels();

        // Should have multiple levels, with db1 and db2 in the same first level
        assert!(!levels.is_empty());

        // First level should contain foundation services
        let first_level: HashSet<_> = levels[0].iter().map(|id| id.as_str()).collect();
        assert!(first_level.contains("db1") || first_level.contains("db2"));
    }

    #[test]
    fn test_detect_missing_dependencies() {
        let mut topo = create_dependency_topology();

        // Register a database that provides "sql"
        topo.register_service(
            ServiceDefinition::new("postgres")
                .provides("sql")
        ).unwrap();

        // Register an API that requires "sql" and "cache" but only depends on postgres
        topo.register_service(
            ServiceDefinition::new("api")
                .requires_cap("sql")
                .requires_cap("cache")  // This is missing!
                .depends_on("postgres")
        ).unwrap();

        let missing = topo.detect_missing_dependencies(&"api".to_string());

        assert_eq!(missing.len(), 1);
        assert_eq!(missing[0].capability, "cache");
    }

    #[test]
    fn test_find_capability_providers() {
        let mut topo = create_dependency_topology();

        topo.register_service(ServiceDefinition::new("postgres").provides("sql")).unwrap();
        topo.register_service(ServiceDefinition::new("mysql").provides("sql")).unwrap();
        topo.register_service(ServiceDefinition::new("redis").provides("cache")).unwrap();

        let sql_providers = topo.find_capability_providers("sql");
        assert_eq!(sql_providers.len(), 2);

        let cache_providers = topo.find_capability_providers("cache");
        assert_eq!(cache_providers.len(), 1);
    }

    #[test]
    fn test_dependency_chain() {
        let mut topo = create_dependency_topology();

        topo.register_service(ServiceDefinition::new("a").at_z(0.1)).unwrap();
        topo.register_service(ServiceDefinition::new("b").depends_on("a")).unwrap();
        topo.register_service(ServiceDefinition::new("c").depends_on("b")).unwrap();
        topo.register_service(ServiceDefinition::new("d").depends_on("c")).unwrap();

        let chain = topo.dependency_chain(&"d".to_string());

        // d depends on c, which depends on b, which depends on a
        assert_eq!(chain.len(), 3);
        assert!(chain.iter().any(|id| id.as_str() == "a"));
        assert!(chain.iter().any(|id| id.as_str() == "b"));
        assert!(chain.iter().any(|id| id.as_str() == "c"));
    }

    #[test]
    fn test_foundation_and_endpoint_services() {
        let mut topo = create_dependency_topology();

        topo.register_service(ServiceDefinition::new("db").at_z(0.1)).unwrap();
        topo.register_service(ServiceDefinition::new("api").depends_on("db")).unwrap();
        topo.register_service(ServiceDefinition::new("web").depends_on("api")).unwrap();

        let foundations = topo.foundation_services();
        let endpoints = topo.endpoint_services();

        // db has no outgoing edges (no dependencies) - it's a foundation
        assert!(foundations.iter().any(|id| id.as_str() == "db"));

        // web has no incoming edges (no dependents) - it's an endpoint
        assert!(endpoints.iter().any(|id| id.as_str() == "web"));
    }

    #[test]
    fn test_explicit_z_validation() {
        let mut topo = create_dependency_topology();

        topo.register_service(ServiceDefinition::new("db").at_z(0.5)).unwrap();

        // Try to add a service that depends on db but at a lower Z - should fail
        let result = topo.register_service(
            ServiceDefinition::new("api")
                .depends_on("db")
                .at_z(0.3)  // Below db!
        );

        assert!(matches!(result, Err(DependencyError::InvalidZPosition { .. })));
    }

    #[test]
    fn test_can_add_dependency() {
        let mut topo = create_dependency_topology();

        topo.register_service(ServiceDefinition::new("low").at_z(0.2)).unwrap();
        topo.register_service(ServiceDefinition::new("high").at_z(0.8)).unwrap();

        // high → low is valid
        assert!(topo.can_add_dependency(&"high".to_string(), &"low".to_string()));

        // low → high is invalid (would go upward)
        assert!(!topo.can_add_dependency(&"low".to_string(), &"high".to_string()));
    }
}
