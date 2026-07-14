//! Core RelationalTopology generic struct.
//!
//! The main data structure that combines voxel grid, z-slice indexing,
//! constraint validation, and edge storage into a unified topology.

use super::constraint::{Constraint, ConstraintError};
use super::edge::{EdgeMetadata, RelEdge};
use super::point::RelPoint3D;
use super::voxel::{VoxelGrid, VoxelKey};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Entity identifier (typically service ID, node ID, etc.).
pub type EntityId = String;

/// Entity in relational space.
#[derive(Clone, Debug)]
pub struct RelEntity {
    /// Unique identifier.
    pub id: EntityId,

    /// Position in 3D relational space.
    pub position: RelPoint3D,

    /// Entity-specific metadata.
    pub metadata: EntityMetadata,
}

impl RelEntity {
    /// Create a new entity.
    pub fn new(id: impl Into<EntityId>, position: RelPoint3D) -> Self {
        Self {
            id: id.into(),
            position,
            metadata: EntityMetadata::default(),
        }
    }

    /// Create with metadata.
    pub fn with_metadata(id: impl Into<EntityId>, position: RelPoint3D, metadata: EntityMetadata) -> Self {
        Self {
            id: id.into(),
            position,
            metadata,
        }
    }
}

/// Entity metadata.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct EntityMetadata {
    /// Human-readable name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    /// Description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// Domain/category for X-axis clustering.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub domain: Option<String>,

    /// Scope for Y-axis clustering.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,

    /// Capabilities this entity provides.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub provides: Vec<String>,

    /// Capabilities this entity requires.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub requires: Vec<String>,

    /// Additional key-value properties.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub properties: HashMap<String, String>,
}

impl EntityMetadata {
    /// Create empty metadata.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the name.
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Set the domain.
    pub fn with_domain(mut self, domain: impl Into<String>) -> Self {
        self.domain = Some(domain.into());
        self
    }

    /// Set the scope.
    pub fn with_scope(mut self, scope: impl Into<String>) -> Self {
        self.scope = Some(scope.into());
        self
    }

    /// Add a provided capability.
    pub fn provides(mut self, cap: impl Into<String>) -> Self {
        self.provides.push(cap.into());
        self
    }

    /// Add a required capability.
    pub fn requires(mut self, cap: impl Into<String>) -> Self {
        self.requires.push(cap.into());
        self
    }
}

/// Generic relational topology with configurable constraint and edge metadata.
///
/// # Type Parameters
///
/// - `C`: Constraint type that validates edges (e.g., DependencyConstraint)
/// - `M`: Edge metadata type (e.g., DependencyMeta, CommunicationMeta)
///
/// # Performance
///
/// | Operation           | Complexity |
/// |---------------------|------------|
/// | Register entity     | O(1)       |
/// | Get entity          | O(1)       |
/// | Add edge            | O(1)       |
/// | Validate edge       | O(1)       |
/// | Find in voxel       | O(1)       |
/// | Z-ordered iteration | O(L)       |
/// | Find below Z        | O(Z)       |
pub struct RelationalTopology<C: Constraint, M: EdgeMetadata> {
    /// Entity storage by ID.
    entities: HashMap<EntityId, RelEntity>,

    /// Voxel grid for spatial indexing.
    voxel_grid: VoxelGrid,

    /// Outgoing edges by entity ID.
    outgoing: HashMap<EntityId, Vec<RelEdge<M>>>,

    /// Incoming edges by entity ID.
    incoming: HashMap<EntityId, Vec<RelEdge<M>>>,

    /// Constraint for edge validation.
    constraint: C,

    /// Edge metadata type marker (for type inference).
    _edge_marker: std::marker::PhantomData<M>,
}

impl<C: Constraint, M: EdgeMetadata> RelationalTopology<C, M> {
    /// Create a new topology with the given constraint.
    pub fn new(constraint: C) -> Self {
        Self {
            entities: HashMap::new(),
            voxel_grid: VoxelGrid::new(),
            outgoing: HashMap::new(),
            incoming: HashMap::new(),
            constraint,
            _edge_marker: std::marker::PhantomData,
        }
    }

    /// Get the constraint.
    pub fn constraint(&self) -> &C {
        &self.constraint
    }

    /// Get the constraint name.
    pub fn constraint_name(&self) -> &'static str {
        self.constraint.name()
    }

    // ==================== Entity Operations ====================

    /// Register a new entity at the given position - O(1).
    ///
    /// Returns the voxel key and z-slice where the entity was placed.
    pub fn register(&mut self, entity: RelEntity) -> Result<(VoxelKey, usize), ConstraintError> {
        let id = entity.id.clone();
        let position = entity.position;

        // Insert into voxel grid
        let (voxel_key, z_slice) = self.voxel_grid.insert(id.clone(), &position);

        // Store entity
        self.entities.insert(id.clone(), entity);

        // Initialize edge lists
        self.outgoing.entry(id.clone()).or_default();
        self.incoming.entry(id).or_default();

        Ok((voxel_key, z_slice))
    }

    /// Deregister an entity and all its edges - O(E) where E = edges involving entity.
    pub fn deregister(&mut self, id: &EntityId) -> Option<RelEntity> {
        let entity = self.entities.remove(id)?;

        // Remove from voxel grid
        self.voxel_grid.remove(id, &entity.position);

        // Remove outgoing edges
        if let Some(outgoing) = self.outgoing.remove(id) {
            for edge in outgoing {
                // Remove corresponding incoming edge from target
                if let Some(incoming) = self.incoming.get_mut(&edge.to) {
                    incoming.retain(|e| &e.from != id);
                }
            }
        }

        // Remove incoming edges
        if let Some(incoming) = self.incoming.remove(id) {
            for edge in incoming {
                // Remove corresponding outgoing edge from source
                if let Some(outgoing) = self.outgoing.get_mut(&edge.from) {
                    outgoing.retain(|e| &e.to != id);
                }
            }
        }

        Some(entity)
    }

    /// Get an entity by ID - O(1).
    pub fn get(&self, id: &EntityId) -> Option<&RelEntity> {
        self.entities.get(id)
    }

    /// Get an entity mutably by ID - O(1).
    pub fn get_mut(&mut self, id: &EntityId) -> Option<&mut RelEntity> {
        self.entities.get_mut(id)
    }

    /// Check if an entity exists - O(1).
    pub fn contains(&self, id: &EntityId) -> bool {
        self.entities.contains_key(id)
    }

    /// Get total entity count.
    pub fn entity_count(&self) -> usize {
        self.entities.len()
    }

    /// Iterate all entities.
    pub fn entities(&self) -> impl Iterator<Item = &RelEntity> {
        self.entities.values()
    }

    /// Iterate all entity IDs.
    pub fn entity_ids(&self) -> impl Iterator<Item = &EntityId> {
        self.entities.keys()
    }

    // ==================== Edge Operations ====================

    /// Check if an edge between two entities would be valid - O(1).
    ///
    /// This does NOT add the edge, just validates it.
    pub fn can_connect(&self, from: &EntityId, to: &EntityId) -> Result<(), ConstraintError> {
        let from_entity = self.entities.get(from)
            .ok_or_else(|| ConstraintError::EntityNotFound(from.clone()))?;
        let to_entity = self.entities.get(to)
            .ok_or_else(|| ConstraintError::EntityNotFound(to.clone()))?;

        self.constraint.validate_edge(from_entity, to_entity)
    }

    /// Add a directed edge from one entity to another - O(1).
    ///
    /// The edge is validated against the topology's constraint before being added.
    pub fn add_edge(&mut self, from: EntityId, to: EntityId, metadata: M) -> Result<(), ConstraintError> {
        // Validate the edge
        self.can_connect(&from, &to)?;

        // Get positions for edge vector computation
        let from_pos = self.entities.get(&from)
            .ok_or_else(|| ConstraintError::EntityNotFound(from.clone()))?.position;
        let to_pos = self.entities.get(&to)
            .ok_or_else(|| ConstraintError::EntityNotFound(to.clone()))?.position;

        // Create edge
        let edge = RelEdge::new(from.clone(), to.clone(), &from_pos, &to_pos, metadata);

        // Store in both directions
        self.outgoing.entry(from.clone()).or_default().push(edge.clone());
        self.incoming.entry(to).or_default().push(edge);

        Ok(())
    }

    /// Remove an edge between two entities.
    pub fn remove_edge(&mut self, from: &EntityId, to: &EntityId) -> bool {
        let mut removed = false;

        if let Some(outgoing) = self.outgoing.get_mut(from) {
            let before = outgoing.len();
            outgoing.retain(|e| &e.to != to);
            removed = outgoing.len() < before;
        }

        if let Some(incoming) = self.incoming.get_mut(to) {
            incoming.retain(|e| &e.from != from);
        }

        removed
    }

    /// Get outgoing edges from an entity - O(1).
    pub fn outgoing_edges(&self, id: &EntityId) -> &[RelEdge<M>] {
        self.outgoing.get(id).map(|v| v.as_slice()).unwrap_or(&[])
    }

    /// Get incoming edges to an entity - O(1).
    pub fn incoming_edges(&self, id: &EntityId) -> &[RelEdge<M>] {
        self.incoming.get(id).map(|v| v.as_slice()).unwrap_or(&[])
    }

    /// Get out-degree (number of outgoing edges) - O(1).
    pub fn out_degree(&self, id: &EntityId) -> usize {
        self.outgoing.get(id).map(|v| v.len()).unwrap_or(0)
    }

    /// Get in-degree (number of incoming edges) - O(1).
    pub fn in_degree(&self, id: &EntityId) -> usize {
        self.incoming.get(id).map(|v| v.len()).unwrap_or(0)
    }

    /// Get total edge count.
    pub fn edge_count(&self) -> usize {
        self.outgoing.values().map(|v| v.len()).sum()
    }

    /// Iterate all edges in the topology.
    pub fn all_edges(&self) -> impl Iterator<Item = &RelEdge<M>> {
        self.outgoing.values().flat_map(|edges| edges.iter())
    }

    // ==================== Spatial Queries ====================

    /// Get entities in a specific voxel bucket - O(1).
    pub fn entities_in_voxel(&self, key: VoxelKey) -> Vec<&RelEntity> {
        self.voxel_grid
            .get_bucket(key)
            .iter()
            .filter_map(|id| self.entities.get(id))
            .collect()
    }

    /// Get entities at a specific position - O(1).
    pub fn entities_at(&self, position: &RelPoint3D) -> Vec<&RelEntity> {
        self.voxel_grid
            .entities_at(position)
            .iter()
            .filter_map(|id| self.entities.get(id))
            .collect()
    }

    /// Get entities in a Z-slice - O(1).
    pub fn entities_at_z(&self, z_bucket: usize) -> Vec<&RelEntity> {
        self.voxel_grid
            .get_z_slice(z_bucket)
            .iter()
            .filter_map(|id| self.entities.get(id))
            .collect()
    }

    /// Iterate entities in Z-order (ascending) - O(L) where L = 10 slices.
    ///
    /// This gives load/startup order for dependency topologies.
    pub fn z_ordered_iter(&self) -> impl Iterator<Item = &RelEntity> {
        self.voxel_grid
            .z_ordered_iter()
            .filter_map(|id| self.entities.get(id))
    }

    /// Iterate entities in reverse Z-order (descending) - O(L).
    ///
    /// This gives shutdown order for dependency topologies.
    pub fn z_ordered_iter_rev(&self) -> impl Iterator<Item = &RelEntity> {
        self.voxel_grid
            .z_ordered_iter_rev()
            .filter_map(|id| self.entities.get(id))
    }

    /// Get entities below a Z threshold (providers) - O(Z).
    pub fn entities_below_z(&self, z_bucket: usize) -> impl Iterator<Item = &RelEntity> {
        self.voxel_grid
            .entities_below_z(z_bucket)
            .filter_map(|id| self.entities.get(id))
    }

    /// Get entities above a Z threshold (dependents) - O(Z).
    pub fn entities_above_z(&self, z_bucket: usize) -> impl Iterator<Item = &RelEntity> {
        self.voxel_grid
            .entities_above_z(z_bucket)
            .filter_map(|id| self.entities.get(id))
    }

    /// Get entities near a position (in neighboring voxels).
    pub fn entities_near(&self, position: &RelPoint3D) -> Vec<&RelEntity> {
        self.voxel_grid
            .entities_near(position)
            .filter_map(|id| self.entities.get(id))
            .collect()
    }

    // ==================== Dependency-Specific Operations ====================

    /// Get the load order for all entities - O(L).
    ///
    /// Returns entities in dependency-safe order (providers first).
    pub fn load_order(&self) -> Vec<&EntityId> {
        self.voxel_grid.z_ordered_iter().collect()
    }

    /// Get the shutdown order for all entities - O(L).
    ///
    /// Returns entities in reverse dependency order (dependents first).
    pub fn shutdown_order(&self) -> Vec<&EntityId> {
        self.voxel_grid.z_ordered_iter_rev().collect()
    }

    /// Find potential providers for an entity (entities below its Z) - O(Z).
    pub fn find_providers(&self, id: &EntityId) -> Vec<&RelEntity> {
        let entity = match self.entities.get(id) {
            Some(e) => e,
            None => return Vec::new(),
        };

        let z_bucket = entity.position.z_slice(self.voxel_grid.grid_size());
        self.entities_below_z(z_bucket).collect()
    }

    /// Find potential dependents for an entity (entities above its Z) - O(Z).
    pub fn find_dependents(&self, id: &EntityId) -> Vec<&RelEntity> {
        let entity = match self.entities.get(id) {
            Some(e) => e,
            None => return Vec::new(),
        };

        let z_bucket = entity.position.z_slice(self.voxel_grid.grid_size());
        self.entities_above_z(z_bucket).collect()
    }

    /// Get direct dependencies (outgoing edges) for an entity.
    pub fn dependencies(&self, id: &EntityId) -> Vec<&EntityId> {
        self.outgoing_edges(id).iter().map(|e| &e.to).collect()
    }

    /// Get direct dependents (incoming edges) for an entity.
    pub fn dependents(&self, id: &EntityId) -> Vec<&EntityId> {
        self.incoming_edges(id).iter().map(|e| &e.from).collect()
    }

    // ==================== Statistics ====================

    /// Get voxel grid statistics.
    pub fn voxel_stats(&self) -> super::voxel::VoxelGridStats {
        self.voxel_grid.stats()
    }

    /// Get topology statistics.
    pub fn stats(&self) -> TopologyStats {
        let voxel_stats = self.voxel_grid.stats();

        TopologyStats {
            entity_count: self.entities.len(),
            edge_count: self.edge_count(),
            constraint_name: self.constraint.name().to_string(),
            voxel_stats,
        }
    }

    /// Clear all entities and edges.
    pub fn clear(&mut self) {
        self.entities.clear();
        self.voxel_grid.clear();
        self.outgoing.clear();
        self.incoming.clear();
    }
}

/// Topology statistics.
#[derive(Debug, Clone)]
pub struct TopologyStats {
    pub entity_count: usize,
    pub edge_count: usize,
    pub constraint_name: String,
    pub voxel_stats: super::voxel::VoxelGridStats,
}

/// Serializable topology data for JSON/ValKey export.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologyData {
    pub constraint: String,
    pub entity_count: usize,
    pub edge_count: usize,
    pub entities: Vec<EntityData>,
    pub edges: Vec<EdgeData>,
}

/// Serializable entity data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityData {
    pub id: EntityId,
    pub position: (f64, f64, f64),
    pub voxel_key: String,
    pub z_slice: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub domain: Option<String>,
}

/// Serializable edge data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeData {
    pub from: EntityId,
    pub to: EntityId,
    pub z_delta: f64,
    pub distance: f64,
}

impl<C: Constraint, M: EdgeMetadata> RelationalTopology<C, M> {
    /// Export topology to serializable format.
    pub fn to_data(&self) -> TopologyData {
        let entities: Vec<_> = self.entities.values().map(|e| {
            let (vx, vy, vz) = e.position.to_voxel_key(10);
            EntityData {
                id: e.id.clone(),
                position: e.position.to_f64_tuple(),
                voxel_key: format!("{}{}{}", vx, vy, vz),
                z_slice: e.position.z_slice(10),
                name: e.metadata.name.clone(),
                domain: e.metadata.domain.clone(),
            }
        }).collect();

        let edges: Vec<_> = self.outgoing.values()
            .flat_map(|edges| edges.iter())
            .map(|e| EdgeData {
                from: e.from.clone(),
                to: e.to.clone(),
                z_delta: e.z_delta.to_f64(),
                distance: e.distance().to_f64(),
            })
            .collect();

        TopologyData {
            constraint: self.constraint.name().to_string(),
            entity_count: self.entities.len(),
            edge_count: self.edge_count(),
            entities,
            edges,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relational_topology::constraint::DependencyConstraint;
    use crate::relational_topology::edge::DependencyMeta;

    type DepTopology = RelationalTopology<DependencyConstraint, DependencyMeta>;

    fn create_test_topology() -> DepTopology {
        RelationalTopology::new(DependencyConstraint::new())
    }

    #[test]
    fn test_register_entity() {
        let mut topo = create_test_topology();

        let entity = RelEntity::new("service_a", RelPoint3D::from_f64(0.5, 0.5, 0.5));
        let result = topo.register(entity);

        assert!(result.is_ok());
        assert_eq!(topo.entity_count(), 1);
        assert!(topo.contains(&"service_a".to_string()));
    }

    #[test]
    fn test_valid_dependency_edge() {
        let mut topo = create_test_topology();

        // Provider at low Z, dependent at high Z
        let provider = RelEntity::new("database", RelPoint3D::from_f64(0.5, 0.5, 0.2));
        let dependent = RelEntity::new("api", RelPoint3D::from_f64(0.5, 0.5, 0.8));

        topo.register(provider).unwrap();
        topo.register(dependent).unwrap();

        // api → database (high Z → low Z) should be valid
        let result = topo.add_edge(
            "api".to_string(),
            "database".to_string(),
            DependencyMeta::hard(),
        );

        assert!(result.is_ok());
        assert_eq!(topo.edge_count(), 1);
    }

    #[test]
    fn test_invalid_dependency_edge() {
        let mut topo = create_test_topology();

        let provider = RelEntity::new("database", RelPoint3D::from_f64(0.5, 0.5, 0.2));
        let dependent = RelEntity::new("api", RelPoint3D::from_f64(0.5, 0.5, 0.8));

        topo.register(provider).unwrap();
        topo.register(dependent).unwrap();

        // database → api (low Z → high Z) should be INVALID
        let result = topo.add_edge(
            "database".to_string(),
            "api".to_string(),
            DependencyMeta::hard(),
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_load_order() {
        let mut topo = create_test_topology();

        // Register entities at different Z levels
        topo.register(RelEntity::new("foundation", RelPoint3D::from_f64(0.5, 0.5, 0.1))).unwrap();
        topo.register(RelEntity::new("middleware", RelPoint3D::from_f64(0.5, 0.5, 0.5))).unwrap();
        topo.register(RelEntity::new("application", RelPoint3D::from_f64(0.5, 0.5, 0.9))).unwrap();

        let order = topo.load_order();

        // Load order should be: foundation, middleware, application
        assert_eq!(order.len(), 3);
        assert_eq!(order[0], "foundation");
        assert_eq!(order[1], "middleware");
        assert_eq!(order[2], "application");
    }

    #[test]
    fn test_find_providers() {
        let mut topo = create_test_topology();

        topo.register(RelEntity::new("database", RelPoint3D::from_f64(0.5, 0.5, 0.1))).unwrap();
        topo.register(RelEntity::new("cache", RelPoint3D::from_f64(0.5, 0.5, 0.2))).unwrap();
        topo.register(RelEntity::new("api", RelPoint3D::from_f64(0.5, 0.5, 0.8))).unwrap();

        let providers = topo.find_providers(&"api".to_string());

        // database and cache are below api
        assert_eq!(providers.len(), 2);
    }

    #[test]
    fn test_cycle_impossible() {
        let mut topo = create_test_topology();

        let a = RelEntity::new("a", RelPoint3D::from_f64(0.5, 0.5, 0.5));
        let b = RelEntity::new("b", RelPoint3D::from_f64(0.5, 0.5, 0.3));

        topo.register(a).unwrap();
        topo.register(b).unwrap();

        // a → b is valid (0.5 > 0.3)
        assert!(topo.add_edge("a".to_string(), "b".to_string(), DependencyMeta::hard()).is_ok());

        // b → a would create a cycle, but it's invalid (0.3 < 0.5)
        assert!(topo.add_edge("b".to_string(), "a".to_string(), DependencyMeta::hard()).is_err());

        // Therefore, cycles are geometrically impossible!
    }

    #[test]
    fn test_deregister() {
        let mut topo = create_test_topology();

        let a = RelEntity::new("a", RelPoint3D::from_f64(0.5, 0.5, 0.8));
        let b = RelEntity::new("b", RelPoint3D::from_f64(0.5, 0.5, 0.2));

        topo.register(a).unwrap();
        topo.register(b).unwrap();
        topo.add_edge("a".to_string(), "b".to_string(), DependencyMeta::hard()).unwrap();

        assert_eq!(topo.entity_count(), 2);
        assert_eq!(topo.edge_count(), 1);

        // Deregister 'a'
        let removed = topo.deregister(&"a".to_string());
        assert!(removed.is_some());
        assert_eq!(topo.entity_count(), 1);
        assert_eq!(topo.edge_count(), 0); // Edge should be removed too
    }
}
