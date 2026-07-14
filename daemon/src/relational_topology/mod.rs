//! Relational Topology Engine
//!
//! A generic 3D relational topology engine that transforms graph problems into
//! spatial problems. The key insight is that DAGs can be encoded as geometry
//! where Z-axis monotonicity makes cycles geometrically impossible.
//!
//! # Architecture
//!
//! - **RelPoint3D**: 3D coordinates using Q64.64 fixed-point for determinism
//! - **VoxelGrid**: O(1) spatial lookups via 10×10×10 grid
//! - **ZSliceIndex**: O(L) ordered iteration where L = 10 slices
//! - **Constraint**: Trait for topology-specific edge validation
//!
//! # Use Cases
//!
//! - **Dependency Topology**: Services with Z-ordering (provider.z < dependent.z)
//! - **Communication Topology**: Protocol/latency/trust relationships
//! - **Social/Org Topology**: Hierarchy and connection graphs
//!
//! # Performance Guarantees
//!
//! | Operation         | Complexity |
//! |-------------------|------------|
//! | Register entity   | O(D)       |
//! | Validate edge     | O(1)       |
//! | Cycle detection   | O(1)       |
//! | Load order        | O(L)       |
//! | Find in voxel     | O(1)       |

mod point;
mod voxel;
mod constraint;
mod edge;
mod topology;
mod dependency;

// Re-exports
pub use point::{RelPoint3D, RelVector3D};
pub use voxel::{VoxelGrid, VoxelKey};
pub use constraint::{Constraint, ConstraintError, DependencyConstraint, CommunicationConstraint};
pub use edge::{RelEdge, EdgeMetadata, DependencyMeta, DependencyType};
// Communication-edge surface + generic empty metadata + serializable
// edge form. Currently exercised by relational_topology/tests.rs to
// demonstrate the cross-protocol surface (HTTP / gRPC / WebSocket) and
// the serialization round-trip. Re-exported here so they form part of
// the documented module API rather than tests-only-via-module-path
// access (GN-D1.02 close: "re-export with documented use case").
pub use edge::{CommunicationMeta, EmptyMeta, RelEdgeData};
pub use topology::{RelationalTopology, RelEntity, EntityId, EntityMetadata};

// Dependency topology specialization
pub use dependency::{
    DependencyTopology, DependencyTopologyExt, DependencyError,
    ServiceDefinition, DependencySpec, RegistrationResult, MissingDependency,
    FunctionalDomain, OperationalScope,
    create_dependency_topology, create_dependency_topology_with_delta,
};

#[cfg(test)]
mod tests;
