//! Edge types for relational topology.
//!
//! Edges are stored as directed vectors from source to target,
//! allowing geometric operations on relationships.

use super::point::{RelPoint3D, RelVector3D};
use super::topology::EntityId;
use crate::geometric_precision::FixedPoint;
use serde::{Deserialize, Serialize};

/// Trait for edge metadata types.
pub trait EdgeMetadata: Clone + Send + Sync + Default {}

/// A directed edge in relational space.
///
/// The edge stores both endpoint IDs and the geometric vector
/// from source to target, enabling spatial operations on relationships.
#[derive(Clone, Debug)]
pub struct RelEdge<M: EdgeMetadata> {
    /// Source entity ID.
    pub from: EntityId,

    /// Target entity ID.
    pub to: EntityId,

    /// Geometric vector from source to target position.
    /// Computed as: `to.position - from.position`
    pub vector: RelVector3D,

    /// Z-delta: `from.z - to.z` (positive = downward edge).
    /// For dependency edges, this should always be positive.
    pub z_delta: FixedPoint,

    /// Edge-specific metadata (e.g., dependency type, protocol info).
    pub metadata: M,
}

impl<M: EdgeMetadata> RelEdge<M> {
    /// Create a new edge from positions.
    pub fn new(from: EntityId, to: EntityId, from_pos: &RelPoint3D, to_pos: &RelPoint3D, metadata: M) -> Self {
        let vector = RelVector3D::from_points(from_pos, to_pos);
        let z_delta = from_pos.z - to_pos.z;

        Self {
            from,
            to,
            vector,
            z_delta,
            metadata,
        }
    }

    /// Check if this edge points "downward" (valid for dependencies).
    ///
    /// Returns true if `z_delta > 0`, meaning `from.z > to.z`.
    pub fn is_downward(&self) -> bool {
        self.z_delta > FixedPoint::from_int(0)
    }

    /// Get the geometric distance of this edge.
    pub fn distance(&self) -> FixedPoint {
        self.vector.magnitude()
    }

    /// Check if this edge connects to the given entity.
    pub fn involves(&self, id: &EntityId) -> bool {
        &self.from == id || &self.to == id
    }

    /// Get the "other" entity given one endpoint.
    pub fn other(&self, id: &EntityId) -> Option<&EntityId> {
        if &self.from == id {
            Some(&self.to)
        } else if &self.to == id {
            Some(&self.from)
        } else {
            None
        }
    }
}

/// Dependency type for service dependencies.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum DependencyType {
    /// Required for operation - service cannot start without it.
    #[default]
    Hard,

    /// Preferred but not required - graceful degradation possible.
    Soft,

    /// Optional feature - service works without it.
    Optional,

    /// Development/testing only - not used in production.
    DevOnly,
}

impl DependencyType {
    /// Check if this dependency is required for service operation.
    pub fn is_required(&self) -> bool {
        matches!(self, Self::Hard)
    }

    /// Check if this dependency should be validated at startup.
    pub fn validate_at_startup(&self) -> bool {
        matches!(self, Self::Hard | Self::Soft)
    }
}

/// Metadata for dependency edges.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DependencyMeta {
    /// Type of dependency relationship.
    pub dep_type: DependencyType,

    /// Optional version constraint (e.g., ">=1.0.0").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version_constraint: Option<String>,

    /// Optional capability requirement.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capability: Option<String>,

    /// Human-readable reason for dependency.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl EdgeMetadata for DependencyMeta {}

impl DependencyMeta {
    /// Create a hard dependency.
    pub fn hard() -> Self {
        Self {
            dep_type: DependencyType::Hard,
            ..Default::default()
        }
    }

    /// Create a soft dependency.
    pub fn soft() -> Self {
        Self {
            dep_type: DependencyType::Soft,
            ..Default::default()
        }
    }

    /// Create an optional dependency.
    pub fn optional() -> Self {
        Self {
            dep_type: DependencyType::Optional,
            ..Default::default()
        }
    }

    /// Add a version constraint.
    pub fn with_version(mut self, constraint: impl Into<String>) -> Self {
        self.version_constraint = Some(constraint.into());
        self
    }

    /// Add a capability requirement.
    pub fn with_capability(mut self, cap: impl Into<String>) -> Self {
        self.capability = Some(cap.into());
        self
    }

    /// Add a reason.
    pub fn with_reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }
}

/// Metadata for communication edges.
///
/// Re-exported from `relational_topology` (mod.rs) — see GN-D1.02 closure
/// (Tier-2 commit 2.1.a): the type is exercised in `relational_topology::tests`
/// to demonstrate HTTP/gRPC/WebSocket trust-boundary semantics.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CommunicationMeta {
    /// Communication protocol (http, grpc, websocket, amqp, etc.).
    pub protocol: String,

    /// Expected latency in milliseconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<u32>,

    /// Bandwidth class (low, medium, high).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bandwidth: Option<String>,

    /// Whether the connection is encrypted.
    #[serde(default)]
    pub encrypted: bool,

    /// Port number if applicable.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
}

impl EdgeMetadata for CommunicationMeta {}

impl CommunicationMeta {
    /// Create HTTP communication metadata.
    pub fn http(encrypted: bool) -> Self {
        Self {
            protocol: if encrypted { "https" } else { "http" }.to_string(),
            encrypted,
            port: Some(if encrypted { 443 } else { 80 }),
            ..Default::default()
        }
    }

    /// Create gRPC communication metadata.
    pub fn grpc(encrypted: bool) -> Self {
        Self {
            protocol: "grpc".to_string(),
            encrypted,
            ..Default::default()
        }
    }

    /// Create WebSocket communication metadata.
    pub fn websocket(encrypted: bool) -> Self {
        Self {
            protocol: if encrypted { "wss" } else { "ws" }.to_string(),
            encrypted,
            ..Default::default()
        }
    }

    /// Add latency expectation.
    pub fn with_latency(mut self, ms: u32) -> Self {
        self.latency_ms = Some(ms);
        self
    }

    /// Add port.
    pub fn with_port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }
}

/// Empty metadata for generic edges.
///
/// Re-exported from `relational_topology` (mod.rs) — see GN-D1.02 closure.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct EmptyMeta;

impl EdgeMetadata for EmptyMeta {}

/// Serializable edge representation for JSON/ValKey storage.
///
/// Re-exported from `relational_topology` (mod.rs) — see GN-D1.02 closure.
/// Used in tests to verify edge → JSON round-trip; production-side
/// serialization happens via the `RelEdge` direct path.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RelEdgeData<M: EdgeMetadata + Serialize> {
    pub from: EntityId,
    pub to: EntityId,
    pub vector: (f64, f64, f64),
    pub z_delta: f64,
    pub distance: f64,
    pub metadata: M,
}

impl<M: EdgeMetadata + Serialize> From<&RelEdge<M>> for RelEdgeData<M> {
    fn from(edge: &RelEdge<M>) -> Self {
        Self {
            from: edge.from.clone(),
            to: edge.to.clone(),
            vector: edge.vector.to_f64_tuple(),
            z_delta: edge.z_delta.to_f64(),
            distance: edge.distance().to_f64(),
            metadata: edge.metadata.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_edge_creation() {
        let from_pos = RelPoint3D::from_f64(0.5, 0.5, 0.8);
        let to_pos = RelPoint3D::from_f64(0.5, 0.5, 0.2);

        let edge = RelEdge::new(
            "dependent".to_string(),
            "provider".to_string(),
            &from_pos,
            &to_pos,
            DependencyMeta::hard(),
        );

        assert!(edge.is_downward());
        assert!((edge.z_delta.to_f64() - 0.6).abs() < 0.001);
    }

    #[test]
    fn test_edge_involves() {
        let from_pos = RelPoint3D::from_f64(0.0, 0.0, 1.0);
        let to_pos = RelPoint3D::from_f64(0.0, 0.0, 0.0);

        let edge = RelEdge::new(
            "a".to_string(),
            "b".to_string(),
            &from_pos,
            &to_pos,
            EmptyMeta,
        );

        assert!(edge.involves(&"a".to_string()));
        assert!(edge.involves(&"b".to_string()));
        assert!(!edge.involves(&"c".to_string()));
    }

    #[test]
    fn test_dependency_meta() {
        let meta = DependencyMeta::hard()
            .with_version(">=1.0.0")
            .with_capability("database")
            .with_reason("Core data storage");

        assert_eq!(meta.dep_type, DependencyType::Hard);
        assert_eq!(meta.version_constraint, Some(">=1.0.0".to_string()));
        assert_eq!(meta.capability, Some("database".to_string()));
    }

    #[test]
    fn test_communication_meta() {
        let meta = CommunicationMeta::grpc(true).with_latency(10).with_port(9090);

        assert_eq!(meta.protocol, "grpc");
        assert!(meta.encrypted);
        assert_eq!(meta.latency_ms, Some(10));
        assert_eq!(meta.port, Some(9090));
    }
}
