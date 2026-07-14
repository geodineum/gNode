//! Constraint traits for topology-specific edge validation.
//!
//! Each topology type defines constraints that determine which edges are valid.
//! For dependency topologies, the key constraint is Z-monotonicity which
//! makes cycles geometrically impossible.

use super::topology::{EntityId, RelEntity};
use crate::geometric_precision::FixedPoint;
use thiserror::Error;

/// Error type for constraint violations.
#[derive(Debug, Error)]
pub enum ConstraintError {
    /// Edge would create a cycle (dependency constraint).
    #[error("Cycle violation: {from} → {to} would create cycle (Z: {from_z:.3} → {to_z:.3})")]
    CycleViolation {
        from: EntityId,
        to: EntityId,
        from_z: f64,
        to_z: f64,
    },

    /// Edge violates topology-specific constraint.
    #[error("Constraint violation: {reason}")]
    ConstraintViolation { reason: String },

    /// Entity not found.
    #[error("Entity not found: {0}")]
    EntityNotFound(EntityId),

    /// Self-reference not allowed.
    #[error("Self-reference not allowed: {0}")]
    SelfReference(EntityId),
}

/// Trait for topology-specific edge constraints.
///
/// Implement this trait to define custom validation logic for edges.
/// The constraint determines which relationships are valid in the topology.
pub trait Constraint: Send + Sync {
    /// Validate whether an edge from `from` to `to` is allowed.
    ///
    /// Returns `Ok(())` if the edge is valid, or an error describing the violation.
    fn validate_edge(&self, from: &RelEntity, to: &RelEntity) -> Result<(), ConstraintError>;

    /// Get a descriptive name for this constraint type.
    fn name(&self) -> &'static str;
}

/// Dependency constraint: edges must point "downward" in Z.
///
/// # Invariant
///
/// For every edge A → B: `Z(B) < Z(A)`
///
/// This ensures that dependencies always point from higher Z (dependents)
/// to lower Z (providers), making cycles geometrically impossible.
///
/// # Proof of Cycle Impossibility
///
/// 1. If A → B is valid: Z(A) > Z(B)
/// 2. If B → A is valid: Z(B) > Z(A)
/// 3. Both cannot be true simultaneously (contradiction)
/// 4. Therefore, no bidirectional edges exist
/// 5. By induction, no cycles of any length can exist
#[derive(Debug, Clone)]
pub struct DependencyConstraint {
    /// Minimum Z-delta required for valid edges (prevents near-equal Z conflicts).
    /// Default: 0 (any positive delta is valid).
    min_z_delta: FixedPoint,
}

impl Default for DependencyConstraint {
    fn default() -> Self {
        Self {
            min_z_delta: FixedPoint::from_int(0),
        }
    }
}

impl DependencyConstraint {
    /// Create a new dependency constraint with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create with a minimum Z-delta requirement.
    ///
    /// This ensures providers are at least `delta` below dependents,
    /// providing some "breathing room" in the hierarchy.
    pub fn with_min_delta(delta: f64) -> Self {
        Self {
            min_z_delta: FixedPoint::from_f64(delta),
        }
    }

    /// Check if a Z-relationship satisfies the constraint.
    ///
    /// Returns true if `from_z > to_z + min_z_delta`.
    pub fn is_valid_z_relationship(&self, from_z: FixedPoint, to_z: FixedPoint) -> bool {
        from_z > to_z + self.min_z_delta
    }
}

impl Constraint for DependencyConstraint {
    fn validate_edge(&self, from: &RelEntity, to: &RelEntity) -> Result<(), ConstraintError> {
        // Check self-reference
        if from.id == to.id {
            return Err(ConstraintError::SelfReference(from.id.clone()));
        }

        // Check Z-monotonicity: from.z > to.z
        if !self.is_valid_z_relationship(from.position.z, to.position.z) {
            return Err(ConstraintError::CycleViolation {
                from: from.id.clone(),
                to: to.id.clone(),
                from_z: from.position.z.to_f64(),
                to_z: to.position.z.to_f64(),
            });
        }

        Ok(())
    }

    fn name(&self) -> &'static str {
        "DependencyConstraint"
    }
}

/// Communication constraint: allows bidirectional or directed edges.
///
/// Unlike dependency constraints, communication edges can be bidirectional.
/// The constraint validates based on protocol compatibility and trust levels.
#[derive(Debug, Clone, Default)]
pub struct CommunicationConstraint {
    /// If true, edges are automatically bidirectional.
    pub bidirectional: bool,

    /// Maximum Z-delta allowed (for trust boundary validation).
    /// None = no limit.
    pub max_trust_delta: Option<FixedPoint>,
}

impl CommunicationConstraint {
    /// Create a new communication constraint.
    pub fn new(bidirectional: bool) -> Self {
        Self {
            bidirectional,
            max_trust_delta: None,
        }
    }

    /// Create with trust boundary checking.
    pub fn with_trust_boundary(bidirectional: bool, max_delta: f64) -> Self {
        Self {
            bidirectional,
            max_trust_delta: Some(FixedPoint::from_f64(max_delta)),
        }
    }
}

impl Constraint for CommunicationConstraint {
    fn validate_edge(&self, from: &RelEntity, to: &RelEntity) -> Result<(), ConstraintError> {
        // Check self-reference
        if from.id == to.id {
            return Err(ConstraintError::SelfReference(from.id.clone()));
        }

        // Check trust boundary if configured
        if let Some(max_delta) = self.max_trust_delta {
            let z_diff = (from.position.z - to.position.z).abs();
            if z_diff > max_delta {
                return Err(ConstraintError::ConstraintViolation {
                    reason: format!(
                        "Trust boundary exceeded: {} → {} crosses {:.2} Z levels (max: {:.2})",
                        from.id,
                        to.id,
                        z_diff.to_f64(),
                        max_delta.to_f64()
                    ),
                });
            }
        }

        Ok(())
    }

    fn name(&self) -> &'static str {
        "CommunicationConstraint"
    }
}

/// No-constraint: allows any edge (for general graph topologies).
#[derive(Debug, Clone, Default)]
#[allow(dead_code)]
pub struct NoConstraint;

impl Constraint for NoConstraint {
    fn validate_edge(&self, from: &RelEntity, to: &RelEntity) -> Result<(), ConstraintError> {
        // Only prevent self-references
        if from.id == to.id {
            return Err(ConstraintError::SelfReference(from.id.clone()));
        }
        Ok(())
    }

    fn name(&self) -> &'static str {
        "NoConstraint"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::point::RelPoint3D;

    fn make_entity(id: &str, z: f64) -> RelEntity {
        RelEntity {
            id: id.to_string(),
            position: RelPoint3D::from_f64(0.5, 0.5, z),
            metadata: Default::default(),
        }
    }

    #[test]
    fn test_dependency_constraint_valid() {
        let constraint = DependencyConstraint::new();
        let provider = make_entity("provider", 0.2);
        let dependent = make_entity("dependent", 0.8);

        // dependent → provider should be valid (0.8 > 0.2)
        assert!(constraint.validate_edge(&dependent, &provider).is_ok());
    }

    #[test]
    fn test_dependency_constraint_invalid() {
        let constraint = DependencyConstraint::new();
        let provider = make_entity("provider", 0.2);
        let dependent = make_entity("dependent", 0.8);

        // provider → dependent should be invalid (0.2 < 0.8)
        let result = constraint.validate_edge(&provider, &dependent);
        assert!(matches!(result, Err(ConstraintError::CycleViolation { .. })));
    }

    #[test]
    fn test_dependency_constraint_self_reference() {
        let constraint = DependencyConstraint::new();
        let entity = make_entity("service", 0.5);

        let result = constraint.validate_edge(&entity, &entity);
        assert!(matches!(result, Err(ConstraintError::SelfReference(_))));
    }

    #[test]
    fn test_dependency_constraint_min_delta() {
        let constraint = DependencyConstraint::with_min_delta(0.1);
        let a = make_entity("a", 0.55);
        let b = make_entity("b", 0.50);

        // 0.55 - 0.50 = 0.05 < 0.1 (min_delta), should fail
        let result = constraint.validate_edge(&a, &b);
        assert!(result.is_err());

        // With larger gap
        let c = make_entity("c", 0.35);
        // 0.55 - 0.35 = 0.2 > 0.1, should pass
        assert!(constraint.validate_edge(&a, &c).is_ok());
    }

    #[test]
    fn test_communication_constraint_bidirectional() {
        let constraint = CommunicationConstraint::new(true);
        let a = make_entity("a", 0.3);
        let b = make_entity("b", 0.7);

        // Both directions should be valid
        assert!(constraint.validate_edge(&a, &b).is_ok());
        assert!(constraint.validate_edge(&b, &a).is_ok());
    }

    #[test]
    fn test_communication_trust_boundary() {
        let constraint = CommunicationConstraint::with_trust_boundary(true, 0.3);
        let internal = make_entity("internal", 0.8);
        let external = make_entity("external", 0.2);

        // Z-diff = 0.6 > 0.3 max_delta, should fail
        let result = constraint.validate_edge(&internal, &external);
        assert!(matches!(
            result,
            Err(ConstraintError::ConstraintViolation { .. })
        ));

        // Within boundary
        let nearby = make_entity("nearby", 0.6);
        assert!(constraint.validate_edge(&internal, &nearby).is_ok());
    }

    #[test]
    fn test_no_constraint() {
        let constraint = NoConstraint;
        let a = make_entity("a", 0.1);
        let b = make_entity("b", 0.9);

        // Any edge should be valid
        assert!(constraint.validate_edge(&a, &b).is_ok());
        assert!(constraint.validate_edge(&b, &a).is_ok());
    }
}
