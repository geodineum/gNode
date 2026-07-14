//! 3D Point and Vector types for relational topology.
//!
//! Uses Q64.64 fixed-point arithmetic for multi-node determinism.

use crate::geometric_precision::FixedPoint;
use serde::{Deserialize, Serialize};
use std::ops::{Add, Sub};

/// 3D point in relational space using Q64.64 fixed-point coordinates.
///
/// # Axis Semantics (configurable by topology type)
///
/// - **X-axis**: Primary clustering dimension (e.g., functional domain)
/// - **Y-axis**: Secondary clustering dimension (e.g., operational scope)
/// - **Z-axis**: Hierarchy/ordering dimension (e.g., dependency level)
///
/// # Invariants
///
/// For dependency topologies: `∀ edge(A→B): Z(B) < Z(A)`
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RelPoint3D {
    pub x: FixedPoint,
    pub y: FixedPoint,
    pub z: FixedPoint,
}

impl RelPoint3D {
    /// Create a new 3D point from Q64.64 fixed-point values.
    pub fn new(x: FixedPoint, y: FixedPoint, z: FixedPoint) -> Self {
        Self { x, y, z }
    }

    /// Create a point from f64 values (convenience method).
    pub fn from_f64(x: f64, y: f64, z: f64) -> Self {
        Self {
            x: FixedPoint::from_f64(x),
            y: FixedPoint::from_f64(y),
            z: FixedPoint::from_f64(z),
        }
    }

    /// Create origin point (0, 0, 0).
    pub fn origin() -> Self {
        Self {
            x: FixedPoint::from_int(0),
            y: FixedPoint::from_int(0),
            z: FixedPoint::from_int(0),
        }
    }

    /// Convert to f64 tuple for serialization/display.
    pub fn to_f64_tuple(&self) -> (f64, f64, f64) {
        (self.x.to_f64(), self.y.to_f64(), self.z.to_f64())
    }

    /// Compute the voxel key for this point using Q64.64 arithmetic.
    ///
    /// Voxel key is computed as: `(floor(x*grid_size), floor(y*grid_size), floor(z*grid_size))`
    ///
    /// This is deterministic across all nodes due to Q64.64 fixed-point.
    pub fn to_voxel_key(&self, grid_size: i32) -> (u8, u8, u8) {
        let grid = FixedPoint::from_int(grid_size);

        // Clamp to [0, 1] range before computing bucket
        let clamp = |v: FixedPoint| -> u8 {
            let scaled = (v * grid).to_int();
            scaled.clamp(0, grid_size - 1) as u8
        };

        (clamp(self.x), clamp(self.y), clamp(self.z))
    }

    /// Get the Z-slice bucket index (0-9 for grid_size=10).
    pub fn z_slice(&self, grid_size: i32) -> usize {
        let grid = FixedPoint::from_int(grid_size);
        let z_scaled = (self.z * grid).to_int();
        z_scaled.clamp(0, grid_size - 1) as usize
    }

    /// Compute Euclidean distance to another point using Q64.64.
    pub fn distance_to(&self, other: &RelPoint3D) -> FixedPoint {
        let dx = self.x - other.x;
        let dy = self.y - other.y;
        let dz = self.z - other.z;

        let sum_sq = dx * dx + dy * dy + dz * dz;
        sum_sq.sqrt()
    }

    /// Compute squared distance (avoids sqrt, useful for comparisons).
    pub fn distance_squared(&self, other: &RelPoint3D) -> FixedPoint {
        let dx = self.x - other.x;
        let dy = self.y - other.y;
        let dz = self.z - other.z;

        dx * dx + dy * dy + dz * dz
    }
}

impl Default for RelPoint3D {
    fn default() -> Self {
        Self::origin()
    }
}

/// 3D vector in relational space (used for edge representation).
///
/// An edge from A to B is stored as vector `B.pos - A.pos`, allowing
/// geometric operations on relationships.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RelVector3D {
    pub dx: FixedPoint,
    pub dy: FixedPoint,
    pub dz: FixedPoint,
}

impl RelVector3D {
    /// Create a new vector.
    pub fn new(dx: FixedPoint, dy: FixedPoint, dz: FixedPoint) -> Self {
        Self { dx, dy, dz }
    }

    /// Create a zero vector.
    pub fn zero() -> Self {
        Self {
            dx: FixedPoint::from_int(0),
            dy: FixedPoint::from_int(0),
            dz: FixedPoint::from_int(0),
        }
    }

    /// Compute vector from point A to point B.
    pub fn from_points(from: &RelPoint3D, to: &RelPoint3D) -> Self {
        Self {
            dx: to.x - from.x,
            dy: to.y - from.y,
            dz: to.z - from.z,
        }
    }

    /// Compute the magnitude (length) of the vector.
    pub fn magnitude(&self) -> FixedPoint {
        let sum_sq = self.dx * self.dx + self.dy * self.dy + self.dz * self.dz;
        sum_sq.sqrt()
    }

    /// Compute squared magnitude (avoids sqrt).
    pub fn magnitude_squared(&self) -> FixedPoint {
        self.dx * self.dx + self.dy * self.dy + self.dz * self.dz
    }

    /// Compute dot product with another vector.
    pub fn dot(&self, other: &RelVector3D) -> FixedPoint {
        self.dx * other.dx + self.dy * other.dy + self.dz * other.dz
    }

    /// Get the Z component (important for dependency ordering).
    pub fn z_delta(&self) -> FixedPoint {
        self.dz
    }

    /// Check if vector points "downward" in Z (required for dependencies).
    ///
    /// For a valid dependency edge A→B, we need Z(B) < Z(A),
    /// which means dz = Z(B) - Z(A) < 0.
    pub fn is_downward(&self) -> bool {
        self.dz < FixedPoint::from_int(0)
    }

    /// Convert to f64 tuple.
    pub fn to_f64_tuple(&self) -> (f64, f64, f64) {
        (self.dx.to_f64(), self.dy.to_f64(), self.dz.to_f64())
    }
}

impl Default for RelVector3D {
    fn default() -> Self {
        Self::zero()
    }
}

// Vector arithmetic
impl Add for RelVector3D {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self {
            dx: self.dx + other.dx,
            dy: self.dy + other.dy,
            dz: self.dz + other.dz,
        }
    }
}

impl Sub for RelVector3D {
    type Output = Self;

    fn sub(self, other: Self) -> Self {
        Self {
            dx: self.dx - other.dx,
            dy: self.dy - other.dy,
            dz: self.dz - other.dz,
        }
    }
}

// Point + Vector = Point
impl Add<RelVector3D> for RelPoint3D {
    type Output = Self;

    fn add(self, v: RelVector3D) -> Self {
        Self {
            x: self.x + v.dx,
            y: self.y + v.dy,
            z: self.z + v.dz,
        }
    }
}

// Point - Point = Vector
impl Sub for RelPoint3D {
    type Output = RelVector3D;

    fn sub(self, other: Self) -> RelVector3D {
        RelVector3D {
            dx: self.x - other.x,
            dy: self.y - other.y,
            dz: self.z - other.z,
        }
    }
}

/// Serializable representation of RelPoint3D for JSON/ValKey storage.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct RelPoint3DData {
    pub x: f64,
    pub y: f64,
    pub z: f64,
    /// Pre-computed voxel key for O(1) Lua lookups.
    pub voxel_key: String,
    /// Z-slice bucket for O(L) ordering.
    pub z_slice: usize,
}

impl From<&RelPoint3D> for RelPoint3DData {
    fn from(p: &RelPoint3D) -> Self {
        let (vx, vy, vz) = p.to_voxel_key(10);
        Self {
            x: p.x.to_f64(),
            y: p.y.to_f64(),
            z: p.z.to_f64(),
            voxel_key: format!("{:01}{:01}{:01}", vx, vy, vz),
            z_slice: p.z_slice(10),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_point_creation() {
        let p = RelPoint3D::from_f64(0.5, 0.3, 0.8);
        let (x, y, z) = p.to_f64_tuple();
        assert!((x - 0.5).abs() < 0.001);
        assert!((y - 0.3).abs() < 0.001);
        assert!((z - 0.8).abs() < 0.001);
    }

    #[test]
    fn test_voxel_key_determinism() {
        let p1 = RelPoint3D::from_f64(0.55, 0.32, 0.87);
        let p2 = RelPoint3D::from_f64(0.55, 0.32, 0.87);

        assert_eq!(p1.to_voxel_key(10), p2.to_voxel_key(10));
        assert_eq!(p1.to_voxel_key(10), (5, 3, 8));
    }

    #[test]
    fn test_z_slice() {
        let p = RelPoint3D::from_f64(0.5, 0.5, 0.75);
        assert_eq!(p.z_slice(10), 7);

        let p2 = RelPoint3D::from_f64(0.5, 0.5, 0.0);
        assert_eq!(p2.z_slice(10), 0);
    }

    #[test]
    fn test_vector_from_points() {
        let a = RelPoint3D::from_f64(0.0, 0.0, 1.0);
        let b = RelPoint3D::from_f64(0.0, 0.0, 0.5);

        let v = RelVector3D::from_points(&a, &b);
        assert!(v.is_downward()); // b.z < a.z, so dz < 0
        assert!(v.dz.to_f64() < 0.0);
    }

    #[test]
    fn test_distance() {
        let a = RelPoint3D::from_f64(0.0, 0.0, 0.0);
        let b = RelPoint3D::from_f64(1.0, 0.0, 0.0);

        let dist = a.distance_to(&b);
        assert!((dist.to_f64() - 1.0).abs() < 0.001);
    }

    #[test]
    fn test_point_vector_arithmetic() {
        let p = RelPoint3D::from_f64(0.5, 0.5, 0.5);
        let v = RelVector3D::new(
            FixedPoint::from_f64(0.1),
            FixedPoint::from_f64(0.0),
            FixedPoint::from_f64(-0.2),
        );

        let result = p + v;
        assert!((result.x.to_f64() - 0.6).abs() < 0.001);
        assert!((result.z.to_f64() - 0.3).abs() < 0.001);
    }
}
