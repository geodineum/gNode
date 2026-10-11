//! Fixed-point arithmetic re-exports from g_math crate.
//!
//! This module was originally 1,028 LOC of hand-rolled Q32.32 arithmetic.
//! It now re-exports from g_math (Q64.64, 18 ULP-validated transcendentals).
//!
//! All `use crate::geometric_precision::*` import paths are preserved.
//! All arithmetic, transcendental, and vector operations are provided by g_math directly.

pub use g_math::fixed_point::{FixedPoint, FixedVector, FixedMatrix};

/// A point in Q64.64, converted from f64 directly (an f32 step would drop precision).
fn to_fixed(point: &[f64]) -> FixedVector {
    FixedVector::from_slice(&point.iter().map(|&v| FixedPoint::from_f64(v)).collect::<Vec<_>>())
}

/// Euclidean distance in Q64.64; `f64::MAX` when the dimension counts differ.
pub fn fixed_distance(point1: &[f64], point2: &[f64]) -> f64 {
    if point1.len() != point2.len() {
        return f64::MAX;
    }
    to_fixed(point1).distance_to(&to_fixed(point2)).to_f64()
}

#[cfg(test)]
mod distance_tests {
    use super::*;

    #[test]
    fn the_distance_is_deterministic_and_commutative() {
        let a = [0.1, 0.5, 0.9];
        let b = [0.4, 0.5, 0.1];
        let d1 = fixed_distance(&a, &b);
        assert_eq!(d1, fixed_distance(&a, &b));
        assert_eq!(d1, fixed_distance(&b, &a));
        assert!((d1 - (0.09f64 + 0.64).sqrt()).abs() < 1e-9);
        assert_eq!(fixed_distance(&a, &a), 0.0);
    }

    #[test]
    fn a_dimension_mismatch_is_infinitely_far() {
        assert_eq!(fixed_distance(&[0.1, 0.2], &[0.1]), f64::MAX);
    }
}
