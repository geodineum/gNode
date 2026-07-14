//! Fixed-point arithmetic re-exports from g_math crate.
//!
//! This module was originally 1,028 LOC of hand-rolled Q32.32 arithmetic.
//! It now re-exports from g_math (Q64.64, 18 ULP-validated transcendentals).
//!
//! All `use crate::geometric_precision::*` import paths are preserved.
//! All arithmetic, transcendental, and vector operations are provided by g_math directly.

pub use g_math::fixed_point::{FixedPoint, FixedVector, FixedMatrix};
