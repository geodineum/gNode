# Determinism Guarantee

gNode performs all geometric operations in **fixed-point** arithmetic — no
floating point anywhere on the topology path. This guarantees bit-identical
results across nodes, platforms, and architectures.

## Why Fixed-Point

Floating-point arithmetic is non-deterministic across platforms: different CPUs,
optimization levels, and rounding modes produce different results. For
distributed service discovery, all nodes must agree on distances, bucket
assignments, and load order. Different results = different service selections =
broken routing.

Fixed-point uses pure integer arithmetic. Integer operations produce identical
results on every platform, every architecture, and every compiler — and there is
no NaN, no Inf, and no platform-dependent rounding.

## Implementation

The geometric fixed-point types are provided by the `g_math` crate (the
ecosystem's deterministic, zero-float arithmetic library) and re-exported
unchanged:

```rust
// daemon/src/geometric_precision.rs
pub use g_math::fixed_point::{FixedPoint, FixedVector, FixedMatrix};
```

g_math is built with its default **`embedded` profile → Q64.64** (64 integer +
64 fractional bits, `i128` storage). This replaced ~1,000 LOC of hand-rolled
Q32.32 (`i64`) arithmetic that previously lived in this module; the wider format
adds headroom and precision while preserving the same pure-integer determinism.

Transcendental functions (`sqrt`, `ln`, `exp`, `sin`, `cos`, `atan`, … — 18 in
total) are g_math's ULP-validated, table-driven implementations. They are
approximations of the true real-valued functions, but **every node computes the
exact same approximated result** — bit-for-bit identical.

> **Cross-node precondition.** Determinism across a constellation requires every
> node's daemon to use the *same* g_math format. The build pins no
> `GMATH_PROFILE`, so all nodes use g_math's default (Q64.64) and agree by
> construction. Do not override `GMATH_PROFILE` on a subset of nodes.

## Properties

| Property | Value |
|----------|-------|
| Format | Q64.64 (`i128`), g_math `embedded` profile |
| Integer range | ±2^63 (≈ ±9.2 × 10^18) |
| Fractional precision | 2^-64 (≈ 5.4 × 10^-20) |
| Cross-platform | Identical on x86_64, ARM64, RISC-V |
| Cross-OS | Identical on Linux, macOS, Windows |
| NaN/Inf | Cannot occur (integer arithmetic) |

## Where It Matters

1. **Bucket key computation** — spatial hash lookup requires identical bucket assignments across nodes
2. **Distance calculations** — service selection depends on geometric distance ordering
3. **Z-score computation** — topological ordering must be consistent cluster-wide

Verified by: `test_bucket_key_determinism` and `test_bucket_key_raw_determinism`
in `daemon/src/lib.rs`.

## References

- Re-export shim: `daemon/src/geometric_precision.rs`
- Arithmetic library: the `g_math` crate (`gMath/`), Q64.64 `embedded` profile
- Architecture: `CLAUDE.md` §10 (Geometric Topology)
