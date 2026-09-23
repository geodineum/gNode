# g_math 0.5.0 upgrade (gNode) - DONE

gNode has been upgraded and verified. `daemon/Cargo.toml` now reads
`g_math = "0.5.0"` (it previously said `"0.4.0"` and the lockfile had resolved
to 0.4.2, so this closes a long gap, not just the 0.5.0 step).

## Verification performed

- `cargo build`: clean.
- `cargo test --lib`: **294 passed, 0 failed**, 3 ignored.
- `cargo test --doc`: 6 failures, which are **pre-existing and unrelated**.
  They are gNode's own doc comments containing box-drawing characters that
  rustdoc tries to compile as Rust. Verified identical on 0.4.x: 6 before,
  6 after.

## What this changed underneath you

gNode uses `FixedPoint`, `FixedVector`, `FixedMatrix` and `sqrt`, on the
default embedded (Q64.64) profile.

- **Divide now rounds to nearest** instead of truncating, so roughly half of
  inexact quotients move by one unit in the last place. Your distance and
  norm computations are the likely places to notice.
- **Multiply** on embedded only changes on exact halfway cases, which are
  rare, so multiply is effectively unchanged for you.
- **Transcendentals and `sqrt` return bits identical to 0.4.x.**
- **Conversions are byte-for-byte unchanged** (`from_f64`, `to_f64`,
  `from_int`, `from_str`), so anything stored via a conversion stays valid.

Because you jumped from 0.4.2, you also picked up everything in between,
including the fused operations, `inv_sqrt`, and several silent-wrap fixes
where arithmetic near the top of a tier used to produce plausible but wrong
values instead of promoting to a wider tier.

## If gNode publishes computed values

Mention it downstream as a precision change: recomputed values may differ in
the last digit from values an earlier gNode produced, so do not mix results
computed across this boundary within one dataset. If gNode only computes and
serves live results, nothing is needed.

Full notice: https://github.com/nierto/gMath/releases/tag/v0.5.0
