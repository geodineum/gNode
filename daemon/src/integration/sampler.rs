// The load sampler: what a provider's latency says about how busy it is.
//
// Derived from each provider's OWN baseline, per command, so a service whose `infer`
// normally takes 292 seconds is not called saturated for being slow — only for being
// slower than itself. That needs no calibrated ops/second ceiling, which would go
// stale with every hardware and code change and could not see load this node did not
// route.
//
// The reading is the queueing identity R = R0/(1-ρ) read backwards:
//
//     inflation I = p50(window) / baseline        ρ = 1 - 1/I
//
// I=1 → 0, I=2 → 0.5, I=4 → 0.75, I=10 → 0.9. Bounded, and it needs one constant
// (the guard below) rather than a per-component measurement campaign.
//
// This module is pure: observations in, writes out, no I/O. The worker owns the
// stream reads, the lease and the FCALL.
//
// SCOPE: dimension 16 (current_load) only. health_status stays `unknown` — deriving
// it needs an entity→component mapping that does not exist yet (a web-profile entity
// like `nierto_com` has no component heartbeat, while `gflow` does), and a health
// value nobody can justify is worse than an honest unknown.

use std::collections::HashMap;

/// Which topology the provider lives in. The daemon executing a command IS the
/// provider for that work, and a daemon's load belongs to its NODE entity — which
/// is also what "route to the least busy node" asks about, and what the first
/// consumer queries. A relayed request's provider is a service entity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Tier { Service, Node }

/// One completed request, as the relay, the command processor or a publisher saw it.
#[derive(Debug, Clone)]
pub struct Observation {
    pub tier: Tier,
    pub site: String,
    pub entity: String,
    pub command: String,
    pub elapsed_ms: u64,
    pub ok: bool,
    pub ts_ms: u64,
}

/// A coordinate the sampler wants written, and why.
#[derive(Debug, Clone, PartialEq)]
pub struct DerivedWrite {
    pub tier: Tier,
    pub site: String,
    pub entity: String,
    pub index: usize,
    pub pd: f64,
    pub zs: i64,
    pub reason: String,
}

#[derive(Debug, Clone)]
pub struct SamplerConfig {
    /// How often the worker ticks.
    pub tick_secs: u64,
    /// Samples older than this do not describe the present.
    pub window_secs: u64,
    /// The window the baseline learns from. Long on purpose: a baseline is what
    /// this command costs when nothing is in its way, and an estate whose busiest
    /// site serves ten commands an hour has no such thing inside a minute.
    pub baseline_window_secs: u64,
    /// Ring capacity per (entity, command).
    pub ring: usize,
    /// Below this many samples in the window, a p50 is noise, not a measurement.
    pub min_samples: usize,
    /// Baseline EWMA weight per tick.
    pub alpha: f64,
    /// The baseline only learns from ticks below this inflation. Without it, a
    /// provider under sustained load teaches the baseline that its overload is
    /// normal and the inflation returns to 1 — a thermometer in the sun.
    pub baseline_guard: f64,
    /// A stored value older than this is not data; the ranker abstains, so the
    /// sampler refreshes the stamp at half of it.
    pub horizon_secs: u64,
    /// Report saturation fast, recovery slowly.
    pub down_ticks: u32,
}

impl Default for SamplerConfig {
    fn default() -> Self {
        Self {
            tick_secs: 15,
            window_secs: 60,
            baseline_window_secs: 3600,
            ring: 256,
            min_samples: 8,
            alpha: 0.05,
            baseline_guard: 1.5,
            horizon_secs: 180,
            down_ticks: 2,
        }
    }
}

impl SamplerConfig {
    pub fn from_env() -> Self {
        let num = |k: &str, d: f64| std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d);
        let d = Self::default();
        Self {
            tick_secs: num("GNODE_SAMPLER_TICK_SECS", d.tick_secs as f64) as u64,
            window_secs: num("GNODE_SAMPLER_WINDOW_SECS", d.window_secs as f64) as u64,
            baseline_window_secs: num("GNODE_SAMPLER_BASELINE_WINDOW_SECS",
                                      d.baseline_window_secs as f64) as u64,
            ring: num("GNODE_SAMPLER_RING", d.ring as f64) as usize,
            min_samples: num("GNODE_SAMPLER_MIN_SAMPLES", d.min_samples as f64) as usize,
            alpha: num("GNODE_SAMPLER_ALPHA", d.alpha),
            baseline_guard: num("GNODE_SAMPLER_BASELINE_GUARD", d.baseline_guard),
            horizon_secs: num("GNODE_SAMPLER_HORIZON_SECS", d.horizon_secs as f64) as u64,
            down_ticks: num("GNODE_SAMPLER_DOWN_TICKS", d.down_ticks as f64) as u32,
        }
    }
}

/// What the axis is allowed to say. 0.00 is `unknown` and never a measurement, so a
/// measured idle provider (0.20) is distinguishable from one nobody has measured.
pub const LOAD_LANDMARKS: [(f64, &str); 6] = [
    (0.00, "unknown"), (0.20, "idle"), (0.40, "light"),
    (0.60, "moderate"), (0.80, "heavy"), (1.00, "saturated"),
];

/// The band a coordinate sits in: the highest landmark at or below it.
pub fn landmark(pd: f64) -> (usize, &'static str) {
    let mut best = (0usize, LOAD_LANDMARKS[0].1);
    for (i, (v, name)) in LOAD_LANDMARKS.iter().enumerate() {
        if pd + 1e-9 >= *v {
            best = (i, name);
        }
    }
    best
}

#[derive(Debug, Default, Clone)]
struct Ring {
    samples: Vec<(u64, u64)>, // (ts_ms, elapsed_ms)
}

impl Ring {
    fn push(&mut self, ts_ms: u64, ms: u64, cap: usize) {
        self.samples.push((ts_ms, ms));
        if self.samples.len() > cap {
            let drop = self.samples.len() - cap;
            self.samples.drain(0..drop);
        }
    }

    /// The median of the samples inside the window, and how many there were.
    fn p50(&self, now_ms: u64, window_ms: u64) -> Option<(f64, usize)> {
        let mut inside: Vec<u64> = self.samples.iter()
            .filter(|(ts, _)| now_ms.saturating_sub(*ts) <= window_ms)
            .map(|(_, ms)| *ms)
            .collect();
        if inside.is_empty() {
            return None;
        }
        inside.sort_unstable();
        let n = inside.len();
        let mid = if n % 2 == 1 {
            inside[n / 2] as f64
        } else {
            (inside[n / 2 - 1] + inside[n / 2]) as f64 / 2.0
        };
        Some((mid, n))
    }
}

#[derive(Debug, Default, Clone)]
struct Band {
    written: Option<usize>,
    pending: Option<usize>,
    pending_ticks: u32,
    last_write_ms: u64,
}

/// When an observation happened, in ms, from the `ts` field and the stream entry id.
///
/// Three things go wrong without this. Publishers disagree about units — Geodine
/// writes `ts=1775515335.4866`, seconds with a fraction, while the contract and the
/// daemon's own publisher write milliseconds. A parser that wants `u64` rejects the
/// first and, falling back to "now", turns a record from March into a measurement of
/// the present: that is how three baselines were learned from six-month-old data.
/// And a record with no usable `ts` at all still has a true time — the stream entry
/// id, whose first component is the append time in ms.
pub fn observed_at_ms(ts_field: Option<&str>, entry_id: &str) -> u64 {
    let from_entry = || entry_id.split('-').next()
        .and_then(|ms| ms.parse::<u64>().ok())
        .unwrap_or(0);
    match ts_field.and_then(|v| v.trim().parse::<f64>().ok()) {
        Some(v) if v.is_finite() && v > 0.0 => {
            // Anything below ~1973 in ms is seconds: no observation predates the
            // epoch by three years, and none of this estate is from 1973.
            if v < 100_000_000_000.0 { (v * 1000.0) as u64 } else { v as u64 }
        }
        _ => from_entry(),
    }
}

/// Utilisation from inflation, via R = R0/(1-ρ). `None` when there is nothing to
/// compare against.
pub fn utilisation(p50: f64, baseline: f64) -> Option<f64> {
    if baseline <= 0.0 || p50 <= 0.0 {
        return None;
    }
    let inflation = p50 / baseline;
    if !inflation.is_finite() {
        return None;
    }
    Some((1.0 - 1.0 / inflation).clamp(0.0, 0.98))
}

/// The coordinate for a utilisation: the measured band starts at `idle`, because
/// 0.00 means unknown.
pub fn coordinate(rho: f64) -> f64 {
    let pd = 0.20 + 0.80 * rho.clamp(0.0, 1.0);
    (pd * 10_000.0).round() / 10_000.0
}

type RingKey = (Tier, String, String, String);   // (tier, site, entity, command)

pub struct Sampler {
    pub cfg: SamplerConfig,
    rings: HashMap<RingKey, Ring>,
    baselines: HashMap<RingKey, f64>,
    bands: HashMap<(Tier, String, String), Band>,
}

impl Sampler {
    pub fn new(cfg: SamplerConfig) -> Self {
        Self { cfg, rings: HashMap::new(), baselines: HashMap::new(), bands: HashMap::new() }
    }

    pub fn observe(&mut self, o: Observation) {
        // A sub-millisecond command would otherwise get a baseline of ZERO, and a
        // zero baseline can never produce a reading — `ping` learned exactly that.
        // One millisecond is the measurement floor, not a real duration.
        let elapsed = o.elapsed_ms.max(1);
        let ring = self.rings.entry((o.tier, o.site, o.entity, o.command)).or_default();
        ring.push(o.ts_ms, elapsed, self.cfg.ring);
    }

    /// Whether an observation is recent enough to describe the present. The ring's
    /// window would ignore it anyway; refusing it at the door keeps a backlog
    /// replay from being mistaken for live traffic.
    pub fn is_current(&self, ts_ms: u64, now_ms: u64) -> bool {
        now_ms.saturating_sub(ts_ms) <= self.cfg.window_secs * 1000
    }

    /// Seed a baseline read back from ValKey, so a restart does not relearn from zero.
    pub fn seed_baseline(&mut self, tier: Tier, site: &str, entity: &str, command: &str, p50: f64) {
        if p50 > 0.0 {
            self.baselines.insert((tier, site.into(), entity.into(), command.into()), p50);
        }
    }

    /// Every baseline, for persisting.
    pub fn baselines(&self) -> Vec<(Tier, String, String, String, f64)> {
        self.baselines.iter()
            .map(|((t, s, e, c), v)| (*t, s.clone(), e.clone(), c.clone(), *v))
            .collect()
    }

    /// One pass: update baselines, compute each entity's load, and return only the
    /// coordinates worth writing.
    pub fn tick(&mut self, now_ms: u64) -> Vec<DerivedWrite> {
        let window_ms = self.cfg.window_secs * 1000;
        let mut per_entity: HashMap<(Tier, String, String), (f64, usize)> = HashMap::new();

        let baseline_window_ms = self.cfg.baseline_window_secs * 1000;
        let keys: Vec<RingKey> = self.rings.keys().cloned().collect();
        for key in keys {
            // The baseline learns first, from the long window, and needs no quorum.
            // Gating it on min_samples cost this estate the whole axis: at ten
            // commands an hour no minute ever held eight samples, so no baseline
            // ever formed — and the first burst to arrive would have been adopted
            // as normal, leaving a flooded provider reading idle.
            let mut bootstrapped = false;
            if let Some((slow, _)) = self.rings[&key].p50(now_ms, baseline_window_ms) {
                let slow = slow.max(1.0);
                match self.baselines.get(&key).copied() {
                    None => {
                        self.baselines.insert(key.clone(), slow);
                        bootstrapped = true;
                    }
                    Some(b) if slow < b => {
                        // A cheaper normal is proof the floor was never that high.
                        self.baselines.insert(key.clone(), slow);
                    }
                    Some(b) => {
                        // Upward is the dangerous direction — under sustained load
                        // it teaches the baseline that the overload is normal and
                        // the inflation returns to 1, a thermometer in the sun. So
                        // it is slow and the guard bounds it.
                        if slow / b < self.cfg.baseline_guard {
                            let updated = (1.0 - self.cfg.alpha) * b + self.cfg.alpha * slow;
                            self.baselines.insert(key.clone(), updated);
                        }
                    }
                }
            }

            // The load reading needs the quorum: below it a p50 is noise. And a
            // baseline born this tick has nothing to be compared against yet —
            // against itself every provider reads idle, which would publish a
            // measurement of nothing.
            if bootstrapped {
                continue;
            }
            let Some((p50, n)) = self.rings[&key].p50(now_ms, window_ms) else { continue };
            if n < self.cfg.min_samples {
                continue;
            }
            let Some(b) = self.baselines.get(&key).copied() else { continue };
            if let Some(rho) = utilisation(p50, b) {
                let slot = per_entity.entry((key.0, key.1.clone(), key.2.clone())).or_insert((0.0, 0));
                slot.0 += rho * n as f64;
                slot.1 += n;
            }
        }

        let mut writes = Vec::new();
        for ((tier, site, entity), (weighted, n)) in per_entity {
            if n == 0 {
                continue;
            }
            let pd = coordinate(weighted / n as f64);
            let (band_idx, band_name) = landmark(pd);
            let band = self.bands.entry((tier, site.clone(), entity.clone())).or_default();

            let reason = match band.written {
                // Never written: say what we now know.
                None => Some(format!("first measurement, {}", band_name)),
                Some(written) if band_idx > written => {
                    // Up immediately: a provider getting busy is news now.
                    Some(format!("rose to {}", band_name))
                }
                Some(written) if band_idx < written => {
                    // Down slowly: one quiet window is not a recovery.
                    if band.pending == Some(band_idx) {
                        band.pending_ticks += 1;
                    } else {
                        band.pending = Some(band_idx);
                        band.pending_ticks = 1;
                    }
                    if band.pending_ticks >= self.cfg.down_ticks {
                        Some(format!("settled to {}", band_name))
                    } else {
                        None
                    }
                }
                Some(_) => {
                    // Unchanged: refresh the stamp before the ranker starts
                    // abstaining on it.
                    let age = now_ms.saturating_sub(band.last_write_ms);
                    if age >= self.cfg.horizon_secs * 1000 / 2 {
                        Some(format!("stamp refresh, still {}", band_name))
                    } else {
                        None
                    }
                }
            };

            if let Some(reason) = reason {
                band.written = Some(band_idx);
                band.pending = None;
                band.pending_ticks = 0;
                band.last_write_ms = now_ms;
                writes.push(DerivedWrite {
                    tier,
                    site,
                    entity,
                    // Service tier: current_load. Node tier: aggregate_load. Both sit
                    // at the same index by construction — the first derived axis
                    // above each schema's hashed cut — and a test pins that.
                    index: crate::integration::handlers::types::SERVICE_SAMPLER_AXES[0],
                    pd,
                    zs: (pd * 1_000_000.0) as i64,
                    reason,
                });
            }
        }
        writes
    }
}

/// Whether the sampler wants observations at all, read once: an env lookup per
/// command would be a syscall on the hot path for a value that cannot change.
static OBSERVE: std::sync::OnceLock<bool> = std::sync::OnceLock::new();

pub fn observations_enabled() -> bool {
    *OBSERVE.get_or_init(|| {
        !matches!(
            std::env::var("GNODE_SAMPLER").unwrap_or_default().to_ascii_lowercase().as_str(),
            "off"
        )
    })
}

/// The XADD that records one observation of this node's own command handling, or
/// `None` when the sampler is off.
///
/// Both lanes publish through this one builder. The lane a command took must not be
/// observable in the measurement, and that only stays true if neither lane owns a
/// copy of the wire format — the Ordered lane had the only copy, so every
/// concurrent command this estate actually serves went unmeasured.
pub fn node_observation_cmd(command: &str, elapsed_ms: u64, ok: bool) -> Option<redis::Cmd> {
    if !observations_enabled() {
        return None;
    }
    let ns = std::env::var("GNODE_TOPOLOGY_NAMESPACE").unwrap_or_else(|_| "geodineum".to_string());
    let node = crate::daemon::GNodeDaemon::node_id_for_lease();
    let mut cmd = redis::cmd("XADD");
    cmd.arg(crate::config::build_health_stream_key(&ns))
        .arg("MAXLEN").arg("~").arg(2000).arg("*")
        .arg("t").arg("rq")
        .arg("si").arg(node)
        .arg("cmd").arg(command)
        .arg("lat").arg(elapsed_ms)
        .arg("ok").arg(if ok { 1 } else { 0 })
        .arg("ts").arg(crate::utils::current_timestamp_ms());
    Some(cmd)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn obs(site: &str, entity: &str, cmd: &str, ms: u64, ts: u64) -> Observation {
        Observation { tier: Tier::Service, site: site.into(), entity: entity.into(),
                      command: cmd.into(), elapsed_ms: ms, ok: true, ts_ms: ts }
    }

    fn feed(s: &mut Sampler, n: usize, ms: u64, ts: u64) {
        for _ in 0..n { s.observe(obs("st", "svc", "get", ms, ts)); }
    }

    #[test]
    fn a_timestamp_is_read_in_whatever_unit_the_publisher_used() {
        // Geodine's actual record, seconds with a fraction.
        assert_eq!(observed_at_ms(Some("1775515335.4866"), "1-0"), 1_775_515_335_486);
        // The daemon's own publisher, already milliseconds.
        assert_eq!(observed_at_ms(Some("1775515335486"), "1-0"), 1_775_515_335_486);
        // No usable ts: the entry id IS the append time, never "now".
        assert_eq!(observed_at_ms(None, "1775515335486-7"), 1_775_515_335_486);
        assert_eq!(observed_at_ms(Some("not a number"), "1775515335486-0"), 1_775_515_335_486);
        assert_eq!(observed_at_ms(Some("0"), "1775515335486-0"), 1_775_515_335_486);
        assert_eq!(observed_at_ms(None, "garbage"), 0, "and an unreadable id is not now either");
    }

    #[test]
    fn a_submillisecond_command_still_gets_a_usable_baseline() {
        // `ping` was learned as a baseline of 0.000 ms, and a zero baseline can
        // never produce a reading, so that command could never contribute.
        let mut s = Sampler::new(SamplerConfig::default());
        for _ in 0..10 { s.observe(obs("st", "svc", "ping", 0, 15_000)); }
        s.tick(15_000);
        assert_eq!(s.baselines().len(), 1);
        assert!(s.baselines()[0].4 >= 1.0, "baseline was {}", s.baselines()[0].4);
    }

    #[test]
    fn an_ancient_observation_is_not_a_measurement_of_now() {
        let s = Sampler::new(SamplerConfig::default());
        let now = 1_800_000_000_000u64;
        assert!(s.is_current(now - 30_000, now));
        assert!(!s.is_current(now - 600_000, now), "six months of backlog is not live traffic");
    }

    #[test]
    fn utilisation_follows_the_queueing_identity() {
        assert_eq!(utilisation(100.0, 100.0), Some(0.0));
        assert_eq!(utilisation(200.0, 100.0), Some(0.5));
        assert_eq!(utilisation(400.0, 100.0), Some(0.75));
        assert_eq!(utilisation(1000.0, 100.0), Some(0.9));
        // Faster than its own baseline is not negative load.
        assert_eq!(utilisation(50.0, 100.0), Some(0.0));
        // Never exactly 1: saturation is an asymptote, not a reading.
        assert_eq!(utilisation(1e9, 100.0), Some(0.98));
        assert_eq!(utilisation(100.0, 0.0), None);
    }

    #[test]
    fn the_measured_band_starts_above_unknown() {
        // The whole point of the recode: a measured idle provider must not look
        // like one nobody measured.
        assert_eq!(coordinate(0.0), 0.20);
        assert_eq!(landmark(coordinate(0.0)).1, "idle");
        assert_eq!(landmark(0.00).1, "unknown");
        assert_eq!(coordinate(1.0), 1.00);
        // The ladder, stated once: ρ is utilisation, and 4x latency inflation
        // (ρ 0.75) is what "heavy" means.
        assert_eq!(landmark(coordinate(0.25)).1, "light");     // 0.40, I=1.33
        assert_eq!(landmark(coordinate(0.50)).1, "moderate");  // 0.60, I=2
        assert_eq!(landmark(coordinate(0.75)).1, "heavy");     // 0.80, I=4
        assert_eq!(landmark(coordinate(0.90)).1, "heavy");     // 0.92, I=10
    }

    #[test]
    fn a_first_window_is_the_baseline_and_not_a_load_reading() {
        let mut s = Sampler::new(SamplerConfig::default());
        feed(&mut s, 10, 100, 1_000);
        assert!(s.tick(1_000).is_empty(), "bootstrapping says nothing about load");
        assert_eq!(s.baselines().len(), 1);
    }

    #[test]
    fn too_few_samples_is_not_a_measurement() {
        let mut s = Sampler::new(SamplerConfig::default());
        feed(&mut s, 3, 100, 1_000);
        assert!(s.tick(1_000).is_empty());
    }

    #[test]
    fn a_sparse_provider_still_learns_a_baseline() {
        // The whole axis depended on this. The quorum used to gate the baseline as
        // well as the reading, and this estate's busiest site serves ten commands
        // an hour — so no minute ever held eight samples, no baseline ever formed,
        // and the first burst to arrive would have been adopted as normal.
        let mut s = Sampler::new(SamplerConfig::default());
        s.observe(obs("st", "svc", "get", 100, 1_000));
        s.tick(1_000);
        assert_eq!(s.baselines().len(), 1, "one honest sample is a better floor than none");
        assert_eq!(s.baselines()[0].4, 100.0);

        // And the floor is then there when the burst does arrive.
        feed(&mut s, 10, 400, 20_000);
        let w = s.tick(20_000);
        assert_eq!(w.len(), 1);
        assert_eq!(landmark(w[0].pd).1, "heavy", "4x its own normal, measured from one prior sample");
    }

    #[test]
    fn a_cheaper_normal_lowers_the_baseline_at_once() {
        // A baseline learned while something else was in the way is too high, and
        // every reading taken against it understates the load. Downward is safe to
        // take immediately: nothing can make a command cheaper than it is.
        let mut s = Sampler::new(SamplerConfig::default());
        s.seed_baseline(Tier::Service, "st", "svc", "get", 400.0);
        feed(&mut s, 10, 100, 15_000);
        s.tick(15_000);
        assert_eq!(s.baselines()[0].4, 100.0);
    }

    #[test]
    fn sustained_overload_does_not_teach_the_baseline() {
        // The guard that separates a signal from a thermometer in the sun.
        let mut s = Sampler::new(SamplerConfig::default());
        s.seed_baseline(Tier::Service, "st", "svc", "get", 100.0);
        for tick in 1..=20u64 {
            let now = tick * 15_000;
            feed(&mut s, 10, 400, now);          // 4x its own baseline, forever
            s.tick(now);
        }
        let b = s.baselines()[0].4;
        assert!(b < 110.0, "baseline drifted to {b} under sustained load");

        // Below the guard it DOES learn, so a genuinely faster provider is tracked.
        let mut s2 = Sampler::new(SamplerConfig::default());
        s2.seed_baseline(Tier::Service, "st", "svc", "get", 100.0);
        for tick in 1..=40u64 {
            let now = tick * 15_000;
            feed(&mut s2, 10, 120, now);         // 1.2x: within the guard
            s2.tick(now);
        }
        assert!(s2.baselines()[0].4 > 110.0, "baseline should follow a real shift");
    }

    #[test]
    fn rising_load_is_reported_at_once_and_recovery_only_after_confirmation() {
        let cfg = SamplerConfig { down_ticks: 2, ..SamplerConfig::default() };
        let mut s = Sampler::new(cfg);
        s.seed_baseline(Tier::Service, "st", "svc", "get", 100.0);

        feed(&mut s, 10, 400, 15_000);
        let w = s.tick(15_000);
        assert_eq!(w.len(), 1);
        assert_eq!(landmark(w[0].pd).1, "heavy", "4x inflation is heavy");
        assert!(w[0].reason.contains("first measurement"));

        // Still heavy: no write, and no stamp refresh yet either.
        feed(&mut s, 10, 400, 30_000);
        assert!(s.tick(30_000).is_empty());

        // Recovery is slow by construction, and in two stages: the slow samples
        // must first age out of the 60s window, and only THEN does a lower band
        // have to survive `down_ticks` before it is written. A quiet provider is
        // therefore reported recovered roughly a window-plus-two-ticks later —
        // deliberately, because one quiet window is not a recovery.
        let mut settled_at = None;
        for tick in 3..=12u64 {
            let now = tick * 15_000;
            feed(&mut s, 10, 100, now);
            for w in s.tick(now) {
                if w.reason.contains("settled") && settled_at.is_none() {
                    settled_at = Some((now, landmark(w.pd).1));
                }
            }
        }
        let (when, band) = settled_at.expect("a provider that went quiet must be reported recovered");
        assert_eq!(band, "idle");
        assert!(when >= 60_000 && when <= 135_000,
                "recovery should take about a window plus a confirmation, took {}ms", when);
    }

    #[test]
    fn an_unchanged_value_is_refreshed_before_the_ranker_abstains() {
        let cfg = SamplerConfig { horizon_secs: 180, ..SamplerConfig::default() };
        let mut s = Sampler::new(cfg);
        s.seed_baseline(Tier::Service, "st", "svc", "get", 100.0);
        feed(&mut s, 10, 400, 15_000);
        assert_eq!(s.tick(15_000).len(), 1);

        // Half the horizon later, the same value is written again so its stamp
        // stays inside the horizon.
        for t in [30_000u64, 45_000, 60_000, 75_000, 90_000] {
            feed(&mut s, 10, 400, t);
            let w = s.tick(t);
            if t < 105_000 && !w.is_empty() {
                assert!(w[0].reason.contains("stamp refresh"), "{:?}", w[0].reason);
            }
        }
        feed(&mut s, 10, 400, 106_000);
        let w = s.tick(106_000);
        assert_eq!(w.len(), 1);
        assert!(w[0].reason.contains("stamp refresh"));
    }

    #[test]
    fn a_slow_command_is_judged_against_itself_not_against_a_fast_one() {
        // Geodine's `infer` runs for minutes and its `ping` for milliseconds. One
        // ceiling for both would call the provider saturated for doing its job.
        let mut s = Sampler::new(SamplerConfig::default());
        s.seed_baseline(Tier::Service, "st", "geodine", "infer", 292_000.0);
        s.seed_baseline(Tier::Service, "st", "geodine", "ping", 5.0);
        for _ in 0..10 { s.observe(obs("st", "geodine", "infer", 292_000, 15_000)); }
        for _ in 0..10 { s.observe(obs("st", "geodine", "ping", 5, 15_000)); }
        let w = s.tick(15_000);
        assert_eq!(w.len(), 1);
        assert_eq!(landmark(w[0].pd).1, "idle", "at its own baseline, it is idle");
    }

    #[test]
    fn a_stale_sample_leaves_the_window() {
        let mut s = Sampler::new(SamplerConfig::default());
        s.seed_baseline(Tier::Service, "st", "svc", "get", 100.0);
        feed(&mut s, 10, 400, 1_000);
        // Two minutes later those samples no longer describe the present.
        assert!(s.tick(121_000).is_empty());
    }

    #[test]
    fn commands_weight_by_how_much_they_were_observed() {
        let mut s = Sampler::new(SamplerConfig::default());
        s.seed_baseline(Tier::Service, "st", "svc", "hot", 100.0);
        s.seed_baseline(Tier::Service, "st", "svc", "rare", 100.0);
        for _ in 0..90 { s.observe(obs("st", "svc", "hot", 100, 15_000)); }   // ρ 0
        for _ in 0..10 { s.observe(obs("st", "svc", "rare", 1000, 15_000)); } // ρ 0.9
        let w = s.tick(15_000);
        assert_eq!(w.len(), 1);
        // The ring caps each command, so the weighting is by observed count.
        assert!(w[0].pd < 0.60, "one rare slow command must not dominate: {}", w[0].pd);
    }
}
