//! SKETCH. Per-item deadline scheduler + recovery timing; replaces the global 1 s ticker.

use super::QosClass;
use super::item::DataId;
use near_async::time::{Duration, Instant};
use std::collections::BinaryHeap;

/// Timing tuning knobs. Reputation decay lives in [`super::reputation::ReputationConfig`].
#[derive(Debug, Clone)]
pub(crate) struct TimingConfig {
    pub(crate) push_grace: Duration,            // ≈200 ms
    pub(crate) first_unit_pull_delay: Duration, // ≈200 ms
    pub(crate) request_timeout: Duration,       // ≈1 s
    pub(crate) backoff_base: Duration,          // ≈ T_rtt
    pub(crate) backoff_multiplier: u32,         // 2
    pub(crate) backoff_cap: Duration,           // ≈2 s
    pub(crate) jitter_frac: f64,                // ± fraction; rng injected for determinism
    pub(crate) escalation_fanout: u8,           // producers contacted on decode-timeout, 2–3
    pub(crate) safety_sweep: Duration,          // ≈5 s
}

impl Default for TimingConfig {
    fn default() -> Self {
        Self {
            push_grace: Duration::milliseconds(200),
            first_unit_pull_delay: Duration::milliseconds(200),
            request_timeout: Duration::seconds(1),
            backoff_base: Duration::milliseconds(200),
            backoff_multiplier: 2,
            backoff_cap: Duration::seconds(2),
            jitter_frac: 0.25,
            escalation_fanout: 3,
            safety_sweep: Duration::seconds(5),
        }
    }
}

/// A queued wake-up. The engine pops due entries and calls `on_deadline(id)`.
#[derive(Debug, PartialEq, Eq)]
struct Deadline {
    at: Instant,
    id: DataId,
    qos: QosClass,
}

// Flipped so the max-heap yields the earliest deadline, `Priority` before `Background`.
impl Ord for Deadline {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.at.cmp(&self.at).then_with(|| other.qos.cmp(&self.qos))
    }
}
impl PartialOrd for Deadline {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Per-item retry-ladder position, advanced on each miss. Lives on the `FetchItem`.
#[derive(Debug, Default)]
pub(crate) struct Backoff {
    attempts: u32,
}

impl Backoff {
    /// Next interval = `min(cap, base * multiplier^attempts)` with ± jitter.
    pub(crate) fn next_interval(&self, _cfg: &TimingConfig) -> Duration {
        Duration::milliseconds(0) // sketch
    }
}

/// Min-heap of deadlines across all items and both QoS lanes.
#[derive(Default)]
pub(crate) struct DeadlineScheduler {
    heap: BinaryHeap<Deadline>,
}

impl DeadlineScheduler {
    pub(crate) fn arm(&mut self, _id: DataId, _at: Instant, _qos: QosClass) {}

    /// Pop entries due at/before `now`; engine MUST lazily validate each (stale ids leak).
    pub(crate) fn drain_due(&mut self, _now: Instant) -> Vec<DataId> {
        Vec::new() // sketch
    }
}
