//! SKETCH. SPICE-internal reputation table that ranks pull sources and exports signals to the network peer scorer.

use super::item::DataId;
use near_async::time::{Duration, Instant};
use near_primitives::types::AccountId;
use std::collections::HashMap;

/// Attributable faults, funnelled from the engine and from consumer `FailedEvent`s.
#[derive(Debug, Clone)]
pub(crate) enum Misbehavior {
    BadMerkleProof,
    DecodeGarbage,
    BadCodeBytes,
    InvalidReceiptProof,
    AccessesInconsistent,
    /// Denied/withheld data it should hold.
    DeniedHeldData,
    /// Our endorsed execution result differs from the certified one.
    CertifiedResultMismatch,
    /// Oversized / out-of-window / unsolicited data.
    ProtocolViolation,
}

impl Misbehavior {
    pub(crate) fn weight(&self) -> f64 {
        0.0 // sketch
    }
}

/// Two channels, two half-lives: slow honesty (~hours) and fast responsiveness (~seconds).
#[derive(Debug, Clone)]
pub(crate) struct ReputationConfig {
    pub(crate) score_halflife: Duration,
    pub(crate) load_halflife: Duration,
    /// Floor for `score`; ceiling is 0 (no positive reinforcement).
    pub(crate) score_floor: f64,
}

/// Per-producer score; both channels decay lazily from one shared anchor.
#[derive(Debug, Clone)]
pub(crate) struct PeerScore {
    score: f64,
    /// Decayed accumulator (EWMA), not a monotonic count.
    timeout_load: f64,
    last_update_at: Instant,
}

impl PeerScore {
    fn touch(&mut self, _now: Instant, _cfg: &ReputationConfig) {}

    fn effective(&self, _now: Instant, _cfg: &ReputationConfig) -> (f64, f64) {
        (0.0, 0.0) // sketch
    }
}

/// SPICE-internal reputation keyed by producer. Sparse: a missing entry reads as neutral-and-live.
pub(crate) struct Reputation {
    scores: HashMap<AccountId, PeerScore>,
    config: ReputationConfig,
    // exporter: NetworkPeerScoreExporter — account→peer bridge (open seam).
}

impl Reputation {
    /// `touch` + `score -= weight`, then export to the network scorer.
    pub(crate) fn report(&mut self, _who: &[AccountId], _what: Misbehavior, _about: &DataId) {}

    /// `touch` + `timeout_load += 1`. The only other write path.
    pub(crate) fn note_timeout(&mut self, _who: &AccountId, _now: Instant) {}

    /// Weighted-sample `n` sources from `pool`, excluding `outstanding`; sampling, not argmax.
    pub(crate) fn select_sources(
        &self,
        pool: &[AccountId],
        _outstanding: &[AccountId],
        _n: usize,
        _now: Instant,
    ) -> Vec<AccountId> {
        pool.to_vec() // sketch
    }

    /// Epoch GC: drop entries for accounts no longer in any relevant producer set.
    pub(crate) fn on_epoch_switch(&mut self, _still_relevant: &dyn Fn(&AccountId) -> bool) {}
}

/// Maps SPICE per-account reputation to per-peer network scoring, which owns banning.
pub(crate) trait NetworkPeerScoreExporter {
    fn export(&self, account: &AccountId, what: &Misbehavior);
}
