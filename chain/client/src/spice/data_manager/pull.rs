use super::item::{CodedTracker, CommitmentState, FetchItem};
use super::{DataId, DataPolicy, SpiceDataManager};
use near_async::time::{Duration, Instant};
use near_primitives::spice::partial_data::SpiceDataCommitment;
use near_primitives::types::AccountId;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, BTreeSet};
use std::hash::{Hash, Hasher as _};
use time::ext::InstantExt as _;

/// One pull request to send: the ordinals asked of `producer` for each item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PullRequest {
    pub(crate) producer: AccountId,
    pub(crate) wants: BTreeMap<DataId, BTreeSet<u64>>,
}

/// Pull related settings controlling the pace/rates.
#[derive(Debug, Clone)]
pub(crate) struct PullConfig {
    /// How long a pull request stays outstanding before it is sent again.
    pub(crate) request_timeout: Duration,
}

impl Default for PullConfig {
    fn default() -> Self {
        Self { request_timeout: Duration::milliseconds(600) }
    }
}

/// Index of the source to ask in `round`. The start is a hash of the key and the
/// requester, so requesters spread over the sources; each round moves one along.
pub(crate) fn rotated_source_index(
    num_sources: usize,
    key: &impl Hash,
    requester: &AccountId,
    round: u64,
) -> usize {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    requester.hash(&mut hasher);
    (hasher.finish().wrapping_add(round) % num_sources as u64) as usize
}

impl CodedTracker {
    /// The member of `pool` at the rotation: moves the rotation past that member.
    fn take_next_source(&mut self, pool: &[AccountId]) -> Option<AccountId> {
        if pool.is_empty() {
            return None;
        }
        let start = (self.rotation_cursor % pool.len() as u64) as usize;
        let source = pool[start].clone();
        // the next rotation starts right after the member asked
        self.rotation_cursor = self.rotation_cursor.wrapping_add(1);
        Some(source)
    }
}

impl FetchItem {
    /// Drops every pull unanswered for `request_timeout` as of `now`.
    pub(super) fn drop_stale_pulls(&mut self, now: Instant, request_timeout: Duration) {
        for (_, state) in &mut self.producers {
            let stale = state
                .requested_at
                .is_some_and(|sent_at| now.signed_duration_since(sent_at) >= request_timeout);
            if stale {
                state.requested_at = None;
            }
        }
    }

    /// The producers with a pull from this item unanswered.
    // TODO(review-split): read by the producer budget in the next step.
    #[allow(dead_code)]
    pub(super) fn outstanding_pulls(&self) -> impl Iterator<Item = &AccountId> {
        self.producers
            .iter()
            .filter(|(_, state)| state.requested_at.is_some())
            .map(|(producer, _)| producer)
    }

    /// Producers to ask at `now`, with the ordinals to ask each. A bound producer is asked
    /// only by its commitment's tracker, one request at a time; an unbound one only for its
    /// own ordinal.
    pub(super) fn pull_wants(&mut self, now: Instant) -> BTreeMap<AccountId, BTreeSet<u64>> {
        let mut wants: BTreeMap<AccountId, BTreeSet<u64>> = BTreeMap::new();
        let live: Vec<SpiceDataCommitment> = self
            .commitments
            .iter()
            .filter(|(_, state)| matches!(state, CommitmentState::Tracking(_)))
            .map(|(commitment, _)| commitment.clone())
            .collect();
        for commitment in live {
            let asked = self.producers.iter().any(|(_, state)| {
                state.commitment.as_ref() == Some(&commitment) && state.requested_at.is_some()
            });
            if asked {
                continue;
            }
            let mut pool: Vec<AccountId> =
                self.contributors(&commitment).into_iter().cloned().collect();
            pool.sort();
            let tracker = self.tracker_mut(&commitment).expect("live commitment is tracked");
            let Some(source) = tracker.take_next_source(&pool) else {
                continue;
            };
            let missing = tracker.missing_ordinals();
            self.producer_mut(&source).expect("pool member is a producer").requested_at = Some(now);
            wants.entry(source).or_default().extend(missing);
        }
        for (ordinal, (producer, state)) in self.producers.iter_mut().enumerate() {
            let engaged = state.commitment.is_some() || state.requested_at.is_some();
            if engaged {
                continue;
            }
            state.requested_at = Some(now);
            wants.entry(producer.clone()).or_default().insert(ordinal as u64);
        }
        wants
    }

    /// `sender` answered: forgets the pull outstanding to it.
    pub(super) fn note_pull_response(&mut self, sender: &AccountId) {
        if let Some(state) = self.producer_mut(sender) {
            state.requested_at = None;
        }
    }
}

impl<P: DataPolicy> SpiceDataManager<P> {
    /// The requests to send at `now`, grouped by producer.
    pub(super) fn pull_requests(&mut self, now: Instant) -> Vec<PullRequest> {
        let request_timeout = self.pull_config.request_timeout;
        for item in self.items.values_mut() {
            item.drop_stale_pulls(now, request_timeout);
        }
        let mut wants_by_producer: BTreeMap<AccountId, BTreeMap<DataId, BTreeSet<u64>>> =
            BTreeMap::new();
        for id in self.items_by_height.values().flatten() {
            let item = self.items.get_mut(id).expect("index entry names a tracked item");
            if !item.is_pullable() {
                continue;
            }
            for (producer, ordinals) in item.pull_wants(now) {
                wants_by_producer
                    .entry(producer)
                    .or_default()
                    .entry(id.clone())
                    .or_default()
                    .extend(ordinals);
            }
        }
        wants_by_producer
            .into_iter()
            .map(|(producer, wants)| PullRequest { producer, wants })
            .collect()
    }
}
