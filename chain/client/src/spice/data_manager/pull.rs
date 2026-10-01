use near_primitives::types::AccountId;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher as _};

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
