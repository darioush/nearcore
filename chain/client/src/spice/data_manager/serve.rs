//! SKETCH. Serve side: producers answer pull requests for data they author.

use super::item::DataId;
use std::collections::HashMap;

/// Byte-budgeted LRU cache of encoded parts, so pull storms never re-encode.
pub(crate) struct EncodeCache {
    budget_bytes: u64,
    used_bytes: u64,
    entries: HashMap<DataId, CachedEncoding>,
}

/// One cached encoding: all N parts + the commitment they hash to.
pub(crate) struct CachedEncoding {
    pub(crate) bytes: u64, // sketch: parts + commitment omitted
}

impl EncodeCache {
    /// Insert an encoding, evicting LRU entries until the budget fits.
    pub(crate) fn insert(&mut self, _id: DataId, _encoding: CachedEncoding) {}

    // Eviction is correctness-free: a miss re-encodes from the stored artifact.
    pub(crate) fn get(&mut self, _id: &DataId) -> Option<&CachedEncoding> {
        None // sketch
    }

    pub(crate) fn evict(&mut self, _id: &DataId) {}
}
