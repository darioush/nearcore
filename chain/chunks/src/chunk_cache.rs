use std::collections::{HashMap, HashSet};
use std::time::Instant;

use crate::logic::chunk_needs_to_be_fetched_from_archival;
use crate::metrics;
use ::time::ext::InstantExt as _;
use near_async::time;
use near_chain::types::EpochManagerAdapter;
use near_primitives::block::Tip;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::{
    ChunkHash, PartialEncodedChunkPart, ReceiptProof, ShardChunkHeader,
};
use near_primitives::types::{BlockHeight, BlockHeightDelta, ShardId};
use std::collections::hash_map::Entry::Occupied;

// This file implements EncodedChunksCache, which provides three main functionalities:
// 1) It stores a map from a chunk hash to all the parts and receipts received so far for the chunk.
//    This map is used to aggregate chunk parts and receipts before the full chunk can be reconstructed
//    or the necessary parts and receipts are received.
//    When a PartialEncodedChunk is received, the parts and receipts it contains are merged to the
//    corresponding chunk entry in the map.
//    Entries in the map are removed if the chunk is found to be invalid or the chunk goes out of
//    horizon [chain_head_height - height_horizon, chain_head_height + MAX_HEIGHTS_AHEAD]
// 2) It stores the set of incomplete chunks, indexed by the block hash of the previous block.
//    A chunk always starts incomplete. It can be marked as complete through
//    `mark_entry_complete`. A complete entry means the chunk has all parts and receipts needed.
// 3) It stores a map from block hash to chunk headers that are ready to be included in a block.
//    This functionality is meant for block producers. When producing a block, the block producer
//    will only include chunks in the block for which it has received the part it owns.
//    Users of the data structure are responsible for adding chunk to this map at the right time.

/// Default height horizon for chunk cache. A chunk is out of rear horizon if its
/// height + DEFAULT_CHUNKS_CACHE_HEIGHT_HORIZON < largest_seen_height.
pub const DEFAULT_CHUNKS_CACHE_HEIGHT_HORIZON: BlockHeightDelta = 128;

/// A chunk is out of front horizon if its height > largest_seen_height + MAX_HEIGHTS_AHEAD
const MAX_HEIGHTS_AHEAD: BlockHeightDelta = 5;

/// EncodedChunksCacheEntry stores the consolidated parts and receipts received for a chunk
/// When a PartialEncodedChunk is received, it can be merged to the existing EncodedChunksCacheEntry
/// for the chunk
pub struct EncodedChunksCacheEntry {
    pub header: ShardChunkHeader,
    pub parts: HashMap<u64, PartialEncodedChunkPart>,
    pub receipts: HashMap<ShardId, ReceiptProof>,

    /// Lifecycle state of this chunk, including request metadata if actively requesting.
    pub(crate) state: ChunkState,

    /// whether this chunk is ready for inclusion for producing a block
    pub ready_for_inclusion: bool,
    /// Whether the header has been **fully** validated.
    /// Every entry added to the cache already has their header "partially" validated
    /// by validate_chunk_header. When the previous block is accepted, they must be
    /// validated again to make sure they are fully validated.
    /// See comments in `validate_chunk_header` for more context on partial vs full validation
    pub header_fully_validated: bool,

    /// Timestamp of when this entry was created used for metrics below
    pub created_at: Instant,
    /// Used to check whether a metric was recorded for the time taken to receive the needed parts for this chunk
    pub received_all_parts: bool,
    /// Used to check whether a metric was recorded for the time taken to receive the needed receipts for this chunk
    pub received_all_receipts: bool,
    /// Used to check whether a metric was recorded for the time taken to make a chunk able to be reconstructed
    pub could_reconstruct: bool,
}

pub struct EncodedChunksCache {
    /// Largest seen height from the head of the chain
    largest_seen_height: BlockHeight,
    /// Height horizon for chunk cache.
    height_horizon: BlockHeightDelta,

    /// A map from a chunk hash to the corresponding EncodedChunksCacheEntry of the chunk
    /// Entries in this map have height in
    /// [chain_head_height - height_horizon, chain_head_height + MAX_HEIGHTS_AHEAD]
    encoded_chunks: HashMap<ChunkHash, EncodedChunksCacheEntry>,
    /// A map from a block height to chunk hashes at this height for all chunk stored in the cache
    /// This is used to gc chunks that are out of horizon
    height_map: HashMap<BlockHeight, HashSet<ChunkHash>>,
    /// A map from block height to shard ID to the chunk hash we've received, so we only process
    /// one chunk per shard per height.
    height_to_shard_to_chunk: HashMap<BlockHeight, HashMap<ShardId, ChunkHash>>,
    /// A map from a block hash to a set of incomplete chunks (does not have all parts and receipts yet)
    /// whose previous block is the block hash.
    incomplete_chunks: HashMap<CryptoHash, HashSet<ChunkHash>>,
}

impl EncodedChunksCacheEntry {
    pub fn from_chunk_header(header: ShardChunkHeader) -> Self {
        EncodedChunksCacheEntry {
            header,
            parts: HashMap::new(),
            receipts: HashMap::new(),
            state: ChunkState::Receiving,
            ready_for_inclusion: false,
            header_fully_validated: false,
            created_at: Instant::now(),
            received_all_parts: false,
            received_all_receipts: false,
            could_reconstruct: false,
        }
    }

    pub fn is_complete(&self) -> bool {
        matches!(self.state, ChunkState::Complete)
    }

    pub fn is_requesting(&self) -> bool {
        matches!(self.state, ChunkState::Requesting(_))
    }

    pub fn request_info(&self) -> Option<&RequestInfo> {
        match &self.state {
            ChunkState::Requesting(info) => Some(info),
            _ => None,
        }
    }

    /// Inserts previously unknown chunks and receipts, returning the part ords that were
    /// previously unknown.
    pub fn merge_in_partial_encoded_chunk(
        &mut self,
        parts: impl Iterator<Item = PartialEncodedChunkPart>,
        receipts: impl Iterator<Item = ReceiptProof>,
    ) -> HashSet<u64> {
        let mut previously_missing_part_ords = HashSet::new();
        for part_info in parts {
            let part_ord = part_info.part_ord;
            self.parts.entry(part_ord).or_insert_with(|| {
                previously_missing_part_ords.insert(part_ord);
                part_info
            });
        }

        for receipt in receipts {
            let shard_id = receipt.1.to_shard_id;
            self.receipts.entry(shard_id).or_insert_with(|| receipt);
        }
        previously_missing_part_ords
    }
}

pub const CHUNK_REQUEST_RETRY: time::Duration = time::Duration::milliseconds(100);
pub const CHUNK_REQUEST_SWITCH_TO_OTHERS: time::Duration = time::Duration::milliseconds(400);
pub const CHUNK_REQUEST_SWITCH_TO_FULL_FETCH: time::Duration = time::Duration::seconds(3);
pub(crate) const CHUNK_REQUEST_RETRY_MAX: time::Duration = time::Duration::seconds(1000);
/// Only request chunks from peers whose latest height >= chunk_height - CHUNK_REQUEST_PEER_HORIZON
pub(crate) const CHUNK_REQUEST_PEER_HORIZON: BlockHeightDelta = 5;

/// The lifecycle state of a chunk in the cache.
///
/// Every chunk in the cache is in exactly one of these states. The state
/// determines whether the chunk is being actively requested and, if so,
/// carries the request metadata. When an entry is removed from the cache
/// the request metadata is automatically dropped, eliminating the class of
/// bugs where request tracking and cache tracking get out of sync.
#[derive(Clone, Debug)]
pub(crate) enum ChunkState {
    /// Parts are arriving (forwarded, unsolicited, or from a block header)
    /// but we haven't decided to actively request yet.
    Receiving,
    /// We are actively requesting missing parts/receipts.
    Requesting(RequestInfo),
    /// All needed parts and receipts have been received. Terminal state.
    Complete,
}

/// The escalation state of a chunk request. Transitions are driven by elapsed
/// time since the request was added.
///
///   RequestingFromProducer --> RequestingFromOthers --> FullFetch --> [evicted]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ChunkRequestState {
    /// Requesting from the original chunk producer only.
    RequestingFromProducer,
    /// Escalated: also requesting own parts from other validators.
    RequestingFromOthers,
    /// Escalated: requesting all parts from anyone (full fetch).
    FullFetch,
}

impl ChunkRequestState {
    pub fn force_request_full(&self) -> bool {
        matches!(self, ChunkRequestState::FullFetch)
    }

    pub fn request_own_parts_from_others(&self) -> bool {
        matches!(self, ChunkRequestState::RequestingFromOthers | ChunkRequestState::FullFetch)
    }
}

/// Metadata for an active chunk request.
#[derive(Clone, Debug)]
pub(crate) struct RequestInfo {
    /// Hash of an ancestor block in the same epoch that is accepted.
    /// Used for epoch_id resolution and archival checks.
    pub ancestor_hash: CryptoHash,
    /// When this request was first added to the pool.
    pub added: time::Instant,
    /// When the last network request was sent.
    pub last_requested: time::Instant,
}

/// A decision returned by tick() telling the caller what network action to take.
#[derive(Debug)]
pub(crate) struct ChunkSendRequest {
    pub chunk_hash: ChunkHash,
    pub height: BlockHeight,
    pub ancestor_hash: CryptoHash,
    pub shard_id: ShardId,
    pub force_request_full: bool,
    pub request_own_parts_from_others: bool,
    pub request_from_archival: bool,
}

impl EncodedChunksCache {
    pub fn new(height_horizon: BlockHeightDelta) -> Self {
        EncodedChunksCache {
            largest_seen_height: 0,
            height_horizon,
            encoded_chunks: HashMap::new(),
            height_map: HashMap::new(),
            height_to_shard_to_chunk: HashMap::new(),
            incomplete_chunks: HashMap::new(),
        }
    }

    pub fn get(&self, chunk_hash: &ChunkHash) -> Option<&EncodedChunksCacheEntry> {
        self.encoded_chunks.get(chunk_hash)
    }

    /// Mark an entry as complete. Drops any RequestInfo automatically.
    /// Removes the chunk from the incomplete_chunks index.
    pub fn mark_complete(&mut self, chunk_hash: &ChunkHash) {
        if let Some(entry) = self.encoded_chunks.get_mut(chunk_hash) {
            entry.state = ChunkState::Complete;
            let previous_block_hash = entry.header.prev_block_hash().clone();
            self.remove_chunk_from_incomplete_chunks(&previous_block_hash, chunk_hash);
        } else {
            tracing::warn!(target: "chunks", ?chunk_hash, "cannot mark non-existent entry as complete");
        }
    }

    /// Transition an entry to Requesting state with the given request metadata.
    /// Only transitions from Receiving; entries already in Requesting, Complete,
    /// or not in the cache are left unchanged. Returns true if the transition occurred.
    pub fn start_requesting(&mut self, chunk_hash: &ChunkHash, request_info: RequestInfo) -> bool {
        match self.encoded_chunks.get_mut(chunk_hash) {
            Some(entry) => match &entry.state {
                ChunkState::Receiving => {
                    entry.state = ChunkState::Requesting(request_info);
                    true
                }
                ChunkState::Requesting(_) | ChunkState::Complete => false,
            },
            None => false,
        }
    }

    pub fn get_request_info(&self, chunk_hash: &ChunkHash) -> Option<&RequestInfo> {
        self.encoded_chunks.get(chunk_hash)?.request_info()
    }

    pub fn is_requesting(&self, chunk_hash: &ChunkHash) -> bool {
        self.encoded_chunks.get(chunk_hash).is_some_and(|e| e.is_requesting())
    }

    pub fn requesting_count(&self) -> usize {
        self.encoded_chunks.values().filter(|e| e.is_requesting()).count()
    }

    /// Called periodically; returns the set of requests to send now.
    /// Iterates all cache entries, advancing escalation state for Requesting
    /// entries and evicting expired ones.
    pub fn tick(
        &mut self,
        now: time::Instant,
        chain_header_head: &Tip,
        epoch_manager: &dyn EpochManagerAdapter,
    ) -> Vec<ChunkSendRequest> {
        let mut actions = Vec::new();
        let mut to_evict = Vec::new();

        for (chunk_hash, entry) in &mut self.encoded_chunks {
            let info = match &mut entry.state {
                ChunkState::Requesting(info) => info,
                _ => continue,
            };

            if now - info.added >= CHUNK_REQUEST_RETRY_MAX {
                tracing::debug!(target: "chunks", ?chunk_hash, shard_id = %entry.header.shard_id(), "evicted chunk requested that was never fetched");
                to_evict.push(chunk_hash.clone());
                continue;
            }

            if now - info.last_requested < CHUNK_REQUEST_RETRY {
                continue;
            }
            info.last_requested = now;

            let mut state = advance_state(now - info.added);
            let (is_old, fetch_from_archival) = staleness_and_archival(
                epoch_manager,
                &info.ancestor_hash,
                entry.header.prev_block_hash(),
                chain_header_head,
            );

            if is_old && state == ChunkRequestState::RequestingFromProducer {
                state = ChunkRequestState::RequestingFromOthers;
            }

            actions.push(ChunkSendRequest {
                chunk_hash: chunk_hash.clone(),
                height: entry.header.height_created(),
                ancestor_hash: info.ancestor_hash,
                shard_id: entry.header.shard_id(),
                force_request_full: state.force_request_full(),
                request_own_parts_from_others: state.request_own_parts_from_others(),
                request_from_archival: fetch_from_archival,
            });
        }

        for chunk_hash in to_evict {
            self.remove(&chunk_hash);
        }

        actions
    }

    pub fn mark_received_all_receipts(&mut self, chunk_hash: &ChunkHash) {
        let Some(entry) = self.encoded_chunks.get_mut(chunk_hash) else {
            return;
        };
        if entry.received_all_receipts {
            return;
        }
        let time_to_last_receipt = Instant::now().signed_duration_since(entry.created_at);
        metrics::PARTIAL_CHUNK_TIME_TO_LAST_RECEIPT_PART_SECONDS
            .with_label_values(&[entry.header.shard_id().to_string().as_str()])
            .observe(time_to_last_receipt.as_seconds_f64());
        entry.received_all_receipts = true;
    }

    pub fn mark_received_all_parts(&mut self, chunk_hash: &ChunkHash) {
        let Some(entry) = self.encoded_chunks.get_mut(chunk_hash) else {
            return;
        };
        if entry.received_all_parts {
            return;
        }
        let time_to_last_part = Instant::now().signed_duration_since(entry.created_at);
        metrics::PARTIAL_CHUNK_TIME_TO_LAST_CHUNK_PART_SECONDS
            .with_label_values(&[entry.header.shard_id().to_string().as_str()])
            .observe(time_to_last_part.as_seconds_f64());
        entry.received_all_parts = true;
    }

    pub fn mark_can_reconstruct(&mut self, chunk_hash: &ChunkHash) {
        let Some(entry) = self.encoded_chunks.get_mut(chunk_hash) else {
            return;
        };
        if entry.could_reconstruct {
            return;
        }
        let time_to_reconstruct = Instant::now().signed_duration_since(entry.created_at);
        metrics::PARTIAL_CHUNK_TIME_TO_RECONSTRUCT_SECONDS
            .with_label_values(&[entry.header.shard_id().to_string().as_str()])
            .observe(time_to_reconstruct.as_seconds_f64());
        entry.could_reconstruct = true;
    }

    pub fn mark_entry_validated(&mut self, chunk_hash: &ChunkHash) {
        if let Some(entry) = self.encoded_chunks.get_mut(chunk_hash) {
            entry.header_fully_validated = true;
        } else {
            tracing::warn!(?chunk_hash, "no entry exist");
        }
    }

    /// Get a list of incomplete chunks whose previous block hash is `prev_block_hash`
    pub fn get_incomplete_chunks(
        &self,
        prev_block_hash: &CryptoHash,
    ) -> Option<&HashSet<ChunkHash>> {
        self.incomplete_chunks.get(prev_block_hash)
    }

    pub fn remove(&mut self, chunk_hash: &ChunkHash) -> Option<EncodedChunksCacheEntry> {
        if let Some(entry) = self.encoded_chunks.remove(chunk_hash) {
            self.remove_chunk_from_incomplete_chunks(entry.header.prev_block_hash(), chunk_hash);
            Some(entry)
        } else {
            None
        }
    }

    // Remove the chunk from the `incomplete_chunks` map. This is an internal function.
    // Use `mark_entry_complete` instead for outside calls
    fn remove_chunk_from_incomplete_chunks(
        &mut self,
        prev_block_hash: &CryptoHash,
        chunk_hash: &ChunkHash,
    ) {
        if let Occupied(mut entry) = self.incomplete_chunks.entry(*prev_block_hash) {
            entry.get_mut().remove(chunk_hash);
            if entry.get().is_empty() {
                entry.remove();
            }
        }
    }

    // Create an empty entry from the header and insert it if there is no entry for the chunk already
    // Return a mutable reference to the entry
    pub fn get_or_insert_from_header(
        &mut self,
        chunk_header: &ShardChunkHeader,
    ) -> &mut EncodedChunksCacheEntry {
        let chunk_hash = chunk_header.chunk_hash();
        self.encoded_chunks.entry(chunk_hash.clone()).or_insert_with_key(|chunk_hash| {
            self.height_map
                .entry(chunk_header.height_created())
                .or_default()
                .insert(chunk_hash.clone());
            self.height_to_shard_to_chunk
                .entry(chunk_header.height_created())
                .or_default()
                .insert(chunk_header.shard_id(), chunk_hash.clone());
            self.incomplete_chunks
                .entry(*chunk_header.prev_block_hash())
                .or_default()
                .insert(chunk_hash.clone());
            EncodedChunksCacheEntry::from_chunk_header(chunk_header.clone())
        })
    }

    pub fn height_within_front_horizon(&self, height: BlockHeight) -> bool {
        height >= self.largest_seen_height && height <= self.largest_seen_height + MAX_HEIGHTS_AHEAD
    }

    pub fn height_within_rear_horizon(&self, height: BlockHeight) -> bool {
        height + self.height_horizon >= self.largest_seen_height
            && height <= self.largest_seen_height
    }

    pub fn height_within_horizon(&self, height: BlockHeight) -> bool {
        self.height_within_front_horizon(height) || self.height_within_rear_horizon(height)
    }

    pub fn get_chunk_hash_by_height_and_shard(
        &self,
        height: BlockHeight,
        shard_id: ShardId,
    ) -> Option<&ChunkHash> {
        self.height_to_shard_to_chunk.get(&height)?.get(&shard_id)
    }

    /// Add parts and receipts stored in a partial encoded chunk to the corresponding chunk entry,
    /// returning the set of part ords that were previously unknown.
    pub fn merge_in_partial_encoded_chunk(
        &mut self,
        chunk_header: &ShardChunkHeader,
        parts: impl Iterator<Item = PartialEncodedChunkPart>,
        receipts: impl Iterator<Item = ReceiptProof>,
    ) -> HashSet<u64> {
        let entry = self.get_or_insert_from_header(chunk_header);
        entry.merge_in_partial_encoded_chunk(parts, receipts)
    }

    /// Remove a chunk from the cache if it is outside of horizon
    pub fn remove_from_cache_if_outside_horizon(&mut self, chunk_hash: &ChunkHash) {
        if let Some(entry) = self.encoded_chunks.get(chunk_hash) {
            let height = entry.header.height_created();
            if !self.height_within_horizon(height) {
                self.remove(&chunk_hash);
            }
        }
    }

    /// Update largest seen height and removes chunks from the cache that are outside of horizon
    pub fn update_largest_seen_height(&mut self, new_height: BlockHeight) {
        let old_largest_seen_height = self.largest_seen_height;
        self.largest_seen_height = new_height;
        for height in old_largest_seen_height.saturating_sub(self.height_horizon)
            ..self.largest_seen_height.saturating_sub(self.height_horizon)
        {
            if let Some(chunks_to_remove) = self.height_map.remove(&height) {
                for chunk_hash in chunks_to_remove {
                    if self.encoded_chunks.get(&chunk_hash).is_some_and(|e| e.is_requesting()) {
                        continue;
                    }
                    self.remove(&chunk_hash);
                }
            }
            self.height_to_shard_to_chunk.remove(&height);
        }
    }

    /// Marks the chunk for inclusion in a block; returns true if we haven't already
    /// called for this chunk. Requires that the chunk is already in the cache.
    pub fn mark_chunk_for_inclusion(&mut self, chunk_hash: &ChunkHash) -> bool {
        let entry = self.encoded_chunks.get_mut(chunk_hash).unwrap();
        if entry.ready_for_inclusion {
            false
        } else {
            entry.ready_for_inclusion = true;
            true
        }
    }
}

/// Determine the current escalation state based on elapsed time since request was added.
pub(crate) fn advance_state(elapsed: std::time::Duration) -> ChunkRequestState {
    if elapsed >= CHUNK_REQUEST_SWITCH_TO_FULL_FETCH {
        ChunkRequestState::FullFetch
    } else if elapsed >= CHUNK_REQUEST_SWITCH_TO_OTHERS {
        ChunkRequestState::RequestingFromOthers
    } else {
        ChunkRequestState::RequestingFromProducer
    }
}

/// Compute whether the chunk's block is old (more than one block behind the tip)
/// and whether the request needs an archival node. Returns `(is_old, fetch_from_archival)`.
pub(crate) fn staleness_and_archival(
    epoch_manager: &dyn EpochManagerAdapter,
    ancestor_hash: &CryptoHash,
    prev_block_hash: &CryptoHash,
    tip: &Tip,
) -> (bool, bool) {
    let fetch_from_archival = chunk_needs_to_be_fetched_from_archival(
        ancestor_hash,
        &tip.last_block_hash,
        epoch_manager,
    )
    .unwrap_or_else(|err| {
        debug_assert!(false);
        tracing::error!(target: "chunks", ?err, "cannot determine whether to request chunk from archival node, defaulting to not");
        false
    });
    let is_old = tip.last_block_hash != *prev_block_hash && tip.prev_block_hash != *prev_block_hash;
    (is_old, fetch_from_archival)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use near_crypto::KeyType;
    use near_primitives::hash::CryptoHash;
    use near_primitives::sharding::{ShardChunkHeader, ShardChunkHeaderV2};
    use near_primitives::types::Balance;
    use near_primitives::types::{Gas, ShardId};
    use near_primitives::validator_signer::InMemoryValidatorSigner;

    fn create_chunk_header(height: u64, shard_id: ShardId) -> ShardChunkHeader {
        let signer =
            InMemoryValidatorSigner::from_random("test".parse().unwrap(), KeyType::ED25519);
        ShardChunkHeader::V2(ShardChunkHeaderV2::new(
            CryptoHash::default(),
            CryptoHash::default(),
            CryptoHash::default(),
            CryptoHash::default(),
            1,
            height,
            shard_id,
            Gas::ZERO,
            Gas::ZERO,
            Balance::ZERO,
            CryptoHash::default(),
            CryptoHash::default(),
            vec![],
            &signer,
        ))
    }

    #[test]
    fn test_incomplete_chunks() {
        let mut cache = EncodedChunksCache::new(DEFAULT_CHUNKS_CACHE_HEIGHT_HORIZON);
        let header0 = create_chunk_header(1, ShardId::new(0));
        let header1 = create_chunk_header(1, ShardId::new(1));
        cache.get_or_insert_from_header(&header0);
        cache.merge_in_partial_encoded_chunk(
            &header1,
            Vec::new().into_iter(),
            Vec::new().into_iter(),
        );
        assert_eq!(
            cache.get_incomplete_chunks(&CryptoHash::default()).unwrap(),
            &HashSet::from([header0.chunk_hash().clone(), header1.chunk_hash().clone()])
        );
        cache.mark_complete(&header0.chunk_hash());
        assert_eq!(
            cache.get_incomplete_chunks(&CryptoHash::default()).unwrap(),
            &[header1.chunk_hash().clone()].into_iter().collect::<HashSet<_>>()
        );
        cache.mark_complete(&header1.chunk_hash());
        assert_eq!(cache.get_incomplete_chunks(&CryptoHash::default()), None);
    }

    #[test]
    fn test_cache_removal() {
        let mut cache = EncodedChunksCache::new(DEFAULT_CHUNKS_CACHE_HEIGHT_HORIZON);
        let header = create_chunk_header(1, ShardId::new(0));
        cache.merge_in_partial_encoded_chunk(
            &header,
            Vec::new().into_iter(),
            Vec::new().into_iter(),
        );
        assert!(!cache.height_map.is_empty());

        cache.update_largest_seen_height(2000);
        assert!(cache.encoded_chunks.is_empty());
        assert!(cache.height_map.is_empty());
    }

    #[test]
    fn test_mark_complete_drops_request_info() {
        let mut cache = EncodedChunksCache::new(DEFAULT_CHUNKS_CACHE_HEIGHT_HORIZON);
        let header = create_chunk_header(1, ShardId::new(0));
        let chunk_hash = header.chunk_hash();
        cache.get_or_insert_from_header(&header);

        let now = time::Instant::now();
        cache.start_requesting(
            &chunk_hash,
            RequestInfo { ancestor_hash: CryptoHash::default(), added: now, last_requested: now },
        );
        assert!(cache.is_requesting(&chunk_hash));

        cache.mark_complete(&chunk_hash);
        assert!(!cache.is_requesting(&chunk_hash));
        assert!(cache.get(&chunk_hash).unwrap().is_complete());
        assert!(cache.get_request_info(&chunk_hash).is_none());
    }

    #[test]
    fn test_remove_drops_request_info() {
        let mut cache = EncodedChunksCache::new(DEFAULT_CHUNKS_CACHE_HEIGHT_HORIZON);
        let header = create_chunk_header(1, ShardId::new(0));
        let chunk_hash = header.chunk_hash();
        cache.get_or_insert_from_header(&header);

        let now = time::Instant::now();
        cache.start_requesting(
            &chunk_hash,
            RequestInfo { ancestor_hash: CryptoHash::default(), added: now, last_requested: now },
        );
        assert!(cache.is_requesting(&chunk_hash));
        assert_eq!(cache.requesting_count(), 1);

        cache.remove(&chunk_hash);
        assert!(!cache.is_requesting(&chunk_hash));
        assert_eq!(cache.requesting_count(), 0);
    }

    #[test]
    fn test_start_requesting_only_from_receiving() {
        let mut cache = EncodedChunksCache::new(DEFAULT_CHUNKS_CACHE_HEIGHT_HORIZON);
        let header = create_chunk_header(1, ShardId::new(0));
        let chunk_hash = header.chunk_hash();
        cache.get_or_insert_from_header(&header);

        let now = time::Instant::now();
        let info =
            RequestInfo { ancestor_hash: CryptoHash::default(), added: now, last_requested: now };

        // Receiving -> Requesting works
        assert!(cache.start_requesting(&chunk_hash, info.clone()));
        assert!(cache.is_requesting(&chunk_hash));

        // Requesting -> Requesting doesn't overwrite
        assert!(!cache.start_requesting(&chunk_hash, info.clone()));

        // Complete -> Requesting doesn't work
        cache.mark_complete(&chunk_hash);
        assert!(!cache.start_requesting(&chunk_hash, info));
    }

    #[test]
    fn test_gc_skips_requesting_chunks() {
        let mut cache = EncodedChunksCache::new(DEFAULT_CHUNKS_CACHE_HEIGHT_HORIZON);
        let header = create_chunk_header(1, ShardId::new(0));
        let chunk_hash = header.chunk_hash();
        cache.get_or_insert_from_header(&header);

        let now = time::Instant::now();
        cache.start_requesting(
            &chunk_hash,
            RequestInfo { ancestor_hash: CryptoHash::default(), added: now, last_requested: now },
        );

        cache.update_largest_seen_height(2000);
        assert!(cache.get(&chunk_hash).is_some());
    }
}
