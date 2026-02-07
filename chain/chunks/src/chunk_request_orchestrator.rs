use crate::logic::chunk_needs_to_be_fetched_from_archival;
use near_async::time::{self, Clock};
use near_chain::types::EpochManagerAdapter;
use near_primitives::block::Tip;
use near_primitives::errors::EpochError;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::{ChunkHash, ShardChunkHeader};
use near_primitives::stateless_validation::ChunkProductionKey;
use near_primitives::types::{AccountId, BlockHeight, BlockHeightDelta, ShardId};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub const CHUNK_REQUEST_RETRY: time::Duration = time::Duration::milliseconds(100);
pub const CHUNK_REQUEST_SWITCH_TO_OTHERS: time::Duration = time::Duration::milliseconds(400);
pub const CHUNK_REQUEST_SWITCH_TO_FULL_FETCH: time::Duration = time::Duration::seconds(3);
pub(crate) const CHUNK_REQUEST_RETRY_MAX: time::Duration = time::Duration::seconds(1000);
// Only request chunks from peers whose latest height >= chunk_height - CHUNK_REQUEST_PEER_HORIZON
pub(crate) const CHUNK_REQUEST_PEER_HORIZON: BlockHeightDelta = 5;

/// The escalation state of a chunk request. Transitions are driven by elapsed
/// time since the request was added.
///
///   WaitingForForwarding --> RequestingFromProducer --> RequestingFromOthers --> FullFetch --> [evicted]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ChunkRequestState {
    /// Waiting for chunk parts to be forwarded before sending any requests.
    WaitingForForwarding,
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

    pub fn should_send_request(&self) -> bool {
        !matches!(self, ChunkRequestState::WaitingForForwarding)
    }
}

/// A decision returned by the orchestrator telling ShardsManagerActor what
/// network action to take.
#[derive(Debug)]
pub(crate) enum ChunkRequestAction {
    /// Send a partial encoded chunk request.
    SendRequest {
        chunk_hash: ChunkHash,
        height: BlockHeight,
        ancestor_hash: CryptoHash,
        shard_id: ShardId,
        state: ChunkRequestState,
        request_from_archival: bool,
    },
    /// No action needed.
    None,
}

#[derive(Clone, Debug)]
pub(crate) struct ChunkRequestInfo {
    pub height: BlockHeight,
    // hash of the ancestor hash used for the request, i.e., the first block up the
    // parent chain of the block that has missing chunks that is approved
    pub ancestor_hash: CryptoHash,
    // previous block hash of the chunk
    pub prev_block_hash: CryptoHash,
    pub shard_id: ShardId,
    pub added: time::Instant,
    pub last_requested: time::Instant,
}

pub(crate) struct RequestPool {
    pub retry_duration: time::Duration,
    pub switch_to_others_duration: time::Duration,
    pub switch_to_full_fetch_duration: time::Duration,
    pub max_duration: time::Duration,
    pub requests: HashMap<ChunkHash, ChunkRequestInfo>,
}

impl RequestPool {
    pub fn new(
        retry_duration: time::Duration,
        switch_to_others_duration: time::Duration,
        switch_to_full_fetch_duration: time::Duration,
        max_duration: time::Duration,
    ) -> Self {
        Self {
            retry_duration,
            switch_to_others_duration,
            switch_to_full_fetch_duration,
            max_duration,
            requests: HashMap::default(),
        }
    }
    pub fn contains_key(&self, chunk_hash: &ChunkHash) -> bool {
        self.requests.contains_key(chunk_hash)
    }
    pub fn len(&self) -> usize {
        self.requests.len()
    }

    pub fn insert(&mut self, chunk_hash: ChunkHash, chunk_request: ChunkRequestInfo) {
        self.requests.insert(chunk_hash, chunk_request);
    }

    pub fn get_request_info(&self, chunk_hash: &ChunkHash) -> Option<&ChunkRequestInfo> {
        self.requests.get(chunk_hash)
    }

    pub fn remove(&mut self, chunk_hash: &ChunkHash) {
        self.requests.remove(chunk_hash);
    }

    pub fn fetch(&mut self, current_time: time::Instant) -> Vec<(ChunkHash, ChunkRequestInfo)> {
        let mut removed_requests = HashSet::<ChunkHash>::default();
        let mut requests = Vec::new();
        for (chunk_hash, chunk_request) in &mut self.requests {
            if current_time - chunk_request.added >= self.max_duration {
                tracing::debug!(target: "chunks", ?chunk_hash, shard_id = %chunk_request.shard_id, "evicted chunk requested that was never fetched");
                removed_requests.insert(chunk_hash.clone());
                continue;
            }
            if current_time - chunk_request.last_requested >= self.retry_duration {
                chunk_request.last_requested = current_time;
                requests.push((chunk_hash.clone(), chunk_request.clone()));
            }
        }
        for chunk_hash in removed_requests {
            self.requests.remove(&chunk_hash);
        }
        requests
    }
}

/// Orchestrates chunk request lifecycle: tracking which chunks are needed,
/// managing the request pool, and deciding when/how to escalate requests.
pub(crate) struct ChunkRequestOrchestrator {
    clock: Clock,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    pool: RequestPool,
}

impl ChunkRequestOrchestrator {
    pub fn new(clock: Clock, epoch_manager: Arc<dyn EpochManagerAdapter>) -> Self {
        Self {
            clock,
            epoch_manager,
            pool: RequestPool::new(
                CHUNK_REQUEST_RETRY,
                CHUNK_REQUEST_SWITCH_TO_OTHERS,
                CHUNK_REQUEST_SWITCH_TO_FULL_FETCH,
                CHUNK_REQUEST_RETRY_MAX,
            ),
        }
    }

    pub fn contains(&self, chunk_hash: &ChunkHash) -> bool {
        self.pool.contains_key(chunk_hash)
    }

    pub fn get_request_info(&self, chunk_hash: &ChunkHash) -> Option<&ChunkRequestInfo> {
        self.pool.get_request_info(chunk_hash)
    }

    pub fn insert(&mut self, chunk_hash: ChunkHash, chunk_request: ChunkRequestInfo) {
        self.pool.insert(chunk_hash, chunk_request);
    }

    pub fn remove(&mut self, chunk_hash: &ChunkHash) {
        self.pool.remove(chunk_hash);
    }

    pub fn pending_count(&self) -> usize {
        self.pool.len()
    }

    pub fn requests(&self) -> &HashMap<ChunkHash, ChunkRequestInfo> {
        &self.pool.requests
    }

    /// Called periodically; returns the set of requests to send now.
    /// Advances state for each request based on elapsed time and evicts expired ones.
    pub fn tick(&mut self, chain_header_head: &Tip) -> Vec<ChunkRequestAction> {
        let requests = self.pool.fetch(self.clock.now().into());
        let mut actions = Vec::with_capacity(requests.len());
        for (chunk_hash, chunk_request) in requests {
            let mut state = self.advance_state(&chunk_request);

            let fetch_from_archival = chunk_needs_to_be_fetched_from_archival(
                &chunk_request.ancestor_hash,
                &chain_header_head.last_block_hash,
                self.epoch_manager.as_ref(),
            )
            .unwrap_or_else(|err| {
                debug_assert!(false);
                tracing::error!(target: "chunks", ?err, "error during re-requesting partial encoded chunk, cannot determine whether to request from an archival node, defaulting to not");
                false
            });

            let old_block = chain_header_head.last_block_hash != chunk_request.prev_block_hash
                && chain_header_head.prev_block_hash != chunk_request.prev_block_hash;

            // old_block implies at least RequestingFromOthers (request_own_parts_from_others = true)
            if old_block && state == ChunkRequestState::RequestingFromProducer {
                state = ChunkRequestState::RequestingFromOthers;
            }

            actions.push(ChunkRequestAction::SendRequest {
                chunk_hash,
                height: chunk_request.height,
                ancestor_hash: chunk_request.ancestor_hash,
                shard_id: chunk_request.shard_id,
                state,
                request_from_archival: fetch_from_archival,
            });
        }
        actions
    }

    /// Determine the current escalation state based on elapsed time since request was added.
    fn advance_state(&self, info: &ChunkRequestInfo) -> ChunkRequestState {
        let elapsed = self.clock.now() - info.added;
        if elapsed >= self.pool.switch_to_full_fetch_duration {
            ChunkRequestState::FullFetch
        } else if elapsed >= self.pool.switch_to_others_duration {
            ChunkRequestState::RequestingFromOthers
        } else {
            ChunkRequestState::RequestingFromProducer
        }
    }

    /// Record that we need this chunk. Returns the action to take now (if any).
    ///
    /// `chunk_header`: the chunk being requested
    /// `ancestor_hash`: hash of an ancestor block of the requested chunk (same epoch, processed)
    /// `mark_only`: if true, only add the request to the pool, don't send it
    /// `is_chunk_complete`: closure returning whether the chunk is already complete in cache,
    ///                      or None if the chunk is not in cache at all
    /// `chain_header_head`: current header head tip
    /// `me`: this node's account id, if it is a validator
    pub fn track_needed_chunk(
        &mut self,
        chunk_header: &ShardChunkHeader,
        ancestor_hash: CryptoHash,
        mark_only: bool,
        is_chunk_complete: impl FnOnce(&ChunkHash) -> Option<bool>,
        chain_header_head: &Tip,
        me: Option<&AccountId>,
    ) -> ChunkRequestAction {
        let height = chunk_header.height_created();
        let shard_id = chunk_header.shard_id();
        let chunk_hash = chunk_header.chunk_hash().clone();

        if self.pool.contains_key(&chunk_hash) {
            tracing::debug!(target: "chunks", height, %shard_id, ?chunk_hash, "not requesting chunk, already being requested");
            return ChunkRequestAction::None;
        }

        match is_chunk_complete(&chunk_hash) {
            Some(true) => {
                tracing::debug!(target: "chunks", height, %shard_id, ?chunk_hash, "not requesting chunk, already complete");
                return ChunkRequestAction::None;
            }
            Some(false) => {
                // chunk is in cache but not complete, proceed
            }
            None => {
                // Not in cache at all. In all code paths that lead here, the header was already
                // inserted. If missing, it was completed and GC-ed.
                tracing::debug!(target: "chunks", height, %shard_id, ?chunk_hash, "not requesting chunk, already complete and GC-ed");
                return ChunkRequestAction::None;
            }
        }

        let prev_block_hash = *chunk_header.prev_block_hash();
        let now = self.clock.now().into();
        self.pool.insert(
            chunk_hash.clone(),
            ChunkRequestInfo {
                height,
                prev_block_hash,
                ancestor_hash,
                shard_id,
                last_requested: now,
                added: now,
            },
        );

        if mark_only {
            tracing::debug!(target: "chunks", height, %shard_id, ?chunk_hash, "marked the chunk as being requested but did not send the request yet");
            return ChunkRequestAction::None;
        }

        let fetch_from_archival = chunk_needs_to_be_fetched_from_archival(
            &ancestor_hash,
            &chain_header_head.last_block_hash,
            self.epoch_manager.as_ref(),
        )
        .unwrap_or_else(|err| {
            tracing::error!(target: "chunks", ?err, "error during requesting partial encoded chunk, cannot determine whether to request from an archival node, defaulting to not");
            false
        });
        let old_block = chain_header_head.last_block_hash != prev_block_hash
            && chain_header_head.prev_block_hash != prev_block_hash;

        let should_wait = self
            .should_wait_for_chunk_forwarding(
                &ancestor_hash,
                shard_id,
                height + 1,
                me,
            )
            .unwrap_or_else(|_| {
                debug_assert!(false, "{:?} must be accepted", ancestor_hash);
                tracing::error!(target: "chunks", ?ancestor_hash, "requesting chunk whose ancestor_hash is not accepted");
                false
            });

        let initial_state = if should_wait && !fetch_from_archival && !old_block {
            ChunkRequestState::WaitingForForwarding
        } else if old_block {
            ChunkRequestState::RequestingFromOthers
        } else {
            ChunkRequestState::RequestingFromProducer
        };

        if !initial_state.should_send_request() {
            tracing::debug!(target: "chunks", ?initial_state, "delaying the chunk request");
            return ChunkRequestAction::None;
        }

        tracing::debug!(target: "chunks", height, %shard_id, ?chunk_hash, "requesting");
        ChunkRequestAction::SendRequest {
            chunk_hash,
            height,
            ancestor_hash,
            shard_id,
            state: initial_state,
            request_from_archival: fetch_from_archival,
        }
    }

    /// Check whether the node should wait for chunk parts being forwarded to it.
    /// The node will wait if it's a block producer or a chunk producer that is responsible
    /// for producing the next chunk in this shard.
    pub fn should_wait_for_chunk_forwarding(
        &self,
        prev_hash: &CryptoHash,
        shard_id: ShardId,
        next_chunk_height: BlockHeight,
        me: Option<&AccountId>,
    ) -> Result<bool, EpochError> {
        // chunks will not be forwarded to non-validators
        let me = match me {
            None => return Ok(false),
            Some(it) => it,
        };
        let epoch_id = self.epoch_manager.get_epoch_id_from_prev_block(prev_hash)?;
        let block_producers = self.epoch_manager.get_epoch_block_producers_ordered(&epoch_id)?;
        for bp in block_producers {
            if bp.account_id() == me {
                return Ok(true);
            }
        }
        let chunk_producer = self
            .epoch_manager
            .get_chunk_producer_info(&ChunkProductionKey {
                epoch_id,
                height_created: next_chunk_height,
                shard_id,
            })?
            .take_account_id();
        if &chunk_producer == me {
            return Ok(true);
        }
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use near_async::time::FakeClock;
    use near_epoch_manager::test_utils::setup_epoch_manager_with_block_and_chunk_producers;
    use near_primitives::hash::hash;
    use near_primitives::sharding::ShardChunkHeaderV3;
    use near_primitives::types::EpochId;
    use near_store::test_utils::create_test_store;

    fn test_epoch_manager() -> Arc<dyn EpochManagerAdapter> {
        let store = create_test_store();
        let epoch_manager = setup_epoch_manager_with_block_and_chunk_producers(
            store,
            vec!["test".parse().unwrap()],
            vec![],
            1,
            2,
        );
        Arc::new(epoch_manager.into_handle())
    }

    fn default_tip() -> Tip {
        Tip {
            height: 0,
            last_block_hash: CryptoHash::default(),
            prev_block_hash: CryptoHash::default(),
            epoch_id: EpochId::default(),
            next_epoch_id: EpochId::default(),
        }
    }

    fn insert_test_request(
        orchestrator: &mut ChunkRequestOrchestrator,
        chunk_hash: ChunkHash,
        added: time::Instant,
    ) {
        orchestrator.insert(
            chunk_hash,
            ChunkRequestInfo {
                height: 0,
                ancestor_hash: CryptoHash::default(),
                prev_block_hash: CryptoHash::default(),
                shard_id: ShardId::new(0),
                added,
                last_requested: added,
            },
        );
    }

    #[test]
    fn test_state_force_request_full() {
        assert!(!ChunkRequestState::WaitingForForwarding.force_request_full());
        assert!(!ChunkRequestState::RequestingFromProducer.force_request_full());
        assert!(!ChunkRequestState::RequestingFromOthers.force_request_full());
        assert!(ChunkRequestState::FullFetch.force_request_full());
    }

    #[test]
    fn test_state_request_own_parts_from_others() {
        assert!(!ChunkRequestState::WaitingForForwarding.request_own_parts_from_others());
        assert!(!ChunkRequestState::RequestingFromProducer.request_own_parts_from_others());
        assert!(ChunkRequestState::RequestingFromOthers.request_own_parts_from_others());
        assert!(ChunkRequestState::FullFetch.request_own_parts_from_others());
    }

    #[test]
    fn test_state_should_send_request() {
        assert!(!ChunkRequestState::WaitingForForwarding.should_send_request());
        assert!(ChunkRequestState::RequestingFromProducer.should_send_request());
        assert!(ChunkRequestState::RequestingFromOthers.should_send_request());
        assert!(ChunkRequestState::FullFetch.should_send_request());
    }

    #[test]
    fn test_tick_requesting_from_producer() {
        let clock = FakeClock::default();
        let epoch_manager = test_epoch_manager();
        let mut orchestrator = ChunkRequestOrchestrator::new(clock.clock(), epoch_manager);
        let tip = default_tip();

        let chunk_hash = ChunkHash(hash(&[1]));
        insert_test_request(&mut orchestrator, chunk_hash.clone(), clock.now().into());

        // Advance past retry duration but before switch_to_others (100ms < t < 400ms).
        clock.advance(CHUNK_REQUEST_RETRY + time::Duration::milliseconds(50));
        let actions = orchestrator.tick(&tip);
        assert_eq!(actions.len(), 1);
        assert_matches!(&actions[0], ChunkRequestAction::SendRequest { state, .. } => {
            assert_eq!(*state, ChunkRequestState::RequestingFromProducer);
        });
    }

    #[test]
    fn test_tick_escalates_to_requesting_from_others() {
        let clock = FakeClock::default();
        let epoch_manager = test_epoch_manager();
        let mut orchestrator = ChunkRequestOrchestrator::new(clock.clock(), epoch_manager);
        let tip = default_tip();

        let chunk_hash = ChunkHash(hash(&[1]));
        insert_test_request(&mut orchestrator, chunk_hash.clone(), clock.now().into());

        // Advance past switch_to_others (400ms) but before switch_to_full_fetch (3s).
        clock.advance(CHUNK_REQUEST_SWITCH_TO_OTHERS + time::Duration::milliseconds(50));
        let actions = orchestrator.tick(&tip);
        assert_eq!(actions.len(), 1);
        assert_matches!(&actions[0], ChunkRequestAction::SendRequest { state, .. } => {
            assert_eq!(*state, ChunkRequestState::RequestingFromOthers);
        });
    }

    #[test]
    fn test_tick_escalates_to_full_fetch() {
        let clock = FakeClock::default();
        let epoch_manager = test_epoch_manager();
        let mut orchestrator = ChunkRequestOrchestrator::new(clock.clock(), epoch_manager);
        let tip = default_tip();

        let chunk_hash = ChunkHash(hash(&[1]));
        insert_test_request(&mut orchestrator, chunk_hash.clone(), clock.now().into());

        // Advance past switch_to_full_fetch (3s) but before max_duration (1000s).
        clock.advance(CHUNK_REQUEST_SWITCH_TO_FULL_FETCH + time::Duration::milliseconds(50));
        let actions = orchestrator.tick(&tip);
        assert_eq!(actions.len(), 1);
        assert_matches!(&actions[0], ChunkRequestAction::SendRequest { state, .. } => {
            assert_eq!(*state, ChunkRequestState::FullFetch);
        });
    }

    #[test]
    fn test_tick_evicts_after_max_duration() {
        let clock = FakeClock::default();
        let epoch_manager = test_epoch_manager();
        let mut orchestrator = ChunkRequestOrchestrator::new(clock.clock(), epoch_manager);
        let tip = default_tip();

        let chunk_hash = ChunkHash(hash(&[1]));
        insert_test_request(&mut orchestrator, chunk_hash.clone(), clock.now().into());

        // Advance past max_duration (1000s). Request should be evicted, not returned.
        clock.advance(CHUNK_REQUEST_RETRY_MAX + time::Duration::seconds(1));
        let actions = orchestrator.tick(&tip);
        assert!(actions.is_empty());
        assert!(!orchestrator.contains(&chunk_hash));
    }

    #[test]
    fn test_tick_respects_retry_interval() {
        let clock = FakeClock::default();
        let epoch_manager = test_epoch_manager();
        let mut orchestrator = ChunkRequestOrchestrator::new(clock.clock(), epoch_manager);
        let tip = default_tip();

        let chunk_hash = ChunkHash(hash(&[1]));
        insert_test_request(&mut orchestrator, chunk_hash.clone(), clock.now().into());

        // First tick after retry interval: should return the request.
        clock.advance(CHUNK_REQUEST_RETRY + time::Duration::milliseconds(1));
        let actions = orchestrator.tick(&tip);
        assert_eq!(actions.len(), 1);

        // Immediately tick again: should NOT return the request (retry interval not elapsed).
        let actions = orchestrator.tick(&tip);
        assert!(actions.is_empty());

        // Advance past another retry interval: should return again.
        clock.advance(CHUNK_REQUEST_RETRY + time::Duration::milliseconds(1));
        let actions = orchestrator.tick(&tip);
        assert_eq!(actions.len(), 1);
    }

    #[test]
    fn test_tick_old_block_promotes_to_requesting_from_others() {
        let clock = FakeClock::default();
        let epoch_manager = test_epoch_manager();
        let mut orchestrator = ChunkRequestOrchestrator::new(clock.clock(), epoch_manager);

        // Insert a request whose prev_block_hash does NOT match the tip.
        let chunk_hash = ChunkHash(hash(&[1]));
        let now = clock.now().into();
        orchestrator.insert(
            chunk_hash.clone(),
            ChunkRequestInfo {
                height: 0,
                ancestor_hash: CryptoHash::default(),
                prev_block_hash: hash(&[99]), // Different from tip
                shard_id: ShardId::new(0),
                added: now,
                last_requested: now,
            },
        );

        // Advance past retry but before switch_to_others. Without old_block, this would
        // be RequestingFromProducer.
        clock.advance(CHUNK_REQUEST_RETRY + time::Duration::milliseconds(50));

        let tip = default_tip();
        let actions = orchestrator.tick(&tip);
        assert_eq!(actions.len(), 1);
        assert_matches!(&actions[0], ChunkRequestAction::SendRequest { state, .. } => {
            // old_block should promote RequestingFromProducer → RequestingFromOthers.
            assert_eq!(*state, ChunkRequestState::RequestingFromOthers);
        });
    }

    fn test_chunk_header(
        epoch_manager: &dyn EpochManagerAdapter,
        height: BlockHeight,
    ) -> ShardChunkHeader {
        let epoch_id = EpochId::default();
        let shard_layout = epoch_manager.get_shard_layout(&epoch_id).unwrap();
        let shard_id = shard_layout.shard_ids().next().unwrap();
        ShardChunkHeader::V3(ShardChunkHeaderV3::new_dummy(height, shard_id, CryptoHash::default()))
    }

    #[test]
    fn test_track_needed_chunk_non_validator_skips_forwarding_wait() {
        let clock = FakeClock::default();
        let epoch_manager = test_epoch_manager();
        let mut orchestrator = ChunkRequestOrchestrator::new(clock.clock(), epoch_manager.clone());
        let tip = default_tip();

        let chunk_header = test_chunk_header(epoch_manager.as_ref(), 3);
        let chunk_hash = chunk_header.chunk_hash();

        // me=None: not a validator, should NOT wait for forwarding.
        let action = orchestrator.track_needed_chunk(
            &chunk_header,
            CryptoHash::default(),
            false,
            |_| Some(false), // in cache but not complete
            &tip,
            None, // not a validator
        );

        // Should immediately send request (RequestingFromProducer).
        assert_matches!(action, ChunkRequestAction::SendRequest { state, .. } => {
            assert_eq!(state, ChunkRequestState::RequestingFromProducer);
        });
        assert!(orchestrator.contains(&chunk_hash));
    }

    #[test]
    fn test_track_needed_chunk_validator_waits_for_forwarding() {
        let clock = FakeClock::default();
        let epoch_manager = test_epoch_manager();
        let mut orchestrator = ChunkRequestOrchestrator::new(clock.clock(), epoch_manager.clone());
        let tip = default_tip();

        let chunk_header = test_chunk_header(epoch_manager.as_ref(), 3);
        let chunk_hash = chunk_header.chunk_hash();
        let me: AccountId = "test".parse().unwrap();

        // me=Some("test"): a block producer, should wait for forwarding.
        let action = orchestrator.track_needed_chunk(
            &chunk_header,
            CryptoHash::default(),
            false,
            |_| Some(false),
            &tip,
            Some(&me),
        );

        // Should NOT send request yet (WaitingForForwarding → None).
        assert_matches!(action, ChunkRequestAction::None);
        // But should be tracked in the pool.
        assert!(orchestrator.contains(&chunk_hash));
    }

    #[test]
    fn test_track_needed_chunk_duplicate_returns_none() {
        let clock = FakeClock::default();
        let epoch_manager = test_epoch_manager();
        let mut orchestrator = ChunkRequestOrchestrator::new(clock.clock(), epoch_manager.clone());
        let tip = default_tip();

        let chunk_header = test_chunk_header(epoch_manager.as_ref(), 3);

        // First call: inserts.
        let _action = orchestrator.track_needed_chunk(
            &chunk_header,
            CryptoHash::default(),
            false,
            |_| Some(false),
            &tip,
            None,
        );

        // Second call with same chunk: should return None (already tracked).
        let action = orchestrator.track_needed_chunk(
            &chunk_header,
            CryptoHash::default(),
            false,
            |_| Some(false),
            &tip,
            None,
        );
        assert_matches!(action, ChunkRequestAction::None);
    }

    #[test]
    fn test_track_needed_chunk_complete_returns_none() {
        let clock = FakeClock::default();
        let epoch_manager = test_epoch_manager();
        let mut orchestrator = ChunkRequestOrchestrator::new(clock.clock(), epoch_manager.clone());
        let tip = default_tip();

        let chunk_header = test_chunk_header(epoch_manager.as_ref(), 3);

        // Chunk is already complete: should return None and not add to pool.
        let action = orchestrator.track_needed_chunk(
            &chunk_header,
            CryptoHash::default(),
            false,
            |_| Some(true), // complete
            &tip,
            None,
        );
        assert_matches!(action, ChunkRequestAction::None);
        assert!(!orchestrator.contains(&chunk_header.chunk_hash()));
    }

    #[test]
    fn test_track_needed_chunk_mark_only() {
        let clock = FakeClock::default();
        let epoch_manager = test_epoch_manager();
        let mut orchestrator = ChunkRequestOrchestrator::new(clock.clock(), epoch_manager.clone());
        let tip = default_tip();

        let chunk_header = test_chunk_header(epoch_manager.as_ref(), 3);

        // mark_only=true: should add to pool but return None.
        let action = orchestrator.track_needed_chunk(
            &chunk_header,
            CryptoHash::default(),
            true, // mark_only
            |_| Some(false),
            &tip,
            None,
        );
        assert_matches!(action, ChunkRequestAction::None);
        assert!(orchestrator.contains(&chunk_header.chunk_hash()));
    }

    #[test]
    fn test_remove_stops_tracking() {
        let clock = FakeClock::default();
        let epoch_manager = test_epoch_manager();
        let mut orchestrator = ChunkRequestOrchestrator::new(clock.clock(), epoch_manager);

        let chunk_hash = ChunkHash(hash(&[1]));
        insert_test_request(&mut orchestrator, chunk_hash.clone(), clock.now().into());
        assert!(orchestrator.contains(&chunk_hash));
        assert_eq!(orchestrator.pending_count(), 1);

        orchestrator.remove(&chunk_hash);
        assert!(!orchestrator.contains(&chunk_hash));
        assert_eq!(orchestrator.pending_count(), 0);
    }
}
