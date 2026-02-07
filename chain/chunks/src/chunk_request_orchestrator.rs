use crate::logic::chunk_needs_to_be_fetched_from_archival;
use near_async::time::{self, Clock};
use near_chain::types::EpochManagerAdapter;
use near_primitives::block::Tip;
use near_primitives::errors::EpochError;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::{ChunkHash, ShardChunkHeader};
use near_primitives::stateless_validation::ChunkProductionKey;
use near_primitives::types::{AccountId, BlockHeight, BlockHeightDelta, ShardId};
use std::collections::HashMap;
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

#[derive(Clone, Debug)]
pub(crate) struct ChunkRequestInfo {
    pub height: BlockHeight,
    // hash of the ancestor hash used for the request, i.e., the first block up the
    // parent chain of the block that has missing chunks that is approved
    pub ancestor_hash: CryptoHash,
    pub prev_block_hash: CryptoHash,
    pub shard_id: ShardId,
    pub added: time::Instant,
    pub last_requested: time::Instant,
}

/// Orchestrates chunk request lifecycle: tracking which chunks are needed,
/// managing the request pool, and deciding when/how to escalate requests.
pub(crate) struct ChunkRequestOrchestrator {
    clock: Clock,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    requests: HashMap<ChunkHash, ChunkRequestInfo>,
}

impl ChunkRequestOrchestrator {
    pub fn new(clock: Clock, epoch_manager: Arc<dyn EpochManagerAdapter>) -> Self {
        Self { clock, epoch_manager, requests: HashMap::default() }
    }

    pub fn contains(&self, chunk_hash: &ChunkHash) -> bool {
        self.requests.contains_key(chunk_hash)
    }

    pub fn get_request_info(&self, chunk_hash: &ChunkHash) -> Option<&ChunkRequestInfo> {
        self.requests.get(chunk_hash)
    }

    pub fn insert(&mut self, chunk_hash: ChunkHash, chunk_request: ChunkRequestInfo) {
        self.requests.insert(chunk_hash, chunk_request);
    }

    pub fn remove(&mut self, chunk_hash: &ChunkHash) {
        self.requests.remove(chunk_hash);
    }

    pub fn pending_count(&self) -> usize {
        self.requests.len()
    }

    pub fn requests(&self) -> &HashMap<ChunkHash, ChunkRequestInfo> {
        &self.requests
    }

    /// Called periodically; returns the set of requests to send now.
    /// Advances state for each request based on elapsed time and evicts expired ones.
    pub fn tick(&mut self, chain_header_head: &Tip) -> Vec<ChunkSendRequest> {
        let current_time: time::Instant = self.clock.now().into();

        self.requests.retain(|chunk_hash, chunk_request| {
            if current_time - chunk_request.added >= CHUNK_REQUEST_RETRY_MAX {
                tracing::debug!(target: "chunks", ?chunk_hash, shard_id = %chunk_request.shard_id, "evicted chunk requested that was never fetched");
                return false;
            }
            true
        });

        // Collect requests that are due for a retry.
        let mut due_requests = Vec::new();
        for (chunk_hash, chunk_request) in &mut self.requests {
            if current_time - chunk_request.last_requested >= CHUNK_REQUEST_RETRY {
                chunk_request.last_requested = current_time;
                due_requests.push((chunk_hash.clone(), chunk_request.clone()));
            }
        }

        let mut actions = Vec::with_capacity(due_requests.len());
        for (chunk_hash, chunk_request) in due_requests {
            let mut state = Self::advance_state(current_time - chunk_request.added);
            let (is_old, fetch_from_archival) = self.staleness_and_archival(
                &chunk_request.ancestor_hash,
                &chunk_request.prev_block_hash,
                chain_header_head,
            );

            if is_old && state == ChunkRequestState::RequestingFromProducer {
                state = ChunkRequestState::RequestingFromOthers;
            }

            actions.push(ChunkSendRequest {
                chunk_hash,
                height: chunk_request.height,
                ancestor_hash: chunk_request.ancestor_hash,
                shard_id: chunk_request.shard_id,
                force_request_full: state.force_request_full(),
                request_own_parts_from_others: state.request_own_parts_from_others(),
                request_from_archival: fetch_from_archival,
            });
        }
        actions
    }

    /// Compute whether the chunk's block is old (more than one block behind the tip)
    /// and whether the request needs an archival node. Returns `(is_old, fetch_from_archival)`.
    fn staleness_and_archival(
        &self,
        ancestor_hash: &CryptoHash,
        prev_block_hash: &CryptoHash,
        tip: &Tip,
    ) -> (bool, bool) {
        let fetch_from_archival = chunk_needs_to_be_fetched_from_archival(
            ancestor_hash,
            &tip.last_block_hash,
            self.epoch_manager.as_ref(),
        )
        .unwrap_or_else(|err| {
            debug_assert!(false);
            tracing::error!(target: "chunks", ?err, "cannot determine whether to request chunk from archival node, defaulting to not");
            false
        });
        let is_old =
            tip.last_block_hash != *prev_block_hash && tip.prev_block_hash != *prev_block_hash;
        (is_old, fetch_from_archival)
    }

    /// Determine the current escalation state based on elapsed time since request was added.
    fn advance_state(elapsed: std::time::Duration) -> ChunkRequestState {
        if elapsed >= CHUNK_REQUEST_SWITCH_TO_FULL_FETCH {
            ChunkRequestState::FullFetch
        } else if elapsed >= CHUNK_REQUEST_SWITCH_TO_OTHERS {
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
    ) -> Option<ChunkSendRequest> {
        let height = chunk_header.height_created();
        let shard_id = chunk_header.shard_id();
        let chunk_hash = chunk_header.chunk_hash().clone();

        if self.requests.contains_key(&chunk_hash) {
            tracing::debug!(target: "chunks", height, %shard_id, ?chunk_hash, "not requesting chunk, already being requested");
            return None;
        }

        match is_chunk_complete(&chunk_hash) {
            Some(true) => {
                tracing::debug!(target: "chunks", height, %shard_id, ?chunk_hash, "not requesting chunk, already complete");
                return None;
            }
            Some(false) => {
                // chunk is in cache but not complete, proceed
            }
            None => {
                // Not in cache at all. In all code paths that lead here, the header was already
                // inserted. If missing, it was completed and GC-ed.
                tracing::debug!(target: "chunks", height, %shard_id, ?chunk_hash, "not requesting chunk, already complete and GC-ed");
                return None;
            }
        }

        let prev_block_hash = *chunk_header.prev_block_hash();
        let now = self.clock.now().into();
        self.requests.insert(
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
            return None;
        }

        let (is_old, fetch_from_archival) =
            self.staleness_and_archival(&ancestor_hash, &prev_block_hash, chain_header_head);

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

        let initial_state = if should_wait && !fetch_from_archival && !is_old {
            ChunkRequestState::WaitingForForwarding
        } else if is_old {
            ChunkRequestState::RequestingFromOthers
        } else {
            ChunkRequestState::RequestingFromProducer
        };

        if !initial_state.should_send_request() {
            tracing::debug!(target: "chunks", ?initial_state, "delaying the chunk request");
            return None;
        }

        tracing::debug!(target: "chunks", height, %shard_id, ?chunk_hash, "requesting");
        Some(ChunkSendRequest {
            chunk_hash,
            height,
            ancestor_hash,
            shard_id,
            force_request_full: initial_state.force_request_full(),
            request_own_parts_from_others: initial_state.request_own_parts_from_others(),
            request_from_archival: fetch_from_archival,
        })
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
    use near_async::time::FakeClock;
    use near_epoch_manager::test_utils::setup_epoch_manager_with_block_and_chunk_producers;
    use near_primitives::hash::hash;
    use near_primitives::sharding::ShardChunkHeaderV3;
    use near_primitives::types::EpochId;
    use near_store::test_utils::create_test_store;

    struct TestFixture {
        clock: FakeClock,
        orchestrator: ChunkRequestOrchestrator,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        tip: Tip,
    }

    impl TestFixture {
        fn new() -> Self {
            let clock = FakeClock::default();
            let store = create_test_store();
            let epoch_manager = setup_epoch_manager_with_block_and_chunk_producers(
                store,
                vec!["test".parse().unwrap()],
                vec![],
                1,
                2,
            );
            let epoch_manager: Arc<dyn EpochManagerAdapter> = Arc::new(epoch_manager.into_handle());
            let orchestrator = ChunkRequestOrchestrator::new(clock.clock(), epoch_manager.clone());
            let tip = Tip {
                height: 0,
                last_block_hash: CryptoHash::default(),
                prev_block_hash: CryptoHash::default(),
                epoch_id: EpochId::default(),
                next_epoch_id: EpochId::default(),
            };
            Self { clock, orchestrator, epoch_manager, tip }
        }

        fn insert_request(&mut self, chunk_hash: ChunkHash) {
            let added = self.clock.now().into();
            self.orchestrator.insert(
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

        fn chunk_header(&self, height: BlockHeight) -> ShardChunkHeader {
            let epoch_id = EpochId::default();
            let shard_layout = self.epoch_manager.get_shard_layout(&epoch_id).unwrap();
            let shard_id = shard_layout.shard_ids().next().unwrap();
            ShardChunkHeader::V3(ShardChunkHeaderV3::new_dummy(
                height,
                shard_id,
                CryptoHash::default(),
            ))
        }
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
        let mut t = TestFixture::new();
        t.insert_request(ChunkHash(hash(&[1])));
        t.clock.advance(CHUNK_REQUEST_RETRY + time::Duration::milliseconds(50));
        let actions = t.orchestrator.tick(&t.tip);
        assert_eq!(actions.len(), 1);
        assert!(!actions[0].force_request_full);
        assert!(!actions[0].request_own_parts_from_others);
    }

    #[test]
    fn test_tick_escalates_to_requesting_from_others() {
        let mut t = TestFixture::new();
        t.insert_request(ChunkHash(hash(&[1])));
        t.clock.advance(CHUNK_REQUEST_SWITCH_TO_OTHERS + time::Duration::milliseconds(50));
        let actions = t.orchestrator.tick(&t.tip);
        assert_eq!(actions.len(), 1);
        assert!(!actions[0].force_request_full);
        assert!(actions[0].request_own_parts_from_others);
    }

    #[test]
    fn test_tick_escalates_to_full_fetch() {
        let mut t = TestFixture::new();
        t.insert_request(ChunkHash(hash(&[1])));
        t.clock.advance(CHUNK_REQUEST_SWITCH_TO_FULL_FETCH + time::Duration::milliseconds(50));
        let actions = t.orchestrator.tick(&t.tip);
        assert_eq!(actions.len(), 1);
        assert!(actions[0].force_request_full);
        assert!(actions[0].request_own_parts_from_others);
    }

    #[test]
    fn test_tick_evicts_after_max_duration() {
        let mut t = TestFixture::new();
        let chunk_hash = ChunkHash(hash(&[1]));
        t.insert_request(chunk_hash.clone());
        t.clock.advance(CHUNK_REQUEST_RETRY_MAX + time::Duration::seconds(1));
        let actions = t.orchestrator.tick(&t.tip);
        assert!(actions.is_empty());
        assert!(!t.orchestrator.contains(&chunk_hash));
    }

    #[test]
    fn test_tick_respects_retry_interval() {
        let mut t = TestFixture::new();
        t.insert_request(ChunkHash(hash(&[1])));

        t.clock.advance(CHUNK_REQUEST_RETRY + time::Duration::milliseconds(1));
        assert_eq!(t.orchestrator.tick(&t.tip).len(), 1);

        assert!(t.orchestrator.tick(&t.tip).is_empty());

        t.clock.advance(CHUNK_REQUEST_RETRY + time::Duration::milliseconds(1));
        assert_eq!(t.orchestrator.tick(&t.tip).len(), 1);
    }

    #[test]
    fn test_tick_old_block_promotes_to_requesting_from_others() {
        let mut t = TestFixture::new();
        let now = t.clock.now().into();
        t.orchestrator.insert(
            ChunkHash(hash(&[1])),
            ChunkRequestInfo {
                height: 0,
                ancestor_hash: CryptoHash::default(),
                prev_block_hash: hash(&[99]),
                shard_id: ShardId::new(0),
                added: now,
                last_requested: now,
            },
        );
        t.clock.advance(CHUNK_REQUEST_RETRY + time::Duration::milliseconds(50));
        let actions = t.orchestrator.tick(&t.tip);
        assert_eq!(actions.len(), 1);
        assert!(!actions[0].force_request_full);
        assert!(actions[0].request_own_parts_from_others);
    }

    #[test]
    fn test_track_needed_chunk_non_validator_skips_forwarding_wait() {
        let mut t = TestFixture::new();
        let chunk_header = t.chunk_header(3);
        let chunk_hash = chunk_header.chunk_hash();

        let request = t.orchestrator.track_needed_chunk(
            &chunk_header,
            CryptoHash::default(),
            false,
            |_| Some(false),
            &t.tip,
            None,
        );
        let request = request.expect("expected a send request");
        assert!(!request.force_request_full);
        assert!(!request.request_own_parts_from_others);
        assert!(t.orchestrator.contains(&chunk_hash));
    }

    #[test]
    fn test_track_needed_chunk_validator_waits_for_forwarding() {
        let mut t = TestFixture::new();
        let chunk_header = t.chunk_header(3);
        let chunk_hash = chunk_header.chunk_hash();
        let me: AccountId = "test".parse().unwrap();

        let request = t.orchestrator.track_needed_chunk(
            &chunk_header,
            CryptoHash::default(),
            false,
            |_| Some(false),
            &t.tip,
            Some(&me),
        );
        assert!(request.is_none());
        assert!(t.orchestrator.contains(&chunk_hash));
    }

    #[test]
    fn test_track_needed_chunk_duplicate_returns_none() {
        let mut t = TestFixture::new();
        let chunk_header = t.chunk_header(3);

        t.orchestrator.track_needed_chunk(
            &chunk_header,
            CryptoHash::default(),
            false,
            |_| Some(false),
            &t.tip,
            None,
        );

        let request = t.orchestrator.track_needed_chunk(
            &chunk_header,
            CryptoHash::default(),
            false,
            |_| Some(false),
            &t.tip,
            None,
        );
        assert!(request.is_none());
    }

    #[test]
    fn test_track_needed_chunk_complete_returns_none() {
        let mut t = TestFixture::new();
        let chunk_header = t.chunk_header(3);

        let request = t.orchestrator.track_needed_chunk(
            &chunk_header,
            CryptoHash::default(),
            false,
            |_| Some(true),
            &t.tip,
            None,
        );
        assert!(request.is_none());
        assert!(!t.orchestrator.contains(&chunk_header.chunk_hash()));
    }

    #[test]
    fn test_track_needed_chunk_mark_only() {
        let mut t = TestFixture::new();
        let chunk_header = t.chunk_header(3);

        let request = t.orchestrator.track_needed_chunk(
            &chunk_header,
            CryptoHash::default(),
            true,
            |_| Some(false),
            &t.tip,
            None,
        );
        assert!(request.is_none());
        assert!(t.orchestrator.contains(&chunk_header.chunk_hash()));
    }

    #[test]
    fn test_remove_stops_tracking() {
        let mut t = TestFixture::new();
        let chunk_hash = ChunkHash(hash(&[1]));
        t.insert_request(chunk_hash.clone());
        assert!(t.orchestrator.contains(&chunk_hash));
        assert_eq!(t.orchestrator.pending_count(), 1);

        t.orchestrator.remove(&chunk_hash);
        assert!(!t.orchestrator.contains(&chunk_hash));
        assert_eq!(t.orchestrator.pending_count(), 0);
    }
}
