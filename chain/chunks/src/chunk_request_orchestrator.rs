use near_async::time::{self, Clock};
use near_chain::types::EpochManagerAdapter;
use near_primitives::errors::EpochError;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::ChunkHash;
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

    pub fn fetch(&mut self) -> Vec<(ChunkHash, ChunkRequestInfo)> {
        self.pool.fetch(self.clock.now().into())
    }

    pub fn switch_to_full_fetch_duration(&self) -> time::Duration {
        self.pool.switch_to_full_fetch_duration
    }

    pub fn switch_to_others_duration(&self) -> time::Duration {
        self.pool.switch_to_others_duration
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
