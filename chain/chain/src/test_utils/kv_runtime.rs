use super::ValidatorSchedule;
use crate::BlockHeader;
use crate::types::{
    ApplyChunkBlockContext, ApplyChunkResult, ApplyChunkShardContext,
    PrepareTransactionsBlockContext, PrepareTransactionsChunkContext, PreparedTransactions,
    RuntimeAdapter, RuntimeStorageConfig,
};
use borsh::{BorshDeserialize, BorshSerialize};
use itertools::Itertools;
use near_async::time::Duration;
use near_chain_configs::{DEFAULT_GC_NUM_EPOCHS_TO_KEEP, ProtocolConfig};
use near_chain_primitives::Error;
use near_crypto::{KeyType, PublicKey, SecretKey};
use near_epoch_manager::EpochManagerAdapter;
use near_parameters::RuntimeConfig;
use near_pool::types::TransactionGroupIterator;
use near_primitives::account::{AccessKey, Account, AccountContract};
use near_primitives::apply::ApplyChunkReason;
use near_primitives::bandwidth_scheduler::BandwidthRequests;
use near_primitives::block::Tip;
use near_primitives::chunk_apply_stats::ChunkApplyStatsV0;
use near_primitives::congestion_info::{CongestionInfo, ExtendedCongestionInfo};
use near_primitives::epoch_block_info::BlockInfo;
use near_primitives::epoch_info::{EpochInfo, RngSeed};
use near_primitives::epoch_manager::EpochConfig;
use near_primitives::epoch_manager::ShardConfig;
use near_primitives::errors::{EpochError, InvalidTxError};
use near_primitives::hash::{CryptoHash, hash};
use near_primitives::receipt::{ActionReceipt, Receipt, ReceiptEnum, ReceiptV0};
use near_primitives::shard_layout::{ShardLayout, ShardUId};
use near_primitives::state_part::PartId;
use near_primitives::stateless_validation::ChunkProductionKey;
use near_primitives::stateless_validation::validator_assignment::ChunkValidatorAssignments;
use near_primitives::transaction::{
    Action, ExecutionMetadata, ExecutionOutcome, ExecutionOutcomeWithId, ExecutionStatus,
    SignedTransaction, TransferAction, ValidatedTransaction,
};
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{
    AccountId, ApprovalStake, Balance, BlockHeight, EpochHeight, EpochId, Nonce, NumShards,
    ShardId, ShardIndex, StateRoot, StateRootNode, ValidatorInfoIdentifier,
};
use near_primitives::version::{PROTOCOL_VERSION, ProtocolVersion};
use near_primitives::views::{
    AccessKeyInfoView, AccessKeyList, CallResult, ContractCodeView, EpochValidatorInfo,
    QueryRequest, QueryResponse, QueryResponseKind, ViewStateResult,
};
use near_store::test_utils::TestTriesBuilder;
use near_store::{
    DBCol, ShardTries, Store, StoreUpdate, Trie, TrieChanges, WrappedTrieChanges,
    set_genesis_height, set_genesis_state_roots,
};
use near_vm_runner::{ContractCode, ContractRuntimeCache, NoContractRuntimeCache};
use node_runtime::SignedValidPeriodTransactions;
use parking_lot::RwLock;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

/// Simple key value runtime for tests.
///
/// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
/// WARNING: If you choose to use KeyValueRuntime for your tests, BE PREPARED TO
/// HAVE A BAD TIME. Use it only if you understand it to its entirety. It has
/// implicit behavior, very specific partially implemented logic, and is generally
/// incorrect. USE NightshadeRuntime WHENEVER POSSIBLE. YOU HAVE BEEN WARNED.
/// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
///
/// Major differences with production `NightshadeRuntime`:
///   * Uses in-memory storage
///   * Doesn't have WASM runtime, so can only process simple transfer
///     transaction
///   * Uses hard-coded validator schedule instead of using `EpochManager` and
///     staking to assign block and chunk producers.
pub struct KeyValueRuntime {
    store: Store,
    tries: ShardTries,
    num_shards: NumShards,
    epoch_length: u64,
    no_gc: bool,
    runtime_config: RuntimeConfig,

    // A mapping state_root => {account id => amounts}, for transactions and receipts
    state: RwLock<HashMap<StateRoot, KVState>>,
    state_size: RwLock<HashMap<StateRoot, u64>>,
    headers_cache: RwLock<HashMap<CryptoHash, BlockHeader>>,
    contract_cache: NoContractRuntimeCache,
}

/// DEPRECATED. DO NOT USE for new tests. Use the real EpochManager, familiarize
/// yourself with how block producers, chunk producers, epoch transitions, etc.
/// work, and write your test to be compatible with what's in production.
/// MockEpochManager is simpler, but it deviates considerably from the production
/// validator selection and epoch management behavior.
pub struct MockEpochManager {
    store: Store,
    num_shards: NumShards,
    epoch_length: u64,
    /// A pre determined list of validator sets. We rotate validator set in this list.
    /// Epoch i uses validators from `validators_by_valset[i % validators_by_valset.len()]`.
    validators_by_valset: Vec<EpochValidatorSet>,
    /// Maps from account id to validator stake for all validators, both block producers and
    /// chunk producers
    validators: HashMap<AccountId, ValidatorStake>,

    headers_cache: RwLock<HashMap<CryptoHash, BlockHeader>>,
    hash_to_epoch: RwLock<HashMap<CryptoHash, EpochId>>,
    hash_to_next_epoch_approvals_req: RwLock<HashMap<CryptoHash, bool>>,
    hash_to_next_epoch: RwLock<HashMap<CryptoHash, EpochId>>,
    /// Maps EpochId to index of `validators_by_valset` to determine validators for an epoch
    hash_to_valset: RwLock<HashMap<EpochId, u64>>,
    epoch_start: RwLock<HashMap<CryptoHash, u64>>,
}

/// Stores the validator information in an epoch.
/// Block producers are specified by `block_producers`
/// Chunk producers have two types, validators who are also block producers and chunk only producers.
/// Block producers are assigned to shards via `validator_groups`.
/// Each shard will have `block_producers.len() / validator_groups` of validators who are also block
/// producers
struct EpochValidatorSet {
    block_producers: Vec<ValidatorStake>,
    /// index of this list is shard_id
    chunk_producers: Vec<Vec<ValidatorStake>>,
}

#[derive(BorshSerialize, BorshDeserialize, Hash, PartialEq, Eq, Ord, PartialOrd, Clone, Debug)]
struct AccountNonce(AccountId, Nonce);

#[derive(BorshSerialize, BorshDeserialize, Clone, Debug)]
struct KVState {
    amounts: HashMap<AccountId, u128>,
    receipt_nonces: HashSet<CryptoHash>,
    tx_nonces: HashSet<AccountNonce>,
}

impl MockEpochManager {
    pub fn new(store: Store, epoch_length: u64) -> Arc<Self> {
        let vs =
            ValidatorSchedule::new().block_producers_per_epoch(vec![vec!["test".parse().unwrap()]]);
        Self::new_with_validators(store, vs, epoch_length)
    }

    pub fn new_with_validators(
        store: Store,
        vs: ValidatorSchedule,
        epoch_length: u64,
    ) -> Arc<Self> {
        let map_with_default_hash1 = HashMap::from([(CryptoHash::default(), EpochId::default())]);
        let map_with_default_hash2 = HashMap::from([(CryptoHash::default(), 0)]);
        let map_with_default_hash3 = HashMap::from([(EpochId::default(), 0)]);

        let mut validators = HashMap::new();
        let mut validators_by_valset: Vec<EpochValidatorSet> = vs
            .block_producers
            .iter()
            .map(|account_ids| {
                let block_producers: Vec<ValidatorStake> = account_ids
                    .iter()
                    .map(|account_id| {
                        let stake = ValidatorStake::new(
                            account_id.clone(),
                            SecretKey::from_seed(KeyType::ED25519, account_id.as_ref())
                                .public_key(),
                            1_000_000,
                        );
                        validators.insert(account_id.clone(), stake.clone());
                        stake
                    })
                    .collect();

                // cspell:ignore coef
                let validators_per_shard = block_producers.len() / vs.validator_groups as usize;
                let coefficient = block_producers.len() / vs.num_shards as usize;

                let chunk_producers: Vec<Vec<ValidatorStake>> = (0..vs.num_shards)
                    .map(|shard_index| {
                        let shard_index = shard_index as usize;
                        let offset =
                            shard_index * coefficient / validators_per_shard * validators_per_shard;
                        block_producers[offset..offset + validators_per_shard].to_vec()
                    })
                    .collect();

                EpochValidatorSet { block_producers, chunk_producers }
            })
            .collect();

        if !vs.chunk_only_producers.is_empty() {
            assert_eq!(validators_by_valset.len(), vs.chunk_only_producers.len());
            for (epoch_idx, epoch_cops) in vs.chunk_only_producers.into_iter().enumerate() {
                assert_eq!(epoch_cops.len() as u64, vs.num_shards);
                for (shard_idx, shard_cops) in epoch_cops.into_iter().enumerate() {
                    for account_id in shard_cops {
                        let stake = ValidatorStake::new(
                            account_id.clone(),
                            SecretKey::from_seed(KeyType::ED25519, account_id.as_ref())
                                .public_key(),
                            1_000_000,
                        );
                        let prev = validators.insert(account_id, stake.clone());
                        assert!(prev.is_none(), "chunk only produced is also a block producer");
                        validators_by_valset[epoch_idx].chunk_producers[shard_idx].push(stake)
                    }
                }
            }
        }

        Arc::new(MockEpochManager {
            store,
            num_shards: vs.num_shards,
            epoch_length,
            validators,
            validators_by_valset,
            headers_cache: RwLock::new(HashMap::new()),
            hash_to_epoch: RwLock::new(HashMap::new()),
            hash_to_next_epoch_approvals_req: RwLock::new(HashMap::new()),
            hash_to_next_epoch: RwLock::new(map_with_default_hash1),
            hash_to_valset: RwLock::new(map_with_default_hash3),
            epoch_start: RwLock::new(map_with_default_hash2),
        })
    }

    /// Get epoch and index of validator set by the hash of previous block.
    /// Note that it also fills in-memory chain info and there is some
    /// assumption that it is called for all previous blocks.
    /// TODO (#8269): should we call it recursively for previous blocks if info is not found?
    fn get_epoch_and_valset(
        &self,
        prev_hash: CryptoHash,
    ) -> Result<(EpochId, usize, EpochId), EpochError> {
        if prev_hash == CryptoHash::default() {
            return Ok((EpochId(prev_hash), 0, EpochId(prev_hash)));
        }
        let prev_block_header =
            self.get_block_header(&prev_hash)?.ok_or(EpochError::MissingBlock(prev_hash))?;

        let mut hash_to_epoch = self.hash_to_epoch.write();
        let mut hash_to_next_epoch_approvals_req = self.hash_to_next_epoch_approvals_req.write();
        let mut hash_to_next_epoch = self.hash_to_next_epoch.write();
        let mut hash_to_valset = self.hash_to_valset.write();
        let mut epoch_start_map = self.epoch_start.write();

        let prev_prev_hash = *prev_block_header.prev_hash();
        let prev_epoch = hash_to_epoch.get(&prev_prev_hash);
        let prev_next_epoch = hash_to_next_epoch.get(&prev_prev_hash).unwrap();
        let prev_valset = match prev_epoch {
            Some(prev_epoch) => Some(*hash_to_valset.get(prev_epoch).unwrap()),
            None => None,
        };

        let prev_epoch_start = *epoch_start_map.get(&prev_prev_hash).unwrap();

        let last_final_height = if prev_block_header.last_final_block() == &CryptoHash::default() {
            0
        } else {
            self.get_block_header(prev_block_header.last_final_block()).unwrap().unwrap().height()
        };

        let increment_epoch = prev_prev_hash == CryptoHash::default() // genesis is in its own epoch
            || last_final_height + 3 >= prev_epoch_start + self.epoch_length;

        let needs_next_epoch_approvals = !increment_epoch
            && last_final_height + 3 < prev_epoch_start + self.epoch_length
            && prev_block_header.height() + 3 >= prev_epoch_start + self.epoch_length;

        let (epoch, next_epoch, valset, epoch_start) = if increment_epoch {
            let new_valset = match prev_valset {
                None => 0,
                Some(prev_valset) => prev_valset + 1,
            };
            (*prev_next_epoch, EpochId(prev_hash), new_valset, prev_block_header.height() + 1)
        } else {
            (*prev_epoch.unwrap(), *prev_next_epoch, prev_valset.unwrap(), prev_epoch_start)
        };

        hash_to_next_epoch.insert(prev_hash, next_epoch);
        hash_to_epoch.insert(prev_hash, epoch);
        hash_to_next_epoch_approvals_req.insert(prev_hash, needs_next_epoch_approvals);
        hash_to_valset.insert(epoch, valset);
        hash_to_valset.insert(next_epoch, valset + 1);
        epoch_start_map.insert(prev_hash, epoch_start);

        Ok((epoch, valset as usize % self.validators_by_valset.len(), next_epoch))
    }

    fn get_block_producers(&self, valset: usize) -> &[ValidatorStake] {
        &self.validators_by_valset[valset].block_producers
    }

    fn get_chunk_producers(&self, valset: usize, shard_index: ShardIndex) -> Vec<ValidatorStake> {
        self.validators_by_valset[valset].chunk_producers[shard_index].clone()
    }

    fn get_valset_for_epoch(&self, epoch_id: &EpochId) -> Result<usize, EpochError> {
        // conveniently here if the prev_hash is passed mistakenly instead of the epoch_hash,
        // the `unwrap` will trigger
        Ok(*self
            .hash_to_valset
            .read()
            .get(epoch_id)
            .ok_or(EpochError::EpochOutOfBounds(*epoch_id))? as usize
            % self.validators_by_valset.len())
    }

    fn get_block_header(&self, hash: &CryptoHash) -> Result<Option<BlockHeader>, EpochError> {
        let mut headers_cache = self.headers_cache.write();
        if headers_cache.get(hash).is_some() {
            return Ok(Some(headers_cache.get(hash).unwrap().clone()));
        }
        if let Some(result) = self.store.get_ser(DBCol::BlockHeader, hash.as_ref())? {
            headers_cache.insert(*hash, result);
            return Ok(Some(headers_cache.get(hash).unwrap().clone()));
        }
        Ok(None)
    }
}

impl KeyValueRuntime {
    pub fn new(store: Store, epoch_manager: &MockEpochManager) -> Arc<Self> {
        Self::new_with_no_gc(store, epoch_manager, false)
    }
    pub fn new_with_no_gc(
        store: Store,
        epoch_manager: &MockEpochManager,
        no_gc: bool,
    ) -> Arc<Self> {
        let epoch_id = EpochId::default();
        let shard_layout = epoch_manager.get_shard_layout(&epoch_id).unwrap();
        let epoch_length = epoch_manager.get_epoch_config(&epoch_id).unwrap().epoch_length;
        let tries = TestTriesBuilder::new()
            .with_store(store.clone())
            .with_shard_layout(shard_layout.clone())
            .build();
        let mut initial_amounts = HashMap::new();
        for (i, validator_stake) in epoch_manager
            .validators_by_valset
            .iter()
            .flat_map(|set| set.block_producers.iter())
            .enumerate()
        {
            initial_amounts.insert(validator_stake.account_id().clone(), (1000 + 100 * i) as u128);
        }

        let kv_state = KVState {
            amounts: initial_amounts,
            receipt_nonces: HashSet::default(),
            tx_nonces: HashSet::default(),
        };
        let data = borsh::to_vec(&kv_state).unwrap();
        let data_len = data.len() as u64;
        // StateRoot is actually faked here.
        // We cannot do any reasonable validations of it in test_utils.
        let state = HashMap::from([(Trie::EMPTY_ROOT, kv_state)]);
        let state_size = HashMap::from([(Trie::EMPTY_ROOT, data_len)]);

        let mut store_update = store.store_update();
        let genesis_roots: Vec<CryptoHash> =
            shard_layout.shard_ids().map(|_| Trie::EMPTY_ROOT).collect();
        set_genesis_state_roots(&mut store_update, &genesis_roots);
        set_genesis_height(&mut store_update, &0);
        store_update.commit().expect("Store failed on genesis initialization");

        Arc::new(KeyValueRuntime {
            store,
            tries,
            no_gc,
            num_shards: shard_layout.num_shards(),
            epoch_length,
            headers_cache: RwLock::new(HashMap::new()),
            state: RwLock::new(state),
            state_size: RwLock::new(state_size),
            contract_cache: NoContractRuntimeCache,
            runtime_config: RuntimeConfig::test(),
        })
    }

    fn get_block_header(&self, hash: &CryptoHash) -> Result<Option<BlockHeader>, EpochError> {
        let mut headers_cache = self.headers_cache.write();
        if headers_cache.get(hash).is_some() {
            return Ok(Some(headers_cache.get(hash).unwrap().clone()));
        }
        if let Some(result) = self.store.get_ser(DBCol::BlockHeader, hash.as_ref())? {
            headers_cache.insert(*hash, result);
            return Ok(Some(headers_cache.get(hash).unwrap().clone()));
        }
        Ok(None)
    }

    fn get_congestion_info() -> CongestionInfo {
        CongestionInfo::default()
    }
}

pub fn account_id_to_shard_id(account_id: &AccountId, num_shards: NumShards) -> ShardId {
    #[allow(deprecated)]
    let shard_layout = ShardLayout::v0(num_shards, 0);
    shard_layout.account_id_to_shard_id(account_id)
}

#[derive(BorshSerialize, BorshDeserialize)]
struct ReceiptNonce {
    from: AccountId,
    to: AccountId,
    amount: Balance,
    nonce: Nonce,
}

fn create_receipt_nonce(
    from: AccountId,
    to: AccountId,
    amount: Balance,
    nonce: Nonce,
) -> CryptoHash {
    CryptoHash::hash_borsh(ReceiptNonce { from, to, amount, nonce })
}

impl EpochManagerAdapter for MockEpochManager {
    fn epoch_exists(&self, epoch_id: &EpochId) -> bool {
        self.hash_to_valset.write().contains_key(epoch_id)
    }

    fn shard_ids(&self, epoch_id: &EpochId) -> Result<Vec<ShardId>, EpochError> {
        Ok(self.get_shard_layout(epoch_id)?.shard_ids().collect())
    }

    fn num_total_parts(&self) -> usize {
        12 + (self.num_shards as usize + 1) % 50
    }

    fn num_data_parts(&self) -> usize {
        // Same as in Nightshade Runtime
        let total_parts = self.num_total_parts();
        if total_parts <= 3 { 1 } else { (total_parts - 1) / 3 }
    }

    fn get_part_owner(&self, epoch_id: &EpochId, part_id: u64) -> Result<AccountId, EpochError> {
        let validators = &self.get_epoch_block_producers_ordered(epoch_id)?;
        // if we don't use data_parts and total_parts as part of the formula here, the part owner
        //     would not depend on height, and tests wouldn't catch passing wrong height here
        let idx = part_id as usize + self.num_data_parts() + self.num_total_parts();
        Ok(validators[idx as usize % validators.len()].account_id().clone())
    }

    fn get_block_info(&self, _hash: &CryptoHash) -> Result<Arc<BlockInfo>, EpochError> {
        Ok(Default::default())
    }

    fn get_epoch_config_from_protocol_version(
        &self,
        _protocol_version: ProtocolVersion,
    ) -> EpochConfig {
        EpochConfig::mock(self.epoch_length, self.get_shard_layout(&EpochId::default()).unwrap())
    }

    fn get_epoch_config(&self, epoch_id: &EpochId) -> Result<EpochConfig, EpochError> {
        Ok(EpochConfig::mock(self.epoch_length, self.get_shard_layout(epoch_id).unwrap()))
    }

    /// Return the epoch info containing the mocked data.
    /// Epoch id is unused.
    /// Available mocked data:
    /// - validators
    /// - block producers
    /// - chunk producers
    /// All the other fields have a hardcoded value or left empty.
    fn get_epoch_info(&self, _epoch_id: &EpochId) -> Result<Arc<EpochInfo>, EpochError> {
        let validators = self.validators.iter().map(|(_, stake)| stake.clone()).collect();
        let mut validator_to_index = HashMap::new();
        for (i, (account_id, _)) in self.validators.iter().enumerate() {
            validator_to_index.insert(account_id.clone(), i as u64);
        }
        let bp_settlement = self.validators_by_valset[0]
            .block_producers
            .iter()
            .map(|stake| *validator_to_index.get(stake.account_id()).unwrap())
            .collect();
        let cp_settlement = self.validators_by_valset[0]
            .chunk_producers
            .iter()
            .map(|vec| {
                vec.iter()
                    .map(|stake| *validator_to_index.get(stake.account_id()).unwrap())
                    .collect()
            })
            .collect();
        Ok(Arc::new(EpochInfo::new(
            10,
            validators,
            validator_to_index,
            bp_settlement,
            cp_settlement,
            BTreeMap::new(),
            HashMap::new(),
            HashMap::new(),
            1,
            1,
            PROTOCOL_VERSION,
            RngSeed::default(),
            Default::default(),
        )))
    }

    fn get_shard_layout(&self, _epoch_id: &EpochId) -> Result<ShardLayout, EpochError> {
        #[allow(deprecated)]
        Ok(ShardLayout::v0(self.num_shards, 0))
    }

    fn get_shard_layout_from_protocol_version(
        &self,
        _protocol_version: ProtocolVersion,
    ) -> ShardLayout {
        self.get_shard_layout(&EpochId::default()).unwrap()
    }

    fn get_shard_config(&self, _epoch_id: &EpochId) -> Result<ShardConfig, EpochError> {
        panic!("get_shard_config not implemented for KeyValueRuntime");
    }

    fn is_next_block_epoch_start(&self, parent_hash: &CryptoHash) -> Result<bool, EpochError> {
        if parent_hash == &CryptoHash::default() {
            return Ok(true);
        }
        let prev_block_header =
            self.get_block_header(parent_hash)?.ok_or(EpochError::MissingBlock(*parent_hash))?;
        let prev_prev_hash = *prev_block_header.prev_hash();
        Ok(self.get_epoch_and_valset(*parent_hash)?.0
            != self.get_epoch_and_valset(prev_prev_hash)?.0)
    }

    fn is_last_block_in_finished_epoch(&self, hash: &CryptoHash) -> Result<bool, EpochError> {
        self.is_next_block_epoch_start(hash)
    }

    fn get_epoch_id_from_prev_block(
        &self,
        parent_hash: &CryptoHash,
    ) -> Result<EpochId, EpochError> {
        Ok(self.get_epoch_and_valset(*parent_hash)?.0)
    }

    fn get_epoch_height_from_prev_block(
        &self,
        _prev_block_hash: &CryptoHash,
    ) -> Result<EpochHeight, EpochError> {
        Ok(0)
    }

    fn get_epoch_start_from_epoch_id(
        &self,
        _epoch_id: &EpochId,
    ) -> Result<BlockHeight, EpochError> {
        Ok(0)
    }

    fn get_next_epoch_id(&self, block_hash: &CryptoHash) -> Result<EpochId, EpochError> {
        let (_, _, next_epoch_id) = self.get_epoch_and_valset(*block_hash)?;
        Ok(next_epoch_id)
    }

    fn get_next_epoch_id_from_prev_block(
        &self,
        parent_hash: &CryptoHash,
    ) -> Result<EpochId, EpochError> {
        Ok(self.get_epoch_and_valset(*parent_hash)?.2)
    }

    fn get_prev_shard_ids(
        &self,
        prev_hash: &CryptoHash,
        shard_ids: Vec<ShardId>,
    ) -> Result<Vec<(ShardId, ShardIndex)>, Error> {
        let mut prev_shard_ids = vec![];
        let shard_layout = self.get_shard_layout_from_prev_block(prev_hash)?;
        for shard_id in shard_ids {
            // This is not correct if there was a resharding event in between
            // the previous and current block.
            let prev_shard_id = shard_id;
            let prev_shard_index = shard_layout.get_shard_index(prev_shard_id)?;
            prev_shard_ids.push((prev_shard_id, prev_shard_index));
        }

        Ok(prev_shard_ids)
    }

    fn get_prev_shard_id_from_prev_hash(
        &self,
        prev_hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<(ShardLayout, ShardId, ShardIndex), EpochError> {
        let shard_layout = self.get_shard_layout_from_prev_block(prev_hash)?;
        // This is not correct if there was a resharding event in between
        // the previous and current block.
        let prev_shard_id = shard_id;
        let prev_shard_index = shard_layout.get_shard_index(prev_shard_id)?;
        Ok((shard_layout, prev_shard_id, prev_shard_index))
    }

    fn get_shard_layout_from_prev_block(
        &self,
        _parent_hash: &CryptoHash,
    ) -> Result<ShardLayout, EpochError> {
        #[allow(deprecated)]
        Ok(ShardLayout::v0(self.num_shards, 0))
    }

    fn get_epoch_id(&self, block_hash: &CryptoHash) -> Result<EpochId, EpochError> {
        let (epoch_id, _, _) = self.get_epoch_and_valset(*block_hash)?;
        Ok(epoch_id)
    }

    fn compare_epoch_id(
        &self,
        epoch_id: &EpochId,
        other_epoch_id: &EpochId,
    ) -> Result<Ordering, EpochError> {
        if epoch_id.0 == other_epoch_id.0 {
            return Ok(Ordering::Equal);
        }
        match (self.get_valset_for_epoch(epoch_id), self.get_valset_for_epoch(other_epoch_id)) {
            (Ok(index1), Ok(index2)) => Ok(index1.cmp(&index2)),
            _ => Err(EpochError::EpochOutOfBounds(*epoch_id)),
        }
    }

    fn get_epoch_start_height(&self, block_hash: &CryptoHash) -> Result<BlockHeight, EpochError> {
        let epoch_id = self.get_epoch_id(block_hash)?;
        match self.get_block_header(&epoch_id.0)? {
            Some(block_header) => Ok(block_header.height()),
            None => Ok(0),
        }
    }

    fn get_prev_epoch_id_from_prev_block(
        &self,
        prev_block_hash: &CryptoHash,
    ) -> Result<EpochId, EpochError> {
        let mut candidate_hash = *prev_block_hash;
        loop {
            let header = self
                .get_block_header(&candidate_hash)?
                .ok_or(EpochError::MissingBlock(candidate_hash))?;
            candidate_hash = *header.prev_hash();
            if self.is_next_block_epoch_start(&candidate_hash)? {
                break Ok(self.get_epoch_and_valset(candidate_hash)?.0);
            }
        }
    }

    fn get_estimated_protocol_upgrade_block_height(
        &self,
        _block_hash: CryptoHash,
    ) -> Result<Option<BlockHeight>, EpochError> {
        Ok(None)
    }

    fn get_epoch_block_producers_ordered(
        &self,
        epoch_id: &EpochId,
    ) -> Result<Vec<ValidatorStake>, EpochError> {
        let validators = self.get_block_producers(self.get_valset_for_epoch(epoch_id)?);
        Ok(validators.iter().map(|x| x.clone()).collect())
    }

    fn get_epoch_block_approvers_ordered(
        &self,
        parent_hash: &CryptoHash,
    ) -> Result<Vec<ApprovalStake>, EpochError> {
        let (_cur_epoch, cur_valset, next_epoch) = self.get_epoch_and_valset(*parent_hash)?;
        let mut validators = self
            .get_block_producers(cur_valset)
            .iter()
            .map(|x| x.get_approval_stake(false))
            .collect::<Vec<_>>();
        if *self.hash_to_next_epoch_approvals_req.write().get(parent_hash).unwrap() {
            let validators_copy = validators.clone();
            validators.extend(
                self.get_block_producers(self.get_valset_for_epoch(&next_epoch)?)
                    .iter()
                    .filter(|x| {
                        !validators_copy.iter().any(|entry| &entry.account_id == x.account_id())
                    })
                    .map(|x| x.get_approval_stake(true)),
            );
        }
        let validators = validators.into_iter().map(|stake| stake).collect::<Vec<_>>();
        Ok(validators)
    }

    fn get_epoch_chunk_producers(
        &self,
        _epoch_id: &EpochId,
    ) -> Result<Vec<ValidatorStake>, EpochError> {
        tracing::warn!("not implemented, returning a dummy value");
        Ok(vec![])
    }

    fn get_epoch_chunk_producers_for_shard(
        &self,
        epoch_id: &EpochId,
        shard_id: ShardId,
    ) -> Result<Vec<AccountId>, EpochError> {
        let valset = self.get_valset_for_epoch(epoch_id)?;
        let shard_layout = self.get_shard_layout(epoch_id)?;
        let shard_index = shard_layout.get_shard_index(shard_id)?;
        let chunk_producers = self.get_chunk_producers(valset, shard_index);
        Ok(chunk_producers.into_iter().map(|vs| vs.take_account_id()).collect())
    }

    /// We need to override the default implementation to make
    /// `Chain::should_produce_state_witness_for_this_or_next_epoch` work
    /// since `get_epoch_chunk_producers` returns empty Vec which results
    /// in state transition data not being saved on disk.
    fn is_chunk_producer_for_epoch(
        &self,
        _epoch_id: &EpochId,
        _account_id: &AccountId,
    ) -> Result<bool, EpochError> {
        Ok(true)
    }

    fn get_block_producer(
        &self,
        epoch_id: &EpochId,
        height: BlockHeight,
    ) -> Result<AccountId, EpochError> {
        self.get_block_producer_info(epoch_id, height).map(|validator| validator.take_account_id())
    }

    fn get_block_producer_info(
        &self,
        epoch_id: &EpochId,
        height: BlockHeight,
    ) -> Result<ValidatorStake, EpochError> {
        let validators = self.get_block_producers(self.get_valset_for_epoch(epoch_id)?);
        Ok(validators[(height as usize) % validators.len()].clone())
    }

    fn get_chunk_producer_info(
        &self,
        key: &ChunkProductionKey,
    ) -> Result<ValidatorStake, EpochError> {
        let valset = self.get_valset_for_epoch(&key.epoch_id)?;
        let shard_layout = self.get_shard_layout(&key.epoch_id)?;
        let shard_index = shard_layout.get_shard_index(key.shard_id)?;
        let chunk_producers = self.get_chunk_producers(valset, shard_index);
        let index = (shard_index + key.height_created as usize + 1) % chunk_producers.len();
        Ok(chunk_producers[index].clone())
    }

    fn get_chunk_validator_assignments(
        &self,
        epoch_id: &EpochId,
        _shard_id: ShardId,
        _height: BlockHeight,
    ) -> Result<Arc<ChunkValidatorAssignments>, EpochError> {
        let chunk_validators = self
            .get_block_producers(self.get_valset_for_epoch(epoch_id)?)
            .into_iter()
            .cloned()
            .map(|validator| validator.account_and_stake())
            .collect();
        Ok(Arc::new(ChunkValidatorAssignments::new(chunk_validators)))
    }

    fn get_validator_by_account_id(
        &self,
        epoch_id: &EpochId,
        account_id: &AccountId,
    ) -> Result<ValidatorStake, EpochError> {
        let validators = &self.validators_by_valset[self.get_valset_for_epoch(epoch_id)?];
        for validator_stake in &validators.block_producers {
            if validator_stake.account_id() == account_id {
                return Ok(validator_stake.clone());
            }
        }
        for validator_stake in validators.chunk_producers.iter().flatten() {
            if validator_stake.account_id() == account_id {
                return Ok(validator_stake.clone());
            }
        }
        Err(EpochError::NotAValidator(account_id.clone(), *epoch_id))
    }

    fn get_validator_info(
        &self,
        _epoch_id: ValidatorInfoIdentifier,
    ) -> Result<EpochValidatorInfo, EpochError> {
        Ok(EpochValidatorInfo {
            current_validators: vec![],
            next_validators: vec![],
            current_fishermen: vec![],
            next_fishermen: vec![],
            current_proposals: vec![],
            prev_epoch_kickout: vec![],
            epoch_start_height: 0,
            epoch_height: 1,
        })
    }

    fn add_validator_proposals(
        &self,
        _block_info: BlockInfo,
        _random_value: CryptoHash,
    ) -> Result<StoreUpdate, EpochError> {
        Ok(self.store.store_update())
    }

    fn get_epoch_protocol_version(
        &self,
        _epoch_id: &EpochId,
    ) -> Result<ProtocolVersion, EpochError> {
        Ok(PROTOCOL_VERSION)
    }

    fn init_after_epoch_sync(
        &self,
        _store_update: &mut StoreUpdate,
        _prev_epoch_first_block_info: BlockInfo,
        _prev_epoch_last_block_info: BlockInfo,
        _prev_epoch_prev_last_block_info: BlockInfo,
        _prev_epoch_id: &EpochId,
        _prev_epoch_info: EpochInfo,
        _epoch_id: &EpochId,
        _epoch_info: EpochInfo,
        _next_epoch_id: &EpochId,
        _next_epoch_info: EpochInfo,
    ) -> Result<(), EpochError> {
        Ok(())
    }

    fn should_validate_signatures(&self) -> bool {
        false
    }

    fn cares_about_shard_in_epoch(
        &self,
        epoch_id: &EpochId,
        account_id: &AccountId,
        shard_id: ShardId,
    ) -> Result<bool, EpochError> {
        // This `unwrap` here tests that in all code paths we check that the epoch exists before
        //    we check if we care about a shard. Please do not remove the unwrap, fix the logic of
        //    the calling function.
        let epoch_valset = self.get_valset_for_epoch(epoch_id).unwrap();
        let shard_layout = self.get_shard_layout(epoch_id)?;
        let shard_index = shard_layout.get_shard_index(shard_id)?;
        let chunk_producers = self.get_chunk_producers(epoch_valset, shard_index);
        for validator in chunk_producers {
            if validator.account_id() == account_id {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn cares_about_shard_from_prev_block(
        &self,
        parent_hash: &CryptoHash,
        account_id: &AccountId,
        shard_id: ShardId,
    ) -> Result<bool, EpochError> {
        // This `unwrap` here tests that in all code paths we check that the epoch exists before
        //    we check if we care about a shard. Please do not remove the unwrap, fix the logic of
        //    the calling function.
        let epoch_valset = self.get_epoch_and_valset(*parent_hash).unwrap();
        let shard_layout = self.get_shard_layout_from_prev_block(parent_hash)?;
        let shard_index = shard_layout.get_shard_index(shard_id)?;
        let chunk_producers = self.get_chunk_producers(epoch_valset.1, shard_index);
        for validator in chunk_producers {
            if validator.account_id() == account_id {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn cares_about_shard_next_epoch_from_prev_block(
        &self,
        parent_hash: &CryptoHash,
        account_id: &AccountId,
        shard_id: ShardId,
    ) -> Result<bool, EpochError> {
        // This `unwrap` here tests that in all code paths we check that the epoch exists before
        //    we check if we care about a shard. Please do not remove the unwrap, fix the logic of
        //    the calling function.
        let epoch_valset = self.get_epoch_and_valset(*parent_hash).unwrap();
        let shard_layout = self.get_shard_layout_from_prev_block(parent_hash)?;
        let shard_index = shard_layout.get_shard_index(shard_id)?;
        let chunk_producers = self.get_chunk_producers(
            (epoch_valset.1 + 1) % self.validators_by_valset.len(),
            shard_index,
        );
        for validator in chunk_producers {
            if validator.account_id() == account_id {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn cared_about_shard_prev_epoch_from_prev_block(
        &self,
        parent_hash: &CryptoHash,
        account_id: &AccountId,
        shard_id: ShardId,
    ) -> Result<bool, EpochError> {
        // This `unwrap` here tests that in all code paths we check that the epoch exists before
        //    we check if we care about a shard. Please do not remove the unwrap, fix the logic of
        //    the calling function.
        let epoch_valset = self.get_epoch_and_valset(*parent_hash).unwrap();
        let shard_layout = self.get_shard_layout_from_prev_block(parent_hash)?;
        let shard_index = shard_layout.get_shard_index(shard_id)?;
        let chunk_producers = self.get_chunk_producers(
            (epoch_valset.1.wrapping_sub(1)) % self.validators_by_valset.len(),
            shard_index,
        );
        for validator in chunk_producers {
            if validator.account_id() == account_id {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn will_shard_layout_change(&self, parent_hash: &CryptoHash) -> Result<bool, EpochError> {
        // Copied from EpochManager (KeyValueRuntime is deprecated anyway).
        let epoch_id = self.get_epoch_id_from_prev_block(parent_hash)?;
        let next_epoch_id = self.get_next_epoch_id_from_prev_block(parent_hash)?;
        let shard_layout = self.get_shard_layout(&epoch_id)?;
        let next_shard_layout = self.get_shard_layout(&next_epoch_id)?;
        Ok(shard_layout != next_shard_layout)
    }

    fn possible_epochs_of_height_around_tip(
        &self,
        _tip: &Tip,
        _height: BlockHeight,
    ) -> Result<Vec<EpochId>, EpochError> {
        // Just collect all known epochs because `MockEpochManager` is used for
        // tests which lifetime is short.
        let epochs = self.hash_to_epoch.read();
        let next_epochs = self.hash_to_next_epoch.read();
        let all_epochs = epochs
            .keys()
            .chain(next_epochs.keys())
            .cloned()
            .map(|c| EpochId(c))
            .collect::<HashSet<_>>();
        let vec = all_epochs.into_iter().collect_vec();
        Ok(vec)
    }

    fn get_epoch_all_validators(
        &self,
        _epoch_id: &EpochId,
    ) -> Result<Vec<ValidatorStake>, EpochError> {
        Ok(self.validators.iter().map(|(_, v)| v.clone()).collect())
    }
}

impl RuntimeAdapter for KeyValueRuntime {
    fn store(&self) -> &Store {
        &self.store
    }

    fn get_tries(&self) -> ShardTries {
        self.tries.clone()
    }

    fn get_trie_for_shard(
        &self,
        shard_id: ShardId,
        _block_hash: &CryptoHash,
        state_root: StateRoot,
        _use_flat_storage: bool,
    ) -> Result<Trie, Error> {
        Ok(self.tries.get_trie_for_shard(ShardUId::new(0, shard_id), state_root))
    }

    fn get_flat_storage_manager(&self) -> near_store::flat::FlatStorageManager {
        self.tries.get_flat_storage_manager()
    }

    fn get_view_trie_for_shard(
        &self,
        shard_id: ShardId,
        _block_hash: &CryptoHash,
        state_root: StateRoot,
    ) -> Result<Trie, Error> {
        Ok(self.tries.get_view_trie_for_shard(ShardUId::new(0, shard_id), state_root))
    }

    fn get_shard_layout(&self, _protocol_version: ProtocolVersion) -> ShardLayout {
        ShardLayout::multi_shard(self.num_shards, 0)
    }

    fn validate_tx(
        &self,
        _shard_layout: &ShardLayout,
        signed_tx: SignedTransaction,
        _protocol_version: ProtocolVersion,
        _receiver_congestion_info: Option<ExtendedCongestionInfo>,
    ) -> Result<ValidatedTransaction, (InvalidTxError, SignedTransaction)> {
        Ok(ValidatedTransaction::new_for_test(signed_tx))
    }

    fn can_verify_and_charge_tx(
        &self,
        _shard_layout: &ShardLayout,
        _gas_price: Balance,
        _state_root: StateRoot,
        _validated_tx: &ValidatedTransaction,
        _current_protocol_version: ProtocolVersion,
    ) -> Result<(), InvalidTxError> {
        Ok(())
    }

    fn prepare_transactions(
        &self,
        _storage: RuntimeStorageConfig,
        _chunk: PrepareTransactionsChunkContext,
        _prev_block: PrepareTransactionsBlockContext,
        transaction_groups: &mut dyn TransactionGroupIterator,
        _chain_validate: &dyn Fn(&SignedTransaction) -> bool,
        _time_limit: Option<Duration>,
    ) -> Result<PreparedTransactions, Error> {
        let mut res = vec![];
        while let Some(iter) = transaction_groups.next() {
            res.push(iter.next().unwrap());
        }
        Ok(PreparedTransactions { transactions: res, limited_by: None })
    }

    fn apply_chunk(
        &self,
        storage_config: RuntimeStorageConfig,
        _apply_reason: ApplyChunkReason,
        chunk: ApplyChunkShardContext,
        block: ApplyChunkBlockContext,
        receipts: &[Receipt],
        transactions: SignedValidPeriodTransactions,
    ) -> Result<ApplyChunkResult, Error> {
        let mut tx_results = vec![];
        let shard_id = chunk.shard_id;

        let mut state = self.state.read().get(&storage_config.state_root).cloned().unwrap();

        let mut balance_transfers = vec![];

        for receipt in receipts {
            if let ReceiptEnum::Action(action) | ReceiptEnum::PromiseYield(action) =
                receipt.receipt()
            {
                assert_eq!(
                    account_id_to_shard_id(receipt.receiver_id(), self.num_shards),
                    shard_id
                );
                if !state.receipt_nonces.contains(receipt.receipt_id()) {
                    state.receipt_nonces.insert(*receipt.receipt_id());
                    if let Action::Transfer(TransferAction { deposit }) = action.actions[0] {
                        balance_transfers.push((
                            receipt.get_hash(),
                            receipt.predecessor_id().clone(),
                            receipt.receiver_id().clone(),
                            deposit,
                            0,
                        ));
                    }
                } else {
                    panic!("receipts should never be applied twice");
                }
            } else {
                unreachable!("only action receipts can be applied");
            }
        }

        for transaction in transactions.iter_nonexpired_transactions() {
            assert_eq!(
                account_id_to_shard_id(transaction.transaction.signer_id(), self.num_shards),
                shard_id
            );
            if transaction.transaction.actions().is_empty() {
                continue;
            }
            if let Action::Transfer(TransferAction { deposit }) =
                transaction.transaction.actions()[0]
            {
                if !state.tx_nonces.contains(&AccountNonce(
                    transaction.transaction.receiver_id().clone(),
                    transaction.transaction.nonce(),
                )) {
                    state.tx_nonces.insert(AccountNonce(
                        transaction.transaction.receiver_id().clone(),
                        transaction.transaction.nonce(),
                    ));
                    balance_transfers.push((
                        transaction.get_hash(),
                        transaction.transaction.signer_id().clone(),
                        transaction.transaction.receiver_id().clone(),
                        deposit,
                        transaction.transaction.nonce(),
                    ));
                } else {
                    balance_transfers.push((
                        transaction.get_hash(),
                        transaction.transaction.signer_id().clone(),
                        transaction.transaction.receiver_id().clone(),
                        0,
                        transaction.transaction.nonce(),
                    ));
                }
            } else {
                unreachable!();
            }
        }

        let mut outgoing_receipts = vec![];

        for (hash, from, to, amount, nonce) in balance_transfers {
            let mut good_to_go = false;

            if account_id_to_shard_id(&from, self.num_shards) != shard_id {
                // This is a receipt, was already debited
                good_to_go = true;
            } else if let Some(balance) = state.amounts.get(&from) {
                if *balance >= amount {
                    let new_balance = balance - amount;
                    state.amounts.insert(from.clone(), new_balance);
                    good_to_go = true;
                }
            }

            if good_to_go {
                let new_receipt_hashes = if account_id_to_shard_id(&to, self.num_shards) == shard_id
                {
                    state.amounts.insert(to.clone(), state.amounts.get(&to).unwrap_or(&0) + amount);
                    vec![]
                } else {
                    assert_ne!(nonce, 0);
                    let receipt = Receipt::V0(ReceiptV0 {
                        predecessor_id: from.clone(),
                        receiver_id: to.clone(),
                        receipt_id: create_receipt_nonce(from.clone(), to.clone(), amount, nonce),
                        receipt: ReceiptEnum::Action(ActionReceipt {
                            signer_id: from.clone(),
                            signer_public_key: PublicKey::empty(KeyType::ED25519),
                            gas_price: block.gas_price,
                            output_data_receivers: vec![],
                            input_data_ids: vec![],
                            actions: vec![Action::Transfer(TransferAction { deposit: amount })],
                        }),
                    });
                    let receipt_hash = receipt.get_hash();
                    outgoing_receipts.push(receipt);
                    vec![receipt_hash]
                };

                tx_results.push(ExecutionOutcomeWithId {
                    id: hash,
                    outcome: ExecutionOutcome {
                        status: ExecutionStatus::SuccessValue(vec![]),
                        logs: vec![],
                        receipt_ids: new_receipt_hashes,
                        gas_burnt: 0,
                        compute_usage: Some(0),
                        tokens_burnt: 0,
                        executor_id: to.clone(),
                        metadata: ExecutionMetadata::V1,
                    },
                });
            }
        }

        let data = borsh::to_vec(&state)?;
        let state_size = data.len() as u64;
        let state_root = hash(&data);
        self.state.write().insert(state_root, state);
        self.state_size.write().insert(state_root, state_size);
        let storage_proof = Some(Default::default());
        Ok(ApplyChunkResult {
            trie_changes: WrappedTrieChanges::new(
                self.get_tries(),
                ShardUId::new(0, shard_id),
                TrieChanges::empty(state_root),
                Default::default(),
                block.height,
            ),
            new_root: state_root,
            outcomes: tx_results,
            outgoing_receipts,
            validator_proposals: vec![],
            total_gas_burnt: 0,
            total_balance_burnt: 0,
            proof: storage_proof,
            processed_delayed_receipts: vec![],
            processed_yield_timeouts: vec![],
            applied_receipts_hash: hash(&borsh::to_vec(receipts).unwrap()),
            congestion_info: Some(Self::get_congestion_info()),
            bandwidth_requests: BandwidthRequests::empty(),
            bandwidth_scheduler_state_hash: CryptoHash::default(),
            contract_updates: Default::default(),
            stats: ChunkApplyStatsV0::dummy(),
        })
    }

    fn query(
        &self,
        _shard_id: ShardUId,
        state_root: &StateRoot,
        block_height: BlockHeight,
        _block_timestamp: u64,
        _prev_block_hash: &CryptoHash,
        block_hash: &CryptoHash,
        _epoch_id: &EpochId,
        request: &QueryRequest,
    ) -> Result<QueryResponse, near_chain_primitives::error::QueryError> {
        match request {
            QueryRequest::ViewAccount { account_id, .. } => Ok(QueryResponse {
                kind: QueryResponseKind::ViewAccount(
                    Account::new(
                        self.state.read().get(state_root).map_or_else(
                            || 0,
                            |state| *state.amounts.get(account_id).unwrap_or(&0),
                        ),
                        0,
                        AccountContract::None,
                        0,
                    )
                    .into(),
                ),
                block_height,
                block_hash: *block_hash,
            }),
            QueryRequest::ViewCode { .. } => Ok(QueryResponse {
                kind: QueryResponseKind::ViewCode(ContractCodeView {
                    code: vec![],
                    hash: CryptoHash::default(),
                }),
                block_height,
                block_hash: *block_hash,
            }),
            QueryRequest::ViewAccessKeyList { .. } => Ok(QueryResponse {
                kind: QueryResponseKind::AccessKeyList(AccessKeyList {
                    keys: vec![AccessKeyInfoView {
                        public_key: PublicKey::empty(KeyType::ED25519),
                        access_key: AccessKey::full_access().into(),
                    }],
                }),
                block_height,
                block_hash: *block_hash,
            }),
            QueryRequest::ViewAccessKey { .. } => Ok(QueryResponse {
                kind: QueryResponseKind::AccessKey(AccessKey::full_access().into()),
                block_height,
                block_hash: *block_hash,
            }),
            QueryRequest::ViewState { .. } => Ok(QueryResponse {
                kind: QueryResponseKind::ViewState(ViewStateResult {
                    values: Default::default(),
                    proof: vec![],
                }),
                block_height,
                block_hash: *block_hash,
            }),
            QueryRequest::CallFunction { .. } => Ok(QueryResponse {
                kind: QueryResponseKind::CallResult(CallResult {
                    result: Default::default(),
                    logs: Default::default(),
                }),
                block_height,
                block_hash: *block_hash,
            }),
        }
    }

    fn obtain_state_part(
        &self,
        _shard_id: ShardId,
        _block_hash: &CryptoHash,
        state_root: &StateRoot,
        part_id: PartId,
    ) -> Result<Vec<u8>, Error> {
        if part_id.idx != 0 {
            return Ok(vec![]);
        }
        let state = self.state.read().get(state_root).unwrap().clone();
        let data = borsh::to_vec(&state).expect("should never fall");
        Ok(data)
    }

    fn validate_state_part(&self, _state_root: &StateRoot, _part_id: PartId, _data: &[u8]) -> bool {
        // We do not care about deeper validation in test_utils
        true
    }

    fn apply_state_part(
        &self,
        _shard_id: ShardId,
        state_root: &StateRoot,
        part_id: PartId,
        data: &[u8],
        _epoch_id: &EpochId,
    ) -> Result<(), Error> {
        if part_id.idx != 0 {
            return Ok(());
        }
        let state = KVState::try_from_slice(data).unwrap();
        self.state.write().insert(*state_root, state.clone());
        let data = borsh::to_vec(&state)?;
        let state_size = data.len() as u64;
        self.state_size.write().insert(*state_root, state_size);
        Ok(())
    }

    fn get_state_root_node(
        &self,
        _shard_id: ShardId,
        _block_hash: &CryptoHash,
        state_root: &StateRoot,
    ) -> Result<StateRootNode, Error> {
        let data = borsh::to_vec(&self.state.read().get(state_root).unwrap().clone())
            .expect("should never fall")
            .into();
        let memory_usage = *self.state_size.read().get(state_root).unwrap();
        Ok(StateRootNode { data, memory_usage })
    }

    fn validate_state_root_node(
        &self,
        _state_root_node: &StateRootNode,
        _state_root: &StateRoot,
    ) -> bool {
        // We do not care about deeper validation in test_utils
        true
    }

    fn get_gc_stop_height(&self, block_hash: &CryptoHash) -> BlockHeight {
        if !self.no_gc {
            // This code is 'incorrect' - as production one is always setting the GC to the
            // first block of the epoch.
            // Unfortunately many tests are depending on this and not setting epochs when
            // they produce blocks.
            let block_height = self
                .get_block_header(block_hash)
                .unwrap_or_default()
                .map(|h| h.height())
                .unwrap_or_default();
            block_height.saturating_sub(DEFAULT_GC_NUM_EPOCHS_TO_KEEP * self.epoch_length)
        /*  // TODO: use this version of the code instead - after we fix the block creation
            // issue in multiple tests.
        // We have to return the first block of the epoch T-DEFAULT_GC_NUM_EPOCHS_TO_KEEP.
        let mut current_header = self.get_block_header(block_hash).unwrap().unwrap();
        for _ in 0..DEFAULT_GC_NUM_EPOCHS_TO_KEEP {
            let last_block_of_prev_epoch = current_header.next_epoch_id();
            current_header =
                self.get_block_header(&last_block_of_prev_epoch.0).unwrap().unwrap();
        }
        loop {
            if current_header.next_epoch_id().0 == *current_header.prev_hash() {
                break;
            }
            current_header =
                self.get_block_header(current_header.prev_hash()).unwrap().unwrap();
        }
        current_header.height()*/
        } else {
            0
        }
    }

    fn get_protocol_config(&self, _epoch_id: &EpochId) -> Result<ProtocolConfig, Error> {
        Err(Error::Other("get_protocol_config should not be used in KeyValueRuntime".into()))
    }

    fn get_runtime_config(&self, _protocol_version: ProtocolVersion) -> &RuntimeConfig {
        &self.runtime_config
    }

    fn will_shard_layout_change_next_epoch(
        &self,
        _parent_hash: &CryptoHash,
    ) -> Result<bool, Error> {
        Ok(false)
    }

    fn compiled_contract_cache(&self) -> &dyn ContractRuntimeCache {
        &self.contract_cache
    }

    fn precompile_contracts(
        &self,
        _epoch_id: &EpochId,
        _contract_codes: Vec<ContractCode>,
    ) -> Result<(), Error> {
        // Note that KeyValueRuntime does not use compiled contract cache, so this is no-op.
        Ok(())
    }
}
