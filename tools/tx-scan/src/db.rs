use crate::source::{
    AccountSource, BlockSource, ContractStatus, HistoricalAccountSource, ScannedBlock,
    ScannedChunk, ScannedTransaction,
};
use anyhow::{Context, anyhow};
use borsh::BorshDeserialize;
use near_chain_configs::GenesisValidationMode;
use near_chain_primitives::Error;
use near_epoch_manager::{EpochManager, EpochManagerAdapter, EpochManagerHandle};
use near_primitives::account::Account;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::{ShardLayout, ShardUId};
use near_primitives::state::FlatStateValue;
use near_primitives::trie_key::TrieKey;
use near_primitives::types::{AccountId, BlockHeight, EpochId, StateRoot};
use near_primitives::views::ActionView;
use near_store::adapter::StoreAdapter;
use near_store::adapter::chain_store::ChainStoreAdapter;
use near_store::adapter::chunk_store::ChunkStoreAdapter;
use near_store::adapter::trie_store::get_shard_uid_mapping;
use near_store::flat::FlatStorageStatus;
use near_store::trie::AccessOptions;
use near_store::{Mode, NodeStorage, Store, Trie, TrieDBStorage};
use nearcore::load_config;
use std::path::Path;
use std::sync::Arc;

/// Reads blocks straight out of a node's RocksDB, opened read only. For a
/// split-storage archival node the split store is used, so cold blocks are
/// reachable.
pub struct StoreSource {
    store: Store,
    chain_store: ChainStoreAdapter,
    chunk_store: ChunkStoreAdapter,
    epoch_manager: Arc<EpochManagerHandle>,
    chain_id: String,
}

impl StoreSource {
    pub fn open(home_dir: &Path) -> anyhow::Result<Self> {
        let near_config = load_config(home_dir, GenesisValidationMode::UnsafeFast)
            .with_context(|| format!("loading config from {}", home_dir.display()))?;
        let opener = NodeStorage::opener(
            home_dir,
            &near_config.config.store,
            near_config.config.cold_store.as_ref(),
            near_config.cloud_storage_context(),
        );
        let storage = opener.open_in_mode(Mode::ReadOnly).context("opening storage")?;
        let store = storage.get_split_store().unwrap_or_else(|| storage.get_hot_store());
        let epoch_manager = EpochManager::new_arc_handle(
            store.clone(),
            &near_config.genesis.config,
            Some(home_dir),
        );
        Ok(Self {
            chain_store: store.chain_store(),
            chunk_store: store.chunk_store(),
            epoch_manager,
            chain_id: near_config.genesis.config.chain_id.clone(),
            store,
        })
    }

    pub fn chain_id(&self) -> &str {
        &self.chain_id
    }

    fn shard_uid_of(
        &self,
        account_id: &AccountId,
        epoch_id: &EpochId,
    ) -> anyhow::Result<(ShardUId, ShardLayout)> {
        let shard_layout = self.epoch_manager.get_shard_layout(epoch_id)?;
        let shard_id = shard_layout.account_id_to_shard_id(account_id);
        Ok((ShardUId::from_shard_id_and_layout(shard_id, &shard_layout), shard_layout))
    }

    /// Trie values are stored under the parent shard after a resharding, so
    /// every read of the `State` column goes through the mapping.
    fn mapped_shard_uid(&self, shard_uid: ShardUId) -> ShardUId {
        get_shard_uid_mapping(&self.store, shard_uid)
    }

    fn account_at_state_root(
        &self,
        account_id: &AccountId,
        shard_uid: ShardUId,
        state_root: StateRoot,
    ) -> anyhow::Result<ContractStatus> {
        let storage = TrieDBStorage::new(self.store.trie_store(), self.mapped_shard_uid(shard_uid));
        let trie = Trie::new(Arc::new(storage), state_root, None);
        let key = TrieKey::Account { account_id: account_id.clone() }.to_vec();
        match trie.get(&key, AccessOptions::NO_SIDE_EFFECTS) {
            Ok(Some(bytes)) => decode_contract_status(&bytes),
            Ok(None) => Ok(ContractStatus::AccountNotFound),
            // Missing trie nodes mean the state was dropped, not that the
            // account has no contract.
            Err(_) => Ok(ContractStatus::Unknown),
        }
    }
}

fn decode_contract_status(bytes: &[u8]) -> anyhow::Result<ContractStatus> {
    let account = Account::try_from_slice(bytes).context("decoding account record")?;
    Ok(ContractStatus::from_account_contract(account.contract().as_ref()))
}

impl AccountSource for StoreSource {
    /// Reads the account from flat storage, which only ever holds the state at
    /// the chain head.
    fn contract_status(&self, account_id: &AccountId) -> anyhow::Result<ContractStatus> {
        let head = self.chain_store.head()?;
        let (shard_uid, _) = self.shard_uid_of(account_id, &head.epoch_id)?;
        let flat_store = self.store.flat_store();
        if !matches!(flat_store.get_flat_storage_status(shard_uid), FlatStorageStatus::Ready(_)) {
            return Ok(ContractStatus::Unknown);
        }
        let key = TrieKey::Account { account_id: account_id.clone() }.to_vec();
        let Some(value) = flat_store.get(shard_uid, &key) else {
            return Ok(ContractStatus::AccountNotFound);
        };
        match value {
            FlatStateValue::Inlined(bytes) => decode_contract_status(&bytes),
            FlatStateValue::Ref(value_ref) => {
                let mapped = self.mapped_shard_uid(shard_uid);
                match self.store.trie_store().get(mapped, &value_ref.hash) {
                    Ok(bytes) => decode_contract_status(&bytes),
                    Err(_) => Ok(ContractStatus::Unknown),
                }
            }
        }
    }
}

impl HistoricalAccountSource for StoreSource {
    fn contract_status_at_height(
        &self,
        account_id: &AccountId,
        height: BlockHeight,
    ) -> anyhow::Result<ContractStatus> {
        let Some(block_hash) = missing_is_none(self.chain_store.get_block_hash_by_height(height))?
        else {
            return Ok(ContractStatus::Unknown);
        };
        let Some(header) = missing_is_none(self.chain_store.get_block_header(&block_hash))? else {
            return Ok(ContractStatus::Unknown);
        };
        let (shard_uid, _) = self.shard_uid_of(account_id, header.epoch_id())?;
        // The chunk extra of a block holds the state root after that block's
        // chunk was applied, which is the state as of `height`.
        let Ok(chunk_extra) = self.chunk_store.get_chunk_extra(&block_hash, &shard_uid) else {
            return Ok(ContractStatus::Unknown);
        };
        self.account_at_state_root(account_id, shard_uid, *chunk_extra.state_root())
    }
}

impl StoreSource {
    /// Shard the chain actually put this account's transactions on, as the scan
    /// observed it. Disagreement with the shard layout means the mapping used
    /// for state reads is wrong, so it is worth failing on.
    pub fn check_recorded_shard(
        &self,
        account_id: &AccountId,
        height: BlockHeight,
        recorded: near_primitives::types::ShardId,
    ) -> anyhow::Result<()> {
        let Some(block_hash) = missing_is_none(self.chain_store.get_block_hash_by_height(height))?
        else {
            return Ok(());
        };
        let Some(header) = missing_is_none(self.chain_store.get_block_header(&block_hash))? else {
            return Ok(());
        };
        let (shard_uid, _) = self.shard_uid_of(account_id, header.epoch_id())?;
        if shard_uid.shard_id() != recorded {
            return Err(anyhow!(
                "{account_id} sent from shard {recorded} at height {height}, but the shard layout \
                 maps it to {}",
                shard_uid.shard_id()
            ));
        }
        Ok(())
    }
}

fn missing_is_none<T>(result: Result<T, Error>) -> anyhow::Result<Option<T>> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(Error::DBNotFoundErr(_)) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

impl BlockSource for StoreSource {
    fn head_height(&self) -> anyhow::Result<BlockHeight> {
        Ok(self.chain_store.head()?.height)
    }

    fn block_at_height(&self, height: BlockHeight) -> anyhow::Result<Option<ScannedBlock>> {
        let Some(block_hash) = missing_is_none(self.chain_store.get_block_hash_by_height(height))?
        else {
            return Ok(None);
        };
        let Some(block) = missing_is_none(self.chain_store.get_block(&block_hash))? else {
            return Ok(None);
        };
        let mut chunks = Vec::new();
        for header in block.chunks().iter_raw() {
            if header.height_included() != height {
                continue;
            }
            // An empty transaction list merkleizes to the default hash, so the
            // header alone rules the chunk out and its body never gets read.
            // Most chunks are empty, so this removes most of the reads.
            if header.tx_root() == &CryptoHash::default() {
                chunks.push(ScannedChunk { shard_id: header.shard_id(), transactions: vec![] });
                continue;
            }
            let chunk = self.chunk_store.get_chunk(&header.chunk_hash()).with_context(|| {
                format!("reading chunk of shard {} at {height}", header.shard_id())
            })?;
            let transactions = chunk
                .to_transactions()
                .iter()
                .map(|signed_transaction| {
                    let transaction = &signed_transaction.transaction;
                    ScannedTransaction {
                        hash: signed_transaction.get_hash(),
                        signer_id: transaction.signer_id().clone(),
                        nonce_index: transaction.nonce().nonce_index(),
                        actions: transaction
                            .actions()
                            .iter()
                            .map(|action| ActionView::from(action.clone()))
                            .collect(),
                    }
                })
                .collect();
            chunks.push(ScannedChunk { shard_id: header.shard_id(), transactions });
        }
        chunks.sort_by_key(|chunk| chunk.shard_id);
        Ok(Some(ScannedBlock { height, chunks }))
    }
}
