use near_primitives::account::AccountContract;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{AccountId, BlockHeight, NonceIndex, ShardId};
use near_primitives::views::ActionView;

/// One transaction as the scan needs it. `nonce_index` is `Some` exactly when
/// the transaction was signed by a gas key.
pub struct ScannedTransaction {
    pub hash: CryptoHash,
    pub signer_id: AccountId,
    pub nonce_index: Option<NonceIndex>,
    pub actions: Vec<ActionView>,
}

impl ScannedTransaction {
    pub fn is_gas_key_signed(&self) -> bool {
        self.nonce_index.is_some()
    }
}

/// A chunk that was newly produced at the block being scanned. Chunks that a
/// block re-includes from an earlier height are never reported.
pub struct ScannedChunk {
    pub shard_id: ShardId,
    pub transactions: Vec<ScannedTransaction>,
}

pub struct ScannedBlock {
    pub height: BlockHeight,
    pub chunks: Vec<ScannedChunk>,
}

/// Whether an account holds contract code, as of whichever block the lookup
/// was made against.
#[derive(Clone, Copy, PartialEq, Eq, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContractStatus {
    NoContract,
    LocalContract,
    GlobalContract,
    AccountNotFound,
    /// The data needed to answer was not there: flat storage not ready, or
    /// trie state dropped. Never report this as `NoContract`.
    Unknown,
}

impl ContractStatus {
    pub fn from_account_contract(contract: &AccountContract) -> Self {
        match contract {
            AccountContract::None => ContractStatus::NoContract,
            AccountContract::Local(_) => ContractStatus::LocalContract,
            AccountContract::Global(_) | AccountContract::GlobalByAccount(_) => {
                ContractStatus::GlobalContract
            }
        }
    }
}

/// The chain data the scan reads. The JSON RPC implementation is used for the
/// proof of concept; a RocksDB implementation replaces it for full-range runs
/// without changing the counting code.
pub trait BlockSource: Sync {
    fn head_height(&self) -> anyhow::Result<BlockHeight>;

    /// `None` when no block exists at `height`, which is normal for skipped
    /// heights.
    fn block_at_height(&self, height: BlockHeight) -> anyhow::Result<Option<ScannedBlock>>;
}

pub trait AccountSource: Sync {
    /// Contract state of the account at the chain head.
    fn contract_status(&self, account_id: &AccountId) -> anyhow::Result<ContractStatus>;
}

/// Contract state of an account as it was at a past height. Kept apart from
/// `AccountSource` because it needs archived trie state, which only a database
/// has.
pub trait HistoricalAccountSource: Sync {
    fn contract_status_at_height(
        &self,
        account_id: &AccountId,
        height: BlockHeight,
    ) -> anyhow::Result<ContractStatus>;
}
