use near_primitives::hash::CryptoHash;
use near_primitives::types::{AccountId, BlockHeight, Gas, ProtocolVersion, ShardId};
use serde::{Deserialize, Serialize};

/// What created a set of receipts: a signed transaction, or another receipt.
///
/// A transaction producer can be given more gas by whoever signs it. A receipt
/// producer cannot: its budget was fixed by already deployed code.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum Producer {
    Transaction { tx_hash: CryptoHash, signer_id: AccountId },
    Receipt { receipt_id: CryptoHash, receiver_id: AccountId },
}

/// Something a producer put in one of its children that a per-byte fee is
/// charged on, reduced to which parameter applies and how many units it covers.
/// Covers both the actions of an action receipt and the value carried by a data
/// receipt.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum ChargedItem {
    /// Charged per byte of `method_name` plus `args`, and once per action.
    /// The name is kept so the census can say which method a call was to, not
    /// just which contract.
    FunctionCall { method_name: String, payload_bytes: u64, attached_gas: Gas },
    /// Charged per byte of contract code.
    DeployContract { code_bytes: u64 },
    /// An added access key. The permission decides which fee applies:
    /// `add_full_access_key` has no per-byte part, while the function call
    /// forms are charged per byte of the null terminated method names.
    AddKey {
        permission: AddedKeyPermission,
        method_names_bytes: u64,
        /// Spending limit on a function call key, in yoctoNEAR. `None` is
        /// unlimited, and full access keys have no allowance at all.
        allowance: Option<u128>,
        /// Prepaid balance a gas key carries, in yoctoNEAR.
        gas_key_balance: Option<u128>,
    },
    /// Bytes of a value a contract returned, charged per output data receiver
    /// by `new_data_receipt_byte`.
    ReturnedData { payload_bytes: u64 },
    /// Charged per byte of the global contract code being published.
    DeployGlobalContract { code_bytes: u64 },
    /// Charged per byte of the identifier, which is a 32 byte code hash or an
    /// account id, so it is far smaller than the code it names.
    UseGlobalContract { identifier_bytes: u64 },
    /// Every other action kind, kept so the census covers all of them.
    Other { kind: String },
}

/// One receipt a producer created, action or data, with the fields the fee
/// analysis reads.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ChildReceipt {
    pub receipt_id: CryptoHash,
    pub receiver_id: AccountId,
    /// Sum of gas attached to this receipt's function calls, after any weight
    /// distribution has already been applied.
    pub attached_gas: Gas,
    /// True when the producer sent this receipt to itself, which is the
    /// condition `send_sir` fees are charged under.
    pub is_self_call: bool,
    /// False when `attached_gas` is a round multiple of 0.1 Tgas, which marks a
    /// hardcoded constant. True values were computed from the gas left over, so
    /// they shrink when the producer burns more.
    pub attached_gas_is_derived: bool,
    pub items: Vec<ChargedItem>,
}

/// Everything one producer did in one chunk.
///
/// `gas_left_after_constant_children` is the amount of extra burn the producer
/// could absorb before failing: gas that was either refunded or handed to
/// children whose attachment was computed from the leftover. It is `None` for a
/// transaction producer, whose limit is its own attached gas and signer balance
/// rather than a runtime gas counter.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ProducerRow {
    pub block_height: BlockHeight,
    pub shard_id: ShardId,
    pub protocol_version: ProtocolVersion,
    pub producer: Producer,
    pub prepaid_gas: Option<Gas>,
    pub gas_burnt: Option<Gas>,
    pub gas_left_after_constant_children: Option<Gas>,
    pub children: Vec<ChildReceipt>,
}

/// Attached gas at or below this granularity is treated as a hardcoded
/// constant. Contracts write round numbers; gas computed from a leftover
/// almost never lands on one.
/// Which of the four `AccessKeyPermission` forms a key was added with. Only
/// the function call forms pay `add_function_call_key_byte`.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum AddedKeyPermission {
    FullAccess,
    FunctionCall,
    GasKeyFullAccess,
    GasKeyFunctionCall,
}

impl AddedKeyPermission {
    /// True for the two forms charged `add_function_call_key_base` and
    /// `add_function_call_key_byte`.
    pub fn is_function_call(&self) -> bool {
        matches!(self, Self::FunctionCall | Self::GasKeyFunctionCall)
    }
}

pub const CONSTANT_ATTACHED_GAS_GRANULARITY: u64 = 100_000_000_000;

pub fn attached_gas_is_derived(attached_gas: Gas) -> bool {
    attached_gas.as_gas() % CONSTANT_ATTACHED_GAS_GRANULARITY != 0
}

/// Per chunk totals, so the scan also answers what a chunk normally holds:
/// how many transactions and receipts it carries, and how much of its gas and
/// compute budget it spends.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ChunkRow {
    pub block_height: BlockHeight,
    pub shard_id: ShardId,
    pub protocol_version: ProtocolVersion,
    pub transactions: u64,
    /// Receipts this chunk produced, from `OutgoingReceipts`.
    pub action_receipts_created: u64,
    pub data_receipts_created: u64,
    /// Outcomes in this chunk whose producer was a receipt rather than a
    /// transaction, which is what the chunk actually executed.
    pub receipts_processed: u64,
    pub gas_burnt: Gas,
    /// Summed from the outcomes. `None` on outcomes written before compute
    /// costs were recorded, which are counted as zero.
    pub compute_usage: u64,
}

/// Totals the extractor can check against a figure the runtime recorded
/// independently, so a run says whether its own accounting held.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct CrossChecks {
    pub chunks_checked: u64,
    /// Chunks where the summed `gas_burnt` of the outcomes did not equal the
    /// `prev_gas_used` the next block's chunk header recorded. Any mismatch
    /// means outcomes were missed, which silently drops producers.
    pub chunks_with_gas_mismatch: u64,
    pub worst_gas_mismatch: i128,
    /// Receipts a chunk produced that no outcome in that chunk claimed as a
    /// child, which would mean the join missed a producer.
    pub unclaimed_receipts: u64,
    /// Receipts claimed by more than one producer, which cannot happen.
    pub doubly_claimed_receipts: u64,
    /// Compared globally over a range: these differ only by what is in flight
    /// at the two edges, which does not grow as the range gets longer.
    pub receipts_created: u64,
    pub receipts_processed: u64,
    /// Receipts whose prepaid gas did not cover what they burned plus what they
    /// committed to constant children. A nonzero count is a modelling error,
    /// the shape the missing execution fees had.
    pub receipts_with_negative_gas_left: u64,
}
