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
    FunctionCall { payload_bytes: u64, attached_gas: Gas },
    /// Charged per byte of contract code.
    DeployContract { code_bytes: u64 },
    /// Charged once per action, plus per byte of the null terminated method names.
    AddFunctionCallKey { method_names_bytes: u64 },
    /// Bytes of a value a contract returned, charged per output data receiver
    /// by `new_data_receipt_byte`.
    ReturnedData { payload_bytes: u64 },
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
