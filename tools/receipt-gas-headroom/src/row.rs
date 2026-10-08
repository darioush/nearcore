use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives::hash::CryptoHash;
use near_primitives::types::{AccountId, BlockHeight, Gas, ProtocolVersion, ShardId};
use serde::{Deserialize, Serialize};

/// What created a set of receipts: a signed transaction, or another receipt.
///
/// A transaction producer can be given more gas by whoever signs it. A receipt
/// producer cannot: its budget was fixed by already deployed code.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub enum Producer {
    Transaction { tx_hash: CryptoHash, signer_id: AccountId },
    Receipt { receipt_id: CryptoHash, receiver_id: AccountId, kind: ExecutedReceiptKind },
}

/// Which receipt form executed. A `PromiseYield` callback runs on gas its
/// creator reserved up to `yield_timeout_length_in_blocks` earlier, so whether
/// it had enough is a separate question from an ordinary call.
#[derive(
    Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, BorshSerialize, BorshDeserialize,
)]
pub enum ExecutedReceiptKind {
    Action,
    PromiseYield,
}

/// Something a producer put in one of its children that a per-byte fee is
/// charged on, reduced to which parameter applies and how many units it covers.
/// Covers both the actions of an action receipt and the value carried by a data
/// receipt.
#[derive(Serialize, Deserialize, Clone, Debug, BorshSerialize, BorshDeserialize)]
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
#[derive(Serialize, Deserialize, Clone, Debug, BorshSerialize, BorshDeserialize)]
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
#[derive(Serialize, Deserialize, Clone, Debug, BorshSerialize, BorshDeserialize)]
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
#[derive(
    Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, BorshSerialize, BorshDeserialize,
)]
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
#[derive(Serialize, Deserialize, Clone, Debug, BorshSerialize, BorshDeserialize)]
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
    /// Receipts the range sent that no outcome in it claimed as a child, by
    /// receipt kind. A `PromiseResume` belongs here by construction: it carries
    /// a payload to a promise that yielded earlier and is nobody's child, so
    /// its count is expected rather than a fault. Any other kind is a producer
    /// the analysis never saw.
    pub unclaimed_receipts: u64,
    pub unclaimed_by_kind: std::collections::BTreeMap<String, u64>,
    /// A few of those, with the block they were sent from, so their real
    /// producer can be traced rather than guessed at.
    pub unclaimed_receipt_samples: Vec<(BlockHeight, CryptoHash)>,
    /// How far into the range each unclaimed receipt was sent. Once children
    /// are resolved across the whole range, the only ones left should belong to
    /// producers that ran before it started, so these should crowd the first
    /// blocks and stop. A yield can wait `yield_timeout_length_in_blocks`, and
    /// a congested buffer longer, so a thin tail is expected and a flat spread
    /// across the range is not.
    pub unclaimed_offset_from_range_start: Histogram,
    /// Outcomes the extractor walked past. Each one drops a producer, and its
    /// children then look unclaimed, so these say which skip is responsible.
    pub skipped_outcome_missing: u64,
    pub skipped_receipt_not_stored: u64,
    pub skipped_producer_not_an_action_receipt: u64,
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

/// Counts of values falling in power of two buckets, so a distribution costs a
/// fixed 64 counters however many values it saw. `max` is kept exactly because
/// the largest payload is what a per-byte fee increase hits hardest, and a
/// bucket would round it away.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Histogram {
    pub count: u64,
    pub sum: u128,
    pub max: u64,
    /// `buckets[i]` counts values in `[2^(i-1), 2^i)`, with zero in bucket 0.
    pub buckets: Vec<u64>,
}

impl Histogram {
    pub fn record(&mut self, value: u64) {
        if self.buckets.is_empty() {
            self.buckets = vec![0; 65];
        }
        self.count += 1;
        self.sum += u128::from(value);
        self.max = self.max.max(value);
        let bucket = if value == 0 { 0 } else { 64 - value.leading_zeros() as usize };
        self.buckets[bucket] += 1;
    }

    /// Smallest bucket upper bound at or past the given share of the values.
    /// Reported as a power of two, so it brackets the true percentile rather
    /// than claiming a precision buckets do not have.
    pub fn percentile_upper_bound(&self, share: f64) -> u64 {
        let target = (self.count as f64 * share) as u64;
        let mut seen = 0;
        for (index, count) in self.buckets.iter().enumerate() {
            seen += count;
            if seen >= target {
                return if index == 0 { 0 } else { 1u64 << (index - 1) };
            }
        }
        self.max
    }
}

/// What a run saw, kept as fixed size summaries so a year does not need the
/// rows read back to answer what a chunk normally holds.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Census {
    pub function_call_payload_bytes: Histogram,
    pub deploy_contract_code_bytes: Histogram,
    pub deploy_global_contract_code_bytes: Histogram,
    pub added_key_method_names_bytes: Histogram,
    pub returned_data_payload_bytes: Histogram,
    pub attached_gas: Histogram,
    pub gas_left_after_constant_children: Histogram,
    pub children_per_producer: Histogram,
    pub actions_per_receipt: Histogram,
    /// Counts by action kind, and for added keys by which permission form.
    pub items_by_kind: std::collections::BTreeMap<String, u64>,
    pub added_keys_by_permission: std::collections::BTreeMap<String, u64>,
    pub self_call_children: u64,
    pub derived_gas_children: u64,
    /// Receipts that ran as a yielded callback, on gas their creator reserved
    /// up to `yield_timeout_length_in_blocks` earlier, and how much of that
    /// budget they had left. A callback given a share of what its creator had
    /// left over rather than a fixed amount gets less when the creator burns
    /// more, and finds out that many blocks later.
    pub yield_callbacks_run: u64,
    pub yield_callback_gas_left: Histogram,
}
