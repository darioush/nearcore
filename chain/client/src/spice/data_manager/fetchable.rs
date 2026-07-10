//! SKETCH. The pluggable per-type surface of the one fetch engine. Generic scheduling
//! lives in the engine; only the per-kind knobs below differ.

use super::item::{DataId, TransferUnit};
use super::reputation::Misbehavior;
use near_primitives::hash::CryptoHash;
use near_primitives::types::AccountId;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Interest {
    /// This node neither needs nor produces the item.
    NotNeeded,
    /// Needed, but the existence gate hasn't opened yet — wait for the push, don't pull.
    WaitForPush,
    /// Needed and plausibly produced — arm the pull.
    Fetchable,
}

/// One data type's configuration.
pub(crate) trait DataKind {
    /// Candidate producers that can serve this item, derived from epoch info.
    fn sources(&self, id: &DataId) -> Result<Vec<AccountId>, near_chain::Error>;

    /// Serve-side authorization: may `who` receive this item? The consumer-side mirror of `sources`.
    fn is_entitled(&self, id: &DataId, who: &AccountId) -> Result<bool, near_chain::Error>;

    /// Erasure-coded (K-of-N) vs whole blob (K=1, content-addressed).
    fn transfer_unit(&self) -> TransferUnit;

    /// Does this node need this item, and has its existence gate opened?
    fn interest(&self, id: &DataId) -> Result<Interest, near_chain::Error>;

    /// Distribution-level verification on completion; returns the culprit on failure.
    /// Semantic validation (state transition / receipt root) is consumer-side, not here.
    fn verify_assembled(&self, id: &DataId, bytes: &[u8]) -> Result<(), Misbehavior>;

    /// Is the durable artifact meaning "we're done with this item" present?
    /// The durable artifact, not the raw data — e.g. a validator's done = its endorsement exists.
    fn is_done(&self, id: &DataId) -> Result<bool, near_chain::Error>;
}

/// `Witness{block, shard}` — coded; sources = chunk producers of `shard`;
/// need = assigned chunk validator; seed = block processed.
pub(crate) struct WitnessKind;

/// `ReceiptProof{block, from, to}` — coded; sources = producers of `from`;
/// need = apply `to` next block; seed = executor's apply-attempt missing-set.
pub(crate) struct ReceiptProofKind;

/// `ContractCode{code_hash}` — content-addressed blob, one item across blocks/shards;
/// sources = anchor shard's producers; need = a hash not in the compiled-contract cache.
pub(crate) struct ContractCodeKind;

pub(crate) fn seed_contract_code_items(
    _block_hash: CryptoHash,
    _accessed: &[near_primitives::stateless_validation::contract_distribution::CodeHash],
) -> Vec<DataId> {
    Vec::new() // sketch
}
