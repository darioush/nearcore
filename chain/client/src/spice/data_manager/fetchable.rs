//! SKETCH. The pluggable per-type surface of the one fetch engine.

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
    /// Candidate producers that can serve this item.
    fn sources(&self, id: &DataId) -> Result<Vec<AccountId>, near_chain::Error>;

    /// Serve-side authorization: may `who` receive this item?
    fn is_entitled(&self, id: &DataId, who: &AccountId) -> Result<bool, near_chain::Error>;

    /// Erasure-coded (K-of-N) vs whole blob (K=1, content-addressed).
    fn transfer_unit(&self) -> TransferUnit;

    /// Does this node need this item, and has its existence gate opened?
    fn interest(&self, id: &DataId) -> Result<Interest, near_chain::Error>;

    /// Distribution-level verification on completion; returns the culprit on failure.
    fn verify_assembled(&self, id: &DataId, bytes: &[u8]) -> Result<(), Misbehavior>;

    /// Is the durable artifact meaning "we're done with this item" present?
    fn is_done(&self, id: &DataId) -> Result<bool, near_chain::Error>;
}

/// `Witness{block, shard}` — coded; sources = chunk producers of `shard`; need = assigned validator.
pub(crate) struct WitnessKind;

/// `ReceiptProof{block, from, to}` — coded; sources = producers of `from`; need = apply `to` next block.
pub(crate) struct ReceiptProofKind;

/// `ContractCode{code_hash}` — content-addressed blob; sources = anchor shard's producers; need = uncached hash.
pub(crate) struct ContractCodeKind;

pub(crate) fn seed_contract_code_items(
    _block_hash: CryptoHash,
    _accessed: &[near_primitives::stateless_validation::contract_distribution::CodeHash],
) -> Vec<DataId> {
    Vec::new() // sketch
}
