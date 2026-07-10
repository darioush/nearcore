//! SKETCH. Centralized admission control: one gate before any buffering.

use super::QosClass;
use super::item::DataId;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{AccountId, BlockHeight};
use std::collections::HashMap;

#[derive(Debug, thiserror::Error)]
pub(crate) enum AdmitError {
    #[error("declared encoded_length exceeds the per-type cap")]
    OversizedDeclared,
    #[error("part/blob size exceeds the per-type cap")]
    OversizedUnit,
    #[error("block is outside [final head, head + speculative_allowance]")]
    OutOfWindow,
    #[error("per-block byte budget exhausted")]
    BlockBudgetExhausted,
    #[error("global byte budget for the {0:?} lane exhausted")]
    ClassBudgetExhausted(QosClass),
    #[error("orphan byte budget (total or per-sender) exhausted")]
    OrphanBudgetExhausted,
    #[error("we neither need nor produce this item")]
    Irrelevant,
    #[error("sender is not a producer / requester is not entitled")]
    Unauthorized,
}

/// INVARIANT: budgets sized above worst-case legitimate traffic, so a
/// `Priority` unit hitting one is a liveness bug.
#[derive(Debug, Clone)]
pub(crate) struct Budgets {
    pub(crate) global_priority_bytes: u64,
    pub(crate) global_background_bytes: u64,
    pub(crate) per_block_bytes: u64,
    pub(crate) orphan_bytes: u64,
    pub(crate) per_sender_orphan_bytes: u64,
    pub(crate) speculative_allowance: BlockHeight,
}

#[derive(Debug, Clone)]
pub(crate) struct SizeCaps {
    pub(crate) max_witness_encoded_len: u64,
    pub(crate) max_receipt_proof_encoded_len: u64,
    pub(crate) max_contract_code_len: u64,
}

/// Units whose block isn't processed yet; bounded total and per-sender, re-run
/// the full gate when the block arrives.
pub(crate) struct OrphanPool {}

pub(crate) struct AdmissionControl {
    budgets: Budgets,
    caps: SizeCaps,
    orphans: OrphanPool,
    used_priority: u64,
    used_background: u64,
    used_per_block: HashMap<CryptoHash, u64>,
}

impl AdmissionControl {
    /// The one gate; unknown-block units go through `admit_orphan` instead.
    pub(crate) fn admit(
        &mut self,
        _id: &DataId,
        _qos: QosClass,
        _sender: &AccountId,
        _declared_len: u64,
        _unit_len: u64,
        _head_height: BlockHeight,
        _final_head_height: BlockHeight,
    ) -> Result<(), AdmitError> {
        Ok(()) // sketch
    }

    pub(crate) fn admit_orphan(
        &mut self,
        _block_hash: &CryptoHash,
        _sender: &AccountId,
        _declared_len: u64,
        _unit_len: u64,
    ) -> Result<(), AdmitError> {
        Ok(()) // sketch
    }

    pub(crate) fn release(&mut self, _id: &DataId, _bytes: u64, _qos: QosClass) {}
}
