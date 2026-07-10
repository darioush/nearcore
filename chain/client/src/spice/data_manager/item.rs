//! Per-item identity, lifecycle state, assembly buffers, and sender attribution.

use super::QosClass;
use super::scheduler::Backoff;
use near_async::time::Instant;
use near_primitives::hash::CryptoHash;
use near_primitives::spice::partial_data::SpiceDataCommitment;
use near_primitives::stateless_validation::contract_distribution::CodeHash;
use near_primitives::types::{AccountId, BlockHeight, ShardId};
use std::collections::HashMap;

/// Unified content id across all fetchable data types; versioned on the wire.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum DataId {
    Witness {
        block_hash: CryptoHash,
        shard_id: ShardId,
    },
    ReceiptProof {
        block_hash: CryptoHash,
        from_shard_id: ShardId,
        to_shard_id: ShardId,
    },
    /// Content-addressed; its interested context lives on [`FetchItem::anchor`].
    ContractCode {
        code_hash: CodeHash,
    },
}

impl DataId {
    /// Anchor block for coded kinds; `None` for contract code.
    pub(crate) fn block_hash(&self) -> Option<&CryptoHash> {
        match self {
            DataId::Witness { block_hash, .. } | DataId::ReceiptProof { block_hash, .. } => {
                Some(block_hash)
            }
            DataId::ContractCode { .. } => None,
        }
    }

    pub(crate) fn transfer_unit(&self) -> TransferUnit {
        match self {
            DataId::Witness { .. } | DataId::ReceiptProof { .. } => TransferUnit::ErasureCoded,
            DataId::ContractCode { .. } => TransferUnit::Blob,
        }
    }
}

/// How the payload is transferred/assembled. The scheduler and scoring ignore this.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransferUnit {
    /// K of N Reed–Solomon parts.
    ErasureCoded,
    /// A single blob whose hash is the id. (K=1.)
    Blob,
}

/// State of one tracked piece of data; the variant's origin is fixed for its lifetime.
pub(crate) enum Item {
    Fetch(FetchItem),
    Produce(ProduceState),
}

/// Data we author. Holds no bytes: the artifact lives in the store, re-served from the
/// manager's byte-budgeted `EncodeCache`.
pub(crate) enum ProduceState {
    Producing,
    Produced,
}

/// The consume-side lifecycle. No `Have` variant; removal is only via head-driven expiry.
#[derive(Debug)]
pub(crate) enum FetchState {
    Need,
    Collecting(Assembly),
    /// Bytes handed to the consumer; awaiting its `Verified`/`Failed`. Keeps a re-pushed part from re-entering `Collecting`.
    Delivered,
    /// Local processing done; only the attribution husk remains, retained until expiry.
    ProcessedLocally,
}

/// Full state of one piece of data we obtain; identity lives in the map key, not here.
pub(crate) struct FetchItem {
    pub(crate) state: FetchState,
    /// Resolved QoS lane: the max over this item's fetch causes.
    pub(crate) qos: QosClass,
    /// Anchor block height, captured at seed time so expiry needs no store lookups.
    pub(crate) height: BlockHeight,
    /// Contract code only: highest-block (block, shard) wanting this hash; shard names the source pool, block resolves the epoch.
    pub(crate) anchor: Option<(CryptoHash, ShardId)>,
    pub(crate) attribution: DataAttribution,
    /// Pull requests on the wire now; suppresses duplicate requests, freed for re-request past `request_timeout`.
    pub(crate) in_flight: Vec<InFlightRequest>,
    pub(crate) backoff: Backoff,
    /// Arrival of the first unit; starts the `first_unit_pull_delay` clock.
    pub(crate) first_unit_at: Option<Instant>,
    pub(crate) next_deadline: Option<Instant>,
}

pub(crate) struct InFlightRequest {
    pub(crate) who: AccountId,
    pub(crate) sent_at: Instant,
    /// Empty ⇒ the whole blob.
    pub(crate) ordinals: Vec<u32>,
}

/// The accumulation buffer, held only from first unit to delivery then dropped.
pub(crate) enum Assembly {
    /// Parallel trackers per commitment: a fabricated commitment must not lock out
    /// assembly under the honest one. Whichever tracker reaches K wins.
    Coded { trackers: HashMap<SpiceDataCommitment, CodedTracker> },
    /// K=1: the first matching response completes and delivers in the same call.
    Blob { expected: CodeHash },
}

pub(crate) struct CodedTracker {
    // tracker: ReedSolomonPartsTracker<SpiceData>, concrete-by-name so no generic climbs up to `Item` and breaks the one-map premise.
    pub(crate) have_ordinals: Vec<bool>,
}

impl Assembly {
    pub(crate) fn is_complete(&self) -> bool {
        false // sketch
    }

    /// Union of gaps across trackers, so a fake-majority commitment can't starve the
    /// honest tracker. Empty for blob.
    pub(crate) fn missing_ordinals(&self) -> Vec<u32> {
        Vec::new() // sketch
    }
}

/// Which sender contributed which unit, per commitment, so a fault can be pinned.
#[derive(Default)]
pub(crate) struct DataAttribution {
    pub(crate) coded: HashMap<SpiceDataCommitment, HashMap<u32, AccountId>>,
    pub(crate) blob_sender: Option<AccountId>,
}

impl DataAttribution {
    /// Culprit set if its decode yields garbage.
    pub(crate) fn vouchers(&self, _commitment: &SpiceDataCommitment) -> Vec<AccountId> {
        Vec::new() // sketch
    }

    /// Culprit set for a semantic `Failed`.
    pub(crate) fn all(&self) -> Vec<AccountId> {
        Vec::new() // sketch
    }
}
