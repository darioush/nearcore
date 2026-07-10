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
    /// Erasure-coded; produced by `shard`'s chunk producers, needed by its validators.
    Witness { block_hash: CryptoHash, shard_id: ShardId },
    /// Erasure-coded; produced by `from` shard, needed by next-block producers of `to`.
    ReceiptProof { block_hash: CryptoHash, from_shard_id: ShardId, to_shard_id: ShardId },
    /// Content-addressed by `code_hash`; the same code across blocks/shards is one fetch, its context on [`FetchItem::anchor`].
    ContractCode { code_hash: CodeHash },
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
    /// Produced by others; we fetch it.
    Fetch(FetchItem),
    /// Produced by us; we serve it.
    Produce(ProduceState),
}

/// Data we author. Holds no bytes: the artifact lives in the store, re-served from the
/// manager's byte-budgeted `EncodeCache`.
pub(crate) enum ProduceState {
    /// Assigned to produce it; execution not finished yet.
    Producing,
    /// Artifact in store; serve any requested units.
    Produced,
}

/// The consume-side lifecycle. No `Have` variant; removal is only via head-driven expiry.
#[derive(Debug)]
pub(crate) enum FetchState {
    /// Wanted, seeded from chain; no unit has arrived yet.
    Need,
    /// At least one unit obtained or the existence gate opened; accumulating toward completion.
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
    /// Who sent which unit; retained until expiry so late faults still map back to senders.
    pub(crate) attribution: DataAttribution,
    /// Pull requests on the wire now; suppresses duplicate requests, freed for re-request past `request_timeout`.
    pub(crate) in_flight: Vec<InFlightRequest>,
    /// Retry/backoff bookkeeping (the scheduler owns only deadlines).
    pub(crate) backoff: Backoff,
    /// Arrival of the first unit; starts the `first_unit_pull_delay` clock.
    pub(crate) first_unit_at: Option<Instant>,
    /// Currently armed deadline; `drain_due` discards stale heap entries against it.
    pub(crate) next_deadline: Option<Instant>,
}

/// One outstanding pull request to one peer.
pub(crate) struct InFlightRequest {
    pub(crate) who: AccountId,
    pub(crate) sent_at: Instant,
    /// Requested ordinals; empty ⇒ the whole blob.
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
    /// Ordinals held — a cheap bitset so `missing_ordinals` never touches part buffers.
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
    /// commitment → (ordinal → sender); bad bytes blame the sender, a garbage decode blames the commitment's vouchers.
    pub(crate) coded: HashMap<SpiceDataCommitment, HashMap<u32, AccountId>>,
    /// The blob responder.
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
