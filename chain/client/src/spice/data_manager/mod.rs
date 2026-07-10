//! `SpiceDataManager` — one per-node generic fetch engine for all SPICE distributed
//! data (witness, receipt proof, contract code). Semantic validation stays consumer-side.

// Sketching
#![allow(dead_code)]

mod admission;
mod fetchable;
mod item;
mod messages;
mod reputation;
mod scheduler;
mod serve;

pub(crate) use admission::{AdmissionControl, AdmitError, Budgets, OrphanPool, SizeCaps};
pub(crate) use fetchable::{ContractCodeKind, DataKind, Interest, ReceiptProofKind, WitnessKind};
pub(crate) use item::{
    Assembly, DataAttribution, DataId, FetchItem, FetchState, InFlightRequest, Item, ProduceState,
    TransferUnit,
};
pub(crate) use messages::{
    DataResponse, FailedEvent, Requester, ResponsePayload, SpiceDataRequest, VerifiedEvent,
    WantUnits,
};
pub(crate) use reputation::{Misbehavior, Reputation, ReputationConfig};
pub(crate) use scheduler::{Backoff, DeadlineScheduler, TimingConfig};
pub(crate) use serve::{CachedEncoding, EncodeCache};

use near_primitives::types::BlockHeight;
use std::collections::{BTreeMap, HashMap};

/// QoS lane carried by every fetch and serve-request. An item takes the max lane over its causes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum QosClass {
    /// Consensus-critical: we are an assigned validator / next-block producer.
    Priority,
    /// RPC / state-sync driven. Bounded by its own budget; never starves `Priority`.
    Background,
}

/// Owns all per-item fetch state, attribution, scheduling, admission, and reputation.
pub(crate) struct SpiceDataManager {
    /// Per-item lifecycle, consume- and produce-side in one map. `Have` lives in the store.
    items: HashMap<DataId, Item>,

    /// Index for head-driven expiry — drain ≤ final execution head, no global scan.
    items_by_height: BTreeMap<BlockHeight, Vec<DataId>>,

    /// Per-item retry/escalation deadlines. Owns `when` only; retry state lives on items.
    scheduler: DeadlineScheduler,

    /// Pre-buffer gate: size caps, distance-to-head, per-class byte budgets, orphan pool.
    admission: AdmissionControl,

    /// SPICE-internal producer reputation; the only failure memory. Aims `select_sources`.
    reputation: Reputation,

    timing: TimingConfig,

    /// Serve-side encoded-parts cache: global byte budget, LRU. Miss ⇒ re-encode from store.
    encode_cache: EncodeCache,
    // Collaborators (epoch manager, chain store, network adapter, event senders, signer)
    // injected at construction; omitted in the sketch.
}

// Illustrative surface — bodies omitted in the sketch.
impl SpiceDataManager {
    /// Seed items this node will need from a processed block. Idempotent; re-admits parked orphans.
    pub(crate) fn seed_needs(&mut self, _block_hash: near_primitives::hash::CryptoHash) {}

    /// A unit arrived: admit, record senders, insert into `Assembly`. On completion,
    /// distribution-verify (decode + hash) and deliver to the consumer.
    pub(crate) fn on_data_received(&mut self, _resp: DataResponse) -> Result<(), AdmitError> {
        Ok(())
    }

    /// A due deadline fired: time out stale in-flight requests, then re-pull the missing ordinals.
    pub(crate) fn on_deadline(&mut self, _id: &DataId) {}

    /// Heads advanced: expire items ≤ final execution head, re-arm any whose gate just opened.
    /// Certification comparator must run BEFORE expiry drops the attribution it needs.
    pub(crate) fn on_heads_advanced(&mut self) {}

    /// Consumer verdict. `Verified` shrinks the item to its attribution husk; `Failed` funnels
    /// into reputation via the retained attribution.
    pub(crate) fn on_verified(&mut self, _ev: VerifiedEvent) {}
    pub(crate) fn on_failed(&mut self, _ev: FailedEvent) {}

    /// Serve a pull request from an `Item::Produce` entry: authorize, resolve from `encode_cache`
    /// (miss ⇒ re-encode), reply on the requester's lane. Not-yet-produced ⇒ signed `NotAvailable`.
    pub(crate) fn serve_request(
        &mut self,
        _req: SpiceDataRequest,
    ) -> Result<DataResponse, AdmitError> {
        Err(AdmitError::Irrelevant) // sketch
    }

    /// Epoch switched: GC reputation entries for accounts that left the producer sets.
    pub(crate) fn on_epoch_switch(&mut self) {}
}
