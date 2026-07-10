//! SKETCH. Wire request (unified across kinds) + consumer-facing events.

use super::QosClass;
use super::item::DataId;
use super::reputation::Misbehavior;
use near_primitives::types::AccountId;

/// Which units the requester wants — always explicit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WantUnits {
    /// Empty is invalid.
    Ordinals(Vec<u32>),
    Blob,
}

/// Who is asking — determines authorization and QoS lane.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Requester {
    Validator(AccountId),
    /// RPC / state-sync: response routes back over the requesting connection only.
    NonValidator,
}

impl Requester {
    pub(crate) fn qos(&self) -> QosClass {
        match self {
            Requester::Validator(_) => QosClass::Priority,
            Requester::NonValidator => QosClass::Background,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SpiceDataRequest {
    pub(crate) data_id: DataId,
    pub(crate) want: WantUnits,
    pub(crate) requester: Requester,
    // signature omitted in the sketch (present for `Validator`).
}

#[derive(Debug)]
pub(crate) struct DataResponse {
    pub(crate) data_id: DataId,
    pub(crate) sender: AccountId,
    pub(crate) qos: QosClass,
    pub(crate) payload: ResponsePayload,
}

#[derive(Debug)]
pub(crate) enum ResponsePayload {
    /// Concrete payload types omitted in the sketch.
    Units,
    /// Signed NAK; carries no liveness penalty.
    NotAvailable,
}

/// Consumer → manager: validation succeeded, artifact persisted.
#[derive(Debug)]
pub(crate) struct VerifiedEvent {
    pub(crate) data_id: DataId,
}

/// Consumer → manager: validation failed; funnels `kind` into reputation.
#[derive(Debug)]
pub(crate) struct FailedEvent {
    pub(crate) data_id: DataId,
    pub(crate) kind: Misbehavior,
}
