//! Spice test-loop tests written in the compressed form: a chain kind and topology, faults armed
//! by sender, a run, then checks on what fired and who endorsed.

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::setup::spice_partial_data_faults::{SpicePartialDataFaults, SpicePartialDataObserved};
use near_async::time::Duration;
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::test_utils::pre_spice_protocol_version;
use near_primitives::types::{AccountId, BlockHeight};
use near_primitives::upgrade_schedule::ProtocolUpgradeVotingSchedule;
use near_primitives::version::ProtocolFeature;
use parking_lot::Mutex;
use std::sync::Arc;

enum ChainKind {
    /// Spice from the first block.
    Spice,
    /// Starts pre-spice and votes straight up to spice.
    PreToSpice,
}

#[derive(Clone, Copy, Debug)]
enum Fault {
    DropParts,
    CorruptParts,
    Equivocate,
    DropAccesses,
}

/// Which producers a fault is armed against, by index.
enum Who {
    All,
    AllBut(usize),
    One(usize),
}

struct Scenario {
    env: TestLoopEnv,
    producers: Vec<AccountId>,
    recipients: Vec<AccountId>,
    faults: Arc<Mutex<SpicePartialDataFaults>>,
    observed: Arc<Mutex<SpicePartialDataObserved>>,
    last_pre_spice: Option<CryptoHash>,
}

impl Scenario {
    /// `producers` produce blocks and chunks, `recipients` only validate chunks, so they endorse
    /// only after receiving the witness.
    fn new(chain: ChainKind, producers: usize, recipients: usize) -> Self {
        init_test_logger();
        let mut builder = TestLoopBuilder::new().validators(producers, recipients);
        if let ChainKind::PreToSpice = chain {
            builder = builder
                .epoch_length(10)
                .protocol_version(pre_spice_protocol_version())
                .protocol_upgrade_schedule(ProtocolUpgradeVotingSchedule::new_immediate(
                    ProtocolFeature::Spice.protocol_version(),
                ));
        }
        let mut env = builder.build();
        let accounts =
            env.node_datas.iter().map(|node| node.account_id.clone()).collect::<Vec<_>>();
        let (faults, observed) = env.install_spice_partial_data_faults();
        Self {
            env,
            producers: accounts[..producers].to_vec(),
            recipients: accounts[producers..].to_vec(),
            faults,
            observed,
            last_pre_spice: None,
        }
    }

    fn arm(self, fault: Fault, who: Who) -> Self {
        let senders: Vec<AccountId> = match who {
            Who::All => self.producers.clone(),
            Who::AllBut(index) => self
                .producers
                .iter()
                .enumerate()
                .filter(|(i, _)| *i != index)
                .map(|(_, a)| a.clone())
                .collect(),
            Who::One(index) => vec![self.producers[index].clone()],
        };
        let mut faults = self.faults.lock();
        let set = match fault {
            Fault::DropParts => &mut faults.drop_from,
            Fault::CorruptParts => &mut faults.corrupt_from,
            Fault::Equivocate => &mut faults.equivocate_from,
            Fault::DropAccesses => &mut faults.drop_contract_accesses_from,
        };
        set.extend(senders);
        drop(faults);
        self
    }

    fn observer(&self) -> &AccountId {
        &self.recipients[0]
    }

    /// Runs until the head is the first spice block, and remembers its parent.
    fn run_to_first_spice_block(mut self) -> Self {
        let observer = self.observer().clone();
        self.env
            .runner_for_account(&observer)
            .run_until(|node| node.head_block().is_spice_block(), Duration::seconds(100));
        let head = self.env.node_for_account(&observer).head_block();
        self.last_pre_spice = Some(*head.header().prev_hash());
        self
    }

    /// Runs `heights` blocks past the certified frontier; panics if certification stalls.
    fn run_certified(mut self, heights: BlockHeight) -> Self {
        let observer = self.observer().clone();
        let target =
            self.env.node_for_account(&observer).last_certified_block_header().height() + heights;
        self.env.runner_for_account(&observer).run_until_certified(target);
        self
    }

    fn fired(self, fault: Fault) -> Self {
        let observed = self.observed.lock();
        let count = match fault {
            Fault::DropParts => observed.dropped,
            Fault::CorruptParts => observed.corrupted,
            Fault::Equivocate => observed.equivocated,
            Fault::DropAccesses => observed.dropped_contract_accesses,
        };
        assert!(count > 0, "{fault:?} never fired");
        drop(observed);
        self
    }

    fn every_recipient_requested(self) -> Self {
        for recipient in &self.recipients {
            assert!(
                self.observed.lock().data_requests.contains_key(recipient),
                "{recipient} never requested"
            );
        }
        self
    }

    fn no_requests(self) -> Self {
        assert!(self.observed.lock().data_requests.is_empty(), "someone had to request data");
        self
    }

    fn every_recipient_endorsed(self) -> Self {
        for recipient in &self.recipients {
            assert!(
                self.observed.lock().endorsements.contains_key(recipient),
                "{recipient} never endorsed"
            );
        }
        self
    }

    fn no_recipient_endorsed(self) -> Self {
        for recipient in &self.recipients {
            assert!(
                !self.observed.lock().endorsements.contains_key(recipient),
                "{recipient} endorsed"
            );
        }
        self
    }

    /// Some recipient endorsed a chunk of the last pre-spice block. Under pull recovery a
    /// recipient recovers only some heights, so not every recipient has to.
    fn some_recipient_endorsed_the_last_pre_spice_block(self) -> Self {
        let last_pre_spice = self.last_pre_spice.expect("run to the first spice block first");
        let observed = self.observed.lock();
        assert!(
            observed
                .endorsements
                .values()
                .flatten()
                .any(|chunk| chunk.block_hash == last_pre_spice),
            "no recipient endorsed the last pre-spice block"
        );
        drop(observed);
        self
    }

    /// Every recipient endorsed a chunk of the last pre-spice block: the boundary witness reached
    /// it despite the faults.
    fn every_recipient_endorsed_the_last_pre_spice_block(self) -> Self {
        let last_pre_spice = self.last_pre_spice.expect("run to the first spice block first");
        let observed = self.observed.lock();
        for recipient in &self.recipients {
            let endorsed = observed.endorsements.get(recipient).into_iter().flatten();
            assert!(
                endorsed.clone().any(|chunk| chunk.block_hash == last_pre_spice),
                "{recipient} never endorsed the last pre-spice block"
            );
        }
        drop(observed);
        self
    }
}

// ---- Regenerated from the compressed forms of existing tests ----

// DF1  given D1 ; drop-parts P1..P3 ; drop-accesses all P | run cert+8 → fired(drop) ;
//      fired(drop-accesses) ; requested(every R) ; endorsed(every R)
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn df1_one_answering_producer() {
    Scenario::new(ChainKind::Spice, 4, 2)
        .arm(Fault::DropParts, Who::AllBut(0))
        .arm(Fault::DropAccesses, Who::All)
        .run_certified(8)
        .fired(Fault::DropParts)
        .fired(Fault::DropAccesses)
        .every_recipient_requested()
        .every_recipient_endorsed();
}

// DF5  given D1 ; corrupt-parts P0 | run cert+4 → fired(corrupt) ; no-requests ; endorsed(every R)
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn df5_corrupted_parts() {
    Scenario::new(ChainKind::Spice, 4, 2)
        .arm(Fault::CorruptParts, Who::One(0))
        .run_certified(4)
        .fired(Fault::CorruptParts)
        .no_requests()
        .every_recipient_endorsed();
}

// DF6  given D1 ; equivocate P0 | run cert+4 → fired(equivocate) ; no-requests ; endorsed(every R)
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn df6_equivocating_producer() {
    Scenario::new(ChainKind::Spice, 4, 2)
        .arm(Fault::Equivocate, Who::One(0))
        .run_certified(4)
        .fired(Fault::Equivocate)
        .no_requests()
        .every_recipient_endorsed();
}

// DF9  given D1 ; drop-parts all P | run cert+4 → fired(drop) ; ¬endorsed(every R)
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn df9_starve_the_recipients() {
    Scenario::new(ChainKind::Spice, 4, 2)
        .arm(Fault::DropParts, Who::All)
        .run_certified(4)
        .fired(Fault::DropParts)
        .no_recipient_endorsed();
}

// ---- New: data faults across the activation boundary (empty cell #1) ----

// given chain(pre→spice), 4P+2R ; corrupt-parts P0 | run →FS ; run cert+4 →
//   fired(corrupt) ; endorsed(every R, LPS)
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn gap_corrupted_parts_across_the_activation_boundary() {
    Scenario::new(ChainKind::PreToSpice, 4, 2)
        .arm(Fault::CorruptParts, Who::One(0))
        .run_to_first_spice_block()
        .run_certified(4)
        .fired(Fault::CorruptParts)
        .every_recipient_endorsed_the_last_pre_spice_block();
}

// given chain(pre→spice), 4P+2R ; drop-parts P1..P3 ; drop-accesses all P | run →FS ; run cert+8 →
//   fired(drop) ; requested(every R) ; endorsed(every R) ; endorsed(some R, LPS)
// Recipients recover scattered heights on both sides of the boundary (one missed LPS..LPS+2),
// the partial recovery data_faults.rs leaves as a TODO, so only some recipient must endorse LPS.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn gap_one_answering_producer_across_the_activation_boundary() {
    Scenario::new(ChainKind::PreToSpice, 4, 2)
        .arm(Fault::DropParts, Who::AllBut(0))
        .arm(Fault::DropAccesses, Who::All)
        .run_to_first_spice_block()
        .run_certified(8)
        .fired(Fault::DropParts)
        .every_recipient_requested()
        .every_recipient_endorsed()
        .some_recipient_endorsed_the_last_pre_spice_block();
}
