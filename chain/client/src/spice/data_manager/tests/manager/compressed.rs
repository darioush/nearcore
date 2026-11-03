//! Tests written in the compressed form: blocks by name, then a script of events, each with
//! the outcome it must have.

use super::*;

/// What a message of parts led to, without its payload.
#[derive(Debug, PartialEq)]
enum Got {
    Collecting,
    Decoded,
    NotWanted,
    AlreadySettled,
    Fault,
}

/// A chain of named blocks and a data manager, driven by a script of events.
struct Scenario {
    chain: Chain,
    manager: TestManager,
    blocks: HashMap<&'static str, Arc<Block>>,
    tip: Arc<Block>,
}

impl Scenario {
    /// A chain holding only its genesis.
    fn new() -> Self {
        let (chain, _) = chain_with_blocks(0);
        let manager = TestManager::new(&chain);
        let tip = chain.genesis_block();
        Self { chain, manager, blocks: HashMap::new(), tip }
    }

    fn block(&self, name: &str) -> &Arc<Block> {
        self.blocks.get(name).unwrap_or_else(|| panic!("no block named {name}"))
    }

    fn item(&self, name: &str) -> DataId {
        receipt_id(self.block(name), 0, 1)
    }

    fn name_of(&self, id: &DataId) -> &'static str {
        self.blocks.iter().find(|(_, block)| block.hash() == id.block_hash()).unwrap().0
    }

    /// Stored blocks on top of the last `chain` block, one height apart.
    fn chain(mut self, names: &[&'static str]) -> Self {
        for &name in names {
            let block =
                process_block_at(&mut self.chain, &self.tip, self.tip.header().height() + 1);
            self.blocks.insert(name, block.clone());
            self.tip = block;
        }
        self
    }

    /// A stored block on `on` at `height`, canonical or not as the chain decides.
    fn stored(mut self, name: &'static str, on: &str, height: BlockHeight) -> Self {
        let prev = self.block(on).clone();
        let block = process_block_at(&mut self.chain, &prev, height);
        self.blocks.insert(name, block);
        self
    }

    /// A block on `on` at `height` the chain does not have yet.
    fn unknown(mut self, name: &'static str, on: &str, height: BlockHeight) -> Self {
        let block = build_block_at(&self.chain, self.block(on), height);
        self.blocks.insert(name, block);
        self
    }

    /// The chain receives the block built by `unknown` and refuses it.
    fn chain_refuses(mut self, name: &str) -> Self {
        let block = self.block(name).clone();
        let result = process_block_sync(
            &mut self.chain,
            block.into(),
            Provenance::PRODUCED,
            &mut BlockProcessingArtifact::default(),
        );
        assert!(result.is_err(), "the chain accepted {name}");
        self
    }

    fn final_head(self, name: &str) -> Self {
        self.manager.set_final_head(self.block(name));
        self
    }

    fn final_execution_head(self, name: &str) -> Self {
        self.manager.set_final_execution_head(self.block(name));
        self
    }

    /// The block's trigger: the manager learns it was processed.
    fn trig(self, name: &str) -> Self {
        self.trig_decodes(name, &[])
    }

    /// The block's trigger decodes exactly the items of `expected`.
    fn trig_decodes(mut self, name: &str, expected: &[&str]) -> Self {
        let hash = *self.block(name).hash();
        let now = self.manager.clock.now();
        let output = self.manager.manager.on_block_processed(&hash, now);
        let decoded: Vec<&str> = output.decoded.iter().map(|(id, _)| self.name_of(id)).collect();
        assert_eq!(decoded, expected, "decoded at the trigger of {name}");
        self
    }

    fn track(mut self, name: &str) -> Self {
        let block = self.block(name).clone();
        self.manager.track_block(&block);
        self
    }

    /// Producer `producer` of shard 0 sends enough parts of the block's item.
    fn parts(mut self, producer: usize, name: &str, expected: Got) -> Self {
        let id = self.item(name);
        let (commitment, mut parts) = encode_to_wire(&encoder(), &receipt_data(0, 1));
        parts.truncate(DATA_PARTS);
        let result =
            self.manager.manager.receive_from(&producers()[producer], &id, &commitment, parts);
        let got = match result {
            Ok(PartsOutcome::Collecting) => Got::Collecting,
            Ok(PartsOutcome::Decoded(_)) => Got::Decoded,
            Ok(PartsOutcome::NotWanted) => Got::NotWanted,
            Ok(PartsOutcome::AlreadySettled) => Got::AlreadySettled,
            Err(_) => Got::Fault,
        };
        assert_eq!(got, expected, "parts from producer {producer} for {name}");
        self
    }

    /// The block's item is decoded and its proof saved, so its next trigger marks it done.
    fn deliver_and_save(mut self, name: &str) -> Self {
        let id = self.item(name);
        self.manager.deliver(&producers()[0], &id, &receipt_data(0, 1));
        save_proof(&self.chain, self.block(name), &receipt_data(0, 1));
        self
    }

    fn is_parked(&self, name: &str) -> bool {
        let hash = self.block(name).hash();
        self.manager.manager.orphans.iter().any(|(parked, _)| parked == hash)
    }

    fn check(self, check: impl FnOnce(&Self)) -> Self {
        check(&self);
        self
    }
}

// ---- Regenerated from the compressed forms of existing tests ----

// O14  given A←B←C, FH=C, trig A | P0 parts item(B) → Collecting ; trig B → [B]
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn o14_canonical_at_or_below_final_head_not_triggered_decodes_at_its_trigger() {
    Scenario::new()
        .chain(&["A", "B", "C"])
        .final_head("C")
        .trig("A")
        .parts(0, "B", Got::Collecting)
        .trig_decodes("B", &["B"]);
}

// O15  given A←B←C4, B←F3, FH=C4, trig A | P0 parts item(F3) → NotWanted
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn o15_fork_at_or_below_final_head_is_not_wanted() {
    Scenario::new()
        .chain(&["A", "B"])
        .stored("F3", "B", 3)
        .stored("C4", "B", 4)
        .final_head("C4")
        .trig("A")
        .parts(0, "F3", Got::NotWanted);
}

// O16  given A←B, A←F2, trig A,B | P0 parts item(F2) → Collecting ; trig F2 → [F2]
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn o16_fork_above_final_head_not_triggered_decodes_at_its_trigger() {
    Scenario::new()
        .chain(&["A", "B"])
        .trig("A")
        .trig("B")
        .stored("F2", "A", 2)
        .parts(0, "F2", Got::Collecting)
        .trig_decodes("F2", &["F2"]);
}

// ---- New: the empty cells of the coverage table ----

// given A←B, A←F2, trig A | P0 parts item(F2) → Collecting ; B←C3, FH=C3 ; trig F2 → [] ; F2 not parked
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn gap_final_head_passes_a_parked_fork_before_its_trigger() {
    Scenario::new()
        .chain(&["A", "B"])
        .stored("F2", "A", 2)
        .trig("A")
        .parts(0, "F2", Got::Collecting)
        .stored("C3", "B", 3)
        .final_head("C3")
        .trig_decodes("F2", &[])
        .check(|s| assert!(!s.is_parked("F2")))
        .check(|s| assert!(!s.manager.is_tracking(&s.item("F2"))));
}

// given A←B←C, trig A | P0 parts item(B) → Collecting ; FEH=B ; trig B → [] ; B not tracked
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn gap_final_execution_head_passes_a_parked_block_before_its_trigger() {
    Scenario::new()
        .chain(&["A", "B", "C"])
        .trig("A")
        .parts(0, "B", Got::Collecting)
        .final_execution_head("B")
        .trig_decodes("B", &[])
        .check(|s| assert!(!s.is_parked("B")))
        .check(|s| assert!(!s.manager.is_tracking(&s.item("B"))));
}

// given A←B←C, FEH=B, trig A | P0 parts item(B) → NotWanted
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn gap_parts_for_a_canonical_block_at_or_below_the_final_execution_head_not_triggered() {
    Scenario::new().chain(&["A", "B", "C"]).final_execution_head("B").trig("A").parts(
        0,
        "B",
        Got::NotWanted,
    );
}

// given A←B, B←F3 unknown, B←C4, FH=C4, trig A
//   | P0 parts item(F3) → Collecting ; chain refuses F3 ; trig C4 → F3 still parked
// The manager cannot know an unknown block's height, and the chain never processes a fork below
// the final head, so only the orphan LRU bounds these parts.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn gap_parts_for_an_unknown_fork_already_at_or_below_the_final_head_wait_for_eviction() {
    Scenario::new()
        .chain(&["A", "B"])
        .unknown("F3", "B", 3)
        .stored("C4", "B", 4)
        .final_head("C4")
        .trig("A")
        .parts(0, "F3", Got::Collecting)
        .chain_refuses("F3")
        .trig("C4")
        .check(|s| assert!(s.is_parked("F3")));
}

// given A←B, B←F3, B←C4, track F3, deliver+save F3, trig F3 (done) | FH=C4 ; trig C4 → F3 forgotten
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn gap_a_done_fork_item_is_forgotten_when_the_final_head_passes_it() {
    Scenario::new()
        .chain(&["A", "B"])
        .stored("F3", "B", 3)
        .stored("C4", "B", 4)
        .track("F3")
        .deliver_and_save("F3")
        .trig("F3")
        .check(|s| assert!(s.manager.manager.done_items.contains(&s.item("F3"))))
        .final_head("C4")
        .trig("C4")
        .check(|s| s.manager.assert_forgotten(&s.item("F3")));
}

// given A←B←C, A←F2 | P0 parts item(F2) → Collecting ; FEH=B ; trig C → F2 not parked
// F2 is never triggered: is its parked message dropped once execution is past its height?
// Today it stays until the orphan LRU evicts it, although its stored header gives its height.
#[test]
#[ignore = "known gap: parked parts for a stored block below the final execution head wait for eviction"]
fn gap_parts_parked_for_a_block_never_triggered_are_dropped_once_execution_passes_it() {
    Scenario::new()
        .chain(&["A", "B", "C"])
        .stored("F2", "A", 2)
        .parts(0, "F2", Got::Collecting)
        .final_execution_head("B")
        .trig("C")
        .check(|s| assert!(!s.is_parked("F2"), "parts for F2 are still parked"));
}
