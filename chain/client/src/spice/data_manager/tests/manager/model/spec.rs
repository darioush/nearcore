//! Abstract model of the data manager's receipt-proof parking, mirroring
//! `receipt_parking.qnt` action for action. Plain Rust, no nearcore types, so it can also be
//! built standalone (in release) for the exhaustive check.
//!
//! World: blocks A (h1), B (h2, canonical) and F (h2, fork of B). One item per block. Producers
//! p0..p2 each send their own part; K = 3 distinct parts decode.

use stateright::{Model, Property};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Blk {
    A,
    B,
    F,
}

pub const BLOCKS: [Blk; 3] = [Blk::A, Blk::B, Blk::F];

impl Blk {
    pub fn height(self) -> u64 {
        match self {
            Blk::A => 1,
            Blk::B | Blk::F => 2,
        }
    }

    fn parent(self) -> Option<Blk> {
        match self {
            Blk::A => None,
            Blk::B | Blk::F => Some(Blk::A),
        }
    }

    pub fn from_name(name: &str) -> Blk {
        match name {
            "A" => Blk::A,
            "B" => Blk::B,
            "F" => Blk::F,
            _ => panic!("unknown block {name}"),
        }
    }
}

pub fn canon(height: u64) -> Blk {
    match height {
        1 => Blk::A,
        2 => Blk::B,
        _ => panic!("no canonical block at {height}"),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Msg {
    /// Producer `i` sends its own part, signed with its key.
    Producer(usize),
    /// A validator that doesn't produce the item.
    NonProducer,
    /// Producer 0 signing with a key not valid for the block's epochs.
    BadSig,
}

pub const MSGS: [Msg; 5] =
    [Msg::Producer(0), Msg::Producer(1), Msg::Producer(2), Msg::NonProducer, Msg::BadSig];

impl Msg {
    pub fn from_name(name: &str) -> Msg {
        match name {
            "p0" => Msg::Producer(0),
            "p1" => Msg::Producer(1),
            "p2" => Msg::Producer(2),
            "nonproducer" => Msg::NonProducer,
            "badsig" => Msg::BadSig,
            _ => panic!("unknown message {name}"),
        }
    }
}

const K: usize = 3;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Op {
    ChainAdd(Blk),
    SetFinalHead(u64),
    SetFinalExecutionHead(u64),
    SaveProof(Blk),
    Trigger(Blk),
    Receive(Blk, Msg),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Outcome {
    Collecting,
    Decoded,
    AlreadySettled,
    NotWanted,
    NotAProducer,
    InvalidSignature,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct Item {
    pub parts: BTreeSet<usize>,
    pub decoded: bool,
}

/// What the last operation did, for the covers.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct Step {
    pub outcome: Option<(Outcome, &'static str)>,
    pub trigger_why: &'static str,
    pub decoded_at: Option<Blk>,
    pub pruned: BTreeSet<Blk>,
    pub was_trigger: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct State {
    // environment
    pub chain_has: BTreeSet<Blk>,
    pub env_final_head: u64,
    pub env_final_exec: u64,
    pub store_done: BTreeSet<Blk>,
    // manager
    pub final_head: u64,
    pub final_exec: u64,
    pub items: BTreeMap<Blk, Item>,
    pub delivered: BTreeSet<Blk>,
    pub done: BTreeSet<Blk>,
    pub orphans: BTreeMap<Blk, BTreeSet<usize>>,
    pub processed: BTreeSet<Blk>,
    // bookkeeping
    pub decodes: BTreeMap<Blk, u32>,
    pub last: Step,
}

impl State {
    fn canonical_at(&self, height: u64) -> bool {
        self.chain_has.contains(&canon(height))
    }

    fn untracked_outcome(&self, b: Blk, m: Msg) -> (Outcome, &'static str) {
        let known = self.chain_has.contains(&b);
        let h = b.height();
        if self.processed.contains(&b) {
            (Outcome::NotWanted, "processed")
        } else if known && h <= self.final_exec {
            (Outcome::NotWanted, "belowFE")
        } else if m == Msg::NonProducer {
            if known {
                (Outcome::NotAProducer, "untrackedKnown")
            } else {
                (Outcome::NotWanted, "guessMiss")
            }
        } else if m == Msg::BadSig {
            (Outcome::InvalidSignature, "untracked")
        } else if known && h <= self.final_head && canon(h) != b {
            (Outcome::NotWanted, "forkBelowFH")
        } else if known {
            (Outcome::Collecting, "parkKnown")
        } else {
            (Outcome::Collecting, "parkUnknown")
        }
    }

    fn tracked_outcome(&self, b: Blk, m: Msg) -> (Outcome, &'static str) {
        let item = &self.items[&b];
        match m {
            Msg::NonProducer => (Outcome::NotAProducer, "tracked"),
            Msg::BadSig => (Outcome::InvalidSignature, "tracked"),
            Msg::Producer(_) if self.done.contains(&b) => (Outcome::AlreadySettled, "done"),
            Msg::Producer(_) if item.decoded => (Outcome::AlreadySettled, "decoded"),
            Msg::Producer(i) if item.parts.contains(&i) => (Outcome::Collecting, "duplicate"),
            Msg::Producer(_) if item.parts.len() + 1 >= K => (Outcome::Decoded, "direct"),
            Msg::Producer(_) => (Outcome::Collecting, "direct"),
        }
    }

    pub fn enabled(&self, op: Op) -> bool {
        match op {
            Op::ChainAdd(b) => {
                !self.chain_has.contains(&b)
                    && b.parent().is_none_or(|p| self.chain_has.contains(&p))
                    && (b != Blk::F
                        || (self.chain_has.contains(&Blk::B) && self.env_final_head < 2))
            }
            Op::SetFinalHead(h) => h > self.env_final_head && self.canonical_at(h),
            Op::SetFinalExecutionHead(h) => h > self.env_final_exec && h <= self.env_final_head,
            Op::SaveProof(b) => self.chain_has.contains(&b) && !self.store_done.contains(&b),
            Op::Trigger(b) => self.chain_has.contains(&b),
            Op::Receive(..) => true,
        }
    }

    /// Applies an enabled `op`.
    pub fn apply(&self, op: Op) -> State {
        let mut s = self.clone();
        s.last = Step::default();
        match op {
            Op::ChainAdd(b) => {
                s.chain_has.insert(b);
            }
            Op::SetFinalHead(h) => s.env_final_head = h,
            Op::SetFinalExecutionHead(h) => s.env_final_exec = h,
            Op::SaveProof(b) => {
                s.store_done.insert(b);
            }
            Op::Receive(b, m) => {
                let tracked = s.items.contains_key(&b);
                let (outcome, why) =
                    if tracked { s.tracked_outcome(b, m) } else { s.untracked_outcome(b, m) };
                if let Msg::Producer(i) = m {
                    if outcome == Outcome::Collecting && !tracked {
                        s.orphans.entry(b).or_default().insert(i);
                    }
                    if tracked && matches!(outcome, Outcome::Collecting | Outcome::Decoded) {
                        let item = s.items.get_mut(&b).unwrap();
                        item.parts.insert(i);
                        item.decoded = outcome == Outcome::Decoded;
                    }
                }
                if outcome == Outcome::Decoded {
                    s.delivered.insert(b);
                    *s.decodes.entry(b).or_default() += 1;
                }
                s.last.outcome = Some((outcome, why));
            }
            Op::Trigger(b) => s.trigger(b),
        }
        s
    }

    fn trigger(&mut self, b: Blk) {
        self.last.was_trigger = true;
        // 1. final execution head
        let fe = self.final_exec.max(self.env_final_exec);
        let expired: BTreeSet<Blk> =
            self.items.keys().copied().filter(|x| x.height() <= fe).collect();
        self.items.retain(|x, _| !expired.contains(x));
        self.processed.retain(|x| x.height() > fe);
        // 2. final head
        let fh = self.final_head.max(self.env_final_head);
        let forks: BTreeSet<Blk> = self
            .items
            .keys()
            .copied()
            .filter(|x| {
                let h = x.height();
                h > self.final_head && h <= fh && !(self.canonical_at(h) && canon(h) == *x)
            })
            .collect();
        self.items.retain(|x, _| !forks.contains(x));
        let gone: BTreeSet<Blk> = expired.union(&forks).copied().collect();
        self.final_exec = fe;
        self.final_head = fh;
        // 3. track
        let h = b.height();
        let was_tracked = self.items.contains_key(&b);
        let skip =
            h <= fe || (h <= fh && canon(h) != b) || self.store_done.contains(&b) || was_tracked;
        if !skip {
            self.items.insert(b, Item::default());
        }
        // 4. release parked
        let parked = self.orphans.remove(&b).unwrap_or_default();
        self.done.retain(|x| !gone.contains(x));
        self.delivered.retain(|x| !gone.contains(x));
        let mut decoded_now = false;
        if !parked.is_empty() {
            if let Some(item) = self.items.get_mut(&b) {
                if !self.done.contains(&b) && !item.decoded {
                    item.parts.extend(parked.iter().copied());
                    decoded_now = item.parts.len() >= K;
                    item.decoded = decoded_now;
                }
            }
        }
        if decoded_now {
            self.delivered.insert(b);
            *self.decodes.entry(b).or_default() += 1;
        }
        self.processed.insert(b);
        // 5. done
        let newly_done: Vec<Blk> = self.delivered.intersection(&self.store_done).copied().collect();
        for x in newly_done {
            self.delivered.remove(&x);
            self.done.insert(x);
        }
        self.last.trigger_why = if !parked.is_empty() && !self.items.contains_key(&b) {
            "parkedDropped"
        } else if self.store_done.contains(&b) && !was_tracked && !self.items.contains_key(&b) {
            "skipDone"
        } else {
            ""
        };
        self.last.decoded_at = decoded_now.then_some(b);
        self.last.pruned = gone;
    }

    fn recv_why(&self, outcome: Outcome, why: &str) -> bool {
        self.last.outcome.is_some_and(|(o, w)| o == outcome && w == why)
    }
}

pub struct ParkingModel;

pub const COVERS: [&str; 19] = [
    "park unknown header",
    "park known header",
    "decode at trigger",
    "decode direct",
    "duplicate part",
    "not wanted: processed",
    "not wanted: below final execution",
    "not wanted: guess miss",
    "not wanted: fork below final head",
    "not a producer: tracked",
    "not a producer: untracked known",
    "bad signature: tracked",
    "bad signature: untracked",
    "already settled: done",
    "already settled: decoded",
    "parked dropped at trigger",
    "trigger skips done item",
    "fork item pruned",
    "stale orphan below final execution",
];

impl Model for ParkingModel {
    type State = State;
    type Action = Op;

    fn init_states(&self) -> Vec<State> {
        vec![State::default()]
    }

    fn actions(&self, s: &State, actions: &mut Vec<Op>) {
        for b in BLOCKS {
            for op in [Op::ChainAdd(b), Op::SaveProof(b), Op::Trigger(b)] {
                if s.enabled(op) {
                    actions.push(op);
                }
            }
            for m in MSGS {
                actions.push(Op::Receive(b, m));
            }
        }
        for h in [1, 2] {
            for op in [Op::SetFinalHead(h), Op::SetFinalExecutionHead(h)] {
                if s.enabled(op) {
                    actions.push(op);
                }
            }
        }
    }

    fn next_state(&self, s: &State, op: Op) -> Option<State> {
        Some(s.apply(op))
    }

    fn properties(&self) -> Vec<Property<Self>> {
        use Outcome::*;
        vec![
            Property::always(
                "delivered/done ⊆ tracked, disjoint",
                |_: &ParkingModel, s: &State| {
                    s.delivered.iter().chain(&s.done).all(|x| s.items.contains_key(x))
                        && s.delivered.is_disjoint(&s.done)
                },
            ),
            Property::always(
                "no item at or below final execution",
                |_: &ParkingModel, s: &State| s.items.keys().all(|x| x.height() > s.final_exec),
            ),
            Property::always("no parking for processed blocks", |_: &ParkingModel, s: &State| {
                s.processed.iter().all(|x| !s.orphans.contains_key(x))
            }),
            Property::always("done only if stored", |_: &ParkingModel, s: &State| {
                s.done.is_subset(&s.store_done)
            }),
            Property::always(
                "no fork item at or below final head",
                |_: &ParkingModel, s: &State| {
                    !(s.items.contains_key(&Blk::F) && Blk::F.height() <= s.final_head)
                },
            ),
            Property::always("decode at most once", |_: &ParkingModel, s: &State| {
                s.decodes.values().all(|n| *n <= 1)
            }),
            Property::sometimes(COVERS[0], |_: &ParkingModel, s: &State| {
                s.recv_why(Collecting, "parkUnknown")
            }),
            Property::sometimes(COVERS[1], |_: &ParkingModel, s: &State| {
                s.recv_why(Collecting, "parkKnown")
            }),
            Property::sometimes(COVERS[2], |_: &ParkingModel, s: &State| {
                s.last.decoded_at.is_some()
            }),
            Property::sometimes(COVERS[3], |_: &ParkingModel, s: &State| {
                s.recv_why(Decoded, "direct")
            }),
            Property::sometimes(COVERS[4], |_: &ParkingModel, s: &State| {
                s.recv_why(Collecting, "duplicate")
            }),
            Property::sometimes(COVERS[5], |_: &ParkingModel, s: &State| {
                s.recv_why(NotWanted, "processed")
            }),
            Property::sometimes(COVERS[6], |_: &ParkingModel, s: &State| {
                s.recv_why(NotWanted, "belowFE")
            }),
            Property::sometimes(COVERS[7], |_: &ParkingModel, s: &State| {
                s.recv_why(NotWanted, "guessMiss")
            }),
            Property::sometimes(COVERS[8], |_: &ParkingModel, s: &State| {
                s.recv_why(NotWanted, "forkBelowFH")
            }),
            Property::sometimes(COVERS[9], |_: &ParkingModel, s: &State| {
                s.recv_why(NotAProducer, "tracked")
            }),
            Property::sometimes(COVERS[10], |_: &ParkingModel, s: &State| {
                s.recv_why(NotAProducer, "untrackedKnown")
            }),
            Property::sometimes(COVERS[11], |_: &ParkingModel, s: &State| {
                s.recv_why(InvalidSignature, "tracked")
            }),
            Property::sometimes(COVERS[12], |_: &ParkingModel, s: &State| {
                s.recv_why(InvalidSignature, "untracked")
            }),
            Property::sometimes(COVERS[13], |_: &ParkingModel, s: &State| {
                s.recv_why(AlreadySettled, "done")
            }),
            Property::sometimes(COVERS[14], |_: &ParkingModel, s: &State| {
                s.recv_why(AlreadySettled, "decoded")
            }),
            Property::sometimes(COVERS[15], |_: &ParkingModel, s: &State| {
                s.last.trigger_why == "parkedDropped"
            }),
            Property::sometimes(COVERS[16], |_: &ParkingModel, s: &State| {
                s.last.trigger_why == "skipDone"
            }),
            Property::sometimes(COVERS[17], |_: &ParkingModel, s: &State| {
                s.last.pruned.contains(&Blk::F) && Blk::F.height() > s.final_exec
            }),
            Property::sometimes(COVERS[18], |_: &ParkingModel, s: &State| {
                s.orphans.keys().any(|x| x.height() <= s.final_exec)
            }),
        ]
    }
}
