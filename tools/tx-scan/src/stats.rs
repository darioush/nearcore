use crate::source::{ContractStatus, ScannedBlock, ScannedTransaction};
use near_primitives::types::{AccountId, BlockHeight, ShardId};
use near_primitives::views::ActionView;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, VecDeque};

/// Largest chunk distance measured, in produced chunks of the same shard.
pub const MAX_DISTANCE_CHUNKS: usize = 10;

/// Distances run from 0, the chunk itself, up to `MAX_DISTANCE_CHUNKS`.
pub const DISTANCE_COUNT: usize = MAX_DISTANCE_CHUNKS + 1;

/// A chunk is measured only once every chunk it looks back at is buffered.
const BUFFER_CHUNKS: usize = DISTANCE_COUNT;

/// In-flight counts at or above this land in the last histogram bucket.
const IN_FLIGHT_BUCKETS: usize = 8;

/// Transaction hashes kept per `DelegateV2` sighting, so a non-zero count can
/// be inspected on chain. Bounded so a busy chain cannot grow the checkpoint.
const MAX_RECORDED_SIGHTINGS: usize = 1000;

pub fn action_view_kind(action: &ActionView) -> &'static str {
    match action {
        ActionView::CreateAccount => "CreateAccount",
        ActionView::DeployContract { .. } => "DeployContract",
        ActionView::FunctionCall { .. } => "FunctionCall",
        ActionView::Transfer { .. } => "Transfer",
        ActionView::Stake { .. } => "Stake",
        ActionView::AddKey { .. } => "AddKey",
        ActionView::DeleteKey { .. } => "DeleteKey",
        ActionView::DeleteAccount { .. } => "DeleteAccount",
        ActionView::Delegate { .. } => "Delegate",
        ActionView::DelegateV2 { .. } => "DelegateV2",
        ActionView::DeployGlobalContract { .. } => "DeployGlobalContract",
        ActionView::DeployGlobalContractByAccountId { .. } => "DeployGlobalContractByAccountId",
        ActionView::UseGlobalContract { .. } => "UseGlobalContract",
        ActionView::UseGlobalContractByAccountId { .. } => "UseGlobalContractByAccountId",
        ActionView::DeterministicStateInit { .. } => "DeterministicStateInit",
        ActionView::TransferToGasKey { .. } => "TransferToGasKey",
        ActionView::WithdrawFromGasKey { .. } => "WithdrawFromGasKey",
    }
}

#[derive(Serialize, Deserialize)]
pub struct DistanceStats {
    pub distance_chunks: usize,
    /// Transactions that have another transaction of the same signer exactly
    /// this many produced chunks earlier on the same shard.
    pub transactions_with_earlier_match_at_distance: u64,
    /// Bucket `i` counts transactions for which the signer had `i + 1`
    /// transactions in this chunk and the previous `distance_chunks`; the last
    /// bucket also absorbs everything larger. Every bucket above the first
    /// holds a transaction that shares its window with another one.
    pub in_flight_histogram: Vec<u64>,
}

impl DistanceStats {
    /// Transactions that have at least one more transaction of the same signer
    /// within `distance_chunks` before them, each counted once.
    pub fn transactions_with_earlier_match_within(&self) -> u64 {
        self.in_flight_histogram.iter().skip(1).sum()
    }

    fn new(distance_chunks: usize) -> Self {
        Self {
            distance_chunks,
            transactions_with_earlier_match_at_distance: 0,
            in_flight_histogram: vec![0; IN_FLIGHT_BUCKETS],
        }
    }
}

#[derive(Default, Serialize, Deserialize)]
pub struct ShardWindow {
    pub produced_chunks: u64,
    pub measured_chunks: u64,
    /// Signers of the non-gas-key transactions of each chunk still inside the
    /// buffer, oldest first.
    buffered: VecDeque<Vec<AccountId>>,
}

#[derive(Serialize, Deserialize)]
pub struct ShardWindowEntry {
    pub shard_id: ShardId,
    pub window: ShardWindow,
}

#[derive(Serialize, Deserialize)]
pub struct DelegateV2Sighting {
    pub transaction_hash: String,
    pub height: BlockHeight,
    pub shard_id: ShardId,
    pub signer_id: AccountId,
}

#[derive(Serialize, Deserialize)]
pub struct AccountConcurrency {
    /// Entry `d` is the most transactions this account had in one chunk plus
    /// the `d` produced chunks before it.
    pub max_in_flight: Vec<u32>,
    /// Entry `d` holds how often each in-flight count occurred at that
    /// distance: bucket `i` counts this account's transactions that had `i + 1`
    /// in flight. The peak alone cannot say whether the account sits near it or
    /// touched it once.
    pub in_flight_histogram: Vec<Vec<u64>>,
    /// This account's measured transactions, counted from the first one that
    /// had company. Earlier solo transactions are not counted, which only
    /// distorts accounts that ran alone for a long time before their first
    /// burst.
    pub transactions_counted: u64,
    /// First and last block height at which this account had two or more
    /// transactions in flight. A contract deploy cannot be undone, so checking
    /// both ends settles the whole span: if neither has a contract, none of it
    /// did.
    pub first_height: BlockHeight,
    pub last_height: BlockHeight,
    /// Shard the account sent from. Recorded so the state lookup can check its
    /// own shard mapping against what the chain actually did.
    pub shard_id: ShardId,
}

#[derive(Serialize, Deserialize)]
pub struct ScanState {
    pub chain_id: String,
    pub start_height: BlockHeight,
    pub end_height: BlockHeight,
    pub next_height: BlockHeight,

    pub blocks_scanned: u64,
    pub heights_without_block: u64,
    pub chunks_scanned: u64,
    pub transactions_total: u64,
    pub transactions_gas_key_signed: u64,
    /// Non-gas-key transactions that sat in a chunk with a complete
    /// neighbourhood, so the distance table could measure them.
    pub transactions_measured: u64,

    pub action_counts: BTreeMap<String, u64>,
    pub delegate_inner_action_counts: BTreeMap<String, u64>,
    pub delegate_v2_sightings: Vec<DelegateV2Sighting>,
    pub delegate_v2_sightings_dropped: u64,

    pub distances: Vec<DistanceStats>,
    pub shard_windows: Vec<ShardWindowEntry>,
    pub concurrent_accounts: BTreeMap<AccountId, AccountConcurrency>,
    /// Contract state at the chain head, filled by the scan.
    pub contract_status: BTreeMap<AccountId, ContractStatus>,
    /// Contract state at the heights where the account was concurrent, filled
    /// by the separate historical pass.
    #[serde(default)]
    pub historical_contract_status: BTreeMap<AccountId, HistoricalContractStatus>,
}

/// Contract state at both ends of an account's concurrency span. A deploy
/// cannot be undone, so equal ends settle the whole span.
#[derive(Clone, Copy, Serialize, Deserialize)]
pub struct HistoricalContractStatus {
    pub at_first_height: ContractStatus,
    pub at_last_height: ContractStatus,
}

impl ScanState {
    pub fn new(chain_id: String, start_height: BlockHeight, end_height: BlockHeight) -> Self {
        Self {
            chain_id,
            start_height,
            end_height,
            next_height: start_height,
            blocks_scanned: 0,
            heights_without_block: 0,
            chunks_scanned: 0,
            transactions_total: 0,
            transactions_gas_key_signed: 0,
            transactions_measured: 0,
            action_counts: BTreeMap::new(),
            delegate_inner_action_counts: BTreeMap::new(),
            delegate_v2_sightings: Vec::new(),
            delegate_v2_sightings_dropped: 0,
            distances: (0..DISTANCE_COUNT).map(DistanceStats::new).collect(),
            shard_windows: Vec::new(),
            concurrent_accounts: BTreeMap::new(),
            contract_status: BTreeMap::new(),
            historical_contract_status: BTreeMap::new(),
        }
    }

    pub fn record_missing_height(&mut self, height: BlockHeight) {
        self.heights_without_block += 1;
        self.next_height = height + 1;
    }

    pub fn record_block(&mut self, block: &ScannedBlock) {
        self.blocks_scanned += 1;
        for chunk in &block.chunks {
            self.chunks_scanned += 1;
            let mut signers = Vec::new();
            for transaction in &chunk.transactions {
                self.transactions_total += 1;
                self.record_actions(transaction, block.height, chunk.shard_id);
                if transaction.is_gas_key_signed() {
                    self.transactions_gas_key_signed += 1;
                } else {
                    signers.push(transaction.signer_id.clone());
                }
            }
            self.push_chunk(chunk.shard_id, block.height, signers);
        }
        self.next_height = block.height + 1;
    }

    fn record_actions(
        &mut self,
        transaction: &ScannedTransaction,
        height: BlockHeight,
        shard_id: ShardId,
    ) {
        for action in &transaction.actions {
            *self.action_counts.entry(action_view_kind(action).to_string()).or_default() += 1;
            let inner_actions = match action {
                ActionView::Delegate { delegate_action, .. } => delegate_action.get_actions(),
                ActionView::DelegateV2 { delegate_action, .. } => {
                    self.record_delegate_v2_sighting(transaction, height, shard_id);
                    delegate_action.get_actions()
                }
                _ => continue,
            };
            for inner in &inner_actions {
                let name: &str = inner.as_ref();
                *self.delegate_inner_action_counts.entry(name.to_string()).or_default() += 1;
            }
        }
    }

    fn record_delegate_v2_sighting(
        &mut self,
        transaction: &ScannedTransaction,
        height: BlockHeight,
        shard_id: ShardId,
    ) {
        if self.delegate_v2_sightings.len() >= MAX_RECORDED_SIGHTINGS {
            self.delegate_v2_sightings_dropped += 1;
            return;
        }
        self.delegate_v2_sightings.push(DelegateV2Sighting {
            transaction_hash: transaction.hash.to_string(),
            height,
            shard_id,
            signer_id: transaction.signer_id.clone(),
        });
    }

    fn shard_window_index(&mut self, shard_id: ShardId) -> usize {
        if let Some(index) = self.shard_windows.iter().position(|e| e.shard_id == shard_id) {
            return index;
        }
        self.shard_windows.push(ShardWindowEntry { shard_id, window: ShardWindow::default() });
        self.shard_windows.len() - 1
    }

    fn push_chunk(&mut self, shard_id: ShardId, height: BlockHeight, signers: Vec<AccountId>) {
        let index = self.shard_window_index(shard_id);
        let window = &mut self.shard_windows[index].window;
        window.produced_chunks += 1;
        window.buffered.push_back(signers);
        if window.buffered.len() > BUFFER_CHUNKS {
            window.buffered.pop_front();
        }
        if window.buffered.len() < BUFFER_CHUNKS {
            return;
        }
        let measured = measure_newest_chunk(&window.buffered);
        window.measured_chunks += 1;
        self.apply_measured(measured, shard_id, height);
    }

    fn apply_measured(
        &mut self,
        measured: Vec<MeasuredTransaction>,
        shard_id: ShardId,
        height: BlockHeight,
    ) {
        self.transactions_measured += measured.len() as u64;
        for transaction in measured {
            for (distance, stats) in self.distances.iter_mut().enumerate() {
                if transaction.has_earlier_match_at_distance[distance] {
                    stats.transactions_with_earlier_match_at_distance += 1;
                }
                let in_flight = transaction.in_flight_within[distance];
                let bucket = (in_flight as usize).min(IN_FLIGHT_BUCKETS) - 1;
                stats.in_flight_histogram[bucket] += 1;
            }
            let had_company = transaction.in_flight_within.iter().any(|&count| count >= 2);
            // Once an account is tracked, its later solo transactions count
            // too, so the histogram shows how often it runs alone.
            if !had_company && !self.concurrent_accounts.contains_key(&transaction.signer_id) {
                continue;
            }
            let record =
                self.concurrent_accounts.entry(transaction.signer_id).or_insert_with(|| {
                    AccountConcurrency {
                        max_in_flight: vec![0; DISTANCE_COUNT],
                        in_flight_histogram: vec![vec![0; IN_FLIGHT_BUCKETS]; DISTANCE_COUNT],
                        transactions_counted: 0,
                        first_height: height,
                        last_height: height,
                        shard_id,
                    }
                });
            record.transactions_counted += 1;
            if had_company {
                record.last_height = height;
                record.shard_id = shard_id;
            }
            for (distance, &count) in transaction.in_flight_within.iter().enumerate() {
                record.max_in_flight[distance] = record.max_in_flight[distance].max(count);
                let bucket = (count as usize).min(IN_FLIGHT_BUCKETS) - 1;
                record.in_flight_histogram[distance][bucket] += 1;
            }
        }
    }

    /// Produced chunks that look back past the start of the range, so the
    /// distance table leaves them out. There are at most
    /// `MAX_DISTANCE_CHUNKS` of them per shard.
    pub fn unmeasured_chunks(&self) -> u64 {
        self.shard_windows
            .iter()
            .map(|entry| entry.window.produced_chunks - entry.window.measured_chunks)
            .sum()
    }

    pub fn measured_chunks(&self) -> u64 {
        self.shard_windows.iter().map(|entry| entry.window.measured_chunks).sum()
    }
}

struct MeasuredTransaction {
    signer_id: AccountId,
    /// Entry `d` is true when the same signer sent another transaction exactly
    /// `d` produced chunks earlier.
    has_earlier_match_at_distance: Vec<bool>,
    /// Entry `d` counts the signer's transactions in this chunk and the `d`
    /// produced chunks before it, including this one. It is how many of that
    /// account's transactions a queue would already hold when this one arrives,
    /// if execution lagged `d` chunks.
    in_flight_within: Vec<u32>,
}

/// Measures every transaction of the newest buffered chunk, which is the only
/// one whose whole backward reach is buffered.
fn measure_newest_chunk(buffered: &VecDeque<Vec<AccountId>>) -> Vec<MeasuredTransaction> {
    let anchor = BUFFER_CHUNKS - 1;
    let mut per_slot: HashMap<&AccountId, [u32; BUFFER_CHUNKS]> = HashMap::new();
    for (slot, signers) in buffered.iter().enumerate().take(BUFFER_CHUNKS) {
        for signer_id in signers {
            per_slot.entry(signer_id).or_insert([0; BUFFER_CHUNKS])[slot] += 1;
        }
    }
    buffered[anchor]
        .iter()
        .map(|signer_id| {
            let slots = &per_slot[signer_id];
            let mut has_earlier_match_at_distance = Vec::with_capacity(DISTANCE_COUNT);
            let mut in_flight_within = Vec::with_capacity(DISTANCE_COUNT);
            let mut in_flight = 0;
            for distance in 0..DISTANCE_COUNT {
                let at_distance = slots[anchor - distance];
                in_flight += at_distance;
                // At distance zero the transaction itself sits in the slot, so
                // a match needs a second one there.
                let minimum = if distance == 0 { 1 } else { 0 };
                has_earlier_match_at_distance.push(at_distance > minimum);
                in_flight_within.push(in_flight);
            }
            MeasuredTransaction {
                signer_id: signer_id.clone(),
                has_earlier_match_at_distance,
                in_flight_within,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::{ScannedChunk, ScannedTransaction};
    use near_primitives::hash::CryptoHash;
    use near_primitives::types::NonceIndex;

    fn transaction(signer: &str, nonce_index: Option<NonceIndex>) -> ScannedTransaction {
        ScannedTransaction {
            hash: CryptoHash::default(),
            signer_id: signer.parse().unwrap(),
            nonce_index,
            actions: vec![ActionView::CreateAccount],
        }
    }

    fn account(name: &str) -> AccountId {
        name.parse().unwrap()
    }

    /// Gives one shard enough empty chunks to fill the backward reach, then the
    /// chunks under test, so every one of them gets measured.
    fn scan_one_shard(chunks: Vec<Vec<ScannedTransaction>>) -> ScanState {
        let mut state = ScanState::new("test".to_string(), 0, 1000);
        let leading_empty = std::iter::repeat_with(Vec::new).take(MAX_DISTANCE_CHUNKS);
        for (index, transactions) in leading_empty.chain(chunks).enumerate() {
            let chunk = ScannedChunk { shard_id: 0.into(), transactions };
            state.record_block(&ScannedBlock { height: index as BlockHeight, chunks: vec![chunk] });
        }
        state
    }

    #[test]
    fn backward_distances_of_two_bursts() {
        let state = scan_one_shard(vec![
            vec![transaction("alice.near", None), transaction("alice.near", None)],
            vec![transaction("bob.near", None)],
            vec![transaction("bob.near", None)],
            vec![transaction("alice.near", None)],
        ]);
        assert_eq!(state.transactions_total, 5);
        assert_eq!(state.transactions_measured, 5);

        // Both alice transactions of the first chunk see each other at distance
        // zero. Her fourth-chunk transaction looks back three chunks to them.
        // The second bob transaction looks back one chunk to the first.
        let exact: Vec<u64> = state
            .distances
            .iter()
            .map(|stats| stats.transactions_with_earlier_match_at_distance)
            .collect();
        assert_eq!(exact, vec![2, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0]);

        let cumulative: Vec<u64> = state
            .distances
            .iter()
            .map(|stats| stats.transactions_with_earlier_match_within())
            .collect();
        assert_eq!(cumulative, vec![2, 3, 3, 4, 4, 4, 4, 4, 4, 4, 4]);

        // At distance three: one transaction alone, three in a pair, and
        // alice's last one with three of hers in flight.
        assert_eq!(state.distances[0].in_flight_histogram[..3], [3, 2, 0]);
        assert_eq!(state.distances[3].in_flight_histogram[..3], [1, 3, 1]);

        assert_eq!(state.concurrent_accounts[&account("alice.near")].max_in_flight[0], 2);
        assert_eq!(state.concurrent_accounts[&account("alice.near")].max_in_flight[3], 3);
        assert_eq!(state.concurrent_accounts[&account("bob.near")].max_in_flight[0], 1);
        assert_eq!(state.concurrent_accounts[&account("bob.near")].max_in_flight[1], 2);
    }

    #[test]
    fn an_accounts_solo_transactions_count_once_it_is_tracked() {
        // One burst of three in a chunk, then four solo chunks. The peak hides
        // that the account runs alone most of the time; the histogram does not.
        let mut chunks = vec![vec![
            transaction("alice.near", None),
            transaction("alice.near", None),
            transaction("alice.near", None),
        ]];
        for _ in 0..4 {
            chunks.push(vec![transaction("alice.near", None)]);
        }
        let state = scan_one_shard(chunks);
        let alice = &state.concurrent_accounts[&account("alice.near")];
        assert_eq!(alice.transactions_counted, 7);
        assert_eq!(alice.max_in_flight[0], 3);
        // At distance zero: three transactions saw three in flight, and the
        // four later ones were alone.
        assert_eq!(alice.in_flight_histogram[0][0], 4);
        assert_eq!(alice.in_flight_histogram[0][2], 3);
        assert_eq!(alice.in_flight_histogram[0].iter().sum::<u64>(), 7);
    }

    #[test]
    fn a_lone_transaction_never_matches() {
        let state = scan_one_shard(vec![vec![transaction("alice.near", None)]]);
        assert_eq!(state.transactions_measured, 1);
        for stats in &state.distances {
            assert_eq!(stats.transactions_with_earlier_match_at_distance, 0);
            assert_eq!(stats.transactions_with_earlier_match_within(), 0);
        }
        assert!(state.concurrent_accounts.is_empty());
    }

    #[test]
    fn a_match_just_past_the_largest_distance_is_not_counted() {
        let mut chunks = vec![vec![transaction("alice.near", None)]];
        chunks.extend(std::iter::repeat_with(Vec::new).take(MAX_DISTANCE_CHUNKS));
        chunks.push(vec![transaction("alice.near", None)]);
        let state = scan_one_shard(chunks);
        assert_eq!(state.transactions_measured, 2);
        assert!(state.concurrent_accounts.is_empty());
    }

    #[test]
    fn gas_key_transactions_are_counted_but_not_measured() {
        let state = scan_one_shard(vec![vec![
            transaction("alice.near", Some(0)),
            transaction("alice.near", Some(1)),
            transaction("bob.near", None),
        ]]);
        assert_eq!(state.transactions_total, 3);
        assert_eq!(state.transactions_gas_key_signed, 2);
        assert_eq!(state.transactions_measured, 1);
        assert_eq!(state.distances[0].transactions_with_earlier_match_at_distance, 0);
    }

    #[test]
    fn each_shard_keeps_its_own_chunk_sequence() {
        let mut state = ScanState::new("test".to_string(), 0, 1000);
        for height in 0..MAX_DISTANCE_CHUNKS as BlockHeight + 2 {
            let chunks = vec![
                ScannedChunk {
                    shard_id: 0.into(),
                    transactions: vec![transaction("alice.near", None)],
                },
                ScannedChunk {
                    shard_id: 1.into(),
                    transactions: vec![transaction("alice.near", None)],
                },
            ];
            state.record_block(&ScannedBlock { height, chunks });
        }
        // Alice sends once per chunk on each shard. Her shard 0 transactions
        // must never match her shard 1 ones, so nothing lands at distance zero.
        assert_eq!(state.distances[0].transactions_with_earlier_match_at_distance, 0);
        assert_eq!(state.distances[1].transactions_with_earlier_match_at_distance, 4);
    }

    #[test]
    fn chunks_before_the_full_reach_are_left_out() {
        let mut state = ScanState::new("test".to_string(), 0, 1000);
        for height in 0..5 {
            let chunk = ScannedChunk { shard_id: 0.into(), transactions: vec![] };
            state.record_block(&ScannedBlock { height, chunks: vec![chunk] });
        }
        assert_eq!(state.chunks_scanned, 5);
        assert_eq!(state.measured_chunks(), 0);
        assert_eq!(state.unmeasured_chunks(), 5);
    }
}
