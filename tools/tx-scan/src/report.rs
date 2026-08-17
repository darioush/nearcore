use crate::source::ContractStatus;
use crate::stats::{ScanState, in_flight_bucket_labels};
use near_primitives::types::BlockHeight;
use serde::Serialize;
use std::collections::BTreeMap;
use std::fmt::Write;

#[derive(Default, Serialize)]
pub struct ContractBreakdown {
    pub no_contract: u64,
    pub local_contract: u64,
    pub global_contract: u64,
    pub account_not_found: u64,
    /// The lookup ran but the data was not there.
    pub unknown: u64,
    /// No lookup ran for this account.
    pub not_looked_up: u64,
}

#[derive(Serialize)]
pub struct DistanceReport {
    pub distance_chunks: usize,
    pub transactions_with_earlier_match_at_distance: u64,
    pub transactions_with_earlier_match_within: u64,
    pub share_with_earlier_match_within: f64,
    pub in_flight_histogram: Vec<u64>,
    pub accounts_with_earlier_match_within: u64,
    pub accounts_by_contract_status: ContractBreakdown,
}

#[derive(Serialize)]
pub struct Report {
    pub chain_id: String,
    pub start_height: BlockHeight,
    pub end_height: BlockHeight,
    pub blocks_scanned: u64,
    pub heights_without_block: u64,
    pub chunks_scanned: u64,
    pub chunks_measured: u64,
    pub chunks_unmeasured: u64,
    pub transactions_total: u64,
    pub transactions_gas_key_signed: u64,
    pub transactions_measured: u64,
    pub action_counts: Vec<(String, u64)>,
    pub delegate_inner_action_counts: Vec<(String, u64)>,
    pub delegate_v2_count: u64,
    /// Range each in-flight histogram bucket covers.
    pub in_flight_bucket_labels: Vec<String>,
    pub distances: Vec<DistanceReport>,
}

pub fn build(state: &ScanState) -> Report {
    let distances = state
        .distances
        .iter()
        .enumerate()
        .map(|(distance, stats)| {
            let mut breakdown = ContractBreakdown::default();
            let mut accounts_with_earlier_match_within = 0;
            for (account_id, concurrency) in &state.concurrent_accounts {
                if concurrency.max_in_flight[distance] < 2 {
                    continue;
                }
                accounts_with_earlier_match_within += 1;
                match state.contract_status.get(account_id) {
                    Some(ContractStatus::NoContract) => breakdown.no_contract += 1,
                    Some(ContractStatus::LocalContract) => breakdown.local_contract += 1,
                    Some(ContractStatus::GlobalContract) => breakdown.global_contract += 1,
                    Some(ContractStatus::AccountNotFound) => breakdown.account_not_found += 1,
                    Some(ContractStatus::Unknown) => breakdown.unknown += 1,
                    None => breakdown.not_looked_up += 1,
                }
            }
            let share_with_earlier_match_within = match state.transactions_measured {
                0 => 0.0,
                total => stats.transactions_with_earlier_match_within() as f64 / total as f64,
            };
            DistanceReport {
                distance_chunks: stats.distance_chunks,
                transactions_with_earlier_match_at_distance: stats
                    .transactions_with_earlier_match_at_distance,
                transactions_with_earlier_match_within: stats
                    .transactions_with_earlier_match_within(),
                share_with_earlier_match_within,
                in_flight_histogram: stats.in_flight_histogram.clone(),
                accounts_with_earlier_match_within,
                accounts_by_contract_status: breakdown,
            }
        })
        .collect();

    Report {
        chain_id: state.chain_id.clone(),
        start_height: state.start_height,
        end_height: state.end_height,
        blocks_scanned: state.blocks_scanned,
        heights_without_block: state.heights_without_block,
        chunks_scanned: state.chunks_scanned,
        chunks_measured: state.measured_chunks(),
        chunks_unmeasured: state.unmeasured_chunks(),
        transactions_total: state.transactions_total,
        transactions_gas_key_signed: state.transactions_gas_key_signed,
        transactions_measured: state.transactions_measured,
        action_counts: sorted_desc(&state.action_counts),
        delegate_inner_action_counts: sorted_desc(&state.delegate_inner_action_counts),
        delegate_v2_count: state.action_counts.get("DelegateV2").copied().unwrap_or(0),
        in_flight_bucket_labels: in_flight_bucket_labels(),
        distances,
    }
}

fn sorted_desc(counts: &BTreeMap<String, u64>) -> Vec<(String, u64)> {
    let mut entries: Vec<(String, u64)> =
        counts.iter().map(|(name, count)| (name.clone(), *count)).collect();
    entries.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    entries
}

pub fn render_text(state: &ScanState, report: &Report) -> String {
    let mut text = String::new();
    let _ = writeln!(
        text,
        "\nchain {} heights {}..={} ({} blocks scanned, {} heights had no block)",
        report.chain_id,
        report.start_height,
        report.end_height,
        report.blocks_scanned,
        report.heights_without_block
    );
    let _ = writeln!(
        text,
        "{} chunks, {} transactions, {} signed by gas keys",
        report.chunks_scanned, report.transactions_total, report.transactions_gas_key_signed
    );
    let _ = writeln!(
        text,
        "{} transactions in {} chunks were measured; {} chunks at the start of the range look \
         back past it and were left out",
        report.transactions_measured, report.chunks_measured, report.chunks_unmeasured
    );

    let _ = writeln!(text, "\n== actions by kind ==");
    for (name, count) in &report.action_counts {
        let _ = writeln!(text, "  {count:>12}  {name}");
    }
    if !report.delegate_inner_action_counts.is_empty() {
        let _ = writeln!(text, "\n== actions nested inside delegate actions ==");
        for (name, count) in &report.delegate_inner_action_counts {
            let _ = writeln!(text, "  {count:>12}  {name}");
        }
    }
    let _ = writeln!(text, "\nDelegateV2 actions: {}", report.delegate_v2_count);
    for sighting in state.delegate_v2_sightings.iter().take(20) {
        let _ = writeln!(
            text,
            "  {} at height {} shard {} signer {}",
            sighting.transaction_hash, sighting.height, sighting.shard_id, sighting.signer_id
        );
    }
    if state.delegate_v2_sightings_dropped > 0 {
        let _ = writeln!(
            text,
            "  ({} more sightings were not recorded)",
            state.delegate_v2_sightings_dropped
        );
    }

    let _ = writeln!(text, "\n== earlier transactions of the same signer, by chunk distance ==");
    let _ = writeln!(
        text,
        "distance counts produced chunks of the same shard; gas-key transactions are excluded\n\
         exact:      transactions with another of the same signer exactly this many chunks \
         earlier\n\
         cumulative: transactions with at least one more of the same signer in the previous this \
         many chunks, counted once\n\
         in flight:  how many of that signer's transactions sit in this chunk and the previous \
         this many, which is what a queue already holds when this one arrives"
    );
    let _ = writeln!(
        text,
        "\n{:>8}  {:>14}  {:>17}  {:>9}  {:>8}  {}",
        "distance",
        "exact txs",
        "cumulative txs",
        "cum share",
        "accounts",
        format!("in flight histogram ({})", in_flight_bucket_labels().join(","))
    );
    for distance in &report.distances {
        let label = if distance.distance_chunks == 0 {
            "same".to_string()
        } else {
            format!("+{}", distance.distance_chunks)
        };
        let histogram = distance
            .in_flight_histogram
            .iter()
            .map(|count| count.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let _ = writeln!(
            text,
            "{:>8}  {:>14}  {:>17}  {:>8.3}%  {:>8}  {histogram}",
            label,
            distance.transactions_with_earlier_match_at_distance,
            distance.transactions_with_earlier_match_within,
            distance.share_with_earlier_match_within * 100.0,
            distance.accounts_with_earlier_match_within
        );
    }

    let _ = writeln!(
        text,
        "\n== accounts with two or more transactions in flight, by contract state =="
    );
    let _ = writeln!(
        text,
        "contract state is read at the current head, so 'no contract' proves the account had \
         none during the scan, while 'has contract' may be a later deployment"
    );
    let _ = writeln!(
        text,
        "\n{:>8}  {:>8}  {:>11}  {:>14}  {:>15}  {:>17}  {:>7}  {:>13}",
        "distance",
        "accounts",
        "no contract",
        "local contract",
        "global contract",
        "account not found",
        "unknown",
        "not looked up"
    );
    for distance in &report.distances {
        let label = if distance.distance_chunks == 0 {
            "same".to_string()
        } else {
            format!("+{}", distance.distance_chunks)
        };
        let breakdown = &distance.accounts_by_contract_status;
        let _ = writeln!(
            text,
            "{:>8}  {:>8}  {:>11}  {:>14}  {:>15}  {:>17}  {:>7}  {:>13}",
            label,
            distance.accounts_with_earlier_match_within,
            breakdown.no_contract,
            breakdown.local_contract,
            breakdown.global_contract,
            breakdown.account_not_found,
            breakdown.unknown,
            breakdown.not_looked_up
        );
    }
    text
}
