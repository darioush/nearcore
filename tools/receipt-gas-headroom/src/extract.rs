use crate::frame::FrameWriter;
use crate::row::{
    AddedKeyPermission, Census, ChargedItem, ChildReceipt, ChunkRow, CrossChecks,
    ExecutedReceiptKind, Producer, ProducerRow, attached_gas_is_derived,
};
use anyhow::Context;
use indicatif::{ProgressBar, ProgressStyle};
use near_chain::{ChainStore, ChainStoreAccess};
use near_parameters::{ActionCosts, RuntimeConfigStore};
use near_primitives::account::AccessKeyPermission;
use near_primitives::action::Action;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{Receipt, VersionedReceiptEnum};
use near_primitives::types::ProtocolVersion;
use near_primitives::types::{AccountId, BlockHeight, Gas, ShardId};
use std::collections::{HashMap, HashSet};
use std::io::Write;

/// Charged the same way `null_terminated_method_names_len` does in
/// `near-vm-runner`, which is not public outside that crate.
fn method_names_bytes(method_names: &[String]) -> u64 {
    method_names.iter().map(|name| name.len() as u64 + 1).sum()
}

fn charged_item(action: &Action) -> ChargedItem {
    match action {
        Action::FunctionCall(call) => ChargedItem::FunctionCall {
            method_name: call.method_name.clone(),
            payload_bytes: call.method_name.len() as u64 + call.args.len() as u64,
            attached_gas: call.gas,
        },
        Action::DeployContract(deploy) => {
            ChargedItem::DeployContract { code_bytes: deploy.code.len() as u64 }
        }
        Action::AddKey(add_key) => {
            let permission = &add_key.access_key.permission;
            let kind = match permission {
                AccessKeyPermission::FullAccess => AddedKeyPermission::FullAccess,
                AccessKeyPermission::FunctionCall(_) => AddedKeyPermission::FunctionCall,
                AccessKeyPermission::GasKeyFullAccess(_) => AddedKeyPermission::GasKeyFullAccess,
                AccessKeyPermission::GasKeyFunctionCall(_, _) => {
                    AddedKeyPermission::GasKeyFunctionCall
                }
            };
            let function_call = permission.function_call_permission();
            let gas_key_balance = match permission {
                AccessKeyPermission::GasKeyFullAccess(info)
                | AccessKeyPermission::GasKeyFunctionCall(info, _) => {
                    Some(info.balance.as_yoctonear())
                }
                _ => None,
            };
            ChargedItem::AddKey {
                permission: kind,
                method_names_bytes: function_call
                    .map_or(0, |permission| method_names_bytes(&permission.method_names)),
                allowance: function_call.and_then(|permission| {
                    permission.allowance.map(|amount| amount.as_yoctonear())
                }),
                gas_key_balance,
            }
        }
        Action::CreateAccount(_) => ChargedItem::Other { kind: "CreateAccount".to_owned() },
        Action::Transfer(_) => ChargedItem::Other { kind: "Transfer".to_owned() },
        Action::Stake(_) => ChargedItem::Other { kind: "Stake".to_owned() },
        Action::DeleteKey(_) => ChargedItem::Other { kind: "DeleteKey".to_owned() },
        Action::DeleteAccount(_) => ChargedItem::Other { kind: "DeleteAccount".to_owned() },
        Action::Delegate(_) => ChargedItem::Other { kind: "Delegate".to_owned() },
        Action::DelegateV2(_) => ChargedItem::Other { kind: "DelegateV2".to_owned() },
        Action::DeployGlobalContract(deploy) => {
            ChargedItem::DeployGlobalContract { code_bytes: deploy.code.len() as u64 }
        }
        Action::UseGlobalContract(use_global) => ChargedItem::UseGlobalContract {
            identifier_bytes: use_global.contract_identifier.len() as u64,
        },
        Action::DeterministicStateInit(_) => {
            ChargedItem::Other { kind: "DeterministicStateInit".to_owned() }
        }
        Action::UniversalStateInit(_) => {
            ChargedItem::Other { kind: "UniversalStateInit".to_owned() }
        }
        Action::TransferToGasKey(_) => ChargedItem::Other { kind: "TransferToGasKey".to_owned() },
        Action::WithdrawFromGasKey(_) => {
            ChargedItem::Other { kind: "WithdrawFromGasKey".to_owned() }
        }
    }
}

fn record_item(item: &ChargedItem, census: &mut Census) {
    let kind = match item {
        ChargedItem::FunctionCall { payload_bytes, .. } => {
            census.function_call_payload_bytes.record(*payload_bytes);
            "FunctionCall"
        }
        ChargedItem::DeployContract { code_bytes } => {
            census.deploy_contract_code_bytes.record(*code_bytes);
            "DeployContract"
        }
        ChargedItem::DeployGlobalContract { code_bytes } => {
            census.deploy_global_contract_code_bytes.record(*code_bytes);
            "DeployGlobalContract"
        }
        ChargedItem::UseGlobalContract { .. } => "UseGlobalContract",
        ChargedItem::ReturnedData { payload_bytes } => {
            census.returned_data_payload_bytes.record(*payload_bytes);
            "ReturnedData"
        }
        ChargedItem::AddKey { permission, method_names_bytes, .. } => {
            census.added_key_method_names_bytes.record(*method_names_bytes);
            *census.added_keys_by_permission.entry(format!("{permission:?}")).or_default() += 1;
            "AddKey"
        }
        ChargedItem::Other { kind } => kind.as_str(),
    };
    *census.items_by_kind.entry(kind.to_owned()).or_default() += 1;
}

fn child_receipt(receipt: &Receipt) -> Option<ChildReceipt> {
    let (attached_gas, items) = match receipt.versioned_receipt() {
        VersionedReceiptEnum::Action(action_receipt)
        | VersionedReceiptEnum::PromiseYield(action_receipt) => {
            let attached_gas = action_receipt
                .actions()
                .iter()
                .filter_map(|action| match action {
                    Action::FunctionCall(call) => Some(call.gas),
                    _ => None,
                })
                .fold(Gas::ZERO, |total, gas| total.saturating_add(gas));
            (attached_gas, action_receipt.actions().iter().map(charged_item).collect())
        }
        // `new_data_receipt_byte` is charged on the returned value, once per
        // output data receiver, with `sir` meaning the value comes back to the
        // account that produced it. See `value_return` in near-vm-runner.
        VersionedReceiptEnum::Data(data_receipt) => {
            let payload_bytes = data_receipt.data.as_ref().map_or(0, |data| data.len() as u64);
            (Gas::ZERO, vec![ChargedItem::ReturnedData { payload_bytes }])
        }
        // `promise_yield_resume` pays `yield_resume_byte`, an ext cost with no
        // send or execution split, so none of the per-byte action fees apply.
        VersionedReceiptEnum::PromiseResume(_) => {
            (Gas::ZERO, vec![ChargedItem::Other { kind: "PromiseResume".to_owned() }])
        }
        VersionedReceiptEnum::GlobalContractDistribution(_) => return None,
    };
    Some(ChildReceipt {
        receipt_id: *receipt.receipt_id(),
        receiver_id: receipt.receiver_id().clone(),
        attached_gas,
        is_self_call: receipt.receiver_id() == receipt.predecessor_id(),
        attached_gas_is_derived: attached_gas_is_derived(attached_gas),
        items,
    })
}

/// Reads one chunk and writes one row per producer that ran in it.
///
/// `OutgoingReceipts` and `OutcomeIds` share the `(block_hash, shard_id)` key,
/// so the receipts a chunk produced and the outcomes that produced them are
/// both here. Nothing has to be carried across blocks.
fn extract_chunk(
    chain_store: &ChainStore,
    block_hash: &CryptoHash,
    block_height: BlockHeight,
    shard_id: ShardId,
    protocol_version: ProtocolVersion,
    transactions_in_chunk: u64,
    transaction_signers: &HashMap<CryptoHash, AccountId>,
    configs: &RuntimeConfigStore,
    gas_used_from_next_header: Option<Gas>,
    receipts_by_id: &HashMap<CryptoHash, Receipt>,
    claimed_in_range: &mut HashSet<CryptoHash>,
    checks: &mut CrossChecks,
    census: &mut Census,
    out: &mut FrameWriter<impl Write>,
    chunk_out: &mut FrameWriter<impl Write>,
) -> anyhow::Result<usize> {
    let produced = match chain_store.get_outgoing_receipts(block_hash, shard_id) {
        Ok(receipts) => receipts,
        // A shard with no chunk in this block produced nothing.
        Err(_) => return Ok(0),
    };
    let outcome_ids = chain_store.get_outcomes_by_block_hash_and_shard_id(block_hash, shard_id);

    let mut claimed: HashMap<CryptoHash, u32> = HashMap::new();
    let mut chunk_row = ChunkRow {
        block_height,
        shard_id,
        protocol_version,
        transactions: transactions_in_chunk,
        action_receipts_created: 0,
        data_receipts_created: 0,
        receipts_processed: 0,
        gas_burnt: Gas::ZERO,
        compute_usage: 0,
    };
    for receipt in produced.iter() {
        match receipt.versioned_receipt() {
            VersionedReceiptEnum::Action(_) | VersionedReceiptEnum::PromiseYield(_) => {
                chunk_row.action_receipts_created += 1
            }
            _ => chunk_row.data_receipts_created += 1,
        }
    }

    let mut rows_written = 0;
    for outcome_id in outcome_ids {
        let Some(outcome_with_proof) =
            chain_store.get_outcome_by_id_and_block_hash(&outcome_id, block_hash)
        else {
            checks.skipped_outcome_missing += 1;
            continue;
        };
        let outcome = outcome_with_proof.outcome;
        chunk_row.gas_burnt = chunk_row.gas_burnt.saturating_add(outcome.gas_burnt);
        chunk_row.compute_usage += outcome.compute_usage.unwrap_or_default();

        // Resolved against every receipt the range sent, not just this chunk's.
        // A chunk sends what it can, buffering the rest for a later one, so a
        // producer's children are not all in the chunk that ran it.
        for receipt_id in &outcome.receipt_ids {
            *claimed.entry(*receipt_id).or_default() += 1;
        }
        let children: Vec<ChildReceipt> = outcome
            .receipt_ids
            .iter()
            .filter_map(|receipt_id| receipts_by_id.get(receipt_id))
            .filter_map(child_receipt)
            .collect();
        census.children_per_producer.record(children.len() as u64);
        for child in &children {
            if child.is_self_call {
                census.self_call_children += 1;
            }
            if child.attached_gas_is_derived {
                census.derived_gas_children += 1;
            }
            census.attached_gas.record(child.attached_gas.as_gas());
            census.actions_per_receipt.record(child.items.len() as u64);
            for item in &child.items {
                record_item(item, census);
            }
        }

        // An outcome id is either a transaction hash or a receipt id. The
        // transactions of this chunk are the only transaction hashes that can
        // appear, so a miss means the producer was a receipt.
        let (producer, prepaid_gas, gas_burnt, gas_left) =
            match transaction_signers.get(&outcome_id) {
                Some(signer_id) => (
                    Producer::Transaction { tx_hash: outcome_id, signer_id: signer_id.clone() },
                    None,
                    None,
                    None,
                ),
                None => {
                    chunk_row.receipts_processed += 1;
                    let Some(receipt) = chain_store.get_receipt(&outcome_id) else {
                        checks.skipped_receipt_not_stored += 1;
                        continue;
                    };
                    let (kind, action_receipt) = match receipt.versioned_receipt() {
                        VersionedReceiptEnum::Action(inner) => (ExecutedReceiptKind::Action, inner),
                        VersionedReceiptEnum::PromiseYield(inner) => {
                            (ExecutedReceiptKind::PromiseYield, inner)
                        }
                        _ => {
                            checks.skipped_producer_not_an_action_receipt += 1;
                            continue;
                        }
                    };
                    // A receipt's budget is the gas attached to its function calls
                    // plus the execution fees bought for it, the same sum
                    // `refund_unspent_gas_and_deposits` refunds against. Leaving
                    // the fees out makes a refund receipt, whose actions attach no
                    // gas at all, look like it burned more than it had.
                    let config = configs.get_config(protocol_version);
                    let attached_gas =
                        node_runtime::config::total_prepaid_gas(action_receipt.actions())
                            .context("prepaid gas overflow")?;
                    let exec_fees = node_runtime::config::total_prepaid_exec_fees(
                        config,
                        action_receipt.actions(),
                        receipt.receiver_id(),
                    )
                    .context("prepaid exec fee overflow")?
                    .gas
                    .checked_add(config.fees.fee(ActionCosts::new_action_receipt).exec_fee().gas)
                    .context("prepaid exec fee overflow")?;
                    let prepaid_gas =
                        attached_gas.checked_add(exec_fees).context("prepaid gas overflow")?;
                    let constant_children_gas = children
                        .iter()
                        .filter(|child| !child.attached_gas_is_derived)
                        .fold(Gas::ZERO, |total, child| total.saturating_add(child.attached_gas));
                    let shortfall = i128::from(prepaid_gas.as_gas())
                        - i128::from(outcome.gas_burnt.as_gas())
                        - i128::from(constant_children_gas.as_gas());
                    if shortfall < 0 {
                        checks.receipts_with_negative_gas_left += 1;
                    }
                    let headroom: u64 = shortfall.max(0).try_into().unwrap_or(u64::MAX);
                    census.gas_left_after_constant_children.record(headroom);
                    if kind == ExecutedReceiptKind::PromiseYield {
                        census.yield_callbacks_run += 1;
                        census.yield_callback_gas_left.record(headroom);
                    }
                    let gas_left = prepaid_gas
                        .saturating_sub(outcome.gas_burnt)
                        .saturating_sub(constant_children_gas);
                    (
                        Producer::Receipt {
                            receipt_id: outcome_id,
                            receiver_id: receipt.receiver_id().clone(),
                            kind,
                        },
                        Some(prepaid_gas),
                        Some(outcome.gas_burnt),
                        Some(gas_left),
                    )
                }
            };

        let row = ProducerRow {
            block_height,
            shard_id,
            protocol_version,
            producer,
            prepaid_gas,
            gas_burnt,
            gas_left_after_constant_children: gas_left,
            children,
        };
        out.write(&row)?;
        rows_written += 1;
    }
    checks.chunks_checked += 1;
    checks.receipts_created += chunk_row.action_receipts_created + chunk_row.data_receipts_created;
    checks.receipts_processed += chunk_row.receipts_processed;
    checks.doubly_claimed_receipts += claimed.values().filter(|count| **count > 1).count() as u64;
    for (receipt_id, _) in claimed.iter() {
        claimed_in_range.insert(*receipt_id);
    }
    if gas_used_from_next_header.is_none() {
        checks.chunks_without_a_gas_figure += 1;
    }
    if let Some(recorded) = gas_used_from_next_header {
        let difference = i128::from(chunk_row.gas_burnt.as_gas()) - i128::from(recorded.as_gas());
        if difference != 0 {
            checks.chunks_with_gas_mismatch += 1;
            if difference.abs() > checks.worst_gas_mismatch.abs() {
                checks.worst_gas_mismatch = difference;
            }
        }
    }

    chunk_out.write(&chunk_row)?;
    Ok(rows_written)
}

pub fn extract_range(
    chain_store: &ChainStore,
    start_height: BlockHeight,
    end_height: BlockHeight,
    out: &mut FrameWriter<impl Write>,
    chunk_out: &mut FrameWriter<impl Write>,
) -> anyhow::Result<(usize, CrossChecks, Census)> {
    let configs = RuntimeConfigStore::new();
    let mut checks = CrossChecks::default();
    let mut census = Census::default();

    // A chunk sends what its outgoing limits allow and buffers the rest for a
    // later one, so the receipts a producer made are spread across the chunks
    // that sent them. Indexing every receipt the range sent first means a
    // producer's children resolve wherever they were sent from, which reading
    // one chunk at a time cannot do. Sequential, unlike a lookup per child.
    let blocks = end_height.saturating_sub(start_height) + 1;
    let progress = ProgressBar::new(blocks * 2);
    progress.set_style(
        ProgressStyle::with_template("{msg} [{bar:40}] {pos}/{len} blocks {percent}% eta {eta}")
            .unwrap()
            .progress_chars("=> "),
    );

    let mut receipts_by_id: HashMap<CryptoHash, Receipt> = HashMap::new();
    let mut sent_at: Vec<(BlockHeight, CryptoHash)> = Vec::new();
    progress.set_message("indexing sent receipts");
    for height in start_height..=end_height {
        progress.inc(1);
        let Ok(block_hash) = chain_store.get_block_hash_by_height(height) else { continue };
        let Ok(block) = chain_store.get_block(&block_hash) else { continue };
        for chunk_header in block.chunks().iter_raw() {
            let Ok(sent) = chain_store.get_outgoing_receipts(&block_hash, chunk_header.shard_id())
            else {
                continue;
            };
            for receipt in sent.iter() {
                receipts_by_id.insert(*receipt.receipt_id(), receipt.clone());
                sent_at.push((height, *receipt.receipt_id()));
            }
        }
    }
    tracing::info!(
        target: "receipt-gas-headroom",
        receipts = receipts_by_id.len(),
        "indexed every receipt the range sent"
    );

    let mut claimed_in_range: HashSet<CryptoHash> = HashSet::new();
    let mut total_rows = 0;
    progress.set_message("extracting producers ");
    for height in start_height..=end_height {
        progress.inc(1);
        let Ok(block_hash) = chain_store.get_block_hash_by_height(height) else { continue };
        let Ok(block) = chain_store.get_block(&block_hash) else { continue };
        let protocol_version = block.header().latest_protocol_version();

        // A chunk header records `prev_gas_used`, the gas the previous block's
        // chunk for that shard used, so the next block holds the runtime's own
        // figure for the chunk being read here.
        let mut gas_used_per_shard = HashMap::new();
        if let Ok(next_hash) = chain_store.get_block_hash_by_height(height + 1) {
            if let Ok(next_block) = chain_store.get_block(&next_hash) {
                for chunk_header in next_block.chunks().iter_raw() {
                    // A shard with no chunk in the next block carries its old
                    // header forward, so `prev_gas_used` would describe some
                    // earlier block rather than this one. Only a header the
                    // next block newly included says anything about this chunk.
                    if chunk_header.is_new_chunk(height + 1) {
                        gas_used_per_shard
                            .insert(chunk_header.shard_id(), chunk_header.prev_gas_used());
                    }
                }
            }
        }

        let mut transaction_signers = HashMap::new();
        let mut transactions_per_shard = HashMap::new();
        for chunk_header in block.chunks().iter_raw() {
            let Ok(chunk) = chain_store.get_chunk(&chunk_header.chunk_hash()) else { continue };
            let transactions = chunk.to_transactions();
            transactions_per_shard.insert(chunk_header.shard_id(), transactions.len() as u64);
            for transaction in transactions {
                transaction_signers
                    .insert(transaction.get_hash(), transaction.transaction.signer_id().clone());
            }
        }

        for chunk_header in block.chunks().iter_raw() {
            let shard_id = chunk_header.shard_id();
            total_rows += extract_chunk(
                chain_store,
                &block_hash,
                height,
                shard_id,
                protocol_version,
                transactions_per_shard.get(&shard_id).copied().unwrap_or_default(),
                &transaction_signers,
                &configs,
                gas_used_per_shard.get(&shard_id).copied(),
                &receipts_by_id,
                &mut claimed_in_range,
                &mut checks,
                &mut census,
                out,
                chunk_out,
            )?;
        }
    }
    progress.finish_with_message("extract finished    ");

    // A receipt still unclaimed after the whole range was walked belongs to a
    // producer that ran before the range started, so these should crowd its
    // first blocks rather than spread through it.
    for (height, receipt_id) in sent_at {
        if claimed_in_range.contains(&receipt_id) {
            continue;
        }
        checks.unclaimed_receipts += 1;
        let kind = match receipts_by_id.get(&receipt_id).map(|r| r.versioned_receipt()) {
            Some(VersionedReceiptEnum::Action(_)) => "Action",
            Some(VersionedReceiptEnum::PromiseYield(_)) => "PromiseYield",
            Some(VersionedReceiptEnum::Data(_)) => "Data",
            Some(VersionedReceiptEnum::PromiseResume(_)) => "PromiseResume",
            Some(VersionedReceiptEnum::GlobalContractDistribution(_)) => {
                "GlobalContractDistribution"
            }
            None => "Unknown",
        };
        *checks.unclaimed_by_kind.entry(kind.to_owned()).or_default() += 1;
        checks.unclaimed_offset_from_range_start.record(height.saturating_sub(start_height));
        if kind != "PromiseResume" && checks.unclaimed_receipt_samples.len() < 20 {
            checks.unclaimed_receipt_samples.push((height, receipt_id));
        }
    }

    Ok((total_rows, checks, census))
}
