use crate::row::{
    ChargedItem, ChildReceipt, ChunkRow, Producer, ProducerRow, attached_gas_is_derived,
};
use anyhow::Context;
use near_chain::{ChainStore, ChainStoreAccess};
use near_parameters::{ActionCosts, RuntimeConfigStore};
use near_primitives::action::Action;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{Receipt, VersionedReceiptEnum};
use near_primitives::types::ProtocolVersion;
use near_primitives::types::{AccountId, BlockHeight, Gas, ShardId};
use std::collections::HashMap;
use std::io::Write;

/// Charged the same way `null_terminated_method_names_len` does in
/// `near-vm-runner`, which is not public outside that crate.
fn method_names_bytes(method_names: &[String]) -> u64 {
    method_names.iter().map(|name| name.len() as u64 + 1).sum()
}

fn charged_item(action: &Action) -> ChargedItem {
    match action {
        Action::FunctionCall(call) => ChargedItem::FunctionCall {
            payload_bytes: call.method_name.len() as u64 + call.args.len() as u64,
            attached_gas: call.gas,
        },
        Action::DeployContract(deploy) => {
            ChargedItem::DeployContract { code_bytes: deploy.code.len() as u64 }
        }
        Action::AddKey(add_key) => match add_key.access_key.permission.function_call_permission() {
            Some(permission) => ChargedItem::AddFunctionCallKey {
                method_names_bytes: method_names_bytes(&permission.method_names),
            },
            None => ChargedItem::Other { kind: "AddFullAccessKey".to_owned() },
        },
        Action::CreateAccount(_) => ChargedItem::Other { kind: "CreateAccount".to_owned() },
        Action::Transfer(_) => ChargedItem::Other { kind: "Transfer".to_owned() },
        Action::Stake(_) => ChargedItem::Other { kind: "Stake".to_owned() },
        Action::DeleteKey(_) => ChargedItem::Other { kind: "DeleteKey".to_owned() },
        Action::DeleteAccount(_) => ChargedItem::Other { kind: "DeleteAccount".to_owned() },
        Action::Delegate(_) => ChargedItem::Other { kind: "Delegate".to_owned() },
        Action::DelegateV2(_) => ChargedItem::Other { kind: "DelegateV2".to_owned() },
        Action::DeployGlobalContract(_) => {
            ChargedItem::Other { kind: "DeployGlobalContract".to_owned() }
        }
        Action::UseGlobalContract(_) => ChargedItem::Other { kind: "UseGlobalContract".to_owned() },
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
    out: &mut impl Write,
    chunk_out: &mut impl Write,
) -> anyhow::Result<usize> {
    let produced = match chain_store.get_outgoing_receipts(block_hash, shard_id) {
        Ok(receipts) => receipts,
        // A shard with no chunk in this block produced nothing.
        Err(_) => return Ok(0),
    };
    let outcome_ids = chain_store.get_outcomes_by_block_hash_and_shard_id(block_hash, shard_id);

    let produced_by_id: HashMap<CryptoHash, &Receipt> =
        produced.iter().map(|receipt| (*receipt.receipt_id(), receipt)).collect();

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
            continue;
        };
        let outcome = outcome_with_proof.outcome;
        chunk_row.gas_burnt = chunk_row.gas_burnt.saturating_add(outcome.gas_burnt);
        chunk_row.compute_usage += outcome.compute_usage.unwrap_or_default();

        let children: Vec<ChildReceipt> = outcome
            .receipt_ids
            .iter()
            .filter_map(|receipt_id| produced_by_id.get(receipt_id))
            .filter_map(|receipt| child_receipt(receipt))
            .collect();

        // An outcome id is either a transaction hash or a receipt id. The
        // transactions of this chunk are the only transaction hashes that can
        // appear, so a miss means the producer was a receipt.
        let (producer, prepaid_gas, gas_burnt, gas_left) = match transaction_signers
            .get(&outcome_id)
        {
            Some(signer_id) => (
                Producer::Transaction { tx_hash: outcome_id, signer_id: signer_id.clone() },
                None,
                None,
                None,
            ),
            None => {
                chunk_row.receipts_processed += 1;
                let Some(receipt) = chain_store.get_receipt(&outcome_id) else { continue };
                let (VersionedReceiptEnum::Action(action_receipt)
                | VersionedReceiptEnum::PromiseYield(action_receipt)) = receipt.versioned_receipt()
                else {
                    continue;
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
                let gas_left = prepaid_gas
                    .saturating_sub(outcome.gas_burnt)
                    .saturating_sub(constant_children_gas);
                (
                    Producer::Receipt {
                        receipt_id: outcome_id,
                        receiver_id: receipt.receiver_id().clone(),
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
        serde_json::to_writer(&mut *out, &row)?;
        out.write_all(b"\n")?;
        rows_written += 1;
    }
    serde_json::to_writer(&mut *chunk_out, &chunk_row)?;
    chunk_out.write_all(b"\n")?;
    Ok(rows_written)
}

pub fn extract_range(
    chain_store: &ChainStore,
    start_height: BlockHeight,
    end_height: BlockHeight,
    out: &mut impl Write,
    chunk_out: &mut impl Write,
) -> anyhow::Result<usize> {
    let configs = RuntimeConfigStore::new();
    let mut total_rows = 0;
    for height in start_height..=end_height {
        let Ok(block_hash) = chain_store.get_block_hash_by_height(height) else { continue };
        let Ok(block) = chain_store.get_block(&block_hash) else { continue };
        let protocol_version = block.header().latest_protocol_version();

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
                out,
                chunk_out,
            )?;
        }
    }
    Ok(total_rows)
}
