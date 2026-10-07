use crate::row::{ChargedItem, ChildReceipt, Producer, ProducerRow};
use near_parameters::{ActionCosts, RuntimeConfigStore};
use near_primitives::hash::CryptoHash;
use near_primitives::types::{AccountId, Gas, ProtocolVersion};
use serde::Serialize;
use std::collections::HashMap;

/// One candidate parameter change, kept apart from the others so a run never
/// mixes them. Two receipts that each survive one change on their own may fail
/// when both ship together, so a combined verdict needs its own run.
#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum Analysis {
    /// Raise `send_sir` to `send_not_sir` on `function_call_cost_per_byte` and
    /// `deploy_contract_cost_per_byte`.
    SendSirCallAndDeploy,
    /// Raise `send_sir` to `send_not_sir` on every per-byte fee that still has
    /// the two differing: the two above plus `add_key` per byte,
    /// `data_receipt_creation` per byte, and both global contract fees.
    SendSirAllPerByte,
    /// Raise the execution fee of every added key so the existing
    /// `min_gas_purchase_price` skim collects `account_creation_charge`.
    AddKeyExecution,
}

/// The execution fee that makes the skim reach 7 mNEAR at
/// `min_gas_purchase_price`, sized the way `create_account` was:
/// `min_gas_purchase_price * exec_fee >= account_creation_charge`.
pub const ADDED_KEY_EXECUTION_TARGET: Gas = Gas::from_gas(7_200_000_000_000);

/// How much more a producer burns for one charged item under the candidate change.
fn extra_gas(
    item: &ChargedItem,
    is_self_call: bool,
    analysis: Analysis,
    protocol_version: ProtocolVersion,
    configs: &RuntimeConfigStore,
) -> Gas {
    let fees = &configs.get_config(protocol_version).fees;
    let send_sir_rise_to_not_sir = |cost: ActionCosts| -> Gas {
        let fee = fees.fee(cost);
        fee.send_fee(false).gas.saturating_sub(fee.send_fee(true).gas)
    };
    match (analysis, item) {
        // The `send_sir` rises only apply where the producer sent to itself.
        (_, _) if analysis != Analysis::AddKeyExecution && !is_self_call => Gas::ZERO,

        (
            Analysis::SendSirCallAndDeploy | Analysis::SendSirAllPerByte,
            ChargedItem::FunctionCall { payload_bytes, .. },
        ) => {
            send_sir_rise_to_not_sir(ActionCosts::function_call_byte).saturating_mul(*payload_bytes)
        }

        (
            Analysis::SendSirCallAndDeploy | Analysis::SendSirAllPerByte,
            ChargedItem::DeployContract { code_bytes },
        ) => {
            send_sir_rise_to_not_sir(ActionCosts::deploy_contract_byte).saturating_mul(*code_bytes)
        }

        // Only the function call forms pay `add_function_call_key_*`. A full
        // access key pays `add_full_access_key`, whose send fees do not differ.
        (
            Analysis::SendSirAllPerByte,
            ChargedItem::AddKey { permission, method_names_bytes, .. },
        ) if permission.is_function_call() => {
            send_sir_rise_to_not_sir(ActionCosts::add_function_call_key_byte)
                .saturating_mul(*method_names_bytes)
        }

        (Analysis::SendSirAllPerByte, ChargedItem::ReturnedData { payload_bytes }) => {
            send_sir_rise_to_not_sir(ActionCosts::new_data_receipt_byte)
                .saturating_mul(*payload_bytes)
        }

        (Analysis::SendSirAllPerByte, ChargedItem::DeployGlobalContract { code_bytes }) => {
            send_sir_rise_to_not_sir(ActionCosts::deploy_global_contract_byte)
                .saturating_mul(*code_bytes)
        }

        (Analysis::SendSirAllPerByte, ChargedItem::UseGlobalContract { identifier_bytes }) => {
            send_sir_rise_to_not_sir(ActionCosts::use_global_contract_byte)
                .saturating_mul(*identifier_bytes)
        }

        // Charged on every key, whoever the receiver is, so no `is_self_call` test.
        // Pricing a key at `account_creation_charge` is about the state it
        // leaves behind, not about who the receiver is, so every added key
        // counts, full access ones included.
        (Analysis::AddKeyExecution, ChargedItem::AddKey { permission, .. }) => {
            let base = if permission.is_function_call() {
                ActionCosts::add_function_call_key_base
            } else {
                ActionCosts::add_full_access_key
            };
            ADDED_KEY_EXECUTION_TARGET.saturating_sub(fees.fee(base).exec_fee().gas)
        }

        _ => Gas::ZERO,
    }
}

fn extra_gas_for_child(
    child: &ChildReceipt,
    analysis: Analysis,
    protocol_version: ProtocolVersion,
    configs: &RuntimeConfigStore,
) -> Gas {
    child
        .items
        .iter()
        .map(|item| extra_gas(item, child.is_self_call, analysis, protocol_version, configs))
        .fold(Gas::ZERO, |total, gas| total.saturating_add(gas))
}

/// What to assume about gas a producer never received because its own producer
/// burned more. The exact figure needs the parent's loss split across its
/// derived children, so both ends of that range are offered instead: if the two
/// runs name the same failures, the split is never needed.
#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum InheritedLoss {
    /// Nobody inherits anything. Undercounts failures, so a lower bound.
    None,
    /// Every derived child inherits its parent's whole loss, unsplit.
    /// Overcounts, so an upper bound.
    WholeParentLoss,
}

#[derive(Serialize, Clone, Debug)]
pub struct Failure {
    pub receipt_id: CryptoHash,
    pub receiver_id: AccountId,
    pub block_height: near_primitives::types::BlockHeight,
    pub extra_burn: Gas,
    pub inherited_loss: Gas,
    pub gas_left_after_constant_children: Gas,
}

#[derive(Serialize, Default, Debug)]
pub struct Report {
    pub rows_read: u64,
    pub transaction_producers: u64,
    pub receipt_producers: u64,
    /// Producers that would run out of gas. These are the blockers: their
    /// budget was fixed by deployed code and nobody can raise it.
    pub failures: Vec<Failure>,
    /// Transactions that would need more gas attached. The signer can fix these.
    pub transactions_needing_more_gas: u64,
}

pub fn evaluate(
    rows: impl Iterator<Item = anyhow::Result<ProducerRow>>,
    analysis: Analysis,
    inherited_loss: InheritedLoss,
    configs: &RuntimeConfigStore,
) -> anyhow::Result<Report> {
    let mut report = Report::default();
    let mut loss_by_receipt: HashMap<CryptoHash, Gas> = HashMap::new();

    for row in rows {
        let row = row?;
        report.rows_read += 1;

        let extra_burn = row
            .children
            .iter()
            .map(|child| extra_gas_for_child(child, analysis, row.protocol_version, configs))
            .fold(Gas::ZERO, |total, gas| total.saturating_add(gas));

        let receipt_id = match &row.producer {
            Producer::Transaction { .. } => {
                report.transaction_producers += 1;
                if extra_burn > Gas::ZERO {
                    report.transactions_needing_more_gas += 1;
                }
                continue;
            }
            Producer::Receipt { receipt_id, .. } => *receipt_id,
        };
        report.receipt_producers += 1;

        let inherited = match inherited_loss {
            InheritedLoss::None => Gas::ZERO,
            InheritedLoss::WholeParentLoss => {
                loss_by_receipt.remove(&receipt_id).unwrap_or(Gas::ZERO)
            }
        };
        let gas_left = row.gas_left_after_constant_children.unwrap_or(Gas::ZERO);
        let total_extra = extra_burn.saturating_add(inherited);

        if total_extra > gas_left {
            let Producer::Receipt { receiver_id, .. } = &row.producer else { unreachable!() };
            report.failures.push(Failure {
                receipt_id,
                receiver_id: receiver_id.clone(),
                block_height: row.block_height,
                extra_burn,
                inherited_loss: inherited,
                gas_left_after_constant_children: gas_left,
            });
            // A producer that runs out of gas creates no children, so there is
            // nothing downstream to charge the loss to.
            continue;
        }

        if inherited_loss == InheritedLoss::WholeParentLoss && total_extra > Gas::ZERO {
            for child in row.children.iter().filter(|child| child.attached_gas_is_derived) {
                loss_by_receipt.insert(child.receipt_id, total_extra);
            }
        }
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row::ChildReceipt;
    use near_primitives::types::ShardId;
    use near_primitives::version::PROTOCOL_VERSION;

    fn tgas(amount: u64) -> Gas {
        Gas::from_gas(amount * 1_000_000_000_000)
    }

    fn self_call_with_payload(payload_bytes: u64) -> ChildReceipt {
        ChildReceipt {
            receipt_id: CryptoHash::hash_bytes(b"child"),
            receiver_id: "caller.near".parse().unwrap(),
            attached_gas: tgas(5),
            is_self_call: true,
            attached_gas_is_derived: false,
            items: vec![ChargedItem::FunctionCall {
                method_name: "callback".to_owned(),
                payload_bytes,
                attached_gas: tgas(5),
            }],
        }
    }

    fn receipt_producer(gas_left: Gas, children: Vec<ChildReceipt>) -> ProducerRow {
        ProducerRow {
            block_height: 1,
            shard_id: ShardId::new(0),
            protocol_version: PROTOCOL_VERSION,
            producer: Producer::Receipt {
                receipt_id: CryptoHash::hash_bytes(b"producer"),
                receiver_id: "caller.near".parse().unwrap(),
            },
            prepaid_gas: Some(tgas(100)),
            gas_burnt: Some(tgas(1)),
            gas_left_after_constant_children: Some(gas_left),
            children,
        }
    }

    fn run(rows: Vec<ProducerRow>, analysis: Analysis, inherited: InheritedLoss) -> Report {
        let configs = RuntimeConfigStore::new();
        evaluate(rows.into_iter().map(Ok), analysis, inherited, &configs).unwrap()
    }

    #[test]
    fn self_call_within_headroom_does_not_fail() {
        let payload_bytes = 1_000;
        let ample_gas_left = tgas(50);
        let report = run(
            vec![receipt_producer(ample_gas_left, vec![self_call_with_payload(payload_bytes)])],
            Analysis::SendSirCallAndDeploy,
            InheritedLoss::None,
        );
        assert_eq!(report.receipt_producers, 1);
        assert!(report.failures.is_empty());
    }

    #[test]
    fn self_call_beyond_headroom_fails() {
        let payload_bytes = 1_000_000;
        let scant_gas_left = tgas(1);
        let report = run(
            vec![receipt_producer(scant_gas_left, vec![self_call_with_payload(payload_bytes)])],
            Analysis::SendSirCallAndDeploy,
            InheritedLoss::None,
        );
        assert_eq!(report.failures.len(), 1);
        assert_eq!(report.failures[0].gas_left_after_constant_children, scant_gas_left);
    }

    #[test]
    fn call_to_another_account_is_not_charged_send_sir() {
        let payload_bytes = 1_000_000;
        let scant_gas_left = tgas(1);
        let mut child = self_call_with_payload(payload_bytes);
        child.is_self_call = false;
        let report = run(
            vec![receipt_producer(scant_gas_left, vec![child])],
            Analysis::SendSirCallAndDeploy,
            InheritedLoss::None,
        );
        assert!(report.failures.is_empty());
    }

    #[test]
    fn add_key_execution_is_charged_whoever_the_receiver_is() {
        let scant_gas_left = tgas(1);
        let mut child = self_call_with_payload(0);
        child.is_self_call = false;
        child.items = vec![ChargedItem::AddKey {
            permission: crate::row::AddedKeyPermission::FunctionCall,
            method_names_bytes: 0,
            allowance: None,
            gas_key_balance: None,
        }];
        let report = run(
            vec![receipt_producer(scant_gas_left, vec![child])],
            Analysis::AddKeyExecution,
            InheritedLoss::None,
        );
        assert_eq!(report.failures.len(), 1);
    }

    #[test]
    fn returned_data_is_charged_only_by_all_per_byte_analysis() {
        let scant_gas_left = tgas(1);
        let mut child = self_call_with_payload(0);
        child.items = vec![ChargedItem::ReturnedData { payload_bytes: 1_000_000 }];
        let only_call_and_deploy = run(
            vec![receipt_producer(scant_gas_left, vec![child.clone()])],
            Analysis::SendSirCallAndDeploy,
            InheritedLoss::None,
        );
        assert!(only_call_and_deploy.failures.is_empty());
        let all_per_byte = run(
            vec![receipt_producer(scant_gas_left, vec![child])],
            Analysis::SendSirAllPerByte,
            InheritedLoss::None,
        );
        assert_eq!(all_per_byte.failures.len(), 1);
    }

    #[test]
    fn transaction_producer_is_counted_apart_from_failures() {
        let payload_bytes = 1_000_000;
        let mut row = receipt_producer(Gas::ZERO, vec![self_call_with_payload(payload_bytes)]);
        row.producer = Producer::Transaction {
            tx_hash: CryptoHash::hash_bytes(b"tx"),
            signer_id: "signer.near".parse().unwrap(),
        };
        let report = run(vec![row], Analysis::SendSirCallAndDeploy, InheritedLoss::None);
        assert!(report.failures.is_empty());
        assert_eq!(report.transaction_producers, 1);
        assert_eq!(report.transactions_needing_more_gas, 1);
    }

    #[test]
    fn whole_parent_loss_reaches_derived_child_but_none_does_not() {
        let parent_id = CryptoHash::hash_bytes(b"parent");
        let child_id = CryptoHash::hash_bytes(b"child");
        let mut derived_child = self_call_with_payload(1_000_000);
        derived_child.receipt_id = child_id;
        derived_child.attached_gas_is_derived = true;

        let ample_gas_left = tgas(500);
        let mut parent = receipt_producer(ample_gas_left, vec![derived_child]);
        parent.producer = Producer::Receipt {
            receipt_id: parent_id,
            receiver_id: "caller.near".parse().unwrap(),
        };

        let scant_gas_left = tgas(1);
        let mut child = receipt_producer(scant_gas_left, vec![]);
        child.producer =
            Producer::Receipt { receipt_id: child_id, receiver_id: "caller.near".parse().unwrap() };

        let rows = vec![parent, child];
        let without_inheritance =
            run(rows.clone(), Analysis::SendSirCallAndDeploy, InheritedLoss::None);
        assert!(without_inheritance.failures.is_empty());

        let with_inheritance =
            run(rows, Analysis::SendSirCallAndDeploy, InheritedLoss::WholeParentLoss);
        assert_eq!(with_inheritance.failures.len(), 1);
        assert_eq!(with_inheritance.failures[0].receipt_id, child_id);
    }

    #[test]
    fn failed_parent_does_not_pass_loss_to_its_children() {
        let parent_id = CryptoHash::hash_bytes(b"parent");
        let child_id = CryptoHash::hash_bytes(b"child");
        let mut derived_child = self_call_with_payload(1_000_000);
        derived_child.receipt_id = child_id;
        derived_child.attached_gas_is_derived = true;

        let scant_gas_left = tgas(1);
        let mut parent = receipt_producer(scant_gas_left, vec![derived_child]);
        parent.producer = Producer::Receipt {
            receipt_id: parent_id,
            receiver_id: "caller.near".parse().unwrap(),
        };

        let mut child = receipt_producer(scant_gas_left, vec![]);
        child.producer =
            Producer::Receipt { receipt_id: child_id, receiver_id: "caller.near".parse().unwrap() };

        let report = run(
            vec![parent, child],
            Analysis::SendSirCallAndDeploy,
            InheritedLoss::WholeParentLoss,
        );
        assert_eq!(report.failures.len(), 1);
        assert_eq!(report.failures[0].receipt_id, parent_id);
    }
}
