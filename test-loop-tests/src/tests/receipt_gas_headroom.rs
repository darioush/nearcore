use crate::setup::builder::TestLoopBuilder;
use crate::utils::transactions::make_accounts;
use near_async::time::Duration;
use near_chain::ChainStore;
use near_o11y::testonly::init_test_logger;
use near_primitives::types::{AccountId, Balance, Gas};
use near_receipt_gas_headroom_tool::extract::extract_range;
use near_receipt_gas_headroom_tool::{ChargedItem, ChunkRow, Producer, ProducerRow};
use std::collections::HashSet;

/// Reads back the rows `extract` wrote, so the assertions see the same JSON
/// lines a real run produces rather than an in-memory shortcut.
fn parse_rows(bytes: &[u8]) -> Vec<ProducerRow> {
    std::str::from_utf8(bytes)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn parse_chunk_rows(bytes: &[u8]) -> Vec<ChunkRow> {
    std::str::from_utf8(bytes)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

/// Asks the contract to call itself with a fixed gas amount, the shape a
/// hardcoded callback has. Every spec carries the promise index it expects
/// back, which `call_promise` asserts on.
fn fixed_gas_self_call(contract: &AccountId, attached: Gas) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!([
        { "create": {
            "account_id": contract,
            "method_name": "log_something",
            "arguments": [],
            "amount": "0",
            "gas": attached.as_gas(),
        },
          "id": 0 },
    ]))
    .unwrap()
}

#[test]
fn extract_records_self_calls_and_tells_fixed_gas_from_derived() {
    init_test_logger();

    let epoch_length = 10;
    let accounts = make_accounts(4);
    let contract: AccountId = accounts[0].clone();
    let caller: AccountId = accounts[1].clone();

    let mut env = TestLoopBuilder::new()
        .epoch_length(epoch_length)
        // Without this the receipts are collected before the extract runs.
        .gc_num_epochs_to_keep(100)
        .add_user_accounts(&accounts, Balance::from_near(1_000_000))
        .build();

    let timeout = Duration::seconds(90);
    let deploy = env.node(0).tx_deploy_test_contract(&contract);
    env.node_runner(0).run_tx(deploy, timeout);

    // Fully derived gas: the contract calls itself with gas_fixed = 0 and
    // gas_weight = 1, so each hop receives whatever the previous one left.
    let recursive = env.node(0).tx_call(
        &caller,
        &contract,
        "max_self_recursion_delay",
        0u32.to_be_bytes().to_vec(),
        Balance::ZERO,
        // Enough for a few hops: the method stops once under 5 Tgas remain.
        Gas::from_gas(30_000_000_000_000),
    );
    env.node_runner(0).run_tx(recursive, timeout);

    // Fixed gas: a self-call carrying a hardcoded amount.
    let fixed = env.node(0).tx_call(
        &caller,
        &contract,
        "call_promise",
        fixed_gas_self_call(&contract, Gas::from_gas(5_000_000_000_000)),
        Balance::ZERO,
        Gas::from_gas(100_000_000_000_000),
    );
    env.node_runner(0).run_tx(fixed, timeout);

    let node = env.node(0);
    let head_height = node.head().height;
    let chain_store = ChainStore::new(node.store(), true, 1000);

    let mut rows_out = Vec::new();
    let mut chunk_out = Vec::new();
    let rows_written =
        extract_range(&chain_store, 1, head_height, &mut rows_out, &mut chunk_out).unwrap();

    let rows = parse_rows(&rows_out);
    let chunk_rows = parse_chunk_rows(&chunk_out);
    assert_eq!(rows.len(), rows_written);
    assert!(!rows.is_empty());
    assert!(!chunk_rows.is_empty());

    let mut seen_producers = HashSet::new();
    let mut receipt_producers = 0;
    for row in &rows {
        match &row.producer {
            Producer::Transaction { tx_hash, .. } => {
                assert!(seen_producers.insert(*tx_hash), "producer emitted twice");
                assert!(row.gas_left_after_constant_children.is_none());
            }
            Producer::Receipt { receipt_id, .. } => {
                receipt_producers += 1;
                assert!(seen_producers.insert(*receipt_id), "producer emitted twice");
                let prepaid = row.prepaid_gas.unwrap();
                let burnt = row.gas_burnt.unwrap();
                assert!(
                    burnt <= prepaid,
                    "burnt {burnt:?} exceeded prepaid {prepaid:?}, so prepaid is missing a fee"
                );
                assert!(row.gas_left_after_constant_children.unwrap() <= prepaid);
            }
        }
    }
    assert!(receipt_producers > 0);

    // Every child belongs to exactly one producer, which is what the
    // (block_hash, shard_id) join between OutgoingReceipts and OutcomeIds buys.
    let mut seen_children = HashSet::new();
    for row in &rows {
        for child in &row.children {
            assert!(seen_children.insert(child.receipt_id), "child claimed by two producers");
        }
    }

    let self_calls: Vec<_> =
        rows.iter().flat_map(|row| &row.children).filter(|child| child.is_self_call).collect();
    assert!(!self_calls.is_empty(), "the contract calling itself should record a self call");

    let derived = self_calls.iter().filter(|child| child.attached_gas_is_derived).count();
    let fixed = self_calls.iter().filter(|child| !child.attached_gas_is_derived).count();
    assert!(derived > 0, "max_self_recursion_delay forwards what is left, so gas is derived");
    assert!(fixed > 0, "call_promise attaches a hardcoded amount, so gas is a constant");

    let function_call_payloads: usize = rows
        .iter()
        .flat_map(|row| &row.children)
        .flat_map(|child| &child.items)
        .filter_map(|item| match item {
            ChargedItem::FunctionCall { payload_bytes, .. } => Some(*payload_bytes as usize),
            _ => None,
        })
        .sum();
    assert!(function_call_payloads > 0, "function call payloads should be counted for the fee");

    let gas_burnt_in_chunks: u64 =
        chunk_rows.iter().map(|chunk| chunk.gas_burnt.as_gas()).sum::<u64>();
    assert!(gas_burnt_in_chunks > 0, "running a contract should burn gas");
}
