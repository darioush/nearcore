use crate::setup::builder::TestLoopBuilder;
use crate::utils::node::TestLoopNode;
use crate::utils::transactions::execute_money_transfers;
use itertools::Itertools;
use near_async::time::Duration;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_o11y::testonly::init_test_logger;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{AccountId, Balance, ShardId};

/// Runs chain with sequence of chunks with empty state changes, long enough to
/// cover 5 epochs which is default GC period.
/// After that, it checks that memtrie for the shard can be loaded.
/// This is a repro for #11583 where flat storage head was not moved at all at
/// this scenario, so chain data related to that block was garbage collected,
/// and loading memtrie failed because of missing `ChunkExtra` with desired
/// state root.
#[test]
fn test_load_memtrie_after_empty_chunks() {
    init_test_logger();
    let builder = TestLoopBuilder::new();

    let num_accounts = 3;
    let num_clients = 2;
    let epoch_length = 5;
    // Set 2 shards, first of which doesn't have any validators.
    let boundary_accounts = ["account1"].iter().map(|a| a.parse().unwrap()).collect();
    let shard_layout = ShardLayout::multi_shard_custom(boundary_accounts, 1);
    let accounts = (num_accounts - num_clients..num_accounts)
        .map(|i| format!("account{}", i).parse().unwrap())
        .collect::<Vec<AccountId>>();
    let client_accounts = accounts.iter().take(num_clients).cloned().collect_vec();
    let validators_spec = ValidatorsSpec::desired_roles(
        &client_accounts.iter().map(|t| t.as_str()).collect_vec(),
        &[],
    );

    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .shard_layout(shard_layout.clone())
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&accounts, Balance::from_near(1_000_000))
        .genesis_height(10000)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::build_store_from_genesis(&genesis);
    let mut env = builder
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(client_accounts)
        .build();

    execute_money_transfers(&mut env.test_loop, &env.node_datas, &accounts).unwrap();

    // Make sure the chain progresses for several epochs.
    let client_handle = env.node_datas[0].client_sender.actor_handle();
    env.test_loop.run_until(
        |test_loop_data| {
            test_loop_data.get(&client_handle).client.chain.head().unwrap().height
                > 10000 + epoch_length * 10
        },
        Duration::seconds(10),
    );

    // Find client currently tracking shard with index 0.
    let shard_uid = shard_layout.shard_uids().next().unwrap();
    let shard_id = shard_uid.shard_id();
    let tracked_shards_per_node = env
        .node_datas
        .iter()
        .map(|node_data| TestLoopNode { data: &env.test_loop.data, node_data }.tracked_shards())
        .collect_vec();
    tracing::info!(?tracked_shards_per_node, "current tracked shards");
    let idx = tracked_shards_per_node
        .iter()
        .enumerate()
        .find_map(|(idx, shards)| if shards.contains(&shard_id) { Some(idx) } else { None })
        .expect("Not found any client tracking shard 0");

    // Unload memtrie and load it back, check that it doesn't panic.
    let client =
        TestLoopNode { data: &env.test_loop.data, node_data: &env.node_datas[idx] }.client();
    client.runtime_adapter.get_tries().unload_memtrie(&shard_uid);
    client
        .runtime_adapter
        .get_tries()
        .load_memtrie(&shard_uid, None, true)
        .expect("Couldn't load memtrie");
}

/// Reproduces the race between in-flight optimistic-block apply and memtrie root GC.
///
/// An apply task clones `Arc<RwLock<MemTries>>` inside `get_trie_for_shard`, then later
/// resolves the prev_state_root via `MemTries::get_root`. If memtrie GC runs between those
/// two steps and evicts the root, the apply fails with `StorageInconsistentState`.
///
/// The test parks all nodes' OB applies at a breakpoint, lets the chain skip that height
/// and advance far enough for natural GC to evict the stale roots, then resumes the
/// parked applies and checks `RuntimeAdapter::chunk_apply_fatal_error_count`.
#[test]
#[cfg(feature = "test_features")]
fn test_ob_apply_panics_when_root_gced() {
    init_test_logger();

    let accounts: Vec<AccountId> =
        (0..4).map(|i| format!("account{}", i).parse().unwrap()).collect();

    let mut env = TestLoopBuilder::new()
        .num_shards(1)
        .validators(accounts.len(), 0)
        .add_user_accounts(&accounts, Balance::from_near(1_000_000))
        .enable_yield_points()
        .build();

    // Arm a breakpoint that parks ALL optimistic-block applies. With every node's OB
    // parked, the normal block at that height is queued (BlockPendingOptimisticExecution)
    // on every node. The next block producer builds on the previous height, skipping it.
    // The chain then advances past the parked height and natural GC evicts the stale root.
    let bp = env
        .test_loop
        .breakpoint("after_trie_for_apply")
        .when(|ctx| ctx.get("block_type") == Some("Optimistic"))
        .arm();

    env.test_loop.run_until(|_| bp.hit_count() == accounts.len(), Duration::seconds(10));
    let all_hits = bp.drain_hits();

    // Disarm so subsequent OB applies (at higher heights) flow through.
    drop(bp);

    // Advance the chain far enough for natural GC to evict the parked memtrie roots.
    env.test_loop.run_for(Duration::seconds(10));

    // Resume all parked OB applies. Each tries to look up a state root that GC has evicted.
    for hit in all_hits {
        hit.resume();
    }
    env.test_loop.run_for(Duration::seconds(1));

    let error_count: usize = env
        .node_datas
        .iter()
        .map(|nd| {
            let client = &env.test_loop.data.get(&nd.client_sender.actor_handle()).client;
            client.runtime_adapter.chunk_apply_fatal_error_count()
        })
        .sum();
    assert_eq!(error_count, accounts.len());
}

/// Reproduces the race between in-flight OB apply and `freeze_parent_memtrie` during resharding.
///
/// `freeze_parent_memtrie` replaces the parent shard's `MemTries` contents with an empty
/// `MemTries::new()` inside the same `Arc<RwLock<MemTries>>`. An in-flight apply that already
/// cloned this Arc (via `get_trie_for_shard`) will see the empty MemTries when it tries to
/// resolve `prev_state_root`, causing a `StorageInconsistentState` panic.
///
/// The test parks all OB applies at a breakpoint, lets the chain skip that height and advance
/// through the epoch boundary where resharding calls `freeze_parent_memtrie`, then resumes
/// the parked OB applies and checks that OBs for the frozen parent shard fail.
#[test]
#[cfg(feature = "test_features")]
fn test_ob_apply_panics_when_parent_memtrie_frozen() {
    use crate::utils::setups::derive_new_epoch_config_from_boundary;
    use near_chain_configs::test_genesis::TestGenesisBuilder;
    use near_primitives::epoch_manager::EpochConfigStore;
    use near_primitives::version::PROTOCOL_VERSION;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    init_test_logger();

    let epoch_length: u64 = 10;
    let num_accounts = 8;
    let num_producers: usize = 3;
    let num_validators: usize = 2;
    let num_clients = num_producers + num_validators;
    let accounts: Vec<AccountId> =
        (0..num_accounts).map(|i| format!("account{}", i).parse().unwrap()).collect();
    let clients: Vec<AccountId> = accounts[..num_clients].to_vec();
    let producers: Vec<AccountId> = accounts[..num_producers].to_vec();
    let validators: Vec<AccountId> = accounts[num_producers..num_clients].to_vec();

    let base_protocol_version = PROTOCOL_VERSION - 2;
    let base_shard_layout = {
        let boundary_accounts = vec!["account1".parse().unwrap(), "account3".parse().unwrap()];
        let shard_ids = vec![ShardId::new(5), ShardId::new(3), ShardId::new(6)];
        let shards_split_map = [(ShardId::new(0), shard_ids.clone())].into_iter().collect();
        ShardLayout::v2(boundary_accounts, shard_ids, Some(shards_split_map))
    };
    let mut base_epoch_config = EpochConfigStore::for_chain_id("mainnet", None)
        .unwrap()
        .get_config(base_protocol_version)
        .as_ref()
        .clone();
    base_epoch_config.num_block_producer_seats = num_producers as u64;
    base_epoch_config.num_chunk_producer_seats = num_producers as u64;
    base_epoch_config.num_chunk_validator_seats = num_clients as u64;
    let base_epoch_config = base_epoch_config.with_shard_layout(base_shard_layout.clone());

    let new_boundary_account: AccountId = "account6".parse().unwrap();
    let (new_epoch_config, _) =
        derive_new_epoch_config_from_boundary(&base_epoch_config, &new_boundary_account);

    let epoch_config_store = EpochConfigStore::test(BTreeMap::from([
        (base_protocol_version, Arc::new(base_epoch_config)),
        (base_protocol_version + 1, Arc::new(new_epoch_config)),
    ]));

    let builder = TestLoopBuilder::new();
    let genesis = TestGenesisBuilder::new()
        .genesis_time_from_clock(&builder.clock())
        .shard_layout(base_shard_layout)
        .protocol_version(base_protocol_version)
        .epoch_length(epoch_length)
        .validators_spec(ValidatorsSpec::desired_roles(
            &producers.iter().map(|a| a.as_str()).collect_vec(),
            &validators.iter().map(|a| a.as_str()).collect_vec(),
        ))
        .add_user_accounts_simple(&accounts, Balance::from_near(1_000_000))
        .build();

    let mut env = builder
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients)
        .track_all_shards()
        .enable_yield_points()
        .config_modifier(|config, _| {
            let mut resharding_config = config.resharding_config.get();
            resharding_config.batch_delay = Duration::milliseconds(1);
            config.resharding_config.update(resharding_config);
        })
        .build();

    // Advance close to the resharding epoch boundary (height ~22) so that parked OBs
    // hold recent roots that won't be GC'd before the freeze. This makes the test
    // specific to the freeze bug rather than also triggering GC-related failures.
    let client_handle = env.node_datas[0].client_sender.actor_handle();
    env.test_loop.run_until(
        |data| data.get(&client_handle).client.chain.head().unwrap().height >= 18,
        Duration::seconds(15),
    );

    // Park OB applies for shard 6 (the parent being split). With shard 6's OBs incomplete,
    // the overall OB result on each node is partial, so the block at that height is queued
    // (BlockPendingOptimisticExecution). The next block producer builds on the previous
    // height, skipping it. The chain advances through the resharding epoch boundary where
    // freeze_parent_memtrie wipes s6's MemTries inside the same Arc the parked OBs hold.
    let bp = env
        .test_loop
        .breakpoint("after_trie_for_apply")
        .when(|ctx| ctx.get("block_type") == Some("Optimistic") && ctx.get("shard_id") == Some("6"))
        .arm();

    env.test_loop.run_until(|_| bp.hit_count() >= num_clients, Duration::seconds(10));
    let all_hits = bp.drain_hits();
    let num_parked = all_hits.len();
    drop(bp);

    // Advance the chain through the resharding boundary. freeze_parent_memtrie replaces
    // s6's MemTries with MemTries::new() inside the same Arc the parked OBs hold.
    let client_handle = env.node_datas[0].client_sender.actor_handle();
    env.test_loop.run_until(
        |data| data.get(&client_handle).client.chain.head().unwrap().height >= 30,
        Duration::seconds(30),
    );

    // Verify resharding actually happened.
    let client = &env.test_loop.data.get(&client_handle).client;
    let tip = client.chain.head().unwrap();
    let shard_layout = client.epoch_manager.get_shard_layout(&tip.epoch_id).unwrap();
    assert_eq!(shard_layout.num_shards(), 4, "expected 4 shards after resharding");

    // Resume all parked OB applies. Each tries to look up a state root in the now-empty
    // MemTries and fails.
    for hit in all_hits {
        hit.resume();
    }
    env.test_loop.run_for(Duration::seconds(1));

    let error_count: usize = env
        .node_datas
        .iter()
        .map(|nd| {
            let client = &env.test_loop.data.get(&nd.client_sender.actor_handle()).client;
            client.runtime_adapter.chunk_apply_fatal_error_count()
        })
        .sum();
    assert_eq!(error_count, num_parked);
}

/// Same race as `test_ob_apply_panics_when_parent_memtrie_frozen`, but triggered via a chain
/// fork instead of optimistic blocks.
///
/// A single node double-signs at the resharding boundary, producing two competing blocks at
/// the same height with different parents. The fork block's shard 6 apply clones
/// `Arc<RwLock<MemTries>>` and is parked at a breakpoint. The canonical block then processes
/// normally: its postprocess calls `freeze_parent_memtrie`, replacing the MemTries contents
/// inside the same Arc. When the parked fork apply resumes, it sees empty MemTries and fails.
#[test]
#[cfg(feature = "test_features")]
fn test_fork_apply_panics_when_parent_memtrie_frozen() {
    use crate::utils::setups::derive_new_epoch_config_from_boundary;
    use near_chain_configs::test_genesis::TestGenesisBuilder;
    use near_client::client_actor::AdvProduceBlockHeightSelection;
    use near_primitives::epoch_manager::EpochConfigStore;
    use near_primitives::version::PROTOCOL_VERSION;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    init_test_logger();

    let epoch_length: u64 = 10;
    let num_accounts = 4;
    let accounts: Vec<AccountId> =
        (0..num_accounts).map(|i| format!("account{}", i).parse().unwrap()).collect();

    let base_protocol_version = PROTOCOL_VERSION - 2;
    let base_shard_layout = {
        let boundary_accounts = vec!["account1".parse().unwrap(), "account3".parse().unwrap()];
        let shard_ids = vec![ShardId::new(5), ShardId::new(3), ShardId::new(6)];
        let shards_split_map = [(ShardId::new(0), shard_ids.clone())].into_iter().collect();
        ShardLayout::v2(boundary_accounts, shard_ids, Some(shards_split_map))
    };
    let mut base_epoch_config = EpochConfigStore::for_chain_id("mainnet", None)
        .unwrap()
        .get_config(base_protocol_version)
        .as_ref()
        .clone();
    base_epoch_config.num_block_producer_seats = 1;
    base_epoch_config.num_chunk_producer_seats = 1;
    base_epoch_config.num_chunk_validator_seats = 1;
    let base_epoch_config = base_epoch_config.with_shard_layout(base_shard_layout.clone());

    let new_boundary_account: AccountId = "account6".parse().unwrap();
    let (new_epoch_config, _) =
        derive_new_epoch_config_from_boundary(&base_epoch_config, &new_boundary_account);

    let epoch_config_store = EpochConfigStore::test(BTreeMap::from([
        (base_protocol_version, Arc::new(base_epoch_config)),
        (base_protocol_version + 1, Arc::new(new_epoch_config)),
    ]));

    let builder = TestLoopBuilder::new();
    let genesis = TestGenesisBuilder::new()
        .genesis_time_from_clock(&builder.clock())
        .shard_layout(base_shard_layout)
        .protocol_version(base_protocol_version)
        .epoch_length(epoch_length)
        .validators_spec(ValidatorsSpec::desired_roles(&[accounts[0].as_str()], &[]))
        .add_user_accounts_simple(&accounts, Balance::from_near(1_000_000))
        .build();

    let mut env = builder
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(vec![accounts[0].clone()])
        .track_all_shards()
        .enable_yield_points()
        .config_modifier(|config, _| {
            let mut resharding_config = config.resharding_config.get();
            resharding_config.batch_delay = Duration::milliseconds(1);
            config.resharding_config.update(resharding_config);
        })
        .build();

    // Advance to height 20 (one before the resharding boundary at height 21).
    let client_handle = env.node_datas[0].client_sender.actor_handle();
    env.test_loop.run_until(
        |data| data.get(&client_handle).client.chain.head().unwrap().height >= 20,
        Duration::seconds(15),
    );

    // Arm breakpoint for shard 6 Normal applies. The fork block's apply will be the first
    // hit; we take it and disarm so the canonical block's apply flows through.
    let bp = env
        .test_loop
        .breakpoint("after_trie_for_apply")
        .when(|ctx| ctx.get("block_type") == Some("Normal") && ctx.get("shard_id") == Some("6"))
        .arm();

    // Produce a fork block at the resharding boundary: height 21 based on height 19
    // (skipping 20 to avoid double signing). This creates a competing block whose parent
    // differs from the canonical chain.
    let fork_producer_handle = env.node_datas[0].client_sender.actor_handle();
    env.test_loop.send_adhoc_event("produce_fork".to_string(), move |data| {
        let client_actor = data.get_mut(&fork_producer_handle);
        client_actor.adv_produce_blocks_on(
            1,
            true,
            AdvProduceBlockHeightSelection::SelectedHeightOnSelectedBlock {
                produced_block_height: 21,
                base_block_height: 19,
            },
        );
    });

    // Wait for the fork block's shard 6 apply to hit the breakpoint.
    env.test_loop.run_until(|_| bp.hit_count() >= 1, Duration::seconds(10));
    let fork_hit = bp.take_hit().unwrap();
    drop(bp);

    // Advance the canonical chain through the resharding boundary. The canonical block at
    // height 21 (with parent 20) processes normally, and its postprocess calls
    // freeze_parent_memtrie, replacing s6's MemTries inside the same Arc the fork apply holds.
    let client_handle = env.node_datas[0].client_sender.actor_handle();
    env.test_loop.run_until(
        |data| data.get(&client_handle).client.chain.head().unwrap().height >= 30,
        Duration::seconds(30),
    );

    // Verify resharding happened.
    let client = &env.test_loop.data.get(&client_handle).client;
    let tip = client.chain.head().unwrap();
    let shard_layout = client.epoch_manager.get_shard_layout(&tip.epoch_id).unwrap();
    assert_eq!(shard_layout.num_shards(), 4, "expected 4 shards after resharding");

    // Resume the parked fork apply. It tries to look up the state root in the now-empty
    // MemTries and fails.
    fork_hit.resume();
    env.test_loop.run_for(Duration::seconds(1));

    let client = &env.test_loop.data.get(&env.node_datas[0].client_sender.actor_handle()).client;
    let error_count = client.runtime_adapter.chunk_apply_fatal_error_count();
    assert!(error_count >= 1, "expected at least 1 fatal error from fork apply, got {error_count}");
}

/// Parks an OB apply deeper in the runtime — inside `remove_account`, right before the trie
/// iterator is taken — then freezes the parent memtrie via resharding. Earlier trie reads
/// (DelayedReceiptQueue::load, etc.) succeed because the root is still live. On resume,
/// `lock_for_iter` sees empty MemTries and fails.
///
/// Uses a two-step breakpoint: first `after_trie_for_apply` catches the OB coroutine early,
/// then `before_remove_account_iter` catches it deeper after resume.
#[test]
#[cfg(feature = "test_features")]
fn test_ob_remove_account_iter_panics_when_parent_memtrie_frozen() {
    use crate::utils::setups::derive_new_epoch_config_from_boundary;
    use near_chain_configs::test_genesis::TestGenesisBuilder;
    use near_primitives::epoch_manager::EpochConfigStore;
    use near_primitives::test_utils::create_user_test_signer;
    use near_primitives::transaction::SignedTransaction;
    use near_primitives::version::PROTOCOL_VERSION;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    init_test_logger();

    let epoch_length: u64 = 10;
    let num_accounts = 8;
    let accounts: Vec<AccountId> =
        (0..num_accounts).map(|i| format!("account{}", i).parse().unwrap()).collect();

    let base_protocol_version = PROTOCOL_VERSION - 2;
    let base_shard_layout = {
        let boundary_accounts = vec!["account1".parse().unwrap(), "account3".parse().unwrap()];
        let shard_ids = vec![ShardId::new(5), ShardId::new(3), ShardId::new(6)];
        let shards_split_map = [(ShardId::new(0), shard_ids.clone())].into_iter().collect();
        ShardLayout::v2(boundary_accounts, shard_ids, Some(shards_split_map))
    };
    let mut base_epoch_config = EpochConfigStore::for_chain_id("mainnet", None)
        .unwrap()
        .get_config(base_protocol_version)
        .as_ref()
        .clone();
    base_epoch_config.num_block_producer_seats = 1;
    base_epoch_config.num_chunk_producer_seats = 1;
    base_epoch_config.num_chunk_validator_seats = 1;
    let base_epoch_config = base_epoch_config.with_shard_layout(base_shard_layout.clone());

    let new_boundary_account: AccountId = "account6".parse().unwrap();
    let (new_epoch_config, _) =
        derive_new_epoch_config_from_boundary(&base_epoch_config, &new_boundary_account);

    let epoch_config_store = EpochConfigStore::test(BTreeMap::from([
        (base_protocol_version, Arc::new(base_epoch_config)),
        (base_protocol_version + 1, Arc::new(new_epoch_config)),
    ]));

    let builder = TestLoopBuilder::new();
    let genesis = TestGenesisBuilder::new()
        .genesis_time_from_clock(&builder.clock())
        .shard_layout(base_shard_layout)
        .protocol_version(base_protocol_version)
        .epoch_length(epoch_length)
        .validators_spec(ValidatorsSpec::desired_roles(&[accounts[0].as_str()], &[]))
        .add_user_accounts_simple(&accounts, Balance::from_near(1_000_000))
        .build();

    let mut env = builder
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(vec![accounts[0].clone()])
        .track_all_shards()
        .enable_yield_points()
        .config_modifier(|config, _| {
            let mut resharding_config = config.resharding_config.get();
            resharding_config.batch_delay = Duration::milliseconds(1);
            config.resharding_config.update(resharding_config);
        })
        .build();

    let client_handle = env.node_datas[0].client_sender.actor_handle();

    // Advance to height 18, then submit delete_account for account4 (shard 6).
    // The tx lands in a chunk around height 19-20, so the OB at the resharding boundary
    // (height 21) will include the delete_account local receipt.
    env.test_loop.run_until(
        |data| data.get(&client_handle).client.chain.head().unwrap().height >= 18,
        Duration::seconds(15),
    );

    let victim: AccountId = "account4".parse().unwrap();
    let signer = create_user_test_signer(&victim);
    let head = env.test_loop.data.get(&client_handle).client.chain.head().unwrap();
    let delete_tx = SignedTransaction::delete_account(
        1,
        victim.clone(),
        victim.clone(),
        accounts[0].clone(),
        &signer.into(),
        head.last_block_hash,
    );
    env.node(0).submit_tx(delete_tx);

    // Step 1: Arm early breakpoint for OB shard 6 applies. The OB fires before the canonical
    // block, so the first hit is the OB containing our delete_account.
    let bp1 = env
        .test_loop
        .breakpoint("after_trie_for_apply")
        .when(|ctx| ctx.get("block_type") == Some("Optimistic") && ctx.get("shard_id") == Some("6"))
        .arm();

    // Advance until the OB containing the delete_account hits the breakpoint.
    env.test_loop.run_until(|_| bp1.hit_count() >= 1, Duration::seconds(15));
    let early_hit = bp1.take_hit().unwrap();
    drop(bp1);

    // Step 2: Arm the deeper breakpoint inside remove_account, then resume. The coroutine
    // continues past initial trie reads (root still alive) and parks at the iterator.
    let bp2 = env
        .test_loop
        .breakpoint("before_remove_account_iter")
        .when(move |ctx| ctx.get("account_id") == Some("account4"))
        .arm();

    early_hit.resume();
    env.test_loop.run_until(|_| bp2.hit_count() >= 1, Duration::seconds(10));
    let iter_hit = bp2.take_hit().unwrap();
    drop(bp2);

    // Step 3: Advance the canonical chain through resharding. Freeze replaces MemTries.
    env.test_loop.run_until(
        |data| data.get(&client_handle).client.chain.head().unwrap().height >= 30,
        Duration::seconds(30),
    );

    let client = &env.test_loop.data.get(&client_handle).client;
    let tip = client.chain.head().unwrap();
    let shard_layout = client.epoch_manager.get_shard_layout(&tip.epoch_id).unwrap();
    assert_eq!(shard_layout.num_shards(), 4, "expected 4 shards after resharding");

    // Resume the parked OB apply. The iterator tries to look up the root in the now-empty
    // MemTries and panics (unwrap at iter.rs). pending_shard_jobs catches the panic via
    // catch_unwind, so we detect it through the panic hook.
    let panic_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = panic_count.clone();
    let prev_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |_| {
        counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }));

    iter_hit.resume();
    env.test_loop.run_for(Duration::seconds(1));

    std::panic::set_hook(prev_hook);
    let panics = panic_count.load(std::sync::atomic::Ordering::Relaxed);
    assert!(panics >= 1, "expected at least 1 panic from memtrie root lookup, got {panics}");
}
