// FIXME(nagisa): Is there a good reason we're triggering this? Luckily though this is just test
// code so we're in the clear.
#![allow(clippy::arc_with_non_send_sync)]

use actix::{Actor, Addr, Context};
use futures::{FutureExt, future};
use near_async::actix::AddrWithAutoSpanContextExt;
use near_async::actix_wrapper::{ActixWrapper, spawn_actix_actor};
use near_async::futures::ActixFutureSpawner;
use near_async::messaging::{
    CanSend, IntoMultiSender, IntoSender, LateBoundSender, SendAsync, Sender, noop,
};
use near_async::time::{Clock, Duration, Instant, Utc};
use near_chain::rayon_spawner::RayonAsyncComputationSpawner;
use near_chain::resharding::resharding_actor::ReshardingActor;
use near_chain::resharding::types::ReshardingSender;
use near_chain::state_snapshot_actor::SnapshotCallbacks;
use near_chain::test_utils::{KeyValueRuntime, MockEpochManager, ValidatorSchedule};
use near_chain::types::{ChainConfig, RuntimeAdapter};
use near_chain::{Chain, ChainGenesis, DoomslugThresholdMode};
use near_chain_configs::{
    ChunkDistributionNetworkConfig, ClientConfig, Genesis, MutableConfigValue, ReshardingConfig,
    ReshardingHandle, TrackedShardsConfig,
};
use near_chunks::adapter::ShardsManagerRequestFromClient;
use near_chunks::client::ShardsManagerResponse;
use near_chunks::shards_manager_actor::{ShardsManagerActor, start_shards_manager};
use near_chunks::test_utils::SynchronousShardsManagerAdapter;
use near_client::adversarial::Controls;
use near_client::client_actor::ClientActorInner;
use near_client::{
    Client, ClientActor, PartialWitnessActor, PartialWitnessSenderForClient, RpcHandler,
    RpcHandlerConfig, StartClientResult, SyncStatus, ViewClientActor, ViewClientActorInner,
    start_client,
};
use near_client::{RpcHandlerActor, spawn_rpc_handler_actor};
use near_crypto::{KeyType, PublicKey};
use near_epoch_manager::shard_tracker::ShardTracker;
use near_epoch_manager::{EpochManager, EpochManagerAdapter};
use near_network::client::{
    AnnounceAccountRequest, BlockApproval, BlockHeadersRequest, BlockHeadersResponse, BlockRequest,
    BlockResponse, ChunkEndorsementMessage, OptimisticBlockMessage, SetNetworkInfo,
    StateRequestHeader, StateRequestPart, StateResponseReceived,
};
use near_network::shards_manager::ShardsManagerRequestFromNetwork;
use near_network::state_witness::{
    ChunkContractAccessesMessage, ContractCodeRequestMessage, ContractCodeResponseMessage,
    PartialEncodedContractDeploysMessage, PartialEncodedStateWitnessForwardMessage,
    PartialEncodedStateWitnessMessage, PartialWitnessSenderForNetwork,
};
use near_network::types::{BlockInfo, PeerChainInfo};
use near_network::types::{
    ConnectedPeerInfo, FullPeerInfo, NetworkRequests, NetworkResponses, PeerManagerAdapter,
};
use near_network::types::{NetworkInfo, PeerManagerMessageRequest, PeerManagerMessageResponse};
use near_network::types::{PeerInfo, PeerType};
use near_o11y::WithSpanContextExt;
use near_primitives::block::ApprovalInner;
use near_primitives::epoch_info::RngSeed;
use near_primitives::genesis::GenesisId;
use near_primitives::hash::{CryptoHash, hash};
use near_primitives::network::PeerId;
use near_primitives::test_utils::create_test_signer;
use near_primitives::types::{AccountId, BlockHeightDelta, EpochId, NumBlocks, NumSeats, ShardId};
use near_primitives::validator_signer::{EmptyValidatorSigner, ValidatorSigner};
use near_primitives::version::{PROTOCOL_VERSION, get_protocol_upgrade_schedule};
use near_store::adapter::StoreAdapter;
use near_store::genesis::initialize_genesis_state;
use near_store::test_utils::create_test_store;
use near_telemetry::TelemetryActor;
use nearcore::NightshadeRuntime;
use num_rational::Ratio;
use parking_lot::RwLock;
use rand::{Rng, thread_rng};
use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::ops::DerefMut;
use std::sync::{Arc, OnceLock};

use crate::utils::block_stats::BlockStats;
use crate::utils::peer_manager_mock::PeerManagerMock;

pub const TEST_SEED: RngSeed = [3; 32];

/// min block production time in milliseconds
pub const MIN_BLOCK_PROD_TIME: Duration = Duration::milliseconds(100);
/// max block production time in milliseconds
pub const MAX_BLOCK_PROD_TIME: Duration = Duration::milliseconds(200);

/// Sets up ClientActor and ViewClientActor viewing the same store/runtime.
fn setup_with_mock_epoch_manager(
    clock: Clock,
    vs: ValidatorSchedule,
    epoch_length: BlockHeightDelta,
    account_id: AccountId,
    skip_sync_wait: bool,
    min_block_prod_time: u64,
    max_block_prod_time: u64,
    enable_doomslug: bool,
    archive: bool,
    state_sync_enabled: bool,
    network_adapter: PeerManagerAdapter,
    transaction_validity_period: NumBlocks,
    genesis_time: Utc,
    chunk_distribution_config: Option<ChunkDistributionNetworkConfig>,
) -> (
    Addr<ClientActor>,
    Addr<ViewClientActor>,
    Addr<RpcHandlerActor>,
    ShardsManagerAdapterForTest,
    PartialWitnessSenderForNetwork,
) {
    let store = create_test_store();
    let num_validator_seats = vs.all_block_producers().count() as NumSeats;
    let epoch_manager = MockEpochManager::new_with_validators(store.clone(), vs, epoch_length);
    let runtime = KeyValueRuntime::new_with_no_gc(store.clone(), epoch_manager.as_ref(), archive);

    setup(SetupOptions {
        epoch_manager,
        genesis_time,
        transaction_validity_period,
        epoch_length,
        account_id,
        skip_sync_wait,
        min_block_prod_time,
        max_block_prod_time,
        num_validator_seats,
        archive,
        state_sync_enabled,
        chunk_distribution_config,
        clock,
        runtime,
        network_adapter,
        store,
        enable_doomslug,
    })
}

/// Sets up ClientActor and ViewClientActor viewing the same store/runtime using real epoch
/// manager.
fn setup_with_real_epoch_manager(
    clock: Clock,
    validators: Vec<AccountId>,
    epoch_length: BlockHeightDelta,
    account_id: AccountId,
    skip_sync_wait: bool,
    min_block_prod_time: u64,
    max_block_prod_time: u64,
    enable_doomslug: bool,
    archive: bool,
    state_sync_enabled: bool,
    network_adapter: PeerManagerAdapter,
    transaction_validity_period: NumBlocks,
    genesis_time: Utc,
    chunk_distribution_config: Option<ChunkDistributionNetworkConfig>,
) -> (
    (
        Addr<ClientActor>,
        Addr<ViewClientActor>,
        Addr<RpcHandlerActor>,
        ShardsManagerAdapterForTest,
        PartialWitnessSenderForNetwork,
    ),
    tempfile::TempDir,
) {
    let store = create_test_store();

    let num_validator_seats = validators.len() as NumSeats;

    let mut validators = validators;

    // Certain tests depend on these accounts existing so we make them available here.
    // This is mostly due to historical reasons - those tests used to use heavily mocked testing
    // environment that didn't check account existence.
    for account in ["test2", "test"].into_iter().map(|acc| acc.parse().unwrap()) {
        if !validators.contains(&account) {
            validators.push(account);
        }
    }

    let mut genesis = Genesis::test(validators, num_validator_seats);
    genesis.config.epoch_length = epoch_length;
    initialize_genesis_state(store.clone(), &genesis, None);

    let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config, None);

    let tempdir = tempfile::TempDir::new().unwrap();

    let runtime = NightshadeRuntime::test(
        tempdir.path(),
        store.clone(),
        &genesis.config,
        epoch_manager.clone(),
    );

    (
        setup(SetupOptions {
            epoch_manager,
            genesis_time,
            transaction_validity_period,
            epoch_length,
            account_id,
            skip_sync_wait,
            min_block_prod_time,
            max_block_prod_time,
            num_validator_seats,
            archive,
            state_sync_enabled,
            chunk_distribution_config,
            clock,
            runtime,
            network_adapter,
            store,
            enable_doomslug,
        }),
        tempdir,
    )
}

struct SetupOptions {
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    genesis_time: time::OffsetDateTime,
    transaction_validity_period: u64,
    epoch_length: u64,
    account_id: AccountId,
    skip_sync_wait: bool,
    min_block_prod_time: u64,
    max_block_prod_time: u64,
    num_validator_seats: u64,
    archive: bool,
    state_sync_enabled: bool,
    chunk_distribution_config: Option<ChunkDistributionNetworkConfig>,
    clock: Clock,
    runtime: Arc<dyn RuntimeAdapter>,
    network_adapter: PeerManagerAdapter,
    store: near_store::Store,
    enable_doomslug: bool,
}

fn setup(
    SetupOptions {
        epoch_manager,
        genesis_time,
        transaction_validity_period,
        epoch_length,
        account_id,
        skip_sync_wait,
        min_block_prod_time,
        max_block_prod_time,
        num_validator_seats,
        archive,
        state_sync_enabled,
        chunk_distribution_config,
        clock,
        runtime,
        network_adapter,
        store,
        enable_doomslug,
    }: SetupOptions,
) -> (
    Addr<ActixWrapper<ClientActorInner>>,
    Addr<near_async::actix_wrapper::SyncActixWrapper<ViewClientActorInner>>,
    Addr<near_async::actix_wrapper::SyncActixWrapper<RpcHandler>>,
    ShardsManagerAdapterForTest,
    PartialWitnessSenderForNetwork,
) {
    let shard_tracker = ShardTracker::new(TrackedShardsConfig::AllShards, epoch_manager.clone());
    let chain_genesis = ChainGenesis {
        time: genesis_time,
        height: 0,
        gas_limit: 1_000_000,
        min_gas_price: 100,
        max_gas_price: 1_000_000_000,
        total_supply: 3_000_000_000_000_000_000_000_000_000_000_000,
        gas_price_adjustment_rate: Ratio::from_integer(0),
        transaction_validity_period,
        epoch_length,
        protocol_version: PROTOCOL_VERSION,
        chain_id: "integration_test".to_string(),
    };

    let signer = MutableConfigValue::new(
        Some(Arc::new(create_test_signer(account_id.as_str()))),
        "validator_signer",
    );
    let telemetry = ActixWrapper::new(TelemetryActor::default()).start();
    let config = {
        let mut base = ClientConfig::test(
            skip_sync_wait,
            min_block_prod_time,
            max_block_prod_time,
            num_validator_seats,
            archive,
            true,
            state_sync_enabled,
        );
        base.chunk_distribution_network = chunk_distribution_config;
        base
    };

    let adv = Controls::default();

    let view_client_addr = ViewClientActorInner::spawn_actix_actor(
        clock.clone(),
        signer.clone(),
        chain_genesis.clone(),
        epoch_manager.clone(),
        shard_tracker.clone(),
        runtime.clone(),
        network_adapter.clone(),
        config.clone(),
        adv.clone(),
    );

    let client_adapter_for_partial_witness_actor = LateBoundSender::new();
    let (partial_witness_addr, _) = spawn_actix_actor(PartialWitnessActor::new(
        clock.clone(),
        network_adapter.clone(),
        client_adapter_for_partial_witness_actor.as_multi_sender(),
        signer.clone(),
        epoch_manager.clone(),
        runtime.clone(),
        Arc::new(RayonAsyncComputationSpawner),
        Arc::new(RayonAsyncComputationSpawner),
    ));
    let partial_witness_adapter = partial_witness_addr.with_auto_span_context();

    let (resharding_sender_addr, _) = spawn_actix_actor(ReshardingActor::new(
        epoch_manager.clone(),
        runtime.clone(),
        ReshardingHandle::new(),
        config.resharding_config.clone(),
    ));
    let resharding_sender = resharding_sender_addr.with_auto_span_context();

    let shards_manager_adapter_for_client = LateBoundSender::new();
    let StartClientResult { client_actor, tx_pool, chunk_endorsement_tracker, .. } = start_client(
        clock,
        config.clone(),
        chain_genesis,
        epoch_manager.clone(),
        shard_tracker.clone(),
        runtime.clone(),
        PeerId::new(PublicKey::empty(KeyType::ED25519)),
        Arc::new(ActixFutureSpawner),
        network_adapter.clone(),
        shards_manager_adapter_for_client.as_sender(),
        signer.clone(),
        telemetry.with_auto_span_context().into_sender(),
        None,
        None,
        adv,
        None,
        partial_witness_adapter.clone().into_multi_sender(),
        enable_doomslug,
        Some(TEST_SEED),
        resharding_sender.into_multi_sender(),
    );

    let rpc_handler_config = RpcHandlerConfig {
        handler_threads: config.transaction_request_handler_threads,
        tx_routing_height_horizon: config.tx_routing_height_horizon,
        epoch_length: config.epoch_length,
        transaction_validity_period,
    };

    let rpc_handler_addr = spawn_rpc_handler_actor(
        rpc_handler_config,
        tx_pool,
        chunk_endorsement_tracker,
        epoch_manager.clone(),
        shard_tracker.clone(),
        signer,
        runtime,
        network_adapter.clone(),
    );

    let validator_signer = Some(Arc::new(EmptyValidatorSigner::new(account_id)));
    let (shards_manager_addr, _) = start_shards_manager(
        epoch_manager.clone(),
        epoch_manager,
        shard_tracker,
        network_adapter.into_sender(),
        client_actor.clone().with_auto_span_context().into_sender(),
        MutableConfigValue::new(validator_signer, "validator_signer"),
        store,
        config.chunk_request_retry_period,
    );
    let shards_manager_adapter = shards_manager_addr.with_auto_span_context();
    shards_manager_adapter_for_client.bind(shards_manager_adapter.clone());

    client_adapter_for_partial_witness_actor.bind(client_actor.clone().with_auto_span_context());

    (
        client_actor,
        view_client_addr,
        rpc_handler_addr,
        shards_manager_adapter.into_multi_sender(),
        partial_witness_adapter.into_multi_sender(),
    )
}

/// Sets up ClientActor and ViewClientActor with mock PeerManager.
pub fn setup_mock(
    clock: Clock,
    validators: Vec<AccountId>,
    account_id: AccountId,
    skip_sync_wait: bool,
    enable_doomslug: bool,
    peer_manager_mock: Box<
        dyn FnMut(
            &PeerManagerMessageRequest,
            &mut Context<PeerManagerMock>,
            Addr<ClientActor>,
            Addr<RpcHandlerActor>,
        ) -> PeerManagerMessageResponse,
    >,
) -> ActorHandlesForTesting {
    setup_mock_with_validity_period(
        clock,
        validators,
        account_id,
        skip_sync_wait,
        enable_doomslug,
        peer_manager_mock,
        100,
    )
}

pub fn setup_mock_with_validity_period(
    clock: Clock,
    validators: Vec<AccountId>,
    account_id: AccountId,
    skip_sync_wait: bool,
    enable_doomslug: bool,
    mut peermanager_mock: Box<
        dyn FnMut(
            &PeerManagerMessageRequest,
            &mut Context<PeerManagerMock>,
            Addr<ClientActor>,
            Addr<RpcHandlerActor>,
        ) -> PeerManagerMessageResponse,
    >,
    transaction_validity_period: NumBlocks,
) -> ActorHandlesForTesting {
    let network_adapter = LateBoundSender::new();
    let (
        (
            client_addr,
            view_client_addr,
            rpc_handler_addr,
            shards_manager_adapter,
            partial_witness_sender,
        ),
        runtime_tempdir,
    ) = setup_with_real_epoch_manager(
        clock.clone(),
        validators,
        10,
        account_id,
        skip_sync_wait,
        MIN_BLOCK_PROD_TIME.whole_milliseconds() as u64,
        MAX_BLOCK_PROD_TIME.whole_milliseconds() as u64,
        enable_doomslug,
        false,
        true,
        network_adapter.as_multi_sender(),
        transaction_validity_period,
        clock.now_utc(),
        None,
    );
    let client_addr1 = client_addr.clone();
    let rpc_handler_addr1 = rpc_handler_addr.clone();

    let network_actor = PeerManagerMock::new(move |msg, ctx| {
        peermanager_mock(&msg, ctx, client_addr1.clone(), rpc_handler_addr1.clone())
    })
    .start();

    network_adapter.bind(network_actor);

    ActorHandlesForTesting {
        client_actor: client_addr,
        view_client_actor: view_client_addr,
        rpc_handler_actor: rpc_handler_addr,
        shards_manager_adapter,
        partial_witness_sender,
        runtime_tempdir: Some(runtime_tempdir.into()),
    }
}

#[derive(Clone)]
pub struct ActorHandlesForTesting {
    pub client_actor: Addr<ClientActor>,
    pub view_client_actor: Addr<ViewClientActor>,
    pub rpc_handler_actor: Addr<RpcHandlerActor>,
    pub shards_manager_adapter: ShardsManagerAdapterForTest,
    pub partial_witness_sender: PartialWitnessSenderForNetwork,
    // If testing something with runtime that needs runtime home dir users should make sure that
    // this TempDir isn't dropped before test finishes, but is dropped after to avoid leaking temp
    // dirs.
    pub runtime_tempdir: Option<Arc<tempfile::TempDir>>,
}

fn send_chunks<T, I, F>(
    connectors: &[ActorHandlesForTesting],
    recipients: I,
    target: T,
    drop_chunks: bool,
    send_to: F,
) where
    T: Eq,
    I: Iterator<Item = (usize, T)>,
    F: Fn(&ShardsManagerAdapterForTest),
{
    for (i, name) in recipients {
        if name == target {
            if !drop_chunks || !thread_rng().gen_ratio(1, 5) {
                send_to(&connectors[i].shards_manager_adapter);
            }
        }
    }
}

/// Helper to ensure default processing of network message `msg`.
/// See comments for `setup_mock_all_validators` for argument definitions.
fn process_peer_manager_message_default(
    msg: PeerManagerMessageRequest,
    drop_chunks: bool,
    tamper_with_fg: bool,
    check_block_stats: bool,
    account_id: AccountId,
    validators: Vec<AccountId>,
    key_pairs: Vec<PeerInfo>,
    addresses: Vec<CryptoHash>,
    last_height: Arc<RwLock<Vec<u64>>>,
    block_stats: Arc<RwLock<BlockStats>>,
    announced_accounts: Arc<RwLock<HashSet<(AccountId, EpochId)>>>,
    largest_endorsed_height: Arc<RwLock<Vec<u64>>>,
    largest_skipped_height: Arc<RwLock<Vec<u64>>>,
    // Maps block hashes to heights. May not include genesis block.
    hash_to_height: Arc<RwLock<HashMap<CryptoHash, u64>>>,
    client_sender: &LateBoundSender<Addr<ActixWrapper<ClientActorInner>>>,
    connectors: &[ActorHandlesForTesting],
) {
    let my_ord = validators.iter().position(|it| it == &account_id).unwrap();
    let my_key_pair = key_pairs[my_ord].clone();
    let my_address = addresses[my_ord];

    {
        let last_height = last_height.read();
        let peers: Vec<_> = key_pairs
            .iter()
            .take(connectors.len())
            .enumerate()
            .map(|(i, peer_info)| ConnectedPeerInfo {
                full_peer_info: FullPeerInfo {
                    peer_info: peer_info.clone(),
                    chain_info: PeerChainInfo {
                        genesis_id: GenesisId {
                            chain_id: "unittest".to_string(),
                            hash: Default::default(),
                        },
                        // TODO: add the correct hash here
                        last_block: Some(BlockInfo {
                            height: last_height[i],
                            hash: CryptoHash::default(),
                        }),
                        tracked_shards: vec![0, 1, 2, 3].into_iter().map(ShardId::new).collect(),
                        archival: true,
                    },
                },
                received_bytes_per_sec: 0,
                sent_bytes_per_sec: 0,
                last_time_peer_requested: near_async::time::Instant::now(),
                last_time_received_message: near_async::time::Instant::now(),
                connection_established_time: near_async::time::Instant::now(),
                peer_type: PeerType::Outbound,
                nonce: 3,
            })
            .collect();
        let peers2 = peers.iter().filter_map(|it| it.full_peer_info.clone().into()).collect();
        let info = NetworkInfo {
            connected_peers: peers,
            tier1_connections: vec![],
            num_connected_peers: key_pairs.len(),
            peer_max_count: key_pairs.len() as u32,
            highest_height_peers: peers2,
            sent_bytes_per_sec: 0,
            received_bytes_per_sec: 0,
            known_producers: vec![],
            tier1_accounts_keys: vec![],
            tier1_accounts_data: vec![],
        };
        client_sender.send(SetNetworkInfo(info).with_span_context());
    }

    match msg.as_network_requests_ref() {
        NetworkRequests::Block { block } => {
            if check_block_stats {
                let block_stats2 = &mut *block_stats.write();
                block_stats2.add_block(block);
                block_stats2.check_stats(false);
            }

            for actor_handles in connectors {
                actor_handles.client_actor.do_send(
                    BlockResponse {
                        block: block.clone(),
                        peer_id: PeerInfo::random().id,
                        was_requested: false,
                    }
                    .with_span_context(),
                );
            }

            let mut last_height1 = last_height.write();

            let my_height = &mut last_height1[my_ord];

            *my_height = max(*my_height, block.header().height());

            hash_to_height.write().insert(*block.header().hash(), block.header().height());
        }
        NetworkRequests::OptimisticBlock { chunk_producers, optimistic_block } => {
            // TODO(#10584): maybe go through an adapter to facilitate testing.
            for (i, name) in validators.iter().enumerate() {
                if !chunk_producers.contains(name) {
                    continue;
                }
                let msg = OptimisticBlockMessage {
                    optimistic_block: optimistic_block.clone(),
                    from_peer: PeerInfo::random().id,
                };
                connectors[i].client_actor.do_send(msg.with_span_context());
            }
        }
        NetworkRequests::PartialEncodedChunkRequest { target, request, .. } => {
            send_chunks(
                connectors,
                validators.iter().map(|s| Some(s.clone())).enumerate(),
                target.account_id.as_ref().map(|s| s.clone()),
                drop_chunks,
                |c| {
                    c.send(ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkRequest {
                        partial_encoded_chunk_request: request.clone(),
                        route_back: my_address,
                    });
                },
            );
        }
        NetworkRequests::PartialEncodedChunkResponse { route_back, response } => {
            send_chunks(connectors, addresses.iter().enumerate(), route_back, drop_chunks, |c| {
                c.send(ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkResponse {
                    partial_encoded_chunk_response: response.clone(),
                    received_time: Instant::now(),
                });
            });
        }
        NetworkRequests::PartialEncodedChunkMessage { account_id, partial_encoded_chunk } => {
            send_chunks(
                connectors,
                validators.iter().cloned().enumerate(),
                account_id.clone(),
                drop_chunks,
                |c| {
                    c.send(ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunk(
                        partial_encoded_chunk.clone().into(),
                    ));
                },
            );
        }
        NetworkRequests::PartialEncodedChunkForward { account_id, forward } => {
            send_chunks(
                connectors,
                validators.iter().cloned().enumerate(),
                account_id.clone(),
                drop_chunks,
                |c| {
                    c.send(ShardsManagerRequestFromNetwork::ProcessPartialEncodedChunkForward(
                        forward.clone(),
                    ));
                },
            );
        }
        NetworkRequests::BlockRequest { hash, peer_id } => {
            for (i, peer_info) in key_pairs.iter().enumerate() {
                let peer_id = peer_id.clone();
                if peer_info.id == peer_id {
                    let me = connectors[my_ord].client_actor.clone();
                    actix::spawn(
                        connectors[i]
                            .view_client_actor
                            .send(BlockRequest(*hash).with_span_context())
                            .then(move |response| {
                                let response = response.unwrap();
                                match response {
                                    Some(block) => {
                                        me.do_send(
                                            BlockResponse {
                                                block: *block,
                                                peer_id,
                                                was_requested: true,
                                            }
                                            .with_span_context(),
                                        );
                                    }
                                    None => {}
                                }
                                future::ready(())
                            }),
                    );
                }
            }
        }
        NetworkRequests::BlockHeadersRequest { hashes, peer_id } => {
            for (i, peer_info) in key_pairs.iter().enumerate() {
                let peer_id = peer_id.clone();
                if peer_info.id == peer_id {
                    let me = connectors[my_ord].client_actor.clone();
                    actix::spawn(
                        connectors[i]
                            .view_client_actor
                            .send(BlockHeadersRequest(hashes.clone()).with_span_context())
                            .then(move |response| {
                                let response = response.unwrap();
                                match response {
                                    Some(headers) => {
                                        me.do_send(
                                            BlockHeadersResponse(headers, peer_id)
                                                .with_span_context(),
                                        );
                                    }
                                    None => {}
                                }
                                future::ready(())
                            }),
                    );
                }
            }
        }
        NetworkRequests::StateRequestHeader { shard_id, sync_hash, .. } => {
            for (i, peer_info) in key_pairs.iter().enumerate() {
                let peer_id = peer_info.id.clone();
                let me = connectors[my_ord].client_actor.clone();
                actix::spawn(
                    connectors[i]
                        .view_client_actor
                        .send(
                            StateRequestHeader { shard_id: *shard_id, sync_hash: *sync_hash }
                                .with_span_context(),
                        )
                        .then(move |response| {
                            let response = response.unwrap();
                            match response {
                                Some(response) => {
                                    me.do_send(
                                        StateResponseReceived {
                                            peer_id,
                                            state_response_info: response.0,
                                        }
                                        .with_span_context(),
                                    );
                                }
                                None => {}
                            }
                            future::ready(())
                        }),
                );
            }
        }
        NetworkRequests::StateRequestPart { shard_id, sync_hash, part_id, .. } => {
            for (i, _) in validators.iter().enumerate() {
                let me = connectors[my_ord].client_actor.clone();
                actix::spawn(
                    connectors[i]
                        .view_client_actor
                        .send(
                            StateRequestPart {
                                shard_id: *shard_id,
                                sync_hash: *sync_hash,
                                part_id: *part_id,
                            }
                            .with_span_context(),
                        )
                        .then(move |response| {
                            let response = response.unwrap();
                            match response {
                                Some(response) => {
                                    me.do_send(
                                        StateResponseReceived {
                                            peer_id: PeerId::random(),
                                            state_response_info: response.0,
                                        }
                                        .with_span_context(),
                                    );
                                }
                                None => {}
                            }
                            future::ready(())
                        }),
                );
            }
        }
        NetworkRequests::AnnounceAccount(announce_account) => {
            let mut aa = announced_accounts.write();
            let key = (announce_account.account_id.clone(), announce_account.epoch_id);
            if aa.get(&key).is_none() {
                aa.insert(key);
                for actor_handles in connectors {
                    actor_handles.view_client_actor.do_send(
                        AnnounceAccountRequest(vec![(announce_account.clone(), None)])
                            .with_span_context(),
                    )
                }
            }
        }
        NetworkRequests::Approval { approval_message } => {
            let height_mod = approval_message.approval.target_height % 300;

            let do_propagate = if tamper_with_fg {
                if height_mod < 100 {
                    false
                } else if height_mod < 200 {
                    let mut rng = rand::thread_rng();
                    rng.r#gen()
                } else {
                    true
                }
            } else {
                true
            };

            let approval = approval_message.approval.clone();

            if do_propagate {
                for (i, name) in validators.iter().enumerate() {
                    if name == &approval_message.target {
                        connectors[i].client_actor.do_send(
                            BlockApproval(approval.clone(), my_key_pair.id.clone())
                                .with_span_context(),
                        );
                    }
                }
            }

            // Verify doomslug invariant
            match approval.inner {
                ApprovalInner::Endorsement(parent_hash) => {
                    assert!(approval.target_height > largest_skipped_height.read()[my_ord]);
                    largest_endorsed_height.write()[my_ord] = approval.target_height;

                    if let Some(prev_height) = hash_to_height.read().get(&parent_hash) {
                        assert_eq!(prev_height + 1, approval.target_height);
                    }
                }
                ApprovalInner::Skip(prev_height) => {
                    largest_skipped_height.write()[my_ord] = approval.target_height;
                    let e = largest_endorsed_height.read()[my_ord];
                    // `e` is the *target* height of the last endorsement. `prev_height`
                    // is allowed to be anything >= to the source height, which is e-1.
                    assert!(
                        prev_height + 1 >= e,
                        "New: {}->{}, Old: {}->{}",
                        prev_height,
                        approval.target_height,
                        e - 1,
                        e
                    );
                }
            };
        }
        NetworkRequests::ChunkEndorsement(account, endorsement) => {
            for (i, name) in validators.iter().enumerate() {
                if name == account {
                    connectors[i]
                        .rpc_handler_actor
                        .do_send(ChunkEndorsementMessage(endorsement.clone()).with_span_context());
                }
            }
        }
        NetworkRequests::PartialEncodedStateWitness(partial_witnesses) => {
            for (account, partial_witness) in partial_witnesses {
                for (i, name) in validators.iter().enumerate() {
                    if name == account {
                        connectors[i]
                            .partial_witness_sender
                            .send(PartialEncodedStateWitnessMessage(partial_witness.clone()));
                    }
                }
            }
        }
        NetworkRequests::PartialEncodedStateWitnessForward(accounts, partial_witness) => {
            for account in accounts {
                for (i, name) in validators.iter().enumerate() {
                    if name == account {
                        connectors[i].partial_witness_sender.send(
                            PartialEncodedStateWitnessForwardMessage(partial_witness.clone()),
                        );
                    }
                }
            }
        }
        NetworkRequests::ChunkContractAccesses(accounts, accesses) => {
            for account in accounts {
                for (i, name) in validators.iter().enumerate() {
                    if name == account {
                        connectors[i]
                            .partial_witness_sender
                            .send(ChunkContractAccessesMessage(accesses.clone()));
                    }
                }
            }
        }
        NetworkRequests::ContractCodeRequest(account, request) => {
            for (i, name) in validators.iter().enumerate() {
                if name == account {
                    connectors[i]
                        .partial_witness_sender
                        .send(ContractCodeRequestMessage(request.clone()));
                }
            }
        }
        NetworkRequests::ContractCodeResponse(account, response) => {
            for (i, name) in validators.iter().enumerate() {
                if name == account {
                    connectors[i]
                        .partial_witness_sender
                        .send(ContractCodeResponseMessage(response.clone()));
                }
            }
        }
        NetworkRequests::PartialEncodedContractDeploys(accounts, deploys) => {
            for account in accounts {
                for (i, name) in validators.iter().enumerate() {
                    if name == account {
                        connectors[i]
                            .partial_witness_sender
                            .send(PartialEncodedContractDeploysMessage(deploys.clone()));
                    }
                }
            }
        }
        NetworkRequests::ForwardTx(_, _)
        | NetworkRequests::BanPeer { .. }
        | NetworkRequests::TxStatus(_, _, _)
        | NetworkRequests::SnapshotHostInfo { .. }
        | NetworkRequests::ChunkStateWitnessAck(_, _)
        | NetworkRequests::EpochSyncRequest { .. }
        | NetworkRequests::EpochSyncResponse { .. } => {}
    }
}

/// Setup multiple clients talking to each other via a mock network.
///
/// # Arguments
///
/// `vs` - the set of validators and how they are assigned to shards in different epochs.
///
/// `key_pairs` - keys for `validators`
///
/// `skip_sync_wait`
///
/// `block_prod_time` - Minimum block production time, assuming there is enough approvals. The
///                 maximum block production time depends on the value of `tamper_with_fg`, and is
///                 equal to `block_prod_time` if `tamper_with_fg` is `true`, otherwise it is
///                 `block_prod_time * 2`
///
/// `drop_chunks` - if set to true, 10% of all the chunk messages / requests will be dropped
///
/// `tamper_with_fg` - if set to true, will split the heights into groups of 100. For some groups
///                 all the approvals will be dropped (thus completely disabling the finality gadget
///                 and introducing large number of forks if `block_prod_time` is sufficiently small),
///                 for some groups will keep all the approvals (and test the fg invariants), and
///                 for some will drop 50% of the approvals.
///                 This was designed to tamper with the finality gadget when we
///                 had it, unclear if has much effect today. Must be disabled if doomslug is
///                 enabled (see below), because doomslug will stall if approvals are not delivered.
///
/// `epoch_length` - approximate length of the epoch as measured
///                 by the block heights difference of it's last and first block.
///
/// `enable_doomslug` - If false, blocks will be created when at least one approval is present, without
///                   waiting for 2/3. This allows for more forks. `cross_shard_tx` has modes
///                   both with enabled doomslug (to test "production" setting) and with disabled
///                   doomslug (to test higher fork fullness)
///
/// `peer_manager_mock` - the callback that is called for each message sent. Called before
///                 the default processing. `mock` returns `(response, perform_default)`. If
///                 `perform_default` is false, then the message is not processed or broadcasted
///                 further and `response` is returned to the requester immediately. Otherwise
///                 the default action is performed, that might (and likely will) overwrite the
///                 `response` before it is sent back to the requester.
pub fn setup_mock_all_validators(
    clock: Clock,
    vs: ValidatorSchedule,
    key_pairs: Vec<PeerInfo>,
    skip_sync_wait: bool,
    block_prod_time: u64,
    drop_chunks: bool,
    tamper_with_fg: bool,
    epoch_length: BlockHeightDelta,
    enable_doomslug: bool,
    archive: Vec<bool>,
    check_block_stats: bool,
    chunk_distribution_config: Option<ChunkDistributionNetworkConfig>,
    peer_manager_mock: Box<
        dyn FnMut(
            // Peer validators
            &[ActorHandlesForTesting],
            // Validator that sends the message
            AccountId,
            // The message itself
            &PeerManagerMessageRequest,
        ) -> (PeerManagerMessageResponse, /* perform default */ bool),
    >,
) -> (Vec<ActorHandlesForTesting>, Arc<RwLock<BlockStats>>) {
    let peer_manager_mock = Arc::new(RwLock::new(peer_manager_mock));
    let validators = vs.all_validators().cloned().collect::<Vec<_>>();
    let addresses: Vec<_> = (0..key_pairs.len()).map(|i| hash(vec![i as u8].as_ref())).collect();
    let genesis_time = clock.now_utc();
    let mut ret = vec![];

    let connectors: Arc<OnceLock<Vec<ActorHandlesForTesting>>> = Default::default();

    let announced_accounts = Arc::new(RwLock::new(HashSet::new()));

    let last_height = Arc::new(RwLock::new(vec![0; key_pairs.len()]));
    let largest_endorsed_height = Arc::new(RwLock::new(vec![0u64; key_pairs.len()]));
    let largest_skipped_height = Arc::new(RwLock::new(vec![0u64; key_pairs.len()]));
    let hash_to_height = Arc::new(RwLock::new(HashMap::new()));
    let block_stats = Arc::new(RwLock::new(BlockStats::new(clock.clone())));

    for (index, account_id) in validators.clone().into_iter().enumerate() {
        let account_id1 = account_id.clone();
        let vs = vs.clone();
        let block_stats1 = block_stats.clone();
        let validators_clone2 = validators.clone();
        let key_pairs = key_pairs.clone();
        let addresses = addresses.clone();
        let connectors1 = connectors.clone();
        let network_mock1 = peer_manager_mock.clone();
        let announced_accounts1 = announced_accounts.clone();
        let last_height = last_height.clone();
        let largest_endorsed_height1 = largest_endorsed_height.clone();
        let largest_skipped_height1 = largest_skipped_height.clone();
        let hash_to_height1 = hash_to_height.clone();
        let archive1 = archive.clone();
        let chunk_distribution_config1 = chunk_distribution_config.clone();
        let client_sender = LateBoundSender::new();
        let client_sender1 = client_sender.clone();
        let pm = PeerManagerMock::new(move |msg, _ctx| {
            // Note: this `.wait` will block until all `ClientActors` are created.
            let connectors1 = connectors1.wait();
            let mut guard = network_mock1.write();
            let (resp, perform_default) =
                guard.deref_mut()(connectors1.as_slice(), account_id1.clone(), &msg);
            drop(guard);

            if perform_default {
                process_peer_manager_message_default(
                    msg,
                    drop_chunks,
                    tamper_with_fg,
                    check_block_stats,
                    account_id1.clone(),
                    validators_clone2.clone(),
                    key_pairs.clone(),
                    addresses.clone(),
                    last_height.clone(),
                    block_stats1.clone(),
                    announced_accounts1.clone(),
                    largest_endorsed_height1.clone(),
                    largest_skipped_height1.clone(),
                    hash_to_height1.clone(),
                    client_sender1.as_ref(),
                    connectors1.as_slice(),
                );
            }
            resp
        })
        .start();

        let (
            client_addr,
            view_client_addr,
            rpc_handler_addr,
            shards_manager_adapter,
            partial_witness_sender,
        ) = setup_with_mock_epoch_manager(
            clock.clone(),
            vs,
            epoch_length,
            account_id.clone(),
            skip_sync_wait,
            block_prod_time,
            block_prod_time * 3,
            enable_doomslug,
            archive1[index],
            false,
            pm.into_multi_sender(),
            10000,
            genesis_time,
            // ctx,
            chunk_distribution_config1,
        );
        client_sender.bind(client_addr.clone());

        ret.push(ActorHandlesForTesting {
            client_actor: client_addr,
            view_client_actor: view_client_addr,
            rpc_handler_actor: rpc_handler_addr,
            shards_manager_adapter,
            partial_witness_sender,
            runtime_tempdir: None,
        });
    }
    hash_to_height.write().insert(CryptoHash::default(), 0);
    connectors.set(ret.clone()).ok().unwrap();
    (ret, block_stats)
}

/// Sets up ClientActor and ViewClientActor without network.
pub fn setup_no_network(
    clock: Clock,
    validators: Vec<AccountId>,
    account_id: AccountId,
    skip_sync_wait: bool,
    enable_doomslug: bool,
) -> ActorHandlesForTesting {
    setup_no_network_with_validity_period(
        clock,
        validators,
        account_id,
        skip_sync_wait,
        100,
        enable_doomslug,
    )
}

pub fn setup_no_network_with_validity_period(
    clock: Clock,
    validators: Vec<AccountId>,
    account_id: AccountId,
    skip_sync_wait: bool,
    transaction_validity_period: NumBlocks,
    enable_doomslug: bool,
) -> ActorHandlesForTesting {
    let my_account_id = account_id.clone();
    setup_mock_with_validity_period(
        clock,
        validators,
        account_id,
        skip_sync_wait,
        enable_doomslug,
        Box::new(move |request, _, _client, rpc_handler| {
            // Handle network layer sending messages to self
            match request {
                PeerManagerMessageRequest::NetworkRequests(NetworkRequests::ChunkEndorsement(
                    account_id,
                    endorsement,
                )) => {
                    if account_id == &my_account_id {
                        let future = rpc_handler.send_async(
                            ChunkEndorsementMessage(endorsement.clone()).with_span_context(),
                        );
                        // Don't ignore the future or else the message may not actually be handled.
                        actix::spawn(future);
                    }
                }
                _ => {}
            };
            PeerManagerMessageResponse::NetworkResponses(NetworkResponses::NoResponse)
        }),
        transaction_validity_period,
    )
}

pub fn setup_client_with_runtime(
    clock: Clock,
    num_validator_seats: NumSeats,
    enable_doomslug: bool,
    network_adapter: PeerManagerAdapter,
    shards_manager_adapter: SynchronousShardsManagerAdapter,
    chain_genesis: ChainGenesis,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    shard_tracker: ShardTracker,
    runtime: Arc<dyn RuntimeAdapter>,
    rng_seed: RngSeed,
    archive: bool,
    save_trie_changes: bool,
    snapshot_callbacks: Option<SnapshotCallbacks>,
    partial_witness_adapter: PartialWitnessSenderForClient,
    validator_signer: Arc<ValidatorSigner>,
    resharding_sender: ReshardingSender,
) -> Client {
    let mut config =
        ClientConfig::test(true, 10, 20, num_validator_seats, archive, save_trie_changes, true);
    config.epoch_length = chain_genesis.epoch_length;
    let protocol_upgrade_schedule = get_protocol_upgrade_schedule(&chain_genesis.chain_id);
    let mut client = Client::new(
        clock,
        config,
        chain_genesis,
        epoch_manager,
        shard_tracker,
        runtime,
        network_adapter,
        shards_manager_adapter.into_sender(),
        MutableConfigValue::new(Some(validator_signer), "validator_signer"),
        enable_doomslug,
        rng_seed,
        snapshot_callbacks,
        Arc::new(RayonAsyncComputationSpawner),
        partial_witness_adapter,
        resharding_sender,
        Arc::new(ActixFutureSpawner),
        noop().into_multi_sender(), // state sync ignored for these tests
        noop().into_multi_sender(), // apply chunks ping not necessary for these tests
        protocol_upgrade_schedule,
    )
    .unwrap();
    client.sync_handler.sync_status = SyncStatus::NoSync;
    client
}

pub fn setup_synchronous_shards_manager(
    clock: Clock,
    account_id: Option<AccountId>,
    client_adapter: Sender<ShardsManagerResponse>,
    network_adapter: PeerManagerAdapter,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    shard_tracker: ShardTracker,
    runtime: Arc<dyn RuntimeAdapter>,
    chain_genesis: &ChainGenesis,
) -> SynchronousShardsManagerAdapter {
    // Initialize the chain, to make sure that if the store is empty, we write the genesis
    // into the store, and as a short cut to get the parameters needed to instantiate
    // ShardsManager. This way we don't have to wait to construct the Client first.
    // TODO(#8324): This should just be refactored so that we can construct Chain first
    // before anything else.
    let chunk_store = runtime.store().chunk_store();
    let chain = Chain::new(
        clock.clone(),
        epoch_manager.clone(),
        shard_tracker.clone(),
        runtime,
        chain_genesis,
        DoomslugThresholdMode::TwoThirds, // irrelevant
        ChainConfig {
            save_trie_changes: true,
            background_migration_threads: 1,
            resharding_config: MutableConfigValue::new(
                ReshardingConfig::default(),
                "resharding_config",
            ),
        }, // irrelevant
        None,
        Arc::new(RayonAsyncComputationSpawner),
        MutableConfigValue::new(None, "validator_signer"),
        noop().into_multi_sender(),
    )
    .unwrap();
    let chain_head = chain.head().unwrap();
    let chain_header_head = chain.header_head().unwrap();
    let validator_signer = account_id.map(|id| Arc::new(EmptyValidatorSigner::new(id)));
    let shards_manager = ShardsManagerActor::new(
        clock,
        MutableConfigValue::new(validator_signer, "validator_signer"),
        epoch_manager.clone(),
        epoch_manager,
        shard_tracker,
        network_adapter.request_sender,
        client_adapter,
        chunk_store,
        chain_head,
        chain_header_head,
        Duration::hours(1),
    );
    SynchronousShardsManagerAdapter::new(shards_manager)
}

pub fn setup_tx_request_handler(
    chain_genesis: ChainGenesis,
    client: &Client,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    shard_tracker: ShardTracker,
    runtime: Arc<dyn RuntimeAdapter>,
    network_adapter: PeerManagerAdapter,
) -> RpcHandler {
    let client_config = ClientConfig::test(true, 10, 20, 0, true, true, true);
    let config = RpcHandlerConfig {
        handler_threads: 1,
        tx_routing_height_horizon: client_config.tx_routing_height_horizon,
        epoch_length: chain_genesis.epoch_length,
        transaction_validity_period: chain_genesis.transaction_validity_period,
    };

    RpcHandler::new(
        config,
        client.chunk_producer.sharded_tx_pool.clone(),
        client.chunk_endorsement_tracker.clone(),
        epoch_manager,
        shard_tracker,
        client.validator_signer.clone(),
        runtime,
        network_adapter,
    )
}

/// A multi-sender for both the client and network parts of the ShardsManager API.
#[derive(Clone, near_async::MultiSend, near_async::MultiSenderFrom)]
pub struct ShardsManagerAdapterForTest {
    pub client: Sender<ShardsManagerRequestFromClient>,
    pub network: Sender<ShardsManagerRequestFromNetwork>,
}
