use near_async::futures::AsyncComputationSpawner;
use near_chain_primitives::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_primitives::block::Block;
use near_store::ShardTries;

pub fn maybe_start_memtrie_preload_for_resharding(
    epoch_manager: &dyn EpochManagerAdapter,
    shard_tracker: &ShardTracker,
    tries: &ShardTries,
    memtrie_loading_spawner: &dyn AsyncComputationSpawner,
    block: &Block,
) -> Result<(), Error> {
    let Some(parent_shard_uid) =
        epoch_manager.get_resharding_parent_shard_uid(block.header().epoch_id(), block.hash())?
    else {
        return Ok(());
    };

    if !shard_tracker.cares_about_shard_this_or_next_epoch(
        block.header().prev_hash(),
        parent_shard_uid.shard_id(),
    ) {
        return Ok(());
    }

    if tries.get_memtries(parent_shard_uid).is_some() {
        return Ok(());
    }

    tracing::info!(
        target: "memtrie",
        ?parent_shard_uid,
        "detected upcoming resharding, starting background memtrie load"
    );
    tries.spawn_background_memtrie_loading_for_shard(parent_shard_uid, memtrie_loading_spawner);
    Ok(())
}
