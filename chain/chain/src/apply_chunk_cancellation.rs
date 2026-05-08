//! Cancellation signaling for in-flight `apply_chunk` jobs.
//!
//! When a memtrie root that an in-flight apply_chunk worker depends on is about
//! to be GC'd, the chain must tell the worker to bail with a recoverable error
//! instead of panicking on missing memtrie state. This module implements that
//! coordination as a registry of per-job cancellation flags, owned by `Chain`
//! (and a separate one by `ChunkExecutorActor` in SPICE mode), so that
//! `core/store` stays free of apply-job lifecycle state.
//!
//! ## Wiring
//!
//! 1. Just before scheduling a job, the chain calls
//!    [`ApplyChunkCancellationRegistry::register`] with
//!    `prev_block.header().height()` (i.e. the height at which the dep memtrie
//!    root was inserted, NOT the height of the block being applied; the two
//!    differ when heights are skipped).
//! 2. The returned [`ChunkApplicationCancellation`] is moved into the spawned
//!    closure and on into `apply_chunk`. The worker polls
//!    [`is_cancelled`](ChunkApplicationCancellation::is_cancelled).
//! 3. Just before pruning memtries, the chain calls
//!    [`cancel_up_to_height`](ApplyChunkCancellationRegistry::cancel_up_to_height) with the
//!    same height it will pass to `MemTries::delete_until_height`.
//! 4. When `apply_chunk` returns, the cancellation is dropped and its slot is
//!    removed from the registry.
//!
//! The cancel predicate is `prev_block_height < prune_height`, which mirrors
//! `MemTries::delete_until_height`'s own deletion predicate exactly. That
//! identical boundary is what makes this robust under skipped heights.

use near_primitives::shard_layout::ShardUId;
use near_primitives::types::BlockHeight;
use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Per-job cancellation handle for an in-flight `apply_chunk` worker.
///
/// Plumbed into `apply_chunk` as `Option<ChunkApplicationCancellation>`. The
/// chain flips it via [`ApplyChunkCancellationRegistry::cancel_up_to_height`] just
/// before pruning the memtrie root the worker depends on; the worker polls
/// [`is_cancelled`](Self::is_cancelled) and bails with a recoverable error.
///
/// On `Drop`, removes the corresponding registration slot from the registry,
/// so a single value carries both the polling state and the dereg-on-completion
/// lifetime.
pub struct ChunkApplicationCancellation {
    flag: Arc<AtomicBool>,
    shard_uid: ShardUId,
    prev_block_height: BlockHeight,
    registry: Arc<ApplyChunkCancellationRegistry>,
}

impl ChunkApplicationCancellation {
    pub fn is_cancelled(&self) -> bool {
        self.flag.load(Ordering::Acquire)
    }
}

impl Drop for ChunkApplicationCancellation {
    fn drop(&mut self) {
        let mut entries = self.registry.entries.lock();
        if let Some(pos) = entries.iter().position(|entry| {
            entry.shard_uid == self.shard_uid
                && entry.prev_block_height == self.prev_block_height
                && Arc::ptr_eq(&entry.flag, &self.flag)
        }) {
            entries.swap_remove(pos);
        }
    }
}

/// Registry of in-flight `apply_chunk` jobs that may need to be cancelled when
/// the memtrie root they depend on is pruned.
///
/// Owned by `Chain` (and separately by `ChunkExecutorActor` in SPICE mode).
/// Linear scan is fine — at any moment the registry holds at most a handful of
/// entries (bounded by `BlocksInProcessing` capacity times shard count).
pub struct ApplyChunkCancellationRegistry {
    entries: Mutex<Vec<Entry>>,
}

struct Entry {
    shard_uid: ShardUId,
    prev_block_height: BlockHeight,
    flag: Arc<AtomicBool>,
}

impl ApplyChunkCancellationRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self { entries: Mutex::new(Vec::new()) })
    }

    /// Register an in-flight job. The returned [`ChunkApplicationCancellation`]
    /// MUST live as long as the job — typically by being moved into the closure
    /// spawned on the apply pool and through into `apply_chunk`. On drop the
    /// slot is removed.
    ///
    /// `prev_block_height` is the height at which the memtrie root the job
    /// reads was inserted (i.e. `prev_block.header().height()`), NOT the
    /// height of the block being applied.
    pub fn register(
        self: &Arc<Self>,
        shard_uid: ShardUId,
        prev_block_height: BlockHeight,
    ) -> ChunkApplicationCancellation {
        let flag = Arc::new(AtomicBool::new(false));
        self.entries.lock().push(Entry { shard_uid, prev_block_height, flag: flag.clone() });
        ChunkApplicationCancellation { flag, shard_uid, prev_block_height, registry: self.clone() }
    }

    /// Signal cancellation to any in-flight job whose dep memtrie root is
    /// about to be pruned. Call BEFORE invoking
    /// `ShardTries::delete_memtrie_roots_up_to_height(shard_uid, prune_height)`
    /// so workers see the flag flip before the data they need disappears.
    pub fn cancel_up_to_height(&self, shard_uid: ShardUId, prune_height: BlockHeight) {
        let entries = self.entries.lock();
        for entry in entries.iter() {
            if entry.shard_uid == shard_uid && entry.prev_block_height < prune_height {
                entry.flag.store(true, Ordering::Release);
            }
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.entries.lock().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signals_only_jobs_whose_dep_root_is_pruned() {
        let registry = ApplyChunkCancellationRegistry::new();
        let shard_a = ShardUId { version: 1, shard_id: 0 };
        let shard_b = ShardUId { version: 1, shard_id: 1 };

        let prev10 = registry.register(shard_a, 10);
        let prev11 = registry.register(shard_a, 11);
        let prev12_b = registry.register(shard_b, 12);

        assert!(!prev10.is_cancelled());
        assert!(!prev11.is_cancelled());
        assert!(!prev12_b.is_cancelled());

        // Prune shard_a at 11: deletes roots inserted at heights `< 11`, i.e.
        // height 10. prev10 is cancelled; prev11 (root still alive) and
        // prev12_b (different shard) are not.
        registry.cancel_up_to_height(shard_a, 11);
        assert!(prev10.is_cancelled());
        assert!(!prev11.is_cancelled());
        assert!(!prev12_b.is_cancelled());

        registry.cancel_up_to_height(shard_a, 12);
        assert!(prev11.is_cancelled());
        assert!(!prev12_b.is_cancelled());

        // Skipped-height case: a fork block whose prev is at height 8 (heights
        // 9, 10 skipped). Pruning at 9 cancels it because `8 < 9`. Registering
        // by the block's own height instead would mis-key and miss the prune.
        let skipped_dep = registry.register(shard_a, 8);
        registry.cancel_up_to_height(shard_a, 9);
        assert!(skipped_dep.is_cancelled());

        // Distinct registrations at the same key see only their own flag.
        let prev12_a_fresh = registry.register(shard_a, 12);
        assert!(!prev12_a_fresh.is_cancelled());
        registry.cancel_up_to_height(shard_a, 13);
        assert!(prev12_a_fresh.is_cancelled());
        assert!(!prev12_b.is_cancelled(), "shard_b unaffected");

        drop(prev10);
        drop(prev11);
        drop(prev12_b);
        drop(skipped_dep);
        drop(prev12_a_fresh);
        assert_eq!(registry.len(), 0);
    }
}
