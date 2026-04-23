# State sync under SPICE

This document describes how state sync works under SPICE (Separation of
Execution from Consensus). Read [sync.md](sync.md) first for the non-SPICE
baseline; this is an addendum, not a replacement.

## What changes, and why

Under non-SPICE, a chunk header carries `prev_state_root` — the state root
the chunk was produced against. State sync uses this field as its source of
truth: the downloaded `state_root_node` must match
`B_prev.chunks[shard_id].prev_state_root`.

Under SPICE, chunk headers do not carry state roots. Execution happens
asynchronously and produces a `ChunkExecutionResult` (wrapping a `ChunkExtra`
with the real `state_root`), which reaches chain as
`SpiceCoreStatement::ChunkExecutionResult` in some later block's body — once
enough validator endorsements have aggregated.

### Design principle

> The sync target state root comes from the on-chain certified execution
> result, not the chunk header.

Being "on chain as a `ChunkExecutionResult`" already implies validator-quorum
certification: block producers only include the statement once enough
endorsements are visible (`spice_core.rs:415-432`). Consensus finality on
`sync_hash` is still required for safety, same as non-SPICE. But the block
that *carries* the `SpiceCoreStatement::ChunkExecutionResult` does **not**
itself need to be consensus-final — the endorsement quorum is self-certifying,
and the receiver verifies it directly (see seam 3 below).

## The four seams

State sync diverges from the non-SPICE path in exactly four places:

```
1. sync_hash selection       │  chain/chain/src/state_sync/utils.rs
2. State root discovery      │  chain/chain/src/state_sync/adapter.rs
                             │  chain/client/src/sync/state/
3. State header validation   │  chain/chain/src/state_sync/adapter.rs
4. Post-sync catch-up        │  chain/chain/src/chain.rs
                             │  chain/client/src/chunk_executor_actor.rs
```

The rest of this document walks each seam.

---

## Seam 1: sync_hash selection

**Non-SPICE rule** (`state_sync/utils.rs:139`, `on_new_header`): the sync_hash
is the first consensus-final block B such that B_prev has ≥ 2 new chunks per
shard and B_prev_prev does not. When this rule is first satisfied,
`DBCol::StateSyncHashes[epoch_id] = sync.hash()` is written.

**SPICE addition**: before writing `StateSyncHashes`, also require that the
on-chain execution results for every shard of `sync_prev_block` are present.
Concretely:

```rust
// pseudocode, before store_update.set_ser(DBCol::StateSyncHashes, ...)
if cfg!(feature = "protocol_feature_spice") && spice_enabled {
    if spice_core_reader.get_block_execution_results(sync_prev_block)?.is_none() {
        // not ready; do not commit. The loop retries on the next header.
        continue;
    }
}
```

No invalidation is needed: `StateSyncNewChunks` persists across headers until
finalization actually writes `StateSyncHashes`. Callers of `get_sync_hash`
already tolerate `Ok(None)` (`sync/handler.rs:185`, `client.rs:2300`,
`chain.rs:3949`), so delayed finalization is safe.

**Rationale for wait-on-serving-side**: the alternative — publish the
sync_hash eagerly and have every downloading node wait for execution results
independently — surfaces the sync_hash slightly faster but duplicates the
wait on every client and complicates the "is this sync_hash usable" contract.
Per the execution-lag bound established by PR #15600 (one epoch), the wait
here is bounded.

---

## Seam 2: state root discovery

### The V3 header

Under non-SPICE, `ShardStateSyncResponseHeaderV2`
(`core/primitives/src/state_sync.rs:93`) ties `state_root_node` to
`chunk.prev_state_root`, which the downloader reads at
`chain/client/src/sync/state/shard.rs:79`:

```rust
let state_root = header.chunk_prev_state_root();
```

Under SPICE, `chunk.prev_state_root` is the zero hash; the real state root
is in the execution result. Propose a new variant:

```rust
pub struct ShardStateSyncResponseHeaderV3 {
    // all V2 fields: chunk, chunk_proof, prev_chunk_header, prev_chunk_proof,
    // incoming_receipts_proofs, root_proofs, state_root_node.

    /// On-chain certified execution result for (B_prev_chunk, shard_id).
    /// state_root_node is keyed on execution_result.chunk_extra.state_root,
    /// not chunk.prev_state_root (which is meaningless under SPICE).
    pub spice_execution_result: ChunkExecutionResult,

    /// Validator signatures over execution_result.compute_hash(), sufficient
    /// for a stake-weighted endorsement quorum. Drawn from
    /// get_chunk_validator_assignments(B_prev_chunk.epoch, shard_id,
    /// B_prev_chunk.height).
    pub spice_endorsement_signatures: Vec<(AccountId, Signature)>,
}
```

The `state_root_node` field keeps its non-SPICE semantics — state *at
B_prev_chunk*, before applying `chunk`. The target the downloader reads
becomes `spice_execution_result.chunk_extra.state_root`.

### Why anchor on B_prev_chunk (not B_prev)

The serving-side builder at
`chain/chain/src/state_sync/adapter.rs:220-224` constructs
`state_root_node` from `chunk_header.prev_state_root()`, which is the state
*before* applying `chunk` — i.e., state at B_prev_chunk. The downloader
mirrors this: after parts are applied,
`create_flat_storage_for_shard` sets flat head to `chunk.prev_block()`
(`sync/state/shard.rs:290`), and `chunk` itself is applied locally as the
first post-sync step.

Keeping this anchor preserves the existing "sync to pre-chunk, then apply"
flow. Under SPICE that means we want
`execution_result(B_prev_chunk, shard_id).chunk_extra.state_root`.

### Why direct endorsement signatures, not merkle inclusion

`SpiceBlockBodyV3` stores `core_statements` as a flat `Vec<SpiceCoreStatement>`
(`core/primitives/src/block_body.rs:55-64`). The block header commits to the
body only via `block_body_hash = hash_borsh(BlockBody)`
(`block_header.rs:1180`). **There is no dedicated merkle commitment over core
statements.**

Three validation designs were considered:

| Design | Proof size | Protocol change? |
|---|---|---|
| Direct endorsement signatures | O(validators) | no |
| Whole-body inclusion proof | O(block body size) | no |
| New `core_statements_root` merkle commitment | O(log n) | yes |

Direct signatures win: self-contained (no block body needed), reuses the
existing endorsement-validation primitives, and scales with validator count
not block body size. The receiver has the epoch's validator set after
header sync, so `EpochManagerAdapter::get_chunk_validator_assignments` plus
`compute_endorsement_state` (already used in
`validate_core_statements_in_block`) gives exact parity with producer-side
certification.

---

## Seam 3: state header validation

**Non-SPICE** (`adapter.rs:513-524`): validator-side compares downloaded
`state_root_node` against `chunk_inner.prev_state_root()` pulled from the
chunk header merkle-proven against `B_prev.chunk_headers_root`.

**SPICE** receiver-side validation flow:

1. Verify `chunk_proof`, `prev_chunk_proof`, `root_proofs` as today — the
   chunk headers are still in the block body, so these proofs are still
   meaningful (they attest chunk-in-block, not state-root-anchor).
2. Compute `h = spice_execution_result.compute_hash()`.
3. Look up chunk validator assignments for
   `(B_prev_chunk.epoch_id, shard_id, B_prev_chunk.height)`.
4. Verify each `(account_id, signature)` in `spice_endorsement_signatures`
   against the validator's public key; feed into
   `compute_endorsement_state`.
5. Require `is_endorsed == true`.
6. Check `state_root_node.hash() ==
   spice_execution_result.chunk_extra.state_root`.

The "chunk headers attest chunk-in-block" role of the merkle proofs under
SPICE is worth calling out explicitly: they no longer prove anything about
the state root. That doesn't make them dead — they are still needed to tie
the header's `chunk` / `prev_chunk_header` back to blocks the receiver
trusts via header sync — but their semantic purpose narrows.

### Serving-side construction

`compute_state_response_header` (`adapter.rs:64-251`) is extended under SPICE:

- Additionally look up `SpiceCoreReader::get_execution_result(B_prev_chunk, shard_id)`.
- Additionally look up the endorsement signatures (see below).
- Emit `ShardStateSyncResponseHeaderV3` instead of V2.

**Endorsement signature availability**: endorsements persist in
`DBCol::endorsements` keyed by `(block_hash, shard_id, account_id)`
(`spice_core.rs:53-73`, via `get_endorsements_key`). A serving node that was
online when endorsements landed has them. This is fine for validators; RPC
nodes that joined after endorsement time may not, and will either need to
reconstruct from block bodies (walk blocks containing the
`SpiceCoreStatement::Endorsement` statements) or refuse to serve as a
state-sync provider for affected heights. Treat this as an implementation
detail to address during rollout.

---

## Seam 4: post-sync catch-up

### What the executor needs

`chunk_executor_actor.rs:395` (`try_apply_chunks`) gates execution on:

1. Block is descendant of `spice_final_execution_head` (line 403).
2. For each tracked shard: `prev_block`'s `ChunkExtra` exists (line 456).
3. Receipt proofs from `prev_block` for that shard exist (line 470).

So to have the executor resume at `sync_prev_block + 1` after state sync
finishes for shard `S`, we must:

- Write `ChunkExtra(sync_prev_block, shard_uid_S)` derived from
  `spice_execution_result.chunk_extra`.
- Ensure receipt proofs for `S` are in storage for `sync_prev_block`.
- Eventually advance `spice_final_execution_head` past `sync_prev_block`.

### `reset_heads_post_state_sync` under SPICE

`chain/chain/src/chain.rs:1572` today:

- `body_head` → `sync_prev_block`
- `final_head` → genesis
- `tail` / `chunk_tail` → updated

Under SPICE, additionally:

- For each newly-synced shard, write `ChunkExtra(sync_prev_block, shard_uid)`
  from the execution result carried in the header.
- If *all tracked shards* now have `ChunkExtra` at `sync_prev_block`,
  advance `spice_execution_head` and `spice_final_execution_head` to a Tip
  derived from `sync_prev_block`.

### Per-shard vs global `spice_execution_head`

`spice_execution_head` is **global** today
(`core/store/src/adapter/chain_store.rs:111`), a `Tip` at a single block.
Per-shard progress is encoded in `ChunkExtra(block, shard_uid)`. The
implication for state sync: we can write per-shard readiness as soon as
each shard's sync finishes, but the global cursor only advances when every
tracked shard at `sync_prev_block` has its `ChunkExtra`.

No change to the shape of `spice_execution_head` is proposed here — a
per-shard variant may be worth future work, but is not required to make
SPICE state sync correct.

---

## Resolved design questions

Four questions from the task scoping doc, with picks:

1. **Sync-prev-block selection timing** — wait for execution results before
   publishing sync_hash. Simpler and bounded by PR #15600.
2. **State header carries execution result vs chain lookup** — self-contained
   header with the certified `ChunkExecutionResult` + endorsement signatures.
   Decouples the receiver from needing any specific block body on chain.
3. **`reset_heads_post_state_sync` under SPICE** — advance
   `spice_execution_head` / `spice_final_execution_head` to `sync_prev_block`
   when all tracked shards are synced. Per-shard readiness via `ChunkExtra`.
4. **Non-executing nodes serving state sync** — yes. State parts come from
   flat storage + trie, which exist regardless of local execution.
   Certification attestation comes from `ChunkExecutionResult` which all
   nodes with the block see.

## Gating and rollout

- `ProtocolFeature::Spice.enabled` branches at each seam.
- `ShardStateSyncResponseHeader::V3` selected on the serving side when
  producing headers for SPICE epochs; V2 remains for non-SPICE.
- Downloader dispatches on the variant tag, not the protocol version — V3
  carries enough to self-describe its validation rules.

## Resharding interaction

If a node state-syncs a shard across a resharding boundary (e.g., begins
tracking a child shard), the target state root is the child's `ChunkExtra`
state root, which the executor writes at the resharding block. Children
have their own `SpiceCoreStatement::ChunkExecutionResult`, so the normal
execution-result lookup in seam 2 returns the right thing. No extra
plumbing is needed, provided the current SPICE+resharding work has landed.

## Slicing

Acceptance for each PR is framed around un-ignoring existing tests gated by
`#[cfg_attr(feature = "protocol_feature_spice", ignore)]`. Un-ignore in the
same PR that enables the test — not a separate cleanup pass. That way every
PR has a concrete pass/fail signal and any surprises surface as real test
failures, not as guesses.

Roughly in dependency order:

1. **sync_hash gating under SPICE** (seam 1). Small, testable in isolation
   by a unit test that verifies `get_sync_hash` returns `None` while
   execution results are absent and `Some` once they land. No existing
   tests flip.
2. **Header V3 definition + serving-side construction** (seams 2 & 3
   producer side). Schema + builder, no behavior change on downloader yet.
   Acceptance: borsh round-trip + protocol schema updates. No existing
   tests flip.
3. **Downloader uses V3's execution result for state_root**. One-liner
   equivalent at `shard.rs:79`. Un-ignore the simplest test in
   `sync/state_sync.rs` (the first one at line 159). If the minimum "state
   sync completes" scenario passes, the plumbing is end-to-end.
4. **Receiver-side validation using endorsement signatures** (seam 3
   receiver side). Medium; reuses `compute_endorsement_state`. Acceptance:
   PR 3's test still passes; add negative tests for forged endorsements.
5. **`reset_heads_post_state_sync` SPICE extension + `ChunkExtra` write**
   (seam 4). Largest piece; interacts with flat head and the executor's
   gate. Acceptance: un-ignore the bulk of `sync/state_sync.rs` (18 tests),
   `sync/state_sync_added_node.rs` (5), `sync/sync_then_catchup.rs` (2),
   `sync/syncing.rs` (1). ~26 tests flip. Sub-PRs may be needed for
   stragglers.
6. **Pipeline edges**. Un-ignore `sync/near_horizon.rs` (5),
   `sync/far_horizon.rs` (11), `sync/epoch_sync.rs` (8), `sync/gc.rs` (3),
   `sync/validator_kickout.rs` (2), `sync/continuous_epoch_sync.rs` (1).
   ~30 tests flip. These exercise state sync as part of larger flows and
   will surface any lingering catch-up or head-reset bugs.

Resharding-intersecting tests (`single_shard_tracking.rs` and the ~11
ignored `resharding_v3.rs` tests) are **out of scope for this work**.
They depend on state sync interacting with child-shard `ChunkExtra` across
the resharding boundary and are tracked as a follow-up.

Each un-ignore replaces `#[cfg_attr(feature = "protocol_feature_spice",
ignore)]` with no attribute (don't leave a vestigial `#[ignore]`). Verify
in both modes:

```
cargo nextest run --features test_features -p test-loop-tests \
  -E 'test(state_sync)'
cargo nextest run --features test_features,protocol_feature_spice \
  -p test-loop-tests -E 'test(state_sync)'
```

## Open gotchas

- **Global `spice_execution_head`** — state sync for a subset of tracked
  shards leaves the global cursor behind. Fine, but worth asserting in
  tests that partial state sync doesn't lock out execution on already-synced
  shards in unexpected ways.
- **Flat head + resharding TODO** (`chunk_executor_actor.rs:1006`) — flat
  head advancement interacts with state-sync-seeded flat state; revisit
  during seam 4 implementation.
- **Endorsement availability on RPC nodes** — nodes that were offline
  during endorsement aggregation may not have the raw signatures. Either
  reconstruct from block bodies or exclude those nodes as serving peers.
- **Execution-lag assumption** — the "wait for results before publishing
  sync_hash" bound relies on PR #15600's guarantee that execution lags by
  at most one epoch. If that bound weakens, sync_hash may never finalize
  within the window.

## Prerequisites

- **PR #15600** (execution-gated epoch transition): bounds how far execution
  can lag, which in turn bounds how long sync_hash waits for results.
- **Current SPICE+resharding work**: state sync into a child shard requires
  the executor to write child `ChunkExtra` at the resharding block.

## Tests

Strategy: drive correctness off the ~57 existing `sync/*` tests currently
gated by `#[cfg_attr(feature = "protocol_feature_spice", ignore)]`, not new
SPICE-specific scenarios. Hand-written tests would duplicate coverage and
bias toward paths we already understand; flipping the existing suite
surfaces the real gaps.

In-scope test files (un-ignored across PRs 3-6):

- `sync/state_sync.rs` (18)
- `sync/state_sync_added_node.rs` (5)
- `sync/sync_then_catchup.rs` (2)
- `sync/syncing.rs` (1)
- `sync/near_horizon.rs` (5)
- `sync/far_horizon.rs` (11)
- `sync/epoch_sync.rs` (8)
- `sync/gc.rs` (3)
- `sync/validator_kickout.rs` (2)
- `sync/continuous_epoch_sync.rs` (1)

Out of scope for this work (tracked as follow-up):

- `single_shard_tracking.rs` (1)
- `resharding_v3.rs` (~11) — state-sync-across-resharding; depends on
  child-shard `ChunkExtra` interactions.

Other SPICE-ignored tests (`yield_timeouts.rs`, `max_receipt_size.rs`,
`protocol_upgrade.rs`, `sharded_rpc.rs`, etc.) are ignored for reasons
unrelated to state sync and remain out of scope.
