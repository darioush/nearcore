# Compressed data-manager tests (orphans.rs + lifecycle.rs)

Notation
- Chain: A←B←C←D = canonical blocks at heights 1,2,3,4, all *stored in the chain* (chain_with_blocks).
  Fh = fork block at height h, Ch = canonical block at h. `B unknown` = built, not in the chain.
- Manager events: `trig X` (on_block_processed), `track X` (track_block), `chain+ X` (X enters the chain),
  `FH=X` / `FEH=X` (final head / final execution head written), `cert≤h` (chunks up to h certified),
  `certifier(X; chunks)` (a block certifying those chunks), `deliver X by P` (enough parts → Decoded),
  `save X` (proof on disk), `push P ord i`.
- item(X) = the one proof the node needs from X (0→1). unneeded(X) = 1→0.
- Messages: `Pi parts` = shard-0 producer i sends enough parts; `Pi part` = one part;
  `Pi/k'` = signed with a rotated key; `S1` = a shard-1 producer.
- Outcomes: Collecting, Decoded, NotWanted, AlreadySettled, InvalidSignature, EmptyMessage;
  `→ [X]` = decoded list at a trigger; `own-ord` = every producer asked for its own ordinal.

## orphans.rs
O1  parts_for_an_unprocessed_block_are_parked_and_decoded_at_its_trigger_once
    given A←B          | P0 parts item(B) → Collecting ; trig A → [] ; trig B → [B] ; trig B → []
O2  parts_parked_by_several_producers_are_decoded_together_at_the_trigger
    given A←B          | P0,P1,P2 one part each → Collecting ; trig B → [B]
O3  a_parked_message_with_a_bad_merkle_proof_is_dropped_at_the_trigger_and_the_rest_decode
    given A←B          | P0 part with bad proof → Collecting ; P1..P3 part → Collecting ; trig B → [B]
O4  parts_for_an_unknown_block_are_decoded_once_it_is_processed
    given A, B unknown | P0 parts item(B) → Collecting ; chain+ B ; trig B → [B]
O5  parked_parts_signed_with_a_key_not_valid_for_their_block_are_dropped
    given A, B unknown, header head in epoch where P0 holds k'
                       | P0/k' parts item(B) → Collecting ; chain+ B ; trig B → [] ; P0 parts → Decoded
O6  parts_for_a_block_with_a_known_header_are_checked_against_its_epoch
    given A, header head in epoch where P0 holds k'
                       | P0/k' parts item(A) → InvalidSignature ; P0 parts → Collecting ; trig A → [A]
O7  parts_for_a_block_whose_epoch_data_is_not_stored_are_not_wanted
    given header head in an epoch not stored, block unknown | P0 parts → NotWanted
O8  a_parked_receipt_proof_into_an_unknown_shard_is_not_wanted_when_its_block_is_processed
    given A            | P0 parts (A, 0→42) → Collecting ; trig A → [] ; again → NotWanted
O9  an_empty_message_is_not_parked
    given A            | P0 sends no parts for item(A) → EmptyMessage
O10 parts_parked_for_a_block_are_decoded_at_its_trigger_after_a_higher_block_was_processed
    given A←B          | P0 parts item(A) → Collecting ; trig B → [] ; trig A → [A]
O11 parts_for_an_untracked_item_of_a_processed_block_are_not_wanted
    given A←B          | S1 parts unneeded(A) → Collecting ; track A ; again → NotWanted
O12 parts_for_a_stored_block_not_processed_yet_are_parked_after_a_higher_block_is_tracked
    given A←B←C, track C | P0 parts item(B) → Collecting ; track B → [B]
O13 parts_for_an_untracked_item_of_a_processed_block_at_or_below_the_final_execution_head_are_not_wanted
    given A←B←C, trig A,B, FEH=B, trig C
                       | S1 parts unneeded(B) → NotWanted ; P0 (wrong shard) same → NotWanted
O14 parts_for_a_canonical_block_at_or_below_the_final_head_not_processed_yet_are_decoded_at_its_trigger
    given A←B←C, FH=C, trig A | P0 parts item(B) → Collecting ; trig B → [B]
O15 parts_for_a_fork_block_at_or_below_the_final_head_are_not_wanted
    given A←B←C4, B←F3, FH=C4, trig A | P0 parts item(F3) → NotWanted
O16 parts_for_a_fork_block_not_processed_yet_are_decoded_at_its_trigger
    given A←B, A←F2, trig A,B | P0 parts item(F2) → Collecting ; trig F2 → [F2]

## lifecycle.rs
L1  last_pre_spice_block_fetches_proofs_of_a_shard_tracked_only_next_epoch
    given 3 shards, epoch 5, pre-spice chain to LPS; node tracks s0 now, s1 only next epoch
                       | needed(prev LPS) = {2→0, 2→1} ; needed(LPS) = {1→0,1→1,2→0,2→1}
L2  track_block_tracks_exactly_the_needed_items_once
    given chain to h5  | track B5 ×2 → tracking = {item(B5)} ; index[5] has 1
L3  track_block_skips_items_already_on_disk
    given A, save A    | track A → not tracking ; index empty
L4  blocks_at_or_below_the_final_execution_head_are_not_tracked
    given A←B, FEH=A   | trig A → no pulls ; track A, B → item(A) ✗, item(B) ✓
L5  a_processed_block_tracks_its_items
    given A            | trig A → tracking item(A)
L6  a_processed_block_expires_the_items_at_or_below_the_final_execution_head
    given chain to h4, track 2,3,4 | FEH=3 ; trig 4 → items 2,3 ✗, 4 ✓ ; index = {4}
L7  a_fork_item_expires_by_height_with_the_final_execution_head
    given A←B←C4, B←F3, B←F4, all tracked
                       | FEH=B ; trig C4 → F3,F4 ✓ ; FEH=C4 ; trig C4 → F3,F4 ✗ ; index empty
L8  a_fork_item_at_or_below_the_final_head_is_dropped_and_frees_its_window_slot
    given A←B←C4←C5←C6, B←F3, pull window = 1 height, FH=B, track C4,F3,
          certifier(F3; F3 s0) tracked, cert≤4
                       | trig C5 → own-ord for F3 ; FH=C4 ; trig C6 → own-ord for C4 ; F3 forgotten
L9  a_fork_block_at_or_below_the_final_head_is_not_tracked
    given A←B←C4, B←F3, FH=C4, trig C4 | track F3, B → item(F3) ✗, item(B) ✓
L10 a_certified_chunk_makes_its_own_items_pullable_and_not_a_fork_sibling_at_the_same_height
    given A←B←C4, B←F4, track C4,F4 | certifier(C4; C4 s0) tracked → item(C4) pullable, item(F4) not
L11 an_item_is_pulled_from_the_processed_block_that_certifies_its_chunk_and_not_before
    given A, track A, certifier(A) stored | trig A → no pulls ; trig certifier → own-ord
L12 a_certified_chunk_of_a_block_without_a_stored_header_is_skipped
    given A, track A, certifier(A; unknown s0, A s0) | trig certifier → own-ord for item(A)
L13 a_certified_chunk_from_a_shard_this_node_applies_is_not_made_pullable
    given A            | certifier(A; s0, s1) makes pullable = {item(A)}
L14 expiry_drops_the_expired_items_from_every_index
    given A←B←C, track A,B, cert≤2, deliver A, deliver B
                       | FEH=A ; trig C → A forgotten ; B pullable + delivered
L15 pushed_item_is_not_pulled_before_source_chunk_is_certified
    given A←B, track A | push P0 ord 0 → Collecting ; trig A → no pulls, pulling none ;
                         cert≤1 ; trig B → P0 asked gaps {1..4}, Pi asked {i}
L16 a_done_item_is_not_pulled
    given chain to h4, track B, deliver B, save B, trig C | cert≤2 ; trig D → no pulls
L17 a_late_part_for_a_done_item_is_already_settled
    given A←B←C, track C, deliver C, save C, trig C
                       | P4 late part → AlreadySettled ; P3 other commitment → AlreadySettled ;
                         FH=C ; trig C ; P4 → AlreadySettled ; FEH=C ; trig C ; P4 → NotWanted
L18 each_item_is_pulled_from_the_chunk_producers_of_its_source_shard
    given 3 shards, node applies s2, A tracked, cert≤1
                       | trig B → s0 producer i asked {i} for (0→2) ; s1 producer i asked {i} for (1→2)
L19 an_item_stops_being_pulled_only_after_its_delivery_is_in_the_store
    given chain to h4, track 1..3, cert≤3
                       | save 1 (no delivery) ; trig 4 → own-ord for item 1 ; trig 4 → none ; 1 ✓ tracked ;
                         deliver 2 ; save 2 ; trig 4 → 2 not pullable ; 1, 3 pullable
L20 a_rejected_fake_delivery_leaves_the_honest_commitment_pulled_until_it_decodes
    given A←B, track A, cert≤1
                       | liar P0 delivers fake (not saved) ; honest P1 pushes ord 1 ;
                         trig B → P1 asked {0,2,3,4}, P2..P4 own, P0 not asked ;
                         P1 sends rest → Decoded(honest), still tracked ; save A ; trig B → none
L21 a_pull_asks_each_producer_for_the_part_its_push_carries
    given A←B, track A, cert≤1 | trig B → each Pi asked the ordinal its push carries
L22 an_unreadable_processed_block_still_pulls_the_pullable_items_and_stops_pulling_the_done_ones
    given A←B, track A,B, cert≤2, deliver B, save B
                       | trig <unknown hash> → own-ord for item(A), nothing for item(B)
