# Compressed SPICE test-loop tests

Files: activation_boundary.rs (AB, 9), activation_tracking.rs (AT, 3), pre_activation.rs (PA, 3),
data_faults.rs (DF, 11). 26 tests, ~2130 source lines (helpers included) → ~150 lines below.
All paths relative to `test-loop-tests/src/tests/spice/` unless stated.

## 1. Notation

### Chain kinds (genesis + upgrade schedule)
- `chain(pre→spice)` = genesis at pre-spice version, immediate vote to Spice; activates 2 epochs after genesis.
  So the epoch of LPS is the *last pre-spice epoch* (LPE), the next is the *first spice epoch* (FSE).
- `chain(pre)` = genesis at pre-spice version, vote pinned to pre-spice; never crosses.
- `chain(spice)` = default genesis, spice from block 1 (data_faults).

### Positions (heights / blocks)
- `G` genesis. `LPS` last pre-spice block. `FS` first spice block (= child of LPS, height LPS+1).
- `LPS-k`, `FS+k` relative heights. `head`. `end(FSE)` = first block of the epoch after FSE.
- `[LPS-3..LPS]` = the 4 heights before FS, oldest first (the window chunk patterns cover).
- `cert` = last certified height (as seen by the observer node). `cert+k` = run until cert ≥ (cert now)+k.
- `exec(N)` = N's spice execution head; `FEH` = final execution head.

### Topology / roles
- `nP` = n block+chunk producers. `mV` = m chunk-validators-only (produce nothing).
  In DF the V nodes are called **R** (recipients): they can only endorse after receiving + validating the witness.
- `rpc` = rpc node, tracks every shard; the observer unless stated.
- `N0` = node index 0 (a producer). `LT` = late tracker (V that starts tracking a shard at FSE).
- `J` joiner (non-validator client that stakes into FSE). `L` leaver (producer that unstakes out of FSE).
- `Src` = producers of the first shard (by id); `Oth` = producers of the other shard (2-shard DF only).
- Shards: `s0,s1`, or `lo,hi` when it matters which side of the "test1" split an account is on.
- `u→r xshard` = cross-shard transfer from "user" to "receiver".

### Workloads
- `trickle` = one `u→r xshard` 1-yocto transfer per block until head is FS; then run 2 epochs; then run until
  cert ≥ head. (helper `cross_boundary_with_transfer_trickle`, activation_boundary.rs:181)
- `matrix(FS-7..FS-1)` = at every height in that range, 4 transfers: lo→lo, lo→hi, hi→hi, hi→lo; then wait
  for every tx and receipt outcome; then run until cert ≥ FS.

### Faults / rules (`rule: <what> from <whom> [kind] [window]`)
- `miss s@{h,...}` = drop shard s's chunk produced at those heights (DropCondition::ProtocolUpgradeChunkRange,
  offsets relative to FS: LPS = -1).
- `pattern s "...."` over `[LPS-3..LPS]`: `x` missing, `.` present.
- `delay-end k` = every chunk endorsement is held until the endorsed block is k blocks old.
- `drop-parts S [RP|W]`, `delay-parts S by d`, `corrupt-parts S`, `equivocate S`, `drop-accesses S`:
  outbound spice partial data (push, fallback push, request answers) from accounts S. `[RP]` = only receipt
  proofs, `[W]` = only witnesses, none = both. Armed from start unless placed after an event.
- `kill N @pred ; others → X ; restart N` = kill when predicate holds on N, run another node to X, restart.
- `inject K→actor` = send message kind K directly to an actor of N0 (bypasses network).

### Events
- `run →FS` (until head is spice), `run →LPS` (until head is LPS), `run +n blocks`, `run →end(FSE)`,
  `run cert+k`, `run →tail≥h`, `stake A x`, `tx A→B`.

### Outcomes / checks
- `LIVE(a..b)` no height skipped and every chunk present from a to b.
- `FEH>LPS` final execution head past the boundary. `CERT>LPS` cert past LPS.
- `SUPPLY` every block after LPS subtracts from total supply exactly the burn of the blocks its core statements
  newly certify; burn(LPS) enters once, at its certifying block, and equals LPS's pre-spice apply burn.
- `RESULT=APPLY(LPS)` certified result of LPS, per shard, = rpc's pre-spice apply result.
- `STATELESS(X,s)` some validator *without* chunk extra for (X,s) endorsed the certified result of (X,s).
  `STATELESS(X)` = for every shard.
- `ONCE(acct)` balance delta = amount × #transfers to it.
- `VIEW(h)` receiver balance at block h = initial + amount × #(transfers included below h).
- `SPLIT(tx)` tx outcome in block LPS, its receipt outcome in a block > LPS.
- `CARRIERS(a→b)` = a's new chunks after b's last pre-spice chunk, each with exactly one proof, to b.
- `UNDELIV(a→b)=n`, each executes in FS, each ∈ `PENDING(b @LPS)` (incoming receipts of b not applied).
- `endorsed(X)`, `¬endorsed(X)`, `requested(X)` (sent ≥1 SpiceDataRequest), `no-requests` (nobody did),
  `fired(f)` (fault counter f > 0), `cert-stalls`, `head-advances`.
- `NO-SPICE-TRAFFIC` (no spice NetworkRequests emitted), `SPICE-COLS-EMPTY` (10 spice DB columns empty on
  every node), `pre-spice(head)`, `GC(G+1)` (block G+1 collected), `dropped(K@actor)=n`.
- Implicit in every `run …`: the helper panics on timeout, so "the chain gets there" is always checked.

### Inheritance
- `= AB1 + X` means same given/events/checks as AB1 with X added.

## 2. Compressed tests

### activation_boundary.rs
Base `B` = chain(pre→spice), 2P+2V+rpc, 2 shards, 5-block epochs, zero inflation, gas price that moves at
LPS (min price, a gas limit one transfer fills past half), "user" and "receiver" on different shards.

AB1 test_protocol_upgrade_to_spice  (:66)
    given B | trickle → LIVE(LPS..head) ; FEH>LPS ; SUPPLY ; RESULT=APPLY(LPS) ; STATELESS(LPS)
    guard next_gas_price(LPS-1) ≠ next_gas_price(LPS) ; burn(LPS)>0 ; every tx included exactly once

AB2 test_protocol_upgrade_to_spice_missing_chunks_at_boundary  (:89)
    = AB1 + miss s0,s1@{LPS}                      guard mask(LPS)=xx

AB3 test_protocol_upgrade_to_spice_staggered_missing_chunks_at_boundary  (:110)
    = AB1 + miss s0@{LPS}, s1@{LPS-1,LPS}         guard mask(LPS)=xx, mask(LPS-1)=.x

AB4 test_protocol_upgrade_to_spice_with_execution_delay  (:133)
    = AB1 + delay-end 3                           guard LPS certifies after FS (certifying height > LPS+1)

AB5 test_restart_mid_boundary  (:442)
    given chain(pre→spice), 4P, 2 shards
    | kill N0 @head=LPS ; others →FS, +3 blocks ; restart N0 → N0 reaches others' head ; head spice ; exec(N0)>LPS

AB6 test_restart_after_first_spice_block  (:454)
    = AB5 with kill N0 @(head spice ∧ exec(N0) still pre-spice)

AB7 test_protocol_upgrade_to_spice_receipt_in_flight  (:526)
    given B | trickle → ONCE(receiver)            guard some transfer included at LPS

AB8 test_protocol_upgrade_to_spice_view_queries  (:549)
    given B | trickle → VIEW(h) for h ∈ {LPS-2, LPS-1, LPS, FS, FS+1, head-1} ; SPLIT(transfer included at LPS)

AB9 test_protocol_upgrade_to_spice_applies_each_receipt_once_with_mixed_missing_chunks  (:648)
    given chain(pre→spice), 2P+2V+rpc, 2 shards lo/hi, 10-block epochs, senders+targets on both sides
    for (pattern lo, pattern hi, n lo→hi, n hi→lo) in
        ("....","...x",2,0) ("...x","....",0,2) ("....",".x.x",3,0) ("x.x.",".x.x",0,0)
        ("..x.","x..x",3,0) (".xx.","x.xx",0,0) ("xx..","..xx",1,0) ("...x",".xxx",2,0)
    | matrix(FS-7..FS-1) → observed patterns = given ; for (a,b) ∈ {(lo,hi),(hi,lo)}: CARRIERS(a→b),
      UNDELIV(a→b)=n, each in FS and ∈ PENDING(b @LPS), STATELESS(FS,b) ; ONCE(every target) ;
      RESULT=APPLY(LPS) ; STATELESS(LPS)

### activation_tracking.rs
AT1 test_protocol_upgrade_to_spice_with_shard_rotation  (:30)
    given chain(pre→spice), 4P, 2 shards, 10-block epochs, chunk-producer shard assignment shuffled each epoch
    | run →FS ; run →end(FSE) → CERT>LPS
    guard some shard's producer set changes at FS ; every rotated-in producer produced that shard in LPE-1
          (so it holds the state; no state sync)

AT2 test_protocol_upgrade_to_spice_with_validators_tracking_a_shard_from_activation  (:101)
    given chain(pre→spice), 2P + 2V=LT, 2 shards, 10-block epochs, LT track nothing before FSE and s1 from FSE,
          catch-up slow enough to lag past LPS
    | run →LPS → each LT head=LPS without chunk extra(LPS,s1) ;
      LT endorsements of (LPS,s1) land → LT still without chunk extra(LPS,s1) ;
      run →end(FSE) → CERT>LPS ; (LPS,s1) certifies before the all-stake fallback opens ; endorsers ∩ LT ≠ ∅
    guard activation at epoch height 2 ; LT ⊂ designated validators of (LPS,s1) ; designated∖LT can't certify

AT3 test_protocol_upgrade_to_spice_with_validator_set_change_at_activation  (:285)
    given chain(pre→spice), 4P + rpc + J (tracks no shards, funded), 2 shards, 20-block epochs, L = last producer
    | at G: stake J (L's stake), stake L 0 ; run →FS → J ∉ V(LPE), J ∈ V(FSE), L ∈ V(LPE), L ∉ V(FSE) ;
      J reaches LPS → J has chunk extra(LPS,s) for every s it produces in FSE ; J applied no boundary step itself ;
      run →end(FSE), J and L reach head → LIVE(FS..head)
    guard J produces some shard in FSE

### pre_activation.rs
Base `P` = chain(pre), 1 shard, spice-traffic counter on every node.

PA1 test_pre_spice_chain_runs_under_spice_build  (:187)
    given P, 2P, GC keeps 1 epoch | tx user→validator1 ; run →tail≥2 epochs → GC(G+1) ; pre-spice(head) ;
      NO-SPICE-TRAFFIC ; SPICE-COLS-EMPTY
    guard tail=G before ; counter saw some traffic ; head pre-spice at setup

PA2 test_pre_spice_chain_survives_node_restart  (:222)
    given P, 4P, GC keeps 20 epochs | kill N0 @2 epochs ; others +1 epoch ; restart N0 (counter re-installed) ;
      N0 catches up → pre-spice(head) ; NO-SPICE-TRAFFIC ; SPICE-COLS-EMPTY

PA3 test_spice_network_messages_are_dropped_on_pre_spice_chain  (:264)
    given P, 2P | inject one of each spice message kind into N0 (endorsement twice: for a known pre-spice block
      and for an unknown hash) ; run +2 blocks →
      dropped(endorsement@core_writer)=2 ; dropped(partial data, data request, code request @distributor)=1 ;
      dropped(contract accesses, code response, witness @chunk_validator)=1 ; NO-SPICE-TRAFFIC ; SPICE-COLS-EMPTY
    guard unknown hash not in store ; exhaustive over SpiceMessageKind

### data_faults.rs
Base `D1` = chain(spice), 4P+2R, 1 shard, fault handler on every node. `D2` = chain(spice), 8P(4/shard)+2R,
2 shards, Src/Oth. "thr" = parts needed to decode (reed-solomon data parts for the shard's producers).

DF1 test_spice_partial_data_faults_with_one_answering_producer  (:110)
    given D1 ; drop-parts P1..P3 ; drop-accesses all P | run cert+8 → fired(drop) ; fired(drop-accesses) ;
      requested(every R) ; endorsed(every R)
    guard 1 answering < thr

DF2 test_spice_partial_data_faults_with_one_answering_receipt_proof_producer  (:157)
    given D2 ; drop-parts Src[1..] [RP] | run cert+16 → fired(drop) ; requested(every Oth) ; endorsed(every R)
    guard 1 answering < thr(Src)

DF3 test_spice_partial_data_faults_with_a_single_honest_receipt_proof_producer  (:194)
    = DF2 with corrupt-parts Src[1..] [RP] → fired(corrupt) ; requested(every Oth) ; endorsed(every R)

DF4 test_spice_partial_data_faults_with_delayed_pushes  (:222)
    given D1 ; delay-parts P1..P3 by > request interval | run cert+8 → fired(delay) ; requested(every R) ;
      endorsed(every R)
    guard 1 undelayed < thr ; delay > DATA_REQUEST_INTERVAL

DF5 test_spice_partial_data_faults_with_corrupted_parts  (:249)
    given D1 ; corrupt-parts P0 | run cert+4 → fired(corrupt) ; no-requests ; endorsed(every R)
    guard 3 honest ≥ thr

DF6 test_spice_partial_data_faults_with_equivocating_producer  (:272)
    given D1 ; equivocate P0 | run cert+4 → fired(equivocate) ; no-requests ; endorsed(every R)

DF7 test_spice_partial_data_faults_with_dropped_contract_accesses  (:291)
    given D1 ; drop-accesses all P | run cert+4 → fired(drop-accesses) ; no-requests ; endorsed(every R)

DF8 test_spice_partial_data_faults_none_armed  (:308)
    given D1 | run cert+4 → no fault fired ; no-requests ; endorsed(every R)        [baseline for no-requests]

DF9 test_spice_partial_data_faults_starve_the_recipients  (:324)
    given D1 ; drop-parts all P | run cert+4 → fired(drop) ; ¬endorsed(every R)      [control for endorsed(R)]

DF10 test_spice_partial_data_faults_starve_receipt_proofs  (:346)
    given D2 | run +4 blocks → cert>0 ; drop-parts all Src [RP] ; run +8 blocks (drain) ; run +8 blocks →
      cert-stalls (unchanged over the last 8) ; head-advances ; fired(drop)

DF11 test_spice_partial_data_faults_alongside_delayed_endorsements  (:383)
    given D1 ; delay-end 2 ; corrupt-parts P0 | run +14 blocks → fired(corrupt) ; endorsed(every R)

## 3. Decompressor vocabulary

### Knobs (genesis / config)
| knob | used by | helper |
|---|---|---|
| chain(pre→spice) | AB*, AT* | `TestLoopBuilder::protocol_version(pre_spice_protocol_version())` + `ProtocolUpgradeVotingSchedule::new_immediate(Spice)` (core/primitives/src/upgrade_schedule.rs:45, core/primitives/src/test_utils.rs:1418); AB wraps it in `build_pre_spice_chain_voting_for_spice` (activation_boundary.rs:34). Repeated inline in AB5/6, AB9, AT1-3 |
| chain(pre) | PA* | same, schedule pinned to pre-spice; `setup_pre_spice_chain` (pre_activation.rs:51) |
| chain(spice) | DF* | builder default |
| nP+mV | all | `validators(n, m)` (src/setup/builder.rs:175) |
| rpc | AB1-4,7-9, AT3 | `enable_rpc()` (builder.rs:186) |
| shards | all | `num_shards(n)` (builder.rs:206) |
| epoch length | all | `epoch_length` |
| supply-identity tuning (zero inflation, gas prices, adj. rate, gas limit) | AB B | inline in `build_pre_spice_chain_voting_for_spice` (activation_boundary.rs:47-54). none as a named knob ("gas price moves at LPS") |
| user accounts on given side of split | AB, AT3, PA | `add_user_account` + naming trick ("lo…"/"upper…" vs "test1"); none for "account on shard X" |
| shuffle CP assignment | AT1 | `shuffle_shard_assignment_for_chunk_producers` (builder.rs:357) |
| tracked-shard schedule per node | AT2 | `config_modifier` + `TrackedShardsConfig::Schedule` (builder.rs:395) |
| catch-up speed | AT2 | `config_modifier` setting `catchup_step_period` (inline) |
| non-validator client tracking no shards | AT3 | `add_non_validator_client` (builder.rs:322) + `TrackedShardsConfig::NoShards` |
| GC window | PA | `gc_num_epochs_to_keep` (builder.rs:367) |
| delay_warmup (arm drops before warmup) | AB | `delay_warmup` (builder.rs:417) |

### Roles
| role | helper |
|---|---|
| producer / R / rpc | `env.node_datas[..n]`, `env.rpc_node()` (src/setup/env.rs:178) |
| shard producers (Src/Oth) | `shard_producers`, `producers_of_both_shards` (data_faults.rs:63,138) — local |
| LT | inline index range (activation_tracking.rs:136) |
| J, L | inline (activation_tracking.rs:289-291) |
| designated validators of a chunk | `epoch_manager.get_chunk_validator_assignments` inline (activation_tracking.rs:168) |
| rotated-in producer | inline diff of producer sets (activation_tracking.rs:61-83) |

### Positions
| position | helper |
|---|---|
| LPS | `last_pre_spice_block_header` (src/utils/node.rs:144); `is_last_pre_spice_block` (chain/chain/src/spice/boundary.rs:20); AB walks `prev_block` (activation_boundary.rs:250); AB5 re-walks inline (:485) |
| FS | `head_block().is_spice_block()` (core/primitives/src/block.rs:589); `run_until_estimated_first_spice_height` (activation_boundary.rs:600) |
| LPS-k/FS+k | `get_block_by_height` inline |
| end(FSE) | `run_until_new_epoch` (node.rs:615) — only correct when called from inside FSE |
| cert | `last_certified_block_header` (node.rs:105) |
| exec(N), FEH | `chain_store.spice_execution_head()`, `spice_final_execution_head()` inline |

### Events
| event | helper |
|---|---|
| run →pred / +n blocks / →height / →cert / →new epoch | `run_until`, `run_for_number_of_blocks` (node.rs:559), `run_until_head_height` (node.rs:529), `run_until_certified` (node.rs:599), `run_until_new_epoch` (node.rs:615) |
| run cert+k | `run_past_certified_frontier` (data_faults.rs:80) — local |
| trickle | `cross_boundary_with_transfer_trickle` (activation_boundary.rs:181) — local, also scans inclusions |
| matrix | inline (activation_boundary.rs:734-746); `run_until_outcome_available` (node.rs:622) |
| tx / stake | `tx_send_money` (node.rs:324), `tx_from_actions` (node.rs:307), `submit_tx`, `run_tx` (node.rs:667) |
| kill / restart | `kill_node` (env.rs:87), `restart_node` (env.rs:142); `run_restart_across_boundary` (activation_boundary.rs:467) — local |
| inject message kind | none; hand-built per kind in PA3 (pre_activation.rs:277-340) with dummy builders `new_test_execution_result`, `new_test_witness` (:369,:376) |
| run →tail≥h | `run_until` + `tail` (node.rs:66) |

### Rules (faults)
| rule | helper |
|---|---|
| miss s@{h} relative to FS | `DropCondition::ProtocolUpgradeChunkRange` (src/setup/drop_condition.rs:28, impl :229/:337) |
| pattern s "...." | `drop_chunks_marked_missing` (activation_boundary.rs:614) — local |
| delay-end k | `delay_endorsements_propagation` (utils.rs:23) |
| drop/delay/corrupt/equivocate parts, kind filter, drop-accesses | `install_spice_partial_data_faults` + `SpicePartialDataFaults` (src/setup/spice_partial_data_faults.rs:114, :46; corrupt :262, equivocate :277) |
| spice traffic counter | `SpiceTrafficCounter` (pre_activation.rs:141) — local; overlaps with the observer in spice_partial_data_faults.rs:155 |
| drop/tamper at boundary-specific data (LPS witness/receipt proofs only) | none (faults are keyed by sender and kind, not by block/position) |
| drop whole block at a height | `DropCondition::BlocksByHeight` exists (drop_condition.rs:36) but unused here |
| delay/drop contract code | none (TODO in spice_partial_data_faults.rs:43) |

### Outcome / check kinds
| check | helper |
|---|---|
| LIVE | `assert_boundary_crossed_without_missing_chunks` (activation_boundary.rs:269, also FEH>LPS); AT3 reimplements inline (activation_tracking.rs:414-425) |
| SUPPLY | `assert_supply_subtracts_certified_burn_once` (activation_boundary.rs:304) |
| RESULT=APPLY + STATELESS(LPS) | `assert_boundary_results_validated` (activation_boundary.rs:371); oracle `execution_result_and_receipt_proofs_from_pre_spice_apply` (chain/chain/src/spice/boundary_synthesis.rs:53) |
| STATELESS(FS,s) | inline (activation_boundary.rs:872-903) — near-duplicate of the above |
| certifying height + on-chain endorsers | `certifying_height_and_endorsers` (node.rs:114) |
| CERT>LPS | inline compare on `last_certified_block_header` |
| ONCE / VIEW | `view_account_query` (node.rs:235), `view_account_balance_at` (node.rs:220) |
| SPLIT | `execution_outcome_with_proof` (node.rs:175), `tx_receipt_id` (node.rs:188) |
| CARRIERS / UNDELIV / PENDING | `get_undelivered_receipt_proofs` (boundary_synthesis.rs:171), `get_undelivered_receipts` (:261), `get_incoming_receipts_for_shard` (chain crate); pattern readback `observed_chunk_pattern` (activation_boundary.rs:629) |
| boundary step skipped | `shards_applied_itself` (chain/chain/src/spice/boundary.rs:50) |
| chunk extra present/absent (applied locally) | `chain_store.get_chunk_extra` inline |
| validator set membership / producer set | `get_epoch_all_validators`, `get_epoch_chunk_producers_for_shard` inline |
| endorsed / requested / fired / no-requests | `SpicePartialDataObserved` (spice_partial_data_faults.rs:88), `assert_endorsed` (data_faults.rs:102) |
| cert-stalls / head-advances | inline (data_faults.rs:368-374) |
| NO-SPICE-TRAFFIC | `SpiceTrafficCounter::assert_no_spice_traffic` (pre_activation.rs:170) |
| SPICE-COLS-EMPTY | `assert_spice_columns_empty` (pre_activation.rs:87) |
| dropped(K@actor) | `spice_dropped_count` on core writer (chain/chain/src/spice/core_writer_actor.rs:126), distributor (chain/client/src/spice/data_distributor_actor.rs:491), chunk validator (chain/client/src/spice/chunk_validator_actor.rs:195) |
| GC(h) | `get_block` is_err inline |

### Guards (precondition kinds)
- "fault leaves < thr honest parts" (`single_shard_decode_threshold`, data_faults.rs:94; inline reed_solomon in DF2/3).
- "delay > request interval", "gas price moves at LPS", "burn(LPS)>0", "mask(h)=…", "transfer included at LPS",
  "rotation happened and was state-free", "LT required for certification", "set change lands exactly at FSE",
  "J produces under spice", "counter saw traffic", "unknown hash absent".
- No shared helper; each is an inline assert.

Vocabulary size: ~12 knobs, ~8 roles, ~7 positions, ~10 events, ~9 rules (7 implemented), ~22 check kinds,
~12 guard kinds → ≈80 terms.

## 4. Coverage table

Axes: chain kind × perturbation × affected role × shards × workload × restart.

| test | chain | perturbation | affected role | shards | xshard workload | restart | main check |
|---|---|---|---|---|---|---|---|
| AB1 | pre→spice | none | – | 2 | trickle | – | LIVE SUPPLY RESULT STATELESS |
| AB2 | pre→spice | miss both @LPS | producers | 2 | trickle | – | same |
| AB3 | pre→spice | miss staggered @LPS-1,LPS | producers | 2 | trickle | – | same |
| AB4 | pre→spice | delay-end 3 | all | 2 | trickle | – | same, late cert |
| AB5 | pre→spice | kill @LPS | producer | 2 | none | yes | exec>LPS |
| AB6 | pre→spice | kill @FS, exec pre | producer | 2 | none | yes | exec>LPS |
| AB7 | pre→spice | none | – | 2 | trickle | – | ONCE |
| AB8 | pre→spice | none | – | 2 | trickle | – | VIEW SPLIT |
| AB9 | pre→spice | 8 patterns [LPS-3..LPS] | producers | 2 | matrix | – | UNDELIV ONCE STATELESS(FS) |
| AT1 | pre→spice | CP rotation @FS (state kept) | producers | 2 | none | – | CERT>LPS |
| AT2 | pre→spice | V starts tracking @FSE, slow catch-up | V | 2 | none | – | witness-endorse, CERT>LPS |
| AT3 | pre→spice | join+leave @FSE (state sync) | joiner/leaver | 2 | none | – | LIVE |
| PA1 | pre | none (GC) | – | 1 | none | – | inert |
| PA2 | pre | kill | producer | 1 | none | yes | inert |
| PA3 | pre | injected spice msgs | N0 | 1 | none | – | dropped counts, inert |
| DF1 | spice | drop 3/4 + accesses | producers→R | 1 | none | – | requested, endorsed |
| DF2 | spice | drop 3/4 RP | Src→Oth | 2 | implicit | – | requested(Oth) |
| DF3 | spice | corrupt 3/4 RP | Src→Oth | 2 | implicit | – | requested(Oth) |
| DF4 | spice | delay 3/4 | producers→R | 1 | none | – | requested |
| DF5 | spice | corrupt 1/4 | producers→R | 1 | none | – | no-requests |
| DF6 | spice | equivocate 1/4 | producers→R | 1 | none | – | no-requests |
| DF7 | spice | drop accesses all | producers→R | 1 | none | – | no-requests |
| DF8 | spice | none | – | 1 | none | – | baseline |
| DF9 | spice | drop all | producers→R | 1 | none | – | ¬endorsed |
| DF10 | spice | drop all RP of a shard | Src→Oth | 2 | implicit | – | cert-stalls |
| DF11 | spice | corrupt 1/4 + delay-end 2 | producers→R | 1 | none | – | endorsed |

### Empty cells that look meaningful
1. **Data faults × crossing chain.** No DF-style fault is ever armed around LPS. The boundary witness and the
   LPS receipt proofs are distributed by a special path (pre-spice producers synthesise them), and no test drops,
   delays, corrupts or equivocates them, nor forces R to *pull* boundary data. (Also needs a new rule kind:
   fault keyed by block/position, which does not exist.)
2. **In-flight receipts × membership/tracking change.** AT2 and AT3 run with no cross-shard workload. AT3's
   docstring says the joiner executes its first spice chunk "from the receipts the pre-spice producers send", yet
   with no transfers those receipts are empty. No ONCE/UNDELIV check with a joiner, a rotated producer, or a late
   tracker. Same for missing chunks at LPS combined with any of those.
3. **Restart × anything checked.** AB5/AB6 kill a producer with no workload and check only exec>LPS. Missing:
   restart with in-flight receipts (ONCE), restart then SUPPLY/RESULT, restart of an R or rpc node, restart
   under delay-end, restart while LPS is still uncertified (kill after FS and before cert(LPS)). Pre-spice-chain
   restart exists (PA2) but nothing restarts across GC of pre-spice data.
4. **Perturbations on the spice side of the boundary.** Every chunk pattern covers `[LPS-3..LPS]`. No missing
   chunk at FS/FS+1, no skipped height at FS (BlocksByHeight exists, and LIVE asserts the opposite), no fork with
   two LPS candidates or an LPS sibling, and no missing chunks combined with delay-end. All of these are where the
   boundary bootstrap's "first spice chunk applies pending receipts" logic runs.
5. **Kind × fault matrix in DF.** Witness-only faults (`only_kind=Witness`) are never armed. Receipt proofs
   are never delayed or equivocated, and `drop-accesses` is only tested with 1 shard. Restart is never combined
   with faults (`install_spice_partial_data_faults` explicitly supports re-install after restart). The only
   2-shard DF tests use producers as recipients: R endorsement of a 2-shard witness under RP faults is asserted,
   but how much R recovered is not (the TODO at data_faults.rs:9 admits this).
6. Smaller ones: pre-spice chain with 2 shards (PA is all single-shard, so pre-spice cross-shard traffic gating
   is untested). Activation with resharding at the boundary (SUPPLY assumes a fixed layout). Validator-only
   nodes in AT1 (it has 0 V). PA3 injects only to N0 and only once per kind, so it says nothing about
   a message arriving *after* activation for a pre-spice block.

## 5. Hard to compress

- **AB9's expected counts are opaque oracles.** The 8-row table `(pattern, pattern, n, n)` *is* the point and
  stays verbatim, but `n` cannot be derived from anything in the description. It counts gas refunds and
  receipts forwarded from the outgoing buffer, and depends on matrix timing (transfers start at FS-7). A
  decompressor would need a model of receipt routing to regenerate the numbers, or must copy them. Also, for the
  rows with `(0,0)` the per-receiving-shard loop never runs, so `PENDING` and `STATELESS(FS,b)` are **vacuous**
  in 2 of 8 rows. A compressed entry hides this unless you mark it.
- **Check-oracles as single tokens.** `SUPPLY` (~60 lines), `RESULT=APPLY` and `STATELESS` (~60) and `CARRIERS`
  are independent algorithms, not lookups. Compressing them to tokens works only because the decompressor
  already contains them. The entry length is honest, but the vocabulary carries the weight.
- **Means vs. relation for setups that need tuning.** "Gas price moves at LPS" needs 4 numeric knobs (zero
  inflation, min/max price, adjustment rate, 400 Ggas limit). "Catch-up lags the boundary bootstrap" (AT2) is
  realised by `catchup_step_period = 5s`, a race won by timing. The relation is the essence, but no
  decompressor can establish it without a search or the magic values, and the guards only detect failure.
- **AT1's given is luck.** "The shuffle happens to rotate producers only into shards they had two epochs
  earlier" is an emergent property of the RNG seed, checked by a guard. It cannot be written as a `given` that a
  generator can establish. Change the seed or validator count and the test silently becomes a guard failure,
  or, if the guard were weaker, a different test.
- **Run lengths are semi-essential.** PUSH=4, PULL=8, 2×PULL, 14 blocks, 4/8/8 in DF10, 2 epochs after FS in
  trickle. Some are justified in comments ("four is not enough"), so they are tuning that bounds liveness, not
  accidents. I kept them in `cert+k` form, which is half-compressed.
- **Implicit checks in helpers.** Every `run_until*` panics on timeout, so "certification keeps advancing"
  is a real outcome of each DF test, including DF9, where the producers certify among themselves. The trickle
  helper also asserts that each tx is included exactly once and that the transfers cross shards. Neither shows
  in the test body. I had to add the "implicit" note to notation instead of to entries.
- **Interval invariants sampled at points.** AT2's "LT endorses while still lacking chunk extra" is a
  `while`/ordering claim over an interval, checked at two sample points (before and after the endorsements
  land). The `event → outcome` sequence expresses it only approximately.
- **Predicate positions.** AB6's kill point ("head is spice but exec head still pre-spice") is a predicate on
  node state, not a chain position. So is AB5's in practice. The format absorbs them as `@pred`, but they are
  not in the position vocabulary.
- **PA3 is a table, not a scenario.** It is an exhaustive routing map (kind → gating actor → count), plus
  hand-built dummy messages that are pure accident (default hashes, empty witness). The real content is the
  routing table and "unknown hash falls back to head". Exhaustiveness comes from a compile-time `match`, which
  the format can't express.
- **Cross-test dependencies.** DF8 (baseline for `no-requests`) and DF9 (control for `endorsed`) make the
  other DF tests non-vacuous, so their guards live in *other tests*. AB2-4 are best written as `= AB1 + Δ`. I
  added inheritance and "[baseline/control]" annotations. The format as given has no slot for either.
- **Hidden assumptions behind checks.** RESULT=APPLY assumes the rpc node tracks all shards. SUPPLY assumes no
  layout change and zero inflation. DF fault arming assumes 1 shard (asserted) or a Src/Oth split.
  `run_until_new_epoch` as `→end(FSE)` is correct only because it is called inside FSE. These are in neither
  the given nor the guard. They live in the decompressor's preconditions.
- **Minor gap found while compressing:** DF8's "no fault fired" sum omits `dropped_contract_accesses`
  (data_faults.rs:314). This is harmless because nothing arms it, but the baseline is not literally "nothing fired".
