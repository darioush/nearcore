// Abstract model of SpiceDataManager's receipt-proof parking (#16653).
//
// World: blocks A (h1), B (h2, canonical), F (h2, fork of B). One item per block (proof 0->1).
// Producers p0..p2 send their own part; K = 3 distinct parts decode (real: 3 of 5).
// "nonproducer" is a validator that doesn't produce the item; "badsig" is p0 signing with
// a key not valid for the block's epochs.
module receipt_parking {
  // ---------------- universe ----------------
  pure val BLOCKS = Set("A", "B", "F")
  pure val HEIGHT = Map("A" -> 1, "B" -> 2, "F" -> 2)
  pure val PARENT = Map("A" -> "", "B" -> "A", "F" -> "A")
  pure val CANON = Map(1 -> "A", 2 -> "B")
  pure val MSGS = Set("p0", "p1", "p2", "nonproducer", "badsig")
  pure val K = 3
  pure def producerIdx(m: str): int = if (m == "p0") 0 else if (m == "p1") 1 else 2
  pure def isProducer(m: str): bool = Set("p0", "p1", "p2").contains(m)
  pure def max(a: int, b: int): int = if (a > b) a else b

  type Item = { parts: Set[int], decoded: bool }
  type Step = { kind: str, block: str, msg: str, h: int, outcome: str, why: str, decodedAt: Set[str], pruned: Set[str] }

  // ---------------- environment (chain + store) ----------------
  var chainHas: Set[str]      // block (and so header) stored
  var envFH: int              // consensus final head height in the store
  var envFE: int              // final execution head height in the store
  var storeDone: Set[str]     // receipt proof persisted for the block's item

  // ---------------- manager ----------------
  var mFH: int                // ChainHeights::final_head (0 = none/genesis)
  var mFE: int                // ChainHeights::final_execution_head
  var items: str -> Item      // items (keys = tracked)
  var delivered: Set[str]
  var done: Set[str]
  var orphans: str -> Set[int]  // parked producer parts by block (empty set = no entry)
  var processed: Set[str]       // processed_blocks keys

  // ---------------- bookkeeping for checks/covers ----------------
  var decodes: str -> int
  var last: Step

  pure val NOSTEP: Step = { kind: "init", block: "", msg: "", h: 0, outcome: "", why: "", decodedAt: Set(), pruned: Set() }

  def canonicalAt(h: int): bool = chainHas.contains(CANON.get(h))
  def tracked(b: str): bool = items.keys().contains(b)
  pure def without(m: str -> a, k: str): str -> a = m.keys().exclude(Set(k)).mapBy(x => m.get(x))

  action init = all {
    chainHas' = Set(), envFH' = 0, envFE' = 0, storeDone' = Set(),
    mFH' = 0, mFE' = 0, items' = Map(), delivered' = Set(), done' = Set(),
    orphans' = BLOCKS.mapBy(_ => Set()), processed' = Set(),
    decodes' = BLOCKS.mapBy(_ => 0), last' = NOSTEP,
  }

  action unchangedManager = all {
    mFH' = mFH, mFE' = mFE, items' = items, delivered' = delivered, done' = done,
    orphans' = orphans, processed' = processed, decodes' = decodes,
  }

  action unchangedEnv = all {
    chainHas' = chainHas, envFH' = envFH, envFE' = envFE, storeDone' = storeDone,
  }

  // ---------------- environment actions ----------------
  action chainAdd(b: str): bool = all {
    not(chainHas.contains(b)),
    PARENT.get(b) == "" or chainHas.contains(PARENT.get(b)),
    // the fork is stored after the canonical sibling and before its height finalizes
    b != "F" or (chainHas.contains("B") and envFH < 2),
    chainHas' = chainHas.union(Set(b)),
    envFH' = envFH, envFE' = envFE, storeDone' = storeDone,
    unchangedManager,
    last' = { ...NOSTEP, kind: "chainAdd", block: b },
  }

  action setFH(h: int): bool = all {
    h > envFH, canonicalAt(h),
    envFH' = h,
    chainHas' = chainHas, envFE' = envFE, storeDone' = storeDone,
    unchangedManager,
    last' = { ...NOSTEP, kind: "setFH", h: h },
  }

  action setFE(h: int): bool = all {
    h > envFE, h <= envFH,
    envFE' = h,
    chainHas' = chainHas, envFH' = envFH, storeDone' = storeDone,
    unchangedManager,
    last' = { ...NOSTEP, kind: "setFE", h: h },
  }

  action saveProof(b: str): bool = all {
    chainHas.contains(b), not(storeDone.contains(b)),
    storeDone' = storeDone.union(Set(b)),
    chainHas' = chainHas, envFH' = envFH, envFE' = envFE,
    unchangedManager,
    last' = { ...NOSTEP, kind: "saveProof", block: b },
  }

  // ---------------- manager: on_parts_received ----------------
  // Outcome of a message for an untracked item: (outcome, why).
  def untrackedOutcome(b: str, m: str): (str, str) = {
    val known = chainHas.contains(b)
    if (processed.contains(b)) ("NotWanted", "processed")
    else if (known and HEIGHT.get(b) <= mFE) ("NotWanted", "belowFE")
    else if (m == "nonproducer") (if (known) ("NotAProducer", "untrackedKnown") else ("NotWanted", "guessMiss"))
    else if (m == "badsig") ("InvalidSignature", "untracked")
    else if (known and HEIGHT.get(b) <= mFH and CANON.get(HEIGHT.get(b)) != b) ("NotWanted", "forkBelowFH")
    else ("Collecting", if (known) "parkKnown" else "parkUnknown")
  }

  def trackedOutcome(b: str, m: str): (str, str) = {
    val it = items.get(b)
    if (m == "nonproducer") ("NotAProducer", "tracked")
    else if (m == "badsig") ("InvalidSignature", "tracked")
    else if (done.contains(b)) ("AlreadySettled", "done")
    else if (it.decoded) ("AlreadySettled", "decoded")
    else if (it.parts.contains(producerIdx(m))) ("Collecting", "duplicate")
    else if (it.parts.size() + 1 >= K) ("Decoded", "direct")
    else ("Collecting", "direct")
  }

  action receive(b: str, m: str): bool = {
    val res = if (tracked(b)) trackedOutcome(b, m) else untrackedOutcome(b, m)
    val outcome = res._1
    all {
      unchangedEnv,
      mFH' = mFH, mFE' = mFE, done' = done, processed' = processed,
      orphans' = if (outcome == "Collecting" and not(tracked(b)))
                   orphans.setBy(b, s => s.union(Set(producerIdx(m))))
                 else orphans,
      items' = if (tracked(b) and (outcome == "Collecting" or outcome == "Decoded"))
                 items.setBy(b, it => { parts: it.parts.union(Set(producerIdx(m))), decoded: outcome == "Decoded" })
               else items,
      delivered' = if (outcome == "Decoded") delivered.union(Set(b)) else delivered,
      decodes' = if (outcome == "Decoded") decodes.setBy(b, n => n + 1) else decodes,
      last' = { ...NOSTEP, kind: "receive", block: b, msg: m, outcome: outcome, why: res._2 },
    }
  }

  // ---------------- manager: on_block_processed ----------------
  action trigger(b: str): bool = {
    // 1. final execution head: expire items, forget processed blocks at or below it
    val fe = max(mFE, envFE)
    val expired = items.keys().filter(x => HEIGHT.get(x) <= fe)
    val items1 = items.keys().exclude(expired).mapBy(x => items.get(x))
    val processed1 = processed.filter(x => HEIGHT.get(x) > fe)
    // 2. final head: drop fork items at heights newly at or below it
    val fh = max(mFH, envFH)
    val forks = items1.keys().filter(x => HEIGHT.get(x) > mFH and HEIGHT.get(x) <= fh
                                          and not(canonicalAt(HEIGHT.get(x)) and CANON.get(HEIGHT.get(x)) == x))
    val items2 = items1.keys().exclude(forks).mapBy(x => items1.get(x))
    val gone = expired.union(forks)
    // 3. track the block's item
    val h = HEIGHT.get(b)
    val skip = h <= fe or (h <= fh and CANON.get(h) != b) or storeDone.contains(b) or items2.keys().contains(b)
    val items3 = if (skip) items2 else items2.put(b, { parts: Set(), decoded: false })
    // 4. release parked parts into the (now) tracked item
    val parked = orphans.get(b)
    val canAdd = parked != Set() and items3.keys().contains(b)
                 and not(done.exclude(gone).contains(b)) and not(items3.get(b).decoded)
    val newParts = if (canAdd) items3.get(b).parts.union(parked) else Set()
    val decodedNow = canAdd and newParts.size() >= K
    val items4 = if (canAdd) items3.put(b, { parts: newParts, decoded: decodedNow }) else items3
    val delivered1 = delivered.exclude(gone).union(if (decodedNow) Set(b) else Set())
    // 5. delivered items whose proof is stored become done
    val newlyDone = delivered1.intersect(storeDone)
    all {
      chainHas.contains(b),
      unchangedEnv,
      mFE' = fe, mFH' = fh,
      items' = items4,
      processed' = processed1.union(Set(b)),
      orphans' = orphans.set(b, Set()),
      delivered' = delivered1.exclude(newlyDone),
      done' = done.exclude(gone).union(newlyDone),
      decodes' = if (decodedNow) decodes.setBy(b, n => n + 1) else decodes,
      last' = { ...NOSTEP, kind: "trigger", block: b,
                why: if (parked != Set() and not(items3.keys().contains(b))) "parkedDropped"
                     else if (storeDone.contains(b) and not(items2.keys().contains(b))) "skipDone"
                     else "",
                decodedAt: if (decodedNow) Set(b) else Set(),
                pruned: gone },
    }
  }

  action step = any {
    nondet b = BLOCKS.oneOf()
    any { chainAdd(b), saveProof(b), trigger(b) },
    nondet b = BLOCKS.oneOf()
    nondet m = MSGS.oneOf()
    receive(b, m),
    nondet h = Set(1, 2).oneOf()
    any { setFH(h), setFE(h) },
  }

  // ---------------- invariants ----------------
  val deliveredDoneSubsetTracked = delivered.subseteq(items.keys()) and done.subseteq(items.keys())
                                   and delivered.intersect(done) == Set()
  val noItemAtOrBelowFE = items.keys().forall(x => HEIGHT.get(x) > mFE)
  val noParkingForProcessed = processed.forall(x => orphans.get(x) == Set())
  val doneOnlyIfStored = done.subseteq(storeDone)
  val noForkItemAtOrBelowFH = not(items.keys().contains("F") and HEIGHT.get("F") <= mFH)
  val decodeAtMostOnce = BLOCKS.forall(x => decodes.get(x) <= 1)
  val allInvariants = deliveredDoneSubsetTracked and noItemAtOrBelowFE and noParkingForProcessed
                      and doneOnlyIfStored and noForkItemAtOrBelowFH and decodeAtMostOnce

  // ---------------- covers (conditions some test should hit) ----------------
  def recvWhy(o: str, w: str): bool = last.kind == "receive" and last.outcome == o and last.why == w
  val coverParkUnknown      = recvWhy("Collecting", "parkUnknown")
  val coverParkKnown        = recvWhy("Collecting", "parkKnown")
  val coverDecodeAtTrigger  = last.kind == "trigger" and last.decodedAt != Set()
  val coverDecodeDirect     = recvWhy("Decoded", "direct")
  val coverDuplicate        = recvWhy("Collecting", "duplicate")
  val coverNotWantedProcessed = recvWhy("NotWanted", "processed")
  val coverNotWantedBelowFE = recvWhy("NotWanted", "belowFE")
  val coverNotWantedGuess   = recvWhy("NotWanted", "guessMiss")
  val coverNotWantedForkFH  = recvWhy("NotWanted", "forkBelowFH")
  val coverNotProducerTracked = recvWhy("NotAProducer", "tracked")
  val coverNotProducerKnown = recvWhy("NotAProducer", "untrackedKnown")
  val coverBadSigTracked    = recvWhy("InvalidSignature", "tracked")
  val coverBadSigUntracked  = recvWhy("InvalidSignature", "untracked")
  val coverSettledDone      = recvWhy("AlreadySettled", "done")
  val coverSettledDecoded   = recvWhy("AlreadySettled", "decoded")
  val coverParkedDropped    = last.kind == "trigger" and last.why == "parkedDropped"
  val coverSkipDone         = last.kind == "trigger" and last.why == "skipDone"
  val coverForkPruned       = last.kind == "trigger" and last.pruned.contains("F") and HEIGHT.get("F") > mFE
  val coverStaleOrphan      = BLOCKS.exists(x => orphans.get(x) != Set() and HEIGHT.get(x) <= mFE)

  // ---------------- a hand-written test ----------------
  run parkThenDecodeAtTriggerTest =
    init
      .then(receive("A", "p0")).expect(orphans.get("A") == Set(0))
      .then(receive("A", "p1"))
      .then(receive("A", "p2"))
      .then(chainAdd("A"))
      .then(trigger("A"))
      .expect(delivered == Set("A") and last.decodedAt == Set("A") and orphans.get("A") == Set())
      .then(receive("A", "p0"))
      .expect(last.outcome == "AlreadySettled")
}
