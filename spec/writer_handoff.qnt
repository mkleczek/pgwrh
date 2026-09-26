// Design model, not an implementation of PostgreSQL internals.
// See README.md for abstraction boundaries and unproved obligations.
module writer_handoff {
  pure val Nodes = Set(0, 1, 2) // A, B, C; one handoff from A to B.
  pure val SlotIds = 0.to(8)
  pure val MaxTransactions = 8
  pure val MaxAllocations = 16

  // w is a GHOST ordinal in the single shard's transaction history.
  // The real identity is (shard, epoch, author incarnation, original end LSN).
  // lsn is local to the node storing this copy; original never changes.
  type Entry = {
    w: int, epoch: int, author: int, original: int,
    kind: str, floor: int, lsn: int,
  }
  type Replica = {
    up: bool, needsInit: bool, cursor: int, rows: List[int],
    wal: List[Entry], pending: bool, buffer: Entry, allocated: int,
    historyValid: bool, historyPos: int, recycled: int,
    index: int -> int, safe: int, baseline: int,
    input: int, required: int,
  }
  type Slot = {
    created: bool, valid: bool, src: int, dst: int,
    start: int, confirmed: int, restart: int,
    read: int, ack: int, originAny: bool,
  }
  type State = {
    replicas: int -> Replica, slots: int -> Slot,
    log: List[Entry], writer: int, epoch: int, phase: str,
    writeTargets: Set[int], oldRouteClosed: bool, end: int,
    inheritedFloor: int, transfers: Set[int],
  }
  var s: State

  pure def min2(a: int, b: int): int = if (a < b) a else b
  pure def max2(a: int, b: int): int = if (a > b) a else b
  pure def base(n: int): int = 100 * (n + 1)
  pure def address(n: int, allocation: int): int = base(n) + (n + 2) * allocation
  pure def entries(xs: List[Entry]): Set[Entry] = xs.foldl(Set(), (acc, e) => acc.union(Set(e)))
  pure val Empty = {
    w: 0, epoch: 0, author: 0, original: 0, kind: "empty", floor: 0, lsn: 0,
  }
  pure def initialReplica(n: int): Replica = {
    up: true, needsInit: false, cursor: 0, rows: List(),
    wal: List(), pending: false, buffer: Empty, allocated: 0,
    historyValid: true, historyPos: base(n), recycled: base(n),
    index: Map(0 -> base(n)), safe: 0, baseline: 0,
    input: if (n == 0) -1 else n - 1,
    required: if (n == 0) -1 else n - 1,
  }
  pure def freshSlot(src: int, dst: int, at: int, anyOrigin: bool): Slot = {
    created: true, valid: true, src: src, dst: dst,
    start: at, confirmed: at, restart: at,
    read: at, ack: at, originAny: anyOrigin,
  }
  pure val UnusedSlot = {
    created: false, valid: false, src: 0, dst: 0,
    start: 100, confirmed: 100, restart: 100,
    read: 100, ack: 100, originAny: false,
  }
  action init = s' = {
    replicas: Nodes.mapBy(n => initialReplica(n)),
    slots: SlotIds.mapBy(_ => UnusedSlot)
      .set(0, freshSlot(0, 1, base(0), false))
      .set(1, freshSlot(0, 2, base(0), false))
      .set(2, freshSlot(1, 2, base(1), false)),
    log: List(), writer: 0, epoch: 1, phase: "writing",
    writeTargets: Set(0), oldRouteClosed: false, end: 0,
    inheritedFloor: 0, transfers: Set(),
  }

  def replica(n: int): Replica = s.replicas.get(n)
  def slot(k: int): Slot = s.slots.get(k)
  def lastLsn(n: int): int = replica(n).wal.foldl(
    replica(n).index.get(replica(n).baseline), (a, e) => max2(a, e.lsn))
  // Only this publisher's original transactions certify origin=none progress.
  def watermarkAt(n: int, p: int, anyOrigin: bool): int = replica(n).wal
    .select(e => e.lsn <= p and (anyOrigin or e.author == n))
    .foldl(if (not(anyOrigin) and n == 1 and s.epoch == 2)
      max2(replica(n).baseline, s.end) else replica(n).baseline,
      (w, e) => max2(w, e.w))
  def slotFloor(n: int): int = SlotIds
    .filter(k => slot(k).valid and slot(k).src == n)
    .fold(replica(n).cursor,
      (w, k) => min2(w, watermarkAt(n, slot(k).confirmed, slot(k).originAny)))
  def publicationFloor(ignoreInheritance: bool): int = {
    val current = slotFloor(s.writer)
    if (s.epoch == 2 and s.transfers != Set() and not(ignoreInheritance))
      min2(current, s.inheritedFloor)
    else current
  }
  def available(n: int): bool = replica(n).up and not(replica(n).needsInit)
  def idle(n: int): bool = available(n) and not(replica(n).pending)
  def hasRoom(n: int): bool = replica(n).allocated < MaxAllocations
  def nextEntry(k: int): Entry = replica(slot(k).src).wal
    .select(e => e.lsn > slot(k).read).head()
  def hasNext(k: int): bool = replica(slot(k).src).wal
    .select(e => e.lsn > slot(k).read).length() > 0

  // One atomic local transaction writes rows (for data), a transactional marker,
  // and COMMIT. Its WAL is not yet flushed, and must not be acknowledged.
  action commitOriginal(kind: str, floor: int): bool = {
    val n = s.writer
    val r = replica(n)
    val lsn = address(n, r.allocated + 1)
    val e = { w: s.log.length() + 1, epoch: s.epoch, author: n,
      original: lsn, kind: kind, floor: floor, lsn: lsn }
    all {
      idle(n), hasRoom(n), s.log.length() < MaxTransactions,
      s' = { replicas: s.replicas.set(n,
        { pending: true, buffer: e, allocated: r.allocated + 1, ...r }), ...s },
    }
  }
  action write = all {
    s.phase == "writing", s.writeTargets.contains(s.writer),
    commitOriginal("data", 0),
  }
  action announceSafe = all {
    s.phase == "writing", s.writeTargets.contains(s.writer),
    commitOriginal("safe", publicationFloor(false)),
  }

  // The publisher sends only durable WAL. A flush makes row effects, the
  // original watermark, and the subscriber's source cursor durable together.
  action flush(n: int): bool = {
    val r = replica(n)
    val e = r.buffer
    val original = e.author == n
    val isControl = Set("safe", "end", "begin").contains(e.kind)
    val updated = {
      pending: false, buffer: Empty, cursor: e.w,
      rows: if (e.kind == "data") r.rows.append(e.w) else r.rows,
      wal: r.wal.append(e), safe: if (isControl) max2(r.safe, e.floor) else r.safe,
      ...r,
    }
    all {
      available(n), r.pending,
      s' = {
        replicas: s.replicas.set(n, updated),
        log: if (original) s.log.append(e) else s.log,
        slots: if (original) s.slots else s.slots.set(r.input,
          { ack: max2(slot(r.input).ack, slot(r.input).read), ...slot(r.input) }),
        phase: if (original and e.kind == "end") "sealed"
          else if (original and e.kind == "begin") "writing" else s.phase,
        end: if (original and e.kind == "end") e.w else s.end,
        ...s,
      },
    }
  }

  // A decoder sees only flushed entries, and persists W -> local commit end.
  // Its own decoding slot is abstracted: historyPos retains everything it needs.
  action indexWal(n: int): bool = {
    val r = replica(n)
    val idx = r.wal.select(e => e.lsn > r.recycled)
      .foldl(r.index, (m, e) => m.put(e.w, e.lsn))
    all {
      available(n), idx != r.index,
      s' = { replicas: s.replicas.set(n, { index: idx, ...r }), ...s },
    }
  }

  // Native origin filtering may advance a slot even though its subscriber has
  // NOT applied these transactions from another source.
  action readStream(k: int): bool = all {
    slot(k).valid, available(slot(k).src), idle(slot(k).dst), hasNext(k),
    {
      val sl = slot(k)
      val n = sl.dst
      val r = replica(n)
      val e = nextEntry(k)
      val filtered = not(sl.originAny) and e.author != sl.src
      if (filtered or (r.input == k and e.w <= r.cursor)) {
        s' = { slots: s.slots.set(k,
          { read: e.lsn, ack: max2(sl.ack, e.lsn), ...sl }), ...s }
      } else all {
        r.input == k, e.w == r.cursor + 1, e.lsn > replica(sl.src).recycled,
        hasRoom(n),
        s' = {
          replicas: s.replicas.set(n, {
            pending: true, buffer: { lsn: address(n, r.allocated + 1), ...e },
            allocated: r.allocated + 1, ...r,
          }),
          slots: s.slots.set(k, { read: e.lsn, ...sl }), ...s,
        },
      }
    },
  }
  // Feedback is delayed independently of durable apply. Inactive slots count.
  action feedback(k: int): bool = all {
    slot(k).valid, replica(slot(k).src).up, replica(slot(k).dst).up,
    slot(k).ack > slot(k).confirmed,
    s' = { slots: s.slots.set(k, { confirmed: slot(k).ack, ...slot(k) }), ...s },
  }
  action checkpointSlot(k: int): bool = all {
    slot(k).valid, slot(k).restart < slot(k).confirmed,
    s' = { slots: s.slots.set(k, { restart: slot(k).confirmed, ...slot(k) }), ...s },
  }
  action advanceHistory(n: int): bool = {
    val r = replica(n)
    all {
      available(n), r.historyValid, r.index.keys().contains(r.safe),
      r.index.get(r.safe) > r.historyPos,
      s' = { replicas: s.replicas.set(n,
        { historyPos: r.index.get(r.safe), ...r }), ...s },
    }
  }
  // Aggressive legal recycling: real restart_lsn may retain MORE WAL.
  action recycle(n: int): bool = {
    val r = replica(n)
    val h = if (r.historyValid) r.historyPos else lastLsn(n)
    val cut = SlotIds.filter(k => slot(k).valid and slot(k).src == n)
      .fold(h, (p, k) => min2(p, slot(k).restart))
    all {
      r.up, cut > r.recycled,
      s' = { replicas: s.replicas.set(n, { recycled: cut, ...r }), ...s },
    }
  }

  // Exactly one controller is the sole entry point for application writes.
  // Closing its per-shard gate prevents admission of new transactions on A.
  // Existing remote transactions must finish before seal; in this abstraction
  // they are represented by pending WAL. No replica lease protocol is assumed.
  // Controller restart, ambiguous remote outcomes, and stale connections still
  // require a concrete recovery protocol; this action does not establish it.
  action pauseWrites = all {
    s.epoch == 1, s.phase == "writing",
    s' = { writeTargets: Set(), oldRouteClosed: true, phase: "paused", ...s },
  }
  action seal = all {
    s.phase == "paused", commitOriginal("end", slotFloor(0)),
  }
  action promote = all {
    s.phase == "sealed", s.oldRouteClosed, s.writeTargets == Set(),
    idle(1), replica(1).cursor == s.end, replica(1).historyValid,
    {
      val floor = s.log.nth(s.end - 1).floor
      all {
        replica(1).index.keys().contains(floor),
        replica(1).historyPos <= replica(1).index.get(floor),
        s' = {
          writer: 1, epoch: 2, phase: "opening", writeTargets: Set(1),
          inheritedFloor: floor,
          transfers: Set(0, 2).filter(n => not(replica(n).needsInit)),
          replicas: s.replicas.set(1, { input: -1, required: -1, ...replica(1) }),
          ...s,
        },
      }
    },
  }
  action beginEpoch = all {
    s.phase == "opening", commitOriginal("begin", s.inheritedFloor),
  }

  // Clone is LOCAL to B, from B's history slot, never from A's slot or LSN.
  // Pause/drain the subscriber first; its durable cursor then stays fixed.
  action cloneHistory(n: int): bool = {
    val k = 3 + n
    val donor = replica(1)
    all {
      s.epoch == 2, s.transfers.contains(n), idle(n), available(1),
      donor.historyValid, not(slot(k).created),
      s' = {
        replicas: s.replicas.set(n, { input: -1, ...replica(n) }),
        slots: s.slots.set(k, freshSlot(1, n, donor.historyPos, true)), ...s,
      },
    }
  }
  action advanceClone(n: int): bool = {
    val k = 3 + n
    val r = replica(n)
    val donor = replica(1)
    all {
      slot(k).valid, s.transfers.contains(n), idle(n), r.input == -1,
      donor.index.keys().contains(r.cursor),
      {
        val p = donor.index.get(r.cursor)
        all {
          p >= slot(k).confirmed, p >= donor.recycled,
          s' = { slots: s.slots.set(k,
            { confirmed: p, read: p, ack: p, ...slot(k) }), ...s },
        }
      },
    }
  }
  action attachClone(n: int): bool = {
    val k = 3 + n
    val r = replica(n)
    all {
      s.transfers.contains(n), idle(n), r.input == -1, slot(k).valid,
      replica(1).index.keys().contains(r.cursor),
      slot(k).confirmed == replica(1).index.get(r.cursor),
      s' = {
        replicas: s.replicas.set(n, { input: k, required: k, ...r }),
        ...s,
      },
    }
  }
  // B receives acknowledgement of the subscriber's durable installation.
  // A crash or lost acknowledgement between attachClone and this action keeps
  // the inherited bound. Old slots may be retired separately, afterwards.
  action finishTransfer(n: int): bool = all {
    s.transfers.contains(n), available(1), available(n),
    replica(n).required == 3 + n, slot(3 + n).valid,
    s' = { transfers: s.transfers.exclude(Set(n)), ...s },
  }
  action retireUnusedSlot(k: int): bool = all {
    s.epoch == 2, slot(k).valid, not(s.transfers.contains(slot(k).dst)),
    replica(slot(k).dst).required != k,
    s' = { slots: s.slots.set(k, { valid: false, ...slot(k) }), ...s },
  }
  action useOriginNone(n: int): bool = all {
    replica(n).required >= 3, replica(n).required <= 5,
    replica(n).cursor >= s.end,
    {
      val k = replica(n).required
      all {
        slot(k).valid, slot(k).originAny,
        s' = { slots: s.slots.set(k, { originAny: false, ...slot(k) }), ...s },
      }
    },
  }

  action crash(n: int): bool = all {
    replica(n).up,
    s' = {
      replicas: s.replicas.set(n, { up: false, pending: false,
        buffer: Empty, ...replica(n) }),
      slots: SlotIds.mapBy(k => if (slot(k).dst == n)
        { read: slot(k).confirmed, ...slot(k) } else slot(k)), ...s,
    },
  }
  action restart(n: int): bool = all {
    not(replica(n).up),
    s' = { replicas: s.replicas.set(n, { up: true, ...replica(n) }), ...s },
  }
  // Reconnect may resend committed transactions after delayed feedback.
  action reconnect(n: int): bool = all {
    idle(n), replica(n).input >= 0,
    {
      val k = replica(n).input
      s' = { slots: s.slots.set(k, { read: slot(k).confirmed, ...slot(k) }), ...s }
    },
  }
  // Losing the required subscription slot mandates a new snapshot, even if the
  // replica is offline. It is no longer entitled to historical catch-up.
  action loseSlot(k: int): bool = {
    val n = slot(k).dst
    val lostRequired = replica(n).required == k
    all {
      slot(k).valid,
      s' = {
        slots: s.slots.set(k, { valid: false, ...slot(k) }),
        replicas: if (lostRequired) s.replicas.set(n, {
          needsInit: true, input: -1, required: -1,
          pending: false, buffer: Empty, ...replica(n),
        }) else s.replicas,
        transfers: if (lostRequired) s.transfers.exclude(Set(n)) else s.transfers,
        ...s,
      },
    }
  }
  action loseHistory(n: int): bool = all {
    replica(n).historyValid,
    s' = { replicas: s.replicas.set(n, { historyValid: false, ...replica(n) }), ...s },
  }
  // Abstracts the standard coordinated snapshot + fresh logical slot protocol.
  // Each node may reinitialize once, using a unique slot identity (6 + n).
  action reinitialize(n: int): bool = {
    val r = replica(n)
    val src = s.writer
    val k = 6 + n
    val ownLsn = address(n, r.allocated + 1)
    all {
      n != src, r.up, r.needsInit, idle(src), s.phase == "writing", hasRoom(n),
      not(slot(k).created),
      s' = {
        replicas: s.replicas.set(n, {
          needsInit: false, cursor: replica(src).cursor, rows: replica(src).rows,
          wal: List(), pending: false, buffer: Empty, allocated: r.allocated + 1,
          historyValid: true, historyPos: ownLsn, recycled: ownLsn,
          index: Map(replica(src).cursor -> ownLsn), baseline: replica(src).cursor,
          safe: replica(src).cursor, input: k, required: k, ...r,
        }),
        slots: SlotIds.mapBy(j => if (slot(j).dst == n)
          { valid: false, ...slot(j) } else slot(j))
          .set(k, freshSlot(src, n, lastLsn(src), false)),
        transfers: s.transfers.exclude(Set(n)), ...s,
      },
    }
  }

  action step = {
    nondet n = Nodes.oneOf()
    nondet k = SlotIds.oneOf()
    any {
      write, announceSafe, flush(n), indexWal(n), readStream(k), feedback(k),
      checkpointSlot(k), advanceHistory(n), recycle(n), pauseWrites, seal, promote,
      beginEpoch, cloneHistory(n), advanceClone(n), attachClone(n), finishTransfer(n),
      retireUnusedSlot(k),
      useOriginNone(n), crash(n), restart(n), reconnect(n), loseSlot(k),
      loseHistory(n), reinitialize(n), s' = s,
    }
  }
  // A second exploration schedule keeps resources available, so catastrophic
  // slot loss does not dominate traces before the handoff is exercised.
  action healthyStep = {
    nondet n = Nodes.oneOf()
    nondet k = SlotIds.oneOf()
    any {
      write, announceSafe, flush(n), indexWal(n), readStream(k), feedback(k),
      checkpointSlot(k), advanceHistory(n), recycle(n), pauseWrites, seal, promote,
      beginEpoch, cloneHistory(n), advanceClone(n), attachClone(n), finishTransfer(n),
      retireUnusedSlot(k), useOriginNone(n), reconnect(n), s' = s,
    }
  }

  def expectedRows(w: int): List[int] = s.log
    .select(e => e.w <= w and e.kind == "data")
    .foldl(List(), (xs, e) => xs.append(e.w))
  val durablePrefix = Nodes.forall(n =>
    replica(n).cursor >= 0 and replica(n).cursor <= s.log.length()
    and replica(n).rows == expectedRows(replica(n).cursor))
  val controllerAdmission = s.writeTargets.size() <= 1
    and (s.epoch == 2 implies (s.oldRouteClosed and not(s.writeTargets.contains(0))))
    and (s.phase == "writing" implies s.writeTargets.contains(s.writer))
  val epochPredecessor = s.log.indices().forall(i => {
    val e = s.log.nth(i)
    e.w == i + 1 and (e.epoch == 1 implies e.author == 0)
    and (e.epoch == 2 implies (e.author == 1 and s.end > 0 and e.w > s.end))
  })
  val epochBarrier = (s.epoch == 2 implies replica(1).cursor >= s.end)
    and entries(s.log).forall(e =>
      (e.author == 0 and s.end > 0 implies e.w <= s.end)
      and (e.epoch == 2 and e.kind == "data" implies entries(s.log).exists(b =>
        b.kind == "begin" and b.epoch == 2 and b.w < e.w)))
  val feedbackIsDurable = SlotIds.forall(k => {
    val sl = slot(k)
    not(sl.valid) or entries(replica(sl.src).wal).forall(e =>
      (e.lsn > sl.start and e.lsn <= sl.confirmed
        and (sl.originAny or e.author == sl.src))
      implies e.w <= replica(sl.dst).cursor)
  })
  // Every initialized replica, including an offline one, is protected.
  // A freshly snapshotted donor cannot supply transactions before its baseline.
  val historyProtectsSubscribers = Nodes.forall(src => Nodes.forall(dst => {
    val r = replica(src)
    val d = replica(dst)
    (r.historyValid and not(r.needsInit) and not(d.needsInit)
      and r.baseline <= d.cursor)
    implies entries(r.wal).forall(e => e.w > d.cursor implies e.lsn > r.historyPos)
  }))
  val slotsRetainWal = SlotIds.forall(k => {
    val sl = slot(k)
    not(sl.valid) or entries(replica(sl.src).wal).forall(e =>
      e.lsn > sl.confirmed implies e.lsn > replica(sl.src).recycled)
  })
  val slotBounds = SlotIds.forall(k => {
    val sl = slot(k)
    not(sl.valid) or (sl.start <= sl.restart and sl.restart <= sl.confirmed
      and sl.confirmed <= lastLsn(sl.src))
  })
  val markersAgree = Nodes.forall(n => entries(replica(n).wal).forall(e => {
    val original = s.log.nth(e.w - 1)
    e.original == original.original and e.author == original.author
      and e.epoch == original.epoch and e.kind == original.kind
      and e.floor == original.floor
  }))
  val safety = durablePrefix and controllerAdmission and epochPredecessor and epochBarrier
    and feedbackIsDurable and historyProtectsSubscribers and slotsRetainWal
    and slotBounds and markersAgree

  val reachedNewEpoch = s.epoch == 2 and s.phase == "writing"
  val completedTransfers = s.epoch == 2 and s.transfers == Set()
  val releasedHistory = Nodes.exists(n => replica(n).historyPos > base(n)
    and replica(n).baseline == 0)
  val reinitializedReplica = Nodes.exists(n => replica(n).baseline > 0)

  // Deliberately incorrect actions, EXCLUDED from step. Regression runs show
  // that the invariants reject three tempting shortcuts from the design.
  action badEarlyFeedback(k: int): bool = all {
    slot(k).valid,
    s' = { slots: s.slots.set(k, { confirmed: slot(k).read, ...slot(k) }), ...s },
  }
  action badAdvanceToAnnouncement(n: int): bool = {
    val r = replica(n)
    all {
      r.index.keys().contains(r.cursor),
      s' = { replicas: s.replicas.set(n,
        { historyPos: r.index.get(r.cursor), ...r }), ...s },
    }
  }
  action badAnnounceWithoutInheritance = all {
    s.phase == "writing", commitOriginal("safe", publicationFloor(true)),
  }

  run crashBeforeFlushTest = init.then(write).then(flush(0))
    .then(readStream(0)).then(crash(1)).then(restart(1))
    .expect(replica(1).cursor == 0 and safety)
    .then(readStream(0)).then(flush(1)).expect(replica(1).rows == List(1) and safety)

  run duplicateDeliveryTest = init.then(write).then(flush(0))
    .then(readStream(0)).then(flush(1)).then(reconnect(1)).then(readStream(0))
    .expect(replica(1).rows == List(1) and not(replica(1).pending) and safety)

  run earlyFeedbackCounterexampleTest = init.then(write).then(flush(0))
    .then(readStream(0)).then(badEarlyFeedback(0))
    .expect(not(feedbackIsDurable))

  run announcementPositionCounterexampleTest = init.then(crash(2))
    .then(write).then(flush(0)).then(readStream(0)).then(flush(1))
    .then(feedback(0)).then(announceSafe).then(flush(0))
    .then(readStream(0)).then(flush(1)).then(indexWal(1))
    .expect(replica(1).safe == 0 and safety)
    .then(badAdvanceToAnnouncement(1)).expect(not(historyProtectsSubscribers))

  // B->C's normal slot skips A's data and END; it certifies no apply on C.
  run inheritedBoundCounterexampleTest = init.then(write).then(flush(0))
    .then(readStream(0)).then(flush(1)).then(readStream(2)).then(feedback(2))
    .then(pauseWrites).then(seal).then(flush(0))
    .then(readStream(0)).then(flush(1)).then(readStream(2)).then(feedback(2))
    .then(promote).then(beginEpoch).then(flush(1))
    .expect(s.inheritedFloor == 0 and publicationFloor(true) > 0 and safety)
    .then(badAnnounceWithoutInheritance).then(flush(1)).then(indexWal(1))
    .then(advanceHistory(1)).expect(not(historyProtectsSubscribers))

  run offlineHandoffTest = init.then(crash(2)).then(write).then(flush(0))
    .then(readStream(0)).then(flush(1)).then(feedback(0))
    .then(pauseWrites).then(seal).then(flush(0)).then(readStream(0)).then(flush(1))
    .then(promote).then(beginEpoch).then(flush(1)).then(indexWal(1))
    .then(announceSafe).then(flush(1)).expect(replica(1).safe == 0 and safety)
    .then(restart(2)).then(cloneHistory(2)).then(advanceClone(2)).then(attachClone(2))
    .then(finishTransfer(2)).then(retireUnusedSlot(1)).then(retireUnusedSlot(2))
    .then(readStream(5)).then(flush(2)).then(readStream(5)).then(flush(2))
    .then(useOriginNone(2)).then(readStream(5)).then(flush(2))
    .then(readStream(5)).then(flush(2)).expect(replica(2).cursor == 4 and safety)
    .then(cloneHistory(0)).then(advanceClone(0)).then(attachClone(0))
    .then(finishTransfer(0))
    .then(useOriginNone(0)).then(readStream(3)).then(flush(0))
    .then(readStream(3)).then(flush(0)).then(feedback(3)).then(feedback(5))
    .then(announceSafe).then(flush(1)).then(indexWal(1)).then(advanceHistory(1))
    .expect(s.transfers == Set() and replica(1).safe == 4 and safety)

  run missingSlotReinitializesTest = init.then(write).then(flush(0))
    .then(loseSlot(1)).expect(replica(2).needsInit and safety)
    .then(reinitialize(2)).expect(replica(2).cursor == 1 and safety)

  run crashAfterInstallationTest = init.then(pauseWrites).then(seal).then(flush(0))
    .then(readStream(0)).then(flush(1)).then(promote).then(beginEpoch).then(flush(1))
    .then(cloneHistory(2)).then(advanceClone(2)).then(attachClone(2)).then(crash(2))
    .expect(s.transfers.contains(2) and replica(2).required == 5 and safety)
    .then(restart(2)).then(finishTransfer(2))
    .expect(not(s.transfers.contains(2)) and safety)

  run drainBeforeSealTest = init.then(write).then(pauseWrites)
    .expect(replica(0).pending and s.phase == "paused")
    .then(flush(0)).then(seal).then(flush(0)).expect(s.end == 2 and safety)

  run cannotSealUnflushedTest = init.then(write).then(pauseWrites).then(seal).fail()

  run cannotPromoteBehindTest = init.then(write).then(flush(0))
    .then(pauseWrites).then(seal).then(flush(0)).then(promote).fail()
}
