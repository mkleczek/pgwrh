// Selection-independent authority protocol. See controller-handoff.md for the
// durable-operation/refinement contract. No consensus, leases or election.
module handoff_protocol {
  const NodeCount: int
  const MaxEpoch: int
  const MaxTransactions: int
  const MaxRevision: int
  const MaxRestarts: int
  pure val Nodes = 0.to(NodeCount - 1)
  type Grant = { epoch: int, writer: int }
  type Operation = { epoch: int, source: int, target: int, revision: int }
  type Certificate = { operation: Operation, end: int }
  type Placement = { revision: int, holders: Set[int], desired: int }
  type Transaction = { id: int, epoch: int, writer: int }
  type State = {
    // Durable controller journal, independent of its volatile routing cache.
    active: Grant, phase: str, operation: Operation,
    revision: int, holders: Set[int], desired: int,
    configurations: Set[Placement],
    // Commands remain replayable after restart, delivery or later handoffs.
    closeRequests: Set[Operation], acceptRequests: Set[Certificate],
    openRequests: Set[Operation], authorized: Set[Operation],
    // Durable node-local grants, tombstones, terminal records and promises.
    open: Set[Grant], closed: Set[Grant], begun: Set[Grant],
    seals: Set[Certificate], accepted: Set[Certificate], pins: Set[int],
    resident: Set[int], up: int -> bool, edges: Set[(int, int)],
    data: int -> Set[Transaction], replay: int -> Set[Transaction],
    // Reference history/transaction identities express the abstract ordered
    // apply contract; these are NOT an implementable cross-writer clock.
    history: List[Transaction], pending: Set[Transaction], issued: int,
    acknowledged: Set[Transaction],
    controllerUp: bool, recovered: bool, route: int, routeEpoch: int,
    restarts: int,
    // Ghost reachability flags: never used to authorize a protocol transition.
    crashedInTransition: bool, committedWithoutController: bool,
  }
  var s: State
  pure def oldGrant(op: Operation): Grant = { epoch: op.epoch, writer: op.source }
  pure def newGrant(op: Operation): Grant = { epoch: op.epoch + 1, writer: op.target }
  pure def entries(h: List[Transaction]): Set[Transaction] =
    h.foldl(Set(), (xs, t) => xs.union(Set(t)))
  def committed: Set[Transaction] = entries(s.history)
  def available(n: int): bool = s.resident.contains(n) and s.up.get(n)
  val controllerReady = s.controllerUp and s.recovered
  def covers(n: int): bool = s.data.get(n) == committed
  // Conservative inherited obligation: one consumer can remain at the initial
  // baseline forever. Frontiers/advancing consumer baselines are modeled in
  // obligation_frontiers.qnt; here a promised donor retains its whole prefix.
  def donorReady(n: int): bool = covers(n) and committed.subseteq(s.replay.get(n))
  def prefix(n: int): bool = s.data.get(n).subseteq(committed) and
    s.history.indices().forall(i => s.data.get(n).contains(s.history.nth(i)) implies
      0.to(i).forall(j => s.data.get(n).contains(s.history.nth(j))))
  def certificate(op: Operation): Set[Certificate] = s.seals.filter(c => c.operation == op)
  val currentSeal = certificate(s.operation)
  val transition = s.phase != "writing"

  action init = s' = {
    active: { epoch: 1, writer: 0 }, phase: "writing",
    operation: { epoch: 0, source: 0, target: 0, revision: 0 },
    revision: 0, holders: Set(0, 1), desired: 0,
    configurations: Set({ revision: 0, holders: Set(0, 1), desired: 0 }),
    closeRequests: Set(), acceptRequests: Set(), openRequests: Set(), authorized: Set(),
    open: Set({ epoch: 1, writer: 0 }), closed: Set(),
    begun: Set({ epoch: 1, writer: 0 }), seals: Set(), accepted: Set(), pins: Set(0),
    resident: Set(0, 1), up: Nodes.mapBy(_ => true), edges: Set((0, 1), (1, 0)),
    data: Nodes.mapBy(_ => Set()), replay: Nodes.mapBy(_ => Set()),
    history: List(), pending: Set(), issued: 0, acknowledged: Set(),
    controllerUp: true, recovered: true, route: 0, routeEpoch: 1, restarts: 0,
    crashedInTransition: false, committedWithoutController: false,
  }
  action admit = {
    val t = { id: s.issued + 1, epoch: s.routeEpoch, writer: s.route }
    all {
      controllerReady, Set("writing", "planned").contains(s.phase),
      s.route == s.active.writer, s.routeEpoch == s.active.epoch,
      s.open.contains(s.active), s.issued < MaxTransactions,
      s' = { issued: s.issued + 1, pending: s.pending.union(Set(t)), ...s },
    }
  }
  // Already admitted remote work can commit or abort while the controller is
  // down or its gate is closed. A successful commit here includes local flush.
  action commit(t: Transaction): bool = all {
    s.pending.contains(t), available(t.writer),
    s.open.contains({ epoch: t.epoch, writer: t.writer }),
    not(s.closed.contains({ epoch: t.epoch, writer: t.writer })),
    s' = { history: s.history.append(t), pending: s.pending.exclude(Set(t)),
      committedWithoutController: s.committedWithoutController or not(s.controllerUp),
      data: s.data.set(t.writer, s.data.get(t.writer).union(Set(t))),
      replay: s.replay.set(t.writer, s.replay.get(t.writer).union(Set(t))), ...s },
  }
  action abort(t: Transaction): bool = all {
    s.pending.contains(t), available(t.writer),
    s' = { pending: s.pending.exclude(Set(t)), ...s },
  }
  action acknowledge(t: Transaction): bool = all {
    controllerReady, committed.contains(t),
    Nodes.exists(n => available(n) and s.data.get(n).contains(t)),
    s' = { acknowledged: s.acknowledged.union(Set(t)), ...s },
  }

  // Desired assignment is an INPUT (WRH in the runtime). Neither a revision nor
  // a different desired node modifies admission, grants, streams or retention.
  action changeTopology(holders: Set[int], desired: int): bool = all {
    controllerReady, s.revision < MaxRevision,
    holders.subseteq(Nodes), holders.contains(desired),
    holders != s.holders or desired != s.desired,
    s' = { revision: s.revision + 1, holders: holders, desired: desired,
      configurations: s.configurations.union(Set({ revision: s.revision + 1,
        holders: holders, desired: desired })), ...s },
  }
  // Deliberately any authorized candidate, not just desired/rank one. This
  // over-approximates selection policies. Frozen intent survives later revisions.
  action request(target: int): bool = all {
    controllerReady, s.phase == "writing", s.active.epoch < MaxEpoch,
    s.holders.contains(target), target != s.active.writer,
    s' = { phase: "planned", operation: { epoch: s.active.epoch,
      source: s.active.writer, target: target, revision: s.revision }, ...s },
  }
  action cancel = all {
    controllerReady, s.phase == "planned",
    // No commands have been emitted. Reusing the epoch after close is forbidden.
    s' = { phase: "writing", ...s },
  }
  action closeAdmission = all {
    controllerReady, s.phase == "planned",
    s' = { phase: "closing", route: -1, routeEpoch: 0, ...s },
  }
  action sendClose = all {
    controllerReady, s.phase == "closing",
    s' = { closeRequests: s.closeRequests.union(Set(s.operation)), ...s },
  }
  action closeWriter(op: Operation): bool = all {
    s.closeRequests.contains(op), available(op.source),
    s.open.contains(oldGrant(op)),
    // A concrete connection/backend barrier must establish this condition. A
    // controller restart must not interpret a lost connection as a drained tx.
    not(s.pending.exists(t => t.epoch == op.epoch and t.writer == op.source)),
    s' = { open: s.open.exclude(Set(oldGrant(op))),
      closed: s.closed.union(Set(oldGrant(op))),
      seals: s.seals.union(Set({ operation: op, end: s.history.length() })), ...s },
  }
  action observeSeal = all {
    controllerReady, s.phase == "closing", currentSeal.size() == 1,
    s' = { phase: "sealed", ...s },
  }
  action sendAccept = all {
    controllerReady, s.phase == "sealed",
    s' = { acceptRequests: s.acceptRequests.union(currentSeal), ...s },
  }
  action accept(c: Certificate): bool = {
    val n = c.operation.target
    all {
      s.acceptRequests.contains(c), s.seals.contains(c), available(n),
      // Data alone (e.g. snapshot bootstrap) does not establish donor history.
      donorReady(n), s.history.length() == c.end,
      s' = { accepted: s.accepted.union(Set(c)), pins: s.pins.union(Set(n)), ...s },
    }
  }
  // Persist the new authority before emitting an OPEN command. Restart can
  // recover an uncertain activation outcome from this journal, without election.
  action activate = all {
    controllerReady, s.phase == "sealed",
    currentSeal.exists(c => s.accepted.contains(c)),
    s.pins.contains(s.operation.target), donorReady(s.operation.target),
    s' = { active: newGrant(s.operation), phase: "opening",
      authorized: s.authorized.union(Set(s.operation)), ...s },
  }
  action sendOpen = all {
    controllerReady, s.phase == "opening",
    s' = { openRequests: s.openRequests.union(Set(s.operation)), ...s },
  }
  action openWriter(op: Operation): bool = all {
    s.openRequests.contains(op), available(op.target),
    certificate(op).exists(c => s.accepted.contains(c)),
    s.closed.contains(oldGrant(op)),
    // Durable node-local tombstones reject stale OPEN after this epoch closed,
    // even when the same node has become writer again in a later epoch.
    not(s.closed.contains(newGrant(op))),
    s' = { open: s.open.union(Set(newGrant(op))),
      begun: s.begun.union(Set(newGrant(op))), ...s },
  }
  action finish = all {
    controllerReady, s.phase == "opening", s.open.contains(s.active),
    s' = { phase: "writing", route: s.active.writer, routeEpoch: s.active.epoch, ...s },
  }

  action controllerCrash = all {
    s.controllerUp, s.restarts < MaxRestarts,
    s' = { controllerUp: false, recovered: false, route: -1, routeEpoch: 0,
      crashedInTransition: s.crashedInTransition or transition,
      restarts: s.restarts + 1, ...s },
  }
  action controllerRestart = all {
    not(s.controllerUp), s' = { controllerUp: true, ...s },
  }
  action recover = all {
    s.controllerUp, not(s.recovered),
    s' = { recovered: true,
      route: if (Set("writing", "planned").contains(s.phase) and s.open.contains(s.active))
        s.active.writer else -1,
      routeEpoch: if (Set("writing", "planned").contains(s.phase) and s.open.contains(s.active))
        s.active.epoch else 0, ...s },
  }
  action crash(n: int): bool = all {
    s.up.get(n), s' = { up: s.up.set(n, false), ...s },
  }
  action restart(n: int): bool = all {
    not(s.up.get(n)), s' = { up: s.up.set(n, true), ...s },
  }
  action addCopy(n: int): bool = all {
    controllerReady, not(s.resident.contains(n)),
    s.holders.contains(n) or (transition and s.operation.target == n),
    s' = { resident: s.resident.union(Set(n)), ...s },
  }
  action installEdge(src: int, dst: int): bool = all {
    controllerReady, src != dst, s.resident.contains(src), s.resident.contains(dst),
    s' = { edges: s.edges.union(Set((src, dst))), ...s },
  }
  action removeEdge(src: int, dst: int): bool = all {
    controllerReady, s.edges.contains((src, dst)),
    s' = { edges: s.edges.exclude(Set((src, dst))), ...s },
  }
  // Abstract gap-free, ordered, deduplicated data apply and history repair.
  action replicate(src: int, dst: int): bool = {
    val missing = s.history.select(t => not(s.data.get(dst).contains(t)))
    all {
      available(src), available(dst), s.edges.contains((src, dst)), missing.length() > 0,
      {
        val t = missing.head()
        all {
          s.replay.get(src).contains(t),
          s' = { data: s.data.set(dst, s.data.get(dst).union(Set(t))),
            replay: s.replay.set(dst, s.replay.get(dst).union(Set(t))), ...s },
        }
      },
    }
  }
  action repairHistory(src: int, dst: int): bool = all {
    available(src), available(dst), s.edges.contains((src, dst)),
    s.replay.get(src).subseteq(s.data.get(dst)),
    s' = { replay: s.replay.set(dst, s.replay.get(dst).union(s.replay.get(src))), ...s },
  }
  action snapshot(src: int, dst: int): bool = all {
    available(src), available(dst), src != dst, not(s.pins.contains(dst)),
    not(s.open.exists(g => g.writer == dst)),
    s' = { data: s.data.set(dst, s.data.get(src)), replay: s.replay.set(dst, Set()), ...s },
  }
  action releasePin(n: int): bool = all {
    controllerReady, s.pins.contains(n), n != s.active.writer,
    not(transition and (s.operation.source == n or s.operation.target == n)),
    s.pins.contains(s.active.writer), donorReady(s.active.writer),
    s' = { pins: s.pins.exclude(Set(n)), ...s },
  }
  action collect(n: int): bool = all {
    available(n), not(s.pins.contains(n)),
    s' = { replay: s.replay.set(n, Set()), ...s },
  }
  action dropCopy(n: int): bool = all {
    controllerReady, s.resident.contains(n), not(s.holders.contains(n)),
    n != s.active.writer, not(s.pins.contains(n)),
    not(s.open.exists(g => g.writer == n)),
    not(transition and (s.operation.source == n or s.operation.target == n)),
    s' = { resident: s.resident.exclude(Set(n)),
      edges: s.edges.filter(edge => edge._1 != n and edge._2 != n),
      data: s.data.set(n, Set()), replay: s.replay.set(n, Set()), ...s },
  }

  val uniqueAuthority = s.open.size() <= 1 and s.open.forall(g => g == s.active)
  val closedIsFinal = s.open.intersect(s.closed) == Set() and
    s.pending.forall(t => not(s.closed.contains({ epoch: t.epoch, writer: t.writer })))
  val routeIsAuthorized = s.route == -1 or (controllerReady and
    Set("writing", "planned").contains(s.phase) and s.route == s.active.writer and
    s.routeEpoch == s.active.epoch and s.open.contains(s.active))
  val dataIsPrefix = Nodes.forall(n => prefix(n) and s.replay.get(n).subseteq(s.data.get(n)))
  val activeHasCoverage = s.resident.contains(s.active.writer) and donorReady(s.active.writer)
    and s.pins.contains(s.active.writer)
  val pinsProtectHistory = s.pins.forall(n => s.resident.contains(n) and
    s.data.get(n).subseteq(s.replay.get(n)))
  val acknowledgementsAreDurable = s.acknowledged.subseteq(committed) and
    s.acknowledged.subseteq(s.data.get(s.active.writer))
  val epochsAreUnique = s.authorized.forall(a => s.authorized.forall(b =>
    a.epoch != b.epoch or a == b))
  val grantsHaveCertificates = s.begun.forall(g => g.epoch == 1 or
    s.authorized.exists(op => newGrant(op) == g and s.closed.contains(oldGrant(op)) and
      certificate(op).exists(c => s.accepted.contains(c))))
  val requestsAreAuthorized = s.closeRequests.union(s.openRequests).union(s.authorized)
    .forall(op => op.source != op.target and s.begun.contains(oldGrant(op)) and
      s.configurations.exists(cfg => cfg.revision == op.revision and cfg.holders.contains(op.target)))
  val sealsAreTerminal = s.seals.forall(c => s.closed.contains(oldGrant(c.operation)) and
    c.end <= s.history.length() and s.history.indices().forall(i => {
      val t = s.history.nth(i)
      t.epoch == c.operation.epoch implies i < c.end
    }))
  val transactionIdentity = committed.size() == s.history.length() and
    committed.forall(t => s.begun.contains({ epoch: t.epoch, writer: t.writer })) and
    s.pending.forall(t => t.epoch == s.active.epoch and t.writer == s.active.writer)
  val safety = uniqueAuthority and closedIsFinal and routeIsAuthorized and dataIsPrefix
    and activeHasCoverage and pinsProtectHistory and acknowledgementsAreDurable
    and epochsAreUnique and grantsHaveCertificates and requestsAreAuthorized
    and sealsAreTerminal and transactionIdentity

  val repeatedHandoff = s.active.epoch >= 3
  val returnedWriter = s.active.epoch >= 3 and s.active.writer == 0
  val topologyDuringHandoff = transition and s.revision > s.operation.revision
  val restartedDuringHandoff = s.crashedInTransition and s.controllerUp and s.recovered
  val commitWhileControllerDown = s.committedWithoutController
  val completedAcrossRevision = s.phase == "writing" and s.active.epoch > 1
    and s.revision > s.operation.revision

  // The full finite asynchronous relation: no forced writer sequence, handoff
  // phase, failure order, topology schedule or round-robin selection.
  action step = {
    nondet n = Nodes.oneOf()
    nondet dst = Nodes.oneOf()
    any {
      admit, request(n), cancel, closeAdmission, sendClose, observeSeal,
      sendAccept, activate, sendOpen, finish,
      controllerCrash, controllerRestart, recover, crash(n), restart(n),
      addCopy(n), dropCopy(n), installEdge(n, dst), removeEdge(n, dst),
      replicate(n, dst), repairHistory(n, dst), snapshot(n, dst), releasePin(n), collect(n),
      { nondet hs = Nodes.powerset().oneOf(); changeTopology(hs, n) },
      { nondet t = s.pending.oneOf(); any { commit(t), abort(t) } },
      { nondet t = committed.oneOf(); acknowledge(t) },
      { nondet op = s.closeRequests.oneOf(); closeWriter(op) },
      { nondet c = s.acceptRequests.oneOf(); accept(c) },
      { nondet op = s.openRequests.oneOf(); openWriter(op) },
      s' = s,
    }
  }
  // Helpers are used only in directed tests, never as atomic exploration steps.
  action sealCurrent = closeWriter(s.operation)
  action acceptCurrent = { nondet c = currentSeal.oneOf(); accept(c) }
  action openCurrent = openWriter(s.operation)
  action commitFirst = { nondet t = s.pending.oneOf(); commit(t) }
  action recoverController = controllerCrash.then(controllerRestart).then(recover)
  action prepare(n: int): bool = request(n).then(closeAdmission).then(sendClose)
    .then(sealCurrent).then(observeSeal).then(sendAccept)
  action complete = acceptCurrent.then(activate).then(sendOpen).then(openCurrent).then(finish)
  action move(n: int): bool = prepare(n).then(complete)
}

module controller_handoff {
  import handoff_protocol(NodeCount = 4, MaxEpoch = 4, MaxTransactions = 4,
    MaxRevision = 3, MaxRestarts = 3).*

  run arbitraryCandidateTest = init.then(changeTopology(Set(0, 1, 2), 2))
    .then(move(1)).expect(s.active.writer == 1 and s.desired == 2 and safety)

  run desiredChangeDoesNotRouteTest = init.then(changeTopology(Set(1, 2), 2))
    .then(admit).then(commitFirst)
    .expect(s.active.writer == 0 and s.route == 0 and s.desired == 2 and safety)

  run repeatedHandoffWithOfflineConsumerTest = init.then(crash(3))
    .then(changeTopology(Set(0, 1, 2), 1)).then(addCopy(2))
    .then(installEdge(0, 2)).then(installEdge(1, 2)).then(installEdge(2, 0))
    .then(admit).then(commitFirst).then(replicate(0, 1)).then(replicate(0, 2))
    .then(move(1)).then(admit).then(commitFirst).then(replicate(1, 2))
    .then(move(2)).then(admit).then(commitFirst)
    .then(replicate(2, 0)).then(replicate(2, 0)).then(move(0))
    .then(admit).then(commitFirst)
    .expect(s.active.epoch == 4 and s.active.writer == 0 and
      not(s.up.get(3)) and committed.size() == 4 and safety)

  run repeatedHandoffTopologyAndRestartTogetherTest = init.then(crash(3))
    .then(changeTopology(Set(0, 1, 2), 1)).then(addCopy(2))
    .then(installEdge(0, 2)).then(installEdge(1, 2)).then(installEdge(2, 0))
    .then(admit).then(commitFirst).then(replicate(0, 1)).then(replicate(0, 2))
    .then(prepare(1)).then(changeTopology(Set(0, 1, 2), 2))
    .then(recoverController).then(complete).then(admit).then(commitFirst)
    .then(replicate(1, 2)).then(prepare(2)).then(recoverController).then(complete)
    .then(admit).then(commitFirst).then(replicate(2, 0)).then(replicate(2, 0))
    .then(prepare(0)).then(changeTopology(Set(0, 1), 0))
    .then(recoverController).then(complete).then(admit).then(commitFirst)
    .expect(s.active.epoch == 4 and s.active.writer == 0 and s.restarts == 3 and
      s.revision == 3 and not(s.up.get(3)) and committed.size() == 4 and safety)

  run topologyChangeAfterRequestTest = init.then(request(1))
    .then(changeTopology(Set(0, 2), 2)).then(closeAdmission).then(sendClose)
    .then(sealCurrent).then(observeSeal).then(sendAccept).then(complete)
    .expect(s.active.writer == 1 and s.desired == 2 and not(s.holders.contains(1)) and safety)

  run topologyChangeAfterSealTest = init.then(prepare(1))
    .then(changeTopology(Set(0, 2), 2)).then(complete)
    .expect(s.active.writer == 1 and s.operation.revision == 0 and s.revision == 1 and safety)

  run controllerRestartsAtThreeBoundariesTest = init.then(request(1)).then(recoverController)
    .then(closeAdmission).then(sendClose).then(sealCurrent).then(recoverController)
    .then(observeSeal).then(sendAccept).then(acceptCurrent).then(activate)
    .then(sendOpen).then(openCurrent).then(recoverController).then(finish)
    .expect(s.active.writer == 1 and s.restarts == 3 and safety)

  run pendingCommitSurvivesControllerCrashTest = init.then(admit).then(request(1))
    .then(closeAdmission).then(sendClose).then(controllerCrash).then(commitFirst)
    .then(sealCurrent).then(controllerRestart).then(recover).then(observeSeal)
    .then(replicate(0, 1)).then(sendAccept).then(complete)
    .expect(committed.size() == 1 and s.data.get(1) == committed and safety)

  run controllerCannotForgetPendingWorkTest = init.then(admit).then(request(1))
    .then(closeAdmission).then(sendClose).then(recoverController).then(sealCurrent).fail()

  run noAdmissionBeforeRecoveryTest = init.then(controllerCrash).then(controllerRestart)
    .then(admit).fail()

  run closedAdmissionStaysClosedAfterRestartTest = init.then(request(1)).then(closeAdmission)
    .then(recoverController).expect(s.route == -1 and safety).then(admit).fail()

  run recoveryUsesJournalInsteadOfDesiredTest = init.then(changeTopology(Set(1), 1))
    .then(recoverController).then(admit).then(commitFirst)
    .expect(s.active.writer == 0 and s.route == 0 and s.desired == 1 and safety)

  run lostOpenReplyTest = init.then(prepare(1)).then(acceptCurrent).then(activate)
    .then(sendOpen).then(controllerCrash).then(openCurrent)
    .then(controllerRestart).then(recover).expect(s.route == -1 and safety)
    .then(finish).then(admit).then(commitFirst).expect(safety)

  run controllerCrashBeforeSendingOpenTest = init.then(prepare(1)).then(acceptCurrent)
    .then(activate).then(recoverController).then(sendOpen).then(openCurrent).then(finish)
    .expect(s.active.writer == 1 and safety)

  run oldWriterGoneAfterSealTest = init.then(admit).then(commitFirst).then(replicate(0, 1))
    .then(prepare(1)).then(crash(0)).then(recoverController).then(complete)
    .then(admit).then(commitFirst).expect(not(s.up.get(0)) and committed.size() == 2 and safety)

  run unavailableUnsealedWriterBlocksTest = init.then(request(1)).then(closeAdmission)
    .then(sendClose).then(crash(0)).then(sealCurrent).fail()

  run cannotCancelAfterClosureTest = init.then(request(1)).then(closeAdmission).then(cancel).fail()
  run cancelBeforeClosureTest = init.then(request(1)).then(cancel).then(request(1))
    .then(closeAdmission).then(sendClose).then(sealCurrent).then(observeSeal)
    .then(sendAccept).then(complete).expect(safety)
  run overlappingHandoffRejectedTest = init.then(request(1)).then(request(1)).fail()

  run duplicateOpenIsIdempotentTest = init.then(prepare(1)).then(acceptCurrent)
    .then(activate).then(sendOpen).then(openCurrent).then(openCurrent).then(finish).expect(safety)

  run staleOpenRejectedAfterWriterReturnTest = init.then(move(1)).then(move(0))
    .then(openWriter({ epoch: 1, source: 0, target: 1, revision: 0 })).fail()

  run staleCloseRejectedAfterWriterReturnTest = init.then(move(1)).then(move(0))
    .then(closeWriter({ epoch: 1, source: 0, target: 1, revision: 0 })).fail()

  run copyRemovalDoesNotEraseWriterTombstoneTest = init.then(move(1)).then(move(0))
    .then(changeTopology(Set(0), 0)).then(releasePin(1)).then(dropCopy(1))
    .then(changeTopology(Set(0, 1), 1)).then(addCopy(1))
    .then(openWriter({ epoch: 1, source: 0, target: 1, revision: 0 })).fail()

  run catchupNeedsStreamTest = init.then(removeEdge(0, 1)).then(admit).then(commitFirst)
    .then(prepare(1)).then(acceptCurrent).fail()

  run streamRemovalAndRepairTest = init.then(admit).then(commitFirst).then(removeEdge(0, 1))
    .then(prepare(1)).then(recoverController).then(installEdge(0, 1))
    .then(replicate(0, 1)).then(complete).expect(safety)

  run snapshotNeedsHistoryTest = init.then(admit).then(commitFirst).then(snapshot(0, 1))
    .then(prepare(1)).then(acceptCurrent).fail()

  run snapshotHistoryRepairTest = init.then(admit).then(commitFirst).then(snapshot(0, 1))
    .then(prepare(1)).then(repairHistory(0, 1)).then(complete).expect(safety)

  run removedDonorMustTransferProtectionTest = init.then(admit).then(commitFirst)
    .then(replicate(0, 1)).then(move(1)).then(changeTopology(Set(1), 1))
    .then(releasePin(0)).then(collect(0)).then(dropCopy(0))
    .expect(not(s.resident.contains(0)) and donorReady(1) and safety)

  run activeWriterCannotBeRemovedByTopologyTest = init.then(changeTopology(Set(1), 1))
    .then(dropCopy(0)).fail()
  run activeHistoryCannotBeCollectedTest = init.then(admit).then(commitFirst).then(collect(0)).fail()
  run pinnedDonorCannotBeDroppedTest = init.then(move(1)).then(changeTopology(Set(1), 1))
    .then(dropCopy(0)).fail()

  // Mutation scenarios below deliberately bypass one protocol guard. They are
  // excluded from step; each must falsify the property that guard protects.
  action badRecoverFromDesired = s' = { route: s.desired,
    routeEpoch: s.active.epoch, recovered: true, ...s }
  run rankIsNotAuthorityCounterexampleTest = init.then(changeTopology(Set(1), 1))
    .then(controllerCrash).then(controllerRestart).then(badRecoverFromDesired)
    .expect(not(routeIsAuthorized))

  action badSealWithPending = s' = { closed: s.closed.union(Set(s.active)),
    open: Set(), ...s }
  run forgottenInflightCounterexampleTest = init.then(admit).then(badSealWithPending)
    .expect(not(closedIsFinal))

  action badReplayOpen = s' = { open: s.open.union(Set({ epoch: 2, writer: 1 })), ...s }
  run staleOpenCounterexampleTest = init.then(move(1)).then(move(0)).then(badReplayOpen)
    .expect(not(uniqueAuthority) and not(closedIsFinal))

  action badActivateWithoutSeal = s' = { active: { epoch: 2, writer: 1 },
    open: s.open.union(Set({ epoch: 2, writer: 1 })), ...s }
  run unfencedActivationCounterexampleTest = init.then(badActivateWithoutSeal)
    .expect(not(uniqueAuthority))

  action badSnapshotActivation = s' = { active: newGrant(s.operation),
    pins: s.pins.union(Set(s.operation.target)), ...s }
  run dataIsNotReplayCounterexampleTest = init.then(admit).then(commitFirst)
    .then(snapshot(0, 1)).then(prepare(1)).then(badSnapshotActivation)
    .expect(not(activeHasCoverage) and not(pinsProtectHistory))

  action badRemovePinnedCopy = s' = { resident: s.resident.exclude(Set(0)),
    replay: s.replay.set(0, Set()), ...s }
  run removalBeforeTransferCounterexampleTest = init.then(admit).then(commitFirst)
    .then(badRemovePinnedCopy).expect(not(activeHasCoverage) and not(pinsProtectHistory))
}

// Full-relation TLC instances, not restricted phase schedules. Kept small
// enough to exhaust; the default instance has broader bounds for simulation.
module handoff_exhaustive {
  import handoff_protocol(NodeCount = 2, MaxEpoch = 3, MaxTransactions = 2,
    MaxRevision = 1, MaxRestarts = 1).*
}

module handoff_three_node_exhaustive {
  import handoff_protocol(NodeCount = 3, MaxEpoch = 4, MaxTransactions = 0,
    MaxRevision = 1, MaxRestarts = 1).*
}
