// Companion abstraction to writer_handoff.qnt, not a replacement WAL model.
// Durable local effects and one ordered apply path are atomic here. README.md
// states the refinement boundary; none of the ghost history is a runtime API.
module obligation_frontiers {
  pure val Shards = Set(0, 1)
  pure val Nodes = Set(0, 1, 2, 3)
  pure val Resources = 0.to(7)
  pure val MaxEpoch = 4
  pure val MaxWrites = 2
  type Point = { shard: int, epoch: int, author: int, original: int }
  type Claim = { resource: int, generation: int, baseline: Point }
  type Pin = { claim: Claim, donor: int, generation: int, round: int }
  type Receipt = { resource: int, generation: int, point: Point }
  type Seal = { shard: int, epoch: int, terminal: Point }
  type Acceptance = { shard: int, epoch: int, donor: int, generation: int, terminal: Point }
  type State = {
    epoch: int -> int, phase: int -> str, writes: int -> int,
    // Ghost histories and transaction-label sets model row-prefix assertions.
    history: int -> List[Point], data: int -> Set[Point], replay: int -> Set[Point],
    generation: int -> int, up: int -> bool,
    claims: Set[Claim], retired: Set[Claim], pins: Set[Pin], released: Set[Pin],
    emitted: Set[Receipt], known: int -> Set[Receipt],
    seals: Set[Seal], links: int -> Set[Seal], accepted: Set[Acceptance],
    // Authority decisions are serialized by the sole controller, not a CRDT.
    started: Set[(int, int)],
  }
  var s: State

  pure def key(sh: int, n: int): int = 4 * sh + n
  pure def shardOf(r: int): int = r / 4
  pure def nodeOf(r: int): int = r % 4
  pure def owner(sh: int, e: int): int = (e + sh - 1) % 3
  pure def zero(sh: int): Point = { shard: sh, epoch: 0, author: -1, original: 0 }
  // Different source WAL spaces, gaps, and writer reuse. No cross-source max.
  pure def point(sh: int, e: int, i: int): Point = {
    shard: sh, epoch: e, author: owner(sh, e),
    original: 1000 * (owner(sh, e) + 1) + 100 * e + 7 * i,
  }
  pure def initialClaim(r: int): Claim = {
    resource: r, generation: 1, baseline: zero(shardOf(r)),
  }
  pure def initialPin(r: int, n: int): Pin = {
    claim: initialClaim(r), donor: n, generation: 1, round: 1,
  }
  pure def entries(xs: List[Point]): Set[Point] = xs.foldl(Set(), (a, p) => a.union(Set(p)))
  pure def chainKnown(sh: int, e: int, links: Set[Seal]): bool =
    1.to(e - 1).forall(k => links.exists(l => l.shard == sh and l.epoch == k))
  pure def le(a: Point, b: Point, links: Set[Seal]): bool =
    a.shard == b.shard and (
      a == b or a.epoch == 0 or
      (a.epoch == b.epoch and a.author == b.author and a.original <= b.original) or
      (a.epoch > 0 and a.epoch < b.epoch and
        a.epoch.to(b.epoch - 1).forall(e => links.exists(l =>
          l.shard == a.shard and l.epoch == e and
          (e != a.epoch or (l.terminal.epoch == a.epoch and
            l.terminal.author == a.author and a.original <= l.terminal.original))))))
  def writer(sh: int): int = owner(sh, s.epoch.get(sh))
  def terminal(sh: int): Point = s.seals
    .filter(l => l.shard == sh and l.epoch == s.epoch.get(sh))
    .fold(zero(sh), (_, l) => l.terminal)
  def liveClaims: Set[Claim] = s.claims.exclude(s.retired)
  def livePin(p: Pin): bool = not(s.released.contains(p)) and
    liveClaims.contains(p.claim) and
    s.generation.get(key(shardOf(p.claim.resource), p.donor)) == p.generation
  def pinsAt(sh: int, n: int): Set[Pin] = s.pins.filter(p =>
    livePin(p) and shardOf(p.claim.resource) == sh and p.donor == n)
  def tip(r: int): Point = s.history.get(shardOf(r))
    .select(p => s.data.get(r).contains(p)).foldl(zero(shardOf(r)), (_, p) => p)
  def isPrefix(r: int): bool = {
    val h = s.history.get(shardOf(r))
    s.data.get(r).subseteq(entries(h)) and h.indices().forall(i =>
      s.data.get(r).contains(h.nth(i)) implies
        0.to(i).forall(j => s.data.get(r).contains(h.nth(j))))
  }
  def floorAt(n: int, c: Claim): Point = s.known.get(n)
    .filter(a => a.resource == c.resource and a.generation == c.generation and
      chainKnown(a.point.shard, a.point.epoch, s.links.get(n)))
    .fold(c.baseline, (p, a) => if (le(p, a.point, s.links.get(n))) a.point else p)
  def bounds(n: int): Set[Point] = s.pins.filter(p => livePin(p) and p.donor == n)
    .map(p => floorAt(n, p.claim))
  // Minimal mutually incomparable lower bounds. Shard identifiers are names,
  // not an ordered timestamp dimension. Missing links preserve incomparability.
  def frontier(n: int): Set[Point] = {
    val bs = bounds(n)
    bs.filter(q => not(bs.exists(p => p != q and le(p, q, s.links.get(n)))))
  }
  def needed(n: int, p: Point): bool = frontier(n).exists(f =>
    f.shard == p.shard and not(le(p, f, s.links.get(n))))
  def neededByPins(n: int, p: Point): bool = s.pins.exists(pin =>
    livePin(pin) and pin.donor == n and shardOf(pin.claim.resource) == p.shard and
      not(le(p, floorAt(n, pin.claim), s.links.get(n))))
  def pinFor(c: Claim, n: int): bool = s.pins.exists(p =>
    livePin(p) and p.claim == c and p.donor == n)
  def coversClaim(c: Claim, n: int): bool = {
    val r = key(shardOf(c.resource), n)
    s.data.get(r).exclude(s.data.get(c.resource)).subseteq(s.replay.get(r))
  }
  def ready(sh: int, n: int): bool =
    s.data.get(key(sh, n)) == entries(s.history.get(sh)) and
    liveClaims.filter(c => shardOf(c.resource) == sh).forall(c => pinFor(c, n) and coversClaim(c, n))

  pure def initialState(offline: bool): State = {
    epoch: Shards.mapBy(_ => 1), phase: Shards.mapBy(_ => "writing"),
    writes: Shards.mapBy(_ => 0), history: Shards.mapBy(_ => List()),
    data: Resources.mapBy(_ => Set()), replay: Resources.mapBy(_ => Set()),
    generation: Resources.mapBy(_ => 1), up: Nodes.mapBy(n => not(offline and n == 3)),
    claims: Resources.map(r => initialClaim(r)), retired: Set(),
    // Every prospective donor starts with a history anchor. Unlike a snapshot,
    // it retains incoming history before making a donor promise.
    pins: Resources.map(r => Nodes.map(n => initialPin(r, n))).flatten(), released: Set(),
    emitted: Set(), known: Nodes.mapBy(_ => Set()), seals: Set(),
    links: Nodes.mapBy(_ => Set()), accepted: Set(),
    started: Shards.map(sh => (sh, 1)),
  }
  action init = s' = initialState(false)
  action offlineInit = s' = initialState(true)
  action write(sh: int): bool = {
    val n = writer(sh)
    val r = key(sh, n)
    val p = point(sh, s.epoch.get(sh), s.writes.get(sh) + 1)
    all {
      s.phase.get(sh) == "writing", s.up.get(n), ready(sh, n),
      s.writes.get(sh) < MaxWrites,
      s' = { history: s.history.set(sh, s.history.get(sh).append(p)),
        writes: s.writes.set(sh, s.writes.get(sh) + 1),
        data: s.data.set(r, s.data.get(r).union(Set(p))),
        replay: s.replay.set(r, s.replay.get(r).union(Set(p))), ...s },
    }
  }
  // Refinement assumption: source stream order, predecessor completion, row
  // effects and durable cursor update are supplied by F07 / writer_handoff.qnt.
  action applyNext(sh: int, src: int, dst: int): bool = {
    val r = key(sh, dst)
    val missing = s.history.get(sh).select(p => not(s.data.get(r).contains(p)))
    all {
      src != dst, s.up.get(src), s.up.get(dst), missing.length() > 0,
      {
        val p = missing.head()
        all {
          s.replay.get(key(sh, src)).contains(p),
          s' = { data: s.data.set(r, s.data.get(r).union(Set(p))),
            replay: s.replay.set(r, s.replay.get(r).union(Set(p))), ...s },
        }
      },
    }
  }
  action publish(r: int): bool = {
    val a = { resource: r, generation: s.generation.get(r), point: tip(r) }
    all {
      s.up.get(nodeOf(r)), isPrefix(r),
      s' = { emitted: s.emitted.union(Set(a)), ...s },
    }
  }
  action deliver(n: int, a: Receipt): bool = all {
    s.up.get(n), s.emitted.contains(a),
    s' = { known: s.known.set(n, s.known.get(n).union(Set(a))), ...s },
  }
  action gossip(src: int, dst: int): bool = all {
    s.up.get(src), s.up.get(dst),
    s' = { known: s.known.set(dst, s.known.get(dst).union(s.known.get(src))),
      links: s.links.set(dst, s.links.get(dst).union(s.links.get(src))), ...s },
  }
  action deliverSeal(n: int, l: Seal): bool = all {
    s.up.get(n), s.seals.contains(l),
    s' = { links: s.links.set(n, s.links.get(n).union(Set(l))), ...s },
  }
  action seal(sh: int): bool = {
    val n = writer(sh)
    val l = { shard: sh, epoch: s.epoch.get(sh), terminal: tip(key(sh, n)) }
    all {
      s.phase.get(sh) == "writing", s.up.get(n), s.epoch.get(sh) < MaxEpoch,
      // Model closure/drain and durable END as one abstract transition.
      s' = { phase: s.phase.set(sh, "sealed"), seals: s.seals.union(Set(l)),
        links: s.links.set(n, s.links.get(n).union(Set(l))), ...s },
    }
  }
  action acquire(c: Claim, n: int): bool = {
    val sh = shardOf(c.resource)
    val p = { claim: c, donor: n, generation: s.generation.get(key(sh, n)), round: s.epoch.get(sh) }
    all {
      liveClaims.contains(c), s.up.get(n), coversClaim(c, n),
      not(s.released.contains(p)),
      s' = { pins: s.pins.union(Set(p)), ...s },
    }
  }
  action accept(sh: int): bool = {
    val n = owner(sh, s.epoch.get(sh) + 1)
    val a = { shard: sh, epoch: s.epoch.get(sh) + 1, donor: n,
      generation: s.generation.get(key(sh, n)), terminal: terminal(sh) }
    all {
      s.phase.get(sh) == "sealed", s.up.get(n), ready(sh, n),
      s' = { accepted: s.accepted.union(Set(a)), ...s },
    }
  }
  action activate(sh: int): bool = {
    val e = s.epoch.get(sh) + 1
    val n = owner(sh, e)
    all {
      s.phase.get(sh) == "sealed", e <= MaxEpoch, s.up.get(n), ready(sh, n),
      s.accepted.exists(a => a.shard == sh and a.epoch == e and a.donor == n and
        a.generation == s.generation.get(key(sh, n)) and a.terminal == tip(key(sh, n))),
      s' = { epoch: s.epoch.set(sh, e), phase: s.phase.set(sh, "writing"),
        writes: s.writes.set(sh, 0), started: s.started.union(Set((sh, e))), ...s },
    }
  }
  action release(p: Pin): bool = {
    val sh = shardOf(p.claim.resource)
    all {
      s.pins.contains(p), livePin(p), p.donor != writer(sh),
      // A *durable* replacement promise must already exist. Availability or
      // a received-but-uncommitted acquisition command is not a replacement.
      pinFor(p.claim, writer(sh)), ready(sh, writer(sh)),
      s' = { released: s.released.union(Set(p)), ...s },
    }
  }
  action collect(n: int): bool = {
    val fs = frontier(n)
    all {
      s.up.get(n),
      // One node-wide collection accounts for BOTH shards sharing its storage.
      s' = { replay: Resources.mapBy(r => if (nodeOf(r) == n)
        s.replay.get(r).filter(p => fs.exists(f => f.shard == p.shard and
          not(le(p, f, s.links.get(n))))) else s.replay.get(r)), ...s },
    }
  }
  // Repair replayable history, not merely current rows. A real implementation
  // must transfer original identities and recreate local mappings/anchors.
  action repairHistory(sh: int, src: int, dst: int): bool = all {
    src != dst, s.up.get(src), s.up.get(dst), ready(sh, src),
    s.replay.get(key(sh, src)).subseteq(s.data.get(key(sh, dst))),
    s' = { replay: s.replay.set(key(sh, dst),
      s.replay.get(key(sh, dst)).union(s.replay.get(key(sh, src)))), ...s },
  }
  // New resource generation: old receipts/pins remain in their grow-only logs,
  // but cannot certify the new incarnation. Registration and owner pin are
  // atomic here; a real distributed admission protocol must establish this.
  action reenroll(r: int, snapshot: bool): bool = {
    val sh = shardOf(r)
    val src = writer(sh)
    val data = if (snapshot) s.data.get(key(sh, src)) else Set()
    val c = { resource: r, generation: s.generation.get(r) + 1,
      baseline: if (snapshot) tip(key(sh, src)) else zero(sh) }
    val p = { claim: c, donor: src, generation: s.generation.get(key(sh, src)), round: s.epoch.get(sh) }
    all {
      nodeOf(r) != src, s.up.get(nodeOf(r)), s.up.get(src),
      s.phase.get(sh) == "writing", s.generation.get(r) == 1,
      snapshot or entries(s.history.get(sh)).subseteq(s.replay.get(key(sh, src))),
      s' = { generation: s.generation.set(r, c.generation),
        data: s.data.set(r, data), replay: s.replay.set(r, Set()),
        claims: s.claims.union(Set(c)), pins: s.pins.union(Set(p)),
        retired: s.retired.union(liveClaims.filter(old => old.resource == r)), ...s },
    }
  }
  action crash(n: int): bool = all {
    s.up.get(n), s' = { up: s.up.set(n, false), ...s },
  }
  action restart(n: int): bool = all {
    not(s.up.get(n)), s' = { up: s.up.set(n, true), ...s },
  }

  val dataIsPrefix = Resources.forall(r => isPrefix(r))
  val replayIsData = Resources.forall(r => s.replay.get(r).subseteq(s.data.get(r)))
  val activeHasCoverage = Shards.forall(sh => ready(sh, writer(sh)))
  val obligationsAreProtected = s.pins.forall(p => livePin(p) implies coversClaim(p.claim, p.donor))
  val generationsAreScoped = liveClaims.forall(c => c.generation == s.generation.get(c.resource))
  val receiptIsSound = s.emitted.forall(a =>
    a.generation == s.generation.get(a.resource) implies
      entries(s.history.get(shardOf(a.resource)))
        .filter(p => le(p, a.point, s.seals)).subseteq(s.data.get(a.resource)))
  val sealsAreTerminal = s.seals.forall(l =>
    entries(s.history.get(l.shard)).filter(p => p.epoch == l.epoch)
      .forall(p => le(p, l.terminal, s.seals)))
  val activationHasPredecessor = s.started.forall(pair => pair._2 == 1 or
    s.seals.exists(l => l.shard == pair._1 and l.epoch == pair._2 - 1))
  val frontierIsAntichain = Nodes.forall(n => {
    val fs = frontier(n)
    fs.forall(a => fs.forall(b => a == b or
      (not(le(a, b, s.links.get(n))) and not(le(b, a, s.links.get(n))))))
  })
  val frontierPreservesEveryPin = Nodes.forall(n => {
    val fs = frontier(n)
    val bs = bounds(n)
    Shards.forall(sh => entries(s.history.get(sh)).forall(p =>
      fs.exists(f => f.shard == p.shard and not(le(p, f, s.links.get(n)))) ==
      bs.exists(f => f.shard == p.shard and not(le(p, f, s.links.get(n))))))
  })
  val safety = dataIsPrefix and replayIsData and activeHasCoverage and obligationsAreProtected
    and generationsAreScoped and receiptIsSound and sealsAreTerminal and activationHasPredecessor
    and frontierIsAntichain and frontierPreservesEveryPin and retiredHasReplacement
  val retiredHasReplacement = s.retired.forall(c => s.generation.get(c.resource) > c.generation)
  val repeatedHandoff = s.epoch.get(0) == 4
  val offlineProgress = s.epoch.get(0) >= 3 and not(s.up.get(3))
  val collectedHistory = Resources.exists(r => s.replay.get(r) != s.data.get(r))
  val rejoined = Resources.exists(r => s.generation.get(r) == 2)

  action step = {
    nondet sh = Shards.oneOf()
    nondet n = Nodes.oneOf()
    nondet dst = Nodes.oneOf()
    nondet r = Resources.oneOf()
    any {
      write(sh), applyNext(sh, n, dst), publish(r), gossip(n, dst), seal(sh),
      accept(sh), activate(sh), collect(n), repairHistory(sh, n, dst),
      reenroll(r, true), reenroll(r, false), crash(n), restart(n),
      { nondet a = s.emitted.oneOf(); deliver(n, a) },
      { nondet l = s.seals.oneOf(); deliverSeal(n, l) },
      { nondet c = liveClaims.oneOf(); acquire(c, n) },
      { nondet p = s.pins.oneOf(); release(p) },
      s' = s,
    }
  }
  // Focused finite exploration: a real write, catch-up, seal, acceptance,
  // activation and collection, with an independently crashing offline follower.
  action boundedStep = any {
    all { s.epoch.get(0) == 1, s.writes.get(0) == 0, write(0) },
    applyNext(0, 0, 1),
    all { s.epoch.get(0) == 1, s.writes.get(0) == 1,
      s.data.get(1) == entries(s.history.get(0)), seal(0) },
    all { s.epoch.get(0) == 1, accept(0) },
    all { s.epoch.get(0) == 1, activate(0) },
    collect(0), collect(1), crash(3), restart(3),
    all { s.data.get(1).size() == 1, publish(1) },
    deliver(0, { resource: 1, generation: 1, point: point(0, 1, 1) }),
    { nondet c = 0.to(3).oneOf(); release(initialPin(c, 0)) }, s' = s,
  }
  // Separate finite schedule explores all three handoffs with one real write
  // per epoch. It keeps history anchors and subscriber generations fixed.
  action boundedRepeatedStep = any {
    all { s.writes.get(0) == 0, write(0) },
    all { s.epoch.get(0) < MaxEpoch,
      applyNext(0, writer(0), owner(0, s.epoch.get(0) + 1)) },
    all { s.writes.get(0) == 1, s.epoch.get(0) < MaxEpoch,
      s.data.get(key(0, owner(0, s.epoch.get(0) + 1))) == entries(s.history.get(0)), seal(0) },
    accept(0), activate(0), collect(0), collect(1), collect(2),
    crash(3), restart(3), s' = s,
  }
  // Progress-oriented campaign: D stays offline, anchors stay valid, and each
  // epoch contains two data transactions. This is conditional exploration,
  // not a liveness proof or a replacement for the crash/re-enrollment campaign.
  action progressStep = {
    nondet n = Nodes.oneOf()
    nondet dst = Nodes.oneOf()
    any {
      write(0), applyNext(0, writer(0), dst),
      all { s.writes.get(0) == MaxWrites, seal(0) },
      accept(0), activate(0), publish(n), gossip(n, dst), collect(n),
      { nondet a = s.emitted.oneOf(); deliver(n, a) },
      { nondet l = s.seals.oneOf(); deliverSeal(n, l) }, s' = s,
    }
  }
  action healthyStep = {
    nondet sh = Shards.oneOf()
    nondet n = Nodes.oneOf()
    nondet dst = Nodes.oneOf()
    nondet r = Resources.oneOf()
    any {
      write(sh), applyNext(sh, n, dst), publish(r), gossip(n, dst), seal(sh),
      accept(sh), activate(sh), collect(n), repairHistory(sh, n, dst),
      { nondet a = s.emitted.oneOf(); deliver(n, a) },
      { nondet l = s.seals.oneOf(); deliverSeal(n, l) },
      { nondet c = liveClaims.oneOf(); acquire(c, n) },
      { nondet p = s.pins.oneOf(); release(p) }, s' = s,
    }
  }

  // Small executable scenario helpers; no atomicity claim for their composition.
  action sync(sh: int, src: int, dst: int): bool = applyNext(sh, src, dst)
  action move(sh: int): bool = seal(sh).then(accept(sh)).then(activate(sh))
  action deliverLatest(n: int, r: int): bool = {
    val a = { resource: r, generation: s.generation.get(r), point: tip(r) }
    deliver(n, a)
  }
  action learnAll(n: int): bool = all {
    s.up.get(n), s' = { known: s.known.set(n, s.known.get(n).union(s.emitted)),
      links: s.links.set(n, s.links.get(n).union(s.seals)), ...s },
  }
  action badReleaseBeforeAcquire(p: Pin): bool = all {
    s.pins.contains(p),
    s' = { released: s.released.union(Set(p)), ...s },
  }
  action badForgetOfflineClaim(c: Claim): bool = all {
    liveClaims.contains(c), not(s.up.get(nodeOf(c.resource))),
    s' = { retired: s.retired.union(Set(c)), ...s },
  }
  action badCollectOnlyShardZero(n: int): bool = s' = {
    replay: Resources.mapBy(r => if (nodeOf(r) == n)
      s.replay.get(r).filter(p => p.shard == 0 and needed(n, p)) else s.replay.get(r)), ...s,
  }
  action badActivateWithoutSeal(sh: int): bool = s' = {
    epoch: s.epoch.set(sh, s.epoch.get(sh) + 1), writes: s.writes.set(sh, 0),
    started: s.started.union(Set((sh, s.epoch.get(sh) + 1))), ...s,
  }
  // A snapshot supplies current data, not the original replay transactions.
  action badAcceptSnapshot(sh: int): bool = {
    val n = owner(sh, s.epoch.get(sh) + 1)
    s' = { epoch: s.epoch.set(sh, s.epoch.get(sh) + 1), phase: s.phase.set(sh, "writing"),
      writes: s.writes.set(sh, 0), started: s.started.union(Set((sh, s.epoch.get(sh) + 1))), ...s }
  }
  action badReuseOldGeneration(n: int, c: Claim): bool = {
    val old = s.known.get(n).filter(a => a.resource == c.resource)
      .fold(c.baseline, (p, a) => if (le(p, a.point, s.seals)) a.point else p)
    val r = key(shardOf(c.resource), n)
    all {
      liveClaims.contains(c),
      s' = { replay: s.replay.set(r, s.replay.get(r).filter(p => not(le(p, old, s.seals)))), ...s },
    }
  }
  action badApplyNewEpoch(sh: int, src: int, dst: int): bool = {
    val p = s.history.get(sh).select(t => t.epoch == s.epoch.get(sh)).head()
    val r = key(sh, dst)
    all {
      s.replay.get(key(sh, src)).contains(p),
      s' = { data: s.data.set(r, s.data.get(r).union(Set(p))),
        replay: s.replay.set(r, s.replay.get(r).union(Set(p))), ...s },
    }
  }

  run receiptUnionLawsTest = init.expect({
    val a = Set({ resource: 1, generation: 1, point: point(0, 1, 1) })
    val b = Set({ resource: 2, generation: 1, point: point(0, 1, 2) })
    val c = Set({ resource: 3, generation: 2, point: zero(0) })
    a.union(a) == a and a.union(b) == b.union(a) and
      a.union(b).union(c) == a.union(b.union(c))
  })

  run gossipConvergesTest = init.then(write(0)).then(sync(0, 0, 1)).then(publish(1))
    .then(deliverLatest(0, 1)).then(write(0)).then(sync(0, 0, 1)).then(publish(1))
    .then(deliverLatest(2, 1)).then(gossip(0, 2)).then(gossip(2, 0))
    .expect(s.known.get(0) == s.known.get(2) and floorAt(0, initialClaim(1)) == point(0, 1, 2) and safety)

  run rawLsnMergeCounterexampleTest = init.then(write(0))
    .then(sync(0, 0, 1)).then(sync(0, 0, 2)).then(move(0)).then(write(0))
    .then(sync(0, 1, 0)).then(sync(0, 1, 2)).then(move(0)).then(write(0))
    .then(sync(0, 2, 0)).then(sync(0, 2, 1)).then(publish(0))
    .then(move(0)).then(write(0)).then(publish(0)).then(learnAll(0))
    .expect(point(0, 3, 1).original > point(0, 4, 1).original and
      not(le(point(0, 4, 1), point(0, 3, 1), s.seals)) and
      floorAt(0, initialClaim(0)) == point(0, 4, 1) and safety)

  run sealedWriterCanBeUnavailableTest = init.then(write(0)).then(sync(0, 0, 1))
    .then(seal(0)).then(crash(0)).then(accept(0)).then(activate(0)).then(write(0))
    .expect(not(s.up.get(0)) and s.epoch.get(0) == 2 and safety)

  run repeatedHandoffTest = init.then(crash(3)).then(write(0)).then(sync(0, 0, 1)).then(sync(0, 0, 2))
    .then(move(0)).then(write(0)).then(sync(0, 1, 0)).then(sync(0, 1, 2))
    .then(move(0)).then(write(0)).then(sync(0, 2, 0)).then(sync(0, 2, 1))
    .then(move(0)).then(write(0)).expect(repeatedHandoff and offlineProgress and safety)

  run repeatedTransferAndRepairWithOfflineFollowerTest = init.then(crash(3)).then(write(0))
    .then(sync(0, 0, 1)).then(sync(0, 0, 2)).then(move(0))
    .then(release(initialPin(0, 0))).then(release(initialPin(1, 0)))
    .then(release(initialPin(2, 0))).then(release(initialPin(3, 0))).then(collect(0))
    .then(write(0)).then(sync(0, 1, 2)).then(move(0))
    .then(release(initialPin(0, 1))).then(release(initialPin(1, 1)))
    .then(release(initialPin(2, 1))).then(release(initialPin(3, 1))).then(collect(1))
    .then(write(0)).then(sync(0, 2, 0)).then(sync(0, 2, 0))
    .then(repairHistory(0, 2, 0)).then(acquire(initialClaim(0), 0))
    .then(acquire(initialClaim(1), 0)).then(acquire(initialClaim(2), 0))
    .then(acquire(initialClaim(3), 0)).then(move(0)).then(write(0))
    .expect(repeatedHandoff and offlineProgress and s.replay.get(1) == Set() and safety)

  run offlineFollowerDoesNotBlockNewWritesTest = init.then(crash(2)).then(write(0))
    .then(sync(0, 0, 1)).then(move(0)).then(write(0)).then(collect(1))
    .expect(not(s.up.get(2)) and s.epoch.get(0) == 2 and s.history.get(0).length() == 2 and safety)
    .then(restart(2)).then(sync(0, 1, 2)).then(sync(0, 1, 2)).expect(safety)

  run duplicateReorderedReceiptTest = init.then(write(0)).then(sync(0, 0, 1))
    .then(publish(1)).then(write(0)).then(sync(0, 0, 1)).then(publish(1))
    .then(deliverLatest(0, 1)).then(deliverLatest(0, 1))
    .then(deliver(0, { resource: 1, generation: 1, point: point(0, 1, 1) }))
    .expect(floorAt(0, initialClaim(1)) == point(0, 1, 2) and s.known.get(0).size() == 2 and safety)

  run missingEpochLinkHoldsProgressTest = init.then(write(0)).then(sync(0, 0, 1))
    .then(move(0)).then(write(0)).then(publish(1)).then(deliverLatest(2, 1))
    .expect(floorAt(2, initialClaim(1)) == zero(0) and safety)
    .then(learnAll(2)).expect(floorAt(2, initialClaim(1)) == point(0, 2, 1) and safety)

  run crossShardFrontierTest = init.then(write(0)).then(write(1)).then(collect(0))
    .expect(frontier(0) == Set(zero(0), zero(1)) and s.replay.get(0).contains(point(0, 1, 1)) and safety)

  run acknowledgedHistoryCanBeCollectedTest = init.then(write(0))
    .then(sync(0, 0, 1)).then(sync(0, 0, 2)).then(sync(0, 0, 3))
    .then(publish(0)).then(publish(1)).then(publish(2)).then(publish(3)).then(learnAll(0)).then(collect(0))
    .expect(s.replay.get(0) == Set() and s.data.get(0).size() == 1 and safety)

  run releaseThenRepairReturningWriterTest = init.then(write(0))
    .then(sync(0, 0, 1)).then(sync(0, 0, 2)).then(move(0))
    .then(release(initialPin(0, 0))).then(release(initialPin(1, 0))).then(release(initialPin(2, 0))).then(release(initialPin(3, 0)))
    .then(collect(0)).expect(s.replay.get(0) == Set() and safety)
    .then(repairHistory(0, 1, 0)).then(acquire(initialClaim(0), 0))
    .then(acquire(initialClaim(1), 0)).then(acquire(initialClaim(2), 0)).then(acquire(initialClaim(3), 0))
    .then(move(0)).then(move(0)).expect(repeatedHandoff and safety)

  run crashAfterAcquireBeforeReleaseTest = init.then(write(0)).then(sync(0, 0, 1))
    .then(move(0)).then(crash(0)).expect(safety)
    .then(restart(0)).then(release(initialPin(2, 0))).expect(safety)

  run crashAfterDurableReceiptTest = init.then(write(0)).then(sync(0, 0, 1))
    .then(publish(1)).then(crash(1)).then(deliverLatest(0, 1))
    .expect(floorAt(0, initialClaim(1)) == point(0, 1, 1) and safety)
    .then(restart(1)).expect(safety)

  run staleGenerationIsIgnoredTest = init.then(write(0)).then(sync(0, 0, 2))
    .then(publish(2)).then(deliverLatest(0, 2)).then(reenroll(2, false)).then(collect(0))
    .expect(liveClaims.filter(c => c.resource == 2).forall(c => floorAt(0, c) == zero(0)) and safety)

  run snapshotIsNotHistoryTest = init.then(write(0)).then(reenroll(1, true))
    .expect(s.data.get(1).size() == 1 and s.replay.get(1) == Set() and safety)
    .then(seal(0)).then(accept(0)).fail()

  run cannotActivateBeforeSealTest = init.then(activate(0)).fail()
  run cannotReleaseActivePinTest = init.then(release(initialPin(2, 0))).fail()
  run cannotAcceptBehindTest = init.then(write(0)).then(seal(0)).then(accept(0)).fail()

  run forgottenOfflineClaimCounterexampleTest = init.then(crash(3))
    .then(badForgetOfflineClaim(initialClaim(3))).expect(not(retiredHasReplacement))

  run earlyReleaseCounterexampleTest = init.then(badReleaseBeforeAcquire(initialPin(2, 0)))
    .expect(not(activeHasCoverage))

  run crossShardCollectionCounterexampleTest = init.then(write(1)).then(sync(1, 1, 0))
    .then(badCollectOnlyShardZero(0)).expect(not(obligationsAreProtected))

  run staleGenerationCounterexampleTest = init.then(write(0)).then(sync(0, 0, 2))
    .then(publish(2)).then(deliverLatest(0, 2)).then(reenroll(2, false))
    .then(badReuseOldGeneration(0, { resource: 2, generation: 2, baseline: zero(0) }))
    .expect(not(obligationsAreProtected))

  run snapshotPromotionCounterexampleTest = init.then(write(0)).then(reenroll(1, true))
    .then(seal(0)).then(badAcceptSnapshot(0)).expect(not(activeHasCoverage))

  run unfencedPromotionCounterexampleTest = init.then(badActivateWithoutSeal(0))
    .expect(not(activationHasPredecessor))

  run newEpochDoesNotCoverPredecessorCounterexampleTest = init.then(write(0))
    .then(sync(0, 0, 1)).then(move(0)).then(write(0))
    .then(badApplyNewEpoch(0, 1, 2)).expect(not(dataIsPrefix))
}
