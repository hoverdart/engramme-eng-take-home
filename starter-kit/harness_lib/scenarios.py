"""Public scenarios. Each one drives the service over HTTP and checks it against the reference model."""

from __future__ import annotations

import math
import random
import secrets
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Callable

from memkit import core

from .checks import LeakTracker, convergence_deadline, facts_in_text, wait_for_convergence
from .procs import ProcessControl
from .reference import Reference
from .service import ServiceClient, ServiceError, SidecarClient
from .workload import PROJECTS, SOURCES, Event, ItemKey, Workload

MAX_PROBLEMS = 50


@dataclass
class Context:
    service: ServiceClient
    sidecar: SidecarClient
    procs: ProcessControl
    policy: str
    seed: int
    sidecar_defaults: dict
    log: Callable[[str], None]


@dataclass
class Result:
    name: str
    passed: bool
    problems: list[str] = field(default_factory=list)
    metrics: dict[str, str] = field(default_factory=dict)


class Abort(Exception):
    pass


class Run:
    def __init__(self, ctx: Context, name: str):
        self.ctx = ctx
        self.name = name
        self.rng = random.Random(f"{ctx.seed}-{name}")
        self.run_id = f"{name}-{ctx.seed}-{secrets.token_hex(2)}"
        self.workload = Workload(self.rng, self.run_id)
        self.reference = Reference(ctx.policy)
        self.service = ctx.service
        self.events_sent = 0
        self.problems: list[str] = []
        self.metrics: dict[str, str] = {}
        self._lock = threading.Lock()

    def problem(self, message: str) -> None:
        with self._lock:
            if len(self.problems) < MAX_PROBLEMS:
                self.problems.append(message)

    def send(self, events: list[Event], concurrency: int = 32) -> None:
        """Sends events in parallel; each is applied to the reference once acknowledged."""

        def one(event: Event) -> str | None:
            try:
                self.service.send_event(event)
            except ServiceError as exc:
                return str(exc)
            self.reference.apply_event(event)
            with self._lock:
                self.events_sent += 1
            return None

        with ThreadPoolExecutor(concurrency) as pool:
            errors = [e for e in pool.map(one, events) if e]
        if errors:
            raise Abort(f"{len(errors)} writes failed; first: {errors[0]}")

    def converge(self, users: list[str], label: str, minimum_s: float = 0.0) -> bool:
        deadline = max(minimum_s, convergence_deadline(self.events_sent))
        ok, elapsed, problems = wait_for_convergence(self.service, self.reference, users, deadline)
        self.metrics[f"converged {label}"] = f"{elapsed:.1f}s" if ok else f"no (waited {deadline:.0f}s)"
        for p in problems:
            self.problem(f"[{label}] {p}")
        return ok

    def users(self, n: int) -> list[str]:
        return [self.workload.user_id(i) for i in range(n)]

    def items(self, users: list[str], per_user: int, pools: dict[str, list[list[str]]]) -> dict[ItemKey, list[str]]:
        return {
            self.workload.new_item_key(user, self.rng.choice(SOURCES)): self.workload.pick_facts(pools[user])
            for user in users
            for _ in range(per_user)
        }


# Scenarios


def basic(run: Run) -> None:
    users = run.users(3)
    pools = {u: run.workload.fact_pool(3, 5) for u in users}
    items = run.items(users, 15, pools)
    run.send([run.workload.upsert(key, 1, facts) for key, facts in items.items()], concurrency=4)
    if not run.converge(users, "after ingest"):
        return
    for _ in range(5):
        user = run.rng.choice(users)
        fact = run.rng.choice(sorted(run.reference.user_state(user).atomics.values(), key=lambda a: a.fact_key))
        results = run.service.recall(user, fact.text, k=5)
        if not any(isinstance(m, dict) and fact.text in facts_in_text(str(m.get("text", ""))) for m in results):
            run.problem(f"recall for the exact text {fact.text!r} did not return it in the top 5")


def dupes(run: Run, n_users: int = 8, per_user: int = 40) -> None:
    users = run.users(n_users)
    pools = {u: run.workload.fact_pool(4, 6) for u in users}
    events = [run.workload.upsert(key, 1, facts) for key, facts in run.items(users, per_user, pools).items()]
    sends = [e for e in events for _ in range(run.rng.randint(1, 5))]
    run.rng.shuffle(sends)
    run.send(sends)
    run.converge(users, "after duplicate deliveries")


def reorder(run: Run, n_users: int = 6, per_user: int = 30) -> None:
    users = run.users(n_users)
    pools = {u: run.workload.fact_pool(4, 6) for u in users}
    sends = []
    for key in run.items(users, per_user, pools):
        versions = run.rng.randint(2, 4)
        for version in range(1, versions + 1):
            if version == versions and run.rng.random() < 0.15:
                event = run.workload.delete(key, version)
            else:
                event = run.workload.upsert(key, version, run.workload.pick_facts(pools[key[0]]))
            sends.extend([event] * run.rng.randint(1, 2))
    run.rng.shuffle(sends)
    run.send(sends)
    run.converge(users, "after out-of-order versions")


def cross_source(run: Run) -> None:
    users = run.users(6)
    # A small pool spread over many items puts most facts in several items and sources.
    pools = {u: run.workload.fact_pool(3, 4) for u in users}
    events = [run.workload.upsert(key, 1, facts) for key, facts in run.items(users, 40, pools).items()]
    run.rng.shuffle(events)
    run.send(events)
    shared = sum(
        1
        for u in users
        for atomic in run.reference.user_state(u).atomics.values()
        if len({source for source, _ in atomic.items}) > 1
    )
    run.metrics["facts stated in several sources"] = str(shared)
    run.converge(users, "after cross-source ingest")


def edit(run: Run) -> None:
    users = run.users(6)
    pools = {u: run.workload.fact_pool(4, 6) for u in users}
    v1 = run.items(users, 30, pools)
    first = {key: run.workload.upsert(key, 1, facts) for key, facts in v1.items()}
    run.send(list(first.values()))
    if not run.converge(users, "after first versions"):
        return
    ids_before = {
        (u, m["fact_key"]): m["id"] for u in users for m in run.service.list_memories(u) if m.get("kind") == "atomic"
    }

    latest = dict(first)
    sends = []
    for key in run.rng.sample(sorted(v1), len(v1) // 2):
        kept = run.rng.sample(v1[key], run.rng.randint(1, len(v1[key])))
        for version in range(2, 2 + run.rng.choice((1, 1, 2))):
            added = [f for f in run.workload.pick_facts(pools[key[0]], max_facts=2) if f not in kept]
            event = run.workload.upsert(key, version, (kept + added)[: core.MAX_FACTS_PER_ITEM])
            sends.append(event)
            latest[key] = event
    run.rng.shuffle(sends)
    run.send(sends)
    if not run.converge(users, "after edits"):
        return

    ids_after = {
        (u, m["fact_key"]): m["id"] for u in users for m in run.service.list_memories(u) if m.get("kind") == "atomic"
    }
    for key, event in latest.items():
        before = {f.fact_key for f in core.extract(first[key].content)}
        after = {f.fact_key for f in core.extract(event.content)}
        for fact_key in before & after:
            old, new = ids_before.get((key[0], fact_key)), ids_after.get((key[0], fact_key))
            if old and new and old != new:
                run.problem(f"{key[0]}: memory {old} became {new} although item {key[2]} kept stating its fact")


def backfill(run: Run, backfill_items: int = 3000, live_events: int = 20) -> None:
    heavy, other = run.users(2)
    pool = run.workload.fact_pool(6, 8)
    big = [
        run.workload.upsert(run.workload.new_item_key(heavy, "gdrive"), 1, run.workload.pick_facts(pool))
        for _ in range(backfill_items)
    ]
    live = []
    for i in range(live_events):
        user = heavy if i % 2 == 0 else other
        fact = run.workload.fact("project", run.rng.choice(PROJECTS), amount=9_000_000 + i)
        live.append((run.workload.upsert(run.workload.new_item_key(user, "gmail"), 1, [fact]), fact))

    sender = threading.Thread(target=lambda: _capture(run, lambda: run.send(big)))
    sender.start()
    time.sleep(1.0)

    latencies: list[float] = []
    pollers = []
    for event, fact in live:
        run.send([event], concurrency=1)
        acked = time.monotonic()
        poller = threading.Thread(target=_poll_until_visible, args=(run, event.user_id, fact, acked, latencies))
        poller.start()
        pollers.append(poller)
        time.sleep(0.5)
    for thread in [sender, *pollers]:
        thread.join()

    latencies.sort()
    p95 = latencies[max(0, math.ceil(0.95 * len(latencies)) - 1)] if latencies else float("inf")
    run.metrics["live events recallable, p95"] = f"{p95:.2f}s"
    if p95 > 5.0:
        run.problem(f"live events took {p95:.2f}s (p95) to become recallable during the backfill; the limit is 5s")
    run.converge([heavy, other], "after backfill")


def _capture(run: Run, fn: Callable[[], None]) -> None:
    try:
        fn()
    except Abort as exc:
        run.problem(str(exc))


def _poll_until_visible(run: Run, user: str, fact: str, acked: float, latencies: list[float]) -> None:
    timeout = 60.0
    while time.monotonic() - acked < timeout:
        try:
            results = run.service.recall(user, fact, k=10)
        except ServiceError:
            results = []
        if any(isinstance(m, dict) and fact in facts_in_text(str(m.get("text", ""))) for m in results):
            with run._lock:
                latencies.append(time.monotonic() - acked)
            return
        time.sleep(0.25)
    with run._lock:
        latencies.append(timeout)


def delete(run: Run) -> None:
    users = run.users(6)
    pools = {u: run.workload.fact_pool(3, 5) for u in users}
    events = {key: run.workload.upsert(key, 1, facts) for key, facts in run.items(users, 40, pools).items()}
    run.send(list(events.values()))
    if not run.converge(users, "after ingest"):
        return

    ids = {u: [m["id"] for m in run.service.list_memories(u)] for u in users}
    backers = {u: {a.text: set(a.items) for a in run.reference.user_state(u).atomics.values()} for u in users}
    tracker = LeakTracker(backers)

    forgets: list[tuple] = [("delete", key) for key in run.rng.sample(sorted(events), len(events) * 3 // 10)]
    disconnected = [(u, run.rng.choice(SOURCES)) for u in run.rng.sample(users, 2)]
    forgets += [("disconnect", u, s) for u, s in disconnected]
    run.rng.shuffle(forgets)

    def forget(action: tuple) -> None:
        if action[0] == "delete":
            key = action[1]
            run.service.user_delete(key)
            run.reference.user_delete(key)
            tracker.forgotten(key[0], [(key[1], key[2])])
        else:
            _, user, source = action
            run.service.disconnect(user, source)
            run.reference.disconnect(user, source)
            tracker.forgotten(user, [(k[1], k[2]) for k in events if k[0] == user and k[1] == source])

    stop = threading.Event()
    probes = [0]

    def probe(seed: int) -> None:
        rng = random.Random(seed)
        while not stop.is_set():
            user = rng.choice(users)
            # Snapshot before the request: anything forgotten by now must not appear in the answer.
            facts, items = tracker.snapshot(user)
            try:
                action = rng.random()
                if action < 0.3:
                    memories = run.service.list_memories(user)
                elif action < 0.7:
                    query = rng.choice(sorted(facts)) if facts and rng.random() < 0.5 else rng.choice(sorted(backers[user]))
                    memories = run.service.recall(user, query, k=10)
                else:
                    found = run.service.get_memory(user, rng.choice(ids[user]))
                    memories = [found] if found else []
            except ServiceError as exc:
                run.problem(f"read failed during deletes: {exc}")
                continue
            for leak in tracker.leaks(memories, facts, items):
                run.problem(f"{user}: {leak}")
            with run._lock:
                probes[0] += 1

    probers = [threading.Thread(target=probe, args=(run.rng.random(),)) for _ in range(8)]
    for thread in probers:
        thread.start()
    try:
        with ThreadPoolExecutor(8) as pool:
            list(pool.map(forget, forgets))
        time.sleep(2.0)
    finally:
        stop.set()
        for thread in probers:
            thread.join()
    run.metrics["read probes during deletes"] = str(probes[0])
    if not run.converge(users, "after deletes and disconnects"):
        return

    backfill_events = []
    for user, source in disconnected:
        run.service.connect(user, source)
        run.reference.connect(user, source)
        backfill_events += [e for k, e in events.items() if k[0] == user and k[1] == source]
    run.send(backfill_events)
    run.converge(users, "after reconnect and backfill")


def crash(run: Run) -> None:
    reason = run.ctx.procs.unavailable_reason()
    if reason:
        run.problem(f"cannot run the crash scenario: {reason}")
        return
    users = run.users(6)
    pools = {u: run.workload.fact_pool(3, 5) for u in users}
    items = run.items(users, 40, pools)
    sends = []
    for key in items:
        for version in range(1, run.rng.randint(1, 2) + 1):
            event = run.workload.upsert(key, version, run.workload.pick_facts(pools[key[0]]))
            sends.extend([event] * run.rng.randint(1, 2))
    run.rng.shuffle(sends)
    to_delete = run.rng.sample(sorted(items), len(items) // 5)

    kills = [0]

    def killer() -> None:
        for _ in range(4):
            time.sleep(run.rng.uniform(1.5, 4.0))
            run.ctx.procs.kill()
            kills[0] += 1
            time.sleep(0.3)
            run.ctx.procs.start()

    killer_thread = threading.Thread(target=killer)
    killer_thread.start()
    try:
        run.send(sends, concurrency=16)

        def forget(key: ItemKey) -> None:
            run.service.user_delete(key)
            run.reference.user_delete(key)

        with ThreadPoolExecutor(8) as pool:
            list(pool.map(forget, to_delete))
    finally:
        killer_thread.join()
    run.metrics["kills"] = str(kills[0])
    if not run.service.wait_healthy(120):
        run.problem("the service did not become healthy within 120s of the last restart")
        return
    run.converge(users, "after crashes", minimum_s=20.0)


SCENARIOS: dict[str, Callable[[Run], None]] = {
    "basic": basic,
    "dupes": dupes,
    "reorder": reorder,
    "cross-source": cross_source,
    "edit": edit,
    "backfill": backfill,
    "delete": delete,
    "crash": crash,
}


def run_scenario(ctx: Context, name: str, fn: Callable[[Run], None] | None = None) -> Result:
    run = Run(ctx, name)
    ctx.sidecar.reset(ctx.sidecar_defaults)
    started = time.monotonic()
    try:
        (fn or SCENARIOS[name])(run)
    except Abort as exc:
        run.problem(str(exc))
    except ServiceError as exc:
        run.problem(f"service error: {exc}")
    run.metrics["duration"] = f"{time.monotonic() - started:.1f}s"
    run.metrics["events sent"] = str(run.events_sent)
    stats = ctx.sidecar.stats()
    for key in ("extract_completed", "consolidate_completed", "rate_limited", "transient_failures", "invalid_requests"):
        if key in stats:
            run.metrics[f"sidecar {key.replace('_', ' ')}"] = str(stats[key])
    return Result(name, passed=not run.problems, problems=run.problems, metrics=run.metrics)
