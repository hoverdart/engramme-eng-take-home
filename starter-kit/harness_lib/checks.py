"""Checks that compare the service's memories with the reference model."""

from __future__ import annotations

import re
import threading
import time
from collections import defaultdict
from typing import Iterable

from .reference import Reference, UserState
from .service import ServiceClient, ServiceError

_SENTENCE_SPLIT = re.compile(r"(?<=[.!?])\s+")


def facts_in_text(text: str) -> set[str]:
    """Fact sentences contained in a memory's text. A summary's first sentence carries a 'Label: ' prefix."""
    found = set()
    for sentence in _SENTENCE_SPLIT.split(text.strip()):
        found.add(sentence)
        if ": " in sentence:
            found.add(sentence.split(": ", 1)[1])
    return found


def _validate(memory: object) -> str | None:
    if not isinstance(memory, dict):
        return "memory is not a JSON object"
    for field, kind in (("id", str), ("kind", str), ("topic", str), ("text", str), ("lineage", dict)):
        if not isinstance(memory.get(field), kind):
            return f"memory {memory.get('id')!r} is missing `{field}` or it has the wrong type"
    if memory["kind"] not in ("atomic", "summary"):
        return f"memory {memory['id']!r} has unknown kind {memory['kind']!r}"
    if memory["kind"] == "atomic" and not isinstance(memory.get("fact_key"), str):
        return f"atomic memory {memory['id']!r} has no `fact_key`"
    items = memory["lineage"].get("items")
    if not isinstance(items, list) or not all(
        isinstance(i, dict) and isinstance(i.get("source_id"), str) and isinstance(i.get("item_id"), str)
        for i in items
    ):
        return f"memory {memory['id']!r} has malformed `lineage.items`"
    if memory["kind"] == "summary" and not isinstance(memory["lineage"].get("memories"), list):
        return f"summary {memory['id']!r} has no `lineage.memories`"
    return None


def _items(memory: dict) -> frozenset[tuple[str, str]]:
    return frozenset((i["source_id"], i["item_id"]) for i in memory["lineage"]["items"])


def compare_user(user_id: str, expected: UserState, actual: list[dict]) -> list[str]:
    problems: list[str] = []
    for memory in actual:
        error = _validate(memory)
        if error:
            return [f"{user_id}: {error}"]

    atomics_by_key: dict[str, list[dict]] = defaultdict(list)
    summaries_by_topic: dict[str, list[dict]] = defaultdict(list)
    for memory in actual:
        if memory["kind"] == "atomic":
            atomics_by_key[memory["fact_key"]].append(memory)
        else:
            summaries_by_topic[memory["topic"]].append(memory)
    key_by_id = {m["id"]: m["fact_key"] for m in actual if m["kind"] == "atomic"}

    for key, memories in atomics_by_key.items():
        if len(memories) > 1:
            problems.append(f"{user_id}: {len(memories)} atomic memories share fact_key {key}")
        want = expected.atomics.get(key)
        got = memories[0]
        if want is None:
            problems.append(f"{user_id}: unexpected atomic memory {got['text']!r}")
            continue
        if got["text"] != want.text:
            problems.append(f"{user_id}: atomic {key} has text {got['text']!r}, expected {want.text!r}")
        if _items(got) != want.items:
            problems.append(
                f"{user_id}: atomic {want.text!r} has lineage {sorted(_items(got))}, expected {sorted(want.items)}"
            )
    for key, want in expected.atomics.items():
        if key not in atomics_by_key:
            problems.append(f"{user_id}: missing atomic memory {want.text!r}")

    for topic, memories in summaries_by_topic.items():
        if len(memories) > 1:
            problems.append(f"{user_id}: {len(memories)} summaries for topic {topic}")
        want = expected.summaries.get(topic)
        got = memories[0]
        if want is None:
            problems.append(f"{user_id}: unexpected summary for topic {topic}")
            continue
        member_ids = got["lineage"]["memories"]
        unknown = [m for m in member_ids if m not in key_by_id]
        if unknown:
            problems.append(f"{user_id}: summary {topic} lists memories that do not exist: {unknown[:3]}")
        members = frozenset(key_by_id[m] for m in member_ids if m in key_by_id)
        if members != want.fact_keys:
            problems.append(f"{user_id}: summary {topic} merges {len(members)} memories, expected {len(want.fact_keys)}")
        if got["text"] != want.text:
            problems.append(f"{user_id}: summary {topic} text is out of date")
        if _items(got) != want.items:
            problems.append(f"{user_id}: summary {topic} lineage items do not match its members")
    for topic in expected.summaries:
        if topic not in summaries_by_topic:
            problems.append(f"{user_id}: missing summary for topic {topic}")
    return problems


def check_users(service: ServiceClient, reference: Reference, users: Iterable[str]) -> list[str]:
    problems = []
    for user_id in users:
        try:
            actual = service.list_memories(user_id)
        except ServiceError as exc:
            problems.append(f"{user_id}: {exc}")
            continue
        problems.extend(compare_user(user_id, reference.user_state(user_id), actual))
    return problems


def wait_for_convergence(
    service: ServiceClient, reference: Reference, users: list[str], deadline_s: float
) -> tuple[bool, float, list[str]]:
    """Polls until the service matches the reference or the deadline passes."""
    start = time.monotonic()
    while True:
        problems = check_users(service, reference, users)
        elapsed = time.monotonic() - start
        if not problems:
            return True, elapsed, []
        if elapsed >= deadline_s:
            return False, elapsed, problems
        time.sleep(min(1.0, max(0.0, deadline_s - elapsed)))


def convergence_deadline(events_sent: int) -> float:
    """10 seconds, plus 1 second per 100 events so large backlogs have time to drain."""
    return 10.0 + events_sent / 100.0


class LeakTracker:
    """Tracks which facts and items must no longer be readable, as of each acknowledged forget."""

    def __init__(self, fact_backers: dict[str, dict[str, set[tuple[str, str]]]]):
        # user -> fact text -> items that state it. Assumes no new ingest while tracking.
        self._backers = fact_backers
        self._lock = threading.Lock()
        self._forgotten: dict[str, set[tuple[str, str]]] = defaultdict(set)
        self._forbidden_facts: dict[str, frozenset[str]] = defaultdict(frozenset)
        self._forbidden_items: dict[str, frozenset[tuple[str, str]]] = defaultdict(frozenset)

    def forgotten(self, user_id: str, items: Iterable[tuple[str, str]]) -> None:
        with self._lock:
            forgotten = self._forgotten[user_id]
            forgotten.update(items)
            self._forbidden_items[user_id] = frozenset(forgotten)
            self._forbidden_facts[user_id] = frozenset(
                text for text, backers in self._backers.get(user_id, {}).items() if backers <= forgotten
            )

    def snapshot(self, user_id: str) -> tuple[frozenset[str], frozenset[tuple[str, str]]]:
        with self._lock:
            return self._forbidden_facts[user_id], self._forbidden_items[user_id]

    @staticmethod
    def leaks(
        memories: Iterable[dict], forbidden_facts: frozenset[str], forbidden_items: frozenset[tuple[str, str]]
    ) -> list[str]:
        found = []
        for memory in memories:
            if not isinstance(memory, dict) or not isinstance(memory.get("text"), str):
                continue
            leaked = facts_in_text(memory["text"]) & forbidden_facts
            if leaked:
                found.append(f"memory {memory.get('id')!r} still shows forgotten fact {sorted(leaked)[0]!r}")
            lineage = memory.get("lineage") if isinstance(memory.get("lineage"), dict) else {}
            for item in lineage.get("items") or []:
                if isinstance(item, dict) and (item.get("source_id"), item.get("item_id")) in forbidden_items:
                    found.append(f"memory {memory.get('id')!r} still lists forgotten item {item.get('item_id')!r}")
                    break
        return found
