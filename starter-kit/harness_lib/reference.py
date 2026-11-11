"""Reference model: the memories a correct service holds for a given set of acknowledged writes.

It recomputes everything from scratch, so it is simple rather than fast.
"""

from __future__ import annotations

import threading
from collections import defaultdict
from dataclasses import dataclass

from memkit import core

from .workload import Event, ItemKey

POLICIES = ("permanent", "newer_version_restores")


@dataclass(frozen=True)
class ExpectedAtomic:
    fact_key: str
    topic: str
    text: str
    items: frozenset[tuple[str, str]]  # (source_id, item_id)


@dataclass(frozen=True)
class ExpectedSummary:
    topic: str
    text: str
    fact_keys: frozenset[str]
    items: frozenset[tuple[str, str]]


@dataclass(frozen=True)
class UserState:
    atomics: dict[str, ExpectedAtomic]  # by fact_key
    summaries: dict[str, ExpectedSummary]  # by topic


class Reference:
    def __init__(self, policy: str = "permanent"):
        if policy not in POLICIES:
            raise ValueError(f"redelete_policy must be one of {POLICIES}, got {policy!r}")
        self.policy = policy
        self._lock = threading.Lock()
        self._items: dict[ItemKey, Event] = {}
        self._user_deleted: dict[ItemKey, int] = {}  # item -> version current when the user deleted it
        self._disconnected: set[tuple[str, str]] = set()

    def apply_event(self, event: Event) -> None:
        with self._lock:
            key = event.key
            current = self._items.get(key)
            if current is not None and event.version <= current.version:
                return
            tombstone = self._user_deleted.get(key)
            if tombstone is not None:
                if self.policy == "permanent" or event.version <= tombstone:
                    return
                del self._user_deleted[key]
            self._items[key] = event

    def user_delete(self, key: ItemKey) -> None:
        with self._lock:
            current = self._items.get(key)
            self._user_deleted[key] = current.version if current else 0

    def disconnect(self, user_id: str, source_id: str) -> None:
        with self._lock:
            self._disconnected.add((user_id, source_id))

    def connect(self, user_id: str, source_id: str) -> None:
        with self._lock:
            self._disconnected.discard((user_id, source_id))

    def live_items(self, user_id: str) -> dict[ItemKey, str]:
        with self._lock:
            return {
                key: event.content
                for key, event in self._items.items()
                if key[0] == user_id
                and event.kind == "upsert"
                and key not in self._user_deleted
                and (key[0], key[1]) not in self._disconnected
            }

    def user_state(self, user_id: str) -> UserState:
        return expected_state(self.live_items(user_id))


def expected_state(live_items: dict[ItemKey, str]) -> UserState:
    texts: dict[str, core.AtomicMemory] = {}
    backers: dict[str, set[tuple[str, str]]] = defaultdict(set)
    for (_, source_id, item_id), content in live_items.items():
        for fact in core.extract(content):
            texts[fact.fact_key] = fact
            backers[fact.fact_key].add((source_id, item_id))

    atomics = {
        key: ExpectedAtomic(key, fact.topic, fact.text, frozenset(backers[key]))
        for key, fact in texts.items()
    }
    by_topic: dict[str, list[core.AtomicMemory]] = defaultdict(list)
    for fact in texts.values():
        by_topic[fact.topic].append(fact)

    summaries = {}
    for topic, facts in by_topic.items():
        if len(facts) < core.MIN_FACTS_PER_SUMMARY:
            continue
        keys = frozenset(f.fact_key for f in facts)
        items = frozenset().union(*(backers[k] for k in keys))
        summaries[topic] = ExpectedSummary(topic, core.consolidate(facts).text, keys, items)
    return UserState(atomics, summaries)
