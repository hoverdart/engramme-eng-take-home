"""Seeded generator for users, facts, items and source events."""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from memkit import core

PEOPLE = (
    "Ana", "Ben", "Chen", "Dana", "Eli", "Farah", "Gus", "Hana",
    "Ivan", "Jo", "Kai", "Lena", "Milo", "Nia", "Omar", "Pia",
)
PROJECTS = ("Atlas", "Beacon", "Cobalt", "Delta", "Ember", "Falcon", "Granite", "Harbor", "Indigo", "Juniper")
CITIES = ("Lisbon", "Osaka", "Denver", "Nairobi", "Oslo", "Lima", "Hanoi", "Porto", "Quito", "Riga")
NOISE = (
    "Thanks for the update.",
    "Let me know if this works.",
    "Sent from my phone.",
    "See the notes below.",
    "Looping in the team.",
    "Happy to discuss on our next call.",
)
SOURCES = ("gmail", "gcal", "gdrive")

ItemKey = tuple[str, str, str]  # (user_id, source_id, item_id)

_TEMPLATES_BY_SLOT = {
    slot: [template for template, topic_slot in core.TEMPLATES if topic_slot == slot]
    for slot in ("project", "city")
}
_EPOCH = datetime(2026, 1, 1, tzinfo=timezone.utc)


@dataclass(frozen=True)
class Event:
    user_id: str
    source_id: str
    item_id: str
    version: int
    kind: str  # "upsert" or "delete"
    content: str | None
    occurred_at: str

    @property
    def key(self) -> ItemKey:
        return (self.user_id, self.source_id, self.item_id)

    def to_json(self) -> dict:
        body = {
            "user_id": self.user_id,
            "source_id": self.source_id,
            "item_id": self.item_id,
            "version": self.version,
            "kind": self.kind,
            "occurred_at": self.occurred_at,
        }
        if self.content is not None:
            body["content"] = self.content
        return body


class Workload:
    def __init__(self, rng: random.Random, run_id: str):
        self.rng = rng
        self.run_id = run_id
        self._clock = 0
        self._item_counter = 0

    def user_id(self, n: int) -> str:
        return f"{self.run_id}-u{n}"

    def new_item_key(self, user_id: str, source_id: str) -> ItemKey:
        self._item_counter += 1
        return (user_id, source_id, f"item-{self._item_counter}")

    def fact_pool(self, n_topics: int, facts_per_topic: int) -> list[list[str]]:
        """Facts grouped by topic. Topics are a mix of projects and trips."""
        rng = self.rng
        n_projects = (n_topics + 1) // 2
        topics = [("project", p) for p in rng.sample(PROJECTS, n_projects)]
        topics += [("city", c) for c in rng.sample(CITIES, n_topics - n_projects)]
        pool = []
        for slot, value in topics:
            facts: set[str] = set()
            while len(facts) < facts_per_topic:
                facts.add(self.fact(slot, value))
            pool.append(sorted(facts))
        return pool

    def fact(self, slot: str, value: str, amount: int | None = None) -> str:
        rng = self.rng
        template = rng.choice(_TEMPLATES_BY_SLOT[slot]) if amount is None else "The {project} project budget is {amount} dollars."
        values = {
            "person": rng.choice(PEOPLE),
            "project": value if slot == "project" else rng.choice(PROJECTS),
            "city": value if slot == "city" else rng.choice(CITIES),
            "date": f"{rng.choice(core.MONTHS)} {rng.randint(1, 28)}",
            "amount": str(amount if amount is not None else rng.randrange(10, 500) * 1000),
        }
        return template.format(**values)

    def pick_facts(self, pool: list[list[str]], max_facts: int = 4) -> list[str]:
        """1-4 facts, mostly from one topic, sometimes with one from another topic."""
        rng = self.rng
        topic = rng.choice(pool)
        facts = rng.sample(topic, rng.randint(1, min(max_facts, len(topic))))
        if len(pool) > 1 and rng.random() < 0.3:
            other = rng.choice([t for t in pool if t is not topic])
            extra = rng.choice(other)
            if extra not in facts:
                facts.append(extra)
        return facts

    def content(self, facts: list[str]) -> str:
        sentences = list(facts) + self.rng.sample(NOISE, self.rng.randint(0, 2))
        self.rng.shuffle(sentences)
        return " ".join(sentences)

    def upsert(self, key: ItemKey, version: int, facts: list[str]) -> Event:
        return Event(*key, version=version, kind="upsert", content=self.content(facts), occurred_at=self._tick())

    def delete(self, key: ItemKey, version: int) -> Event:
        return Event(*key, version=version, kind="delete", content=None, occurred_at=self._tick())

    def _tick(self) -> str:
        self._clock += 1
        return (_EPOCH + timedelta(seconds=self._clock)).isoformat().replace("+00:00", "Z")
