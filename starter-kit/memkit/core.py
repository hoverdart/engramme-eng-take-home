"""Deterministic stand-ins for the extraction, consolidation and embedding models.

The sidecar serves extract() and consolidate() with realistic latency, failures and a
rate limit. Call them through the sidecar; the harness imports this module directly to
build its reference model.
"""

from __future__ import annotations

import hashlib
import math
import re
from dataclasses import dataclass
from functools import lru_cache
from typing import Sequence

EMBED_DIM = 64
MAX_FACTS_PER_ITEM = 5
MIN_FACTS_PER_SUMMARY = 3

# Each template is one kind of fact. The second element names the slot that decides the topic.
TEMPLATES: list[tuple[str, str]] = [
    ("{person} is leading the {project} project.", "project"),
    ("{person} joined the {project} project.", "project"),
    ("The {project} project deadline is {date}.", "project"),
    ("The {project} project budget is {amount} dollars.", "project"),
    ("{person} is flying to {city} on {date}.", "city"),
    ("The hotel in {city} is booked for {date}.", "city"),
    ("{person} recommended a restaurant in {city}.", "city"),
]

MONTHS = (
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
)

_SLOT_PATTERNS = {
    "person": r"[A-Z][a-z]+",
    "project": r"[A-Z][a-z]+",
    "city": r"[A-Z][a-z]+",
    "date": r"(?:" + "|".join(MONTHS) + r") [0-9]{1,2}",
    "amount": r"[0-9]+",
}
_TOPIC_PREFIX = {"project": "project", "city": "trip"}
_SENTENCE_SPLIT = re.compile(r"(?<=[.!?])\s+")


def _compile(template: str) -> re.Pattern[str]:
    parts = re.split(r"\{(\w+)\}", template)
    pattern = []
    for i, part in enumerate(parts):
        if i % 2:
            pattern.append(f"(?P<{part}>{_SLOT_PATTERNS[part]})")
        else:
            pattern.append(re.escape(part))
    return re.compile("^" + "".join(pattern) + "$")


_PATTERNS = [(_compile(template), slot) for template, slot in TEMPLATES]


@dataclass(frozen=True)
class AtomicMemory:
    fact_key: str
    topic: str
    text: str

    def to_dict(self) -> dict:
        return {"fact_key": self.fact_key, "topic": self.topic, "text": self.text}

    @classmethod
    def from_dict(cls, data: dict) -> "AtomicMemory":
        return cls(fact_key=data["fact_key"], topic=data["topic"], text=data["text"])


@dataclass(frozen=True)
class SummaryMemory:
    topic: str
    text: str

    def to_dict(self) -> dict:
        return {"topic": self.topic, "text": self.text}


class ConsolidationError(ValueError):
    """The input to consolidate() is invalid. Retrying the same input will not help."""


def fact_key_for(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def match_fact(sentence: str) -> AtomicMemory | None:
    for pattern, slot in _PATTERNS:
        match = pattern.match(sentence)
        if match:
            topic = f"{_TOPIC_PREFIX[slot]}:{match.group(slot).lower()}"
            return AtomicMemory(fact_key=fact_key_for(sentence), topic=topic, text=sentence)
    return None


@lru_cache(maxsize=200_000)
def _extract(content: str) -> tuple[AtomicMemory, ...]:
    seen: set[str] = set()
    facts: list[AtomicMemory] = []
    for sentence in _SENTENCE_SPLIT.split(content.strip()):
        fact = match_fact(sentence.strip())
        if fact is None or fact.fact_key in seen:
            continue
        seen.add(fact.fact_key)
        facts.append(fact)
        if len(facts) == MAX_FACTS_PER_ITEM:
            break
    return tuple(facts)


def extract(content: str) -> list[AtomicMemory]:
    """Return the atomic memories stated in an item's content, in order of appearance."""
    return list(_extract(content))


def topic_label(topic: str) -> str:
    kind, _, name = topic.partition(":")
    name = name.capitalize()
    return f"Project {name}" if kind == "project" else f"Trip to {name}"


def consolidate(memories: Sequence[AtomicMemory]) -> SummaryMemory:
    """Merge the atomic memories of one topic into a summary. Order of input does not matter."""
    if len(memories) < MIN_FACTS_PER_SUMMARY:
        raise ConsolidationError(
            f"a summary needs at least {MIN_FACTS_PER_SUMMARY} atomic memories, got {len(memories)}"
        )
    topics = {m.topic for m in memories}
    if len(topics) != 1:
        raise ConsolidationError(f"all memories must share one topic, got {sorted(topics)}")
    keys = [m.fact_key for m in memories]
    if len(set(keys)) != len(keys):
        raise ConsolidationError("duplicate fact_key in input")
    for memory in memories:
        if match_fact(memory.text) != memory:
            raise ConsolidationError(f"not a memory produced by extract(): {memory.text!r}")
    topic = topics.pop()
    texts = sorted(m.text for m in memories)
    return SummaryMemory(topic=topic, text=f"{topic_label(topic)}: " + " ".join(texts))


def embed(text: str) -> list[float]:
    """Hashed bag-of-words embedding, L2-normalized. The TypeScript client matches it exactly."""
    vector = [0.0] * EMBED_DIM
    for token in re.findall(r"[a-z0-9]+", text.lower()):
        digest = hashlib.sha256(token.encode("utf-8")).digest()
        bucket = int.from_bytes(digest[:4], "big") % EMBED_DIM
        vector[bucket] += 1.0 if digest[4] % 2 == 0 else -1.0
    norm = math.sqrt(sum(v * v for v in vector))
    if norm == 0:
        return vector
    return [v / norm for v in vector]


def cosine(a: Sequence[float], b: Sequence[float]) -> float:
    """Cosine similarity of two embed() vectors, which are already unit length."""
    return sum(x * y for x, y in zip(a, b))
