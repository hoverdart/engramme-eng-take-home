"""Tests for the memkit functions, sidecar and Python client. Run: python3 -m unittest discover tests"""

import json
import sys
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

KIT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(KIT))

from memkit import InvalidRequestError, MemkitClient, RetryableError, core  # noqa: E402
from memkit.sidecar import make_server  # noqa: E402

EMBED_FIXTURE = KIT / "testdata" / "embed_vectors.json"


class CoreTest(unittest.TestCase):
    def test_extract_finds_facts_and_ignores_noise(self):
        content = "Thanks for the update. Ana is leading the Atlas project. Sent from my phone."
        [fact] = core.extract(content)
        self.assertEqual(fact.text, "Ana is leading the Atlas project.")
        self.assertEqual(fact.topic, "project:atlas")

    def test_same_fact_has_same_key_in_any_item(self):
        a = core.extract("Ben is flying to Oslo on May 4. Looping in the team.")
        b = core.extract("See the notes below. Ben is flying to Oslo on May 4.")
        self.assertEqual(a, b)
        self.assertEqual(a[0].topic, "trip:oslo")

    def test_extract_caps_and_dedupes(self):
        facts = [f"The Atlas project budget is {n} dollars." for n in range(8)]
        content = " ".join(facts + facts)
        self.assertEqual(len(core.extract(content)), core.MAX_FACTS_PER_ITEM)

    def test_consolidate_is_order_independent(self):
        facts = core.extract(
            "Ana joined the Beacon project. The Beacon project deadline is June 2. Gus is leading the Beacon project."
        )
        self.assertEqual(core.consolidate(facts), core.consolidate(list(reversed(facts))))
        self.assertTrue(core.consolidate(facts).text.startswith("Project Beacon: "))

    def test_consolidate_rejects_bad_input(self):
        facts = core.extract("Ana joined the Beacon project. Ben joined the Beacon project. Hana is flying to Lima on May 1.")
        with self.assertRaises(core.ConsolidationError):
            core.consolidate(facts)  # mixed topics
        with self.assertRaises(core.ConsolidationError):
            core.consolidate(facts[:2])  # too few
        forged = core.AtomicMemory(facts[0].fact_key, facts[0].topic, "Made up.")
        with self.assertRaises(core.ConsolidationError):
            core.consolidate([forged, facts[1], facts[1]])

    def test_embed_matches_fixture(self):
        for case in json.loads(EMBED_FIXTURE.read_text()):
            self.assertEqual(core.embed(case["text"]), case["vector"])

    def test_embed_exact_text_scores_highest(self):
        query = core.embed("Ana is leading the Atlas project.")
        same = core.cosine(query, core.embed("Ana is leading the Atlas project."))
        other = core.cosine(query, core.embed("The hotel in Oslo is booked for May 4."))
        self.assertAlmostEqual(same, 1.0)
        self.assertLess(other, same)


class SidecarTest(unittest.TestCase):
    def start(self, **config):
        settings = {"latency_min_ms": 0, "latency_max_ms": 0, "failure_rate": 0.0, "max_concurrency": 20}
        settings.update(config)
        server = make_server(0, settings, seed=1)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.shutdown)
        return MemkitClient(f"http://127.0.0.1:{server.server_address[1]}")

    def test_extract_and_consolidate_round_trip(self):
        client = self.start()
        facts = client.extract("Ana joined the Beacon project. Ben joined the Beacon project. Gus joined the Beacon project.")
        self.assertEqual(client.consolidate(facts), core.consolidate(facts))

    def test_invalid_consolidate_is_not_retryable(self):
        client = self.start()
        with self.assertRaises(InvalidRequestError):
            client.consolidate(client.extract("Ana joined the Beacon project."))

    def test_concurrency_limit_returns_429(self):
        client = self.start(latency_min_ms=200, latency_max_ms=200, max_concurrency=5)

        def call(_):
            try:
                client.extract("Ana joined the Beacon project.")
                return "ok"
            except RetryableError as exc:
                return exc.status

        with ThreadPoolExecutor(20) as pool:
            outcomes = list(pool.map(call, range(20)))
        self.assertEqual(outcomes.count("ok"), 5)
        self.assertEqual(outcomes.count(429), 15)

    def test_transient_failures_are_retryable(self):
        client = self.start(failure_rate=1.0)
        with self.assertRaises(RetryableError) as caught:
            client.extract("Ana joined the Beacon project.")
        self.assertEqual(caught.exception.status, 503)


if __name__ == "__main__":
    unittest.main()
