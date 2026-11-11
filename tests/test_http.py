"""Exercise routing, pagination and recall through real local HTTP requests."""
import json
import tempfile
import threading
import unittest
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from service.app import Handler, Server, Store
from memkit import core


class HttpTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        # Only test data uses local model functions; no shared sidecar is called.
        self.store = Store(str(Path(self.directory.name) / "memory.db"), client=core, workers=0)
        self.server = Server(("127.0.0.1", 0), Handler)
        self.server.store = self.store
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self):
        self.server.shutdown()
        self.thread.join()
        self.server.server_close()
        self.store.close()
        self.directory.cleanup()

    def request(self, method, path, body=None):
        payload = json.dumps(body).encode() if body is not None else None
        request = Request(self.base + path, data=payload, method=method,
                          headers={"Content-Type": "application/json"})
        try:
            response = urlopen(request, timeout=5)
        except HTTPError as exc:
            response = exc
        with response:
            return response.code, json.load(response)

    def event(self, **changes):
        event = dict(user_id="u", source_id="mail", item_id="one", version=1,
                     kind="upsert", content="Ana is leading the Atlas project.",
                     occurred_at="2026-01-01T00:00:00Z")
        event.update(changes)
        return event

    def seed(self):
        facts = ["Ana is leading the Atlas project.", "Ben joined the Atlas project.",
                 "The Atlas project deadline is March 3.",
                 "The Atlas project budget is 120000 dollars."]
        for index, fact in enumerate(facts):
            self.assertEqual(self.request("POST", "/v1/events",
                                          self.event(item_id=str(index), content=fact))[0], 202)
        for _ in range(10):
            tasks = [dict(row) for row in self.store.db.execute("SELECT * FROM tasks")]
            if not tasks:
                return
            for task in tasks:
                data = self.store.prepare(task)
                if data is not None:
                    result = core.extract(data) if task["kind"] == "extract" else core.consolidate(data)
                    self.store.complete(task, result)
        self.fail("test fixture did not converge")

    def test_pagination_returns_every_memory_once_and_lookup_is_user_scoped(self):
        self.seed()
        expected = {memory["id"] for memory, _ in self.store.memories("u")}
        self.assertEqual(len(expected), 5)
        seen = []
        cursor = ""
        for _ in range(10):
            status, page = self.request("GET", "/v1/users/u/memories?" +
                                        urlencode(dict(limit=2, cursor=cursor)))
            self.assertEqual(status, 200)
            self.assertLessEqual(len(page["memories"]), 2)
            for memory in page["memories"]:
                seen.append(memory["id"])
                self.assertEqual(self.request("GET", "/v1/users/u/memories/" + memory["id"]),
                                 (200, memory))
                self.assertEqual(self.request("GET", "/v1/users/other/memories/" + memory["id"])[0], 404)
            cursor = page["next_cursor"]
            if cursor is None:
                break
        self.assertIsNone(cursor)
        self.assertEqual(set(seen), expected)
        self.assertEqual(len(seen), len(expected))

    def test_recall_returns_cosine_ranked_atomic_and_summary_memories(self):
        self.seed()
        query = "The Atlas project budget is 120000 dollars."
        vector = core.embed(query)
        expected = sorted([(core.cosine(vector, embedding), memory["id"])
                           for memory, embedding in self.store.memories("u")],
                          key=lambda pair: (-pair[0], pair[1]))
        status, result = self.request("GET", "/v1/users/u/recall?" + urlencode(dict(q=query, k=100)))
        self.assertEqual(status, 200)
        self.assertEqual([m["id"] for m in result["memories"]], [mid for _, mid in expected])
        self.assertEqual({m["kind"] for m in result["memories"]}, {"atomic", "summary"})
        for memory, (score, _) in zip(result["memories"], expected):
            self.assertAlmostEqual(memory["score"], score)
        _, top = self.request("GET", "/v1/users/u/recall?" + urlencode(dict(q=query, k=1)))
        self.assertEqual(top["memories"], result["memories"][:1])

    def test_invalid_input_returns_400_without_accepting_work(self):
        for changes in (dict(version=True), dict(version=0), dict(version="1"),
                        dict(kind="unknown"), dict(content=None), dict(user_id="bad/id")):
            with self.subTest(changes=changes):
                status, body = self.request("POST", "/v1/events", self.event(**changes))
                self.assertEqual(status, 400)
                self.assertIn("error", body)
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM items").fetchone()[0], 0)
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM tasks").fetchone()[0], 0)
        for path in ("recall?k=0", "recall?k=101", "recall?k=invalid", "memories?limit=0"):
            self.assertEqual(self.request("GET", "/v1/users/u/" + path)[0], 400)


if __name__ == "__main__":
    unittest.main()
