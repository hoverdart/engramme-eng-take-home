"""Deterministic unit tests for correctness boundaries (no network/model shortcuts in service).

Run from repository root: python3 -m unittest discover -s tests -v
FakeClient is test-only. Production always calls the supplied sidecar.
"""
import random
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from service.app import Scheduler, Store, taskKey
from memkit import core
from harness_lib.checks import compare_user
from harness_lib.reference import Reference
from harness_lib.workload import Event


class FakeClient:
    extract = staticmethod(core.extract)
    consolidate = staticmethod(core.consolidate)
    embed = staticmethod(core.embed)


FACTS = [
    "Ana is leading the Atlas project.",
    "Ben joined the Atlas project.",
    "The Atlas project deadline is March 3.",
    "The Atlas project budget is 120000 dollars.",
]


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = str(Path(self.directory.name) / "memory.db")
        self.store = Store(self.path, client=FakeClient(), workers=0)

    def tearDown(self):
        self.store.close()
        self.directory.cleanup()

    def event(self, iid="one", facts=FACTS[:1], version=1, sid="mail", uid="u", kind="upsert"):
        event = dict(user_id=uid, source_id=sid, item_id=iid, version=version,
                     kind=kind, content=" ".join(facts), occurred_at="2026-01-01T00:00:00Z")
        self.store.event(event)
        return event

    def tasks(self, kind=None):
        return [dict(row) for row in self.store.db.execute("SELECT * FROM tasks ORDER BY kind,key")
                if kind is None or row["kind"] == kind]

    def execute(self, task):
        data = self.store.prepare(task)
        if data is None:
            return False
        result = FakeClient.extract(data) if task["kind"] == "extract" else FakeClient.consolidate(data)
        return self.store.complete(task, result)

    def drain(self):
        for _ in range(100):
            tasks = self.tasks()
            if not tasks:
                return
            for task in tasks:
                self.execute(task)
        self.fail("durable task queue did not converge")

    def memories(self, uid="u"):
        return [m for m, _ in self.store.memories(uid)]

    def atomics(self, uid="u"):
        return [m for m in self.memories(uid) if m["kind"] == "atomic"]

    def test_duplicate_and_reordered_events_keep_latest_version(self):
        self.event(version=3, facts=FACTS[2:3])
        self.event(version=1, facts=FACTS[:1])
        self.event(version=3, facts=FACTS[1:2])
        self.assertEqual(len(self.tasks()), 1)
        self.drain()
        self.assertEqual([m["text"] for m in self.atomics()], FACTS[2:3])

    def test_upstream_delete_version_prevents_old_replay(self):
        self.event(version=4, kind="delete")
        self.event(version=3)
        self.drain()
        self.assertEqual(self.memories(), [])
        self.event(version=5)
        self.drain()
        self.assertEqual(len(self.atomics()), 1)

    def test_cross_source_dedup_and_clean_shared_lineage(self):
        self.event(sid="mail")
        self.event(sid="calendar")
        self.drain()
        atomic = self.atomics()[0]
        self.assertEqual(len(atomic["lineage"]["items"]), 2)
        self.store.forget("u", "mail", "one")
        after = self.atomics()[0]
        self.assertEqual(after["id"], atomic["id"])
        self.assertEqual(after["lineage"]["items"], [{"source_id": "calendar", "item_id": "one"}])
        self.store.forget("u", "calendar", "one")
        self.assertEqual(self.memories(), [])

    def test_old_extract_cannot_commit_after_edit(self):
        self.event()
        task = self.tasks()[0]
        result = FakeClient.extract(self.store.prepare(task))
        self.event(version=2, facts=FACTS[1:2])
        self.assertFalse(self.store.complete(task, result))
        self.drain()
        self.assertEqual([m["text"] for m in self.atomics()], FACTS[1:2])

    def test_inflight_extract_cannot_resurrect_forgotten_item(self):
        self.event()
        task = self.tasks()[0]
        result = FakeClient.extract(self.store.prepare(task))
        self.store.forget("u", "mail", "one")
        self.assertFalse(self.store.complete(task, result))
        self.event(version=500)
        self.drain()
        self.assertEqual(self.memories(), [])

    def test_forget_unknown_item_survives_future_delivery(self):
        self.store.forget("u", "mail", "one")
        self.event(version=1000)
        self.drain()
        self.assertEqual(self.memories(), [])

    def test_disconnect_cancels_inflight_extract_and_reconnect_restores(self):
        self.event()
        task = self.tasks()[0]
        result = FakeClient.extract(self.store.prepare(task))
        self.store.source("u", "mail", 0)
        self.assertFalse(self.store.complete(task, result))
        self.assertEqual(self.memories(), [])
        self.store.source("u", "mail", 1)
        self.event()  # replay same version after reconnect
        self.drain()
        self.assertEqual(len(self.atomics()), 1)

    def test_disconnect_cleans_cache_and_reconnect_excludes_forget(self):
        self.event("keep", FACTS[:2])
        self.event("forget", FACTS[2:])
        self.drain()
        self.assertEqual(len(self.memories()), 5)
        self.store.forget("u", "mail", "forget")
        self.store.source("u", "mail", 0)
        self.assertEqual(self.memories(), [])
        self.event("keep", FACTS[1:2], version=2)
        self.store.source("u", "mail", 1)
        self.event("forget", FACTS[2:], version=999)
        self.drain()
        self.assertEqual([m["text"] for m in self.atomics()], FACTS[1:2])

    def test_reconnect_replacement_is_not_completed_by_pre_disconnect_call(self):
        self.event()
        old = self.tasks("extract")[0]
        result = FakeClient.extract(self.store.prepare(old))
        self.store.source("u", "mail", 0)
        self.store.source("u", "mail", 1)
        replacement = self.tasks("extract")[0]
        self.assertEqual(old["generation"], replacement["generation"])
        self.assertNotEqual(old["created"], replacement["created"])
        self.assertIsNone(self.store.prepare(old))
        self.assertFalse(self.store.complete(old, result))
        self.assertEqual(self.tasks("extract"), [replacement])
        self.assertEqual(self.memories(), [])
        self.drain()
        self.assertEqual([m["text"] for m in self.atomics()], FACTS[:1])

    def test_pre_disconnect_failure_cannot_block_reconnected_job(self):
        self.event()
        old = self.tasks("extract")[0]
        self.store.source("u", "mail", 0)
        self.store.source("u", "mail", 1)
        replacement = self.tasks("extract")[0]
        self.store.retry(old, RuntimeError("old connection failed"), permanent=True)
        self.assertTrue(self.store.healthy())
        self.assertEqual(self.tasks("extract"), [replacement])
        self.drain()
        self.assertEqual([m["text"] for m in self.atomics()], FACTS[:1])

    def test_summary_removed_synchronously_when_fact_disappears(self):
        for index, fact in enumerate(FACTS):
            self.event(str(index), [fact])
        self.drain()
        self.assertEqual(len(self.memories()), 5)
        self.store.forget("u", "mail", "3")
        self.assertFalse(any(m["kind"] == "summary" for m in self.memories()))
        self.assertFalse(any(FACTS[3] in m["text"] for m in self.memories()))
        self.drain()
        summaries = [m for m in self.memories() if m["kind"] == "summary"]
        self.assertEqual(len(summaries), 1)
        self.assertEqual(len(summaries[0]["lineage"]["memories"]), 3)
        self.store.forget("u", "mail", "2")
        self.drain()
        self.assertFalse(any(m["kind"] == "summary" for m in self.memories()))

    def test_stale_summary_cannot_commit_after_forget(self):
        self.event(facts=FACTS)
        self.execute(self.tasks("extract")[0])
        task = self.tasks("summary")[0]
        result = FakeClient.consolidate(self.store.prepare(task))
        self.store.forget("u", "mail", "one")
        self.assertFalse(self.store.complete(task, result))
        self.assertEqual(self.memories(), [])

    def test_summary_lineage_updates_without_rebuilding_same_facts(self):
        self.event(facts=FACTS)
        self.event(sid="calendar", facts=FACTS)
        self.drain()
        before = next(m for m in self.memories() if m["kind"] == "summary")
        self.store.forget("u", "mail", "one")
        after = next(m for m in self.memories() if m["kind"] == "summary")
        self.assertEqual(before["id"], after["id"])
        self.assertEqual(after["lineage"]["items"], [{"source_id": "calendar", "item_id": "one"}])
        self.assertEqual(self.tasks(), [])

    def test_unchanged_facts_keep_ids_across_edit(self):
        self.event(facts=FACTS[:3])
        self.drain()
        ids = {m["fact_key"]: m["id"] for m in self.atomics()}
        self.event(version=2, facts=FACTS[1:])
        self.drain()
        self.assertTrue(all(m["id"] == ids[m["fact_key"]] for m in self.atomics() if m["fact_key"] in ids))

    def test_reopen_recovers_pending_acknowledged_write_and_tombstone(self):
        self.event()
        self.store.forget("u", "mail", "never")
        self.store.close()
        self.store = Store(self.path, client=FakeClient(), workers=0)
        self.assertEqual(len(self.tasks()), 1)
        queued = self.store.scheduler.take()
        self.assertEqual(queued["generation"], 1)
        self.execute(queued)
        self.event("never", version=30)
        self.drain()
        self.assertEqual(len(self.atomics()), 1)
        self.assertEqual(self.atomics()[0]["lineage"]["items"], [{"source_id": "mail", "item_id": "one"}])

    def test_transaction_rollback_does_not_schedule_uncommitted_work(self):
        with self.assertRaises(RuntimeError):
            with self.store.transaction():
                self.store.enqueue("extract", "u", "mail", "bad", generation=1)
                raise RuntimeError("simulated failure before commit")
        self.assertEqual(self.tasks(), [])
        self.assertEqual(self.store.scheduler.pending, {})

    def test_retry_schedule_is_durable(self):
        self.event()
        task = self.tasks()[0]
        self.store.retry(task, RuntimeError("transient test failure"))
        self.assertEqual(self.tasks()[0]["attempt"], 1)
        self.assertGreater(self.tasks()[0]["available"], time.time())
        self.store.close()
        self.store = Store(self.path, client=FakeClient(), workers=0)
        self.assertIn(task["key"], self.store.scheduler.pending)
        self.drain()
        self.assertEqual(len(self.atomics()), 1)

    def test_concurrent_versions_and_user_forget_are_serializable(self):
        barrier = threading.Barrier(32)
        def operation(index):
            barrier.wait()
            if index == 15:
                self.store.forget("u", "mail", "one")
            else:
                self.event(version=index+1, facts=FACTS[index % 4:index % 4+1])
        with ThreadPoolExecutor(max_workers=32) as pool:
            list(pool.map(operation, range(32)))
        self.drain()
        self.assertEqual(self.memories(), [])
        self.assertEqual(self.tasks(), [])

    def test_user_isolation_for_identical_facts_and_item_ids(self):
        self.event(uid="u")
        self.event(uid="other")
        self.drain()
        self.assertNotEqual(self.atomics("u")[0]["id"], self.atomics("other")[0]["id"])
        self.store.forget("u", "mail", "one")
        self.assertEqual(self.memories("u"), [])
        self.assertEqual(len(self.memories("other")), 1)


    def test_mixed_interleavings_and_restarts_match_reference(self):
        # Delay model responses across edits/deletes and warm read caches before
        # writes. Restarts discard in-flight responses but retain durable jobs.
        # The independent supplied reference defines the final expected state.
        for seed in range(20):
            with self.subTest(seed=seed):
                self.store.close()
                path = str(Path(self.directory.name) / f"mixed-{seed}.db")
                self.store = Store(path, client=FakeClient(), workers=0)
                reference = Reference("permanent")
                rng = random.Random(seed)
                inFlight = []
                for _ in range(400):
                    uid = rng.choice(["u", "other"])
                    sid = rng.choice(["mail", "calendar"])
                    iid = str(rng.randrange(12))
                    action = rng.randrange(11)
                    if action < 5:
                        event = Event(uid, sid, iid, rng.randrange(1, 10),
                                      "delete" if action == 0 else "upsert",
                                      " ".join(rng.sample(FACTS, rng.randrange(5))),
                                      "2026-01-01T00:00:00Z")
                        self.store.event(event.to_json())
                        reference.apply_event(event)
                    elif action == 5:
                        self.store.forget(uid, sid, iid)
                        reference.user_delete((uid, sid, iid))
                    elif action == 6:
                        connected = rng.randrange(2)
                        self.store.source(uid, sid, connected)
                        (reference.connect if connected else reference.disconnect)(uid, sid)
                    elif action == 7:
                        tasks = self.tasks()
                        if tasks:
                            task = rng.choice(tasks)
                            data = self.store.prepare(task)
                            if data is not None:
                                result = (FakeClient.extract(data) if task["kind"] == "extract"
                                          else FakeClient.consolidate(data))
                                inFlight.append((task, result))
                    elif action == 8 and inFlight:
                        self.store.complete(*inFlight.pop(rng.randrange(len(inFlight))))
                    elif action == 9:
                        self.store.close()
                        self.store = Store(path, client=FakeClient(), workers=0)
                        inFlight.clear()
                    else:
                        self.memories(uid)
                for task, result in inFlight:
                    self.store.complete(task, result)
                self.drain()
                for uid in ("u", "other"):
                    self.assertEqual(compare_user(uid, reference.user_state(uid), self.memories(uid)), [])


class SchedulerTests(unittest.TestCase):
    def job(self, key, sid="bulk", generation=1, kind="extract", priority=0):
        return dict(key=key, uid="u", sid=sid, generation=generation,
                    kind=kind, priority=priority, available=0)

    def test_source_round_robin_does_not_queue_live_work_behind_bulk(self):
        scheduler = Scheduler()
        for i in range(100):
            scheduler.put(self.job(str(i)))
        scheduler.put(self.job("live", sid="live"))
        first = [scheduler.take()["key"] for _ in range(2)]
        self.assertIn("live", first)
        scheduler.stop()

    def test_replaced_and_cancelled_jobs_are_not_dispatched(self):
        scheduler = Scheduler()
        scheduler.put(self.job("replaced", generation=1))
        scheduler.put(self.job("cancelled"))
        scheduler.put(self.job("replaced", generation=2))
        scheduler.cancel("cancelled")
        self.assertEqual(scheduler.take()["generation"], 2)
        self.assertEqual(scheduler.pending, {})
        scheduler.stop()

    def test_work_classes_all_get_service(self):
        scheduler = Scheduler()
        for i in range(20):
            scheduler.put(self.job("ordinary"+str(i)))
            scheduler.put(self.job("edit"+str(i), priority=1))
            scheduler.put(self.job("summary"+str(i), kind="summary"))
        first = [scheduler.take() for _ in range(5)]
        self.assertTrue(any(t["kind"] == "summary" for t in first))
        self.assertTrue(any(t["priority"] for t in first))
        self.assertEqual(sum(t["kind"] == "extract" and not t["priority"] for t in first), 3)
        scheduler.stop()


if __name__ == "__main__":
    unittest.main()
