"""SQLite-backed memory service. All intelligence calls use the supplied sidecar.

One process owns the database and a bounded, source-fair worker pool. SQLite is
both the source of truth and the outbox; the in-memory scheduler is disposable.
"""
from __future__ import annotations

import hashlib
import heapq
import json
import logging
import os
import random
import re
import sqlite3
import sys
import threading
import time
from collections import OrderedDict, defaultdict, deque
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "starter-kit"))
from memkit import MemkitClient, RetryableError
from memkit.core import AtomicMemory

LOG = logging.getLogger("memory-service")


def identifier(*parts):
    return "mem_" + hashlib.sha256(json.dumps(parts, separators=(",", ":")).encode()).hexdigest()[:32]


def taskKey(kind, *parts):
    return json.dumps([kind, *parts], separators=(",", ":"))


class Scheduler:
    """Weighted work classes; round robin across (user, source) within a class.

    Updates replace queued work, while heap tokens discard obsolete schedules.
    Backoff waits consume no workers. All persistent state lives in `tasks`.
    """
    cycle = ("edit", "extract", "extract", "extract", "summary")

    def __init__(self):
        self.cv = threading.Condition()
        self.pending = {}
        self.heap = []
        self.lanes = {kind: OrderedDict() for kind in set(self.cycle)}
        self.serial = 0
        self.turn = 0
        self.stopped = False

    def put(self, task):
        task = dict(task)
        with self.cv:
            self.serial += 1
            token = self.serial
            self.pending[task["key"]] = (token, task)
            heapq.heappush(self.heap, (task["available"], token, task["key"]))
            self.cv.notify_all()

    def cancel(self, key):
        with self.cv:
            self.pending.pop(key, None)

    def take(self):
        with self.cv:
            while not self.stopped:
                now = time.time()
                while self.heap and self.heap[0][0] <= now:
                    _, token, key = heapq.heappop(self.heap)
                    current = self.pending.get(key)
                    if not current or current[0] != token:
                        continue
                    task = current[1]
                    group = "summary" if task["kind"] == "summary" else ("edit" if task["priority"] else "extract")
                    lane = (task["uid"], task["sid"])
                    self.lanes[group].setdefault(lane, deque()).append((key, token))
                for _ in self.cycle:
                    group = self.cycle[self.turn % len(self.cycle)]
                    self.turn += 1
                    lanes = self.lanes[group]
                    while lanes:
                        lane, queue = lanes.popitem(last=False)
                        key, token = queue.popleft()
                        if queue:
                            lanes[lane] = queue
                        current = self.pending.get(key)
                        if current and current[0] == token:
                            del self.pending[key]
                            return current[1]
                timeout = max(0.001, self.heap[0][0] - now) if self.heap else 1.0
                self.cv.wait(min(timeout, 1.0))
            return None

    def stop(self):
        with self.cv:
            self.stopped = True
            self.cv.notify_all()


SCHEMA = """
CREATE TABLE IF NOT EXISTS sources (
 uid TEXT NOT NULL, sid TEXT NOT NULL, connected INTEGER NOT NULL DEFAULT 1,
 PRIMARY KEY(uid,sid));
CREATE TABLE IF NOT EXISTS items (
 uid TEXT NOT NULL, sid TEXT NOT NULL, iid TEXT NOT NULL, version INTEGER NOT NULL,
 kind TEXT NOT NULL, content TEXT NOT NULL, forgotten INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY(uid,sid,iid));
CREATE TABLE IF NOT EXISTS atomics (
 uid TEXT NOT NULL, fact_key TEXT NOT NULL, id TEXT NOT NULL, topic TEXT NOT NULL,
 text TEXT NOT NULL, vector TEXT NOT NULL, PRIMARY KEY(uid,fact_key));
CREATE INDEX IF NOT EXISTS atomic_topic ON atomics(uid,topic);
CREATE TABLE IF NOT EXISTS supports (
 uid TEXT NOT NULL, sid TEXT NOT NULL, iid TEXT NOT NULL, fact_key TEXT NOT NULL,
 PRIMARY KEY(uid,sid,iid,fact_key));
CREATE INDEX IF NOT EXISTS support_fact ON supports(uid,fact_key);
CREATE TABLE IF NOT EXISTS topics (
 uid TEXT NOT NULL, topic TEXT NOT NULL, generation INTEGER NOT NULL,
 PRIMARY KEY(uid,topic));
CREATE TABLE IF NOT EXISTS summaries (
 uid TEXT NOT NULL, topic TEXT NOT NULL, id TEXT NOT NULL, text TEXT NOT NULL,
 vector TEXT NOT NULL, PRIMARY KEY(uid,topic));
CREATE TABLE IF NOT EXISTS tasks (
 key TEXT PRIMARY KEY, kind TEXT NOT NULL, uid TEXT NOT NULL, sid TEXT NOT NULL,
 iid TEXT NOT NULL, topic TEXT NOT NULL, generation INTEGER NOT NULL,
 available REAL NOT NULL, created REAL NOT NULL, attempt INTEGER NOT NULL DEFAULT 0,
 priority INTEGER NOT NULL DEFAULT 0, error TEXT);
CREATE INDEX IF NOT EXISTS task_source ON tasks(uid,sid,kind);
CREATE INDEX IF NOT EXISTS task_errors ON tasks(error) WHERE error IS NOT NULL;
"""


class Store:
    def __init__(self, path, client=None, workers=20):
        self.path = path
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA busy_timeout=5000")
        self.db.executescript(SCHEMA)
        self.lock = threading.RLock()
        self.scheduler = Scheduler()
        self.cache = OrderedDict()
        self.client = client or MemkitClient()
        self.threads = []
        self.toSchedule = []
        self.toCancel = []
        for task in self.db.execute("SELECT * FROM tasks WHERE error IS NULL"):
            self.scheduler.put(task)
        for i in range(min(20, workers)):
            thread = threading.Thread(target=self.worker, name=f"derive-{i}", daemon=True)
            thread.start()
            self.threads.append(thread)

    @contextmanager
    def transaction(self):
        # SQLite FULL + WAL is the durability boundary. Publish scheduler changes
        # only after COMMIT: a rollback must never leak an in-memory-only job.
        # The global lock makes this intentionally single-writer local design
        # simple to audit; database indexes keep each critical section short.
        with self.lock:
            self.toSchedule, self.toCancel = [], []
            self.db.execute("BEGIN IMMEDIATE")
            try:
                yield
                self.db.execute("COMMIT")
            except BaseException:
                self.db.execute("ROLLBACK")
                # Invalidation on rollback is harmless; never cache uncommitted data.
                raise
            else:
                for key in self.toCancel:
                    self.scheduler.cancel(key)
                for key in self.toSchedule:
                    task = self.db.execute("SELECT * FROM tasks WHERE key=?", (key,)).fetchone()
                    if task and task["error"] is None:
                        self.scheduler.put(task)

    def invalidate(self, uid):
        self.cache.pop(uid, None)

    def cancelTask(self, key):
        self.db.execute("DELETE FROM tasks WHERE key=?", (key,))
        self.toCancel.append(key)

    def enqueue(self, kind, uid, sid="", iid="", topic="", generation=0, priority=0):
        # One durable job per item/topic coalesces repeated updates. Summary
        # debounce waits 350 ms for a burst, capped at 2 s from its first change
        # so a continuously changing topic still gets scheduled fairly.
        key = taskKey(kind, uid, topic) if kind == "summary" else taskKey(kind, uid, sid, iid)
        now = time.time()
        old = self.db.execute("SELECT created FROM tasks WHERE key=?", (key,)).fetchone()
        created = old[0] if old else now
        available = min(now + 0.35, created + 2.0) if kind == "summary" else now
        self.db.execute("""INSERT INTO tasks(key,kind,uid,sid,iid,topic,generation,available,created,priority)
          VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(key) DO UPDATE SET
          generation=excluded.generation, available=excluded.available,
          priority=excluded.priority, attempt=0, error=NULL""",
          (key, kind, uid, sid, iid, topic, generation, available, created, priority))
        self.toSchedule.append(key)

    def changeTopics(self, uid, topics):
        # Only atomic membership changes require fresh summary text. Adding or
        # removing a backing item for an otherwise-live fact changes lineage
        # immediately, but does not spend another sidecar consolidation call.
        for topic in topics:
            self.db.execute("""INSERT INTO topics VALUES(?,?,1) ON CONFLICT(uid,topic)
                DO UPDATE SET generation=generation+1""", (uid, topic))
            generation = self.db.execute("SELECT generation FROM topics WHERE uid=? AND topic=?", (uid, topic)).fetchone()[0]
            # Never expose a summary whose set of facts may contain removed text.
            self.db.execute("DELETE FROM summaries WHERE uid=? AND topic=?", (uid, topic))
            count = self.db.execute("SELECT count(*) FROM atomics WHERE uid=? AND topic=?", (uid, topic)).fetchone()[0]
            if count >= 3:
                self.enqueue("summary", uid, topic=topic, generation=generation)
            else:
                self.cancelTask(taskKey("summary", uid, topic))
        self.invalidate(uid)

    def removeSupports(self, uid, sid, iid=None):
        clause = "s.uid=? AND s.sid=?" + (" AND s.iid=?" if iid is not None else "")
        args = (uid, sid) if iid is None else (uid, sid, iid)
        facts = self.db.execute(f"""SELECT DISTINCT a.fact_key,a.topic FROM supports s
            JOIN atomics a ON a.uid=s.uid AND a.fact_key=s.fact_key WHERE {clause}""", args).fetchall()
        self.db.execute("DELETE FROM supports WHERE " + clause.replace("s.", ""), args)
        changed = set()
        for fact in facts:
            if not self.db.execute("SELECT 1 FROM supports WHERE uid=? AND fact_key=? LIMIT 1", (uid, fact["fact_key"])).fetchone():
                self.db.execute("DELETE FROM atomics WHERE uid=? AND fact_key=?", (uid, fact["fact_key"]))
                changed.add(fact["topic"])
        self.invalidate(uid)
        return changed

    def event(self, event):
        # The version check, source row, item replacement, lineage changes and
        # derivation outbox all commit together. HTTP acknowledgement happens
        # only after this function returns, so an acknowledged write can survive
        # a kill between the response and the first extraction attempt.
        uid, sid, iid = event["user_id"], event["source_id"], event["item_id"]
        version, kind = event["version"], event["kind"]
        with self.transaction():
            old = self.db.execute("SELECT version,forgotten FROM items WHERE uid=? AND sid=? AND iid=?", (uid, sid, iid)).fetchone()
            if old and (old["forgotten"] or version <= old["version"]):
                return
            self.db.execute("INSERT OR IGNORE INTO sources VALUES(?,?,1)", (uid, sid))
            changed = self.removeSupports(uid, sid, iid) if old else set()
            self.db.execute("""INSERT INTO items VALUES(?,?,?,?,?,?,0) ON CONFLICT(uid,sid,iid)
                DO UPDATE SET version=excluded.version,kind=excluded.kind,content=excluded.content""",
                (uid, sid, iid, version, kind, event.get("content", "") if kind == "upsert" else ""))
            connected = self.db.execute("SELECT connected FROM sources WHERE uid=? AND sid=?", (uid, sid)).fetchone()[0]
            if kind == "upsert" and connected:
                self.enqueue("extract", uid, sid, iid, generation=version, priority=int(old is not None))
            else:
                self.cancelTask(taskKey("extract", uid, sid, iid))
            self.changeTopics(uid, changed)

    def forget(self, uid, sid, iid):
        # A permanent tombstone also exists for a never-before-seen item. This
        # closes the race where a delete arrives before the original event.
        # Clear stored content, remove backing edges, and invalidate summaries
        # before committing; no background cleanup is needed for clean reads.
        with self.transaction():
            changed = self.removeSupports(uid, sid, iid)
            self.db.execute("""INSERT INTO items VALUES(?,?,?,0,'delete','',1)
                ON CONFLICT(uid,sid,iid) DO UPDATE SET forgotten=1,kind='delete',content=''""", (uid, sid, iid))
            self.cancelTask(taskKey("extract", uid, sid, iid))
            self.changeTopics(uid, changed)

    def source(self, uid, sid, connected):
        # Disconnect hides every contribution but retains the latest upstream
        # item/version. Reconnect reschedules retained items even when backfill
        # repeats an already-seen version. User-forgotten items never qualify.
        with self.transaction():
            old = self.db.execute("SELECT connected FROM sources WHERE uid=? AND sid=?", (uid, sid)).fetchone()
            if old and old[0] == connected:
                return
            self.db.execute("INSERT INTO sources VALUES(?,?,?) ON CONFLICT(uid,sid) DO UPDATE SET connected=excluded.connected", (uid, sid, connected))
            if not connected:
                changed = self.removeSupports(uid, sid)
                for task in self.db.execute("SELECT key FROM tasks WHERE uid=? AND sid=? AND kind='extract'", (uid, sid)).fetchall():
                    self.cancelTask(task[0])
                self.changeTopics(uid, changed)
            else:
                for item in self.db.execute("SELECT iid,version FROM items WHERE uid=? AND sid=? AND kind='upsert' AND forgotten=0", (uid, sid)).fetchall():
                    self.enqueue("extract", uid, sid, item["iid"], generation=item["version"])

    def currentTask(self, task):
        current = self.db.execute("SELECT * FROM tasks WHERE key=?", (task["key"],)).fetchone()
        # Reconnect can create a new job for the SAME upstream item version.
        # Its creation time distinguishes that lifetime from a cancelled call
        # still in flight; neither its result nor its failure owns the new job.
        return current if (current and current["generation"] == task["generation"]
                           and current["created"] == task["created"]) else None

    def prepare(self, task):
        with self.lock:
            if not self.currentTask(task):
                return None
            if task["kind"] == "extract":
                item = self.db.execute("""SELECT i.* FROM items i JOIN sources s USING(uid,sid)
                  WHERE i.uid=? AND i.sid=? AND i.iid=? AND i.forgotten=0 AND i.kind='upsert'
                  AND s.connected=1 AND i.version=?""", (task["uid"], task["sid"], task["iid"], task["generation"])).fetchone()
                return item["content"] if item else None
            return [AtomicMemory(r["fact_key"], r["topic"], r["text"]) for r in
                self.db.execute("SELECT * FROM atomics WHERE uid=? AND topic=? ORDER BY fact_key", (task["uid"], task["topic"]))]

    def complete(self, task, result):
        # The network call runs without the database lock. Re-check its durable
        # generation at commit time: an edit, forget, disconnect or topic change
        # may have happened while the sidecar was working. A stale result is
        # discarded rather than resurrecting superseded data.
        with self.transaction():
            if not self.currentTask(task):
                return False
            uid = task["uid"]
            if task["kind"] == "extract":
                changed = set()
                for fact in result:
                    vector = json.dumps(self.client.embed(fact.text), separators=(",", ":"))
                    inserted = self.db.execute("INSERT OR IGNORE INTO atomics VALUES(?,?,?,?,?,?)", (uid, fact.fact_key, identifier("atomic", uid, fact.fact_key), fact.topic, fact.text, vector)).rowcount
                    self.db.execute("INSERT OR IGNORE INTO supports VALUES(?,?,?,?)", (uid, task["sid"], task["iid"], fact.fact_key))
                    if inserted:
                        changed.add(fact.topic)
                self.changeTopics(uid, changed)
            else:
                vector = json.dumps(self.client.embed(result.text), separators=(",", ":"))
                self.db.execute("INSERT OR REPLACE INTO summaries VALUES(?,?,?,?,?)", (uid, task["topic"], identifier("summary", uid, task["topic"]), result.text, vector))
                self.invalidate(uid)
            self.cancelTask(task["key"])
            return True

    def retry(self, task, exc, permanent=False):
        with self.transaction():
            current = self.currentTask(task)
            if not current:
                return
            attempt = current["attempt"] + 1
            delay = min(2.0, 0.05 * 2 ** min(attempt, 6)) * random.uniform(0.5, 1.5)
            self.db.execute("UPDATE tasks SET attempt=?,available=?,error=? WHERE key=?", (attempt, time.time() + delay, str(exc) if permanent else None, task["key"]))
            if not permanent:
                self.toSchedule.append(task["key"])
            LOG.warning("%s job %s: %s", "blocked" if permanent else "retry", task["key"], exc)

    def worker(self):
        while (task := self.scheduler.take()) is not None:
            try:
                data = self.prepare(task)
                if data is None:
                    continue
                result = self.client.extract(data) if task["kind"] == "extract" else self.client.consolidate(data)
                self.complete(task, result)
            except RetryableError as exc:
                self.retry(task, exc)
            except Exception as exc:
                # Do not acknowledge a silently broken derivation. Preserve its job
                # for diagnosis; health reports it as degraded until intervention.
                # Restart deliberately preserves blocked jobs instead of hiding them.
                LOG.exception("non-retryable derivation failure")
                self.retry(task, exc, permanent=True)

    def memories(self, uid):
        # Build one consistent per-user snapshot while holding the store lock.
        # Cache entries are immutable to callers and invalidated in every write
        # transaction that changes lineage/text. A read started after a forget
        # acknowledgement therefore cannot observe an old cached contribution.
        with self.lock:
            if uid in self.cache:
                self.cache.move_to_end(uid)
                return self.cache[uid]
            backers = defaultdict(list)
            for row in self.db.execute("SELECT fact_key,sid,iid FROM supports WHERE uid=? ORDER BY sid,iid", (uid,)):
                backers[row["fact_key"]].append({"source_id": row["sid"], "item_id": row["iid"]})
            result = []
            byTopic = defaultdict(list)
            for row in self.db.execute("SELECT * FROM atomics WHERE uid=?", (uid,)):
                memory = {"id": row["id"], "kind": "atomic", "fact_key": row["fact_key"], "topic": row["topic"], "text": row["text"], "lineage": {"items": backers[row["fact_key"]], "memories": []}}
                byTopic[row["topic"]].append(memory)
                result.append((memory, json.loads(row["vector"])))
            for row in self.db.execute("SELECT * FROM summaries WHERE uid=?", (uid,)):
                members = byTopic[row["topic"]]
                items = sorted({(i["source_id"], i["item_id"]) for m in members for i in m["lineage"]["items"]})
                memory = {"id": row["id"], "kind": "summary", "topic": row["topic"], "text": row["text"], "lineage": {"items": [{"source_id": s, "item_id": i} for s, i in items], "memories": sorted(m["id"] for m in members)}}
                result.append((memory, json.loads(row["vector"])))
            result.sort(key=lambda pair: pair[0]["id"])
            self.cache[uid] = result
            if len(self.cache) > 128:
                self.cache.popitem(last=False)
            return result

    def healthy(self):
        with self.lock:
            return self.db.execute("SELECT 1 FROM tasks WHERE error IS NOT NULL LIMIT 1").fetchone() is None

    def close(self):
        self.scheduler.stop()
        for thread in self.threads:
            thread.join(timeout=12)
        self.db.close()


class Server(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 128


class Handler(BaseHTTPRequestHandler):
    # HTTP/1.0 intentionally closes each connection, avoiding indefinitely idle
    # per-connection threads. The harness uses independent short-lived requests.
    def log_message(self, *args):
        pass

    def reply(self, status, body):
        payload = json.dumps(body, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def body(self):
        length = int(self.headers.get("Content-Length", "0"))
        if length < 0 or length > 2_000_000:
            raise ValueError("body must be at most 2 MB")
        data = json.loads(self.rfile.read(length) or b"{}")
        if not isinstance(data, dict):
            raise ValueError("body must be an object")
        return data

    def dispatch(self):
        parsed = urlsplit(self.path)
        parts = [unquote(p) for p in parsed.path.strip("/").split("/")]
        store = self.server.store
        if self.command == "GET" and parts == ["v1", "health"]:
            healthy = store.healthy()
            return self.reply(200 if healthy else 503, {"status": "ok" if healthy else "blocked derivation"})
        if self.command == "POST" and parts == ["v1", "events"]:
            event = self.body()
            for field in ("user_id", "source_id", "item_id"):
                if not isinstance(event.get(field), str) or not re.fullmatch(r"[A-Za-z0-9-]+", event[field]):
                    raise ValueError(f"invalid {field}")
            if type(event.get("version")) is not int or event["version"] <= 0:
                raise ValueError("version must be a positive integer")
            if event.get("kind") not in ("upsert", "delete"):
                raise ValueError("kind must be upsert or delete")
            if event["kind"] == "upsert" and not isinstance(event.get("content"), str):
                raise ValueError("upsert requires string content")
            if not isinstance(event.get("occurred_at"), str):
                raise ValueError("occurred_at must be a string")
            store.event(event)
            return self.reply(202, {"accepted": True})
        if len(parts) < 4 or parts[:2] != ["v1", "users"]:
            return self.reply(404, {"error": "not found"})
        uid, resource = parts[2:4]
        if self.command == "DELETE" and resource == "items" and len(parts) == 6:
            store.forget(uid, parts[4], parts[5])
            return self.reply(200, {"forgotten": True})
        if self.command == "POST" and resource == "sources" and len(parts) == 6 and parts[5] in ("connect", "disconnect"):
            store.source(uid, parts[4], int(parts[5] == "connect"))
            return self.reply(200, {"connected": parts[5] == "connect"})
        if self.command != "GET":
            return self.reply(404, {"error": "not found"})
        query = parse_qs(parsed.query)
        if resource == "recall" and len(parts) == 4:
            k = int(query.get("k", ["10"])[0])
            if not 1 <= k <= 100:
                raise ValueError("k must be between 1 and 100")
            vector = store.client.embed(query.get("q", [""])[0])
            ranked = heapq.nsmallest(k, ((-sum(a*b for a,b in zip(vector, v)), m["id"], m) for m,v in store.memories(uid)))
            return self.reply(200, {"memories": [dict(m, score=-score) for score, _, m in ranked]})
        if resource == "memories":
            memories = [m for m, _ in store.memories(uid)]
            if len(parts) == 5:
                memory = next((m for m in memories if m["id"] == parts[4]), None)
                return self.reply(200 if memory else 404, memory or {"error": "not found"})
            if len(parts) == 4:
                limit = int(query.get("limit", ["500"])[0])
                if not 1 <= limit <= 5000:
                    raise ValueError("limit must be between 1 and 5000")
                cursor = query.get("cursor", [""])[0]
                # Keyset pagination avoids offset shifts when earlier rows vanish.
                candidates = [m for m in memories if m["id"] > cursor]
                page = candidates[:limit]
                return self.reply(200, {"memories": page, "next_cursor": page[-1]["id"] if len(candidates) > limit else None})
        return self.reply(404, {"error": "not found"})

    def handleRequest(self):
        try:
            self.dispatch()
        except (ValueError, KeyError) as exc:
            self.reply(400, {"error": str(exc)})
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception:
            LOG.exception("request failed")
            self.reply(500, {"error": "internal error"})

    do_GET = do_POST = do_DELETE = handleRequest


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    store = Store(os.environ.get("DATABASE_PATH", "data/memory.sqlite3"))
    server = Server((os.environ.get("HOST", "0.0.0.0"), int(os.environ.get("PORT", "8080"))), Handler)
    server.store = store
    LOG.info("listening on %s, durable database %s", server.server_address, store.path)
    try:
        server.serve_forever()
    finally:
        server.server_close()
        store.close()


if __name__ == "__main__":
    main()
