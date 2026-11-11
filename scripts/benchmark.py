#!/usr/bin/env python3
"""Storage-loaded HTTP benchmark, not a 200,000-event end-to-end replay.

An isolated fixture holds 200,000 live items and distinct per-user atomic facts
across 500 users. A real Store/HTTP server then handles concurrent writes/recall;
new derivations use the normal sidecar with its unchanged latency and failures.
The fixture uses deterministic kit functions ONLY to prepare known test data.
Never run this concurrently with a harness scenario: both share the sidecar's
20-call budget. Nothing is inserted into the application's Docker database.
"""
import json
import math
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import quote
from urllib.request import Request, urlopen

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from service.app import Handler, Server, Store, identifier
from memkit import core


def requestJson(method, path, body=None):
    payload = json.dumps(body).encode() if body is not None else None
    request = Request('http://127.0.0.1:18080' + path, data=payload, method=method,
                      headers={'Content-Type': 'application/json'})
    with urlopen(request, timeout=30) as response:
        return json.load(response)


def percentile95(values):
    # Nearest-rank percentile: retain a clear, reproducible definition.
    ordered = sorted(values)
    return ordered[math.ceil(0.95 * len(ordered)) - 1]


def seedFixture(databasePath):
    store = Store(databasePath, workers=0)
    # Each user has 400 UNIQUE facts, one per live item. Different users reuse
    # the same text but still get independently namespaced memory identities.
    facts = [core.extract(f'The Atlas project budget is {120000+i} dollars.')[0]
             for i in range(400)]
    summary = core.consolidate(facts)
    vectors = [json.dumps(core.embed(fact.text)) for fact in facts]
    summaryVector = json.dumps(core.embed(summary.text))
    with store.transaction():
        for userIndex in range(500):
            userId = f'bench-{userIndex}'
            store.db.execute('INSERT INTO sources VALUES(?,?,1)', (userId, 'mail'))
            store.db.executemany('INSERT INTO items VALUES(?,?,?,?,?,?,0)',
                [(userId, 'mail', f'item-{i}', 1, 'upsert', fact.text) for i,fact in enumerate(facts)])
            store.db.executemany('INSERT INTO atomics VALUES(?,?,?,?,?,?)',
                [(userId, fact.fact_key, identifier('atomic', userId, fact.fact_key),
                  fact.topic, fact.text, vectors[i]) for i,fact in enumerate(facts)])
            store.db.executemany('INSERT INTO supports VALUES(?,?,?,?)',
                [(userId, 'mail', f'item-{i}', fact.fact_key) for i,fact in enumerate(facts)])
            store.db.execute('INSERT INTO topics VALUES(?,?,1)', (userId, summary.topic))
            store.db.execute('INSERT INTO summaries VALUES(?,?,?,?,?)',
                (userId, summary.topic, identifier('summary', userId, summary.topic),
                 summary.text, summaryVector))
    store.close()
    return facts


def main():
    print('Seeding isolated 200,000-item / 500-user / 200,000-atomic fixture...', flush=True)
    with tempfile.TemporaryDirectory(prefix='engramme-benchmark-') as directory:
        databasePath = str(Path(directory) / 'memory.sqlite3')
        facts = seedFixture(databasePath)
        store = Store(databasePath)
        server = Server(('127.0.0.1', 18080), Handler)
        server.store = store
        serverThread = threading.Thread(target=server.serve_forever, daemon=True)
        serverThread.start()
        stopProbes = threading.Event()
        recallLatencies, writeLatencies, errors = [], [], []
        listLock = threading.Lock()
        writeCount = 5000

        def writeEvent(index):
            userIndex = index % 500
            # Update ten distinct existing items per user. Unchanged fact text
            # deliberately tests the real remove/rederive and version path.
            itemIndex = index // 500
            event = dict(user_id=f'bench-{userIndex}', source_id='mail',
                         item_id=f'item-{itemIndex}', version=2, kind='upsert',
                         content=facts[itemIndex].text, occurred_at='2026-11-11T00:00:00Z')
            started = time.perf_counter()
            requestJson('POST', '/v1/events', event)
            with listLock:
                writeLatencies.append(1000 * (time.perf_counter() - started))

        def recallProbe(probeIndex):
            index = probeIndex
            while not stopProbes.is_set():
                started = time.perf_counter()
                try:
                    result = requestJson('GET', f'/v1/users/bench-{index % 500}/recall?q=' + quote(facts[index % 400].text) + '&k=10')
                    assert len(result['memories']) == 10
                    with listLock:
                        recallLatencies.append(1000 * (time.perf_counter() - started))
                except Exception as exc:
                    with listLock:
                        errors.append(str(exc))
                index += 8
                # Keep all eight readers active without a synthetic busy loop.
                time.sleep(0.01)

        probes = [threading.Thread(target=recallProbe, args=(i,)) for i in range(8)]
        for probe in probes:
            probe.start()
        started = time.perf_counter()
        try:
            # Exactly 32 possible in-flight HTTP requests: 24 writers + 8 readers.
            with ThreadPoolExecutor(max_workers=24) as pool:
                list(pool.map(writeEvent, range(writeCount)))
            duration = time.perf_counter() - started
        finally:
            stopProbes.set()
            for probe in probes:
                probe.join()
        rate = writeCount / duration
        recallP95 = percentile95(recallLatencies)
        print(f'Writes: {writeCount}; duration: {duration:.3f}s; ingest: {rate:.3f} events/s')
        print(f'Write ack p95: {percentile95(writeLatencies):.3f}ms')
        print(f'Recall probes: {len(recallLatencies)}; p95: {recallP95:.3f}ms; errors: {len(errors)}')
        print('Waiting for real sidecar derivations to drain...', flush=True)
        deadline = time.monotonic() + 120
        pending = 1
        while pending and time.monotonic() < deadline:
            with store.lock:
                pending = store.db.execute('SELECT count(*) FROM tasks').fetchone()[0]
            if pending:
                time.sleep(0.5)
        with store.lock:
            counts = {table: store.db.execute(f'SELECT count(*) FROM {table}').fetchone()[0]
                      for table in ('items', 'atomics', 'supports', 'summaries')}
        print(f'Converged counts: {counts}; pending tasks: {pending}')
        server.shutdown()
        server.server_close()
        serverThread.join()
        store.close()
        passed = (rate >= 200 and recallP95 < 150 and not errors and not pending
                  and counts == dict(items=200000, atomics=200000, supports=200000, summaries=500))
        print('PASS storage-loaded benchmark' if passed else 'FAIL storage-loaded benchmark')
        return 0 if passed else 1


if __name__ == '__main__':
    sys.exit(main())
