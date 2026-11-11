# Engramme memory service

**Demo video: pending.**

A local Python service backed by SQLite, with durable ingestion, cross-source deduplication, lineage, incremental summaries, and immediate clean reads after forgetting. Part 2 is in [STATISTICS.md](STATISTICS.md); its calculations are reproducible with `python3 calculate_statistics.py`.

## Run

Requirements: Docker with Compose; Python 3.10+ on the host for the supplied harness and demo. No Python packages, API keys, or cloud services are required.

From this directory:

```sh
docker compose up --build
```

In another terminal:

```sh
./harness run --scenario basic
./harness run --scenario all
python3 scripts/demo.py
python3 calculate_statistics.py
```

The service listens on `http://localhost:8080`, and the supplied sidecar on `http://localhost:7070`. Compose binds both ports to loopback. `docker compose up` also builds missing images, so a fresh checkout needs no separate installation step. Stop with `docker compose down`; the named `memory-data` volume is retained. Do not delete that volume if the stored data is needed.

For an interactive demonstration of the API behavior, use `python3 scripts/demo.py --pause`. The complete original brief is preserved in [ASSIGNMENT.md](ASSIGNMENT.md), and the wire protocol is in [starter-kit/API.md](starter-kit/API.md).

## Design

### Process and storage

One application process serves HTTP requests and owns a pool of at most 20 derivation workers. SQLite stores the source of truth and the durable work queue. Transactions are serialized by a process-local lock, with WAL mode and `synchronous=FULL`. The database file, WAL, and shared-memory file live together in a named Docker volume. The application is deliberately **not** labeled persistent: the crash harness kills it. The sidecar retains the supplied persistent label.

The principal tables are:

- `items`: the highest accepted upstream version, current content/kind, and permanent user-delete tombstone for each `(user, source, item)`.
- `sources`: connection state for each `(user, source)`.
- `atomics`: one memory and embedding per `(user, fact_key)`.
- `supports`: item-to-fact edges, indexed in both directions. A fact remains live while at least one item supports it.
- `topics`: a monotonically increasing generation for each user's topic.
- `summaries`: the current complete summary for a topic.
- `tasks`: durable extraction/consolidation jobs, their input generation, retry time, and attempt count.

Atomic IDs are deterministic hashes of user and fact key, so an unchanged fact keeps the same ID across edits and shared-source changes. Summary IDs are deterministic hashes of user and topic. Summary lineage is assembled from the topic's current atomic memories and their live supporting items, rather than copying a second set of item edges.

### What a 2xx promises

An accepted event and its required derivation job commit in one SQLite transaction before the response is sent. A 2xx therefore means durable acceptance, not that extraction has already finished. Duplicate and older versions are acknowledged without changing state. Upstream deletes advance the version just like upserts.

Derivation calls happen outside the database lock. Their results and failures apply only if the durable task still has the same generation and creation time. A newer edit replaces the extraction generation; a user delete or disconnect cancels the task. Reconnect creates a fresh task even when the upstream version is unchanged, so a pre-disconnect call cannot complete or block its replacement. Old calls cannot recreate a forgotten item. Summary generation changes reject a result calculated from an obsolete fact set. Deterministic sidecar calls can safely be repeated after a lost response or process crash.

### Scheduling and summaries

The 20 workers bound combined `extract` and `consolidate` concurrency. The service calls these functions only through the supplied sidecar; embeddings use the supplied local implementation. No real model API is used.

The scheduler cycles through `edit, extract, extract, extract, summary`, skipping empty classes. Within each class it rotates across `(user, source)` lanes. This prevents a large backlog from one source from being the single FIFO queue in front of another source's live events. There is no explicit backfill bit in the event API, so source fairness provides the separation. Edits get a distinct work class, and both ordinary extraction and consolidation keep a share of work under sustained load.

Consolidation is incremental by topic. A change to a topic's atomic fact set invalidates its old summary and replaces its pending summary job. Rebuilds wait for 350 ms of quiet, with a 2-second cap on postponement from the first outstanding change. Topics with fewer than three facts have no summary. A change only to a fact's supporting items does not require another model call: its text is unchanged and lineage is assembled from current edges.

Retryable failures use exponential backoff with jitter. Waiting jobs release their worker slots. Unexpected failures stay visible as blocked durable jobs and cause the health endpoint to report degradation; they are not silently discarded. They require diagnosis and intervention: restarting preserves blocked jobs rather than automatically retrying them.

### Edits and forgetting

An edit removes the item's previous support edges synchronously, then extracts the new content asynchronously. This favors clean, conservative reads over serving stale facts. A fact can briefly disappear while an edit is re-derived, but its deterministic ID is preserved if it returns unchanged.

For a user delete, one transaction:

1. Sets a permanent tombstone and clears the stored item content.
2. Removes that item's support edges.
3. Removes any atomics that now have no other supporting item.
4. Removes affected summaries, schedules eligible replacements, cancels obsolete extraction, and invalidates cached reads.

Reads beginning after the acknowledgment use the committed state, so neither stale text nor forgotten lineage remains accessible through list, recall, or memory-by-ID. Facts still supported elsewhere remain. An invalidated summary is absent until safely rebuilt; incomplete summaries are never returned.

`takehome.json` declares `permanent`: even a higher upstream version cannot restore an item explicitly forgotten by the user. Tombstones do not expire. Disconnect uses the same synchronous support cleanup but retains item versions/content without creating user-delete tombstones. Connect re-enqueues the latest retained live items, so same-version backfills work and permanently forgotten items stay absent.

### Recall and pagination

Recall computes exact cosine similarity over the user's stored 64-dimensional vectors and returns the best `k` atomic or summary memories with lineage. Results are cached per user in a bounded 128-user LRU cache and invalidated by relevant writes. Memory listing uses stable ID ordering and keyset cursors. Pagination is not a multi-request snapshot under concurrent writes; the harness's final convergence checks run after input stops.

### Crash recovery

The SQL transaction contains both the logical write and durable work. If a process dies before commit, the write was not acknowledged. If it dies after commit but before scheduling, startup reconstructs the in-memory scheduler from `tasks`. If it dies during a sidecar call or before committing the result, that job runs again. Memory changes and task completion commit together, so a durable job cannot be lost in a gap between two stores.

## Verification

Run the public scenarios and record their exact output rather than inferring hidden-test success. `all` includes actual application-container SIGKILL/restart tests. The scripted demo checks live HTTP behavior for shared lineage, edit IDs, summary updates, immediate clean reads, and reconnect tombstones. The statistics script checks its closed-form values against independent numerical CDF bisection.

The reviewed service passed all eight public scenarios with seed `20261110`: basic, dupes, reorder, cross-source, edit, backfill, delete, and crash. Backfill live-visibility p95 was 0.28 seconds; deletion checks ran 4,931 read probes; the crash test performed four SIGKILLs. The run recorded zero rate-limit errors. See [the full harness output](evidence/review-harness.txt).

The 26 service and HTTP tests pass, including 8,000 seeded operations mixing edits, deletes, reconnects, delayed results, and database restarts. The original implementation also passed the public scenarios with seeds `20261111` and `20261112`. See [VERIFICATION.md](VERIFICATION.md) for commands, scope, and raw evidence.

The full **20,000-item backfill** passed against the reference model: live recall p95 **0.29 seconds**, convergence **122.4 seconds** after sending stopped, and zero rate-limit errors. This is within the scenario's 210.2-second convergence budget. See [the reviewed run](evidence/review-backfill-20000.txt).

An isolated **200,000-item / 500-user** storage-loaded benchmark measured **508.245 accepted events/s** and **75.377 ms recall p95**, with 24 writers and 8 readers. It starts from a prepared fixture, then makes 5,000 real HTTP updates, waits for real sidecar derivations to converge, and verifies the final text and lineage for all 500 users against the reference model. It is not a claim that 200,000 events were replayed end-to-end, and hidden tests have not been run.

```sh
python3 -m unittest discover -s tests -v
python3 scripts/stress_backfill.py
python3 scripts/benchmark.py
```

Run these load checks sequentially, with no other active harness: they share the sidecar's concurrency budget. The benchmark uses a temporary database and port 18080; it does not alter the Docker service's database.

## Tradeoffs and limits

- This is a single-process service. Multiple application replicas against the same database are unsupported: the lock and worker-budget ownership are local to the process.
- SQLite's single writer, synchronous durability, and synchronous disconnect cleanup can make very large writes or disconnects block other requests. The choice keeps the correctness boundary small and inspectable.
- Recall is exact and linear in a user's memories. Cached materialized lineage can be large for heavily shared facts. A user with unusually high cardinality is more expensive than an evenly distributed dataset.
- Source fairness does not distinguish live and backfill traffic within the same source lane. Sustained work beyond sidecar capacity still creates a durable backlog. Admission throughput is distinct from derivation throughput.
- The API has no authentication, tenant authorization, or production hardening; those are out of scope. Keep it local.
- Permanent deletion removes API-visible information and clears current item content, but does not promise forensic erasure from historical SQLite/WAL pages, backups, or disk blocks.
- Tombstones and topic generation records are retained indefinitely. Disk quota handling and operational repair tooling remain future work.

At 100× the data, partition ownership by user, use a transactional database/outbox with explicit worker leases and a shared concurrency budget, move to per-user vector indexes, and fetch lineage only for selected results. Large disconnects would use a durable source-generation visibility fence so reads become clean immediately while edge cleanup proceeds in batches. Add queue-age/error metrics, admission control, migrations, and a recovery runbook before operating that system.

AI assistance: this implementation, supporting documentation, demo tooling, and statistics work were prepared with OpenAI Codex assistance.
