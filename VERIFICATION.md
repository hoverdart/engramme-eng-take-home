# Verification record

Run on November 11, 2026. Laptop: Apple M4 Max, macOS 26.6.2. Docker application and sidecar: Python 3.12.16. Host tools and isolated storage benchmark: Python 3.14.6. No managed services or real LLM/embedding APIs were used.

## Results

- `docker compose up --build -d --wait`: both services healthy. First-time image builds and named-volume setup succeeded.
- `./harness run --scenario all --seed 20261111`: **8/8 passed**. [Raw output](evidence/harness-20261111.txt).
- `./harness run --scenario all --seed 20261112`: **8/8 passed on final service source**, after the camelCase/comment pass and health-query index. [Raw output](evidence/harness-final-20261112.txt).
- `python3 -m unittest discover -s tests -v`: **20/20 passed**. Covers stale results, concurrent edit/forget, clean caches, shared lineage, version rules, permanent tombstones, reconnect, persisted jobs/retries, rollback, isolation, ID stability, and work-class/source fairness. [Raw output](evidence/unit-tests.txt).
- `python3 -m unittest discover -s starter-kit/tests -v`: **11/11 passed**. The supplied tests print Python resource-cleanup warnings; no assertion failed. [Raw output](evidence/starter-tests.txt).
- `node starter-kit/clients/typescript/test.mjs`: embedding fixtures and sidecar round trip passed.
- `python3 scripts/demo.py`: **all live HTTP assertions passed**. [Raw output](evidence/demo.txt).
- `python3 calculate_statistics.py`: closed-form CDF substitution and independent bisection checks passed. [Raw output](evidence/statistics.txt).
- `python3 -m compileall -q service scripts calculate_statistics.py tests`: passed.

## Full-sized backfill

Run `python3 scripts/stress_backfill.py` while Compose is healthy and no other harness is running. This reuses the public workload, reference model and checks, with 20,000 backfill items instead of 3,000, plus 20 live events. Sidecar defaults remain 50–200 ms latency, 2% transient failure probability and 20 shared calls.

[Raw output](evidence/backfill-20000.txt): **PASS**.

- Live events recallable p95: **0.29 s**, against a 5 s limit.
- Convergence after sending stopped: **123.4 s**, against `10 + 20020/100 = 210.2 s`.
- Total duration: **134.8 s**.
- 20,020 completed extractions; 13 completed consolidations.
- 402 transient failures handled; **0 rate-limit errors**, **0 invalid sidecar calls**.

The two official crash runs each performed four real application-container SIGKILL/restarts. The first delete run performed 5,600 read probes during concurrent forgetting; the second performed 2,359. Both passed.

## Storage-loaded performance floor

Run `python3 scripts/benchmark.py` after all other derivations have drained. It starts an isolated instance of the same service on loopback port 18080 and removes its temporary database afterward. The regular Docker application/database are not modified.

The fixture contains **200,000 live items, 200,000 atomic memories and support edges, and 500 summaries across 500 users**: 400 distinct facts per user. Test-only deterministic kit functions prepare this known fixture. The benchmark then performs **5,000 actual HTTP updates** with **24 concurrent writers and 8 concurrent recall readers**. New derivations call the real, unchanged sidecar. Every updated item has a distinct source item identity; these are genuine newer versions, not duplicate no-op requests.

[Raw output](evidence/benchmark-200000.txt): **PASS**.

- Accepted-write duration: **8.938 s**.
- Ingest: **559.399 events/s**, against the 200 events/s floor.
- Write acknowledgment p95: **58.922 ms**.
- Recall: **1,262 probes**, **62.639 ms p95**, against the 150 ms floor.
- Read errors: **0**.
- All pending jobs drained. Final fixture counts returned to 200,000 items/atomics/supports and 500 summaries.

This is a **storage-loaded benchmark**, not a 200,000-event end-to-end replay or a long-duration soak test. Its percentile is nearest-rank over measured requests. It checks final row counts; full text/lineage correctness is checked separately by the public reference-model scenarios and regression tests. The benchmark uses native host Python with the sidecar in Docker; public and 20,000-item tests run the application itself in Docker. Results characterize this machine and workload, not every hidden distribution or hardware configuration.

The hidden test suite was not available. No hidden-test pass is claimed.
