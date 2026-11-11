# Verification record

Reviewed on November 10, 2026 (Pacific; November 11 UTC). Laptop: Apple M4 Max, macOS 26.6.2. Docker application and sidecar: Python 3.12.16. Host tools and isolated storage benchmark: Python 3.14.6. No managed services or real LLM/embedding APIs were used. Load checks ran sequentially to keep the shared sidecar budget isolated.

## Results

- `docker compose up --build -d --wait`: rebuilt application and healthy services. [Raw output](evidence/review-build.txt).
- `./harness run --scenario all --seed 20261110`: **8/8 passed on the reviewed service**, including four real application-container SIGKILL/restarts. Zero rate-limit errors and zero invalid sidecar calls. [Raw output](evidence/review-harness.txt).
- `python3 -m unittest discover -s tests -v`: **26/26 passed**. Covers stale results, concurrent edit/forget, clean caches, shared lineage, version rules, permanent tombstones, reconnect, persisted jobs/retries, rollback, isolation, ID stability, work-class/source fairness, HTTP pagination, recall ranking/scores, and input rejection. A seeded reference comparison runs **20 seeds × 400 operations**, including delayed derivations and database restarts. [Raw output](evidence/review-unit-tests.txt).
- `python3 -m unittest discover -s starter-kit/tests -v`: **11/11 passed**. The supplied tests print Python resource-cleanup warnings; no assertion failed. [Raw output](evidence/review-starter-tests.txt).
- `node starter-kit/clients/typescript/test.mjs`: embedding fixtures and sidecar round trip passed without skipping. [Raw output](evidence/review-typescript.txt).
- `python3 scripts/demo.py`: **all live HTTP assertions passed**. [Raw output](evidence/review-demo.txt).
- `python3 calculate_statistics.py`: closed-form CDF substitution and independent bisection checks passed; all three levels and percentage denominators were reviewed against the brief. [Raw output](evidence/review-statistics.txt).
- `python3 -m compileall -q service scripts calculate_statistics.py tests`: passed.

The review fixed a same-version reconnect race: a pre-disconnect extraction could match the replacement task using the upstream version alone. Results and failures now also match the task's creation time. Two focused regressions fail against the previous implementation and pass with the fix; an old failure cannot block a reconnected job.

## Full-sized backfill

Run `python3 scripts/stress_backfill.py` while Compose is healthy and no other harness is running. This reuses the public workload, reference model and checks, with 20,000 backfill items instead of 3,000, plus 20 live events. Sidecar defaults remain 50–200 ms latency, 2% transient failure probability and 20 shared calls.

[Raw output](evidence/review-backfill-20000.txt): **PASS**.

- Live events recallable p95: **0.29 s**, against a 5 s limit.
- Convergence after sending stopped: **122.4 s**, against `10 + 20020/100 = 210.2 s`.
- Total duration: **133.8 s**.
- 20,020 completed extractions; 13 completed consolidations.
- 421 transient failures handled; **0 rate-limit errors**, **0 invalid sidecar calls**.

The reviewed delete scenario performed **4,931 read probes** during concurrent forgetting and passed, including reconnect and backfill afterward.

## Storage-loaded performance floor

Run `python3 scripts/benchmark.py` after all other derivations have drained. It starts an isolated instance of the same service on loopback port 18080 and removes its temporary database afterward. The regular Docker application/database are not modified.

The fixture contains **200,000 live items, 200,000 atomic memories and support edges, and 500 summaries across 500 users**: 400 distinct facts per user. Test-only deterministic kit functions prepare this known fixture. The benchmark then performs **5,000 actual HTTP updates** with **24 concurrent writers and 8 concurrent recall readers**. New derivations call the real, unchanged sidecar. Every updated item has a distinct source item identity; these are genuine newer versions, not duplicate no-op requests.

[Raw output](evidence/review-benchmark-200000.txt): **PASS**.

- Accepted-write duration: **9.838 s**.
- Ingest: **508.245 events/s**, against the 200 events/s floor.
- Write acknowledgment p95: **72.419 ms**.
- Recall: **1,286 probes**, **75.377 ms p95**, against the 150 ms floor.
- Read errors: **0**.
- All pending jobs drained. Final fixture counts returned to 200,000 items/atomics/supports and 500 summaries.
- Final HTTP reads for **all 500 users** matched the reference model's atomic text, summary text, and lineage, with zero problems. This replaces the original benchmark's count-only final check. Benchmark resources are closed on failure as well as success.

This is a **storage-loaded benchmark**, not a 200,000-event end-to-end replay or a long-duration soak test. Its percentile is nearest-rank over measured requests. The benchmark uses native host Python with the sidecar in Docker; public and 20,000-item tests run the application itself in Docker. Results characterize this machine and workload, not every hidden distribution or hardware configuration.

Earlier public runs on the original implementation also passed with seeds [20261111](evidence/harness-20261111.txt) and [20261112](evidence/harness-final-20261112.txt). Their raw evidence is retained separately from the reviewed runs above.

## Recorded demonstration

The video linked at the top of the README combines the candidate's recorded narration with actual local application captures. Its backfill waiting interval is visibly accelerated. The captured runs passed:

- HTTP remembering, editing, shared lineage, immediate clean reads after forgetting, summary rebuilding, and reconnect tombstones. [Raw output](evidence/recording-demo.txt).
- The full 20,000-item backfill: **0.30 s live recall p95**, **123.7 s convergence** after sending stopped, zero rate-limit errors, and a successful reference-model comparison. [Raw output](evidence/recording-backfill-20000.txt).
- Four real application-container SIGKILL/restarts, followed by a successful reference-model comparison. [Raw output](evidence/recording-crash.txt).
- All three statistics levels, with analytic-CDF substitution and independent bisection checks. [Raw output](evidence/recording-statistics.txt).

The video also covers the current tradeoffs and what would change at 100× the data. [Chapter timestamps](video-chapters.txt).

The hidden test suite was not available. No hidden-test pass is claimed.
