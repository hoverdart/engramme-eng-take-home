#!/usr/bin/env python3
"""Run the public reference-checked backfill scenario at the full 20,000-item size.

This is a test driver, not application code. Only the harness uses sidecar admin
endpoints to restore the official latency/failure/concurrency settings.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'starter-kit'))
from harness_lib.__main__ import print_result
from harness_lib.procs import ProcessControl
from harness_lib.scenarios import Context, backfill, run_scenario
from harness_lib.service import ServiceClient, SidecarClient
from memkit.sidecar import DEFAULT_CONFIG


def main():
    # Reuse the official workload, live-event visibility probes and reference
    # comparison. The only difference from the public scenario is its size.
    context = Context(
        service=ServiceClient('http://localhost:8080'),
        sidecar=SidecarClient('http://localhost:7070'),
        procs=ProcessControl('.'), policy='permanent', seed=20261112,
        sidecar_defaults=dict(DEFAULT_CONFIG), log=print,
    )
    print('20,000-item backfill + 20 live events; seed 20261112', flush=True)
    result = run_scenario(context, 'backfill-20000', lambda run: backfill(run, backfill_items=20000))
    print_result(result)
    return 0 if result.passed else 1


if __name__ == '__main__':
    sys.exit(main())
