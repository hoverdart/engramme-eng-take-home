"""Command line for the public test harness. Run it as `./harness run --scenario basic`."""

from __future__ import annotations

import argparse
import json
import random
import sys
from pathlib import Path

from memkit.sidecar import DEFAULT_CONFIG

from .procs import ProcessControl
from .reference import POLICIES
from .scenarios import SCENARIOS, Context, Result, run_scenario
from .service import ServiceClient, SidecarClient


def load_policy(path: str) -> str:
    config_path = Path(path)
    if not config_path.exists():
        print(f"note: {path} not found; assuming redelete_policy \"permanent\"", file=sys.stderr)
        return "permanent"
    policy = json.loads(config_path.read_text()).get("redelete_policy", "permanent")
    if policy not in POLICIES:
        sys.exit(f"{path}: redelete_policy must be one of {POLICIES}, got {policy!r}")
    return policy


def print_result(result: Result) -> None:
    status = "PASS" if result.passed else "FAIL"
    print(f"\n{status}  {result.name}")
    for key, value in result.metrics.items():
        print(f"      {key}: {value}")
    for problem in result.problems:
        print(f"    - {problem}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="harness", description="Public test harness for the memory service take-home.")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("list", help="list the public scenarios")
    run = commands.add_parser("run", help="run one scenario, or all of them")
    run.add_argument("--scenario", required=True, choices=[*SCENARIOS, "all"])
    run.add_argument("--service-url", default="http://localhost:8080")
    run.add_argument("--sidecar-url", default="http://localhost:7070")
    run.add_argument("--config", default="takehome.json", help="path to your takehome.json")
    run.add_argument("--seed", type=int, help="reuse a seed to replay the same workload")
    run.add_argument("--compose-dir", default=".", help="directory of your docker-compose.yml, for the crash scenario")
    run.add_argument("--kill-cmd", help="shell command that SIGKILLs your service (replaces docker compose)")
    run.add_argument("--start-cmd", help="shell command that starts your service in the background")
    args = parser.parse_args(argv)

    if args.command == "list":
        print("\n".join(SCENARIOS))
        return 0

    seed = args.seed if args.seed is not None else random.randrange(1, 1_000_000)
    service = ServiceClient(args.service_url)
    sidecar = SidecarClient(args.sidecar_url)
    if not sidecar.healthy():
        print(f"error: the memkit sidecar is not reachable at {args.sidecar_url}", file=sys.stderr)
        return 2
    if not service.wait_healthy(10):
        print(f"error: GET {args.service_url}/v1/health did not return 200", file=sys.stderr)
        return 2

    ctx = Context(
        service=service,
        sidecar=sidecar,
        procs=ProcessControl(args.compose_dir, args.kill_cmd, args.start_cmd),
        policy=load_policy(args.config),
        seed=seed,
        sidecar_defaults=dict(DEFAULT_CONFIG),
        log=lambda message: print(message, flush=True),
    )
    names = list(SCENARIOS) if args.scenario == "all" else [args.scenario]
    print(f"seed {seed}, redelete_policy {ctx.policy}")
    results = []
    for name in names:
        print(f"\nrunning {name} ...", flush=True)
        result = run_scenario(ctx, name)
        print_result(result)
        results.append(result)

    passed = sum(r.passed for r in results)
    print(f"\n{passed}/{len(results)} scenarios passed (seed {seed})")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
