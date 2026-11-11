"""HTTP sidecar that serves extract() and consolidate() the way a real model API would:
slowly, sometimes failing, and under one shared concurrency limit.

Run it with `python -m memkit.sidecar`. Settings come from environment variables:
MEMKIT_PORT, MEMKIT_LATENCY_MIN_MS, MEMKIT_LATENCY_MAX_MS, MEMKIT_FAILURE_RATE,
MEMKIT_MAX_CONCURRENCY and MEMKIT_SEED.
"""

from __future__ import annotations

import json
import os
import random
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Callable

from . import core

DEFAULT_CONFIG = {
    "latency_min_ms": 50,
    "latency_max_ms": 200,
    "failure_rate": 0.02,
    "max_concurrency": 20,
}


class SidecarState:
    def __init__(self, config: dict, seed: int | None):
        self._lock = threading.Lock()
        self._rng = random.Random(seed)
        self._active = 0
        self.config = dict(config)
        self.stats = self._empty_stats()

    @staticmethod
    def _empty_stats() -> dict:
        return {
            "extract_calls": 0,
            "consolidate_calls": 0,
            "extract_completed": 0,
            "consolidate_completed": 0,
            "rate_limited": 0,
            "transient_failures": 0,
            "invalid_requests": 0,
            "peak_concurrency": 0,
        }

    def count(self, key: str) -> None:
        with self._lock:
            self.stats[key] += 1

    def try_acquire(self, op: str) -> bool:
        with self._lock:
            self.stats[f"{op}_calls"] += 1
            if self._active >= self.config["max_concurrency"]:
                self.stats["rate_limited"] += 1
                return False
            self._active += 1
            self.stats["peak_concurrency"] = max(self.stats["peak_concurrency"], self._active)
            return True

    def release(self) -> None:
        with self._lock:
            self._active -= 1

    def draw(self) -> tuple[float, bool]:
        with self._lock:
            low, high = self.config["latency_min_ms"], self.config["latency_max_ms"]
            latency = self._rng.uniform(low, max(low, high)) / 1000
            fails = self._rng.random() < self.config["failure_rate"]
            return latency, fails

    def update_config(self, changes: dict) -> None:
        unknown = set(changes) - set(DEFAULT_CONFIG)
        if unknown:
            raise ValueError(f"unknown config keys: {sorted(unknown)}")
        with self._lock:
            self.config.update(changes)

    def snapshot(self) -> tuple[dict, dict]:
        with self._lock:
            return dict(self.config), dict(self.stats)

    def reset_stats(self) -> None:
        with self._lock:
            self.stats = self._empty_stats()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    state: SidecarState  # set by make_server()

    def log_message(self, format: str, *args) -> None:  # noqa: A002 - signature from the base class
        pass

    def _send(self, status: int, body: dict) -> None:
        payload = json.dumps(body).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _read_json(self) -> dict | None:
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b""
        try:
            body = json.loads(raw or b"{}")
        except json.JSONDecodeError:
            return None
        return body if isinstance(body, dict) else None

    def _invalid(self, message: str) -> None:
        self.state.count("invalid_requests")
        self._send(400, {"error": message})

    def _model_call(self, op: str, work: Callable[[], dict]) -> None:
        if not self.state.try_acquire(op):
            self._send(429, {"error": "rate limited: too many concurrent calls, retry later"})
            return
        try:
            latency, fails = self.state.draw()
            time.sleep(latency)
            if fails:
                self.state.count("transient_failures")
                self._send(503, {"error": "transient failure, retry"})
                return
            self._send(200, work())
            self.state.count(f"{op}_completed")
        finally:
            self.state.release()

    def do_GET(self) -> None:  # noqa: N802 - name required by BaseHTTPRequestHandler
        if self.path == "/v1/health":
            self._send(200, {"ok": True})
        elif self.path == "/admin/stats":
            self._send(200, self.state.snapshot()[1])
        elif self.path == "/admin/config":
            self._send(200, self.state.snapshot()[0])
        else:
            self._send(404, {"error": "not found"})

    def do_POST(self) -> None:  # noqa: N802 - name required by BaseHTTPRequestHandler
        body = self._read_json()
        if body is None:
            self._invalid("request body must be a JSON object")
            return

        if self.path == "/v1/extract":
            content = body.get("content")
            if not isinstance(content, str):
                self._invalid("`content` must be a string")
                return
            self._model_call(
                "extract",
                lambda: {"memories": [m.to_dict() for m in core.extract(content)]},
            )

        elif self.path == "/v1/consolidate":
            try:
                memories = [core.AtomicMemory.from_dict(m) for m in body["memories"]]
                summary = core.consolidate(memories)
            except (KeyError, TypeError) as exc:
                self._invalid(f"`memories` must be a list of {{fact_key, topic, text}}: {exc}")
                return
            except core.ConsolidationError as exc:
                self._invalid(str(exc))
                return
            self._model_call("consolidate", lambda: {"summary": summary.to_dict()})

        elif self.path == "/v1/embed":
            text = body.get("text")
            if not isinstance(text, str):
                self._invalid("`text` must be a string")
                return
            self._send(200, {"vector": core.embed(text)})

        elif self.path == "/admin/config":
            try:
                self.state.update_config(body)
            except ValueError as exc:
                self._invalid(str(exc))
                return
            self._send(200, self.state.snapshot()[0])

        elif self.path == "/admin/stats/reset":
            self.state.reset_stats()
            self._send(200, self.state.snapshot()[1])

        else:
            self._send(404, {"error": "not found"})


class _Server(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 512


def make_server(port: int, config: dict, seed: int | None) -> ThreadingHTTPServer:
    handler = type("BoundHandler", (Handler,), {"state": SidecarState(config, seed)})
    return _Server(("0.0.0.0", port), handler)


def main() -> None:
    env = os.environ
    config = {
        "latency_min_ms": float(env.get("MEMKIT_LATENCY_MIN_MS", DEFAULT_CONFIG["latency_min_ms"])),
        "latency_max_ms": float(env.get("MEMKIT_LATENCY_MAX_MS", DEFAULT_CONFIG["latency_max_ms"])),
        "failure_rate": float(env.get("MEMKIT_FAILURE_RATE", DEFAULT_CONFIG["failure_rate"])),
        "max_concurrency": int(env.get("MEMKIT_MAX_CONCURRENCY", DEFAULT_CONFIG["max_concurrency"])),
    }
    seed = int(env["MEMKIT_SEED"]) if "MEMKIT_SEED" in env else None
    port = int(env.get("MEMKIT_PORT", "7070"))
    server = make_server(port, config, seed)
    print(f"memkit sidecar listening on :{port} with {config}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
