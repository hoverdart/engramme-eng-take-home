"""HTTP clients for the candidate's service and the memkit sidecar."""

from __future__ import annotations

import json
import random
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass

from .workload import Event, ItemKey


class ServiceError(Exception):
    """The service answered in a way the API does not allow."""


@dataclass
class Response:
    status: int
    body: object
    raw: str


def http(method: str, url: str, body: dict | None = None, timeout: float = 10.0) -> Response:
    """Raises OSError (including timeouts) when the server cannot be reached."""
    data = json.dumps(body).encode("utf-8") if body is not None else None
    headers = {"Content-Type": "application/json"} if data is not None else {}
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            status, raw = response.status, response.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        status, raw = exc.code, exc.read().decode("utf-8", "replace")
    except urllib.error.URLError as exc:
        raise OSError(str(exc.reason)) from None
    try:
        parsed = json.loads(raw) if raw else None
    except json.JSONDecodeError:
        parsed = None
    return Response(status, parsed, raw)


def _q(part: str) -> str:
    return urllib.parse.quote(part, safe="")


class ServiceClient:
    def __init__(self, base_url: str, write_deadline_s: float = 180.0):
        self.base_url = base_url.rstrip("/")
        self.write_deadline_s = write_deadline_s
        self._lock = threading.Lock()
        self.writes = 0
        self.write_retries = 0

    # Writes are retried until acknowledged, the way a source or a user's client would.

    def send_event(self, event: Event) -> None:
        self._write("POST", "/v1/events", event.to_json())

    def user_delete(self, key: ItemKey) -> None:
        user_id, source_id, item_id = key
        self._write("DELETE", f"/v1/users/{_q(user_id)}/items/{_q(source_id)}/{_q(item_id)}")

    def disconnect(self, user_id: str, source_id: str) -> None:
        self._write("POST", f"/v1/users/{_q(user_id)}/sources/{_q(source_id)}/disconnect", {})

    def connect(self, user_id: str, source_id: str) -> None:
        self._write("POST", f"/v1/users/{_q(user_id)}/sources/{_q(source_id)}/connect", {})

    def _write(self, method: str, path: str, body: dict | None = None) -> None:
        deadline = time.monotonic() + self.write_deadline_s
        delay = 0.05
        while True:
            try:
                response = http(method, self.base_url + path, body)
                if 200 <= response.status < 300:
                    with self._lock:
                        self.writes += 1
                    return
                if 400 <= response.status < 500 and response.status != 429:
                    raise ServiceError(f"{method} {path} returned {response.status}: {response.raw[:200]}")
            except OSError:
                pass
            if time.monotonic() > deadline:
                raise ServiceError(f"{method} {path} was never acknowledged within {self.write_deadline_s:.0f}s")
            with self._lock:
                self.write_retries += 1
            time.sleep(delay * (0.5 + random.random()))
            delay = min(delay * 2, 2.0)

    # Reads.

    def list_memories(self, user_id: str) -> list[dict]:
        memories: list[dict] = []
        cursor = None
        for _ in range(10_000):
            query = "?limit=500" + (f"&cursor={_q(cursor)}" if cursor else "")
            body = self._read(f"/v1/users/{_q(user_id)}/memories{query}")
            page = body.get("memories")
            if not isinstance(page, list):
                raise ServiceError("list response has no `memories` array")
            memories.extend(page)
            cursor = body.get("next_cursor")
            if not cursor:
                return memories
        raise ServiceError("list pagination never ended")

    def recall(self, user_id: str, query: str, k: int = 10) -> list[dict]:
        body = self._read(f"/v1/users/{_q(user_id)}/recall?q={_q(query)}&k={k}")
        results = body.get("memories")
        if not isinstance(results, list):
            raise ServiceError("recall response has no `memories` array")
        return results

    def get_memory(self, user_id: str, memory_id: str) -> dict | None:
        return self._read(f"/v1/users/{_q(user_id)}/memories/{_q(memory_id)}", allow_404=True)

    def healthy(self) -> bool:
        try:
            return http("GET", self.base_url + "/v1/health", timeout=2).status == 200
        except OSError:
            return False

    def wait_healthy(self, timeout_s: float) -> bool:
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            if self.healthy():
                return True
            time.sleep(0.2)
        return False

    def _read(self, path: str, allow_404: bool = False):
        last_error = ""
        for attempt in range(5):
            try:
                response = http("GET", self.base_url + path)
            except OSError as exc:
                last_error = str(exc)
            else:
                if response.status == 200 and isinstance(response.body, dict):
                    return response.body
                if response.status == 404 and allow_404:
                    return None
                last_error = f"status {response.status}: {response.raw[:200]}"
                if response.status < 500:
                    break
            time.sleep(0.1 * (attempt + 1))
        raise ServiceError(f"GET {path} failed: {last_error}")


class SidecarClient:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")

    def healthy(self) -> bool:
        try:
            return http("GET", self.base_url + "/v1/health", timeout=2).status == 200
        except OSError:
            return False

    def reset(self, config: dict) -> None:
        """Restores default latency, failure rate and limit, so a scenario never inherits changes."""
        http("POST", self.base_url + "/admin/config", config)
        http("POST", self.base_url + "/admin/stats/reset", {})

    def configure(self, changes: dict) -> None:
        http("POST", self.base_url + "/admin/config", changes)

    def stats(self) -> dict:
        response = http("GET", self.base_url + "/admin/stats")
        return response.body if isinstance(response.body, dict) else {}
