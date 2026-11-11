"""Thin Python client for the memkit sidecar.

It never retries: deciding when and how to retry is part of the exercise.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from typing import Sequence

from . import core
from .core import AtomicMemory, SummaryMemory


class MemkitError(Exception):
    pass


class RetryableError(MemkitError):
    """Rate limited (429), a transient failure (503), or the sidecar was unreachable."""

    def __init__(self, message: str, status: int | None = None):
        super().__init__(message)
        self.status = status


class InvalidRequestError(MemkitError):
    """The sidecar rejected the input (400). The same call will fail again."""


class MemkitClient:
    def __init__(self, base_url: str | None = None, timeout: float = 10.0):
        self.base_url = (base_url or os.environ.get("MEMKIT_URL", "http://localhost:7070")).rstrip("/")
        self.timeout = timeout

    def extract(self, content: str) -> list[AtomicMemory]:
        body = self._post("/v1/extract", {"content": content})
        return [AtomicMemory.from_dict(m) for m in body["memories"]]

    def consolidate(self, memories: Sequence[AtomicMemory]) -> SummaryMemory:
        body = self._post("/v1/consolidate", {"memories": [m.to_dict() for m in memories]})
        return SummaryMemory(**body["summary"])

    @staticmethod
    def embed(text: str) -> list[float]:
        """Computed locally; identical to the sidecar's /v1/embed and free of rate limits."""
        return core.embed(text)

    def _post(self, path: str, payload: dict) -> dict:
        request = urllib.request.Request(
            self.base_url + path,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                return json.loads(response.read())
        except urllib.error.HTTPError as exc:
            message = exc.read().decode("utf-8", "replace")
            if exc.code in (429, 503):
                raise RetryableError(message, exc.code) from None
            if exc.code == 400:
                raise InvalidRequestError(message) from None
            raise MemkitError(f"unexpected status {exc.code}: {message}") from None
        except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
            raise RetryableError(f"sidecar unreachable: {exc}") from None
