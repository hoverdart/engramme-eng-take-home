# API reference

Exact request and response shapes for the service you build and for the memkit sidecar. The brief explains the guarantees behind them.

All bodies are JSON. IDs you receive (`user_id`, `source_id`, `item_id`) are strings of letters, digits and `-`.

## The memory object

Every endpoint that returns memories uses this shape.

```json
{
  "id": "mem_7f3a91",
  "kind": "atomic",
  "topic": "project:atlas",
  "text": "Ana is leading the Atlas project.",
  "fact_key": "3f2a9c1e5b7d4e8a",
  "lineage": {
    "items": [
      {"source_id": "gmail", "item_id": "item-17"},
      {"source_id": "gcal", "item_id": "item-42"}
    ],
    "memories": []
  }
}
```

| Field | Meaning |
| --- | --- |
| `id` | Chosen by you. It must stay the same while the memory exists: an atomic memory keeps its ID as long as at least one live item states its fact. |
| `kind` | `atomic` or `summary`. |
| `topic` | From `extract()` or `consolidate()`. |
| `text` | Exactly the text `extract()` or `consolidate()` returned. |
| `fact_key` | Atomic memories only. From `extract()`. |
| `lineage.items` | Atomic: every live item that states the fact. Summary: every live item behind any of its atomic memories. Order does not matter. |
| `lineage.memories` | Summary only: the IDs of the atomic memories it merged. Empty or absent for atomic memories. |

A user has at most one atomic memory per `fact_key` and at most one summary per `topic`.

## Service endpoints

A 2xx response to a write means it is durably accepted: it must survive a `SIGKILL` that happens right after. Any 2xx status is fine. Writes are idempotent; the harness retries them freely.

### `POST /v1/events`

One event from a source.

```json
{
  "user_id": "u1",
  "source_id": "gmail",
  "item_id": "item-17",
  "version": 3,
  "kind": "upsert",
  "content": "Ana is leading the Atlas project. Thanks for the update.",
  "occurred_at": "2026-01-01T00:00:12Z"
}
```

- `version` is a positive integer that increases with every change to the item upstream.
- `kind` is `upsert` (the item exists with this content) or `delete` (the item was removed upstream). `content` is omitted for `delete`.
- An event whose `version` is less than or equal to the item's current version must be ignored. A `delete` counts as a version.

### `DELETE /v1/users/{user_id}/items/{source_id}/{item_id}`

The user asks the service to forget the item. Return 2xx even if the item is unknown or already forgotten.

### `POST /v1/users/{user_id}/sources/{source_id}/disconnect`
### `POST /v1/users/{user_id}/sources/{source_id}/connect`

Empty body. Both are idempotent. After `connect`, the harness replays the source's items as `POST /v1/events` with their existing versions.

### `GET /v1/users/{user_id}/recall?q=…&k=…`

Top `k` memories (default 10, at most 100) by cosine similarity between `embed(q)` and `embed(memory.text)`, highest first. Atomic and summary memories both count.

```json
{"memories": [{"id": "mem_7f3a91", "kind": "atomic", "...": "...", "score": 0.93}]}
```

### `GET /v1/users/{user_id}/memories?limit=…&cursor=…`

Every live memory for the user, in pages. `limit` defaults to 500. `next_cursor` is an opaque string, or `null` on the last page. Any order is fine.

```json
{"memories": [{"id": "mem_7f3a91", "...": "..."}], "next_cursor": "500"}
```

### `GET /v1/users/{user_id}/memories/{memory_id}`

One memory, or 404 if it does not exist or no longer exists.

### `GET /v1/health`

200 once the service is ready for traffic.

## Sidecar endpoints

The sidecar listens on port 7070 by default. Errors have the shape `{"error": "..."}`.

| Status | Meaning |
| --- | --- |
| 400 | The input is invalid. The same call will fail again. |
| 429 | More than 20 `extract` and `consolidate` calls are in flight at once. Retry later. |
| 503 | Transient failure (about 2% of calls). Retry. |

### `POST /v1/extract`

Rate-limited. Takes 50–200 ms.

```json
// request
{"content": "Ana is leading the Atlas project. Thanks for the update."}
// response
{"memories": [{"fact_key": "3f2a9c1e5b7d4e8a", "topic": "project:atlas", "text": "Ana is leading the Atlas project."}]}
```

Returns 0–5 memories. The same fact gives the same `fact_key` and `text` in any item.

### `POST /v1/consolidate`

Rate-limited. Takes 50–200 ms.

```json
// request: at least 3 memories exactly as extract() returned them, all with one topic
{"memories": [{"fact_key": "...", "topic": "project:atlas", "text": "..."}, "..."]}
// response
{"summary": {"topic": "project:atlas", "text": "Project Atlas: ..."}}
```

The result does not depend on the order of `memories`.

### `POST /v1/embed`

Not rate-limited, no added latency. The Python and TypeScript clients compute the same vector locally.

```json
// request
{"text": "Ana is leading the Atlas project."}
// response: 64 floats, unit length
{"vector": [0.0, 0.316, "..."]}
```

### `GET /v1/health`

200 when the sidecar is up.

### `/admin/*`

Used by the harness to reset settings and read call counts. Your service must not call these.

## `takehome.json`

Put it at the root of your repo.

```json
{"redelete_policy": "permanent"}
```

`redelete_policy` is `permanent` or `newer_version_restores`. See "Decisions left to you" in the brief.
