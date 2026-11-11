# Memory service take-home: starter kit

This kit supports the memory service exercise in the [take-home brief](../README.md). The statistics exercise is separate.

This kit holds everything the brief says we give you:

- **`memkit/`**: the sidecar that serves the fake `extract()` and `consolidate()` models, plus a Python client.
- **`clients/typescript/`**: a TypeScript client.
- **`harness` and `harness_lib/`**: the public test harness.
- **`API.md`**: exact request and response shapes for your service and the sidecar.
- **`examples/`**: a `docker-compose.yml` and a `takehome.json` to start from.

It needs Python 3.10 or newer. Nothing else is required; the kit has no third-party dependencies.

## Run the sidecar

With Docker, from this directory:

```bash
docker build -t memkit-sidecar . && docker run -p 7070:7070 memkit-sidecar
```

Without Docker:

```bash
python3 -m memkit.sidecar
```

Settings come from environment variables: `MEMKIT_PORT` (7070), `MEMKIT_LATENCY_MIN_MS` (50), `MEMKIT_LATENCY_MAX_MS` (200), `MEMKIT_FAILURE_RATE` (0.02), `MEMKIT_MAX_CONCURRENCY` (20) and `MEMKIT_SEED`. The harness resets them to these defaults before every scenario, so tune them only for your own experiments.

## Call it from your service

The clients never retry. When and how to retry is part of the exercise.

**Python:** `pip install -e path/to/starter-kit`, then:

```python
from memkit import MemkitClient, RetryableError, InvalidRequestError

client = MemkitClient("http://localhost:7070")  # or set MEMKIT_URL
facts = client.extract("Ana is leading the Atlas project. Thanks!")
summary = client.consolidate(facts_for_one_topic)  # needs at least 3
vector = client.embed(summary.text)                 # computed locally
```

**TypeScript (Node 18.17+):** `npm install path/to/starter-kit/clients/typescript`, then:

```ts
import { MemkitClient, RetryableError, InvalidRequestError } from "memkit-client";

const client = new MemkitClient("http://localhost:7070");
const facts = await client.extract("Ana is leading the Atlas project. Thanks!");
const vector = client.embed(facts[0].text);
```

**Any other language:** call the sidecar over HTTP as described in `API.md`. Use `POST /v1/embed` for vectors; it is not rate-limited.

## What the fake models understand

Item content is a few sentences. `extract()` recognizes sentences of these shapes and ignores everything else:

| Sentence | Topic |
| --- | --- |
| `Ana is leading the Atlas project.` | `project:atlas` |
| `Ana joined the Atlas project.` | `project:atlas` |
| `The Atlas project deadline is March 3.` | `project:atlas` |
| `The Atlas project budget is 120000 dollars.` | `project:atlas` |
| `Ana is flying to Lisbon on March 3.` | `trip:lisbon` |
| `The hotel in Lisbon is booked for March 3.` | `trip:lisbon` |
| `Ana recommended a restaurant in Lisbon.` | `trip:lisbon` |

`consolidate()` sorts its inputs' text and joins them under a label such as `Project Atlas:`. `embed()` is a hashed bag of words, so a query that repeats a memory's words scores it highest.

## Run the harness

Start the sidecar and your service, then:

```bash
./harness list
./harness run --scenario basic
./harness run --scenario all
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `--service-url` | `http://localhost:8080` | Your service |
| `--sidecar-url` | `http://localhost:7070` | The sidecar |
| `--config` | `takehome.json` | Where to read your `redelete_policy` |
| `--seed` | random | Replays the same workload; every run prints its seed |
| `--compose-dir` | `.` | Where your `docker-compose.yml` is, for the `crash` scenario |
| `--kill-cmd`, `--start-cmd` | none | Shell commands that SIGKILL and restart your service, for running `crash` without Docker |

Each scenario uses fresh user IDs, so you can run them repeatedly against the same service. The hidden scenarios we grade with are harder versions of these.

The `crash` scenario kills every container in your compose project except those labeled `takehome.persistent=true`, then runs `docker compose up -d`. Label your database and the sidecar, as in `examples/docker-compose.yml`.

## Tests

```bash
python3 -m unittest discover tests
node clients/typescript/test.mjs
```
