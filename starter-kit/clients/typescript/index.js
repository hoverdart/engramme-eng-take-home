// Thin client for the memkit sidecar. It never retries: deciding when and how to retry is
// part of the exercise. Types are in index.d.ts.
import { createHash } from "node:crypto";

export const EMBED_DIM = 64;

export class MemkitError extends Error {}

/** Rate limited (429), a transient failure (503), or the sidecar was unreachable. */
export class RetryableError extends MemkitError {
  constructor(message, status = undefined) {
    super(message);
    this.status = status;
  }
}

/** The sidecar rejected the input (400). The same call will fail again. */
export class InvalidRequestError extends MemkitError {}

export class MemkitClient {
  constructor(baseUrl = process.env.MEMKIT_URL ?? "http://localhost:7070", { timeoutMs = 10_000 } = {}) {
    this.baseUrl = baseUrl.replace(/\/+$/, "");
    this.timeoutMs = timeoutMs;
  }

  async extract(content) {
    const body = await this.#post("/v1/extract", { content });
    return body.memories;
  }

  async consolidate(memories) {
    const body = await this.#post("/v1/consolidate", { memories });
    return body.summary;
  }

  /** Computed locally; identical to the sidecar's /v1/embed and free of rate limits. */
  embed(text) {
    return embed(text);
  }

  async #post(path, payload) {
    let response;
    try {
      response = await fetch(this.baseUrl + path, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
        signal: AbortSignal.timeout(this.timeoutMs),
      });
    } catch (err) {
      throw new RetryableError(`sidecar unreachable: ${err.message}`);
    }
    const text = await response.text();
    if (response.ok) return JSON.parse(text);
    if (response.status === 429 || response.status === 503) throw new RetryableError(text, response.status);
    if (response.status === 400) throw new InvalidRequestError(text);
    throw new MemkitError(`unexpected status ${response.status}: ${text}`);
  }
}

/** Hashed bag-of-words embedding, L2-normalized. Matches memkit.core.embed in Python exactly. */
export function embed(text) {
  const vector = new Array(EMBED_DIM).fill(0);
  for (const token of text.toLowerCase().match(/[a-z0-9]+/g) ?? []) {
    const digest = createHash("sha256").update(token, "utf8").digest();
    const bucket = digest.readUInt32BE(0) % EMBED_DIM;
    vector[bucket] += digest[4] % 2 === 0 ? 1 : -1;
  }
  let sumSquares = 0;
  for (const v of vector) sumSquares += v * v;
  const norm = Math.sqrt(sumSquares);
  return norm === 0 ? vector : vector.map((v) => v / norm);
}

/** Cosine similarity of two embed() vectors, which are already unit length. */
export function cosine(a, b) {
  let sum = 0;
  for (let i = 0; i < a.length; i++) sum += a[i] * b[i];
  return sum;
}
