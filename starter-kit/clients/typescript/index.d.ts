export declare const EMBED_DIM: 64;

export interface AtomicMemory {
  fact_key: string;
  topic: string;
  text: string;
}

export interface SummaryMemory {
  topic: string;
  text: string;
}

export declare class MemkitError extends Error {}

/** Rate limited (429), a transient failure (503), or the sidecar was unreachable. */
export declare class RetryableError extends MemkitError {
  readonly status: 429 | 503 | undefined;
}

/** The sidecar rejected the input (400). The same call will fail again. */
export declare class InvalidRequestError extends MemkitError {}

export declare class MemkitClient {
  /** Defaults to $MEMKIT_URL, then http://localhost:7070. */
  constructor(baseUrl?: string, options?: { timeoutMs?: number });
  extract(content: string): Promise<AtomicMemory[]>;
  consolidate(memories: AtomicMemory[]): Promise<SummaryMemory>;
  /** Computed locally; identical to the sidecar's /v1/embed and free of rate limits. */
  embed(text: string): number[];
}

export declare function embed(text: string): number[];
export declare function cosine(a: number[], b: number[]): number;
