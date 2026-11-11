// Checks that embed() matches the Python implementation, and, when a sidecar is running
// (MEMKIT_URL, default http://localhost:7070), that the client can call it. Run: node test.mjs
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { InvalidRequestError, MemkitClient, embed } from "./index.js";

const fixture = JSON.parse(readFileSync(new URL("../../testdata/embed_vectors.json", import.meta.url)));
for (const { text, vector } of fixture) {
  assert.deepEqual(embed(text), vector, `embed mismatch for ${JSON.stringify(text)}`);
}
console.log(`embed matches Python for ${fixture.length} texts`);

const client = new MemkitClient();
let facts;
try {
  facts = await client.extract("Ana joined the Beacon project. Ben joined the Beacon project. Gus joined the Beacon project.");
} catch (err) {
  console.log(`skipping sidecar checks: ${err.message}`);
  process.exit(0);
}
assert.equal(facts.length, 3);
const summary = await client.consolidate(facts);
assert.equal(summary.topic, "project:beacon");
await assert.rejects(client.consolidate(facts.slice(0, 1)), InvalidRequestError);
console.log("sidecar round trip ok");
