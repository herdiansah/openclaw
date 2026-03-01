#!/usr/bin/env node

import fs from "node:fs/promises";
import path from "node:path";
import crypto from "node:crypto";

const ROOT_DIR = path.resolve(path.dirname(new URL(import.meta.url).pathname), "..");
const MEMORY_DIR = path.join(ROOT_DIR, "memory");
const STATE_FILE = path.join(MEMORY_DIR, "qdrant-index-state.json");

const cfg = {
  enabled: String(process.env.OPENCLAW_QDRANT_MEMORY_ENABLED || "false").toLowerCase() === "true",
  qdrantUrl: (process.env.OPENCLAW_QDRANT_URL || "http://127.0.0.1:6333").replace(/\/$/, ""),
  qdrantApiKey: process.env.OPENCLAW_QDRANT_API_KEY || "",
  collection: process.env.OPENCLAW_QDRANT_COLLECTION || "openclaw_memory",
  embeddingApiUrl:
    process.env.OPENCLAW_QDRANT_EMBEDDING_API_URL || "https://api.openai.com/v1/embeddings",
  embeddingApiKey:
    process.env.OPENCLAW_QDRANT_EMBEDDING_API_KEY || process.env.OPENAI_API_KEY || "",
  embeddingModel: process.env.OPENCLAW_QDRANT_EMBEDDING_MODEL || "text-embedding-3-small",
  embeddingDim: Number(process.env.OPENCLAW_QDRANT_EMBEDDING_DIM || "1536"),
  batchSize: Number(process.env.OPENCLAW_QDRANT_BATCH_SIZE || "32"),
  chunkChars: Number(process.env.OPENCLAW_QDRANT_CHUNK_CHARS || "1200"),
};

function log(msg) {
  process.stdout.write(`${msg}\n`);
}

function sha256(text) {
  return crypto.createHash("sha256").update(text).digest("hex");
}

function hexToUuid(hex) {
  const h = hex.slice(0, 32);
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20, 32)}`;
}

function chunkText(text, maxChars) {
  const paragraphs = text
    .split(/\n\s*\n/g)
    .map((p) => p.trim())
    .filter(Boolean);

  const chunks = [];
  let current = "";

  for (const p of paragraphs) {
    if (!current) {
      if (p.length <= maxChars) {
        current = p;
      } else {
        for (let i = 0; i < p.length; i += maxChars) chunks.push(p.slice(i, i + maxChars));
      }
      continue;
    }

    const candidate = `${current}\n\n${p}`;
    if (candidate.length <= maxChars) {
      current = candidate;
      continue;
    }

    chunks.push(current);
    if (p.length <= maxChars) {
      current = p;
    } else {
      current = "";
      for (let i = 0; i < p.length; i += maxChars) {
        const part = p.slice(i, i + maxChars);
        if (part.length === maxChars) chunks.push(part);
        else current = part;
      }
    }
  }

  if (current) chunks.push(current);
  return chunks;
}

async function fetchJson(url, options) {
  const res = await fetch(url, options);
  const text = await res.text();
  let data;
  try {
    data = text ? JSON.parse(text) : {};
  } catch {
    data = { raw: text };
  }
  if (!res.ok) {
    throw new Error(`HTTP ${res.status} ${res.statusText} @ ${url} :: ${JSON.stringify(data)}`);
  }
  return data;
}

function qdrantHeaders() {
  const headers = { "Content-Type": "application/json" };
  if (cfg.qdrantApiKey) headers["api-key"] = cfg.qdrantApiKey;
  return headers;
}

function embeddingHeaders() {
  if (!cfg.embeddingApiKey) {
    throw new Error("OPENCLAW_QDRANT_EMBEDDING_API_KEY or OPENAI_API_KEY is required");
  }
  return {
    "Content-Type": "application/json",
    Authorization: `Bearer ${cfg.embeddingApiKey}`,
  };
}

async function ensureCollection() {
  const url = `${cfg.qdrantUrl}/collections/${cfg.collection}`;
  try {
    await fetchJson(url, {
      method: "PUT",
      headers: qdrantHeaders(),
      body: JSON.stringify({ vectors: { size: cfg.embeddingDim, distance: "Cosine" } }),
    });
  } catch (err) {
    const msg = String(err?.message || err);
    if (msg.includes("HTTP 409")) return;
    throw err;
  }
}

async function deleteBySource(source) {
  const url = `${cfg.qdrantUrl}/collections/${cfg.collection}/points/delete?wait=true`;
  await fetchJson(url, {
    method: "POST",
    headers: qdrantHeaders(),
    body: JSON.stringify({
      filter: { must: [{ key: "source", match: { value: source } }] },
    }),
  });
}

async function embed(text) {
  const data = await fetchJson(cfg.embeddingApiUrl, {
    method: "POST",
    headers: embeddingHeaders(),
    body: JSON.stringify({ model: cfg.embeddingModel, input: text }),
  });
  if (!data?.data?.[0]?.embedding) {
    throw new Error(`Invalid embedding response: ${JSON.stringify(data)}`);
  }
  return data.data[0].embedding;
}

async function upsertPoints(points) {
  if (!points.length) return;
  const url = `${cfg.qdrantUrl}/collections/${cfg.collection}/points?wait=true`;
  await fetchJson(url, {
    method: "PUT",
    headers: qdrantHeaders(),
    body: JSON.stringify({ points }),
  });
}

async function collectMemoryFiles() {
  const files = [];
  for (const rel of ["MEMORY.md", path.join("memory", `${new Date().toISOString().slice(0, 10)}.md`)]) {
    const abs = path.join(ROOT_DIR, rel);
    try {
      await fs.access(abs);
      files.push(abs);
    } catch {
      // skip
    }
  }

  try {
    const entries = await fs.readdir(MEMORY_DIR, { withFileTypes: true });
    for (const e of entries.filter((x) => x.isFile() && x.name.endsWith(".md")).sort((a, b) => a.name.localeCompare(b.name))) {
      const p = path.join(MEMORY_DIR, e.name);
      if (!files.includes(p)) files.push(p);
    }
  } catch {
    // skip
  }

  return files;
}

async function writeState(summary) {
  const nowTs = Math.floor(Date.now() / 1000);
  const nowIso = new Date().toISOString();
  await fs.mkdir(MEMORY_DIR, { recursive: true });
  await fs.writeFile(
    STATE_FILE,
    `${JSON.stringify({ last_index_ts: nowTs, last_index_iso: nowIso, ...summary }, null, 2)}\n`,
    "utf8",
  );
}

async function main() {
  if (!cfg.enabled) {
    log("Qdrant memory sidecar disabled (OPENCLAW_QDRANT_MEMORY_ENABLED!=true). No action.");
    return;
  }

  log(`Ensuring Qdrant collection '${cfg.collection}'...`);
  await ensureCollection();

  const files = await collectMemoryFiles();
  if (!files.length) {
    await writeState({ files_indexed: 0, chunks_indexed: 0 });
    log("No memory files found.");
    return;
  }

  let totalChunks = 0;
  log(`Indexing ${files.length} file(s)...`);

  for (const file of files) {
    const source = path.relative(ROOT_DIR, file);
    log(`- Refresh source: ${source}`);
    await deleteBySource(source);

    const raw = await fs.readFile(file, "utf8");
    const chunks = chunkText(raw, cfg.chunkChars);
    totalChunks += chunks.length;

    const points = [];
    for (let i = 0; i < chunks.length; i += 1) {
      const text = chunks[i];
      const vector = await embed(text);
      const id = hexToUuid(sha256(`${source}\n${i}\n${text}`));
      points.push({
        id,
        vector,
        payload: {
          source,
          chunk_index: i,
          text,
          updated_at: new Date().toISOString(),
        },
      });

      if (points.length >= cfg.batchSize) {
        await upsertPoints(points.splice(0, points.length));
      }
    }

    if (points.length) await upsertPoints(points);
  }

  await writeState({ files_indexed: files.length, chunks_indexed: totalChunks });
  log(`Qdrant indexing complete. files=${files.length}, chunks=${totalChunks}`);
}

main().catch((err) => {
  process.stderr.write(`ERROR: ${err.message}\n`);
  process.exit(1);
});
