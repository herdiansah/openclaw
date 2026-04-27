/**
 * qdrant-auto-context plugin
 *
 * Automatically injects vector memory context into every agent prompt via the
 * before_prompt_build hook. Also triggers background re-indexing at session
 * start via qdrant-memory-run-from-env.sh.
 *
 * Controlled entirely by qdrant-setup/qdrant-memory.env:
 *   OPENCLAW_QDRANT_MEMORY_ENABLED=true    — master on/off switch
 *   OPENCLAW_QDRANT_MIN_SCORE=0.55         — minimum cosine similarity (0–1)
 *   OPENCLAW_QDRANT_QUERY_LIMIT=5          — max results to inject
 *   OPENCLAW_QDRANT_AUTO_CONTEXT_MIN_LEN=15 — skip prompts shorter than this
 *
 * Never throws; all errors are swallowed so a Qdrant outage never breaks agents.
 */
import { readFileSync, existsSync } from "node:fs";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const __dirname = dirname(fileURLToPath(import.meta.url));
// Plugin lives at extensions/qdrant-auto-context/ → root is two levels up
const ROOT_DIR = join(__dirname, "..", "..");
const ENV_FILE = join(ROOT_DIR, "qdrant-setup", "qdrant-memory.env");
const RUN_SCRIPT = join(ROOT_DIR, "scripts", "qdrant-memory-run-from-env.sh");

/**
 * Parse a flat KEY=VALUE env file.  Comments (#) and blank lines are skipped.
 * Values are NOT shell-unquoted; simple bare values and quoted strings work.
 */
function loadEnvFile(filePath) {
  const out = {};
  if (!existsSync(filePath)) return out;
  try {
    const lines = readFileSync(filePath, "utf8").split("\n");
    for (const raw of lines) {
      const line = raw.trim();
      if (!line || line.startsWith("#")) continue;
      const eq = line.indexOf("=");
      if (eq < 1) continue;
      const key = line.slice(0, eq).trim();
      let val = line.slice(eq + 1).trim();
      // Strip surrounding single or double quotes if present
      if (
        (val.startsWith('"') && val.endsWith('"')) ||
        (val.startsWith("'") && val.endsWith("'"))
      ) {
        val = val.slice(1, -1);
      }
      out[key] = val;
    }
  } catch {
    // Unreadable env file — fall through with empty config
  }
  return out;
}

function pick(env, key, fallback = "") {
  return env[key] ?? process.env[key] ?? fallback;
}

function pickNum(env, key, fallback) {
  const raw = env[key] ?? process.env[key];
  const n = Number(raw);
  return Number.isFinite(n) && n > 0 ? n : fallback;
}

function pickBool(env, key, fallback = false) {
  const raw = env[key] ?? process.env[key];
  if (raw === undefined || raw === null) return fallback;
  return String(raw).toLowerCase() === "true";
}

async function fetchJson(url, options) {
  const res = await fetch(url, options);
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`HTTP ${res.status} @ ${url}: ${body.slice(0, 200)}`);
  }
  return res.json();
}

export default {
  id: "qdrant-auto-context",

  register(api) {
    const env = loadEnvFile(ENV_FILE);

    const cfg = {
      enabled: pickBool(env, "OPENCLAW_QDRANT_MEMORY_ENABLED"),
      qdrantUrl: pick(env, "OPENCLAW_QDRANT_URL", "http://127.0.0.1:6333").replace(/\/$/, ""),
      qdrantApiKey: pick(env, "OPENCLAW_QDRANT_API_KEY"),
      collection: pick(env, "OPENCLAW_QDRANT_COLLECTION", "openclaw_memory"),
      embeddingApiUrl: pick(
        env,
        "OPENCLAW_QDRANT_EMBEDDING_API_URL",
        "https://api.openai.com/v1/embeddings",
      ),
      embeddingApiKey: pick(env, "OPENCLAW_QDRANT_EMBEDDING_API_KEY"),
      embeddingModel: pick(env, "OPENCLAW_QDRANT_EMBEDDING_MODEL", "text-embedding-3-small"),
      limit: pickNum(env, "OPENCLAW_QDRANT_QUERY_LIMIT", 5),
      minScore: pickNum(env, "OPENCLAW_QDRANT_MIN_SCORE", 0.55),
      minPromptLen: pickNum(env, "OPENCLAW_QDRANT_AUTO_CONTEXT_MIN_LEN", 15),
    };

    if (!cfg.enabled) {
      api.logger.info("[qdrant-auto-context] disabled (OPENCLAW_QDRANT_MEMORY_ENABLED!=true)");
      return;
    }

    if (!cfg.embeddingApiKey) {
      api.logger.warn("[qdrant-auto-context] no embedding API key set — auto-context disabled");
      return;
    }

    /**
     * True when the configured URL is the Gemini native embedContent endpoint
     * (googleapis.com …:embedContent).  In that case the request/response shape
     * and auth header differ from the OpenAI-compatible standard.
     */
    function isGeminiNative(url) {
      const u = url.toLowerCase();
      return u.includes("generativelanguage.googleapis.com") && u.includes(":embedcontent");
    }

    function normalizeGeminiModel(model) {
      const m = String(model || "").trim();
      if (!m) return "models/gemini-embedding-001";
      return m.startsWith("models/") ? m : `models/${m}`;
    }

    /** Compute embedding vector for a text string.
     *  Supports both Gemini native embedContent and OpenAI-compatible APIs. */
    async function embed(text) {
      const gemini = isGeminiNative(cfg.embeddingApiUrl);
      const headers = { "Content-Type": "application/json" };
      let body;

      if (gemini) {
        headers["x-goog-api-key"] = cfg.embeddingApiKey;
        body = JSON.stringify({
          model: normalizeGeminiModel(cfg.embeddingModel),
          content: { parts: [{ text }] },
        });
      } else {
        headers["Authorization"] = `Bearer ${cfg.embeddingApiKey}`;
        body = JSON.stringify({ model: cfg.embeddingModel, input: text });
      }

      const data = await fetchJson(cfg.embeddingApiUrl, { method: "POST", headers, body });

      const vec = gemini ? data?.embedding?.values : data?.data?.[0]?.embedding;
      if (!Array.isArray(vec) || vec.length === 0) {
        throw new Error(`Embedding API returned empty vector (gemini=${gemini})`);
      }
      return vec;
    }

    /** Search Qdrant collection, return results above minScore */
    async function searchQdrant(vector) {
      const url = `${cfg.qdrantUrl}/collections/${cfg.collection}/points/search`;
      const headers = { "Content-Type": "application/json" };
      if (cfg.qdrantApiKey) headers["api-key"] = cfg.qdrantApiKey;

      const data = await fetchJson(url, {
        method: "POST",
        headers,
        body: JSON.stringify({
          vector,
          limit: cfg.limit,
          with_payload: true,
          with_vector: false,
        }),
      });

      return (data?.result ?? [])
        .filter((r) => (r?.score ?? 0) >= cfg.minScore)
        .map((r) => ({
          score: Number(r.score ?? 0),
          source: String(r?.payload?.source ?? "<unknown>"),
          text: String(r?.payload?.text ?? ""),
          kind: String(r?.payload?.kind ?? "memory"),
        }));
    }

    /** Format results into a concise context block for prepending */
    function formatContext(results) {
      const parts = ["### Memory Context (auto-retrieved from Qdrant)"];
      for (const [i, r] of results.entries()) {
        const text = r.text.replace(/\s+/g, " ").trim();
        const snippet = text.length > 320 ? `${text.slice(0, 320)}…` : text;
        parts.push(`${i + 1}. [score=${r.score.toFixed(3)} source=${r.source}]`);
        parts.push(`   ${snippet}`);
      }
      return parts.join("\n");
    }

    // -------------------------------------------------------------------------
    // Hook: before_prompt_build
    // Runs before every agent LLM call.  Embeds the user prompt, searches
    // Qdrant, and prepends relevant chunks as context.
    // -------------------------------------------------------------------------
    api.on(
      "before_prompt_build",
      async ({ prompt }) => {
        if (!prompt || prompt.length < cfg.minPromptLen) return;
        try {
          const vector = await embed(prompt);
          const results = await searchQdrant(vector);
          if (!results.length) return;
          return { prependContext: formatContext(results) };
        } catch (err) {
          // Never surface Qdrant errors to the agent — log quietly and move on
          api.logger.debug?.(`[qdrant-auto-context] query failed: ${err?.message}`);
        }
      },
      { priority: 50 },
    );

    // -------------------------------------------------------------------------
    // Hook: session_start
    // Triggers a background re-index (respects the interval check in the
    // run-from-env script — no-ops if index is still fresh).
    // -------------------------------------------------------------------------
    api.on("session_start", async () => {
      try {
        if (!existsSync(RUN_SCRIPT)) return;
        const child = spawn("/bin/bash", [RUN_SCRIPT], {
          detached: true,
          stdio: "ignore",
          // Pass the loaded env vars so the script picks up config without
          // needing to re-source the file itself
          env: { ...process.env, ...env },
        });
        child.unref();
      } catch {
        // Background indexing failure must never surface to the user
      }
    });

    api.logger.info(
      `[qdrant-auto-context] active — collection=${cfg.collection} limit=${cfg.limit} minScore=${cfg.minScore}`,
    );
  },
};
