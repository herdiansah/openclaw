#!/usr/bin/env node

import fs from "node:fs/promises";
import path from "node:path";

const ROOT_DIR = path.resolve(path.dirname(new URL(import.meta.url).pathname), "..");
const rankingRulesPath = process.env.OPENCLAW_QDRANT_RANKING_RULES_FILE || path.join(ROOT_DIR, "qdrant-setup", "query-ranking-rules.json");

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
  limit: Number(process.env.OPENCLAW_QDRANT_QUERY_LIMIT || "10"),
  minScore: Number(process.env.OPENCLAW_QDRANT_MIN_SCORE || "0.5"),
  recencyBoost: Number(process.env.OPENCLAW_QDRANT_RECENCY_BOOST || "0.1"),
  keywordBoost: Number(process.env.OPENCLAW_QDRANT_KEYWORD_BOOST || "0.12"),
  tagBoost: Number(process.env.OPENCLAW_QDRANT_TAG_BOOST || "0.18"),
  filenameBoost: Number(process.env.OPENCLAW_QDRANT_FILENAME_BOOST || "0.1"),
  sectionBoost: Number(process.env.OPENCLAW_QDRANT_SECTION_BOOST || "0.08"),
  sourceCodeBoost: Number(process.env.OPENCLAW_QDRANT_SOURCE_CODE_BOOST || "0.18"),
  docsPenalty: Number(process.env.OPENCLAW_QDRANT_DOCS_PENALTY || "0.08"),
  pathSignalBoost: Number(process.env.OPENCLAW_QDRANT_PATH_SIGNAL_BOOST || "0.12"),
  exactPathBoost: Number(process.env.OPENCLAW_QDRANT_EXACT_PATH_BOOST || "0.22"),
  symbolBoost: Number(process.env.OPENCLAW_QDRANT_SYMBOL_BOOST || "0.14"),
  synonymBoost: Number(process.env.OPENCLAW_QDRANT_SYNONYM_BOOST || "0.16"),
  roleBoost: Number(process.env.OPENCLAW_QDRANT_ROLE_BOOST || "0.14"),
  explicitDateBoost: Number(process.env.OPENCLAW_QDRANT_EXPLICIT_DATE_BOOST || "0.28"),
  longTermMemoryPenaltyOnExplicitDate: Number(process.env.OPENCLAW_QDRANT_LONGTERM_DATE_PENALTY || "0.18"),
  todayMemoryBoost: Number(process.env.OPENCLAW_QDRANT_TODAY_MEMORY_BOOST || "0.24"),
  pgFileBoost: Number(process.env.OPENCLAW_QDRANT_PG_FILE_BOOST || "0.18"),
  envIntentBoost: Number(process.env.OPENCLAW_QDRANT_ENV_INTENT_BOOST || "0.16"),
};

const defaultRankingRules = {
  domainRules: [
    {
      name: "grading",
      trigger: ["grading", "grade", "score", "result", "results"],
      synonyms: ["results", "results-pg", "score", "scores", "grading", "grade", "exam_sessions", "total_score"],
      roles: ["results", "questions", "scores"],
    },
    {
      name: "auth",
      trigger: ["auth", "login", "token", "jwt", "session"],
      synonyms: ["auth", "login", "token", "jwt", "middleware", "verifytoken", "requirerole"],
      roles: ["auth", "middleware", "login"],
    },
    {
      name: "queue",
      trigger: ["queue", "worker", "redis", "job", "bullmq"],
      synonyms: ["queue", "worker", "redis", "bullmq", "job", "client", "prefix"],
      roles: ["worker", "queue", "client"],
    },
    {
      name: "exam-domain",
      trigger: ["exam", "question", "answer", "student", "class"],
      synonyms: ["exam", "exams", "question", "questions", "answer", "answers", "student", "students", "class", "classes"],
      roles: ["questions", "students", "classes", "exams"],
    },
    {
      name: "database",
      trigger: ["database", "sql", "postgres", "mysql", "query"],
      synonyms: ["database", "sql", "postgres", "mysql", "query", "pool", "transaction", "db"],
      roles: ["database", "config", "db"],
    },
  ],
};

let rankingRules = defaultRankingRules;

async function loadRankingRules() {
  try {
    const raw = await fs.readFile(rankingRulesPath, "utf8");
    const parsed = JSON.parse(raw);
    if (Array.isArray(parsed?.domainRules)) {
      rankingRules = parsed;
    }
  } catch {
    rankingRules = defaultRankingRules;
  }
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
  if (cfg.qdrantApiKey) {headers["api-key"] = cfg.qdrantApiKey;}
  return headers;
}

async function embed(input) {
  if (!cfg.embeddingApiKey) {
    throw new Error("OPENCLAW_QDRANT_EMBEDDING_API_KEY or OPENAI_API_KEY is required");
  }
  const data = await fetchJson(cfg.embeddingApiUrl, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${cfg.embeddingApiKey}`,
    },
    body: JSON.stringify({ model: cfg.embeddingModel, input }),
  });
  return data?.data?.[0]?.embedding;
}

function tokenize(text) {
  return String(text || "")
    .toLowerCase()
    .replace(/[^\p{L}\p{N}\s-]/gu, " ")
    .split(/\s+/)
    .map((s) => s.trim())
    .filter((s) => s.length >= 2);
}

function unique(values) {
  return [...new Set(values.filter(Boolean))];
}

function inferQueryTags(query) {
  const q = query.toLowerCase();
  const tags = [];
  const rules = [
    ["weather", ["cuaca", "weather", "bmkg", "hujan", "cerah", "forecast"]],
    ["news", ["berita", "news", "headline"]],
    ["finance", ["emas", "gold", "crypto", "bitcoin", "wallet", "harga"]],
    ["infra", ["server", "vps", "nginx", "pm2", "deploy", "qdrant"]],
    ["automation", ["cron", "heartbeat", "workflow", "automation"]],
    ["calendar", ["calendar", "meeting", "jadwal", "event"]],
    ["project", ["project", "phase", "bug", "fix", "release"]],
  ];
  for (const [tag, keywords] of rules) {
    if (keywords.some((keyword) => q.includes(keyword))) {
      tags.push(tag);
    }
  }
  return unique(tags);
}

function buildFilter(kind, project, dateFrom, dateTo, tags) {
  const must = [];
  if (kind && kind !== "all") {
    must.push({ key: "kind", match: { value: kind } });
  }
  if (project) {
    must.push({ key: "project_id", match: { value: project } });
  }
  if (dateFrom) {
    must.push({ key: "date", range: { gte: dateFrom } });
  }
  if (dateTo) {
    must.push({ key: "date", range: { lte: dateTo } });
  }
  if (tags?.length) {
    must.push({ should: tags.map((tag) => ({ key: "tags", match: { value: tag } })), min_should: 1 });
  }
  return must.length ? { must } : null;
}

async function search(vector, limit, filter) {
  const url = `${cfg.qdrantUrl}/collections/${cfg.collection}/points/search`;
  const body = { vector, limit, with_payload: true, with_vector: false };
  if (filter) {body.filter = filter;}
  return fetchJson(url, {
    method: "POST",
    headers: qdrantHeaders(),
    body: JSON.stringify(body),
  });
}

function applyRecencyBoost(results, boostFactor) {
  if (boostFactor <= 0) {return results;}

  const now = Date.now();
  const oneDayMs = 24 * 60 * 60 * 1000;

  return results.map((item) => {
    const updatedAt = item.updated_at ? new Date(item.updated_at).getTime() : 0;
    const daysSinceUpdate = (now - updatedAt) / oneDayMs;
    const recencyFactor = Math.max(0, 1 - daysSinceUpdate / 30);
    return {
      ...item,
      score: item.score + recencyFactor * boostFactor,
    };
  });
}

function overlapRatio(queryTokens, text) {
  if (!queryTokens.length) {return 0;}
  const haystack = new Set(tokenize(text));
  let hits = 0;
  for (const token of queryTokens) {
    if (haystack.has(token)) {hits += 1;}
  }
  return hits / queryTokens.length;
}

function inferQueryIntent(query, kind) {
  const q = String(query || "").toLowerCase();
  const technicalHints = [
    "sql", "query", "function", "class", "method", "endpoint", "api", "worker", "queue", "redis",
    "postgres", "bug", "fix", "implement", "source", "file", "code", "module", "import", "ts", "js",
  ];
  const docsHints = ["doc", "docs", "guide", "readme", "overview", "summary", "explain"];

  const technical = kind === "code" || technicalHints.some((hint) => q.includes(hint));
  const docsPreferred = docsHints.some((hint) => q.includes(hint));
  const wantsPg = /\b(pg|postgres|postgresql)\b/.test(q);
  const wantsEnv = /\benv\b|environment|config/.test(q);
  const wantsToday = /\btoday\b|hari ini|sekarang/.test(q);
  return { technical, docsPreferred, wantsPg, wantsEnv, wantsToday };
}

function extractExplicitDateHints(query) {
  const q = String(query || "").toLowerCase();
  const hints = [];

  const isoMatches = q.match(/\b\d{4}-\d{2}-\d{2}\b/g) || [];
  hints.push(...isoMatches);

  const dayMonthYearMatches = q.match(/\b(\d{1,2})\s+(januari|februari|maret|april|mei|juni|juli|agustus|september|oktober|november|desember|january|february|march|april|may|june|july|august|september|october|november|december)(?:\s+(\d{4}))?\b/g) || [];
  hints.push(...dayMonthYearMatches);

  const todayHints = q.match(/\btoday\b|hari ini|sekarang/g) || [];
  hints.push(...todayHints);

  return unique(hints.map((s) => s.trim()));
}

function buildSynonymContext(query) {
  const queryTokens = unique(tokenize(query));
  const q = ` ${String(query || "").toLowerCase()} `;

  const synonyms = [];
  const roles = [];
  for (const rule of rankingRules.domainRules || []) {
    if (rule.trigger?.some((token) => queryTokens.includes(token) || q.includes(` ${String(token).toLowerCase()} `))) {
      synonyms.push(...(rule.synonyms || []));
      roles.push(...(rule.roles || []));
    }
  }

  return {
    queryTokens,
    explicitDateHints: extractExplicitDateHints(query),
    synonyms: unique(synonyms.map((s) => s.toLowerCase())),
    roles: unique(roles.map((s) => s.toLowerCase())),
  };
}

function extractQuerySymbols(query) {
  const rawTokens = String(query || "")
    .split(/\s+/)
    .map((s) => s.trim())
    .filter(Boolean);

  const symbols = [];
  for (const token of rawTokens) {
    if (/[./_-]/.test(token) || /^[a-z]+(?:[A-Z][a-z0-9]+)+$/.test(token) || /\.\w+$/.test(token)) {
      symbols.push(token.toLowerCase());
    }
  }

  return unique(symbols);
}

function computeExactPathRatio(queryTokens, item) {
  const rel = String(item.rel_path || item.source || "").toLowerCase();
  if (!queryTokens.length || !rel) {return 0;}
  let hits = 0;
  for (const token of queryTokens) {
    if (rel.includes(token)) {hits += 1;}
  }
  return hits / queryTokens.length;
}

function computeSymbolRatio(querySymbols, item) {
  if (!querySymbols.length) {return 0;}
  const haystack = `${item.source}\n${item.rel_path || ""}\n${item.text.slice(0, 1200)}`.toLowerCase();
  let hits = 0;
  for (const symbol of querySymbols) {
    if (haystack.includes(symbol)) {hits += 1;}
  }
  return hits / querySymbols.length;
}

function computePathSignal(item) {
  const rel = String(item.rel_path || item.source || "").toLowerCase();
  let signal = 0;
  if (rel.includes("/src/") || rel.startsWith("src/")) {signal += 1;}
  if (rel.includes("/api/") || rel.startsWith("api/")) {signal += 0.8;}
  if (rel.includes("/apps/")) {signal += 0.6;}
  if (rel.includes("/worker/")) {signal += 0.7;}
  if (rel.includes("/infra/")) {signal += 0.4;}
  if (rel.includes("readme") || rel.includes("/docs/") || rel.endsWith(".md")) {signal -= 0.7;}
  return signal;
}

function computeSynonymRatio(synonyms, item) {
  if (!synonyms.length) {return 0;}
  const haystack = `${item.source}\n${item.rel_path || ""}\n${item.text.slice(0, 1600)}`.toLowerCase();
  let hits = 0;
  for (const synonym of synonyms) {
    if (haystack.includes(synonym)) {hits += 1;}
  }
  return hits / synonyms.length;
}

function computeRoleRatio(roles, item) {
  if (!roles.length) {return 0;}
  const haystack = `${item.source}\n${item.rel_path || ""}`.toLowerCase();
  let hits = 0;
  for (const role of roles) {
    if (haystack.includes(role)) {hits += 1;}
  }
  return hits / roles.length;
}

function applyKeywordAndTagBoost(results, query, queryTags, intent) {
  const synonymContext = buildSynonymContext(query);
  const queryTokens = synonymContext.queryTokens;
  const querySymbols = extractQuerySymbols(query);
  return results.map((item) => {
    const itemTags = Array.isArray(item.tags) ? item.tags : [];
    const keywordRatio = overlapRatio(queryTokens, item.text);
    const sourceRatio = overlapRatio(queryTokens, item.source);
    const sectionRatio = overlapRatio(queryTokens, item.section_header || "");
    const exactPathRatio = computeExactPathRatio(queryTokens, item);
    const symbolRatio = computeSymbolRatio(querySymbols, item);
    const synonymRatio = computeSynonymRatio(synonymContext.synonyms, item);
    const roleRatio = computeRoleRatio(synonymContext.roles, item);
    const explicitDateRatio = computeSymbolRatio(synonymContext.explicitDateHints, {
      source: item.source,
      rel_path: item.rel_path,
      text: `${item.date || ""}\n${item.text}`,
    });
    const tagHits = queryTags.filter((tag) => itemTags.includes(tag)).length;
    const tagRatio = queryTags.length ? tagHits / queryTags.length : 0;
    const pathSignal = computePathSignal(item);

    let boosted =
      item.score +
      keywordRatio * cfg.keywordBoost +
      sourceRatio * cfg.filenameBoost +
      sectionRatio * cfg.sectionBoost +
      exactPathRatio * cfg.exactPathBoost +
      symbolRatio * cfg.symbolBoost +
      synonymRatio * cfg.synonymBoost +
      roleRatio * cfg.roleBoost +
      explicitDateRatio * cfg.explicitDateBoost +
      tagRatio * cfg.tagBoost +
      Math.max(pathSignal, 0) * cfg.pathSignalBoost;

    if (synonymContext.explicitDateHints.length && item.kind === "memory") {
      if (item.source === "MEMORY.md") {
        boosted -= cfg.longTermMemoryPenaltyOnExplicitDate;
      }
      if (intent.wantsToday && item.date === new Date().toISOString().slice(0, 10)) {
        boosted += cfg.todayMemoryBoost;
      }
    }

    if (intent.technical && item.kind === "code") {
      const rel = `${item.rel_path || ""}`.toLowerCase();
      if (intent.wantsPg && (rel.includes("-pg.") || rel.includes("postgres") || rel.includes("database-pg"))) {
        boosted += cfg.pgFileBoost;
      }
      if (intent.wantsEnv && (rel.endsWith("env.ts") || rel.endsWith("env.js") || rel.includes("/env."))) {
        boosted += cfg.envIntentBoost;
      }
      if (intent.wantsEnv && (rel.includes("/db/pool") || rel.endsWith("pool.ts") || rel.endsWith("pool.js"))) {
        boosted -= cfg.envIntentBoost * 0.35;
      }
      if (!item.tags?.includes("docs")) {
        boosted += cfg.sourceCodeBoost;
      }
      if (item.tags?.includes("docs") && !intent.docsPreferred) {
        boosted -= cfg.docsPenalty;
      }
    }

    return {
      ...item,
      score: Math.max(0, boosted),
      keyword_ratio: keywordRatio,
      source_ratio: sourceRatio,
      section_ratio: sectionRatio,
      exact_path_ratio: exactPathRatio,
      symbol_ratio: symbolRatio,
      synonym_ratio: synonymRatio,
      role_ratio: roleRatio,
      explicit_date_ratio: explicitDateRatio,
      tag_ratio: tagRatio,
      path_signal: pathSignal,
    };
  });
}

function compareResults(a, b) {
  return (
    (b.score - a.score) ||
    ((b.exact_path_ratio || 0) - (a.exact_path_ratio || 0)) ||
    ((b.symbol_ratio || 0) - (a.symbol_ratio || 0)) ||
    ((b.source_ratio || 0) - (a.source_ratio || 0)) ||
    ((b.path_signal || 0) - (a.path_signal || 0)) ||
    ((b.keyword_ratio || 0) - (a.keyword_ratio || 0)) ||
    String(a.source || "").localeCompare(String(b.source || ""))
  );
}

function trim(text, n = 220) {
  if (!text) {return "";}
  return text.length > n ? `${text.slice(0, n)}...` : text;
}

async function main() {
  const args = process.argv.slice(2);
  let json = false;
  let limit = cfg.limit;
  let kind = "all";
  let project = "";
  let dateFrom = null;
  let dateTo = null;
  let explicitTags = [];
  const queryParts = [];

  for (let i = 0; i < args.length; i += 1) {
    const a = args[i];
    if (a === "--json") {
      json = true;
      continue;
    }
    if (a === "--limit" && i + 1 < args.length) {
      limit = Number(args[i + 1]) || cfg.limit;
      i += 1;
      continue;
    }
    if (a.startsWith("--limit=")) {
      limit = Number(a.split("=")[1]) || cfg.limit;
      continue;
    }
    if (a === "--kind" && i + 1 < args.length) {
      kind = args[i + 1];
      i += 1;
      continue;
    }
    if (a.startsWith("--kind=")) {
      kind = a.split("=")[1];
      continue;
    }
    if (a === "--project" && i + 1 < args.length) {
      project = args[i + 1];
      i += 1;
      continue;
    }
    if (a.startsWith("--project=")) {
      project = a.split("=")[1];
      continue;
    }
    if (a === "--date-from" && i + 1 < args.length) {
      dateFrom = args[i + 1];
      i += 1;
      continue;
    }
    if (a.startsWith("--date-from=")) {
      dateFrom = a.split("=")[1];
      continue;
    }
    if (a === "--date-to" && i + 1 < args.length) {
      dateTo = args[i + 1];
      i += 1;
      continue;
    }
    if (a.startsWith("--date-to=")) {
      dateTo = a.split("=")[1];
      continue;
    }
    if (a === "--tags" && i + 1 < args.length) {
      explicitTags = args[i + 1].split(",").map((s) => s.trim()).filter(Boolean);
      i += 1;
      continue;
    }
    if (a.startsWith("--tags=")) {
      explicitTags = a.split("=")[1].split(",").map((s) => s.trim()).filter(Boolean);
      continue;
    }
    queryParts.push(a);
  }

  const query = queryParts.join(" ").trim();
  if (!query) {
    process.stderr.write(
      "Usage: scripts/qdrant-memory-query.mjs [--json] [--limit N] [--kind all|memory|code] [--project id] [--date-from YYYY-MM-DD] [--date-to YYYY-MM-DD] [--tags a,b] <query>\n",
    );
    process.exit(1);
  }

  if (!cfg.enabled) {
    process.stderr.write("Qdrant memory sidecar disabled (OPENCLAW_QDRANT_MEMORY_ENABLED!=true).\n");
    process.exit(1);
  }

  await loadRankingRules();

  const queryTags = unique([...inferQueryTags(query), ...explicitTags]);
  const intent = inferQueryIntent(query, kind);
  const vector = await embed(query);
  if (!vector) {throw new Error("Failed to compute embedding vector");}

  const filter = buildFilter(kind, project, dateFrom, dateTo, explicitTags);
  const out = await search(vector, Math.max(limit * 3, 20), filter);
  let results = (out?.result || []).map((item) => ({
    score: Number(item?.score || 0),
    source: item?.payload?.source || "<unknown>",
    text: item?.payload?.text || "",
    chunk_index: item?.payload?.chunk_index ?? null,
    kind: item?.payload?.kind || "unknown",
    project_id: item?.payload?.project_id || null,
    rel_path: item?.payload?.rel_path || null,
    date: item?.payload?.date || null,
    updated_at: item?.payload?.updated_at || null,
    tags: item?.payload?.tags || [],
    section_header: item?.payload?.section_header || null,
    word_count: item?.payload?.word_count || null,
  }));

  results = applyRecencyBoost(results, cfg.recencyBoost);
  results = applyKeywordAndTagBoost(results, query, queryTags, intent);
  results = results.filter((r) => r.score >= cfg.minScore);
  results.sort(compareResults);
  results = results.slice(0, limit);

  if (json) {
    process.stdout.write(`${JSON.stringify({ query, count: results.length, kind, project, dateFrom, dateTo, queryTags, intent, rankingRulesPath, results }, null, 2)}\n`);
    return;
  }

  if (!results.length) {
    process.stdout.write("No results.\n");
    return;
  }

  for (const [i, r] of results.entries()) {
    const suffix = r.project_id ? ` project=${r.project_id}` : "";
    const dateStr = r.date ? ` date=${r.date}` : "";
    const tagStr = r.tags?.length ? ` tags=${r.tags.join(",")}` : "";
    process.stdout.write(`${i + 1}. score=${r.score.toFixed(4)} kind=${r.kind}${suffix}${dateStr}${tagStr} source=${r.source}\n`);
    process.stdout.write(`   ${trim(r.text)}\n`);
  }
}

main().catch((err) => {
  process.stderr.write(`ERROR: ${err.message}\n`);
  process.exit(1);
});
