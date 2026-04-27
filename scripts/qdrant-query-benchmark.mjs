#!/usr/bin/env node

import fs from "node:fs/promises";
import path from "node:path";
import { spawn } from "node:child_process";

const ROOT_DIR = path.resolve(path.dirname(new URL(import.meta.url).pathname), "..");
const benchmarkPath = process.argv[2] || path.join(ROOT_DIR, "qdrant-setup", "query-benchmark.json");
const queryScript = path.join(ROOT_DIR, "scripts", "qdrant-memory-query.mjs");

function runNode(args, env = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn("node", args, {
      cwd: ROOT_DIR,
      env: { ...process.env, ...env },
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (d) => { stdout += String(d); });
    child.stderr.on("data", (d) => { stderr += String(d); });
    child.on("error", reject);
    child.on("close", (code) => {
      if (code !== 0) {
        reject(new Error(`Command failed (${code}): ${stderr || stdout}`));
        return;
      }
      resolve(stdout);
    });
  });
}

function normalizeExpected(expectedSources) {
  return new Set((expectedSources || []).map((s) => String(s).trim()).filter(Boolean));
}

function resultRanks(results, expected) {
  const sources = results.map((r) => r.source);
  const matches = sources
    .map((source, idx) => ({ source, rank: idx + 1 }))
    .filter((x) => expected.has(x.source));
  return {
    top1: matches.some((m) => m.rank === 1),
    top3: matches.some((m) => m.rank <= 3),
    top5: matches.some((m) => m.rank <= 5),
    bestRank: matches.length ? Math.min(...matches.map((m) => m.rank)) : null,
  };
}

async function main() {
  const raw = await fs.readFile(benchmarkPath, "utf8");
  const benchmark = JSON.parse(raw);
  const cases = Array.isArray(benchmark?.cases) ? benchmark.cases : [];
  if (!cases.length) {
    throw new Error(`No benchmark cases found in ${benchmarkPath}`);
  }

  const caseResults = [];

  for (const testCase of cases) {
    const args = [queryScript, "--json", "--limit", "5"];
    if (testCase.kind) {args.push("--kind", testCase.kind);}
    if (testCase.project) {args.push("--project", testCase.project);}
    args.push(testCase.query);

    const stdout = await runNode(args);
    const parsed = JSON.parse(stdout);
    const expected = normalizeExpected(testCase.expectedSources);
    const ranks = resultRanks(parsed.results || [], expected);

    caseResults.push({
      name: testCase.name,
      query: testCase.query,
      expectedSources: [...expected],
      notes: testCase.notes || "",
      topSource: parsed.results?.[0]?.source || null,
      topScore: parsed.results?.[0]?.score || null,
      ...ranks,
    });
  }

  const summary = {
    total: caseResults.length,
    top1Hits: caseResults.filter((r) => r.top1).length,
    top3Hits: caseResults.filter((r) => r.top3).length,
    top5Hits: caseResults.filter((r) => r.top5).length,
  };
  summary.top1Rate = Number((summary.top1Hits / summary.total).toFixed(3));
  summary.top3Rate = Number((summary.top3Hits / summary.total).toFixed(3));
  summary.top5Rate = Number((summary.top5Hits / summary.total).toFixed(3));

  process.stdout.write(`${JSON.stringify({ benchmarkPath, summary, cases: caseResults }, null, 2)}\n`);
}

main().catch((err) => {
  process.stderr.write(`ERROR: ${err.message}\n`);
  process.exit(1);
});
