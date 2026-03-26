#!/usr/bin/env node
/**
 * OpenAI Sidecar - Multi-Account Version
 * Supports 4 accounts with load balancing and rotation
 */

import { existsSync, readFileSync, writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import https from "https";

const __dirname = dirname(fileURLToPath(import.meta.url));
const TOKENS_DIR = join(__dirname, "tokens");
const CONFIG_FILE = join(__dirname, "config-multi.json");
const BASE_URL = "https://chatgpt.com/backend-api";
const RESPONSES_URL = `${BASE_URL}/codex/responses`;
const DEFAULT_MODEL = "gpt-5.3-codex";

// Load config
function loadConfig() {
  if (!existsSync(CONFIG_FILE)) {
    throw new Error(`Config not found: ${CONFIG_FILE}\nRun: ./openai-multi-wrapper.sh --setup-all`);
  }
  return JSON.parse(readFileSync(CONFIG_FILE, "utf-8"));
}

// Get token for specific account
function getTokenForAccount(accountId) {
  const config = loadConfig();
  const account = config.accounts[accountId];
  
  if (!account) {
    throw new Error(`Account ${accountId} not found in config`);
  }
  
  const tokenFile = join(__dirname, account.tokenFile);
  if (!existsSync(tokenFile)) {
    throw new Error(`Token file not found for account ${accountId}: ${tokenFile}`);
  }
  
  const raw = JSON.parse(readFileSync(tokenFile, "utf-8"));
  const data = raw["openai-codex"] || raw;
  const access = data.access || data.access_token;
  const expires = data.expires || data.expires_at;
  const accountIdFromToken = data.accountId;
  
  if (!access) {
    throw new Error(`No token found for account ${accountId}`);
  }
  
  const expiresAt = typeof expires === 'string' ? new Date(expires).getTime() : expires;
  if (expiresAt && Date.now() >= expiresAt) {
    throw new Error(`Token expired for account ${accountId}`);
  }
  
  return { access, expires: expiresAt, accountId: accountIdFromToken || accountId };
}

// Auto-select account based on strategy
function selectAccount(config, strategy = 'load-balance') {
  const enabledAccounts = Object.entries(config.accounts)
    .filter(([_, acc]) => acc.enabled)
    .map(([id, acc]) => ({ id, ...acc }));
  
  if (enabledAccounts.length === 0) {
    throw new Error('No enabled accounts');
  }
  
  let selected = null;
  
  switch (strategy) {
    case 'round-robin':
      enabledAccounts.sort((a, b) => (a.lastUsed || 0) - (b.lastUsed || 0));
      selected = enabledAccounts[0];
      break;
    case 'load-balance':
      enabledAccounts.sort((a, b) => {
        const aUsage = (a.quotaUsed || 0) / (a.quotaLimit || 100);
        const bUsage = (b.quotaUsed || 0) / (b.quotaLimit || 100);
        return aUsage - bUsage;
      });
      selected = enabledAccounts[0];
      break;
    case 'priority':
      enabledAccounts.sort((a, b) => a.priority - b.priority);
      selected = enabledAccounts[0];
      break;
    default:
      selected = enabledAccounts[0];
  }
  
  return selected;
}

// Record usage
function recordUsage(accountId, tokens = 1) {
  try {
    const config = loadConfig();
    if (config.accounts[accountId]) {
      config.accounts[accountId].lastUsed = Date.now();
      config.accounts[accountId].quotaUsed = (config.accounts[accountId].quotaUsed || 0) + tokens;
      writeFileSync(CONFIG_FILE, JSON.stringify(config, null, 2));
    }
  } catch (e) {
    console.error('Warning: Could not record usage:', e.message);
  }
}

function extractAccountIdFromJwt(token) {
  try {
    const parts = token.split(".");
    if (parts.length !== 3) return null;
    const payload = JSON.parse(Buffer.from(parts[1], "base64url").toString("utf8"));
    return payload?.["https://api.openai.com/auth"]?.chatgpt_account_id || null;
  } catch {
    return null;
  }
}

function buildHeaders(token, accountId) {
  return {
    Authorization: `Bearer ${token}`,
    "chatgpt-account-id": accountId,
    "OpenAI-Beta": "responses=experimental",
    originator: "pi",
    accept: "application/json",
    "content-type": "application/json",
    "user-agent": "pi (linux)"
  };
}

function extractOutputText(resp) {
  const out = resp?.output;
  if (Array.isArray(out)) {
    const texts = [];
    for (const item of out) {
      if (item?.type === "message" && Array.isArray(item.content)) {
        for (const c of item.content) {
          if (c?.type === "output_text" && c.text) texts.push(c.text);
          if (c?.type === "refusal" && c.refusal) texts.push(c.refusal);
        }
      }
    }
    if (texts.length) return texts.join("\n");
  }
  return resp?.output_text || "";
}

async function parseSSEText(response) {
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buf = "";
  let text = "";

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buf += decoder.decode(value, { stream: true });

    let idx;
    while ((idx = buf.indexOf("\n\n")) !== -1) {
      const chunk = buf.slice(0, idx);
      buf = buf.slice(idx + 2);
      const lines = chunk.split("\n").filter(l => l.startsWith("data:"));
      for (const l of lines) {
        const payload = l.slice(5).trim();
        if (!payload || payload === "[DONE]") continue;
        let ev;
        try { ev = JSON.parse(payload); } catch { continue; }
        if (ev.type === "response.output_text.delta" && ev.delta) text += ev.delta;
        if (ev.type === "response.refusal.delta" && ev.delta) text += ev.delta;
        if (ev.type === "response.output_item.done" && ev.item?.type === "message" && Array.isArray(ev.item.content) && !text) {
          for (const c of ev.item.content) {
            if (c?.type === "output_text" && c.text) text += c.text;
            if (c?.type === "refusal" && c.refusal) text += c.refusal;
          }
        }
      }
    }
  }

  return text.trim();
}

async function callOpenAI(prompt, model, tokenData, stream = false) {
  const { access, accountId } = tokenData;
  const headers = buildHeaders(access, accountId);
  
  const payload = {
    model: model || DEFAULT_MODEL,
    input: [{ type: "message", content: [{ type: "input_text", text: prompt }] }],
    stream: stream,
    temperature: 0.7,
    max_output_tokens: 4096
  };

  return new Promise((resolve, reject) => {
    const url = new URL(RESPONSES_URL);
    if (stream) url.searchParams.set("stream", "true");
    
    const req = https.request(url, {
      method: "POST",
      headers,
      timeout: 120000
    }, (res) => {
      if (stream) {
        parseSSEText(res).then(resolve).catch(reject);
        return;
      }
      
      let data = "";
      res.on("data", chunk => data += chunk);
      res.on("end", () => {
        try {
          const resp = JSON.parse(data);
          resolve(extractOutputText(resp));
        } catch (e) {
          reject(new Error(`Parse error: ${e.message}\nResponse: ${data.slice(0, 500)}`));
        }
      });
    });
    
    req.on("error", reject);
    req.on("timeout", () => {
      req.destroy();
      reject(new Error("Request timed out"));
    });
    
    req.write(JSON.stringify(payload));
    req.end();
  });
}

// Main
async function main() {
  const args = process.argv.slice(2);
  
  // Parse arguments
  let prompt = "";
  let model = DEFAULT_MODEL;
  let accountId = process.env.OPENAI_ACCOUNT || null;
  let strategy = "load-balance";
  let useEnvToken = !!process.env.OPENAI_TOKEN;
  
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg === "--model" && args[i + 1]) {
      model = args[++i];
    } else if (arg === "--account" && args[i + 1]) {
      accountId = args[++i];
    } else if (arg === "--strategy" && args[i + 1]) {
      strategy = args[++i];
    } else if (arg === "--stream") {
      // Enable streaming
    } else if (!arg.startsWith("--")) {
      prompt += (prompt ? " " : "") + arg;
    }
  }
  
  // Handle env token (from wrapper script)
  if (useEnvToken && process.env.OPENAI_TOKEN && process.env.OPENAI_ACCOUNT) {
    const tokenData = {
      access: process.env.OPENAI_TOKEN,
      accountId: process.env.OPENAI_ACCOUNT
    };
    
    try {
      const result = await callOpenAI(prompt, model, tokenData, false);
      console.log(result);
      recordUsage(process.env.OPENAI_ACCOUNT, 1);
      process.exit(0);
    } catch (e) {
      console.error(`❌ Account ${process.env.OPENAI_ACCOUNT} failed:`, e.message);
      process.exit(1);
    }
  }
  
  // Auto-select or specific account
  const config = loadConfig();
  
  let tokenData;
  let selectedAccountId = accountId;
  
  if (accountId) {
    // Specific account requested
    tokenData = getTokenForAccount(accountId);
  } else {
    // Auto-select
    const selected = selectAccount(config, strategy);
    selectedAccountId = selected.id;
    tokenData = getTokenForAccount(selected.id);
    console.error(`📡 Using Account ${selected.id} (strategy: ${strategy})`);
  }
  
  if (!prompt) {
    console.error("Usage: node openai-sidecar-multi.js [options] <prompt>");
    console.error("Options: --model <name>, --account <1-4>, --strategy <load-balance|round-robin|priority>");
    process.exit(1);
  }
  
  try {
    const result = await callOpenAI(prompt, model, tokenData, false);
    console.log(result);
    recordUsage(selectedAccountId, 1);
  } catch (e) {
    console.error(`❌ Error:`, e.message);
    process.exit(1);
  }
}

main().catch(e => {
  console.error("Fatal:", e.message);
  process.exit(1);
});
