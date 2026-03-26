#!/usr/bin/env node
/**
 * Extract all OpenAI Codex accounts from OpenClaw auth-profiles.json
 * and save to openai-sidecar multi-account format
 */

import { readFileSync, writeFileSync, mkdirSync, existsSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const OPENCLAW_AUTH = process.env.HOME + '/.openclaw/agents/main/agent/auth-profiles.json';
const SIDECAR_TOKENS_DIR = join(__dirname, 'tokens');

console.log('🔐 OpenAI Multi-Account Extractor');
console.log('==================================\n');

// Check if auth file exists
if (!existsSync(OPENCLAW_AUTH)) {
  console.error(`❌ OpenClaw auth file not found: ${OPENCLAW_AUTH}`);
  console.error('\nMake sure you have OpenAI Codex authenticated in OpenClaw:');
  console.error('  openclaw models auth login --provider openai-codex');
  process.exit(1);
}

// Read auth profiles
const authData = JSON.parse(readFileSync(OPENCLAW_AUTH, 'utf-8'));

// Find all openai-codex profiles
const codexAccounts = [];

for (const [key, value] of Object.entries(authData)) {
  if (key.startsWith('openai-codex')) {
    console.log(`📄 Found: ${key}`);
    
    if (value.access && value.expires) {
      codexAccounts.push({
        key,
        access: value.access,
        refresh: value.refresh,
        expires: value.expires,
        accountId: extractAccountId(value.access)
      });
    }
  }
}

if (codexAccounts.length === 0) {
  console.error('\n❌ No OpenAI Codex accounts found in auth-profiles.json');
  console.error('\nLogin first:');
  console.error('  openclaw models auth login --provider openai-codex');
  process.exit(1);
}

console.log(`\n✅ Found ${codexAccounts.length} account(s)\n`);

// Ensure tokens directory exists
if (!existsSync(SIDECAR_TOKENS_DIR)) {
  mkdirSync(SIDECAR_TOKENS_DIR, { recursive: true });
  console.log(`📁 Created tokens directory: ${SIDECAR_TOKENS_DIR}\n`);
}

// Save each account to sidecar format
codexAccounts.forEach((account, index) => {
  const accountNum = index + 1;
  const tokenFile = join(SIDECAR_TOKENS_DIR, `account-${accountNum}.json`);
  
  const tokenData = {
    access: account.access,
    refresh: account.refresh,
    expires: account.expires,
    accountId: account.accountId,
    email: extractEmail(account.access)
  };
  
  writeFileSync(tokenFile, JSON.stringify(tokenData, null, 2));
  
  console.log(`✅ Account ${accountNum}: ${tokenData.email || account.accountId}`);
  console.log(`   Token: ${tokenFile}`);
  console.log(`   Expires: ${new Date(account.expires).toLocaleString()}`);
  console.log('');
});

// Update config-multi.json to enable accounts
const configFile = join(__dirname, 'config-multi.json');
if (existsSync(configFile)) {
  const config = JSON.parse(readFileSync(configFile, 'utf-8'));
  
  // Enable accounts based on what we found
  for (let i = 1; i <= 4; i++) {
    if (config.accounts[i]) {
      config.accounts[i].enabled = i <= codexAccounts.length;
      config.accounts[i].lastUsed = null;
      config.accounts[i].quotaUsed = 0;
    }
  }
  
  writeFileSync(configFile, JSON.stringify(config, null, 2));
  console.log('✅ Updated config-multi.json\n');
}

console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
console.log('\n🎉 Setup complete!');
console.log('\nNext steps:');
console.log('  1. Verify: ./openai-multi-wrapper.sh --status');
console.log('  2. Test: ./openai-multi-wrapper.sh "Hello!"');
console.log('  3. Use in agents: sessions_spawn({ cwd: "/root/clawd/openai-sidecar", env: { OPENAI_ACCOUNT: "2" } })');
console.log('');

// Helper functions
function extractAccountId(token) {
  try {
    const parts = token.split('.');
    if (parts.length !== 3) return 'unknown';
    const payload = JSON.parse(Buffer.from(parts[1], 'base64url').toString('utf8'));
    return payload?.['https://api.openai.com/auth']?.chatgpt_account_id || 'unknown';
  } catch {
    return 'unknown';
  }
}

function extractEmail(token) {
  try {
    const parts = token.split('.');
    if (parts.length !== 3) return null;
    const payload = JSON.parse(Buffer.from(parts[1], 'base64url').toString('utf8'));
    return payload?.['https://api.openai.com/profile']?.email || null;
  } catch {
    return null;
  }
}
