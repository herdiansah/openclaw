#!/usr/bin/env node
/**
 * OpenAI Sidecar - Multi-Account Manager
 * Handles load balancing, rotation, and quota tracking for 4 OpenAI accounts
 */

import fs from 'fs';
import path from 'path';
import https from 'https';
import { execSync } from 'child_process';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const SCRIPT_DIR = __dirname;
const CONFIG_FILE = path.join(SCRIPT_DIR, 'config-multi.json');
const TOKENS_DIR = path.join(SCRIPT_DIR, 'tokens');

// Load config
function loadConfig() {
  if (!fs.existsSync(CONFIG_FILE)) {
    console.error('❌ Config file not found:', CONFIG_FILE);
    console.error('Run: ./openai-sidecar-oauth.sh --setup-multi');
    process.exit(1);
  }
  return JSON.parse(fs.readFileSync(CONFIG_FILE, 'utf8'));
}

// Save config
function saveConfig(config) {
  fs.writeFileSync(CONFIG_FILE, JSON.stringify(config, null, 2));
}

// Get token from account file
function getToken(accountId) {
  const config = loadConfig();
  const account = config.accounts[accountId];
  if (!account) {
    throw new Error(`Account ${accountId} not found`);
  }
  
  const tokenFile = path.join(SCRIPT_DIR, account.tokenFile);
  if (!fs.existsSync(tokenFile)) {
    throw new Error(`Token file not found for account ${accountId}`);
  }
  
  const tokenData = JSON.parse(fs.readFileSync(tokenFile, 'utf8'));
  
  // Check expiration
  if (tokenData.expires && Date.now() >= tokenData.expires) {
    throw new Error(`Token expired for account ${accountId}`);
  }
  
  return tokenData.access || tokenData.access_token;
}

// Health check for an account
async function checkHealth(accountId) {
  try {
    const token = getToken(accountId);
    return new Promise((resolve) => {
      const req = https.request({
        hostname: 'api.openai.com',
        port: 443,
        path: '/v1/models',
        method: 'GET',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        },
        timeout: 10000
      }, (res) => {
        let data = '';
        res.on('data', chunk => data += chunk);
        res.on('end', () => {
          if (res.statusCode === 200) {
            resolve({ ok: true, status: res.statusCode });
          } else {
            resolve({ ok: false, status: res.statusCode, error: data });
          }
        });
      });
      
      req.on('error', (e) => {
        resolve({ ok: false, error: e.message });
      });
      req.on('timeout', () => {
        req.destroy();
        resolve({ ok: false, error: 'timeout' });
      });
      req.end();
    });
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

// Select account based on strategy
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
      // Select least recently used
      enabledAccounts.sort((a, b) => {
        const aTime = a.lastUsed || 0;
        const bTime = b.lastUsed || 0;
        return aTime - bTime;
      });
      selected = enabledAccounts[0];
      break;
      
    case 'load-balance':
      // Select account with lowest quota usage
      enabledAccounts.sort((a, b) => {
        const aUsage = a.quotaUsed / (a.quotaLimit || 100);
        const bUsage = b.quotaUsed / (b.quotaLimit || 100);
        return aUsage - bUsage;
      });
      selected = enabledAccounts[0];
      break;
      
    case 'priority':
      // Select by priority order
      enabledAccounts.sort((a, b) => a.priority - b.priority);
      selected = enabledAccounts[0];
      break;
      
    case 'random':
      selected = enabledAccounts[Math.floor(Math.random() * enabledAccounts.length)];
      break;
      
    default:
      selected = enabledAccounts[0];
  }
  
  return selected;
}

// Update account usage
function recordUsage(accountId, tokens = 1) {
  const config = loadConfig();
  if (config.accounts[accountId]) {
    config.accounts[accountId].lastUsed = Date.now();
    config.accounts[accountId].quotaUsed = (config.accounts[accountId].quotaUsed || 0) + tokens;
    saveConfig(config);
  }
}

// Reset quota for all accounts
function resetQuota() {
  const config = loadConfig();
  Object.values(config.accounts).forEach(acc => {
    acc.quotaUsed = 0;
  });
  saveConfig(config);
  console.log('✅ Quota reset for all accounts');
}

// Setup new account
async function setupAccount(accountId) {
  const config = loadConfig();
  const account = config.accounts[accountId];
  
  if (!account) {
    console.error(`❌ Account ${accountId} not found in config`);
    return false;
  }
  
  console.log(`\n🔐 Setting up Account ${accountId}...`);
  console.log('This will open a browser for OAuth login.');
  console.log('Please login with the correct OpenAI account.\n');
  
  // Use the existing OAuth setup script
  const setupScript = path.join(SCRIPT_DIR, 'oauth-login.js');
  
  try {
    // Run OAuth login
    const tokenFile = path.join(SCRIPT_DIR, account.tokenFile);
    
    // Ensure tokens directory exists
    if (!fs.existsSync(TOKENS_DIR)) {
      fs.mkdirSync(TOKENS_DIR, { recursive: true });
    }
    
    console.log(`Token will be saved to: ${tokenFile}`);
    console.log('\nStarting OAuth flow...\n');
    
    // Execute OAuth login
    execSync(`node "${setupScript}"`, { 
      stdio: 'inherit',
      env: { ...process.env, SIDECAR_TOKEN_FILE: tokenFile }
    });
    
    // Verify token was created
    if (fs.existsSync(tokenFile)) {
      const health = await checkHealth(accountId);
      if (health.ok) {
        account.healthStatus = 'healthy';
        account.enabled = true;
        saveConfig(config);
        console.log(`\n✅ Account ${accountId} setup complete!`);
        console.log(`   Status: Healthy`);
        return true;
      } else {
        account.healthStatus = 'unhealthy';
        saveConfig(config);
        console.log(`\n⚠️ Account ${accountId} setup complete but health check failed`);
        console.log(`   Error: ${health.error || health.status}`);
        return false;
      }
    } else {
      console.error(`\n❌ Token file not created for account ${accountId}`);
      return false;
    }
  } catch (e) {
    console.error(`\n❌ Setup failed for account ${accountId}: ${e.message}`);
    return false;
  }
}

// Show status of all accounts
async function showStatus() {
  const config = loadConfig();
  
  console.log('\n🔐 OpenAI Sidecar - Multi-Account Status');
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n');
  
  const rotationStrategy = config.rotation?.strategy || 'load-balance';
  console.log(`Rotation Strategy: ${rotationStrategy}`);
  console.log(`Failover: ${config.rotation?.failover ? '✅ Enabled' : '❌ Disabled'}\n`);
  
  for (const [id, account] of Object.entries(config.accounts)) {
    const tokenFile = path.join(SCRIPT_DIR, account.tokenFile);
    const hasToken = fs.existsSync(tokenFile);
    
    let status = '⚪ Not setup';
    let quotaInfo = '-';
    let healthInfo = 'unknown';
    
    if (hasToken) {
      try {
        const tokenData = JSON.parse(fs.readFileSync(tokenFile, 'utf8'));
        const expiresAt = tokenData.expires || tokenData.expires_at;
        const expiresAtMs = typeof expiresAt === 'string' ? new Date(expiresAt).getTime() : expiresAt;
        
        if (expiresAtMs && Date.now() >= expiresAtMs) {
          status = '❌ Expired';
        } else {
          status = '✅ Active';
          
          // Check health
          const health = await checkHealth(id);
          healthInfo = health.ok ? '✅ Healthy' : `❌ ${health.error || health.status}`;
        }
        
        // Quota info
        const quotaUsed = account.quotaUsed || 0;
        const quotaLimit = account.quotaLimit || 100;
        const quotaPct = Math.round((quotaUsed / quotaLimit) * 100);
        quotaInfo = `${quotaUsed}/${quotaLimit} (${quotaPct}%)`;
        
        // Last used
        if (account.lastUsed) {
          const lastUsed = new Date(account.lastUsed);
          quotaInfo += ` | Last: ${lastUsed.toLocaleString()}`;
        }
      } catch (e) {
        status = `❌ Error: ${e.message}`;
      }
    }
    
    console.log(`Account ${id}: ${account.name || 'Unnamed'}`);
    console.log(`  Status: ${status}`);
    console.log(`  Health: ${healthInfo}`);
    console.log(`  Quota: ${quotaInfo}`);
    console.log(`  Priority: ${account.priority}`);
    console.log(`  Models: ${account.models?.join(', ') || 'default'}`);
    console.log(`  Token: ${tokenFile}`);
    console.log('');
  }
  
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n');
}

// Main CLI
async function main() {
  const args = process.argv.slice(2);
  const command = args[0];
  
  switch (command) {
    case 'setup':
      const accountId = args[1];
      if (!accountId) {
        console.error('Usage: node multi-account-manager.js setup <account-id>');
        console.error('Example: node multi-account-manager.js setup 1');
        process.exit(1);
      }
      await setupAccount(accountId);
      break;
      
    case 'setup-all':
      console.log('🔐 Setting up all 4 accounts...\n');
      for (let i = 1; i <= 4; i++) {
        await setupAccount(String(i));
        if (i < 4) {
          console.log('\n---\n');
          console.log('Press Enter to continue to next account...');
          await new Promise(resolve => process.stdin.once('data', resolve));
        }
      }
      console.log('\n✅ All accounts setup complete!\n');
      break;
      
    case 'status':
      await showStatus();
      break;
      
    case 'select':
      const config = loadConfig();
      const strategy = args[1] || config.rotation?.strategy || 'load-balance';
      const selected = selectAccount(config, strategy);
      console.log(`Selected Account: ${selected.id}`);
      console.log(`Name: ${selected.name}`);
      console.log(`Strategy: ${strategy}`);
      console.log(`Quota: ${selected.quotaUsed}/${selected.quotaLimit}`);
      break;
      
    case 'reset-quota':
      resetQuota();
      break;
      
    case 'health':
      const config2 = loadConfig();
      for (const [id, account] of Object.entries(config2.accounts)) {
        const tokenFile = path.join(SCRIPT_DIR, account.tokenFile);
        if (fs.existsSync(tokenFile)) {
          const health = await checkHealth(id);
          console.log(`Account ${id}: ${health.ok ? '✅ Healthy' : `❌ ${health.error || health.status}`}`);
        } else {
          console.log(`Account ${id}: ⚪ Not setup`);
        }
      }
      break;
      
    case 'get-token':
      const accId = args[1];
      if (!accId) {
        console.error('Usage: node multi-account-manager.js get-token <account-id>');
        process.exit(1);
      }
      try {
        const token = getToken(accId);
        console.log(token);
      } catch (e) {
        console.error(`Error: ${e.message}`);
        process.exit(1);
      }
      break;
      
    default:
      console.log('OpenAI Sidecar - Multi-Account Manager');
      console.log('======================================\n');
      console.log('Usage: node multi-account-manager.js <command>\n');
      console.log('Commands:');
      console.log('  setup <id>       Setup single account (1-4)');
      console.log('  setup-all        Setup all 4 accounts interactively');
      console.log('  status           Show status of all accounts');
      console.log('  select [strat]   Select best account (round-robin|load-balance|priority|random)');
      console.log('  reset-quota      Reset quota usage for all accounts');
      console.log('  health           Check health of all accounts');
      console.log('  get-token <id>   Get token for specific account');
      console.log('');
  }
}

main().catch(console.error);
