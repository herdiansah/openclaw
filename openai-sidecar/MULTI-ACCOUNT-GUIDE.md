# OpenAI Sidecar - Multi-Account Guide

**4 OpenAI Pro Accounts → Load Balanced Pool**

---

## 🚀 Quick Start

### Option A: Manual Token Copy (Recommended for Existing OpenClaw Users)

If you already have OpenAI Codex authenticated in OpenClaw:

**1. Login to Account 1 via OpenClaw:**
```bash
openclaw models auth login --provider openai-codex
# Follow the OAuth flow in your browser
```

**2. Extract Token to Multi-Account:**
```bash
cd /root/clawd/openai-sidecar
./extract-openclaw-token.sh 1
```

**3. Switch to Account 2 in OpenClaw:**
```bash
# Logout current account (if needed)
openclaw models auth logout --provider openai-codex

# Login with account 2
openclaw models auth login --provider openai-codex
```

**4. Extract Token for Account 2:**
```bash
./extract-openclaw-token.sh 2
```

**5. Repeat for Accounts 3 & 4**

**6. Verify All Accounts:**
```bash
./openai-multi-wrapper.sh --status
```

---

### Option B: Interactive OAuth Setup

```bash
cd /root/clawd/openai-sidecar
./openai-multi-wrapper.sh --setup-all
```

This will:
- Create token files for 4 accounts
- Prompt you to login to each account sequentially
- Save OAuth tokens separately for each account
- Configure load balancing

### 2. Verify Setup

```bash
./openai-multi-wrapper.sh --status
```

Expected output:
```
🔐 OpenAI Sidecar - Multi-Account Status
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Rotation Strategy: load-balance
Failover: ✅ Enabled

Account 1: Account 1
  Status: ✅ Active
  Health: ✅ Healthy
  Quota: 0/100 (0%)
  ...

Account 2: Account 2
  Status: ✅ Active
  Health: ✅ Healthy
  Quota: 0/100 (0%)
  ...

[etc for all 4 accounts]
```

### 3. Use It!

```bash
# Auto-select best account (load balancing)
./openai-multi-wrapper.sh "Hello, how are you?"

# Use specific account
./openai-multi-wrapper.sh --account 2 "Hello!"

# Force rotation
./openai-multi-wrapper.sh --rotate "Test"

# With specific model
./openai-multi-wrapper.sh --account 3 --model gpt-4o "Hi"
```

---

## 📊 Architecture

```
┌─────────────────────────────────────────────────────────┐
│              OpenAI Sidecar Multi-Account               │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  ┌──────────────┐    ┌──────────────┐                  │
│  │   Account 1  │    │   Account 2  │                  │
│  │  token-1.json│    │  token-2.json│                  │
│  └──────────────┘    └──────────────┘                  │
│                                                         │
│  ┌──────────────┐    ┌──────────────┐                  │
│  │   Account 3  │    │   Account 4  │                  │
│  │  token-3.json│    │  token-4.json│                  │
│  └──────────────┘    └──────────────┘                  │
│                                                         │
│              ⬇ Load Balancer ⬇                         │
│                                                         │
│  Strategy: load-balance | round-robin | priority       │
│  Failover: Auto-retry on failure                       │
│  Quota Tracking: Per-account usage                     │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

---

## 🎯 Rotation Strategies

### Load Balance (Default)
Selects account with lowest quota usage.

```bash
./openai-multi-wrapper.sh --strategy load-balance "prompt"
```

### Round-Robin
Selects least recently used account.

```bash
./openai-multi-wrapper.sh --strategy round-robin "prompt"
```

### Priority
Uses account priority order (1 → 2 → 3 → 4).

```bash
./openai-multi-wrapper.sh --strategy priority "prompt"
```

### Random
Random account selection.

```bash
./openai-multi-wrapper.sh --strategy random "prompt"
```

---

## 🔧 Configuration

Edit `/root/clawd/openai-sidecar/config-multi.json`:

```json
{
  "accounts": {
    "1": {
      "name": "Primary Account",
      "tokenFile": "tokens/account-1.json",
      "enabled": true,
      "priority": 1,
      "models": ["gpt-4o", "gpt-4.5", "gpt-o3"],
      "quotaLimit": 100,
      "quotaUsed": 0
    },
    "2": { ... },
    "3": { ... },
    "4": { ... }
  },
  "rotation": {
    "strategy": "load-balance",
    "failover": true,
    "healthCheckInterval": 300
  }
}
```

### Config Options

| Field | Description | Default |
|-------|-------------|---------|
| `name` | Account label | "Account N" |
| `enabled` | Include in rotation | true |
| `priority` | Priority order (1=highest) | 1-4 |
| `models` | Allowed models | All |
| `quotaLimit` | Max requests before rotation | 100 |
| `quotaUsed` | Current usage counter | 0 |

---

## 🤖 Agent Integration

### Via Environment Variable

```javascript
// Use account 2
sessions_spawn({
  task: 'Build feature X',
  cwd: '/root/clawd/openai-sidecar',
  env: { OPENAI_ACCOUNT: '2' }
})

// Use account 3
sessions_spawn({
  task: 'Review PR',
  cwd: '/root/clawd/openai-sidecar',
  env: { OPENAI_ACCOUNT: '3' }
})
```

### Auto Load Balancing

```javascript
// Auto-select best account
sessions_spawn({
  task: 'Generate docs',
  cwd: '/root/clawd/openai-sidecar',
  env: { OPENAI_AUTO_BALANCE: 'true' }
})
```

### Parallel Agents (All 4 Accounts)

```javascript
// Spawn 4 agents simultaneously, each using different account
const agents = [
  sessions_spawn({
    task: 'Task 1',
    cwd: '/root/clawd/openai-sidecar',
    env: { OPENAI_ACCOUNT: '1' }
  }),
  sessions_spawn({
    task: 'Task 2',
    cwd: '/root/clawd/openai-sidecar',
    env: { OPENAI_ACCOUNT: '2' }
  }),
  sessions_spawn({
    task: 'Task 3',
    cwd: '/root/clawd/openai-sidecar',
    env: { OPENAI_ACCOUNT: '3' }
  }),
  sessions_spawn({
    task: 'Task 4',
    cwd: '/root/clawd/openai-sidecar',
    env: { OPENAI_ACCOUNT: '4' }
  })
];

await Promise.all(agents);
```

---

## 📈 Monitoring

### Check All Accounts Status

```bash
./openai-multi-wrapper.sh --status
```

### Health Check

```bash
./openai-multi-wrapper.sh --health
```

Output:
```
Account 1: ✅ Healthy
Account 2: ✅ Healthy
Account 3: ❌ Token expired
Account 4: ⚪ Not setup
```

### Reset Quota Counters

```bash
./openai-multi-wrapper.sh --reset-quota
```

### Manual Account Selection

```bash
node multi-account-manager.js select load-balance
# Output: Selected Account: 2
```

---

## 🔐 Security

- **Token Isolation**: Each account has separate token file
- **No Cross-Contamination**: Account 1 token never used for Account 2
- **Auto Expiry Check**: Tokens validated before each request
- **Failover Safe**: Failed accounts skipped automatically

### Token Files

```
/root/clawd/openai-sidecar/tokens/
├── account-1.json  # OAuth token for account 1
├── account-2.json  # OAuth token for account 2
├── account-3.json  # OAuth token for account 3
└── account-4.json  # OAuth token for account 4
```

**⚠️ Never commit these to git!** (Already in `.gitignore`)

---

## 🛠️ Troubleshooting

### "No enabled accounts"

Run setup:
```bash
./openai-multi-wrapper.sh --setup-all
```

### "Token expired"

Re-authenticate specific account:
```bash
node multi-account-manager.js setup 2
```

### "Health check failed"

Check token validity:
```bash
./openai-multi-wrapper.sh --health
```

If expired, re-run setup for that account.

### Reset Everything

```bash
# Delete all tokens
rm -rf /root/clawd/openai-sidecar/tokens/*

# Reset config
cp config-multi.json.example config-multi.json

# Re-setup
./openai-multi-wrapper.sh --setup-all
```

---

## 📝 Commands Reference

| Command | Description |
|---------|-------------|
| `--setup-all` | Setup all 4 accounts interactively |
| `--status` | Show status of all accounts |
| `--health` | Check health of all accounts |
| `--reset-quota` | Reset quota usage counters |
| `--account N` | Use specific account (1-4) |
| `--rotate` | Auto-select with load balancing |
| `--strategy S` | Set rotation strategy |
| `--model M` | Specify model |
| `--help` | Show help |

---

## 🎯 Best Practices

1. **Label Your Accounts**: Edit `config-multi.json` to give accounts meaningful names
2. **Monitor Quota**: Check status regularly with `--status`
3. **Rotate Tokens**: Re-authenticate before expiry (tokens last ~2 days)
4. **Use Load Balancing**: Default strategy is best for most cases
5. **Parallel Tasks**: Use different accounts for parallel agents

---

## 📊 Example: Heavy Batch Processing

```bash
#!/bin/bash
# Process 100 tasks across 4 accounts

for i in {1..100}; do
  account=$((($i % 4) + 1))
  ./openai-multi-wrapper.sh --account $account "Process task $i" &
  
  # Limit concurrency
  if [ $(($i % 4)) -eq 0 ]; then
    wait
  fi
done

wait
echo "All tasks complete!"
```

---

**Version:** 1.0  
**Last Updated:** 2026-03-25  
**Author:** Kuproy 🤡
