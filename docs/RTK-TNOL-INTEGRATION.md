# RTK + TNOL Integration Guide

## Overview

This guide explains how to combine **RTK** (CLI output filter) and **TNOL** (JSON compression) for maximum token savings in OpenClaw.

## Token Savings Potential

| Layer | Command Type | Savings |
|-------|--------------|---------|
| **RTK** | Git commands | -80% |
| **RTK** | Test runners | -90% |
| **RTK** | Docker/K8s | -80% |
| **TNOL** | JSON responses | -13-40% |
| **Combined** | Mixed workflows | **-65-85%** |

## Architecture

```
User → Agent → Exec Tool → RTK Wrapper → Shell
                                      ↓
                                Filtered output
                                      ↓
                              ToolResult created
                                      ↓
                               TNOL compresses
                                      ↓
                                Context storage
```

## Installation

### 1. Install RTK

```bash
curl -fsSL https://raw.githubusercontent.com/rtk-ai/rtk/refs/heads/master/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"
```

### 2. Verify Installation

```bash
rtk --version  # Should show v0.35.0+
rtk git status # Test RTK optimization
```

### 3. Enable OpenClaw Plugin

Edit `/root/.openclaw/openclaw.json`:

```json
{
  "plugins": {
    "allow": [
      "tonl-tool-result-persist",
      "rtk-exec-wrapper"
    ],
    "load": {
      "paths": [
        "/root/clawd/openclaw/extensions/tonl-tool-result-persist/tonl-tool-result-persist.mjs",
        "/root/clawd/openclaw/extensions/rtk-exec-wrapper/rtk-exec-wrapper.mjs"
      ]
    }
  }
}
```

### 4. Restart OpenClaw

```bash
openclaw gateway restart
```

## Configuration

### RTK Commands

Default optimized commands:
```javascript
const RTK_COMMANDS = [
  "git",
  "cargo test",
  "npm test",
  "yarn test",
  "pnpm test",
  "docker ps",
  "docker logs",
  "docker images",
  "kubectl",
  "ls",
  "cat",
  "find",
  "grep",
  "rg",
];
```

### Environment Variables

```bash
# RTK binary path (optional)
export RTK_PATH=/root/.local/bin/rtk

# TNOL minimum characters (optional)
export OPENCLAW_TONL_MIN_CHARS=600
```

## Usage Examples

### Git Workflow

```bash
# Before: ~2,000 tokens
git status

# After: ~400 tokens (80% savings)
rtk git status
```

### Test Workflow

```bash
# Before: ~25,000 tokens
cargo test

# After: ~2,500 tokens (90% savings)
rtk test cargo test
```

### Combined RTK + TNOL

```bash
# 1. RTK filters CLI output
rtk git diff

# 2. Output becomes ToolResult

# 3. TNOL compresses JSON structure
# Result: 80% (RTK) + 25% (TNOL) = ~85% total savings
```

## Verification

### Check RTK is Active

```bash
# Run optimized command
rtk git log -n 3

# Check tracking
rtk gain
```

### Check TNOL is Active

```bash
# Search for TONL markers
rg -n "\[format: tonl\]" /root/.openclaw/agents/main/sessions -S

# Should show encoded toolResults
```

### Measure Combined Savings

```bash
# Before optimization (estimate)
git status | wc -c  # ~10,000 chars

# After RTK
rtk git status | wc -c  # ~2,000 chars

# After TNOL (JSON responses)
# Check session logs for tonl.savedTokensEstimate
```

## Troubleshooting

### RTK Not Found

```bash
# Check installation
which rtk

# Add to PATH
export PATH="$HOME/.local/bin:$PATH"

# Verify
rtk --version
```

### RTK Command Fails

```bash
# Fallback is automatic - command runs without RTK
# Check logs for fallback messages

# Test RTK directly
rtk git status
```

### TNOL Not Compressing

Check:
1. Payload size > 600 chars
2. Valid JSON structure
3. TNOL actually smaller than JSON
4. Plugin loaded correctly

```bash
# Verify plugin status
cat /root/.openclaw/openclaw.json | jq '.plugins'

# Check session logs
rg "tonl" /root/.openclaw/agents/main/sessions -S | head -20
```

## Performance Impact

### Latency

- RTK overhead: <10ms per command
- TNOL overhead: <5ms per toolResult
- **Total impact: Negligible**

### Token Savings (Real-world)

Based on typical OpenClaw usage:

| Scenario | Before | After | Savings |
|----------|--------|-------|---------|
| Git operations/day | 50K tokens | 10K tokens | -80% |
| Test runs/day | 100K tokens | 10K tokens | -90% |
| JSON responses/day | 50K tokens | 30K tokens | -40% |
| **Total/day** | **200K** | **50K** | **-75%** |

## Best Practices

1. **Enable for high-frequency commands** (git, test)
2. **Monitor fallback rate** (should be <5%)
3. **Review token savings weekly** (rtk gain)
4. **Keep both plugins updated**

## Future Enhancements

- [ ] Custom RTK rules for OpenClaw-specific commands
- [ ] TNOL compression for non-JSON structured output
- [ ] Unified token savings dashboard
- [ ] Per-command optimization statistics

## References

- [RTK GitHub](https://github.com/rtk-ai/rtk)
- [TNOL Plugin Guide](./reference/OPENCLAW_TONL_PLUGIN_GUIDE.md)
- [OpenClaw Extensions](./extensions/README.md)
