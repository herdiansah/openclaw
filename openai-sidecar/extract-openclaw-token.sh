#!/bin/bash
# Extract OpenAI Codex token from OpenClaw auth-profiles.json
# Usage: ./extract-openclaw-token.sh [account-number]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OPENCLAW_AUTH="$HOME/.openclaw/agents/main/agent/auth-profiles.json"
TOKENS_DIR="$SCRIPT_DIR/tokens"

# Check if jq is available
if ! command -v jq &> /dev/null; then
    echo "❌ jq is required. Install with: apt install jq"
    exit 1
fi

# Check if auth file exists
if [ ! -f "$OPENCLAW_AUTH" ]; then
    echo "❌ OpenClaw auth file not found: $OPENCLAW_AUTH"
    exit 1
fi

# Get account number from argument or prompt
ACCOUNT_NUM="${1:-}"
if [ -z "$ACCOUNT_NUM" ]; then
    echo "Which account token to extract? (1-4)"
    read -p "Account number: " ACCOUNT_NUM
fi

if [[ ! "$ACCOUNT_NUM" =~ ^[1-4]$ ]]; then
    echo "❌ Invalid account number. Must be 1-4."
    exit 1
fi

# Ensure tokens directory exists
mkdir -p "$TOKENS_DIR"

TOKEN_FILE="$TOKENS_DIR/account-$ACCOUNT_NUM.json"

echo ""
echo "📄 Extracting OpenAI Codex token from OpenClaw..."
echo "   Source: $OPENCLAW_AUTH"
echo "   Target: $TOKEN_FILE"
echo ""

# Extract openai-codex profile
TOKEN_DATA=$(jq '.["openai-codex"] // .["openai-codex:default"]' "$OPENCLAW_AUTH")

if [ "$TOKEN_DATA" = "null" ] || [ -z "$TOKEN_DATA" ]; then
    echo "❌ No openai-codex token found in auth-profiles.json"
    echo ""
    echo "Make sure you're logged in to OpenAI Codex via OpenClaw:"
    echo "  openclaw models auth login --provider openai-codex"
    exit 1
fi

# Check if it's OAuth or API key
AUTH_TYPE=$(echo "$TOKEN_DATA" | jq -r 'type')

if [ "$AUTH_TYPE" = "string" ]; then
    # It's a simple token string
    echo "✅ Found token (API key format)"
    echo "$TOKEN_DATA" | jq --arg expires "$(($(date +%s)000 + 172800000))" '{
        access: .,
        expires: ($expires | tonumber),
        accountId: "unknown"
    }' > "$TOKEN_FILE"
elif [ "$AUTH_TYPE" = "object" ]; then
    # It's an object with access/expires fields
    ACCESS=$(echo "$TOKEN_DATA" | jq -r '.access // .access_token // .token // empty')
    EXPIRES=$(echo "$TOKEN_DATA" | jq -r '.expires // .expires_at // empty')
    ACCOUNT_ID=$(echo "$TOKEN_DATA" | jq -r '.accountId // .account_id // "unknown"')
    
    if [ -z "$ACCESS" ]; then
        echo "❌ Could not extract access token"
        echo "Raw data:"
        echo "$TOKEN_DATA" | jq .
        exit 1
    fi
    
    # If expires is a date string, convert to milliseconds
    if [[ "$EXPIRES" =~ ^[0-9]+$ ]]; then
        # Already a number, check if it's seconds or milliseconds
        if [ "$EXPIRES" -lt 10000000000 ]; then
            # Seconds, convert to milliseconds
            EXPIRES_MS=$((EXPIRES * 1000))
        else
            EXPIRES_MS="$EXPIRES"
        fi
    else
        # Default to 2 days from now
        EXPIRES_MS=$(($(date +%s)000 + 172800000))
    fi
    
    # Create token file
    jq -n \
        --arg access "$ACCESS" \
        --argjson expires "$EXPIRES_MS" \
        --arg accountId "$ACCOUNT_ID" \
        '{
            access: $access,
            expires: $expires,
            accountId: $accountId
        }' > "$TOKEN_FILE"
    
    echo "✅ Token extracted successfully!"
fi

echo ""
echo "Token saved to: $TOKEN_FILE"
echo ""
echo "Account $ACCOUNT_NUM status:"
node "$SCRIPT_DIR/multi-account-manager.js" health 2>/dev/null | grep "Account $ACCOUNT_NUM" || echo "  (Run: ./openai-multi-wrapper.sh --health)"
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "Next steps:"
echo "1. Switch OpenClaw to next account"
echo "2. Run: ./extract-openclaw-token.sh $((ACCOUNT_NUM + 1))"
echo ""
