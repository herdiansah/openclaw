#!/bin/bash
# OpenAI Sidecar - Multi-Account Wrapper with Load Balancing
# Supports 4 accounts with auto rotation and failover
#
# Usage:
#   ./openai-multi-wrapper.sh "prompt"
#   ./openai-multi-wrapper.sh --account 2 "prompt"
#   ./openai-multi-wrapper.sh --rotate "prompt"
#   ./openai-multi-wrapper.sh --status
#   ./openai-multi-wrapper.sh --setup-all

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MANAGER="$SCRIPT_DIR/multi-account-manager.js"
SIDECAR_JS="$SCRIPT_DIR/openai-sidecar-multi.js"
CONFIG_FILE="$SCRIPT_DIR/config-multi.json"

# Ensure tokens directory exists
mkdir -p "$SCRIPT_DIR/tokens"

# Get token for specific account
get_token() {
    local account_id="$1"
    node "$MANAGER" get-token "$account_id" 2>/dev/null
}

# Select best account based on strategy
select_account() {
    local strategy="${1:-load-balance}"
    node "$MANAGER" select "$strategy" 2>/dev/null | grep "Selected Account:" | awk '{print $3}'
}

# Make API call with specific account
call_api() {
    local account_id="$1"
    local prompt="$2"
    local model="${3:-gpt-4o}"
    
    # Get token
    local token
    token=$(get_token "$account_id")
    
    if [ -z "$token" ]; then
        echo "❌ No token for account $account_id" >&2
        return 1
    fi
    
    # Call OpenAI API via sidecar JS
    OPENAI_ACCOUNT="$account_id" OPENAI_TOKEN="$token" node "$SIDECAR_JS" "$prompt" --model "$model"
}

# Make API call with rotation/failover
call_with_rotation() {
    local prompt="$1"
    local model="${2:-gpt-4o}"
    local strategy="${3:-load-balance}"
    
    # Get config
    if [ ! -f "$CONFIG_FILE" ]; then
        echo "❌ Config not found. Run: $0 --setup-all" >&2
        exit 1
    fi
    
    # Get all enabled accounts
    local accounts
    accounts=$(node -e "
        const config = require('$CONFIG_FILE');
        const enabled = Object.entries(config.accounts)
            .filter(([_, acc]) => acc.enabled)
            .map(([id, _]) => id);
        console.log(enabled.join(' '));
    " 2>/dev/null)
    
    if [ -z "$accounts" ]; then
        echo "❌ No enabled accounts. Run: $0 --setup-all" >&2
        exit 1
    fi
    
    # Select account based on strategy
    local selected
    selected=$(select_account "$strategy")
    
    if [ -z "$selected" ]; then
        echo "❌ Could not select account" >&2
        exit 1
    fi
    
    echo "📡 Using Account $selected (strategy: $strategy)" >&2
    
    # Try the selected account
    if call_api "$selected" "$prompt" "$model"; then
        # Record usage
        node "$MANAGER" reset-quota >/dev/null 2>&1 || true
        return 0
    fi
    
    # Failover: try other accounts
    echo "⚠️ Account $selected failed, trying failover..." >&2
    for acc in $accounts; do
        if [ "$acc" != "$selected" ]; then
            echo "📡 Trying Account $acc..." >&2
            if call_api "$acc" "$prompt" "$model"; then
                return 0
            fi
        fi
    done
    
    echo "❌ All accounts failed" >&2
    return 1
}

# Show status
show_status() {
    node "$MANAGER" status
}

# Setup all accounts
setup_all() {
    echo "🔐 OpenAI Multi-Account Setup"
    echo "=============================="
    echo ""
    echo "This will setup 4 separate OpenAI accounts."
    echo "Each account needs its own OpenAI Pro subscription."
    echo ""
    echo "You will be prompted to login for each account."
    echo "Make sure to use different accounts for each!"
    echo ""
    read -p "Press Enter to start setup..."
    
    node "$MANAGER" setup-all
}

# Show help
show_help() {
    cat << EOF
OpenAI Sidecar - Multi-Account Wrapper
======================================

Usage: $0 [options] <prompt>

Commands:
  --setup-all      Setup all 4 accounts (one-time)
  --status         Show status of all accounts
  --health         Check health of all accounts
  --reset-quota    Reset quota usage counters
  --help           Show this help

Options:
  --account <N>    Use specific account (1-4)
  --rotate         Auto-select account with load balancing
  --strategy <S>   Rotation strategy: load-balance|round-robin|priority|random
  --model <M>      Model to use (default: gpt-4o)

Examples:
  $0 "Hello!"                          # Auto-select best account
  $0 --rotate "Hello!"                 # Same as above (explicit)
  $0 --account 2 "Hello!"              # Use account 2
  $0 --account 3 --model gpt-4o "Hi"   # Account 3 with specific model
  $0 --strategy round-robin "Test"     # Round-robin selection

Agent Integration:
  export OPENAI_ACCOUNT=2
  $0 "prompt"
  
  # Or in sessions_spawn:
  sessions_spawn({
    task: 'Build X',
    cwd: '/root/clawd/openai-sidecar',
    env: { OPENAI_ACCOUNT: '3' }
  })

Setup Flow:
  1. Run: $0 --setup-all
  2. Login to each of the 4 accounts when prompted
  3. Verify: $0 --status
  4. Use: $0 "your prompt"

EOF
}

# Parse arguments
ACCOUNT=""
STRATEGY="load-balance"
MODEL="gpt-4o"
PROMPT=""
ROTATE=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --account)
            ACCOUNT="$2"
            shift 2
            ;;
        --rotate)
            ROTATE=true
            shift
            ;;
        --strategy)
            STRATEGY="$2"
            shift 2
            ;;
        --model)
            MODEL="$2"
            shift 2
            ;;
        --setup-all)
            setup_all
            exit 0
            ;;
        --status)
            show_status
            exit 0
            ;;
        --health)
            node "$MANAGER" health
            exit 0
            ;;
        --reset-quota)
            node "$MANAGER" reset-quota
            exit 0
            ;;
        --help)
            show_help
            exit 0
            ;;
        -*)
            echo "Unknown option: $1" >&2
            show_help
            exit 1
            ;;
        *)
            PROMPT="$PROMPT $1"
            shift
            ;;
    esac
done

# Trim prompt
PROMPT=$(echo "$PROMPT" | xargs)

# Handle commands
if [ -z "$PROMPT" ] && [ -z "$ACCOUNT" ] && [ "$ROTATE" = false ]; then
    show_help
    exit 0
fi

# Execute based on mode
if [ -n "$ACCOUNT" ]; then
    # Specific account
    call_api "$ACCOUNT" "$PROMPT" "$MODEL"
elif [ "$ROTATE" = true ]; then
    # Auto rotation
    call_with_rotation "$PROMPT" "$MODEL" "$STRATEGY"
else
    # Default: auto-select with load balancing
    call_with_rotation "$PROMPT" "$MODEL" "$STRATEGY"
fi
