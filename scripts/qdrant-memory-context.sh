#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${OPENCLAW_QDRANT_ENV_FILE:-$ROOT_DIR/qdrant-setup/qdrant-memory.env}"
QUERY_SCRIPT="$ROOT_DIR/scripts/qdrant-memory-query.mjs"

if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  set -a
  source "$ENV_FILE"
  set +a
fi

QUERY="$*"
if [[ -z "$QUERY" ]]; then
  echo "Usage: scripts/qdrant-memory-context.sh <query>"
  exit 1
fi

VECTOR_QUERY_FIRST="${OPENCLAW_QDRANT_VECTOR_QUERY_FIRST:-false}"
NATIVE_FALLBACK="${OPENCLAW_QDRANT_NATIVE_FALLBACK:-true}"
NATIVE_MAX_HITS="${OPENCLAW_QDRANT_NATIVE_FALLBACK_MAX_HITS:-8}"
LIMIT="${OPENCLAW_QDRANT_QUERY_LIMIT:-5}"

fallback_native() {
  local query="$1"
  local max_hits="$2"
  local files=("$ROOT_DIR/MEMORY.md")
  while IFS= read -r f; do files+=("$f"); done < <(find "$ROOT_DIR/memory" -maxdepth 1 -type f -name '*.md' | sort)

  echo "mode=native-fallback"
  local out
  out="$(rg -n -i -m "$max_hits" -- "$query" "${files[@]}" 2>/dev/null || true)"
  if [[ -z "$out" ]]; then
    local regex
    regex="$(echo "$query" | tr -cs '[:alnum:]' '\n' | awk 'length($0)>=4' | awk '!seen[tolower($0)]++' | paste -sd'|' -)"
    if [[ -n "$regex" ]]; then
      out="$(rg -n -i -m "$max_hits" -e "$regex" "${files[@]}" 2>/dev/null || true)"
    fi
  fi
  if [[ -z "$out" ]]; then
    echo "hits=0"
  else
    echo "$out" | sed "s#^$ROOT_DIR/##"
  fi
}

if [[ "$VECTOR_QUERY_FIRST" == "true" && "${OPENCLAW_QDRANT_MEMORY_ENABLED:-false}" == "true" ]]; then
  if json="$(node "$QUERY_SCRIPT" --json --limit "$LIMIT" "$QUERY" 2>/tmp/qdrant-memory-query.err)"; then
    count="$(node -e 'const d=JSON.parse(process.argv[1]);process.stdout.write(String(d.count||0));' "$json")"
    if (( count > 0 )); then
      echo "mode=vector-first"
      echo "$json" | node -e '
        const d = JSON.parse(require("fs").readFileSync(0, "utf8"));
        for (const [i, r] of d.results.entries()) {
          const score = Number(r.score || 0).toFixed(4);
          const text = String(r.text || "").replace(/\s+/g, " ").trim();
          const short = text.length > 260 ? `${text.slice(0, 260)}...` : text;
          process.stdout.write(`${i + 1}. score=${score} source=${r.source}\n`);
          process.stdout.write(`   ${short}\n`);
        }
      '
      exit 0
    fi
  fi

  if [[ "$NATIVE_FALLBACK" == "true" ]]; then
    fallback_native "$QUERY" "$NATIVE_MAX_HITS"
    exit 0
  fi

  echo "mode=vector-first"
  echo "hits=0"
  exit 0
fi

fallback_native "$QUERY" "$NATIVE_MAX_HITS"
