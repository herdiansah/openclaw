# OpenClaw Qdrant + Embedding Sidecar Integration

This repository integration adds a **non-breaking sidecar** for memory indexing/search using Qdrant and OpenAI-compatible embeddings.

## Safety
- No API key is committed.
- Runtime secret file is ignored by git.
- Core OpenClaw native memory remains available.

## Added scripts
- `scripts/qdrant-memory-index.mjs`
- `scripts/qdrant-memory-index-if-due.sh`
- `scripts/qdrant-memory-run-from-env.sh`
- `scripts/qdrant-memory-query.mjs`
- `scripts/qdrant-memory-context.sh`
- `qdrant-setup/projects.example.json`

## Env setup
1. Copy template:
```bash
cp qdrant-setup/qdrant-memory.env.example qdrant-setup/qdrant-memory.env
```
2. Fill values in `qdrant-setup/qdrant-memory.env`:
- `OPENCLAW_QDRANT_MEMORY_ENABLED=true`
- `OPENCLAW_QDRANT_URL=http://127.0.0.1:6333`
- `OPENCLAW_QDRANT_EMBEDDING_API_URL=<your endpoint>`
- `OPENCLAW_QDRANT_EMBEDDING_API_KEY=<your key>`
- `OPENCLAW_QDRANT_EMBEDDING_MODEL=<your model>`
- `OPENCLAW_QDRANT_EMBEDDING_DIM=<vector dimension>`

Optional codebase indexing:
- `OPENCLAW_QDRANT_CODE_INDEX_ENABLED=true`
- Copy projects template:
```bash
cp qdrant-setup/projects.example.json qdrant-setup/projects.json
```
- Edit `qdrant-setup/projects.json` with your project paths.

Auto-discover projects (recommended first pass):
```bash
pnpm qdrant:memory:projects:scan
```
This writes `qdrant-setup/projects.json` with discovered project folders (all disabled by default).

## Manual operations
### Index now
```bash
scripts/qdrant-memory-run-from-env.sh
```

### Semantic query
```bash
set -a; source qdrant-setup/qdrant-memory.env; set +a
scripts/qdrant-memory-query.mjs --limit 5 "appbuilder build success"
```

### Semantic query scoped to one project
```bash
set -a; source qdrant-setup/qdrant-memory.env; set +a
scripts/qdrant-memory-query.mjs --kind code --project app-builder --limit 5 "build pipeline"
```

### Context retrieval (policy-aware)
```bash
scripts/qdrant-memory-context.sh "appbuilder build success"
```

## Reversible retrieval policy
In `qdrant-setup/qdrant-memory.env`:
- Vector-first ON:
```env
OPENCLAW_QDRANT_VECTOR_QUERY_FIRST=true
```
- Revert to native-only behavior:
```env
OPENCLAW_QDRANT_VECTOR_QUERY_FIRST=false
```

Optional full disable:
```env
OPENCLAW_QDRANT_MEMORY_ENABLED=false
```

Project-targeted context retrieval:
```env
OPENCLAW_QDRANT_ACTIVE_PROJECT=app-builder
```

## Cron example (optional)
```cron
*/30 * * * * cd /path/to/openclaw && /usr/bin/flock -n /tmp/openclaw-qdrant-index.lock ./scripts/qdrant-memory-run-from-env.sh >> ./memory/cron-qdrant-memory.log 2>&1
```

## Backup & Restore

### Create snapshot
```bash
curl -X POST "http://127.0.0.1:6333/collections/openclaw_memory/snapshots"
```
Snapshots are saved to `/var/lib/qdrant/snapshots/openclaw_memory/`.

Copy to a safe location with a readable name:
```bash
cp /var/lib/qdrant/snapshots/openclaw_memory/<snapshot-name>.snapshot \
   qdrant-setup/backup-<model>-<dim>dim-$(date +%Y%m%d).snapshot
```

### Restore snapshot
```bash
curl -X POST "http://127.0.0.1:6333/collections/openclaw_memory/snapshots/recover" \
  -H "Content-Type: application/json" \
  -d '{"location": "file:///var/lib/qdrant/snapshots/openclaw_memory/<snapshot-name>.snapshot"}'
```

> **Note:** Always create a snapshot before changing embedding model or vector dimension, since the collection must be dropped and recreated — the snapshot is the only way to roll back.

### Current backups
| File | Model | Dim | Date | Points |
|------|-------|-----|------|--------|
| `backup-gemini-3072dim-20260328.snapshot` | gemini-embedding-001 | 3072 | 2026-03-28 | 5150 |

## Verifying the qdrant-auto-context plugin

The `qdrant-auto-context` plugin fires on every agent session. Use these commands to confirm it is loaded and running.

### Check plugin is active (quick)
```bash
grep "qdrant-auto-context" /tmp/openclaw/openclaw-$(date +%Y-%m-%d).log | tail -5 | python3 -c "
import sys, json
for line in sys.stdin:
    try:
        d = json.loads(line)
        print(d['time'], '|', d['1'])
    except:
        print(line.strip()[:120])
"
```
Expected output: `[qdrant-auto-context] active — collection=openclaw_memory limit=5 minScore=0.55`

### Count activations today
```bash
grep "qdrant-auto-context" /tmp/openclaw/openclaw-$(date +%Y-%m-%d).log | wc -l
```

### Follow live as agents are called
```bash
tail -f /tmp/openclaw/openclaw-$(date +%Y-%m-%d).log | grep --line-buffered "qdrant"
```

### Check for errors
```bash
grep -i "qdrant" /tmp/openclaw/openclaw-$(date +%Y-%m-%d).log | grep -i "error\|warn\|fail" | tail -10
```

> **Note:** Log path uses today's date automatically via `$(date +%Y-%m-%d)` — no editing needed day to day.

## Notes
- `scripts/qdrant-memory-index.mjs` handles:
  - deterministic UUID point IDs for Qdrant
  - idempotent collection create (`409 already exists` is accepted)
- Code indexing stores payload metadata:
  - `kind=code`
  - `project_id=<id>`
  - `rel_path=<project-relative-file-path>`
- If embedding model changes dimension, update `OPENCLAW_QDRANT_EMBEDDING_DIM` and re-index.
