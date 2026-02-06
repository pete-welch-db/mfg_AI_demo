#!/usr/bin/env bash
# Commit, push, sync workspace Git folder, and deploy the bundle in one command.
#
# Usage:
#   ./scripts/ship.sh "commit message"            # commit + push + sync + deploy
#   ./scripts/ship.sh "commit message" --run       # also run the orchestration job after deploy
#
# Prereqs:
#   - git configured with push access to origin
#   - databricks CLI authenticated (databricks configure or DATABRICKS_HOST/TOKEN env vars)

set -euo pipefail

# ── Args ──────────────────────────────────────────────────────────────────────
COMMIT_MSG="${1:-}"
RUN_JOB=false
TARGET="dev"
WORKSPACE_REPO_PATH="/Workspace/Users/pete.welch@databricks.com/mfg_AI_demo"

for arg in "$@"; do
  case "$arg" in
    --run) RUN_JOB=true ;;
  esac
done

if [[ -z "$COMMIT_MSG" ]]; then
  echo "Usage: ./scripts/ship.sh \"<commit message>\" [--run]"
  exit 1
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

BRANCH="$(git rev-parse --abbrev-ref HEAD)"

# ── 1. Commit & push ─────────────────────────────────────────────────────────
echo "=== 1. Commit & push (branch=${BRANCH}) ==="
git add -A
# If nothing changed, commit exits non-zero and the script stops intentionally.
git commit -m "$COMMIT_MSG"
git push origin "$BRANCH"

# ── 2. Sync Databricks workspace Git folder ──────────────────────────────────
echo ""
echo "=== 2. Sync workspace Git folder to latest commit ==="

# Resolve the Repos object ID from the workspace path.
REPO_ID=""
REPO_ID=$(databricks repos list --output json 2>/dev/null \
  | python3 -c "
import sys, json
data = json.load(sys.stdin)
repos = data.get('repos', data) if isinstance(data, dict) else data
for r in (repos if isinstance(repos, list) else []):
    p = r.get('path', '')
    if p.rstrip('/') == '$WORKSPACE_REPO_PATH'.rstrip('/'):
        print(r['id'])
        sys.exit(0)
sys.exit(1)
" 2>/dev/null) || true

if [[ -n "$REPO_ID" ]]; then
  echo "  Repo ID: $REPO_ID — updating to branch $BRANCH ..."
  databricks repos update "$REPO_ID" --branch "$BRANCH"
  echo "  Workspace Git folder synced."
else
  echo "  WARNING: Could not resolve Databricks Repo at $WORKSPACE_REPO_PATH."
  echo "  Skipping repos sync. Bundle deploy will still push files via sync config."
fi

# ── 3. Bundle deploy ─────────────────────────────────────────────────────────
echo ""
echo "=== 3. Bundle deploy (target=${TARGET}) ==="
databricks bundle deploy -t "$TARGET" --force

# ── 4. (Optional) Run orchestration job ──────────────────────────────────────
if $RUN_JOB; then
  echo ""
  echo "=== 4. Run orchestration job ==="
  databricks bundle run -t "$TARGET" mfg_demo_orchestration
fi

echo ""
echo "Done. Changes committed, pushed, workspace synced, and bundle deployed."
