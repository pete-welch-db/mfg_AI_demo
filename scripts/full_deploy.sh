#!/usr/bin/env bash
# Validate bundle, deploy (jobs, pipelines, dashboard, app, notebooks), run orchestration job, deploy Genie, update app env.
# Remote workspace path: /Workspace/Users/pete.welch@databricks.com/mfg_AI_demo (set in databricks.yml).
#
# Prereqs: databricks CLI configured (e.g. databricks configure --host https://adb-984752964297111.11.azuredatabricks.net)
# For Genie: set DATABRICKS_HOST and DATABRICKS_TOKEN (or rely on CLI profile), and ensure warehouse exists.
#
# Usage: ./scripts/full_deploy.sh [target]
# Example: ./scripts/full_deploy.sh dev

set -e
TARGET="${1:-dev}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

echo "=== 1. Validate bundle (target=$TARGET) ==="
databricks bundle validate -t "$TARGET"

echo ""
echo "=== 2. Deploy bundle (notebooks, pipelines, jobs, dashboard, app to workspace) ==="
databricks bundle deploy -t "$TARGET" --force

echo ""
echo "=== 3. Run orchestration job (data → gold → ML → UC metrics → alerts → dashboard refresh) ==="
databricks bundle run -t "$TARGET" mfg_demo_orchestration

echo ""
echo "=== 4. Deploy Genie spaces (Operations + Planning) ==="
if [[ -z "$DATABRICKS_HOST" ]]; then
  echo "  DATABRICKS_HOST not set; using CLI profile. Set DATABRICKS_HOST and DATABRICKS_TOKEN if Genie deploy fails."
fi
python3 scripts/deploy_genie_space.py --space all || true

echo ""
echo "=== 5. Update app/app.yaml and .env with Genie (and dashboard) IDs ==="
./scripts/update_env_ids.sh "$TARGET" || true

echo ""
echo "Done. Workspace root: /Workspace/Users/pete.welch@databricks.com/mfg_AI_demo"
echo "  - Jobs (including mfg_demo_orchestration): see Workflows in the workspace"
echo "  - Dashboard: DENSO Operations (Lakeview)"
echo "  - Genie: run scripts/deploy_genie_space.py with DATABRICKS_TOKEN set if step 4 failed"
