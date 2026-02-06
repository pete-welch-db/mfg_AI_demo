#!/usr/bin/env bash
# Deploy the bundle and run the full orchestration job (data → gold → ML → UC metrics → alerts → dashboard refresh).
# Usage: ./scripts/deploy_and_run.sh [target]
# Example: ./scripts/deploy_and_run.sh dev
# Requires: databricks CLI logged in. warehouse_id is read from bundle config (targets.dev.variables in databricks.yml).

set -e
TARGET="${1:-dev}"

echo "Deploying bundle (target=${TARGET})..."
databricks bundle deploy -t "$TARGET" --force

echo "Running orchestration job mfg_demo_orchestration..."
databricks bundle run -t "$TARGET" mfg_demo_orchestration

echo "Done. Open the job run in the workspace to see task progress and the refreshed dashboard."
