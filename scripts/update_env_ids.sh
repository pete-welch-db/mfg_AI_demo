#!/bin/bash
# Update .env and app/app.yaml with deployed Genie space ID (and optional dashboard embed URL).
# Run after: databricks bundle deploy, then scripts/deploy_genie_space.py.
#
# Usage: ./scripts/update_env_ids.sh [target]

set -e

TARGET="${1:-dev}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

if [[ -f .env ]]; then
  set -a
  source .env
  set +a
fi

echo "Updating env with deployed resource IDs..."

# Genie space IDs from deploy script output
GENIE_ID=""
GENIE_PLANNING_ID=""
if [[ -f .genie_space_id ]]; then
  GENIE_ID=$(cat .genie_space_id)
  echo "  GENIE_SPACE_ID: $GENIE_ID"
fi
if [[ -f .genie_planning_space_id ]]; then
  GENIE_PLANNING_ID=$(cat .genie_planning_space_id)
  echo "  GENIE_PLANNING_SPACE_ID: $GENIE_PLANNING_ID"
fi

# Optional: dashboard embed URL from bundle summary
DASHBOARD_ID=""
if command -v databricks &>/dev/null; then
  DASHBOARD_ID=$(databricks bundle summary --target "$TARGET" 2>/dev/null | grep -A2 "dashboard" | grep "id:" | awk '{print $2}' | tr -d '"' | head -1 || true)
fi
ORG_ID=""
if [[ -n "$DATABRICKS_HOST" ]]; then
  ORG_ID=$(echo "$DATABRICKS_HOST" | grep -oE 'adb-[0-9]+' | cut -d'-' -f2 || true)
fi
EMBED_URL=""
if [[ -n "$DASHBOARD_ID" && -n "$DATABRICKS_HOST" ]]; then
  if [[ -n "$ORG_ID" ]]; then
    EMBED_URL="${DATABRICKS_HOST}/embed/dashboardsv3/${DASHBOARD_ID}?o=${ORG_ID}"
  else
    EMBED_URL="${DATABRICKS_HOST}/embed/dashboardsv3/${DASHBOARD_ID}"
  fi
fi

update_env() {
  local key=$1
  local value=$2
  [[ -z "$value" ]] && return
  if [[ -f .env ]]; then
    if grep -q "^${key}=" .env 2>/dev/null; then
      if [[ "$(uname)" == "Darwin" ]]; then
        sed -i '' "s|^${key}=.*|${key}=${value}|" .env
      else
        sed -i "s|^${key}=.*|${key}=${value}|" .env
      fi
    else
      echo "${key}=${value}" >> .env
    fi
    echo "  Updated .env: ${key}"
  fi
}

touch .env
[[ -n "$GENIE_ID" ]] && update_env "GENIE_SPACE_ID" "$GENIE_ID"
[[ -n "$GENIE_PLANNING_ID" ]] && update_env "GENIE_PLANNING_SPACE_ID" "$GENIE_PLANNING_ID"
[[ -n "$EMBED_URL" ]] && update_env "DASHBOARD_EMBED_URL" "$EMBED_URL"

# Remind to set app.yaml for Databricks App deploy
if [[ -n "$GENIE_ID" || -n "$GENIE_PLANNING_ID" ]]; then
  echo ""
  echo "For Databricks App deploy, set in app/app.yaml env:"
  [[ -n "$GENIE_ID" ]] && echo "  GENIE_SPACE_ID: \"$GENIE_ID\""
  [[ -n "$GENIE_PLANNING_ID" ]] && echo "  GENIE_PLANNING_SPACE_ID: \"$GENIE_PLANNING_ID\""
  [[ -n "$EMBED_URL" ]] && echo "  DASHBOARD_EMBED_URL: \"$EMBED_URL\""
fi

echo ""
echo "Done."
