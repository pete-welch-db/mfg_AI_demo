#!/usr/bin/env python3
"""
Deploy DENSO Genie space(s): Operations and/or Planning.

Usage:
    export DATABRICKS_HOST=https://... DATABRICKS_TOKEN=...
    python scripts/deploy_genie_space.py [--space operations|planning|all]

Default: --space operations. Use --space all to deploy both for the Supervisor.
Output: .genie_space_id (Operations), .genie_planning_space_id (Planning). Add to app/app.yaml.
"""

import argparse
import json
import os
import re
import sys
import uuid
from pathlib import Path


def generate_id():
    """Generate a Databricks-style ID."""
    return uuid.uuid4().hex[:32]


SPACE_CONFIGS = {
    "operations": ("genie_space_config.json", ".genie_space_id", "GENIE_SPACE_ID"),
    "planning": ("genie_planning_config.json", ".genie_planning_space_id", "GENIE_PLANNING_SPACE_ID"),
}


def deploy_one(host, token, repo_root, config_path, catalog, schema, table_prefix, tables, warehouse_id, parent_path):
    """Deploy or update one Genie space. Returns (result_id, config_title) or (None, None)."""
    with open(config_path) as f:
        config = json.load(f)
    # Resolve table identifiers: replace default catalog.schema with deploy-time catalog.schema
    default_prefix = "welch.denso_ai."
    tables = [t.replace(default_prefix, table_prefix) if t.startswith(default_prefix) else t for t in config["tables"]]
    w = __import__("databricks.sdk", fromlist=["WorkspaceClient"]).WorkspaceClient(host=host, token=token)
    sample_queries = config.get("sample_queries", [])
    serialized_space_obj = {
        "version": 2,
        "config": {
            "sample_questions": [{"id": generate_id(), "question": [q["question"]]} for q in sample_queries
        },
        "data_sources": {"tables": [{"identifier": t} for t in sorted(tables)]},
        "instructions": {"text_instructions": [{"id": generate_id(), "content": [config.get("instructions", "")]}]},
    }
    serialized_space = json.dumps(serialized_space_obj)
    import requests
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    existing_space_id = None
    try:
        resp = requests.get(f"{host}/api/2.0/genie/spaces", headers=headers)
        if resp.status_code == 200:
            for space in resp.json().get("spaces", []):
                if space.get("title") == config["title"]:
                    existing_space_id = space.get("space_id")
                    break
    except Exception:
        pass
    try:
        if existing_space_id:
            resp = requests.patch(
                f"{host}/api/2.0/genie/spaces/{existing_space_id}",
                headers=headers,
                json={"title": config["title"], "description": config.get("description", ""), "warehouse_id": warehouse_id, "serialized_space": serialized_space},
            )
            if resp.status_code in [200, 201]:
                return existing_space_id, config["title"]
            raise Exception(f"Update failed: {resp.status_code} - {resp.text}")
        result = w.genie.create_space(
            warehouse_id=warehouse_id, title=config["title"], description=config.get("description", ""),
            parent_path=parent_path, serialized_space=serialized_space,
        )
        result_id = getattr(result, "space_id", None) or getattr(result, "id", None)
        if not result_id:
            match = re.search(r"space_id='([^']+)'", str(result))
            result_id = match.group(1) if match else str(result)
        return result_id, config["title"]
    except Exception as e:
        print(f"Error: {e}")
        return None, config["title"]


def main():
    parser = argparse.ArgumentParser(description="Deploy Operations and/or Planning Genie spaces")
    parser.add_argument("--space", choices=["operations", "planning", "all"], default="operations", help="Which space(s) to deploy")
    args = parser.parse_args()

    host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
    token = os.environ.get("DATABRICKS_TOKEN")
    if not host or not token:
        print("ERROR: DATABRICKS_HOST and DATABRICKS_TOKEN required")
        sys.exit(1)
    try:
        from databricks.sdk import WorkspaceClient
    except ImportError:
        print("ERROR: pip install databricks-sdk")
        sys.exit(1)

    repo_root = Path(__file__).resolve().parent.parent
    catalog = os.environ.get("DEMO_CATALOG", "welch")
    schema = os.environ.get("DEMO_SCHEMA", "denso_ai")
    table_prefix = f"{catalog}.{schema}."
    warehouse_id = os.environ.get("GENIE_WAREHOUSE_ID")
    if not warehouse_id and os.environ.get("DATABRICKS_HTTP_PATH", ""):
        hp = os.environ["DATABRICKS_HTTP_PATH"]
        if "/warehouses/" in hp:
            warehouse_id = hp.split("/warehouses/")[-1].split("/")[0]
    if not warehouse_id:
        print("ERROR: GENIE_WAREHOUSE_ID or DATABRICKS_HTTP_PATH (with /warehouses/...) required")
        sys.exit(1)
    parent_path = os.environ.get("GENIE_PARENT_PATH", "/Shared")

    to_deploy = ["operations", "planning"] if args.space == "all" else [args.space]
    exit_code = 0
    for name in to_deploy:
        config_file, id_file, env_var = SPACE_CONFIGS[name]
        config_path = repo_root / "genie" / config_file
        if not config_path.exists():
            print(f"Skip {name}: {config_path} not found")
            continue
        print(f"Deploying {name}: {config_path.name}")
        result_id, title = deploy_one(host, token, repo_root, config_path, catalog, schema, table_prefix, [], warehouse_id, parent_path)
        if result_id:
            out = repo_root / id_file
            with open(out, "w") as f:
                f.write(result_id)
            print(f"  ✅ {title}: {result_id} -> {out.name}")
            print(f"  Add to app/app.yaml: {env_var}={result_id}")
        else:
            exit_code = 1
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
