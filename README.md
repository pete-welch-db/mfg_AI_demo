# mfg_AI_demo

DENSO North America Manufacturing Operations demo (Databricks Asset Bundle).

## Before you deploy (first-time checklist)

1. **CLI and workspace**  
   Install the [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/index.html) and configure it for your workspace (e.g. `databricks configure --host https://adb-984752964297111.11.azuredatabricks.net`), or set `DATABRICKS_HOST` and `DATABRICKS_TOKEN` (or use OAuth).

2. **SQL warehouse**  
   In the workspace, create a **SQL warehouse** (Serverless or Pro). Copy its **ID** (from the warehouse’s connection details or URL). You’ll need it for the dashboard and app.

3. **Warehouse ID**  
   The dev target is configured with a SQL warehouse ID in `databricks.yml` under `targets.dev.variables.warehouse_id`. Edit that value to use a different warehouse.

4. **Catalog and schema**  
   Defaults are catalog `welch`, schema `denso_ai`. The first job run (via `00_common_setup`) will create them if they don’t exist; your identity needs permission to create catalog/schema (or create `welch` and `denso_ai` in Unity Catalog beforehand).

5. **Validate**  
   Run `databricks bundle validate -t dev` to check the bundle before deploying.

## Deploy and run (automatic pipeline on deploy)

To deploy the bundle and run the full orchestration (data → gold → ML → UC metrics → alerts → **dashboard refresh**) in one go:

```bash
./scripts/deploy_and_run.sh dev
```

Or deploy first, then run the orchestration job from the workspace (Bundle → run **mfg_demo_orchestration**) or CLI:

```bash
databricks bundle deploy -t dev
databricks bundle run -t dev mfg_demo_orchestration
```

The dev target has `warehouse_id` set in config (`targets.dev.variables` in `databricks.yml`) so the dashboard refresh task runs without passing `-v`. In CI/CD, add a step after `databricks bundle deploy` that runs `databricks bundle run -t <target> mfg_demo_orchestration` to trigger the pipeline automatically on every deploy.