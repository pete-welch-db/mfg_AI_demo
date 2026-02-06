# mfg_AI_demo

DENSO North America Manufacturing Operations demo (Databricks Asset Bundle).

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

Ensure `warehouse_id` is set in your bundle variables (or target override) so the dashboard refresh task can run. In CI/CD, add a step after `databricks bundle deploy` that runs `databricks bundle run -t <target> mfg_demo_orchestration` to trigger the pipeline automatically on every deploy.