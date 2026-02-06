# Bundle resources

- **jobs/orchestration.yml** – **Single orchestration job** (`mfg_demo_orchestration`): runs all notebooks in order and refreshes the Operations dashboard. Task order: generate_bronze → run_dlt → ml_training → uc_metrics → lakehouse_monitoring (after DLT), alert_kpi (after uc_metrics), **refresh_dashboard** (dashboard task). Run after deploy via `databricks bundle run -t dev mfg_demo_orchestration` or `./scripts/deploy_and_run.sh dev`. Requires `warehouse_id` for the dashboard refresh task.
- **jobs/alert_kpi.yml** – Scheduled job that runs `06_alerts_kpi`: checks margin < 8%, on-time < 95%, OEE < 75%, writes to `alerts_kpi_breaches` and can drive email/Slack. Set `DEMO_APP_URL` and `DEMO_GENIE_URL` as job env for links in alerts.
- **jobs/lakehouse_monitoring.yml** – Job to create/refresh Lakehouse Monitoring (data profiling) on gold and ml_* tables.
- **jobs/uc_metrics.yml** – Job to create/refresh UC metric views for the dashboard and Genie (run by orchestration after ML).
- **pipelines/dlt_medallion.yml** – DLT pipeline for medallion (bronze → silver → gold).
- **dashboards/operations_dashboard.yml** – Lakeview dashboard (DENSO Operations); refreshed by the orchestration job’s dashboard task.

## Model serving (what-if in app)

The app’s “What-if (model serving)” tab calls a **real-time serving endpoint**. To enable it:

1. In Databricks, deploy the registered model `{catalog}.{schema}.delivery_risk_model` to a **Serving endpoint** (Serving UI or API).
2. Set **SERVING_ENDPOINT_NAME** in the app’s env (or app.yaml) to that endpoint name.

The app will send feature payloads to the endpoint and display P(late delivery).
