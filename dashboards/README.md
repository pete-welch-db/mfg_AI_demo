# Dashboards

AI/BI (Lakeview) dashboard for the DENSO Operations demo.

## Operations dashboard (`operations.lvdash.json`)

- **KPIs (90d)**: OEE %, On-time delivery %, Operating margin %, Delivery risk (avg late probability).
- **Charts**: OEE / On-time / Margin by plant (bar charts with cross-filter on plant).
- **Filters**: Plant single-select; cross-filtering is enabled so clicking a bar filters other widgets.
- **Data**: All datasets query Unity Catalog **metric views** (`mv_plant_oee`, `mv_on_time_delivery`, `mv_plant_margin`, `mv_delivery_risk`) using `MEASURE(...)`.

### Prerequisites

1. **Catalog/schema**: The dashboard SQL uses `welch.denso_ai` by default. To use a different catalog or schema, edit the dataset `queryLines` in `operations.lvdash.json` to replace `welch.denso_ai` with your `catalog.schema` (or override bundle variables and regenerate the dashboard).
2. **Metric views**: Run the **UC metrics** job (or notebook `07_uc_metrics`) so that the four metric views exist. The job is defined in `resources/jobs/uc_metrics.yml` and can be run after the data pipeline and ML training.

### Deploying via the bundle

The bundle resource `resources/dashboards/operations_dashboard.yml` references `../../dashboards/operations.lvdash.json`. Run:

```bash
databricks bundle deploy --target dev
```

to sync the dashboard. Set `warehouse_id` in your bundle variables (or in the dashboard resource) so the dashboard has a SQL warehouse for queries.

## Customizing or replacing the dashboard

1. Create or edit a Lakeview dashboard in the workspace (e.g. add widgets, change layout).
2. Export it: Workspace → dashboard → … → Export (or use API/CLI to get the `.lvdash.json`).
3. Save the file as `operations.lvdash.json` in this folder and redeploy.

If you remove `operations.lvdash.json`, comment out or remove the dashboard resource in `resources/dashboards/operations_dashboard.yml` to avoid bundle errors.
