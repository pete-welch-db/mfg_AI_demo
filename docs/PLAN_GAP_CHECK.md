# Plan vs implementation – gap check

This document compares the codebase to the original DENSO Databricks Presales Demo technical plan. and records what was aligned and what remains optional.

---

## 1) Demo mission

- **Plan:** Director of Manufacturing Operations, NA; single view of plant performance and material flow; improve margin and on-time delivery for electrification programs.
- **Implementation:** Matches; persona and KPIs drive app tabs, Genie instructions, and alerts.

---

## 2) Data model

| Plan requirement | Status |
|------------------|--------|
| Dimensions: dim_plant, dim_product, dim_customer, dim_supplier, dim_date, dim_work_center, dim_carrier | Done in `01_generate_bronze.py` (silver_dim_*). dim_date has date, year, quarter, month, week, day_of_week, is_weekend. |
| Facts: 6 bronze tables with _ingested_at, _source_file or _source_batch_id | Done; notebook uses _ingested_at and _source_batch_id. |
| Gold: gold_plant_oee, gold_on_time_delivery, gold_plant_margin, gold_inventory_lead_time, gold_capacity_utilization | Done in DLT and in `04_gold_metrics.py`. |
| Catalog/schema configurable, never hard-coded | Done (widgets/env in notebooks, config in app, variables in bundle). |

**Optional gap:** Plan says gold_on_time_delivery “by plant, customer/program, **product family**, period”. DLT gold aggregates by plant_id, customer_id, date only (no product_family). Adding product_family would require joining silver_shipments to silver_dim_product in the DLT gold; left as a possible enhancement.

---

## 3) DLT pipeline

| Plan requirement | Status |
|------------------|--------|
| Location pipelines/dlt_medallion.py | Done. |
| Bronze: _ingested_at, _source_file (or _source_batch_id) | Bronze written by notebook with those fields; DLT reads bronze and builds silver/gold. |
| Silver: dedupe, type casting, expectations | Done (valid_plant_id, valid_event_ts, valid_date, order_qty_non_neg, margin_reasonable, etc.). |
| Gold: all five KPI tables | Done. |
| Catalog/schema from pipeline config | Done via spark.conf / env and resources/pipelines/dlt_medallion.yml. |

---

## 4) ML design

| Plan requirement | Status |
|------------------|--------|
| Notebook 03_ml_training.py | Done. |
| Late delivery risk model; MLflow UC registry; catalog.schema.model_name | Done. |
| Score to ml_delivery_risk_predictions (shipment_id, prediction, probability, model_version, scored_at) | Done. |
| ML training job in resources/jobs | Done: `resources/jobs/ml_training.yml`. |

---

## 5) Streamlit app

| Plan requirement | Status |
|------------------|--------|
| app.py routing only; config.py only place env read | Done. |
| app.yaml: DATABRICKS_HOST, HTTP_PATH, CATALOG, SCHEMA, GENIE_SPACE_ID, DASHBOARD_EMBED_URL, USE_MOCK_DATA | Done. |
| data/ with databricks-sql-connector; pd.to_numeric(..., errors="coerce"); mock fallback when USE_MOCK_DATA | Done. |
| Executive summary: margin vs 9–10%, on-time %, **top 3 plants by OEE**, **critical component lead-time alert** | Done: `get_top_plants_oee(3)` and `get_lead_time_alert()` added; executive view shows both. |
| Plant performance (OEE), On-time delivery, Margin by program, Inventory & lead time, Predictions | All present. |

**Optional:** Plan says Plant performance “filters for date range and **program**”. Current view has date filters only; program filter would require joining OEE to product/customer for program_type.

---

## 6) Genie

| Plan requirement | Status |
|------------------|--------|
| Instructions: persona, metrics, tables/joins, 5+ example SQL | Done in genie_space_config.json. |
| Gold + silver dimensions in space; 8–12 sample questions mapped to KPIs | Done (10 sample questions). |
| scripts/deploy_genie_space.py; output GENIE_SPACE_ID | Done; supports --space operations|planning|all. |
| Planning space + Supervisor (multi-agent) | Done: genie_planning_config.json, Supervisor view. |

---

## 7) DAB and deployment

| Plan requirement | Status |
|------------------|--------|
| databricks.yml at root; bundle name, include, variables, sync, resources | Done. Added **app_name** variable and **sync** (notebooks/**, pipelines/**, app/**). |
| Variables: catalog, schema, warehouse_id, **app_name** | Done. |
| resources/jobs: data generation + pipeline run, ML training job | Done: **data_pipeline.yml** (01_generate_bronze → DLT), **ml_training.yml** (03_ml_training). |
| resources/pipelines: DLT with catalog/schema | Done. |
| resources/dashboards (optional) | Done: resources/dashboards/operations_dashboard.yml and dashboards/operations.lvdash.json; see dashboards/README.md. |
| scripts/update_env_ids.sh | Done; includes GENIE_PLANNING_SPACE_ID. |

---

## 8) Acceptance checklist (plan §8)

- **Bundle:** validate and deploy – structure in place; run `databricks bundle validate` and `databricks bundle deploy -t dev`.
- **Catalog/schema:** No hard-coded catalog/schema; all from variables or app config.
- **Medallion:** Bronze has _ingested_at and _source_batch_id; silver deduped/typed; gold matches KPIs.
- **DLT:** Pipeline defines bronze → silver → gold with expectations.
- **ML:** 03_ml_training.py; UC registry; ml_delivery_risk_predictions.
- **App:** app.py routing only; config.py only env; connector + coercion + mock; app.yaml has required keys.
- **Genie:** Gold + silver; instructions and sample questions; GENIE_SPACE_ID (and GENIE_PLANNING_SPACE_ID) for app.
- **Data:** Faker/numpy seeded; dimensions first; referential integrity; realistic patterns.
- **Deployment order:** Steps 1–7 in plan; update_env_ids.sh and app.yaml reminder support post-deploy IDs.

---

## 9) Additional features (plan §9)

- Genie in app (Ask Genie tab) – Done.
- Lakehouse Monitoring – Done (notebook 05 + job).
- Model serving (what-if) – Done (view + serving_client; endpoint via UI/API + SERVING_ENDPOINT_NAME).
- Alerting (margin &lt; 8%, on-time &lt; 95%, OEE &lt; 75%) – Done (06_alerts_kpi + alert job).
- More Genie spaces (Planning) + Supervisor – Done.

---

## Summary of changes made in this gap check

1. **Executive summary:** Added `get_top_plants_oee(limit=3)` and `get_lead_time_alert()`; executive view shows “Top 3 plants by OEE” and “Critical component lead-time alert”.
2. **databricks.yml:** Added **app_name** variable; added **sync** with include for notebooks/**, pipelines/**, app/**.
3. **resources/jobs/data_pipeline.yml:** New job to run 01_generate_bronze then DLT pipeline (data generation + pipeline run).
4. **resources/jobs/ml_training.yml:** New job to run 03_ml_training (ML training job).
5. **Job clusters:** Set spark_env_vars DEMO_CATALOG and DEMO_SCHEMA on data_pipeline and ml_training so %run 00_common_setup gets catalog/schema when widgets are not set.

All plan items are implemented. Optional enhancements (product_family, program filter, dashboards) have been added.
