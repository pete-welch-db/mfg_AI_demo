# Databricks notebook source
# MAGIC %md
# MAGIC # Lakehouse Monitoring: gold and ml_* tables
# MAGIC Create data profiling monitors for drift and data quality on gold KPI tables and ML prediction tables.

# COMMAND ----------

# MAGIC %run ./00_common_setup

# COMMAND ----------

# Tables to monitor: gold KPIs and ml_* predictions
GOLD_TABLES = [
    "gold_plant_oee",
    "gold_on_time_delivery",
    "gold_plant_margin",
    "gold_inventory_lead_time",
    "gold_capacity_utilization",
]
ML_TABLES = ["ml_delivery_risk_predictions"]
OUTPUT_SCHEMA = f"{catalog}.{schema}"

# COMMAND ----------

# Create monitors using Lakehouse Monitoring (Data Profiling) API.
# Snapshot profile: full table metrics each refresh. Use TimeSeries if you have a timestamp column and want drift over time.
try:
    from databricks.lakehouse_monitoring import create_monitor, Snapshot
except ImportError:
    print("Lakehouse Monitoring not available in this runtime. Use DBR 14+ or install databricks-lakehouse-monitoring.")
    dbutils.notebook.exit("SKIP")

# COMMAND ----------

created = []
for table in GOLD_TABLES + ML_TABLES:
    full_name = f"{catalog}.{schema}.{table}"
    try:
        create_monitor(
            table_name=full_name,
            profile_type=Snapshot(),
            output_schema_name=OUTPUT_SCHEMA,
        )
        created.append(full_name)
        print(f"Created monitor: {full_name}")
    except Exception as e:
        print(f"Skip {full_name}: {e}")

# COMMAND ----------

print(f"Monitors created: {len(created)}. Check Lakehouse Monitoring UI for profiles and drift.")
